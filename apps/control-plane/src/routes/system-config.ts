import { createHash } from "node:crypto";
import { Hono, type Context } from "hono";
import type {
  Source,
  Budget,
  SystemConfigBackupBudget,
  SystemConfigBackupPayload,
  SystemConfigBackupSource,
  SystemConfigPackageCreateInput,
  SystemConfigPackageItem,
  SystemConfigPackageListInput,
  SystemConfigPackageListResponse,
  SystemConfigRestoreResult,
} from "../contracts";
import {
  validateSystemConfigPackageCreateInput,
  validateSystemConfigPackageListInput,
  validateSystemConfigRestoreInput,
} from "../contracts";
import type {
  AgentBinding,
  AppendAuditLogInput,
  SourceBinding,
} from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

const SYSTEM_CONFIG_BACKUP_SCHEMA_VERSION = "system-config-backup.v1";
const SYSTEM_CONFIG_PACKAGE_DEFAULT_LIMIT = 50;
const SYSTEM_AGENT_RELEASE_DEFAULT_LIMIT = 20;
const AGENT_RELEASE_CHANNELS = new Set(["stable", "beta", "canary"]);

export const systemConfigRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
type SystemConfigPackageTargetSelectors = {
  agentIds?: string[];
  deviceIds?: string[];
  channels?: string[];
  hostnames?: string[];
};

type SystemConfigPackageRecord = SystemConfigPackageItem & {
  targetSelectors: SystemConfigPackageTargetSelectors;
  requiresApproval: boolean;
  requiredApprovals: 0 | 1 | 2;
  isPublished: boolean;
  publishedAt?: string;
};

type SystemConfigPackageApprovalDecision = "approved" | "rejected";

type SystemConfigPackageApprovalRecord = {
  approvalId: string;
  tenantId: string;
  packageId: string;
  version: string;
  approverUserId: string;
  decision: SystemConfigPackageApprovalDecision;
  comment?: string;
  createdAt: string;
  updatedAt: string;
};

type SystemConfigPackageWatchQuery = {
  agentId?: string;
  deviceId?: string;
  channel?: string;
  hostname?: string;
};

const memorySystemConfigPackages = new Map<string, SystemConfigPackageRecord[]>();
const memorySystemConfigPackageApprovals = new Map<
  string,
  SystemConfigPackageApprovalRecord[]
>();
const memoryAgentReleases = new Map<string, AgentReleaseItem[]>();

type AgentReleaseChannel = "stable" | "beta" | "canary";

interface AgentReleaseArtifact {
  os: string;
  arch: string;
  downloadUrl: string;
  checksumSha256?: string;
  signature?: string;
  signatureAlgorithm?: "ed25519";
  rolloutRing?: string;
  rolloutPercentage?: number;
  minAgentVersion?: string;
  fileName?: string;
  installHint?: string;
}

interface AgentReleaseCreateInput {
  version: string;
  channel?: AgentReleaseChannel;
  notes?: string;
  publishedAt?: string;
  artifacts: AgentReleaseArtifact[];
}

interface AgentReleaseListInput {
  limit?: number;
  channel?: AgentReleaseChannel;
  os?: string;
  arch?: string;
}

interface AgentReleaseItem {
  releaseId: string;
  tenantId: string;
  version: string;
  channel: AgentReleaseChannel;
  notes?: string;
  publishedAt: string;
  artifacts: AgentReleaseArtifact[];
  createdAt: string;
  updatedAt: string;
}

interface AgentReleaseCheckInput {
  currentVersion: string;
  channel: AgentReleaseChannel;
  os: string;
  arch: string;
  agentId?: string;
  deviceId?: string;
  hostname?: string;
  ring: string;
}

interface AgentReleaseCheckResult {
  checkedAt: string;
  currentVersion: string;
  channel: AgentReleaseChannel;
  os: string;
  arch: string;
  updateAvailable: boolean;
  comparison: "upgrade_available" | "up_to_date" | "ahead_of_latest" | "no_release";
  latestRelease: AgentReleaseItem | null;
  selectedArtifact: AgentReleaseArtifact | null;
  instructions: string;
  evaluatedRing?: string;
  rolloutBucket?: number;
  selectionReason?:
    | "matched"
    | "no_candidate"
    | "ring_mismatch"
    | "rollout_percentage_blocked"
    | "min_agent_version_blocked";
}

interface AgentReleaseBatchCheckSampleInput {
  label: string;
  currentVersion: string;
  agentId?: string;
  deviceId?: string;
  hostname?: string;
  ring?: string;
}

interface AgentReleaseBatchCheckInput {
  channel: AgentReleaseChannel;
  os: string;
  arch: string;
  samples: AgentReleaseBatchCheckSampleInput[];
}

interface AgentReleaseBatchCheckResult {
  items: Array<AgentReleaseCheckResult & { label: string }>;
  total: number;
}

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 system-config 审计日志失败。", error);
  }
}

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }

  const auth = c.get("auth");
  if (!auth) {
    return c.json({ message: "未认证：请先登录。" }, 401);
  }
  return auth;
}

function normalizeSourceSignaturePart(value: unknown): string {
  if (typeof value === "string") {
    return value.trim().toLowerCase();
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "";
}

function buildSourceSignature(
  source: Pick<
    SystemConfigBackupSource,
    | "type"
    | "location"
    | "sourceRegion"
    | "accessMode"
    | "syncCron"
    | "syncRetentionDays"
    | "sshConfig"
    | "enabled"
  >
): string {
  const ssh = source.sshConfig;
  return [
    normalizeSourceSignaturePart(source.type),
    normalizeSourceSignaturePart(source.location),
    normalizeSourceSignaturePart(source.sourceRegion),
    normalizeSourceSignaturePart(source.accessMode),
    normalizeSourceSignaturePart(source.syncCron),
    normalizeSourceSignaturePart(source.syncRetentionDays),
    normalizeSourceSignaturePart(source.enabled),
    normalizeSourceSignaturePart(ssh?.host),
    normalizeSourceSignaturePart(ssh?.port),
    normalizeSourceSignaturePart(ssh?.user),
    normalizeSourceSignaturePart(ssh?.authType),
    normalizeSourceSignaturePart(ssh?.keyPath),
    normalizeSourceSignaturePart(ssh?.knownHostsPath),
  ].join("|");
}

function toSystemConfigBackupSource(
  source: SystemConfigBackupSource & {
    id?: string;
    createdAt?: string;
  }
): SystemConfigBackupSource {
  return {
    name: source.name,
    type: source.type,
    location: source.location,
    sourceRegion: source.sourceRegion,
    sshConfig: source.sshConfig,
    accessMode: source.accessMode,
    syncCron: source.syncCron,
    syncRetentionDays: source.syncRetentionDays,
    enabled: source.enabled,
  };
}

function toSystemConfigBackupBudget(budget: Budget): SystemConfigBackupBudget {
  return {
    scope: budget.scope,
    sourceId: budget.sourceId,
    organizationId: budget.organizationId,
    userId: budget.userId,
    model: budget.model,
    period: budget.period,
    tokenLimit: budget.tokenLimit,
    costLimit: budget.costLimit,
    thresholds: budget.thresholds,
    alertThreshold: budget.thresholds.warning,
  };
}

function buildBackupFileName(tenantId: string): string {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  return `system-config-${tenantId}-${timestamp}.json`;
}

type AgentRuntimeStatus = "online" | "stale" | "never_seen";

interface AgentRuntimeDefaults {
  heartbeatIntervalSeconds: number;
  staleAfterSeconds: number;
  ingestProtocol: "http" | "grpc";
  ingestEndpoint?: string;
  sampleGenerateCount: number;
}

interface AgentRuntimeView {
  id: string;
  agentId: string;
  tenantId: string;
  deviceId?: string;
  displayName: string;
  hostname: string;
  version?: string;
  sourceCount: number;
  sourceIds: string[];
  sourceNames: string[];
  runtimeStatus: AgentRuntimeStatus;
  lastHeartbeatAt: string | null;
  lastConfigFetchedAt: string | null;
  lastConfigVersion?: string;
  lastError?: string;
  lastIngestStatusCode: number | null;
  lastAccepted: number;
  lastRejected: number;
  heartbeatIntervalSeconds: number;
  staleAfterSeconds: number;
  ingestProtocol: "http" | "grpc";
  ingestEndpoint?: string;
  updatedAt: string;
}

interface AgentRuntimeConfigResponse {
  tenantId: string;
  agent: {
    agentId: string;
    deviceId?: string;
    hostname: string;
    version?: string;
    displayName: string;
  };
  runtime: AgentRuntimeDefaults;
  bindings: {
    sourceCount: number;
    sourceIds: string[];
    sources: Array<{
      sourceId: string;
      name: string;
      accessMode: string;
      enabled: boolean;
      location: string;
      sourceRegion?: string;
    }>;
  };
  configVersion: string;
  updatedAt: string;
}

interface AgentRuntimeViewListResponse {
  items: AgentRuntimeView[];
  total: number;
  generatedAt: string;
}

interface AgentHeartbeatPayload {
  agentId: string;
  sessionId?: string;
  hostname?: string;
  version?: string;
  daemon: boolean;
  occurredAt: string;
  configVersion?: string;
  configFetchedAt?: string;
  heartbeatIntervalSec?: number;
  ingestProtocol?: "http" | "grpc";
  ingestEndpoint?: string;
  sourceCount: number;
  sourceIds: string[];
  lastIngestStatusCode?: number;
  lastAccepted: number;
  lastRejected: number;
  lastError?: string;
}

function readPositiveIntEnv(name: string, fallback: number): number {
  const rawValue = process.env[name];
  if (!rawValue) {
    return fallback;
  }
  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return parsed;
}

function readIngestProtocolEnv(): "http" | "grpc" {
  const value =
    process.env.AGENT_DAEMON_DEFAULT_INGEST_PROTOCOL?.trim().toLowerCase();
  return value === "grpc" ? "grpc" : "http";
}

function getAgentRuntimeDefaults(): AgentRuntimeDefaults {
  return {
    heartbeatIntervalSeconds: readPositiveIntEnv(
      "AGENT_DAEMON_HEARTBEAT_INTERVAL_SECONDS",
      30
    ),
    staleAfterSeconds: readPositiveIntEnv(
      "AGENT_DAEMON_STALE_AFTER_SECONDS",
      90
    ),
    ingestProtocol: readIngestProtocolEnv(),
    ingestEndpoint: normalizeOptionalString(
      process.env.AGENT_DAEMON_DEFAULT_INGEST_ENDPOINT
    ),
    sampleGenerateCount: readPositiveIntEnv(
      "AGENT_DAEMON_SAMPLE_GENERATE_COUNT",
      5
    ),
  };
}

function toIsoTimeOrNull(value: string | undefined): string | null {
  if (!value) {
    return null;
  }
  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? null : new Date(timestamp).toISOString();
}

function toAgentMetadata(binding: AgentBinding) {
  const metadata = isRecord(binding.metadata) ? binding.metadata : {};
  const hostname =
    normalizeOptionalString(metadata.hostname) ??
    normalizeOptionalString(binding.displayName) ??
    binding.agentId;
  const version = normalizeOptionalString(metadata.version);
  return {
    hostname,
    version,
    displayName: normalizeOptionalString(binding.displayName) ?? hostname,
  };
}

function toAgentRuntimeStatus(
  occurredAt: string | undefined,
  staleAfterSeconds: number,
  now = Date.now()
): AgentRuntimeStatus {
  if (!occurredAt) {
    return "never_seen";
  }
  const occurredAtMs = Date.parse(occurredAt);
  if (Number.isNaN(occurredAtMs)) {
    return "stale";
  }
  return now - occurredAtMs > staleAfterSeconds * 1000 ? "stale" : "online";
}

function buildAgentConfigVersion(
  agent: AgentBinding,
  sourceBindings: SourceBinding[],
  sources: Source[],
  defaults: AgentRuntimeDefaults
): string {
  const signature = [
    agent.updatedAt,
    defaults.heartbeatIntervalSeconds,
    defaults.staleAfterSeconds,
    defaults.ingestProtocol,
    defaults.ingestEndpoint ?? "",
    defaults.sampleGenerateCount,
    ...sourceBindings
      .slice()
      .sort((left, right) => left.sourceId.localeCompare(right.sourceId))
      .map((binding) =>
        [
          binding.sourceId,
          binding.accessMode,
          binding.bindingType,
          binding.updatedAt,
        ].join(":")
      ),
    ...sources
      .slice()
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((source) =>
        [
          source.id,
          source.createdAt,
          source.accessMode ?? "",
          source.enabled ? "1" : "0",
          source.location,
          source.sourceRegion ?? "",
        ].join(":")
      ),
  ].join("|");
  return `cfg:${createHash("sha256").update(signature).digest("hex").slice(0, 16)}`;
}

function buildAgentConfigUpdatedAt(
  agent: AgentBinding,
  sourceBindings: SourceBinding[],
  sources: Source[]
): string {
  const candidates = [
    agent.updatedAt,
    ...sourceBindings.map((item) => item.updatedAt),
    ...sources.map((item) => item.createdAt),
  ]
    .map((item) => Date.parse(item))
    .filter((item) => Number.isFinite(item));
  if (candidates.length === 0) {
    return new Date().toISOString();
  }
  return new Date(Math.max(...candidates)).toISOString();
}

async function buildAgentRuntimeConfigResponse(
  tenantId: string,
  agentId: string
): Promise<AgentRuntimeConfigResponse | null> {
  const [agentBindings, sourceBindings, sources] = await Promise.all([
    repository.listAgentBindings(tenantId),
    repository.listSourceBindings(tenantId),
    repository.listSources(tenantId),
  ]);
  const agent = agentBindings.find((item) => item.agentId === agentId);
  if (!agent) {
    return null;
  }

  const boundSourceBindings = sourceBindings.filter(
    (item) => item.agentId === agentId
  );
  const sourceIdSet = new Set(boundSourceBindings.map((item) => item.sourceId));
  const boundSources = sources.filter((item) => sourceIdSet.has(item.id));
  const defaults = getAgentRuntimeDefaults();
  const metadata = toAgentMetadata(agent);

  return {
    tenantId,
    agent: {
      agentId: agent.agentId,
      deviceId: agent.deviceId ?? undefined,
      hostname: metadata.hostname,
      version: metadata.version,
      displayName: metadata.displayName,
    },
    runtime: defaults,
    bindings: {
      sourceCount: boundSourceBindings.length,
      sourceIds: boundSourceBindings.map((item) => item.sourceId),
      sources: boundSourceBindings.map((binding) => {
        const source = boundSources.find((item) => item.id === binding.sourceId);
        return {
          sourceId: binding.sourceId,
          name: source?.name ?? binding.sourceId,
          accessMode: String(
            source?.accessMode ?? binding.accessMode ?? "realtime"
          ),
          enabled: source?.enabled ?? true,
          location: source?.location ?? "",
          sourceRegion: source?.sourceRegion,
        };
      }),
    },
    configVersion: buildAgentConfigVersion(
      agent,
      boundSourceBindings,
      boundSources,
      defaults
    ),
    updatedAt: buildAgentConfigUpdatedAt(agent, boundSourceBindings, boundSources),
  };
}

async function listAgentRuntimeViewsForTenant(
  tenantId: string
): Promise<AgentRuntimeView[]> {
  const [agentBindings, sourceBindings, sources, heartbeats] = await Promise.all([
    repository.listAgentBindings(tenantId),
    repository.listSourceBindings(tenantId),
    repository.listSources(tenantId),
    repository.listAgentRuntimeHeartbeats(tenantId),
  ]);
  const heartbeatByAgentId = new Map(
    heartbeats.map((item) => [item.agentId, item] as const)
  );
  const defaults = getAgentRuntimeDefaults();

  return agentBindings.map((binding) => {
    const metadata = toAgentMetadata(binding);
    const boundSourceBindings = sourceBindings.filter(
      (item) => item.agentId === binding.agentId
    );
    const sourceIdSet = new Set(boundSourceBindings.map((item) => item.sourceId));
    const boundSources = sources.filter((item) => sourceIdSet.has(item.id));
    const heartbeat = heartbeatByAgentId.get(binding.agentId);
    return {
      id: binding.agentId,
      agentId: binding.agentId,
      tenantId,
      deviceId: binding.deviceId ?? undefined,
      displayName: metadata.displayName,
      hostname: heartbeat?.hostname ?? metadata.hostname,
      version: heartbeat?.version ?? metadata.version,
      sourceCount: boundSourceBindings.length,
      sourceIds: boundSourceBindings.map((item) => item.sourceId),
      sourceNames: boundSources.map((item) => item.name),
      runtimeStatus: toAgentRuntimeStatus(
        heartbeat?.occurredAt,
        defaults.staleAfterSeconds
      ),
      lastHeartbeatAt: toIsoTimeOrNull(heartbeat?.occurredAt),
      lastConfigFetchedAt: toIsoTimeOrNull(heartbeat?.configFetchedAt),
      lastConfigVersion: normalizeOptionalString(heartbeat?.configVersion),
      lastError: normalizeOptionalString(heartbeat?.lastError),
      lastIngestStatusCode:
        typeof heartbeat?.lastIngestStatusCode === "number"
          ? heartbeat.lastIngestStatusCode
          : null,
      lastAccepted: heartbeat?.lastAccepted ?? 0,
      lastRejected: heartbeat?.lastRejected ?? 0,
      heartbeatIntervalSeconds:
        heartbeat?.heartbeatIntervalSec ?? defaults.heartbeatIntervalSeconds,
      staleAfterSeconds: defaults.staleAfterSeconds,
      ingestProtocol:
        heartbeat?.ingestProtocol === "grpc" ? "grpc" : defaults.ingestProtocol,
      ingestEndpoint:
        normalizeOptionalString(heartbeat?.ingestEndpoint) ??
        defaults.ingestEndpoint,
      updatedAt: heartbeat?.updatedAt ?? binding.updatedAt,
    };
  });
}

function normalizeHeartbeatPayload(body: unknown): AgentHeartbeatPayload | null {
  if (!isRecord(body)) {
    return null;
  }
  const agentId = normalizeOptionalString(body.agentId);
  const occurredAt = normalizeOptionalString(body.occurredAt);
  if (!agentId || !occurredAt || Number.isNaN(Date.parse(occurredAt))) {
    return null;
  }
  const sourceIds = Array.isArray(body.sourceIds)
    ? body.sourceIds
        .map((item) => normalizeOptionalString(item))
        .filter((item): item is string => Boolean(item))
    : [];
  const sourceCount =
    typeof body.sourceCount === "number" &&
    Number.isInteger(body.sourceCount) &&
    body.sourceCount >= 0
      ? body.sourceCount
      : sourceIds.length;
  const lastIngestStatusCode =
    typeof body.lastIngestStatusCode === "number" &&
    Number.isInteger(body.lastIngestStatusCode) &&
    body.lastIngestStatusCode >= 0
      ? body.lastIngestStatusCode
      : undefined;
  const lastAccepted =
    typeof body.lastAccepted === "number" &&
    Number.isInteger(body.lastAccepted) &&
    body.lastAccepted >= 0
      ? body.lastAccepted
      : 0;
  const lastRejected =
    typeof body.lastRejected === "number" &&
    Number.isInteger(body.lastRejected) &&
    body.lastRejected >= 0
      ? body.lastRejected
      : 0;
  const heartbeatIntervalSec =
    typeof body.heartbeatIntervalSec === "number" &&
    Number.isInteger(body.heartbeatIntervalSec) &&
    body.heartbeatIntervalSec > 0
      ? body.heartbeatIntervalSec
      : undefined;
  const ingestProtocol =
    normalizeOptionalString(body.ingestProtocol) === "grpc" ? "grpc" : "http";

  return {
    agentId,
    sessionId: normalizeOptionalString(body.sessionId),
    hostname: normalizeOptionalString(body.hostname),
    version: normalizeOptionalString(body.version),
    daemon: body.daemon === true,
    occurredAt,
    configVersion: normalizeOptionalString(body.configVersion),
    configFetchedAt: normalizeOptionalString(body.configFetchedAt),
    heartbeatIntervalSec,
    ingestProtocol,
    ingestEndpoint: normalizeOptionalString(body.ingestEndpoint),
    sourceCount,
    sourceIds,
    lastIngestStatusCode,
    lastAccepted,
    lastRejected,
    lastError: normalizeOptionalString(body.lastError),
  };
}

function cloneSystemConfigPackage(
  item: SystemConfigPackageRecord
): SystemConfigPackageRecord {
  return {
    ...item,
    payload: JSON.parse(JSON.stringify(item.payload)) as Record<string, unknown>,
    targetSelectors: JSON.parse(
      JSON.stringify(item.targetSelectors),
    ) as SystemConfigPackageTargetSelectors,
  };
}

function cloneSystemConfigPackageApproval(
  item: SystemConfigPackageApprovalRecord
): SystemConfigPackageApprovalRecord {
  return { ...item };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeOptionalString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function normalizeAgentReleaseChannel(
  value: unknown,
  fallback: AgentReleaseChannel = "stable"
): AgentReleaseChannel | "invalid" {
  const normalized = normalizeOptionalString(value)?.toLowerCase();
  if (!normalized) {
    return fallback;
  }
  return AGENT_RELEASE_CHANNELS.has(normalized)
    ? (normalized as AgentReleaseChannel)
    : "invalid";
}

function normalizeAgentReleaseTarget(value: unknown): string | undefined {
  return normalizeOptionalString(value)?.toLowerCase();
}

function isIsoDatetime(value: string): boolean {
  return !Number.isNaN(Date.parse(value));
}

function cloneAgentReleaseArtifact(
  item: AgentReleaseArtifact
): AgentReleaseArtifact {
  return { ...item };
}

function cloneAgentRelease(item: AgentReleaseItem): AgentReleaseItem {
  return {
    ...item,
    artifacts: item.artifacts.map(cloneAgentReleaseArtifact),
  };
}

function validateAgentReleaseArtifacts(
  rawArtifacts: unknown
):
  | { success: true; data: AgentReleaseArtifact[] }
  | { success: false; error: string } {
  if (!Array.isArray(rawArtifacts) || rawArtifacts.length === 0) {
    return {
      success: false,
      error: "artifacts 必须是非空数组。",
    };
  }
  const artifacts: AgentReleaseArtifact[] = [];
  for (const [index, item] of rawArtifacts.entries()) {
    if (!isRecord(item)) {
      return {
        success: false,
        error: `artifacts[${index}] 必须是对象。`,
      };
    }
    const os = normalizeAgentReleaseTarget(item.os);
    const arch = normalizeAgentReleaseTarget(item.arch);
    const downloadUrl = normalizeOptionalString(
      item.downloadUrl ?? item.download_url
    );
    const checksumSha256 = normalizeOptionalString(
      item.checksumSha256 ?? item.checksum_sha256
    );
    const signature = normalizeOptionalString(item.signature);
    const signatureAlgorithm = normalizeOptionalString(
      item.signatureAlgorithm ?? item.signature_algorithm
    )?.toLowerCase();
    const rolloutRing = normalizeOptionalString(
      item.rolloutRing ?? item.rollout_ring
    );
    const rolloutPercentageValue =
      item.rolloutPercentage ?? item.rollout_percentage;
    const minAgentVersion = normalizeOptionalString(
      item.minAgentVersion ?? item.min_agent_version
    );
    const fileName = normalizeOptionalString(item.fileName ?? item.file_name);
    const installHint = normalizeOptionalString(
      item.installHint ?? item.install_hint
    );
    if (!os) {
      return {
        success: false,
        error: `artifacts[${index}].os 必须是非空字符串。`,
      };
    }
    if (!arch) {
      return {
        success: false,
        error: `artifacts[${index}].arch 必须是非空字符串。`,
      };
    }
    if (!downloadUrl) {
      return {
        success: false,
        error: `artifacts[${index}].downloadUrl 必须是非空字符串。`,
      };
    }
    try {
      const parsed = new URL(downloadUrl);
      if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
        return {
          success: false,
          error: `artifacts[${index}].downloadUrl 仅支持 http/https。`,
        };
      }
    } catch {
      return {
        success: false,
        error: `artifacts[${index}].downloadUrl 必须是合法 URL。`,
      };
    }
    if ((signature && !signatureAlgorithm) || (!signature && signatureAlgorithm)) {
      return {
        success: false,
        error: `artifacts[${index}] 的 signature 与 signatureAlgorithm 必须同时提供。`,
      };
    }
    if (signatureAlgorithm && signatureAlgorithm !== "ed25519") {
      return {
        success: false,
        error: `artifacts[${index}].signatureAlgorithm 仅支持 ed25519。`,
      };
    }
    let rolloutPercentage: number | undefined;
    if (rolloutPercentageValue !== undefined) {
      if (
        typeof rolloutPercentageValue !== "number" ||
        !Number.isInteger(rolloutPercentageValue) ||
        rolloutPercentageValue < 0 ||
        rolloutPercentageValue > 100
      ) {
        return {
          success: false,
          error: `artifacts[${index}].rolloutPercentage 必须是 0-100 的整数。`,
        };
      }
      rolloutPercentage = rolloutPercentageValue;
    }
    artifacts.push({
      os,
      arch,
      downloadUrl,
      checksumSha256,
      signature,
      signatureAlgorithm:
        signatureAlgorithm === "ed25519" ? "ed25519" : undefined,
      rolloutRing,
      rolloutPercentage,
      minAgentVersion,
      fileName,
      installHint,
    });
  }
  return { success: true, data: artifacts };
}

function validateAgentReleaseCreateInput(
  input: unknown
):
  | { success: true; data: AgentReleaseCreateInput }
  | { success: false; error: string } {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }
  const version = normalizeOptionalString(input.version);
  if (!version) {
    return { success: false, error: "version 必填且必须为非空字符串。" };
  }
  const channel = normalizeAgentReleaseChannel(input.channel);
  if (channel === "invalid") {
    return {
      success: false,
      error: "channel 仅支持 stable/beta/canary。",
    };
  }
  const notes = normalizeOptionalString(input.notes);
  if (input.notes !== undefined && !notes) {
    return { success: false, error: "notes 必须是非空字符串。" };
  }
  const publishedAt = normalizeOptionalString(
    input.publishedAt ?? input.published_at
  );
  if (publishedAt !== undefined && !isIsoDatetime(publishedAt)) {
    return {
      success: false,
      error: "publishedAt 必须是 ISO 日期字符串。",
    };
  }
  const artifactsResult = validateAgentReleaseArtifacts(input.artifacts);
  if (!artifactsResult.success) {
    return artifactsResult;
  }
  return {
    success: true,
    data: {
      version,
      channel,
      notes,
      publishedAt,
      artifacts: artifactsResult.data,
    },
  };
}

function parsePositiveInteger(value: unknown): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const raw = normalizeOptionalString(value);
  if (!raw) {
    return undefined;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed)) {
    return Number.NaN;
  }
  return parsed;
}

function validateAgentReleaseListInput(
  input: unknown
):
  | { success: true; data: AgentReleaseListInput }
  | { success: false; error: string } {
  if (input !== undefined && !isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }
  const raw = (input as Record<string, unknown> | undefined) ?? {};
  const limit = parsePositiveInteger(raw.limit);
  if (
    limit !== undefined &&
    (!Number.isInteger(limit) || limit < 1 || limit > 200)
  ) {
    return { success: false, error: "limit 必须是 1 到 200 的整数。" };
  }
  const channel = normalizeAgentReleaseChannel(raw.channel, "stable");
  if (raw.channel !== undefined && channel === "invalid") {
    return {
      success: false,
      error: "channel 仅支持 stable/beta/canary。",
    };
  }
  const os = normalizeAgentReleaseTarget(raw.os);
  if (raw.os !== undefined && !os) {
    return { success: false, error: "os 必须是非空字符串。" };
  }
  const arch = normalizeAgentReleaseTarget(raw.arch);
  if (raw.arch !== undefined && !arch) {
    return { success: false, error: "arch 必须是非空字符串。" };
  }
  return {
    success: true,
    data: {
      limit,
      channel:
        raw.channel === undefined || channel === "invalid" ? undefined : channel,
      os,
      arch,
    },
  };
}

function validateAgentReleaseCheckInput(
  input: unknown
):
  | { success: true; data: AgentReleaseCheckInput }
  | { success: false; error: string } {
  if (!isRecord(input)) {
    return { success: false, error: "查询参数必须是对象。" };
  }
  const currentVersion = normalizeOptionalString(
    input.currentVersion ?? input.current_version
  );
  if (!currentVersion) {
    return {
      success: false,
      error: "currentVersion 必填且必须为非空字符串。",
    };
  }
  const channel = normalizeAgentReleaseChannel(input.channel);
  if (channel === "invalid") {
    return {
      success: false,
      error: "channel 仅支持 stable/beta/canary。",
    };
  }
  const os = normalizeAgentReleaseTarget(input.os);
  if (!os) {
    return { success: false, error: "os 必填且必须为非空字符串。" };
  }
  const arch = normalizeAgentReleaseTarget(input.arch);
  if (!arch) {
    return { success: false, error: "arch 必填且必须为非空字符串。" };
  }
  const agentId = normalizeOptionalString(input.agentId ?? input.agent_id);
  if (
    (input.agentId !== undefined || input.agent_id !== undefined) &&
    !agentId
  ) {
    return { success: false, error: "agentId 必须为非空字符串。" };
  }
  const deviceId = normalizeOptionalString(input.deviceId ?? input.device_id);
  if (
    (input.deviceId !== undefined || input.device_id !== undefined) &&
    !deviceId
  ) {
    return { success: false, error: "deviceId 必须为非空字符串。" };
  }
  const hostname = normalizeOptionalString(input.hostname)?.toLowerCase();
  if (input.hostname !== undefined && !hostname) {
    return { success: false, error: "hostname 必须为非空字符串。" };
  }
  const ring = normalizeOptionalString(input.ring)?.toLowerCase() ?? "stable";
  return {
    success: true,
    data: {
      currentVersion,
      channel,
      os,
      arch,
      agentId,
      deviceId,
      hostname,
      ring,
    },
  };
}

function validateAgentReleaseBatchCheckInput(
  input: unknown,
):
  | { success: true; data: AgentReleaseBatchCheckInput }
  | { success: false; error: string } {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }
  const channel = normalizeAgentReleaseChannel(input.channel);
  if (channel === "invalid") {
    return { success: false, error: "channel 仅支持 stable/beta/canary。" };
  }
  const os = normalizeAgentReleaseTarget(input.os);
  if (!os) {
    return { success: false, error: "os 必填且必须为非空字符串。" };
  }
  const arch = normalizeAgentReleaseTarget(input.arch);
  if (!arch) {
    return { success: false, error: "arch 必填且必须为非空字符串。" };
  }
  if (!Array.isArray(input.samples) || input.samples.length === 0) {
    return { success: false, error: "samples 必须是非空数组。" };
  }
  const samples: AgentReleaseBatchCheckSampleInput[] = [];
  for (const [index, sample] of input.samples.entries()) {
    if (!isRecord(sample)) {
      return { success: false, error: `samples[${index}] 必须是对象。` };
    }
    const label = normalizeOptionalString(sample.label);
    if (!label) {
      return { success: false, error: `samples[${index}].label 必填。` };
    }
    const currentVersion = normalizeOptionalString(
      sample.currentVersion ?? sample.current_version,
    );
    if (!currentVersion) {
      return {
        success: false,
        error: `samples[${index}].currentVersion 必填且必须为非空字符串。`,
      };
    }
    const agentId = normalizeOptionalString(sample.agentId ?? sample.agent_id);
    const deviceId = normalizeOptionalString(sample.deviceId ?? sample.device_id);
    const hostname = normalizeOptionalString(sample.hostname)?.toLowerCase();
    const ring = normalizeOptionalString(sample.ring)?.toLowerCase();
    samples.push({
      label,
      currentVersion,
      agentId,
      deviceId,
      hostname,
      ring,
    });
  }
  return {
    success: true,
    data: {
      channel,
      os,
      arch,
      samples,
    },
  };
}

interface ParsedAgentReleaseVersion {
  major: number;
  minor: number;
  patch: number;
  prerelease: string[];
}

function parseAgentReleaseVersion(
  value: string
): ParsedAgentReleaseVersion | null {
  const normalized = value.trim().replace(/^v/i, "");
  const [withoutBuildMetadata] = normalized.split("+", 1);
  const [corePart, prereleasePart = ""] = withoutBuildMetadata.split("-", 2);
  const coreSegments = corePart.split(".");
  if (coreSegments.length === 0 || coreSegments.length > 3) {
    return null;
  }
  const core = [0, 0, 0];
  for (const [index, segment] of coreSegments.entries()) {
    if (!/^\d+$/.test(segment ?? "")) {
      return null;
    }
    core[index] = Number(segment);
  }
  const prerelease = prereleasePart
    .split(".")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
  return {
    major: core[0] ?? 0,
    minor: core[1] ?? 0,
    patch: core[2] ?? 0,
    prerelease,
  };
}

function compareAgentReleaseVersions(a: string, b: string): number {
  const parsedA = parseAgentReleaseVersion(a);
  const parsedB = parseAgentReleaseVersion(b);
  if (!parsedA || !parsedB) {
    return a.localeCompare(b);
  }
  const coreCompare =
    parsedA.major - parsedB.major ||
    parsedA.minor - parsedB.minor ||
    parsedA.patch - parsedB.patch;
  if (coreCompare !== 0) {
    return coreCompare;
  }
  if (parsedA.prerelease.length === 0 && parsedB.prerelease.length === 0) {
    return 0;
  }
  if (parsedA.prerelease.length === 0) {
    return 1;
  }
  if (parsedB.prerelease.length === 0) {
    return -1;
  }
  const maxLength = Math.max(
    parsedA.prerelease.length,
    parsedB.prerelease.length
  );
  for (let index = 0; index < maxLength; index += 1) {
    const left = parsedA.prerelease[index];
    const right = parsedB.prerelease[index];
    if (left === right) {
      continue;
    }
    if (left === undefined) {
      return -1;
    }
    if (right === undefined) {
      return 1;
    }
    const leftNumeric = /^\d+$/.test(left);
    const rightNumeric = /^\d+$/.test(right);
    if (leftNumeric && rightNumeric) {
      return Number(left) - Number(right);
    }
    if (leftNumeric) {
      return -1;
    }
    if (rightNumeric) {
      return 1;
    }
    const lexical = left.localeCompare(right);
    if (lexical !== 0) {
      return lexical;
    }
  }
  return 0;
}

function sortAgentReleases(items: AgentReleaseItem[]): AgentReleaseItem[] {
  return items.slice().sort((left, right) => {
    const versionCompare = compareAgentReleaseVersions(
      right.version,
      left.version
    );
    if (versionCompare !== 0) {
      return versionCompare;
    }
    const publishedCompare = right.publishedAt.localeCompare(left.publishedAt);
    if (publishedCompare !== 0) {
      return publishedCompare;
    }
    return right.createdAt.localeCompare(left.createdAt);
  });
}

function createAgentReleaseRecord(
  tenantId: string,
  input: AgentReleaseCreateInput
): AgentReleaseItem {
  const now = new Date().toISOString();
  return {
    releaseId: crypto.randomUUID(),
    tenantId,
    version: input.version,
    channel: input.channel ?? "stable",
    notes: input.notes,
    publishedAt: input.publishedAt ?? now,
    artifacts: input.artifacts.map(cloneAgentReleaseArtifact),
    createdAt: now,
    updatedAt: now,
  };
}

function saveAgentRelease(
  tenantId: string,
  input: AgentReleaseCreateInput
): AgentReleaseItem {
  const currentItems = memoryAgentReleases.get(tenantId) ?? [];
  const record = createAgentReleaseRecord(tenantId, input);
  memoryAgentReleases.set(tenantId, [record, ...currentItems]);
  return cloneAgentRelease(record);
}

function listAgentReleases(
  tenantId: string,
  input: AgentReleaseListInput
): {
  items: AgentReleaseItem[];
  total: number;
  filters: AgentReleaseListInput;
} {
  const limit = input.limit ?? SYSTEM_AGENT_RELEASE_DEFAULT_LIMIT;
  const filteredAll = sortAgentReleases(memoryAgentReleases.get(tenantId) ?? [])
    .filter((item) => {
      if (input.channel && item.channel !== input.channel) {
        return false;
      }
      if (!input.os && !input.arch) {
        return true;
      }
      return item.artifacts.some(
        (artifact) =>
          (!input.os || artifact.os === input.os) &&
          (!input.arch || artifact.arch === input.arch)
      );
    });
  const filteredItems = filteredAll.slice(0, limit).map(cloneAgentRelease);
  return {
    items: filteredItems,
    total: filteredAll.length,
    filters: {
      limit,
      channel: input.channel,
      os: input.os,
      arch: input.arch,
    },
  };
}

function getAgentReleaseById(
  tenantId: string,
  releaseId: string
): AgentReleaseItem | null {
  const item = (memoryAgentReleases.get(tenantId) ?? []).find(
    (candidate) => candidate.releaseId === releaseId
  );
  return item ? cloneAgentRelease(item) : null;
}

function findMatchingAgentReleaseArtifact(
  release: AgentReleaseItem,
  input: AgentReleaseCheckInput,
  tenantId: string
): AgentReleaseArtifact | null {
  const rolloutBucket = computeAgentReleaseRolloutBucket(tenantId, input);
  const artifact = release.artifacts.find((candidate) => {
    if (candidate.os !== input.os || candidate.arch !== input.arch) {
      return false;
    }
    return evaluateAgentReleaseArtifactSelection(
      candidate,
      input,
      tenantId,
      rolloutBucket,
    ).matched;
  });
  return artifact ? cloneAgentReleaseArtifact(artifact) : null;
}

function computeAgentReleaseRolloutBucket(
  tenantId: string,
  input: Pick<
    AgentReleaseCheckInput,
    "agentId" | "deviceId" | "hostname" | "currentVersion"
  >
): number {
  const seed = `${tenantId}:${input.deviceId || input.agentId || input.hostname || input.currentVersion}`;
  const digest = createHash("sha256").update(seed).digest("hex");
  return Number.parseInt(digest.slice(0, 8), 16) % 100;
}

function evaluateAgentReleaseArtifactSelection(
  artifact: AgentReleaseArtifact,
  input: AgentReleaseCheckInput,
  tenantId: string,
  rolloutBucket: number,
):
  | { matched: true }
  | {
      matched: false;
      reason:
        | "ring_mismatch"
        | "rollout_percentage_blocked"
        | "min_agent_version_blocked";
    } {
  if (
    artifact.minAgentVersion &&
    compareAgentReleaseVersions(input.currentVersion, artifact.minAgentVersion) < 0
  ) {
    return { matched: false, reason: "min_agent_version_blocked" };
  }
  if (
    artifact.rolloutRing &&
    artifact.rolloutRing.trim().toLowerCase() !== input.ring
  ) {
    return { matched: false, reason: "ring_mismatch" };
  }
  if (
    artifact.rolloutPercentage !== undefined &&
    artifact.rolloutPercentage < 100 &&
    rolloutBucket >= artifact.rolloutPercentage
  ) {
    return { matched: false, reason: "rollout_percentage_blocked" };
  }
  return { matched: true };
}

function checkAgentReleaseUpdate(
  tenantId: string,
  input: AgentReleaseCheckInput
): AgentReleaseCheckResult {
  const rolloutBucket = computeAgentReleaseRolloutBucket(tenantId, input);
  const latestRelease =
    sortAgentReleases(memoryAgentReleases.get(tenantId) ?? []).find((item) => {
      if (item.channel !== input.channel) {
        return false;
      }
      return findMatchingAgentReleaseArtifact(item, input, tenantId) !== null;
    }) ?? null;

  const instructions = "当前仅提供升级检查结果，不执行真实下载升级。";
  if (!latestRelease) {
    const releaseCandidates = sortAgentReleases(
      memoryAgentReleases.get(tenantId) ?? [],
    ).filter((item) => item.channel === input.channel);
    let selectionReason: AgentReleaseCheckResult["selectionReason"] = "no_candidate";
    for (const item of releaseCandidates) {
      const candidates = item.artifacts.filter(
        (artifact) => artifact.os === input.os && artifact.arch === input.arch,
      );
      for (const artifact of candidates) {
        const evaluation = evaluateAgentReleaseArtifactSelection(
          artifact,
          input,
          tenantId,
          rolloutBucket,
        );
        if (!evaluation.matched) {
          selectionReason = evaluation.reason;
          break;
        }
      }
      if (selectionReason !== "no_candidate") {
        break;
      }
    }
    return {
      checkedAt: new Date().toISOString(),
      currentVersion: input.currentVersion,
      channel: input.channel,
      os: input.os,
      arch: input.arch,
      updateAvailable: false,
      comparison: "no_release",
      latestRelease: null,
      selectedArtifact: null,
      instructions,
      evaluatedRing: input.ring,
      rolloutBucket,
      selectionReason,
    };
  }
  const selectedArtifact = findMatchingAgentReleaseArtifact(
    latestRelease,
    input,
    tenantId
  );
  const versionCompare = compareAgentReleaseVersions(
    input.currentVersion,
    latestRelease.version
  );
  return {
    checkedAt: new Date().toISOString(),
    currentVersion: input.currentVersion,
    channel: input.channel,
    os: input.os,
    arch: input.arch,
    updateAvailable: versionCompare < 0,
    comparison:
      versionCompare < 0
        ? "upgrade_available"
        : versionCompare === 0
          ? "up_to_date"
          : "ahead_of_latest",
    latestRelease: cloneAgentRelease(latestRelease),
    selectedArtifact,
    instructions,
    evaluatedRing: input.ring,
    rolloutBucket,
    selectionReason: "matched",
  };
}

systemConfigRoutes.get("/system/config/agents/views", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const items = await listAgentRuntimeViewsForTenant(auth.tenantId);
  const response: AgentRuntimeViewListResponse = {
    items,
    total: items.length,
    generatedAt: new Date().toISOString(),
  };
  return c.json(response);
});

systemConfigRoutes.get("/system/config/agent-runtime", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const agentId = normalizeOptionalString(c.req.query("agentId"));
  if (!agentId) {
    return c.json({ message: "agentId 不能为空。" }, 400);
  }

  const response = await buildAgentRuntimeConfigResponse(auth.tenantId, agentId);
  if (!response) {
    return c.json({ message: `未找到 agent ${agentId}。` }, 404);
  }
  return c.json(response);
});

systemConfigRoutes.post("/system/config/agent-heartbeat", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const payload = normalizeHeartbeatPayload(body);
  if (!payload) {
    return c.json(
      {
        message: "heartbeat 请求体不合法，必须至少包含 agentId 与 occurredAt。",
      },
      400
    );
  }

  const runtimeConfig = await buildAgentRuntimeConfigResponse(
    auth.tenantId,
    payload.agentId
  );
  if (!runtimeConfig) {
    return c.json({ message: `未找到 agent ${payload.agentId}。` }, 404);
  }

  const heartbeat = await repository.upsertAgentRuntimeHeartbeat(auth.tenantId, payload);
  return c.json(
    {
      agentId: heartbeat.agentId,
      tenantId: auth.tenantId,
      configVersion: heartbeat.configVersion ?? runtimeConfig.configVersion,
      occurredAt: heartbeat.occurredAt,
      receivedAt: heartbeat.updatedAt,
    },
    202
  );
});

function normalizePackagePayload(
  payload: SystemConfigPackageCreateInput["payload"]
): Record<string, unknown> {
  return payload
    ? (JSON.parse(JSON.stringify(payload)) as Record<string, unknown>)
    : {};
}

function normalizeStringArray(values: unknown, lowercase = false): string[] | undefined {
  if (!Array.isArray(values)) {
    return undefined;
  }
  const normalized = Array.from(
    new Set(
      values
        .filter((item): item is string => typeof item === "string")
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
        .map((item) => (lowercase ? item.toLowerCase() : item)),
    ),
  );
  return normalized.length > 0 ? normalized : undefined;
}

function parseSystemConfigPackageMetadata(input: unknown):
  | {
      success: true;
      data: {
        targetSelectors: SystemConfigPackageTargetSelectors;
        requiresApproval: boolean;
        requiredApprovals: 0 | 1 | 2;
      };
    }
  | { success: false; error: string } {
  if (!isRecord(input)) {
    return {
      success: true,
      data: {
        targetSelectors: {},
        requiresApproval: false,
        requiredApprovals: 0,
      },
    };
  }

  if (
    input.requiresApproval !== undefined &&
    typeof input.requiresApproval !== "boolean"
  ) {
    return { success: false, error: "requiresApproval 必须为布尔值。" };
  }
  const rawRequiredApprovals = input.requiredApprovals ?? input.required_approvals;
  let requiredApprovals: 0 | 1 | 2;
  if (rawRequiredApprovals === undefined) {
    requiredApprovals = input.requiresApproval === true ? 1 : 0;
  } else if (
    typeof rawRequiredApprovals === "number" &&
    Number.isInteger(rawRequiredApprovals) &&
    (rawRequiredApprovals === 0 ||
      rawRequiredApprovals === 1 ||
      rawRequiredApprovals === 2)
  ) {
    requiredApprovals = rawRequiredApprovals;
  } else {
    return { success: false, error: "requiredApprovals 仅支持 0/1/2。" };
  }
  if (input.requiresApproval === true && requiredApprovals === 0) {
    return {
      success: false,
      error: "requiresApproval=true 时 requiredApprovals 不能为 0。",
    };
  }
  if (input.requiresApproval !== true && requiredApprovals !== 0) {
    return {
      success: false,
      error: "requiresApproval=false 时 requiredApprovals 必须为 0。",
    };
  }

  const rawSelectors = input.targetSelectors ?? input.target_selectors;
  if (rawSelectors !== undefined && !isRecord(rawSelectors)) {
    return { success: false, error: "targetSelectors 必须是对象。" };
  }

  const selectorsRecord = isRecord(rawSelectors) ? rawSelectors : {};
  const targetSelectors: SystemConfigPackageTargetSelectors = {
    agentIds: normalizeStringArray(
      selectorsRecord.agentIds ?? selectorsRecord.agent_ids,
    ),
    deviceIds: normalizeStringArray(
      selectorsRecord.deviceIds ?? selectorsRecord.device_ids,
    ),
    channels: normalizeStringArray(
      selectorsRecord.channels,
      true,
    ),
    hostnames: normalizeStringArray(
      selectorsRecord.hostnames,
      true,
    ),
  };

  return {
    success: true,
    data: {
      targetSelectors,
      requiresApproval: input.requiresApproval === true,
      requiredApprovals,
    },
  };
}

function validateSystemConfigPackageApprovalCreateInput(input: unknown):
  | {
      success: true;
      data: { decision: SystemConfigPackageApprovalDecision; comment?: string };
    }
  | { success: false; error: string } {
  if (!isRecord(input)) {
    return { success: false, error: "请求体必须是对象。" };
  }
  const decision = normalizeOptionalString(input.decision)?.toLowerCase();
  if (decision !== "approved" && decision !== "rejected") {
    return { success: false, error: "decision 仅支持 approved/rejected。" };
  }
  const comment = normalizeOptionalString(input.comment);
  if (input.comment !== undefined && !comment) {
    return { success: false, error: "comment 必须是非空字符串。" };
  }
  return {
    success: true,
    data: {
      decision,
      comment,
    },
  };
}

function validateSystemConfigPackageWatchQuery(
  input: Record<string, string>,
):
  | { success: true; data: SystemConfigPackageWatchQuery }
  | { success: false; error: string } {
  const agentId = normalizeOptionalString(input.agentId ?? input.agent_id);
  if (input.agentId !== undefined || input.agent_id !== undefined) {
    if (!agentId) {
      return { success: false, error: "agentId 必须为非空字符串。" };
    }
  }
  const deviceId = normalizeOptionalString(input.deviceId ?? input.device_id);
  if (input.deviceId !== undefined || input.device_id !== undefined) {
    if (!deviceId) {
      return { success: false, error: "deviceId 必须为非空字符串。" };
    }
  }
  const channel = normalizeOptionalString(input.channel)?.toLowerCase();
  if (input.channel !== undefined && !channel) {
    return { success: false, error: "channel 必须为非空字符串。" };
  }
  const hostname = normalizeOptionalString(input.hostname)?.toLowerCase();
  if (input.hostname !== undefined && !hostname) {
    return { success: false, error: "hostname 必须为非空字符串。" };
  }

  return {
    success: true,
    data: {
      agentId,
      deviceId,
      channel,
      hostname,
    },
  };
}

function packageSelectorMatches(
  candidates: string[] | undefined,
  current: string | undefined,
): boolean {
  if (!candidates || candidates.length === 0) {
    return true;
  }
  if (!current) {
    return false;
  }
  return candidates.includes(current);
}

function createSystemConfigPackageRecord(
  tenantId: string,
  input: SystemConfigPackageCreateInput,
  options: {
    targetSelectors: SystemConfigPackageTargetSelectors;
    requiresApproval: boolean;
    requiredApprovals: 0 | 1 | 2;
  }
): SystemConfigPackageRecord {
  const now = new Date().toISOString();
  return {
    packageId: crypto.randomUUID(),
    tenantId,
    version: input.version,
    issuedAt: input.issuedAt ?? now,
    signatureStatus: input.signatureStatus ?? "unknown",
    payload: normalizePackagePayload(input.payload),
    targetSelectors: options.targetSelectors,
    requiresApproval: options.requiresApproval,
    requiredApprovals: options.requiredApprovals,
    isPublished: false,
    publishedAt: undefined,
    createdAt: now,
    updatedAt: now,
  };
}

function saveSystemConfigPackage(
  tenantId: string,
  input: SystemConfigPackageCreateInput,
  options: {
    targetSelectors: SystemConfigPackageTargetSelectors;
    requiresApproval: boolean;
    requiredApprovals: 0 | 1 | 2;
  }
): SystemConfigPackageRecord {
  const currentItems = memorySystemConfigPackages.get(tenantId) ?? [];
  const record = createSystemConfigPackageRecord(tenantId, input, options);
  memorySystemConfigPackages.set(tenantId, [
    record,
    ...currentItems,
  ]);
  return cloneSystemConfigPackage(record);
}

function listSystemConfigPackages(
  tenantId: string,
  input: SystemConfigPackageListInput
): SystemConfigPackageListResponse {
  const limit = input.limit ?? SYSTEM_CONFIG_PACKAGE_DEFAULT_LIMIT;
  const sortedItems = (memorySystemConfigPackages.get(tenantId) ?? [])
    .slice()
    .sort(
      (a, b) =>
        b.createdAt.localeCompare(a.createdAt) ||
        b.packageId.localeCompare(a.packageId)
    );
  const items = sortedItems
    .slice(0, limit)
    .map(cloneSystemConfigPackage);
  return {
    items,
    total: sortedItems.length,
    filters: {
      limit,
    },
  };
}

function getSystemConfigPackageById(
  tenantId: string,
  packageId: string
): SystemConfigPackageRecord | null {
  const item = (memorySystemConfigPackages.get(tenantId) ?? []).find(
    (candidate) => candidate.packageId === packageId
  );
  return item ? cloneSystemConfigPackage(item) : null;
}

function listSystemConfigPackageApprovals(
  tenantId: string,
  packageId: string
): SystemConfigPackageApprovalRecord[] {
  return (memorySystemConfigPackageApprovals.get(tenantId) ?? [])
    .filter((candidate) => candidate.packageId === packageId)
    .sort(
      (left, right) =>
        right.updatedAt.localeCompare(left.updatedAt) ||
        right.approvalId.localeCompare(left.approvalId),
    )
    .map(cloneSystemConfigPackageApproval);
}

function upsertSystemConfigPackageApproval(
  tenantId: string,
  packageRecord: SystemConfigPackageRecord,
  approverUserId: string,
  input: { decision: SystemConfigPackageApprovalDecision; comment?: string }
): { record: SystemConfigPackageApprovalRecord; created: boolean } {
  const items = memorySystemConfigPackageApprovals.get(tenantId) ?? [];
  const index = items.findIndex(
    (candidate) =>
      candidate.packageId === packageRecord.packageId &&
      candidate.version === packageRecord.version &&
      candidate.approverUserId === approverUserId,
  );
  const now = new Date().toISOString();
  if (index < 0) {
    const next: SystemConfigPackageApprovalRecord = {
      approvalId: crypto.randomUUID(),
      tenantId,
      packageId: packageRecord.packageId,
      version: packageRecord.version,
      approverUserId,
      decision: input.decision,
      comment: input.comment,
      createdAt: now,
      updatedAt: now,
    };
    items.unshift(next);
    memorySystemConfigPackageApprovals.set(tenantId, items);
    return { record: cloneSystemConfigPackageApproval(next), created: true };
  }
  const current = items[index]!;
  const next: SystemConfigPackageApprovalRecord = {
    ...current,
    decision: input.decision,
    comment: input.comment,
    updatedAt: now,
  };
  items[index] = next;
  memorySystemConfigPackageApprovals.set(tenantId, items);
  return { record: cloneSystemConfigPackageApproval(next), created: false };
}

function countApprovedSystemConfigPackageApprovals(
  tenantId: string,
  packageRecord: SystemConfigPackageRecord
): number {
  const approvedUsers = new Set<string>();
  for (const approval of memorySystemConfigPackageApprovals.get(tenantId) ?? []) {
    if (
      approval.packageId !== packageRecord.packageId ||
      approval.version !== packageRecord.version ||
      approval.decision !== "approved"
    ) {
      continue;
    }
    approvedUsers.add(approval.approverUserId);
  }
  return approvedUsers.size;
}

function publishSystemConfigPackage(
  tenantId: string,
  packageId: string
):
  | { record: SystemConfigPackageRecord; error?: undefined }
  | { record?: undefined; error: "not_found" | "approvals_required" } {
  const items = memorySystemConfigPackages.get(tenantId) ?? [];
  const index = items.findIndex((candidate) => candidate.packageId === packageId);
  if (index < 0) {
    return { error: "not_found" };
  }
  const current = items[index];
  if (!current) {
    return { error: "not_found" };
  }
  if (
    current.requiresApproval &&
    countApprovedSystemConfigPackageApprovals(tenantId, current) <
      current.requiredApprovals
  ) {
    return { error: "approvals_required" };
  }
  const next: SystemConfigPackageRecord = {
    ...current,
    isPublished: true,
    publishedAt: current.publishedAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
  items[index] = next;
  memorySystemConfigPackages.set(tenantId, items);
  return { record: cloneSystemConfigPackage(next) };
}

function findLatestPublishedSystemConfigPackage(
  tenantId: string,
  query: SystemConfigPackageWatchQuery
): SystemConfigPackageRecord | null {
  const matched = (memorySystemConfigPackages.get(tenantId) ?? [])
    .filter((item) => item.isPublished)
    .filter((item) =>
      packageSelectorMatches(item.targetSelectors.agentIds, query.agentId),
    )
    .filter((item) =>
      packageSelectorMatches(item.targetSelectors.deviceIds, query.deviceId),
    )
    .filter((item) =>
      packageSelectorMatches(item.targetSelectors.channels, query.channel),
    )
    .filter((item) =>
      packageSelectorMatches(item.targetSelectors.hostnames, query.hostname),
    )
    .sort((left, right) => {
      const publishedCompare = (right.publishedAt ?? "").localeCompare(
        left.publishedAt ?? "",
      );
      if (publishedCompare !== 0) {
        return publishedCompare;
      }
      const updatedCompare = right.updatedAt.localeCompare(left.updatedAt);
      if (updatedCompare !== 0) {
        return updatedCompare;
      }
      return right.packageId.localeCompare(left.packageId);
    });
  return matched.length > 0 ? cloneSystemConfigPackage(matched[0]!) : null;
}

systemConfigRoutes.get("/system/config/backup", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const tenantId = auth.tenantId;
  const [sources, budgets, pricingCatalog] = await Promise.all([
    repository.listSources(tenantId),
    repository.listBudgets(tenantId),
    repository.getPricingCatalog(tenantId),
  ]);

  const payload: SystemConfigBackupPayload = {
    schemaVersion: SYSTEM_CONFIG_BACKUP_SCHEMA_VERSION,
    tenantId,
    exportedAt: new Date().toISOString(),
    exportedBy: {
      userId: auth.userId,
      email: auth.email,
    },
    sources: sources.map((item) =>
      toSystemConfigBackupSource(item as SystemConfigBackupSource)
    ),
    budgets: budgets.map(toSystemConfigBackupBudget),
    pricingCatalog: pricingCatalog
      ? {
          note: pricingCatalog.version.note,
          entries: pricingCatalog.entries,
        }
      : undefined,
  };

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.system_config_backup_exported",
    level: "info",
    detail: `Exported tenant system config backup for tenant ${tenantId}.`,
    metadata: {
      requestId,
      tenantId,
      userId: auth.userId,
      sourceCount: payload.sources.length,
      budgetCount: payload.budgets.length,
      pricingEntryCount: payload.pricingCatalog?.entries.length ?? 0,
      sourceLocations: payload.sources.slice(0, 5).map((item) => item.location),
      pricingNote: payload.pricingCatalog?.note,
      schemaVersion: payload.schemaVersion,
    },
  });

  c.header(
    "content-disposition",
    `attachment; filename="${buildBackupFileName(tenantId)}"`
  );
  return c.json(payload);
});

systemConfigRoutes.post("/system/config/packages", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateSystemConfigPackageCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const metadataResult = parseSystemConfigPackageMetadata(body);
  if (!metadataResult.success) {
    return c.json({ message: metadataResult.error }, 400);
  }

  const item = saveSystemConfigPackage(auth.tenantId, result.data, metadataResult.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.system_config_package_created",
    level: "info",
    detail: `Created system config package ${item.packageId} version ${item.version} for tenant ${auth.tenantId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      userId: auth.userId,
      packageId: item.packageId,
      version: item.version,
      signatureStatus: item.signatureStatus,
      targetSelectors: item.targetSelectors,
      requiresApproval: item.requiresApproval,
      requiredApprovals: item.requiredApprovals,
      isPublished: item.isPublished,
      payloadKeys: Object.keys(item.payload).slice(0, 20),
    },
  });
  return c.json(item, 201);
});

systemConfigRoutes.post("/system/config/packages/:packageId/approvals", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const packageId = c.req.param("packageId")?.trim();
  if (!packageId) {
    return c.json({ message: "packageId 必须为非空字符串。" }, 400);
  }
  const packageRecord = getSystemConfigPackageById(auth.tenantId, packageId);
  if (!packageRecord) {
    return c.json({ message: `未找到配置包 ${packageId}。` }, 404);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateSystemConfigPackageApprovalCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const approval = upsertSystemConfigPackageApproval(
    auth.tenantId,
    packageRecord,
    auth.userId,
    result.data,
  );
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: approval.created
      ? "control_plane.system_config_package_approval_created"
      : "control_plane.system_config_package_approval_updated",
    level: "info",
    detail: approval.created
      ? `Created config package approval ${approval.record.approvalId} for package ${packageId}.`
      : `Updated config package approval ${approval.record.approvalId} for package ${packageId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      userId: auth.userId,
      packageId,
      approvalId: approval.record.approvalId,
      version: approval.record.version,
      decision: approval.record.decision,
      approverUserId: approval.record.approverUserId,
    },
  });
  return c.json(approval.record, approval.created ? 201 : 200);
});

systemConfigRoutes.get("/system/config/packages/:packageId/approvals", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const packageId = c.req.param("packageId")?.trim();
  if (!packageId) {
    return c.json({ message: "packageId 必须为非空字符串。" }, 400);
  }
  const packageRecord = getSystemConfigPackageById(auth.tenantId, packageId);
  if (!packageRecord) {
    return c.json({ message: `未找到配置包 ${packageId}。` }, 404);
  }
  return c.json({
    items: listSystemConfigPackageApprovals(auth.tenantId, packageId),
    total: listSystemConfigPackageApprovals(auth.tenantId, packageId).length,
  });
});

systemConfigRoutes.get("/system/config/packages", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateSystemConfigPackageListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  return c.json(listSystemConfigPackages(auth.tenantId, result.data));
});

systemConfigRoutes.get("/system/config/packages/:packageId", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const packageId = c.req.param("packageId")?.trim();
  if (!packageId) {
    return c.json({ message: "packageId 必须为非空字符串。" }, 400);
  }

  const item = getSystemConfigPackageById(auth.tenantId, packageId);
  if (!item) {
    return c.json({ message: `未找到配置包 ${packageId}。` }, 404);
  }
  return c.json(item);
});

systemConfigRoutes.post("/system/config/packages/:packageId/publish", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const packageId = c.req.param("packageId")?.trim();
  if (!packageId) {
    return c.json({ message: "packageId 必须为非空字符串。" }, 400);
  }

  const publishResult = publishSystemConfigPackage(auth.tenantId, packageId);
  if (publishResult.error === "not_found") {
    return c.json({ message: `未找到配置包 ${packageId}。` }, 404);
  }
  if (publishResult.error === "approvals_required") {
    return c.json({ message: `配置包 ${packageId} 尚未满足审批门槛。` }, 409);
  }
  const item = publishResult.record;
  if (!item) {
    return c.json({ message: `配置包 ${packageId} 发布结果缺失。` }, 500);
  }

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.system_config_package_published",
    level: "info",
    detail: `Published system config package ${item.packageId} for tenant ${auth.tenantId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      userId: auth.userId,
      packageId: item.packageId,
      version: item.version,
      publishedAt: item.publishedAt,
      requiresApproval: item.requiresApproval,
    },
  });

  return c.json(item);
});

systemConfigRoutes.get("/system/config/packages/watch/latest", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateSystemConfigPackageWatchQuery(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const item = findLatestPublishedSystemConfigPackage(auth.tenantId, result.data);
  if (!item) {
    return c.json({ message: "当前没有命中的已发布配置包。" }, 404);
  }
  return c.json(item);
});

systemConfigRoutes.post("/system/agent-releases", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateAgentReleaseCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const item = saveAgentRelease(auth.tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.agent_release_created",
    level: "info",
    detail: `Created agent release ${item.releaseId} version ${item.version} for tenant ${auth.tenantId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      userId: auth.userId,
      releaseId: item.releaseId,
      version: item.version,
      channel: item.channel,
      publishedAt: item.publishedAt,
      artifactCount: item.artifacts.length,
      targets: item.artifacts.map((artifact) => `${artifact.os}/${artifact.arch}`),
    },
  });
  return c.json(item, 201);
});

systemConfigRoutes.get("/system/agent-releases", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateAgentReleaseListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  return c.json(listAgentReleases(auth.tenantId, result.data));
});

systemConfigRoutes.get("/system/agent-releases/check", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateAgentReleaseCheckInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  return c.json(checkAgentReleaseUpdate(auth.tenantId, result.data));
});

systemConfigRoutes.post("/system/agent-releases/check/batch", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateAgentReleaseBatchCheckInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const items = result.data.samples.map((sample) => ({
    label: sample.label,
    ...checkAgentReleaseUpdate(auth.tenantId, {
      currentVersion: sample.currentVersion,
      channel: result.data.channel,
      os: result.data.os,
      arch: result.data.arch,
      agentId: sample.agentId,
      deviceId: sample.deviceId,
      hostname: sample.hostname,
      ring: sample.ring ?? "stable",
    }),
  }));

  const payload: AgentReleaseBatchCheckResult = {
    items,
    total: items.length,
  };
  return c.json(payload);
});

systemConfigRoutes.get("/system/agent-releases/:releaseId", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const releaseId = c.req.param("releaseId")?.trim();
  if (!releaseId) {
    return c.json({ message: "releaseId 必须为非空字符串。" }, 400);
  }

  const item = getAgentReleaseById(auth.tenantId, releaseId);
  if (!item) {
    return c.json({ message: `未找到 Agent Release ${releaseId}。` }, 404);
  }
  return c.json(item);
});

systemConfigRoutes.post("/system/config/restore", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateSystemConfigRestoreInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const tenantId = auth.tenantId;
  const backup = result.data.backup;
  if (backup.tenantId !== tenantId) {
    return c.json({ message: "backup.tenantId 与当前租户不一致，禁止跨租户恢复。" }, 403);
  }

  const dryRun = result.data.dryRun ?? false;
  const restoreSources = result.data.restoreSources ?? true;
  const restoreBudgets = result.data.restoreBudgets ?? true;
  const restorePricingCatalog = result.data.restorePricingCatalog ?? true;

  const warnings: string[] = [];
  const summary: SystemConfigRestoreResult["summary"] = {
    sources: {
      total: restoreSources ? backup.sources.length : 0,
      created: 0,
      skipped: 0,
    },
    budgets: {
      total: restoreBudgets ? backup.budgets.length : 0,
      upserted: 0,
      skipped: 0,
    },
    pricingCatalog: {
      included: restorePricingCatalog && Boolean(backup.pricingCatalog),
      restored: false,
      entryCount: backup.pricingCatalog?.entries.length ?? 0,
    },
  };

  if (restoreSources) {
    const existingSources = await repository.listSources(tenantId);
    const signatures = new Set(
      existingSources.map((item) =>
        buildSourceSignature(toSystemConfigBackupSource(item as SystemConfigBackupSource))
      )
    );
    const backupSignatures = new Set<string>();

    for (const source of backup.sources) {
      const signature = buildSourceSignature(source);
      if (signatures.has(signature) || backupSignatures.has(signature)) {
        summary.sources.skipped += 1;
        continue;
      }

      if (!dryRun) {
        await repository.createSource(tenantId, source);
      }
      summary.sources.created += 1;
      signatures.add(signature);
      backupSignatures.add(signature);
    }
  }

  if (restoreBudgets) {
    for (const budget of backup.budgets) {
      const bindingError = await repository.validateBudgetScopeBinding(tenantId, budget);
      if (bindingError) {
        summary.budgets.skipped += 1;
        warnings.push(
          `budget(scope=${budget.scope}) 绑定校验失败：${bindingError.message}`
        );
        continue;
      }

      if (!dryRun) {
        await repository.upsertBudget(tenantId, budget);
      }
      summary.budgets.upserted += 1;
    }
  }

  if (
    restorePricingCatalog &&
    backup.pricingCatalog &&
    backup.pricingCatalog.entries.length > 0
  ) {
    if (!dryRun) {
      await repository.upsertPricingCatalog(tenantId, {
        note:
          backup.pricingCatalog.note ??
          `Restored from backup ${backup.exportedAt}`,
        entries: backup.pricingCatalog.entries,
      });
    }
    summary.pricingCatalog.restored = true;
  }

  const response: SystemConfigRestoreResult = {
    tenantId,
    dryRun,
    restoredAt: new Date().toISOString(),
    summary,
    warnings,
  };

  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId,
    eventId: `cp:${requestId}`,
    action: dryRun
      ? "control_plane.system_config_restore_dry_run"
      : "control_plane.system_config_restore_applied",
    level: warnings.length > 0 ? "warning" : "info",
    detail: dryRun
      ? `Previewed tenant system config restore for tenant ${tenantId}.`
      : `Applied tenant system config restore for tenant ${tenantId}.`,
    metadata: {
      requestId,
      tenantId,
      userId: auth.userId,
      schemaVersion: backup.schemaVersion,
      backupExportedAt: backup.exportedAt,
      restoreSources,
      restoreBudgets,
      restorePricingCatalog,
      dryRun,
      sourceLocations: backup.sources.slice(0, 5).map((item) => item.location),
      pricingNote: backup.pricingCatalog?.note,
      summary,
      warningCount: warnings.length,
    },
  });

  return c.json(response);
});
