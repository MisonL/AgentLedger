import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import type { AuditItem, AuditListInput, AuditLevel } from "../contracts";

export const EVIDENCE_BUNDLE_SCHEMA_VERSION = "evidence-bundle.v1";
const EVIDENCE_CHAIN_GENESIS = "agentledger:evidence:genesis";

type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface EvidenceBundleManifest {
  schemaVersion: typeof EVIDENCE_BUNDLE_SCHEMA_VERSION;
  generatedAt: string;
  tenantId: string;
  generatedBy: {
    userId: string;
    email?: string;
  };
  filters: {
    level?: string;
    from?: string;
    to?: string;
    limit?: number;
    eventId?: string;
    action?: string;
    keyword?: string;
  };
  recordCount: number;
  hashAlgorithm: "sha256";
  signatureAlgorithm: "hmac-sha256";
}

export interface EvidenceBundleRecord {
  index: number;
  audit: AuditItem;
  recordHash: string;
  chainHash: string;
}

export interface EvidenceBundle {
  manifest: EvidenceBundleManifest;
  records: EvidenceBundleRecord[];
  rootHash: string;
  signature: string;
}

export interface BuildEvidenceBundleInput {
  tenantId: string;
  generatedBy: {
    userId: string;
    email?: string;
  };
  filters: EvidenceBundleFilterInput;
  audits: AuditItem[];
  signingKey: string;
  generatedAt?: string;
}

export interface VerifyEvidenceBundleResult {
  success: boolean;
  errors: string[];
}

export interface EvidenceBundleFilterInput extends AuditListInput {
  eventId?: string;
  action?: string;
  keyword?: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, allowedKeys: readonly string[]): boolean {
  const currentKeys = Object.keys(value);
  if (currentKeys.length !== allowedKeys.length) {
    return false;
  }
  const allowSet = new Set(allowedKeys);
  return currentKeys.every((key) => allowSet.has(key));
}

function canonicalize(value: unknown): JsonValue {
  if (value === undefined) {
    return null;
  }
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => canonicalize(item));
  }
  if (isRecord(value)) {
    const sortedKeys = Object.keys(value).sort((a, b) => a.localeCompare(b));
    const normalized: Record<string, JsonValue> = {};
    for (const key of sortedKeys) {
      const current = value[key];
      if (current === undefined) {
        continue;
      }
      normalized[key] = canonicalize(current);
    }
    return normalized;
  }
  return String(value);
}

function stableStringify(value: unknown): string {
  return JSON.stringify(canonicalize(value));
}

function sha256Hex(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function hmacSha256Hex(secret: string, value: string): string {
  return createHmac("sha256", secret).update(value, "utf8").digest("hex");
}

function normalizeAuditItem(item: AuditItem): AuditItem {
  return {
    id: item.id,
    eventId: item.eventId,
    action: item.action,
    level: item.level,
    detail: item.detail,
    metadata: (canonicalize(item.metadata) as Record<string, unknown>) ?? {},
    createdAt: item.createdAt,
  };
}

function normalizeAuditFilters(input: EvidenceBundleFilterInput): EvidenceBundleManifest["filters"] {
  return {
    level: input.level,
    from: input.from,
    to: input.to,
    limit: input.limit,
    eventId: input.eventId,
    action: input.action,
    keyword: input.keyword,
  };
}

function isAuditLevelValue(value: unknown): value is AuditLevel {
  return value === "info" || value === "warning" || value === "error" || value === "critical";
}

function parseAuditItem(value: unknown): AuditItem | null {
  if (!isRecord(value)) {
    return null;
  }
  if (!hasOnlyKeys(value, ["id", "eventId", "action", "level", "detail", "metadata", "createdAt"])) {
    return null;
  }
  const id = typeof value.id === "string" ? value.id : "";
  const eventId = typeof value.eventId === "string" ? value.eventId : "";
  const action = typeof value.action === "string" ? value.action : "";
  const level = value.level;
  const detail = typeof value.detail === "string" ? value.detail : "";
  const createdAt = typeof value.createdAt === "string" ? value.createdAt : "";
  if (!id || !eventId || !action || !createdAt || !isAuditLevelValue(level) || !isRecord(value.metadata)) {
    return null;
  }
  return {
    id,
    eventId,
    action,
    level,
    detail,
    metadata: value.metadata,
    createdAt,
  };
}

function toNonNegativeInteger(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  const normalized = Math.trunc(value);
  return normalized >= 0 ? normalized : null;
}

function compareHexDigestSafe(left: string, right: string): boolean {
  if (left.length === 0 || right.length === 0 || left.length !== right.length) {
    return false;
  }
  try {
    const leftBuffer = Buffer.from(left, "hex");
    const rightBuffer = Buffer.from(right, "hex");
    if (leftBuffer.length === 0 || rightBuffer.length === 0) {
      return false;
    }
    if (leftBuffer.length !== rightBuffer.length) {
      return false;
    }
    return timingSafeEqual(leftBuffer, rightBuffer);
  } catch {
    return false;
  }
}

export function buildEvidenceBundle(input: BuildEvidenceBundleInput): EvidenceBundle {
  const generatedAt = input.generatedAt ?? new Date().toISOString();
  const normalizedAudits = input.audits
    .map((item) => normalizeAuditItem(item))
    .sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id));

  const records: EvidenceBundleRecord[] = [];
  let previousChainHash = sha256Hex(EVIDENCE_CHAIN_GENESIS);

  for (let index = 0; index < normalizedAudits.length; index += 1) {
    const audit = normalizedAudits[index] as AuditItem;
    const recordHash = sha256Hex(
      stableStringify({
        index,
        audit,
      }),
    );
    const chainHash = sha256Hex(`${previousChainHash}:${recordHash}`);
    previousChainHash = chainHash;

    records.push({
      index,
      audit,
      recordHash,
      chainHash,
    });
  }

  const rootHash =
    records.length > 0 ? records[records.length - 1].chainHash : sha256Hex(EVIDENCE_CHAIN_GENESIS);
  const manifest: EvidenceBundleManifest = {
    schemaVersion: EVIDENCE_BUNDLE_SCHEMA_VERSION,
    generatedAt,
    tenantId: input.tenantId,
    generatedBy: {
      userId: input.generatedBy.userId,
      email: input.generatedBy.email,
    },
    filters: normalizeAuditFilters(input.filters),
    recordCount: records.length,
    hashAlgorithm: "sha256",
    signatureAlgorithm: "hmac-sha256",
  };
  const signature = hmacSha256Hex(
    input.signingKey,
    stableStringify({
      manifest,
      rootHash,
    }),
  );

  return {
    manifest,
    records,
    rootHash,
    signature,
  };
}

export function verifyEvidenceBundle(
  bundle: unknown,
  signingKey: string,
): VerifyEvidenceBundleResult {
  const errors: string[] = [];
  if (!isRecord(bundle)) {
    return {
      success: false,
      errors: ["bundle 必须是对象。"],
    };
  }

  const manifest = bundle.manifest;
  const records = bundle.records;
  const rootHash = typeof bundle.rootHash === "string" ? bundle.rootHash : "";
  const signature = typeof bundle.signature === "string" ? bundle.signature : "";

  if (!isRecord(manifest)) {
    errors.push("manifest 缺失或格式非法。");
  }
  if (!Array.isArray(records)) {
    errors.push("records 缺失或格式非法。");
  }
  if (!rootHash) {
    errors.push("rootHash 缺失。");
  }
  if (!signature) {
    errors.push("signature 缺失。");
  }
  if (errors.length > 0) {
    return {
      success: false,
      errors,
    };
  }

  const normalizedManifest = manifest as Record<string, unknown>;
  const normalizedRecords = records as unknown[];

  if (normalizedManifest.schemaVersion !== EVIDENCE_BUNDLE_SCHEMA_VERSION) {
    errors.push(
      `schemaVersion 非法：期望 ${EVIDENCE_BUNDLE_SCHEMA_VERSION}，实际 ${String(
        normalizedManifest.schemaVersion,
      )}。`,
    );
  }
  const recordCount = toNonNegativeInteger(normalizedManifest.recordCount);
  if (recordCount === null) {
    errors.push("manifest.recordCount 必须为非负整数。");
  } else if (recordCount !== normalizedRecords.length) {
    errors.push(
      `manifest.recordCount 与 records.length 不一致：${recordCount} != ${normalizedRecords.length}。`,
    );
  }

  let previousChainHash = sha256Hex(EVIDENCE_CHAIN_GENESIS);
  for (let index = 0; index < normalizedRecords.length; index += 1) {
    const record = normalizedRecords[index];
    if (!isRecord(record)) {
      errors.push(`records[${index}] 不是对象。`);
      continue;
    }

    const recordIndex = toNonNegativeInteger(record.index);
    if (recordIndex === null || recordIndex !== index) {
      errors.push(`records[${index}].index 非法或不连续。`);
      continue;
    }
    const recordHash = typeof record.recordHash === "string" ? record.recordHash : "";
    const chainHash = typeof record.chainHash === "string" ? record.chainHash : "";
    const audit = record.audit;
    if (!recordHash || !chainHash || !isRecord(audit)) {
      errors.push(`records[${index}] 缺少必要字段。`);
      continue;
    }

    const parsedAudit = parseAuditItem(audit);
    if (!parsedAudit) {
      errors.push(`records[${index}] audit 格式非法。`);
      continue;
    }
    const normalizedAudit = normalizeAuditItem(parsedAudit);
    const expectedRecordHash = sha256Hex(
      stableStringify({
        index,
        audit: normalizedAudit,
      }),
    );
    if (!compareHexDigestSafe(recordHash, expectedRecordHash)) {
      errors.push(`records[${index}] recordHash 校验失败。`);
    }

    const expectedChainHash = sha256Hex(`${previousChainHash}:${expectedRecordHash}`);
    if (!compareHexDigestSafe(chainHash, expectedChainHash)) {
      errors.push(`records[${index}] chainHash 校验失败。`);
    }
    previousChainHash = expectedChainHash;
  }

  const expectedRootHash =
    normalizedRecords.length > 0 ? previousChainHash : sha256Hex(EVIDENCE_CHAIN_GENESIS);
  if (!compareHexDigestSafe(rootHash, expectedRootHash)) {
    errors.push("rootHash 校验失败。");
  }

  const expectedSignature = hmacSha256Hex(
    signingKey,
    stableStringify({
      manifest: normalizedManifest,
      rootHash,
    }),
  );
  if (!compareHexDigestSafe(signature, expectedSignature)) {
    errors.push("signature 校验失败。");
  }

  return {
    success: errors.length === 0,
    errors,
  };
}
