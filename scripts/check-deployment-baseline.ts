#!/usr/bin/env bun

import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

const defaultGovernanceEvalInterval = "60s";
const defaultGovernanceWeeklyWeekday = "monday";
const defaultGovernanceWeeklyTimeUTC = "09:00";

const defaultIntegrationChannels = "webhook";
const defaultIntegrationCallbackPath = "/api/v1/integrations/callbacks/alerts";
const defaultIntegrationWebhookTimeout = "10s";
const defaultIntegrationCallbackSignatureTTL = "5m";
const defaultIntegrationRetryMax = 5;
const defaultIntegrationRetryBaseDelay = "2s";
const defaultIntegrationRetryMaxDelay = "60s";
const defaultIntegrationAlertDedupeWindow = "0s";
const defaultIntegrationAlertDedupeMaxEntries = 20000;
const defaultIntegrationConsumerAckWait = "90s";
const defaultIntegrationDLQPublishTimeout = "5s";
const defaultIntegrationSMTPPort = 587;
const defaultIntegrationSMTPTLSMode = "starttls";

const supportedProfiles = ["release-gate", "governance", "integration"] as const;
const supportedChannels = ["webhook", "wecom", "dingtalk", "feishu", "email", "email_webhook", "ticket"] as const;
const supportedSMTPModes = ["none", "starttls", "tls"] as const;

type PrecheckProfile = (typeof supportedProfiles)[number];
type OutputFormat = "text" | "json";
type Severity = "error" | "warning";

type ValidationIssue = {
  level: Severity;
  key: string;
  message: string;
};

type ValidationResult = {
  profile: PrecheckProfile;
  ok: boolean;
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
  checkedKeys: string[];
  loadedEnvFiles: string[];
};

type CliOptions = {
  profile: PrecheckProfile;
  envFiles: string[];
  allowLocal: boolean;
  format: OutputFormat;
  help: boolean;
};

type ValidationContext = {
  env: Record<string, string>;
  allowLocal: boolean;
  errors: ValidationIssue[];
  warnings: ValidationIssue[];
  checkedKeys: Set<string>;
};

function isSupportedProfile(value: string): value is PrecheckProfile {
  return supportedProfiles.includes(value as PrecheckProfile);
}

function parseCliOptions(
  argv: string[]
): { success: true; options: CliOptions } | { success: false; error: string } {
  const options: CliOptions = {
    profile: "release-gate",
    envFiles: [],
    allowLocal: false,
    format: "text",
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--profile") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        return { success: false, error: "--profile 需要提供有效值。" };
      }
      if (!isSupportedProfile(value)) {
        return {
          success: false,
          error: `--profile 仅支持 ${supportedProfiles.join(", ")}。`,
        };
      }
      options.profile = value;
      index += 1;
      continue;
    }
    if (token === "--env-file") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        return { success: false, error: "--env-file 需要提供文件路径。" };
      }
      options.envFiles.push(value);
      index += 1;
      continue;
    }
    if (token === "--allow-local") {
      options.allowLocal = true;
      continue;
    }
    if (token === "--format") {
      const value = argv[index + 1];
      if (value !== "text" && value !== "json") {
        return { success: false, error: "--format 仅支持 text/json。" };
      }
      options.format = value;
      index += 1;
      continue;
    }
    return { success: false, error: `未知参数：${token}` };
  }

  options.envFiles = Array.from(new Set(options.envFiles));
  return { success: true, options };
}

function printUsage(): void {
  console.log("用法: bun run ./scripts/check-deployment-baseline.ts --profile <release-gate|governance|integration> [--env-file .env] [--env-file .env.prod] [--allow-local] [--format text|json]");
  console.log("说明: 校验 release gate 或 governance/integration 部署前的关键环境变量、URL、时长与通道绑定基线。后传入的 --env-file 会覆盖前面的值，也会覆盖当前 shell 的同名环境变量。");
}

function stripWrappingQuotes(value: string): string {
  if (value.length >= 2 && ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'")))) {
    return value.slice(1, -1);
  }
  return value;
}

export function parseEnvFileContent(content: string): Record<string, string> {
  const entries: Record<string, string> = {};
  const lines = content.replace(/^\uFEFF/, "").split(/\r?\n/);

  lines.forEach((rawLine, index) => {
    const line = rawLine.trim();
    if (line === "" || line.startsWith("#")) {
      return;
    }

    const normalized = line.startsWith("export ") ? line.slice("export ".length).trim() : line;
    const separator = normalized.indexOf("=");
    if (separator <= 0) {
      throw new Error(`第 ${index + 1} 行不是合法的 KEY=VALUE：${rawLine}`);
    }

    const key = normalized.slice(0, separator).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      throw new Error(`第 ${index + 1} 行环境变量名非法：${key}`);
    }

    const rawValue = normalized.slice(separator + 1).trim();
    entries[key] = stripWrappingQuotes(rawValue);
  });

  return entries;
}

function loadEnvFiles(envFiles: string[]): { env: Record<string, string>; loadedEnvFiles: string[] } {
  const merged: Record<string, string> = {};
  const loadedEnvFiles: string[] = [];

  for (const filePath of envFiles) {
    const absolutePath = path.resolve(filePath);
    if (!existsSync(absolutePath)) {
      throw new Error(`env 文件不存在：${filePath}`);
    }
    const content = readFileSync(absolutePath, "utf8");
    Object.assign(merged, parseEnvFileContent(content));
    loadedEnvFiles.push(absolutePath);
  }

  return { env: merged, loadedEnvFiles };
}

function buildEffectiveEnv(baseEnv: NodeJS.ProcessEnv, envFiles: string[]): { env: Record<string, string>; loadedEnvFiles: string[] } {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(baseEnv)) {
    if (typeof value === "string") {
      env[key] = value;
    }
  }

  const fromFiles = loadEnvFiles(envFiles);
  Object.assign(env, fromFiles.env);
  return { env, loadedEnvFiles: fromFiles.loadedEnvFiles };
}

function addIssue(ctx: ValidationContext, level: Severity, key: string, message: string): void {
  const issue: ValidationIssue = { level, key, message };
  if (level === "error") {
    ctx.errors.push(issue);
    return;
  }
  ctx.warnings.push(issue);
}

function readEnv(ctx: ValidationContext, key: string): string {
  ctx.checkedKeys.add(key);
  return (ctx.env[key] ?? "").trim();
}

function readEffectiveValue(ctx: ValidationContext, key: string, fallback: string): string {
  const value = readEnv(ctx, key);
  if (value === "") {
    return fallback;
  }
  return value;
}

function parseInteger(raw: string): number | null {
  if (!/^-?\d+$/.test(raw.trim())) {
    return null;
  }
  return Number.parseInt(raw, 10);
}

function parseDurationToMilliseconds(raw: string): number | null {
  let remaining = raw.trim();
  if (remaining === "") {
    return null;
  }

  let sign = 1;
  if (remaining.startsWith("+")) {
    remaining = remaining.slice(1);
  } else if (remaining.startsWith("-")) {
    sign = -1;
    remaining = remaining.slice(1);
  }

  if (remaining === "") {
    return null;
  }

  const units: Record<string, number> = {
    ns: 1e-6,
    us: 1e-3,
    "µs": 1e-3,
    "μs": 1e-3,
    ms: 1,
    s: 1000,
    m: 60_000,
    h: 3_600_000,
  };

  let total = 0;
  while (remaining.length > 0) {
    const match = /^(\d+(?:\.\d+)?)(ns|us|µs|μs|ms|s|m|h)/.exec(remaining);
    if (!match) {
      return null;
    }
    total += Number.parseFloat(match[1]) * units[match[2]];
    remaining = remaining.slice(match[0].length);
  }
  return total * sign;
}

function isLocalHostname(hostname: string): boolean {
  const normalized = hostname.trim().toLowerCase().replace(/^\[(.*)\]$/, "$1");
  return normalized === "localhost" || normalized === "127.0.0.1" || normalized === "::1" || normalized === "0.0.0.0";
}

function validateHTTPURL(ctx: ValidationContext, key: string, raw: string, requireNonLocal: boolean): URL | null {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    addIssue(ctx, "error", key, "必须是合法的 URL。");
    return null;
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    addIssue(ctx, "error", key, "协议仅支持 http/https。");
    return null;
  }
  if (parsed.hostname === "") {
    addIssue(ctx, "error", key, "必须包含主机名。");
    return null;
  }
  if (requireNonLocal && !ctx.allowLocal && isLocalHostname(parsed.hostname)) {
    addIssue(ctx, "error", key, "当前为部署预检，默认禁止 loopback/localhost 地址；如需本地演练请显式加 --allow-local。");
  }
  return parsed;
}

function validateRelativeCallbackPath(ctx: ValidationContext, key: string, raw: string): void {
  const value = raw.trim();
  if (value === "") {
    addIssue(ctx, "error", key, "不能为空。");
    return;
  }
  if (value.includes("://") || value.startsWith("//")) {
    addIssue(ctx, "error", key, "必须为相对路径，不能包含 scheme/host。");
    return;
  }
  if (!value.startsWith("/")) {
    addIssue(ctx, "error", key, "必须以 / 开头。");
    return;
  }

  try {
    const parsed = new URL(value, "https://example.invalid");
    if (!parsed.pathname.startsWith("/")) {
      addIssue(ctx, "error", key, "路径必须为绝对 path。");
    }
  } catch {
    addIssue(ctx, "error", key, "不是合法的相对路径。");
  }
}

function validateNonEmpty(ctx: ValidationContext, key: string, message = "不能为空。"): string | null {
  const value = readEnv(ctx, key);
  if (value === "") {
    addIssue(ctx, "error", key, message);
    return null;
  }
  return value;
}

function validateDurationEnv(
  ctx: ValidationContext,
  key: string,
  fallback: string,
  rules: { mustBePositive?: boolean; allowZero?: boolean } = {}
): number | null {
  const value = readEffectiveValue(ctx, key, fallback);
  const duration = parseDurationToMilliseconds(value);
  if (duration === null) {
    addIssue(ctx, "error", key, "必须是合法的 Go duration，例如 5s、2m、1h30m。");
    return null;
  }
  if (rules.mustBePositive && duration <= 0) {
    addIssue(ctx, "error", key, "必须大于 0。");
    return null;
  }
  if (!rules.allowZero && !rules.mustBePositive && duration < 0) {
    addIssue(ctx, "error", key, "不能为负数。");
    return null;
  }
  return duration;
}

function validateIntegerEnv(
  ctx: ValidationContext,
  key: string,
  fallback: number,
  rules: { min?: number; max?: number } = {}
): number | null {
  const raw = readEffectiveValue(ctx, key, String(fallback));
  const value = parseInteger(raw);
  if (value === null) {
    addIssue(ctx, "error", key, "必须是整数。");
    return null;
  }
  if (typeof rules.min === "number" && value < rules.min) {
    addIssue(ctx, "error", key, `必须 >= ${rules.min}。`);
    return null;
  }
  if (typeof rules.max === "number" && value > rules.max) {
    addIssue(ctx, "error", key, `必须 <= ${rules.max}。`);
    return null;
  }
  return value;
}

function validateEmailAddress(ctx: ValidationContext, key: string, raw: string): void {
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(raw)) {
    addIssue(ctx, "error", key, "必须是合法邮箱地址。");
  }
}

function validatePostgresURL(ctx: ValidationContext, key: string, raw: string, requireNonLocal: boolean): void {
  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    addIssue(ctx, "error", key, "必须是合法的 PostgreSQL 连接串。");
    return;
  }

  if (parsed.protocol !== "postgres:" && parsed.protocol !== "postgresql:") {
    addIssue(ctx, "error", key, "协议仅支持 postgres/postgresql。");
    return;
  }
  if (parsed.hostname === "") {
    addIssue(ctx, "error", key, "必须包含主机名。");
    return;
  }
  if (requireNonLocal && !ctx.allowLocal && isLocalHostname(parsed.hostname)) {
    addIssue(ctx, "error", key, "当前为部署预检，默认禁止 loopback/localhost 数据库地址；如需本地演练请显式加 --allow-local。");
  }
}

function validateNatsURL(ctx: ValidationContext, key: string): void {
  const raw = validateNonEmpty(ctx, key);
  if (!raw) {
    return;
  }

  const endpoints = raw
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  if (endpoints.length === 0) {
    addIssue(ctx, "error", key, "至少需要一个 NATS 地址。");
    return;
  }

  for (const endpoint of endpoints) {
    let parsed: URL;
    try {
      parsed = new URL(endpoint);
    } catch {
      addIssue(ctx, "error", key, `包含非法 NATS 地址：${endpoint}`);
      continue;
    }

    if (!["nats:", "tls:", "ws:", "wss:"].includes(parsed.protocol)) {
      addIssue(ctx, "error", key, `NATS 地址协议不受支持：${endpoint}`);
      continue;
    }
    if (parsed.hostname === "") {
      addIssue(ctx, "error", key, `NATS 地址缺少主机名：${endpoint}`);
      continue;
    }
    if (!ctx.allowLocal && isLocalHostname(parsed.hostname)) {
      addIssue(ctx, "error", key, `检测到 loopback/localhost NATS 地址：${endpoint}；如需本地演练请显式加 --allow-local。`);
    }
  }
}

function validateReleaseGateProfile(ctx: ValidationContext): void {
  const databaseURL = validateNonEmpty(ctx, "GOV_E2E_DATABASE_URL");
  if (databaseURL) {
    validatePostgresURL(ctx, "GOV_E2E_DATABASE_URL", databaseURL, true);
  }
}

function validateGovernanceProfile(ctx: ValidationContext): void {
  validateNatsURL(ctx, "NATS_URL");

  const databaseURL = validateNonEmpty(ctx, "DATABASE_URL");
  if (databaseURL) {
    validatePostgresURL(ctx, "DATABASE_URL", databaseURL, true);
  }

  validateDurationEnv(ctx, "GOV_EVAL_INTERVAL", defaultGovernanceEvalInterval, { mustBePositive: true });

  const weekday = readEffectiveValue(ctx, "GOV_WEEKLY_REPORT_WEEKDAY", defaultGovernanceWeeklyWeekday).toLowerCase();
  if (!["0", "1", "2", "3", "4", "5", "6", "sun", "sunday", "mon", "monday", "tue", "tuesday", "wed", "wednesday", "thu", "thursday", "fri", "friday", "sat", "saturday"].includes(weekday)) {
    addIssue(ctx, "error", "GOV_WEEKLY_REPORT_WEEKDAY", "仅支持 0-6 或 monday~sunday。");
  }

  const timeUTC = readEffectiveValue(ctx, "GOV_WEEKLY_REPORT_TIME_UTC", defaultGovernanceWeeklyTimeUTC);
  const match = /^(\d{2}):(\d{2})$/.exec(timeUTC);
  if (!match) {
    addIssue(ctx, "error", "GOV_WEEKLY_REPORT_TIME_UTC", "必须是 HH:MM（UTC）格式。");
  } else {
    const hour = Number.parseInt(match[1], 10);
    const minute = Number.parseInt(match[2], 10);
    if (hour < 0 || hour > 23) {
      addIssue(ctx, "error", "GOV_WEEKLY_REPORT_TIME_UTC", "小时必须在 00-23。");
    }
    if (minute < 0 || minute > 59) {
      addIssue(ctx, "error", "GOV_WEEKLY_REPORT_TIME_UTC", "分钟必须在 00-59。");
    }
  }
}

function validateIntegrationChannels(ctx: ValidationContext): void {
  const rawChannels = readEffectiveValue(ctx, "INTEGRATION_CHANNELS", defaultIntegrationChannels);
  const channels = Array.from(
    new Set(
      rawChannels
        .split(",")
        .map((item) => item.trim().toLowerCase())
        .filter(Boolean)
    )
  );

  if (channels.length === 0) {
    addIssue(ctx, "error", "INTEGRATION_CHANNELS", "至少需要一个启用通道。");
    return;
  }

  for (const channel of channels) {
    if (!(supportedChannels as readonly string[]).includes(channel)) {
      addIssue(ctx, "error", "INTEGRATION_CHANNELS", `不支持的通道：${channel}`);
      continue;
    }

    switch (channel) {
      case "webhook": {
        const value = validateNonEmpty(ctx, "INTEGRATION_WEBHOOK_URL", "启用 webhook 通道时必须提供。");
        if (value) {
          validateHTTPURL(ctx, "INTEGRATION_WEBHOOK_URL", value, false);
        }
        break;
      }
      case "wecom": {
        const value = validateNonEmpty(ctx, "INTEGRATION_WECOM_WEBHOOK_URL", "启用 wecom 通道时必须提供。");
        if (value) {
          validateHTTPURL(ctx, "INTEGRATION_WECOM_WEBHOOK_URL", value, false);
        }
        break;
      }
      case "dingtalk": {
        const value = validateNonEmpty(ctx, "INTEGRATION_DINGTALK_WEBHOOK_URL", "启用 dingtalk 通道时必须提供。");
        if (value) {
          validateHTTPURL(ctx, "INTEGRATION_DINGTALK_WEBHOOK_URL", value, false);
        }
        break;
      }
      case "feishu": {
        const value = validateNonEmpty(ctx, "INTEGRATION_FEISHU_WEBHOOK_URL", "启用 feishu 通道时必须提供。");
        if (value) {
          validateHTTPURL(ctx, "INTEGRATION_FEISHU_WEBHOOK_URL", value, false);
        }
        break;
      }
      case "email": {
        validateNonEmpty(ctx, "INTEGRATION_EMAIL_SMTP_HOST", "启用 email 通道时必须提供。");
        validateNonEmpty(ctx, "INTEGRATION_EMAIL_SMTP_USER", "启用 email 通道时必须提供。");
        validateNonEmpty(ctx, "INTEGRATION_EMAIL_SMTP_PASS", "启用 email 通道时必须提供。");

        const from = validateNonEmpty(ctx, "INTEGRATION_EMAIL_FROM", "启用 email 通道时必须提供。");
        if (from) {
          validateEmailAddress(ctx, "INTEGRATION_EMAIL_FROM", from);
        }

        validateIntegerEnv(ctx, "INTEGRATION_EMAIL_SMTP_PORT", defaultIntegrationSMTPPort, { min: 1 });

        const tlsMode = readEffectiveValue(ctx, "INTEGRATION_EMAIL_SMTP_TLS_MODE", defaultIntegrationSMTPTLSMode).toLowerCase();
        if (!(supportedSMTPModes as readonly string[]).includes(tlsMode)) {
          addIssue(ctx, "error", "INTEGRATION_EMAIL_SMTP_TLS_MODE", "仅支持 none/starttls/tls。");
        }
        break;
      }
      case "email_webhook": {
        const value = validateNonEmpty(ctx, "INTEGRATION_EMAIL_WEBHOOK_URL", "启用 email_webhook 通道时必须提供。");
        if (value) {
          validateHTTPURL(ctx, "INTEGRATION_EMAIL_WEBHOOK_URL", value, false);
        }
        const from = validateNonEmpty(ctx, "INTEGRATION_EMAIL_FROM", "启用 email_webhook 通道时必须提供。");
        if (from) {
          validateEmailAddress(ctx, "INTEGRATION_EMAIL_FROM", from);
        }
        break;
      }
      case "ticket": {
        const value = validateNonEmpty(ctx, "INTEGRATION_TICKET_WEBHOOK_URL", "启用 ticket 通道时必须提供。");
        if (value) {
          validateHTTPURL(ctx, "INTEGRATION_TICKET_WEBHOOK_URL", value, false);
        }
        break;
      }
      default:
        break;
    }
  }
}

function validateIntegrationProfile(ctx: ValidationContext): void {
  validateNatsURL(ctx, "NATS_URL");

  const controlPlaneURL = validateNonEmpty(ctx, "CONTROL_PLANE_BASE_URL");
  if (controlPlaneURL) {
    validateHTTPURL(ctx, "CONTROL_PLANE_BASE_URL", controlPlaneURL, true);
  }

  validateNonEmpty(ctx, "INTEGRATION_CALLBACK_SECRET");
  validateRelativeCallbackPath(
    ctx,
    "INTEGRATION_CALLBACK_PATH",
    readEffectiveValue(ctx, "INTEGRATION_CALLBACK_PATH", defaultIntegrationCallbackPath)
  );

  validateIntegrationChannels(ctx);

  const webhookTimeout = validateDurationEnv(ctx, "INTEGRATION_WEBHOOK_TIMEOUT", defaultIntegrationWebhookTimeout, {
    mustBePositive: true,
  });
  const callbackTTL = validateDurationEnv(
    ctx,
    "INTEGRATION_CALLBACK_SIGNATURE_TTL",
    defaultIntegrationCallbackSignatureTTL,
    { mustBePositive: true }
  );
  const retryBaseDelay = validateDurationEnv(ctx, "INTEGRATION_RETRY_BASE_DELAY", defaultIntegrationRetryBaseDelay, {
    mustBePositive: true,
  });
  const retryMaxDelay = validateDurationEnv(ctx, "INTEGRATION_RETRY_MAX_DELAY", defaultIntegrationRetryMaxDelay, {
    mustBePositive: true,
  });
  const alertDedupeWindow = validateDurationEnv(
    ctx,
    "INTEGRATION_ALERT_DEDUPE_WINDOW",
    defaultIntegrationAlertDedupeWindow,
    { allowZero: true }
  );
  const consumerAckWait = validateDurationEnv(ctx, "INTEGRATION_CONSUMER_ACK_WAIT", defaultIntegrationConsumerAckWait, {
    mustBePositive: true,
  });
  const dlqPublishTimeout = validateDurationEnv(
    ctx,
    "INTEGRATION_DLQ_PUBLISH_TIMEOUT",
    defaultIntegrationDLQPublishTimeout,
    { mustBePositive: true }
  );
  const retryMax = validateIntegerEnv(ctx, "INTEGRATION_RETRY_MAX", defaultIntegrationRetryMax, { min: 0 });
  const alertDedupeMaxEntries = validateIntegerEnv(
    ctx,
    "INTEGRATION_ALERT_DEDUPE_MAX_ENTRIES",
    defaultIntegrationAlertDedupeMaxEntries,
    { min: 0 }
  );

  if (retryBaseDelay !== null && retryMaxDelay !== null && retryMaxDelay < retryBaseDelay) {
    addIssue(ctx, "error", "INTEGRATION_RETRY_MAX_DELAY", "必须大于等于 INTEGRATION_RETRY_BASE_DELAY。");
  }
  if (alertDedupeWindow !== null && alertDedupeWindow > 0 && alertDedupeMaxEntries !== null && alertDedupeMaxEntries <= 0) {
    addIssue(ctx, "error", "INTEGRATION_ALERT_DEDUPE_MAX_ENTRIES", "启用去重窗口时必须 > 0。");
  }
  if (retryMax !== null && retryMax === 0) {
    addIssue(ctx, "warning", "INTEGRATION_RETRY_MAX", "当前配置为 0，callback / downstream 失败后将直接进入 DLQ 或失败返回。");
  }
  if (webhookTimeout !== null && consumerAckWait !== null && webhookTimeout >= consumerAckWait) {
    addIssue(ctx, "warning", "INTEGRATION_CONSUMER_ACK_WAIT", "建议保证 CONSUMER_ACK_WAIT 大于单次 WEBHOOK_TIMEOUT，避免重试窗口过窄。");
  }
  if (callbackTTL !== null && callbackTTL < 60_000) {
    addIssue(ctx, "warning", "INTEGRATION_CALLBACK_SIGNATURE_TTL", "当前签名窗口小于 60s，跨网络部署容易误判为重放。");
  }
  if (dlqPublishTimeout !== null && dlqPublishTimeout < 1_000) {
    addIssue(ctx, "warning", "INTEGRATION_DLQ_PUBLISH_TIMEOUT", "DLQ 发布超时小于 1s，在高延迟总线环境下可能过于激进。");
  }
}

export function validateDeploymentBaseline(
  profile: PrecheckProfile,
  env: Record<string, string>,
  options: { allowLocal?: boolean; loadedEnvFiles?: string[] } = {}
): ValidationResult {
  const ctx: ValidationContext = {
    env,
    allowLocal: options.allowLocal ?? false,
    errors: [],
    warnings: [],
    checkedKeys: new Set<string>(),
  };

  switch (profile) {
    case "release-gate":
      validateReleaseGateProfile(ctx);
      break;
    case "governance":
      validateGovernanceProfile(ctx);
      break;
    case "integration":
      validateIntegrationProfile(ctx);
      break;
    default: {
      const exhaustive: never = profile;
      throw new Error(`未知 profile: ${exhaustive}`);
    }
  }

  return {
    profile,
    ok: ctx.errors.length === 0,
    errors: ctx.errors,
    warnings: ctx.warnings,
    checkedKeys: Array.from(ctx.checkedKeys).sort((left, right) => left.localeCompare(right)),
    loadedEnvFiles: options.loadedEnvFiles ?? [],
  };
}

function printTextResult(result: ValidationResult): void {
  console.log(
    `[deployment-baseline] profile=${result.profile} status=${result.ok ? "passed" : "failed"} checked=${result.checkedKeys.length}`
  );

  if (result.loadedEnvFiles.length > 0) {
    console.log(`[deployment-baseline] env-files=${result.loadedEnvFiles.join(",")}`);
  }

  for (const warning of result.warnings) {
    console.warn(`WARN ${warning.key}: ${warning.message}`);
  }
  for (const error of result.errors) {
    console.error(`ERROR ${error.key}: ${error.message}`);
  }
}

export async function runDeploymentBaselinePrecheckCli(
  argv: string[],
  baseEnv: NodeJS.ProcessEnv = process.env
): Promise<number> {
  const parsed = parseCliOptions(argv);
  if (!parsed.success) {
    console.error(`参数错误: ${parsed.error}`);
    printUsage();
    return 1;
  }

  const { options } = parsed;
  if (options.help) {
    printUsage();
    return 0;
  }

  let effectiveEnv: Record<string, string>;
  let loadedEnvFiles: string[] = [];
  try {
    const loaded = buildEffectiveEnv(baseEnv, options.envFiles);
    effectiveEnv = loaded.env;
    loadedEnvFiles = loaded.loadedEnvFiles;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`读取环境文件失败: ${message}`);
    return 1;
  }

  const result = validateDeploymentBaseline(options.profile, effectiveEnv, {
    allowLocal: options.allowLocal,
    loadedEnvFiles,
  });

  if (options.format === "json") {
    console.log(JSON.stringify(result, null, 2));
  } else {
    printTextResult(result);
  }

  return result.ok ? 0 : 1;
}

if (import.meta.main) {
  const exitCode = await runDeploymentBaselinePrecheckCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
