import { createHmac, randomBytes } from "node:crypto";

const DEFAULT_RETRY_MAX = 5;
const DEFAULT_RETRY_BASE_DELAY_MS = 2_000;
const DEFAULT_RETRY_MAX_DELAY_MS = 60_000;

export type IntegrationRetryPolicy = {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
};

function toBoundedInteger(
  value: string | undefined,
  fallback: number,
  min: number,
  max: number
): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number.parseInt(value.trim(), 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  if (parsed < min) {
    return min;
  }
  if (parsed > max) {
    return max;
  }
  return parsed;
}

export function computeIntegrationCallbackSignature(
  secret: string,
  timestamp: string,
  nonce: string,
  body: string
): string {
  return createHmac("sha256", secret).update(`${timestamp}\n${nonce}\n${body}`).digest("hex");
}

export function generateIntegrationSignatureNonce(byteLength = 16): string {
  return randomBytes(Math.max(4, byteLength)).toString("hex");
}

export function shouldRetryIntegrationAttempt(attempt: number, maxRetries: number): boolean {
  if (maxRetries < 0) {
    return false;
  }
  const normalizedAttempt = attempt <= 0 ? 1 : Math.trunc(attempt);
  return normalizedAttempt <= maxRetries;
}

export function computeIntegrationBackoffDelayMs(
  attempt: number,
  baseDelayMs: number,
  maxDelayMs: number
): number {
  let base = Number.isFinite(baseDelayMs) && baseDelayMs > 0 ? Math.trunc(baseDelayMs) : DEFAULT_RETRY_BASE_DELAY_MS;
  let max = Number.isFinite(maxDelayMs) && maxDelayMs > 0 ? Math.trunc(maxDelayMs) : DEFAULT_RETRY_MAX_DELAY_MS;
  if (max < base) {
    max = base;
  }
  const normalizedAttempt = attempt <= 0 ? 1 : Math.trunc(attempt);
  if (normalizedAttempt <= 1) {
    return Math.min(base, max);
  }
  let delay = base;
  for (let index = 1; index < normalizedAttempt; index += 1) {
    if (delay >= max || delay > Math.floor(max / 2)) {
      return max;
    }
    delay *= 2;
  }
  return delay > max ? max : delay;
}

export function isRetryableIntegrationStatusCode(statusCode: number): boolean {
  return statusCode === 429 || statusCode >= 500;
}

export function resolveIntegrationRetryPolicyFromEnv(): IntegrationRetryPolicy {
  const maxRetries = toBoundedInteger(
    Bun.env.INTEGRATION_RETRY_MAX,
    DEFAULT_RETRY_MAX,
    0,
    20
  );
  const baseDelayMs = toBoundedInteger(
    Bun.env.INTEGRATION_RETRY_BASE_DELAY_MS,
    DEFAULT_RETRY_BASE_DELAY_MS,
    1,
    120_000
  );
  const maxDelayMs = toBoundedInteger(
    Bun.env.INTEGRATION_RETRY_MAX_DELAY_MS,
    DEFAULT_RETRY_MAX_DELAY_MS,
    baseDelayMs,
    300_000
  );
  return {
    maxRetries,
    baseDelayMs,
    maxDelayMs,
  };
}
