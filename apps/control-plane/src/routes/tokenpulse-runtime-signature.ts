import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import type { TokenPulseRuntimeEventIngestInput } from "../contracts";

export const TOKENPULSE_RUNTIME_SPEC_VERSION = "v1";
export const TOKENPULSE_RUNTIME_DEFAULT_KEY_ID = "tokenpulse-runtime-v1";
export const TOKENPULSE_RUNTIME_SIGNATURE_WINDOW_MS = 5 * 60 * 1000;
export const TOKENPULSE_RUNTIME_SPEC_VERSION_HEADER =
  "x-tokenpulse-spec-version";
export const TOKENPULSE_RUNTIME_KEY_ID_HEADER = "x-tokenpulse-key-id";
export const TOKENPULSE_RUNTIME_TIMESTAMP_HEADER = "x-tokenpulse-timestamp";
export const TOKENPULSE_RUNTIME_SIGNATURE_HEADER = "x-tokenpulse-signature";
export const TOKENPULSE_RUNTIME_IDEMPOTENCY_KEY_HEADER =
  "x-tokenpulse-idempotency-key";

function isLowercaseHex(value: string): boolean {
  return /^[0-9a-f]+$/.test(value);
}

export function resolveTokenPulseRuntimeKeyId(): string {
  return (
    Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_KEY_ID?.trim() ||
    TOKENPULSE_RUNTIME_DEFAULT_KEY_ID
  );
}

export function resolveTokenPulseRuntimeSecret(): string | null {
  const secret = Bun.env.AGENTLEDGER_TOKENPULSE_WEBHOOK_SECRET?.trim();
  return secret && secret.length > 0 ? secret : null;
}

export function buildTokenPulseRuntimeIdempotencyPayload(
  input: Pick<
    TokenPulseRuntimeEventIngestInput,
    "tenantId" | "traceId" | "provider" | "model" | "startedAt"
  >,
): string {
  return JSON.stringify({
    tenantId: input.tenantId,
    traceId: input.traceId,
    provider: input.provider,
    model: input.model,
    startedAt: input.startedAt,
  });
}

export function computeTokenPulseRuntimeIdempotencyKey(
  input: Pick<
    TokenPulseRuntimeEventIngestInput,
    "tenantId" | "traceId" | "provider" | "model" | "startedAt"
  >,
): string {
  return createHash("sha256")
    .update(buildTokenPulseRuntimeIdempotencyPayload(input), "utf8")
    .digest("hex");
}

export function buildTokenPulseRuntimeSigningString(input: {
  specVersion: string;
  keyId: string;
  timestamp: string;
  idempotencyKey: string;
  rawBody: string;
}): string {
  return [
    input.specVersion,
    input.keyId,
    input.timestamp,
    input.idempotencyKey,
    input.rawBody,
  ].join("\n");
}

export function computeTokenPulseRuntimeSignature(
  secret: string,
  input: {
    specVersion: string;
    keyId: string;
    timestamp: string;
    idempotencyKey: string;
    rawBody: string;
  },
): string {
  return createHmac("sha256", secret)
    .update(buildTokenPulseRuntimeSigningString(input), "utf8")
    .digest("hex");
}

export function isTokenPulseRuntimeSignatureValid(
  expected: string,
  provided: string | undefined,
): boolean {
  const normalized = provided?.trim().toLowerCase() ?? "";
  if (
    expected.length !== normalized.length ||
    !isLowercaseHex(expected) ||
    !isLowercaseHex(normalized)
  ) {
    return false;
  }
  return timingSafeEqual(
    Buffer.from(expected, "hex"),
    Buffer.from(normalized, "hex"),
  );
}

export function parseTokenPulseRuntimeTimestampMs(
  value: string | undefined,
): number | null {
  const normalized = value?.trim();
  if (!normalized || !/^\d+$/.test(normalized)) {
    return null;
  }
  const numeric = Number(normalized);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return numeric >= 1e12 ? numeric : numeric * 1000;
}

export function isTokenPulseRuntimeTimestampWithinWindow(
  timestampMs: number,
  nowMs: number,
  windowMs = TOKENPULSE_RUNTIME_SIGNATURE_WINDOW_MS,
): boolean {
  return Math.abs(nowMs - timestampMs) <= windowMs;
}
