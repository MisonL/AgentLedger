import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";

const DEFAULT_TOKEN_SECRET = "agentledger-control-plane-dev-secret";
const DEFAULT_ACCESS_TOKEN_EXPIRES_IN_SECONDS = 15 * 60;
const DEFAULT_REFRESH_TOKEN_EXPIRES_IN_SECONDS = 30 * 24 * 60 * 60;
const TOKEN_HEADER = {
  alg: "HS256",
  typ: "JWT",
} as const;

export const TOKEN_TYPE = "Bearer";

export type AuthTokenType = "access" | "refresh";

export interface AuthTokenPayload {
  sub: string;
  tid: string;
  typ: AuthTokenType;
  sid?: string;
  st?: string;
  iat: number;
  exp: number;
}

export interface IssueAccessTokenInput {
  userId: string;
  tenantId: string;
  sessionId?: string;
}

export interface IssueRefreshTokenInput {
  userId: string;
  tenantId: string;
  sessionId: string;
  sessionToken: string;
}

export interface IssuedToken {
  token: string;
  expiresIn: number;
  expiresAt: string;
}

export type VerifyTokenResult =
  | {
      success: true;
      payload: AuthTokenPayload;
    }
  | {
      success: false;
      code: "malformed" | "invalid_signature" | "invalid_payload" | "expired" | "token_type_mismatch";
    };

function parsePositiveInteger(rawValue: string | undefined, fallback: number): number {
  if (!rawValue) {
    return fallback;
  }
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  const normalized = Math.trunc(parsed);
  if (normalized <= 0) {
    return fallback;
  }
  return normalized;
}

function getSigningSecret(): string {
  const configured = (Bun.env.AUTH_TOKEN_SECRET ?? "").trim();
  if (configured.length > 0) {
    return configured;
  }
  return DEFAULT_TOKEN_SECRET;
}

function getAccessTokenExpiresInSeconds(): number {
  return parsePositiveInteger(
    Bun.env.AUTH_ACCESS_TOKEN_EXPIRES_IN_SECONDS,
    DEFAULT_ACCESS_TOKEN_EXPIRES_IN_SECONDS
  );
}

function getRefreshTokenExpiresInSeconds(): number {
  return parsePositiveInteger(
    Bun.env.AUTH_REFRESH_TOKEN_EXPIRES_IN_SECONDS,
    DEFAULT_REFRESH_TOKEN_EXPIRES_IN_SECONDS
  );
}

function normalizeTenantId(tenantId: string): string {
  const trimmed = tenantId.trim();
  return trimmed.length > 0 ? trimmed : "default";
}

function toBase64UrlFromBase64(value: string): string {
  return value.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function toBase64Url(value: string): string {
  return toBase64UrlFromBase64(Buffer.from(value, "utf8").toString("base64"));
}

function fromBase64Url(value: string): string | null {
  const normalized = value.trim();
  if (!normalized || /[^A-Za-z0-9\-_]/.test(normalized)) {
    return null;
  }
  const paddingLength = (4 - (normalized.length % 4)) % 4;
  const padded = `${normalized}${"=".repeat(paddingLength)}`;
  const base64 = padded.replace(/-/g, "+").replace(/_/g, "/");
  try {
    return Buffer.from(base64, "base64").toString("utf8");
  } catch {
    return null;
  }
}

function sign(content: string): string {
  const digest = createHmac("sha256", getSigningSecret()).update(content).digest("base64");
  return toBase64UrlFromBase64(digest);
}

function safeStringEquals(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left, "utf8");
  const rightBuffer = Buffer.from(right, "utf8");
  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }
  return timingSafeEqual(leftBuffer, rightBuffer);
}

function issueToken(payload: AuthTokenPayload, expiresIn: number): IssuedToken {
  const encodedHeader = toBase64Url(JSON.stringify(TOKEN_HEADER));
  const encodedPayload = toBase64Url(JSON.stringify(payload));
  const signingContent = `${encodedHeader}.${encodedPayload}`;
  const signature = sign(signingContent);

  return {
    token: `${signingContent}.${signature}`,
    expiresIn,
    expiresAt: new Date(payload.exp * 1000).toISOString(),
  };
}

function buildPayload(
  type: AuthTokenType,
  expiresIn: number,
  input: {
    userId: string;
    tenantId: string;
    sessionId?: string;
    sessionToken?: string;
  }
): AuthTokenPayload {
  const nowInSeconds = Math.floor(Date.now() / 1000);
  return {
    sub: input.userId,
    tid: normalizeTenantId(input.tenantId),
    typ: type,
    sid: input.sessionId,
    st: input.sessionToken,
    iat: nowInSeconds,
    exp: nowInSeconds + expiresIn,
  };
}

function verifyToken(token: string, expectedType: AuthTokenType): VerifyTokenResult {
  const trimmedToken = token.trim();
  if (trimmedToken.length === 0) {
    return {
      success: false,
      code: "malformed",
    };
  }

  const parts = trimmedToken.split(".");
  if (parts.length !== 3) {
    return {
      success: false,
      code: "malformed",
    };
  }

  const [headerPart, payloadPart, signaturePart] = parts;
  if (!headerPart || !payloadPart || !signaturePart) {
    return {
      success: false,
      code: "malformed",
    };
  }

  const signingContent = `${headerPart}.${payloadPart}`;
  const expectedSignature = sign(signingContent);
  if (!safeStringEquals(expectedSignature, signaturePart)) {
    return {
      success: false,
      code: "invalid_signature",
    };
  }

  const headerText = fromBase64Url(headerPart);
  const payloadText = fromBase64Url(payloadPart);
  if (!headerText || !payloadText) {
    return {
      success: false,
      code: "invalid_payload",
    };
  }

  let parsedHeader: unknown;
  let parsedPayload: unknown;
  try {
    parsedHeader = JSON.parse(headerText);
    parsedPayload = JSON.parse(payloadText);
  } catch {
    return {
      success: false,
      code: "invalid_payload",
    };
  }

  if (
    typeof parsedHeader !== "object" ||
    parsedHeader === null ||
    (parsedHeader as { alg?: unknown }).alg !== TOKEN_HEADER.alg ||
    (parsedHeader as { typ?: unknown }).typ !== TOKEN_HEADER.typ
  ) {
    return {
      success: false,
      code: "invalid_payload",
    };
  }

  if (typeof parsedPayload !== "object" || parsedPayload === null) {
    return {
      success: false,
      code: "invalid_payload",
    };
  }

  const payload = parsedPayload as Partial<AuthTokenPayload>;
  if (
    typeof payload.sub !== "string" ||
    payload.sub.trim().length === 0 ||
    typeof payload.tid !== "string" ||
    payload.tid.trim().length === 0 ||
    (payload.typ !== "access" && payload.typ !== "refresh") ||
    typeof payload.iat !== "number" ||
    !Number.isFinite(payload.iat) ||
    typeof payload.exp !== "number" ||
    !Number.isFinite(payload.exp)
  ) {
    return {
      success: false,
      code: "invalid_payload",
    };
  }

  if (payload.typ !== expectedType) {
    return {
      success: false,
      code: "token_type_mismatch",
    };
  }

  if (payload.typ === "refresh") {
    if (
      typeof payload.sid !== "string" ||
      payload.sid.trim().length === 0 ||
      typeof payload.st !== "string" ||
      payload.st.trim().length === 0
    ) {
      return {
        success: false,
        code: "invalid_payload",
      };
    }
  }

  const nowInSeconds = Math.floor(Date.now() / 1000);
  if (payload.exp <= nowInSeconds) {
    return {
      success: false,
      code: "expired",
    };
  }

  return {
    success: true,
    payload: {
      sub: payload.sub,
      tid: payload.tid,
      typ: payload.typ,
      sid: payload.sid,
      st: payload.st,
      iat: payload.iat,
      exp: payload.exp,
    },
  };
}

export function createAuthSessionToken(): string {
  return toBase64UrlFromBase64(randomBytes(32).toString("base64"));
}

export function getRefreshSessionExpiresAt(): string {
  return new Date(Date.now() + getRefreshTokenExpiresInSeconds() * 1000).toISOString();
}

export function issueAccessToken(input: IssueAccessTokenInput): IssuedToken {
  const expiresIn = getAccessTokenExpiresInSeconds();
  return issueToken(
    buildPayload("access", expiresIn, {
      userId: input.userId,
      tenantId: input.tenantId,
      sessionId: input.sessionId,
    }),
    expiresIn
  );
}

export function issueRefreshToken(input: IssueRefreshTokenInput): IssuedToken {
  const expiresIn = getRefreshTokenExpiresInSeconds();
  return issueToken(
    buildPayload("refresh", expiresIn, {
      userId: input.userId,
      tenantId: input.tenantId,
      sessionId: input.sessionId,
      sessionToken: input.sessionToken,
    }),
    expiresIn
  );
}

export function verifyAccessToken(token: string): VerifyTokenResult {
  return verifyToken(token, "access");
}

export function verifyRefreshToken(token: string): VerifyTokenResult {
  return verifyToken(token, "refresh");
}
