interface TimePaginationCursor {
  timestamp: string;
  id: string;
}

function decodeTimePaginationCursor(raw: string): TimePaginationCursor | null {
  let decoded: unknown;
  try {
    decoded = JSON.parse(Buffer.from(raw, "base64url").toString("utf8"));
  } catch {
    return null;
  }

  if (typeof decoded !== "object" || decoded === null) {
    return null;
  }
  const record = decoded as Record<string, unknown>;
  const timestamp =
    typeof record.timestamp === "string" ? record.timestamp.trim() : "";
  const id = typeof record.id === "string" ? record.id.trim() : "";
  if (!timestamp || !id) {
    return null;
  }
  if (!Number.isFinite(Date.parse(timestamp))) {
    return null;
  }
  return { timestamp, id };
}

export function parseOptionalTimePaginationCursor(
  value: string | undefined,
): { success: true; cursor?: string } | { success: false; error: string } {
  if (value === undefined) {
    return { success: true };
  }
  const normalized = value.trim();
  if (normalized.length === 0) {
    return { success: false, error: "cursor 必须为非空字符串。" };
  }
  if (!decodeTimePaginationCursor(normalized)) {
    return { success: false, error: "cursor 格式非法。" };
  }
  return {
    success: true,
    cursor: normalized,
  };
}
