const rawUrl = process.env.FR505_WAIT_URL?.trim();
const expectedStatus = process.env.FR505_WAIT_EXPECT_STATUS?.trim();
const timeoutMs = Number.parseInt(process.env.FR505_WAIT_TIMEOUT_MS ?? "20000", 10);
const intervalMs = Number.parseInt(process.env.FR505_WAIT_INTERVAL_MS ?? "250", 10);

if (!rawUrl) {
  console.error("缺少 FR505_WAIT_URL");
  process.exit(2);
}

if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
  console.error("FR505_WAIT_TIMEOUT_MS 必须为正整数");
  process.exit(2);
}

if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
  console.error("FR505_WAIT_INTERVAL_MS 必须为正整数");
  process.exit(2);
}

const deadline = Date.now() + timeoutMs;
let lastError = "";

while (Date.now() < deadline) {
  try {
    const response = await fetch(rawUrl, { method: "GET" });
    const bodyText = await response.text();
    if (!response.ok) {
      lastError = `HTTP ${response.status}: ${bodyText}`;
      await Bun.sleep(intervalMs);
      continue;
    }

    let payload: unknown;
    try {
      payload = JSON.parse(bodyText);
    } catch (error) {
      lastError = `响应不是合法 JSON: ${(error as Error).message}`;
      await Bun.sleep(intervalMs);
      continue;
    }

    const status = (payload as Record<string, unknown>)?.status;
    if (expectedStatus && status !== expectedStatus) {
      lastError = `status 字段不匹配，期望=${expectedStatus} 实际=${String(status)}`;
      await Bun.sleep(intervalMs);
      continue;
    }

    console.log(`探测通过: ${rawUrl}`);
    process.exit(0);
  } catch (error) {
    lastError = (error as Error).message;
    await Bun.sleep(intervalMs);
  }
}

console.error(`探测超时: ${rawUrl} (${timeoutMs}ms)`);
if (lastError) {
  console.error(`最后一次错误: ${lastError}`);
}
process.exit(1);
