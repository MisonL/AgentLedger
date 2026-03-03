const port = Number.parseInt(process.env.FR505_MOCK_INGEST_PORT ?? "18081", 10);

if (!Number.isFinite(port) || port <= 0) {
  console.error("FR505_MOCK_INGEST_PORT 必须为正整数");
  process.exit(2);
}

const server = Bun.serve({
  port,
  async fetch(request) {
    const { pathname } = new URL(request.url);

    if (request.method === "GET" && pathname === "/healthz") {
      return Response.json({ status: "ok", service: "mock-ingestion" });
    }

    if (request.method === "POST" && pathname === "/v1/ingest") {
      let payload: unknown;
      try {
        payload = await request.json();
      } catch {
        return Response.json({ message: "invalid json" }, { status: 400 });
      }

      const requestPayload = payload as Record<string, unknown>;
      const batchID = typeof requestPayload.batch_id === "string" ? requestPayload.batch_id : "smoke-batch";
      const events = Array.isArray(requestPayload.events) ? requestPayload.events : [];

      return Response.json({
        batch_id: batchID,
        accepted: events.length,
        rejected: 0,
        duration_ms: 1,
        errors: [],
      });
    }

    return Response.json({ message: "not found" }, { status: 404 });
  },
});

console.log(`[mock-ingestion] listening on http://127.0.0.1:${server.port}`);
await new Promise(() => {});
