import { createApp } from "./app";

const port = Number(Bun.env.PORT ?? 8080);
const app = createApp();

Bun.serve({
  port,
  fetch: app.fetch,
});

console.info(`[control-plane] 服务已启动：http://localhost:${port}`);
