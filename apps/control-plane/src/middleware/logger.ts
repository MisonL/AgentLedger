import type { MiddlewareHandler } from "hono";
import type { AppEnv } from "../types";

export const loggerMiddleware: MiddlewareHandler<AppEnv> = async (c, next) => {
  const start = Date.now();
  await next();
  const durationMs = Date.now() - start;
  const requestId = c.get("requestId");

  console.info(
    `[control-plane] ${c.req.method} ${c.req.path} ${c.res.status} ${durationMs}ms rid=${requestId}`
  );
};
