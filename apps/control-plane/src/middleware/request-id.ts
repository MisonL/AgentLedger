import type { MiddlewareHandler } from "hono";
import type { AppEnv } from "../types";

export const requestIdMiddleware: MiddlewareHandler<AppEnv> = async (c, next) => {
  const incomingId = c.req.header("x-request-id");
  const requestId = incomingId?.trim() || crypto.randomUUID();

  c.set("requestId", requestId);
  c.res.headers.set("x-request-id", requestId);

  await next();

  // 路由处理后再补一次，确保所有响应都带 request id。
  c.res.headers.set("x-request-id", requestId);
};
