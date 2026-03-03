import type { MiddlewareHandler } from "hono";
import { HTTPException } from "hono/http-exception";
import type { AppEnv } from "../types";

export const errorHandlerMiddleware: MiddlewareHandler<AppEnv> = async (c, next) => {
  try {
    await next();
  } catch (error) {
    const requestId = c.get("requestId");

    if (error instanceof HTTPException) {
      return c.json(
        {
          message: error.message || "请求处理失败。",
          requestId,
        },
        error.status
      );
    }

    console.error("[control-plane] 未处理异常", {
      requestId,
      error,
    });

    return c.json(
      {
        message: "服务器内部错误。",
        requestId,
      },
      500
    );
  }
};
