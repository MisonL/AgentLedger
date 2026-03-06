import { Hono } from "hono";
import { cors } from "hono/cors";
import { errorHandlerMiddleware } from "./middleware/error-handler";
import { loggerMiddleware } from "./middleware/logger";
import { requestIdMiddleware } from "./middleware/request-id";
import { apiV1Routes } from "./routes/api-v1";
import { apiV2Routes } from "./routes/api-v2";
import type { AppEnv } from "./types";

export function createApp() {
  const app = new Hono<AppEnv>();

  app.use("*", requestIdMiddleware);
  app.use("*", loggerMiddleware);
  app.use("*", errorHandlerMiddleware);
  app.use("/api/*", cors());

  app.route("/api/v1", apiV1Routes);
  app.route("/api/v2", apiV2Routes);

  app.notFound((c) => {
    return c.json(
      {
        message: "路由不存在。",
        requestId: c.get("requestId"),
      },
      404
    );
  });

  return app;
}
