import { Hono, type Context } from "hono";
import { validatePricingCatalogUpsertInput } from "../contracts";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const pricingRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const DEFAULT_PRICING_CATALOG_VERSION_LIMIT = 20;
const MAX_PRICING_CATALOG_VERSION_LIMIT = 200;

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }

  const auth = c.get("auth");
  if (!auth) {
    return c.json({ message: "未认证：请先登录。" }, 401);
  }
  return auth;
}

function parsePricingCatalogVersionLimit(
  value: string | undefined
): { success: true; limit: number } | { success: false; error: string } {
  if (value === undefined) {
    return { success: true, limit: DEFAULT_PRICING_CATALOG_VERSION_LIMIT };
  }

  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return { success: false, error: "limit 必须是 1 到 200 的整数。" };
  }

  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > MAX_PRICING_CATALOG_VERSION_LIMIT) {
    return { success: false, error: "limit 必须是 1 到 200 的整数。" };
  }

  return { success: true, limit: parsed };
}

pricingRoutes.get("/pricing/catalog", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const catalog = await repository.getPricingCatalog(auth.tenantId);
  if (!catalog) {
    return c.json({ message: "当前租户尚未配置 pricing catalog。" }, 404);
  }

  return c.json(catalog);
});

pricingRoutes.get("/pricing/catalog/versions", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const limitResult = parsePricingCatalogVersionLimit(c.req.query("limit"));
  if (!limitResult.success) {
    return c.json({ message: limitResult.error }, 400);
  }

  const items = await repository.listPricingCatalogVersions(auth.tenantId, limitResult.limit);
  return c.json({
    items,
    total: items.length,
    limit: limitResult.limit,
  });
});

pricingRoutes.put("/pricing/catalog", async (c) => {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validatePricingCatalogUpsertInput(body);
  if (!result.success) {
    return c.json(
      {
        message: result.error,
      },
      400
    );
  }

  const catalog = await repository.upsertPricingCatalog(auth.tenantId, result.data);
  return c.json(catalog);
});
