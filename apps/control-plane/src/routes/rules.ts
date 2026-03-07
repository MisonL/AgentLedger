import { Hono, type Context } from "hono";
import {
  type RuleAssetVersionDiffLine,
  type RuleAssetVersionDiffResponse,
  validateRuleApprovalCreateInput,
  validateRuleApprovalListInput,
  validateRuleAssetCreateInput,
  validateRuleAssetListInput,
  validateRuleAssetVersionCreateInput,
  validateRulePublishInput,
  validateRuleRollbackInput,
} from "../contracts";
import type { AppendAuditLogInput } from "../data/repository";
import { getControlPlaneRepository } from "../data/repository";
import { authMiddleware } from "../middleware/auth";
import type { AppEnv } from "../types";

export const ruleRoutes = new Hono<AppEnv>();
const repository = getControlPlaneRepository();
const WRITABLE_ROLES = new Set(["owner", "maintainer"]);

async function appendAuditLogSafely(input: AppendAuditLogInput): Promise<void> {
  try {
    await repository.appendAuditLog(input);
  } catch (error) {
    console.warn("[control-plane] 写入 rules 审计日志失败。", error);
  }
}

function unauthorized(c: Context<AppEnv>) {
  return c.json({ message: "未认证：请先登录。" }, 401);
}

function forbidden(c: Context<AppEnv>, mode: "read" | "write") {
  if (mode === "write") {
    return c.json(
      { message: "无写入权限：仅 owner/maintainer 可执行写操作。" },
      403,
    );
  }
  return c.json({ message: "无权访问该租户资源。" }, 403);
}

async function requireAuthContext(c: Context<AppEnv>) {
  const authResult = await authMiddleware(c, async () => {});
  if (authResult instanceof Response) {
    return authResult;
  }
  const auth = c.get("auth");
  if (!auth) {
    return unauthorized(c);
  }
  return auth;
}

async function requireTenantAccess(c: Context<AppEnv>, mode: "read" | "write") {
  const auth = await requireAuthContext(c);
  if (auth instanceof Response) {
    return auth;
  }
  const membership = await repository.getTenantMemberByUser(
    auth.tenantId,
    auth.userId,
  );
  if (!membership) {
    return forbidden(c, mode);
  }
  if (mode === "write" && !WRITABLE_ROLES.has(membership.tenantRole)) {
    return forbidden(c, mode);
  }
  return auth;
}

function parseVersionLimit(value: string | undefined): number | null {
  if (value === undefined) {
    return 50;
  }
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return 50;
  }
  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 200) {
    return null;
  }
  return parsed;
}

function parseRuleVersionNumber(value: string | undefined): number | null {
  if (value === undefined) {
    return null;
  }
  const trimmed = value.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return null;
  }
  return parsed;
}

function splitRuleVersionContent(content: string): string[] {
  if (content.length === 0) {
    return [];
  }
  return content.split(/\r?\n/);
}

function buildRuleVersionDiffLines(
  fromContent: string,
  toContent: string,
): RuleAssetVersionDiffResponse {
  const fromLines = splitRuleVersionContent(fromContent);
  const toLines = splitRuleVersionContent(toContent);
  const dp = Array.from({ length: fromLines.length + 1 }, () =>
    Array<number>(toLines.length + 1).fill(0),
  );

  for (let fromIndex = fromLines.length - 1; fromIndex >= 0; fromIndex -= 1) {
    for (let toIndex = toLines.length - 1; toIndex >= 0; toIndex -= 1) {
      if (fromLines[fromIndex] === toLines[toIndex]) {
        dp[fromIndex]![toIndex] =
          (dp[fromIndex + 1]?.[toIndex + 1] ?? 0) + 1;
        continue;
      }
      dp[fromIndex]![toIndex] = Math.max(
        dp[fromIndex + 1]?.[toIndex] ?? 0,
        dp[fromIndex]?.[toIndex + 1] ?? 0,
      );
    }
  }

  const lines: RuleAssetVersionDiffLine[] = [];
  let fromIndex = 0;
  let toIndex = 0;
  let oldLineNumber = 1;
  let newLineNumber = 1;
  let added = 0;
  let removed = 0;
  let unchanged = 0;

  while (fromIndex < fromLines.length && toIndex < toLines.length) {
    if (fromLines[fromIndex] === toLines[toIndex]) {
      lines.push({
        type: "unchanged",
        content: fromLines[fromIndex] ?? "",
        oldLineNumber,
        newLineNumber,
      });
      fromIndex += 1;
      toIndex += 1;
      oldLineNumber += 1;
      newLineNumber += 1;
      unchanged += 1;
      continue;
    }

    if ((dp[fromIndex + 1]?.[toIndex] ?? 0) >= (dp[fromIndex]?.[toIndex + 1] ?? 0)) {
      lines.push({
        type: "removed",
        content: fromLines[fromIndex] ?? "",
        oldLineNumber,
      });
      fromIndex += 1;
      oldLineNumber += 1;
      removed += 1;
      continue;
    }

    lines.push({
      type: "added",
      content: toLines[toIndex] ?? "",
      newLineNumber,
    });
    toIndex += 1;
    newLineNumber += 1;
    added += 1;
  }

  while (fromIndex < fromLines.length) {
    lines.push({
      type: "removed",
      content: fromLines[fromIndex] ?? "",
      oldLineNumber,
    });
    fromIndex += 1;
    oldLineNumber += 1;
    removed += 1;
  }

  while (toIndex < toLines.length) {
    lines.push({
      type: "added",
      content: toLines[toIndex] ?? "",
      newLineNumber,
    });
    toIndex += 1;
    newLineNumber += 1;
    added += 1;
  }

  return {
    assetId: "",
    fromVersion: 0,
    toVersion: 0,
    lines,
    summary: {
      added,
      removed,
      unchanged,
      changed: added > 0 || removed > 0,
    },
  };
}

ruleRoutes.get("/rules/assets", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const result = validateRuleAssetListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const payload = await repository.listRuleAssets(auth.tenantId, result.data);
  return c.json({
    items: payload.items,
    total: payload.total,
    filters: result.data,
  });
});

ruleRoutes.post("/rules/assets", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }

  const body = await c.req.json().catch(() => undefined);
  const result = validateRuleAssetCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const asset = await repository.createRuleAsset(auth.tenantId, result.data);
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.rule_asset_created",
    level: "info",
    detail: `Created rule asset ${asset.id}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: asset.id,
      status: asset.status,
      latestVersion: asset.latestVersion,
      requiredApprovals: asset.requiredApprovals,
    },
  });
  return c.json(asset, 201);
});

ruleRoutes.get("/rules/assets/:id", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }

  const asset = await repository.getRuleAssetById(auth.tenantId, assetId);
  if (!asset) {
    return c.json({ message: `未找到规则资产 ${assetId}。` }, 404);
  }
  return c.json(asset);
});

ruleRoutes.get("/rules/assets/:id/versions", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }

  const limit = parseVersionLimit(c.req.query("limit"));
  if (limit === null) {
    return c.json({ message: "limit 必须是 1 到 200 的整数。" }, 400);
  }
  const items = await repository.listRuleAssetVersions(
    auth.tenantId,
    assetId,
    limit,
  );
  return c.json({
    items,
    total: items.length,
  });
});

ruleRoutes.get("/rules/assets/:id/versions/diff", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }

  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }

  const fromVersion = parseRuleVersionNumber(c.req.query("fromVersion"));
  const toVersion = parseRuleVersionNumber(c.req.query("toVersion"));
  if (fromVersion === null || toVersion === null) {
    return c.json(
      { message: "fromVersion 和 toVersion 必须是正整数。" },
      400,
    );
  }
  if (fromVersion === toVersion) {
    return c.json(
      { message: "fromVersion 和 toVersion 不能相同。" },
      400,
    );
  }

  const [fromVersionItem, toVersionItem] = await Promise.all([
    repository.getRuleAssetVersionByNumber(auth.tenantId, assetId, fromVersion),
    repository.getRuleAssetVersionByNumber(auth.tenantId, assetId, toVersion),
  ]);
  if (!fromVersionItem || !toVersionItem) {
    return c.json(
      {
        message: `未找到规则版本 diff：fromVersion=${fromVersion}、toVersion=${toVersion}。`,
      },
      404,
    );
  }

  const diff = buildRuleVersionDiffLines(
    fromVersionItem.content,
    toVersionItem.content,
  );
  diff.assetId = assetId;
  diff.fromVersion = fromVersionItem.version;
  diff.toVersion = toVersionItem.version;
  return c.json(diff);
});

ruleRoutes.post("/rules/assets/:id/versions", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateRuleAssetVersionCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }

  const version = await repository.createRuleAssetVersion(
    auth.tenantId,
    assetId,
    result.data,
    {
      createdByUserId: auth.userId,
    },
  );
  if (!version) {
    return c.json({ message: `未找到规则资产 ${assetId}。` }, 404);
  }
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.rule_asset_version_created",
    level: "info",
    detail: `Created rule version ${version.version} for asset ${assetId}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: assetId,
      version: version.version,
    },
  });
  return c.json(version, 201);
});

ruleRoutes.post("/rules/assets/:id/publish", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateRulePublishInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const requestedVersion = result.data.version;
  const currentAsset = await repository.getRuleAssetById(auth.tenantId, assetId);
  if (!currentAsset) {
    return c.json({ message: `未找到规则资产 ${assetId}。` }, 404);
  }
  const targetVersion = await repository.getRuleAssetVersionByNumber(
    auth.tenantId,
    assetId,
    requestedVersion,
  );
  if (!targetVersion) {
    return c.json(
      { message: `规则版本 ${requestedVersion} 不存在，无法发布。` },
      409,
    );
  }
  let approvalSummary:
    | {
        approved: number;
        rejected: number;
      }
    | undefined;
  if (currentAsset.requiredApprovals > 1) {
    approvalSummary = await repository.getRuleApprovalSummary(
      auth.tenantId,
      assetId,
      requestedVersion,
    );
    if (approvalSummary.approved < currentAsset.requiredApprovals) {
      return c.json(
        {
          message: `规则版本 ${requestedVersion} 需要 ${currentAsset.requiredApprovals} 个 approved 审批，当前仅 ${approvalSummary.approved} 个。`,
        },
        409,
      );
    }
  }
  const asset = await repository.publishRuleAssetVersion(
    auth.tenantId,
    assetId,
    result.data,
  );
  if (!asset) {
    return c.json({ message: `未找到规则资产 ${assetId}。` }, 404);
  }
  if (asset.publishedVersion !== requestedVersion) {
    return c.json(
      { message: `规则版本 ${requestedVersion} 不存在，无法发布。` },
      409,
    );
  }
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.rule_asset_published",
    level: "info",
    detail: `Published rule asset ${asset.id} to version ${asset.publishedVersion}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: asset.id,
      version: requestedVersion,
      publishedVersion: asset.publishedVersion,
      status: asset.status,
      requiredApprovals: currentAsset.requiredApprovals,
      approvedApprovals: approvalSummary?.approved ?? 0,
      publishedByUserId: auth.userId,
    },
  });
  return c.json(asset);
});

ruleRoutes.post("/rules/assets/:id/rollback", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateRuleRollbackInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const requestedVersion = result.data.version;
  const asset = await repository.rollbackRuleAssetVersion(
    auth.tenantId,
    assetId,
    result.data,
  );
  if (!asset) {
    return c.json({ message: `未找到规则资产 ${assetId}。` }, 404);
  }
  if (asset.publishedVersion !== requestedVersion) {
    return c.json(
      { message: `规则版本 ${requestedVersion} 不存在，无法回滚。` },
      409,
    );
  }
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: "control_plane.rule_asset_rolled_back",
    level: "warning",
    detail: `Rolled back rule asset ${asset.id} to version ${asset.publishedVersion}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: asset.id,
      version: requestedVersion,
      publishedVersion: asset.publishedVersion,
      status: asset.status,
      rolledBackByUserId: auth.userId,
      reason: result.data.reason,
    },
  });
  return c.json(asset);
});

ruleRoutes.get("/rules/assets/:id/approvals", async (c) => {
  const auth = await requireTenantAccess(c, "read");
  if (auth instanceof Response) {
    return auth;
  }
  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }
  const result = validateRuleApprovalListInput(c.req.query());
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const payload = await repository.listRuleApprovals(
    auth.tenantId,
    assetId,
    result.data,
  );
  return c.json({
    items: payload.items,
    total: payload.total,
    filters: result.data,
  });
});

ruleRoutes.post("/rules/assets/:id/approvals", async (c) => {
  const auth = await requireTenantAccess(c, "write");
  if (auth instanceof Response) {
    return auth;
  }
  const assetId = c.req.param("id")?.trim();
  if (!assetId) {
    return c.json({ message: "assetId 必须为非空字符串。" }, 400);
  }
  const body = await c.req.json().catch(() => undefined);
  const result = validateRuleApprovalCreateInput(body);
  if (!result.success) {
    return c.json({ message: result.error }, 400);
  }
  const approvalResult = await repository.createRuleApproval(
    auth.tenantId,
    assetId,
    result.data,
    {
      approverUserId: auth.userId,
      approverEmail: auth.email,
    },
  );
  if (!approvalResult) {
    return c.json({ message: "规则版本不存在，无法提交审批。" }, 404);
  }
  const { approval, created } = approvalResult;
  const requestId = c.get("requestId");
  await appendAuditLogSafely({
    tenantId: auth.tenantId,
    eventId: `cp:${requestId}`,
    action: created
      ? "control_plane.rule_approval_created"
      : "control_plane.rule_approval_updated",
    level: "info",
    detail: created
      ? `Created rule approval ${approval.id} for asset ${assetId} version ${approval.version}.`
      : `Updated rule approval ${approval.id} for asset ${assetId} version ${approval.version}.`,
    metadata: {
      requestId,
      tenantId: auth.tenantId,
      resourceId: approval.id,
      assetId,
      operation: created ? "created" : "updated",
      version: approval.version,
      decision: approval.decision,
      approverUserId: approval.approverUserId,
      approverEmail: approval.approverEmail,
      reason: approval.reason,
    },
  });
  return c.json(approval, created ? 201 : 200);
});
