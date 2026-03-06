import { afterEach, describe, expect, test, vi } from "bun:test";
import { getControlPlaneRepository } from "../apps/control-plane/src/data/repository";
import { runBackfillSourceRegionCli } from "./backfill-source-region";

const repository = getControlPlaneRepository();

afterEach(() => {
  vi.restoreAllMocks();
});

describe("backfill-source-region cli", () => {
  test("dry-run 返回 0 并输出 would_update 结果", async () => {
    const tenantId = `tenant-backfill-dry-run-${Date.now().toString(36)}`;
    await repository.upsertTenantResidencyPolicy(tenantId, {
      tenantId,
      mode: "single_region",
      primaryRegion: "cn-shanghai",
      replicaRegions: [],
      allowCrossRegionTransfer: false,
      requireTransferApproval: false,
      updatedAt: new Date().toISOString(),
    });
    const source = await repository.createSource(tenantId, {
      name: `source-dry-run-${tenantId}`,
      type: "local",
      location: `~/.codex/sessions/${tenantId}`,
    });

    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    const exitCode = await runBackfillSourceRegionCli([
      "--tenant",
      tenantId,
      "--source",
      source.id,
      "--dry-run",
      "--output",
      "json",
    ]);

    expect(exitCode).toBe(0);
    const payload = JSON.parse(String(logSpy.mock.calls[0]?.[0] ?? "{}")) as {
      dryRun: boolean;
      items: Array<{ sourceId: string; status: string; appliedRegion?: string }>;
    };
    expect(payload.dryRun).toBe(true);
    expect(payload.items).toHaveLength(1);
    expect(payload.items[0]).toMatchObject({
      sourceId: source.id,
      status: "would_update",
      appliedRegion: "cn-shanghai",
    });
  });

  test("真实执行后会写回 sourceRegion", async () => {
    const tenantId = `tenant-backfill-apply-${Date.now().toString(36)}`;
    await repository.upsertTenantResidencyPolicy(tenantId, {
      tenantId,
      mode: "single_region",
      primaryRegion: "cn-hangzhou",
      replicaRegions: [],
      allowCrossRegionTransfer: false,
      requireTransferApproval: false,
      updatedAt: new Date().toISOString(),
    });
    const source = await repository.createSource(tenantId, {
      name: `source-apply-${tenantId}`,
      type: "local",
      location: `~/.codex/sessions/${tenantId}`,
    });

    const exitCode = await runBackfillSourceRegionCli([
      "--tenant",
      tenantId,
      "--source",
      source.id,
    ]);

    expect(exitCode).toBe(0);
    const listed = await repository.listSources(tenantId);
    const updated = listed.find((item) => item.id === source.id);
    expect(updated?.sourceRegion).toBe("cn-hangzhou");
  });

  test("未配置主地域时返回非 0", async () => {
    const tenantId = `tenant-backfill-no-policy-${Date.now().toString(36)}`;
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const exitCode = await runBackfillSourceRegionCli([
      "--tenant",
      tenantId,
    ]);

    expect(exitCode).toBe(1);
    expect(String(errorSpy.mock.calls[0]?.[0] ?? "")).toContain("未配置主地域");
  });
});
