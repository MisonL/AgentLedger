#!/usr/bin/env bun

import { getControlPlaneRepository } from "../apps/control-plane/src/data/repository";

interface CliOptions {
  tenantId?: string;
  sourceIds: string[];
  dryRun: boolean;
  output: "text" | "json";
  help: boolean;
}

function parseCliOptions(argv: string[]): { success: true; options: CliOptions } | { success: false; error: string } {
  const options: CliOptions = {
    sourceIds: [],
    dryRun: false,
    output: "text",
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--tenant") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        return { success: false, error: "--tenant 需要提供 tenantId。" };
      }
      options.tenantId = value.trim();
      index += 1;
      continue;
    }
    if (token === "--source") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        return { success: false, error: "--source 需要提供 sourceId。" };
      }
      const normalized = value.trim();
      if (normalized.length === 0) {
        return { success: false, error: "--source 需要提供非空 sourceId。" };
      }
      options.sourceIds.push(normalized);
      index += 1;
      continue;
    }
    if (token === "--dry-run") {
      options.dryRun = true;
      continue;
    }
    if (token === "--output") {
      const value = argv[index + 1];
      if (value !== "text" && value !== "json") {
        return { success: false, error: "--output 仅支持 text/json。" };
      }
      options.output = value;
      index += 1;
      continue;
    }
    return { success: false, error: `未知参数：${token}` };
  }

  options.sourceIds = Array.from(new Set(options.sourceIds));
  return { success: true, options };
}

function printUsage(): void {
  console.log(
    "用法: bun run ./scripts/backfill-source-region.ts --tenant <tenantId> [--source <sourceId>] [--source <sourceId>] [--dry-run] [--output text|json]"
  );
  console.log("说明: 按租户主地域回填缺失的 sources.source_region；默认真实执行，--dry-run 仅预演。");
}

export async function runBackfillSourceRegionCli(argv: string[]): Promise<number> {
  const parsed = parseCliOptions(argv);
  if (!parsed.success) {
    console.error(`参数错误: ${parsed.error}`);
    printUsage();
    return 1;
  }

  const { options } = parsed;
  if (options.help) {
    printUsage();
    return 0;
  }

  if (!options.tenantId) {
    console.error("参数错误: 必须提供 --tenant。");
    printUsage();
    return 1;
  }

  const repository = getControlPlaneRepository();
  const result = await repository.backfillSourceRegionsFromTenantPrimaryRegion(options.tenantId, {
    dryRun: options.dryRun,
    sourceIds: options.sourceIds.length > 0 ? options.sourceIds : undefined,
  });

  if (!result) {
    console.error(`回填失败: tenant ${options.tenantId} 未配置主地域。`);
    return 1;
  }

  if (options.output === "json") {
    console.log(JSON.stringify(result, null, 2));
    return 0;
  }

  console.log(
    `[source-region-backfill] tenant=${result.tenantId} primaryRegion=${result.primaryRegion} dryRun=${String(
      result.dryRun
    )}`
  );
  console.log(
    `[source-region-backfill] totalMissing=${result.totalMissing} updated=${result.updated} skipped=${result.skipped}`
  );
  for (const item of result.items) {
    console.log(
      `- ${item.sourceId} ${item.status} ${item.appliedRegion ?? ""} ${item.reason ?? ""}`.trim()
    );
  }
  return 0;
}

if (import.meta.main) {
  const exitCode = await runBackfillSourceRegionCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
