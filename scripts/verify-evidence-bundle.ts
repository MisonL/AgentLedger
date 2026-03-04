#!/usr/bin/env bun

import { verifyEvidenceBundle } from "../apps/control-plane/src/security/evidence-bundle";

interface CliOptions {
  filePath?: string;
  signingKey?: string;
  help: boolean;
}

function parseCliOptions(argv: string[]): { success: true; options: CliOptions } | { success: false; error: string } {
  const options: CliOptions = {
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--file") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        return { success: false, error: "--file 需要提供文件路径。" };
      }
      options.filePath = value;
      index += 1;
      continue;
    }
    if (token === "--signing-key") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        return { success: false, error: "--signing-key 需要提供密钥值。" };
      }
      options.signingKey = value;
      index += 1;
      continue;
    }
    return { success: false, error: `未知参数：${token}` };
  }

  return { success: true, options };
}

function printUsage(): void {
  console.log("用法: bun run ./scripts/verify-evidence-bundle.ts --file <bundle.json> [--signing-key <secret>]");
  console.log("说明: 若未传 --signing-key，则读取环境变量 EVIDENCE_BUNDLE_SIGNING_KEY。");
}

export async function runEvidenceBundleVerifyCli(argv: string[]): Promise<number> {
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

  if (!options.filePath) {
    console.error("参数错误: 必须提供 --file。");
    printUsage();
    return 1;
  }

  const signingKey = options.signingKey ?? Bun.env.EVIDENCE_BUNDLE_SIGNING_KEY?.trim();
  if (!signingKey) {
    console.error("校验失败: 未提供签名密钥，请设置 --signing-key 或 EVIDENCE_BUNDLE_SIGNING_KEY。");
    return 1;
  }

  let payload: unknown;
  try {
    const content = await Bun.file(options.filePath).text();
    payload = JSON.parse(content);
  } catch (error) {
    console.error(`校验失败: 无法读取或解析文件 ${options.filePath}。`, error);
    return 1;
  }

  const verifyResult = verifyEvidenceBundle(payload, signingKey);
  if (!verifyResult.success) {
    console.error("校验失败:");
    for (const issue of verifyResult.errors) {
      console.error(`- ${issue}`);
    }
    return 1;
  }

  const bundle = payload as {
    manifest?: { recordCount?: unknown; tenantId?: unknown };
    rootHash?: unknown;
  };
  console.log("校验通过。");
  console.log(`tenantId: ${String(bundle.manifest?.tenantId ?? "")}`);
  console.log(`recordCount: ${String(bundle.manifest?.recordCount ?? "")}`);
  console.log(`rootHash: ${String(bundle.rootHash ?? "")}`);
  return 0;
}

if (import.meta.main) {
  const exitCode = await runEvidenceBundleVerifyCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
