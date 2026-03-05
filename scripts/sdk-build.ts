#!/usr/bin/env bun

import { runSdkGenerateCli } from "./sdk-generate";
import { runSdkVerifyCli } from "./sdk-verify";
import { runSdkTestCli } from "./sdk-test";
import { runSdkBundleCli } from "./sdk-bundle";

interface CliOptions {
  inputPath?: string;
  outputDir: string;
  bundleOutputDir: string;
  help: boolean;
}

type ParseResult =
  | { success: true; options: CliOptions }
  | { success: false; error: string };

function printUsage(): void {
  console.log("用法: bun run ./scripts/sdk-build.ts [--input <openapi.json>] [--output <sdk-dir>] [--bundle-output <dist-dir>]");
  console.log("说明: 一键执行 sdk:generate -> sdk:verify -> sdk:test -> sdk:bundle。\n");
}

function parseCliOptions(argv: string[]): ParseResult {
  const options: CliOptions = {
    outputDir: "clients/sdk",
    bundleOutputDir: "dist/sdk",
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--input" || token === "-i") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供 OpenAPI 路径。` };
      }
      options.inputPath = value;
      index += 1;
      continue;
    }
    if (token === "--output" || token === "-o") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供 SDK 输出目录。` };
      }
      options.outputDir = value;
      index += 1;
      continue;
    }
    if (token === "--bundle-output") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供打包输出目录。` };
      }
      options.bundleOutputDir = value;
      index += 1;
      continue;
    }
    return { success: false, error: `未知参数：${token}` };
  }

  return { success: true, options };
}

export async function runSdkBuildCli(argv: string[]): Promise<number> {
  const parsed = parseCliOptions(argv);
  if (!parsed.success) {
    console.error(`参数错误：${parsed.error}`);
    printUsage();
    return 1;
  }

  const { options } = parsed;
  if (options.help) {
    printUsage();
    return 0;
  }

  const sharedArgs = ["--output", options.outputDir];
  if (options.inputPath) {
    sharedArgs.unshift(options.inputPath);
    sharedArgs.unshift("--input");
  }

  const generateCode = await runSdkGenerateCli(sharedArgs);
  if (generateCode !== 0) {
    return generateCode;
  }

  const verifyCode = await runSdkVerifyCli(sharedArgs);
  if (verifyCode !== 0) {
    return verifyCode;
  }

  const testCode = await runSdkTestCli(["--output", options.outputDir]);
  if (testCode !== 0) {
    return testCode;
  }

  const bundleCode = await runSdkBundleCli([
    "--input",
    options.outputDir,
    "--output",
    options.bundleOutputDir,
  ]);
  if (bundleCode !== 0) {
    return bundleCode;
  }

  console.log("SDK 一键构建完成。\n- 目录生成\n- 校验通过\n- 测试通过\n- 产物打包完成");
  return 0;
}

if (import.meta.main) {
  const exitCode = await runSdkBuildCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
