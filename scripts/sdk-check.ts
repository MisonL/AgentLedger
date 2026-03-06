#!/usr/bin/env bun

import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runSdkGenerateCli } from "./sdk-generate";
import { runSdkTestCli } from "./sdk-test";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

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
  console.log(
    "用法: bun run ./scripts/sdk-check.ts [--input <openapi.json>] [--output <sdk-dir>] [--bundle-output <dist-sdk-dir>]"
  );
  console.log(
    "说明: 纯只读校验 SDK 一致性（不写入文件）：clients/sdk + dist/sdk + SHA256SUMS。\n"
  );
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
        return { success: false, error: `${token} 需要提供 SDK 打包目录。` };
      }
      options.bundleOutputDir = value;
      index += 1;
      continue;
    }
    return { success: false, error: `未知参数：${token}` };
  }

  return { success: true, options };
}

function sha256Hex(content: Buffer): string {
  return createHash("sha256").update(content).digest("hex");
}

async function verifySha256Sums(bundleDir: string): Promise<string[]> {
  const issues: string[] = [];
  const absoluteBundleDir = path.resolve(repoRoot, bundleDir);
  const sumsPath = path.join(absoluteBundleDir, "SHA256SUMS.txt");

  let sumsContent = "";
  try {
    sumsContent = await readFile(sumsPath, "utf8");
  } catch (error) {
    issues.push(`${bundleDir}/SHA256SUMS.txt 不存在或不可读。`);
    return issues;
  }

  const lines = sumsContent
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length === 0) {
    issues.push(`${bundleDir}/SHA256SUMS.txt 为空。`);
    return issues;
  }

  for (const line of lines) {
    const match = line.match(/^([a-f0-9]{64})\s{2}(.+)$/i);
    if (!match) {
      issues.push(`SHA256SUMS 格式非法：${line}`);
      continue;
    }
    const expectedHash = match[1].toLowerCase();
    const relativePath = match[2];
    const filePath = path.join(absoluteBundleDir, relativePath);
    try {
      const content = await readFile(filePath);
      const actualHash = sha256Hex(content);
      if (actualHash !== expectedHash) {
        issues.push(`SHA256 不匹配：${bundleDir}/${relativePath}`);
      }
    } catch {
      issues.push(`SHA256SUMS 引用文件不存在：${bundleDir}/${relativePath}`);
    }
  }

  return issues;
}

export async function runSdkCheckCli(argv: string[]): Promise<number> {
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

  const checkArgs = ["--output", options.outputDir, "--check"];
  if (options.inputPath) {
    checkArgs.unshift(options.inputPath);
    checkArgs.unshift("--input");
  }
  const checkClientsCode = await runSdkGenerateCli(checkArgs);
  if (checkClientsCode !== 0) {
    return checkClientsCode;
  }
  const clientsTestCode = await runSdkTestCli(["--output", options.outputDir]);
  if (clientsTestCode !== 0) {
    return clientsTestCode;
  }

  const checkBundleArgs = ["--output", options.bundleOutputDir, "--check"];
  if (options.inputPath) {
    checkBundleArgs.unshift(options.inputPath);
    checkBundleArgs.unshift("--input");
  }
  const checkBundleCode = await runSdkGenerateCli(checkBundleArgs);
  if (checkBundleCode !== 0) {
    return checkBundleCode;
  }
  const bundleTestCode = await runSdkTestCli(["--output", options.bundleOutputDir]);
  if (bundleTestCode !== 0) {
    return bundleTestCode;
  }

  const hashIssues = await verifySha256Sums(options.bundleOutputDir);
  if (hashIssues.length > 0) {
    console.error("SDK SHA256SUMS 校验失败：");
    for (const issue of hashIssues) {
      console.error(`- ${issue}`);
    }
    return 1;
  }

  console.log("SDK 只读校验通过：clients/sdk + dist/sdk + SHA256SUMS");
  return 0;
}

if (import.meta.main) {
  const exitCode = await runSdkCheckCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}

