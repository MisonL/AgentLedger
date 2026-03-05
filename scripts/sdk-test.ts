#!/usr/bin/env bun

import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { asRecord } from "./sdk-common";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

interface CliOptions {
  outputDir: string;
  help: boolean;
}

type ParseResult =
  | { success: true; options: CliOptions }
  | { success: false; error: string };

function printUsage(): void {
  console.log("用法: bun run ./scripts/sdk-test.ts [--output <dir>]");
  console.log("说明: 校验 8 语言 SDK 是否完整覆盖 operations，并检查入口源码包含对应方法名。\n");
}

function parseCliOptions(argv: string[]): ParseResult {
  const options: CliOptions = {
    outputDir: "clients/sdk",
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--output" || token === "-o") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供输出目录。` };
      }
      options.outputDir = value;
      index += 1;
      continue;
    }
    return { success: false, error: `未知参数：${token}` };
  }

  return { success: true, options };
}

interface OperationLike {
  id?: unknown;
  methodNameCamel?: unknown;
  methodNameSnake?: unknown;
  methodNamePascal?: unknown;
}

interface LanguageLike {
  id?: unknown;
  directory?: unknown;
  operationCount?: unknown;
}

function resolveEntryFile(languageId: string): string {
  switch (languageId) {
    case "typescript":
      return "src/index.ts";
    case "python":
      return "agentledger_sdk/client.py";
    case "go":
      return "client.go";
    case "java":
      return "src/main/java/com/agentledger/sdk/AgentLedgerClient.java";
    case "csharp":
      return "AgentLedgerClient.cs";
    case "php":
      return "src/AgentLedgerClient.php";
    case "ruby":
      return "lib/agentledger_sdk/client.rb";
    case "swift":
      return "Sources/AgentLedgerSDK/AgentLedgerClient.swift";
    default:
      return "README.md";
  }
}

function extractMethodName(operation: OperationLike, languageId: string): string {
  if (languageId === "go" || languageId === "csharp") {
    return typeof operation.methodNamePascal === "string" ? operation.methodNamePascal : "";
  }
  if (languageId === "python" || languageId === "ruby") {
    return typeof operation.methodNameSnake === "string" ? operation.methodNameSnake : "";
  }
  return typeof operation.methodNameCamel === "string" ? operation.methodNameCamel : "";
}

async function mustReadJson(filePath: string): Promise<unknown> {
  const content = await readFile(filePath, "utf8");
  return JSON.parse(content) as unknown;
}

export async function runSdkTestCli(argv: string[]): Promise<number> {
  const parsed = parseCliOptions(argv);
  if (!parsed.success) {
    console.error(`参数错误：${parsed.error}`);
    printUsage();
    return 1;
  }

  if (parsed.options.help) {
    printUsage();
    return 0;
  }

  const outputDir = path.resolve(repoRoot, parsed.options.outputDir);
  const issues: string[] = [];

  try {
    const metadataPath = path.join(outputDir, "metadata.json");
    const rootOperationsPath = path.join(outputDir, "operations.json");
    const metadata = asRecord(await mustReadJson(metadataPath));
    const rootOperationsRaw = await mustReadJson(rootOperationsPath);

    if (!metadata) {
      console.error("metadata.json 非法，必须是对象。");
      return 1;
    }

    if (!Array.isArray(rootOperationsRaw)) {
      console.error("operations.json 非法，必须是数组。");
      return 1;
    }

    const operations = rootOperationsRaw as OperationLike[];
    if (!Array.isArray(metadata.languages)) {
      console.error("metadata.languages 非法，必须是数组。");
      return 1;
    }

    for (const languageRaw of metadata.languages as unknown[]) {
      const language = languageRaw as LanguageLike;
      const languageId = typeof language.id === "string" ? language.id : "";
      const languageDir =
        typeof language.directory === "string" && language.directory.trim().length > 0
          ? language.directory
          : languageId;
      if (!languageId || !languageDir) {
        issues.push("metadata.languages 含非法项（id/directory 缺失）。");
        continue;
      }

      const expectedCount = Number(language.operationCount);
      if (!Number.isInteger(expectedCount) || expectedCount !== operations.length) {
        issues.push(`${languageId} 的 operationCount 与 root operations 不一致。`);
      }

      const languageRoot = path.join(outputDir, languageDir);
      const languageOperationsPath = path.join(languageRoot, "operations.json");
      const entryFile = path.join(languageRoot, resolveEntryFile(languageId));

      try {
        await access(languageOperationsPath);
      } catch {
        issues.push(`${languageId} 缺少 operations.json。`);
        continue;
      }
      try {
        await access(entryFile);
      } catch {
        issues.push(`${languageId} 缺少入口源码：${resolveEntryFile(languageId)}`);
        continue;
      }

      const languageOperationsRaw = await mustReadJson(languageOperationsPath);
      if (!Array.isArray(languageOperationsRaw)) {
        issues.push(`${languageId} operations.json 非数组。`);
        continue;
      }
      if (languageOperationsRaw.length !== operations.length) {
        issues.push(`${languageId} operations 数量不匹配。`);
      }

      const sourceText = await readFile(entryFile, "utf8");
      for (const operation of operations) {
        if (typeof operation.id !== "string") {
          continue;
        }
        if (!sourceText.includes(operation.id)) {
          issues.push(`${languageId} 入口源码缺少 operation 标记：${operation.id}`);
          continue;
        }
        const methodName = extractMethodName(operation, languageId);
        if (methodName && !sourceText.includes(methodName)) {
          issues.push(`${languageId} 入口源码缺少 operation 方法：${methodName}`);
        }
      }
    }

    if (issues.length > 0) {
      console.error("SDK 测试失败：");
      for (const issue of issues) {
        console.error(`- ${issue}`);
      }
      return 1;
    }

    console.log(`SDK 测试通过：${path.relative(repoRoot, outputDir) || outputDir}`);
    console.log(`覆盖 operation 数量：${operations.length}`);
    return 0;
  } catch (error) {
    console.error("SDK 测试执行失败。", error);
    return 1;
  }
}

if (import.meta.main) {
  const exitCode = await runSdkTestCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
