#!/usr/bin/env bun

import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildOpenApiDocument } from "../apps/control-plane/src/routes/open-platform";
import {
  asRecord,
  computeDigest,
  extractOpenApiOperations,
  toRepoRelativePath,
  type OpenApiOperationSpec,
} from "./sdk-common";
import { runSdkGenerateCli } from "./sdk-generate";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

interface CliOptions {
  inputPath?: string;
  outputDir: string;
  help: boolean;
}

type ParseResult =
  | { success: true; options: CliOptions }
  | { success: false; error: string };

function printUsage(): void {
  console.log("用法: bun run ./scripts/sdk-verify.ts [--input <openapi.json>] [--output <dir>]");
  console.log("说明: 默认校验并生成到 clients/sdk，执行 generate + check + metadata 覆盖验证。");
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
    if (token === "--input" || token === "-i") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供本地 OpenAPI 文件路径。` };
      }
      options.inputPath = value;
      index += 1;
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

interface OpenApiInput {
  inputPath?: string;
  sourceLabel: string;
  document: Record<string, unknown>;
}

async function loadOpenApiFromFile(inputPath: string): Promise<Record<string, unknown>> {
  const absolutePath = path.resolve(repoRoot, inputPath);
  const content = await readFile(absolutePath, "utf8");
  const parsed = JSON.parse(content) as unknown;
  const document = asRecord(parsed);
  if (!document) {
    throw new Error(`OpenAPI 文档必须是 JSON 对象：${toRepoRelativePath(repoRoot, absolutePath)}`);
  }
  return document;
}

async function resolveOpenApiInput(inputPath?: string): Promise<OpenApiInput> {
  if (inputPath) {
    return {
      inputPath,
      sourceLabel: toRepoRelativePath(repoRoot, path.resolve(repoRoot, inputPath)),
      document: await loadOpenApiFromFile(inputPath),
    };
  }

  const document = asRecord(buildOpenApiDocument());
  if (!document) {
    throw new Error("buildOpenApiDocument() 返回值不是合法对象。");
  }

  return {
    sourceLabel: "internal:buildOpenApiDocument()",
    document,
  };
}

function collectOperationKey(operation: Pick<OpenApiOperationSpec, "method" | "path" | "operationId">): string {
  return `${operation.method} ${operation.path} ${operation.operationId}`;
}

function validateGeneratedMetadata(
  metadataRaw: unknown,
  expectedOperations: OpenApiOperationSpec[],
  expectedSourceLabel: string,
  expectedDigest: string
): string[] {
  const issues: string[] = [];
  const metadata = asRecord(metadataRaw);
  if (!metadata) {
    return ["metadata.json 必须是对象。"];
  }

  if (metadata.schemaVersion !== 2) {
    issues.push("metadata.schemaVersion 必须等于 2。");
  }

  const openapi = asRecord(metadata.openapi);
  if (!openapi) {
    issues.push("metadata.openapi 必须是对象。");
  } else {
    if (openapi.source !== expectedSourceLabel) {
      issues.push(`metadata.openapi.source 不匹配：expect=${expectedSourceLabel}, actual=${String(openapi.source)}`);
    }
    if (openapi.digest !== expectedDigest) {
      issues.push(`metadata.openapi.digest 不匹配：expect=${expectedDigest}, actual=${String(openapi.digest)}`);
    }
  }

  const expectedOperationKeys = new Set<string>();
  for (const operation of expectedOperations) {
    expectedOperationKeys.add(collectOperationKey(operation));
  }

  const actualOperationKeys = new Set<string>();
  if (!Array.isArray(metadata.operations)) {
    issues.push("metadata.operations 必须是数组。");
  } else {
    if (metadata.operations.length !== expectedOperations.length) {
      issues.push(
        `metadata.operations 数量不匹配：expect=${expectedOperations.length}, actual=${metadata.operations.length}`
      );
    }

    metadata.operations.forEach((item, index) => {
      const operation = asRecord(item);
      if (!operation) {
        issues.push(`metadata.operations[${index}] 非法，必须是对象。`);
        return;
      }

      const method = typeof operation.method === "string" ? operation.method : "";
      const pathValue = typeof operation.path === "string" ? operation.path : "";
      const operationId = typeof operation.operationId === "string" ? operation.operationId : "";
      if (!method || !pathValue || !operationId) {
        issues.push(`metadata.operations[${index}] 缺少 method/path/operationId。`);
        return;
      }

      const key = `${method} ${pathValue} ${operationId}`;
      if (actualOperationKeys.has(key)) {
        issues.push(`metadata.operations 存在重复映射：${key}`);
        return;
      }
      actualOperationKeys.add(key);
    });

    for (const expectedKey of expectedOperationKeys) {
      if (!actualOperationKeys.has(expectedKey)) {
        issues.push(`metadata.operations 缺少映射：${expectedKey}`);
      }
    }
    for (const actualKey of actualOperationKeys) {
      if (!expectedOperationKeys.has(actualKey)) {
        issues.push(`metadata.operations 存在多余映射：${actualKey}`);
      }
    }
  }

  if (!Array.isArray(metadata.languages)) {
    issues.push("metadata.languages 必须是数组。");
  } else {
    for (const language of metadata.languages) {
      const item = asRecord(language);
      if (!item) {
        issues.push("metadata.languages 存在非法对象。");
        continue;
      }
      if (typeof item.id !== "string" || item.id.trim().length === 0) {
        issues.push("metadata.languages.id 缺失。");
      }
      if (item.operationCount !== expectedOperations.length) {
        issues.push(`metadata.languages.${String(item.id)}.operationCount 不匹配。`);
      }
      if (!Array.isArray(item.generatedFiles) || item.generatedFiles.length === 0) {
        issues.push(`metadata.languages.${String(item.id)}.generatedFiles 为空。`);
      }
    }
  }

  return issues;
}

export async function runSdkVerifyCli(argv: string[]): Promise<number> {
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

  try {
    const openapiInput = await resolveOpenApiInput(options.inputPath);
    const operations = extractOpenApiOperations(openapiInput.document);
    if (operations.length === 0) {
      console.error("OpenAPI 中未解析到 operation，无法进行 SDK 验证。\n");
      return 1;
    }

    const generateArgs = ["--output", options.outputDir];
    if (openapiInput.inputPath) {
      generateArgs.unshift(openapiInput.inputPath);
      generateArgs.unshift("--input");
    }

    const checkArgs = [...generateArgs, "--check"];

    const generateCode = await runSdkGenerateCli(generateArgs);
    if (generateCode !== 0) {
      return generateCode;
    }
    const checkCode = await runSdkGenerateCli(checkArgs);
    if (checkCode !== 0) {
      return checkCode;
    }

    const metadataPath = path.resolve(repoRoot, options.outputDir, "metadata.json");
    const metadataRaw = JSON.parse(await readFile(metadataPath, "utf8")) as unknown;
    const metadataIssues = validateGeneratedMetadata(
      metadataRaw,
      operations,
      openapiInput.sourceLabel,
      computeDigest(openapiInput.document)
    );
    if (metadataIssues.length > 0) {
      console.error("SDK 元数据校验失败：");
      for (const issue of metadataIssues) {
        console.error(`- ${issue}`);
      }
      return 1;
    }

    console.log(`SDK 验证通过：${toRepoRelativePath(repoRoot, path.resolve(repoRoot, options.outputDir))}`);
    return 0;
  } catch (error) {
    console.error("SDK 验证失败。", error);
    return 1;
  }
}

if (import.meta.main) {
  const exitCode = await runSdkVerifyCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
