#!/usr/bin/env bun

import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { buildOpenApiDocument } from "../apps/control-plane/src/routes/open-platform";
import { runSdkGenerateCli } from "./sdk-generate";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

interface CliOptions {
  inputPath?: string;
  outputDir: string;
  keepTemp: boolean;
  help: boolean;
}

type ParseResult =
  | { success: true; options: CliOptions }
  | { success: false; error: string };

function printUsage(): void {
  console.log("用法: bun run ./scripts/sdk-verify.ts [--input <openapi.json>] [--output <dir>] [--keep-temp]");
  console.log("说明: 默认校验并生成到 clients/sdk，自动执行 generate + check 闭环。");
}

function parseCliOptions(argv: string[]): ParseResult {
  const options: CliOptions = {
    outputDir: "clients/sdk",
    keepTemp: false,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--keep-temp") {
      options.keepTemp = true;
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

function asObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function hasNestedPath(root: Record<string, unknown>, pathTokens: string[]): boolean {
  let cursor: unknown = root;
  for (const token of pathTokens) {
    const objectCursor = asObject(cursor);
    if (!objectCursor || !(token in objectCursor)) {
      return false;
    }
    cursor = objectCursor[token];
  }
  return true;
}

function toRepoRelativePath(targetPath: string): string {
  const relative = path.relative(repoRoot, targetPath);
  if (!relative || relative.startsWith("..")) {
    return targetPath;
  }
  return relative.split(path.sep).join("/");
}

function validateOpenApiDocument(document: Record<string, unknown>): string[] {
  const issues: string[] = [];
  const requiredPaths = [
    ["components", "securitySchemes", "bearerAuth"],
    ["components", "responses", "BadRequestError"],
    ["components", "responses", "UnauthorizedError"],
    ["components", "parameters", "PageLimit"],
    ["components", "parameters", "PageCursor"],
    ["components", "schemas", "ApiKey"],
    ["components", "schemas", "WebhookEndpoint"],
    ["paths", "/api/v1/api-keys"],
    ["paths", "/api/v1/webhooks"],
  ];

  for (const tokenPath of requiredPaths) {
    if (!hasNestedPath(document, tokenPath)) {
      issues.push(`缺少字段：${tokenPath.join(".")}`);
    }
  }

  if ("tenantId" in document) {
    issues.push("OpenAPI 顶层不应包含 tenantId。");
  }

  return issues;
}

async function loadOpenApiFromFile(inputPath: string): Promise<Record<string, unknown>> {
  const absolutePath = path.resolve(repoRoot, inputPath);
  const content = await readFile(absolutePath, "utf8");
  const parsed = JSON.parse(content) as unknown;
  const document = asObject(parsed);
  if (!document) {
    throw new Error(`OpenAPI 文档必须是 JSON 对象：${toRepoRelativePath(absolutePath)}`);
  }
  return document;
}

async function resolveOpenApiInput(
  inputPath: string | undefined,
  tempResources: string[]
): Promise<{ inputPath: string; document: Record<string, unknown> }> {
  if (inputPath) {
    const document = await loadOpenApiFromFile(inputPath);
    return {
      inputPath,
      document,
    };
  }

  const document = asObject(buildOpenApiDocument());
  if (!document) {
    throw new Error("buildOpenApiDocument() 返回值不是合法对象。");
  }
  const tempDirectory = await mkdtemp(path.join(tmpdir(), "agentledger-sdk-verify-"));
  tempResources.push(tempDirectory);
  const snapshotPath = path.join(tempDirectory, "openapi.json");
  await writeFile(`${snapshotPath}`, `${JSON.stringify(document, null, 2)}\n`, "utf8");
  return {
    inputPath: snapshotPath,
    document,
  };
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

  const tempResources: string[] = [];
  try {
    const openapiInput = await resolveOpenApiInput(options.inputPath, tempResources);
    const schemaIssues = validateOpenApiDocument(openapiInput.document);
    if (schemaIssues.length > 0) {
      console.error("OpenAPI 文档结构校验失败：");
      for (const issue of schemaIssues) {
        console.error(`- ${issue}`);
      }
      return 1;
    }

    const generateArgs = [
      "--input",
      openapiInput.inputPath,
      "--output",
      options.outputDir,
    ];
    const checkArgs = [...generateArgs, "--check"];
    const generateCode = await runSdkGenerateCli(generateArgs);
    if (generateCode !== 0) {
      return generateCode;
    }
    const checkCode = await runSdkGenerateCli(checkArgs);
    if (checkCode !== 0) {
      return checkCode;
    }

    console.log(
      `SDK 验证通过：${toRepoRelativePath(path.resolve(repoRoot, options.outputDir))}`
    );
    return 0;
  } catch (error) {
    console.error("SDK 验证失败。", error);
    return 1;
  } finally {
    if (!options.keepTemp) {
      for (const resource of tempResources) {
        await rm(resource, { recursive: true, force: true });
      }
    }
  }
}

if (import.meta.main) {
  const exitCode = await runSdkVerifyCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
