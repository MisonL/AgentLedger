#!/usr/bin/env bun

import { createHash } from "node:crypto";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildOpenApiDocument } from "../apps/control-plane/src/routes/open-platform";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const METADATA_FILE = "metadata.json";

const SDK_LANGUAGES = [
  { id: "typescript", name: "TypeScript" },
  { id: "python", name: "Python" },
  { id: "go", name: "Go" },
  { id: "java", name: "Java" },
  { id: "csharp", name: "C#" },
  { id: "php", name: "PHP" },
  { id: "ruby", name: "Ruby" },
  { id: "swift", name: "Swift" },
] as const;

interface CliOptions {
  inputPath?: string;
  outputDir: string;
  check: boolean;
  help: boolean;
}

interface OpenApiLoadResult {
  document: Record<string, unknown>;
  sourceLabel: string;
}

interface SdkMetadata {
  schemaVersion: number;
  generator: string;
  openapi: {
    source: string;
    title: string;
    version: string;
    specVersion: string;
    digest: string;
  };
  languages: Array<{
    id: string;
    name: string;
    directory: string;
    placeholderFiles: string[];
  }>;
}

type CliResult =
  | { success: true; options: CliOptions }
  | { success: false; error: string };

function printUsage(): void {
  console.log("用法: bun run ./scripts/sdk-generate.ts [--input <openapi.json>] [--output <dir>] [--check]");
  console.log("说明: 默认输出到 clients/sdk；不传 --input 时使用本地 buildOpenApiDocument()。");
}

function parseCliOptions(argv: string[]): CliResult {
  const options: CliOptions = {
    outputDir: "clients/sdk",
    check: false,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--check") {
      options.check = true;
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

function toRepoRelativePath(targetPath: string): string {
  const relative = path.relative(repoRoot, targetPath);
  if (!relative || relative.startsWith("..")) {
    return targetPath;
  }
  return relative.split(path.sep).join("/");
}

function stringifyWithTrailingNewline(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

function resolveErrorCode(error: unknown): string | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }
  const code = Reflect.get(error, "code");
  return typeof code === "string" ? code : undefined;
}

function parseOpenApiLike(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

async function loadOpenApiDocument(inputPath?: string): Promise<OpenApiLoadResult> {
  if (!inputPath) {
    const document = parseOpenApiLike(buildOpenApiDocument());
    if (!document) {
      throw new Error("buildOpenApiDocument() 返回值不是合法对象。");
    }
    return {
      document,
      sourceLabel: "internal:buildOpenApiDocument()",
    };
  }

  const absoluteInputPath = path.resolve(repoRoot, inputPath);
  const content = await readFile(absoluteInputPath, "utf8");
  const parsed = JSON.parse(content) as unknown;
  const document = parseOpenApiLike(parsed);
  if (!document) {
    throw new Error(`OpenAPI 文档必须是 JSON 对象：${toRepoRelativePath(absoluteInputPath)}`);
  }

  return {
    document,
    sourceLabel: toRepoRelativePath(absoluteInputPath),
  };
}

function resolveInfoField(document: Record<string, unknown>, key: "title" | "version"): string {
  const info = parseOpenApiLike(document.info);
  const value = info?.[key];
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : "unknown";
}

function resolveSpecVersion(document: Record<string, unknown>): string {
  const version = document.openapi;
  return typeof version === "string" && version.trim().length > 0 ? version.trim() : "unknown";
}

function computeDigest(document: Record<string, unknown>): string {
  return createHash("sha256").update(JSON.stringify(document)).digest("hex");
}

function buildMetadata(document: Record<string, unknown>, sourceLabel: string): SdkMetadata {
  const digest = computeDigest(document);
  const title = resolveInfoField(document, "title");
  const version = resolveInfoField(document, "version");
  const specVersion = resolveSpecVersion(document);

  return {
    schemaVersion: 1,
    generator: "agentledger-sdk-minimal",
    openapi: {
      source: sourceLabel,
      title,
      version,
      specVersion,
      digest,
    },
    languages: SDK_LANGUAGES.map((language) => ({
      id: language.id,
      name: language.name,
      directory: language.id,
      placeholderFiles: ["README.md", ".gitkeep"],
    })),
  };
}

function buildReadmeContent(language: (typeof SDK_LANGUAGES)[number], metadata: SdkMetadata): string {
  return [
    `# ${language.name} SDK`,
    "",
    "该目录由 `bun run sdk:generate` 自动生成（最小占位版）。",
    "",
    `- OpenAPI 标题：${metadata.openapi.title}`,
    `- OpenAPI 版本：${metadata.openapi.version}`,
    `- OpenAPI Spec：${metadata.openapi.specVersion}`,
    `- 文档摘要：${metadata.openapi.digest}`,
    "",
    "后续可在此目录接入真实代码生成器输出。",
    "",
  ].join("\n");
}

async function readTextIfExists(filePath: string): Promise<string | null> {
  try {
    return await readFile(filePath, "utf8");
  } catch (error) {
    if (resolveErrorCode(error) === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function ensureDirectoryExists(
  directoryPath: string,
  checkMode: boolean,
  issues: string[]
): Promise<void> {
  try {
    const current = await stat(directoryPath);
    if (!current.isDirectory()) {
      issues.push(`路径不是目录：${toRepoRelativePath(directoryPath)}`);
    }
    return;
  } catch (error) {
    if (resolveErrorCode(error) !== "ENOENT") {
      throw error;
    }
  }

  if (checkMode) {
    issues.push(`缺少目录：${toRepoRelativePath(directoryPath)}`);
    return;
  }
  await mkdir(directoryPath, { recursive: true });
}

async function ensureFileContent(
  filePath: string,
  expectedContent: string,
  checkMode: boolean,
  issues: string[]
): Promise<void> {
  const current = await readTextIfExists(filePath);
  if (current === expectedContent) {
    return;
  }

  if (checkMode) {
    if (current === null) {
      issues.push(`缺少文件：${toRepoRelativePath(filePath)}`);
    } else {
      issues.push(`文件内容不一致：${toRepoRelativePath(filePath)}`);
    }
    return;
  }

  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, expectedContent, "utf8");
}

export async function runSdkGenerateCli(argv: string[]): Promise<number> {
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

  let openapi: OpenApiLoadResult;
  try {
    openapi = await loadOpenApiDocument(options.inputPath);
  } catch (error) {
    console.error("加载 OpenAPI 文档失败。", error);
    return 1;
  }

  const metadata = buildMetadata(openapi.document, openapi.sourceLabel);
  const metadataContent = stringifyWithTrailingNewline(metadata);
  const outputDir = path.resolve(repoRoot, options.outputDir);
  const issues: string[] = [];

  await ensureDirectoryExists(outputDir, options.check, issues);

  for (const language of SDK_LANGUAGES) {
    const languageDir = path.join(outputDir, language.id);
    await ensureDirectoryExists(languageDir, options.check, issues);
    await ensureFileContent(
      path.join(languageDir, "README.md"),
      buildReadmeContent(language, metadata),
      options.check,
      issues
    );
    await ensureFileContent(path.join(languageDir, ".gitkeep"), "", options.check, issues);
  }

  await ensureFileContent(
    path.join(outputDir, METADATA_FILE),
    metadataContent,
    options.check,
    issues
  );

  if (options.check) {
    if (issues.length > 0) {
      console.error("SDK 目录校验失败：");
      for (const issue of issues) {
        console.error(`- ${issue}`);
      }
      return 1;
    }
    console.log(`SDK 校验通过：${toRepoRelativePath(outputDir)}`);
    return 0;
  }

  console.log(`SDK 占位目录已生成：${toRepoRelativePath(outputDir)}`);
  console.log(`语言数量：${SDK_LANGUAGES.length}`);
  return 0;
}

if (import.meta.main) {
  const exitCode = await runSdkGenerateCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
