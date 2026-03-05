#!/usr/bin/env bun

import { cp, mkdir, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

interface CliOptions {
  inputDir: string;
  outputDir: string;
  clean: boolean;
  help: boolean;
}

type ParseResult =
  | { success: true; options: CliOptions }
  | { success: false; error: string };

function printUsage(): void {
  console.log("用法: bun run ./scripts/sdk-bundle.ts [--input <dir>] [--output <dir>] [--no-clean]");
  console.log("说明: 将 SDK 目录复制到 dist 产物目录，并生成 SHA256SUMS.txt。\n");
}

function parseCliOptions(argv: string[]): ParseResult {
  const options: CliOptions = {
    inputDir: "clients/sdk",
    outputDir: "dist/sdk",
    clean: true,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--no-clean") {
      options.clean = false;
      continue;
    }
    if (token === "--input" || token === "-i") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供输入目录。` };
      }
      options.inputDir = value;
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

async function listFilesRecursive(rootDir: string): Promise<string[]> {
  const result: string[] = [];

  async function walk(currentDir: string): Promise<void> {
    const items = await readdir(currentDir, { withFileTypes: true });
    for (const item of items) {
      const absolutePath = path.join(currentDir, item.name);
      if (item.isDirectory()) {
        await walk(absolutePath);
        continue;
      }
      if (!item.isFile()) {
        continue;
      }
      result.push(absolutePath);
    }
  }

  await walk(rootDir);
  result.sort((left, right) => left.localeCompare(right));
  return result;
}

function sha256Hex(content: Buffer): string {
  return createHash("sha256").update(content).digest("hex");
}

export async function runSdkBundleCli(argv: string[]): Promise<number> {
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

  const inputDir = path.resolve(repoRoot, options.inputDir);
  const outputDir = path.resolve(repoRoot, options.outputDir);

  try {
    const inputStats = await stat(inputDir);
    if (!inputStats.isDirectory()) {
      console.error(`输入路径不是目录：${inputDir}`);
      return 1;
    }

    if (options.clean) {
      await rm(outputDir, { recursive: true, force: true });
    }
    await mkdir(path.dirname(outputDir), { recursive: true });
    await cp(inputDir, outputDir, { recursive: true });

    const files = await listFilesRecursive(outputDir);
    const lines: string[] = [];
    for (const filePath of files) {
      const relativePath = path.relative(outputDir, filePath).split(path.sep).join("/");
      if (relativePath === "SHA256SUMS.txt") {
        continue;
      }
      const content = await readFile(filePath);
      lines.push(`${sha256Hex(content)}  ${relativePath}`);
    }

    const sumPath = path.join(outputDir, "SHA256SUMS.txt");
    await writeFile(sumPath, `${lines.join("\n")}\n`, "utf8");

    console.log(`SDK 产物已输出：${path.relative(repoRoot, outputDir) || outputDir}`);
    console.log(`文件数量：${lines.length}`);
    return 0;
  } catch (error) {
    console.error("SDK 打包失败。", error);
    return 1;
  }
}

if (import.meta.main) {
  const exitCode = await runSdkBundleCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
