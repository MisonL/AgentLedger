#!/usr/bin/env bun

import path from "node:path";
import { fileURLToPath } from "node:url";

interface CliOptions {
  threshold: number;
  help: boolean;
}

interface P0GoldenReport {
  totalSamples: number;
  correctSamples: number;
  accuracy: number;
  totalCases: number;
  passedCases: number;
  failedCases?: string[] | null;
  mismatches?: Array<{
    caseId: string;
    client: string;
    field: string;
    expected: string;
    actual: string;
  }> | null;
}

const MIN_THRESHOLD = 99;

function parseCliOptions(argv: string[]): { success: true; options: CliOptions } | { success: false; error: string } {
  const options: CliOptions = {
    threshold: MIN_THRESHOLD,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];

    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }

    if (token === "--threshold") {
      const value = argv[index + 1];
      if (!value || value.startsWith("--")) {
        return { success: false, error: "--threshold 需要提供数值。" };
      }
      const threshold = Number(value);
      if (!Number.isFinite(threshold)) {
        return { success: false, error: `--threshold 必须是数字，收到: ${value}` };
      }
      options.threshold = threshold;
      index += 1;
      continue;
    }

    return { success: false, error: `未知参数：${token}` };
  }

  if (options.threshold < MIN_THRESHOLD) {
    return {
      success: false,
      error: `阈值必须 >= ${MIN_THRESHOLD}，收到: ${options.threshold}`,
    };
  }
  if (options.threshold > 100) {
    return {
      success: false,
      error: `阈值必须 <= 100，收到: ${options.threshold}`,
    };
  }

  return { success: true, options };
}

function printUsage(): void {
  console.log("用法: bun run ./scripts/puller-p0-accuracy.ts [--threshold <99-100>]");
  console.log("说明: 运行 services/puller 的 P0 goldens 准确率测试，并输出总样本、正确数、正确率。");
}

function toEnvRecord(input: NodeJS.ProcessEnv): Record<string, string> {
  const output: Record<string, string> = {};
  for (const [key, value] of Object.entries(input)) {
    if (typeof value === "string") {
      output[key] = value;
    }
  }
  return output;
}

function decodeProcessOutput(value: Uint8Array | null): string {
  if (!value || value.length === 0) {
    return "";
  }
  return Buffer.from(value).toString("utf8");
}

function parseP0GoldenReport(output: string): P0GoldenReport | null {
  const pattern = /P0_GOLDEN_REPORT=(\{.*\})/g;
  let parsed: P0GoldenReport | null = null;
  for (const match of output.matchAll(pattern)) {
    const raw = match[1];
    try {
      parsed = JSON.parse(raw) as P0GoldenReport;
    } catch {
      continue;
    }
  }
  return parsed;
}

function printSummary(report: P0GoldenReport, threshold: number): void {
  const failedCases = report.failedCases ?? [];
  console.log(`[puller-p0-accuracy] 阈值: ${threshold.toFixed(2)}%`);
  console.log(`[puller-p0-accuracy] 总样本: ${report.totalSamples}`);
  console.log(`[puller-p0-accuracy] 正确数: ${report.correctSamples}`);
  console.log(`[puller-p0-accuracy] 正确率: ${report.accuracy.toFixed(4)}%`);
  console.log(`[puller-p0-accuracy] 通过用例: ${report.passedCases}/${report.totalCases}`);

  if (failedCases.length > 0) {
    console.log(`[puller-p0-accuracy] 失败用例: ${failedCases.join(", ")}`);
  }
}

export async function runPullerP0AccuracyCli(argv: string[]): Promise<number> {
  const parsed = parseCliOptions(argv);
  if (!parsed.success) {
    console.error(`参数错误: ${parsed.error}`);
    printUsage();
    return 1;
  }

  if (parsed.options.help) {
    printUsage();
    return 0;
  }

  const { threshold } = parsed.options;
  const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

  const child = Bun.spawnSync({
    cmd: ["go", "test", "./services/puller", "-run", "^TestP0GoldenAccuracyGate$", "-count=1", "-v"],
    cwd: repoRoot,
    stdout: "pipe",
    stderr: "pipe",
    env: {
      ...toEnvRecord(process.env),
      PULLER_P0_GOLDEN_ACCURACY_THRESHOLD: threshold.toString(),
    },
  });

  const stdout = decodeProcessOutput(child.stdout);
  const stderr = decodeProcessOutput(child.stderr);
  const merged = `${stdout}\n${stderr}`;

  const report = parseP0GoldenReport(merged);
  if (!report) {
    if (merged.trim().length > 0) {
      console.error(merged.trim());
    }
    console.error("[puller-p0-accuracy] 未解析到 P0_GOLDEN_REPORT，请检查测试输出。");
    return child.exitCode === 0 ? 1 : child.exitCode;
  }

  printSummary(report, threshold);

  if (child.exitCode !== 0) {
    if (merged.trim().length > 0) {
      console.error("[puller-p0-accuracy] Go 测试输出:");
      console.error(merged.trim());
    }
    return child.exitCode;
  }

  return 0;
}

if (import.meta.main) {
  const exitCode = await runPullerP0AccuracyCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
