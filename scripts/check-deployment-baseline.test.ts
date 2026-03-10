import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  parseEnvFileContent,
  runDeploymentBaselinePrecheckCli,
  validateDeploymentBaseline,
} from "./check-deployment-baseline";

const tempDirectories: string[] = [];

afterEach(async () => {
  while (tempDirectories.length > 0) {
    const current = tempDirectories.pop();
    if (!current) {
      continue;
    }
    await rm(current, { recursive: true, force: true });
  }
});

describe("parseEnvFileContent", () => {
  test("支持 export 与引号值", () => {
    const parsed = parseEnvFileContent(`
      export DATABASE_URL="postgres://demo:secret@db.example.com:5432/app"
      NATS_URL=nats://nats.internal:4222
      # 注释
      GOV_WEEKLY_REPORT_TIME_UTC='09:30'
    `);

    expect(parsed).toEqual({
      DATABASE_URL: "postgres://demo:secret@db.example.com:5432/app",
      NATS_URL: "nats://nats.internal:4222",
      GOV_WEEKLY_REPORT_TIME_UTC: "09:30",
    });
  });
});

describe("validateDeploymentBaseline", () => {
  test("release-gate 校验通过", () => {
    const result = validateDeploymentBaseline("release-gate", {
      GOV_E2E_DATABASE_URL: "postgres://release:secret@pg-release.example.com:5432/agentledger",
    });

    expect(result.ok).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  test("governance profile 拦截 loopback，allowLocal 可放行", () => {
    const env = {
      DATABASE_URL: "postgres://dev:secret@127.0.0.1:5432/agentledger",
      NATS_URL: "nats://127.0.0.1:4222",
      GOV_EVAL_INTERVAL: "30s",
      GOV_WEEKLY_REPORT_WEEKDAY: "monday",
      GOV_WEEKLY_REPORT_TIME_UTC: "09:00",
    };

    const failed = validateDeploymentBaseline("governance", env);
    expect(failed.ok).toBe(false);
    expect(failed.errors.map((item) => item.key)).toEqual(["NATS_URL", "DATABASE_URL"]);

    const passed = validateDeploymentBaseline("governance", env, { allowLocal: true });
    expect(passed.ok).toBe(true);
  });

  test("integration profile 会校验 channel 与重试时间窗", () => {
    const result = validateDeploymentBaseline("integration", {
      NATS_URL: "nats://nats.example.com:4222",
      CONTROL_PLANE_BASE_URL: "https://control-plane.example.com",
      INTEGRATION_CALLBACK_SECRET: "integration-secret",
      INTEGRATION_CHANNELS: "webhook,email",
      INTEGRATION_WEBHOOK_URL: "https://hooks.example.com/alerts",
      INTEGRATION_EMAIL_SMTP_HOST: "smtp.example.com",
      INTEGRATION_EMAIL_SMTP_USER: "mailer",
      INTEGRATION_EMAIL_SMTP_PASS: "secret",
      INTEGRATION_EMAIL_FROM: "alerts@example.com",
      INTEGRATION_RETRY_BASE_DELAY: "10s",
      INTEGRATION_RETRY_MAX_DELAY: "5s",
    });

    expect(result.ok).toBe(false);
    expect(result.errors).toEqual([
      {
        level: "error",
        key: "INTEGRATION_RETRY_MAX_DELAY",
        message: "必须大于等于 INTEGRATION_RETRY_BASE_DELAY。",
      },
    ]);
  });

  test("integration profile 使用默认 webhook 通道时要求 webhook url", () => {
    const result = validateDeploymentBaseline("integration", {
      NATS_URL: "nats://nats.example.com:4222",
      CONTROL_PLANE_BASE_URL: "https://control-plane.example.com",
      INTEGRATION_CALLBACK_SECRET: "integration-secret",
    });

    expect(result.ok).toBe(false);
    expect(result.errors).toEqual([
      {
        level: "error",
        key: "INTEGRATION_WEBHOOK_URL",
        message: "启用 webhook 通道时必须提供。",
      },
    ]);
  });

  test("integration profile 成功路径可给出 warning", () => {
    const result = validateDeploymentBaseline("integration", {
      NATS_URL: "nats://nats.example.com:4222",
      CONTROL_PLANE_BASE_URL: "https://control-plane.example.com",
      INTEGRATION_CALLBACK_SECRET: "integration-secret",
      INTEGRATION_CHANNELS: "ticket",
      INTEGRATION_TICKET_WEBHOOK_URL: "https://ticket.example.com/hooks/alerts",
      INTEGRATION_RETRY_MAX: "0",
    });

    expect(result.ok).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.warnings).toEqual([
      {
        level: "warning",
        key: "INTEGRATION_RETRY_MAX",
        message: "当前配置为 0，callback / downstream 失败后将直接进入 DLQ 或失败返回。",
      },
    ]);
  });
});

describe("runDeploymentBaselinePrecheckCli", () => {
  test("支持从 env-file 读取治理部署配置", async () => {
    const dir = await mkdtemp(join(tmpdir(), "agentledger-deploy-precheck-"));
    tempDirectories.push(dir);
    const envPath = join(dir, ".env.governance");
    await writeFile(
      envPath,
      [
        "DATABASE_URL=postgres://gov:secret@pg.example.com:5432/governance",
        "NATS_URL=nats://nats.example.com:4222",
        "GOV_EVAL_INTERVAL=45s",
        "GOV_WEEKLY_REPORT_WEEKDAY=wed",
        "GOV_WEEKLY_REPORT_TIME_UTC=09:15",
      ].join("\n"),
      "utf8"
    );

    const exitCode = await runDeploymentBaselinePrecheckCli([
      "--profile",
      "governance",
      "--env-file",
      envPath,
    ]);

    expect(exitCode).toBe(0);
  });

  test("env-file 缺失时返回失败", async () => {
    const dir = await mkdtemp(join(tmpdir(), "agentledger-deploy-precheck-"));
    tempDirectories.push(dir);

    const exitCode = await runDeploymentBaselinePrecheckCli([
      "--profile",
      "release-gate",
      "--env-file",
      join(dir, "missing.env"),
    ]);

    expect(exitCode).toBe(1);
  });
});
