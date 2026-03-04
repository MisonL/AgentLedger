import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { buildEvidenceBundle } from "../apps/control-plane/src/security/evidence-bundle";
import { runEvidenceBundleVerifyCli } from "./verify-evidence-bundle";

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

describe("verify-evidence-bundle cli", () => {
  test("合法文件返回 0", async () => {
    const signingKey = "verify-script-test-key";
    const bundle = buildEvidenceBundle({
      tenantId: "tenant-script-test",
      generatedBy: {
        userId: "script-user",
      },
      filters: {
        action: "test.script.evidence",
      },
      audits: [
        {
          id: "audit-script-1",
          eventId: "event-script-1",
          action: "test.script.evidence",
          level: "info",
          detail: "script detail",
          metadata: {
            source: "unit-test",
          },
          createdAt: "2026-03-04T00:00:00.000Z",
        },
      ],
      signingKey,
      generatedAt: "2026-03-04T00:00:00.000Z",
    });

    const dir = await mkdtemp(join(tmpdir(), "agentledger-evidence-"));
    tempDirectories.push(dir);
    const filePath = join(dir, "bundle.json");
    await writeFile(filePath, JSON.stringify(bundle), "utf8");

    const exitCode = await runEvidenceBundleVerifyCli([
      "--file",
      filePath,
      "--signing-key",
      signingKey,
    ]);
    expect(exitCode).toBe(0);
  });

  test("篡改文件返回 1", async () => {
    const signingKey = "verify-script-test-key";
    const bundle = buildEvidenceBundle({
      tenantId: "tenant-script-test",
      generatedBy: {
        userId: "script-user",
      },
      filters: {},
      audits: [
        {
          id: "audit-script-1",
          eventId: "event-script-1",
          action: "test.script.evidence",
          level: "info",
          detail: "script detail",
          metadata: {},
          createdAt: "2026-03-04T00:00:00.000Z",
        },
      ],
      signingKey,
      generatedAt: "2026-03-04T00:00:00.000Z",
    });
    const tampered = structuredClone(bundle);
    tampered.records[0]!.recordHash = "abcdef";

    const dir = await mkdtemp(join(tmpdir(), "agentledger-evidence-"));
    tempDirectories.push(dir);
    const filePath = join(dir, "bundle.json");
    await writeFile(filePath, JSON.stringify(tampered), "utf8");

    const exitCode = await runEvidenceBundleVerifyCli([
      "--file",
      filePath,
      "--signing-key",
      signingKey,
    ]);
    expect(exitCode).toBe(1);
  });

  test("非法 audit.level 篡改返回 1", async () => {
    const signingKey = "verify-script-test-key";
    const bundle = buildEvidenceBundle({
      tenantId: "tenant-script-test",
      generatedBy: {
        userId: "script-user",
      },
      filters: {},
      audits: [
        {
          id: "audit-script-1",
          eventId: "event-script-1",
          action: "test.script.evidence",
          level: "info",
          detail: "script detail",
          metadata: {},
          createdAt: "2026-03-04T00:00:00.000Z",
        },
      ],
      signingKey,
      generatedAt: "2026-03-04T00:00:00.000Z",
    });
    const tampered = structuredClone(bundle) as unknown as {
      records: Array<{
        audit: Record<string, unknown>;
      }>;
    };
    tampered.records[0]!.audit.level = "invalid-level";

    const dir = await mkdtemp(join(tmpdir(), "agentledger-evidence-"));
    tempDirectories.push(dir);
    const filePath = join(dir, "bundle.json");
    await writeFile(filePath, JSON.stringify(tampered), "utf8");

    const exitCode = await runEvidenceBundleVerifyCli([
      "--file",
      filePath,
      "--signing-key",
      signingKey,
    ]);
    expect(exitCode).toBe(1);
  });
});
