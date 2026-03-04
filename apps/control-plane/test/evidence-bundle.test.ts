import { describe, expect, test } from "bun:test";
import type { AuditItem } from "../src/contracts";
import {
  buildEvidenceBundle,
  verifyEvidenceBundle,
} from "../src/security/evidence-bundle";

function createAudit(seed: string, createdAt: string): AuditItem {
  return {
    id: `audit-${seed}`,
    eventId: `event-${seed}`,
    action: "test.audit",
    level: "info",
    detail: `audit-${seed}`,
    metadata: {
      seed,
    },
    createdAt,
  };
}

describe("Evidence Bundle", () => {
  test("build + verify 成功", () => {
    const signingKey = "bundle-unit-test-key";
    const bundle = buildEvidenceBundle({
      tenantId: "tenant-evidence-test",
      generatedBy: {
        userId: "user-evidence-test",
        email: "user-evidence-test@example.com",
      },
      filters: {
        action: "test.audit",
        limit: 50,
      },
      audits: [
        createAudit("a", "2026-03-01T00:00:00.000Z"),
        createAudit("b", "2026-03-02T00:00:00.000Z"),
      ],
      signingKey,
      generatedAt: "2026-03-04T00:00:00.000Z",
    });

    const result = verifyEvidenceBundle(bundle, signingKey);
    expect(result.success).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  test("记录被篡改后校验失败", () => {
    const signingKey = "bundle-unit-test-key";
    const bundle = buildEvidenceBundle({
      tenantId: "tenant-evidence-test",
      generatedBy: {
        userId: "user-evidence-test",
      },
      filters: {},
      audits: [createAudit("a", "2026-03-01T00:00:00.000Z")],
      signingKey,
      generatedAt: "2026-03-04T00:00:00.000Z",
    });

    const tampered = structuredClone(bundle);
    tampered.records[0]!.audit.detail = "tampered";
    const result = verifyEvidenceBundle(tampered, signingKey);
    expect(result.success).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });

  test("audit 字段被改成非法值后校验失败", () => {
    const signingKey = "bundle-unit-test-key";
    const bundle = buildEvidenceBundle({
      tenantId: "tenant-evidence-test",
      generatedBy: {
        userId: "user-evidence-test",
      },
      filters: {},
      audits: [createAudit("a", "2026-03-01T00:00:00.000Z")],
      signingKey,
      generatedAt: "2026-03-04T00:00:00.000Z",
    });

    const tamperedLevel = structuredClone(bundle) as unknown as {
      records: Array<{
        audit: Record<string, unknown>;
      }>;
    };
    tamperedLevel.records[0]!.audit.level = "invalid-level";
    const levelResult = verifyEvidenceBundle(tamperedLevel, signingKey);
    expect(levelResult.success).toBe(false);

    const tamperedMetadata = structuredClone(bundle) as unknown as {
      records: Array<{
        audit: Record<string, unknown>;
      }>;
    };
    tamperedMetadata.records[0]!.audit.metadata = "tampered";
    const metadataResult = verifyEvidenceBundle(tamperedMetadata, signingKey);
    expect(metadataResult.success).toBe(false);
  });
});
