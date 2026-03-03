import { describe, expect, test } from "bun:test";
import { getControlPlaneRepository } from "../src/data/repository";

const repository = getControlPlaneRepository();

function createNonce(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("Control Plane Repository - budget binding validation", () => {
  test("scope=org: organizationId 需要存在且属于当前 tenant", async () => {
    const nonce = createNonce("repo-budget-org");
    const tenantA = await repository.createTenant({
      id: `tenant-${nonce}-a`,
      name: `租户A-${nonce}`,
    });
    const tenantB = await repository.createTenant({
      id: `tenant-${nonce}-b`,
      name: `租户B-${nonce}`,
    });

    const orgA = await repository.createOrganization(tenantA.id, {
      name: `组织A-${nonce}`,
    });
    const orgB = await repository.createOrganization(tenantB.id, {
      name: `组织B-${nonce}`,
    });

    const valid = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "org",
      organizationId: orgA.id,
    });
    expect(valid).toBeNull();

    const crossTenant = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "org",
      organizationId: orgB.id,
    });
    expect(crossTenant?.field).toBe("organizationId");

    const missing = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "org",
      organizationId: `missing-org-${nonce}`,
    });
    expect(missing?.field).toBe("organizationId");
  });

  test("scope=user: userId 需要存在且属于当前 tenant", async () => {
    const nonce = createNonce("repo-budget-user");
    const tenantA = await repository.createTenant({
      id: `tenant-${nonce}-a`,
      name: `租户A-${nonce}`,
    });
    const tenantB = await repository.createTenant({
      id: `tenant-${nonce}-b`,
      name: `租户B-${nonce}`,
    });
    const member = await repository.createLocalUser({
      email: `member-${nonce}@example.com`,
      passwordHash: "hashed-password",
      displayName: `成员-${nonce}`,
    });
    const outsider = await repository.createLocalUser({
      email: `outsider-${nonce}@example.com`,
      passwordHash: "hashed-password",
      displayName: `外部成员-${nonce}`,
    });

    await repository.addTenantMember({
      tenantId: tenantA.id,
      userId: member.id,
      tenantRole: "member",
    });
    await repository.addTenantMember({
      tenantId: tenantB.id,
      userId: outsider.id,
      tenantRole: "member",
    });

    const valid = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "user",
      userId: member.id,
    });
    expect(valid).toBeNull();

    const crossTenant = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "user",
      userId: outsider.id,
    });
    expect(crossTenant?.field).toBe("userId");

    const missing = await repository.validateBudgetScopeBinding(tenantA.id, {
      scope: "user",
      userId: `missing-user-${nonce}`,
    });
    expect(missing?.field).toBe("userId");
  });
});
