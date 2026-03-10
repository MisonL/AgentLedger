import { afterEach, describe, expect, test } from "bun:test";
import { generateKeyPairSync } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

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

async function runWriteAgentDistributionManifestCli(args: string[]) {
  const proc = Bun.spawn({
    cmd: ["bun", "./scripts/write-agent-distribution-manifest.ts", ...args],
    cwd: "/Volumes/Work/code/AgentLedger",
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdoutBuffer, stderrBuffer, exitCode] = await Promise.all([
    new Response(proc.stdout).arrayBuffer(),
    new Response(proc.stderr).arrayBuffer(),
    proc.exited,
  ]);
  return {
    exitCode,
    stdout: Buffer.from(stdoutBuffer).toString("utf8"),
    stderr: Buffer.from(stderrBuffer).toString("utf8"),
  };
}

describe("write-agent-distribution-manifest cli", () => {
  test("可为 archive 生成带签名的 release manifest", async () => {
    const dir = await mkdtemp(join(tmpdir(), "agentledger-distribution-manifest-"));
    tempDirectories.push(dir);

    const archivePath = join(dir, "agent-darwin-amd64.tar.gz");
    const outputPath = join(dir, "release-manifest.json");
    const privateKeyPath = join(dir, "signing-private.pem");

    await writeFile(archivePath, "agent-distribution-archive", "utf8");

    const { privateKey } = generateKeyPairSync("ed25519");
    await writeFile(
      privateKeyPath,
      privateKey.export({ format: "pem", type: "pkcs8" }),
      "utf8",
    );

    const result = await runWriteAgentDistributionManifestCli([
      "--archive",
      archivePath,
      "--output",
      outputPath,
      "--os",
      "darwin",
      "--arch",
      "amd64",
      "--version",
      "1.2.3",
      "--channel",
      "stable",
      "--binary",
      "package/agent",
      "--installer",
      "package/silent-install.sh",
      "--envExample",
      "package/.env.example",
      "--publicKeyExample",
      "package/AGENT_RELEASE_SIGNING_PUBLIC_KEY.pem.example",
      "--installHint",
      "tar -xzf agent-darwin-amd64.tar.gz && bash package/silent-install.sh",
      "--privateKeyFile",
      privateKeyPath,
    ]);

    expect(result.exitCode).toBe(0);

    const manifest = JSON.parse(await readFile(outputPath, "utf8")) as {
      formatVersion: number;
      releaseId: string;
      version: string;
      channel: string;
      artifact: {
        os: string;
        arch: string;
        fileName: string;
        checksumSha256: string;
        signature?: string;
        signatureAlgorithm?: string;
        signerFingerprint?: string;
      };
      packageLayout: {
        binary: string;
        installer: string;
      };
      installHint: string;
    };

    expect(manifest.formatVersion).toBe(1);
    expect(manifest.releaseId).toBe("release-stable-1.2.3-darwin-amd64");
    expect(manifest.version).toBe("1.2.3");
    expect(manifest.channel).toBe("stable");
    expect(manifest.artifact).toMatchObject({
      os: "darwin",
      arch: "amd64",
      fileName: "agent-darwin-amd64.tar.gz",
      signatureAlgorithm: "ed25519",
    });
    expect(manifest.artifact.checksumSha256).toHaveLength(64);
    expect(manifest.artifact.signature).toBeTruthy();
    expect(manifest.artifact.signerFingerprint).toHaveLength(64);
    expect(manifest.packageLayout).toMatchObject({
      binary: "package/agent",
      installer: "package/silent-install.sh",
    });
    expect(manifest.installHint).toContain("silent-install.sh");
  });
});
