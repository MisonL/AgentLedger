import { createServer } from "node:http";
import { generateKeyPairSync, sign } from "node:crypto";

type Args = {
  port: number;
  publicKeyOut: string;
  os: string;
  arch: string;
};

function parseArgs(argv: string[]): Args {
  const values = new Map<string, string>();
  for (const item of argv) {
    const normalized = item.trim();
    if (!normalized.startsWith("--")) continue;
    const [key, ...rest] = normalized.slice(2).split("=");
    values.set(key, rest.join("="));
  }
  const port = Number(values.get("port"));
  const publicKeyOut = values.get("public-key-out") ?? "";
  const os = values.get("os") ?? "";
  const arch = values.get("arch") ?? "";
  if (!Number.isFinite(port) || port <= 0) {
    throw new Error("port 必须为正整数");
  }
  if (!publicKeyOut) {
    throw new Error("public-key-out 不能为空");
  }
  if (!os || !arch) {
    throw new Error("os/arch 不能为空");
  }
  return { port, publicKeyOut, os, arch };
}

const args = parseArgs(process.argv.slice(2));
const artifactBody = Buffer.from(`agent-distribution-update-${args.os}-${args.arch}`);
const { publicKey, privateKey } = generateKeyPairSync("ed25519");
await Bun.write(args.publicKeyOut, publicKey.export({ type: "spki", format: "pem" }));

const signature = sign(null, artifactBody, privateKey).toString("base64");
const checksumSha256 = new Bun.CryptoHasher("sha256").update(artifactBody).digest("hex");
const artifactFileName = args.os === "windows" ? `agent-${args.os}-${args.arch}.exe` : `agent-${args.os}-${args.arch}`;

const server = createServer((req, res) => {
  if (!req.url) {
    res.statusCode = 400;
    res.end("missing url");
    return;
  }
  const url = new URL(req.url, `http://127.0.0.1:${args.port}`);
  if (url.pathname === "/healthz") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ status: "ok" }));
    return;
  }
  if (url.pathname === "/downloads/agent") {
    res.statusCode = 200;
    res.end(artifactBody);
    return;
  }
  if (url.pathname === "/api/v1/system/agent-releases/check") {
    res.setHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify({
        checkedAt: "2026-03-08T20:00:00Z",
        currentVersion: url.searchParams.get("currentVersion") ?? "0.0.0",
        channel: url.searchParams.get("channel") ?? "stable",
        os: args.os,
        arch: args.arch,
        updateAvailable: true,
        comparison: "upgrade_available",
        latestRelease: {
          releaseId: "distribution-release-1",
          tenantId: "distribution",
          version: "9.9.9",
          channel: url.searchParams.get("channel") ?? "stable",
          publishedAt: "2026-03-08T19:59:00Z",
          artifacts: [
            {
              os: args.os,
              arch: args.arch,
              downloadUrl: `http://127.0.0.1:${args.port}/downloads/agent`,
              checksumSha256,
              signature,
              signatureAlgorithm: "ed25519",
              fileName: artifactFileName,
              installHint: "distribution verify only",
            },
          ],
          createdAt: "2026-03-08T19:59:00Z",
          updatedAt: "2026-03-08T19:59:00Z",
        },
        selectedArtifact: {
          os: args.os,
          arch: args.arch,
          downloadUrl: `http://127.0.0.1:${args.port}/downloads/agent`,
          checksumSha256,
          signature,
          signatureAlgorithm: "ed25519",
          fileName: artifactFileName,
          installHint: "distribution verify only",
        },
        instructions: "distribution verify mock",
      }),
    );
    return;
  }
  res.statusCode = 404;
  res.end("not found");
});

server.listen(args.port, "127.0.0.1", () => {
  console.log(`mock-agent-distribution-update listening on ${args.port}`);
});
