import { mkdtempSync, readFileSync, statSync, writeFileSync, existsSync, chmodSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";

type Args = {
  distributionDir: string;
  os?: string;
  arch?: string;
  currentVersion: string;
  signaturePublicKeyFile?: string;
};

type Manifest = {
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
    envExample: string;
    signingPublicKeyExample: string;
  };
  installHint: string;
};

function parseArgs(argv: string[]): Args {
  const args: Args = {
    distributionDir: path.resolve("dist/agent-distribution"),
    currentVersion: "0.0.0",
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    switch (token) {
      case "--distribution-dir":
        args.distributionDir = path.resolve(argv[index + 1] ?? "");
        index += 1;
        break;
      case "--os":
        args.os = argv[index + 1]?.trim();
        index += 1;
        break;
      case "--arch":
        args.arch = argv[index + 1]?.trim();
        index += 1;
        break;
      case "--current-version":
        args.currentVersion = argv[index + 1]?.trim() || "0.0.0";
        index += 1;
        break;
      case "--signature-public-key-file":
        args.signaturePublicKeyFile = path.resolve(argv[index + 1] ?? "");
        index += 1;
        break;
      case "--help":
      case "-h":
        console.log(`用法:
  bun run ./scripts/verify-agent-distribution.ts [--distribution-dir dist/agent-distribution] [--os <os>] [--arch <arch>] [--current-version <ver>] [--signature-public-key-file <path>]
`);
        process.exit(0);
      default:
        throw new Error(`未知参数: ${token}`);
    }
  }

  return args;
}

function detectNativeTarget(): { os: string; arch: string } {
  const os =
    process.platform === "win32"
      ? "windows"
      : process.platform === "darwin"
        ? "darwin"
        : "linux";
  const arch =
    process.arch === "x64"
      ? "amd64"
      : process.arch === "arm64"
        ? "arm64"
        : process.arch;
  return { os, arch };
}

function ensureFile(filePath: string, label: string): void {
  if (!existsSync(filePath) || !statSync(filePath).isFile()) {
    throw new Error(`${label} 不存在: ${filePath}`);
  }
}

async function runCommand(
  command: string,
  args: string[],
): Promise<{ stdout: string; stderr: string }> {
  const subprocess = Bun.spawn({
    cmd: [command, ...args],
    stdout: "pipe",
    stderr: "pipe",
  });
  const [stdoutBuffer, stderrBuffer, exitCode] = await Promise.all([
    new Response(subprocess.stdout).arrayBuffer(),
    new Response(subprocess.stderr).arrayBuffer(),
    subprocess.exited,
  ]);
  const stdout = Buffer.from(stdoutBuffer).toString("utf8").trim();
  const stderr = Buffer.from(stderrBuffer).toString("utf8").trim();
  if (exitCode !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} 执行失败 (exit=${exitCode})\nstdout:\n${stdout}\nstderr:\n${stderr}`,
    );
  }
  return { stdout, stderr };
}

async function main(): Promise<void> {
  const args = parseArgs(Bun.argv.slice(2));
  const native = detectNativeTarget();
  const targetOS = args.os || native.os;
  const targetArch = args.arch || native.arch;
  const targetDir = path.join(args.distributionDir, targetOS, targetArch);
  const manifestPath = path.join(targetDir, "release-manifest.json");

  ensureFile(manifestPath, "分发清单");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as Manifest;

  const binaryPath = path.join(targetDir, manifest.packageLayout.binary);
  const installerPath = path.join(targetDir, manifest.packageLayout.installer);
  const envExamplePath = path.join(targetDir, manifest.packageLayout.envExample);
  const publicKeyExamplePath = path.join(
    targetDir,
    manifest.packageLayout.signingPublicKeyExample,
  );
  const packageSumsPath = path.join(targetDir, "package", "SHA256SUMS.txt");
  const archivePath = path.join(targetDir, manifest.artifact.fileName);

  ensureFile(binaryPath, "原生 agent 二进制");
  ensureFile(installerPath, "静默安装模板");
  ensureFile(envExamplePath, ".env.example");
  ensureFile(publicKeyExamplePath, "公钥占位文件");
  ensureFile(packageSumsPath, "包内 SHA256SUMS.txt");
  ensureFile(archivePath, "分发归档");

  if (targetOS !== "windows") {
    chmodSync(binaryPath, 0o755);
  }

  const tempRoot = mkdtempSync(path.join(tmpdir(), "agentledger-distribution-verify-"));
  const configDir = path.join(tempRoot, "config");
  const tokenFile = path.join(tempRoot, "token.json");
  writeFileSync(
    tokenFile,
    `${JSON.stringify(
      {
        access_token: "distribution-token",
        token_type: "Bearer",
        expires_at: "2099-01-01T00:00:00Z",
      },
      null,
      2,
    )}\n`,
    "utf8",
  );

  const versionArgs = targetOS === "windows" ? ["--short"] : ["--short"];
  const versionResult = await runCommand(binaryPath, ["version", ...versionArgs]);
  if (!versionResult.stdout) {
    throw new Error("agent version --short 输出为空");
  }

  const statusResult = await runCommand(binaryPath, [
    "status",
    `--config-dir=${configDir}`,
    `--token-file=${tokenFile}`,
  ]);
  const statusPayload = JSON.parse(statusResult.stdout) as {
    component?: string;
    token?: { found?: boolean };
    update?: { status?: string };
  };
  if (statusPayload.component !== "agent-cli") {
    throw new Error(`agent status component 异常: ${statusPayload.component ?? "(empty)"}`);
  }

  const server = createServer((req, res) => {
    if (!req.url) {
      res.statusCode = 404;
      res.end("not found");
      return;
    }
    const requestUrl = new URL(req.url, "http://127.0.0.1");
    if (requestUrl.pathname === "/api/v1/system/agent-releases/check") {
      if ((req.headers.authorization ?? "").trim() !== "Bearer distribution-token") {
        res.statusCode = 401;
        res.end("unauthorized");
        return;
      }
      const now = new Date().toISOString();
      const artifact = {
        os: manifest.artifact.os,
        arch: manifest.artifact.arch,
        downloadUrl: `http://127.0.0.1:${address.port}/artifacts/${manifest.artifact.fileName}`,
        checksumSha256: manifest.artifact.checksumSha256,
        ...(manifest.artifact.signature
          ? {
              signature: manifest.artifact.signature,
              signatureAlgorithm: manifest.artifact.signatureAlgorithm,
            }
          : {}),
        fileName: manifest.artifact.fileName,
        installHint: manifest.installHint,
      };
      const payload = {
        checkedAt: now,
        currentVersion: args.currentVersion,
        channel: manifest.channel,
        os: manifest.artifact.os,
        arch: manifest.artifact.arch,
        updateAvailable: true,
        comparison: "upgrade_available",
        latestRelease: {
          releaseId: manifest.releaseId,
          tenantId: "distribution-local",
          version: manifest.version,
          channel: manifest.channel,
          notes: "local distribution acceptance",
          publishedAt: now,
          artifacts: [artifact],
          createdAt: now,
          updatedAt: now,
        },
        selectedArtifact: artifact,
        instructions: "local distribution acceptance",
      };
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(payload));
      return;
    }
    if (requestUrl.pathname === `/artifacts/${manifest.artifact.fileName}`) {
      res.setHeader("Content-Type", "application/octet-stream");
      res.end(readFileSync(archivePath));
      return;
    }
    res.statusCode = 404;
    res.end("not found");
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });

  const address = server.address();
  if (!address || typeof address === "string") {
    server.close();
    throw new Error("无法解析 mock release server 地址");
  }

  try {
    const updateCheckResult = await runCommand(binaryPath, [
      "update",
      "check",
      `--gateway=http://127.0.0.1:${address.port}`,
      `--token-file=${tokenFile}`,
      `--current-version=${args.currentVersion}`,
      `--channel=${manifest.channel}`,
      `--os=${manifest.artifact.os}`,
      `--arch=${manifest.artifact.arch}`,
    ]);
    const updateCheckPayload = JSON.parse(updateCheckResult.stdout) as {
      update_available?: boolean;
      latest_release?: { releaseId?: string; version?: string };
      selected_artifact?: { fileName?: string };
    };
    if (updateCheckPayload.update_available !== true) {
      throw new Error("agent update check 未返回 update_available=true");
    }

    const resolvedPublicKey = args.signaturePublicKeyFile;

    if (resolvedPublicKey) {
      if (!manifest.artifact.signature || manifest.artifact.signatureAlgorithm !== "ed25519") {
        throw new Error("当前分发包未包含可验证签名，无法执行 update download 验收");
      }
      ensureFile(resolvedPublicKey, "升级签名公钥");

      await runCommand(binaryPath, [
        "update",
        "download",
        `--gateway=http://127.0.0.1:${address.port}`,
        `--token-file=${tokenFile}`,
        `--config-dir=${configDir}`,
        `--current-version=${args.currentVersion}`,
        `--channel=${manifest.channel}`,
        `--os=${manifest.artifact.os}`,
        `--arch=${manifest.artifact.arch}`,
        `--signature-public-key-file=${resolvedPublicKey}`,
      ]);

      const updateStatusResult = await runCommand(binaryPath, [
        "update",
        "status",
        `--config-dir=${configDir}`,
      ]);
      const updateStatusPayload = JSON.parse(updateStatusResult.stdout) as {
        status?: string;
        update?: { downloaded_signature_status?: string; downloaded_release_id?: string };
      };
      if (updateStatusPayload.status !== "downloaded") {
        throw new Error(`update status 期望 downloaded，实际为 ${updateStatusPayload.status ?? "(empty)"}`);
      }
      if (updateStatusPayload.update?.downloaded_signature_status !== "verified") {
        throw new Error(
          `downloaded_signature_status 期望 verified，实际为 ${updateStatusPayload.update?.downloaded_signature_status ?? "(empty)"}`,
        );
      }
    }
  } finally {
    server.close();
  }

  console.log("Agent 分发最小验收通过。");
  console.log(`target: ${targetOS}/${targetArch}`);
  console.log(`version: ${versionResult.stdout}`);
  console.log(`package: ${targetDir}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
