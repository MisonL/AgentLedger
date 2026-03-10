import { createHash, createPrivateKey, createPublicKey, sign as signBytes } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

type Args = {
  archive: string;
  output: string;
  os: string;
  arch: string;
  version: string;
  channel: string;
  binary: string;
  installer: string;
  envExample: string;
  publicKeyExample: string;
  installHint: string;
  privateKeyFile?: string;
};

function parseArgs(argv: string[]): Args {
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith("--") || value === undefined) {
      throw new Error(`参数格式非法: ${key ?? "(empty)"}`);
    }
    values.set(key.slice(2), value);
  }

  const required = [
    "archive",
    "output",
    "os",
    "arch",
    "version",
    "channel",
    "binary",
    "installer",
    "envExample",
    "publicKeyExample",
    "installHint",
  ] as const;

  for (const item of required) {
    if (!values.get(item)?.trim()) {
      throw new Error(`缺少 --${item}`);
    }
  }

  return {
    archive: values.get("archive")!.trim(),
    output: values.get("output")!.trim(),
    os: values.get("os")!.trim(),
    arch: values.get("arch")!.trim(),
    version: values.get("version")!.trim(),
    channel: values.get("channel")!.trim(),
    binary: values.get("binary")!.trim(),
    installer: values.get("installer")!.trim(),
    envExample: values.get("envExample")!.trim(),
    publicKeyExample: values.get("publicKeyExample")!.trim(),
    installHint: values.get("installHint")!.trim(),
    privateKeyFile: values.get("privateKeyFile")?.trim() || undefined,
  };
}

function sha256Hex(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}

function sanitizeReleaseToken(value: string): string {
  return value.replace(/[^a-zA-Z0-9._-]+/g, "-");
}

function buildManifest(args: Args) {
  const archiveBody = readFileSync(args.archive);
  const checksumSha256 = sha256Hex(archiveBody);

  let signature = "";
  let signatureAlgorithm = "";
  let signerFingerprint = "";

  if (args.privateKeyFile) {
    const privateKeyPem = readFileSync(args.privateKeyFile, "utf8");
    const privateKey = createPrivateKey(privateKeyPem);
    const publicKey = createPublicKey(privateKey).export({
      type: "spki",
      format: "der",
    }) as Buffer;
    signature = signBytes(null, archiveBody, privateKey).toString("base64");
    signatureAlgorithm = "ed25519";
    signerFingerprint = sha256Hex(publicKey);
  }

  const archiveFileName = path.basename(args.archive);
  const releaseId = [
    "release",
    sanitizeReleaseToken(args.channel),
    sanitizeReleaseToken(args.version),
    sanitizeReleaseToken(args.os),
    sanitizeReleaseToken(args.arch),
  ].join("-");

  return {
    formatVersion: 1,
    releaseId,
    version: args.version,
    channel: args.channel,
    artifact: {
      os: args.os,
      arch: args.arch,
      fileName: archiveFileName,
      checksumSha256,
      ...(signature
        ? {
            signature,
            signatureAlgorithm,
            signerFingerprint,
          }
        : {}),
    },
    packageLayout: {
      binary: args.binary,
      installer: args.installer,
      envExample: args.envExample,
      signingPublicKeyExample: args.publicKeyExample,
    },
    installHint: args.installHint,
  };
}

try {
  const args = parseArgs(Bun.argv.slice(2));
  const manifest = buildManifest(args);
  writeFileSync(args.output, `${JSON.stringify(manifest, null, 2)}\n`, "utf8");
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
}
