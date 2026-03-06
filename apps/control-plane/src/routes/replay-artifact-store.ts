import { createHash } from "node:crypto";
import { mkdir, rename, unlink } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { ReplayArtifact, ReplayArtifactStorageBackend } from "../data/repository";

type ReplayArtifactStorageMode = ReplayArtifactStorageBackend;

type ReplayArtifactStoreConfig = {
  mode: ReplayArtifactStorageMode;
  localRoot: string;
  objectPrefix: string;
  objectBucket?: string;
  objectEndpoint?: string;
  objectRegion?: string;
  objectAccessKeyId?: string;
  objectSecretAccessKey?: string;
  objectSessionToken?: string;
};

export type StoredReplayArtifact = {
  storageBackend: ReplayArtifactStorageBackend;
  storageKey: string;
  byteSize: number;
  checksum: string;
  metadata: Record<string, unknown>;
};

const replayArtifactStoreRepoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
const replayArtifactStoreDefaultLocalRoot = join(
  replayArtifactStoreRepoRoot,
  "data",
  "replay-artifacts"
);

function firstNonEmptyString(...values: unknown[]): string | undefined {
  for (const value of values) {
    if (typeof value !== "string") {
      continue;
    }
    const trimmed = value.trim();
    if (trimmed.length > 0) {
      return trimmed;
    }
  }
  return undefined;
}

function parseMode(value: string | undefined): ReplayArtifactStorageMode {
  switch ((value ?? "").trim().toLowerCase()) {
    case "object":
      return "object";
    case "hybrid":
      return "hybrid";
    default:
      return "local";
  }
}

export function resolveReplayArtifactLocalRoot(input?: {
  cwd?: string;
  envLocalRoot?: string;
}): string {
  const cwd = input?.cwd ?? process.cwd();
  const envLocalRoot = firstNonEmptyString(input?.envLocalRoot, Bun.env.REPLAY_STORAGE_LOCAL_ROOT);
  if (envLocalRoot) {
    return resolve(cwd, envLocalRoot);
  }
  return replayArtifactStoreDefaultLocalRoot;
}

function getConfig(): ReplayArtifactStoreConfig {
  return {
    mode: parseMode(Bun.env.REPLAY_STORAGE_MODE),
    localRoot: resolveReplayArtifactLocalRoot(),
    objectPrefix: (firstNonEmptyString(Bun.env.REPLAY_STORAGE_OBJECT_PREFIX) ?? "replay-artifacts").replace(
      /^\/+|\/+$/g,
      ""
    ),
    objectBucket: firstNonEmptyString(Bun.env.REPLAY_STORAGE_OBJECT_BUCKET),
    objectEndpoint: firstNonEmptyString(Bun.env.REPLAY_STORAGE_OBJECT_ENDPOINT),
    objectRegion: firstNonEmptyString(Bun.env.REPLAY_STORAGE_OBJECT_REGION, Bun.env.AWS_REGION),
    objectAccessKeyId: firstNonEmptyString(
      Bun.env.REPLAY_STORAGE_OBJECT_ACCESS_KEY_ID,
      Bun.env.AWS_ACCESS_KEY_ID,
      Bun.env.ALIBABA_CLOUD_ACCESS_KEY_ID
    ),
    objectSecretAccessKey: firstNonEmptyString(
      Bun.env.REPLAY_STORAGE_OBJECT_SECRET_ACCESS_KEY,
      Bun.env.AWS_SECRET_ACCESS_KEY,
      Bun.env.ALIBABA_CLOUD_ACCESS_KEY_SECRET
    ),
    objectSessionToken: firstNonEmptyString(
      Bun.env.REPLAY_STORAGE_OBJECT_SESSION_TOKEN,
      Bun.env.AWS_SESSION_TOKEN,
      Bun.env.ALIBABA_CLOUD_SECURITY_TOKEN
    ),
  };
}

function buildRelativeKey(
  tenantId: string,
  datasetId: string,
  runId: string,
  artifactType: string
): string {
  return [tenantId, datasetId, runId, `${artifactType}.json`]
    .map((item) => item.replace(/[^a-zA-Z0-9._-]+/g, "-"))
    .join("/");
}

function sha256Hex(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}

async function writeLocalFile(rootDir: string, relativeKey: string, content: Uint8Array): Promise<string> {
  const targetPath = join(rootDir, relativeKey);
  await mkdir(dirname(targetPath), { recursive: true });
  const tempPath = `${targetPath}.${Date.now()}.${Math.random().toString(36).slice(2, 8)}.tmp`;
  await Bun.write(tempPath, content);
  await rename(tempPath, targetPath);
  return targetPath;
}

function getObjectClient(config: ReplayArtifactStoreConfig): { file: (key: string) => unknown } {
  const S3ClientCtor = (Bun as unknown as {
    S3Client?: new (options: Record<string, unknown>) => { file: (key: string) => unknown };
  }).S3Client;
  if (!S3ClientCtor) {
    throw new Error("bun_s3_client_unavailable");
  }
  if (!config.objectBucket) {
    throw new Error("replay_object_bucket_required");
  }
  return new S3ClientCtor({
    bucket: config.objectBucket,
    endpoint: config.objectEndpoint,
    region: config.objectRegion,
    accessKeyId: config.objectAccessKeyId,
    secretAccessKey: config.objectSecretAccessKey,
    sessionToken: config.objectSessionToken,
  });
}

async function writeObjectFile(
  config: ReplayArtifactStoreConfig,
  relativeKey: string,
  content: Uint8Array
): Promise<string> {
  const client = getObjectClient(config);
  const objectKey = [config.objectPrefix, relativeKey].filter(Boolean).join("/");
  await Bun.write(client.file(objectKey) as Bun.BunFile, content);
  return objectKey;
}

export async function storeReplayArtifact(input: {
  tenantId: string;
  datasetId: string;
  runId: string;
  artifactType: string;
  content: Uint8Array;
}): Promise<StoredReplayArtifact> {
  const config = getConfig();
  const checksum = sha256Hex(input.content);
  const relativeKey = buildRelativeKey(input.tenantId, input.datasetId, input.runId, input.artifactType);
  const metadata: Record<string, unknown> = {};
  let storageKey = "";

  if (config.mode === "local" || config.mode === "hybrid") {
    const localPath = await writeLocalFile(config.localRoot, relativeKey, input.content);
    metadata.localPath = localPath;
    if (!storageKey) {
      storageKey = localPath;
    }
  }

  if (config.mode === "object" || config.mode === "hybrid") {
    const objectKey = await writeObjectFile(config, relativeKey, input.content);
    metadata.objectKey = objectKey;
    if (config.objectBucket) {
      metadata.objectBucket = config.objectBucket;
    }
    if (config.objectEndpoint) {
      metadata.objectEndpoint = config.objectEndpoint;
    }
    if (config.mode === "object") {
      storageKey = objectKey;
    }
  }

  if (!storageKey) {
    throw new Error("replay_artifact_store_unavailable");
  }

  return {
    storageBackend: config.mode,
    storageKey,
    byteSize: input.content.byteLength,
    checksum,
    metadata,
  };
}

async function readLocalArtifact(pathLike: string): Promise<Uint8Array | null> {
  const normalizedPath = isAbsolute(pathLike) ? pathLike : resolve(pathLike);
  const file = Bun.file(normalizedPath);
  if (!(await file.exists())) {
    return null;
  }
  return new Uint8Array(await file.arrayBuffer());
}

async function readObjectArtifact(config: ReplayArtifactStoreConfig, objectKey: string): Promise<Uint8Array | null> {
  const client = getObjectClient(config);
  const file = client.file(objectKey) as Bun.BunFile;
  if (!(await file.exists())) {
    return null;
  }
  return new Uint8Array(await file.arrayBuffer());
}

export async function readReplayArtifactContent(artifact: ReplayArtifact): Promise<Uint8Array | null> {
  const config = getConfig();
  const localPath = firstNonEmptyString(
    artifact.metadata.localPath,
    artifact.storageBackend === "local" || artifact.storageBackend === "hybrid"
      ? artifact.storageKey
      : undefined
  );
  if (localPath) {
    const content = await readLocalArtifact(localPath);
    if (content) {
      return content;
    }
  }

  const objectKey = firstNonEmptyString(
    artifact.metadata.objectKey,
    artifact.storageBackend === "object" ? artifact.storageKey : undefined
  );
  if (objectKey) {
    return readObjectArtifact(config, objectKey);
  }

  return null;
}

export async function deleteReplayArtifactContent(artifact: ReplayArtifact): Promise<void> {
  const localPath = firstNonEmptyString(artifact.metadata.localPath);
  if (localPath) {
    await unlink(localPath).catch(() => undefined);
  }
}
