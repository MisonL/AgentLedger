import { describe, expect, test } from "bun:test";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { resolveReplayArtifactLocalRoot } from "../src/routes/replay-artifact-store";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

describe("Replay Artifact Store", () => {
  test("默认本地根目录固定到仓库根 data/replay-artifacts，不受 cwd 影响", () => {
    const controlPlaneCwd = join(repoRoot, "apps", "control-plane");

    const localRoot = resolveReplayArtifactLocalRoot({
      cwd: controlPlaneCwd,
      envLocalRoot: undefined,
    });

    expect(localRoot).toBe(join(repoRoot, "data", "replay-artifacts"));
  });

  test("环境变量可覆盖默认目录，relative 路径按调用 cwd 解析", () => {
    const controlPlaneCwd = join(repoRoot, "apps", "control-plane");

    const localRoot = resolveReplayArtifactLocalRoot({
      cwd: controlPlaneCwd,
      envLocalRoot: ".tmp/replay-artifacts",
    });

    expect(localRoot).toBe(join(controlPlaneCwd, ".tmp", "replay-artifacts"));
  });

  test("环境变量为绝对路径时保持绝对路径", () => {
    const controlPlaneCwd = join(repoRoot, "apps", "control-plane");
    const absoluteRoot = join(repoRoot, ".tmp", "absolute-replay-artifacts");

    const localRoot = resolveReplayArtifactLocalRoot({
      cwd: controlPlaneCwd,
      envLocalRoot: absoluteRoot,
    });

    expect(localRoot).toBe(absoluteRoot);
  });
});
