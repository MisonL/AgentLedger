#!/usr/bin/env bun

import { mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildOpenApiDocument } from "../apps/control-plane/src/routes/open-platform";
import {
  asRecord,
  computeDigest,
  extractOpenApiOperations,
  resolveInfoField,
  resolveSpecVersion,
  stringifyWithTrailingNewline,
  toRepoRelativePath,
  type OpenApiOperationSpec,
} from "./sdk-common";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const METADATA_FILE = "metadata.json";
const OPERATIONS_FILE = "operations.json";

const SDK_LANGUAGES = [
  { id: "typescript", name: "TypeScript" },
  { id: "python", name: "Python" },
  { id: "go", name: "Go" },
  { id: "java", name: "Java" },
  { id: "csharp", name: "C#" },
  { id: "php", name: "PHP" },
  { id: "ruby", name: "Ruby" },
  { id: "swift", name: "Swift" },
] as const;

type SdkLanguageId = (typeof SDK_LANGUAGES)[number]["id"];

interface CliOptions {
  inputPath?: string;
  outputDir: string;
  check: boolean;
  help: boolean;
}

interface OpenApiLoadResult {
  document: Record<string, unknown>;
  sourceLabel: string;
}

interface SdkMetadata {
  schemaVersion: number;
  generator: string;
  openapi: {
    source: string;
    title: string;
    version: string;
    specVersion: string;
    digest: string;
  };
  operations: Array<{
    id: string;
    operationId: string;
    method: string;
    path: string;
    methodNameCamel: string;
    methodNameSnake: string;
    methodNamePascal: string;
    hasRequestBody: boolean;
    pathParams: string[];
    wirePathParams: string[];
    queryParams: string[];
    compatibilityAliases?: OpenApiOperationSpec["compatibilityAliases"];
  }>;
  languages: Array<{
    id: string;
    name: string;
    directory: string;
    operationCount: number;
    generatedFiles: string[];
  }>;
}

type CliResult =
  | { success: true; options: CliOptions }
  | { success: false; error: string };

interface OperationCompatibilityRule {
  pathAliases?: Array<{
    canonical: string;
    wire: string;
    aliases: string[];
  }>;
  queryAliases?: Array<{
    canonical: string;
    aliases: string[];
  }>;
  bodyAliases?: Array<{
    canonical: string;
    aliases: string[];
  }>;
}

function printUsage(): void {
  console.log("用法: bun run ./scripts/sdk-generate.ts [--input <openapi.json>] [--output <dir>] [--check]");
  console.log("说明: 默认输出到 clients/sdk；不传 --input 时使用本地 buildOpenApiDocument()。\n      生成 8 语言可调用 SDK（全 operation 覆盖）");
}

function parseCliOptions(argv: string[]): CliResult {
  const options: CliOptions = {
    outputDir: "clients/sdk",
    check: false,
    help: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    if (token === "--check") {
      options.check = true;
      continue;
    }
    if (token === "--input" || token === "-i") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供本地 OpenAPI 文件路径。` };
      }
      options.inputPath = value;
      index += 1;
      continue;
    }
    if (token === "--output" || token === "-o") {
      const value = argv[index + 1];
      if (!value || value.startsWith("-")) {
        return { success: false, error: `${token} 需要提供输出目录。` };
      }
      options.outputDir = value;
      index += 1;
      continue;
    }
    return { success: false, error: `未知参数：${token}` };
  }

  return { success: true, options };
}

function resolveErrorCode(error: unknown): string | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }
  const code = Reflect.get(error, "code");
  return typeof code === "string" ? code : undefined;
}

function parseOpenApiLike(value: unknown): Record<string, unknown> | null {
  return asRecord(value);
}

async function loadOpenApiDocument(inputPath?: string): Promise<OpenApiLoadResult> {
  if (!inputPath) {
    const document = parseOpenApiLike(buildOpenApiDocument());
    if (!document) {
      throw new Error("buildOpenApiDocument() 返回值不是合法对象。");
    }
    return {
      document,
      sourceLabel: "internal:buildOpenApiDocument()",
    };
  }

  const absoluteInputPath = path.resolve(repoRoot, inputPath);
  const content = await readFile(absoluteInputPath, "utf8");
  const parsed = JSON.parse(content) as unknown;
  const document = parseOpenApiLike(parsed);
  if (!document) {
    throw new Error(`OpenAPI 文档必须是 JSON 对象：${toRepoRelativePath(repoRoot, absoluteInputPath)}`);
  }

  return {
    document,
    sourceLabel: toRepoRelativePath(repoRoot, absoluteInputPath),
  };
}

function buildLanguageReadme(
  language: (typeof SDK_LANGUAGES)[number],
  metadata: SdkMetadata
): string {
  const replayExample = buildReplayReadmeExample(language.id);
  return [
    `# ${language.name} SDK`,
    "",
    "该目录由 `bun run sdk:generate` 自动生成。",
    "",
    `- OpenAPI 标题：${metadata.openapi.title}`,
    `- OpenAPI 版本：${metadata.openapi.version}`,
    `- OpenAPI Spec：${metadata.openapi.specVersion}`,
    `- 文档摘要：${metadata.openapi.digest}`,
    `- operation 数量：${metadata.operations.length}`,
    "",
    "Replay v2 canonical：",
    "",
    "- v2 replay 统一使用 `datasetId` / `runId` 作为主语义。",
    "- `baselineId` / `jobId` 仍被客户端接受，但只作为兼容别名。",
    "- 传给 v2 replay 的 `datasetId` 应使用 `createReplayDatasetV2` 返回的资源 ID。",
    "- 支持通过 `materializeReplayDatasetCasesV2` 将历史 session 物化为真实 replay cases。",
    "",
    `入口：\`${resolveLanguageEntryHint(language.id)}\``,
    "",
    "示例：",
    "",
    replayExample,
    "",
  ].join("\n");
}

function buildReplayReadmeExample(languageId: SdkLanguageId): string {
  switch (languageId) {
    case "typescript":
      return [
        "```ts",
        "import { AgentLedgerClient } from \"@agentledger/sdk-typescript\";",
        "",
        "const client = new AgentLedgerClient({",
        "  baseUrl: \"https://control-plane.example.com\",",
        "  token: process.env.AGENTLEDGER_TOKEN,",
        "});",
        "",
        "const datasetId = \"dataset_123\"; // 来自 createReplayDatasetV2 返回值",
        "const runId = \"run_456\"; // 来自 createReplayRunV2 / getReplayRunV2",
        "",
        "await client.replaceReplayDatasetCasesV2({",
        "  path: { datasetId },",
        "  body: {",
        "    items: [",
        "      {",
        "        caseId: \"case-1\",",
        "        input: \"hello\",",
        "        expectedOutput: \"world\",",
        "      },",
        "    ],",
        "  },",
        "});",
        "",
        "await client.listReplayDatasetCasesV2({",
        "  path: { datasetId },",
        "  query: { limit: 20 },",
        "});",
        "",
        "await client.createReplayRunV2({",
        "  body: { datasetId, candidateLabel: \"candidate-v2\" },",
        "});",
        "",
        "await client.listReplayRunsV2({",
        "  query: { datasetId, status: \"completed\" },",
        "});",
        "",
        "await client.getReplayRunDiffsV2({",
        "  path: { runId },",
        "  query: { datasetId, limit: 20 },",
        "});",
        "",
        "await client.getReplayRunArtifactsV2({",
        "  path: { runId },",
        "});",
        "```",
      ].join("\n");
    case "python":
      return [
        "```python",
        "from agentledger_sdk import AgentLedgerClient, OperationRequest",
        "",
        "client = AgentLedgerClient(",
        "    base_url=\"https://control-plane.example.com\",",
        "    token=\"${AGENTLEDGER_TOKEN}\",",
        ")",
        "",
        "dataset_id = \"dataset_123\"  # 来自 createReplayDatasetV2 返回值",
        "run_id = \"run_456\"  # 来自 createReplayRunV2 / getReplayRunV2",
        "",
        "client.replace_replay_dataset_cases_v2(",
        "    OperationRequest(",
        "        path={\"datasetId\": dataset_id},",
        "        body={",
        "            \"items\": [",
        "                {\"caseId\": \"case-1\", \"input\": \"hello\", \"expectedOutput\": \"world\"}",
        "            ]",
        "        },",
        "    )",
        ")",
        "",
        "client.list_replay_dataset_cases_v2(",
        "    OperationRequest(path={\"datasetId\": dataset_id}, query={\"limit\": 20})",
        ")",
        "",
        "client.create_replay_run_v2(",
        "    OperationRequest(body={\"datasetId\": dataset_id, \"candidateLabel\": \"candidate-v2\"})",
        ")",
        "",
        "client.list_replay_runs_v2(",
        "    OperationRequest(query={\"datasetId\": dataset_id, \"status\": \"completed\"})",
        ")",
        "",
        "client.get_replay_run_diffs_v2(",
        "    OperationRequest(path={\"runId\": run_id}, query={\"datasetId\": dataset_id, \"limit\": 20})",
        ")",
        "",
        "client.get_replay_run_artifacts_v2(",
        "    OperationRequest(path={\"runId\": run_id})",
        ")",
        "```",
      ].join("\n");
    case "go":
      return [
        "```go",
        "client := agentledgersdk.NewClient(\"https://control-plane.example.com\", os.Getenv(\"AGENTLEDGER_TOKEN\"))",
        "",
        "datasetID := \"dataset_123\" // 来自 CreateReplayDatasetV2 返回值",
        "runID := \"run_456\" // 来自 CreateReplayRunV2 / GetReplayRunV2",
        "",
        "_, _ = client.ReplaceReplayDatasetCasesV2(ctx, &agentledgersdk.OperationRequest{",
        "    PathParams: map[string]string{\"datasetId\": datasetID},",
        "    Body: map[string]any{",
        "        \"items\": []map[string]any{",
        "            {\"caseId\": \"case-1\", \"input\": \"hello\", \"expectedOutput\": \"world\"},",
        "        },",
        "    },",
        "})",
        "",
        "_, _ = client.ListReplayDatasetCasesV2(ctx, &agentledgersdk.OperationRequest{",
        "    PathParams: map[string]string{\"datasetId\": datasetID},",
        "    Query: map[string]string{\"limit\": \"20\"},",
        "})",
        "",
        "_, _ = client.CreateReplayRunV2(ctx, &agentledgersdk.OperationRequest{",
        "    Body: map[string]any{\"datasetId\": datasetID, \"candidateLabel\": \"candidate-v2\"},",
        "})",
        "",
        "_, _ = client.ListReplayRunsV2(ctx, &agentledgersdk.OperationRequest{",
        "    Query: map[string]string{\"datasetId\": datasetID, \"status\": \"completed\"},",
        "})",
        "",
        "_, _ = client.GetReplayRunDiffsV2(ctx, &agentledgersdk.OperationRequest{",
        "    PathParams: map[string]string{\"runId\": runID},",
        "    Query: map[string]string{\"datasetId\": datasetID, \"limit\": \"20\"},",
        "})",
        "",
        "_, _ = client.GetReplayRunArtifactsV2(ctx, &agentledgersdk.OperationRequest{",
        "    PathParams: map[string]string{\"runId\": runID},",
        "})",
        "```",
      ].join("\n");
    case "java":
      return [
        "```java",
        "var client = new AgentLedgerClient(\"https://control-plane.example.com\", System.getenv(\"AGENTLEDGER_TOKEN\"));",
        "",
        "var datasetId = \"dataset_123\"; // 来自 createReplayDatasetV2 返回值",
        "var runId = \"run_456\"; // 来自 createReplayRunV2 / getReplayRunV2",
        "",
        "var replaceCases = new AgentLedgerClient.OperationRequest();",
        "replaceCases.path.put(\"datasetId\", datasetId);",
        "replaceCases.body = \"\"\"",
        "{\"items\":[{\"caseId\":\"case-1\",\"input\":\"hello\",\"expectedOutput\":\"world\"}]}",
        "\"\"\";",
        "client.replaceReplayDatasetCasesV2(replaceCases);",
        "",
        "var listCases = new AgentLedgerClient.OperationRequest();",
        "listCases.path.put(\"datasetId\", datasetId);",
        "listCases.query.put(\"limit\", \"20\");",
        "client.listReplayDatasetCasesV2(listCases);",
        "",
        "var createRun = new AgentLedgerClient.OperationRequest();",
        "createRun.body = \"\"\"",
        "{\"datasetId\":\"dataset_123\",\"candidateLabel\":\"candidate-v2\"}",
        "\"\"\";",
        "client.createReplayRunV2(createRun);",
        "",
        "var listRuns = new AgentLedgerClient.OperationRequest();",
        "listRuns.query.put(\"datasetId\", datasetId);",
        "listRuns.query.put(\"status\", \"completed\");",
        "client.listReplayRunsV2(listRuns);",
        "",
        "var diffs = new AgentLedgerClient.OperationRequest();",
        "diffs.path.put(\"runId\", runId);",
        "diffs.query.put(\"datasetId\", datasetId);",
        "diffs.query.put(\"limit\", \"20\");",
        "client.getReplayRunDiffsV2(diffs);",
        "",
        "var artifacts = new AgentLedgerClient.OperationRequest();",
        "artifacts.path.put(\"runId\", runId);",
        "client.getReplayRunArtifactsV2(artifacts);",
        "```",
      ].join("\n");
    case "csharp":
      return [
        "```csharp",
        "var client = new AgentLedgerClient(\"https://control-plane.example.com\", Environment.GetEnvironmentVariable(\"AGENTLEDGER_TOKEN\"));",
        "",
        "var datasetId = \"dataset_123\"; // 来自 CreateReplayDatasetV2 返回值",
        "var runId = \"run_456\"; // 来自 CreateReplayRunV2 / GetReplayRunV2",
        "",
        "await client.ReplaceReplayDatasetCasesV2(new OperationRequest",
        "{",
        "    Path = new Dictionary<string, string> { [\"datasetId\"] = datasetId },",
        "    Body = new",
        "    {",
        "        items = new[]",
        "        {",
        "            new { caseId = \"case-1\", input = \"hello\", expectedOutput = \"world\" },",
        "        },",
        "    },",
        "});",
        "",
        "await client.ListReplayDatasetCasesV2(new OperationRequest",
        "{",
        "    Path = new Dictionary<string, string> { [\"datasetId\"] = datasetId },",
        "    Query = new Dictionary<string, string> { [\"limit\"] = \"20\" },",
        "});",
        "",
        "await client.CreateReplayRunV2(new OperationRequest",
        "{",
        "    Body = new { datasetId, candidateLabel = \"candidate-v2\" },",
        "});",
        "",
        "await client.ListReplayRunsV2(new OperationRequest",
        "{",
        "    Query = new Dictionary<string, string> { [\"datasetId\"] = datasetId, [\"status\"] = \"completed\" },",
        "});",
        "",
        "await client.GetReplayRunDiffsV2(new OperationRequest",
        "{",
        "    Path = new Dictionary<string, string> { [\"runId\"] = runId },",
        "    Query = new Dictionary<string, string> { [\"datasetId\"] = datasetId, [\"limit\"] = \"20\" },",
        "});",
        "",
        "await client.GetReplayRunArtifactsV2(new OperationRequest",
        "{",
        "    Path = new Dictionary<string, string> { [\"runId\"] = runId },",
        "});",
        "```",
      ].join("\n");
    case "php":
      return [
        "```php",
        "$client = new AgentLedger\\Sdk\\AgentLedgerClient(",
        "    'https://control-plane.example.com',",
        "    getenv('AGENTLEDGER_TOKEN') ?: null,",
        ");",
        "",
        "$datasetId = 'dataset_123'; // 来自 createReplayDatasetV2 返回值",
        "$runId = 'run_456'; // 来自 createReplayRunV2 / getReplayRunV2",
        "",
        "$client->replaceReplayDatasetCasesV2([",
        "    'path' => ['datasetId' => $datasetId],",
        "    'body' => [",
        "        'items' => [",
        "            ['caseId' => 'case-1', 'input' => 'hello', 'expectedOutput' => 'world'],",
        "        ],",
        "    ],",
        "]);",
        "",
        "$client->listReplayDatasetCasesV2([",
        "    'path' => ['datasetId' => $datasetId],",
        "    'query' => ['limit' => 20],",
        "]);",
        "",
        "$client->createReplayRunV2([",
        "    'body' => ['datasetId' => $datasetId, 'candidateLabel' => 'candidate-v2'],",
        "]);",
        "",
        "$client->listReplayRunsV2([",
        "    'query' => ['datasetId' => $datasetId, 'status' => 'completed'],",
        "]);",
        "",
        "$client->getReplayRunDiffsV2([",
        "    'path' => ['runId' => $runId],",
        "    'query' => ['datasetId' => $datasetId, 'limit' => 20],",
        "]);",
        "",
        "$client->getReplayRunArtifactsV2([",
        "    'path' => ['runId' => $runId],",
        "]);",
        "```",
      ].join("\n");
    case "ruby":
      return [
        "```ruby",
        "client = AgentLedgerSdk::Client.new(",
        "  base_url: \"https://control-plane.example.com\",",
        "  token: ENV.fetch(\"AGENTLEDGER_TOKEN\", nil),",
        ")",
        "",
        "dataset_id = \"dataset_123\" # 来自 createReplayDatasetV2 返回值",
        "run_id = \"run_456\" # 来自 createReplayRunV2 / getReplayRunV2",
        "",
        "client.replace_replay_dataset_cases_v2(",
        "  path: { \"datasetId\" => dataset_id },",
        "  body: {",
        "    items: [",
        "      { caseId: \"case-1\", input: \"hello\", expectedOutput: \"world\" },",
        "    ],",
        "  },",
        ")",
        "",
        "client.list_replay_dataset_cases_v2(",
        "  path: { \"datasetId\" => dataset_id },",
        "  query: { limit: 20 },",
        ")",
        "",
        "client.create_replay_run_v2(",
        "  body: { datasetId: dataset_id, candidateLabel: \"candidate-v2\" },",
        ")",
        "",
        "client.list_replay_runs_v2(",
        "  query: { datasetId: dataset_id, status: \"completed\" },",
        ")",
        "",
        "client.get_replay_run_diffs_v2(",
        "  path: { \"runId\" => run_id },",
        "  query: { datasetId: dataset_id, limit: 20 },",
        ")",
        "",
        "client.get_replay_run_artifacts_v2(",
        "  path: { \"runId\" => run_id },",
        ")",
        "```",
      ].join("\n");
    case "swift":
      return [
        "```swift",
        "let client = AgentLedgerClient(",
        "    baseUrl: \"https://control-plane.example.com\",",
        "    token: ProcessInfo.processInfo.environment[\"AGENTLEDGER_TOKEN\"]",
        ")",
        "",
        "let datasetId = \"dataset_123\" // 来自 createReplayDatasetV2 返回值",
        "let runId = \"run_456\" // 来自 createReplayRunV2 / getReplayRunV2",
        "",
        "try await client.replaceReplayDatasetCasesV2(",
        "    request: OperationRequest(",
        "        path: [\"datasetId\": datasetId],",
        "        body: [",
        "            \"items\": [",
        "                [\"caseId\": \"case-1\", \"input\": \"hello\", \"expectedOutput\": \"world\"],",
        "            ],",
        "        ]",
        "    )",
        ")",
        "",
        "try await client.listReplayDatasetCasesV2(",
        "    request: OperationRequest(",
        "        path: [\"datasetId\": datasetId],",
        "        query: [\"limit\": \"20\"]",
        "    )",
        ")",
        "",
        "try await client.createReplayRunV2(",
        "    request: OperationRequest(body: [\"datasetId\": datasetId, \"candidateLabel\": \"candidate-v2\"])",
        ")",
        "",
        "try await client.listReplayRunsV2(",
        "    request: OperationRequest(query: [\"datasetId\": datasetId, \"status\": \"completed\"])",
        ")",
        "",
        "try await client.getReplayRunDiffsV2(",
        "    request: OperationRequest(",
        "        path: [\"runId\": runId],",
        "        query: [\"datasetId\": datasetId, \"limit\": \"20\"]",
        "    )",
        ")",
        "",
        "try await client.getReplayRunArtifactsV2(",
        "    request: OperationRequest(path: [\"runId\": runId])",
        ")",
        "```",
      ].join("\n");
    default:
      return [
        "```text",
        `调用方式请查看本目录源码入口文件（${resolveLanguageEntryHint(languageId)}）。`,
        "```",
      ].join("\n");
  }
}

function buildOperationCompatibilityRule(operation: OpenApiOperationSpec): OperationCompatibilityRule | null {
  const compatibility = operation.compatibilityAliases;
  if (!compatibility) {
    return null;
  }
  const rule: OperationCompatibilityRule = {};
  if (compatibility.path && compatibility.path.length > 0) {
    rule.pathAliases = compatibility.path.map((item) => ({
      canonical: item.canonicalName,
      wire: item.wireName ?? item.canonicalName,
      aliases: item.compatibilityNames,
    }));
  }
  if (compatibility.query && compatibility.query.length > 0) {
    rule.queryAliases = compatibility.query.map((item) => ({
      canonical: item.canonicalName,
      aliases: item.compatibilityNames,
    }));
  }
  if (compatibility.body && compatibility.body.length > 0) {
    rule.bodyAliases = compatibility.body.map((item) => ({
      canonical: item.canonicalName,
      aliases: item.compatibilityNames,
    }));
  }
  return Object.keys(rule).length > 0 ? rule : null;
}

function buildOperationCompatibilityRules(
  operations: OpenApiOperationSpec[]
): Record<string, OperationCompatibilityRule> {
  const rules: Record<string, OperationCompatibilityRule> = {};
  for (const operation of operations) {
    const rule = buildOperationCompatibilityRule(operation);
    if (rule) {
      rules[operation.id] = rule;
    }
  }
  return rules;
}

function resolveLanguageEntryHint(languageId: SdkLanguageId): string {
  switch (languageId) {
    case "typescript":
      return "src/index.ts";
    case "python":
      return "agentledger_sdk/client.py";
    case "go":
      return "client.go";
    case "java":
      return "src/main/java/com/agentledger/sdk/AgentLedgerClient.java";
    case "csharp":
      return "AgentLedgerClient.cs";
    case "php":
      return "src/AgentLedgerClient.php";
    case "ruby":
      return "lib/agentledger_sdk/client.rb";
    case "swift":
      return "Sources/AgentLedgerSDK/AgentLedgerClient.swift";
    default:
      return "README.md";
  }
}

function buildMetadata(
  document: Record<string, unknown>,
  sourceLabel: string,
  operations: OpenApiOperationSpec[]
): SdkMetadata {
  const digest = computeDigest(document);
  const title = resolveInfoField(document, "title");
  const version = resolveInfoField(document, "version");
  const specVersion = resolveSpecVersion(document);

  return {
    schemaVersion: 2,
    generator: "agentledger-sdk-full",
    openapi: {
      source: sourceLabel,
      title,
      version,
      specVersion,
      digest,
    },
    operations: operations.map((operation) => ({
      id: operation.id,
      operationId: operation.operationId,
      method: operation.method,
      path: operation.path,
      methodNameCamel: operation.methodNameCamel,
      methodNameSnake: operation.methodNameSnake,
      methodNamePascal: operation.methodNamePascal,
      hasRequestBody: operation.hasRequestBody,
      pathParams: operation.pathParams,
      wirePathParams: operation.wirePathParams,
      queryParams: operation.queryParams,
      compatibilityAliases: operation.compatibilityAliases,
    })),
    languages: SDK_LANGUAGES.map((language) => ({
      id: language.id,
      name: language.name,
      directory: language.id,
      operationCount: operations.length,
      generatedFiles: [],
    })),
  };
}

function buildOperationsJson(operations: OpenApiOperationSpec[]): string {
  return stringifyWithTrailingNewline(
    operations.map((operation) => ({
      id: operation.id,
      operationId: operation.operationId,
      method: operation.method,
      path: operation.path,
      methodNameCamel: operation.methodNameCamel,
      methodNameSnake: operation.methodNameSnake,
      methodNamePascal: operation.methodNamePascal,
      hasRequestBody: operation.hasRequestBody,
      pathParams: operation.pathParams,
      wirePathParams: operation.wirePathParams,
      queryParams: operation.queryParams,
      compatibilityAliases: operation.compatibilityAliases,
    }))
  );
}

function buildTypescriptFiles(
  operations: OpenApiOperationSpec[],
  metadata: SdkMetadata
): Record<string, string> {
  const compatibilityRules = stringifyWithTrailingNewline(buildOperationCompatibilityRules(operations)).trim();
  const methods = operations
    .map(
      (operation) => `  /** ${operation.id} | ${operation.method} ${operation.path} */\n  async ${operation.methodNameCamel}(request: OperationRequest = {}): Promise<unknown> {\n    return this.request("${operation.method}", "${operation.path}", this.normalizeCompatibilityRequest("${operation.id}", request));\n  }`
    )
    .join("\n\n");

  const indexTs = `type OperationCompatibilityRule = {\n  pathAliases?: Array<{ canonical: string; wire: string; aliases: string[] }>;\n  queryAliases?: Array<{ canonical: string; aliases: string[] }>;\n  bodyAliases?: Array<{ canonical: string; aliases: string[] }>;\n};\n\nconst OPERATION_COMPATIBILITY: Record<string, OperationCompatibilityRule> = ${compatibilityRules} as Record<string, OperationCompatibilityRule>;\n\nexport interface OperationRequest {\n  path?: Record<string, string | number>;\n  query?: Record<string, string | number | boolean | null | undefined>;\n  body?: unknown;\n  headers?: Record<string, string>;\n  signal?: AbortSignal;\n}\n\nexport class AgentLedgerApiError extends Error {\n  readonly status: number;\n  readonly payload: unknown;\n\n  constructor(status: number, message: string, payload?: unknown) {\n    super(message);\n    this.name = \"AgentLedgerApiError\";\n    this.status = status;\n    this.payload = payload;\n  }\n}\n\nexport interface AgentLedgerClientOptions {\n  baseUrl: string;\n  token?: string;\n  defaultHeaders?: Record<string, string>;\n  fetchImpl?: typeof fetch;\n}\n\nexport class AgentLedgerClient {\n  private readonly baseUrl: string;\n  private readonly token?: string;\n  private readonly defaultHeaders: Record<string, string>;\n  private readonly fetchImpl: typeof fetch;\n\n  constructor(options: AgentLedgerClientOptions) {\n    this.baseUrl = options.baseUrl.replace(/\\/$/, \"\");\n    this.token = options.token;\n    this.defaultHeaders = options.defaultHeaders ?? {};\n    this.fetchImpl = options.fetchImpl ?? fetch;\n  }\n\n  private renderPath(pathTemplate: string, pathParams?: Record<string, string | number>): string {\n    return pathTemplate.replace(/\\{([^}]+)\\}/g, (_, rawKey: string) => {\n      const key = String(rawKey).trim();\n      if (!pathParams || !(key in pathParams)) {\n        throw new Error(\`缺少 path 参数：\${key}\`);\n      }\n      return encodeURIComponent(String(pathParams[key]));\n    });\n  }\n\n  private buildQuery(query?: Record<string, string | number | boolean | null | undefined>): string {\n    if (!query) {\n      return \"\";\n    }\n    const params = new URLSearchParams();\n    for (const [key, value] of Object.entries(query)) {\n      if (value === undefined || value === null) {\n        continue;\n      }\n      params.set(key, String(value));\n    }\n    const queryText = params.toString();\n    return queryText.length > 0 ? \`?\${queryText}\` : \"\";\n  }\n\n  private resolveCompatibilityValue(record: Record<string, unknown>, candidates: string[]): unknown {\n    for (const candidate of candidates) {\n      if (candidate in record) {\n        const value = record[candidate];\n        if (value !== undefined && value !== null) {\n          return value;\n        }\n      }\n    }\n    return undefined;\n  }\n\n  private normalizeCompatibilityRequest(operationId: string, request: OperationRequest): OperationRequest {\n    const rule = OPERATION_COMPATIBILITY[operationId];\n    if (!rule) {\n      return request;\n    }\n\n    const path = { ...(request.path ?? {}) } as Record<string, unknown>;\n    const query = { ...(request.query ?? {}) } as Record<string, unknown>;\n    let body = request.body;\n\n    for (const alias of rule.pathAliases ?? []) {\n      const value = this.resolveCompatibilityValue(path, [alias.canonical, ...alias.aliases]);\n      if (value !== undefined) {\n        path[alias.wire] = value;\n      }\n    }\n\n    for (const alias of rule.queryAliases ?? []) {\n      const value = this.resolveCompatibilityValue(query, [alias.canonical, ...alias.aliases]);\n      if (value !== undefined) {\n        query[alias.canonical] = value;\n      }\n    }\n\n    if (body && typeof body === \"object\" && !Array.isArray(body)) {\n      const normalizedBody = { ...(body as Record<string, unknown>) };\n      for (const alias of rule.bodyAliases ?? []) {\n        const value = this.resolveCompatibilityValue(normalizedBody, [alias.canonical, ...alias.aliases]);\n        if (value !== undefined) {\n          normalizedBody[alias.canonical] = value;\n        }\n      }\n      body = normalizedBody;\n    }\n\n    return {\n      ...request,\n      path: path as Record<string, string | number>,\n      query: query as Record<string, string | number | boolean | null | undefined>,\n      body,\n    };\n  }\n\n  private async request(method: string, pathTemplate: string, request: OperationRequest): Promise<unknown> {\n    const url = \`\${this.baseUrl}\${this.renderPath(pathTemplate, request.path)}\${this.buildQuery(request.query)}\`;\n    const headers: Record<string, string> = {\n      \"content-type\": \"application/json\",\n      ...this.defaultHeaders,\n      ...(request.headers ?? {}),\n    };\n    if (this.token && !headers.authorization) {\n      headers.authorization = \`Bearer \${this.token}\`;\n    }\n\n    const response = await this.fetchImpl(url, {\n      method,\n      headers,\n      body: request.body === undefined ? undefined : JSON.stringify(request.body),\n      signal: request.signal,\n    });\n\n    const contentType = response.headers.get(\"content-type\") ?? \"\";\n    const payload = contentType.toLowerCase().includes(\"application/json\")\n      ? await response.json().catch(() => undefined)\n      : await response.text().catch(() => undefined);\n\n    if (!response.ok) {\n      const message =\n        payload && typeof payload === \"object\" && \"message\" in payload && typeof (payload as { message?: unknown }).message === \"string\"\n          ? (payload as { message: string }).message\n          : \`请求失败: \${response.status}\`;\n      throw new AgentLedgerApiError(response.status, message, payload);\n    }\n\n    return payload;\n  }\n\n${methods}\n}\n`;

  const packageJson = stringifyWithTrailingNewline({
    name: "@agentledger/sdk-typescript",
    version: metadata.openapi.version,
    private: true,
    type: "module",
    main: "dist/index.js",
    types: "dist/index.d.ts",
    scripts: {
      build: "tsc -p tsconfig.json",
    },
  });

  const tsconfigJson = stringifyWithTrailingNewline({
    compilerOptions: {
      target: "ES2022",
      module: "ES2022",
      moduleResolution: "Bundler",
      declaration: true,
      outDir: "dist",
      strict: true,
      skipLibCheck: true,
    },
    include: ["src/**/*.ts"],
  });

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[0], metadata),
    "package.json": packageJson,
    "tsconfig.json": tsconfigJson,
    "src/index.ts": indexTs,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildPythonFiles(
  operations: OpenApiOperationSpec[],
  metadata: SdkMetadata
): Record<string, string> {
  const compatibilityRules = stringifyWithTrailingNewline(buildOperationCompatibilityRules(operations)).trim();
  const methods = operations
    .map(
      (operation) => `    # ${operation.id} | ${operation.method} ${operation.path}\n    def ${operation.methodNameSnake}(self, request: Optional[OperationRequest] = None) -> Any:\n        req = self._normalize_compatibility_request("${operation.id}", request or OperationRequest())\n        return self._request("${operation.method}", "${operation.path}", req)`
    )
    .join("\n\n");

  const clientPy = `from __future__ import annotations\n\nfrom dataclasses import dataclass, field\nfrom typing import Any, Dict, Optional\nimport json\nfrom urllib.parse import urlencode, quote\nfrom urllib.request import Request, urlopen\n\n\nOPERATION_COMPATIBILITY: Dict[str, Dict[str, Any]] = json.loads(r'''${compatibilityRules}''')\n\n\nclass AgentLedgerApiError(Exception):\n    def __init__(self, status: int, message: str, payload: Any = None) -> None:\n        super().__init__(message)\n        self.status = status\n        self.payload = payload\n\n\n@dataclass\nclass OperationRequest:\n    path: Dict[str, Any] = field(default_factory=dict)\n    query: Dict[str, Any] = field(default_factory=dict)\n    body: Any = None\n    headers: Dict[str, str] = field(default_factory=dict)\n\n\nclass AgentLedgerClient:\n    def __init__(self, base_url: str, token: Optional[str] = None, timeout: float = 10.0) -> None:\n        self.base_url = base_url.rstrip(\"/\")\n        self.token = token\n        self.timeout = timeout\n\n    def _render_path(self, path_template: str, path_params: Dict[str, Any]) -> str:\n        path_value = path_template\n        for raw_key in [segment[1:-1] for segment in path_template.split(\"/\") if segment.startswith(\"{\") and segment.endswith(\"}\")]:\n            if raw_key not in path_params:\n                raise ValueError(f\"缺少 path 参数: {raw_key}\")\n            path_value = path_value.replace(\"{\" + raw_key + \"}\", quote(str(path_params[raw_key]), safe=\"\"))\n        return path_value\n\n    def _build_query(self, query: Dict[str, Any]) -> str:\n        normalized: Dict[str, str] = {}\n        for key, value in query.items():\n            if value is None:\n                continue\n            normalized[key] = str(value)\n        if not normalized:\n            return \"\"\n        return \"?\" + urlencode(normalized)\n\n    def _resolve_compatibility_value(self, record: Dict[str, Any], candidates: list[str]) -> Any:\n        for candidate in candidates:\n            if candidate in record and record[candidate] is not None:\n                return record[candidate]\n        return None\n\n    def _normalize_compatibility_request(self, operation_id: str, request: OperationRequest) -> OperationRequest:\n        rule = OPERATION_COMPATIBILITY.get(operation_id)\n        if not rule:\n            return request\n\n        path = dict(request.path)\n        query = dict(request.query)\n        body = request.body\n\n        for alias in rule.get(\"pathAliases\", []):\n            value = self._resolve_compatibility_value(path, [alias[\"canonical\"], *alias.get(\"aliases\", [])])\n            if value is not None:\n                path[alias[\"wire\"]] = value\n\n        for alias in rule.get(\"queryAliases\", []):\n            value = self._resolve_compatibility_value(query, [alias[\"canonical\"], *alias.get(\"aliases\", [])])\n            if value is not None:\n                query[alias[\"canonical\"]] = value\n\n        if isinstance(body, dict):\n            normalized_body = dict(body)\n            for alias in rule.get(\"bodyAliases\", []):\n                value = self._resolve_compatibility_value(normalized_body, [alias[\"canonical\"], *alias.get(\"aliases\", [])])\n                if value is not None:\n                    normalized_body[alias[\"canonical\"]] = value\n            body = normalized_body\n\n        return OperationRequest(path=path, query=query, body=body, headers=dict(request.headers))\n\n    def _request(self, method: str, path_template: str, request: OperationRequest) -> Any:\n        url = self.base_url + self._render_path(path_template, request.path) + self._build_query(request.query)\n        headers: Dict[str, str] = {\"content-type\": \"application/json\", **request.headers}\n        if self.token and \"authorization\" not in {key.lower(): value for key, value in headers.items()}:\n            headers[\"authorization\"] = f\"Bearer {self.token}\"\n\n        data = None\n        if request.body is not None:\n            data = json.dumps(request.body).encode(\"utf-8\")\n\n        req = Request(url=url, method=method, headers=headers, data=data)\n        try:\n            with urlopen(req, timeout=self.timeout) as response:\n                raw = response.read().decode(\"utf-8\") if response.length is None or response.length > 0 else \"\"\n                content_type = response.headers.get(\"content-type\", \"\")\n                if \"application/json\" in content_type.lower() and raw:\n                    return json.loads(raw)\n                return raw\n        except Exception as exc:  # noqa: BLE001\n            status = getattr(exc, \"code\", 500)\n            payload = None\n            message = str(exc)\n            raise AgentLedgerApiError(int(status), message, payload) from exc\n\n${methods}\n`;

  const initPy = `from .client import AgentLedgerApiError, AgentLedgerClient, OperationRequest\n\n__all__ = [\n    "AgentLedgerClient",\n    "OperationRequest",\n    "AgentLedgerApiError",\n]\n`;

  const pyprojectToml = `[build-system]\nrequires = ["setuptools>=68", "wheel"]\nbuild-backend = "setuptools.build_meta"\n\n[project]\nname = "agentledger-sdk-python"\nversion = "${metadata.openapi.version}"\ndescription = "AgentLedger Python SDK"\nrequires-python = ">=3.10"\n`;

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[1], metadata),
    "pyproject.toml": `${pyprojectToml}\n`,
    "agentledger_sdk/__init__.py": initPy,
    "agentledger_sdk/client.py": clientPy,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildGoFiles(operations: OpenApiOperationSpec[], metadata: SdkMetadata): Record<string, string> {
  const compatibilityRules = stringifyWithTrailingNewline(buildOperationCompatibilityRules(operations)).trim();
  const methods = operations
    .map(
      (operation) => `// ${operation.id} | ${operation.method} ${operation.path}\nfunc (c *Client) ${operation.methodNamePascal}(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {\n\tif req == nil {\n\t\treq = &OperationRequest{}\n\t}\n\tresolved := c.normalizeCompatibilityRequest("${operation.id}", req)\n\treturn c.request(ctx, "${operation.method}", "${operation.path}", resolved)\n}`
    )
    .join("\n\n");

  const clientGo = `package agentledgersdk\n\nimport (\n\t"bytes"\n\t"context"\n\t"encoding/json"\n\t"fmt"\n\t"io"\n\t"net/http"\n\t"net/url"\n\t"strings"\n\t"time"\n)\n\nconst operationCompatibilityJSON = \`${compatibilityRules}\`\n\ntype operationCompatibilityRule struct {\n\tPathAliases  []operationPathAlias  \`json:"pathAliases"\`\n\tQueryAliases []operationQueryAlias \`json:"queryAliases"\`\n\tBodyAliases  []operationBodyAlias  \`json:"bodyAliases"\`\n}\n\ntype operationPathAlias struct {\n\tCanonical string   \`json:"canonical"\`\n\tWire      string   \`json:"wire"\`\n\tAliases   []string \`json:"aliases"\`\n}\n\ntype operationQueryAlias struct {\n\tCanonical string   \`json:"canonical"\`\n\tAliases   []string \`json:"aliases"\`\n}\n\ntype operationBodyAlias struct {\n\tCanonical string   \`json:"canonical"\`\n\tAliases   []string \`json:"aliases"\`\n}\n\nvar operationCompatibility = mustLoadOperationCompatibility()\n\nfunc mustLoadOperationCompatibility() map[string]operationCompatibilityRule {\n\trules := map[string]operationCompatibilityRule{}\n\tif err := json.Unmarshal([]byte(operationCompatibilityJSON), &rules); err != nil {\n\t\tpanic(err)\n\t}\n\treturn rules\n}\n\nfunc cloneStringMap(input map[string]string) map[string]string {\n\tif len(input) == 0 {\n\t\treturn map[string]string{}\n\t}\n\tcloned := make(map[string]string, len(input))\n\tfor key, value := range input {\n\t\tcloned[key] = value\n\t}\n\treturn cloned\n}\n\nfunc cloneAnyMap(input map[string]any) map[string]any {\n\tif len(input) == 0 {\n\t\treturn map[string]any{}\n\t}\n\tcloned := make(map[string]any, len(input))\n\tfor key, value := range input {\n\t\tcloned[key] = value\n\t}\n\treturn cloned\n}\n\nfunc resolveStringCompatibilityValue(record map[string]string, candidates []string) (string, bool) {\n\tfor _, candidate := range candidates {\n\t\tif value, ok := record[candidate]; ok && strings.TrimSpace(value) != "" {\n\t\t\treturn value, true\n\t\t}\n\t}\n\treturn "", false\n}\n\nfunc resolveAnyCompatibilityValue(record map[string]any, candidates []string) (any, bool) {\n\tfor _, candidate := range candidates {\n\t\tvalue, ok := record[candidate]\n\t\tif !ok || value == nil {\n\t\t\tcontinue\n\t\t}\n\t\tif text, ok := value.(string); ok && strings.TrimSpace(text) == "" {\n\t\t\tcontinue\n\t\t}\n\t\treturn value, true\n\t}\n\treturn nil, false\n}\n\nfunc normalizeCompatibilityBody(body any, rule operationCompatibilityRule) any {\n\tif len(rule.BodyAliases) == 0 || body == nil {\n\t\treturn body\n\t}\n\n\tswitch typed := body.(type) {\n\tcase map[string]any:\n\t\tnormalized := cloneAnyMap(typed)\n\t\tfor _, alias := range rule.BodyAliases {\n\t\t\tcandidates := append([]string{alias.Canonical}, alias.Aliases...)\n\t\t\tif value, ok := resolveAnyCompatibilityValue(normalized, candidates); ok {\n\t\t\t\tnormalized[alias.Canonical] = value\n\t\t\t}\n\t\t}\n\t\treturn normalized\n\tcase map[string]string:\n\t\tnormalized := make(map[string]any, len(typed))\n\t\tfor key, value := range typed {\n\t\t\tnormalized[key] = value\n\t\t}\n\t\tfor _, alias := range rule.BodyAliases {\n\t\t\tcandidates := append([]string{alias.Canonical}, alias.Aliases...)\n\t\t\tif value, ok := resolveAnyCompatibilityValue(normalized, candidates); ok {\n\t\t\t\tnormalized[alias.Canonical] = value\n\t\t\t}\n\t\t}\n\t\treturn normalized\n\tdefault:\n\t\treturn body\n\t}\n}\n\n// ApiError 表示 HTTP 失败响应。\ntype ApiError struct {\n\tStatus int\n\tMessage string\n\tPayload json.RawMessage\n}\n\nfunc (e *ApiError) Error() string {\n\tif e == nil {\n\t\treturn "<nil>"\n\t}\n\treturn e.Message\n}\n\n// OperationRequest 表示单次 API 调用请求。\ntype OperationRequest struct {\n\tPathParams map[string]string\n\tQuery      map[string]string\n\tBody       any\n\tHeaders    map[string]string\n}\n\n// Client 表示 AgentLedger 控制面客户端。\ntype Client struct {\n\tBaseURL    string\n\tToken      string\n\tHTTPClient *http.Client\n}\n\nfunc NewClient(baseURL string, token string) *Client {\n\treturn &Client{\n\t\tBaseURL: strings.TrimRight(baseURL, "/"),\n\t\tToken:   token,\n\t\tHTTPClient: &http.Client{\n\t\t\tTimeout: 15 * time.Second,\n\t\t},\n\t}\n}\n\nfunc (c *Client) normalizeCompatibilityRequest(operationID string, req *OperationRequest) *OperationRequest {\n\trule, ok := operationCompatibility[operationID]\n\tif !ok {\n\t\treturn req\n\t}\n\n\tpathParams := cloneStringMap(req.PathParams)\n\tqueryParams := cloneStringMap(req.Query)\n\tfor _, alias := range rule.PathAliases {\n\t\tcandidates := append([]string{alias.Canonical}, alias.Aliases...)\n\t\tif value, found := resolveStringCompatibilityValue(pathParams, candidates); found {\n\t\t\tpathParams[alias.Wire] = value\n\t\t}\n\t}\n\tfor _, alias := range rule.QueryAliases {\n\t\tcandidates := append([]string{alias.Canonical}, alias.Aliases...)\n\t\tif value, found := resolveStringCompatibilityValue(queryParams, candidates); found {\n\t\t\tqueryParams[alias.Canonical] = value\n\t\t}\n\t}\n\n\treturn &OperationRequest{\n\t\tPathParams: pathParams,\n\t\tQuery:      queryParams,\n\t\tBody:       normalizeCompatibilityBody(req.Body, rule),\n\t\tHeaders:    cloneStringMap(req.Headers),\n\t}\n}\n\nfunc (c *Client) renderPath(pathTemplate string, pathParams map[string]string) (string, error) {\n\tpathValue := pathTemplate\n\tfor {\n\t\tstart := strings.Index(pathValue, "{")\n\t\tif start < 0 {\n\t\t\tbreak\n\t\t}\n\t\tend := strings.Index(pathValue[start:], "}")\n\t\tif end < 0 {\n\t\t\tbreak\n\t\t}\n\t\tend += start\n\t\tkey := strings.TrimSpace(pathValue[start+1 : end])\n\t\tif key == "" {\n\t\t\tbreak\n\t\t}\n\t\tvalue, ok := pathParams[key]\n\t\tif !ok {\n\t\t\treturn "", fmt.Errorf("missing path param: %s", key)\n\t\t}\n\t\tpathValue = strings.ReplaceAll(pathValue, "{"+key+"}", url.PathEscape(value))\n\t}\n\treturn pathValue, nil\n}\n\nfunc (c *Client) request(ctx context.Context, method string, pathTemplate string, req *OperationRequest) (json.RawMessage, error) {\n\tif c.HTTPClient == nil {\n\t\tc.HTTPClient = &http.Client{Timeout: 15 * time.Second}\n\t}\n\tpathValue, err := c.renderPath(pathTemplate, req.PathParams)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\n\tu, err := url.Parse(c.BaseURL + pathValue)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\tquery := u.Query()\n\tfor key, value := range req.Query {\n\t\tif value == "" {\n\t\t\tcontinue\n\t\t}\n\t\tquery.Set(key, value)\n\t}\n\tu.RawQuery = query.Encode()\n\n\tvar bodyReader io.Reader\n\tif req.Body != nil {\n\t\traw, marshalErr := json.Marshal(req.Body)\n\t\tif marshalErr != nil {\n\t\t\treturn nil, marshalErr\n\t\t}\n\t\tbodyReader = bytes.NewReader(raw)\n\t}\n\n\thttpReq, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\thttpReq.Header.Set("content-type", "application/json")\n\tif c.Token != "" {\n\t\thttpReq.Header.Set("authorization", "Bearer "+c.Token)\n\t}\n\tfor key, value := range req.Headers {\n\t\thttpReq.Header.Set(key, value)\n\t}\n\n\tresponse, err := c.HTTPClient.Do(httpReq)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\tdefer response.Body.Close()\n\n\tpayload, err := io.ReadAll(response.Body)\n\tif err != nil {\n\t\treturn nil, err\n\t}\n\n\tif response.StatusCode < 200 || response.StatusCode >= 300 {\n\t\tmessage := fmt.Sprintf("request failed: %d", response.StatusCode)\n\t\treturn nil, &ApiError{\n\t\t\tStatus:  response.StatusCode,\n\t\t\tMessage: message,\n\t\t\tPayload: payload,\n\t\t}\n\t}\n\n\treturn payload, nil\n}\n\n${methods}\n`;

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[2], metadata),
    "go.mod": `module github.com/agentledger/sdk/go\n\ngo 1.24\n`,
    "client.go": clientGo,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildJavaFiles(
  operations: OpenApiOperationSpec[],
  metadata: SdkMetadata
): Record<string, string> {
  const compatibilityCases = operations
    .map(
      (operation) => {
        const rule = buildOperationCompatibilityRule(operation);
        if (!rule) {
          return null;
        }
        const pathAliases = (rule.pathAliases ?? [])
          .map(
            (alias) =>
              `new PathAlias("${alias.canonical}", "${alias.wire}", new String[] { ${alias.aliases.map((item) => `"${item}"`).join(", ")} })`
          )
          .join(", ");
        const queryAliases = (rule.queryAliases ?? [])
          .map(
            (alias) =>
              `new QueryAlias("${alias.canonical}", new String[] { ${alias.aliases.map((item) => `"${item}"`).join(", ")} })`
          )
          .join(", ");
        const bodyAliases = (rule.bodyAliases ?? [])
          .map(
            (alias) =>
              `new BodyAlias("${alias.canonical}", new String[] { ${alias.aliases.map((item) => `"${item}"`).join(", ")} })`
          )
          .join(", ");
        return `      case "${operation.id}":\n        return normalizeOperationRequest(\n          request,\n          new PathAlias[] { ${pathAliases} },\n          new QueryAlias[] { ${queryAliases} },\n          new BodyAlias[] { ${bodyAliases} }\n        );`;
      }
    )
    .filter((item): item is string => Boolean(item))
    .join("\n");
  const methods = operations
    .map(
      (operation) => `  // ${operation.id} | ${operation.method} ${operation.path}\n  public String ${operation.methodNameCamel}(OperationRequest request) throws IOException, InterruptedException {\n    OperationRequest resolved = normalizeCompatibilityRequest("${operation.id}", request == null ? new OperationRequest() : request);\n    return this.request("${operation.method}", "${operation.path}", resolved);\n  }`
    )
    .join("\n\n");

  const javaSource = `package com.agentledger.sdk;\n\nimport java.io.IOException;\nimport java.net.URI;\nimport java.net.URLEncoder;\nimport java.net.http.HttpClient;\nimport java.net.http.HttpRequest;\nimport java.net.http.HttpResponse;\nimport java.nio.charset.StandardCharsets;\nimport java.time.Duration;\nimport java.util.LinkedHashMap;\nimport java.util.Map;\n\npublic class AgentLedgerClient {\n  private record PathAlias(String canonical, String wire, String[] aliases) {}\n\n  private record QueryAlias(String canonical, String[] aliases) {}\n\n  private record BodyAlias(String canonical, String[] aliases) {}\n\n  public static class OperationRequest {\n    public Map<String, String> path = new LinkedHashMap<>();\n    public Map<String, String> query = new LinkedHashMap<>();\n    public String body;\n    public Map<String, String> headers = new LinkedHashMap<>();\n  }\n\n  public static class ApiError extends RuntimeException {\n    public final int status;\n    public final String payload;\n\n    public ApiError(int status, String message, String payload) {\n      super(message);\n      this.status = status;\n      this.payload = payload;\n    }\n  }\n\n  private final String baseUrl;\n  private final String token;\n  private final HttpClient httpClient;\n\n  public AgentLedgerClient(String baseUrl, String token) {\n    this.baseUrl = baseUrl.replaceAll("/+$", "");\n    this.token = token;\n    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();\n  }\n\n  private String resolveCompatibilityValue(Map<String, String> record, String canonical, String[] aliases) {\n    String canonicalValue = record.get(canonical);\n    if (canonicalValue != null && !canonicalValue.isBlank()) {\n      return canonicalValue;\n    }\n    for (String alias : aliases) {\n      String compatibilityValue = record.get(alias);\n      if (compatibilityValue != null && !compatibilityValue.isBlank()) {\n        return compatibilityValue;\n      }\n    }\n    return null;\n  }\n\n  private String normalizeStringBody(String body, BodyAlias[] bodyAliases) {\n    if (body == null || body.isBlank()) {\n      return body;\n    }\n    String normalized = body;\n    for (BodyAlias alias : bodyAliases) {\n      String canonicalToken = "\\\"" + alias.canonical() + "\\\"";\n      if (normalized.contains(canonicalToken)) {\n        continue;\n      }\n      for (String compatibilityName : alias.aliases()) {\n        String compatibilityToken = "\\\"" + compatibilityName + "\\\"";\n        if (normalized.contains(compatibilityToken)) {\n          normalized = normalized.replace(compatibilityToken, canonicalToken);\n          break;\n        }\n      }\n    }\n    return normalized;\n  }\n\n  private OperationRequest normalizeOperationRequest(\n    OperationRequest request,\n    PathAlias[] pathAliases,\n    QueryAlias[] queryAliases,\n    BodyAlias[] bodyAliases\n  ) {\n    OperationRequest normalized = new OperationRequest();\n    normalized.path = request.path == null ? new LinkedHashMap<>() : new LinkedHashMap<>(request.path);\n    normalized.query = request.query == null ? new LinkedHashMap<>() : new LinkedHashMap<>(request.query);\n    normalized.headers = request.headers == null ? new LinkedHashMap<>() : new LinkedHashMap<>(request.headers);\n    normalized.body = request.body;\n\n    for (PathAlias alias : pathAliases) {\n      String value = resolveCompatibilityValue(normalized.path, alias.canonical(), alias.aliases());\n      if (value != null) {\n        normalized.path.put(alias.wire(), value);\n      }\n    }\n    for (QueryAlias alias : queryAliases) {\n      String value = resolveCompatibilityValue(normalized.query, alias.canonical(), alias.aliases());\n      if (value != null) {\n        normalized.query.put(alias.canonical(), value);\n      }\n    }\n    normalized.body = normalizeStringBody(normalized.body, bodyAliases);\n    return normalized;\n  }\n\n  private OperationRequest normalizeCompatibilityRequest(String operationId, OperationRequest request) {\n    switch (operationId) {\n${compatibilityCases}\n      default:\n        return request;\n    }\n  }\n\n  private String renderPath(String template, Map<String, String> pathParams) {\n    String pathValue = template;\n    int start = pathValue.indexOf('{');\n    while (start >= 0) {\n      int end = pathValue.indexOf('}', start);\n      if (end < 0) {\n        break;\n      }\n      String key = pathValue.substring(start + 1, end).trim();\n      String value = pathParams.get(key);\n      if (value == null) {\n        throw new IllegalArgumentException("缺少 path 参数: " + key);\n      }\n      pathValue = pathValue.replace("{" + key + "}", URLEncoder.encode(value, StandardCharsets.UTF_8));\n      start = pathValue.indexOf('{');\n    }\n    return pathValue;\n  }\n\n  private String buildQuery(Map<String, String> query) {\n    if (query == null || query.isEmpty()) {\n      return "";\n    }\n    StringBuilder builder = new StringBuilder();\n    boolean first = true;\n    for (Map.Entry<String, String> entry : query.entrySet()) {\n      if (entry.getValue() == null || entry.getValue().isBlank()) {\n        continue;\n      }\n      if (first) {\n        builder.append('?');\n        first = false;\n      } else {\n        builder.append('&');\n      }\n      builder.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));\n      builder.append('=');\n      builder.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));\n    }\n    return builder.toString();\n  }\n\n  private String request(String method, String pathTemplate, OperationRequest request) throws IOException, InterruptedException {\n    String url = this.baseUrl + renderPath(pathTemplate, request.path) + buildQuery(request.query);\n    HttpRequest.BodyPublisher bodyPublisher = request.body == null\n      ? HttpRequest.BodyPublishers.noBody()\n      : HttpRequest.BodyPublishers.ofString(request.body);\n\n    HttpRequest.Builder builder = HttpRequest.newBuilder()\n      .uri(URI.create(url))\n      .method(method, bodyPublisher)\n      .header("content-type", "application/json");\n\n    if (this.token != null && !this.token.isBlank()) {\n      builder.header("authorization", "Bearer " + this.token);\n    }\n    for (Map.Entry<String, String> header : request.headers.entrySet()) {\n      builder.header(header.getKey(), header.getValue());\n    }\n\n    HttpResponse<String> response = this.httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());\n    if (response.statusCode() < 200 || response.statusCode() >= 300) {\n      throw new ApiError(response.statusCode(), "request failed: " + response.statusCode(), response.body());\n    }\n\n    return response.body();\n  }\n\n${methods}\n}\n`;

  const pomXml = `<?xml version="1.0" encoding="UTF-8"?>\n<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">\n  <modelVersion>4.0.0</modelVersion>\n  <groupId>com.agentledger</groupId>\n  <artifactId>agentledger-sdk-java</artifactId>\n  <version>${metadata.openapi.version}</version>\n  <properties>\n    <maven.compiler.source>17</maven.compiler.source>\n    <maven.compiler.target>17</maven.compiler.target>\n  </properties>\n</project>\n`;

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[3], metadata),
    "pom.xml": pomXml,
    "src/main/java/com/agentledger/sdk/AgentLedgerClient.java": javaSource,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildCSharpFiles(
  operations: OpenApiOperationSpec[],
  metadata: SdkMetadata
): Record<string, string> {
  const compatibilityRules = stringifyWithTrailingNewline(buildOperationCompatibilityRules(operations)).trim();
  const methods = operations
    .map(
      (operation) => `    // ${operation.id} | ${operation.method} ${operation.path}\n    public Task<string> ${operation.methodNamePascal}(OperationRequest? request = null, CancellationToken cancellationToken = default)\n    {\n        request = this.NormalizeCompatibilityRequest("${operation.id}", request ?? new OperationRequest());\n        return this.RequestAsync("${operation.method}", "${operation.path}", request, cancellationToken);\n    }`
    )
    .join("\n\n");

  const source = `using System.Net.Http;\nusing System.Text;\nusing System.Text.Json;\n\nnamespace AgentLedger.Sdk;\n\npublic sealed class OperationCompatibilityRule\n{\n    public List<PathAlias> PathAliases { get; init; } = new();\n    public List<QueryAlias> QueryAliases { get; init; } = new();\n    public List<BodyAlias> BodyAliases { get; init; } = new();\n}\n\npublic sealed class PathAlias\n{\n    public string Canonical { get; init; } = string.Empty;\n    public string Wire { get; init; } = string.Empty;\n    public List<string> Aliases { get; init; } = new();\n}\n\npublic sealed class QueryAlias\n{\n    public string Canonical { get; init; } = string.Empty;\n    public List<string> Aliases { get; init; } = new();\n}\n\npublic sealed class BodyAlias\n{\n    public string Canonical { get; init; } = string.Empty;\n    public List<string> Aliases { get; init; } = new();\n}\n\npublic sealed class OperationRequest\n{\n    public Dictionary<string, string> Path { get; init; } = new();\n    public Dictionary<string, string> Query { get; init; } = new();\n    public object? Body { get; init; }\n    public Dictionary<string, string> Headers { get; init; } = new();\n}\n\npublic sealed class AgentLedgerApiError : Exception\n{\n    public int Status { get; }\n    public string Payload { get; }\n\n    public AgentLedgerApiError(int status, string message, string payload) : base(message)\n    {\n        Status = status;\n        Payload = payload;\n    }\n}\n\npublic sealed class AgentLedgerClient\n{\n    private static readonly Dictionary<string, OperationCompatibilityRule> OperationCompatibility = LoadOperationCompatibility();\n\n    private readonly string _baseUrl;\n    private readonly string? _token;\n    private readonly HttpClient _httpClient;\n\n    public AgentLedgerClient(string baseUrl, string? token = null, HttpClient? httpClient = null)\n    {\n        _baseUrl = baseUrl.TrimEnd('/');\n        _token = token;\n        _httpClient = httpClient ?? new HttpClient { Timeout = TimeSpan.FromSeconds(15) };\n    }\n\n    private static Dictionary<string, OperationCompatibilityRule> LoadOperationCompatibility()\n    {\n        return JsonSerializer.Deserialize<Dictionary<string, OperationCompatibilityRule>>(\n            """\n${compatibilityRules}\n""",\n            new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new Dictionary<string, OperationCompatibilityRule>();\n    }\n\n    private static string? ResolveCompatibilityValue(Dictionary<string, string> record, IEnumerable<string> candidates)\n    {\n        foreach (var candidate in candidates)\n        {\n            if (record.TryGetValue(candidate, out var value) && !string.IsNullOrWhiteSpace(value))\n            {\n                return value;\n            }\n        }\n\n        return null;\n    }\n\n    private static object? ResolveCompatibilityValue(Dictionary<string, object?> record, IEnumerable<string> candidates)\n    {\n        foreach (var candidate in candidates)\n        {\n            if (!record.TryGetValue(candidate, out var value) || value is null)\n            {\n                continue;\n            }\n\n            if (value is string text && string.IsNullOrWhiteSpace(text))\n            {\n                continue;\n            }\n\n            return value;\n        }\n\n        return null;\n    }\n\n    private static object? NormalizeBody(object? body, OperationCompatibilityRule rule)\n    {\n        if (body is null || rule.BodyAliases.Count == 0)\n        {\n            return body;\n        }\n\n        var element = JsonSerializer.SerializeToElement(body);\n        if (element.ValueKind != JsonValueKind.Object)\n        {\n            return body;\n        }\n\n        var normalizedBody = JsonSerializer.Deserialize<Dictionary<string, object?>>(element.GetRawText()) ?? new Dictionary<string, object?>();\n        foreach (var alias in rule.BodyAliases)\n        {\n            var value = ResolveCompatibilityValue(normalizedBody, new[] { alias.Canonical }.Concat(alias.Aliases));\n            if (value is not null)\n            {\n                normalizedBody[alias.Canonical] = value;\n            }\n        }\n\n        return normalizedBody;\n    }\n\n    private OperationRequest NormalizeCompatibilityRequest(string operationId, OperationRequest request)\n    {\n        if (!OperationCompatibility.TryGetValue(operationId, out var rule))\n        {\n            return request;\n        }\n\n        var normalizedPath = new Dictionary<string, string>(request.Path);\n        var normalizedQuery = new Dictionary<string, string>(request.Query);\n        foreach (var alias in rule.PathAliases)\n        {\n            var value = ResolveCompatibilityValue(normalizedPath, new[] { alias.Canonical }.Concat(alias.Aliases));\n            if (!string.IsNullOrWhiteSpace(value))\n            {\n                normalizedPath[alias.Wire] = value;\n            }\n        }\n        foreach (var alias in rule.QueryAliases)\n        {\n            var value = ResolveCompatibilityValue(normalizedQuery, new[] { alias.Canonical }.Concat(alias.Aliases));\n            if (!string.IsNullOrWhiteSpace(value))\n            {\n                normalizedQuery[alias.Canonical] = value;\n            }\n        }\n\n        return new OperationRequest\n        {\n            Path = normalizedPath,\n            Query = normalizedQuery,\n            Body = NormalizeBody(request.Body, rule),\n            Headers = new Dictionary<string, string>(request.Headers),\n        };\n    }\n\n    private string RenderPath(string pathTemplate, Dictionary<string, string> pathParams)\n    {\n        var result = pathTemplate;\n        foreach (var key in pathParams.Keys)\n        {\n            result = result.Replace($"{{{key}}}", Uri.EscapeDataString(pathParams[key]));\n        }\n\n        if (result.Contains('{') || result.Contains('}'))\n        {\n            throw new InvalidOperationException($"缺少 path 参数: {result}");\n        }\n\n        return result;\n    }\n\n    private static string BuildQuery(Dictionary<string, string> query)\n    {\n        if (query.Count == 0)\n        {\n            return string.Empty;\n        }\n\n        var pairs = query\n            .Where(item => !string.IsNullOrWhiteSpace(item.Value))\n            .Select(item => $"{Uri.EscapeDataString(item.Key)}={Uri.EscapeDataString(item.Value)}")\n            .ToArray();\n        return pairs.Length == 0 ? string.Empty : $"?{string.Join("&", pairs)}";\n    }\n\n    private async Task<string> RequestAsync(string method, string pathTemplate, OperationRequest request, CancellationToken cancellationToken)\n    {\n        var url = _baseUrl + RenderPath(pathTemplate, request.Path) + BuildQuery(request.Query);\n        using var httpRequest = new HttpRequestMessage(new HttpMethod(method), url);\n        httpRequest.Headers.TryAddWithoutValidation("content-type", "application/json");\n\n        if (!string.IsNullOrWhiteSpace(_token))\n        {\n            httpRequest.Headers.TryAddWithoutValidation("authorization", $"Bearer {_token}");\n        }\n        foreach (var header in request.Headers)\n        {\n            httpRequest.Headers.TryAddWithoutValidation(header.Key, header.Value);\n        }\n\n        if (request.Body is not null)\n        {\n            var json = JsonSerializer.Serialize(request.Body);\n            httpRequest.Content = new StringContent(json, Encoding.UTF8, "application/json");\n        }\n\n        using var response = await _httpClient.SendAsync(httpRequest, cancellationToken);\n        var payload = await response.Content.ReadAsStringAsync(cancellationToken);\n\n        if (!response.IsSuccessStatusCode)\n        {\n            throw new AgentLedgerApiError((int)response.StatusCode, $"request failed: {(int)response.StatusCode}", payload);\n        }\n\n        return payload;\n    }\n\n${methods}\n}\n`;

  const csproj = `<Project Sdk="Microsoft.NET.Sdk">\n  <PropertyGroup>\n    <TargetFramework>net8.0</TargetFramework>\n    <Nullable>enable</Nullable>\n    <ImplicitUsings>enable</ImplicitUsings>\n  </PropertyGroup>\n</Project>\n`;

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[4], metadata),
    "AgentLedger.Sdk.csproj": csproj,
    "AgentLedgerClient.cs": source,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildPhpFiles(operations: OpenApiOperationSpec[], metadata: SdkMetadata): Record<string, string> {
  const compatibilityRules = stringifyWithTrailingNewline(buildOperationCompatibilityRules(operations)).trim();
  const methods = operations
    .map(
      (operation) => `    // ${operation.id} | ${operation.method} ${operation.path}\n    public function ${operation.methodNameCamel}(array $request = []): mixed\n    {\n        $normalizedRequest = $this->normalizeCompatibilityRequest('${operation.id}', $request);\n        return $this->request('${operation.method}', '${operation.path}', $normalizedRequest);\n    }`
    )
    .join("\n\n");

  const source = `<?php\n\ndeclare(strict_types=1);\n\nnamespace AgentLedger\\Sdk;\n\nfinal class AgentLedgerApiError extends \\RuntimeException\n{\n    public function __construct(public readonly int $status, public readonly mixed $payload)\n    {\n        parent::__construct('request failed: ' . $status);\n    }\n}\n\nfinal class AgentLedgerClient\n{\n    private const OPERATION_COMPATIBILITY_JSON = <<<'JSON'\n${compatibilityRules}\nJSON;\n\n    private ?array $operationCompatibility = null;\n\n    public function __construct(\n        private readonly string $baseUrl,\n        private readonly ?string $token = null,\n        private readonly int $timeoutSeconds = 15\n    ) {\n    }\n\n    private function getOperationCompatibility(): array\n    {\n        if ($this->operationCompatibility !== null) {\n            return $this->operationCompatibility;\n        }\n\n        $decoded = json_decode(self::OPERATION_COMPATIBILITY_JSON, true, 512, JSON_THROW_ON_ERROR);\n        $this->operationCompatibility = is_array($decoded) ? $decoded : [];\n        return $this->operationCompatibility;\n    }\n\n    private function resolveCompatibilityValue(array $record, array $candidates): mixed\n    {\n        foreach ($candidates as $candidate) {\n            if (!array_key_exists($candidate, $record)) {\n                continue;\n            }\n            $value = $record[$candidate];\n            if ($value === null || $value === '') {\n                continue;\n            }\n            return $value;\n        }\n\n        return null;\n    }\n\n    private function normalizeCompatibilityRequest(string $operationId, array $request): array\n    {\n        $rule = $this->getOperationCompatibility()[$operationId] ?? null;\n        if (!is_array($rule)) {\n            return $request;\n        }\n\n        $path = isset($request['path']) && is_array($request['path']) ? $request['path'] : [];\n        $query = isset($request['query']) && is_array($request['query']) ? $request['query'] : [];\n        $headers = isset($request['headers']) && is_array($request['headers']) ? $request['headers'] : [];\n        $body = $request['body'] ?? null;\n\n        foreach (($rule['pathAliases'] ?? []) as $alias) {\n            if (!is_array($alias) || !isset($alias['canonical'], $alias['wire'])) {\n                continue;\n            }\n            $value = $this->resolveCompatibilityValue($path, array_merge([$alias['canonical']], $alias['aliases'] ?? []));\n            if ($value !== null) {\n                $path[(string) $alias['wire']] = $value;\n            }\n        }\n\n        foreach (($rule['queryAliases'] ?? []) as $alias) {\n            if (!is_array($alias) || !isset($alias['canonical'])) {\n                continue;\n            }\n            $value = $this->resolveCompatibilityValue($query, array_merge([$alias['canonical']], $alias['aliases'] ?? []));\n            if ($value !== null) {\n                $query[(string) $alias['canonical']] = $value;\n            }\n        }\n\n        if (is_array($body)) {\n            $normalizedBody = $body;\n            foreach (($rule['bodyAliases'] ?? []) as $alias) {\n                if (!is_array($alias) || !isset($alias['canonical'])) {\n                    continue;\n                }\n                $value = $this->resolveCompatibilityValue($normalizedBody, array_merge([$alias['canonical']], $alias['aliases'] ?? []));\n                if ($value !== null) {\n                    $normalizedBody[(string) $alias['canonical']] = $value;\n                }\n            }\n            $body = $normalizedBody;\n        }\n\n        return [\n            ...$request,\n            'path' => $path,\n            'query' => $query,\n            'body' => $body,\n            'headers' => $headers,\n        ];\n    }\n\n    private function renderPath(string $pathTemplate, array $pathParams): string\n    {\n        $result = $pathTemplate;\n        if (preg_match_all('/\\{([^}]+)\\}/', $pathTemplate, $matches) === false) {\n            return $pathTemplate;\n        }\n        foreach ($matches[1] as $rawKey) {\n            $key = trim((string) $rawKey);\n            if (!array_key_exists($key, $pathParams)) {\n                throw new \\InvalidArgumentException('缺少 path 参数: ' . $key);\n            }\n            $result = str_replace('{' . $key . '}', rawurlencode((string) $pathParams[$key]), $result);\n        }\n        return $result;\n    }\n\n    private function buildQuery(array $query): string\n    {\n        $filtered = [];\n        foreach ($query as $key => $value) {\n            if ($value === null || $value === '') {\n                continue;\n            }\n            $filtered[(string) $key] = (string) $value;\n        }\n        if (count($filtered) === 0) {\n            return '';\n        }\n        return '?' . http_build_query($filtered);\n    }\n\n    private function request(string $method, string $pathTemplate, array $request): mixed\n    {\n        $pathParams = isset($request['path']) && is_array($request['path']) ? $request['path'] : [];\n        $query = isset($request['query']) && is_array($request['query']) ? $request['query'] : [];\n        $headers = isset($request['headers']) && is_array($request['headers']) ? $request['headers'] : [];\n        $body = $request['body'] ?? null;\n\n        $url = rtrim($this->baseUrl, '/') . $this->renderPath($pathTemplate, $pathParams) . $this->buildQuery($query);\n\n        $curl = curl_init($url);\n        if ($curl === false) {\n            throw new \\RuntimeException('curl 初始化失败');\n        }\n\n        $headerLines = ['content-type: application/json'];\n        if ($this->token !== null && $this->token !== '') {\n            $headerLines[] = 'authorization: Bearer ' . $this->token;\n        }\n        foreach ($headers as $key => $value) {\n            $headerLines[] = (string) $key . ': ' . (string) $value;\n        }\n\n        curl_setopt_array($curl, [\n            CURLOPT_RETURNTRANSFER => true,\n            CURLOPT_CUSTOMREQUEST => $method,\n            CURLOPT_TIMEOUT => $this->timeoutSeconds,\n            CURLOPT_HTTPHEADER => $headerLines,\n            CURLOPT_HEADER => true,\n        ]);\n\n        if ($body !== null) {\n            curl_setopt($curl, CURLOPT_POSTFIELDS, json_encode($body, JSON_UNESCAPED_UNICODE));\n        }\n\n        $response = curl_exec($curl);\n        if ($response === false) {\n            $error = curl_error($curl);\n            curl_close($curl);\n            throw new \\RuntimeException($error);\n        }\n\n        $statusCode = curl_getinfo($curl, CURLINFO_RESPONSE_CODE);\n        $headerSize = curl_getinfo($curl, CURLINFO_HEADER_SIZE);\n        $payload = substr((string) $response, (int) $headerSize);\n        curl_close($curl);\n\n        if ($statusCode < 200 || $statusCode >= 300) {\n            throw new AgentLedgerApiError((int) $statusCode, $payload);\n        }\n\n        $decoded = json_decode($payload, true);\n        if (json_last_error() === JSON_ERROR_NONE) {\n            return $decoded;\n        }\n        return $payload;\n    }\n\n${methods}\n}\n`;

  const composerJson = stringifyWithTrailingNewline({
    name: "agentledger/sdk-php",
    version: metadata.openapi.version,
    type: "library",
    autoload: {
      "psr-4": {
        "AgentLedger\\Sdk\\": "src/",
      },
    },
  });

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[5], metadata),
    "composer.json": composerJson,
    "src/AgentLedgerClient.php": source,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildRubyFiles(operations: OpenApiOperationSpec[], metadata: SdkMetadata): Record<string, string> {
  const compatibilityRules = stringifyWithTrailingNewline(buildOperationCompatibilityRules(operations)).trim();
  const methods = operations
    .map(
      (operation) => `    # ${operation.id} | ${operation.method} ${operation.path}\n    def ${operation.methodNameSnake}(request = {})\n      normalized_request = normalize_compatibility_request("${operation.id}", request)\n      request_api("${operation.method}", "${operation.path}", normalized_request)\n    end`
    )
    .join("\n\n");

  const source = `# frozen_string_literal: true\n\nrequire "json"\nrequire "net/http"\nrequire "uri"\nrequire "cgi"\n\nmodule AgentLedgerSdk\n  OPERATION_COMPATIBILITY = JSON.parse(<<~JSON).freeze\n${compatibilityRules}\nJSON\n\n  class ApiError < StandardError\n    attr_reader :status, :payload\n\n    def initialize(status, payload)\n      super("request failed: #{status}")\n      @status = status\n      @payload = payload\n    end\n  end\n\n  class Client\n    def initialize(base_url:, token: nil, timeout_seconds: 15)\n      @base_url = base_url.sub(%r{/+$}, "")\n      @token = token\n      @timeout_seconds = timeout_seconds\n    end\n\n    def resolve_compatibility_value(record, candidates)\n      candidates.each do |candidate|\n        if record.key?(candidate)\n          value = record[candidate]\n        elsif record.key?(candidate.to_sym)\n          value = record[candidate.to_sym]\n        else\n          next\n        end\n\n        next if value.nil?\n        next if value.respond_to?(:empty?) && value.empty?\n\n        return value\n      end\n      nil\n    end\n\n    def normalize_compatibility_request(operation_id, request)\n      rule = OPERATION_COMPATIBILITY[operation_id]\n      return request unless rule\n\n      path_source = request.fetch(:path, request.fetch("path", {}))\n      query_source = request.fetch(:query, request.fetch("query", {}))\n      headers_source = request.fetch(:headers, request.fetch("headers", {}))\n      body = request.key?(:body) ? request[:body] : request["body"]\n\n      path = path_source.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value.to_s }\n      query = query_source.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value.to_s unless value.nil? }\n      headers = headers_source.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value.to_s }\n\n      Array(rule["pathAliases"]).each do |alias_rule|\n        value = resolve_compatibility_value(path, [alias_rule["canonical"], *Array(alias_rule["aliases"])])\n        path[alias_rule["wire"]] = value.to_s unless value.nil?\n      end\n\n      Array(rule["queryAliases"]).each do |alias_rule|\n        value = resolve_compatibility_value(query, [alias_rule["canonical"], *Array(alias_rule["aliases"])])\n        query[alias_rule["canonical"]] = value.to_s unless value.nil?\n      end\n\n      if body.is_a?(Hash)\n        normalized_body = body.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value }\n        Array(rule["bodyAliases"]).each do |alias_rule|\n          value = resolve_compatibility_value(normalized_body, [alias_rule["canonical"], *Array(alias_rule["aliases"])])\n          normalized_body[alias_rule["canonical"]] = value unless value.nil?\n        end\n        body = normalized_body\n      end\n\n      {\n        path: path,\n        query: query,\n        body: body,\n        headers: headers,\n      }\n    end\n\n    def render_path(path_template, path_params)\n      path_value = path_template.dup\n      path_value.scan(/\\{([^}]+)\\}/).flatten.each do |raw_key|\n        key = raw_key.strip\n        raise ArgumentError, "缺少 path 参数: #{key}" unless path_params.key?(key)\n\n        path_value.gsub!("{#{key}}", CGI.escape(path_params[key].to_s))\n      end\n      path_value\n    end\n\n    def build_query(query_hash)\n      filtered = query_hash.each_with_object({}) do |(key, value), memo|\n        next if value.nil?\n\n        text = value.to_s\n        next if text.empty?\n\n        memo[key.to_s] = text\n      end\n      return "" if filtered.empty?\n\n      "?" + URI.encode_www_form(filtered)\n    end\n\n    def request_api(method, path_template, request = {})\n      path_params = request.fetch(:path, request.fetch("path", {}))\n      query = request.fetch(:query, request.fetch("query", {}))\n      body = request.key?(:body) ? request[:body] : request["body"]\n      headers = request.fetch(:headers, request.fetch("headers", {}))\n\n      uri = URI.parse(@base_url + render_path(path_template, path_params) + build_query(query))\n      http = Net::HTTP.new(uri.host, uri.port)\n      http.use_ssl = uri.scheme == "https"\n      http.read_timeout = @timeout_seconds\n\n      klass = Net::HTTP.const_get(method.capitalize)\n      http_request = klass.new(uri.request_uri)\n      http_request["content-type"] = "application/json"\n      http_request["authorization"] = "Bearer #{@token}" if @token && !@token.empty?\n      headers.each { |key, value| http_request[key.to_s] = value.to_s }\n      http_request.body = JSON.generate(body) unless body.nil?\n\n      response = http.request(http_request)\n      payload = response.body.to_s\n\n      unless response.code.to_i.between?(200, 299)\n        raise ApiError.new(response.code.to_i, payload)\n      end\n\n      JSON.parse(payload)\n    rescue JSON::ParserError\n      payload\n    end\n\n${methods}\n  end\nend\n`;

  const gemspec = `Gem::Specification.new do |spec|\n  spec.name        = "agentledger-sdk-ruby"\n  spec.version     = "${metadata.openapi.version}"\n  spec.summary     = "AgentLedger Ruby SDK"\n  spec.files       = Dir["lib/**/*.rb"]\n  spec.require_paths = ["lib"]\nend\n`;

  const entry = `# frozen_string_literal: true\n\nrequire_relative "agentledger_sdk/client"\n`;

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[6], metadata),
    "agentledger-sdk.gemspec": gemspec,
    "lib/agentledger_sdk/client.rb": source,
    "lib/agentledger_sdk.rb": entry,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildSwiftFiles(operations: OpenApiOperationSpec[], metadata: SdkMetadata): Record<string, string> {
  const compatibilityRules = stringifyWithTrailingNewline(buildOperationCompatibilityRules(operations)).trim();
  const methods = operations
    .map(
      (operation) => `    // ${operation.id} | ${operation.method} ${operation.path}\n    public func ${operation.methodNameCamel}(request: OperationRequest = OperationRequest()) async throws -> Any {\n        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "${operation.id}", request: request)\n        return try await self.request(method: "${operation.method}", pathTemplate: "${operation.path}", request: normalizedRequest)\n    }`
    )
    .join("\n\n");

  const source = `import Foundation\n\nprivate struct OperationPathAlias: Decodable {\n    let canonical: String\n    let wire: String\n    let aliases: [String]\n}\n\nprivate struct OperationQueryAlias: Decodable {\n    let canonical: String\n    let aliases: [String]\n}\n\nprivate struct OperationBodyAlias: Decodable {\n    let canonical: String\n    let aliases: [String]\n}\n\nprivate struct OperationCompatibilityRule: Decodable {\n    let pathAliases: [OperationPathAlias]?\n    let queryAliases: [OperationQueryAlias]?\n    let bodyAliases: [OperationBodyAlias]?\n}\n\nprivate let operationCompatibility: [String: OperationCompatibilityRule] = {\n    guard let data = #"""\n${compatibilityRules}\n"""#.data(using: .utf8) else {\n        return [:]\n    }\n    return (try? JSONDecoder().decode([String: OperationCompatibilityRule].self, from: data)) ?? [:]\n}()\n\npublic struct OperationRequest {\n    public var path: [String: String] = [:]\n    public var query: [String: String] = [:]\n    public var body: Any? = nil\n    public var headers: [String: String] = [:]\n\n    public init(path: [String: String] = [:], query: [String: String] = [:], body: Any? = nil, headers: [String: String] = [:]) {\n        self.path = path\n        self.query = query\n        self.body = body\n        self.headers = headers\n    }\n}\n\npublic struct AgentLedgerApiError: Error {\n    public let status: Int\n    public let payload: String\n}\n\npublic final class AgentLedgerClient {\n    private let baseUrl: String\n    private let token: String?\n    private let session: URLSession\n\n    public init(baseUrl: String, token: String? = nil, session: URLSession = .shared) {\n        self.baseUrl = baseUrl.replacingOccurrences(of: "/+$", with: "", options: .regularExpression)\n        self.token = token\n        self.session = session\n    }\n\n    private func resolveCompatibilityValue(record: [String: String], candidates: [String]) -> String? {\n        for candidate in candidates {\n            if let value = record[candidate], !value.isEmpty {\n                return value\n            }\n        }\n        return nil\n    }\n\n    private func resolveCompatibilityValue(record: [String: Any], candidates: [String]) -> Any? {\n        for candidate in candidates {\n            guard let value = record[candidate] else {\n                continue\n            }\n            if let text = value as? String, text.isEmpty {\n                continue\n            }\n            return value\n        }\n        return nil\n    }\n\n    private func normalizeCompatibilityRequest(operationId: String, request: OperationRequest) -> OperationRequest {\n        guard let rule = operationCompatibility[operationId] else {\n            return request\n        }\n\n        var path = request.path\n        var query = request.query\n        var body = request.body\n\n        for alias in rule.pathAliases ?? [] {\n            if let value = resolveCompatibilityValue(record: path, candidates: [alias.canonical] + alias.aliases) {\n                path[alias.wire] = value\n            }\n        }\n\n        for alias in rule.queryAliases ?? [] {\n            if let value = resolveCompatibilityValue(record: query, candidates: [alias.canonical] + alias.aliases) {\n                query[alias.canonical] = value\n            }\n        }\n\n        if var normalizedBody = body as? [String: Any] {\n            for alias in rule.bodyAliases ?? [] {\n                if let value = resolveCompatibilityValue(record: normalizedBody, candidates: [alias.canonical] + alias.aliases) {\n                    normalizedBody[alias.canonical] = value\n                }\n            }\n            body = normalizedBody\n        }\n\n        return OperationRequest(path: path, query: query, body: body, headers: request.headers)\n    }\n\n    private func renderPath(pathTemplate: String, pathParams: [String: String]) throws -> String {\n        var result = pathTemplate\n        let regex = try NSRegularExpression(pattern: "\\\\{([^}]+)\\\\}")\n        let matches = regex.matches(in: pathTemplate, range: NSRange(pathTemplate.startIndex..., in: pathTemplate))\n        for match in matches.reversed() {\n            guard let keyRange = Range(match.range(at: 1), in: pathTemplate) else { continue }\n            let key = String(pathTemplate[keyRange]).trimmingCharacters(in: .whitespaces)\n            guard let value = pathParams[key] else {\n                throw NSError(domain: "AgentLedgerSDK", code: 1, userInfo: [NSLocalizedDescriptionKey: "缺少 path 参数: \\(key)"])\n            }\n            guard let fullRange = Range(match.range(at: 0), in: result) else { continue }\n            result.replaceSubrange(fullRange, with: value.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? value)\n        }\n        return result\n    }\n\n    private func request(method: String, pathTemplate: String, request: OperationRequest) async throws -> Any {\n        let renderedPath = try renderPath(pathTemplate: pathTemplate, pathParams: request.path)\n        guard var components = URLComponents(string: self.baseUrl + renderedPath) else {\n            throw NSError(domain: "AgentLedgerSDK", code: 2, userInfo: [NSLocalizedDescriptionKey: "URL 非法"])\n        }\n        if !request.query.isEmpty {\n            components.queryItems = request.query.map { URLQueryItem(name: $0.key, value: $0.value) }\n        }\n\n        guard let url = components.url else {\n            throw NSError(domain: "AgentLedgerSDK", code: 3, userInfo: [NSLocalizedDescriptionKey: "URL 非法"])\n        }\n\n        var urlRequest = URLRequest(url: url)\n        urlRequest.httpMethod = method\n        urlRequest.setValue("application/json", forHTTPHeaderField: "content-type")\n        if let token = self.token, !token.isEmpty {\n            urlRequest.setValue("Bearer \\(token)", forHTTPHeaderField: "authorization")\n        }\n        for (key, value) in request.headers {\n            urlRequest.setValue(value, forHTTPHeaderField: key)\n        }\n\n        if let body = request.body, JSONSerialization.isValidJSONObject(body) {\n            urlRequest.httpBody = try JSONSerialization.data(withJSONObject: body, options: [])\n        }\n\n        let (data, response) = try await self.session.data(for: urlRequest)\n        guard let http = response as? HTTPURLResponse else {\n            throw NSError(domain: "AgentLedgerSDK", code: 4, userInfo: [NSLocalizedDescriptionKey: "响应类型非法"])\n        }\n\n        let payloadText = String(data: data, encoding: .utf8) ?? ""\n        guard (200...299).contains(http.statusCode) else {\n            throw AgentLedgerApiError(status: http.statusCode, payload: payloadText)\n        }\n\n        if payloadText.isEmpty {\n            return [:]\n        }\n\n        if let object = try? JSONSerialization.jsonObject(with: data, options: []) {\n            return object\n        }\n\n        return payloadText\n    }\n\n${methods}\n}\n`;

  const packageSwift = `// swift-tools-version: 5.9\nimport PackageDescription\n\nlet package = Package(\n    name: "AgentLedgerSDK",\n    platforms: [.macOS(.v13), .iOS(.v16)],\n    products: [\n        .library(name: "AgentLedgerSDK", targets: ["AgentLedgerSDK"])\n    ],\n    targets: [\n        .target(name: "AgentLedgerSDK", path: "Sources/AgentLedgerSDK")\n    ]\n)\n`;

  return {
    "README.md": buildLanguageReadme(SDK_LANGUAGES[7], metadata),
    "Package.swift": packageSwift,
    "Sources/AgentLedgerSDK/AgentLedgerClient.swift": source,
    [OPERATIONS_FILE]: buildOperationsJson(operations),
  };
}

function buildLanguageFiles(
  languageId: SdkLanguageId,
  operations: OpenApiOperationSpec[],
  metadata: SdkMetadata
): Record<string, string> {
  switch (languageId) {
    case "typescript":
      return buildTypescriptFiles(operations, metadata);
    case "python":
      return buildPythonFiles(operations, metadata);
    case "go":
      return buildGoFiles(operations, metadata);
    case "java":
      return buildJavaFiles(operations, metadata);
    case "csharp":
      return buildCSharpFiles(operations, metadata);
    case "php":
      return buildPhpFiles(operations, metadata);
    case "ruby":
      return buildRubyFiles(operations, metadata);
    case "swift":
      return buildSwiftFiles(operations, metadata);
    default:
      return {
        "README.md": "# SDK\n",
      };
  }
}

async function readTextIfExists(filePath: string): Promise<string | null> {
  try {
    return await readFile(filePath, "utf8");
  } catch (error) {
    if (resolveErrorCode(error) === "ENOENT") {
      return null;
    }
    throw error;
  }
}

async function ensureDirectoryExists(
  directoryPath: string,
  checkMode: boolean,
  issues: string[]
): Promise<void> {
  try {
    const current = await stat(directoryPath);
    if (!current.isDirectory()) {
      issues.push(`路径不是目录：${toRepoRelativePath(repoRoot, directoryPath)}`);
    }
    return;
  } catch (error) {
    if (resolveErrorCode(error) !== "ENOENT") {
      throw error;
    }
  }

  if (checkMode) {
    issues.push(`缺少目录：${toRepoRelativePath(repoRoot, directoryPath)}`);
    return;
  }
  await mkdir(directoryPath, { recursive: true });
}

async function ensureFileContent(
  filePath: string,
  expectedContent: string,
  checkMode: boolean,
  issues: string[]
): Promise<void> {
  const current = await readTextIfExists(filePath);
  if (current === expectedContent) {
    return;
  }

  if (checkMode) {
    if (current === null) {
      issues.push(`缺少文件：${toRepoRelativePath(repoRoot, filePath)}`);
    } else {
      issues.push(`文件内容不一致：${toRepoRelativePath(repoRoot, filePath)}`);
    }
    return;
  }

  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, expectedContent, "utf8");
}

async function listFilesRecursive(directoryPath: string): Promise<string[]> {
  const entries = await readdir(directoryPath, { withFileTypes: true });
  const files: string[] = [];

  for (const entry of entries) {
    const absolutePath = path.join(directoryPath, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listFilesRecursive(absolutePath)));
      continue;
    }
    if (!entry.isFile()) {
      continue;
    }
    files.push(absolutePath);
  }

  return files;
}

function shouldIgnoreStaleFile(absolutePath: string): boolean {
  const baseName = path.basename(absolutePath);
  return baseName === ".gitkeep" || baseName === "SHA256SUMS.txt";
}

async function reconcileStaleFiles(
  outputDir: string,
  expectedFiles: Set<string>,
  checkMode: boolean,
  issues: string[]
): Promise<void> {
  let directoryInfo: Awaited<ReturnType<typeof stat>>;
  try {
    directoryInfo = await stat(outputDir);
  } catch (error) {
    if (resolveErrorCode(error) === "ENOENT") {
      return;
    }
    throw error;
  }

  if (!directoryInfo.isDirectory()) {
    return;
  }

  const existingFiles = await listFilesRecursive(outputDir);
  for (const filePath of existingFiles) {
    const normalizedPath = path.resolve(filePath);
    if (expectedFiles.has(normalizedPath) || shouldIgnoreStaleFile(normalizedPath)) {
      continue;
    }

    if (checkMode) {
      issues.push(`存在陈旧文件：${toRepoRelativePath(repoRoot, normalizedPath)}`);
      continue;
    }

    await rm(normalizedPath, { force: true });
  }
}

export async function runSdkGenerateCli(argv: string[]): Promise<number> {
  const parsed = parseCliOptions(argv);
  if (!parsed.success) {
    console.error(`参数错误：${parsed.error}`);
    printUsage();
    return 1;
  }

  const { options } = parsed;
  if (options.help) {
    printUsage();
    return 0;
  }

  let openapi: OpenApiLoadResult;
  try {
    openapi = await loadOpenApiDocument(options.inputPath);
  } catch (error) {
    console.error("加载 OpenAPI 文档失败。", error);
    return 1;
  }

  const operations = extractOpenApiOperations(openapi.document);
  if (operations.length === 0) {
    console.error("未在 OpenAPI 文档中解析到任何 operation。请检查 paths 定义。");
    return 1;
  }

  const metadata = buildMetadata(openapi.document, openapi.sourceLabel, operations);
  const outputDir = path.resolve(repoRoot, options.outputDir);
  const issues: string[] = [];

  await ensureDirectoryExists(outputDir, options.check, issues);

  const rootOperationsContent = buildOperationsJson(operations);
  const expectedFiles = new Set<string>();
  const rootOperationsPath = path.join(outputDir, OPERATIONS_FILE);
  expectedFiles.add(path.resolve(rootOperationsPath));
  await ensureFileContent(
    rootOperationsPath,
    rootOperationsContent,
    options.check,
    issues
  );

  for (const language of SDK_LANGUAGES) {
    const languageDir = path.join(outputDir, language.id);
    await ensureDirectoryExists(languageDir, options.check, issues);

    const files = buildLanguageFiles(language.id, operations, metadata);
    const generatedFiles = Object.keys(files).sort((left, right) => left.localeCompare(right));

    for (const relativePath of generatedFiles) {
      const absolutePath = path.join(languageDir, relativePath);
      expectedFiles.add(path.resolve(absolutePath));
      await ensureFileContent(
        absolutePath,
        files[relativePath],
        options.check,
        issues
      );
    }

    const languageItem = metadata.languages.find((item) => item.id === language.id);
    if (languageItem) {
      languageItem.generatedFiles = generatedFiles;
    }
  }

  await ensureFileContent(
    path.join(outputDir, METADATA_FILE),
    stringifyWithTrailingNewline(metadata),
    options.check,
    issues
  );
  expectedFiles.add(path.resolve(path.join(outputDir, METADATA_FILE)));
  await reconcileStaleFiles(outputDir, expectedFiles, options.check, issues);

  if (options.check) {
    if (issues.length > 0) {
      console.error("SDK 目录校验失败：");
      for (const issue of issues) {
        console.error(`- ${issue}`);
      }
      return 1;
    }
    console.log(`SDK 校验通过：${toRepoRelativePath(repoRoot, outputDir)}`);
    return 0;
  }

  console.log(`SDK 目录已生成：${toRepoRelativePath(repoRoot, outputDir)}`);
  console.log(`语言数量：${SDK_LANGUAGES.length}`);
  console.log(`operation 数量：${operations.length}`);
  return 0;
}

if (import.meta.main) {
  const exitCode = await runSdkGenerateCli(Bun.argv.slice(2));
  if (exitCode !== 0) {
    process.exit(exitCode);
  }
}
