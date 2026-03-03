import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const matrixDocPath = path.join(repoRoot, "docs/09-主流AI客户端支持矩阵.md");
const connectorDeclPath = path.join(repoRoot, "services/puller/connectors.go");
const parserDeclPath = path.join(repoRoot, "services/puller/parser.go");
const collectDeclPath = path.join(repoRoot, "clients/agent/collect.go");
const contractsValidatorPath = path.join(repoRoot, "packages/contracts/src/validators.ts");

type ConnectorRule = {
  pattern: RegExp;
  connector: string;
};

const p0p1ConnectorRules: ConnectorRule[] = [
  { pattern: /\bcodex\b/i, connector: "codex" },
  { pattern: /\bclaude\b/i, connector: "claude" },
  { pattern: /\bgemini\b/i, connector: "gemini" },
  { pattern: /\baider\b/i, connector: "aider" },
  { pattern: /\bopencode\b/i, connector: "opencode" },
  { pattern: /\bqwen\b/i, connector: "qwen-code" },
  { pattern: /\bkimi\b/i, connector: "kimi-cli" },
  { pattern: /\bcursor\b/i, connector: "cursor" },
  { pattern: /\bwindsurf\b/i, connector: "windsurf" },
  { pattern: /\bcodebuddy\s+code\s+cli\b/i, connector: "codebuddy-cli" },
  { pattern: /\bcodebuddy\s+ide\b/i, connector: "codebuddy-ide" },
  { pattern: /通义灵码|lingma/i, connector: "lingma" },
  { pattern: /\bvs\s*code\b/i, connector: "vscode" },
  { pattern: /\btrae\s+cli\b/i, connector: "trae-cli" },
  { pattern: /\btrae\b/i, connector: "trae-ide" },
  { pattern: /\bzed\b/i, connector: "zed" },
];

type ParserEntrypointCheckResult = {
  entrypoints: string[];
  declaredParsers: string[];
  missingEntrypoints: string[];
  hasParseWithConnectorDispatch: boolean;
  hasParseWithConnectorFallback: boolean;
};

function splitMarkdownRow(row: string): string[] {
  const trimmed = row.trim();
  const withoutEdgePipes = trimmed.replace(/^\|/, "").replace(/\|$/, "");
  return withoutEdgePipes.split("|").map((cell) => cell.trim());
}

function normalizeMarkdown(markdown: string): string {
  return markdown.replace(/^\uFEFF/, "").replace(/\r\n/g, "\n").replace(/\r/g, "\n");
}

function isTableSeparatorRow(row: string): boolean {
  const cells = splitMarkdownRow(row);
  return (
    cells.length > 0 &&
    cells.every((cell) => {
      const token = cell.trim();
      return /^:?-{3,}:?$/.test(token);
    })
  );
}

function normalizePriorityCell(value: string): string {
  return value.replace(/[`*_~\s]/g, "").toUpperCase();
}

export function parseClientsByPriorities(markdown: string, priorities: string[]): string[] {
  const normalizedPriorities = new Set(priorities.map((priority) => priority.toUpperCase()));
  const lines = normalizeMarkdown(markdown)
    .split("\n")
    .map((line) => line.trimEnd());
  const clients: string[] = [];
  let index = 0;

  while (index < lines.length) {
    const headerRow = lines[index]?.trim();
    if (!headerRow || !headerRow.startsWith("|")) {
      index += 1;
      continue;
    }

    const separatorRow = lines[index + 1]?.trim();
    if (!separatorRow || !separatorRow.startsWith("|") || !isTableSeparatorRow(separatorRow)) {
      index += 1;
      continue;
    }

    const header = splitMarkdownRow(headerRow);
    const clientIdx = header.findIndex((cell) => cell.includes("客户端"));
    const priorityIdx = header.findIndex((cell) => cell.includes("优先级"));
    if (clientIdx < 0 || priorityIdx < 0) {
      index += 1;
      continue;
    }

    index += 2;
    while (index < lines.length) {
      const row = lines[index]?.trim();
      if (!row || !row.startsWith("|")) {
        break;
      }
      if (isTableSeparatorRow(row)) {
        index += 1;
        continue;
      }

      const cells = splitMarkdownRow(row);
      if (cells.length <= Math.max(clientIdx, priorityIdx)) {
        index += 1;
        continue;
      }

      const priority = normalizePriorityCell(cells[priorityIdx]);
      if (!normalizedPriorities.has(priority)) {
        index += 1;
        continue;
      }

      const client = cells[clientIdx];
      if (client) {
        clients.push(client);
      }
      index += 1;
    }
  }

  return [...new Set(clients)];
}

export function parseP0Clients(markdown: string): string[] {
  return parseClientsByPriorities(markdown, ["P0"]);
}

export function parseP0P1Clients(markdown: string): string[] {
  return parseClientsByPriorities(markdown, ["P0", "P1"]);
}

function parseConnectorNameConstMap(source: string): Map<string, string> {
  const constMap = new Map<string, string>();
  const constRegex = /^\s*(connectorName[A-Za-z0-9_]+)\s*=\s*"([^"]+)"/gm;
  let match: RegExpExecArray | null;
  while ((match = constRegex.exec(source)) !== null) {
    constMap.set(match[1], match[2].trim().toLowerCase());
  }
  return constMap;
}

function sliceDefaultRegistryCall(source: string): string {
  const marker = "var defaultPullerConnectorRegistry = newConnectorRegistry(";
  const start = source.indexOf(marker);
  if (start < 0) {
    return source;
  }

  const callStart = source.indexOf("newConnectorRegistry(", start);
  if (callStart < 0) {
    return source;
  }

  let depth = 0;
  for (let i = callStart; i < source.length; i++) {
    const char = source[i];
    if (char === "(") {
      depth += 1;
      continue;
    }
    if (char !== ")") {
      continue;
    }
    depth -= 1;
    if (depth === 0) {
      return source.slice(callStart, i + 1);
    }
  }
  return source;
}

export function parseDeclaredPullerConnectors(source: string): string[] {
  const constMap = parseConnectorNameConstMap(source);
  const registrySource = sliceDefaultRegistryCall(source);
  const connectorSet = new Set<string>();

  const callRegex = /newFeatureConnector(?:WithParser)?\(\s*([^,]+?)\s*,/g;
  let match: RegExpExecArray | null;
  while ((match = callRegex.exec(registrySource)) !== null) {
    const raw = match[1].trim();
    const unquoted = raw.replace(/^"/, "").replace(/"$/, "").toLowerCase();
    const resolved = constMap.get(raw) ?? unquoted;
    if (resolved) {
      connectorSet.add(resolved);
    }
  }

  return [...connectorSet].sort((a, b) => a.localeCompare(b));
}

export function parseDeclaredCollectTools(source: string): string[] {
  const toolSet = new Set<string>();
  const toolRegex = /^\s*collectTool[A-Za-z0-9_]+\s*=\s*"([^"]+)"/gm;
  let match: RegExpExecArray | null;
  while ((match = toolRegex.exec(source)) !== null) {
    const value = match[1].trim().toLowerCase();
    if (!value || value === "auto") {
      continue;
    }
    toolSet.add(value);
  }
  return [...toolSet].sort((a, b) => a.localeCompare(b));
}

export function parseCollectDefaultDirs(source: string): string[] {
  const dirSet = new Set<string>();
  const dirRegex = /^\s*defaultCollect[A-Za-z0-9_]+Dir\s*=\s*"([^"]+)"/gm;
  let match: RegExpExecArray | null;
  while ((match = dirRegex.exec(source)) !== null) {
    const value = match[1].trim();
    if (value) {
      dirSet.add(value);
    }
  }
  return [...dirSet].sort((a, b) => a.localeCompare(b));
}

export function parseLocalSourceWhitelist(source: string): string[] {
  const match = /const\s+LOCAL_SOURCE_WHITELIST\s*=\s*\[([\s\S]*?)\];?/.exec(source);
  if (!match) {
    return [];
  }
  const content = match[1];
  const itemRegex = /"([^"]+)"/g;
  const result: string[] = [];
  let item: RegExpExecArray | null;
  while ((item = itemRegex.exec(content)) !== null) {
    const value = item[1].trim();
    if (value) {
      result.push(value);
    }
  }
  return [...new Set(result)];
}

function parseDeclaredConnectorParsers(source: string): string[] {
  const parserSet = new Set<string>();
  const parserRegex =
    /newFeatureConnectorWithParser\(\s*[^,]+?\s*,\s*\[\]string\s*\{[\s\S]*?\}\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)/g;
  let match: RegExpExecArray | null;
  while ((match = parserRegex.exec(source)) !== null) {
    const parserName = match[1].trim();
    if (parserName && parserName !== "nil") {
      parserSet.add(parserName);
    }
  }
  return [...parserSet].sort((a, b) => a.localeCompare(b));
}

export function mapClientToConnector(client: string): string | null {
  for (const rule of p0p1ConnectorRules) {
    if (rule.pattern.test(client)) {
      return rule.connector;
    }
  }
  return null;
}

function sliceFunctionBlock(source: string, signaturePattern: RegExp): string | null {
  const match = signaturePattern.exec(source);
  if (!match || match.index === undefined) {
    return null;
  }

  const openBraceIndex = source.indexOf("{", match.index + match[0].length);
  if (openBraceIndex < 0) {
    return null;
  }

  let depth = 0;
  for (let index = openBraceIndex; index < source.length; index += 1) {
    const char = source[index];
    if (char === "{") {
      depth += 1;
      continue;
    }
    if (char !== "}") {
      continue;
    }

    depth -= 1;
    if (depth === 0) {
      return source.slice(openBraceIndex + 1, index);
    }
  }

  return null;
}

function hasFunctionDeclaration(source: string, fnName: string): boolean {
  const pattern = new RegExp(`\\bfunc\\s+${fnName}\\s*\\(`);
  return pattern.test(source);
}

export function parseFeatureConnectorParserEntrypoints(connectorSource: string): string[] {
  const parseBody = sliceFunctionBlock(
    connectorSource,
    /func\s+\(c\s+\*featureConnector\)\s+Parse\s*\(/,
  );
  if (!parseBody) {
    return [];
  }

  const entrypointSet = new Set<string>();
  const callRegex = /(?<!\.)\b([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*ctx\s*,\s*input\s*\)/g;
  let match: RegExpExecArray | null;
  while ((match = callRegex.exec(parseBody)) !== null) {
    entrypointSet.add(match[1]);
  }

  return [...entrypointSet].sort((a, b) => a.localeCompare(b));
}

export function validateParserEntrypoints(
  connectorSource: string,
  parserSource: string,
): ParserEntrypointCheckResult {
  const entrypoints = parseFeatureConnectorParserEntrypoints(connectorSource);
  const declaredParsers = parseDeclaredConnectorParsers(connectorSource);
  const expectedEntrypoints = [...new Set([...entrypoints, ...declaredParsers])].sort((a, b) =>
    a.localeCompare(b),
  );
  const parseWithConnectorBody = sliceFunctionBlock(connectorSource, /func\s+parseWithConnector\s*\(/);
  const hasParseWithConnectorDispatch =
    parseWithConnectorBody !== null &&
    /connector\.Parse\s*\(\s*ctx\s*,\s*input\s*\)/.test(parseWithConnectorBody);
  const hasParseWithConnectorFallback =
    parseWithConnectorBody !== null &&
    /parseLinesConcurrently\s*\(\s*ctx\s*,\s*input\s*\)/.test(parseWithConnectorBody);
  const missingEntrypoints = expectedEntrypoints.filter(
    (entrypoint) =>
      !hasFunctionDeclaration(connectorSource, entrypoint) &&
      !hasFunctionDeclaration(parserSource, entrypoint),
  );

  return {
    entrypoints,
    declaredParsers,
    missingEntrypoints,
    hasParseWithConnectorDispatch,
    hasParseWithConnectorFallback,
  };
}

function fail(message: string): never {
  console.error(`[support-matrix] ${message}`);
  process.exit(1);
}

function main(): void {
  const matrixDoc = readFileSync(matrixDocPath, "utf8");
  const connectorDecl = readFileSync(connectorDeclPath, "utf8");
  const parserDecl = readFileSync(parserDeclPath, "utf8");
  const collectDecl = readFileSync(collectDeclPath, "utf8");
  const contractsValidator = readFileSync(contractsValidatorPath, "utf8");

  const p0p1Clients = parseP0P1Clients(matrixDoc);
  if (p0p1Clients.length === 0) {
    fail(`no P0/P1 clients found in ${path.relative(repoRoot, matrixDocPath)}`);
  }

  const requiredByConnector = new Map<string, string[]>();
  const unmappedClients: string[] = [];
  for (const client of p0p1Clients) {
    const connector = mapClientToConnector(client);
    if (!connector) {
      unmappedClients.push(client);
      continue;
    }
    const clients = requiredByConnector.get(connector) ?? [];
    clients.push(client);
    requiredByConnector.set(connector, clients);
  }

  if (unmappedClients.length > 0) {
    fail(
      `unable to map P0/P1 clients to connector keys: ${unmappedClients
        .sort((a, b) => a.localeCompare(b))
        .join(", ")}`,
    );
  }

  const declaredConnectors = parseDeclaredPullerConnectors(connectorDecl);
  const declaredSet = new Set(declaredConnectors);
  const requiredConnectors = [...requiredByConnector.keys()].sort((a, b) => a.localeCompare(b));
  const requiredSet = new Set(requiredConnectors);
  const declaredCollectTools = parseDeclaredCollectTools(collectDecl);
  const declaredCollectToolSet = new Set(declaredCollectTools);
  const collectDefaultDirs = parseCollectDefaultDirs(collectDecl);
  const localSourceWhitelist = parseLocalSourceWhitelist(contractsValidator);
  const whitelistSet = new Set(localSourceWhitelist);

  const missing = requiredConnectors.filter((connector) => !declaredSet.has(connector));
  const extra = declaredConnectors.filter((connector) => !requiredSet.has(connector));
  const missingCollectTools = requiredConnectors.filter(
    (connector) => !declaredCollectToolSet.has(connector),
  );
  const extraCollectTools = declaredCollectTools.filter((tool) => !requiredSet.has(tool));
  const missingWhitelistEntries = collectDefaultDirs.filter((dir) => !whitelistSet.has(dir));

  const parserCheck = validateParserEntrypoints(connectorDecl, parserDecl);

  console.log(
    `[support-matrix] P0/P1 clients in docs/09: ${p0p1Clients.length}, required connectors: ${requiredConnectors.length}`,
  );
  console.log(`[support-matrix] Declared puller connectors: ${declaredConnectors.join(", ") || "(none)"}`);
  console.log(
    `[support-matrix] Declared agent collect tools: ${declaredCollectTools.join(", ") || "(none)"}`,
  );
  console.log(
    `[support-matrix] Agent collect default dirs: ${collectDefaultDirs.length}, local whitelist dirs: ${localSourceWhitelist.length}`,
  );
  console.log(
    `[support-matrix] featureConnector parser entrypoints: ${parserCheck.entrypoints.join(", ") || "(none)"}`,
  );
  console.log(
    `[support-matrix] connector parser handlers: ${parserCheck.declaredParsers.join(", ") || "(none)"}`,
  );

  let hasFailure = false;
  if (missing.length > 0) {
    hasFailure = true;
    console.error("[support-matrix] FAILED: missing puller connectors for docs/09 P0/P1 clients:");
    for (const connector of missing) {
      const clients = requiredByConnector.get(connector) ?? [];
      console.error(`  - ${connector} <- ${clients.join(" | ")}`);
    }
  }

  if (extra.length > 0) {
    hasFailure = true;
    console.error("[support-matrix] FAILED: redundant puller connectors not referenced by docs/09 P0/P1:");
    for (const connector of extra) {
      console.error(`  - ${connector}`);
    }
  }

  if (missingCollectTools.length > 0) {
    hasFailure = true;
    console.error("[support-matrix] FAILED: missing collect tools for docs/09 P0/P1 clients:");
    for (const tool of missingCollectTools) {
      const clients = requiredByConnector.get(tool) ?? [];
      console.error(`  - ${tool} <- ${clients.join(" | ")}`);
    }
  }

  if (extraCollectTools.length > 0) {
    hasFailure = true;
    console.error("[support-matrix] FAILED: redundant collect tools not referenced by docs/09 P0/P1:");
    for (const tool of extraCollectTools) {
      console.error(`  - ${tool}`);
    }
  }

  if (collectDefaultDirs.length < declaredCollectTools.length) {
    hasFailure = true;
    console.error(
      `[support-matrix] FAILED: collect 默认目录定义数量不足（dirs=${collectDefaultDirs.length}, tools=${declaredCollectTools.length}）。`,
    );
  }

  if (missingWhitelistEntries.length > 0) {
    hasFailure = true;
    console.error(
      `[support-matrix] FAILED: packages/contracts local 白名单缺少 collect 默认目录: ${missingWhitelistEntries.join(", ")}`,
    );
  }

  if (parserCheck.entrypoints.length === 0) {
    hasFailure = true;
    console.error(
      "[support-matrix] FAILED: featureConnector.Parse 未声明 parser 入口调用（例如 parseLinesConcurrently(ctx, input)）。",
    );
  }

  if (!parserCheck.hasParseWithConnectorDispatch) {
    hasFailure = true;
    console.error("[support-matrix] FAILED: parseWithConnector 缺少 connector.Parse(ctx, input) 调度入口。");
  }

  if (!parserCheck.hasParseWithConnectorFallback) {
    hasFailure = true;
    console.error(
      "[support-matrix] FAILED: parseWithConnector 缺少 parseLinesConcurrently(ctx, input) 回退入口。",
    );
  }

  if (parserCheck.missingEntrypoints.length > 0) {
    hasFailure = true;
    console.error(
      `[support-matrix] FAILED: parser 入口函数不存在: ${parserCheck.missingEntrypoints.join(", ")}`,
    );
  }

  if (hasFailure) {
    process.exit(1);
  }

  console.log(
    "[support-matrix] PASSED: docs/09 P0/P1、puller connectors、agent collect、local 白名单与 parser 入口均严格一致。",
  );
}

const isMainModule = (() => {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }
  return path.resolve(entry) === fileURLToPath(import.meta.url);
})();

if (isMainModule) {
  main();
}
