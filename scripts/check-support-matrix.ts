import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const matrixDocPath = path.join(repoRoot, "docs/09-主流AI客户端支持矩阵.md");
const connectorDeclPath = path.join(repoRoot, "services/puller/connectors.go");

type ConnectorRule = {
  pattern: RegExp;
  connector: string;
};

const p0ConnectorRules: ConnectorRule[] = [
  { pattern: /\bcodex\b/i, connector: "codex" },
  { pattern: /\bclaude\b/i, connector: "claude" },
  { pattern: /\bgemini\b/i, connector: "gemini" },
  { pattern: /\baider\b/i, connector: "aider" },
  { pattern: /\bopencode\b/i, connector: "opencode" },
  { pattern: /\bqwen\b/i, connector: "qwen-code" },
  { pattern: /\bcursor\b/i, connector: "cursor" },
  { pattern: /\bvs\s*code\b/i, connector: "vscode" },
  { pattern: /\btrae\b/i, connector: "trae-ide" },
];

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

export function parseP0Clients(markdown: string): string[] {
  const lines = normalizeMarkdown(markdown)
    .split("\n")
    .map((line) => line.trimEnd());
  const p0Clients: string[] = [];
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
      if (priority !== "P0") {
        index += 1;
        continue;
      }
      const client = cells[clientIdx];
      if (client) {
        p0Clients.push(client);
      }
      index += 1;
    }
  }

  return [...new Set(p0Clients)];
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

  const callRegex = /newFeatureConnector\(\s*([^,]+?)\s*,/g;
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

function mapClientToConnector(client: string): string | null {
  for (const rule of p0ConnectorRules) {
    if (rule.pattern.test(client)) {
      return rule.connector;
    }
  }
  return null;
}

function fail(message: string): never {
  console.error(`[support-matrix] ${message}`);
  process.exit(1);
}

function main(): void {
  const matrixDoc = readFileSync(matrixDocPath, "utf8");
  const connectorDecl = readFileSync(connectorDeclPath, "utf8");

  const p0Clients = parseP0Clients(matrixDoc);
  if (p0Clients.length === 0) {
    fail(`no P0 clients found in ${path.relative(repoRoot, matrixDocPath)}`);
  }

  const requiredByConnector = new Map<string, string[]>();
  const unmappedClients: string[] = [];
  for (const client of p0Clients) {
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
      `unable to map P0 clients to connector keys: ${unmappedClients
        .sort((a, b) => a.localeCompare(b))
        .join(", ")}`,
    );
  }

  const declaredConnectors = parseDeclaredPullerConnectors(connectorDecl);
  const declaredSet = new Set(declaredConnectors);
  const requiredConnectors = [...requiredByConnector.keys()].sort((a, b) => a.localeCompare(b));

  const missing = requiredConnectors.filter((connector) => !declaredSet.has(connector));
  const extra = declaredConnectors.filter((connector) => !requiredByConnector.has(connector));

  console.log(
    `[support-matrix] P0 clients in docs/09: ${p0Clients.length}, required connectors: ${requiredConnectors.length}`,
  );
  console.log(`[support-matrix] Declared puller connectors: ${declaredConnectors.join(", ") || "(none)"}`);

  if (missing.length > 0) {
    console.error("[support-matrix] FAILED: missing puller connectors for docs/09 P0 clients:");
    for (const connector of missing) {
      const clients = requiredByConnector.get(connector) ?? [];
      console.error(`  - ${connector} <- ${clients.join(" | ")}`);
    }
    process.exit(1);
  }

  console.log("[support-matrix] PASSED: docs/09 P0 clients are fully covered by puller connectors.");
  if (extra.length > 0) {
    console.log(`[support-matrix] Note: non-P0 connectors currently declared: ${extra.join(", ")}`);
  }
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
