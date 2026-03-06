import { createHash } from "node:crypto";
import path from "node:path";

export const OPENAPI_HTTP_METHODS = [
  "get",
  "post",
  "put",
  "patch",
  "delete",
  "options",
  "head",
] as const;

const OPENAPI_HTTP_METHOD_SET = new Set<string>(OPENAPI_HTTP_METHODS);

export interface OpenApiOperationSpec {
  id: string;
  operationId: string;
  method: string;
  path: string;
  methodNameCamel: string;
  methodNamePascal: string;
  methodNameSnake: string;
  hasRequestBody: boolean;
  pathParams: string[];
  wirePathParams: string[];
  queryParams: string[];
  compatibilityAliases?: OpenApiOperationCompatibilitySpec;
}

export interface OpenApiOperationAliasSpec {
  canonicalName: string;
  wireName?: string;
  compatibilityNames: string[];
}

export interface OpenApiOperationCompatibilitySpec {
  path?: OpenApiOperationAliasSpec[];
  query?: OpenApiOperationAliasSpec[];
  body?: OpenApiOperationAliasSpec[];
}

export function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

export function stringifyWithTrailingNewline(value: unknown): string {
  return `${JSON.stringify(value, null, 2)}\n`;
}

export function computeDigest(document: Record<string, unknown>): string {
  return createHash("sha256").update(JSON.stringify(document)).digest("hex");
}

export function resolveInfoField(
  document: Record<string, unknown>,
  key: "title" | "version"
): string {
  const info = asRecord(document.info);
  const value = info?.[key];
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : "unknown";
}

export function resolveSpecVersion(document: Record<string, unknown>): string {
  const value = document.openapi;
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : "unknown";
}

export function toRepoRelativePath(repoRoot: string, targetPath: string): string {
  const rel = path.relative(repoRoot, targetPath);
  if (!rel || rel.startsWith("..")) {
    return targetPath;
  }
  return rel.split(path.sep).join("/");
}

function isUpper(char: string): boolean {
  return char >= "A" && char <= "Z";
}

function isLower(char: string): boolean {
  return char >= "a" && char <= "z";
}

function isDigit(char: string): boolean {
  return char >= "0" && char <= "9";
}

function isLetter(char: string): boolean {
  return isUpper(char) || isLower(char);
}

function splitIdentifierToken(token: string): string[] {
  if (token.length === 0) {
    return [];
  }

  const parts: string[] = [];
  let start = 0;

  for (let index = 1; index < token.length; index += 1) {
    const previous = token[index - 1] ?? "";
    const current = token[index] ?? "";
    const next = token[index + 1] ?? "";

    const boundaryByCase = isLower(previous) && isUpper(current);
    const boundaryByDigit = isDigit(previous) && isLetter(current);
    const boundaryByAcronym =
      isUpper(previous) &&
      isUpper(current) &&
      isLower(next) &&
      index + 1 < token.length - 1;

    if (boundaryByCase || boundaryByDigit || boundaryByAcronym) {
      parts.push(token.slice(start, index));
      start = index;
    }
  }

  parts.push(token.slice(start));
  return parts.filter((part) => part.length > 0);
}

function normalizeIdentifierParts(input: string): string[] {
  const rawParts = input
    .replace(/\{([^}]+)\}/g, " $1 ")
    .replace(/[^A-Za-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter((part) => part.length > 0);

  const tokens: string[] = [];
  for (const part of rawParts) {
    tokens.push(...splitIdentifierToken(part));
  }
  return tokens;
}

export function toSnakeCase(input: string): string {
  const parts = normalizeIdentifierParts(input).map((part) => part.toLowerCase());
  return parts.join("_");
}

export function toCamelCase(input: string): string {
  const parts = normalizeIdentifierParts(input);
  if (parts.length === 0) {
    return "";
  }
  const [head, ...rest] = parts;
  return [head.toLowerCase(), ...rest.map((item) => `${item[0]?.toUpperCase() ?? ""}${item.slice(1).toLowerCase()}`)].join("");
}

export function toPascalCase(input: string): string {
  const parts = normalizeIdentifierParts(input);
  return parts
    .map((item) => `${item[0]?.toUpperCase() ?? ""}${item.slice(1).toLowerCase()}`)
    .join("");
}

function ensureIdentifierHead(value: string, fallbackPrefix: string): string {
  if (!value) {
    return `${fallbackPrefix}Op`;
  }
  if (!/^[A-Za-z_]/.test(value)) {
    return `${fallbackPrefix}${value[0]?.toUpperCase() ?? ""}${value.slice(1)}`;
  }
  return value;
}

function dedupeName(base: string, used: Set<string>): string {
  if (!used.has(base)) {
    used.add(base);
    return base;
  }
  let index = 2;
  while (used.has(`${base}${index}`)) {
    index += 1;
  }
  const next = `${base}${index}`;
  used.add(next);
  return next;
}

function extractPathParams(pathValue: string): string[] {
  const result: string[] = [];
  const regexp = /\{([^}]+)\}/g;
  let match: RegExpExecArray | null;
  while ((match = regexp.exec(pathValue)) !== null) {
    const raw = match[1]?.trim();
    if (raw && !result.includes(raw)) {
      result.push(raw);
    }
  }
  return result;
}

function collectQueryParams(
  operation: Record<string, unknown>,
  pathItem: Record<string, unknown>
): string[] {
  const names: string[] = [];
  const sources: unknown[] = [pathItem.parameters, operation.parameters];
  for (const source of sources) {
    if (!Array.isArray(source)) {
      continue;
    }
    for (const parameter of source) {
      const item = asRecord(parameter);
      if (!item) {
        continue;
      }
      if (item.in !== "query") {
        continue;
      }
      const name = typeof item.name === "string" ? item.name.trim() : "";
      if (!name || names.includes(name)) {
        continue;
      }
      names.push(name);
    }
  }
  return names;
}

function buildFallbackOperationId(method: string, pathValue: string): string {
  const normalizedPath = pathValue
    .replace(/^\/+/, "")
    .replace(/\//g, "_")
    .replace(/\{([^}]+)\}/g, "by_$1")
    .replace(/[^A-Za-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `${method}_${normalizedPath || "root"}`;
}

function applyReplayCanonicalOperationOverrides(
  operations: OpenApiOperationSpec[]
): OpenApiOperationSpec[] {
  return operations.map((operation) => {
    const nextOperation: OpenApiOperationSpec = {
      ...operation,
      pathParams: [...operation.pathParams],
      wirePathParams: [...operation.wirePathParams],
      queryParams: [...operation.queryParams],
      compatibilityAliases: operation.compatibilityAliases
        ? {
            path: operation.compatibilityAliases.path?.map((item) => ({
              ...item,
              compatibilityNames: [...item.compatibilityNames],
            })),
            query: operation.compatibilityAliases.query?.map((item) => ({
              ...item,
              compatibilityNames: [...item.compatibilityNames],
            })),
            body: operation.compatibilityAliases.body?.map((item) => ({
              ...item,
              compatibilityNames: [...item.compatibilityNames],
            })),
          }
        : undefined,
    };

    switch (nextOperation.operationId) {
      case "listReplayDatasetCasesV2":
      case "replaceReplayDatasetCasesV2":
      case "materializeReplayDatasetCasesV2":
        nextOperation.pathParams = ["datasetId"];
        nextOperation.compatibilityAliases = {
          ...(nextOperation.compatibilityAliases ?? {}),
          path: [
            {
              canonicalName: "datasetId",
              wireName: "id",
              compatibilityNames: ["id", "baselineId"],
            },
          ],
        };
        return nextOperation;
      case "listReplayRunsV2":
        nextOperation.queryParams = nextOperation.queryParams.filter(
          (item) => item !== "baselineId"
        );
        nextOperation.compatibilityAliases = {
          ...(nextOperation.compatibilityAliases ?? {}),
          query: [
            {
              canonicalName: "datasetId",
              compatibilityNames: ["baselineId"],
            },
          ],
        };
        return nextOperation;
      case "createReplayRunV2":
        nextOperation.compatibilityAliases = {
          ...(nextOperation.compatibilityAliases ?? {}),
          body: [
            {
              canonicalName: "datasetId",
              compatibilityNames: ["baselineId"],
            },
          ],
        };
        return nextOperation;
      case "getReplayRunV2":
      case "getReplayRunArtifactsV2":
      case "downloadReplayRunArtifactV2":
        nextOperation.pathParams = nextOperation.pathParams.map((item) =>
          item === "id" ? "runId" : item
        );
        nextOperation.compatibilityAliases = {
          ...(nextOperation.compatibilityAliases ?? {}),
          path: [
            {
              canonicalName: "runId",
              wireName: "id",
              compatibilityNames: ["id", "jobId"],
            },
          ],
        };
        return nextOperation;
      case "getReplayRunDiffsV2":
        nextOperation.pathParams = nextOperation.pathParams.map((item) =>
          item === "id" ? "runId" : item
        );
        nextOperation.queryParams = nextOperation.queryParams.filter(
          (item) => item !== "baselineId"
        );
        nextOperation.compatibilityAliases = {
          ...(nextOperation.compatibilityAliases ?? {}),
          path: [
            {
              canonicalName: "runId",
              wireName: "id",
              compatibilityNames: ["id", "jobId"],
            },
          ],
          query: [
            {
              canonicalName: "datasetId",
              compatibilityNames: ["baselineId"],
            },
          ],
        };
        return nextOperation;
      default:
        return nextOperation;
    }
  });
}

export function extractOpenApiOperations(
  document: Record<string, unknown>
): OpenApiOperationSpec[] {
  const paths = asRecord(document.paths);
  if (!paths) {
    return [];
  }

  const operations: OpenApiOperationSpec[] = [];
  const usedIds = new Set<string>();
  const usedCamelNames = new Set<string>();

  const pathKeys = Object.keys(paths).sort((left, right) => left.localeCompare(right));
  for (const pathKey of pathKeys) {
    const pathItem = asRecord(paths[pathKey]);
    if (!pathItem) {
      continue;
    }
    const pathParams = extractPathParams(pathKey);

    for (const methodKey of OPENAPI_HTTP_METHODS) {
      if (!OPENAPI_HTTP_METHOD_SET.has(methodKey)) {
        continue;
      }
      const operation = asRecord(pathItem[methodKey]);
      if (!operation) {
        continue;
      }

      const fallbackId = buildFallbackOperationId(methodKey, pathKey);
      const rawOperationId =
        typeof operation.operationId === "string" && operation.operationId.trim().length > 0
          ? operation.operationId.trim()
          : fallbackId;

      const idBase = ensureIdentifierHead(toSnakeCase(rawOperationId), "op");
      const operationId = dedupeName(idBase, usedIds);

      const camelBase = ensureIdentifierHead(toCamelCase(rawOperationId), "op");
      const methodNameCamel = dedupeName(camelBase, usedCamelNames);

      operations.push({
        id: operationId,
        operationId: rawOperationId,
        method: methodKey.toUpperCase(),
        path: pathKey,
        methodNameCamel,
        methodNamePascal: ensureIdentifierHead(toPascalCase(methodNameCamel), "Op"),
        methodNameSnake: ensureIdentifierHead(toSnakeCase(methodNameCamel), "op"),
        hasRequestBody: Boolean(operation.requestBody),
        pathParams: [...pathParams],
        wirePathParams: [...pathParams],
        queryParams: collectQueryParams(operation, pathItem),
      });
    }
  }

  return applyReplayCanonicalOperationOverrides(operations);
}
