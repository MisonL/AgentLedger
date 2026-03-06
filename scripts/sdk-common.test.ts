import { describe, expect, it } from "bun:test";
import {
  extractOpenApiOperations,
  toCamelCase,
  toPascalCase,
  toSnakeCase,
} from "./sdk-common";

describe("sdk-common 命名转换", () => {
  it("可正确处理驼峰 operationId", () => {
    expect(toCamelCase("listApiKeys")).toBe("listApiKeys");
    expect(toSnakeCase("listApiKeys")).toBe("list_api_keys");
    expect(toPascalCase("listApiKeys")).toBe("ListApiKeys");
  });

  it("可正确处理包含缩写与数字的命名", () => {
    expect(toCamelCase("ListAPIKeys")).toBe("listApiKeys");
    expect(toSnakeCase("ListAPIKeys")).toBe("list_api_keys");
    expect(toPascalCase("ListAPIKeys")).toBe("ListApiKeys");
    expect(toCamelCase("v1ListAPIs")).toBe("v1ListApis");
    expect(toSnakeCase("v1ListAPIs")).toBe("v1_list_apis");
    expect(toPascalCase("v1ListAPIs")).toBe("V1ListApis");
  });
});

describe("extractOpenApiOperations", () => {
  it("根据 OpenAPI operationId 生成稳定的方法名", () => {
    const operations = extractOpenApiOperations({
      openapi: "3.0.3",
      info: {
        title: "Test API",
        version: "1.0.0",
      },
      paths: {
        "/api/v1/api-keys": {
          get: {
            operationId: "listApiKeys",
            responses: {
              200: {
                description: "ok",
              },
            },
          },
        },
      },
    });

    expect(operations).toHaveLength(1);
    expect(operations[0]).toMatchObject({
      id: "list_api_keys",
      operationId: "listApiKeys",
      methodNameCamel: "listApiKeys",
      methodNameSnake: "list_api_keys",
      methodNamePascal: "ListApiKeys",
    });
  });

  it("对 replay v2 操作应用 canonical 参数覆盖与兼容别名", () => {
    const operations = extractOpenApiOperations({
      openapi: "3.0.3",
      info: {
        title: "Replay API",
        version: "1.0.0",
      },
      paths: {
        "/api/v2/replay/datasets/{id}/cases": {
          get: {
            operationId: "listReplayDatasetCasesV2",
            parameters: [
              { name: "limit", in: "query", schema: { type: "integer" } },
            ],
            responses: {
              200: { description: "ok" },
            },
          },
        },
        "/api/v2/replay/datasets/{id}/materialize": {
          post: {
            operationId: "materializeReplayDatasetCasesV2",
            requestBody: {
              required: true,
            },
            responses: {
              200: { description: "ok" },
            },
          },
        },
        "/api/v2/replay/runs": {
          get: {
            operationId: "listReplayRunsV2",
            parameters: [
              { name: "status", in: "query", schema: { type: "string" } },
              { name: "datasetId", in: "query", schema: { type: "string" } },
              { name: "baselineId", in: "query", schema: { type: "string" } },
            ],
            responses: {
              200: { description: "ok" },
            },
          },
          post: {
            operationId: "createReplayRunV2",
            requestBody: {
              required: true,
            },
            responses: {
              200: { description: "ok" },
            },
          },
        },
        "/api/v2/replay/runs/{id}/diffs": {
          get: {
            operationId: "getReplayRunDiffsV2",
            parameters: [
              { name: "baselineId", in: "query", schema: { type: "string" } },
              { name: "datasetId", in: "query", schema: { type: "string" } },
              { name: "keyword", in: "query", schema: { type: "string" } },
            ],
            responses: {
              200: { description: "ok" },
            },
          },
        },
      },
    });

    expect(operations).toHaveLength(5);
    expect(operations.find((item) => item.operationId === "listReplayDatasetCasesV2")).toMatchObject({
      pathParams: ["datasetId"],
      wirePathParams: ["id"],
      compatibilityAliases: {
        path: [
          {
            canonicalName: "datasetId",
            wireName: "id",
            compatibilityNames: ["id", "baselineId"],
          },
        ],
      },
    });
    expect(
      operations.find((item) => item.operationId === "materializeReplayDatasetCasesV2")
    ).toMatchObject({
      pathParams: ["datasetId"],
      wirePathParams: ["id"],
      compatibilityAliases: {
        path: [
          {
            canonicalName: "datasetId",
            wireName: "id",
            compatibilityNames: ["id", "baselineId"],
          },
        ],
      },
    });
    expect(operations.find((item) => item.operationId === "listReplayRunsV2")).toMatchObject({
      queryParams: ["status", "datasetId"],
      compatibilityAliases: {
        query: [
          {
            canonicalName: "datasetId",
            compatibilityNames: ["baselineId"],
          },
        ],
      },
    });
    expect(operations.find((item) => item.operationId === "createReplayRunV2")).toMatchObject({
      compatibilityAliases: {
        body: [
          {
            canonicalName: "datasetId",
            compatibilityNames: ["baselineId"],
          },
        ],
      },
    });
    expect(operations.find((item) => item.operationId === "getReplayRunDiffsV2")).toMatchObject({
      pathParams: ["runId"],
      wirePathParams: ["id"],
      queryParams: ["datasetId", "keyword"],
      compatibilityAliases: {
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
      },
    });
  });
});
