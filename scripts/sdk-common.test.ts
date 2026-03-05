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
});
