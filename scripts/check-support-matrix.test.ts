import { describe, expect, test } from "bun:test";
import {
  mapClientToConnector,
  parseClientsByPriorities,
  parseDeclaredPullerConnectors,
  parseFeatureConnectorParserEntrypoints,
  parseP0P1Clients,
  validateParserEntrypoints,
} from "./check-support-matrix";

describe("parseClientsByPriorities", () => {
  test("可解析标准 LF Markdown 表格中的 P0/P1 客户端", () => {
    const markdown = `# 支持矩阵

| 客户端 | 优先级 |
| --- | --- |
| OpenAI Codex CLI | P0 |
| Claude Code | P0 |
| Kimi CLI | P1 |
| AmpCode | P2 |
`;

    expect(parseP0P1Clients(markdown)).toEqual([
      "OpenAI Codex CLI",
      "Claude Code",
      "Kimi CLI",
    ]);
  });

  test("可解析带 BOM + CRLF 的表格", () => {
    const markdown =
      "\uFEFF| 客户端 | 优先级 |\r\n| --- | --- |\r\n| VS Code（Cline/Roo/Continue/Copilot Chat） | P0 |\r\n| Windsurf IDE | P1 |\r\n| Zed AI | P2 |\r\n";

    expect(parseP0P1Clients(markdown)).toEqual([
      "VS Code（Cline/Roo/Continue/Copilot Chat）",
      "Windsurf IDE",
    ]);
  });

  test("仅解析包含 客户端/优先级 列的表格", () => {
    const markdown = `| 名称 | 级别 |
| --- | --- |
| foo | P0 |

| 客户端 | 优先级 | 状态 |
| --- | --- | --- |
| Cursor IDE | \`P0\` | 已接入 |
| 通义灵码（Lingma IDE/插件） | _P1_ | 已接入 |
`;

    expect(parseClientsByPriorities(markdown, ["P0", "P1"])).toEqual([
      "Cursor IDE",
      "通义灵码（Lingma IDE/插件）",
    ]);
  });
});

describe("mapClientToConnector", () => {
  test("支持 P1 客户端映射到现有 connector", () => {
    expect(mapClientToConnector("Kimi CLI")).toBe("kimi-cli");
    expect(mapClientToConnector("Windsurf IDE")).toBe("windsurf");
    expect(mapClientToConnector("通义灵码（Lingma IDE/插件）")).toBe("lingma");
    expect(mapClientToConnector("CodeBuddy Code CLI")).toBe("codebuddy-cli");
    expect(mapClientToConnector("CodeBuddy IDE（VS Code/JetBrains）")).toBe("codebuddy-ide");
    expect(mapClientToConnector("Zed AI / JetBrains AI Assistant")).toBe("zed");
  });
});

describe("parseDeclaredPullerConnectors", () => {
  test("可解析 defaultPullerConnectorRegistry 中声明的 connector", () => {
    const source = `const (
  connectorNameCodex = "codex"
  connectorNameTraeIDE = "trae-ide"
)

var defaultPullerConnectorRegistry = newConnectorRegistry(
  newFeatureConnector(connectorNameCodex, "x"),
  newFeatureConnector(connectorNameTraeIDE, "x"),
)
`;

    expect(parseDeclaredPullerConnectors(source)).toEqual(["codex", "trae-ide"]);
  });
});

describe("parser entrypoints", () => {
  test("可解析 featureConnector.Parse 使用的 parser 入口", () => {
    const connectorSource = `func (c *featureConnector) Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
  return parseLinesConcurrently(ctx, input)
}
`;

    expect(parseFeatureConnectorParserEntrypoints(connectorSource)).toEqual([
      "parseLinesConcurrently",
    ]);
  });

  test("校验 parseWithConnector 调度与 parser 入口存在", () => {
    const connectorSource = `func (c *featureConnector) Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
  return parseLinesConcurrently(ctx, input)
}

func parseWithConnector(ctx context.Context, connector pullerConnector, input parseInput) (map[string]parserOutput, error) {
  if connector == nil {
    return parseLinesConcurrently(ctx, input)
  }
  outputs, err := connector.Parse(ctx, input)
  if err == nil {
    return outputs, nil
  }
  return parseLinesConcurrently(ctx, input)
}
`;

    const parserSource = `func parseLinesConcurrently(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
  return nil, nil
}
`;

    expect(validateParserEntrypoints(connectorSource, parserSource)).toEqual({
      entrypoints: ["parseLinesConcurrently"],
      declaredParsers: [],
      missingEntrypoints: [],
      hasParseWithConnectorDispatch: true,
      hasParseWithConnectorFallback: true,
    });
  });

  test("当 parser 入口函数缺失或调度缺失时返回失败信息", () => {
    const connectorSource = `func (c *featureConnector) Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
  return parseLinesConcurrently(ctx, input)
}

func parseWithConnector(ctx context.Context, connector pullerConnector, input parseInput) (map[string]parserOutput, error) {
  return nil, nil
}
`;

    const result = validateParserEntrypoints(connectorSource, "package main");
    expect(result.entrypoints).toEqual(["parseLinesConcurrently"]);
    expect(result.declaredParsers).toEqual([]);
    expect(result.missingEntrypoints).toEqual(["parseLinesConcurrently"]);
    expect(result.hasParseWithConnectorDispatch).toBe(false);
    expect(result.hasParseWithConnectorFallback).toBe(false);
  });

  test("可校验 newFeatureConnectorWithParser 声明的 parser 函数存在", () => {
    const connectorSource = `var defaultPullerConnectorRegistry = newConnectorRegistry(
  newFeatureConnectorWithParser(connectorNameCodex, []string{"codex"}, parseCodexLines),
)

func parseWithConnector(ctx context.Context, connector pullerConnector, input parseInput) (map[string]parserOutput, error) {
  if connector == nil {
    return parseLinesConcurrently(ctx, input)
  }
  outputs, err := connector.Parse(ctx, input)
  if err == nil {
    return outputs, nil
  }
  return parseLinesConcurrently(ctx, input)
}

func (c *featureConnector) Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
  return parseLinesConcurrently(ctx, input)
}
`;
    const parserSource = `func parseLinesConcurrently(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
  return nil, nil
}
`;

    const result = validateParserEntrypoints(connectorSource, parserSource);
    expect(result.declaredParsers).toEqual(["parseCodexLines"]);
    expect(result.missingEntrypoints).toEqual(["parseCodexLines"]);
  });
});
