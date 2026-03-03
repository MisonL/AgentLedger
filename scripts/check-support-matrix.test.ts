import { describe, expect, test } from "bun:test";
import { parseDeclaredPullerConnectors, parseP0Clients } from "./check-support-matrix";

describe("parseP0Clients", () => {
  test("可解析标准 LF Markdown 表格中的 P0 客户端", () => {
    const markdown = `# 支持矩阵

| 客户端 | 优先级 |
| --- | --- |
| OpenAI Codex CLI | P0 |
| Claude Code | P0 |
| Kimi CLI | P1 |
`;

    expect(parseP0Clients(markdown)).toEqual(["OpenAI Codex CLI", "Claude Code"]);
  });

  test("可解析带 BOM + CRLF 的表格", () => {
    const markdown = `\uFEFF| 客户端 | 优先级 |\r\n| --- | --- |\r\n| VS Code（Cline/Roo/Continue/Copilot Chat） | P0 |\r\n| Windsurf IDE | P1 |\r\n`;

    expect(parseP0Clients(markdown)).toEqual(["VS Code（Cline/Roo/Continue/Copilot Chat）"]);
  });

  test("只解析包含 客户端/优先级 列的表格", () => {
    const markdown = `| 名称 | 级别 |
| --- | --- |
| foo | P0 |

| 客户端 | 优先级 | 状态 |
| --- | --- | --- |
| Cursor IDE | \`P0\` | 已接入 |
`;

    expect(parseP0Clients(markdown)).toEqual(["Cursor IDE"]);
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
