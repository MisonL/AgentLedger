package main

import (
	"context"
	"errors"
	"testing"
)

type stubConnector struct {
	name string
	err  error
}

func (c stubConnector) Name() string {
	return c.name
}

func (c stubConnector) Match(source sourceRecord, sourcePath string) bool {
	return true
}

func (c stubConnector) Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
	if c.err != nil {
		return nil, c.err
	}
	return parseLinesConcurrently(ctx, input)
}

func TestDefaultConnectorRegistrySelect(t *testing.T) {
	t.Parallel()

	registry := newConnectorRegistry(
		newFeatureConnectorWithParser(connectorNameCodex, []string{"codex", "openai"}, parseCodexLines),
		newFeatureConnectorWithParser(connectorNameClaude, []string{"claude", "anthropic"}, parseClaudeLines),
		newFeatureConnectorWithParser(connectorNameGemini, []string{"gemini", "gemini-cli", ".gemini"}, parseGeminiLines),
		newFeatureConnectorWithParser(connectorNameAider, []string{"aider", ".aider"}, parseAiderLines),
		newFeatureConnectorWithParser(connectorNameOpenCode, []string{"opencode", "opencode-ai"}, parseOpenCodeLines),
		newFeatureConnectorWithParser(connectorNameQwenCode, []string{"qwen", "qwen-code", "qwen code"}, parseQwenCodeLines),
		newFeatureConnectorWithParser(connectorNameCursor, []string{"cursor"}, parseCursorLines),
		newFeatureConnectorWithParser(connectorNameVSCode, []string{
			"vscode",
			".vscode",
			"visual studio code",
			"github.copilot",
			"copilot-chat",
			"continue.continue",
			"roo-cline",
			"cline",
		}, parseVSCodeLines),
		newFeatureConnectorWithParser(connectorNameTRAEIDE, []string{"trae-ide", "trae ide", ".trae"}, parseTRAEIDELines),
		newFeatureConnectorWithParser(connectorNameKimiCLI, []string{"kimi", "kimi-cli"}, parseKimiCLILines),
		newFeatureConnectorWithParser(connectorNameTRAECLI, []string{"trae-cli", "trae cli"}, parseTRAECLILines),
		newFeatureConnectorWithParser(connectorNameCodeBuddyCLI, []string{"codebuddy-cli", "codebuddy cli"}, parseCodeBuddyCLILines),
		newFeatureConnectorWithParser(connectorNameWindsurf, []string{"windsurf"}, parseWindsurfLines),
		newFeatureConnectorWithParser(connectorNameLingma, []string{"lingma"}, parseLingmaLines),
		newFeatureConnectorWithParser(connectorNameCodeBuddyIDE, []string{"codebuddy-ide", "codebuddy ide"}, parseCodeBuddyIDELines),
		newFeatureConnectorWithParser(connectorNameZed, []string{"zed", ".zed"}, parseZedLines),
	)

	cases := []struct {
		name       string
		source     sourceRecord
		sourcePath string
		want       string
	}{
		{
			name:       "codex_by_provider",
			source:     sourceRecord{Provider: "codex"},
			sourcePath: "/var/log/chat.log",
			want:       connectorNameCodex,
		},
		{
			name:       "claude_by_path",
			source:     sourceRecord{Name: "ssh-source"},
			sourcePath: "/Users/dev/.claude/projects/sessions.jsonl",
			want:       connectorNameClaude,
		},
		{
			name:       "cursor_by_source_name",
			source:     sourceRecord{Name: "Cursor Chat History"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameCursor,
		},
		{
			name:       "gemini_by_path",
			source:     sourceRecord{Name: "ssh-source"},
			sourcePath: "/Users/dev/.gemini/tmp/workspace/chats/session-1.json",
			want:       connectorNameGemini,
		},
		{
			name:       "aider_by_provider",
			source:     sourceRecord{Provider: "Aider"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameAider,
		},
		{
			name:       "opencode_by_source_name",
			source:     sourceRecord{Name: "OpenCode Local History"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameOpenCode,
		},
		{
			name:       "qwen_code_by_source_id",
			source:     sourceRecord{ID: "source-qwen-code-prod"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameQwenCode,
		},
		{
			name:       "vscode_by_extension_path",
			source:     sourceRecord{Name: "ssh-source"},
			sourcePath: "/Users/dev/.vscode/extensions/github.copilot-chat-1.0.0/sessions.log",
			want:       connectorNameVSCode,
		},
		{
			name:       "trae_ide_by_location",
			source:     sourceRecord{Location: "/Applications/TRAE IDE/app"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameTRAEIDE,
		},
		{
			name:       "kimi_cli_by_provider",
			source:     sourceRecord{Provider: "kimi-cli"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameKimiCLI,
		},
		{
			name:       "trae_cli_by_source_name",
			source:     sourceRecord{Name: "TRAE CLI Chat"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameTRAECLI,
		},
		{
			name:       "codebuddy_cli_by_id",
			source:     sourceRecord{ID: "source-codebuddy-cli-cn"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameCodeBuddyCLI,
		},
		{
			name:       "windsurf_by_path",
			source:     sourceRecord{Name: "ssh-source"},
			sourcePath: "/Users/dev/.windsurf/history.log",
			want:       connectorNameWindsurf,
		},
		{
			name:       "lingma_by_provider",
			source:     sourceRecord{Provider: "lingma"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameLingma,
		},
		{
			name:       "codebuddy_ide_by_name",
			source:     sourceRecord{Name: "CodeBuddy IDE Session"},
			sourcePath: "/var/log/app.log",
			want:       connectorNameCodeBuddyIDE,
		},
		{
			name:       "zed_by_path",
			source:     sourceRecord{Name: "ssh-source"},
			sourcePath: "/Users/dev/.zed/sessions/history.log",
			want:       connectorNameZed,
		},
		{
			name:       "no_match",
			source:     sourceRecord{Name: "generic_source"},
			sourcePath: "/var/log/app.log",
			want:       "",
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			selected := registry.Select(tt.source, tt.sourcePath)
			if tt.want == "" {
				if selected != nil {
					t.Fatalf("Select() = %q, want nil", selected.Name())
				}
				return
			}

			if selected == nil {
				t.Fatalf("Select() = nil, want %q", tt.want)
			}
			if selected.Name() != tt.want {
				t.Fatalf("Select().Name() = %q, want %q", selected.Name(), tt.want)
			}
		})
	}
}

func TestParseWithConnectorFallback(t *testing.T) {
	t.Parallel()

	outputs, err := parseWithConnector(context.Background(), stubConnector{
		name: connectorNameCodex,
		err:  errors.New("connector parse failed"),
	}, parseInput{
		Source:      sourceRecord{ID: "source-1"},
		SourcePath:  "/tmp/chat.jsonl",
		Lines:       []lineRecord{{No: 1, Text: `{"text":"hello"}`}},
		JSONLStart:  0,
		NativeStart: 0,
	})
	if err != nil {
		t.Fatalf("parseWithConnector() unexpected error: %v", err)
	}
	if _, ok := outputs[parserKeyJSONL]; !ok {
		t.Fatalf("parseWithConnector() missing jsonl output after fallback")
	}
	if _, ok := outputs[parserKeyNative]; !ok {
		t.Fatalf("parseWithConnector() missing native output after fallback")
	}
}

func TestParseWithConnectorNoFallbackForCancel(t *testing.T) {
	t.Parallel()

	_, err := parseWithConnector(context.Background(), stubConnector{
		name: connectorNameCodex,
		err:  errJobCancelled,
	}, parseInput{})
	if !errors.Is(err, errJobCancelled) {
		t.Fatalf("parseWithConnector() error = %v, want %v", err, errJobCancelled)
	}
}
