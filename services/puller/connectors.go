package main

import (
	"context"
	"errors"
	"strings"
)

const (
	connectorNameCodex    = "codex"
	connectorNameClaude   = "claude"
	connectorNameGemini   = "gemini"
	connectorNameAider    = "aider"
	connectorNameOpenCode = "opencode"
	connectorNameQwenCode = "qwen-code"
	connectorNameCursor   = "cursor"
	connectorNameVSCode   = "vscode"
	connectorNameTRAEIDE  = "trae-ide"
)

type pullerConnector interface {
	Name() string
	Match(source sourceRecord, sourcePath string) bool
	Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error)
}

type connectorRegistry struct {
	connectors []pullerConnector
}

var defaultPullerConnectorRegistry = newConnectorRegistry(
	newFeatureConnector(connectorNameCodex, []string{"codex", "openai"}),
	newFeatureConnector(connectorNameClaude, []string{"claude", "anthropic"}),
	newFeatureConnector(connectorNameGemini, []string{"gemini", "gemini-cli", ".gemini"}),
	newFeatureConnector(connectorNameAider, []string{"aider", ".aider"}),
	newFeatureConnector(connectorNameOpenCode, []string{"opencode", "opencode-ai"}),
	newFeatureConnector(connectorNameQwenCode, []string{"qwen", "qwen-code", "qwen code"}),
	newFeatureConnector(connectorNameCursor, []string{"cursor"}),
	newFeatureConnector(connectorNameVSCode, []string{
		"vscode",
		".vscode",
		"visual studio code",
		"github.copilot",
		"copilot-chat",
		"continue.continue",
		"roo-cline",
		"cline",
	}),
	newFeatureConnector(connectorNameTRAEIDE, []string{"trae", "trae ide", ".trae"}),
)

func newConnectorRegistry(connectors ...pullerConnector) *connectorRegistry {
	registry := &connectorRegistry{
		connectors: make([]pullerConnector, 0, len(connectors)),
	}
	registry.Register(connectors...)
	return registry
}

func (r *connectorRegistry) Register(connectors ...pullerConnector) {
	if r == nil {
		return
	}
	for _, connector := range connectors {
		if connector == nil {
			continue
		}
		r.connectors = append(r.connectors, connector)
	}
}

func (r *connectorRegistry) Select(source sourceRecord, sourcePath string) pullerConnector {
	if r == nil {
		return nil
	}
	for _, connector := range r.connectors {
		if connector.Match(source, sourcePath) {
			return connector
		}
	}
	return nil
}

type featureConnector struct {
	name     string
	features []string
}

func newFeatureConnector(name string, features []string) pullerConnector {
	normalizedFeatures := make([]string, 0, len(features))
	for _, feature := range features {
		trimmed := strings.ToLower(strings.TrimSpace(feature))
		if trimmed == "" {
			continue
		}
		normalizedFeatures = append(normalizedFeatures, trimmed)
	}

	return &featureConnector{
		name:     strings.TrimSpace(name),
		features: normalizedFeatures,
	}
}

func (c *featureConnector) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

func (c *featureConnector) Match(source sourceRecord, sourcePath string) bool {
	if c == nil || len(c.features) == 0 {
		return false
	}

	candidates := []string{
		source.Provider,
		source.Name,
		source.ID,
		source.Hostname,
		source.Location,
		sourcePath,
	}
	for _, candidate := range candidates {
		normalized := strings.ToLower(strings.TrimSpace(candidate))
		if normalized == "" {
			continue
		}
		for _, feature := range c.features {
			if strings.Contains(normalized, feature) {
				return true
			}
		}
	}
	return false
}

func (c *featureConnector) Parse(ctx context.Context, input parseInput) (map[string]parserOutput, error) {
	return parseLinesConcurrently(ctx, input)
}

func (s *pullerService) effectiveConnectorRegistry() *connectorRegistry {
	if s != nil && s.connectors != nil {
		return s.connectors
	}
	return defaultPullerConnectorRegistry
}

func parseWithConnector(ctx context.Context, connector pullerConnector, input parseInput) (map[string]parserOutput, error) {
	if connector == nil {
		return parseLinesConcurrently(ctx, input)
	}

	outputs, err := connector.Parse(ctx, input)
	if err == nil {
		return outputs, nil
	}
	if isParseFallbackError(err) {
		return parseLinesConcurrently(ctx, input)
	}
	return nil, err
}

func isParseFallbackError(err error) bool {
	return err != nil &&
		!errors.Is(err, errJobCancelled) &&
		!errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded)
}
