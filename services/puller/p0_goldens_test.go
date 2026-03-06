package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const (
	defaultP0GoldenAccuracyThreshold = 99.0
)

var requiredP0Connectors = []string{
	connectorNameAider,
	connectorNameClaude,
	connectorNameCodex,
	connectorNameCursor,
	connectorNameGemini,
	connectorNameOpenCode,
	connectorNameQwenCode,
	connectorNameTRAEIDE,
	connectorNameVSCode,
}

type p0GoldenFixture struct {
	Cases []p0GoldenCase `json:"cases"`
}

type p0GoldenCase struct {
	ID       string         `json:"id"`
	Client   string         `json:"client"`
	Source   sourceRecord   `json:"source"`
	Path     string         `json:"sourcePath"`
	Lines    []string       `json:"lines"`
	Expected p0GoldenExpect `json:"expected"`
}

type p0GoldenExpect struct {
	Connector         string   `json:"connector"`
	ParserKey         string   `json:"parserKey"`
	EventIndex        int      `json:"eventIndex"`
	SourceOffset      int64    `json:"sourceOffset"`
	EventType         string   `json:"eventType"`
	Role              string   `json:"role"`
	Text              string   `json:"text"`
	Model             string   `json:"model"`
	OccurredAt        string   `json:"occurredAt"`
	SessionID         string   `json:"sessionId"`
	SessionIDNonEmpty bool     `json:"sessionIdNonEmpty"`
	InputTokens       int64    `json:"inputTokens"`
	OutputTokens      int64    `json:"outputTokens"`
	CostUSD           *float64 `json:"costUsd"`
	CostUSDNil        bool     `json:"costUsdNil"`
}

type p0GoldenMismatch struct {
	CaseID   string `json:"caseId"`
	Client   string `json:"client"`
	Field    string `json:"field"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
}

type p0GoldenReport struct {
	TotalSamples   int                `json:"totalSamples"`
	CorrectSamples int                `json:"correctSamples"`
	Accuracy       float64            `json:"accuracy"`
	TotalCases     int                `json:"totalCases"`
	PassedCases    int                `json:"passedCases"`
	FailedCases    []string           `json:"failedCases"`
	Mismatches     []p0GoldenMismatch `json:"mismatches"`
}

type p0CaseEval struct {
	report      *p0GoldenReport
	caseID      string
	client      string
	hadMismatch bool
}

func (e *p0CaseEval) check(field string, expected, actual string) {
	e.report.TotalSamples++
	if expected == actual {
		e.report.CorrectSamples++
		return
	}
	e.hadMismatch = true
	e.report.Mismatches = append(e.report.Mismatches, p0GoldenMismatch{
		CaseID:   e.caseID,
		Client:   e.client,
		Field:    field,
		Expected: expected,
		Actual:   actual,
	})
}

func TestP0GoldenFixtureCoverage(t *testing.T) {
	fixture, err := loadP0GoldenFixture()
	if err != nil {
		t.Fatalf("load P0 golden fixture failed: %v", err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("P0 golden fixture has no cases")
	}

	seenCaseIDs := make(map[string]struct{}, len(fixture.Cases))
	seenConnectors := make(map[string]struct{}, len(requiredP0Connectors))
	for _, item := range fixture.Cases {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			t.Fatal("fixture contains empty case id")
		}
		if _, exists := seenCaseIDs[id]; exists {
			t.Fatalf("fixture contains duplicated case id: %s", id)
		}
		seenCaseIDs[id] = struct{}{}

		connector := strings.TrimSpace(item.Expected.Connector)
		if connector == "" {
			t.Fatalf("fixture case %s missing expected.connector", id)
		}
		seenConnectors[connector] = struct{}{}
	}

	missing := make([]string, 0)
	for _, connector := range requiredP0Connectors {
		if _, ok := seenConnectors[connector]; ok {
			continue
		}
		missing = append(missing, connector)
	}
	if len(missing) > 0 {
		t.Fatalf("fixture missing P0 connectors: %s", strings.Join(missing, ", "))
	}
}

func TestP0GoldenAccuracyGate(t *testing.T) {
	threshold, err := resolveP0GoldenAccuracyThreshold()
	if err != nil {
		t.Fatalf("resolve P0 golden threshold failed: %v", err)
	}

	report, err := evaluateP0Goldens()
	if err != nil {
		t.Fatalf("evaluate P0 goldens failed: %v", err)
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal P0 golden report failed: %v", err)
	}
	t.Logf("P0_GOLDEN_REPORT=%s", string(encoded))

	if report.TotalSamples == 0 {
		t.Fatal("P0 golden report has zero samples")
	}

	if report.Accuracy+1e-9 < threshold {
		maxToShow := len(report.Mismatches)
		if maxToShow > 20 {
			maxToShow = 20
		}
		for index := 0; index < maxToShow; index++ {
			item := report.Mismatches[index]
			t.Logf(
				"mismatch[%d] case=%s client=%s field=%s expected=%s actual=%s",
				index,
				item.CaseID,
				item.Client,
				item.Field,
				item.Expected,
				item.Actual,
			)
		}
		t.Fatalf(
			"P0 golden accuracy %.4f%% below threshold %.2f%% (cases %d/%d passed)",
			report.Accuracy,
			threshold,
			report.PassedCases,
			report.TotalCases,
		)
	}
}

func resolveP0GoldenAccuracyThreshold() (float64, error) {
	raw := strings.TrimSpace(os.Getenv("PULLER_P0_GOLDEN_ACCURACY_THRESHOLD"))
	if raw == "" {
		return defaultP0GoldenAccuracyThreshold, nil
	}

	parsed, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid PULLER_P0_GOLDEN_ACCURACY_THRESHOLD %q: %w", raw, err)
	}
	if parsed < defaultP0GoldenAccuracyThreshold {
		return 0, fmt.Errorf("PULLER_P0_GOLDEN_ACCURACY_THRESHOLD must be >= %.2f", defaultP0GoldenAccuracyThreshold)
	}
	if parsed > 100 {
		return 0, errors.New("PULLER_P0_GOLDEN_ACCURACY_THRESHOLD must be <= 100")
	}
	return parsed, nil
}

func evaluateP0Goldens() (p0GoldenReport, error) {
	fixture, err := loadP0GoldenFixture()
	if err != nil {
		return p0GoldenReport{}, err
	}

	registry := defaultPullerConnectorRegistry
	report := p0GoldenReport{
		TotalCases: len(fixture.Cases),
	}

	for _, item := range fixture.Cases {
		caseEval := p0CaseEval{
			report: &report,
			caseID: item.ID,
			client: item.Client,
		}

		connector := registry.Select(item.Source, item.Path)
		actualConnector := ""
		if connector != nil {
			actualConnector = connector.Name()
		}
		caseEval.check("connector", item.Expected.Connector, actualConnector)

		input := parseInput{
			Source:      item.Source,
			SourcePath:  item.Path,
			Lines:       toLineRecords(item.Lines),
			JSONLStart:  0,
			NativeStart: 0,
		}

		outputs, parseErr := parseWithConnector(context.Background(), connector, input)
		if parseErr != nil {
			caseEval.check("parseError", "", parseErr.Error())
			report.FailedCases = append(report.FailedCases, item.ID)
			continue
		}
		caseEval.check("parseError", "", "")

		out, hasOutput := outputs[item.Expected.ParserKey]
		caseEval.check("parserKey", "true", strconv.FormatBool(hasOutput))

		var event rawEventWithLine
		hasEvent := false
		if hasOutput && item.Expected.EventIndex >= 0 && item.Expected.EventIndex < len(out.Events) {
			event = out.Events[item.Expected.EventIndex]
			hasEvent = true
		}
		caseEval.check("eventExists", "true", strconv.FormatBool(hasEvent))

		if hasEvent {
			actualSourceOffset := "<nil>"
			if event.Event.SourceOffset != nil {
				actualSourceOffset = strconv.FormatInt(*event.Event.SourceOffset, 10)
			}
			caseEval.check("sourceOffset", strconv.FormatInt(item.Expected.SourceOffset, 10), actualSourceOffset)
			caseEval.check("eventType", item.Expected.EventType, event.Event.EventType)
			caseEval.check("role", item.Expected.Role, event.Event.Role)
			caseEval.check("text", item.Expected.Text, event.Event.Text)
			caseEval.check("model", item.Expected.Model, event.Event.Model)
			caseEval.check("occurredAt", item.Expected.OccurredAt, event.Event.OccurredAt)
			caseEval.check("inputTokens", strconv.FormatInt(item.Expected.InputTokens, 10), strconv.FormatInt(event.Event.Tokens.InputTokens, 10))
			caseEval.check("outputTokens", strconv.FormatInt(item.Expected.OutputTokens, 10), strconv.FormatInt(event.Event.Tokens.OutputTokens, 10))

			if item.Expected.SessionID != "" {
				caseEval.check("sessionId", item.Expected.SessionID, event.Event.SessionID)
			}
			if item.Expected.SessionIDNonEmpty {
				caseEval.check("sessionIdNonEmpty", "true", strconv.FormatBool(strings.TrimSpace(event.Event.SessionID) != ""))
			}

			if item.Expected.CostUSD != nil {
				if event.Event.CostUSD == nil {
					caseEval.check("costUsd", formatCostUSD(*item.Expected.CostUSD), "<nil>")
				} else {
					expected := *item.Expected.CostUSD
					actual := *event.Event.CostUSD
					if math.Abs(expected-actual) > 1e-9 {
						caseEval.check("costUsd", formatCostUSD(expected), formatCostUSD(actual))
					} else {
						caseEval.check("costUsd", formatCostUSD(expected), formatCostUSD(expected))
					}
				}
			} else if item.Expected.CostUSDNil {
				caseEval.check("costUsdNil", "true", strconv.FormatBool(event.Event.CostUSD == nil))
			}
		} else {
			// If the expected event cannot be resolved, keep required checks explicit.
			caseEval.check("sourceOffset", strconv.FormatInt(item.Expected.SourceOffset, 10), "<missing>")
			caseEval.check("eventType", item.Expected.EventType, "<missing>")
			caseEval.check("role", item.Expected.Role, "<missing>")
			caseEval.check("text", item.Expected.Text, "<missing>")
			caseEval.check("model", item.Expected.Model, "<missing>")
			caseEval.check("occurredAt", item.Expected.OccurredAt, "<missing>")
			caseEval.check("inputTokens", strconv.FormatInt(item.Expected.InputTokens, 10), "<missing>")
			caseEval.check("outputTokens", strconv.FormatInt(item.Expected.OutputTokens, 10), "<missing>")
			if item.Expected.SessionID != "" {
				caseEval.check("sessionId", item.Expected.SessionID, "<missing>")
			}
			if item.Expected.SessionIDNonEmpty {
				caseEval.check("sessionIdNonEmpty", "true", "<missing>")
			}
			if item.Expected.CostUSD != nil {
				caseEval.check("costUsd", formatCostUSD(*item.Expected.CostUSD), "<missing>")
			} else if item.Expected.CostUSDNil {
				caseEval.check("costUsdNil", "true", "<missing>")
			}
		}

		if caseEval.hadMismatch {
			report.FailedCases = append(report.FailedCases, item.ID)
			continue
		}
		report.PassedCases++
	}

	sort.Strings(report.FailedCases)
	if report.TotalSamples > 0 {
		report.Accuracy = (float64(report.CorrectSamples) / float64(report.TotalSamples)) * 100
	}
	return report, nil
}

func loadP0GoldenFixture() (p0GoldenFixture, error) {
	fixturePath := filepath.Join("testdata", "p0-goldens.json")
	content, err := os.ReadFile(fixturePath)
	if err != nil {
		return p0GoldenFixture{}, fmt.Errorf("read %s failed: %w", fixturePath, err)
	}

	var fixture p0GoldenFixture
	if err := json.Unmarshal(content, &fixture); err != nil {
		return p0GoldenFixture{}, fmt.Errorf("unmarshal %s failed: %w", fixturePath, err)
	}
	return fixture, nil
}

func toLineRecords(lines []string) []lineRecord {
	if len(lines) == 0 {
		return nil
	}
	result := make([]lineRecord, 0, len(lines))
	for idx, line := range lines {
		result = append(result, lineRecord{
			No:   int64(idx + 1),
			Text: line,
		})
	}
	return result
}

func formatCostUSD(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
