package main

import (
	"reflect"
	"testing"
)

func TestWatermarkCompatibilityHelpers(t *testing.T) {
	t.Parallel()

	if got := normalizeWatermarkParserKey(""); got != defaultWatermarkProvider {
		t.Fatalf("normalizeWatermarkParserKey(\"\") = %q, want %q", got, defaultWatermarkProvider)
	}
	if got := normalizeWatermarkHostKey(""); got != legacyWatermarkHostKey {
		t.Fatalf("normalizeWatermarkHostKey(\"\") = %q, want %q", got, legacyWatermarkHostKey)
	}
	if got := watermarkProviderFromParserKey("jsonl"); got != "jsonl" {
		t.Fatalf("watermarkProviderFromParserKey(\"jsonl\") = %q, want jsonl", got)
	}
}

func TestWatermarkLookupHostKeys(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		hostKey string
		want    []string
	}{
		{
			name:    "exact_and_legacy",
			hostKey: "dev@10.0.0.1:22:/var/log/app.log",
			want:    []string{"dev@10.0.0.1:22:/var/log/app.log", legacyWatermarkHostKey},
		},
		{
			name:    "legacy_only",
			hostKey: legacyWatermarkHostKey,
			want:    []string{legacyWatermarkHostKey},
		},
		{
			name:    "empty_fallback_to_legacy",
			hostKey: "",
			want:    []string{legacyWatermarkHostKey},
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := watermarkLookupHostKeys(tt.hostKey)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("watermarkLookupHostKeys(%q) = %#v, want %#v", tt.hostKey, got, tt.want)
			}
		})
	}
}
