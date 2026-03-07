package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestParseArchiveMode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    archiveMode
		wantErr bool
	}{
		{name: "default", input: "", want: archiveModeLocal},
		{name: "local", input: "local", want: archiveModeLocal},
		{name: "object", input: "object", want: archiveModeObject},
		{name: "hybrid", input: "hybrid", want: archiveModeHybrid},
		{name: "invalid", input: "bad", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArchiveMode(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseArchiveMode(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArchiveMode(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("parseArchiveMode(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDecodeArchiveMessage(t *testing.T) {
	raw := []byte(`{"k":"v"}`)
	payload := []byte(`{"text":"hello"}`)

	msg := []byte(`{
  "tenant":"tenant-a",
  "source":"src-1",
  "session":"ses-1",
  "event":"evt-1",
  "time":"2026-03-02T10:11:12Z",
  "raw":{"k":"v"},
  "payload":{"text":"hello"},
  "metadata":{"batch_id":"batch-1"}
}`)

	job, err := decodeArchiveMessage(msg)
	if err != nil {
		t.Fatalf("decodeArchiveMessage failed: %v", err)
	}
	if job.Tenant != "tenant-a" || job.Source != "src-1" || job.Session != "ses-1" || job.Event != "evt-1" {
		t.Fatalf("decoded identity fields mismatch: %+v", job)
	}
	if !job.OccurredAt.Equal(time.Date(2026, 3, 2, 10, 11, 12, 0, time.UTC)) {
		t.Fatalf("OccurredAt = %v, want %v", job.OccurredAt, time.Date(2026, 3, 2, 10, 11, 12, 0, time.UTC))
	}
	if string(job.RawPayload) != string(raw) {
		t.Fatalf("RawPayload = %s, want %s", string(job.RawPayload), string(raw))
	}
	if string(job.Payload) != string(payload) {
		t.Fatalf("Payload = %s, want %s", string(job.Payload), string(payload))
	}
	if got := job.Metadata["batch_id"]; got != "batch-1" {
		t.Fatalf("Metadata[batch_id] = %v, want batch-1", got)
	}
}

func TestParseArchiveLocalCompression(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    archiveLocalCompression
		wantErr bool
	}{
		{name: "default", input: "", want: archiveLocalCompressionNone},
		{name: "none", input: "none", want: archiveLocalCompressionNone},
		{name: "jsonl alias", input: "jsonl", want: archiveLocalCompressionNone},
		{name: "zstd", input: "zstd", want: archiveLocalCompressionZstd},
		{name: "jsonl plus zstd alias", input: "jsonl+zstd", want: archiveLocalCompressionZstd},
		{name: "invalid", input: "gzip", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArchiveLocalCompression(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseArchiveLocalCompression(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArchiveLocalCompression(%q) unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Fatalf("parseArchiveLocalCompression(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestBuildArchiveRelativeKey(t *testing.T) {
	key := buildArchiveRelativeKey(
		time.Date(2026, 3, 2, 15, 0, 0, 0, time.UTC),
		"tenant/with space",
		"source#1",
		"session:1",
		"evt?1",
		"0123456789abcdef0123456789abcdef",
	)

	if !strings.HasPrefix(key, "2026/03/02/") {
		t.Fatalf("key prefix mismatch: %s", key)
	}
	if !strings.Contains(key, "tenant_with_space") {
		t.Fatalf("tenant segment not sanitized: %s", key)
	}
	if !strings.HasSuffix(key, "evt_1_0123456789abcdef.jsonl") {
		t.Fatalf("filename mismatch: %s", key)
	}
}

func TestLocalArchiveWriterWriteAtomic(t *testing.T) {
	root := t.TempDir()
	writer := &localArchiveWriter{rootDir: root}
	content := []byte(`{"x":1}` + "\n")
	relative := "2026/03/02/tenant/source/session/evt_abc.jsonl"

	path1, err := writer.WriteAtomic(context.Background(), relative, content)
	if err != nil {
		t.Fatalf("WriteAtomic first call failed: %v", err)
	}
	if !strings.HasPrefix(path1, root) {
		t.Fatalf("unexpected archive path: %s", path1)
	}
	if filepath.Clean(path1) != filepath.Clean(filepath.Join(root, filepath.FromSlash(relative))) {
		t.Fatalf("resolved path mismatch: %s", path1)
	}

	data, err := os.ReadFile(path1)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf("file content mismatch: %q vs %q", string(data), string(content))
	}

	path2, err := writer.WriteAtomic(context.Background(), relative, content)
	if err != nil {
		t.Fatalf("WriteAtomic second call failed: %v", err)
	}
	if filepath.Clean(path1) != filepath.Clean(path2) {
		t.Fatalf("path mismatch on duplicate write: %s vs %s", path1, path2)
	}
}

type stubObjectArchiveWriter struct {
	keys     []string
	contents [][]byte
}

func (w *stubObjectArchiveWriter) PutObject(_ context.Context, key string, content []byte) error {
	w.keys = append(w.keys, key)
	w.contents = append(w.contents, append([]byte(nil), content...))
	return nil
}

func TestArchiverArchiveHybridWritesLocalZstdAndObjectJSONL(t *testing.T) {
	root := t.TempDir()
	objectWriter := &stubObjectArchiveWriter{}
	svc := &archiverService{
		mode: archiveModeHybrid,
		localWriter: &localArchiveWriter{
			rootDir:     root,
			compression: archiveLocalCompressionZstd,
		},
		objectWriter:  objectWriter,
		objectBackend: "s3",
		objectPrefix:  "agentledger/archive/raw",
	}

	job := archiveJob{
		Tenant:     "tenant-a",
		Source:     "source-1",
		Session:    "session-1",
		Event:      "evt-1",
		OccurredAt: time.Date(2026, 3, 2, 10, 11, 12, 0, time.UTC),
		RawPayload: []byte(`{"id":"evt-1","ok":true}`),
	}

	records, err := svc.archive(context.Background(), job)
	if err != nil {
		t.Fatalf("archive failed: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("archive record count = %d, want 2", len(records))
	}
	if len(objectWriter.keys) != 1 || len(objectWriter.contents) != 1 {
		t.Fatalf("unexpected object writes: keys=%d contents=%d", len(objectWriter.keys), len(objectWriter.contents))
	}

	localRecord := records[0]
	objectRecord := records[1]
	expectedLine := buildJSONLLine(job.RawPayload)

	if localRecord.Backend != "local" {
		t.Fatalf("local backend = %s, want local", localRecord.Backend)
	}
	if !strings.HasSuffix(localRecord.ObjectKey, ".jsonl.zst") {
		t.Fatalf("local archive path = %s, want .jsonl.zst suffix", localRecord.ObjectKey)
	}
	localCompressed, err := os.ReadFile(localRecord.ObjectKey)
	if err != nil {
		t.Fatalf("ReadFile local archive failed: %v", err)
	}
	if int64(len(localCompressed)) != localRecord.SizeBytes {
		t.Fatalf("local size_bytes = %d, want %d", localRecord.SizeBytes, len(localCompressed))
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer decoder.Close()

	localDecoded, err := decoder.DecodeAll(localCompressed, nil)
	if err != nil {
		t.Fatalf("DecodeAll local archive failed: %v", err)
	}
	if !bytes.Equal(localDecoded, expectedLine) {
		t.Fatalf("decoded local archive mismatch: got %q want %q", string(localDecoded), string(expectedLine))
	}

	if objectRecord.Backend != "s3" {
		t.Fatalf("object backend = %s, want s3", objectRecord.Backend)
	}
	if !strings.HasSuffix(objectRecord.ObjectKey, ".jsonl") {
		t.Fatalf("object key = %s, want .jsonl suffix", objectRecord.ObjectKey)
	}
	if objectWriter.keys[0] != objectRecord.ObjectKey {
		t.Fatalf("object key mismatch: writer=%s record=%s", objectWriter.keys[0], objectRecord.ObjectKey)
	}
	if !bytes.Equal(objectWriter.contents[0], expectedLine) {
		t.Fatalf("object content mismatch: got %q want %q", string(objectWriter.contents[0]), string(expectedLine))
	}
	if objectRecord.SizeBytes != int64(len(expectedLine)) {
		t.Fatalf("object size_bytes = %d, want %d", objectRecord.SizeBytes, len(expectedLine))
	}
}
