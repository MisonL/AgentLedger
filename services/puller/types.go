package main

import (
	"errors"
	"strconv"
	"time"

	"github.com/agentledger/agentledger/services/internal/shared/ingest"
)

const (
	defaultIngestEndpoint = "http://127.0.0.1:8081/v1/ingest"
	parserKeyJSONL        = "jsonl"
	parserKeyNative       = "native"
)

var errJobCancelled = errors.New("sync job cancelled")

type pullerRuntimeConfig struct {
	PollInterval          time.Duration
	JobTimeout            time.Duration
	JobMaxRetries         int
	JobRetryBaseDelay     time.Duration
	SSHTimeout            time.Duration
	IngestTimeout         time.Duration
	IngestEndpoint        string
	IngestBearer          string
	AgentID               string
	InternalToken         string
	ResidencyTargetRegion string
}

type syncJob struct {
	ID              string
	SourceID        string
	Mode            string
	Status          string
	Attempt         int
	StartedAt       time.Time
	CancelRequested bool
}

type sourceRecord struct {
	ID           string
	Name         string
	Type         string
	Location     string
	SourceRegion string
	Enabled      bool
	Provider     string
	Hostname     string
	TenantID     string
	WorkspaceID  string
	Metadata     map[string]any
}

type scheduledSource struct {
	ID         string
	SyncCron   string
	AccessMode string
}

type sshLocation struct {
	User string
	Host string
	Port int
	Path string
}

func (l sshLocation) Target() string {
	if l.User == "" {
		return l.Host
	}
	return l.User + "@" + l.Host
}

func (l sshLocation) HostKey() string {
	port := l.Port
	if port <= 0 {
		port = 22
	}
	return l.Target() + ":" + strconv.Itoa(port) + ":" + l.Path
}

type lineRecord struct {
	No   int64
	Text string
}

type parserOutput struct {
	ParserKey string
	Events    []rawEventWithLine
	Failures  []parseFailure
	MaxLine   int64
}

type rawEventWithLine struct {
	LineNo int64
	Event  ingest.RawEvent
}

type parseFailure struct {
	ParserKey    string
	SourcePath   string
	SourceOffset int64
	Error        string
}

type syncJobResult struct {
	JobID       string
	SourceID    string
	Status      string
	ErrorCode   string
	ErrorDetail string
	Attempt     int
}
