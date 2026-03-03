package ingest

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// MaxBatchEvents 限制单次批量推送事件数，避免单请求占用过多内存与处理时间。
	MaxBatchEvents = 1000
)

// IngestBatch 定义 agent 推送到 ingestion-gateway 的批次结构。
type IngestBatch struct {
	BatchID  string            `json:"batch_id"`
	Agent    AgentInfo         `json:"agent"`
	Source   SourceInfo        `json:"source"`
	Events   []RawEvent        `json:"events"`
	Metadata map[string]string `json:"metadata,omitempty"`
	SentAt   string            `json:"sent_at,omitempty"`
}

// AgentInfo 描述上报事件的 agent 实例身份信息。
type AgentInfo struct {
	AgentID     string `json:"agent_id"`
	TenantID    string `json:"tenant_id,omitempty"`
	WorkspaceID string `json:"workspace_id,omitempty"`
	Hostname    string `json:"hostname,omitempty"`
	Version     string `json:"version,omitempty"`
}

// SourceInfo 描述事件来源信息。
type SourceInfo struct {
	SourceID   string `json:"source_id"`
	Provider   string `json:"provider"`
	SourceType string `json:"source_type,omitempty"`
}

// TokenUsage 描述事件携带的 token 统计。
type TokenUsage struct {
	InputTokens      int64 `json:"input_tokens,omitempty"`
	OutputTokens     int64 `json:"output_tokens,omitempty"`
	CacheReadTokens  int64 `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int64 `json:"cache_write_tokens,omitempty"`
	ReasoningTokens  int64 `json:"reasoning_tokens,omitempty"`
}

// RawEvent 为单条上报事件。
type RawEvent struct {
	EventID      string            `json:"event_id"`
	SessionID    string            `json:"session_id"`
	EventType    string            `json:"event_type"`
	Role         string            `json:"role,omitempty"`
	Text         string            `json:"text,omitempty"`
	Model        string            `json:"model,omitempty"`
	OccurredAt   string            `json:"occurred_at,omitempty"`
	Tokens       TokenUsage        `json:"tokens,omitempty"`
	CostUSD      *float64          `json:"cost_usd,omitempty"`
	CostMode     string            `json:"cost_mode,omitempty"`
	SourcePath   string            `json:"source_path,omitempty"`
	SourceOffset *int64            `json:"source_offset,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	Payload      json.RawMessage   `json:"payload,omitempty"`
}

// RawEnvelope 为发布到 JetStream 的原始事件封装。
type RawEnvelope struct {
	EnvelopeID string     `json:"envelope_id"`
	BatchID    string     `json:"batch_id"`
	AcceptedAt string     `json:"accepted_at"`
	Agent      AgentInfo  `json:"agent"`
	Source     SourceInfo `json:"source"`
	Event      RawEvent   `json:"event"`
	RawHash    string     `json:"raw_hash"`
}

// ValidationError 表示批次校验失败明细。Index 为 -1 时表示批次级错误。
type ValidationError struct {
	Index   int    `json:"index"`
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidateBatch 检查 ingest 批次必填字段与基础格式。
func ValidateBatch(batch IngestBatch) []ValidationError {
	errs := make([]ValidationError, 0)
	if strings.TrimSpace(batch.Agent.AgentID) == "" {
		errs = append(errs, ValidationError{Index: -1, Field: "agent.agent_id", Message: "不能为空"})
	}
	if strings.TrimSpace(batch.Source.SourceID) == "" {
		errs = append(errs, ValidationError{Index: -1, Field: "source.source_id", Message: "不能为空"})
	}
	if strings.TrimSpace(batch.Source.Provider) == "" {
		errs = append(errs, ValidationError{Index: -1, Field: "source.provider", Message: "不能为空"})
	}
	if len(batch.Events) == 0 {
		errs = append(errs, ValidationError{Index: -1, Field: "events", Message: "不能为空"})
	}
	if len(batch.Events) > MaxBatchEvents {
		errs = append(errs, ValidationError{Index: -1, Field: "events", Message: fmt.Sprintf("超过最大限制 %d", MaxBatchEvents)})
	}

	for i, event := range batch.Events {
		if strings.TrimSpace(event.SessionID) == "" {
			errs = append(errs, ValidationError{Index: i, Field: "session_id", Message: "不能为空"})
		}
		if strings.TrimSpace(event.EventType) == "" {
			errs = append(errs, ValidationError{Index: i, Field: "event_type", Message: "不能为空"})
		}
		if raw := strings.TrimSpace(event.OccurredAt); raw != "" {
			if _, err := ParseTimestamp(raw); err != nil {
				errs = append(errs, ValidationError{Index: i, Field: "occurred_at", Message: "时间格式非法"})
			}
		}
	}

	return errs
}

// NormalizeBatch 填充批次中的默认字段。
func NormalizeBatch(batch *IngestBatch, now time.Time) {
	nowUTC := now.UTC()
	if strings.TrimSpace(batch.BatchID) == "" {
		batch.BatchID = NewID("batch")
	}
	if strings.TrimSpace(batch.Source.SourceType) == "" {
		batch.Source.SourceType = "agent"
	}
	if strings.TrimSpace(batch.SentAt) == "" {
		batch.SentAt = nowUTC.Format(time.RFC3339Nano)
	}

	for i := range batch.Events {
		event := &batch.Events[i]
		if strings.TrimSpace(event.EventID) == "" {
			event.EventID = NewID("evt")
		}
		if strings.TrimSpace(event.OccurredAt) == "" {
			event.OccurredAt = nowUTC.Format(time.RFC3339Nano)
		}
		if strings.TrimSpace(event.CostMode) == "" {
			event.CostMode = "reported"
		}
		if strings.TrimSpace(event.SourcePath) == "" {
			event.SourcePath = "agent://push"
		}
		if len(event.Payload) == 0 {
			event.Payload = json.RawMessage("{}")
		}
	}
}

// ParseTimestamp 支持 RFC3339/RFC3339Nano 与 Unix 秒/毫秒时间戳。
func ParseTimestamp(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}

	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, trimmed); err == nil {
			return t.UTC(), nil
		}
	}

	unix, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse timestamp: %w", err)
	}
	if len(trimmed) >= 13 {
		return time.UnixMilli(unix).UTC(), nil
	}
	return time.Unix(unix, 0).UTC(), nil
}

// MarshalEnvelope 序列化 RawEnvelope，并补充 raw_hash。
func MarshalEnvelope(env RawEnvelope) ([]byte, string, error) {
	withoutHash := env
	withoutHash.RawHash = ""

	raw, err := json.Marshal(withoutHash)
	if err != nil {
		return nil, "", fmt.Errorf("marshal envelope: %w", err)
	}

	hash := SHA256Hex(raw)
	withoutHash.RawHash = hash

	finalPayload, err := json.Marshal(withoutHash)
	if err != nil {
		return nil, "", fmt.Errorf("marshal envelope with hash: %w", err)
	}
	return finalPayload, hash, nil
}

// SHA256Hex 输出字节切片的 SHA256 十六进制字符串。
func SHA256Hex(input []byte) string {
	sum := sha256.Sum256(input)
	return hex.EncodeToString(sum[:])
}

// NewID 生成前缀化随机 ID。
func NewID(prefix string) string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
	}
	return fmt.Sprintf("%s_%d_%s", prefix, time.Now().UTC().UnixMilli(), hex.EncodeToString(buf))
}
