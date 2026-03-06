package agentledgersdk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const operationCompatibilityJSON = `{
  "list_replay_dataset_cases_v2": {
    "pathAliases": [
      {
        "canonical": "datasetId",
        "wire": "id",
        "aliases": [
          "id",
          "baselineId"
        ]
      }
    ]
  },
  "replace_replay_dataset_cases_v2": {
    "pathAliases": [
      {
        "canonical": "datasetId",
        "wire": "id",
        "aliases": [
          "id",
          "baselineId"
        ]
      }
    ]
  },
  "materialize_replay_dataset_cases_v2": {
    "pathAliases": [
      {
        "canonical": "datasetId",
        "wire": "id",
        "aliases": [
          "id",
          "baselineId"
        ]
      }
    ]
  },
  "list_replay_runs_v2": {
    "queryAliases": [
      {
        "canonical": "datasetId",
        "aliases": [
          "baselineId"
        ]
      }
    ]
  },
  "create_replay_run_v2": {
    "bodyAliases": [
      {
        "canonical": "datasetId",
        "aliases": [
          "baselineId"
        ]
      }
    ]
  },
  "get_replay_run_v2": {
    "pathAliases": [
      {
        "canonical": "runId",
        "wire": "id",
        "aliases": [
          "id",
          "jobId"
        ]
      }
    ]
  },
  "get_replay_run_artifacts_v2": {
    "pathAliases": [
      {
        "canonical": "runId",
        "wire": "id",
        "aliases": [
          "id",
          "jobId"
        ]
      }
    ]
  },
  "download_replay_run_artifact_v2": {
    "pathAliases": [
      {
        "canonical": "runId",
        "wire": "id",
        "aliases": [
          "id",
          "jobId"
        ]
      }
    ]
  },
  "get_replay_run_diffs_v2": {
    "pathAliases": [
      {
        "canonical": "runId",
        "wire": "id",
        "aliases": [
          "id",
          "jobId"
        ]
      }
    ],
    "queryAliases": [
      {
        "canonical": "datasetId",
        "aliases": [
          "baselineId"
        ]
      }
    ]
  }
}`

type operationCompatibilityRule struct {
	PathAliases  []operationPathAlias  `json:"pathAliases"`
	QueryAliases []operationQueryAlias `json:"queryAliases"`
	BodyAliases  []operationBodyAlias  `json:"bodyAliases"`
}

type operationPathAlias struct {
	Canonical string   `json:"canonical"`
	Wire      string   `json:"wire"`
	Aliases   []string `json:"aliases"`
}

type operationQueryAlias struct {
	Canonical string   `json:"canonical"`
	Aliases   []string `json:"aliases"`
}

type operationBodyAlias struct {
	Canonical string   `json:"canonical"`
	Aliases   []string `json:"aliases"`
}

var operationCompatibility = mustLoadOperationCompatibility()

func mustLoadOperationCompatibility() map[string]operationCompatibilityRule {
	rules := map[string]operationCompatibilityRule{}
	if err := json.Unmarshal([]byte(operationCompatibilityJSON), &rules); err != nil {
		panic(err)
	}
	return rules
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func resolveStringCompatibilityValue(record map[string]string, candidates []string) (string, bool) {
	for _, candidate := range candidates {
		if value, ok := record[candidate]; ok && strings.TrimSpace(value) != "" {
			return value, true
		}
	}
	return "", false
}

func resolveAnyCompatibilityValue(record map[string]any, candidates []string) (any, bool) {
	for _, candidate := range candidates {
		value, ok := record[candidate]
		if !ok || value == nil {
			continue
		}
		if text, ok := value.(string); ok && strings.TrimSpace(text) == "" {
			continue
		}
		return value, true
	}
	return nil, false
}

func normalizeCompatibilityBody(body any, rule operationCompatibilityRule) any {
	if len(rule.BodyAliases) == 0 || body == nil {
		return body
	}

	switch typed := body.(type) {
	case map[string]any:
		normalized := cloneAnyMap(typed)
		for _, alias := range rule.BodyAliases {
			candidates := append([]string{alias.Canonical}, alias.Aliases...)
			if value, ok := resolveAnyCompatibilityValue(normalized, candidates); ok {
				normalized[alias.Canonical] = value
			}
		}
		return normalized
	case map[string]string:
		normalized := make(map[string]any, len(typed))
		for key, value := range typed {
			normalized[key] = value
		}
		for _, alias := range rule.BodyAliases {
			candidates := append([]string{alias.Canonical}, alias.Aliases...)
			if value, ok := resolveAnyCompatibilityValue(normalized, candidates); ok {
				normalized[alias.Canonical] = value
			}
		}
		return normalized
	default:
		return body
	}
}

// ApiError 表示 HTTP 失败响应。
type ApiError struct {
	Status int
	Message string
	Payload json.RawMessage
}

func (e *ApiError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

// OperationRequest 表示单次 API 调用请求。
type OperationRequest struct {
	PathParams map[string]string
	Query      map[string]string
	Body       any
	Headers    map[string]string
}

// Client 表示 AgentLedger 控制面客户端。
type Client struct {
	BaseURL    string
	Token      string
	HTTPClient *http.Client
}

func NewClient(baseURL string, token string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		Token:   token,
		HTTPClient: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *Client) normalizeCompatibilityRequest(operationID string, req *OperationRequest) *OperationRequest {
	rule, ok := operationCompatibility[operationID]
	if !ok {
		return req
	}

	pathParams := cloneStringMap(req.PathParams)
	queryParams := cloneStringMap(req.Query)
	for _, alias := range rule.PathAliases {
		candidates := append([]string{alias.Canonical}, alias.Aliases...)
		if value, found := resolveStringCompatibilityValue(pathParams, candidates); found {
			pathParams[alias.Wire] = value
		}
	}
	for _, alias := range rule.QueryAliases {
		candidates := append([]string{alias.Canonical}, alias.Aliases...)
		if value, found := resolveStringCompatibilityValue(queryParams, candidates); found {
			queryParams[alias.Canonical] = value
		}
	}

	return &OperationRequest{
		PathParams: pathParams,
		Query:      queryParams,
		Body:       normalizeCompatibilityBody(req.Body, rule),
		Headers:    cloneStringMap(req.Headers),
	}
}

func (c *Client) renderPath(pathTemplate string, pathParams map[string]string) (string, error) {
	pathValue := pathTemplate
	for {
		start := strings.Index(pathValue, "{")
		if start < 0 {
			break
		}
		end := strings.Index(pathValue[start:], "}")
		if end < 0 {
			break
		}
		end += start
		key := strings.TrimSpace(pathValue[start+1 : end])
		if key == "" {
			break
		}
		value, ok := pathParams[key]
		if !ok {
			return "", fmt.Errorf("missing path param: %s", key)
		}
		pathValue = strings.ReplaceAll(pathValue, "{"+key+"}", url.PathEscape(value))
	}
	return pathValue, nil
}

func (c *Client) request(ctx context.Context, method string, pathTemplate string, req *OperationRequest) (json.RawMessage, error) {
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	pathValue, err := c.renderPath(pathTemplate, req.PathParams)
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(c.BaseURL + pathValue)
	if err != nil {
		return nil, err
	}
	query := u.Query()
	for key, value := range req.Query {
		if value == "" {
			continue
		}
		query.Set(key, value)
	}
	u.RawQuery = query.Encode()

	var bodyReader io.Reader
	if req.Body != nil {
		raw, marshalErr := json.Marshal(req.Body)
		if marshalErr != nil {
			return nil, marshalErr
		}
		bodyReader = bytes.NewReader(raw)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("content-type", "application/json")
	if c.Token != "" {
		httpReq.Header.Set("authorization", "Bearer "+c.Token)
	}
	for key, value := range req.Headers {
		httpReq.Header.Set(key, value)
	}

	response, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message := fmt.Sprintf("request failed: %d", response.StatusCode)
		return nil, &ApiError{
			Status:  response.StatusCode,
			Message: message,
			Payload: payload,
		}
	}

	return payload, nil
}

// list_api_keys | GET /api/v1/api-keys
func (c *Client) ListApiKeys(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_api_keys", req)
	return c.request(ctx, "GET", "/api/v1/api-keys", resolved)
}

// create_api_key | POST /api/v1/api-keys
func (c *Client) CreateApiKey(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_api_key", req)
	return c.request(ctx, "POST", "/api/v1/api-keys", resolved)
}

// revoke_api_key | POST /api/v1/api-keys/{id}/revoke
func (c *Client) RevokeApiKey(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("revoke_api_key", req)
	return c.request(ctx, "POST", "/api/v1/api-keys/{id}/revoke", resolved)
}

// get_open_api_document | GET /api/v1/openapi.json
func (c *Client) GetOpenApiDocument(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_open_api_document", req)
	return c.request(ctx, "GET", "/api/v1/openapi.json", resolved)
}

// create_quality_event | POST /api/v1/quality/events
func (c *Client) CreateQualityEvent(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_quality_event", req)
	return c.request(ctx, "POST", "/api/v1/quality/events", resolved)
}

// list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
func (c *Client) ListQualityDailyMetrics(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_quality_daily_metrics", req)
	return c.request(ctx, "GET", "/api/v1/quality/metrics/daily", resolved)
}

// list_quality_scorecards | GET /api/v1/quality/scorecards
func (c *Client) ListQualityScorecards(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_quality_scorecards", req)
	return c.request(ctx, "GET", "/api/v1/quality/scorecards", resolved)
}

// update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
func (c *Client) UpdateQualityScorecard(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("update_quality_scorecard", req)
	return c.request(ctx, "PUT", "/api/v1/quality/scorecards/{id}", resolved)
}

// list_replay_baselines | GET /api/v1/replay/baselines
func (c *Client) ListReplayBaselines(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_replay_baselines", req)
	return c.request(ctx, "GET", "/api/v1/replay/baselines", resolved)
}

// create_replay_baseline | POST /api/v1/replay/baselines
func (c *Client) CreateReplayBaseline(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_replay_baseline", req)
	return c.request(ctx, "POST", "/api/v1/replay/baselines", resolved)
}

// list_replay_jobs | GET /api/v1/replay/jobs
func (c *Client) ListReplayJobs(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_replay_jobs", req)
	return c.request(ctx, "GET", "/api/v1/replay/jobs", resolved)
}

// create_replay_job | POST /api/v1/replay/jobs
func (c *Client) CreateReplayJob(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_replay_job", req)
	return c.request(ctx, "POST", "/api/v1/replay/jobs", resolved)
}

// get_replay_job | GET /api/v1/replay/jobs/{id}
func (c *Client) GetReplayJob(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_replay_job", req)
	return c.request(ctx, "GET", "/api/v1/replay/jobs/{id}", resolved)
}

// get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
func (c *Client) GetReplayJobDiff(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_replay_job_diff", req)
	return c.request(ctx, "GET", "/api/v1/replay/jobs/{id}/diff", resolved)
}

// list_webhook_endpoints | GET /api/v1/webhooks
func (c *Client) ListWebhookEndpoints(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_webhook_endpoints", req)
	return c.request(ctx, "GET", "/api/v1/webhooks", resolved)
}

// create_webhook_endpoint | POST /api/v1/webhooks
func (c *Client) CreateWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_webhook_endpoint", req)
	return c.request(ctx, "POST", "/api/v1/webhooks", resolved)
}

// update_webhook_endpoint | PUT /api/v1/webhooks/{id}
func (c *Client) UpdateWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("update_webhook_endpoint", req)
	return c.request(ctx, "PUT", "/api/v1/webhooks/{id}", resolved)
}

// delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
func (c *Client) DeleteWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("delete_webhook_endpoint", req)
	return c.request(ctx, "DELETE", "/api/v1/webhooks/{id}", resolved)
}

// replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
func (c *Client) ReplayWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("replay_webhook_endpoint", req)
	return c.request(ctx, "POST", "/api/v1/webhooks/{id}/replay", resolved)
}

// list_webhook_replay_tasks | GET /api/v1/webhooks/replay-tasks
func (c *Client) ListWebhookReplayTasks(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_webhook_replay_tasks", req)
	return c.request(ctx, "GET", "/api/v1/webhooks/replay-tasks", resolved)
}

// get_webhook_replay_task | GET /api/v1/webhooks/replay-tasks/{id}
func (c *Client) GetWebhookReplayTask(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_webhook_replay_task", req)
	return c.request(ctx, "GET", "/api/v1/webhooks/replay-tasks/{id}", resolved)
}

// create_quality_evaluation_v2 | POST /api/v2/quality/evaluations
func (c *Client) CreateQualityEvaluationV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_quality_evaluation_v2", req)
	return c.request(ctx, "POST", "/api/v2/quality/evaluations", resolved)
}

// list_quality_metrics_v2 | GET /api/v2/quality/metrics
func (c *Client) ListQualityMetricsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_quality_metrics_v2", req)
	return c.request(ctx, "GET", "/api/v2/quality/metrics", resolved)
}

// get_quality_cost_correlation_v2 | GET /api/v2/quality/reports/cost-correlation
func (c *Client) GetQualityCostCorrelationV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_quality_cost_correlation_v2", req)
	return c.request(ctx, "GET", "/api/v2/quality/reports/cost-correlation", resolved)
}

// get_quality_project_trends_v2 | GET /api/v2/quality/reports/project-trends
func (c *Client) GetQualityProjectTrendsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_quality_project_trends_v2", req)
	return c.request(ctx, "GET", "/api/v2/quality/reports/project-trends", resolved)
}

// list_quality_scorecards_v2 | GET /api/v2/quality/scorecards
func (c *Client) ListQualityScorecardsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_quality_scorecards_v2", req)
	return c.request(ctx, "GET", "/api/v2/quality/scorecards", resolved)
}

// update_quality_scorecard_v2 | PUT /api/v2/quality/scorecards/{id}
func (c *Client) UpdateQualityScorecardV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("update_quality_scorecard_v2", req)
	return c.request(ctx, "PUT", "/api/v2/quality/scorecards/{id}", resolved)
}

// list_replay_datasets_v2 | GET /api/v2/replay/datasets
func (c *Client) ListReplayDatasetsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_replay_datasets_v2", req)
	return c.request(ctx, "GET", "/api/v2/replay/datasets", resolved)
}

// create_replay_dataset_v2 | POST /api/v2/replay/datasets
func (c *Client) CreateReplayDatasetV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_replay_dataset_v2", req)
	return c.request(ctx, "POST", "/api/v2/replay/datasets", resolved)
}

// list_replay_dataset_cases_v2 | GET /api/v2/replay/datasets/{id}/cases
func (c *Client) ListReplayDatasetCasesV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_replay_dataset_cases_v2", req)
	return c.request(ctx, "GET", "/api/v2/replay/datasets/{id}/cases", resolved)
}

// replace_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/cases
func (c *Client) ReplaceReplayDatasetCasesV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("replace_replay_dataset_cases_v2", req)
	return c.request(ctx, "POST", "/api/v2/replay/datasets/{id}/cases", resolved)
}

// materialize_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/materialize
func (c *Client) MaterializeReplayDatasetCasesV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("materialize_replay_dataset_cases_v2", req)
	return c.request(ctx, "POST", "/api/v2/replay/datasets/{id}/materialize", resolved)
}

// list_replay_runs_v2 | GET /api/v2/replay/runs
func (c *Client) ListReplayRunsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_replay_runs_v2", req)
	return c.request(ctx, "GET", "/api/v2/replay/runs", resolved)
}

// create_replay_run_v2 | POST /api/v2/replay/runs
func (c *Client) CreateReplayRunV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_replay_run_v2", req)
	return c.request(ctx, "POST", "/api/v2/replay/runs", resolved)
}

// get_replay_run_v2 | GET /api/v2/replay/runs/{id}
func (c *Client) GetReplayRunV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_replay_run_v2", req)
	return c.request(ctx, "GET", "/api/v2/replay/runs/{id}", resolved)
}

// get_replay_run_artifacts_v2 | GET /api/v2/replay/runs/{id}/artifacts
func (c *Client) GetReplayRunArtifactsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_replay_run_artifacts_v2", req)
	return c.request(ctx, "GET", "/api/v2/replay/runs/{id}/artifacts", resolved)
}

// download_replay_run_artifact_v2 | GET /api/v2/replay/runs/{id}/artifacts/{artifactType}/download
func (c *Client) DownloadReplayRunArtifactV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("download_replay_run_artifact_v2", req)
	return c.request(ctx, "GET", "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download", resolved)
}

// get_replay_run_diffs_v2 | GET /api/v2/replay/runs/{id}/diffs
func (c *Client) GetReplayRunDiffsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_replay_run_diffs_v2", req)
	return c.request(ctx, "GET", "/api/v2/replay/runs/{id}/diffs", resolved)
}

// get_residency_policy_v2 | GET /api/v2/residency/policies/current
func (c *Client) GetResidencyPolicyV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("get_residency_policy_v2", req)
	return c.request(ctx, "GET", "/api/v2/residency/policies/current", resolved)
}

// update_residency_policy_v2 | PUT /api/v2/residency/policies/current
func (c *Client) UpdateResidencyPolicyV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("update_residency_policy_v2", req)
	return c.request(ctx, "PUT", "/api/v2/residency/policies/current", resolved)
}

// list_residency_region_mappings_v2 | GET /api/v2/residency/region-mappings
func (c *Client) ListResidencyRegionMappingsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_residency_region_mappings_v2", req)
	return c.request(ctx, "GET", "/api/v2/residency/region-mappings", resolved)
}

// list_residency_replications_v2 | GET /api/v2/residency/replications
func (c *Client) ListResidencyReplicationsV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("list_residency_replications_v2", req)
	return c.request(ctx, "GET", "/api/v2/residency/replications", resolved)
}

// create_residency_replication_v2 | POST /api/v2/residency/replications
func (c *Client) CreateResidencyReplicationV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("create_residency_replication_v2", req)
	return c.request(ctx, "POST", "/api/v2/residency/replications", resolved)
}

// approve_residency_replication_v2 | POST /api/v2/residency/replications/{id}/approvals
func (c *Client) ApproveResidencyReplicationV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("approve_residency_replication_v2", req)
	return c.request(ctx, "POST", "/api/v2/residency/replications/{id}/approvals", resolved)
}

// cancel_residency_replication_v2 | POST /api/v2/residency/replications/{id}/cancel
func (c *Client) CancelResidencyReplicationV2(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	resolved := c.normalizeCompatibilityRequest("cancel_residency_replication_v2", req)
	return c.request(ctx, "POST", "/api/v2/residency/replications/{id}/cancel", resolved)
}
