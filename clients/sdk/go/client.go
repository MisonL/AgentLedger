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
	return c.request(ctx, "GET", "/api/v1/api-keys", req)
}

// create_api_key | POST /api/v1/api-keys
func (c *Client) CreateApiKey(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "POST", "/api/v1/api-keys", req)
}

// revoke_api_key | POST /api/v1/api-keys/{id}/revoke
func (c *Client) RevokeApiKey(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "POST", "/api/v1/api-keys/{id}/revoke", req)
}

// get_open_api_document | GET /api/v1/openapi.json
func (c *Client) GetOpenApiDocument(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/openapi.json", req)
}

// create_quality_event | POST /api/v1/quality/events
func (c *Client) CreateQualityEvent(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "POST", "/api/v1/quality/events", req)
}

// list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
func (c *Client) ListQualityDailyMetrics(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/quality/metrics/daily", req)
}

// list_quality_scorecards | GET /api/v1/quality/scorecards
func (c *Client) ListQualityScorecards(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/quality/scorecards", req)
}

// update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
func (c *Client) UpdateQualityScorecard(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "PUT", "/api/v1/quality/scorecards/{id}", req)
}

// list_replay_baselines | GET /api/v1/replay/baselines
func (c *Client) ListReplayBaselines(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/replay/baselines", req)
}

// create_replay_baseline | POST /api/v1/replay/baselines
func (c *Client) CreateReplayBaseline(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "POST", "/api/v1/replay/baselines", req)
}

// list_replay_jobs | GET /api/v1/replay/jobs
func (c *Client) ListReplayJobs(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/replay/jobs", req)
}

// create_replay_job | POST /api/v1/replay/jobs
func (c *Client) CreateReplayJob(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "POST", "/api/v1/replay/jobs", req)
}

// get_replay_job | GET /api/v1/replay/jobs/{id}
func (c *Client) GetReplayJob(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/replay/jobs/{id}", req)
}

// get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
func (c *Client) GetReplayJobDiff(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/replay/jobs/{id}/diff", req)
}

// list_webhook_endpoints | GET /api/v1/webhooks
func (c *Client) ListWebhookEndpoints(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "GET", "/api/v1/webhooks", req)
}

// create_webhook_endpoint | POST /api/v1/webhooks
func (c *Client) CreateWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "POST", "/api/v1/webhooks", req)
}

// update_webhook_endpoint | PUT /api/v1/webhooks/{id}
func (c *Client) UpdateWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "PUT", "/api/v1/webhooks/{id}", req)
}

// delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
func (c *Client) DeleteWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "DELETE", "/api/v1/webhooks/{id}", req)
}

// replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
func (c *Client) ReplayWebhookEndpoint(ctx context.Context, req *OperationRequest) (json.RawMessage, error) {
	if req == nil {
		req = &OperationRequest{}
	}
	return c.request(ctx, "POST", "/api/v1/webhooks/{id}/replay", req)
}
