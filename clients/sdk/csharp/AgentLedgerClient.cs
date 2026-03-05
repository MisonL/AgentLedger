using System.Net.Http;
using System.Text;
using System.Text.Json;

namespace AgentLedger.Sdk;

public sealed class OperationRequest
{
    public Dictionary<string, string> Path { get; init; } = new();
    public Dictionary<string, string> Query { get; init; } = new();
    public object? Body { get; init; }
    public Dictionary<string, string> Headers { get; init; } = new();
}

public sealed class AgentLedgerApiError : Exception
{
    public int Status { get; }
    public string Payload { get; }

    public AgentLedgerApiError(int status, string message, string payload) : base(message)
    {
        Status = status;
        Payload = payload;
    }
}

public sealed class AgentLedgerClient
{
    private readonly string _baseUrl;
    private readonly string? _token;
    private readonly HttpClient _httpClient;

    public AgentLedgerClient(string baseUrl, string? token = null, HttpClient? httpClient = null)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _token = token;
        _httpClient = httpClient ?? new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
    }

    private string RenderPath(string pathTemplate, Dictionary<string, string> pathParams)
    {
        var result = pathTemplate;
        foreach (var key in pathParams.Keys)
        {
            result = result.Replace($"{{{key}}}", Uri.EscapeDataString(pathParams[key]));
        }

        if (result.Contains('{') || result.Contains('}'))
        {
            throw new InvalidOperationException($"缺少 path 参数: {result}");
        }

        return result;
    }

    private static string BuildQuery(Dictionary<string, string> query)
    {
        if (query.Count == 0)
        {
            return string.Empty;
        }

        var pairs = query
            .Where(item => !string.IsNullOrWhiteSpace(item.Value))
            .Select(item => $"{Uri.EscapeDataString(item.Key)}={Uri.EscapeDataString(item.Value)}")
            .ToArray();
        return pairs.Length == 0 ? string.Empty : $"?{string.Join("&", pairs)}";
    }

    private async Task<string> RequestAsync(string method, string pathTemplate, OperationRequest request, CancellationToken cancellationToken)
    {
        var url = _baseUrl + RenderPath(pathTemplate, request.Path) + BuildQuery(request.Query);
        using var httpRequest = new HttpRequestMessage(new HttpMethod(method), url);
        httpRequest.Headers.TryAddWithoutValidation("content-type", "application/json");

        if (!string.IsNullOrWhiteSpace(_token))
        {
            httpRequest.Headers.TryAddWithoutValidation("authorization", $"Bearer {_token}");
        }
        foreach (var header in request.Headers)
        {
            httpRequest.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }

        if (request.Body is not null)
        {
            var json = JsonSerializer.Serialize(request.Body);
            httpRequest.Content = new StringContent(json, Encoding.UTF8, "application/json");
        }

        using var response = await _httpClient.SendAsync(httpRequest, cancellationToken);
        var payload = await response.Content.ReadAsStringAsync(cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            throw new AgentLedgerApiError((int)response.StatusCode, $"request failed: {(int)response.StatusCode}", payload);
        }

        return payload;
    }

    // list_api_keys | GET /api/v1/api-keys
    public Task<string> ListApiKeys(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/api-keys", request, cancellationToken);
    }

    // create_api_key | POST /api/v1/api-keys
    public Task<string> CreateApiKey(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("POST", "/api/v1/api-keys", request, cancellationToken);
    }

    // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    public Task<string> RevokeApiKey(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("POST", "/api/v1/api-keys/{id}/revoke", request, cancellationToken);
    }

    // get_open_api_document | GET /api/v1/openapi.json
    public Task<string> GetOpenApiDocument(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/openapi.json", request, cancellationToken);
    }

    // create_quality_event | POST /api/v1/quality/events
    public Task<string> CreateQualityEvent(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("POST", "/api/v1/quality/events", request, cancellationToken);
    }

    // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    public Task<string> ListQualityDailyMetrics(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/quality/metrics/daily", request, cancellationToken);
    }

    // list_quality_scorecards | GET /api/v1/quality/scorecards
    public Task<string> ListQualityScorecards(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/quality/scorecards", request, cancellationToken);
    }

    // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    public Task<string> UpdateQualityScorecard(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("PUT", "/api/v1/quality/scorecards/{id}", request, cancellationToken);
    }

    // list_replay_baselines | GET /api/v1/replay/baselines
    public Task<string> ListReplayBaselines(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/replay/baselines", request, cancellationToken);
    }

    // create_replay_baseline | POST /api/v1/replay/baselines
    public Task<string> CreateReplayBaseline(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("POST", "/api/v1/replay/baselines", request, cancellationToken);
    }

    // list_replay_jobs | GET /api/v1/replay/jobs
    public Task<string> ListReplayJobs(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/replay/jobs", request, cancellationToken);
    }

    // create_replay_job | POST /api/v1/replay/jobs
    public Task<string> CreateReplayJob(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("POST", "/api/v1/replay/jobs", request, cancellationToken);
    }

    // get_replay_job | GET /api/v1/replay/jobs/{id}
    public Task<string> GetReplayJob(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/replay/jobs/{id}", request, cancellationToken);
    }

    // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    public Task<string> GetReplayJobDiff(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/replay/jobs/{id}/diff", request, cancellationToken);
    }

    // list_webhook_endpoints | GET /api/v1/webhooks
    public Task<string> ListWebhookEndpoints(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("GET", "/api/v1/webhooks", request, cancellationToken);
    }

    // create_webhook_endpoint | POST /api/v1/webhooks
    public Task<string> CreateWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("POST", "/api/v1/webhooks", request, cancellationToken);
    }

    // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    public Task<string> UpdateWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("PUT", "/api/v1/webhooks/{id}", request, cancellationToken);
    }

    // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    public Task<string> DeleteWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("DELETE", "/api/v1/webhooks/{id}", request, cancellationToken);
    }

    // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    public Task<string> ReplayWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request ??= new OperationRequest();
        return this.RequestAsync("POST", "/api/v1/webhooks/{id}/replay", request, cancellationToken);
    }
}
