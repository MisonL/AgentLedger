using System.Net.Http;
using System.Text;
using System.Text.Json;

namespace AgentLedger.Sdk;

public sealed class OperationCompatibilityRule
{
    public List<PathAlias> PathAliases { get; init; } = new();
    public List<QueryAlias> QueryAliases { get; init; } = new();
    public List<BodyAlias> BodyAliases { get; init; } = new();
}

public sealed class PathAlias
{
    public string Canonical { get; init; } = string.Empty;
    public string Wire { get; init; } = string.Empty;
    public List<string> Aliases { get; init; } = new();
}

public sealed class QueryAlias
{
    public string Canonical { get; init; } = string.Empty;
    public List<string> Aliases { get; init; } = new();
}

public sealed class BodyAlias
{
    public string Canonical { get; init; } = string.Empty;
    public List<string> Aliases { get; init; } = new();
}

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
    private static readonly Dictionary<string, OperationCompatibilityRule> OperationCompatibility = LoadOperationCompatibility();

    private readonly string _baseUrl;
    private readonly string? _token;
    private readonly HttpClient _httpClient;

    public AgentLedgerClient(string baseUrl, string? token = null, HttpClient? httpClient = null)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _token = token;
        _httpClient = httpClient ?? new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
    }

    private static Dictionary<string, OperationCompatibilityRule> LoadOperationCompatibility()
    {
        return JsonSerializer.Deserialize<Dictionary<string, OperationCompatibilityRule>>(
            """
{
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
}
""",
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true }) ?? new Dictionary<string, OperationCompatibilityRule>();
    }

    private static string? ResolveCompatibilityValue(Dictionary<string, string> record, IEnumerable<string> candidates)
    {
        foreach (var candidate in candidates)
        {
            if (record.TryGetValue(candidate, out var value) && !string.IsNullOrWhiteSpace(value))
            {
                return value;
            }
        }

        return null;
    }

    private static object? ResolveCompatibilityValue(Dictionary<string, object?> record, IEnumerable<string> candidates)
    {
        foreach (var candidate in candidates)
        {
            if (!record.TryGetValue(candidate, out var value) || value is null)
            {
                continue;
            }

            if (value is string text && string.IsNullOrWhiteSpace(text))
            {
                continue;
            }

            return value;
        }

        return null;
    }

    private static object? NormalizeBody(object? body, OperationCompatibilityRule rule)
    {
        if (body is null || rule.BodyAliases.Count == 0)
        {
            return body;
        }

        var element = JsonSerializer.SerializeToElement(body);
        if (element.ValueKind != JsonValueKind.Object)
        {
            return body;
        }

        var normalizedBody = JsonSerializer.Deserialize<Dictionary<string, object?>>(element.GetRawText()) ?? new Dictionary<string, object?>();
        foreach (var alias in rule.BodyAliases)
        {
            var value = ResolveCompatibilityValue(normalizedBody, new[] { alias.Canonical }.Concat(alias.Aliases));
            if (value is not null)
            {
                normalizedBody[alias.Canonical] = value;
            }
        }

        return normalizedBody;
    }

    private OperationRequest NormalizeCompatibilityRequest(string operationId, OperationRequest request)
    {
        if (!OperationCompatibility.TryGetValue(operationId, out var rule))
        {
            return request;
        }

        var normalizedPath = new Dictionary<string, string>(request.Path);
        var normalizedQuery = new Dictionary<string, string>(request.Query);
        foreach (var alias in rule.PathAliases)
        {
            var value = ResolveCompatibilityValue(normalizedPath, new[] { alias.Canonical }.Concat(alias.Aliases));
            if (!string.IsNullOrWhiteSpace(value))
            {
                normalizedPath[alias.Wire] = value;
            }
        }
        foreach (var alias in rule.QueryAliases)
        {
            var value = ResolveCompatibilityValue(normalizedQuery, new[] { alias.Canonical }.Concat(alias.Aliases));
            if (!string.IsNullOrWhiteSpace(value))
            {
                normalizedQuery[alias.Canonical] = value;
            }
        }

        return new OperationRequest
        {
            Path = normalizedPath,
            Query = normalizedQuery,
            Body = NormalizeBody(request.Body, rule),
            Headers = new Dictionary<string, string>(request.Headers),
        };
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
        request = this.NormalizeCompatibilityRequest("list_api_keys", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/api-keys", request, cancellationToken);
    }

    // create_api_key | POST /api/v1/api-keys
    public Task<string> CreateApiKey(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_api_key", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v1/api-keys", request, cancellationToken);
    }

    // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    public Task<string> RevokeApiKey(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("revoke_api_key", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v1/api-keys/{id}/revoke", request, cancellationToken);
    }

    // get_open_api_document | GET /api/v1/openapi.json
    public Task<string> GetOpenApiDocument(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_open_api_document", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/openapi.json", request, cancellationToken);
    }

    // create_quality_event | POST /api/v1/quality/events
    public Task<string> CreateQualityEvent(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_quality_event", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v1/quality/events", request, cancellationToken);
    }

    // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    public Task<string> ListQualityDailyMetrics(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_quality_daily_metrics", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/quality/metrics/daily", request, cancellationToken);
    }

    // list_quality_scorecards | GET /api/v1/quality/scorecards
    public Task<string> ListQualityScorecards(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_quality_scorecards", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/quality/scorecards", request, cancellationToken);
    }

    // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    public Task<string> UpdateQualityScorecard(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("update_quality_scorecard", request ?? new OperationRequest());
        return this.RequestAsync("PUT", "/api/v1/quality/scorecards/{id}", request, cancellationToken);
    }

    // list_replay_baselines | GET /api/v1/replay/baselines
    public Task<string> ListReplayBaselines(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_replay_baselines", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/replay/baselines", request, cancellationToken);
    }

    // create_replay_baseline | POST /api/v1/replay/baselines
    public Task<string> CreateReplayBaseline(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_replay_baseline", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v1/replay/baselines", request, cancellationToken);
    }

    // list_replay_jobs | GET /api/v1/replay/jobs
    public Task<string> ListReplayJobs(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_replay_jobs", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/replay/jobs", request, cancellationToken);
    }

    // create_replay_job | POST /api/v1/replay/jobs
    public Task<string> CreateReplayJob(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_replay_job", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v1/replay/jobs", request, cancellationToken);
    }

    // get_replay_job | GET /api/v1/replay/jobs/{id}
    public Task<string> GetReplayJob(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_replay_job", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/replay/jobs/{id}", request, cancellationToken);
    }

    // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    public Task<string> GetReplayJobDiff(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_replay_job_diff", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/replay/jobs/{id}/diff", request, cancellationToken);
    }

    // list_webhook_endpoints | GET /api/v1/webhooks
    public Task<string> ListWebhookEndpoints(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_webhook_endpoints", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/webhooks", request, cancellationToken);
    }

    // create_webhook_endpoint | POST /api/v1/webhooks
    public Task<string> CreateWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_webhook_endpoint", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v1/webhooks", request, cancellationToken);
    }

    // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    public Task<string> UpdateWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("update_webhook_endpoint", request ?? new OperationRequest());
        return this.RequestAsync("PUT", "/api/v1/webhooks/{id}", request, cancellationToken);
    }

    // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    public Task<string> DeleteWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("delete_webhook_endpoint", request ?? new OperationRequest());
        return this.RequestAsync("DELETE", "/api/v1/webhooks/{id}", request, cancellationToken);
    }

    // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    public Task<string> ReplayWebhookEndpoint(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("replay_webhook_endpoint", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v1/webhooks/{id}/replay", request, cancellationToken);
    }

    // list_webhook_replay_tasks | GET /api/v1/webhooks/replay-tasks
    public Task<string> ListWebhookReplayTasks(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_webhook_replay_tasks", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/webhooks/replay-tasks", request, cancellationToken);
    }

    // get_webhook_replay_task | GET /api/v1/webhooks/replay-tasks/{id}
    public Task<string> GetWebhookReplayTask(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_webhook_replay_task", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v1/webhooks/replay-tasks/{id}", request, cancellationToken);
    }

    // create_quality_evaluation_v2 | POST /api/v2/quality/evaluations
    public Task<string> CreateQualityEvaluationV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_quality_evaluation_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/quality/evaluations", request, cancellationToken);
    }

    // list_quality_metrics_v2 | GET /api/v2/quality/metrics
    public Task<string> ListQualityMetricsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_quality_metrics_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/quality/metrics", request, cancellationToken);
    }

    // get_quality_cost_correlation_v2 | GET /api/v2/quality/reports/cost-correlation
    public Task<string> GetQualityCostCorrelationV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_quality_cost_correlation_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/quality/reports/cost-correlation", request, cancellationToken);
    }

    // get_quality_project_trends_v2 | GET /api/v2/quality/reports/project-trends
    public Task<string> GetQualityProjectTrendsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_quality_project_trends_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/quality/reports/project-trends", request, cancellationToken);
    }

    // list_quality_scorecards_v2 | GET /api/v2/quality/scorecards
    public Task<string> ListQualityScorecardsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_quality_scorecards_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/quality/scorecards", request, cancellationToken);
    }

    // update_quality_scorecard_v2 | PUT /api/v2/quality/scorecards/{id}
    public Task<string> UpdateQualityScorecardV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("update_quality_scorecard_v2", request ?? new OperationRequest());
        return this.RequestAsync("PUT", "/api/v2/quality/scorecards/{id}", request, cancellationToken);
    }

    // list_replay_datasets_v2 | GET /api/v2/replay/datasets
    public Task<string> ListReplayDatasetsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_replay_datasets_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/replay/datasets", request, cancellationToken);
    }

    // create_replay_dataset_v2 | POST /api/v2/replay/datasets
    public Task<string> CreateReplayDatasetV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_replay_dataset_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/replay/datasets", request, cancellationToken);
    }

    // list_replay_dataset_cases_v2 | GET /api/v2/replay/datasets/{id}/cases
    public Task<string> ListReplayDatasetCasesV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_replay_dataset_cases_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/replay/datasets/{id}/cases", request, cancellationToken);
    }

    // replace_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/cases
    public Task<string> ReplaceReplayDatasetCasesV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("replace_replay_dataset_cases_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/replay/datasets/{id}/cases", request, cancellationToken);
    }

    // materialize_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/materialize
    public Task<string> MaterializeReplayDatasetCasesV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("materialize_replay_dataset_cases_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/replay/datasets/{id}/materialize", request, cancellationToken);
    }

    // list_replay_runs_v2 | GET /api/v2/replay/runs
    public Task<string> ListReplayRunsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_replay_runs_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/replay/runs", request, cancellationToken);
    }

    // create_replay_run_v2 | POST /api/v2/replay/runs
    public Task<string> CreateReplayRunV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_replay_run_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/replay/runs", request, cancellationToken);
    }

    // get_replay_run_v2 | GET /api/v2/replay/runs/{id}
    public Task<string> GetReplayRunV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_replay_run_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/replay/runs/{id}", request, cancellationToken);
    }

    // get_replay_run_artifacts_v2 | GET /api/v2/replay/runs/{id}/artifacts
    public Task<string> GetReplayRunArtifactsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_replay_run_artifacts_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/replay/runs/{id}/artifacts", request, cancellationToken);
    }

    // download_replay_run_artifact_v2 | GET /api/v2/replay/runs/{id}/artifacts/{artifactType}/download
    public Task<string> DownloadReplayRunArtifactV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("download_replay_run_artifact_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download", request, cancellationToken);
    }

    // get_replay_run_diffs_v2 | GET /api/v2/replay/runs/{id}/diffs
    public Task<string> GetReplayRunDiffsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_replay_run_diffs_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/replay/runs/{id}/diffs", request, cancellationToken);
    }

    // get_residency_policy_v2 | GET /api/v2/residency/policies/current
    public Task<string> GetResidencyPolicyV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("get_residency_policy_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/residency/policies/current", request, cancellationToken);
    }

    // update_residency_policy_v2 | PUT /api/v2/residency/policies/current
    public Task<string> UpdateResidencyPolicyV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("update_residency_policy_v2", request ?? new OperationRequest());
        return this.RequestAsync("PUT", "/api/v2/residency/policies/current", request, cancellationToken);
    }

    // list_residency_region_mappings_v2 | GET /api/v2/residency/region-mappings
    public Task<string> ListResidencyRegionMappingsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_residency_region_mappings_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/residency/region-mappings", request, cancellationToken);
    }

    // list_residency_replications_v2 | GET /api/v2/residency/replications
    public Task<string> ListResidencyReplicationsV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("list_residency_replications_v2", request ?? new OperationRequest());
        return this.RequestAsync("GET", "/api/v2/residency/replications", request, cancellationToken);
    }

    // create_residency_replication_v2 | POST /api/v2/residency/replications
    public Task<string> CreateResidencyReplicationV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("create_residency_replication_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/residency/replications", request, cancellationToken);
    }

    // approve_residency_replication_v2 | POST /api/v2/residency/replications/{id}/approvals
    public Task<string> ApproveResidencyReplicationV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("approve_residency_replication_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/residency/replications/{id}/approvals", request, cancellationToken);
    }

    // cancel_residency_replication_v2 | POST /api/v2/residency/replications/{id}/cancel
    public Task<string> CancelResidencyReplicationV2(OperationRequest? request = null, CancellationToken cancellationToken = default)
    {
        request = this.NormalizeCompatibilityRequest("cancel_residency_replication_v2", request ?? new OperationRequest());
        return this.RequestAsync("POST", "/api/v2/residency/replications/{id}/cancel", request, cancellationToken);
    }
}
