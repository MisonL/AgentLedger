package com.agentledger.sdk;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class AgentLedgerClient {
  private record PathAlias(String canonical, String wire, String[] aliases) {}

  private record QueryAlias(String canonical, String[] aliases) {}

  private record BodyAlias(String canonical, String[] aliases) {}

  public static class OperationRequest {
    public Map<String, String> path = new LinkedHashMap<>();
    public Map<String, String> query = new LinkedHashMap<>();
    public String body;
    public Map<String, String> headers = new LinkedHashMap<>();
  }

  public static class ApiError extends RuntimeException {
    public final int status;
    public final String payload;

    public ApiError(int status, String message, String payload) {
      super(message);
      this.status = status;
      this.payload = payload;
    }
  }

  private final String baseUrl;
  private final String token;
  private final HttpClient httpClient;

  public AgentLedgerClient(String baseUrl, String token) {
    this.baseUrl = baseUrl.replaceAll("/+$", "");
    this.token = token;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  private String resolveCompatibilityValue(Map<String, String> record, String canonical, String[] aliases) {
    String canonicalValue = record.get(canonical);
    if (canonicalValue != null && !canonicalValue.isBlank()) {
      return canonicalValue;
    }
    for (String alias : aliases) {
      String compatibilityValue = record.get(alias);
      if (compatibilityValue != null && !compatibilityValue.isBlank()) {
        return compatibilityValue;
      }
    }
    return null;
  }

  private String normalizeStringBody(String body, BodyAlias[] bodyAliases) {
    if (body == null || body.isBlank()) {
      return body;
    }
    String normalized = body;
    for (BodyAlias alias : bodyAliases) {
      String canonicalToken = "\"" + alias.canonical() + "\"";
      if (normalized.contains(canonicalToken)) {
        continue;
      }
      for (String compatibilityName : alias.aliases()) {
        String compatibilityToken = "\"" + compatibilityName + "\"";
        if (normalized.contains(compatibilityToken)) {
          normalized = normalized.replace(compatibilityToken, canonicalToken);
          break;
        }
      }
    }
    return normalized;
  }

  private OperationRequest normalizeOperationRequest(
    OperationRequest request,
    PathAlias[] pathAliases,
    QueryAlias[] queryAliases,
    BodyAlias[] bodyAliases
  ) {
    OperationRequest normalized = new OperationRequest();
    normalized.path = request.path == null ? new LinkedHashMap<>() : new LinkedHashMap<>(request.path);
    normalized.query = request.query == null ? new LinkedHashMap<>() : new LinkedHashMap<>(request.query);
    normalized.headers = request.headers == null ? new LinkedHashMap<>() : new LinkedHashMap<>(request.headers);
    normalized.body = request.body;

    for (PathAlias alias : pathAliases) {
      String value = resolveCompatibilityValue(normalized.path, alias.canonical(), alias.aliases());
      if (value != null) {
        normalized.path.put(alias.wire(), value);
      }
    }
    for (QueryAlias alias : queryAliases) {
      String value = resolveCompatibilityValue(normalized.query, alias.canonical(), alias.aliases());
      if (value != null) {
        normalized.query.put(alias.canonical(), value);
      }
    }
    normalized.body = normalizeStringBody(normalized.body, bodyAliases);
    return normalized;
  }

  private OperationRequest normalizeCompatibilityRequest(String operationId, OperationRequest request) {
    switch (operationId) {
      case "list_replay_dataset_cases_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] { new PathAlias("datasetId", "id", new String[] { "id", "baselineId" }) },
          new QueryAlias[] {  },
          new BodyAlias[] {  }
        );
      case "replace_replay_dataset_cases_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] { new PathAlias("datasetId", "id", new String[] { "id", "baselineId" }) },
          new QueryAlias[] {  },
          new BodyAlias[] {  }
        );
      case "materialize_replay_dataset_cases_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] { new PathAlias("datasetId", "id", new String[] { "id", "baselineId" }) },
          new QueryAlias[] {  },
          new BodyAlias[] {  }
        );
      case "list_replay_runs_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] {  },
          new QueryAlias[] { new QueryAlias("datasetId", new String[] { "baselineId" }) },
          new BodyAlias[] {  }
        );
      case "create_replay_run_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] {  },
          new QueryAlias[] {  },
          new BodyAlias[] { new BodyAlias("datasetId", new String[] { "baselineId" }) }
        );
      case "get_replay_run_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] { new PathAlias("runId", "id", new String[] { "id", "jobId" }) },
          new QueryAlias[] {  },
          new BodyAlias[] {  }
        );
      case "get_replay_run_artifacts_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] { new PathAlias("runId", "id", new String[] { "id", "jobId" }) },
          new QueryAlias[] {  },
          new BodyAlias[] {  }
        );
      case "download_replay_run_artifact_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] { new PathAlias("runId", "id", new String[] { "id", "jobId" }) },
          new QueryAlias[] {  },
          new BodyAlias[] {  }
        );
      case "get_replay_run_diffs_v2":
        return normalizeOperationRequest(
          request,
          new PathAlias[] { new PathAlias("runId", "id", new String[] { "id", "jobId" }) },
          new QueryAlias[] { new QueryAlias("datasetId", new String[] { "baselineId" }) },
          new BodyAlias[] {  }
        );
      default:
        return request;
    }
  }

  private String renderPath(String template, Map<String, String> pathParams) {
    String pathValue = template;
    int start = pathValue.indexOf('{');
    while (start >= 0) {
      int end = pathValue.indexOf('}', start);
      if (end < 0) {
        break;
      }
      String key = pathValue.substring(start + 1, end).trim();
      String value = pathParams.get(key);
      if (value == null) {
        throw new IllegalArgumentException("缺少 path 参数: " + key);
      }
      pathValue = pathValue.replace("{" + key + "}", URLEncoder.encode(value, StandardCharsets.UTF_8));
      start = pathValue.indexOf('{');
    }
    return pathValue;
  }

  private String buildQuery(Map<String, String> query) {
    if (query == null || query.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    boolean first = true;
    for (Map.Entry<String, String> entry : query.entrySet()) {
      if (entry.getValue() == null || entry.getValue().isBlank()) {
        continue;
      }
      if (first) {
        builder.append('?');
        first = false;
      } else {
        builder.append('&');
      }
      builder.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8));
      builder.append('=');
      builder.append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
    }
    return builder.toString();
  }

  private String request(String method, String pathTemplate, OperationRequest request) throws IOException, InterruptedException {
    String url = this.baseUrl + renderPath(pathTemplate, request.path) + buildQuery(request.query);
    HttpRequest.BodyPublisher bodyPublisher = request.body == null
      ? HttpRequest.BodyPublishers.noBody()
      : HttpRequest.BodyPublishers.ofString(request.body);

    HttpRequest.Builder builder = HttpRequest.newBuilder()
      .uri(URI.create(url))
      .method(method, bodyPublisher)
      .header("content-type", "application/json");

    if (this.token != null && !this.token.isBlank()) {
      builder.header("authorization", "Bearer " + this.token);
    }
    for (Map.Entry<String, String> header : request.headers.entrySet()) {
      builder.header(header.getKey(), header.getValue());
    }

    HttpResponse<String> response = this.httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new ApiError(response.statusCode(), "request failed: " + response.statusCode(), response.body());
    }

    return response.body();
  }

  // list_api_keys | GET /api/v1/api-keys
  public String listApiKeys(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_api_keys", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/api-keys", resolved);
  }

  // create_api_key | POST /api/v1/api-keys
  public String createApiKey(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_api_key", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v1/api-keys", resolved);
  }

  // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
  public String revokeApiKey(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("revoke_api_key", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v1/api-keys/{id}/revoke", resolved);
  }

  // get_open_api_document | GET /api/v1/openapi.json
  public String getOpenApiDocument(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_open_api_document", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/openapi.json", resolved);
  }

  // create_quality_event | POST /api/v1/quality/events
  public String createQualityEvent(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_quality_event", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v1/quality/events", resolved);
  }

  // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
  public String listQualityDailyMetrics(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_quality_daily_metrics", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/quality/metrics/daily", resolved);
  }

  // list_quality_scorecards | GET /api/v1/quality/scorecards
  public String listQualityScorecards(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_quality_scorecards", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/quality/scorecards", resolved);
  }

  // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
  public String updateQualityScorecard(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("update_quality_scorecard", request == null ? new OperationRequest() : request);
    return this.request("PUT", "/api/v1/quality/scorecards/{id}", resolved);
  }

  // list_replay_baselines | GET /api/v1/replay/baselines
  public String listReplayBaselines(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_replay_baselines", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/replay/baselines", resolved);
  }

  // create_replay_baseline | POST /api/v1/replay/baselines
  public String createReplayBaseline(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_replay_baseline", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v1/replay/baselines", resolved);
  }

  // list_replay_jobs | GET /api/v1/replay/jobs
  public String listReplayJobs(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_replay_jobs", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/replay/jobs", resolved);
  }

  // create_replay_job | POST /api/v1/replay/jobs
  public String createReplayJob(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_replay_job", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v1/replay/jobs", resolved);
  }

  // get_replay_job | GET /api/v1/replay/jobs/{id}
  public String getReplayJob(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_replay_job", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/replay/jobs/{id}", resolved);
  }

  // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
  public String getReplayJobDiff(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_replay_job_diff", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/replay/jobs/{id}/diff", resolved);
  }

  // list_webhook_endpoints | GET /api/v1/webhooks
  public String listWebhookEndpoints(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_webhook_endpoints", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/webhooks", resolved);
  }

  // create_webhook_endpoint | POST /api/v1/webhooks
  public String createWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_webhook_endpoint", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v1/webhooks", resolved);
  }

  // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
  public String updateWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("update_webhook_endpoint", request == null ? new OperationRequest() : request);
    return this.request("PUT", "/api/v1/webhooks/{id}", resolved);
  }

  // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
  public String deleteWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("delete_webhook_endpoint", request == null ? new OperationRequest() : request);
    return this.request("DELETE", "/api/v1/webhooks/{id}", resolved);
  }

  // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
  public String replayWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("replay_webhook_endpoint", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v1/webhooks/{id}/replay", resolved);
  }

  // list_webhook_replay_tasks | GET /api/v1/webhooks/replay-tasks
  public String listWebhookReplayTasks(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_webhook_replay_tasks", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/webhooks/replay-tasks", resolved);
  }

  // get_webhook_replay_task | GET /api/v1/webhooks/replay-tasks/{id}
  public String getWebhookReplayTask(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_webhook_replay_task", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v1/webhooks/replay-tasks/{id}", resolved);
  }

  // create_quality_evaluation_v2 | POST /api/v2/quality/evaluations
  public String createQualityEvaluationV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_quality_evaluation_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/quality/evaluations", resolved);
  }

  // list_quality_metrics_v2 | GET /api/v2/quality/metrics
  public String listQualityMetricsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_quality_metrics_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/quality/metrics", resolved);
  }

  // get_quality_cost_correlation_v2 | GET /api/v2/quality/reports/cost-correlation
  public String getQualityCostCorrelationV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_quality_cost_correlation_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/quality/reports/cost-correlation", resolved);
  }

  // get_quality_project_trends_v2 | GET /api/v2/quality/reports/project-trends
  public String getQualityProjectTrendsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_quality_project_trends_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/quality/reports/project-trends", resolved);
  }

  // list_quality_scorecards_v2 | GET /api/v2/quality/scorecards
  public String listQualityScorecardsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_quality_scorecards_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/quality/scorecards", resolved);
  }

  // update_quality_scorecard_v2 | PUT /api/v2/quality/scorecards/{id}
  public String updateQualityScorecardV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("update_quality_scorecard_v2", request == null ? new OperationRequest() : request);
    return this.request("PUT", "/api/v2/quality/scorecards/{id}", resolved);
  }

  // list_replay_datasets_v2 | GET /api/v2/replay/datasets
  public String listReplayDatasetsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_replay_datasets_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/replay/datasets", resolved);
  }

  // create_replay_dataset_v2 | POST /api/v2/replay/datasets
  public String createReplayDatasetV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_replay_dataset_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/replay/datasets", resolved);
  }

  // list_replay_dataset_cases_v2 | GET /api/v2/replay/datasets/{id}/cases
  public String listReplayDatasetCasesV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_replay_dataset_cases_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/replay/datasets/{id}/cases", resolved);
  }

  // replace_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/cases
  public String replaceReplayDatasetCasesV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("replace_replay_dataset_cases_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/replay/datasets/{id}/cases", resolved);
  }

  // materialize_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/materialize
  public String materializeReplayDatasetCasesV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("materialize_replay_dataset_cases_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/replay/datasets/{id}/materialize", resolved);
  }

  // list_replay_runs_v2 | GET /api/v2/replay/runs
  public String listReplayRunsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_replay_runs_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/replay/runs", resolved);
  }

  // create_replay_run_v2 | POST /api/v2/replay/runs
  public String createReplayRunV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_replay_run_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/replay/runs", resolved);
  }

  // get_replay_run_v2 | GET /api/v2/replay/runs/{id}
  public String getReplayRunV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_replay_run_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/replay/runs/{id}", resolved);
  }

  // get_replay_run_artifacts_v2 | GET /api/v2/replay/runs/{id}/artifacts
  public String getReplayRunArtifactsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_replay_run_artifacts_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/replay/runs/{id}/artifacts", resolved);
  }

  // download_replay_run_artifact_v2 | GET /api/v2/replay/runs/{id}/artifacts/{artifactType}/download
  public String downloadReplayRunArtifactV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("download_replay_run_artifact_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download", resolved);
  }

  // get_replay_run_diffs_v2 | GET /api/v2/replay/runs/{id}/diffs
  public String getReplayRunDiffsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_replay_run_diffs_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/replay/runs/{id}/diffs", resolved);
  }

  // get_residency_policy_v2 | GET /api/v2/residency/policies/current
  public String getResidencyPolicyV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("get_residency_policy_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/residency/policies/current", resolved);
  }

  // update_residency_policy_v2 | PUT /api/v2/residency/policies/current
  public String updateResidencyPolicyV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("update_residency_policy_v2", request == null ? new OperationRequest() : request);
    return this.request("PUT", "/api/v2/residency/policies/current", resolved);
  }

  // list_residency_region_mappings_v2 | GET /api/v2/residency/region-mappings
  public String listResidencyRegionMappingsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_residency_region_mappings_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/residency/region-mappings", resolved);
  }

  // list_residency_replications_v2 | GET /api/v2/residency/replications
  public String listResidencyReplicationsV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("list_residency_replications_v2", request == null ? new OperationRequest() : request);
    return this.request("GET", "/api/v2/residency/replications", resolved);
  }

  // create_residency_replication_v2 | POST /api/v2/residency/replications
  public String createResidencyReplicationV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("create_residency_replication_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/residency/replications", resolved);
  }

  // approve_residency_replication_v2 | POST /api/v2/residency/replications/{id}/approvals
  public String approveResidencyReplicationV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("approve_residency_replication_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/residency/replications/{id}/approvals", resolved);
  }

  // cancel_residency_replication_v2 | POST /api/v2/residency/replications/{id}/cancel
  public String cancelResidencyReplicationV2(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = normalizeCompatibilityRequest("cancel_residency_replication_v2", request == null ? new OperationRequest() : request);
    return this.request("POST", "/api/v2/residency/replications/{id}/cancel", resolved);
  }
}
