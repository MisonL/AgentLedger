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
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/api-keys", resolved);
  }

  // create_api_key | POST /api/v1/api-keys
  public String createApiKey(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("POST", "/api/v1/api-keys", resolved);
  }

  // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
  public String revokeApiKey(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("POST", "/api/v1/api-keys/{id}/revoke", resolved);
  }

  // get_open_api_document | GET /api/v1/openapi.json
  public String getOpenApiDocument(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/openapi.json", resolved);
  }

  // create_quality_event | POST /api/v1/quality/events
  public String createQualityEvent(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("POST", "/api/v1/quality/events", resolved);
  }

  // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
  public String listQualityDailyMetrics(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/quality/metrics/daily", resolved);
  }

  // list_quality_scorecards | GET /api/v1/quality/scorecards
  public String listQualityScorecards(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/quality/scorecards", resolved);
  }

  // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
  public String updateQualityScorecard(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("PUT", "/api/v1/quality/scorecards/{id}", resolved);
  }

  // list_replay_baselines | GET /api/v1/replay/baselines
  public String listReplayBaselines(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/replay/baselines", resolved);
  }

  // create_replay_baseline | POST /api/v1/replay/baselines
  public String createReplayBaseline(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("POST", "/api/v1/replay/baselines", resolved);
  }

  // list_replay_jobs | GET /api/v1/replay/jobs
  public String listReplayJobs(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/replay/jobs", resolved);
  }

  // create_replay_job | POST /api/v1/replay/jobs
  public String createReplayJob(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("POST", "/api/v1/replay/jobs", resolved);
  }

  // get_replay_job | GET /api/v1/replay/jobs/{id}
  public String getReplayJob(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/replay/jobs/{id}", resolved);
  }

  // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
  public String getReplayJobDiff(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/replay/jobs/{id}/diff", resolved);
  }

  // list_webhook_endpoints | GET /api/v1/webhooks
  public String listWebhookEndpoints(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("GET", "/api/v1/webhooks", resolved);
  }

  // create_webhook_endpoint | POST /api/v1/webhooks
  public String createWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("POST", "/api/v1/webhooks", resolved);
  }

  // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
  public String updateWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("PUT", "/api/v1/webhooks/{id}", resolved);
  }

  // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
  public String deleteWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("DELETE", "/api/v1/webhooks/{id}", resolved);
  }

  // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
  public String replayWebhookEndpoint(OperationRequest request) throws IOException, InterruptedException {
    OperationRequest resolved = request == null ? new OperationRequest() : request;
    return this.request("POST", "/api/v1/webhooks/{id}/replay", resolved);
  }
}
