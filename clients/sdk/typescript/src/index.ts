type OperationCompatibilityRule = {
  pathAliases?: Array<{ canonical: string; wire: string; aliases: string[] }>;
  queryAliases?: Array<{ canonical: string; aliases: string[] }>;
  bodyAliases?: Array<{ canonical: string; aliases: string[] }>;
};

const OPERATION_COMPATIBILITY: Record<string, OperationCompatibilityRule> = {
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
} as Record<string, OperationCompatibilityRule>;

export interface OperationRequest {
  path?: Record<string, string | number>;
  query?: Record<string, string | number | boolean | null | undefined>;
  body?: unknown;
  headers?: Record<string, string>;
  signal?: AbortSignal;
}

export class AgentLedgerApiError extends Error {
  readonly status: number;
  readonly payload: unknown;

  constructor(status: number, message: string, payload?: unknown) {
    super(message);
    this.name = "AgentLedgerApiError";
    this.status = status;
    this.payload = payload;
  }
}

export interface AgentLedgerClientOptions {
  baseUrl: string;
  token?: string;
  defaultHeaders?: Record<string, string>;
  fetchImpl?: typeof fetch;
}

export class AgentLedgerClient {
  private readonly baseUrl: string;
  private readonly token?: string;
  private readonly defaultHeaders: Record<string, string>;
  private readonly fetchImpl: typeof fetch;

  constructor(options: AgentLedgerClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/$/, "");
    this.token = options.token;
    this.defaultHeaders = options.defaultHeaders ?? {};
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  private renderPath(pathTemplate: string, pathParams?: Record<string, string | number>): string {
    return pathTemplate.replace(/\{([^}]+)\}/g, (_, rawKey: string) => {
      const key = String(rawKey).trim();
      if (!pathParams || !(key in pathParams)) {
        throw new Error(`缺少 path 参数：${key}`);
      }
      return encodeURIComponent(String(pathParams[key]));
    });
  }

  private buildQuery(query?: Record<string, string | number | boolean | null | undefined>): string {
    if (!query) {
      return "";
    }
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null) {
        continue;
      }
      params.set(key, String(value));
    }
    const queryText = params.toString();
    return queryText.length > 0 ? `?${queryText}` : "";
  }

  private resolveCompatibilityValue(record: Record<string, unknown>, candidates: string[]): unknown {
    for (const candidate of candidates) {
      if (candidate in record) {
        const value = record[candidate];
        if (value !== undefined && value !== null) {
          return value;
        }
      }
    }
    return undefined;
  }

  private normalizeCompatibilityRequest(operationId: string, request: OperationRequest): OperationRequest {
    const rule = OPERATION_COMPATIBILITY[operationId];
    if (!rule) {
      return request;
    }

    const path = { ...(request.path ?? {}) } as Record<string, unknown>;
    const query = { ...(request.query ?? {}) } as Record<string, unknown>;
    let body = request.body;

    for (const alias of rule.pathAliases ?? []) {
      const value = this.resolveCompatibilityValue(path, [alias.canonical, ...alias.aliases]);
      if (value !== undefined) {
        path[alias.wire] = value;
      }
    }

    for (const alias of rule.queryAliases ?? []) {
      const value = this.resolveCompatibilityValue(query, [alias.canonical, ...alias.aliases]);
      if (value !== undefined) {
        query[alias.canonical] = value;
      }
    }

    if (body && typeof body === "object" && !Array.isArray(body)) {
      const normalizedBody = { ...(body as Record<string, unknown>) };
      for (const alias of rule.bodyAliases ?? []) {
        const value = this.resolveCompatibilityValue(normalizedBody, [alias.canonical, ...alias.aliases]);
        if (value !== undefined) {
          normalizedBody[alias.canonical] = value;
        }
      }
      body = normalizedBody;
    }

    return {
      ...request,
      path: path as Record<string, string | number>,
      query: query as Record<string, string | number | boolean | null | undefined>,
      body,
    };
  }

  private async request(method: string, pathTemplate: string, request: OperationRequest): Promise<unknown> {
    const url = `${this.baseUrl}${this.renderPath(pathTemplate, request.path)}${this.buildQuery(request.query)}`;
    const headers: Record<string, string> = {
      "content-type": "application/json",
      ...this.defaultHeaders,
      ...(request.headers ?? {}),
    };
    if (this.token && !headers.authorization) {
      headers.authorization = `Bearer ${this.token}`;
    }

    const response = await this.fetchImpl(url, {
      method,
      headers,
      body: request.body === undefined ? undefined : JSON.stringify(request.body),
      signal: request.signal,
    });

    const contentType = response.headers.get("content-type") ?? "";
    const payload = contentType.toLowerCase().includes("application/json")
      ? await response.json().catch(() => undefined)
      : await response.text().catch(() => undefined);

    if (!response.ok) {
      const message =
        payload && typeof payload === "object" && "message" in payload && typeof (payload as { message?: unknown }).message === "string"
          ? (payload as { message: string }).message
          : `请求失败: ${response.status}`;
      throw new AgentLedgerApiError(response.status, message, payload);
    }

    return payload;
  }

  /** list_api_keys | GET /api/v1/api-keys */
  async listApiKeys(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/api-keys", this.normalizeCompatibilityRequest("list_api_keys", request));
  }

  /** create_api_key | POST /api/v1/api-keys */
  async createApiKey(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/api-keys", this.normalizeCompatibilityRequest("create_api_key", request));
  }

  /** revoke_api_key | POST /api/v1/api-keys/{id}/revoke */
  async revokeApiKey(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/api-keys/{id}/revoke", this.normalizeCompatibilityRequest("revoke_api_key", request));
  }

  /** get_open_api_document | GET /api/v1/openapi.json */
  async getOpenApiDocument(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/openapi.json", this.normalizeCompatibilityRequest("get_open_api_document", request));
  }

  /** create_quality_event | POST /api/v1/quality/events */
  async createQualityEvent(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/quality/events", this.normalizeCompatibilityRequest("create_quality_event", request));
  }

  /** list_quality_daily_metrics | GET /api/v1/quality/metrics/daily */
  async listQualityDailyMetrics(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/quality/metrics/daily", this.normalizeCompatibilityRequest("list_quality_daily_metrics", request));
  }

  /** list_quality_scorecards | GET /api/v1/quality/scorecards */
  async listQualityScorecards(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/quality/scorecards", this.normalizeCompatibilityRequest("list_quality_scorecards", request));
  }

  /** update_quality_scorecard | PUT /api/v1/quality/scorecards/{id} */
  async updateQualityScorecard(request: OperationRequest = {}): Promise<unknown> {
    return this.request("PUT", "/api/v1/quality/scorecards/{id}", this.normalizeCompatibilityRequest("update_quality_scorecard", request));
  }

  /** list_replay_baselines | GET /api/v1/replay/baselines */
  async listReplayBaselines(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/baselines", this.normalizeCompatibilityRequest("list_replay_baselines", request));
  }

  /** create_replay_baseline | POST /api/v1/replay/baselines */
  async createReplayBaseline(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/replay/baselines", this.normalizeCompatibilityRequest("create_replay_baseline", request));
  }

  /** list_replay_jobs | GET /api/v1/replay/jobs */
  async listReplayJobs(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/jobs", this.normalizeCompatibilityRequest("list_replay_jobs", request));
  }

  /** create_replay_job | POST /api/v1/replay/jobs */
  async createReplayJob(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/replay/jobs", this.normalizeCompatibilityRequest("create_replay_job", request));
  }

  /** get_replay_job | GET /api/v1/replay/jobs/{id} */
  async getReplayJob(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/jobs/{id}", this.normalizeCompatibilityRequest("get_replay_job", request));
  }

  /** get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff */
  async getReplayJobDiff(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/jobs/{id}/diff", this.normalizeCompatibilityRequest("get_replay_job_diff", request));
  }

  /** list_webhook_endpoints | GET /api/v1/webhooks */
  async listWebhookEndpoints(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/webhooks", this.normalizeCompatibilityRequest("list_webhook_endpoints", request));
  }

  /** create_webhook_endpoint | POST /api/v1/webhooks */
  async createWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/webhooks", this.normalizeCompatibilityRequest("create_webhook_endpoint", request));
  }

  /** update_webhook_endpoint | PUT /api/v1/webhooks/{id} */
  async updateWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("PUT", "/api/v1/webhooks/{id}", this.normalizeCompatibilityRequest("update_webhook_endpoint", request));
  }

  /** delete_webhook_endpoint | DELETE /api/v1/webhooks/{id} */
  async deleteWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("DELETE", "/api/v1/webhooks/{id}", this.normalizeCompatibilityRequest("delete_webhook_endpoint", request));
  }

  /** replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay */
  async replayWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/webhooks/{id}/replay", this.normalizeCompatibilityRequest("replay_webhook_endpoint", request));
  }

  /** list_webhook_replay_tasks | GET /api/v1/webhooks/replay-tasks */
  async listWebhookReplayTasks(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/webhooks/replay-tasks", this.normalizeCompatibilityRequest("list_webhook_replay_tasks", request));
  }

  /** get_webhook_replay_task | GET /api/v1/webhooks/replay-tasks/{id} */
  async getWebhookReplayTask(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/webhooks/replay-tasks/{id}", this.normalizeCompatibilityRequest("get_webhook_replay_task", request));
  }

  /** create_quality_evaluation_v2 | POST /api/v2/quality/evaluations */
  async createQualityEvaluationV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/quality/evaluations", this.normalizeCompatibilityRequest("create_quality_evaluation_v2", request));
  }

  /** list_quality_metrics_v2 | GET /api/v2/quality/metrics */
  async listQualityMetricsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/quality/metrics", this.normalizeCompatibilityRequest("list_quality_metrics_v2", request));
  }

  /** get_quality_cost_correlation_v2 | GET /api/v2/quality/reports/cost-correlation */
  async getQualityCostCorrelationV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/quality/reports/cost-correlation", this.normalizeCompatibilityRequest("get_quality_cost_correlation_v2", request));
  }

  /** get_quality_project_trends_v2 | GET /api/v2/quality/reports/project-trends */
  async getQualityProjectTrendsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/quality/reports/project-trends", this.normalizeCompatibilityRequest("get_quality_project_trends_v2", request));
  }

  /** list_quality_scorecards_v2 | GET /api/v2/quality/scorecards */
  async listQualityScorecardsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/quality/scorecards", this.normalizeCompatibilityRequest("list_quality_scorecards_v2", request));
  }

  /** update_quality_scorecard_v2 | PUT /api/v2/quality/scorecards/{id} */
  async updateQualityScorecardV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("PUT", "/api/v2/quality/scorecards/{id}", this.normalizeCompatibilityRequest("update_quality_scorecard_v2", request));
  }

  /** list_replay_datasets_v2 | GET /api/v2/replay/datasets */
  async listReplayDatasetsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/replay/datasets", this.normalizeCompatibilityRequest("list_replay_datasets_v2", request));
  }

  /** create_replay_dataset_v2 | POST /api/v2/replay/datasets */
  async createReplayDatasetV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/replay/datasets", this.normalizeCompatibilityRequest("create_replay_dataset_v2", request));
  }

  /** list_replay_dataset_cases_v2 | GET /api/v2/replay/datasets/{id}/cases */
  async listReplayDatasetCasesV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/replay/datasets/{id}/cases", this.normalizeCompatibilityRequest("list_replay_dataset_cases_v2", request));
  }

  /** replace_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/cases */
  async replaceReplayDatasetCasesV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/replay/datasets/{id}/cases", this.normalizeCompatibilityRequest("replace_replay_dataset_cases_v2", request));
  }

  /** materialize_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/materialize */
  async materializeReplayDatasetCasesV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/replay/datasets/{id}/materialize", this.normalizeCompatibilityRequest("materialize_replay_dataset_cases_v2", request));
  }

  /** list_replay_runs_v2 | GET /api/v2/replay/runs */
  async listReplayRunsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/replay/runs", this.normalizeCompatibilityRequest("list_replay_runs_v2", request));
  }

  /** create_replay_run_v2 | POST /api/v2/replay/runs */
  async createReplayRunV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/replay/runs", this.normalizeCompatibilityRequest("create_replay_run_v2", request));
  }

  /** get_replay_run_v2 | GET /api/v2/replay/runs/{id} */
  async getReplayRunV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/replay/runs/{id}", this.normalizeCompatibilityRequest("get_replay_run_v2", request));
  }

  /** get_replay_run_artifacts_v2 | GET /api/v2/replay/runs/{id}/artifacts */
  async getReplayRunArtifactsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/replay/runs/{id}/artifacts", this.normalizeCompatibilityRequest("get_replay_run_artifacts_v2", request));
  }

  /** download_replay_run_artifact_v2 | GET /api/v2/replay/runs/{id}/artifacts/{artifactType}/download */
  async downloadReplayRunArtifactV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download", this.normalizeCompatibilityRequest("download_replay_run_artifact_v2", request));
  }

  /** get_replay_run_diffs_v2 | GET /api/v2/replay/runs/{id}/diffs */
  async getReplayRunDiffsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/replay/runs/{id}/diffs", this.normalizeCompatibilityRequest("get_replay_run_diffs_v2", request));
  }

  /** get_residency_policy_v2 | GET /api/v2/residency/policies/current */
  async getResidencyPolicyV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/residency/policies/current", this.normalizeCompatibilityRequest("get_residency_policy_v2", request));
  }

  /** update_residency_policy_v2 | PUT /api/v2/residency/policies/current */
  async updateResidencyPolicyV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("PUT", "/api/v2/residency/policies/current", this.normalizeCompatibilityRequest("update_residency_policy_v2", request));
  }

  /** list_residency_region_mappings_v2 | GET /api/v2/residency/region-mappings */
  async listResidencyRegionMappingsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/residency/region-mappings", this.normalizeCompatibilityRequest("list_residency_region_mappings_v2", request));
  }

  /** list_residency_replications_v2 | GET /api/v2/residency/replications */
  async listResidencyReplicationsV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v2/residency/replications", this.normalizeCompatibilityRequest("list_residency_replications_v2", request));
  }

  /** create_residency_replication_v2 | POST /api/v2/residency/replications */
  async createResidencyReplicationV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/residency/replications", this.normalizeCompatibilityRequest("create_residency_replication_v2", request));
  }

  /** approve_residency_replication_v2 | POST /api/v2/residency/replications/{id}/approvals */
  async approveResidencyReplicationV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/residency/replications/{id}/approvals", this.normalizeCompatibilityRequest("approve_residency_replication_v2", request));
  }

  /** cancel_residency_replication_v2 | POST /api/v2/residency/replications/{id}/cancel */
  async cancelResidencyReplicationV2(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v2/residency/replications/{id}/cancel", this.normalizeCompatibilityRequest("cancel_residency_replication_v2", request));
  }
}
