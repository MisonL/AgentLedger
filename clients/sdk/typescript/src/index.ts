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
    return this.request("GET", "/api/v1/api-keys", request);
  }

  /** create_api_key | POST /api/v1/api-keys */
  async createApiKey(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/api-keys", request);
  }

  /** revoke_api_key | POST /api/v1/api-keys/{id}/revoke */
  async revokeApiKey(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/api-keys/{id}/revoke", request);
  }

  /** get_open_api_document | GET /api/v1/openapi.json */
  async getOpenApiDocument(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/openapi.json", request);
  }

  /** create_quality_event | POST /api/v1/quality/events */
  async createQualityEvent(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/quality/events", request);
  }

  /** list_quality_daily_metrics | GET /api/v1/quality/metrics/daily */
  async listQualityDailyMetrics(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/quality/metrics/daily", request);
  }

  /** list_quality_scorecards | GET /api/v1/quality/scorecards */
  async listQualityScorecards(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/quality/scorecards", request);
  }

  /** update_quality_scorecard | PUT /api/v1/quality/scorecards/{id} */
  async updateQualityScorecard(request: OperationRequest = {}): Promise<unknown> {
    return this.request("PUT", "/api/v1/quality/scorecards/{id}", request);
  }

  /** list_replay_baselines | GET /api/v1/replay/baselines */
  async listReplayBaselines(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/baselines", request);
  }

  /** create_replay_baseline | POST /api/v1/replay/baselines */
  async createReplayBaseline(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/replay/baselines", request);
  }

  /** list_replay_jobs | GET /api/v1/replay/jobs */
  async listReplayJobs(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/jobs", request);
  }

  /** create_replay_job | POST /api/v1/replay/jobs */
  async createReplayJob(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/replay/jobs", request);
  }

  /** get_replay_job | GET /api/v1/replay/jobs/{id} */
  async getReplayJob(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/jobs/{id}", request);
  }

  /** get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff */
  async getReplayJobDiff(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/replay/jobs/{id}/diff", request);
  }

  /** list_webhook_endpoints | GET /api/v1/webhooks */
  async listWebhookEndpoints(request: OperationRequest = {}): Promise<unknown> {
    return this.request("GET", "/api/v1/webhooks", request);
  }

  /** create_webhook_endpoint | POST /api/v1/webhooks */
  async createWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/webhooks", request);
  }

  /** update_webhook_endpoint | PUT /api/v1/webhooks/{id} */
  async updateWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("PUT", "/api/v1/webhooks/{id}", request);
  }

  /** delete_webhook_endpoint | DELETE /api/v1/webhooks/{id} */
  async deleteWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("DELETE", "/api/v1/webhooks/{id}", request);
  }

  /** replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay */
  async replayWebhookEndpoint(request: OperationRequest = {}): Promise<unknown> {
    return this.request("POST", "/api/v1/webhooks/{id}/replay", request);
  }
}
