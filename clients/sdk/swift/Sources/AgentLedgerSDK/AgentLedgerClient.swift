import Foundation

private struct OperationPathAlias: Decodable {
    let canonical: String
    let wire: String
    let aliases: [String]
}

private struct OperationQueryAlias: Decodable {
    let canonical: String
    let aliases: [String]
}

private struct OperationBodyAlias: Decodable {
    let canonical: String
    let aliases: [String]
}

private struct OperationCompatibilityRule: Decodable {
    let pathAliases: [OperationPathAlias]?
    let queryAliases: [OperationQueryAlias]?
    let bodyAliases: [OperationBodyAlias]?
}

private let operationCompatibility: [String: OperationCompatibilityRule] = {
    guard let data = #"""
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
"""#.data(using: .utf8) else {
        return [:]
    }
    return (try? JSONDecoder().decode([String: OperationCompatibilityRule].self, from: data)) ?? [:]
}()

public struct OperationRequest {
    public var path: [String: String] = [:]
    public var query: [String: String] = [:]
    public var body: Any? = nil
    public var headers: [String: String] = [:]

    public init(path: [String: String] = [:], query: [String: String] = [:], body: Any? = nil, headers: [String: String] = [:]) {
        self.path = path
        self.query = query
        self.body = body
        self.headers = headers
    }
}

public struct AgentLedgerApiError: Error {
    public let status: Int
    public let payload: String
}

public final class AgentLedgerClient {
    private let baseUrl: String
    private let token: String?
    private let session: URLSession

    public init(baseUrl: String, token: String? = nil, session: URLSession = .shared) {
        self.baseUrl = baseUrl.replacingOccurrences(of: "/+$", with: "", options: .regularExpression)
        self.token = token
        self.session = session
    }

    private func resolveCompatibilityValue(record: [String: String], candidates: [String]) -> String? {
        for candidate in candidates {
            if let value = record[candidate], !value.isEmpty {
                return value
            }
        }
        return nil
    }

    private func resolveCompatibilityValue(record: [String: Any], candidates: [String]) -> Any? {
        for candidate in candidates {
            guard let value = record[candidate] else {
                continue
            }
            if let text = value as? String, text.isEmpty {
                continue
            }
            return value
        }
        return nil
    }

    private func normalizeCompatibilityRequest(operationId: String, request: OperationRequest) -> OperationRequest {
        guard let rule = operationCompatibility[operationId] else {
            return request
        }

        var path = request.path
        var query = request.query
        var body = request.body

        for alias in rule.pathAliases ?? [] {
            if let value = resolveCompatibilityValue(record: path, candidates: [alias.canonical] + alias.aliases) {
                path[alias.wire] = value
            }
        }

        for alias in rule.queryAliases ?? [] {
            if let value = resolveCompatibilityValue(record: query, candidates: [alias.canonical] + alias.aliases) {
                query[alias.canonical] = value
            }
        }

        if var normalizedBody = body as? [String: Any] {
            for alias in rule.bodyAliases ?? [] {
                if let value = resolveCompatibilityValue(record: normalizedBody, candidates: [alias.canonical] + alias.aliases) {
                    normalizedBody[alias.canonical] = value
                }
            }
            body = normalizedBody
        }

        return OperationRequest(path: path, query: query, body: body, headers: request.headers)
    }

    private func renderPath(pathTemplate: String, pathParams: [String: String]) throws -> String {
        var result = pathTemplate
        let regex = try NSRegularExpression(pattern: "\\{([^}]+)\\}")
        let matches = regex.matches(in: pathTemplate, range: NSRange(pathTemplate.startIndex..., in: pathTemplate))
        for match in matches.reversed() {
            guard let keyRange = Range(match.range(at: 1), in: pathTemplate) else { continue }
            let key = String(pathTemplate[keyRange]).trimmingCharacters(in: .whitespaces)
            guard let value = pathParams[key] else {
                throw NSError(domain: "AgentLedgerSDK", code: 1, userInfo: [NSLocalizedDescriptionKey: "缺少 path 参数: \(key)"])
            }
            guard let fullRange = Range(match.range(at: 0), in: result) else { continue }
            result.replaceSubrange(fullRange, with: value.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? value)
        }
        return result
    }

    private func request(method: String, pathTemplate: String, request: OperationRequest) async throws -> Any {
        let renderedPath = try renderPath(pathTemplate: pathTemplate, pathParams: request.path)
        guard var components = URLComponents(string: self.baseUrl + renderedPath) else {
            throw NSError(domain: "AgentLedgerSDK", code: 2, userInfo: [NSLocalizedDescriptionKey: "URL 非法"])
        }
        if !request.query.isEmpty {
            components.queryItems = request.query.map { URLQueryItem(name: $0.key, value: $0.value) }
        }

        guard let url = components.url else {
            throw NSError(domain: "AgentLedgerSDK", code: 3, userInfo: [NSLocalizedDescriptionKey: "URL 非法"])
        }

        var urlRequest = URLRequest(url: url)
        urlRequest.httpMethod = method
        urlRequest.setValue("application/json", forHTTPHeaderField: "content-type")
        if let token = self.token, !token.isEmpty {
            urlRequest.setValue("Bearer \(token)", forHTTPHeaderField: "authorization")
        }
        for (key, value) in request.headers {
            urlRequest.setValue(value, forHTTPHeaderField: key)
        }

        if let body = request.body, JSONSerialization.isValidJSONObject(body) {
            urlRequest.httpBody = try JSONSerialization.data(withJSONObject: body, options: [])
        }

        let (data, response) = try await self.session.data(for: urlRequest)
        guard let http = response as? HTTPURLResponse else {
            throw NSError(domain: "AgentLedgerSDK", code: 4, userInfo: [NSLocalizedDescriptionKey: "响应类型非法"])
        }

        let payloadText = String(data: data, encoding: .utf8) ?? ""
        guard (200...299).contains(http.statusCode) else {
            throw AgentLedgerApiError(status: http.statusCode, payload: payloadText)
        }

        if payloadText.isEmpty {
            return [:]
        }

        if let object = try? JSONSerialization.jsonObject(with: data, options: []) {
            return object
        }

        return payloadText
    }

    // list_api_keys | GET /api/v1/api-keys
    public func listApiKeys(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_api_keys", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/api-keys", request: normalizedRequest)
    }

    // create_api_key | POST /api/v1/api-keys
    public func createApiKey(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_api_key", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v1/api-keys", request: normalizedRequest)
    }

    // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    public func revokeApiKey(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "revoke_api_key", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v1/api-keys/{id}/revoke", request: normalizedRequest)
    }

    // get_open_api_document | GET /api/v1/openapi.json
    public func getOpenApiDocument(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_open_api_document", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/openapi.json", request: normalizedRequest)
    }

    // create_quality_event | POST /api/v1/quality/events
    public func createQualityEvent(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_quality_event", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v1/quality/events", request: normalizedRequest)
    }

    // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    public func listQualityDailyMetrics(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_quality_daily_metrics", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/quality/metrics/daily", request: normalizedRequest)
    }

    // list_quality_scorecards | GET /api/v1/quality/scorecards
    public func listQualityScorecards(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_quality_scorecards", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/quality/scorecards", request: normalizedRequest)
    }

    // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    public func updateQualityScorecard(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "update_quality_scorecard", request: request)
        return try await self.request(method: "PUT", pathTemplate: "/api/v1/quality/scorecards/{id}", request: normalizedRequest)
    }

    // list_replay_baselines | GET /api/v1/replay/baselines
    public func listReplayBaselines(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_replay_baselines", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/baselines", request: normalizedRequest)
    }

    // create_replay_baseline | POST /api/v1/replay/baselines
    public func createReplayBaseline(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_replay_baseline", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v1/replay/baselines", request: normalizedRequest)
    }

    // list_replay_jobs | GET /api/v1/replay/jobs
    public func listReplayJobs(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_replay_jobs", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/jobs", request: normalizedRequest)
    }

    // create_replay_job | POST /api/v1/replay/jobs
    public func createReplayJob(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_replay_job", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v1/replay/jobs", request: normalizedRequest)
    }

    // get_replay_job | GET /api/v1/replay/jobs/{id}
    public func getReplayJob(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_replay_job", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/jobs/{id}", request: normalizedRequest)
    }

    // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    public func getReplayJobDiff(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_replay_job_diff", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/jobs/{id}/diff", request: normalizedRequest)
    }

    // list_webhook_endpoints | GET /api/v1/webhooks
    public func listWebhookEndpoints(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_webhook_endpoints", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/webhooks", request: normalizedRequest)
    }

    // create_webhook_endpoint | POST /api/v1/webhooks
    public func createWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_webhook_endpoint", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v1/webhooks", request: normalizedRequest)
    }

    // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    public func updateWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "update_webhook_endpoint", request: request)
        return try await self.request(method: "PUT", pathTemplate: "/api/v1/webhooks/{id}", request: normalizedRequest)
    }

    // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    public func deleteWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "delete_webhook_endpoint", request: request)
        return try await self.request(method: "DELETE", pathTemplate: "/api/v1/webhooks/{id}", request: normalizedRequest)
    }

    // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    public func replayWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "replay_webhook_endpoint", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v1/webhooks/{id}/replay", request: normalizedRequest)
    }

    // list_webhook_replay_tasks | GET /api/v1/webhooks/replay-tasks
    public func listWebhookReplayTasks(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_webhook_replay_tasks", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/webhooks/replay-tasks", request: normalizedRequest)
    }

    // get_webhook_replay_task | GET /api/v1/webhooks/replay-tasks/{id}
    public func getWebhookReplayTask(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_webhook_replay_task", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v1/webhooks/replay-tasks/{id}", request: normalizedRequest)
    }

    // create_quality_evaluation_v2 | POST /api/v2/quality/evaluations
    public func createQualityEvaluationV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_quality_evaluation_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/quality/evaluations", request: normalizedRequest)
    }

    // list_quality_metrics_v2 | GET /api/v2/quality/metrics
    public func listQualityMetricsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_quality_metrics_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/quality/metrics", request: normalizedRequest)
    }

    // get_quality_cost_correlation_v2 | GET /api/v2/quality/reports/cost-correlation
    public func getQualityCostCorrelationV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_quality_cost_correlation_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/quality/reports/cost-correlation", request: normalizedRequest)
    }

    // get_quality_project_trends_v2 | GET /api/v2/quality/reports/project-trends
    public func getQualityProjectTrendsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_quality_project_trends_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/quality/reports/project-trends", request: normalizedRequest)
    }

    // list_quality_scorecards_v2 | GET /api/v2/quality/scorecards
    public func listQualityScorecardsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_quality_scorecards_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/quality/scorecards", request: normalizedRequest)
    }

    // update_quality_scorecard_v2 | PUT /api/v2/quality/scorecards/{id}
    public func updateQualityScorecardV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "update_quality_scorecard_v2", request: request)
        return try await self.request(method: "PUT", pathTemplate: "/api/v2/quality/scorecards/{id}", request: normalizedRequest)
    }

    // list_replay_datasets_v2 | GET /api/v2/replay/datasets
    public func listReplayDatasetsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_replay_datasets_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/replay/datasets", request: normalizedRequest)
    }

    // create_replay_dataset_v2 | POST /api/v2/replay/datasets
    public func createReplayDatasetV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_replay_dataset_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/replay/datasets", request: normalizedRequest)
    }

    // list_replay_dataset_cases_v2 | GET /api/v2/replay/datasets/{id}/cases
    public func listReplayDatasetCasesV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_replay_dataset_cases_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/replay/datasets/{id}/cases", request: normalizedRequest)
    }

    // replace_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/cases
    public func replaceReplayDatasetCasesV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "replace_replay_dataset_cases_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/replay/datasets/{id}/cases", request: normalizedRequest)
    }

    // materialize_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/materialize
    public func materializeReplayDatasetCasesV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "materialize_replay_dataset_cases_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/replay/datasets/{id}/materialize", request: normalizedRequest)
    }

    // list_replay_runs_v2 | GET /api/v2/replay/runs
    public func listReplayRunsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_replay_runs_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/replay/runs", request: normalizedRequest)
    }

    // create_replay_run_v2 | POST /api/v2/replay/runs
    public func createReplayRunV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_replay_run_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/replay/runs", request: normalizedRequest)
    }

    // get_replay_run_v2 | GET /api/v2/replay/runs/{id}
    public func getReplayRunV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_replay_run_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/replay/runs/{id}", request: normalizedRequest)
    }

    // get_replay_run_artifacts_v2 | GET /api/v2/replay/runs/{id}/artifacts
    public func getReplayRunArtifactsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_replay_run_artifacts_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/replay/runs/{id}/artifacts", request: normalizedRequest)
    }

    // download_replay_run_artifact_v2 | GET /api/v2/replay/runs/{id}/artifacts/{artifactType}/download
    public func downloadReplayRunArtifactV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "download_replay_run_artifact_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download", request: normalizedRequest)
    }

    // get_replay_run_diffs_v2 | GET /api/v2/replay/runs/{id}/diffs
    public func getReplayRunDiffsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_replay_run_diffs_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/replay/runs/{id}/diffs", request: normalizedRequest)
    }

    // get_residency_policy_v2 | GET /api/v2/residency/policies/current
    public func getResidencyPolicyV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "get_residency_policy_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/residency/policies/current", request: normalizedRequest)
    }

    // update_residency_policy_v2 | PUT /api/v2/residency/policies/current
    public func updateResidencyPolicyV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "update_residency_policy_v2", request: request)
        return try await self.request(method: "PUT", pathTemplate: "/api/v2/residency/policies/current", request: normalizedRequest)
    }

    // list_residency_region_mappings_v2 | GET /api/v2/residency/region-mappings
    public func listResidencyRegionMappingsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_residency_region_mappings_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/residency/region-mappings", request: normalizedRequest)
    }

    // list_residency_replications_v2 | GET /api/v2/residency/replications
    public func listResidencyReplicationsV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "list_residency_replications_v2", request: request)
        return try await self.request(method: "GET", pathTemplate: "/api/v2/residency/replications", request: normalizedRequest)
    }

    // create_residency_replication_v2 | POST /api/v2/residency/replications
    public func createResidencyReplicationV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "create_residency_replication_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/residency/replications", request: normalizedRequest)
    }

    // approve_residency_replication_v2 | POST /api/v2/residency/replications/{id}/approvals
    public func approveResidencyReplicationV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "approve_residency_replication_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/residency/replications/{id}/approvals", request: normalizedRequest)
    }

    // cancel_residency_replication_v2 | POST /api/v2/residency/replications/{id}/cancel
    public func cancelResidencyReplicationV2(request: OperationRequest = OperationRequest()) async throws -> Any {
        let normalizedRequest = self.normalizeCompatibilityRequest(operationId: "cancel_residency_replication_v2", request: request)
        return try await self.request(method: "POST", pathTemplate: "/api/v2/residency/replications/{id}/cancel", request: normalizedRequest)
    }
}
