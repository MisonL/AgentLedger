import Foundation

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
        return try await self.request(method: "GET", pathTemplate: "/api/v1/api-keys", request: request)
    }

    // create_api_key | POST /api/v1/api-keys
    public func createApiKey(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "POST", pathTemplate: "/api/v1/api-keys", request: request)
    }

    // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    public func revokeApiKey(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "POST", pathTemplate: "/api/v1/api-keys/{id}/revoke", request: request)
    }

    // get_open_api_document | GET /api/v1/openapi.json
    public func getOpenApiDocument(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/openapi.json", request: request)
    }

    // create_quality_event | POST /api/v1/quality/events
    public func createQualityEvent(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "POST", pathTemplate: "/api/v1/quality/events", request: request)
    }

    // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    public func listQualityDailyMetrics(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/quality/metrics/daily", request: request)
    }

    // list_quality_scorecards | GET /api/v1/quality/scorecards
    public func listQualityScorecards(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/quality/scorecards", request: request)
    }

    // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    public func updateQualityScorecard(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "PUT", pathTemplate: "/api/v1/quality/scorecards/{id}", request: request)
    }

    // list_replay_baselines | GET /api/v1/replay/baselines
    public func listReplayBaselines(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/baselines", request: request)
    }

    // create_replay_baseline | POST /api/v1/replay/baselines
    public func createReplayBaseline(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "POST", pathTemplate: "/api/v1/replay/baselines", request: request)
    }

    // list_replay_jobs | GET /api/v1/replay/jobs
    public func listReplayJobs(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/jobs", request: request)
    }

    // create_replay_job | POST /api/v1/replay/jobs
    public func createReplayJob(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "POST", pathTemplate: "/api/v1/replay/jobs", request: request)
    }

    // get_replay_job | GET /api/v1/replay/jobs/{id}
    public func getReplayJob(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/jobs/{id}", request: request)
    }

    // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    public func getReplayJobDiff(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/replay/jobs/{id}/diff", request: request)
    }

    // list_webhook_endpoints | GET /api/v1/webhooks
    public func listWebhookEndpoints(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "GET", pathTemplate: "/api/v1/webhooks", request: request)
    }

    // create_webhook_endpoint | POST /api/v1/webhooks
    public func createWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "POST", pathTemplate: "/api/v1/webhooks", request: request)
    }

    // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    public func updateWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "PUT", pathTemplate: "/api/v1/webhooks/{id}", request: request)
    }

    // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    public func deleteWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "DELETE", pathTemplate: "/api/v1/webhooks/{id}", request: request)
    }

    // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    public func replayWebhookEndpoint(request: OperationRequest = OperationRequest()) async throws -> Any {
        return try await self.request(method: "POST", pathTemplate: "/api/v1/webhooks/{id}/replay", request: request)
    }
}
