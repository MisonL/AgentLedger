# frozen_string_literal: true

require "json"
require "net/http"
require "uri"
require "cgi"

module AgentLedgerSdk
  OPERATION_COMPATIBILITY = JSON.parse(<<~JSON).freeze
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
JSON

  class ApiError < StandardError
    attr_reader :status, :payload

    def initialize(status, payload)
      super("request failed: #{status}")
      @status = status
      @payload = payload
    end
  end

  class Client
    def initialize(base_url:, token: nil, timeout_seconds: 15)
      @base_url = base_url.sub(%r{/+$}, "")
      @token = token
      @timeout_seconds = timeout_seconds
    end

    def resolve_compatibility_value(record, candidates)
      candidates.each do |candidate|
        if record.key?(candidate)
          value = record[candidate]
        elsif record.key?(candidate.to_sym)
          value = record[candidate.to_sym]
        else
          next
        end

        next if value.nil?
        next if value.respond_to?(:empty?) && value.empty?

        return value
      end
      nil
    end

    def normalize_compatibility_request(operation_id, request)
      rule = OPERATION_COMPATIBILITY[operation_id]
      return request unless rule

      path_source = request.fetch(:path, request.fetch("path", {}))
      query_source = request.fetch(:query, request.fetch("query", {}))
      headers_source = request.fetch(:headers, request.fetch("headers", {}))
      body = request.key?(:body) ? request[:body] : request["body"]

      path = path_source.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value.to_s }
      query = query_source.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value.to_s unless value.nil? }
      headers = headers_source.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value.to_s }

      Array(rule["pathAliases"]).each do |alias_rule|
        value = resolve_compatibility_value(path, [alias_rule["canonical"], *Array(alias_rule["aliases"])])
        path[alias_rule["wire"]] = value.to_s unless value.nil?
      end

      Array(rule["queryAliases"]).each do |alias_rule|
        value = resolve_compatibility_value(query, [alias_rule["canonical"], *Array(alias_rule["aliases"])])
        query[alias_rule["canonical"]] = value.to_s unless value.nil?
      end

      if body.is_a?(Hash)
        normalized_body = body.each_with_object({}) { |(key, value), memo| memo[key.to_s] = value }
        Array(rule["bodyAliases"]).each do |alias_rule|
          value = resolve_compatibility_value(normalized_body, [alias_rule["canonical"], *Array(alias_rule["aliases"])])
          normalized_body[alias_rule["canonical"]] = value unless value.nil?
        end
        body = normalized_body
      end

      {
        path: path,
        query: query,
        body: body,
        headers: headers,
      }
    end

    def render_path(path_template, path_params)
      path_value = path_template.dup
      path_value.scan(/\{([^}]+)\}/).flatten.each do |raw_key|
        key = raw_key.strip
        raise ArgumentError, "缺少 path 参数: #{key}" unless path_params.key?(key)

        path_value.gsub!("{#{key}}", CGI.escape(path_params[key].to_s))
      end
      path_value
    end

    def build_query(query_hash)
      filtered = query_hash.each_with_object({}) do |(key, value), memo|
        next if value.nil?

        text = value.to_s
        next if text.empty?

        memo[key.to_s] = text
      end
      return "" if filtered.empty?

      "?" + URI.encode_www_form(filtered)
    end

    def request_api(method, path_template, request = {})
      path_params = request.fetch(:path, request.fetch("path", {}))
      query = request.fetch(:query, request.fetch("query", {}))
      body = request.key?(:body) ? request[:body] : request["body"]
      headers = request.fetch(:headers, request.fetch("headers", {}))

      uri = URI.parse(@base_url + render_path(path_template, path_params) + build_query(query))
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = uri.scheme == "https"
      http.read_timeout = @timeout_seconds

      klass = Net::HTTP.const_get(method.capitalize)
      http_request = klass.new(uri.request_uri)
      http_request["content-type"] = "application/json"
      http_request["authorization"] = "Bearer #{@token}" if @token && !@token.empty?
      headers.each { |key, value| http_request[key.to_s] = value.to_s }
      http_request.body = JSON.generate(body) unless body.nil?

      response = http.request(http_request)
      payload = response.body.to_s

      unless response.code.to_i.between?(200, 299)
        raise ApiError.new(response.code.to_i, payload)
      end

      JSON.parse(payload)
    rescue JSON::ParserError
      payload
    end

    # list_api_keys | GET /api/v1/api-keys
    def list_api_keys(request = {})
      normalized_request = normalize_compatibility_request("list_api_keys", request)
      request_api("GET", "/api/v1/api-keys", normalized_request)
    end

    # create_api_key | POST /api/v1/api-keys
    def create_api_key(request = {})
      normalized_request = normalize_compatibility_request("create_api_key", request)
      request_api("POST", "/api/v1/api-keys", normalized_request)
    end

    # revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    def revoke_api_key(request = {})
      normalized_request = normalize_compatibility_request("revoke_api_key", request)
      request_api("POST", "/api/v1/api-keys/{id}/revoke", normalized_request)
    end

    # get_open_api_document | GET /api/v1/openapi.json
    def get_open_api_document(request = {})
      normalized_request = normalize_compatibility_request("get_open_api_document", request)
      request_api("GET", "/api/v1/openapi.json", normalized_request)
    end

    # create_quality_event | POST /api/v1/quality/events
    def create_quality_event(request = {})
      normalized_request = normalize_compatibility_request("create_quality_event", request)
      request_api("POST", "/api/v1/quality/events", normalized_request)
    end

    # list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    def list_quality_daily_metrics(request = {})
      normalized_request = normalize_compatibility_request("list_quality_daily_metrics", request)
      request_api("GET", "/api/v1/quality/metrics/daily", normalized_request)
    end

    # list_quality_scorecards | GET /api/v1/quality/scorecards
    def list_quality_scorecards(request = {})
      normalized_request = normalize_compatibility_request("list_quality_scorecards", request)
      request_api("GET", "/api/v1/quality/scorecards", normalized_request)
    end

    # update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    def update_quality_scorecard(request = {})
      normalized_request = normalize_compatibility_request("update_quality_scorecard", request)
      request_api("PUT", "/api/v1/quality/scorecards/{id}", normalized_request)
    end

    # list_replay_baselines | GET /api/v1/replay/baselines
    def list_replay_baselines(request = {})
      normalized_request = normalize_compatibility_request("list_replay_baselines", request)
      request_api("GET", "/api/v1/replay/baselines", normalized_request)
    end

    # create_replay_baseline | POST /api/v1/replay/baselines
    def create_replay_baseline(request = {})
      normalized_request = normalize_compatibility_request("create_replay_baseline", request)
      request_api("POST", "/api/v1/replay/baselines", normalized_request)
    end

    # list_replay_jobs | GET /api/v1/replay/jobs
    def list_replay_jobs(request = {})
      normalized_request = normalize_compatibility_request("list_replay_jobs", request)
      request_api("GET", "/api/v1/replay/jobs", normalized_request)
    end

    # create_replay_job | POST /api/v1/replay/jobs
    def create_replay_job(request = {})
      normalized_request = normalize_compatibility_request("create_replay_job", request)
      request_api("POST", "/api/v1/replay/jobs", normalized_request)
    end

    # get_replay_job | GET /api/v1/replay/jobs/{id}
    def get_replay_job(request = {})
      normalized_request = normalize_compatibility_request("get_replay_job", request)
      request_api("GET", "/api/v1/replay/jobs/{id}", normalized_request)
    end

    # get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    def get_replay_job_diff(request = {})
      normalized_request = normalize_compatibility_request("get_replay_job_diff", request)
      request_api("GET", "/api/v1/replay/jobs/{id}/diff", normalized_request)
    end

    # list_webhook_endpoints | GET /api/v1/webhooks
    def list_webhook_endpoints(request = {})
      normalized_request = normalize_compatibility_request("list_webhook_endpoints", request)
      request_api("GET", "/api/v1/webhooks", normalized_request)
    end

    # create_webhook_endpoint | POST /api/v1/webhooks
    def create_webhook_endpoint(request = {})
      normalized_request = normalize_compatibility_request("create_webhook_endpoint", request)
      request_api("POST", "/api/v1/webhooks", normalized_request)
    end

    # update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    def update_webhook_endpoint(request = {})
      normalized_request = normalize_compatibility_request("update_webhook_endpoint", request)
      request_api("PUT", "/api/v1/webhooks/{id}", normalized_request)
    end

    # delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    def delete_webhook_endpoint(request = {})
      normalized_request = normalize_compatibility_request("delete_webhook_endpoint", request)
      request_api("DELETE", "/api/v1/webhooks/{id}", normalized_request)
    end

    # replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    def replay_webhook_endpoint(request = {})
      normalized_request = normalize_compatibility_request("replay_webhook_endpoint", request)
      request_api("POST", "/api/v1/webhooks/{id}/replay", normalized_request)
    end

    # list_webhook_replay_tasks | GET /api/v1/webhooks/replay-tasks
    def list_webhook_replay_tasks(request = {})
      normalized_request = normalize_compatibility_request("list_webhook_replay_tasks", request)
      request_api("GET", "/api/v1/webhooks/replay-tasks", normalized_request)
    end

    # get_webhook_replay_task | GET /api/v1/webhooks/replay-tasks/{id}
    def get_webhook_replay_task(request = {})
      normalized_request = normalize_compatibility_request("get_webhook_replay_task", request)
      request_api("GET", "/api/v1/webhooks/replay-tasks/{id}", normalized_request)
    end

    # create_quality_evaluation_v2 | POST /api/v2/quality/evaluations
    def create_quality_evaluation_v2(request = {})
      normalized_request = normalize_compatibility_request("create_quality_evaluation_v2", request)
      request_api("POST", "/api/v2/quality/evaluations", normalized_request)
    end

    # list_quality_metrics_v2 | GET /api/v2/quality/metrics
    def list_quality_metrics_v2(request = {})
      normalized_request = normalize_compatibility_request("list_quality_metrics_v2", request)
      request_api("GET", "/api/v2/quality/metrics", normalized_request)
    end

    # get_quality_cost_correlation_v2 | GET /api/v2/quality/reports/cost-correlation
    def get_quality_cost_correlation_v2(request = {})
      normalized_request = normalize_compatibility_request("get_quality_cost_correlation_v2", request)
      request_api("GET", "/api/v2/quality/reports/cost-correlation", normalized_request)
    end

    # get_quality_project_trends_v2 | GET /api/v2/quality/reports/project-trends
    def get_quality_project_trends_v2(request = {})
      normalized_request = normalize_compatibility_request("get_quality_project_trends_v2", request)
      request_api("GET", "/api/v2/quality/reports/project-trends", normalized_request)
    end

    # list_quality_scorecards_v2 | GET /api/v2/quality/scorecards
    def list_quality_scorecards_v2(request = {})
      normalized_request = normalize_compatibility_request("list_quality_scorecards_v2", request)
      request_api("GET", "/api/v2/quality/scorecards", normalized_request)
    end

    # update_quality_scorecard_v2 | PUT /api/v2/quality/scorecards/{id}
    def update_quality_scorecard_v2(request = {})
      normalized_request = normalize_compatibility_request("update_quality_scorecard_v2", request)
      request_api("PUT", "/api/v2/quality/scorecards/{id}", normalized_request)
    end

    # list_replay_datasets_v2 | GET /api/v2/replay/datasets
    def list_replay_datasets_v2(request = {})
      normalized_request = normalize_compatibility_request("list_replay_datasets_v2", request)
      request_api("GET", "/api/v2/replay/datasets", normalized_request)
    end

    # create_replay_dataset_v2 | POST /api/v2/replay/datasets
    def create_replay_dataset_v2(request = {})
      normalized_request = normalize_compatibility_request("create_replay_dataset_v2", request)
      request_api("POST", "/api/v2/replay/datasets", normalized_request)
    end

    # list_replay_dataset_cases_v2 | GET /api/v2/replay/datasets/{id}/cases
    def list_replay_dataset_cases_v2(request = {})
      normalized_request = normalize_compatibility_request("list_replay_dataset_cases_v2", request)
      request_api("GET", "/api/v2/replay/datasets/{id}/cases", normalized_request)
    end

    # replace_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/cases
    def replace_replay_dataset_cases_v2(request = {})
      normalized_request = normalize_compatibility_request("replace_replay_dataset_cases_v2", request)
      request_api("POST", "/api/v2/replay/datasets/{id}/cases", normalized_request)
    end

    # materialize_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/materialize
    def materialize_replay_dataset_cases_v2(request = {})
      normalized_request = normalize_compatibility_request("materialize_replay_dataset_cases_v2", request)
      request_api("POST", "/api/v2/replay/datasets/{id}/materialize", normalized_request)
    end

    # list_replay_runs_v2 | GET /api/v2/replay/runs
    def list_replay_runs_v2(request = {})
      normalized_request = normalize_compatibility_request("list_replay_runs_v2", request)
      request_api("GET", "/api/v2/replay/runs", normalized_request)
    end

    # create_replay_run_v2 | POST /api/v2/replay/runs
    def create_replay_run_v2(request = {})
      normalized_request = normalize_compatibility_request("create_replay_run_v2", request)
      request_api("POST", "/api/v2/replay/runs", normalized_request)
    end

    # get_replay_run_v2 | GET /api/v2/replay/runs/{id}
    def get_replay_run_v2(request = {})
      normalized_request = normalize_compatibility_request("get_replay_run_v2", request)
      request_api("GET", "/api/v2/replay/runs/{id}", normalized_request)
    end

    # get_replay_run_artifacts_v2 | GET /api/v2/replay/runs/{id}/artifacts
    def get_replay_run_artifacts_v2(request = {})
      normalized_request = normalize_compatibility_request("get_replay_run_artifacts_v2", request)
      request_api("GET", "/api/v2/replay/runs/{id}/artifacts", normalized_request)
    end

    # download_replay_run_artifact_v2 | GET /api/v2/replay/runs/{id}/artifacts/{artifactType}/download
    def download_replay_run_artifact_v2(request = {})
      normalized_request = normalize_compatibility_request("download_replay_run_artifact_v2", request)
      request_api("GET", "/api/v2/replay/runs/{id}/artifacts/{artifactType}/download", normalized_request)
    end

    # get_replay_run_diffs_v2 | GET /api/v2/replay/runs/{id}/diffs
    def get_replay_run_diffs_v2(request = {})
      normalized_request = normalize_compatibility_request("get_replay_run_diffs_v2", request)
      request_api("GET", "/api/v2/replay/runs/{id}/diffs", normalized_request)
    end

    # get_residency_policy_v2 | GET /api/v2/residency/policies/current
    def get_residency_policy_v2(request = {})
      normalized_request = normalize_compatibility_request("get_residency_policy_v2", request)
      request_api("GET", "/api/v2/residency/policies/current", normalized_request)
    end

    # update_residency_policy_v2 | PUT /api/v2/residency/policies/current
    def update_residency_policy_v2(request = {})
      normalized_request = normalize_compatibility_request("update_residency_policy_v2", request)
      request_api("PUT", "/api/v2/residency/policies/current", normalized_request)
    end

    # list_residency_region_mappings_v2 | GET /api/v2/residency/region-mappings
    def list_residency_region_mappings_v2(request = {})
      normalized_request = normalize_compatibility_request("list_residency_region_mappings_v2", request)
      request_api("GET", "/api/v2/residency/region-mappings", normalized_request)
    end

    # list_residency_replications_v2 | GET /api/v2/residency/replications
    def list_residency_replications_v2(request = {})
      normalized_request = normalize_compatibility_request("list_residency_replications_v2", request)
      request_api("GET", "/api/v2/residency/replications", normalized_request)
    end

    # create_residency_replication_v2 | POST /api/v2/residency/replications
    def create_residency_replication_v2(request = {})
      normalized_request = normalize_compatibility_request("create_residency_replication_v2", request)
      request_api("POST", "/api/v2/residency/replications", normalized_request)
    end

    # approve_residency_replication_v2 | POST /api/v2/residency/replications/{id}/approvals
    def approve_residency_replication_v2(request = {})
      normalized_request = normalize_compatibility_request("approve_residency_replication_v2", request)
      request_api("POST", "/api/v2/residency/replications/{id}/approvals", normalized_request)
    end

    # cancel_residency_replication_v2 | POST /api/v2/residency/replications/{id}/cancel
    def cancel_residency_replication_v2(request = {})
      normalized_request = normalize_compatibility_request("cancel_residency_replication_v2", request)
      request_api("POST", "/api/v2/residency/replications/{id}/cancel", normalized_request)
    end
  end
end
