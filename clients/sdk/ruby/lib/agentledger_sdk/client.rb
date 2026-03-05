# frozen_string_literal: true

require "json"
require "net/http"
require "uri"
require "cgi"

module AgentLedgerSdk
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
      request_api("GET", "/api/v1/api-keys", request)
    end

    # create_api_key | POST /api/v1/api-keys
    def create_api_key(request = {})
      request_api("POST", "/api/v1/api-keys", request)
    end

    # revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    def revoke_api_key(request = {})
      request_api("POST", "/api/v1/api-keys/{id}/revoke", request)
    end

    # get_open_api_document | GET /api/v1/openapi.json
    def get_open_api_document(request = {})
      request_api("GET", "/api/v1/openapi.json", request)
    end

    # create_quality_event | POST /api/v1/quality/events
    def create_quality_event(request = {})
      request_api("POST", "/api/v1/quality/events", request)
    end

    # list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    def list_quality_daily_metrics(request = {})
      request_api("GET", "/api/v1/quality/metrics/daily", request)
    end

    # list_quality_scorecards | GET /api/v1/quality/scorecards
    def list_quality_scorecards(request = {})
      request_api("GET", "/api/v1/quality/scorecards", request)
    end

    # update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    def update_quality_scorecard(request = {})
      request_api("PUT", "/api/v1/quality/scorecards/{id}", request)
    end

    # list_replay_baselines | GET /api/v1/replay/baselines
    def list_replay_baselines(request = {})
      request_api("GET", "/api/v1/replay/baselines", request)
    end

    # create_replay_baseline | POST /api/v1/replay/baselines
    def create_replay_baseline(request = {})
      request_api("POST", "/api/v1/replay/baselines", request)
    end

    # list_replay_jobs | GET /api/v1/replay/jobs
    def list_replay_jobs(request = {})
      request_api("GET", "/api/v1/replay/jobs", request)
    end

    # create_replay_job | POST /api/v1/replay/jobs
    def create_replay_job(request = {})
      request_api("POST", "/api/v1/replay/jobs", request)
    end

    # get_replay_job | GET /api/v1/replay/jobs/{id}
    def get_replay_job(request = {})
      request_api("GET", "/api/v1/replay/jobs/{id}", request)
    end

    # get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    def get_replay_job_diff(request = {})
      request_api("GET", "/api/v1/replay/jobs/{id}/diff", request)
    end

    # list_webhook_endpoints | GET /api/v1/webhooks
    def list_webhook_endpoints(request = {})
      request_api("GET", "/api/v1/webhooks", request)
    end

    # create_webhook_endpoint | POST /api/v1/webhooks
    def create_webhook_endpoint(request = {})
      request_api("POST", "/api/v1/webhooks", request)
    end

    # update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    def update_webhook_endpoint(request = {})
      request_api("PUT", "/api/v1/webhooks/{id}", request)
    end

    # delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    def delete_webhook_endpoint(request = {})
      request_api("DELETE", "/api/v1/webhooks/{id}", request)
    end

    # replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    def replay_webhook_endpoint(request = {})
      request_api("POST", "/api/v1/webhooks/{id}/replay", request)
    end
  end
end
