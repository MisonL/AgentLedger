<?php

declare(strict_types=1);

namespace AgentLedger\Sdk;

final class AgentLedgerApiError extends \RuntimeException
{
    public function __construct(public readonly int $status, public readonly mixed $payload)
    {
        parent::__construct('request failed: ' . $status);
    }
}

final class AgentLedgerClient
{
    public function __construct(
        private readonly string $baseUrl,
        private readonly ?string $token = null,
        private readonly int $timeoutSeconds = 15
    ) {
    }

    private function renderPath(string $pathTemplate, array $pathParams): string
    {
        $result = $pathTemplate;
        if (preg_match_all('/\{([^}]+)\}/', $pathTemplate, $matches) === false) {
            return $pathTemplate;
        }
        foreach ($matches[1] as $rawKey) {
            $key = trim((string) $rawKey);
            if (!array_key_exists($key, $pathParams)) {
                throw new \InvalidArgumentException('缺少 path 参数: ' . $key);
            }
            $result = str_replace('{' . $key . '}', rawurlencode((string) $pathParams[$key]), $result);
        }
        return $result;
    }

    private function buildQuery(array $query): string
    {
        $filtered = [];
        foreach ($query as $key => $value) {
            if ($value === null || $value === '') {
                continue;
            }
            $filtered[(string) $key] = (string) $value;
        }
        if (count($filtered) === 0) {
            return '';
        }
        return '?' . http_build_query($filtered);
    }

    private function request(string $method, string $pathTemplate, array $request): mixed
    {
        $pathParams = isset($request['path']) && is_array($request['path']) ? $request['path'] : [];
        $query = isset($request['query']) && is_array($request['query']) ? $request['query'] : [];
        $headers = isset($request['headers']) && is_array($request['headers']) ? $request['headers'] : [];
        $body = $request['body'] ?? null;

        $url = rtrim($this->baseUrl, '/') . $this->renderPath($pathTemplate, $pathParams) . $this->buildQuery($query);

        $curl = curl_init($url);
        if ($curl === false) {
            throw new \RuntimeException('curl 初始化失败');
        }

        $headerLines = ['content-type: application/json'];
        if ($this->token !== null && $this->token !== '') {
            $headerLines[] = 'authorization: Bearer ' . $this->token;
        }
        foreach ($headers as $key => $value) {
            $headerLines[] = (string) $key . ': ' . (string) $value;
        }

        curl_setopt_array($curl, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_TIMEOUT => $this->timeoutSeconds,
            CURLOPT_HTTPHEADER => $headerLines,
            CURLOPT_HEADER => true,
        ]);

        if ($body !== null) {
            curl_setopt($curl, CURLOPT_POSTFIELDS, json_encode($body, JSON_UNESCAPED_UNICODE));
        }

        $response = curl_exec($curl);
        if ($response === false) {
            $error = curl_error($curl);
            curl_close($curl);
            throw new \RuntimeException($error);
        }

        $statusCode = curl_getinfo($curl, CURLINFO_RESPONSE_CODE);
        $headerSize = curl_getinfo($curl, CURLINFO_HEADER_SIZE);
        $payload = substr((string) $response, (int) $headerSize);
        curl_close($curl);

        if ($statusCode < 200 || $statusCode >= 300) {
            throw new AgentLedgerApiError((int) $statusCode, $payload);
        }

        $decoded = json_decode($payload, true);
        if (json_last_error() === JSON_ERROR_NONE) {
            return $decoded;
        }
        return $payload;
    }

    // list_api_keys | GET /api/v1/api-keys
    public function listApiKeys(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/api-keys', $request);
    }

    // create_api_key | POST /api/v1/api-keys
    public function createApiKey(array $request = []): mixed
    {
        return $this->request('POST', '/api/v1/api-keys', $request);
    }

    // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    public function revokeApiKey(array $request = []): mixed
    {
        return $this->request('POST', '/api/v1/api-keys/{id}/revoke', $request);
    }

    // get_open_api_document | GET /api/v1/openapi.json
    public function getOpenApiDocument(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/openapi.json', $request);
    }

    // create_quality_event | POST /api/v1/quality/events
    public function createQualityEvent(array $request = []): mixed
    {
        return $this->request('POST', '/api/v1/quality/events', $request);
    }

    // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    public function listQualityDailyMetrics(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/quality/metrics/daily', $request);
    }

    // list_quality_scorecards | GET /api/v1/quality/scorecards
    public function listQualityScorecards(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/quality/scorecards', $request);
    }

    // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    public function updateQualityScorecard(array $request = []): mixed
    {
        return $this->request('PUT', '/api/v1/quality/scorecards/{id}', $request);
    }

    // list_replay_baselines | GET /api/v1/replay/baselines
    public function listReplayBaselines(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/replay/baselines', $request);
    }

    // create_replay_baseline | POST /api/v1/replay/baselines
    public function createReplayBaseline(array $request = []): mixed
    {
        return $this->request('POST', '/api/v1/replay/baselines', $request);
    }

    // list_replay_jobs | GET /api/v1/replay/jobs
    public function listReplayJobs(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/replay/jobs', $request);
    }

    // create_replay_job | POST /api/v1/replay/jobs
    public function createReplayJob(array $request = []): mixed
    {
        return $this->request('POST', '/api/v1/replay/jobs', $request);
    }

    // get_replay_job | GET /api/v1/replay/jobs/{id}
    public function getReplayJob(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/replay/jobs/{id}', $request);
    }

    // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    public function getReplayJobDiff(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/replay/jobs/{id}/diff', $request);
    }

    // list_webhook_endpoints | GET /api/v1/webhooks
    public function listWebhookEndpoints(array $request = []): mixed
    {
        return $this->request('GET', '/api/v1/webhooks', $request);
    }

    // create_webhook_endpoint | POST /api/v1/webhooks
    public function createWebhookEndpoint(array $request = []): mixed
    {
        return $this->request('POST', '/api/v1/webhooks', $request);
    }

    // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    public function updateWebhookEndpoint(array $request = []): mixed
    {
        return $this->request('PUT', '/api/v1/webhooks/{id}', $request);
    }

    // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    public function deleteWebhookEndpoint(array $request = []): mixed
    {
        return $this->request('DELETE', '/api/v1/webhooks/{id}', $request);
    }

    // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    public function replayWebhookEndpoint(array $request = []): mixed
    {
        return $this->request('POST', '/api/v1/webhooks/{id}/replay', $request);
    }
}
