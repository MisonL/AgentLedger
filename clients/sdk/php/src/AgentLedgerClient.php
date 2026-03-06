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
    private const OPERATION_COMPATIBILITY_JSON = <<<'JSON'
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
JSON;

    private ?array $operationCompatibility = null;

    public function __construct(
        private readonly string $baseUrl,
        private readonly ?string $token = null,
        private readonly int $timeoutSeconds = 15
    ) {
    }

    private function getOperationCompatibility(): array
    {
        if ($this->operationCompatibility !== null) {
            return $this->operationCompatibility;
        }

        $decoded = json_decode(self::OPERATION_COMPATIBILITY_JSON, true, 512, JSON_THROW_ON_ERROR);
        $this->operationCompatibility = is_array($decoded) ? $decoded : [];
        return $this->operationCompatibility;
    }

    private function resolveCompatibilityValue(array $record, array $candidates): mixed
    {
        foreach ($candidates as $candidate) {
            if (!array_key_exists($candidate, $record)) {
                continue;
            }
            $value = $record[$candidate];
            if ($value === null || $value === '') {
                continue;
            }
            return $value;
        }

        return null;
    }

    private function normalizeCompatibilityRequest(string $operationId, array $request): array
    {
        $rule = $this->getOperationCompatibility()[$operationId] ?? null;
        if (!is_array($rule)) {
            return $request;
        }

        $path = isset($request['path']) && is_array($request['path']) ? $request['path'] : [];
        $query = isset($request['query']) && is_array($request['query']) ? $request['query'] : [];
        $headers = isset($request['headers']) && is_array($request['headers']) ? $request['headers'] : [];
        $body = $request['body'] ?? null;

        foreach (($rule['pathAliases'] ?? []) as $alias) {
            if (!is_array($alias) || !isset($alias['canonical'], $alias['wire'])) {
                continue;
            }
            $value = $this->resolveCompatibilityValue($path, array_merge([$alias['canonical']], $alias['aliases'] ?? []));
            if ($value !== null) {
                $path[(string) $alias['wire']] = $value;
            }
        }

        foreach (($rule['queryAliases'] ?? []) as $alias) {
            if (!is_array($alias) || !isset($alias['canonical'])) {
                continue;
            }
            $value = $this->resolveCompatibilityValue($query, array_merge([$alias['canonical']], $alias['aliases'] ?? []));
            if ($value !== null) {
                $query[(string) $alias['canonical']] = $value;
            }
        }

        if (is_array($body)) {
            $normalizedBody = $body;
            foreach (($rule['bodyAliases'] ?? []) as $alias) {
                if (!is_array($alias) || !isset($alias['canonical'])) {
                    continue;
                }
                $value = $this->resolveCompatibilityValue($normalizedBody, array_merge([$alias['canonical']], $alias['aliases'] ?? []));
                if ($value !== null) {
                    $normalizedBody[(string) $alias['canonical']] = $value;
                }
            }
            $body = $normalizedBody;
        }

        return [
            ...$request,
            'path' => $path,
            'query' => $query,
            'body' => $body,
            'headers' => $headers,
        ];
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
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_api_keys', $request);
        return $this->request('GET', '/api/v1/api-keys', $normalizedRequest);
    }

    // create_api_key | POST /api/v1/api-keys
    public function createApiKey(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_api_key', $request);
        return $this->request('POST', '/api/v1/api-keys', $normalizedRequest);
    }

    // revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    public function revokeApiKey(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('revoke_api_key', $request);
        return $this->request('POST', '/api/v1/api-keys/{id}/revoke', $normalizedRequest);
    }

    // get_open_api_document | GET /api/v1/openapi.json
    public function getOpenApiDocument(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_open_api_document', $request);
        return $this->request('GET', '/api/v1/openapi.json', $normalizedRequest);
    }

    // create_quality_event | POST /api/v1/quality/events
    public function createQualityEvent(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_quality_event', $request);
        return $this->request('POST', '/api/v1/quality/events', $normalizedRequest);
    }

    // list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    public function listQualityDailyMetrics(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_quality_daily_metrics', $request);
        return $this->request('GET', '/api/v1/quality/metrics/daily', $normalizedRequest);
    }

    // list_quality_scorecards | GET /api/v1/quality/scorecards
    public function listQualityScorecards(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_quality_scorecards', $request);
        return $this->request('GET', '/api/v1/quality/scorecards', $normalizedRequest);
    }

    // update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    public function updateQualityScorecard(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('update_quality_scorecard', $request);
        return $this->request('PUT', '/api/v1/quality/scorecards/{id}', $normalizedRequest);
    }

    // list_replay_baselines | GET /api/v1/replay/baselines
    public function listReplayBaselines(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_replay_baselines', $request);
        return $this->request('GET', '/api/v1/replay/baselines', $normalizedRequest);
    }

    // create_replay_baseline | POST /api/v1/replay/baselines
    public function createReplayBaseline(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_replay_baseline', $request);
        return $this->request('POST', '/api/v1/replay/baselines', $normalizedRequest);
    }

    // list_replay_jobs | GET /api/v1/replay/jobs
    public function listReplayJobs(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_replay_jobs', $request);
        return $this->request('GET', '/api/v1/replay/jobs', $normalizedRequest);
    }

    // create_replay_job | POST /api/v1/replay/jobs
    public function createReplayJob(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_replay_job', $request);
        return $this->request('POST', '/api/v1/replay/jobs', $normalizedRequest);
    }

    // get_replay_job | GET /api/v1/replay/jobs/{id}
    public function getReplayJob(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_replay_job', $request);
        return $this->request('GET', '/api/v1/replay/jobs/{id}', $normalizedRequest);
    }

    // get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    public function getReplayJobDiff(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_replay_job_diff', $request);
        return $this->request('GET', '/api/v1/replay/jobs/{id}/diff', $normalizedRequest);
    }

    // list_webhook_endpoints | GET /api/v1/webhooks
    public function listWebhookEndpoints(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_webhook_endpoints', $request);
        return $this->request('GET', '/api/v1/webhooks', $normalizedRequest);
    }

    // create_webhook_endpoint | POST /api/v1/webhooks
    public function createWebhookEndpoint(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_webhook_endpoint', $request);
        return $this->request('POST', '/api/v1/webhooks', $normalizedRequest);
    }

    // update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    public function updateWebhookEndpoint(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('update_webhook_endpoint', $request);
        return $this->request('PUT', '/api/v1/webhooks/{id}', $normalizedRequest);
    }

    // delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    public function deleteWebhookEndpoint(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('delete_webhook_endpoint', $request);
        return $this->request('DELETE', '/api/v1/webhooks/{id}', $normalizedRequest);
    }

    // replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    public function replayWebhookEndpoint(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('replay_webhook_endpoint', $request);
        return $this->request('POST', '/api/v1/webhooks/{id}/replay', $normalizedRequest);
    }

    // list_webhook_replay_tasks | GET /api/v1/webhooks/replay-tasks
    public function listWebhookReplayTasks(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_webhook_replay_tasks', $request);
        return $this->request('GET', '/api/v1/webhooks/replay-tasks', $normalizedRequest);
    }

    // get_webhook_replay_task | GET /api/v1/webhooks/replay-tasks/{id}
    public function getWebhookReplayTask(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_webhook_replay_task', $request);
        return $this->request('GET', '/api/v1/webhooks/replay-tasks/{id}', $normalizedRequest);
    }

    // create_quality_evaluation_v2 | POST /api/v2/quality/evaluations
    public function createQualityEvaluationV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_quality_evaluation_v2', $request);
        return $this->request('POST', '/api/v2/quality/evaluations', $normalizedRequest);
    }

    // list_quality_metrics_v2 | GET /api/v2/quality/metrics
    public function listQualityMetricsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_quality_metrics_v2', $request);
        return $this->request('GET', '/api/v2/quality/metrics', $normalizedRequest);
    }

    // get_quality_cost_correlation_v2 | GET /api/v2/quality/reports/cost-correlation
    public function getQualityCostCorrelationV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_quality_cost_correlation_v2', $request);
        return $this->request('GET', '/api/v2/quality/reports/cost-correlation', $normalizedRequest);
    }

    // get_quality_project_trends_v2 | GET /api/v2/quality/reports/project-trends
    public function getQualityProjectTrendsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_quality_project_trends_v2', $request);
        return $this->request('GET', '/api/v2/quality/reports/project-trends', $normalizedRequest);
    }

    // list_quality_scorecards_v2 | GET /api/v2/quality/scorecards
    public function listQualityScorecardsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_quality_scorecards_v2', $request);
        return $this->request('GET', '/api/v2/quality/scorecards', $normalizedRequest);
    }

    // update_quality_scorecard_v2 | PUT /api/v2/quality/scorecards/{id}
    public function updateQualityScorecardV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('update_quality_scorecard_v2', $request);
        return $this->request('PUT', '/api/v2/quality/scorecards/{id}', $normalizedRequest);
    }

    // list_replay_datasets_v2 | GET /api/v2/replay/datasets
    public function listReplayDatasetsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_replay_datasets_v2', $request);
        return $this->request('GET', '/api/v2/replay/datasets', $normalizedRequest);
    }

    // create_replay_dataset_v2 | POST /api/v2/replay/datasets
    public function createReplayDatasetV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_replay_dataset_v2', $request);
        return $this->request('POST', '/api/v2/replay/datasets', $normalizedRequest);
    }

    // list_replay_dataset_cases_v2 | GET /api/v2/replay/datasets/{id}/cases
    public function listReplayDatasetCasesV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_replay_dataset_cases_v2', $request);
        return $this->request('GET', '/api/v2/replay/datasets/{id}/cases', $normalizedRequest);
    }

    // replace_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/cases
    public function replaceReplayDatasetCasesV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('replace_replay_dataset_cases_v2', $request);
        return $this->request('POST', '/api/v2/replay/datasets/{id}/cases', $normalizedRequest);
    }

    // materialize_replay_dataset_cases_v2 | POST /api/v2/replay/datasets/{id}/materialize
    public function materializeReplayDatasetCasesV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('materialize_replay_dataset_cases_v2', $request);
        return $this->request('POST', '/api/v2/replay/datasets/{id}/materialize', $normalizedRequest);
    }

    // list_replay_runs_v2 | GET /api/v2/replay/runs
    public function listReplayRunsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_replay_runs_v2', $request);
        return $this->request('GET', '/api/v2/replay/runs', $normalizedRequest);
    }

    // create_replay_run_v2 | POST /api/v2/replay/runs
    public function createReplayRunV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_replay_run_v2', $request);
        return $this->request('POST', '/api/v2/replay/runs', $normalizedRequest);
    }

    // get_replay_run_v2 | GET /api/v2/replay/runs/{id}
    public function getReplayRunV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_replay_run_v2', $request);
        return $this->request('GET', '/api/v2/replay/runs/{id}', $normalizedRequest);
    }

    // get_replay_run_artifacts_v2 | GET /api/v2/replay/runs/{id}/artifacts
    public function getReplayRunArtifactsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_replay_run_artifacts_v2', $request);
        return $this->request('GET', '/api/v2/replay/runs/{id}/artifacts', $normalizedRequest);
    }

    // download_replay_run_artifact_v2 | GET /api/v2/replay/runs/{id}/artifacts/{artifactType}/download
    public function downloadReplayRunArtifactV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('download_replay_run_artifact_v2', $request);
        return $this->request('GET', '/api/v2/replay/runs/{id}/artifacts/{artifactType}/download', $normalizedRequest);
    }

    // get_replay_run_diffs_v2 | GET /api/v2/replay/runs/{id}/diffs
    public function getReplayRunDiffsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_replay_run_diffs_v2', $request);
        return $this->request('GET', '/api/v2/replay/runs/{id}/diffs', $normalizedRequest);
    }

    // get_residency_policy_v2 | GET /api/v2/residency/policies/current
    public function getResidencyPolicyV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('get_residency_policy_v2', $request);
        return $this->request('GET', '/api/v2/residency/policies/current', $normalizedRequest);
    }

    // update_residency_policy_v2 | PUT /api/v2/residency/policies/current
    public function updateResidencyPolicyV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('update_residency_policy_v2', $request);
        return $this->request('PUT', '/api/v2/residency/policies/current', $normalizedRequest);
    }

    // list_residency_region_mappings_v2 | GET /api/v2/residency/region-mappings
    public function listResidencyRegionMappingsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_residency_region_mappings_v2', $request);
        return $this->request('GET', '/api/v2/residency/region-mappings', $normalizedRequest);
    }

    // list_residency_replications_v2 | GET /api/v2/residency/replications
    public function listResidencyReplicationsV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('list_residency_replications_v2', $request);
        return $this->request('GET', '/api/v2/residency/replications', $normalizedRequest);
    }

    // create_residency_replication_v2 | POST /api/v2/residency/replications
    public function createResidencyReplicationV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('create_residency_replication_v2', $request);
        return $this->request('POST', '/api/v2/residency/replications', $normalizedRequest);
    }

    // approve_residency_replication_v2 | POST /api/v2/residency/replications/{id}/approvals
    public function approveResidencyReplicationV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('approve_residency_replication_v2', $request);
        return $this->request('POST', '/api/v2/residency/replications/{id}/approvals', $normalizedRequest);
    }

    // cancel_residency_replication_v2 | POST /api/v2/residency/replications/{id}/cancel
    public function cancelResidencyReplicationV2(array $request = []): mixed
    {
        $normalizedRequest = $this->normalizeCompatibilityRequest('cancel_residency_replication_v2', $request);
        return $this->request('POST', '/api/v2/residency/replications/{id}/cancel', $normalizedRequest);
    }
}
