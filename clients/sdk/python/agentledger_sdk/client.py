from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
import json
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen


class AgentLedgerApiError(Exception):
    def __init__(self, status: int, message: str, payload: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.payload = payload


@dataclass
class OperationRequest:
    path: Dict[str, Any] = field(default_factory=dict)
    query: Dict[str, Any] = field(default_factory=dict)
    body: Any = None
    headers: Dict[str, str] = field(default_factory=dict)


class AgentLedgerClient:
    def __init__(self, base_url: str, token: Optional[str] = None, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _render_path(self, path_template: str, path_params: Dict[str, Any]) -> str:
        path_value = path_template
        for raw_key in [segment[1:-1] for segment in path_template.split("/") if segment.startswith("{") and segment.endswith("}")]:
            if raw_key not in path_params:
                raise ValueError(f"缺少 path 参数: {raw_key}")
            path_value = path_value.replace("{" + raw_key + "}", quote(str(path_params[raw_key]), safe=""))
        return path_value

    def _build_query(self, query: Dict[str, Any]) -> str:
        normalized: Dict[str, str] = {}
        for key, value in query.items():
            if value is None:
                continue
            normalized[key] = str(value)
        if not normalized:
            return ""
        return "?" + urlencode(normalized)

    def _request(self, method: str, path_template: str, request: OperationRequest) -> Any:
        url = self.base_url + self._render_path(path_template, request.path) + self._build_query(request.query)
        headers: Dict[str, str] = {"content-type": "application/json", **request.headers}
        if self.token and "authorization" not in {key.lower(): value for key, value in headers.items()}:
            headers["authorization"] = f"Bearer {self.token}"

        data = None
        if request.body is not None:
            data = json.dumps(request.body).encode("utf-8")

        req = Request(url=url, method=method, headers=headers, data=data)
        try:
            with urlopen(req, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8") if response.length is None or response.length > 0 else ""
                content_type = response.headers.get("content-type", "")
                if "application/json" in content_type.lower() and raw:
                    return json.loads(raw)
                return raw
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "code", 500)
            payload = None
            message = str(exc)
            raise AgentLedgerApiError(int(status), message, payload) from exc

    # list_api_keys | GET /api/v1/api-keys
    def list_api_keys(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/api-keys", req)

    # create_api_key | POST /api/v1/api-keys
    def create_api_key(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("POST", "/api/v1/api-keys", req)

    # revoke_api_key | POST /api/v1/api-keys/{id}/revoke
    def revoke_api_key(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("POST", "/api/v1/api-keys/{id}/revoke", req)

    # get_open_api_document | GET /api/v1/openapi.json
    def get_open_api_document(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/openapi.json", req)

    # create_quality_event | POST /api/v1/quality/events
    def create_quality_event(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("POST", "/api/v1/quality/events", req)

    # list_quality_daily_metrics | GET /api/v1/quality/metrics/daily
    def list_quality_daily_metrics(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/quality/metrics/daily", req)

    # list_quality_scorecards | GET /api/v1/quality/scorecards
    def list_quality_scorecards(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/quality/scorecards", req)

    # update_quality_scorecard | PUT /api/v1/quality/scorecards/{id}
    def update_quality_scorecard(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("PUT", "/api/v1/quality/scorecards/{id}", req)

    # list_replay_baselines | GET /api/v1/replay/baselines
    def list_replay_baselines(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/replay/baselines", req)

    # create_replay_baseline | POST /api/v1/replay/baselines
    def create_replay_baseline(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("POST", "/api/v1/replay/baselines", req)

    # list_replay_jobs | GET /api/v1/replay/jobs
    def list_replay_jobs(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/replay/jobs", req)

    # create_replay_job | POST /api/v1/replay/jobs
    def create_replay_job(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("POST", "/api/v1/replay/jobs", req)

    # get_replay_job | GET /api/v1/replay/jobs/{id}
    def get_replay_job(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/replay/jobs/{id}", req)

    # get_replay_job_diff | GET /api/v1/replay/jobs/{id}/diff
    def get_replay_job_diff(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/replay/jobs/{id}/diff", req)

    # list_webhook_endpoints | GET /api/v1/webhooks
    def list_webhook_endpoints(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("GET", "/api/v1/webhooks", req)

    # create_webhook_endpoint | POST /api/v1/webhooks
    def create_webhook_endpoint(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("POST", "/api/v1/webhooks", req)

    # update_webhook_endpoint | PUT /api/v1/webhooks/{id}
    def update_webhook_endpoint(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("PUT", "/api/v1/webhooks/{id}", req)

    # delete_webhook_endpoint | DELETE /api/v1/webhooks/{id}
    def delete_webhook_endpoint(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("DELETE", "/api/v1/webhooks/{id}", req)

    # replay_webhook_endpoint | POST /api/v1/webhooks/{id}/replay
    def replay_webhook_endpoint(self, request: Optional[OperationRequest] = None) -> Any:
        req = request or OperationRequest()
        return self._request("POST", "/api/v1/webhooks/{id}/replay", req)
