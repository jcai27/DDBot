from __future__ import annotations

import json
from urllib import parse, request


class SlackApiError(RuntimeError):
    """Raised when Slack API returns ok=false for a method call."""
    def __init__(self, method: str, error: str) -> None:
        super().__init__(f"Slack API error for {method}: {error}")
        self.method = method
        self.error = error


class SlackApiClient:
    """Minimal Slack Web API HTTP client shared across read/write paths."""
    def __init__(self, bot_token: str, timeout_seconds: int = 30) -> None:
        self.bot_token = bot_token
        self.timeout_seconds = timeout_seconds

    def api_get(self, method: str, params: dict[str, str]) -> dict:
        """Call a Slack GET-style endpoint and return decoded JSON payload."""
        query = parse.urlencode({k: v for k, v in params.items() if v != ""})
        url = f"https://slack.com/api/{method}?{query}"
        req = request.Request(url=url, method="GET")
        req.add_header("Authorization", f"Bearer {self.bot_token}")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with request.urlopen(req, timeout=self.timeout_seconds) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload.get("ok"):
            raise SlackApiError(method=method, error=payload.get("error", "unknown_error"))
        return payload

    def api_post(self, method: str, payload: dict) -> dict:
        """Call a Slack POST-style endpoint and return decoded JSON payload."""
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(url=f"https://slack.com/api/{method}", data=body, method="POST")
        req.add_header("Authorization", f"Bearer {self.bot_token}")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        with request.urlopen(req, timeout=self.timeout_seconds) as resp:
            parsed = json.loads(resp.read().decode("utf-8"))
        if not parsed.get("ok"):
            raise SlackApiError(method=method, error=parsed.get("error", "unknown_error"))
        return parsed
