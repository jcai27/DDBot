from __future__ import annotations

import json
from urllib import request


class OpenAIAPIError(RuntimeError):
    pass


class OpenAIClient:
    """Small wrapper for OpenAI chat completions used by extractor and digest."""
    def __init__(self, api_key: str, model: str, timeout_seconds: int = 45) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout_seconds = timeout_seconds

    def json_completion(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> dict:
        """Request a JSON object response and parse it into a Python dict."""
        payload = {
            "model": self.model,
            "temperature": temperature,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        parsed = self._post_json("https://api.openai.com/v1/chat/completions", payload)
        try:
            content = parsed["choices"][0]["message"]["content"]
            return json.loads(content)
        except Exception as exc:  # noqa: BLE001
            raise OpenAIAPIError("OpenAI returned non-JSON content") from exc

    def text_completion(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> str:
        """Request plain text completion and return stripped content."""
        payload = {
            "model": self.model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        parsed = self._post_json("https://api.openai.com/v1/chat/completions", payload)
        try:
            return str(parsed["choices"][0]["message"]["content"]).strip()
        except Exception as exc:  # noqa: BLE001
            raise OpenAIAPIError("OpenAI returned invalid completion payload") from exc

    def _post_json(self, url: str, payload: dict) -> dict:
        """POST JSON payload and raise OpenAIAPIError on API-level failures."""
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(url=url, data=body, method="POST")
        req.add_header("Authorization", f"Bearer {self.api_key}")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        with request.urlopen(req, timeout=self.timeout_seconds) as resp:
            parsed = json.loads(resp.read().decode("utf-8"))

        if "error" in parsed:
            err = parsed["error"]
            msg = err.get("message", "OpenAI API error")
            code = err.get("code")
            raise OpenAIAPIError(f"{msg} (code={code})")

        return parsed
