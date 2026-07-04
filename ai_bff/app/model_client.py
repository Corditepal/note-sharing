from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx


@dataclass(slots=True)
class OpenAICompatibleModelClient:
    base_url: str
    api_key: str = ""
    model_name: str = ""
    temperature: float = 0.3
    max_tokens: int = 1200
    timeout: float = 30.0
    extra_headers: dict[str, str] = field(default_factory=dict)

    @property
    def is_enabled(self) -> bool:
        return bool(self.base_url.strip() and self.api_key.strip() and self.model_name.strip())

    def _headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key.strip():
            headers["Authorization"] = f"Bearer {self.api_key.strip()}"
        headers.update(self.extra_headers)
        return headers

    async def chat(self, system_prompt: str, user_prompt: str, *, response_format: str | None = None) -> str:
        if not self.is_enabled:
            raise RuntimeError("model client is not configured")

        payload: dict[str, Any] = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "stream": False,
        }
        if response_format:
            payload["response_format"] = {"type": response_format}

        url = f"{self.base_url.rstrip('/')}/chat/completions"
        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            response = await client.post(url, json=payload, headers=self._headers())
            response.raise_for_status()
            data = response.json()

        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            raise RuntimeError("model response missing choices")

        first_choice = choices[0] if isinstance(choices, list) else {}
        message = first_choice.get("message") if isinstance(first_choice, dict) else {}
        content = message.get("content") if isinstance(message, dict) else None
        if not content:
            raise RuntimeError("model response missing content")
        return str(content).strip()
