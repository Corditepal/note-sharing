from __future__ import annotations

from dataclasses import dataclass

import httpx


def _unwrap_standard_response(data):
    if isinstance(data, dict) and "data" in data:
        return data.get("data")
    return data


@dataclass(slots=True)
class LoginApiClient:
    base_url: str
    timeout: float = 6.0

    async def fetch_note_preview(self, note_id: int, token: str | None = None) -> dict:
        url = f"{self.base_url.rstrip('/')}/api/v1/noting/notes/files/id_url"
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            response = await client.post(url, params={"noteId": note_id}, headers=headers)
            response.raise_for_status()
            return _unwrap_standard_response(response.json()) or {}

    async def validate_note_exists(self, note_id: int, token: str | None = None) -> dict | None:
        try:
            preview = await self.fetch_note_preview(note_id, token)
        except httpx.HTTPError:
            return None
        return preview if isinstance(preview, dict) and preview else None

    async def search_notes(self, keyword: str, user_id: int | None = None, token: str | None = None) -> list[dict]:
        text = str(keyword or "").strip()
        if len(text) < 2 or user_id is None:
            return []

        url = f"{self.base_url.rstrip('/')}/api/v1/search/notes"
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        payload = {"keyword": text, "userId": user_id}

        try:
            async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = _unwrap_standard_response(response.json())
                if not isinstance(data, list):
                    return []
                return [item for item in data if isinstance(item, dict)]
        except httpx.HTTPError:
            return []

    async def fetch_published_catalog(self, token: str | None = None, limit: int = 5) -> list[dict]:
        capped = max(1, min(int(limit), 10))
        url = f"{self.base_url.rstrip('/')}/api/v1/search/notes/catalog"
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
                response = await client.get(url, params={"limit": capped}, headers=headers)
                if response.status_code == 404:
                    return []
                response.raise_for_status()
                data = _unwrap_standard_response(response.json())
                if not isinstance(data, list):
                    return []
                return [item for item in data if isinstance(item, dict)]
        except httpx.HTTPError:
            return []

    async def fetch_hot_notes(self, token: str | None = None) -> list[dict]:
        url = f"{self.base_url.rstrip('/')}/api/v1/hot/notes"
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
                response = await client.get(url, headers=headers)
                response.raise_for_status()
                data = _unwrap_standard_response(response.json())
                if not isinstance(data, list):
                    return []
                return [item for item in data if isinstance(item, dict)]
        except httpx.HTTPError:
            return []

    async def search_questions(self, keyword: str, token: str | None = None) -> list[dict]:
        url = f"{self.base_url.rstrip('/')}/api/v1/search/questions"
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
                response = await client.get(url, params={"keyword": keyword}, headers=headers)
                response.raise_for_status()
                data = _unwrap_standard_response(response.json())
                if not isinstance(data, list):
                    return []
                return [item for item in data if isinstance(item, dict)]
        except httpx.HTTPError:
            return []

    async def fetch_question_detail(self, question_id: str, token: str | None = None) -> dict:
        url = f"{self.base_url.rstrip('/')}/api/v1/qa/question/{question_id}"
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()
            return _unwrap_standard_response(response.json()) or {}

    async def fetch_note_comments(self, note_id: int, login_user_id: int, token: str | None = None) -> list[dict]:
        url = f"{self.base_url.rstrip('/')}/api/v1/remark/note/list"
        headers = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient(timeout=self.timeout, trust_env=False) as client:
            response = await client.get(url, params={"noteId": note_id, "loginUserId": login_user_id}, headers=headers)
            response.raise_for_status()
            data = _unwrap_standard_response(response.json())
            return data if isinstance(data, list) else []
