from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]

        os.environ[key] = value


_load_dotenv_file(Path(__file__).resolve().parents[1] / ".env")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


@dataclass(slots=True)
class Settings:
    login_api_base_url: str = os.getenv("AI_LOGIN_API_BASE_URL", "http://localhost:8080")
    jwt_secret: str = os.getenv("AI_JWT_SECRET", "")
    auth_disabled: bool = _env_bool("AI_AUTH_DISABLED", False)
    model_provider: str = os.getenv("AI_MODEL_PROVIDER", "openai_compatible")
    model_base_url: str = os.getenv("AI_MODEL_BASE_URL", "https://ark.cn-beijing.volces.com/api/coding/v3")
    model_api_key: str = os.getenv("AI_MODEL_API_KEY", "")
    model_name: str = os.getenv("AI_MODEL_NAME", "ark-code-latest")
    model_temperature: float = float(os.getenv("AI_MODEL_TEMPERATURE", "0.3"))
    model_max_tokens: int = int(os.getenv("AI_MODEL_MAX_TOKENS", "1200"))
    model_timeout: float = float(os.getenv("AI_MODEL_TIMEOUT", "30"))
    host: str = os.getenv("AI_BFF_HOST", "0.0.0.0")
    port: int = int(os.getenv("AI_BFF_PORT", "8000"))
    app_name: str = os.getenv("AI_APP_NAME", "folio-ai-bff")
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            origin.strip()
            for origin in os.getenv(
                "AI_ALLOWED_ORIGINS",
                "http://localhost:8080,http://127.0.0.1:8080,http://localhost:8082,http://127.0.0.1:8082",
            ).split(",")
            if origin.strip()
        ]
    )
    max_message_chars: int = field(default_factory=lambda: _env_int("AI_MAX_MESSAGE_CHARS", 4000))
    max_draft_input_chars: int = field(default_factory=lambda: _env_int("AI_MAX_DRAFT_INPUT_CHARS", 4000))
    max_keyword_text_chars: int = field(default_factory=lambda: _env_int("AI_MAX_KEYWORD_TEXT_CHARS", 4000))
    max_site_search_keyword_chars: int = field(
        default_factory=lambda: _env_int("AI_MAX_SITE_SEARCH_KEYWORD_CHARS", 200)
    )
    max_context_preview_chars: int = field(default_factory=lambda: _env_int("AI_MAX_CONTEXT_PREVIEW_CHARS", 2000))


settings = Settings()
