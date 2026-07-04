from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from dataclasses import dataclass


class AuthError(RuntimeError):
    pass


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _signing_key(secret: str) -> bytes:
    try:
        key = base64.b64decode(secret, validate=True)
    except binascii.Error as exc:
        raise AuthError("invalid jwt secret configuration") from exc

    if not key:
        raise AuthError("invalid jwt secret configuration")

    return key


def decode_jwt(token: str, secret: str) -> dict:
    if not secret.strip():
        raise AuthError("jwt secret not configured")

    try:
        header_b64, payload_b64, signature_b64 = token.split(".")
    except ValueError as exc:
        raise AuthError("invalid token format") from exc

    try:
        header = json.loads(_b64url_decode(header_b64))
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception as exc:  # noqa: BLE001
        raise AuthError("token payload decode failed") from exc

    if header.get("alg") != "HS256":
        raise AuthError("unsupported token algorithm")

    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected_signature = hmac.new(_signing_key(secret), signing_input, hashlib.sha256).digest()
    actual_signature = _b64url_decode(signature_b64)

    if not hmac.compare_digest(expected_signature, actual_signature):
        raise AuthError("token signature mismatch")

    exp = payload.get("exp")
    if exp is not None and int(exp) < time.time():
        raise AuthError("token expired")

    return payload


@dataclass(slots=True)
class AuthUser:
    user_id: str | None
    username: str | None
    role: str | None


def extract_user_from_authorization(
    authorization: str | None,
    *,
    secret: str,
    auth_disabled: bool = False,
) -> AuthUser:
    if auth_disabled:
        return AuthUser(user_id="0", username="auth-disabled", role="User")

    if not secret.strip():
        raise AuthError("jwt secret not configured")

    if not authorization or not authorization.startswith("Bearer "):
        raise AuthError("missing bearer token")

    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise AuthError("missing token")

    payload = decode_jwt(token, secret)
    return AuthUser(
        user_id=str(payload.get("userId") or payload.get("id") or ""),
        username=payload.get("sub") or payload.get("username"),
        role=payload.get("role"),
    )


def extract_user_id_from_token(token: str, secret: str, auth_disabled: bool = False) -> int | None:
    if auth_disabled:
        return 0
    try:
        payload = decode_jwt(token, secret)
        raw = payload.get("userId") if payload.get("userId") is not None else payload.get("id")
        if raw is None:
            return None
        return int(raw)
    except (AuthError, TypeError, ValueError):
        return None
