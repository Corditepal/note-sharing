from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .login_api_client import LoginApiClient

_SITE_RETRIEVAL_INTENT_KEYWORDS = (
    "找",
    "搜索",
    "检索",
    "查",
    "相关",
    "笔记",
    "文章",
    "推荐",
    "看看",
    "有哪些",
    "站内",
    "search",
    "find",
    "note",
)

_QUESTION_INTENT_KEYWORDS = ("问答", "问题", "相似", "qa")

_BROAD_LIST_DISCOVERY_PROBES = (
    "Backend",
    "BFF",
    "Frontend",
    "For",
    "解析",
    "中间层",
)

_BROAD_LIST_SIGNALS = ("有哪些", "推荐", "观看", "看看", "列出", "有什么", "哪些", "可以")

_KEYWORD_STOP_WORDS = frozenset(
    {
        "帮我找",
        "帮我",
        "查找",
        "搜索",
        "检索",
        "相关",
        "笔记",
        "文章",
        "看看",
        "关于",
        "找到",
        "站内",
        "有哪些",
        "推荐",
    }
)

_INTENT_PREFIXES = (
    "帮我找",
    "帮我",
    "查找",
    "搜索",
    "检索",
    "关于",
    "找到",
    "看看",
    "找",
    "查",
)

_LATIN_TOKEN_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9+#.\-]*")
_CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff]+")


def _strip_intent_prefix(token: str) -> str:
    text = str(token or "").strip()
    for prefix in _INTENT_PREFIXES:
        if text.startswith(prefix) and len(text) > len(prefix):
            remainder = text[len(prefix) :].strip()
            if len(remainder) >= 2:
                return remainder
    return text


def _append_candidate(candidates: list[str], value: str) -> None:
    text = str(value or "").strip(" ,，。！？!?\n\t「」\"'")
    if len(text) < 2:
        return
    if text not in candidates:
        candidates.append(text)


def _extract_keywords(text: str) -> list[str]:
    message = str(text or "").strip()
    if not message:
        return []

    candidates: list[str] = []
    for match in re.finditer(r"[A-Za-z0-9][A-Za-z0-9+#.\-]*|[\u4e00-\u9fff]+", message):
        span = match.group(0)
        if _LATIN_TOKEN_RE.fullmatch(span):
            _append_candidate(candidates, span)
            continue
        _append_candidate(candidates, _strip_intent_prefix(span))
    return candidates[:8]


def _parse_note_id(value: Any) -> int | None:
    if value is None:
        return None
    try:
        note_id = int(value)
    except (TypeError, ValueError):
        return None
    if note_id <= 0:
        return None
    return note_id


def _parse_question_id(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def build_note_citation(note_id: Any, title: Any) -> dict[str, Any] | None:
    parsed_id = _parse_note_id(note_id)
    parsed_title = str(title or "").strip()
    if parsed_id is None or not parsed_title:
        return None
    return {
        "type": "note",
        "title": parsed_title,
        "noteId": parsed_id,
        "route": {"tab": "note-detail", "noteId": parsed_id},
    }


def build_question_citation(question_id: Any, title: Any) -> dict[str, Any] | None:
    parsed_id = _parse_question_id(question_id)
    parsed_title = str(title or "").strip()
    if parsed_id is None or not parsed_title:
        return None
    return {
        "type": "question",
        "title": parsed_title,
        "questionId": parsed_id,
        "route": {"tab": "qa-detail", "questionId": parsed_id},
    }


def _get_page(context: dict[str, Any]) -> dict[str, Any]:
    page = context.get("page")
    return page if isinstance(page, dict) else {}


def _get_resource(context: dict[str, Any]) -> dict[str, Any]:
    resource = context.get("resource")
    return resource if isinstance(resource, dict) else {}


def is_site_retrieval_intent(message: str) -> bool:
    text = str(message or "").strip().lower()
    if not text:
        return False
    return any(keyword in text for keyword in _SITE_RETRIEVAL_INTENT_KEYWORDS)


def is_broad_note_list_intent(message: str) -> bool:
    text = str(message or "")
    if not any(token in text for token in ("笔记", "文章")):
        return False
    if not any(signal in text for signal in _BROAD_LIST_SIGNALS):
        return False
    if _LATIN_TOKEN_RE.search(text):
        return False
    return True


def broad_list_search_keywords(message: str) -> list[str]:
    keywords: list[str] = []
    if "笔记" in message:
        _append_candidate(keywords, "笔记")
    if "文章" in message:
        _append_candidate(keywords, "文章")
    for match in _LATIN_TOKEN_RE.finditer(message):
        _append_candidate(keywords, match.group(0))
    for candidate in _extract_keywords(message):
        if candidate in _KEYWORD_STOP_WORDS:
            continue
        _append_candidate(keywords, candidate)
    return keywords or ["笔记"]


def should_include_questions(message: str, *, note_count: int) -> bool:
    text = str(message or "")
    if any(keyword in text for keyword in _QUESTION_INTENT_KEYWORDS):
        return True
    return note_count == 0 and is_site_retrieval_intent(message)


def derive_retrieval_keyword(message: str, context: dict[str, Any]) -> str | None:
    page = _get_page(context)
    resource = _get_resource(context)

    if is_broad_note_list_intent(message):
        return "笔记"

    for candidate in _extract_keywords(message):
        if candidate in _KEYWORD_STOP_WORDS:
            continue
        if len(candidate) >= 2:
            return candidate

    search_keyword = str(page.get("searchKeyword") or "").strip()
    if len(search_keyword) >= 2:
        return search_keyword

    resource_title = str(resource.get("title") or "").strip()
    if len(resource_title) >= 2:
        return resource_title

    return None


def should_run_site_retrieval(message: str, context: dict[str, Any]) -> bool:
    page = _get_page(context)
    if str(page.get("searchKeyword") or "").strip():
        return is_site_retrieval_intent(message) or bool(message.strip())
    return is_site_retrieval_intent(message)


async def build_validated_note_hit(
    client: LoginApiClient,
    item: dict[str, Any],
    token: str | None,
) -> dict[str, Any] | None:
    note_id = _parse_note_id(item.get("noteId") or item.get("id"))
    if note_id is None:
        return None

    preview = await client.validate_note_exists(note_id, token)
    if preview is None:
        return None

    title = str(preview.get("title") or item.get("title") or "").strip()
    snippet = str(item.get("contentSummary") or preview.get("contentSummary") or "").strip()
    citation = build_note_citation(note_id, title)
    if citation is None:
        return None

    return {
        "noteId": note_id,
        "title": title,
        "score": item.get("score"),
        "snippet": snippet[:240] if snippet else "",
        "citation": citation,
    }


def _question_hit_to_dict(item: dict[str, Any]) -> dict[str, Any]:
    question_id = item.get("questionId")
    title = str(item.get("title") or "").strip()
    citation = build_question_citation(question_id, title)
    return {
        "questionId": question_id,
        "title": title,
        "citation": citation,
    }


def format_retrieval_facts(result: SiteRetrievalResult) -> list[str]:
    facts: list[str] = []
    for item in result.notes:
        note_id = item.get("noteId")
        title = str(item.get("title") or "").strip()
        if note_id is not None and title:
            facts.append(f"站内检索命中笔记: noteId={note_id}, title={title}")
    for item in result.questions:
        question_id = item.get("questionId")
        title = str(item.get("title") or "").strip()
        if question_id and title:
            facts.append(f"站内检索命中问答: questionId={question_id}, title={title}")
    return facts


def citations_to_answer_links(citations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for citation in citations:
        if not isinstance(citation, dict):
            continue
        citation_type = citation.get("type")
        route = citation.get("route")
        if citation_type not in ("note", "question") or not isinstance(route, dict):
            continue
        link: dict[str, Any] = {
            "label": str(citation.get("title") or ""),
            "type": citation_type,
            "route": route,
        }
        if citation_type == "note":
            link["noteId"] = citation.get("noteId")
        else:
            link["questionId"] = citation.get("questionId")
        links.append(link)
    return links


@dataclass(slots=True)
class SiteRetrievalResult:
    keyword: str
    notes: list[dict[str, Any]] = field(default_factory=list)
    questions: list[dict[str, Any]] = field(default_factory=list)
    citations: list[dict[str, Any]] = field(default_factory=list)
    empty: bool = True


def _build_site_retrieval_result(
    *,
    keyword: str,
    notes: list[dict[str, Any]],
    questions: list[dict[str, Any]],
) -> SiteRetrievalResult:
    citations: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in notes + questions:
        citation = item.get("citation")
        if not isinstance(citation, dict):
            continue
        key = (str(citation.get("type")), str(citation.get("noteId") or citation.get("questionId")))
        if key in seen:
            continue
        seen.add(key)
        citations.append(citation)

    return SiteRetrievalResult(
        keyword=keyword,
        notes=notes,
        questions=questions,
        citations=citations,
        empty=not citations,
    )


async def _merge_validated_notes_from_hits(
    client: LoginApiClient,
    hits: list[dict[str, Any]],
    token: str | None,
    notes: list[dict[str, Any]],
    seen_ids: set[int],
    limit: int,
) -> None:
    for item in hits:
        if len(notes) >= limit:
            return
        note_id = _parse_note_id(item.get("noteId") or item.get("id"))
        if note_id is None or note_id in seen_ids:
            continue
        validated = await build_validated_note_hit(client, item, token)
        if validated is None:
            continue
        seen_ids.add(note_id)
        notes.append(validated)


async def _retrieve_broad_note_list(
    client: LoginApiClient,
    *,
    user_id: int | None,
    token: str | None,
    limit: int,
    message: str,
    include_questions: bool,
) -> SiteRetrievalResult:
    notes: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    title_probe_keywords: list[str] = []
    keywords = broad_list_search_keywords(message)
    keyword_label = keywords[0]

    catalog_hits = await client.fetch_published_catalog(token=token, limit=limit)
    await _merge_validated_notes_from_hits(client, catalog_hits, token, notes, seen_ids, limit)

    for search_keyword in keywords:
        if len(notes) >= limit:
            break
        keyword_label = search_keyword
        hits = await client.search_notes(search_keyword, user_id=user_id, token=token)
        for item in hits[:15]:
            title = str(item.get("title") or "")
            for match in _LATIN_TOKEN_RE.finditer(title):
                _append_candidate(title_probe_keywords, match.group(0))
        await _merge_validated_notes_from_hits(client, hits, token, notes, seen_ids, limit)

    for probe_keyword in title_probe_keywords:
        if len(notes) >= limit:
            break
        hits = await client.search_notes(probe_keyword, user_id=user_id, token=token)
        await _merge_validated_notes_from_hits(client, hits, token, notes, seen_ids, limit)

    if len(notes) < limit:
        hot_hits = await client.fetch_hot_notes(token=token)
        await _merge_validated_notes_from_hits(client, hot_hits, token, notes, seen_ids, limit)

    if len(notes) < limit and not catalog_hits:
        searched = set(keywords) | set(title_probe_keywords)
        for probe in _BROAD_LIST_DISCOVERY_PROBES:
            if len(notes) >= limit:
                break
            if probe in searched:
                continue
            searched.add(probe)
            hits = await client.search_notes(probe, user_id=user_id, token=token)
            await _merge_validated_notes_from_hits(client, hits, token, notes, seen_ids, limit)

    questions: list[dict[str, Any]] = []
    if include_questions or should_include_questions(message, note_count=len(notes)):
        question_hits = await client.search_questions(keyword_label, token=token)
        questions = [_question_hit_to_dict(item) for item in question_hits[:2]]
        questions = [item for item in questions if item.get("citation")]

    return _build_site_retrieval_result(keyword=keyword_label, notes=notes, questions=questions)


async def retrieve_site_content(
    client: LoginApiClient,
    *,
    keyword: str,
    user_id: int | None,
    token: str | None,
    limit: int = 3,
    include_questions: bool = False,
    message: str = "",
) -> SiteRetrievalResult:
    if is_broad_note_list_intent(message):
        return await _retrieve_broad_note_list(
            client,
            user_id=user_id,
            token=token,
            limit=limit,
            message=message,
            include_questions=include_questions,
        )

    text = str(keyword or "").strip()
    if len(text) < 2:
        return SiteRetrievalResult(keyword=text, empty=True)

    note_hits = await client.search_notes(text, user_id=user_id, token=token)
    notes: list[dict[str, Any]] = []
    for item in note_hits[:limit]:
        validated = await build_validated_note_hit(client, item, token)
        if validated:
            notes.append(validated)

    questions: list[dict[str, Any]] = []
    if include_questions or should_include_questions(message, note_count=len(notes)):
        question_hits = await client.search_questions(text, token=token)
        questions = [_question_hit_to_dict(item) for item in question_hits[:2]]
        questions = [item for item in questions if item.get("citation")]

    return _build_site_retrieval_result(keyword=text, notes=notes, questions=questions)
