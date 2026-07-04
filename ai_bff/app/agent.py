from __future__ import annotations

import json
import re
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from .login_api_client import LoginApiClient
from .model_client import OpenAICompatibleModelClient
from .settings import settings
from .auth import extract_user_id_from_token
from .reference_markers import inject_markers_from_citations, resolve_reference_markers
from .site_retrieval import (
    derive_retrieval_keyword,
    format_retrieval_facts,
    retrieve_site_content,
    should_include_questions,
    should_run_site_retrieval,
)
from .copy import NO_RESULT, REFUSAL, WRITE_DENIED
from .schemas import (
    ChatRequest,
    CitationRoute,
    DraftQuestionRequest,
    DraftQuestionResponse,
    KeywordRequest,
    NoteSummaryRequest,
    NoteSummaryResponse,
    QuestionReferenceRequest,
    QuestionReferenceResponse,
    SimilarQuestionItem,
    SimilarQuestionRequest,
    SiteSearchRequest,
)


class AgentState(TypedDict, total=False):
    request: dict[str, Any]
    auth_token: str | None
    facts: list[str]
    answer: str
    route: dict[str, Any] | None
    citations: list[dict[str, Any]]
    site_retrieval_empty: bool


def _get_context(request: dict[str, Any]) -> dict[str, Any]:
    context = request.get("context")
    if isinstance(context, dict):
        return context
    return {}


def _get_resource(context: dict[str, Any]) -> dict[str, Any]:
    resource = context.get("resource")
    if isinstance(resource, dict):
        return resource
    return {}


def _preview_text(value: Any, limit: int = 180) -> str:
    text = str(value or "").replace("\n", " ").replace("\r", " ").strip()
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _resource_label(resource: dict[str, Any], fallback: str = "当前内容") -> str:
    return str(resource.get("title") or fallback)


def _page_mode(context: dict[str, Any]) -> str:
    page = context.get("page")
    if isinstance(page, dict):
        value = page.get("mode") or page.get("pageMode")
        if isinstance(value, str) and value.strip():
          return value.strip().lower()
    return ""


def _permission_flags(context: dict[str, Any]) -> dict[str, Any]:
    permissions = context.get("permissions")
    if isinstance(permissions, dict):
        return permissions
    return {}


def _is_write_intent(message: str) -> bool:
    text = message.lower()
    keywords = (
        "改写",
        "重写",
        "修改",
        "润色",
        "重构",
        "替换原文",
        "直接写",
        "帮我写",
        "rewrite",
        "revise",
    )
    return any(keyword in text for keyword in keywords)


def _is_write_allowed(context: dict[str, Any]) -> bool:
    permissions = _permission_flags(context)
    return _page_mode(context) == "edit" and bool(permissions.get("canAccessWriteActions"))


def _blocked_write_response(request: dict[str, Any]) -> str:
    return WRITE_DENIED


def _extract_keywords(text: str) -> list[str]:
    raw_parts = [
        part.strip(" ,，。！？!?\n\t")
        for part in text.replace("/", " ").replace("、", " ").split()
    ]
    candidates: list[str] = []
    for item in raw_parts:
        if len(item) < 2:
            continue
        if item not in candidates:
            candidates.append(item)
    return candidates[:8] or ["知识整理", "AI 协作", "笔记助手"]


FOLIO_ASSISTANT_IDENTITY = "笔记分享站站内助手"
OFF_TOPIC_TEXT = "我主要帮你整理笔记和查找站内内容，你可以试试问我某篇笔记讲了什么。"
DISCLAIMER_TEXT = "以下为一般性信息，不构成专业意见。"

CHAT_TASK_APPENDIX = (
    "当前任务：基于给定页面上下文与用户消息作答。"
    "若上下文包含正文片段，优先围绕正文片段反馈。"
    "回答须简洁、具体、可执行。"
    "列举「找到了哪些文章/笔记/问答」时，只能使用「上下文事实」中显式出现的 title、noteId、questionId。"
    "若上下文事实含「站内检索…无命中」，不得列举任何具体文章标题或 ID。"
    "不得输出外部网站 URL；站内资源引用使用 [[folio:note:…]] / [[folio:question:…]] marker，不写 http(s)://。"
)

REFERENCE_MARKERS_APPENDIX = (
    "引用站内检索命中时，在正文中使用 marker 标记，格式："
    "[[folio:note:{noteId}|{title}]] 或 [[folio:question:{questionId}|{title}]]。"
    "仅对「上下文事实」中出现的 noteId/questionId 使用 marker；不得自造 ID。"
    "无检索命中时使用标准空结果文案，不得使用 marker。"
    "不要直接写 markdown 链接或 folio:// URL；链接由系统解析 marker 生成。"
)

SUMMARY_TASK_APPENDIX = (
    "当前任务：为笔记生成摘要。"
    "输出须围绕给定标题与素材，清晰、具体、可执行，适合站内展示。"
)

QA_REFERENCE_TASK_APPENDIX = (
    "当前任务：为问答内容生成引用摘要。"
    "输出须围绕给定问题标题与素材，清晰、具体、可执行，适合站内展示。"
)

DRAFT_QUESTION_TASK_APPENDIX = (
    "当前任务：根据用户提供的关键词或粗糙描述，生成一条适合在问答区发布的提问草稿。"
    "输出必须是 JSON 对象，且仅包含字段：title（字符串）、content（字符串）、tags（字符串数组，1-5 个）。"
    "title 应清晰概括问题；content 应补充背景与具体困惑；tags 与主题相关。"
    "不要编造站内 noteId、questionId 或 URL。"
)


def _base_system_prompt() -> str:
    return "\n".join(
        [
            f"你是{FOLIO_ASSISTANT_IDENTITY}。",
            "",
            "你可以帮助：笔记管理、知识整理、站内内容检索、问答社区、当前页面解释，以及适度闲聊。",
            "不要过度拦截与笔记学习、知识整理相关的正常问题。",
            "",
            "必须拒绝：色情、暴力、仇恨、违法、人身攻击、钓鱼、恶意代码、绕过审核/限流/安全控制等请求。",
            f"遇上述请求，统一回复：{REFUSAL}",
            "",
            "对明显离题请求（如代写论文、替做外部网站操作、与站内无关的大量代码作业），礼貌引导回站内场景。",
            f"引导语可使用：{OFF_TOPIC_TEXT}",
            "",
            "涉及医疗、法律、金融等专业建议时，在回答中附上免责声明。",
            f"免责声明：{DISCLAIMER_TEXT}",
            "",
            "禁止编造 noteId、questionId、answerId、URL、引用、检索结果或数据库记录。",
            f"未检索到站内内容时，明确说明：{NO_RESULT}",
            "",
            "浏览态不得声称已直接改写或保存正文；改写建议须由用户在编辑页确认后自行提交。",
        ]
    )


def _build_content_messages(request: dict[str, Any], facts: list[str]) -> tuple[str, str]:
    context = _get_context(request)
    resource = _get_resource(context)
    message = str(request.get("message") or "")
    page = context.get("page", {})
    resource_title = _resource_label(resource)
    resource_preview = _preview_text(resource.get("contentPreview") or resource.get("content") or "", 420)
    facts_text = "\n".join(f"- {item}" for item in facts if item)
    system_prompt = f"{_base_system_prompt()}\n\n{CHAT_TASK_APPENDIX}\n\n{REFERENCE_MARKERS_APPENDIX}"
    user_prompt = (
        f"当前页面: {page.get('tab') or 'unknown'}\n"
        f"当前资源: {resource.get('kind') or 'unknown'} / {resource_title}\n"
        f"用户消息: {message}\n"
        f"正文片段: {resource_preview or '无'}\n"
        f"上下文事实:\n{facts_text or '- 无'}"
    )
    return system_prompt, user_prompt


def _build_summary_messages(title: str, summary_source: str, facts: list[str]) -> tuple[str, str]:
    system_prompt = f"{_base_system_prompt()}\n\n{SUMMARY_TASK_APPENDIX}"
    facts_text = "\n".join(f"- {item}" for item in facts if item)
    user_prompt = (
        f"笔记标题: {title}\n"
        f"摘要素材: {summary_source}\n"
        f"上下文事实:\n{facts_text or '- 无'}\n"
        "请输出一段适合站内展示的笔记摘要。"
    )
    return system_prompt, user_prompt


def _build_draft_question_messages(user_input: str) -> tuple[str, str]:
    system_prompt = f"{_base_system_prompt()}\n\n{DRAFT_QUESTION_TASK_APPENDIX}"
    user_prompt = (
        f"用户输入: {user_input}\n"
        "请生成问答草稿 JSON，包含 title、content、tags。"
    )
    return system_prompt, user_prompt


def _normalize_draft_tags(raw: Any) -> list[str]:
    if not isinstance(raw, list):
        return []
    tags: list[str] = []
    for item in raw:
        text = str(item or "").strip()
        if not text or text in tags:
            continue
        tags.append(text)
        if len(tags) >= 5:
            break
    return tags


def _fallback_draft_question(user_input: str) -> dict[str, Any]:
    text = user_input.strip()
    title = text[:50] + ("…" if len(text) > 50 else "")
    if not title.endswith("？") and not title.endswith("?"):
        title = f"{title}？"
    content = (
        f"我在学习/实践中遇到与「{text}」相关的问题，"
        f"想请教大家具体的思路、步骤或经验。\n\n"
        f"背景：{text}"
    )
    return {
        "title": title,
        "content": content,
        "tags": _extract_keywords(text)[:5],
    }


def _parse_draft_question_payload(raw: str, user_input: str) -> dict[str, Any]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return _fallback_draft_question(user_input)

    if not isinstance(data, dict):
        return _fallback_draft_question(user_input)

    title = str(data.get("title") or "").strip()
    content = str(data.get("content") or "").strip()
    tags = _normalize_draft_tags(data.get("tags"))

    if not title:
        title = _fallback_draft_question(user_input)["title"]
    if not content:
        content = _fallback_draft_question(user_input)["content"]
    if not tags:
        tags = _extract_keywords(user_input)[:5]

    return {"title": title, "content": content, "tags": tags}


def _build_question_reference_messages(title: str, summary_source: str, facts: list[str]) -> tuple[str, str]:
    system_prompt = f"{_base_system_prompt()}\n\n{QA_REFERENCE_TASK_APPENDIX}"
    facts_text = "\n".join(f"- {item}" for item in facts if item)
    user_prompt = (
        f"问题标题: {title}\n"
        f"问题素材: {summary_source}\n"
        f"上下文事实:\n{facts_text or '- 无'}\n"
        "请输出一段适合站内展示的问答引用摘要。"
    )
    return system_prompt, user_prompt


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


def _build_note_citation(note_id: Any, title: Any) -> dict[str, Any] | None:
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


def _build_question_citation(question_id: Any, title: Any) -> dict[str, Any] | None:
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


def _citation_key(citation: dict[str, Any]) -> tuple[str, str] | None:
    citation_type = str(citation.get("type") or "")
    resource_id = citation.get("noteId") or citation.get("questionId")
    if not citation_type or resource_id is None:
        return None
    return citation_type, str(resource_id)


async def _append_validated_note_citations(
    client: LoginApiClient,
    citations: list[dict[str, Any]],
    hits: list[dict[str, Any]],
    token: str | None,
    *,
    limit: int = 3,
) -> None:
    for item in hits[:limit]:
        if not isinstance(item, dict):
            continue
        note_id = _parse_note_id(item.get("noteId") or item.get("id"))
        if note_id is None:
            continue
        preview = await client.validate_note_exists(note_id, token)
        if preview is None:
            continue
        title = str(preview.get("title") or item.get("title") or "").strip()
        _append_citation(citations, _build_note_citation(note_id, title))


def _append_citation(citations: list[dict[str, Any]], citation: dict[str, Any] | None) -> None:
    if not citation:
        return
    key = _citation_key(citation)
    if key is None:
        return
    for existing in citations:
        if _citation_key(existing) == key:
            return
    citations.append(citation)


def _normalize_citations(raw: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue

        citation_type = str(item.get("type") or "").lower()
        built: dict[str, Any] | None = None
        if citation_type in {"note", "related-note"}:
            built = _build_note_citation(item.get("noteId") or item.get("id"), item.get("title"))
        elif citation_type == "question":
            built = _build_question_citation(item.get("questionId"), item.get("title"))
        elif item.get("noteId") is not None or item.get("id") is not None:
            built = _build_note_citation(item.get("noteId") or item.get("id"), item.get("title"))
        elif item.get("questionId") is not None:
            built = _build_question_citation(item.get("questionId"), item.get("title"))

        if not built:
            continue

        key = _citation_key(built)
        if key is None or key in seen:
            continue
        seen.add(key)
        normalized.append(built)
    return normalized


def _context_user_id(context: Any) -> int | None:
    if not isinstance(context, dict):
        return None
    user = context.get("user") or {}
    user_id = user.get("id")
    try:
        return int(user_id) if user_id is not None else None
    except (TypeError, ValueError):
        return None


def _resolve_user_id(context: dict[str, Any], auth_token: str | None) -> int | None:
    user_id = _context_user_id(context)
    if user_id is not None:
        return user_id
    if not auth_token:
        return None
    return extract_user_id_from_token(auth_token, settings.jwt_secret, settings.auth_disabled)


_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\((https?://[^)]+)\)", re.IGNORECASE)
_BARE_URL_RE = re.compile(r"https?://[^\s\])<>\"'）】]+", re.IGNORECASE)


def _should_force_no_result(state: AgentState) -> bool:
    return bool(state.get("site_retrieval_empty")) and not state.get("citations")


def _strip_external_urls(text: str) -> str:
    cleaned = _MD_LINK_RE.sub(r"\1", text)
    cleaned = _BARE_URL_RE.sub("", cleaned)
    return cleaned.strip()


def _apply_answer_reference_links(
    answer: str,
    citations: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    if not citations:
        return answer, []
    if "[[folio:" not in answer:
        answer = inject_markers_from_citations(answer, citations)
    return resolve_reference_markers(answer, citations)


async def _collect_facts(state: AgentState, client: LoginApiClient) -> AgentState:
    request = state.get("request", {})
    context = _get_context(request)
    resource = _get_resource(context)
    message = str(request.get("message") or "")
    facts: list[str] = []
    citations: list[dict[str, Any]] = list(state.get("citations", []))
    site_retrieval_empty = False
    auth_token = state.get("auth_token")
    user_id = _resolve_user_id(context, auth_token)

    resource_kind = str(resource.get("kind") or "").lower()
    resource_title = _resource_label(resource)
    resource_preview = _preview_text(resource.get("contentPreview") or resource.get("content") or "", 240)
    page_mode = _page_mode(context)

    if resource_preview:
        facts.append(f"当前内容片段: 《{resource_title}》 {resource_preview}")
        if resource.get("contentLength"):
            facts.append(f"内容长度约 {resource.get('contentLength')} 字符")
        if resource.get("reason") == "typing":
            facts.append("这是编辑中的实时内容片段，而不是静态页面信息")
        if resource_kind == "note-editor":
            facts.append("当前处于笔记编辑态，反馈应聚焦结构、表达和可修改点")
        elif resource_kind == "note-detail":
            facts.append("当前处于笔记查看态，反馈应聚焦摘要、观点和补充信息")
        elif resource_kind == "qa-detail":
            facts.append("当前处于问答查看态，反馈应聚焦问题清晰度和回答方向")
    if page_mode:
        facts.append(f"页面模式: {page_mode}")

    note_id = context.get("page", {}).get("viewingNoteId") or context.get("page", {}).get("noteId")
    if note_id:
        preview: dict[str, Any] | None = None
        try:
            preview = await client.fetch_note_preview(int(note_id), auth_token)
            facts.append(f"已连接笔记 {note_id}")
            if isinstance(preview, dict):
                facts.append(f"笔记预览已取回: {preview.get('title') or preview.get('message') or 'unknown'}")
        except Exception as exc:  # noqa: BLE001
            facts.append(f"笔记预览回退到本地模式: {exc.__class__.__name__}")

        if isinstance(preview, dict):
            _append_citation(
                citations,
                _build_note_citation(
                    preview.get("id") or note_id,
                    preview.get("title") or "未命名笔记",
                ),
            )

            if any(keyword in message for keyword in ("总结", "摘要", "标题", "续写", "知识整理")):
                keyword_hint = context.get("page", {}).get("searchKeyword") or preview.get("title") or str(note_id)
                summary_source = preview.get("contentSummary")
                try:
                    related_notes = await client.search_notes(
                        str(keyword_hint),
                        user_id=user_id,
                        token=auth_token,
                    )
                    if not summary_source and related_notes:
                        summary_source = related_notes[0].get("contentSummary")
                    await _append_validated_note_citations(
                        client,
                        citations,
                        related_notes,
                        auth_token,
                        limit=3,
                    )
                except Exception as exc:  # noqa: BLE001
                    facts.append(f"相关笔记检索失败，已跳过: {exc.__class__.__name__}")
                facts.append(
                    str(summary_source or f"当前笔记《{preview.get('title') or '未命名笔记'}》已接入只读代理，后续可继续接入正文摘要。")
                )

    keyword = context.get("page", {}).get("searchKeyword")
    if keyword:
        try:
            questions = await client.search_questions(str(keyword), auth_token)
            facts.append(f"检索到 {len(questions)} 条问答候选")
        except Exception as exc:  # noqa: BLE001
            facts.append(f"问答检索回退到本地模式: {exc.__class__.__name__}")

    question_id = context.get("page", {}).get("questionId") or context.get("route", {}).get("query", {}).get("questionId")
    if question_id and any(keyword in message for keyword in ("问答", "问题", "引用", "相似", "回答")):
        try:
            question = await client.fetch_question_detail(str(question_id), auth_token)
            similar_questions = await client.search_questions(str(question.get("title") or question_id), token=auth_token)
            answer_count = int(question.get("answerCount") or 0)
            content = str(question.get("content") or "")
            facts.append(
                f"问题《{question.get('title') or '未命名问题'}》已接入只读引用，回答数：{answer_count}。"
                + (f"内容摘要：{content[:80]}{'…' if len(content) > 80 else ''}" if content else "")
            )
            _append_citation(
                citations,
                _build_question_citation(
                    question.get("questionId") or question_id,
                    question.get("title"),
                ),
            )
            for item in similar_questions[:3]:
                _append_citation(
                    citations,
                    _build_question_citation(item.get("questionId"), item.get("title")),
                )
        except Exception as exc:  # noqa: BLE001
            facts.append(f"问题引用回退到本地模式: {exc.__class__.__name__}")

    if should_run_site_retrieval(message, context):
        retrieval_keyword = derive_retrieval_keyword(message, context)
        if retrieval_keyword:
            result = await retrieve_site_content(
                client,
                keyword=retrieval_keyword,
                user_id=user_id,
                token=auth_token,
                limit=3,
                include_questions=should_include_questions(message, note_count=0),
                message=message,
            )
            if result.notes:
                facts.append(f"站内笔记检索「{result.keyword}」命中 {len(result.notes)} 条")
            elif result.questions:
                facts.append(f"站内问答检索「{result.keyword}」命中 {len(result.questions)} 条")
            else:
                facts.append(f"站内检索「{result.keyword}」无命中")
                site_retrieval_empty = True
            facts.extend(format_retrieval_facts(result))
            for citation in result.citations:
                _append_citation(citations, citation)

    if not facts:
        facts.append("未命中远端上下文，使用本地稳定规则回复")

    return {
        **state,
        "facts": facts,
        "citations": _normalize_citations(citations),
        "site_retrieval_empty": site_retrieval_empty,
    }


def _draft_answer(state: AgentState) -> AgentState:
    request = state.get("request", {})
    message = str(request.get("message") or "")
    facts = state.get("facts", [])
    context = _get_context(request)
    page = context.get("page", {})
    resource = _get_resource(context)
    resource_kind = str(resource.get("kind") or "").lower()
    resource_title = _resource_label(resource)
    resource_preview = _preview_text(resource.get("contentPreview") or resource.get("content") or "", 260)

    if state.get("site_retrieval_empty") and not state.get("citations"):
        answer = NO_RESULT
    elif resource_preview and any(keyword in message for keyword in ("总结", "摘要", "summary", "反馈", "点评")):
        if resource_kind == "note-editor":
            answer = (
                f"基于正在编辑的笔记《{resource_title}》，我看到的内容片段是：{resource_preview}\n\n"
                f"建议：1. 先补一个能概括全文的标题 2. 把现有内容拆成更清晰的小节 3. 把结论或待办单独拎出来。"
            )
        elif resource_kind == "qa-detail":
            answer = (
                f"基于当前问答《{resource_title}》，问题内容片段是：{resource_preview}\n\n"
                f"建议：1. 明确提问目标 2. 补足必要背景 3. 回答时优先给可执行方案。"
            )
        else:
            answer = (
                f"基于当前内容《{resource_title}》，核心片段是：{resource_preview}\n\n"
                f"建议：1. 提炼核心观点 2. 补充关键论据 3. 增加一个下一步动作。"
            )
    elif any(keyword in message for keyword in ("标题", "起标题", "title")):
        answer = "标题候选：1. 结构化整理 2. 站内引用摘要 3. 可执行知识卡片"
    elif any(keyword in message for keyword in ("关键词", "keywords")):
        answer = f"关键词：{'、'.join(_extract_keywords(message))}"
    elif any(keyword in message for keyword in ("相似", "重复", "问题")):
        answer = "我会先做相似问题检索，当前返回的是稳定占位列表，后续可替换成站内搜索结果。"
    elif any(keyword in message for keyword in ("总结", "摘要", "summary")):
        detail_fact = next(
            (
                fact
                for fact in facts
                if fact
                and not fact.startswith("已连接")
                and not fact.startswith("笔记预览")
                and not fact.startswith("检索到")
                and not fact.startswith("当前内容片段")
                and not fact.startswith("未命中远端上下文")
            ),
            None,
        )
        answer = detail_fact or f"页面摘要：{page.get('tab') or 'unknown'} / {page.get('searchKeyword') or 'no-keyword'}。"
    else:
        answer = f"已收到请求：{message}\n\n上下文事实：\n- " + "\n- ".join(facts)

    citations = _normalize_citations(list(state.get("citations", [])))

    route = None
    if "搜索" in message or "search" in message.lower():
        route = {"path": "/main", "query": {"tab": "search", "keyword": page.get("searchKeyword") or ""}}
    elif "问答" in message or "qa" in message.lower():
        route = {"path": "/main", "query": {"tab": "circle"}}

    return {**state, "answer": answer, "citations": citations, "route": route}


class AgentRuntime:
    def __init__(self, login_api_client: LoginApiClient):
        self._login_api_client = login_api_client
        self._model_client = OpenAICompatibleModelClient(
            base_url=settings.model_base_url,
            api_key=settings.model_api_key,
            model_name=settings.model_name,
            temperature=settings.model_temperature,
            max_tokens=settings.model_max_tokens,
            timeout=settings.model_timeout,
        )
        self._graph = self._build_graph()

    def _build_graph(self):
        builder = StateGraph(AgentState)
        
        async def collect_facts(state: AgentState) -> AgentState:
            return await _collect_facts(state, self._login_api_client)

        builder.add_node("collect_facts", collect_facts)
        builder.add_node("draft_answer", _draft_answer)
        builder.set_entry_point("collect_facts")
        builder.add_edge("collect_facts", "draft_answer")
        builder.add_edge("draft_answer", END)
        return builder.compile()

    async def run(self, request: ChatRequest, auth_token: str | None = None) -> dict[str, Any]:
        request_data = request.model_dump()
        context = _get_context(request_data)
        message = str(request_data.get("message") or "")

        if _is_write_intent(message) and not _is_write_allowed(context):
            return {
                "answer": _blocked_write_response(request_data),
                "answerFormat": "markdown",
                "answerLinks": [],
                "citations": [],
                "route": None,
                "ai_generated": True,
                "source": "policy",
                "blocked": True,
                "reason": "write_not_allowed",
            }

        state = await self._graph.ainvoke({"request": request_data, "auth_token": auth_token})
        citations = _normalize_citations(state.get("citations", []))

        if _should_force_no_result(state):
            answer = NO_RESULT
            answer_links: list[dict[str, Any]] = []
            source = "grounding"
        elif self._model_client.is_enabled:
            answer = state.get("answer", "")
            source = "langgraph"
            try:
                system_prompt, user_prompt = _build_content_messages(request_data, state.get("facts", []))
                answer = await self._model_client.chat(system_prompt, user_prompt)
                source = "openai-compatible"
            except Exception:  # noqa: BLE001
                pass
            answer = _strip_external_urls(answer)
            answer, answer_links = _apply_answer_reference_links(answer, citations)
        else:
            answer = _strip_external_urls(state.get("answer", ""))
            source = "langgraph"
            answer, answer_links = _apply_answer_reference_links(answer, citations)

        return {
            "answer": answer,
            "answerFormat": "markdown",
            "answerLinks": answer_links,
            "citations": citations,
            "route": state.get("route"),
            "ai_generated": True,
            "source": source,
            "blocked": False,
        }

    async def keywords(self, request: KeywordRequest, auth_token: str | None = None) -> dict[str, Any]:
        keywords = _extract_keywords(request.text)
        return {
            "keywords": keywords,
            "explain": f"根据当前文本和上下文提取 {len(keywords)} 个候选关键词",
            "ai_generated": True,
        }

    async def summarize_note(self, request: NoteSummaryRequest, auth_token: str | None = None) -> dict[str, Any]:
        context = _get_context(request.model_dump())
        resource = _get_resource(context)
        note = await self._login_api_client.fetch_note_preview(request.note_id, auth_token)
        title = str(note.get("title") or "未命名笔记")
        keyword = request.keyword or title
        related_notes = await self._login_api_client.search_notes(
            keyword,
            user_id=_resolve_user_id(context, auth_token),
            token=auth_token,
        )
        summary_source = self._pick_summary_source(note, related_notes)
        resource_preview = _preview_text(resource.get("contentPreview") or resource.get("content") or "", 220)
        if resource_preview:
            summary_source = f"基于宿主提供的正文片段：{resource_preview}"
        summary = f"笔记《{title}》：{summary_source}" if summary_source else f"当前笔记《{title}》已接入只读代理，后续可继续接入正文摘要。"

        if self._model_client.is_enabled:
            try:
                system_prompt, user_prompt = _build_summary_messages(title, summary_source or "无", [
                    f"笔记标题：{title}",
                    f"相关笔记数量：{len(related_notes)}",
                    f"正文片段：{resource_preview or '无'}",
                ])
                summary = await self._model_client.chat(system_prompt, user_prompt)
            except Exception:  # noqa: BLE001
                pass

        citations: list[dict[str, Any]] = []
        _append_citation(citations, _build_note_citation(note.get("id") or request.note_id, title))
        await _append_validated_note_citations(
            self._login_api_client,
            citations,
            related_notes,
            auth_token,
            limit=3,
        )

        return NoteSummaryResponse(
            note=note,
            summary=summary,
            related_notes=related_notes[:3],
            citations=_normalize_citations(citations),
            ai_generated=True,
        ).model_dump()

    async def reference_question(self, request: QuestionReferenceRequest, auth_token: str | None = None) -> dict[str, Any]:
        context = _get_context(request.model_dump())
        resource = _get_resource(context)
        question = await self._login_api_client.fetch_question_detail(request.question_id, auth_token)
        question_title = str(question.get("title") or "未命名问题")
        similar_questions = await self._login_api_client.search_questions(question_title, token=auth_token)
        answer_count = int(question.get("answerCount") or 0)
        summary = self._build_question_summary(question, answer_count)
        resource_preview = _preview_text(resource.get("contentPreview") or resource.get("content") or "", 220)
        if resource_preview:
            summary = f"基于宿主提供的问答片段：{resource_preview}"

        if self._model_client.is_enabled:
            try:
                system_prompt, user_prompt = _build_question_reference_messages(
                    question_title,
                    summary or "无",
                    [
                        f"回答数量：{answer_count}",
                        f"相似问题数量：{len(similar_questions)}",
                        f"问答片段：{resource_preview or '无'}",
                    ],
                )
                summary = await self._model_client.chat(system_prompt, user_prompt)
            except Exception:  # noqa: BLE001
                pass

        references = []
        for answer in (question.get("answers") or [])[:2]:
            references.append(
                {
                    "type": "answer",
                    "answerId": answer.get("answerId"),
                    "authorName": answer.get("authorName"),
                    "content": answer.get("content"),
                }
            )
        for item in similar_questions[:3]:
            references.append(
                {
                    "type": "question",
                    "questionId": item.get("questionId"),
                    "title": item.get("title"),
                    "likeCount": item.get("likeCount"),
                    "answerCount": item.get("answerCount"),
                }
            )

        citations: list[dict[str, Any]] = []
        _append_citation(
            citations,
            _build_question_citation(question.get("questionId") or request.question_id, question_title),
        )

        return QuestionReferenceResponse(
            question=question,
            references=references,
            citations=_normalize_citations(citations),
            summary=summary,
            ai_generated=True,
        ).model_dump()

    async def similar_questions(self, request: SimilarQuestionRequest, auth_token: str | None = None) -> dict[str, Any]:
        similar = await self._login_api_client.search_questions(request.question, token=auth_token)
        limit = request.limit or 3
        items = []
        for item in similar:
            if len(items) >= limit:
                break

            if not isinstance(item, dict):
                continue

            question_id = item.get("questionId")
            title = item.get("title")
            if not question_id or not title:
                continue

            question_id = str(question_id)
            items.append(
                SimilarQuestionItem(
                    questionId=question_id,
                    title=str(title),
                    route=CitationRoute(tab="qa-detail", questionId=question_id),
                ).model_dump()
            )

        return {"items": items, "ai_generated": True}

    async def draft_question(self, request: DraftQuestionRequest, auth_token: str | None = None) -> dict[str, Any]:
        user_input = str(request.input or "").strip()
        if not user_input:
            raise ValueError("input is required")

        draft = _fallback_draft_question(user_input)

        if self._model_client.is_enabled:
            try:
                system_prompt, user_prompt = _build_draft_question_messages(user_input)
                raw = await self._model_client.chat(
                    system_prompt,
                    user_prompt,
                    response_format="json_object",
                )
                draft = _parse_draft_question_payload(raw, user_input)
            except Exception:  # noqa: BLE001
                draft = _fallback_draft_question(user_input)

        return DraftQuestionResponse(
            title=str(draft["title"]),
            content=str(draft["content"]),
            tags=_normalize_draft_tags(draft.get("tags")),
            ai_generated=True,
        ).model_dump()

    async def site_search(self, request: SiteSearchRequest, auth_token: str | None = None) -> dict[str, Any]:
        keyword = str(request.keyword or "").strip()
        context = _get_context(request.model_dump())
        user_id = _resolve_user_id(context, auth_token)
        result = await retrieve_site_content(
            self._login_api_client,
            keyword=keyword,
            user_id=user_id,
            token=auth_token,
            limit=request.limit,
            include_questions=request.includeQuestions,
            message=keyword,
        )
        return {
            "keyword": result.keyword,
            "notes": result.notes,
            "questions": result.questions,
            "citations": _normalize_citations(result.citations),
            "empty": result.empty,
            "ai_generated": True,
        }

    async def note_reference(self, note_id: int, auth_token: str | None = None) -> dict[str, Any]:
        note = await self._login_api_client.fetch_note_preview(note_id, auth_token)
        user_id = extract_user_id_from_token(auth_token, settings.jwt_secret, settings.auth_disabled) if auth_token else None
        related_notes = await self._login_api_client.search_notes(
            str(note.get("title") or note_id),
            user_id=user_id,
            token=auth_token,
        )
        citation = _build_note_citation(note.get("id") or note_id, note.get("title"))
        return {
            "note": note,
            "related_notes": related_notes[:5],
            "citations": [citation] if citation else [],
            "ai_generated": True,
        }

    def _pick_summary_source(self, note: dict[str, Any], related_notes: list[dict[str, Any]]) -> str | None:
        if isinstance(note.get("contentSummary"), str) and note["contentSummary"].strip():
            return note["contentSummary"].strip()
        for item in related_notes:
            content_summary = item.get("contentSummary")
            if isinstance(content_summary, str) and content_summary.strip():
                return content_summary.strip()
        return None

    def _build_question_summary(self, question: dict[str, Any], answer_count: int) -> str:
        title = str(question.get("title") or "未命名问题")
        content = str(question.get("content") or "")
        if content.strip():
            return f"问题《{title}》：内容摘要 {content[:80]}{'…' if len(content) > 80 else ''}。回答数：{answer_count}。"
        return f"问题《{title}》已接入只读引用，当前有 {answer_count} 个回答。"
