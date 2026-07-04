from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from .settings import settings

_CONTEXT_PREVIEW_KEYS = ("contentPreview", "content", "selectedText")


class _ContextPreviewMixin(BaseModel):
    @model_validator(mode="after")
    def _truncate_context_preview(self) -> _ContextPreviewMixin:
        context = getattr(self, "context", None)
        if context is not None:
            object.__setattr__(self, "context", clamp_context_preview(context))
        return self


class CitationRoute(BaseModel):
    tab: Literal["note-detail", "qa-detail"]
    noteId: int | None = None
    questionId: str | None = None


class NoteCitation(BaseModel):
    type: Literal["note"] = "note"
    title: str
    noteId: int
    route: CitationRoute


class QuestionCitation(BaseModel):
    type: Literal["question"] = "question"
    title: str
    questionId: str
    route: CitationRoute


StructuredCitation = Annotated[Union[NoteCitation, QuestionCitation], Field(discriminator="type")]


class AnswerLink(BaseModel):
    label: str
    type: Literal["note", "question"]
    noteId: int | None = None
    questionId: str | None = None
    route: CitationRoute


class HostContext(BaseModel):
    version: str = Field(default="1.0")
    timestamp: str | None = None
    route: dict[str, Any] = Field(default_factory=dict)
    page: dict[str, Any] = Field(default_factory=dict)
    resource: dict[str, Any] = Field(default_factory=dict)
    session: dict[str, Any] = Field(default_factory=dict)
    user: dict[str, Any] = Field(default_factory=dict)
    permissions: dict[str, Any] = Field(default_factory=dict)


def clamp_context_preview(context: HostContext | dict[str, Any]) -> HostContext | dict[str, Any]:
    max_chars = settings.max_context_preview_chars
    if isinstance(context, HostContext):
        resource = dict(context.resource or {})
        changed = False
        for key in _CONTEXT_PREVIEW_KEYS:
            value = resource.get(key)
            if isinstance(value, str) and len(value) > max_chars:
                resource[key] = value[:max_chars]
                changed = True
        if not changed:
            return context
        return context.model_copy(update={"resource": resource})

    if not isinstance(context, dict):
        return context

    updated = dict(context)
    resource = updated.get("resource")
    if not isinstance(resource, dict):
        return updated

    resource = dict(resource)
    for key in _CONTEXT_PREVIEW_KEYS:
        value = resource.get(key)
        if isinstance(value, str) and len(value) > max_chars:
            resource[key] = value[:max_chars]
    updated["resource"] = resource
    return updated


def _raise_length_error(field_name: str, limit: int) -> None:
    raise ValueError(f"{field_name} exceeds maximum length ({limit})")


class ChatRequest(_ContextPreviewMixin):
    message: str
    context: HostContext | dict[str, Any] = Field(default_factory=dict)
    mode: Literal["local", "iframe"] = "local"

    @field_validator("message")
    @classmethod
    def validate_message_length(cls, value: str) -> str:
        if len(value) > settings.max_message_chars:
            _raise_length_error("message", settings.max_message_chars)
        return value


class ChatResponse(BaseModel):
    answer: str
    answerFormat: Literal["markdown", "plain"] = "markdown"
    answerLinks: list[AnswerLink] = Field(default_factory=list)
    citations: list[StructuredCitation] = Field(default_factory=list)
    route: dict[str, Any] | None = None
    ai_generated: bool = True
    source: str = "bff"


class KeywordRequest(_ContextPreviewMixin):
    text: str
    context: HostContext | dict[str, Any] = Field(default_factory=dict)

    @field_validator("text")
    @classmethod
    def validate_text_length(cls, value: str) -> str:
        if len(value) > settings.max_keyword_text_chars:
            _raise_length_error("text", settings.max_keyword_text_chars)
        return value


class KeywordResponse(BaseModel):
    keywords: list[str]
    explain: str
    ai_generated: bool = True


class SimilarQuestionRequest(_ContextPreviewMixin):
    question: str
    context: HostContext | dict[str, Any] = Field(default_factory=dict)
    limit: int = 3

    @field_validator("question")
    @classmethod
    def validate_question_length(cls, value: str) -> str:
        if len(value) > settings.max_keyword_text_chars:
            _raise_length_error("question", settings.max_keyword_text_chars)
        return value


class SimilarQuestionItem(BaseModel):
    questionId: str
    title: str
    route: CitationRoute


class SimilarQuestionResponse(BaseModel):
    items: list[SimilarQuestionItem]
    ai_generated: bool = True


class NoteSummaryRequest(_ContextPreviewMixin):
    note_id: int
    keyword: str | None = None
    context: HostContext | dict[str, Any] = Field(default_factory=dict)


class NoteSummaryResponse(BaseModel):
    note: dict[str, Any]
    summary: str
    related_notes: list[dict[str, Any]] = Field(default_factory=list)
    citations: list[StructuredCitation] = Field(default_factory=list)
    ai_generated: bool = True


class QuestionReferenceRequest(_ContextPreviewMixin):
    question_id: str
    context: HostContext | dict[str, Any] = Field(default_factory=dict)


class QuestionReferenceResponse(BaseModel):
    question: dict[str, Any]
    references: list[dict[str, Any]] = Field(default_factory=list)
    citations: list[StructuredCitation] = Field(default_factory=list)
    summary: str
    ai_generated: bool = True


class DraftQuestionRequest(_ContextPreviewMixin):
    input: str
    context: HostContext | dict[str, Any] = Field(default_factory=dict)

    @field_validator("input")
    @classmethod
    def validate_input_length(cls, value: str) -> str:
        if len(value) > settings.max_draft_input_chars:
            _raise_length_error("input", settings.max_draft_input_chars)
        return value


class DraftQuestionResponse(BaseModel):
    title: str
    content: str
    tags: list[str] = Field(default_factory=list)
    ai_generated: bool = True


class SiteSearchRequest(_ContextPreviewMixin):
    keyword: str
    limit: int = Field(default=3, ge=1, le=5)
    includeQuestions: bool = False
    context: HostContext | dict[str, Any] = Field(default_factory=dict)

    @field_validator("keyword")
    @classmethod
    def validate_keyword_length(cls, value: str) -> str:
        if len(value) > settings.max_site_search_keyword_chars:
            _raise_length_error("keyword", settings.max_site_search_keyword_chars)
        return value


class SiteSearchResponse(BaseModel):
    keyword: str
    notes: list[dict[str, Any]] = Field(default_factory=list)
    questions: list[dict[str, Any]] = Field(default_factory=list)
    citations: list[StructuredCitation] = Field(default_factory=list)
    empty: bool = False
    ai_generated: bool = True
