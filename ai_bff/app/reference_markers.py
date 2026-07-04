from __future__ import annotations

import re
from typing import Any

from .site_retrieval import citations_to_answer_links

_MARKER_RE = re.compile(
    r"\[\[folio:(note|question):([^|\]]+)\|([^\]]*)\]\]",
    re.IGNORECASE,
)


def _citation_index(citations: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for citation in citations:
        if not isinstance(citation, dict):
            continue
        citation_type = str(citation.get("type") or "").lower()
        if citation_type == "note":
            note_id = citation.get("noteId")
            if note_id is not None:
                index[("note", str(note_id))] = citation
        elif citation_type == "question":
            question_id = citation.get("questionId")
            if question_id:
                index[("question", str(question_id))] = citation
    return index


def inject_markers_from_citations(answer: str, citations: list[dict[str, Any]]) -> str:
    if "[[folio:" in answer or not citations:
        return answer

    lines: list[str] = []
    for index, citation in enumerate(citations, start=1):
        citation_type = str(citation.get("type") or "").lower()
        title = str(citation.get("title") or "").strip()
        if citation_type == "note":
            note_id = citation.get("noteId")
            if note_id is not None and title:
                lines.append(f"{index}. [[folio:note:{note_id}|{title}]]")
        elif citation_type == "question":
            question_id = citation.get("questionId")
            if question_id and title:
                lines.append(f"{index}. [[folio:question:{question_id}|{title}]]")

    if not lines:
        return answer

    return f"{answer.rstrip()}\n\n相关站内内容：\n" + "\n".join(lines)


def resolve_reference_markers(
    answer: str,
    citations: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]]]:
    index = _citation_index(citations)
    seen_keys: list[tuple[str, str]] = []
    seen_set: set[tuple[str, str]] = set()

    def _replace(match: re.Match[str]) -> str:
        citation_type = match.group(1).lower()
        citation_id = match.group(2).strip()
        marker_title = match.group(3).strip()
        key = (citation_type, citation_id)
        citation = index.get(key)
        if not citation:
            return marker_title or citation_id

        title = str(citation.get("title") or marker_title or "")
        if key not in seen_set:
            seen_set.add(key)
            seen_keys.append(key)

        if citation_type == "note":
            return f"[{title}](folio://note/{citation_id})"
        return f"[{title}](folio://question/{citation_id})"

    resolved = _MARKER_RE.sub(_replace, answer)
    ordered = [index[key] for key in seen_keys if key in index]
    return resolved, citations_to_answer_links(ordered)


if __name__ == "__main__":
    sample_citations = [
        {
            "type": "note",
            "title": "新建笔记",
            "noteId": 1,
            "route": {"tab": "note-detail", "noteId": 1},
        }
    ]
    resolved, links = resolve_reference_markers(
        "命中 [[folio:note:1|wrong title]]",
        sample_citations,
    )
    assert "[新建笔记](folio://note/1)" in resolved
    assert len(links) == 1
    assert links[0]["noteId"] == 1

    ghost, ghost_links = resolve_reference_markers(
        "ghost [[folio:note:999|幽灵]]",
        sample_citations,
    )
    assert ghost == "ghost 幽灵"
    assert ghost_links == []

    dup, dup_links = resolve_reference_markers(
        "[[folio:note:1|A]] and [[folio:note:1|B]]",
        sample_citations,
    )
    assert dup.count("folio://note/1") == 2
    assert len(dup_links) == 1

    print("reference_markers self-check passed")
