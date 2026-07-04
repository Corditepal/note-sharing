from __future__ import annotations

import asyncio
import json
from typing import Any, TypeVar

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, ValidationError

from .agent import AgentRuntime
from .auth import AuthError, extract_user_from_authorization
from .login_api_client import LoginApiClient
from .schemas import (
    ChatRequest,
    DraftQuestionRequest,
    KeywordRequest,
    NoteSummaryRequest,
    QuestionReferenceRequest,
    SimilarQuestionRequest,
    SiteSearchRequest,
)
from .settings import settings

app = FastAPI(title=settings.app_name)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

agent_runtime = AgentRuntime(LoginApiClient(settings.login_api_base_url))

ModelT = TypeVar("ModelT", bound=BaseModel)


async def _parse_model(model_cls: type[ModelT], request: Request) -> ModelT:
    try:
        return model_cls.model_validate(await request.json())
    except ValidationError as exc:
        detail = _validation_error_detail(exc)
        raise HTTPException(status_code=422, detail=detail) from exc


def _validation_error_detail(exc: ValidationError) -> str:
    errors = exc.errors()
    if not errors:
        return "validation error"
    message = str(errors[0].get("msg") or "validation error")
    if message.startswith("Value error, "):
        message = message.removeprefix("Value error, ")
    return message


def _auth_token(authorization: str | None) -> str | None:
    try:
        user = extract_user_from_authorization(
            authorization,
            secret=settings.jwt_secret,
            auth_disabled=settings.auth_disabled,
        )
        if not settings.auth_disabled and not user.user_id and not user.username:
            raise AuthError("empty user payload")
        if not authorization or not authorization.startswith("Bearer "):
            return None
        return authorization.removeprefix("Bearer ").strip()
    except AuthError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


def _stream_sse(data: dict[str, Any]):
    async def generator():
        answer = data.get("answer", "")
        if not answer:
            yield "data: {}\n\n"
            return

        chunks = answer.split()
        for index, chunk in enumerate(chunks):
            payload = {"delta": chunk + (" " if index < len(chunks) - 1 else "")}
            yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
            await asyncio.sleep(0.02)

        yield f"data: {json.dumps({'done': True, 'answer': answer, 'answerFormat': data.get('answerFormat', 'markdown'), 'citations': data.get('citations', []), 'answerLinks': data.get('answerLinks', []), 'route': data.get('route'), 'blocked': data.get('blocked', False), 'reason': data.get('reason')}, ensure_ascii=False)}\n\n"

    return generator()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": settings.app_name,
        "login_api_base_url": settings.login_api_base_url,
    }


@app.get("/shell", response_class=HTMLResponse)
async def shell():
    return HTMLResponse(
        """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>FOLIO AI Shell</title>
  <style>
    :root { color-scheme: dark; }
    body {
      margin: 0;
      font-family: Inter, "PingFang SC", "Microsoft YaHei", system-ui, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(34, 197, 94, 0.24), transparent 40%),
        linear-gradient(180deg, #0f172a, #020617);
      color: #e2e8f0;
      height: 100vh;
      display: flex;
      flex-direction: column;
    }
    header {
      padding: 16px 18px;
      border-bottom: 1px solid rgba(148, 163, 184, 0.16);
    }
    header h1 { margin: 0; font-size: 18px; }
    header p { margin: 6px 0 0; color: #94a3b8; font-size: 12px; }
    #context {
      padding: 12px 18px;
      font-size: 12px;
      color: #cbd5e1;
      border-bottom: 1px solid rgba(148, 163, 184, 0.12);
      min-height: 64px;
      white-space: pre-wrap;
    }
    #log {
      flex: 1;
      overflow: auto;
      padding: 14px 18px;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .msg {
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(15, 23, 42, 0.72);
      border: 1px solid rgba(148, 163, 184, 0.12);
      white-space: pre-wrap;
    }
    .msg.user { background: rgba(34, 197, 94, 0.14); }
    footer {
      padding: 14px 18px 18px;
      border-top: 1px solid rgba(148, 163, 184, 0.12);
      display: flex;
      gap: 10px;
    }
    textarea {
      flex: 1;
      min-height: 58px;
      resize: none;
      border-radius: 14px;
      border: 1px solid rgba(148, 163, 184, 0.18);
      background: rgba(2, 6, 23, 0.72);
      color: #e2e8f0;
      padding: 10px 12px;
    }
    button {
      border: none;
      border-radius: 14px;
      padding: 0 16px;
      background: linear-gradient(135deg, #22c55e, #16a34a);
      color: white;
      font-weight: 600;
      cursor: pointer;
    }
    .chips {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      padding: 0 18px 12px;
    }
    .chip {
      border: 1px solid rgba(148, 163, 184, 0.16);
      background: rgba(148, 163, 184, 0.08);
      color: #e2e8f0;
      border-radius: 999px;
      padding: 7px 12px;
      cursor: pointer;
      font-size: 12px;
    }
  </style>
</head>
<body>
  <header>
    <h1>FOLIO AI Shell</h1>
    <p>iframe 宿主页面 · 通过 postMessage 与上层宿主通信</p>
  </header>
  <div id="context">等待宿主上下文...</div>
  <div class="chips">
    <button class="chip" data-action="title">生成标题</button>
    <button class="chip" data-action="summary">总结当前页</button>
    <button class="chip" data-action="keywords">提炼关键词</button>
    <button class="chip" data-action="route">请求跳转</button>
  </div>
  <div id="log"></div>
  <footer>
    <textarea id="input" placeholder="输入一条消息"></textarea>
    <button id="send">发送</button>
  </footer>
    <script>
    const version = "1.0";
    const log = document.getElementById("log");
    const input = document.getElementById("input");
    const contextBox = document.getElementById("context");
    let hostContext = null;
    let authToken = "";
    let assistantNode = null;
    let assistantNodeId = 0;

    function append(role, text) {
      const el = document.createElement("div");
      el.className = "msg " + role;
      el.textContent = text;
      log.appendChild(el);
      log.scrollTop = log.scrollHeight;
      return el;
    }

    function updateNode(node, text) {
      if (!node) return;
      node.textContent = text;
      log.scrollTop = log.scrollHeight;
    }

    function sendToHost(type, payload) {
      window.parent.postMessage({ version, type, payload }, "*");
    }

    function buildHeaders() {
      const headers = { "Content-Type": "application/json" };
      if (authToken) {
        headers.Authorization = "Bearer " + authToken;
      }
      return headers;
    }

    function getChatContext() {
      return hostContext || { version };
    }

    async function streamChat(message) {
      if (!authToken) {
        updateNode(assistantNode, "未收到登录令牌，无法调用 BFF。");
        return;
      }

      const response = await fetch("/api/v1/agent/chat/stream", {
        method: "POST",
        headers: buildHeaders(),
        body: JSON.stringify({
          message,
          context: getChatContext(),
          mode: "iframe"
        })
      });

      if (!response.ok) {
        throw new Error("BFF " + response.status);
      }

      if (!response.body) {
        const data = await response.json();
        updateNode(assistantNode, data.answer || "未返回内容");
        if (data.route) {
          sendToHost("ai.route", data.route);
        }
        return;
      }

      const reader = response.body.getReader();
      const decoder = new TextDecoder("utf-8");
      let buffer = "";
      let finalAnswer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const blocks = buffer.split("\n\n");
        buffer = blocks.pop() || "";

        for (const block of blocks) {
          const line = block.split("\n").find(item => item.startsWith("data:"));
          if (!line) continue;
          const raw = line.slice(5).trim();
          if (!raw) continue;

          let evt = null;
          try {
            evt = JSON.parse(raw);
          } catch (error) {
            continue;
          }

          if (evt.delta) {
            finalAnswer += evt.delta;
            updateNode(assistantNode, finalAnswer);
          }

          if (evt.done) {
            finalAnswer = evt.answer || finalAnswer;
            updateNode(assistantNode, finalAnswer);
            if (evt.route) {
              sendToHost("ai.route", evt.route);
            }
          }
        }
      }

      if (buffer.trim()) {
        const line = buffer.split("\n").find(item => item.startsWith("data:"));
        if (line) {
          const raw = line.slice(5).trim();
          if (raw) {
            try {
              const evt = JSON.parse(raw);
              if (evt.done && evt.answer) {
                updateNode(assistantNode, evt.answer);
                if (evt.route) {
                  sendToHost("ai.route", evt.route);
                }
              }
            } catch (error) {}
          }
        }
      }
    }

    async function handleChatMessage(message) {
      if (!message) return;
      append("user", message);
      assistantNode = append("assistant", "正在向 BFF 请求内容反馈...");
      try {
        await streamChat(message);
      } catch (error) {
        updateNode(assistantNode, "BFF 请求失败：" + (error && error.message ? error.message : "unknown"));
      }
    }

    window.addEventListener("message", (event) => {
      const data = event.data || {};
      if (!data || data.version !== version) {
        return;
      }
      if (data.type === "ai.context") {
        hostContext = data.payload || null;
        contextBox.textContent = JSON.stringify(hostContext, null, 2);
        authToken = (hostContext && hostContext.session && hostContext.session.authToken) ? String(hostContext.session.authToken) : "";
        sendToHost("ai.ready", { ok: true });
      }
      if (data.type === "ai.chat") {
        const message = data.payload && data.payload.message ? data.payload.message : "";
        handleChatMessage(message);
      }
    });

    document.getElementById("send").addEventListener("click", () => {
      const message = input.value.trim();
      if (!message) return;
      handleChatMessage(message);
      input.value = "";
    });

    document.querySelectorAll(".chip").forEach((button) => {
      button.addEventListener("click", () => {
        const action = button.getAttribute("data-action");
        const mapping = {
          title: "请根据当前上下文生成标题候选，并说明每个标题适合的场景。",
          summary: "请总结当前页面内容，并给出可执行建议。",
          keywords: "请抽取当前页面的关键词，并按重要性排序。",
          route: "请给出一个推荐跳转。"
        };
        const message = mapping[action];
        if (message) {
          handleChatMessage(message);
        }
      });
    });

    sendToHost("ai.ready", { ok: true });
  </script>
</body>
</html>
        """.strip()
    )


@app.post("/api/v1/agent/chat")
async def chat(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(ChatRequest, request)
    result = await agent_runtime.run(payload, auth_token)
    return JSONResponse(result)


@app.post("/api/v1/agent/chat/stream")
async def chat_stream(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(ChatRequest, request)
    result = await agent_runtime.run(payload, auth_token)
    return StreamingResponse(_stream_sse(result), media_type="text/event-stream")


@app.post("/api/v1/agent/keywords")
async def keywords(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(KeywordRequest, request)
    result = await agent_runtime.keywords(payload, auth_token)
    return JSONResponse(result)


@app.post("/api/v1/agent/similar-questions")
async def similar_questions(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(SimilarQuestionRequest, request)
    result = await agent_runtime.similar_questions(payload, auth_token)
    return JSONResponse(result)


@app.post("/api/v1/agent/draft-question")
async def draft_question(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(DraftQuestionRequest, request)
    if not payload.input.strip():
        raise HTTPException(status_code=422, detail="input is required")
    result = await agent_runtime.draft_question(payload, auth_token)
    return JSONResponse(result)


@app.post("/api/v1/agent/site-search")
async def site_search(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(SiteSearchRequest, request)
    keyword = payload.keyword.strip()
    if len(keyword) < 2:
        raise HTTPException(status_code=422, detail="keyword must be at least 2 characters")
    result = await agent_runtime.site_search(payload, auth_token)
    return JSONResponse(result)


@app.post("/api/v1/agent/notes/summary")
async def note_summary(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(NoteSummaryRequest, request)
    result = await agent_runtime.summarize_note(payload, auth_token)
    return JSONResponse(result)


@app.post("/api/v1/agent/questions/reference")
async def question_reference(request: Request, authorization: str | None = Header(default=None)):
    auth_token = _auth_token(authorization)
    payload = await _parse_model(QuestionReferenceRequest, request)
    result = await agent_runtime.reference_question(payload, auth_token)
    return JSONResponse(result)
