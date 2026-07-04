import { getAiBffOrigin } from '@/config/ai'
import { AI_MESSAGES } from '@/constants/aiMessages'

const AI_FETCH_TIMEOUT_MS = 12000

function getAiToken() {
  return localStorage.getItem('token') || ''
}

function buildAiHeaders() {
  const headers = {
    'Content-Type': 'application/json'
  }

  const token = getAiToken()
  if (token) {
    headers.Authorization = `Bearer ${token}`
  }

  return headers
}

export function mapAiFetchError(error, response) {
  if (error?.name === 'AbortError') {
    return AI_MESSAGES.networkError
  }

  const status = response?.status ?? error?.status
  if (status === 401) {
    return AI_MESSAGES.unauthenticated
  }
  if (status === 403) {
    return AI_MESSAGES.writeDenied
  }
  if (error instanceof TypeError) {
    return AI_MESSAGES.bffUnavailable
  }
  if (typeof status === 'number' && status >= 500) {
    return AI_MESSAGES.bffUnavailable
  }
  return AI_MESSAGES.bffUnavailable
}

export async function postAiJson(path, payload) {
  const origin = getAiBffOrigin().replace(/\/$/, '')
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), AI_FETCH_TIMEOUT_MS)

  try {
    const response = await fetch(`${origin}${path}`, {
      method: 'POST',
      headers: buildAiHeaders(),
      body: JSON.stringify(payload),
      signal: controller.signal
    })

    if (!response.ok) {
      const message = mapAiFetchError(null, response)
      throw Object.assign(new Error(message), { status: response.status })
    }

    return response.json()
  } catch (error) {
    if (error instanceof Error && error.status) {
      throw error
    }
    throw Object.assign(new Error(mapAiFetchError(error, null)), {
      status: error?.status
    })
  } finally {
    clearTimeout(timeoutId)
  }
}

export async function probeAiBff() {
  const origin = getAiBffOrigin().replace(/\/$/, '')
  const response = await fetch(`${origin}/health`, {
    method: 'GET',
    headers: buildAiHeaders()
  })

  if (!response.ok) {
    const message = mapAiFetchError(null, response)
    throw Object.assign(new Error(message), { status: response.status })
  }

  return response.json()
}

export function buildAiRequestContext(resource, extras = {}) {
  return {
    version: '1.0',
    page: extras.page || {},
    resource,
    user: extras.user || {},
    permissions: extras.permissions || {}
  }
}

export async function fetchSimilarQuestions(question, { limit = 3, context = {} } = {}) {
  const text = String(question || '').trim()
  if (!text) {
    return { items: [] }
  }

  return postAiJson('/api/v1/agent/similar-questions', {
    question: text,
    limit,
    context
  })
}

export async function fetchDraftQuestion(input, { context = {} } = {}) {
  const text = String(input || '').trim()
  if (!text) {
    throw new Error(AI_MESSAGES.draftInputEmpty)
  }

  return postAiJson('/api/v1/agent/draft-question', {
    input: text,
    context
  })
}
