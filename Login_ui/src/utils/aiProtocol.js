import { AI_PROTOCOL_VERSION } from '@/config/ai'

function toPlainQuery(query) {
  const result = {}

  if (!query || typeof query !== 'object') {
    return result
  }

  Object.entries(query).forEach(([key, value]) => {
    if (value === undefined || value === null) {
      return
    }

    if (Array.isArray(value)) {
      result[key] = value.map(item => String(item))
      return
    }

    result[key] = String(value)
  })

  return result
}

function toPreviewText(value, limit = 320) {
  if (!value) {
    return ''
  }

  const normalized = String(value)
    .replace(/<[^>]*>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()

  if (!normalized) {
    return ''
  }

  if (normalized.length <= limit) {
    return normalized
  }

  return `${normalized.slice(0, limit).trim()}…`
}

export function buildAiHostSnapshot({
  route,
  userInfo,
  currentTab,
  pageMode,
  searchKeyword,
  viewingNoteId,
  selectedWorkspaceId,
  editingNotebookId,
  editingSpaceId,
  resource,
  authToken,
  permissions
}) {
  return {
    version: AI_PROTOCOL_VERSION,
    timestamp: new Date().toISOString(),
    route: {
      name: route?.name || null,
      path: route?.path || null,
      query: toPlainQuery(route?.query)
    },
    page: {
      tab: currentTab || null,
      mode: pageMode || null,
      searchKeyword: searchKeyword || null,
      viewingNoteId: viewingNoteId || null,
      selectedWorkspaceId: selectedWorkspaceId || null,
      editingNotebookId: editingNotebookId || null,
      editingSpaceId: editingSpaceId || null
    },
    resource: resource
      ? {
          kind: resource.kind || null,
          id: resource.id || null,
          title: resource.title || null,
          fileType: resource.fileType || null,
          status: resource.status || null,
          noteId: resource.noteId || null,
          questionId: resource.questionId || null,
          notebookId: resource.notebookId || null,
          spaceId: resource.spaceId || null,
          contentPreview: toPreviewText(resource.contentPreview || resource.content || ''),
          contentLength: resource.contentLength || 0,
          commentCount: resource.commentCount || 0,
          answerCount: resource.answerCount || 0,
          isDirty: Boolean(resource.isDirty),
          updatedAt: resource.updatedAt || null,
          selectedText: toPreviewText(resource.selectedText || '', 160),
          tags: Array.isArray(resource.tags) ? resource.tags.slice(0, 10) : []
        }
      : null,
    session: {
      authToken: authToken || null
    },
    user: {
      id: userInfo?.id || null,
      username: userInfo?.username || null,
      role: userInfo?.role || null
    },
    permissions: {
      canAccessWriteActions:
        typeof permissions?.canAccessWriteActions === 'boolean'
          ? permissions.canAccessWriteActions
          : userInfo?.role === 'Admin' || userInfo?.role === 'User'
    }
  }
}

export function isAiHostMessage(message) {
  return Boolean(message && message.version === AI_PROTOCOL_VERSION && typeof message.type === 'string')
}

export function isAllowedRouteTarget(target) {
  if (!target || typeof target !== 'object') {
    return false
  }

  const path = typeof target.path === 'string' ? target.path : ''
  const routeName = typeof target.routeName === 'string' ? target.routeName : ''

  if (!path && !routeName) {
    return false
  }

  return !/^javascript:/i.test(path) && !/^https?:\/\//i.test(path)
}

export function buildRouteTarget(target) {
  if (!isAllowedRouteTarget(target)) {
    return null
  }

  return {
    routeName: target.routeName || null,
    path: target.path || null,
    params: target.params && typeof target.params === 'object' ? target.params : {},
    query: target.query && typeof target.query === 'object' ? target.query : {}
  }
}

function parseNoteId(value) {
  if (value === undefined || value === null || value === '') {
    return null
  }

  const noteId = Number(value)
  if (!Number.isInteger(noteId) || noteId <= 0) {
    return null
  }

  return noteId
}

function parseQuestionId(value) {
  if (value === undefined || value === null) {
    return null
  }

  const questionId = String(value).trim()
  return questionId || null
}

export function isRouteableCitation(citation) {
  if (!citation || typeof citation !== 'object') {
    return false
  }

  const title = String(citation.title || '').trim()
  const route = citation.route
  if (!title || !route || typeof route !== 'object') {
    return false
  }

  if (citation.type === 'note') {
    const noteId = parseNoteId(citation.noteId ?? route.noteId)
    return route.tab === 'note-detail' && noteId !== null
  }

  if (citation.type === 'question') {
    const questionId = parseQuestionId(citation.questionId ?? route.questionId)
    return route.tab === 'qa-detail' && questionId !== null
  }

  return false
}

export function buildCitationNavigationTarget(citation) {
  if (!isRouteableCitation(citation)) {
    return null
  }

  const title = String(citation.title || '').trim()

  if (citation.type === 'note') {
    const noteId = parseNoteId(citation.noteId ?? citation.route?.noteId)
    if (noteId === null) {
      return null
    }

    return {
      type: 'note',
      noteId,
      title
    }
  }

  const questionId = parseQuestionId(citation.questionId ?? citation.route?.questionId)
  if (questionId === null) {
    return null
  }

  return {
    type: 'question',
    questionId,
    title
  }
}

const FOLIO_LINK_HREF_RE = /^folio:\/\/(note|question)\/([^/?#]+)\/?$/i

export function parseFolioLinkHref(href) {
  const match = String(href || '').trim().match(FOLIO_LINK_HREF_RE)
  if (!match) {
    return null
  }

  const type = match[1].toLowerCase()
  const idPart = decodeURIComponent(match[2])

  if (type === 'note') {
    const noteId = parseNoteId(idPart)
    return noteId !== null ? { type: 'note', noteId } : null
  }

  const questionId = parseQuestionId(idPart)
  return questionId ? { type: 'question', questionId } : null
}

export function findCitationForFolioLink(href, citations = []) {
  const parsed = parseFolioLinkHref(href)
  if (!parsed) {
    return null
  }

  for (const citation of citations) {
    if (!citation || typeof citation !== 'object') {
      continue
    }

    if (parsed.type === 'note' && citation.type === 'note') {
      const noteId = parseNoteId(citation.noteId ?? citation.route?.noteId)
      if (noteId === parsed.noteId) {
        return citation
      }
    }

    if (parsed.type === 'question' && citation.type === 'question') {
      const questionId = parseQuestionId(citation.questionId ?? citation.route?.questionId)
      if (questionId === parsed.questionId) {
        return citation
      }
    }
  }

  return null
}

export function buildInlineLinkNavigationTarget(href, citations = []) {
  return buildCitationNavigationTarget(findCitationForFolioLink(href, citations))
}
