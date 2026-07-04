import MarkdownIt from 'markdown-it'
import DOMPurify from 'dompurify'

export const FOLIO_LINK_URI_REGEXP = /^folio:\/\/(note|question)\/.+/i

const mdParser = new MarkdownIt({
  html: false,
  linkify: false,
  breaks: true,
  typographer: false
})

const SANITIZE_OPTIONS = {
  ALLOWED_TAGS: ['p', 'br', 'strong', 'em', 'code', 'pre', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'blockquote', 'a'],
  ALLOWED_ATTR: ['href'],
  ALLOWED_URI_REGEXP: FOLIO_LINK_URI_REGEXP
}

export function renderAssistantMarkdown(source) {
  const text = String(source || '')
  if (!text.trim()) {
    return ''
  }

  const html = mdParser.render(text)
  return DOMPurify.sanitize(html, SANITIZE_OPTIONS)
}

export function resolveAssistantContentFormat(answerFormat) {
  return answerFormat === 'plain' ? 'plain' : 'markdown'
}
