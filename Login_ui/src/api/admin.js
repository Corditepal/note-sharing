import request from './request'

// 管理员登录
export const adminLogin = async (email, password) => {
  const res = await request.post('/auth/admin/login', {
    email,
    password
  })
  return res.data
}

// ========== 用户管理 ==========
// 获取所有在线用户
export const getOnlineUsers = async () => {
  const res = await request.get('/admin/online-users')
  return res.data
}

// 获取所有用户（分页）
export const getAllUsers = async (page = 1, size = 30) => {
  const res = await request.get('/admin/users/all', {
    params: { page, size }
  })
  return res.data
}

// 根据邮箱获取用户信息（包含发布文章和关注列表）
export const getUserInfoByEmail = async (email) => {
  const res = await request.get('/admin/user/by-email', {
    params: { email }
  })
  return res.data
}

// ========== 笔记管理 ==========
// 统计笔记总数
export const getNoteCount = async () => {
  const res = await request.get('/admin/notes/count')
  return res.data
}

// 获取所有笔记列表
export const getNoteList = async () => {
  const res = await request.get('/admin/notes/list')
  return res.data
}

// 搜索笔记（管理员端，使用Elasticsearch搜索）
export const searchNotes = async (keyword) => {
  // 注意：后端API要求userId不能为null，但管理员搜索不需要记录用户行为
  // 这里传入0作为占位符，后端会判断如果userId为0或无效则不记录搜索行为
  const res = await request.post('/search/notes', {
    keyword: keyword,
    userId: 0 // 管理员搜索使用0作为占位符
  })
  return res.data
}

// ========== 评论管理 ==========
// 统计评论总数
export const getRemarkCount = async () => {
  const res = await request.get('/admin/remarks/count')
  return res.data
}

// 获取所有评论列表
export const getRemarkList = async () => {
  const res = await request.get('/admin/remarks/list')
  return res.data
}

// 管理员删除评论（可删除任何评论）
export const adminDeleteRemark = async (remarkId) => {
  const res = await request.delete(`/admin/remarks/${remarkId}`)
  return res.data
}

// ========== 敏感词检查 ==========
// 检查纯文本敏感词
export const checkSensitiveText = async (text) => {
  const res = await request.post('/admin/sensitive/check/text', { text })
  return res.data
}

// 检查笔记敏感词（全文模式）
export const checkNoteSensitive = async (noteId) => {
  const res = await request.get(`/admin/sensitive/check/note/${noteId}`)
  return res.data
}

// 检查笔记敏感词（全文模式，兼容旧接口）
export const checkNoteSensitiveFull = async (noteId) => {
  const res = await request.get(`/admin/sensitive/check/note/${noteId}/full`)
  return res.data
}

// 批量检查笔记敏感词
export const batchCheckSensitive = async (noteIds) => {
  const res = await request.post('/admin/sensitive/check/batch', { noteIds })
  return res.data
}

// ========== 敏感词过滤 ==========
// 快速过滤检查文本
export const fastFilterText = async (text) => {
  const res = await request.post('/admin/sensitive/fast-filter', { text })
  return res.data
}

// 重新加载敏感词库
export const reloadSensitiveWords = async () => {
  const res = await request.post('/admin/sensitive/reload')
  return res.data
}

// 深度检查文本
export const deepCheckText = async (text) => {
  const res = await request.post('/admin/sensitive/deep-check', { text })
  return res.data
}

// 重新加载深度检查词库
export const reloadDeepCheckWords = async () => {
  const res = await request.post('/admin/sensitive/deep-reload')
  return res.data
}

// ========== 问答管理 ==========
// 统计问题总数
export const getQuestionCount = async () => {
  const res = await request.get('/admin/questions/count')
  return res.data
}

// 获取所有问题列表
export const getQuestionList = async () => {
  const res = await request.get('/admin/questions/list')
  return res.data
}

// 管理员删除问题（可删除任何问题）
export const adminDeleteQuestion = async (questionId) => {
  const res = await request.delete(`/admin/questions/${questionId}`)
  return res.data
}

// 统计回答总数
export const getAnswerCount = async () => {
  const res = await request.get('/admin/answers/count')
  return res.data
}

// ========== 问答评论管理 ==========
// 统计问答评论总数（包括Comment和Reply）
export const getQACommentCount = async () => {
  const res = await request.get('/admin/qa-comments/count')
  return res.data
}

// 获取所有问答评论列表（包括Comment和Reply）
export const getQACommentList = async () => {
  const res = await request.get('/admin/qa-comments/list')
  return res.data
}

// 管理员删除问答评论（可删除任何评论或回复）
export const adminDeleteQAComment = async (type, id, questionId, answerId, commentId) => {
  const params = { questionId, answerId }
  if (commentId) {
    params.commentId = commentId
  }
  const res = await request.delete(`/admin/qa-comments/${type}/${id}`, { params })
  return res.data
}

// 获取问答评论树（用于查看评论结构）
export const getQACommentTree = async (questionId, answerId, commentId) => {
  const res = await request.get('/admin/qa-comments/tree', {
    params: { questionId, answerId, commentId }
  })
  return res.data
}

// 统计评论下的回复数量
export const countCommentReplies = async (questionId, answerId, commentId) => {
  const res = await request.get('/admin/qa-comments/count-replies', {
    params: { questionId, answerId, commentId }
  })
  return res.data
}

// ========== 内容审查管理 ==========
// 提交审查记录（标记笔记为审核中）
export const submitModeration = async (noteId, checkResult) => {
  const res = await request.post('/admin/moderation/submit', {
    noteId: noteId,
    status: checkResult.status || 'FLAGGED',
    riskLevel: checkResult.riskLevel || 'MEDIUM',
    score: checkResult.score || 0,
    categories: checkResult.categories || [],
    findings: checkResult.findings || []
  })
  return res.data
}

// 获取待处理的审查记录列表
export const getPendingModerations = async () => {
  const res = await request.get('/admin/moderation/pending')
  return res.data
}

// 获取待审核的笔记列表
export const getPendingNotes = async () => {
  const res = await request.get('/admin/moderation/pending-notes')
  return res.data
}

// 获取审查记录详情
export const getModerationDetail = async (id) => {
  const res = await request.get(`/admin/moderation/${id}`)
  return res.data
}

// 获取笔记的审查历史
export const getNoteModerationHistory = async (noteId) => {
  const res = await request.get(`/admin/moderation/note/${noteId}`)
  return res.data
}

// 审查笔记（调用深度检测）
export const reviewNote = async (noteId) => {
  const res = await request.get(`/admin/moderation/review/${noteId}`)
  return res.data
}

// 处理审查结果（通过/未通过，发布/退回，发送私信）
export const handleReviewResult = async (moderationId, approved, adminComment) => {
  const res = await request.post('/admin/moderation/review-result', {
    moderationId,
    approved,
    adminComment
  })
  return res.data
}

// 处理审查记录
export const handleModeration = async (id, adminComment, isHandled = true) => {
  const res = await request.post(`/admin/moderation/${id}/handle`, { 
    adminComment: adminComment || '',
    isHandled: isHandled
  })
  return res.data
}
