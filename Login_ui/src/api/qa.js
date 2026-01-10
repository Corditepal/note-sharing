import service from './request'

// 问答模块 API 封装

// 问题
export const createQuestion = (payload) =>
  service.post('/qa/question', payload).then(res => res.data.data)

export const getQuestionDetail = (questionId) =>
  service.get(`/qa/question/${questionId}`).then(res => res.data.data)

export const deleteQuestion = (questionId) =>
  service.delete(`/qa/question/${questionId}`).then(res => res.data.data)

export const likeQuestion = (userId, questionId) =>
  service.post('/qa/question/like', { userId, questionId }).then(res => res.data.data)

export const favoriteQuestion = (userId, questionId) =>
  service.post('/qa/question/favorite', { userId, questionId }).then(res => res.data.data)

// 回答
export const createAnswer = (payload) =>
  service.post('/qa/answer', payload).then(res => res.data.data)

export const deleteAnswer = (questionId, answerId) =>
  service.delete('/qa/answer', { data: { questionId, answerId } }).then(res => res.data.data)

export const likeAnswer = (userId, questionId, answerId) =>
  service.post('/qa/answer/like', { userId, questionId, answerId }).then(res => res.data.data)

// 评论
export const createComment = (payload) =>
  service.post('/qa/comment', payload).then(res => res.data.data)

export const deleteComment = (questionId, answerId, commentId) =>
  service.delete('/qa/comment', { data: { questionId, answerId, commentId } }).then(res => res.data.data)

export const likeComment = (userId, questionId, answerId, commentId) =>
  service.post('/qa/comment/like', { userId, questionId, answerId, commentId }).then(res => res.data.data)

// 回复（二级评论）
export const createReply = (payload) =>
  service.post('/qa/reply', payload).then(res => res.data.data)

export const deleteReply = (questionId, answerId, commentId, replyId) =>
  service.delete('/qa/reply', { data: { questionId, answerId, commentId, replyId } }).then(res => res.data.data)

export const likeReply = (userId, questionId, answerId, commentId, replyId) =>
  service.post('/qa/reply/like', { userId, questionId, answerId, commentId, replyId }).then(res => res.data.data)

// 搜索问答
export const searchQuestions = (keyword, userId) =>
  service.get('/search/questions', {
    params: { keyword, userId }
  }).then(res => res.data.data)

// 推荐问答
export const getRecommendedQAs = (userId, topN = 10) =>
  service.get('/recommend/QAs', {
    params: { userId, topN }
  }).then(res => res.data.data)



