// src/api/remark.js
import service from './request'

/**
 * [对应后端: GET /api/v1/remark/note/list]
 * 获取指定笔记的评论列表
 * @param {number} noteId - 笔记ID
 * @param {number} loginUserId - 当前登录用户ID
 * @returns {Promise<Array>} 返回评论列表 (RemarkVO数组)
 */
export const getRemarksByNote = (noteId, loginUserId) => {
  return service.get('/remark/note/list', {
    params: {
      noteId: noteId,
      loginUserId: loginUserId
    }
  }).then(res => res.data.data)
}

/**
 * [对应后端: POST /api/v1/remark/insert]
 * 插入一条新评论
 * @param {Object} remarkData - 评论数据 (RemarkInsertDTO)
 * @param {number} remarkData.noteId - 评论属于哪篇笔记
 * @param {number} remarkData.userId - 评论用户ID
 * @param {string} remarkData.username - 评论用户名
 * @param {string} remarkData.replyToUsername - 回复的目标用户名（可选）
 * @param {string} remarkData.content - 评论内容
 * @param {string} remarkData.parentId - 父评论ID（楼中楼，可选）
 * @param {boolean} remarkData.isReceive - 是否是回复（可选）
 * @param {string} remarkData.receiveToRemarkId - 回复哪条评论的ID（可选）
 * @returns {Promise<boolean>} 返回操作结果
 */
export const insertRemark = (remarkData) => {
  return service.post('/remark/insert', remarkData)
    .then(res => res.data.data)
}

/**
 * [对应后端: POST /api/v1/remark/delete]
 * 删除一条评论
 * @param {string} remarkId - 评论ID
 * @returns {Promise<boolean>} 返回操作结果
 */
export const deleteRemark = (remarkId) => {
  return service.post('/remark/delete', {
    id: remarkId
  }).then(res => res.data.data)
}

/**
 * [对应后端: POST /api/v1/remark/like]
 * 点赞一条评论
 * @param {string} remarkId - 评论ID
 * @param {number} loginUserId - 当前登录用户ID
 * @returns {Promise<boolean>} 返回操作结果
 */
export const likeRemark = (remarkId, loginUserId) => {
  return service.post('/remark/like', null, {
    params: {
      remarkId: remarkId,
      loginUserId: loginUserId
    }
  }).then(res => res.data.data)
}

/**
 * [对应后端: POST /api/v1/remark/cancelLike]
 * 取消点赞一条评论
 * @param {string} remarkId - 评论ID
 * @param {number} loginUserId - 当前登录用户ID
 * @returns {Promise<boolean>} 返回操作结果
 */
export const cancelLikeRemark = (remarkId, loginUserId) => {
  return service.post('/remark/cancelLike', null, {
    params: {
      remarkId: remarkId,
      loginUserId: loginUserId
    }
  }).then(res => res.data.data)
}
