<template>
  <div class="note-detail-page">
    <div v-if="loading" class="loading-state">
      <div class="loader"></div>
      <p>加载中...</p>
    </div>

    <div v-else-if="error" class="error-state">
      <p>{{ error }}</p>
      <button class="back-button" @click="goBack">返回</button>
    </div>

    <div v-else-if="noteDetail" class="detail-layout">
      <div class="detail-container">
        <!-- 返回按钮 -->
        <div class="back-header">
          <button class="back-link-button" @click="goBack">
            <svg class="back-icon" viewBox="0 0 16 16" fill="currentColor">
              <path fill-rule="evenodd" d="M15 8a.5.5 0 0 0-.5-.5H2.707l3.147-3.146a.5.5 0 1 0-.708-.708l-4 4a.5.5 0 0 0 0 .708l4 4a.5.5 0 0 0 .708-.708L2.707 8.5H14.5A.5.5 0 0 0 15 8z"/>
            </svg>
            <span>返回</span>
          </button>
        </div>
        
        <!-- 头部：标题 -->
        <header class="detail-header">
          <h1 class="note-title">{{ noteDetail.title || '无标题' }}</h1>
        </header>

        <!-- 作者和统计信息 -->
        <div class="note-meta-section">
          <div class="author-info">
            <svg class="author-icon" viewBox="0 0 16 16" fill="currentColor">
              <path d="M8 8a3 3 0 100-6 3 3 0 000 6zm2-3a2 2 0 11-4 0 2 2 0 014 0zm4 8c0 1-1 1-1 1H3s-1 0-1-1 1-4 6-4 6 3 6 4zm-1-.004c-.001-.246-.154-.986-.832-1.664C11.516 10.68 10.289 10 8 10c-2.29 0-3.516.68-4.168 1.332-.678.678-.83 1.418-.832 1.664h10z"/>
            </svg>
            <span class="author-name">{{ stats.authorName || '未知作者' }}</span>
            <span v-if="noteDetail.createdAt" class="create-time">
              <svg class="time-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 3.5a.5.5 0 00-1 0V9a.5.5 0 00.252.434l3.5 2a.5.5 0 00.496-.868L8 8.71V3.5z"/>
                <path d="M8 16A8 8 0 108 0a8 8 0 000 16zm7-8A7 7 0 111 8a7 7 0 0114 0z"/>
              </svg>
              {{ formatTime(noteDetail.createdAt) }}
            </span>
          </div>
          <div class="stats-info">
            <span class="stat-item">
              <svg class="stat-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 4a.5.5 0 01.5.5v3h3a.5.5 0 010 1h-3v3a.5.5 0 01-1 0v-3h-3a.5.5 0 010-1h3v-3A.5.5 0 018 4z"/>
              </svg>
              {{ stats.views || 0 }} 阅读
            </span>
            <span class="stat-item">
              <svg class="stat-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 15A7 7 0 118 1a7 7 0 010 14zm0 1A8 8 0 108 0a8 8 0 000 16z"/>
                <path d="M8 4a.5.5 0 00-.5.5v3h-3a.5.5 0 000 1h3v3a.5.5 0 001 0v-3h3a.5.5 0 000-1h-3v-3A.5.5 0 008 4z"/>
              </svg>
              {{ stats.likes || 0 }} 点赞
            </span>
            <span class="stat-item">
              <svg class="stat-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M2 2v13.5a.5.5 0 00.74.439L8 13.069l5.26 2.87A.5.5 0 0014 15.5V2a2 2 0 00-2-2H4a2 2 0 00-2 2z"/>
              </svg>
              {{ stats.favorites || 0 }} 收藏
            </span>
            <span class="stat-item">
              <svg class="stat-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M2.5 1A1.5 1.5 0 001 2.5v11A1.5 1.5 0 002.5 15h6.086a1.5 1.5 0 001.06-.44l4.915-4.914A1.5 1.5 0 0015 7.586V2.5A1.5 1.5 0 0013.5 1h-11zM2 2.5a.5.5 0 01.5-.5h11a.5.5 0 01.5.5v7.086a.5.5 0 01-.146.353l-4.915 4.915a.5.5 0 01-.353.146H2.5a.5.5 0 01-.5-.5v-11z"/>
                <path d="M5.5 6a.5.5 0 000 1h5a.5.5 0 000-1h-5zM5 8.5a.5.5 0 01.5-.5h5a.5.5 0 010 1h-5a.5.5 0 01-.5-.5zm0 2a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5z"/>
              </svg>
              {{ stats.comments || 0 }} 评论
            </span>
          </div>
        </div>

        <!-- 主题部分：笔记内容（不可编辑） -->
        <div class="note-content-section">
          <div v-if="contentLoading" class="content-loading">
            <div class="loader"></div>
            <p>加载笔记内容...</p>
          </div>
          <div v-else-if="noteDetail.fileType === 'pdf'" class="pdf-preview-container">
            <VuePdfEmbed
              v-if="fileUrl"
              :source="fileUrl"
              class="pdf-viewer"
              :width="800"
            />
            <p v-else class="no-content">无法加载PDF文件</p>
          </div>
          <div v-else-if="noteDetail.fileType === 'md'" class="markdown-content">
            <div 
              v-if="fileUrl && markdownContent" 
              class="markdown-body"
              v-html="markdownContent"
            ></div>
            <p v-else class="no-content">无法加载笔记内容</p>
          </div>
          <div v-else-if="!noteDetail.fileType" class="no-content">
            <p>不支持的文件类型，仅支持 md 或 pdf 格式</p>
          </div>
          <div v-else class="no-content">
            <p>不支持的文件类型：{{ noteDetail.fileType }}，仅支持 md 或 pdf 格式</p>
          </div>
        </div>

        <!-- 尾部：评论部分 -->
        <div class="comments-section">
          <div class="comments-header">
            <h2 class="comments-title">评论</h2>
            <span class="comments-count">({{ comments.length || 0 }})</span>
          </div>

          <!-- 发表评论表单 -->
          <div v-if="userStore.isLoggedIn" class="comment-form">
            <div class="comment-form-header">
              <span class="comment-form-label">发表评论</span>
            </div>
            <textarea
              v-model="newCommentContent"
              class="comment-input"
              placeholder="写下你的评论..."
              rows="4"
              :disabled="commentSubmitting"
            ></textarea>
            <div class="comment-form-actions">
              <button
                class="comment-submit-btn"
                :disabled="!newCommentContent.trim() || commentSubmitting"
                @click="handleSubmitComment"
              >
                {{ commentSubmitting ? '提交中...' : '发表评论' }}
              </button>
            </div>
          </div>
          <div v-else class="comment-login-tip">
            <p>请先登录后再发表评论</p>
          </div>

          <!-- 评论列表 -->
          <div v-if="commentsLoading" class="comments-loading">
            <div class="loader"></div>
            <p>加载评论中...</p>
          </div>
          <div v-else-if="comments.length === 0" class="comments-empty">
            <p>暂无评论，快来发表第一条评论吧~</p>
          </div>
          <div v-else class="comments-list">
            <div
              v-for="comment in comments"
              :key="comment._id"
              class="comment-item"
            >
              <div class="comment-main">
                <div class="comment-header">
                  <div class="comment-author-info">
                    <img 
                      :src="'/assets/avatars/avatar.png'" 
                      :alt="comment.username"
                      class="comment-avatar"
                    />
                    <span class="comment-author">{{ comment.username || '匿名用户' }}</span>
                  </div>
                  <span class="comment-time">{{ formatTime(comment.createdAt) }}</span>
                </div>
                <div class="comment-content-wrapper">
                  <p class="comment-content">
                    <span v-if="comment.replyToUsername" class="reply-to">
                      回复 @{{ comment.replyToUsername }}：
                    </span>
                    <span>{{ comment.content }}</span>
                  </p>
                </div>
                <div class="comment-actions">
                  <button
                    class="comment-action-btn"
                    :class="{ active: comment.LikedOrNot }"
                    :disabled="commentActionLoading[comment._id]"
                    @click="handleToggleLike(comment)"
                  >
                    <svg class="action-icon-small" viewBox="0 0 16 16" fill="currentColor">
                      <path d="m8 2.748-.717-.737C5.6.281 2.514.878 1.4 3.053c-.523 1.023-.641 2.5.314 4.385C2.972 9.59 5.614 12.368 8 14.25c2.386-1.882 5.028-4.659 6.286-6.813.955-1.886.838-3.362.314-4.385C13.486.878 10.4.28 8.717 2.01z"/>
                    </svg>
                    <span>{{ comment.likeCount || 0 }}</span>
                  </button>
                  <button
                    v-if="userStore.isLoggedIn"
                    class="comment-action-btn"
                    @click="handleReply(comment)"
                  >
                    回复
                  </button>
                  <button
                    v-if="userStore.isLoggedIn"
                    class="comment-action-btn danger"
                    :disabled="commentActionLoading[comment._id]"
                    @click="handleDeleteComment(comment)"
                  >
                    删除
                  </button>
                </div>
              </div>

              <!-- 回复表单 -->
              <div v-if="replyingTo === comment._id" class="reply-form">
                <textarea
                  v-model="replyContent"
                  class="comment-input reply-input"
                  :placeholder="`回复 @${comment.username}:`"
                  rows="3"
                  :disabled="commentSubmitting"
                ></textarea>
                <div class="reply-form-actions">
                  <button
                    class="comment-submit-btn small"
                    :disabled="!replyContent.trim() || commentSubmitting"
                    @click="handleSubmitReply(comment)"
                  >
                    {{ commentSubmitting ? '提交中...' : '回复' }}
                  </button>
                  <button
                    class="comment-cancel-btn"
                    @click="cancelReply"
                  >
                    取消
                  </button>
                </div>
              </div>

              <!-- 子评论（回复） -->
              <div v-if="comment.replies && comment.replies.length > 0" class="comment-replies">
                <div
                  v-for="replyGroup in organizeReplies(comment.replies, comment._id)"
                  :key="replyGroup.mainReply._id"
                  class="reply-wrapper"
                >
                  <!-- 二级评论 -->
                  <div class="comment-item reply-item">
                    <div class="comment-main">
                      <div class="comment-header">
                        <div class="comment-author-info">
                          <img 
                            :src="'/assets/avatars/avatar.png'" 
                            :alt="replyGroup.mainReply.username"
                            class="comment-avatar"
                          />
                          <span class="comment-author">{{ replyGroup.mainReply.username || '匿名用户' }}</span>
                        </div>
                        <div class="comment-header-right">
                          <span class="comment-time">{{ formatTime(replyGroup.mainReply.createdAt) }}</span>
                        </div>
                      </div>
                      <div class="comment-content-wrapper">
                        <p class="comment-content">
                          <template v-if="replyGroup.mainReply.replyToUsername">
                            <span v-if="replyGroup.mainReply.replyToUsername !== comment.username && findReplyTarget(replyGroup.mainReply, comment.replies, comment)" class="reply-to">
                              回复 @{{ findReplyTarget(replyGroup.mainReply, comment.replies, comment) }}：回复 @{{ replyGroup.mainReply.replyToUsername }}：
                            </span>
                            <span v-else class="reply-to">
                              回复 @{{ replyGroup.mainReply.replyToUsername }}：
                            </span>
                          </template>
                          <span>{{ replyGroup.mainReply.content }}</span>
                        </p>
                      </div>
                      <div class="comment-actions">
                        <button
                          class="comment-action-btn"
                          :class="{ active: replyGroup.mainReply.LikedOrNot }"
                          :disabled="commentActionLoading[replyGroup.mainReply._id]"
                          @click="handleToggleLike(replyGroup.mainReply)"
                        >
                          <svg class="action-icon-small" viewBox="0 0 16 16" fill="currentColor">
                            <path d="m8 2.748-.717-.737C5.6.281 2.514.878 1.4 3.053c-.523 1.023-.641 2.5.314 4.385C2.972 9.59 5.614 12.368 8 14.25c2.386-1.882 5.028-4.659 6.286-6.813.955-1.886.838-3.362.314-4.385C13.486.878 10.4.28 8.717 2.01z"/>
                          </svg>
                          <span>{{ replyGroup.mainReply.likeCount || 0 }}</span>
                        </button>
                        <button
                          v-if="userStore.isLoggedIn"
                          class="comment-action-btn"
                          @click="handleReplyToReply(comment, replyGroup.mainReply)"
                        >
                          回复
                        </button>
                        <button
                          v-if="userStore.isLoggedIn"
                          class="comment-action-btn danger"
                          :disabled="commentActionLoading[replyGroup.mainReply._id]"
                          @click="handleDeleteComment(replyGroup.mainReply)"
                        >
                          删除
                        </button>
                      </div>
                    </div>

                    <!-- 回复回复的表单 -->
                    <div v-if="replyingTo === replyGroup.mainReply._id" class="reply-form">
                      <textarea
                        v-model="replyContent"
                        class="comment-input reply-input"
                        :placeholder="`回复 @${replyGroup.mainReply.username}:`"
                        rows="3"
                        :disabled="commentSubmitting"
                      ></textarea>
                      <div class="reply-form-actions">
                        <button
                          class="comment-submit-btn small"
                          :disabled="!replyContent.trim() || commentSubmitting"
                          @click="handleSubmitReply(comment, replyGroup.mainReply)"
                        >
                          {{ commentSubmitting ? '提交中...' : '回复' }}
                        </button>
                        <button
                          class="comment-cancel-btn"
                          @click="cancelReply"
                        >
                          取消
                        </button>
                      </div>
                    </div>
                  </div>

                  <!-- 回复二级评论的评论（三级评论） -->
                  <div v-if="replyGroup.nestedReplies && replyGroup.nestedReplies.length > 0" class="nested-replies">
                    <div
                      v-for="nestedReply in replyGroup.nestedReplies"
                      :key="nestedReply._id"
                      class="nested-reply-item"
                    >
                      <div class="comment-main">
                        <div class="comment-header">
                          <div class="comment-author-info">
                            <div class="comment-level-badge nested">再回复</div>
                            <img 
                              :src="'/assets/avatars/avatar.png'" 
                              :alt="nestedReply.username"
                              class="comment-avatar"
                            />
                            <span class="comment-author">{{ nestedReply.username || '匿名用户' }}</span>
                          </div>
                          <div class="comment-header-right">
                            <span class="comment-time">{{ formatTime(nestedReply.createdAt) }}</span>
                          </div>
                        </div>
                        <div class="comment-content-wrapper">
                          <p class="comment-content">
                            <template v-if="nestedReply.replyToUsername">
                              <span class="reply-to">
                                回复 @{{ nestedReply.replyToUsername }}：
                              </span>
                            </template>
                            <span>{{ nestedReply.content }}</span>
                          </p>
                        </div>
                        <div class="comment-actions">
                          <button
                            class="comment-action-btn"
                            :class="{ active: nestedReply.LikedOrNot }"
                            :disabled="commentActionLoading[nestedReply._id]"
                            @click="handleToggleLike(nestedReply)"
                          >
                            <svg class="action-icon-small" viewBox="0 0 16 16" fill="currentColor">
                              <path d="m8 2.748-.717-.737C5.6.281 2.514.878 1.4 3.053c-.523 1.023-.641 2.5.314 4.385C2.972 9.59 5.614 12.368 8 14.25c2.386-1.882 5.028-4.659 6.286-6.813.955-1.886.838-3.362.314-4.385C13.486.878 10.4.28 8.717 2.01z"/>
                            </svg>
                            <span>{{ nestedReply.likeCount || 0 }}</span>
                          </button>
                          <button
                            v-if="userStore.isLoggedIn"
                            class="comment-action-btn danger"
                            :disabled="commentActionLoading[nestedReply._id]"
                            @click="handleDeleteComment(nestedReply)"
                          >
                            删除
                          </button>
                        </div>
                      </div>
                    </div>
                  </div>

                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <aside class="action-sidebar">
        <button
          class="action-button"
          :class="{ active: isLiked }"
          :disabled="actionLoading.likes"
          @click="handleToggleStat('likes')"
        >
          <svg class="action-icon" viewBox="0 0 16 16" fill="currentColor">
            <path d="m8 2.748-.717-.737C5.6.281 2.514.878 1.4 3.053c-.523 1.023-.641 2.5.314 4.385C2.972 9.59 5.614 12.368 8 14.25c2.386-1.882 5.028-4.659 6.286-6.813.955-1.886.838-3.362.314-4.385C13.486.878 10.4.28 8.717 2.01z"/>
            <path d="M8 15C-7.333 4.868 3.279-3.04 7.824 1.143c.06.055.119.112.176.171a3.12 3.12 0 0 1 .176-.17C12.72-3.042 23.333 4.867 8 15z"/>
          </svg>
          <div class="action-text">
            <span class="action-label">喜爱</span>
            <span class="action-count">{{ stats.likes || 0 }}</span>
          </div>
        </button>
        <button
          class="action-button"
          :class="{ active: isFavorited }"
          :disabled="actionLoading.favorites"
          @click="handleToggleStat('favorites')"
        >
          <svg class="action-icon" viewBox="0 0 16 16" fill="currentColor">
            <path d="M2 2v12.5a.5.5 0 00.76.43L8 12.101l5.24 2.83A.5.5 0 0014 14.5V2a2 2 0 00-2-2H4a2 2 0 00-2 2z"/>
          </svg>
          <div class="action-text">
            <span class="action-label">收藏</span>
            <span class="action-count">{{ stats.favorites || 0 }}</span>
          </div>
        </button>
      </aside>
    </div>

    <!-- 消息提示组件 -->
    <MessageToast
      v-if="showToast"
      :message="toastMessage"
      :type="toastType"
      :duration="toastDuration"
      @close="hideMessage"
    />
  </div>
</template>

<script setup>
import { ref, onMounted, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { getFileUrlByNoteId, getNoteStats, changeNoteStat } from '@/api/note'
import { getRemarksByNote, insertRemark, deleteRemark, likeRemark, cancelLikeRemark } from '@/api/remark'
import { useUserStore } from '@/stores/user'
import { formatTime } from '@/utils/time'
import VuePdfEmbed from 'vue-pdf-embed'
import MarkdownIt from 'markdown-it'
import MessageToast from '@/components/MessageToast.vue'
import { useMessage } from '@/utils/message'

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()

// 消息提示
const { showToast, toastMessage, toastType, toastDuration, showSuccess, showError, hideMessage } = useMessage()

const props = defineProps({
  noteId: {
    type: [Number, String],
    required: true
  },
  initialStats: {
    type: Object,
    default: null
  },
  initialTitle: {
    type: String,
    default: null
  }
})

const emit = defineEmits(['stats-updated'])

const loading = ref(true)
const contentLoading = ref(false)
const error = ref(null)
const noteDetail = ref(null)
// 初始化统计信息，优先使用从搜索结果传递过来的数据
const stats = ref(
  props.initialStats ? {
    authorName: props.initialStats.authorName || '未知作者',
    views: props.initialStats.views || 0,
    likes: props.initialStats.likes || 0,
    favorites: props.initialStats.favorites || 0,
    comments: props.initialStats.comments || 0
  } : {
    authorName: '',
    views: 0,
    likes: 0,
    favorites: 0,
    comments: 0
  }
)
const isLiked = ref(false)
const isFavorited = ref(false)
const actionLoading = ref({
  likes: false,
  favorites: false
})
const fileUrl = ref(null)
const markdownContent = ref('')

// 评论相关状态
const comments = ref([])
const commentsLoading = ref(false)
const newCommentContent = ref('')
const commentSubmitting = ref(false)
const replyingTo = ref(null)
const replyContent = ref('')
const replyToParentId = ref(null) // 回复的父评论ID
const replyToRemarkId = ref(null) // 回复的目标评论ID
const replyToUsername = ref(null) // 回复的目标用户名
const commentActionLoading = ref({})

// 配置markdown-it解析器
const mdParser = new MarkdownIt({
  breaks: true,
  html: true,
  linkify: true,
  typographer: true
})

const ACTION_KEY_PREFIX = 'note_stat_action'

const getCurrentUserId = () => {
  const storeId = userStore.userInfo?.id
  if (storeId) return storeId
  try {
    return JSON.parse(localStorage.getItem('userInfo') || '{}')?.id || null
  } catch (err) {
    return null
  }
}

const getActionStorageKey = (field) => {
  const userId = getCurrentUserId()
  const noteId = noteDetail.value?.noteId || props.noteId
  if (!userId || !noteId) return null
  return `${ACTION_KEY_PREFIX}:${field}:${noteId}:${userId}`
}

const restoreActionState = () => {
  const likeKey = getActionStorageKey('likes')
  const favoriteKey = getActionStorageKey('favorites')
  isLiked.value = likeKey ? localStorage.getItem(likeKey) === '1' : false
  isFavorited.value = favoriteKey ? localStorage.getItem(favoriteKey) === '1' : false
}

const persistActionState = (field, active) => {
  const key = getActionStorageKey(field)
  if (!key) return
  if (active) {
    localStorage.setItem(key, '1')
  } else {
    localStorage.removeItem(key)
  }
}

const setActionLoading = (field, value) => {
  actionLoading.value = {
    ...actionLoading.value,
    [field]: value
  }
}

const updateStatsFromResponse = (statsData) => {
  if (!statsData) return
  stats.value = {
    authorName: statsData.authorName || stats.value.authorName || '未知作者',
    views: statsData.views ?? stats.value.views ?? 0,
    likes: statsData.likes ?? stats.value.likes ?? 0,
    favorites: statsData.favorites ?? stats.value.favorites ?? 0,
    comments: statsData.comments ?? stats.value.comments ?? 0
  }
}

const handleToggleStat = async (field) => {
  if (!noteDetail.value?.noteId) return
  const userId = getCurrentUserId()
  if (!userId) {
    console.warn('用户未登录，无法进行操作')
    return
  }

  const flagRef = field === 'likes' ? isLiked : isFavorited
  if (actionLoading.value[field]) return

  setActionLoading(field, true)
  const delta = flagRef.value ? -1 : 1

  try {
    const updated = await changeNoteStat(noteDetail.value.noteId, userId, field, delta)
    updateStatsFromResponse(updated)
    flagRef.value = delta > 0
    persistActionState(field, flagRef.value)
    
    // 通知父组件统计信息已更新
    emit('stats-updated', {
      noteId: noteDetail.value.noteId,
      likes: stats.value.likes,
      favorites: stats.value.favorites,
      comments: stats.value.comments
    })
  } catch (err) {
    console.error('更新笔记统计失败:', err)
  } finally {
    setActionLoading(field, false)
  }
}

// 获取笔记详情和统计信息
const fetchNoteDetail = async () => {
  loading.value = true
  error.value = null
  isLiked.value = false
  isFavorited.value = false

  try {
    // 确保noteId是有效的数字
    const noteId = typeof props.noteId === 'string' ? Number(props.noteId) : props.noteId
    if (!noteId || isNaN(noteId) || noteId <= 0) {
      throw new Error('无效的笔记ID')
    }

    console.log('获取笔记详情，noteId:', noteId)

    // 优先使用从搜索结果传递过来的统计信息
    if (props.initialStats) {
      updateStatsFromResponse(props.initialStats)
    } else {
      // 如果没有传递统计信息，则从API获取
      try {
        const statsData = await getNoteStats(noteId)
        updateStatsFromResponse(statsData)
      } catch (statsError) {
        console.error('获取统计信息失败:', statsError)
        // 统计信息获取失败不影响主要内容显示
        stats.value = {
          authorName: '未知作者',
          views: 0,
          likes: 0,
          favorites: 0,
          comments: 0
        }
      }
    }

    // 获取笔记信息（包含fileType和url）
    let noteInfo
    try {
      noteInfo = await getFileUrlByNoteId(noteId)
      if (!noteInfo || !noteInfo.url) {
        throw new Error('无法获取笔记信息')
      }
    } catch (urlError) {
      console.error('获取笔记信息失败:', urlError)
      if (urlError.response) {
        console.error('响应数据:', urlError.response.data)
        console.error('响应状态:', urlError.response.status)
      }
      throw new Error(`无法获取笔记信息: ${urlError.message || '未知错误'}`)
    }

    // 从后端返回的数据中获取fileType和url
    const fileType = noteInfo.fileType || 'md'
    const urlString = noteInfo.url || ''

    // 只支持md和pdf格式，如果不是这两种格式，设置fileType为null，在模板中显示错误信息
    if (fileType !== 'md' && fileType !== 'pdf') {
      console.warn(`不支持的文件类型: ${fileType}，仅支持 md 或 pdf 格式`)
      noteDetail.value = {
        title: noteInfo.title || props.initialTitle || route.query.title || '无标题笔记',
        fileType: null, // 设置为null，模板中会显示不支持的文件类型提示
        noteId: noteId,
        createdAt: noteInfo.createdAt || null
      }
      restoreActionState()
      loading.value = false
      return // 提前返回，不加载文件内容，但保持标题、作者、点赞、收藏等功能正常
    }

    fileUrl.value = urlString

    // 构建笔记详情对象（优先使用后端返回的标题，然后是搜索结果传递的标题，最后是路由参数）
    noteDetail.value = {
      title: noteInfo.title || props.initialTitle || route.query.title || '无标题笔记',
      fileType: fileType,
      noteId: noteId,
      createdAt: noteInfo.createdAt || null
    }
    restoreActionState()

    // 加载评论列表
    await fetchComments()

    // 如果是Markdown文件，获取内容并转换为HTML
    if (noteDetail.value.fileType === 'md') {
      contentLoading.value = true
      try {
        const response = await fetch(fileUrl.value, {
          method: 'GET',
          cache: 'no-cache'
        })
        if (response.ok) {
          const text = await response.text()
          markdownContent.value = mdParser.render(text)
        } else {
          throw new Error('无法加载笔记内容')
        }
      } catch (err) {
        console.error('加载Markdown内容失败:', err)
        error.value = '无法加载笔记内容'
      } finally {
        contentLoading.value = false
      }
    }

    // 增加阅读量
    try {
      const userId = JSON.parse(localStorage.getItem('userInfo') || '{}')?.id
      if (userId) {
        // 调用增加阅读量的API（可选，如果后端支持）
        // await incrementNoteView(noteId, userId)
      }
    } catch (err) {
      console.error('增加阅读量失败:', err)
    }

  } catch (err) {
    console.error('获取笔记详情失败:', err)
    error.value = err.message || '加载笔记详情失败，请稍后重试'
  } finally {
    loading.value = false
  }
}

// 返回上一页
const goBack = () => {
  // 根据来源 tab 返回对应页面
  const fromTab = route.query.fromTab
  
  console.log('[NoteDetailView] 返回按钮点击，fromTab:', fromTab, '完整 query:', JSON.stringify(route.query))
  
  // 如果没有 fromTab，默认返回热榜
  if (!fromTab) {
    console.log('[NoteDetailView] 没有 fromTab，返回热榜')
    router.push({
      path: route.path,
      query: { tab: 'hot' }
    })
    return
  }
  
  // 如果来自搜索结果，返回到搜索结果页面并保留搜索参数
  if (fromTab === 'search') {
    const keyword = route.query.keyword
    const searchType = route.query.searchType || 'notes'
    
    console.log('[NoteDetailView] 返回搜索页面，keyword:', keyword, 'searchType:', searchType)
    
    // 构建新的查询参数，只保留必要的参数
    const newQuery = {
      tab: 'search'
    }
    
    // 保留搜索参数（必须存在才添加）
    if (keyword) {
      newQuery.keyword = keyword
    }
    if (searchType) {
      newQuery.searchType = searchType
    }
    
    // 直接更新路由，确保返回到搜索页面
    router.push({
      path: route.path,
      query: newQuery
    })
    return
  }
  
  // 其他来源（hot、recommend等），返回到对应的 tab，只保留 tab 参数
  console.log('[NoteDetailView] 返回到:', fromTab)
  router.push({
    path: route.path,
    query: { tab: fromTab }
  })
}

// 监听noteId变化
watch(() => props.noteId, () => {
  if (props.noteId) {
    fetchNoteDetail()
  }
})

// 监听initialStats变化，如果统计信息更新了，也要更新显示
watch(() => props.initialStats, (newStats) => {
  if (newStats) {
    stats.value = {
      authorName: newStats.authorName || '未知作者',
      views: newStats.views || 0,
      likes: newStats.likes || 0,
      favorites: newStats.favorites || 0,
      comments: newStats.comments || 0
    }
  }
}, { immediate: true, deep: true })

// 监听initialTitle变化，如果标题更新了，也要更新显示
watch(() => props.initialTitle, (newTitle) => {
  if (newTitle && noteDetail.value) {
    noteDetail.value.title = newTitle
  }
}, { immediate: true })

// 监听noteId变化，重新加载评论
watch(() => props.noteId, () => {
  if (props.noteId) {
    fetchComments()
  }
})

// 获取评论列表
const fetchComments = async () => {
  if (!noteDetail.value?.noteId) return
  
  const noteId = noteDetail.value.noteId
  const userId = getCurrentUserId()
  
  if (!userId) {
    console.warn('用户未登录，无法获取评论')
    return
  }

  commentsLoading.value = true
  try {
    const remarks = await getRemarksByNote(noteId, userId)
    console.log('获取到的评论数据:', remarks)
    // 调试：打印第一条评论的详细信息
    if (remarks && remarks.length > 0) {
      console.log('第一条评论详情:', JSON.stringify(remarks[0], null, 2))
    }
    comments.value = remarks || []
  } catch (err) {
    console.error('获取评论列表失败:', err)
    comments.value = []
  } finally {
    commentsLoading.value = false
  }
}

// 发表评论
const handleSubmitComment = async () => {
  if (!newCommentContent.value.trim() || !noteDetail.value?.noteId) return
  
  const userId = getCurrentUserId()
  const username = userStore.userInfo?.username || '匿名用户'
  
  if (!userId) {
    console.warn('用户未登录，无法发表评论')
    return
  }

  commentSubmitting.value = true
  try {
    const remarkData = {
      noteId: noteDetail.value.noteId,
      userId: userId,
      username: username,
      content: newCommentContent.value.trim(),
      parentId: null,
      isReply: false,
      replyToRemarkId: null,
      replyToUsername: null
    }
    
    await insertRemark(remarkData)
    newCommentContent.value = ''
    
    // 重新获取评论列表
    await fetchComments()
    
    // 更新评论数统计（创建评论时增加1）
    if (stats.value) {
      stats.value.comments = (stats.value.comments || 0) + 1
      // 通知父组件评论数量已更新
      emit('stats-updated', {
        noteId: noteDetail.value.noteId,
        comments: stats.value.comments
      })
    }
  } catch (err) {
    console.error('发表评论失败:', err)
    showError('发表评论失败，请稍后重试')
  } finally {
    commentSubmitting.value = false
  }
}

// 回复评论
const handleReply = (comment) => {
  replyingTo.value = comment._id
  replyToParentId.value = comment.parentId || comment._id
  replyToRemarkId.value = comment._id
  replyToUsername.value = comment.username
  replyContent.value = ''
}

// 回复子评论
const handleReplyToReply = (parentComment, reply) => {
  replyingTo.value = reply._id
  replyToParentId.value = reply._id  // 回复二级评论时，parentId应该是二级评论的ID
  replyToRemarkId.value = reply._id
  replyToUsername.value = reply.username
  replyContent.value = ''
}

// 提交回复
const handleSubmitReply = async (parentComment, targetReply = null) => {
  if (!replyContent.value.trim() || !noteDetail.value?.noteId) return
  
  const userId = getCurrentUserId()
  const username = userStore.userInfo?.username || '匿名用户'
  
  if (!userId) {
    console.warn('用户未登录，无法回复')
    return
  }

  commentSubmitting.value = true
  try {
    // 如果回复的是二级评论，使用二级评论的信息
    const replyTarget = targetReply || parentComment
    const finalReplyToUsername = replyToUsername.value || replyTarget.username
    const finalReplyToRemarkId = replyToRemarkId.value || replyTarget._id
    // 如果回复的是二级评论（targetReply存在），parentId应该是二级评论的ID；否则是主评论的ID
    const finalParentId = targetReply ? targetReply._id : (replyToParentId.value || parentComment._id)
    
    const remarkData = {
      noteId: noteDetail.value.noteId,
      userId: userId,
      username: username,
      content: replyContent.value.trim(),
      parentId: finalParentId,
      isReply: true,
      replyToRemarkId: finalReplyToRemarkId,
      replyToUsername: finalReplyToUsername
    }
    
    console.log('提交回复数据:', remarkData)
    
    await insertRemark(remarkData)
    cancelReply()
    
    // 重新获取评论列表
    await fetchComments()
    
    // 更新评论数统计（创建回复时增加1）
    if (stats.value) {
      stats.value.comments = (stats.value.comments || 0) + 1
      // 通知父组件评论数量已更新
      emit('stats-updated', {
        noteId: noteDetail.value.noteId,
        comments: stats.value.comments
      })
    }
  } catch (err) {
    console.error('回复评论失败:', err)
    showError('回复失败，请稍后重试')
  } finally {
    commentSubmitting.value = false
  }
}

// 取消回复
const cancelReply = () => {
  replyingTo.value = null
  replyToParentId.value = null
  replyToRemarkId.value = null
  replyToUsername.value = null
  replyContent.value = ''
}

// 组织回复，将回复二级评论的评论和二级评论分组
const organizeReplies = (replies, parentCommentId) => {
  if (!replies || replies.length === 0) return []
  
  // 找出所有二级评论（parentId指向主评论）
  const secondLevelReplies = replies.filter(reply => reply.parentId === parentCommentId)
  
  // 为每个二级评论找出它的回复（parentId指向该二级评论）
  return secondLevelReplies.map(mainReply => {
    const nestedReplies = replies.filter(reply => reply.parentId === mainReply._id)
    return {
      mainReply,
      nestedReplies
    }
  })
}

// 查找被回复的回复回复的是谁（用于多级回复显示）
// currentReply: 当前回复
// replies: 所有回复列表
// parentComment: 主评论
const findReplyTarget = (currentReply, replies, parentComment) => {
  if (!replies || !currentReply || !currentReply.replyToUsername) return null
  
  // 如果回复的是主评论，不需要显示多级
  if (currentReply.replyToUsername === parentComment?.username) {
    return null
  }
  
  // 找到被回复的那个回复（通过用户名匹配）
  const targetReply = replies.find(r => r.username === currentReply.replyToUsername)
  
  // 如果找到了，返回它回复的是谁
  if (targetReply && targetReply.replyToUsername) {
    return targetReply.replyToUsername
  }
  
  return null
}

// 点赞/取消点赞
const handleToggleLike = async (comment) => {
  const userId = getCurrentUserId()
  if (!userId) {
    console.warn('用户未登录，无法点赞')
    return
  }

  if (commentActionLoading.value[comment._id]) return

  commentActionLoading.value[comment._id] = true
  try {
    if (comment.LikedOrNot) {
      // 取消点赞
      await cancelLikeRemark(comment._id, userId)
      comment.LikedOrNot = false
      comment.likeCount = Math.max(0, (comment.likeCount || 0) - 1)
    } else {
      // 点赞
      await likeRemark(comment._id, userId)
      comment.LikedOrNot = true
      comment.likeCount = (comment.likeCount || 0) + 1
    }
  } catch (err) {
    console.error('点赞操作失败:', err)
    showError('操作失败，请稍后重试')
  } finally {
    commentActionLoading.value[comment._id] = false
  }
}

// 计算评论及其所有子评论的总数（递归计算）
const countCommentAndReplies = (comment) => {
  if (!comment) return 0
  let count = 1 // 评论本身
  
  // 如果有子评论（replies），递归计算
  if (comment.replies && Array.isArray(comment.replies)) {
    for (const reply of comment.replies) {
      count += countCommentAndReplies(reply)
    }
  }
  
  return count
}

// 删除评论
const handleDeleteComment = async (comment) => {
  if (!confirm('确定要删除这条评论吗？')) return

  const userId = getCurrentUserId()
  if (!userId) {
    console.warn('用户未登录，无法删除评论')
    return
  }

  if (commentActionLoading.value[comment._id]) return

  commentActionLoading.value[comment._id] = true
  try {
    // 计算要删除的评论数量（包括该评论及其所有子评论）
    const deleteCount = countCommentAndReplies(comment)
    
    await deleteRemark(comment._id)
    
    // 重新获取评论列表
    await fetchComments()
    
    // 更新评论数统计（减去被删除的评论及其所有子评论的数量）
    if (stats.value) {
      stats.value.comments = Math.max(0, (stats.value.comments || 0) - deleteCount)
      // 通知父组件评论数量已更新
      emit('stats-updated', {
        noteId: noteDetail.value.noteId,
        comments: stats.value.comments
      })
    }
  } catch (err) {
    console.error('删除评论失败:', err)
    showError('删除失败，请稍后重试')
  } finally {
    commentActionLoading.value[comment._id] = false
  }
}

onMounted(() => {
  if (props.noteId) {
    fetchNoteDetail()
  }
})
</script>

<style scoped>
:global(:root) {
  --brand-primary: #007FFF;
  --surface-base: #ffffff;
  --surface-muted: #f6f6f6;
  --line-soft: #e2e2e2;
  --text-strong: #111c17;
  --text-secondary: #666;
  --text-muted: #999;
}

.note-detail-page {
  min-height: 100vh;
  background: var(--surface-muted);
  padding: 20px;
  display: flex;
  justify-content: center;
}

.detail-layout {
  display: flex;
  align-items: flex-start;
  gap: 16px;
  width: 100%;
  max-width: 1200px;
}

.detail-container {
  max-width: 900px;
  margin: 0;
  background: var(--surface-base);
  border-radius: 8px;
  padding: 40px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  flex: 1;
}

/* 返回按钮区域 */
.back-header {
  margin-bottom: 20px;
}

.back-link-button {
  display: flex;
  align-items: center;
  gap: 6px;
  background: none;
  border: none;
  color: var(--brand-primary);
  font-size: 14px;
  cursor: pointer;
  padding: 8px 0;
  transition: color 0.2s;
}

.back-link-button:hover {
  color: #006EDC;
}

.back-icon {
  width: 16px;
  height: 16px;
}

/* 头部标题 */
.detail-header {
  margin-bottom: 24px;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--line-soft);
}

.note-title {
  margin: 0;
  font-size: 28px;
  font-weight: 600;
  color: var(--text-strong);
  line-height: 1.4;
}

/* 作者和统计信息 */
.note-meta-section {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 16px;
  margin-bottom: 32px;
  padding-bottom: 20px;
  border-bottom: 1px solid var(--line-soft);
}

.author-info {
  display: flex;
  align-items: center;
  gap: 8px;
}

.author-icon {
  width: 18px;
  height: 18px;
  color: var(--text-secondary);
}

.author-name {
  font-size: 15px;
  color: var(--text-secondary);
  font-weight: 500;
}

.create-time {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 14px;
  color: var(--text-muted);
  margin-left: 16px;
}

.time-icon {
  width: 14px;
  height: 14px;
  flex-shrink: 0;
}

.stats-info {
  display: flex;
  align-items: center;
  gap: 24px;
  flex-wrap: wrap;
}

.stat-item {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 14px;
  color: var(--text-muted);
}

.stat-icon {
  width: 16px;
  height: 16px;
  flex-shrink: 0;
}

/* 笔记内容区域 */
.note-content-section {
  margin-bottom: 40px;
  min-height: 200px;
}

.content-loading {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 60px 20px;
  gap: 16px;
}

.pdf-preview-container {
  width: 100%;
  display: flex;
  justify-content: center;
}

.pdf-viewer {
  max-width: 100%;
}

.markdown-content {
  width: 100%;
}

.markdown-body {
  font-size: 16px;
  line-height: 1.8;
  color: var(--text-strong);
  word-wrap: break-word;
}

.markdown-body :deep(h1),
.markdown-body :deep(h2),
.markdown-body :deep(h3),
.markdown-body :deep(h4),
.markdown-body :deep(h5),
.markdown-body :deep(h6) {
  margin-top: 24px;
  margin-bottom: 16px;
  font-weight: 600;
  line-height: 1.25;
}

.markdown-body :deep(h1) {
  font-size: 2em;
  border-bottom: 1px solid var(--line-soft);
  padding-bottom: 8px;
}

.markdown-body :deep(h2) {
  font-size: 1.5em;
  border-bottom: 1px solid var(--line-soft);
  padding-bottom: 8px;
}

.markdown-body :deep(p) {
  margin-bottom: 16px;
}

.markdown-body :deep(code) {
  background: var(--surface-muted);
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 0.9em;
}

.markdown-body :deep(pre) {
  background: var(--surface-muted);
  padding: 16px;
  border-radius: 6px;
  overflow-x: auto;
  margin-bottom: 16px;
}

.markdown-body :deep(pre code) {
  background: none;
  padding: 0;
}

.markdown-body :deep(blockquote) {
  border-left: 4px solid var(--brand-primary);
  padding-left: 16px;
  margin: 16px 0;
  color: var(--text-secondary);
}

.markdown-body :deep(ul),
.markdown-body :deep(ol) {
  margin-bottom: 16px;
  padding-left: 24px;
}

.markdown-body :deep(li) {
  margin-bottom: 8px;
}

.markdown-body :deep(img) {
  max-width: 100%;
  height: auto;
  border-radius: 6px;
  margin: 16px 0;
}

.markdown-body :deep(table) {
  width: 100%;
  border-collapse: collapse;
  margin-bottom: 16px;
}

.markdown-body :deep(th),
.markdown-body :deep(td) {
  border: 1px solid var(--line-soft);
  padding: 8px 12px;
  text-align: left;
}

.markdown-body :deep(th) {
  background: var(--surface-muted);
  font-weight: 600;
}

.no-content {
  text-align: center;
  padding: 60px 20px;
  color: var(--text-muted);
  font-size: 15px;
}

/* 评论区域 */
.comments-section {
  margin-top: 40px;
  padding-top: 32px;
  border-top: 1px solid var(--line-soft);
}

.comments-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 24px;
}

.comments-title {
  margin: 0;
  font-size: 20px;
  font-weight: 600;
  color: var(--text-strong);
}

.comments-count {
  font-size: 16px;
  color: var(--text-muted);
}

/* 评论表单 */
.comment-form {
  margin-bottom: 32px;
  padding: 20px;
  background: var(--surface-muted);
  border-radius: 8px;
  border: 1px solid var(--line-soft);
}

.comment-form-header {
  margin-bottom: 12px;
}

.comment-form-label {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-strong);
}

.comment-input {
  width: 100%;
  padding: 12px;
  border: 1px solid var(--line-soft);
  border-radius: 6px;
  font-size: 14px;
  font-family: inherit;
  resize: vertical;
  background: var(--surface-base);
  color: var(--text-strong);
  transition: border-color 0.2s;
}

.comment-input:focus {
  outline: none;
  border-color: var(--brand-primary);
}

.comment-input:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.comment-form-actions {
  margin-top: 12px;
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

.comment-submit-btn {
  padding: 8px 20px;
  background: var(--brand-primary);
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
}

.comment-submit-btn:hover:not(:disabled) {
  background: #006EDC;
}

.comment-submit-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.comment-submit-btn.small {
  padding: 6px 16px;
  font-size: 13px;
}

.comment-cancel-btn {
  padding: 6px 16px;
  background: transparent;
  color: var(--text-secondary);
  border: 1px solid var(--line-soft);
  border-radius: 6px;
  font-size: 13px;
  cursor: pointer;
  transition: border-color 0.2s, color 0.2s;
}

.comment-cancel-btn:hover {
  border-color: var(--text-secondary);
  color: var(--text-strong);
}

.comment-login-tip {
  padding: 20px;
  text-align: center;
  background: var(--surface-muted);
  border-radius: 8px;
  border: 1px dashed var(--line-soft);
  margin-bottom: 32px;
}

.comment-login-tip p {
  margin: 0;
  color: var(--text-muted);
  font-size: 14px;
}

/* 评论列表 */
.comments-loading {
  padding: 60px 20px;
  text-align: center;
}

.comments-empty {
  padding: 60px 20px;
  text-align: center;
  background: var(--surface-muted);
  border-radius: 8px;
  border: 1px dashed var(--line-soft);
}

.comments-empty p {
  margin: 0;
  color: var(--text-muted);
  font-size: 15px;
}

.comments-list {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.comment-item {
  padding: 16px;
  border-radius: 8px;
  background: var(--surface-base);
  border: 2px solid var(--line-soft);
  transition: all 0.2s;
  position: relative;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
}

/* 只在主评论（非回复项）上显示顶部绿线 */
.comment-item:not(.reply-item)::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 3px;
  background: linear-gradient(90deg, var(--brand-primary), rgba(0, 127, 255, 0.3));
  border-radius: 10px 10px 0 0;
  opacity: 0;
  transition: opacity 0.2s;
  z-index: 1;
}

.comment-item:hover {
  border-color: var(--brand-primary);
  box-shadow: 0 4px 12px rgba(0, 127, 255, 0.15);
  transform: translateY(-2px);
}

.comment-item:not(.reply-item):hover::before {
  opacity: 1;
}

.comment-item:last-child {
  margin-bottom: 0;
}

.comment-main {
  display: flex;
  flex-direction: column;
  gap: 12px;
  position: relative;
}

.comment-item .comment-main {
  padding: 4px 0;
}

.reply-item .comment-main {
  gap: 8px;
}

.comment-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.comment-author-info {
  display: flex;
  align-items: center;
  gap: 10px;
}

.comment-level-badge {
  padding: 2px 6px;
  border-radius: 10px;
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.3px;
  flex-shrink: 0;
}

.comment-level-badge.primary {
  background: linear-gradient(135deg, var(--brand-primary), #0056b3);
  color: white;
  box-shadow: 0 2px 4px rgba(0, 127, 255, 0.3);
}

.comment-level-badge.reply {
  background: linear-gradient(135deg, rgba(0, 127, 255, 0.15), rgba(0, 127, 255, 0.25));
  color: var(--brand-primary);
  border: 1px solid rgba(0, 127, 255, 0.3);
}

.comment-avatar {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  object-fit: cover;
  border: 2px solid var(--line-soft);
  flex-shrink: 0;
  background: var(--surface-muted);
  transition: border-color 0.2s, transform 0.2s;
}

.comment-item:hover .comment-avatar {
  border-color: var(--brand-primary);
  transform: scale(1.05);
}

.comment-author {
  font-weight: 600;
  font-size: 15px;
  color: var(--text-strong);
}

.comment-header-right {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.comment-time {
  font-size: 13px;
  color: var(--text-muted);
}

.reply-to-inline {
  font-size: 13px;
  color: var(--brand-primary);
}

.comment-content-wrapper {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.reply-to {
  font-size: 15px;
  color: var(--brand-primary);
  font-weight: 600;
  margin-right: 4px;
  padding: 2px 6px;
  background: rgba(0, 127, 255, 0.1);
  border-radius: 4px;
  display: inline-block;
}

.reply-item .reply-to {
  font-size: 13px;
  padding: 1px 4px;
}

.comment-content {
  margin: 0;
  font-size: 14px;
  line-height: 1.6;
  color: var(--text-secondary);
  word-wrap: break-word;
  display: inline;
  padding: 6px 0;
}

.comment-content span {
  display: inline;
}

.comment-actions {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.comment-action-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 4px 8px;
  background: transparent;
  border: none;
  color: var(--text-muted);
  font-size: 13px;
  cursor: pointer;
  border-radius: 4px;
  transition: background-color 0.2s, color 0.2s;
}

.comment-action-btn:hover:not(:disabled) {
  background: var(--surface-muted);
  color: var(--text-strong);
}

.comment-action-btn.active {
  color: var(--brand-primary);
}

.comment-action-btn.danger {
  color: var(--feedback-danger);
}

.comment-action-btn.danger:hover:not(:disabled) {
  background: rgba(220, 53, 69, 0.1);
}

.comment-action-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.action-icon-small {
  width: 14px;
  height: 14px;
}

/* 回复表单 */
.reply-form {
  margin-top: 16px;
  margin-left: 0;
  padding: 16px;
  background: var(--surface-muted);
  border-radius: 6px;
  border: 1px solid rgba(0, 127, 255, 0.2);
  border-left: 3px solid var(--brand-primary);
  position: relative;
  z-index: 10;
  animation: slideDown 0.2s ease-out;
}

@keyframes slideDown {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.reply-item .reply-form {
  margin-left: 0;
  background: rgba(0, 127, 255, 0.05);
  border-left: 3px solid var(--brand-primary);
  box-shadow: 0 2px 8px rgba(0, 127, 255, 0.1);
}

.reply-input {
  margin-bottom: 8px;
}

.reply-form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

/* 子评论容器 */
.comment-replies {
  margin-top: 16px;
  margin-left: 24px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

/* 二级评论包装器 - 包含二级评论和它的回复 */
.reply-wrapper {
  background: var(--surface-muted);
  border: 1px solid rgba(0, 127, 255, 0.2);
  border-radius: 8px;
  padding: 12px;
  position: relative;
  transition: all 0.2s;
}

.reply-wrapper:hover {
  border-color: var(--brand-primary);
  box-shadow: 0 2px 8px rgba(0, 127, 255, 0.1);
}

/* 二级评论 */
.reply-item {
  padding: 0;
  margin-bottom: 0;
  border-radius: 0;
  background: transparent;
  border: none;
  border-left: 3px solid var(--brand-primary);
  padding-left: 12px;
  position: relative;
  transition: all 0.2s;
  overflow: visible;
}

.reply-item:hover {
  background: transparent;
  border-color: var(--brand-primary);
  transform: none;
  box-shadow: none;
}

.reply-item .comment-avatar {
  width: 28px;
  height: 28px;
  border-width: 1.5px;
}

.reply-item .comment-author {
  font-size: 13px;
  font-weight: 500;
}

.reply-item .comment-content {
  font-size: 13px;
  line-height: 1.6;
}

.reply-item .comment-time {
  font-size: 11px;
}

.reply-item .comment-action-btn {
  font-size: 12px;
  padding: 3px 6px;
}

.reply-item .action-icon-small {
  width: 12px;
  height: 12px;
}

/* 三级评论（回复的回复） */
.nested-replies {
  margin-top: 10px;
  margin-left: 16px;
  padding-left: 12px;
  border-left: 2px solid rgba(0, 127, 255, 0.3);
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.nested-reply-item {
  padding: 8px 10px;
  background: rgba(0, 127, 255, 0.03);
  border-radius: 6px;
  border-left: 2px solid rgba(0, 127, 255, 0.4);
  position: relative;
}

.nested-reply-item::before {
  content: '↪';
  position: absolute;
  left: -14px;
  top: 10px;
  color: rgba(0, 127, 255, 0.5);
  font-size: 14px;
}

.nested-reply-item .comment-avatar {
  width: 24px;
  height: 24px;
  border-width: 1px;
}

.nested-reply-item .comment-author {
  font-size: 12px;
  font-weight: 500;
}

.nested-reply-item .comment-content {
  font-size: 12px;
  line-height: 1.5;
}

.nested-reply-item .comment-time {
  font-size: 10px;
}

.nested-reply-item .comment-action-btn {
  font-size: 11px;
  padding: 2px 5px;
}

.nested-reply-item .action-icon-small {
  width: 11px;
  height: 11px;
}

.comment-level-badge.nested {
  background: linear-gradient(135deg, rgba(0, 127, 255, 0.1), rgba(0, 127, 255, 0.2));
  color: var(--brand-primary);
  border: 1px solid rgba(0, 127, 255, 0.25);
  font-size: 10px;
  padding: 1px 6px;
}


.action-sidebar {
  position: sticky;
  top: 80px;
  display: flex;
  flex-direction: column;
  gap: 12px;
  width: 150px;
}

.action-button {
  display: flex;
  align-items: center;
  gap: 10px;
  width: 100%;
  padding: 12px 14px;
  border-radius: 12px;
  border: 1px solid var(--line-soft);
  background: var(--surface-base);
  color: var(--text-strong);
  cursor: pointer;
  transition: background-color 0.2s, border-color 0.2s, transform 0.1s;
}

.action-button:hover {
  border-color: var(--brand-primary);
  transform: translateY(-1px);
}

.action-button:disabled {
  opacity: 0.65;
  cursor: not-allowed;
  transform: none;
}

.action-button.active {
  background: var(--brand-primary);
  color: white;
  border-color: var(--brand-primary);
}

.action-icon {
  width: 18px;
  height: 18px;
  flex-shrink: 0;
}

.action-text {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 2px;
  line-height: 1.2;
}

.action-label {
  font-size: 14px;
}

.action-count {
  font-size: 13px;
  color: var(--brand-primary);
}

.action-button.active .action-count,
.action-button.active .action-label {
  color: white;
}

.action-button.active .action-icon {
  color: white;
}

@media (max-width: 1024px) {
  .detail-layout {
    flex-direction: column;
  }

  .action-sidebar {
    position: static;
    width: 100%;
    flex-direction: row;
  }

  .action-button {
    flex: 1;
    justify-content: center;
  }

  .action-text {
    align-items: center;
  }
}

/* 加载和错误状态 */
.loading-state,
.error-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  min-height: 400px;
  gap: 16px;
}

.loader {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  border: 4px solid var(--line-soft);
  border-top-color: var(--brand-primary);
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.error-state p {
  color: var(--text-secondary);
  font-size: 16px;
}

.back-button {
  padding: 10px 20px;
  background: var(--brand-primary);
  color: white;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  cursor: pointer;
  transition: background-color 0.2s;
}

.back-button:hover {
  background: #006EDC;
}

@media (max-width: 768px) {
  .note-detail-page {
    padding: 12px;
  }

  .detail-container {
    padding: 24px 20px;
  }

  .note-title {
    font-size: 22px;
  }

  .note-meta-section {
    flex-direction: column;
    align-items: flex-start;
  }

  .stats-info {
    width: 100%;
    justify-content: space-between;
  }

  .stat-item {
    font-size: 13px;
  }

  .comment-avatar {
    width: 32px;
    height: 32px;
  }

  .comment-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 8px;
  }

  .comment-header-right {
    width: 100%;
    justify-content: space-between;
  }

  .comment-replies {
    margin-left: 16px;
    padding-left: 16px;
  }

  .reply-item::before {
    left: -20px;
    font-size: 16px;
  }

  .comment-level-badge {
    font-size: 10px;
    padding: 1px 6px;
  }
}
</style>

