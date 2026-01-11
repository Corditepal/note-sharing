<template>
  <div class="comments-page">
    <section class="comments-hero">
      <div>
        <p class="section-label">评论管理</p>
        <div class="hero-title">
          <h2>我的评论</h2>
          <span>Comments</span>
        </div>
        <p class="hero-desc">
          分别查看「我的笔记收到的评论」与「我发送的评论」，支持删除操作并保持样式与交互一致。
        </p>
      </div>
      <div class="hero-meta">
        <div>
          <p>收到评论</p>
          <strong>{{ receivedComments.length }}</strong>
        </div>
        <div>
          <p>已发送</p>
          <strong>{{ sentComments.length }}</strong>
        </div>
      </div>
    </section>

    <div class="comments-layout">
      <section class="comment-panel">
        <header>
          <div>
            <p class="section-label">Received</p>
            <h3>我的笔记中的评论</h3>
          </div>
          <button class="ghost-btn" type="button">导出</button>
        </header>
        <div v-if="loading" class="comments-loading">
          <div class="loader"></div>
          <p>加载中...</p>
        </div>
        <ul v-else-if="receivedComments.length > 0">
          <li v-for="comment in receivedComments" :key="comment._id" class="comment-card">
            <div class="comment-bullet" :data-status="comment.status || '已读'"></div>
            <div class="comment-body">
              <div class="comment-head">
                <strong>{{ comment.noteTitle || '笔记 #' + comment.noteId }}</strong>
                <span>{{ formatTime(comment.createdAt) }}</span>
              </div>
              <div class="comment-author-section">
                <img 
                  :src="'/assets/avatars/avatar.png'" 
                  :alt="comment.username"
                  class="comment-avatar"
                />
                <span class="comment-author-name">{{ comment.username || '匿名用户' }}</span>
              </div>
              <p class="comment-content">{{ comment.content }}</p>
              <div class="comment-meta">
                <span v-if="comment.likeCount !== undefined">点赞：{{ comment.likeCount }}</span>
              </div>
              <div class="comment-actions">
                <button type="button" class="text-link danger" @click="handleDelete('received', comment._id)">删除</button>
              </div>
            </div>
          </li>
        </ul>
        <div v-else class="comments-empty">
          <p>暂无收到的评论</p>
        </div>
      </section>

      <section class="comment-panel">
        <header>
          <div>
            <p class="section-label">Sent</p>
            <h3>我发送的评论</h3>
          </div>
          <button class="ghost-btn" type="button">批量删除</button>
        </header>
        <div v-if="loading" class="comments-loading">
          <div class="loader"></div>
          <p>加载中...</p>
        </div>
        <ul v-else-if="sentComments.length > 0">
          <li v-for="comment in sentComments" :key="comment._id" class="comment-card">
            <div class="comment-bullet" data-status="sent"></div>
            <div class="comment-body">
              <div class="comment-head">
                <strong>目标笔记：{{ comment.noteTitle || '笔记 #' + comment.noteId }}</strong>
                <span>{{ formatTime(comment.createdAt) }}</span>
              </div>
              <div class="comment-author-section">
                <img 
                  :src="'/assets/avatars/avatar.png'" 
                  :alt="comment.username"
                  class="comment-avatar"
                />
                <span class="comment-author-name">{{ comment.username || '匿名用户' }}</span>
              </div>
              <p class="comment-content">
                <span v-if="comment.replyToUsername" class="reply-to">
                  回复 @{{ comment.replyToUsername }}：
                </span>
                <span>{{ comment.content }}</span>
              </p>
              <div class="comment-meta">
                <span v-if="comment.likeCount !== undefined">点赞：{{ comment.likeCount }}</span>
                <span v-if="comment.isReceive">类型：回复</span>
              </div>
              <div class="comment-actions">
                <button type="button" class="text-link danger" @click="handleDelete('sent', comment._id)">删除评论</button>
              </div>
            </div>
          </li>
        </ul>
        <div v-else class="comments-empty">
          <p>暂无发送的评论</p>
        </div>
      </section>
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
import { ref, onMounted } from 'vue'
import { deleteRemark } from '@/api/remark'
import { useUserStore } from '@/stores/user'
import { formatTime } from '@/utils/time'
import MessageToast from '@/components/MessageToast.vue'
import { useMessage } from '@/utils/message'

const userStore = useUserStore()

// 消息提示
const { showToast, toastMessage, toastType, toastDuration, showSuccess, showError, hideMessage } = useMessage()

/**
 * 注意：后端目前没有直接获取用户评论列表的接口
 * 需要后端提供以下接口：
 * - GET /api/v1/remark/user/received - 获取我的笔记收到的评论
 * - GET /api/v1/remark/user/sent - 获取我发送的评论
 * 
 * 当前实现：使用模拟数据展示，删除功能已实现
 */

const receivedComments = ref([])
const sentComments = ref([])
const loading = ref(false)

// 获取当前用户ID
const getCurrentUserId = () => {
  const storeId = userStore.userInfo?.id
  if (storeId) return storeId
  try {
    return JSON.parse(localStorage.getItem('userInfo') || '{}')?.id || null
  } catch (err) {
    return null
  }
}

// 加载评论数据（模拟数据，实际需要后端接口）
const loadComments = async () => {
  loading.value = true
  try {
    // TODO: 调用后端接口获取评论列表
    // const userId = getCurrentUserId()
    // if (!userId) return
    
    // 模拟数据 - 实际应该从后端获取
    // const received = await getReceivedComments(userId)
    // const sent = await getSentComments(userId)
    
    // 暂时使用空数组，等待后端接口
    receivedComments.value = []
    sentComments.value = []
  } catch (err) {
    console.error('加载评论失败:', err)
  } finally {
    loading.value = false
  }
}

// 删除评论
const handleDelete = async (type, commentId) => {
  if (!confirm('确定要删除这条评论吗？')) return

  try {
    await deleteRemark(commentId)
    
    // 从列表中移除
    if (type === 'received') {
      receivedComments.value = receivedComments.value.filter((item) => item._id !== commentId)
    } else {
      sentComments.value = sentComments.value.filter((item) => item._id !== commentId)
    }
    
    showSuccess('删除成功')
  } catch (err) {
    console.error('删除评论失败:', err)
    showError('删除失败，请稍后重试')
  }
}

onMounted(() => {
  loadComments()
})
</script>

<style scoped>
.comments-page {
  display: flex;
  flex-direction: column;
  gap: 28px;
  padding: clamp(16px, 3vw, 36px);
}

.comments-hero {
  background: var(--surface-base);
  border-radius: var(--radius-lg);
  border: 1px solid var(--line-soft);
  padding: 32px clamp(20px, 4vw, 48px);
  box-shadow: var(--shadow-card);
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 24px;
  flex-wrap: wrap;
}

.hero-title {
  display: flex;
  align-items: center;
  gap: 16px;
  margin: 8px 0;
}

.hero-title span {
  letter-spacing: 0.4em;
  color: var(--text-muted);
  text-transform: uppercase;
  font-size: 13px;
}

.hero-meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 12px;
}

.hero-meta div {
  border-radius: var(--radius-md);
  border: 1px solid var(--line-soft);
  background: var(--surface-soft);
  padding: 16px 18px;
}

.hero-meta p {
  font-size: 12px;
  letter-spacing: 0.2em;
  color: var(--text-muted);
  margin-bottom: 6px;
}

.hero-meta strong {
  font-size: 24px;
}

.comments-layout {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 24px;
}

.comment-panel {
  background: var(--surface-base);
  border-radius: var(--radius-lg);
  border: 1px solid var(--line-soft);
  padding: 28px;
  box-shadow: var(--shadow-soft);
}

.comment-panel header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 18px;
  gap: 12px;
}

.comment-panel h3 {
  margin-top: 6px;
  font-size: 20px;
}

.comment-panel ul {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.comment-card {
  display: grid;
  grid-template-columns: 12px 1fr;
  gap: 16px;
  padding: 16px 0;
  border-bottom: 1px solid var(--line-soft);
}

.comment-card:last-child {
  border-bottom: none;
}

.comment-bullet {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  margin-top: 8px;
  background: var(--brand-primary);
  position: relative;
}

.comment-bullet::after {
  content: '';
  position: absolute;
  left: 50%;
  top: 12px;
  width: 1px;
  height: calc(100% - 12px);
  background: var(--line-soft);
  transform: translateX(-50%);
}

.comment-card:last-child .comment-bullet::after {
  display: none;
}

.comment-bullet[data-status='未读'] {
  background: var(--feedback-warning);
}

.comment-body {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.comment-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.comment-head span {
  font-size: 13px;
  color: var(--text-muted);
}

.comment-author-section {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
}

.comment-avatar {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  object-fit: cover;
  border: 2px solid var(--line-soft);
  flex-shrink: 0;
  background: var(--surface-muted);
  transition: border-color 0.2s, transform 0.2s;
}

.comment-avatar:hover {
  border-color: var(--brand-primary);
  transform: scale(1.05);
}

.comment-author-name {
  font-weight: 600;
  font-size: 14px;
  color: var(--text-strong);
}

.comment-content {
  font-size: 15px;
  color: var(--text-secondary);
  line-height: 1.7;
  display: inline;
}

.comment-content span {
  display: inline;
}

.reply-to {
  font-size: 15px;
  color: var(--brand-primary);
  font-weight: 600;
  margin-right: 2px;
}

.comment-meta {
  font-size: 13px;
  color: var(--text-muted);
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
}

.comment-actions {
  display: flex;
  gap: 16px;
}

.ghost-btn {
  border-radius: 999px;
  border: 1px solid var(--line-soft);
  padding: 8px 18px;
  background: transparent;
  color: var(--text-secondary);
}

.ghost-btn:hover {
  color: var(--brand-primary);
  border-color: var(--brand-primary);
}

.text-link {
  border: none;
  background: none;
  color: var(--brand-secondary);
  font-weight: 600;
  padding: 0;
}

.text-link.danger {
  color: var(--feedback-danger);
}

.text-link:hover {
  color: var(--brand-primary);
}

.comments-loading {
  padding: 60px 20px;
  text-align: center;
}

.loader {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  border: 4px solid var(--line-soft);
  border-top-color: var(--brand-primary);
  animation: spin 1s linear infinite;
  margin: 0 auto 16px;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.comments-empty {
  padding: 40px 20px;
  text-align: center;
  color: var(--text-muted);
  font-size: 14px;
}

@media (max-width: 640px) {
  .comments-hero,
  .comment-panel {
    border-radius: 24px;
    padding: 24px;
  }

  .comment-head {
    flex-direction: column;
    align-items: flex-start;
  }

  .comment-avatar {
    width: 28px;
    height: 28px;
  }

  .comment-author-name {
    font-size: 13px;
  }
}
</style>

