<template>
  <div class="qa-management-view">
    <div class="view-header">
      <h2 class="page-title">问答管理</h2>
      <div class="header-actions">
        <div class="stat-item">
          <span class="stat-label">问题总数：</span>
          <span class="stat-value">{{ questionCount }}</span>
        </div>
        <div class="stat-item">
          <span class="stat-label">回答总数：</span>
          <span class="stat-value">{{ answerCount }}</span>
        </div>
        <button class="refresh-btn" @click="loadQuestions">刷新</button>
      </div>
    </div>

    <!-- 搜索框 -->
    <div class="search-container">
      <div class="search-box">
        <img src="/assets/icons/icon-search.svg" alt="搜索" class="search-icon" />
        <input
          v-model="searchQuery"
          type="text"
          class="search-input"
          placeholder="搜索问题标题、内容..."
          @keyup.enter="handleSearch"
          @input="handleSearchInput"
        />
        <button v-if="searchQuery" class="clear-btn" @click="clearSearch" title="清除搜索">
          <svg viewBox="0 0 16 16" fill="currentColor">
            <path d="M2.146 2.854a.5.5 0 1 1 .708-.708L8 7.293l5.146-5.147a.5.5 0 0 1 .708.708L8.707 8l5.147 5.146a.5.5 0 0 1-.708.708L8 8.707l-5.146 5.147a.5.5 0 0 1-.708-.708L7.293 8 2.146 2.854Z"/>
          </svg>
        </button>
        <button class="search-btn" @click="handleSearch">搜索</button>
      </div>
      <div v-if="isSearching" class="search-info">
        <span>搜索关键词：<strong>{{ searchQuery }}</strong></span>
        <span v-if="searchResults.length > 0">找到 <strong>{{ searchResults.length }}</strong> 条结果</span>
      </div>
    </div>

    <!-- 问题列表 -->
    <div class="questions-list">
      <div v-if="loading" class="loading-state">
        <div class="loader"></div>
        <p>加载中...</p>
      </div>
      <div v-else-if="paginatedQuestions.length === 0" class="empty-state">
        {{ isSearching ? '未找到匹配的问题' : '暂无问题' }}
      </div>
      <div v-else v-for="question in paginatedQuestions" :key="question.questionId" class="question-card">
        <!-- 问题头部 -->
        <div class="question-header" @click="toggleQuestion(question.questionId)">
          <div class="question-header-left">
            <button class="expand-btn" :class="{ expanded: expandedQuestions.has(question.questionId) }">
              <svg viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 4a.5.5 0 0 1 .5.5v3h3a.5.5 0 0 1 0 1h-3v3a.5.5 0 0 1-1 0v-3h-3a.5.5 0 0 1 0-1h3v-3A.5.5 0 0 1 8 4z"/>
              </svg>
            </button>
            <div class="question-info">
              <h3 class="question-title">
                <span v-html="highlightKeyword(question.title || '-')"></span>
                <span v-if="question.answerCount > 0" class="replied-badge">replied</span>
              </h3>
              <div class="question-meta">
                <span class="meta-item">ID: {{ question.questionId }}</span>
                <span class="meta-item">提问者: {{ question.authorName || '未知' }}</span>
                <span class="meta-item">回答数: {{ question.answerCount || 0 }}</span>
                <span class="meta-item">点赞: {{ question.likeCount || 0 }}</span>
                <span class="meta-item">收藏: {{ question.favoriteCount || 0 }}</span>
                <span class="meta-item">时间: {{ formatTime(question.createdAt) }}</span>
              </div>
            </div>
          </div>
          <div class="question-actions">
            <button class="action-btn view-btn" @click.stop="viewQuestion(question)">查看</button>
            <button class="action-btn delete-btn" @click.stop="handleDeleteQuestion(question)">删除</button>
          </div>
        </div>

        <!-- 展开的回答树 -->
        <div v-if="expandedQuestions.has(question.questionId)" class="answers-tree">
          <div v-if="!question.answers || question.answers.length === 0" class="no-answers">
            暂无回答
          </div>
          <div v-else v-for="answer in question.answers" :key="answer.answerId" class="answer-item">
            <!-- 回答 -->
            <div class="answer-header">
              <div class="answer-info">
                <div class="answer-author">
                  <span class="answer-author-name">{{ answer.authorName || '未知用户' }}</span>
                  <span class="answer-time">{{ formatTime(answer.createdAt) }}</span>
                </div>
                <div class="answer-content">{{ answer.content }}</div>
                <div class="answer-meta">
                  <span>点赞: {{ answer.likeCount || 0 }}</span>
                  <span>评论: {{ answer.comments?.length || 0 }}</span>
                </div>
              </div>
              <div class="answer-actions">
                <button class="action-btn-small delete-btn" @click="handleDeleteAnswer(question.questionId, answer)">删除回答</button>
              </div>
            </div>

            <!-- 评论列表 -->
            <div v-if="answer.comments && answer.comments.length > 0" class="comments-section">
              <div v-for="comment in answer.comments" :key="comment.commentId" class="comment-item">
                <!-- 评论 -->
                <div class="comment-header">
                  <div class="comment-info">
                    <span class="comment-author">{{ comment.authorName || '未知用户' }}</span>
                    <span class="comment-time">{{ formatTime(comment.createdAt) }}</span>
                  </div>
                  <div class="comment-content">{{ comment.content }}</div>
                  <div class="comment-meta">
                    <span>点赞: {{ comment.likeCount || 0 }}</span>
                    <span>回复: {{ comment.replies?.length || 0 }}</span>
                  </div>
                </div>
                <div class="comment-actions">
                  <button class="action-btn-small delete-btn" @click="handleDeleteComment(question.questionId, answer.answerId, comment)">删除评论</button>
                </div>

                <!-- 回复列表 -->
                <div v-if="comment.replies && comment.replies.length > 0" class="replies-section">
                  <div v-for="reply in comment.replies" :key="reply.replyId" class="reply-item">
                    <div class="reply-header">
                      <span class="reply-author">{{ reply.authorName || '未知用户' }}</span>
                      <span class="reply-time">{{ formatTime(reply.createdAt) }}</span>
                    </div>
                    <div class="reply-content">{{ reply.content }}</div>
                    <div class="reply-meta">
                      <span>点赞: {{ reply.likeCount || 0 }}</span>
                    </div>
                    <div class="reply-actions">
                      <button class="action-btn-small delete-btn" @click="handleDeleteReply(question.questionId, answer.answerId, comment.commentId, reply)">删除回复</button>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 分页组件 -->
    <div class="pagination-container" v-if="!loading && (isSearching ? searchResults.length : questions.length) > 0">
      <div class="pagination-info">
        显示第 {{ (currentPage - 1) * pageSize + 1 }} - {{ Math.min(currentPage * pageSize, isSearching ? searchResults.length : questions.length) }} 条，共 {{ isSearching ? searchResults.length : questions.length }} 条
      </div>
      <div class="pagination">
        <button 
          class="page-btn" 
          :disabled="currentPage === 1" 
          @click="goToPage(currentPage - 1)"
        >
          上一页
        </button>
        <div class="page-numbers">
          <button
            v-for="page in visiblePages"
            :key="page"
            class="page-number"
            :class="{ active: page === currentPage }"
            @click="goToPage(page)"
            :disabled="page === '...'"
          >
            {{ page }}
          </button>
        </div>
        <button 
          class="page-btn" 
          :disabled="currentPage === totalPages" 
          @click="goToPage(currentPage + 1)"
        >
          下一页
        </button>
      </div>
    </div>

    <!-- 消息提示组件 -->
    <MessageToast
      v-if="showToast"
      :message="toastMessage"
      :type="toastType"
      :duration="toastDuration"
      :auto-close="toastType !== 'confirm'"
      :show-close="toastType !== 'confirm'"
      @close="hideMessage"
      @confirm="handleConfirm"
      @cancel="handleCancel"
    />
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { getQuestionCount, getQuestionList, adminDeleteQuestion, getAnswerCount } from '../../api/admin'
import { searchQuestions, deleteAnswer, deleteComment, deleteReply } from '../../api/qa'
import MessageToast from '../MessageToast.vue'
import { useMessage } from '../../utils/message'

const questionCount = ref(0)
const answerCount = ref(0)
const questions = ref([])
const searchResults = ref([])
const loading = ref(false)
const currentPage = ref(1)
const pageSize = 30
const searchQuery = ref('')
const isSearching = ref(false)
const searchTimeout = ref(null)
const expandedQuestions = ref(new Set())

// 消息提示
const {
  showToast,
  toastMessage,
  toastType,
  toastDuration,
  showSuccess,
  showError,
  showConfirm,
  handleConfirm: handleConfirmCallback,
  handleCancel: handleCancelCallback,
  hideMessage
} = useMessage()

// 计算总页数
const totalPages = computed(() => {
  const dataSource = isSearching.value ? searchResults.value : questions.value
  return Math.ceil(dataSource.length / pageSize)
})

// 计算当前页显示的数据
const paginatedQuestions = computed(() => {
  const dataSource = isSearching.value ? searchResults.value : questions.value
  const start = (currentPage.value - 1) * pageSize
  const end = start + pageSize
  return dataSource.slice(start, end)
})

// 计算可见的页码
const visiblePages = computed(() => {
  const pages = []
  const total = totalPages.value
  const current = currentPage.value
  
  if (total <= 7) {
    for (let i = 1; i <= total; i++) {
      pages.push(i)
    }
  } else {
    if (current <= 4) {
      for (let i = 1; i <= 5; i++) {
        pages.push(i)
      }
      pages.push('...')
      pages.push(total)
    } else if (current >= total - 3) {
      pages.push(1)
      pages.push('...')
      for (let i = total - 4; i <= total; i++) {
        pages.push(i)
      }
    } else {
      pages.push(1)
      pages.push('...')
      for (let i = current - 1; i <= current + 1; i++) {
        pages.push(i)
      }
      pages.push('...')
      pages.push(total)
    }
  }
  
  return pages
})

const loadQuestions = async (preservePage = false) => {
  loading.value = true
  try {
    const [questionCountRes, answerCountRes, listRes] = await Promise.all([
      getQuestionCount(),
      getAnswerCount(),
      getQuestionList()
    ])
    
    questionCount.value = questionCountRes?.data?.questionCount || questionCountRes?.questionCount || 0
    answerCount.value = answerCountRes?.data?.answerCount || answerCountRes?.answerCount || 0
    questions.value = listRes?.data || listRes || []
    
    if (!preservePage) {
      currentPage.value = 1
    } else {
      const newTotalPages = Math.ceil(questions.value.length / pageSize)
      if (currentPage.value > newTotalPages && newTotalPages > 0) {
        currentPage.value = newTotalPages
      }
    }
  } catch (error) {
    console.error('加载问题列表失败:', error)
    questions.value = []
  } finally {
    loading.value = false
  }
}

const formatTime = (timeStr) => {
  if (!timeStr) return '-'
  try {
    return new Date(timeStr).toLocaleString('zh-CN')
  } catch {
    return timeStr
  }
}

const toggleQuestion = (questionId) => {
  if (expandedQuestions.value.has(questionId)) {
    expandedQuestions.value.delete(questionId)
  } else {
    expandedQuestions.value.add(questionId)
  }
}

const viewQuestion = (question) => {
  const questionId = question.questionId
  if (questionId) {
    window.open(`/main?tab=qa-detail&questionId=${questionId}`, '_blank')
  }
}

const handleDeleteQuestion = async (question) => {
  const questionId = question.questionId
  if (!questionId) {
    showError('问题ID不存在')
    return
  }
  
  const questionTitle = question.title || '该问题'
  const answerCount = question.answerCount || 0
  const confirmMessage = `确定要删除问题"${questionTitle}"吗？\n\n` +
    `此问题下有 ${answerCount} 个回答，删除时将一并删除所有回答、评论和回复。\n\n` +
    `此操作不可恢复！`
  
  try {
    const confirmed = await showConfirm(confirmMessage)
    if (!confirmed) {
      return
    }
    
    await adminDeleteQuestion(questionId)
    showSuccess('删除成功')
    await loadQuestions(true)
  } catch (error) {
    console.error('删除问题失败:', error)
    showError('删除失败：' + (error.response?.data?.message || error.message || '未知错误'))
  }
}

const handleConfirm = () => {
  handleConfirmCallback()
}

const handleCancel = () => {
  handleCancelCallback()
}

const handleDeleteAnswer = async (questionId, answer) => {
  const answerId = answer.answerId
  const commentCount = answer.comments?.length || 0
  
  const confirmMessage = `确定要删除这条回答吗？\n\n` +
    `回答ID: ${answerId}\n` +
    `作者: ${answer.authorName || '未知'}\n` +
    `内容: ${(answer.content || '').substring(0, 50)}${(answer.content || '').length > 50 ? '...' : ''}\n` +
    (commentCount > 0 ? `\n⚠️ 注意：此回答下有 ${commentCount} 条评论，删除时将一并删除！\n` : '') +
    `\n删除后无法恢复！`
  
  try {
    const confirmed = await showConfirm(confirmMessage)
    if (!confirmed) {
      return
    }
    
    await deleteAnswer(questionId, answerId)
    showSuccess(commentCount > 0 ? `删除成功！已删除回答及其 ${commentCount} 条评论。` : '删除成功！')
    await loadQuestions(true)
  } catch (error) {
    console.error('删除回答失败:', error)
    showError('删除失败：' + (error.response?.data?.message || error.message || '未知错误'))
  }
}

const handleDeleteComment = async (questionId, answerId, comment) => {
  const commentId = comment.commentId
  const replyCount = comment.replies?.length || 0
  
  const confirmMessage = `确定要删除这条评论吗？\n\n` +
    `评论ID: ${commentId}\n` +
    `作者: ${comment.authorName || '未知'}\n` +
    `内容: ${(comment.content || '').substring(0, 50)}${(comment.content || '').length > 50 ? '...' : ''}\n` +
    (replyCount > 0 ? `\n⚠️ 注意：此评论下有 ${replyCount} 条回复，删除时将一并删除！\n` : '') +
    `\n删除后无法恢复！`
  
  try {
    const confirmed = await showConfirm(confirmMessage)
    if (!confirmed) {
      return
    }
    
    await deleteComment(questionId, answerId, commentId)
    showSuccess(replyCount > 0 ? `删除成功！已删除评论及其 ${replyCount} 条回复。` : '删除成功！')
    await loadQuestions(true)
  } catch (error) {
    console.error('删除评论失败:', error)
    showError('删除失败：' + (error.response?.data?.message || error.message || '未知错误'))
  }
}

const handleDeleteReply = async (questionId, answerId, commentId, reply) => {
  const replyId = reply.replyId
  
  const confirmMessage = `确定要删除这条回复吗？\n\n` +
    `回复ID: ${replyId}\n` +
    `作者: ${reply.authorName || '未知'}\n` +
    `内容: ${(reply.content || '').substring(0, 50)}${(reply.content || '').length > 50 ? '...' : ''}\n` +
    `\n删除后无法恢复！`
  
  try {
    const confirmed = await showConfirm(confirmMessage)
    if (!confirmed) {
      return
    }
    
    await deleteReply(questionId, answerId, commentId, replyId)
    showSuccess('删除成功！')
    await loadQuestions(true)
  } catch (error) {
    console.error('删除回复失败:', error)
    showError('删除失败：' + (error.response?.data?.message || error.message || '未知错误'))
  }
}

const goToPage = (page) => {
  if (page >= 1 && page <= totalPages.value && page !== currentPage.value && page !== '...') {
    currentPage.value = page
    const questionsList = document.querySelector('.questions-list')
    if (questionsList) {
      questionsList.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }
}

// 高亮关键词
const highlightKeyword = (text) => {
  if (!text || !searchQuery.value.trim() || !isSearching.value) return text
  const keyword = searchQuery.value.trim()
  const regex = new RegExp(`(${keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi')
  return String(text).replace(regex, '<mark class="highlight">$1</mark>')
}

// 处理搜索输入（防抖）
const handleSearchInput = () => {
  if (searchTimeout.value) {
    clearTimeout(searchTimeout.value)
  }
  
  if (!searchQuery.value.trim()) {
    clearSearch()
    return
  }
  
  searchTimeout.value = setTimeout(() => {
    handleSearch()
  }, 500)
}

// 执行搜索
const handleSearch = async () => {
  const keyword = searchQuery.value.trim()
  
  if (!keyword) {
    clearSearch()
    return
  }
  
  loading.value = true
  isSearching.value = true
  currentPage.value = 1
  
  try {
    const results = await searchQuestions(keyword, 0)
    searchResults.value = results?.data || results || []
  } catch (error) {
    console.error('搜索问题失败:', error)
    searchResults.value = []
  } finally {
    loading.value = false
  }
}

// 清除搜索
const clearSearch = () => {
  searchQuery.value = ''
  isSearching.value = false
  searchResults.value = []
  currentPage.value = 1
  if (searchTimeout.value) {
    clearTimeout(searchTimeout.value)
    searchTimeout.value = null
  }
}

let refreshTimer = null

onMounted(() => {
  loadQuestions()
  refreshTimer = setInterval(() => {
    loadQuestions(true)
  }, 30000)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
})
</script>

<style scoped>
.qa-management-view {
  max-width: 1400px;
  margin: 0 auto;
}

.view-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
  flex-wrap: wrap;
  gap: 16px;
}

.page-title {
  font-size: 24px;
  font-weight: 600;
  color: #333;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 16px;
}

.stat-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.stat-label {
  font-size: 14px;
  color: #666;
}

.stat-value {
  font-size: 16px;
  font-weight: 600;
  color: var(--brand-primary);
}

.refresh-btn {
  padding: 8px 16px;
  border: 1px solid #ddd;
  background: white;
  color: #666;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
}

.refresh-btn:hover {
  background: #f5f5f5;
  border-color: #ccc;
}

.search-container {
  margin-bottom: 20px;
  background: white;
  padding: 20px;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

.search-box {
  display: flex;
  align-items: center;
  gap: 12px;
  position: relative;
}

.search-icon {
  position: absolute;
  left: 12px;
  width: 18px;
  height: 18px;
  color: #999;
  pointer-events: none;
  z-index: 1;
  object-fit: contain;
}

.search-input {
  flex: 1;
  padding: 10px 40px 10px 40px;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-size: 14px;
  transition: all 0.2s;
}

.search-input:focus {
  outline: none;
  border-color: var(--brand-primary);
  box-shadow: 0 0 0 3px rgba(34, 191, 163, 0.1);
}

.clear-btn {
  position: absolute;
  right: 80px;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: transparent;
  border: none;
  cursor: pointer;
  color: #999;
  padding: 0;
  transition: color 0.2s;
}

.clear-btn:hover {
  color: #333;
}

.clear-btn svg {
  width: 16px;
  height: 16px;
}

.search-btn {
  padding: 10px 20px;
  border: 1px solid var(--brand-primary);
  background: var(--brand-primary);
  color: white;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
  white-space: nowrap;
}

.search-btn:hover {
  background: var(--brand-primary-hover);
  border-color: var(--brand-primary-hover);
}

.search-info {
  margin-top: 12px;
  display: flex;
  align-items: center;
  gap: 16px;
  font-size: 13px;
  color: #666;
}

.search-info strong {
  color: var(--brand-primary);
  font-weight: 600;
}

.questions-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.loading-state,
.empty-state {
  text-align: center;
  padding: 60px 20px;
  color: #999;
  background: white;
  border-radius: 8px;
}

.loader {
  border: 3px solid #f3f3f3;
  border-top: 3px solid var(--brand-primary);
  border-radius: 50%;
  width: 40px;
  height: 40px;
  animation: spin 1s linear infinite;
  margin: 0 auto 16px;
}

@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

.question-card {
  background: white;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
  overflow: hidden;
}

.question-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  cursor: pointer;
  transition: background 0.2s;
  border-bottom: 1px solid #f0f0f0;
}

.question-header:hover {
  background: #f8f9fa;
}

.question-header-left {
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 1;
}

.expand-btn {
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: none;
  background: transparent;
  cursor: pointer;
  color: #666;
  transition: transform 0.2s;
  flex-shrink: 0;
}

.expand-btn.expanded {
  transform: rotate(45deg);
}

.expand-btn svg {
  width: 16px;
  height: 16px;
}

.question-info {
  flex: 1;
  min-width: 0;
}

.question-title {
  font-size: 16px;
  font-weight: 600;
  color: #333;
  margin: 0 0 8px 0;
  display: flex;
  align-items: center;
  gap: 8px;
}

.question-title :deep(.highlight) {
  background: #fff3cd;
  padding: 2px 4px;
  border-radius: 3px;
  font-weight: 600;
  color: #007FFF;
}

.replied-badge {
  display: inline-block;
  padding: 2px 8px;
  background: #22bfa3;
  color: white;
  border-radius: 12px;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.question-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  font-size: 13px;
  color: #666;
}

.meta-item {
  white-space: nowrap;
}

.question-actions {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
}

.action-btn {
  padding: 6px 12px;
  border: 1px solid #007FFF;
  background: white;
  color: #007FFF;
  border-radius: 4px;
  cursor: pointer;
  font-size: 13px;
  transition: all 0.2s;
}

.action-btn:hover {
  background: #007FFF;
  color: white;
}

.action-btn.view-btn {
  border-color: var(--brand-primary);
  color: var(--brand-primary);
}

.action-btn.view-btn:hover {
  background: var(--brand-primary);
  border-color: var(--brand-primary);
  color: white;
}

.action-btn.delete-btn {
  border-color: #dc3545;
  color: #dc3545;
}

.action-btn.delete-btn:hover {
  background: #dc3545;
  color: white;
}

.action-btn-small {
  padding: 4px 8px;
  border: 1px solid #dc3545;
  background: white;
  color: #dc3545;
  border-radius: 4px;
  cursor: pointer;
  font-size: 12px;
  transition: all 0.2s;
}

.action-btn-small.delete-btn:hover {
  background: #dc3545;
  color: white;
}

.answers-tree {
  padding: 16px 20px;
  background: #f8f9fa;
  border-top: 1px solid #e9ecef;
}

.no-answers {
  text-align: center;
  padding: 20px;
  color: #999;
  font-size: 14px;
}

.answer-item {
  background: white;
  border-radius: 6px;
  padding: 16px;
  margin-bottom: 16px;
  border: 1px solid #e9ecef;
}

.answer-item:last-child {
  margin-bottom: 0;
}

.answer-header {
  display: flex;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 12px;
}

.answer-info {
  flex: 1;
}

.answer-author {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}

.answer-author-name {
  font-weight: 600;
  color: #333;
  font-size: 14px;
}

.answer-time {
  font-size: 12px;
  color: #999;
}

.answer-content {
  color: #333;
  line-height: 1.6;
  margin-bottom: 8px;
  font-size: 14px;
}

.answer-meta {
  display: flex;
  gap: 16px;
  font-size: 12px;
  color: #666;
}

.answer-actions {
  flex-shrink: 0;
}

.comments-section {
  margin-top: 16px;
  padding-left: 20px;
  border-left: 3px solid #007FFF;
}

.comment-item {
  background: #f8f9fa;
  border-radius: 6px;
  padding: 12px;
  margin-bottom: 12px;
}

.comment-item:last-child {
  margin-bottom: 0;
}

.comment-header {
  margin-bottom: 8px;
}

.comment-info {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 6px;
}

.comment-author {
  font-weight: 500;
  color: #333;
  font-size: 13px;
}

.comment-time {
  font-size: 11px;
  color: #999;
}

.comment-content {
  color: #333;
  line-height: 1.5;
  font-size: 13px;
  margin-bottom: 6px;
}

.comment-meta {
  display: flex;
  gap: 12px;
  font-size: 11px;
  color: #666;
}

.comment-actions {
  margin-top: 8px;
}

.replies-section {
  margin-top: 12px;
  padding-left: 16px;
  border-left: 2px solid #22bfa3;
}

.reply-item {
  background: white;
  border-radius: 4px;
  padding: 10px;
  margin-bottom: 8px;
  border: 1px solid #e9ecef;
}

.reply-item:last-child {
  margin-bottom: 0;
}

.reply-header {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 6px;
}

.reply-author {
  font-weight: 500;
  color: #333;
  font-size: 12px;
}

.reply-time {
  font-size: 10px;
  color: #999;
}

.reply-content {
  color: #333;
  line-height: 1.5;
  font-size: 12px;
  margin-bottom: 6px;
}

.reply-meta {
  font-size: 10px;
  color: #666;
  margin-bottom: 6px;
}

.reply-actions {
  margin-top: 6px;
}

.pagination-container {
  margin-top: 24px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 16px;
}

.pagination-info {
  font-size: 14px;
  color: #666;
}

.pagination {
  display: flex;
  align-items: center;
  gap: 8px;
}

.page-btn {
  padding: 8px 16px;
  border: 1px solid var(--brand-primary);
  background: white;
  color: var(--brand-primary);
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
  outline: none;
}

.page-btn:hover:not(:disabled) {
  background: var(--surface-soft);
  border-color: var(--brand-primary-hover);
  color: var(--brand-primary-hover);
}

.page-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.page-numbers {
  display: flex;
  gap: 4px;
}

.page-number {
  min-width: 36px;
  height: 36px;
  padding: 0 8px;
  border: 1px solid var(--brand-primary);
  background: white;
  color: var(--brand-primary);
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
  outline: none;
}

.page-number:hover:not(.active):not(:disabled) {
  background: var(--surface-soft);
  border-color: var(--brand-primary-hover);
  color: var(--brand-primary-hover);
}

.page-number.active {
  background: var(--brand-primary);
  border-color: var(--brand-primary);
  color: white;
  font-weight: 600;
}

.page-number:disabled {
  cursor: default;
  border: none;
  background: transparent;
}

@media (max-width: 640px) {
  .pagination-container {
    flex-direction: column;
    align-items: stretch;
  }
  
  .pagination-info {
    text-align: center;
  }
  
  .pagination {
    justify-content: center;
  }
  
  .question-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }
  
  .question-actions {
    width: 100%;
    justify-content: flex-end;
  }
}
</style>
