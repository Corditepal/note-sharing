<template>
  <div class="favorites-page">
    <section class="favorites-panel">
      <!-- 标签页切换按钮 -->
      <div class="tab-buttons">
        <button
          v-for="tab in tabs"
          :key="tab.value"
          class="tab-button"
          :class="{ active: activeTab === tab.value }"
          @click="switchTab(tab.value)"
        >
          {{ tab.label }}
        </button>
      </div>

      <!-- 加载状态 -->
      <div v-if="loading" class="state-card">
        <span class="loader" aria-hidden="true"></span>
        <p>加载中...</p>
        <small>正在获取收藏内容</small>
      </div>

      <!-- 错误状态 -->
      <div v-else-if="error" class="state-card error">
        <p>{{ error }}</p>
        <button class="retry-button" type="button" @click="fetchFavorites">重试</button>
      </div>

      <!-- 内容列表 -->
      <div v-else-if="displayList.length > 0" class="results-list">
        <div class="results-header">
          <p class="results-count">
            找到 <strong>{{ displayList.length }}</strong> 条{{ getTabLabel() }}
          </p>
        </div>
        <article
          v-for="item in displayList"
          :key="item.id"
          class="result-card"
        >
          <div class="result-content" @click="handleItemClick(item)">
            <h3 class="result-title">{{ item.title }}</h3>
            <p class="result-summary">{{ item.content || item.contentSummary || '暂无内容' }}</p>
            <div class="result-meta">
              <div class="meta-left">
                <span class="meta-author">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 8a3 3 0 100-6 3 3 0 000 6zm2-3a2 2 0 11-4 0 2 2 0 014 0zm4 8c0 1-1 1-1 1H3s-1 0-1-1 1-4 6-4 6 3 6 4zm-1-.004c-.001-.246-.154-.986-.832-1.664C11.516 10.68 10.289 10 8 10c-2.29 0-3.516.68-4.168 1.332-.678.678-.83 1.418-.832 1.664h10z"/>
                  </svg>
                  {{ item.authorName || `用户 #${item.authorId}` || '未知作者' }}
                </span>
                <span class="meta-time">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 3.5a.5.5 0 00-1 0V9a.5.5 0 00.252.434l3.5 2a.5.5 0 00.496-.868L8 8.71V3.5z"/>
                    <path d="M8 16A8 8 0 108 0a8 8 0 000 16zm7-8A7 7 0 111 8a7 7 0 0114 0z"/>
                  </svg>
                  {{ getDisplayTime(item) }}
                </span>
              </div>
              <div class="meta-right">
                <span v-if="item.type === 'note'" class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 4a.5.5 0 01.5.5v3h3a.5.5 0 010 1h-3v3a.5.5 0 01-1 0v-3h-3a.5.5 0 010-1h3v-3A.5.5 0 018 4z"/>
                  </svg>
                  {{ item.viewCount || 0 }} 阅读
                </span>
                <span v-if="item.type === 'note'" class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 15A7 7 0 118 1a7 7 0 010 14zm0 1A8 8 0 108 0a8 8 0 000 16z"/>
                    <path d="M8 4a.5.5 0 00-.5.5v3h-3a.5.5 0 000 1h3v3a.5.5 0 001 0v-3h3a.5.5 0 000-1h-3v-3A.5.5 0 008 4z"/>
                  </svg>
                  {{ item.likeCount || 0 }} 点赞
                </span>
                <span v-if="item.type === 'note'" class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M2 2v13.5a.5.5 0 00.74.439L8 13.069l5.26 2.87A.5.5 0 0014 15.5V2a2 2 0 00-2-2H4a2 2 0 00-2 2z"/>
                  </svg>
                  {{ item.favoriteCount || 0 }} 收藏
                </span>
                <span v-if="item.type === 'note'" class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M2.5 1A1.5 1.5 0 001 2.5v11A1.5 1.5 0 002.5 15h6.086a1.5 1.5 0 001.06-.44l4.915-4.914A1.5 1.5 0 0015 7.586V2.5A1.5 1.5 0 0013.5 1h-11zM2 2.5a.5.5 0 01.5-.5h11a.5.5 0 01.5.5v7.086a.5.5 0 01-.146.353l-4.915 4.915a.5.5 0 01-.353.146H2.5a.5.5 0 01-.5-.5v-11z"/>
                    <path d="M5.5 6a.5.5 0 000 1h5a.5.5 0 000-1h-5zM5 8.5a.5.5 0 01.5-.5h5a.5.5 0 010 1h-5a.5.5 0 01-.5-.5zm0 2a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5z"/>
                  </svg>
                  {{ item.commentCount || 0 }} 评论
                </span>
                <span v-if="item.type === 'question'" class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 4a.5.5 0 01.5.5v3h3a.5.5 0 010 1h-3v3a.5.5 0 01-1 0v-3h-3a.5.5 0 010-1h3v-3A.5.5 0 018 4z"/>
                  </svg>
                  {{ item.likeCount || 0 }} 赞同
                </span>
                <span v-if="item.type === 'question'" class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M2.5 1A1.5 1.5 0 001 2.5v11A1.5 1.5 0 002.5 15h6.086a1.5 1.5 0 001.06-.44l4.915-4.914A1.5 1.5 0 0015 7.586V2.5A1.5 1.5 0 0013.5 1h-11zM2 2.5a.5.5 0 01.5-.5h11a.5.5 0 01.5.5v7.086a.5.5 0 01-.146.353l-4.915 4.915a.5.5 0 01-.353.146H2.5a.5.5 0 01-.5-.5v-11z"/>
                    <path d="M5.5 6a.5.5 0 000 1h5a.5.5 0 000-1h-5zM5 8.5a.5.5 0 01.5-.5h5a.5.5 0 010 1h-5a.5.5 0 01-.5-.5zm0 2a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5z"/>
                  </svg>
                  {{ item.answerCount || 0 }} 回答
                </span>
                <span v-if="item.tags && item.tags.length" class="meta-tags">
                  <span v-for="tag in item.tags" :key="tag" class="tag-chip">#{{ tag }}</span>
                </span>
              </div>
            </div>
          </div>
          <!-- 取消收藏按钮 -->
          <button
            class="delete-btn"
            @click.stop="handleRemoveFavorite(item)"
            title="取消收藏"
          >
            <svg class="delete-icon" viewBox="0 0 16 16" fill="currentColor">
              <path d="M2 2v13.5a.5.5 0 00.74.439L8 13.069l5.26 2.87A.5.5 0 0014 15.5V2a2 2 0 00-2-2H4a2 2 0 00-2 2z"/>
            </svg>
          </button>
        </article>
      </div>

      <!-- 空状态 -->
      <div v-else class="state-card">
        <p>暂无{{ getTabLabel() }}</p>
        <small>{{ getEmptyHint() }}</small>
      </div>
    </section>

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
import { computed, ref, onMounted, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user'
import { getFavoriteNotes } from '@/api/note'
import { getFavoriteQuestions, favoriteQuestion } from '@/api/qa'
import { changeNoteStat } from '@/api/note'
import { useMessage } from '@/utils/message'
import { formatTime } from '@/utils/time'
import MessageToast from '@/components/MessageToast.vue'

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()

// 消息提示
const { showToast, toastMessage, toastType, toastDuration, showSuccess, showError, hideMessage } = useMessage()

// 标签页配置
const tabs = [
  { label: '收藏笔记', value: 'notes' },
  { label: '收藏问答', value: 'questions' }
]

const activeTab = ref('notes')
const loading = ref(false)
const error = ref('')
const favoriteNotes = ref([])
const favoriteQuestions = ref([])

// 切换标签页
const switchTab = (tab) => {
  if (activeTab.value === tab) return
  activeTab.value = tab
  fetchFavorites()
}

// 获取收藏列表
const fetchFavorites = async () => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    error.value = '请先登录'
    return
  }

  loading.value = true
  error.value = ''

  try {
    if (activeTab.value === 'notes') {
      const notes = await getFavoriteNotes(userId)
      favoriteNotes.value = Array.isArray(notes) ? notes : []
    } else {
      const questions = await getFavoriteQuestions(userId)
      favoriteQuestions.value = Array.isArray(questions) ? questions : []
    }
  } catch (err) {
    console.error('获取收藏列表失败:', err)
    const backendMsg = err.response?.data?.message || err.response?.data?.error
    error.value = backendMsg || err.message || '获取收藏列表失败，请稍后重试'
    // 如果 API 不存在，使用空数组（开发阶段）
    if (activeTab.value === 'notes') {
      favoriteNotes.value = []
    } else {
      favoriteQuestions.value = []
    }
  } finally {
    loading.value = false
  }
}

// 根据当前标签页筛选显示列表
const displayList = computed(() => {
  const userId = userStore.userInfo?.id
  if (!userId) return []

  switch (activeTab.value) {
    case 'notes':
      // 收藏笔记
      return favoriteNotes.value.map(note => ({
        id: note.noteId,
        type: 'note',
        noteId: note.noteId,
        title: note.title || '无标题',
        contentSummary: note.contentSummary,
        authorName: note.authorName,
        authorId: note.authorId,
        viewCount: note.viewCount || 0,
        likeCount: note.likeCount || 0,
        favoriteCount: note.favoriteCount || 0,
        commentCount: note.commentCount || 0,
        updatedAt: note.updatedAt || note.createdAt
      }))

    case 'questions':
      // 收藏的问题
      return favoriteQuestions.value.map(question => ({
        id: question.questionId,
        type: 'question',
        questionId: question.questionId,
        title: question.title || '无标题',
        content: question.content,
        authorName: question.authorName || `用户 #${question.authorId}`,
        authorId: question.authorId,
        likeCount: question.likeCount || 0,
        favoriteCount: question.favoriteCount || 0,
        answerCount: question.answerCount || (question.answers?.length || 0),
        tags: question.tags || [],
        createdAt: question.createdAt
      }))

    default:
      return []
  }
})

// 获取标签页名称
const getTabLabel = () => {
  const tab = tabs.find(t => t.value === activeTab.value)
  return tab ? tab.label : '内容'
}

// 获取空状态提示
const getEmptyHint = () => {
  switch (activeTab.value) {
    case 'notes':
      return '去热榜页面收藏一些笔记吧'
    case 'questions':
      return '去问答页面收藏一些问题吧'
    default:
      return ''
  }
}

// 获取显示时间
const getDisplayTime = (item) => {
  if (!item) return '时间未知'
  const time = item.updatedAt || item.createdAt
  if (time) {
    return formatTime(time) || '时间未知'
  }
  return '时间未知'
}

// 处理点击
const handleItemClick = (item) => {
  if (item.type === 'note') {
    router.push({
      path: route.path,
      query: {
        ...route.query,
        tab: 'note-detail',
        noteId: item.noteId,
        title: item.title,
        fromTab: 'favorites'
      }
    })
  } else if (item.type === 'question') {
    router.push({
      path: route.path,
      query: {
        ...route.query,
        tab: 'qa-detail',
        questionId: item.questionId
      }
    })
  }
}

// 取消收藏
const handleRemoveFavorite = async (item) => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录')
    return
  }

  try {
    if (item.type === 'note') {
      await changeNoteStat(item.noteId, userId, 'favorites', -1)
      favoriteNotes.value = favoriteNotes.value.filter(note => note.noteId !== item.noteId)
    } else {
      await favoriteQuestion(userId, item.questionId)
      favoriteQuestions.value = favoriteQuestions.value.filter(q => q.questionId !== item.questionId)
    }
    showSuccess('已取消收藏')
  } catch (err) {
    console.error('取消收藏失败:', err)
    showError('取消收藏失败')
  }
}

// 监听标签页切换
watch(activeTab, () => {
  fetchFavorites()
})

// 组件挂载时获取收藏列表
onMounted(() => {
  fetchFavorites()
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

.favorites-page {
  min-height: 100vh;
  padding: 20px 24px 100px;
  background: transparent;
}

.favorites-panel {
  width: min(1200px, 100%);
  margin: 0 auto;
  background: var(--surface-base);
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  padding: 32px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  min-height: 400px;
}

.tab-buttons {
  display: flex;
  gap: 8px;
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--line-soft);
}

.tab-button {
  padding: 8px 16px;
  border: none;
  background: transparent;
  color: var(--text-secondary);
  font-size: 14px;
  cursor: pointer;
  border-radius: 6px;
  transition: all 0.2s;
}

.tab-button:hover {
  background: var(--surface-muted);
  color: var(--text-strong);
}

.tab-button.active {
  background: var(--brand-primary);
  color: #fff;
  font-weight: 600;
}

.results-header {
  margin-bottom: 20px;
  padding-bottom: 12px;
  border-bottom: 1px solid var(--line-soft);
}

.results-count {
  margin: 0;
  font-size: 14px;
  color: var(--text-secondary);
}

.results-count strong {
  color: var(--brand-primary);
  font-weight: 600;
}

.results-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.result-card {
  padding: 20px;
  border-radius: 8px;
  border: 1px solid var(--line-soft);
  background: var(--surface-base);
  transition: border-color 0.2s, box-shadow 0.2s;
  cursor: pointer;
  position: relative;
}

.result-card:hover {
  border-color: var(--brand-primary);
  box-shadow: 0 2px 8px rgba(0, 127, 255, 0.1);
}

.result-content {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.result-title {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
  color: var(--text-strong);
  line-height: 1.5;
}

.result-summary {
  margin: 0;
  font-size: 14px;
  color: var(--text-secondary);
  line-height: 1.6;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.result-meta {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px;
  margin-top: 8px;
}

.meta-left,
.meta-right {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.meta-author,
.meta-time,
.meta-stat {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 13px;
  color: var(--text-muted);
}

.meta-icon {
  width: 14px;
  height: 14px;
  flex-shrink: 0;
}

.meta-tags {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
}

.tag-chip {
  background: #eef2ff;
  color: #4338ca;
  padding: 4px 8px;
  border-radius: 6px;
  font-size: 12px;
}

.delete-btn {
  position: absolute;
  top: 16px;
  right: 16px;
  background: transparent;
  border: none;
  padding: 6px;
  cursor: pointer;
  color: var(--text-muted);
  border-radius: 4px;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 10;
}

.delete-btn:hover {
  background: rgba(220, 53, 69, 0.1);
  color: #dc3545;
}

.delete-icon {
  width: 16px;
  height: 16px;
}

.state-card {
  border-radius: 8px;
  border: 1px dashed var(--line-soft);
  padding: 60px 24px;
  text-align: center;
  color: var(--text-secondary);
  display: flex;
  flex-direction: column;
  gap: 10px;
  align-items: center;
}

.state-card.error {
  color: #ff4d4f;
}

.state-card p {
  margin: 0;
  font-size: 16px;
  color: var(--text-strong);
}

.state-card small {
  color: var(--text-muted);
  font-size: 13px;
}

.loader {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: 3px solid var(--line-soft);
  border-top-color: var(--brand-primary);
  animation: spin 1s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.retry-button {
  margin-top: 12px;
  padding: 8px 16px;
  border: 1px solid var(--line-soft);
  border-radius: 6px;
  background: var(--surface-base);
  color: var(--text-strong);
  cursor: pointer;
  transition: all 0.2s;
}

.retry-button:hover {
  border-color: #007FFF;
  color: #007FFF;
}

@media (max-width: 768px) {
  .favorites-page {
    padding: 16px;
  }

  .favorites-panel {
    padding: 20px;
  }

  .result-meta {
    flex-direction: column;
    align-items: flex-start;
  }

  .meta-left,
  .meta-right {
    width: 100%;
    justify-content: flex-start;
  }
}
</style>
