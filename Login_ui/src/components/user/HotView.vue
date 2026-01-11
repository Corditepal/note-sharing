<template>
  <div class="hot-page">
    <section class="hot-panel">
      <header class="panel-header">
        <div class="headline">
          <h2 class="panel-title">今日热榜</h2>
          <p class="panel-subtitle">基于阅读、点赞、收藏等综合热度</p>
        </div>
        <button
          class="refresh-button"
          type="button"
          :disabled="loading"
          @click="fetchHotList"
        >
          <span v-if="loading">刷新中...</span>
          <span v-else>刷新</span>
        </button>
      </header>

      <div v-if="loading" class="state-card">
        <span class="loader" aria-hidden="true"></span>
        <p>加载热榜中...</p>
        <small>正在获取最热门的笔记</small>
      </div>

      <div v-else-if="error" class="state-card error">
        <p>{{ error }}</p>
        <button class="retry-button" type="button" @click="fetchHotList">重试</button>
      </div>

      <div v-else-if="hotList.length > 0" class="results-list">
        <article
          v-for="(item, index) in hotList"
          :key="item.noteId"
          class="result-card"
          @click="handleResultClick(item)"
        >
          <div class="rank-badge" :class="{ top: index < 3 }">
            {{ index + 1 }}
          </div>
          <div class="result-content">
            <h3 class="result-title">{{ item.title || '无标题' }}</h3>
            <p class="result-summary">{{ item.contentSummary || item.title || '暂无摘要' }}</p>
            <div class="result-meta">
              <div class="meta-left">
                <span class="meta-author">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 8a3 3 0 100-6 3 3 0 000 6zm2-3a2 2 0 11-4 0 2 2 0 014 0zm4 8c0 1-1 1-1 1H3s-1 0-1-1 1-4 6-4 6 3 6 4zm-1-.004c-.001-.246-.154-.986-.832-1.664C11.516 10.68 10.289 10 8 10c-2.29 0-3.516.68-4.168 1.332-.678.678-.83 1.418-.832 1.664h10z"/>
                  </svg>
                  {{ item.authorName || '未知作者' }}
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
                <span class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 4a.5.5 0 01.5.5v3h3a.5.5 0 010 1h-3v3a.5.5 0 01-1 0v-3h-3a.5.5 0 010-1h3v-3A.5.5 0 018 4z"/>
                  </svg>
                  {{ item.viewCount || 0 }} 阅读
                </span>
                <span class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 15A7 7 0 118 1a7 7 0 010 14zm0 1A8 8 0 108 0a8 8 0 000 16z"/>
                    <path d="M8 4a.5.5 0 00-.5.5v3h-3a.5.5 0 000 1h3v3a.5.5 0 001 0v-3h3a.5.5 0 000-1h-3v-3A.5.5 0 008 4z"/>
                  </svg>
                  {{ item.likeCount || 0 }} 点赞
                </span>
                <span class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M2 2v13.5a.5.5 0 00.74.439L8 13.069l5.26 2.87A.5.5 0 0014 15.5V2a2 2 0 00-2-2H4a2 2 0 00-2 2z"/>
                  </svg>
                  {{ item.favoriteCount || 0 }} 收藏
                </span>
                <span class="meta-stat">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M2.5 1A1.5 1.5 0 001 2.5v11A1.5 1.5 0 002.5 15h6.086a1.5 1.5 0 001.06-.44l4.915-4.914A1.5 1.5 0 0015 7.586V2.5A1.5 1.5 0 0013.5 1h-11zM2 2.5a.5.5 0 01.5-.5h11a.5.5 0 01.5.5v7.086a.5.5 0 01-.146.353l-4.915 4.915a.5.5 0 01-.353.146H2.5a.5.5 0 01-.5-.5v-11z"/>
                    <path d="M5.5 6a.5.5 0 000 1h5a.5.5 0 000-1h-5zM5 8.5a.5.5 0 01.5-.5h5a.5.5 0 010 1h-5a.5.5 0 01-.5-.5zm0 2a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5z"/>
                  </svg>
                  {{ item.commentCount || 0 }} 评论
                </span>
              </div>
            </div>
          </div>
        </article>
      </div>

      <div v-else class="state-card placeholder">
        <p>暂时没有热榜数据</p>
        <small>稍后再来看看，或者刷新试试</small>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { getHotNotes, changeNoteStat, getFileUrlByNoteId } from '@/api/note'
import { useUserStore } from '@/stores/user'
import { formatTime } from '@/utils/time'

const emit = defineEmits(['open-note-detail'])

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()

const hotList = ref([])
const loading = ref(false)
const error = ref('')
const VIEW_CACHE_PREFIX = 'note_view_ts'

// 更新列表中指定笔记的评论数量
const updateCommentCount = (noteId, commentCount) => {
  const item = hotList.value.find(item => item.noteId === noteId)
  if (item) {
    item.commentCount = commentCount
  }
}

// 更新列表中指定笔记的点赞数量
const updateLikeCount = (noteId, likeCount) => {
  const item = hotList.value.find(item => item.noteId === noteId)
  if (item) {
    item.likeCount = likeCount
  }
}

// 更新列表中指定笔记的收藏数量
const updateFavoriteCount = (noteId, favoriteCount) => {
  const item = hotList.value.find(item => item.noteId === noteId)
  if (item) {
    item.favoriteCount = favoriteCount
  }
}

// 暴露方法供父组件调用
defineExpose({
  updateCommentCount,
  updateLikeCount,
  updateFavoriteCount
})

const getViewCacheKey = (noteId, userId) => {
  if (!noteId || !userId) return null
  return `${VIEW_CACHE_PREFIX}:${noteId}:${userId}`
}

const canIncreaseView = (noteId, userId) => {
  const key = getViewCacheKey(noteId, userId)
  if (!key) return false
  try {
    const ts = Number(localStorage.getItem(key) || 0)
    if (!ts) return true
    return Date.now() - ts >= 5 * 60 * 1000
  } catch (err) {
    console.warn('读取本地阅读缓存失败:', err)
    return true
  }
}

const markViewIncreased = (noteId, userId) => {
  const key = getViewCacheKey(noteId, userId)
  if (!key) return
  try {
    localStorage.setItem(key, String(Date.now()))
  } catch (err) {
    console.warn('写入本地阅读缓存失败:', err)
  }
}

// 获取显示时间
const getDisplayTime = (item) => {
  if (!item) return '时间未知'
  // 优先使用已获取的时间字段
  const time = item.updatedAt || item.updated_at || item.createdAt || item.created_at
  if (time) {
    return formatTime(time) || '时间未知'
  }
  // 如果时间字段为空，返回加载中状态（会在fetchNoteTimes中异步更新）
  return item._timeLoading ? '加载中...' : '时间未知'
}

// 批量获取笔记时间信息
const fetchNoteTimes = async (items) => {
  if (!items || items.length === 0) return
  
  // 找出没有时间的笔记
  const notesWithoutTime = items.filter(item => 
    item.noteId && 
    !item.updatedAt && 
    !item.updated_at && 
    !item.createdAt && 
    !item.created_at &&
    !item._timeLoading
  )
  
  if (notesWithoutTime.length === 0) return
  
  // 标记为加载中
  notesWithoutTime.forEach(item => { item._timeLoading = true })
  
  // 批量获取时间信息
  const promises = notesWithoutTime.map(async (item) => {
    try {
      const noteInfo = await getFileUrlByNoteId(item.noteId)
      if (noteInfo) {
        // 使用updatedAt，如果没有则使用createdAt
        item.updatedAt = noteInfo.updatedAt || noteInfo.createdAt
        item.createdAt = noteInfo.createdAt
      }
    } catch (err) {
      console.warn(`获取笔记 ${item.noteId} 时间失败:`, err)
    } finally {
      item._timeLoading = false
    }
  })
  
  await Promise.all(promises)
}

const fetchHotList = async () => {
  loading.value = true
  error.value = ''
  try {
    const data = await getHotNotes()
    hotList.value = Array.isArray(data) ? data : []
    
    // 批量获取没有时间的笔记的时间信息
    if (hotList.value.length > 0) {
      await fetchNoteTimes(hotList.value)
    }
  } catch (err) {
    console.error('获取热榜失败:', err)
    error.value = '获取热榜失败，请稍后重试'
    hotList.value = []
  } finally {
    loading.value = false
  }
}

const handleResultClick = async (item) => {
  if (!item || !item.noteId) {
    console.error('热榜数据无效:', item)
    return
  }

  try {
    const userId = userStore.userInfo?.id
    let latestStats = null

    if (userId) {
      try {
        if (canIncreaseView(item.noteId, userId)) {
          latestStats = await changeNoteStat(item.noteId, userId, 'views', 1)
          markViewIncreased(item.noteId, userId)
          if (latestStats?.views !== undefined) {
            item.viewCount = latestStats.views
            item.likeCount = latestStats.likes ?? item.likeCount
            item.favoriteCount = latestStats.favorites ?? item.favoriteCount
            item.commentCount = latestStats.comments ?? item.commentCount
            item.authorName = latestStats.authorName || item.authorName
          }
        }
      } catch (err) {
        console.error('增加阅读量失败:', err)
        item.viewCount = (item.viewCount || 0) + 1
      }
    }

    emit('open-note-detail', {
      noteId: item.noteId,
      title: item.title || '无标题',
      fileType: item.fileType,
      fromTab: 'hot',
      authorName: latestStats?.authorName ?? item.authorName,
      viewCount: latestStats?.views ?? item.viewCount ?? 0,
      likeCount: latestStats?.likes ?? item.likeCount ?? 0,
      favoriteCount: latestStats?.favorites ?? item.favoriteCount ?? 0,
      commentCount: latestStats?.comments ?? item.commentCount ?? 0
    })
    
    // 注意：路由更新由 MainView 的 handleOpenNoteDetail 统一处理
  } catch (error) {
    console.error('打开笔记详情页失败:', error)
  }
}

onMounted(() => {
  fetchHotList()
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

.hot-page {
  min-height: 100vh;
  padding: 20px 24px 100px;
  background: var(--surface-muted);
  display: flex;
  flex-direction: column;
}

.hot-panel {
  width: min(1200px, 100%);
  margin: 0 auto;
  background: var(--surface-base);
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  padding: 32px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  min-height: calc(100vh - 200px);
  display: flex;
  flex-direction: column;
  flex: 1;
}

.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--line-soft);
  padding-bottom: 16px;
  margin-bottom: 20px;
  gap: 16px;
}

.headline {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.panel-title {
  margin: 0;
  font-size: 22px;
  color: var(--text-strong);
  font-weight: 700;
}

.panel-subtitle {
  margin: 0;
  color: var(--text-muted);
  font-size: 13px;
}

.refresh-button {
  border: 1px solid var(--line-soft);
  background: var(--surface-base);
  border-radius: 6px;
  padding: 8px 12px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: all 0.2s;
}

.refresh-button:hover {
  border-color: var(--brand-primary);
  color: var(--brand-primary);
}

.refresh-button:disabled {
  cursor: not-allowed;
  opacity: 0.7;
}

.results-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
  flex: 1;
  min-height: 0;
}

.result-card {
  display: flex;
  gap: 14px;
  padding: 18px 20px;
  border-radius: 10px;
  border: 1px solid var(--line-soft);
  background: var(--surface-base);
  transition: border-color 0.2s, box-shadow 0.2s, transform 0.2s;
  cursor: pointer;
  align-items: flex-start;
}

.result-card:hover {
  border-color: var(--brand-primary);
  box-shadow: 0 2px 10px rgba(0, 127, 255, 0.1);
  transform: translateY(-1px);
}

.rank-badge {
  width: 34px;
  height: 34px;
  border-radius: 8px;
  background: #eef3ff;
  color: var(--brand-primary);
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: 700;
  flex-shrink: 0;
}

.rank-badge.top {
  background: #ffe8cc;
  color: #d46b08;
}

.result-content {
  display: flex;
  flex-direction: column;
  gap: 10px;
  flex: 1;
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
  margin-top: 4px;
}

.meta-left,
.meta-right {
  display: flex;
  align-items: center;
  gap: 14px;
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
  flex: 1;
  justify-content: center;
  min-height: 400px;
}

.state-card.error {
  border-color: #ffccc7;
  background: #fff1f0;
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

.retry-button {
  border: 1px solid var(--line-soft);
  background: var(--surface-base);
  border-radius: 6px;
  padding: 6px 12px;
  cursor: pointer;
}

.loader {
  width: 32px;
  height: 32px;
  border-radius: 50%;
  border: 3px solid var(--line-soft);
  border-top-color: var(--brand-primary);
  animation: spin 1s linear infinite;
}

.placeholder small {
  max-width: 400px;
  line-height: 1.6;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 768px) {
  .hot-page {
    padding: 16px;
  }

  .hot-panel {
    padding: 20px;
  }

  .result-card {
    flex-direction: row;
    align-items: flex-start;
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

