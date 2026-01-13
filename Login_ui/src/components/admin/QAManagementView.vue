<template>
  <div class="qa-management-view">
    <div class="view-header">
      <h2 class="page-title">问答管理</h2>
      <div class="header-actions">
        <div class="stat-item">
          <span class="stat-label">问题总数：</span>
          <span class="stat-value">{{ questionCount }}</span>
        </div>
        <button class="refresh-btn" @click="loadQuestions">刷新</button>
      </div>
    </div>

    <!-- 搜索框 -->
    <div class="search-container">
      <div class="search-box">
        <svg class="search-icon" viewBox="0 0 16 16" fill="currentColor">
          <path d="M11.742 10.344a6.5 6.5 0 1 0-1.397 1.398h-.001c.03.04.062.078.098.115l3.85 3.85a1 1 0 0 0 1.415-1.414l-3.85-3.85a1.007 1.007 0 0 0-.115-.1zM12 6.5a5.5 5.5 0 1 1-11 0 5.5 5.5 0 0 1 11 0z"/>
        </svg>
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

    <div class="table-container">
      <table class="data-table">
        <thead>
          <tr>
            <th>问题ID</th>
            <th>标题</th>
            <th>提问者</th>
            <th>回答数</th>
            <th>点赞数</th>
            <th>收藏数</th>
            <th>创建时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading">
            <td colspan="8" class="loading-cell">加载中...</td>
          </tr>
          <tr v-else-if="paginatedQuestions.length === 0">
            <td colspan="8" class="empty-cell">
              {{ isSearching ? '未找到匹配的问题' : '暂无问题' }}
            </td>
          </tr>
          <tr v-else v-for="question in paginatedQuestions" :key="question.questionId">
            <td>{{ question.questionId }}</td>
            <td class="title-cell" v-html="highlightKeyword(question.title || '-')"></td>
            <td class="author-cell">
              <div v-if="question.authorName || question.authorId">
                <div v-if="question.authorName" class="author-name">{{ question.authorName }}</div>
                <div v-if="question.authorId" class="author-id">ID: {{ question.authorId }}</div>
              </div>
              <span v-else>-</span>
            </td>
            <td>{{ question.answerCount || 0 }}</td>
            <td>{{ question.likeCount || 0 }}</td>
            <td>{{ question.favoriteCount || 0 }}</td>
            <td>{{ formatTime(question.createdAt) }}</td>
            <td>
              <button class="action-btn" @click="viewQuestion(question)">查看</button>
              <button class="action-btn delete-btn" @click="handleDeleteQuestion(question)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
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
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, computed, watch } from 'vue'
import { getQuestionCount, getQuestionList, adminDeleteQuestion } from '../../api/admin'
import { searchQuestions } from '../../api/qa'

const questionCount = ref(0)
const questions = ref([])
const searchResults = ref([])
const loading = ref(false)
const currentPage = ref(1)
const pageSize = 30
const searchQuery = ref('')
const isSearching = ref(false)
const searchTimeout = ref(null)

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
    // 如果总页数少于等于7页，显示所有页码
    for (let i = 1; i <= total; i++) {
      pages.push(i)
    }
  } else {
    // 如果总页数大于7页，显示部分页码
    if (current <= 4) {
      // 当前页在前4页
      for (let i = 1; i <= 5; i++) {
        pages.push(i)
      }
      pages.push('...')
      pages.push(total)
    } else if (current >= total - 3) {
      // 当前页在后4页
      pages.push(1)
      pages.push('...')
      for (let i = total - 4; i <= total; i++) {
        pages.push(i)
      }
    } else {
      // 当前页在中间
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
    const [countRes, listRes] = await Promise.all([
      getQuestionCount(),
      getQuestionList()
    ])
    
    const oldCount = questionCount.value
    questionCount.value = countRes?.data?.questionCount || countRes?.questionCount || 0
    questions.value = listRes?.data || listRes || []
    
    // 如果数据量发生变化，可能需要调整页码
    if (!preservePage) {
      currentPage.value = 1
    } else {
      // 如果当前页超出范围，调整到最后一页
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

const viewQuestion = (question) => {
  // 可以跳转到问题详情页或打开新窗口
  const questionId = question.questionId
  if (questionId) {
    window.open(`/main?tab=qa-detail&questionId=${questionId}`, '_blank')
  }
}

const handleDeleteQuestion = async (question) => {
  const questionId = question.questionId
  if (!questionId) {
    alert('问题ID不存在')
    return
  }
  
  const questionTitle = question.title || '该问题'
  const confirmMessage = `确定要删除问题"${questionTitle}"吗？此操作不可恢复。`
  
  if (!confirm(confirmMessage)) {
    return
  }
  
  try {
    await adminDeleteQuestion(questionId)
    alert('删除成功')
    // 重新加载问题列表
    await loadQuestions(true) // 保持当前页码
  } catch (error) {
    console.error('删除问题失败:', error)
    alert('删除失败：' + (error.response?.data?.message || error.message || '未知错误'))
  }
}

const goToPage = (page) => {
  if (page >= 1 && page <= totalPages.value && page !== currentPage.value && page !== '...') {
    currentPage.value = page
    // 滚动到表格顶部
    const tableContainer = document.querySelector('.table-container')
    if (tableContainer) {
      tableContainer.scrollIntoView({ behavior: 'smooth', block: 'start' })
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
  
  // 如果输入框为空，清除搜索
  if (!searchQuery.value.trim()) {
    clearSearch()
    return
  }
  
  // 延迟500ms执行搜索（防抖）
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
    const results = await searchQuestions(keyword, 0) // 管理员搜索使用0作为占位符
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
  // 每30秒自动刷新数据
  refreshTimer = setInterval(() => {
    loadQuestions(true) // 保持当前页码
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
  max-width: 1200px;
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
  color: #007FFF;
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

.table-container {
  background: white;
  border-radius: 8px;
  overflow: hidden;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

.data-table {
  width: 100%;
  border-collapse: collapse;
}

.data-table thead {
  background: #f8f9fa;
}

.data-table th {
  padding: 12px 16px;
  text-align: left;
  font-size: 14px;
  font-weight: 600;
  color: #333;
  border-bottom: 2px solid #e9ecef;
}

.data-table td {
  padding: 12px 16px;
  font-size: 14px;
  color: #666;
  border-bottom: 1px solid #f0f0f0;
}

.data-table tbody tr:hover {
  background: #f8f9fa;
}

.title-cell {
  max-width: 300px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.title-cell :deep(.highlight) {
  background: #fff3cd;
  padding: 2px 4px;
  border-radius: 3px;
  font-weight: 600;
  color: #007FFF;
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
  border-color: #007FFF;
  box-shadow: 0 0 0 3px rgba(0, 127, 255, 0.1);
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
  border: 1px solid #007FFF;
  background: #007FFF;
  color: white;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
  white-space: nowrap;
}

.search-btn:hover {
  background: #0066cc;
  border-color: #0066cc;
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
  color: #007FFF;
  font-weight: 600;
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

.action-btn.delete-btn {
  border-color: #dc3545;
  color: #dc3545;
  margin-left: 8px;
}

.action-btn.delete-btn:hover {
  background: #dc3545;
  color: white;
}

.author-cell {
  min-width: 150px;
}

.author-name {
  font-weight: 500;
  color: #333;
  margin-bottom: 4px;
}

.author-id {
  font-size: 12px;
  color: #666;
}

.loading-cell,
.empty-cell {
  text-align: center;
  color: #999;
  padding: 40px;
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
  border: 1px solid #ddd;
  background: white;
  color: #666;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
}

.page-btn:hover:not(:disabled) {
  background: #f5f5f5;
  border-color: #007FFF;
  color: #007FFF;
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
  border: 1px solid #ddd;
  background: white;
  color: #666;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
  display: flex;
  align-items: center;
  justify-content: center;
}

.page-number:hover:not(.active):not(:disabled) {
  background: #f5f5f5;
  border-color: #007FFF;
  color: #007FFF;
}

.page-number.active {
  background: #007FFF;
  border-color: #007FFF;
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
}
</style>
