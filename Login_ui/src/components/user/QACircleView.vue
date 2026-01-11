<template>
  <div class="qa-page">
    <section class="qa-panel">
      <!-- 标签页切换按钮 -->
      <div class="tab-buttons">
        <button
          v-for="tab in tabs"
          :key="tab.value"
          class="tab-button"
          :class="{ active: activeTab === tab.value }"
          @click="activeTab = tab.value"
        >
          {{ tab.label }}
        </button>
      </div>

      <!-- 加载状态 -->
      <div v-if="loading" class="state-card">
        <span class="loader" aria-hidden="true"></span>
        <p>加载中...</p>
        <small>正在获取问答数据</small>
      </div>

      <!-- 问题列表 -->
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
            <p class="result-summary">{{ item.content }}</p>
            <div class="result-meta">
              <div class="meta-left">
                <span class="meta-author">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 8a3 3 0 100-6 3 3 0 000 6zm2-3a2 2 0 11-4 0 2 2 0 014 0zm4 8c0 1-1 1-1 1H3s-1 0-1-1 1-4 6-4 6 3 6 4zm-1-.004c-.001-.246-.154-.986-.832-1.664C11.516 10.68 10.289 10 8 10c-2.29 0-3.516.68-4.168 1.332-.678.678-.83 1.418-.832 1.664h10z"/>
                  </svg>
                  {{ item.authorName || `用户 #${item.authorId}` }}
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
                  {{ item.likeCount || 0 }} 赞同
                </span>
                <span class="meta-stat">
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
          <!-- 删除按钮（仅对问题类型且是作者时显示） -->
          <button
            v-if="item.type === 'question' && item.authorId === userStore.userInfo?.id"
            class="delete-btn"
            @click.stop="handleDeleteQuestion(item)"
            title="删除问题"
          >
            <svg class="delete-icon" viewBox="0 0 16 16" fill="currentColor">
              <path d="M5.5 5.5A.5.5 0 016 6v6a.5.5 0 01-1 0V6a.5.5 0 01.5-.5zm2.5 0a.5.5 0 01.5.5v6a.5.5 0 01-1 0V6a.5.5 0 01.5-.5zm3 .5a.5.5 0 00-1 0v6a.5.5 0 001 0V6z"/>
              <path fill-rule="evenodd" d="M14.5 3a1 1 0 01-1 1H13v9a2 2 0 01-2 2H5a2 2 0 01-2-2V4h-.5a1 1 0 01-1-1V2a1 1 0 011-1H6a1 1 0 011-1h2a1 1 0 011 1h3.5a1 1 0 011 1v1zM4.118 4L4 4.059V13a1 1 0 001 1h6a1 1 0 001-1V4.059L11.882 4H4.118zM2.5 3V2h11v1h-11z"/>
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

    <!-- 发起提问对话框 -->
    <div v-if="askVisible" class="dialog-mask">
      <div class="dialog">
        <header class="dialog-header">
          <h4>发起提问</h4>
        </header>
        <div class="dialog-body">
          <input v-model="askForm.title" type="text" placeholder="一句话描述你的问题" />
          <textarea
            v-model="askForm.content"
            rows="4"
            placeholder="补充细节，描述场景、遇到的问题等"
          ></textarea>
          <input v-model="askForm.tagsText" type="text" placeholder="标签，使用逗号分隔，例如：Java, Vue3" />
        </div>
        <footer class="dialog-footer">
          <button class="ghost-btn" type="button" @click="closeAskDialog">取消</button>
          <button class="primary-btn" type="button" @click="handleCreateQuestion">提交</button>
        </footer>
      </div>
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
import { ref, computed, reactive, watch, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user'
import {
  createQuestion,
  getQuestionDetail,
  deleteQuestion
} from '@/api/qa'
import { formatTime } from '@/utils/time'
import MessageToast from '@/components/MessageToast.vue'
import { useMessage } from '@/utils/message'

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()

// 消息提示
const { showToast, toastMessage, toastType, toastDuration, showSuccess, showError, hideMessage } = useMessage()

// 标签页配置
const tabs = [
  { label: '我的问题', value: 'my-questions' },
  { label: '我的回答', value: 'my-answers' },
  { label: '所有问答', value: 'all' }
]

const activeTab = ref('my-questions')
const loading = ref(false)
const questionList = ref([]) // 存储所有问题
const askVisible = ref(false)
const askForm = reactive({
  title: '',
  content: '',
  tagsText: ''
})

// 从localStorage加载问题列表
const loadQuestionsFromStorage = () => {
  try {
    const stored = localStorage.getItem('qa_question_list')
    if (stored) {
      questionList.value = JSON.parse(stored)
    }
  } catch (err) {
    console.warn('加载本地问题列表失败:', err)
  }
}

// 保存问题列表到localStorage
const saveQuestionsToStorage = () => {
  try {
    localStorage.setItem('qa_question_list', JSON.stringify(questionList.value))
  } catch (err) {
    console.warn('保存问题列表失败:', err)
  }
}

// 添加或更新问题到列表
const upsertQuestion = (questionVO) => {
  if (!questionVO || !questionVO.questionId) return
  
  const idx = questionList.value.findIndex(q => q.questionId === questionVO.questionId)
  const questionData = {
    questionId: questionVO.questionId,
    authorId: questionVO.authorId,
    title: questionVO.title,
    content: questionVO.content,
    tags: questionVO.tags || [],
    createdAt: questionVO.createdAt,
    likeCount: questionVO.likeCount || 0,
    favoriteCount: questionVO.favoriteCount || 0,
    answerCount: questionVO.answers?.length || 0,
    answers: questionVO.answers || [],
    authorName: `用户 #${questionVO.authorId}` // 可以后续优化获取用户名
  }
  
  if (idx >= 0) {
    questionList.value[idx] = questionData
  } else {
    questionList.value.unshift(questionData)
  }
  
  saveQuestionsToStorage()
}

// 根据当前标签页筛选显示列表
const displayList = computed(() => {
  const userId = userStore.userInfo?.id
  if (!userId) return []
  
  switch (activeTab.value) {
    case 'my-questions':
      // 我的问题：筛选authorId等于当前用户ID的问题
      return questionList.value
        .filter(q => q.authorId === userId)
        .map(q => ({
          id: q.questionId,
          type: 'question',
          questionId: q.questionId,
          title: q.title,
          content: q.content,
          authorId: q.authorId,
          authorName: q.authorName,
          createdAt: q.createdAt,
          likeCount: q.likeCount,
          answerCount: q.answerCount,
          tags: q.tags
        }))
    
    case 'my-answers':
      // 我的回答：遍历所有问题，找到answers中authorId等于当前用户ID的
      const myAnswers = []
      questionList.value.forEach(q => {
        if (q.answers && q.answers.length > 0) {
          q.answers.forEach(answer => {
            if (answer.authorId === userId) {
              myAnswers.push({
                id: `${q.questionId}-${answer.answerId}`,
                type: 'answer',
                questionId: q.questionId,
                answerId: answer.answerId,
                title: `回答：${q.title}`,
                content: answer.content,
                authorId: answer.authorId,
                authorName: q.authorName,
                createdAt: answer.createdAt || q.createdAt,
                likeCount: answer.likeCount || 0,
                answerCount: q.answerCount,
                tags: q.tags,
                questionTitle: q.title // 保存问题标题用于显示
              })
            }
          })
        }
      })
      return myAnswers
    
    case 'all':
      // 所有问答：显示所有问题
      return questionList.value.map(q => ({
        id: q.questionId,
        type: 'question',
        questionId: q.questionId,
        title: q.title,
        content: q.content,
        authorId: q.authorId,
        authorName: q.authorName,
        createdAt: q.createdAt,
        likeCount: q.likeCount,
        answerCount: q.answerCount,
        tags: q.tags
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
    case 'my-questions':
      return '你还没有提问过，点击右上角"发起提问"开始吧'
    case 'my-answers':
      return '你还没有回答过任何问题'
    case 'all':
      return '还没有问答内容，点击右上角"发起提问"创建第一个问题吧'
    default:
      return ''
  }
}

// 获取显示时间
const getDisplayTime = (item) => {
  if (!item || !item.createdAt) return '时间未知'
  return formatTime(item.createdAt) || '时间未知'
}

// 处理点击问题或回答
const handleItemClick = async (item) => {
  if (!item || !item.questionId) return
  
  try {
    // 跳转到问答详情页，通过路由参数传递questionId
    router.replace({
      path: route.path,
      query: {
        ...route.query,
        tab: 'qa-detail',
        questionId: item.questionId,
        answerId: item.type === 'answer' ? item.answerId : undefined
      }
    })
  } catch (error) {
    console.error('跳转失败:', error)
  }
}

// 打开提问对话框
const openAskDialog = () => {
  askVisible.value = true
}

// 关闭提问对话框
const closeAskDialog = () => {
  askVisible.value = false
  askForm.title = ''
  askForm.content = ''
  askForm.tagsText = ''
}

// 创建问题
const handleCreateQuestion = async () => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  const title = askForm.title.trim()
  const content = askForm.content.trim()
  if (!title || !content) {
    showError('标题和内容不能为空')
    return
  }
  
  const tags = askForm.tagsText
    .split(/[,，]/)
    .map(t => t.trim())
    .filter(Boolean)
  
  loading.value = true
  try {
    const data = await createQuestion({
      authorId: userId,
      title,
      content,
      tags
    })
    
    if (data) {
      upsertQuestion(data)
      closeAskDialog()
      
      // 切换到"我的问题"标签页
      activeTab.value = 'my-questions'
      
      // 跳转到问题详情页
      await handleItemClick({
        questionId: data.questionId,
        type: 'question'
      })
    }
  } catch (err) {
    console.error('创建问题失败', err)
    showError('创建问题失败，请检查后端接口或输入内容')
  } finally {
    loading.value = false
  }
}

// 删除问题
const handleDeleteQuestion = async (item) => {
  if (!item || !item.questionId) return
  
  const confirmed = window.confirm('确定要删除这个问题吗？删除后无法恢复。')
  if (!confirmed) return
  
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  // 检查是否是作者
  if (item.authorId !== userId) {
    showError('只能删除自己的问题')
    return
  }
  
  loading.value = true
  try {
    await deleteQuestion(item.questionId)
    
    // 从列表中移除
    questionList.value = questionList.value.filter(q => q.questionId !== item.questionId)
    saveQuestionsToStorage()
    
    showSuccess('删除成功')
  } catch (err) {
    console.error('删除问题失败', err)
    showError('删除问题失败，请稍后重试')
  } finally {
    loading.value = false
  }
}

// 监听路由中的questionId，如果有则加载问题详情
watch(() => route.query.questionId, async (questionId) => {
  if (questionId && !questionList.value.find(q => q.questionId === questionId)) {
    // 如果问题不在列表中，则加载
    loading.value = true
    try {
      const data = await getQuestionDetail(questionId)
      if (data) {
        upsertQuestion(data)
      }
    } catch (err) {
      console.error('获取问题详情失败', err)
    } finally {
      loading.value = false
    }
  }
}, { immediate: true })

// 组件挂载时加载本地存储的问题列表
onMounted(() => {
  loadQuestionsFromStorage()
  
  // 如果路由中有questionId，加载该问题
  if (route.query.questionId) {
    const questionId = route.query.questionId
    if (!questionList.value.find(q => q.questionId === questionId)) {
      loading.value = true
      getQuestionDetail(questionId)
        .then(data => {
          if (data) {
            upsertQuestion(data)
          }
        })
        .catch(err => {
          console.error('获取问题详情失败', err)
        })
        .finally(() => {
          loading.value = false
        })
    }
  }
})

defineExpose({ openAskDialog })
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

.qa-page {
  min-height: 100vh;
  padding: 20px 24px 100px;
  background: var(--surface-muted);
}

.qa-panel {
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

.dialog-mask {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 50;
}

.dialog {
  width: min(680px, 92vw);
  background: #fff;
  border-radius: 12px;
  padding: 16px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.dialog-header h4 {
  margin: 0;
  font-size: 18px;
  font-weight: 600;
}

.dialog-body {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.dialog-body input,
.dialog-body textarea {
  width: 100%;
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  padding: 10px;
  font-size: 14px;
}

.dialog-body textarea {
  resize: vertical;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

.primary-btn {
  background: var(--brand-primary);
  color: #fff;
  border: none;
  padding: 8px 16px;
  border-radius: 8px;
  cursor: pointer;
  font-size: 14px;
}

.ghost-btn {
  border: 1px solid var(--line-soft);
  background: #fff;
  color: var(--text-strong);
  padding: 8px 16px;
  border-radius: 8px;
  cursor: pointer;
  font-size: 14px;
}

.primary-btn:hover,
.ghost-btn:hover {
  opacity: 0.9;
}

@media (max-width: 768px) {
  .qa-page {
    padding: 16px;
  }

  .qa-panel {
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
