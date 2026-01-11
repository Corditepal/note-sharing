<template>
  <div class="qa-detail-page">
    <div v-if="loading" class="loading-state">
      <div class="loader"></div>
      <p>加载中...</p>
    </div>

    <div v-else-if="error" class="error-state">
      <p>{{ error }}</p>
      <button class="back-button" @click="goBack">返回</button>
    </div>

    <div v-else-if="question" class="detail-layout">
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

        <!-- 问题详情 -->
        <div class="question-section">
          <h1 class="question-title">{{ question.title }}</h1>
          <div class="question-meta">
            <span class="meta-item">
              <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 8a3 3 0 100-6 3 3 0 000 6zm2-3a2 2 0 11-4 0 2 2 0 014 0zm4 8c0 1-1 1-1 1H3s-1 0-1-1 1-4 6-4 6 3 6 4zm-1-.004c-.001-.246-.154-.986-.832-1.664C11.516 10.68 10.289 10 8 10c-2.29 0-3.516.68-4.168 1.332-.678.678-.83 1.418-.832 1.664h10z"/>
              </svg>
              提问者 #{{ question.authorId }}
            </span>
            <span class="meta-item">
              <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 3.5a.5.5 0 00-1 0V9a.5.5 0 00.252.434l3.5 2a.5.5 0 00.496-.868L8 8.71V3.5z"/>
                <path d="M8 16A8 8 0 108 0a8 8 0 000 16zm7-8A7 7 0 111 8a7 7 0 0114 0z"/>
              </svg>
              {{ formatTime(question.createdAt) }}
            </span>
          </div>
          <div v-if="question.tags && question.tags.length" class="tag-list">
            <span v-for="tag in question.tags" :key="tag" class="tag-chip">#{{ tag }}</span>
          </div>
          <div class="question-content">{{ question.content }}</div>
          <div class="question-actions">
            <button class="action-btn" @click="handleLikeQuestion">
              <svg class="action-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M8 4a.5.5 0 01.5.5v3h3a.5.5 0 010 1h-3v3a.5.5 0 01-1 0v-3h-3a.5.5 0 010-1h3v-3A.5.5 0 018 4z"/>
              </svg>
              赞同 {{ question.likeCount || 0 }}
            </button>
            <button class="action-btn" @click="handleFavoriteQuestion">
              <svg class="action-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M2 2v13.5a.5.5 0 00.74.439L8 13.069l5.26 2.87A.5.5 0 0014 15.5V2a2 2 0 00-2-2H4a2 2 0 00-2 2z"/>
              </svg>
              收藏 {{ question.favoriteCount || 0 }}
            </button>
            <button
              v-if="question.authorId === userStore.userInfo?.id"
              class="action-btn delete-action-btn"
              @click="handleDeleteQuestion"
            >
              <svg class="action-icon" viewBox="0 0 16 16" fill="currentColor">
                <path d="M5.5 5.5A.5.5 0 016 6v6a.5.5 0 01-1 0V6a.5.5 0 01.5-.5zm2.5 0a.5.5 0 01.5.5v6a.5.5 0 01-1 0V6a.5.5 0 01.5-.5zm3 .5a.5.5 0 00-1 0v6a.5.5 0 001 0V6z"/>
                <path fill-rule="evenodd" d="M14.5 3a1 1 0 01-1 1H13v9a2 2 0 01-2 2H5a2 2 0 01-2-2V4h-.5a1 1 0 01-1-1V2a1 1 0 011-1H6a1 1 0 011-1h2a1 1 0 011 1h3.5a1 1 0 011 1v1zM4.118 4L4 4.059V13a1 1 0 001 1h6a1 1 0 001-1V4.059L11.882 4H4.118zM2.5 3V2h11v1h-11z"/>
              </svg>
              删除问题
            </button>
          </div>
        </div>

        <!-- 回答编辑器 -->
        <div class="answer-editor-section">
          <h3 class="section-title">写下你的回答</h3>
          <textarea
            v-model="answerInput"
            rows="4"
            placeholder="写下你的回答..."
            class="answer-textarea"
          ></textarea>
          <div class="editor-actions">
            <button class="primary-btn" @click="handleCreateAnswer">发布回答</button>
          </div>
        </div>

        <!-- 回答列表 -->
        <div class="answers-section">
          <h3 class="section-title">
            {{ question.answers?.length || 0 }} 个回答
          </h3>
          <div v-if="question.answers && question.answers.length" class="answers-list">
            <div
              v-for="answer in question.answers"
              :key="answer.answerId"
              class="answer-card"
              :class="{ highlight: answerIdToHighlight === answer.answerId }"
            >
              <div class="answer-meta">
                <span class="meta-item">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 8a3 3 0 100-6 3 3 0 000 6zm2-3a2 2 0 11-4 0 2 2 0 014 0zm4 8c0 1-1 1-1 1H3s-1 0-1-1 1-4 6-4 6 3 6 4zm-1-.004c-.001-.246-.154-.986-.832-1.664C11.516 10.68 10.289 10 8 10c-2.29 0-3.516.68-4.168 1.332-.678.678-.83 1.418-.832 1.664h10z"/>
                  </svg>
                  回答者 #{{ answer.authorId }}
                </span>
                <span class="meta-item">
                  <svg class="meta-icon" viewBox="0 0 16 16" fill="currentColor">
                    <path d="M8 3.5a.5.5 0 00-1 0V9a.5.5 0 00.252.434l3.5 2a.5.5 0 00.496-.868L8 8.71V3.5z"/>
                    <path d="M8 16A8 8 0 108 0a8 8 0 000 16zm7-8A7 7 0 111 8a7 7 0 0114 0z"/>
                  </svg>
                  {{ formatTime(answer.createdAt) }}
                </span>
              </div>
              <div class="answer-content-wrapper">
                <div class="answer-content">{{ answer.content }}</div>
                <div class="answer-actions">
                  <button class="text-action-btn" @click="handleLikeAnswer(answer)">
                    赞同 {{ answer.likeCount || 0 }}
                  </button>
                  <button class="text-action-btn" @click="toggleCommentInput(answer.answerId)">
                    评论 {{ answer.comments?.length || 0 }}
                  </button>
                  <button
                    v-if="answer.authorId === userStore.userInfo?.id"
                    class="text-action-btn delete-text-btn"
                    @click="handleDeleteAnswer(answer)"
                    title="删除回答"
                  >
                    删除
                  </button>
                </div>
              </div>

              <!-- 评论输入框 -->
              <div v-if="activeCommentAnswerId === answer.answerId" class="comment-input-section">
                <textarea
                  v-model="commentInputs[answer.answerId]"
                  rows="3"
                  placeholder="写下你的评论..."
                  class="comment-textarea"
                ></textarea>
                <div class="comment-input-actions">
                  <button class="ghost-btn" @click="cancelComment(answer.answerId)">取消</button>
                  <button class="primary-btn" @click="handleCreateComment(answer)">发布评论</button>
                </div>
              </div>

              <!-- 评论列表 -->
              <div v-if="answer.comments && answer.comments.length" class="comments-section">
                <div
                  v-for="comment in answer.comments"
                  :key="comment.commentId"
                  class="comment-item"
                >
                  <div class="comment-header">
                    <div class="comment-meta">
                      <span class="comment-author">评论者 #{{ comment.authorId }}</span>
                      <span class="comment-time">{{ formatTime(comment.createdAt) }}</span>
                    </div>
                    <div class="comment-header-actions">
                      <button class="comment-action-btn" @click="handleLikeComment(answer, comment)">
                        <svg class="action-icon" viewBox="0 0 16 16" fill="currentColor">
                          <path d="M8 4a.5.5 0 01.5.5v3h3a.5.5 0 010 1h-3v3a.5.5 0 01-1 0v-3h-3a.5.5 0 010-1h3v-3A.5.5 0 018 4z"/>
                        </svg>
                        {{ comment.likeCount || 0 }}
                      </button>
                      <button
                        v-if="comment.authorId === userStore.userInfo?.id"
                        class="comment-delete-btn"
                        @click="handleDeleteComment(answer, comment)"
                        title="删除评论"
                      >
                        删除
                      </button>
                    </div>
                  </div>
                  <div class="comment-content">{{ comment.content }}</div>
                  
                  <!-- 回复按钮 -->
                  <button 
                    v-if="activeReplyCommentId !== comment.commentId"
                    class="reply-btn"
                    @click="toggleReplyInput(answer.answerId, comment.commentId)"
                  >
                    回复
                  </button>
                  
                  <!-- 回复输入框 -->
                  <div v-if="activeReplyCommentId === comment.commentId && activeReplyAnswerId === answer.answerId" class="reply-input-section">
                    <textarea
                      v-model="replyInputs[`${answer.answerId}_${comment.commentId}`]"
                      rows="2"
                      placeholder="写下你的回复..."
                      class="reply-textarea"
                    ></textarea>
                    <div class="reply-input-actions">
                      <button class="ghost-btn" @click="cancelReply(answer.answerId, comment.commentId)">取消</button>
                      <button class="primary-btn" @click="handleCreateReply(answer, comment)">发布回复</button>
                    </div>
                  </div>

                  <!-- 回复列表 -->
                  <div v-if="comment.replies && comment.replies.length" class="replies-section">
                    <div
                      v-for="reply in comment.replies"
                      :key="reply.replyId"
                      class="reply-item"
                    >
                      <div class="reply-header">
                        <div class="reply-meta">
                          <span class="reply-author">回复者 #{{ reply.authorId }}</span>
                          <span class="reply-time">{{ formatTime(reply.createdAt) }}</span>
                        </div>
                        <div class="reply-header-actions">
                          <button class="comment-action-btn" @click="handleLikeReply(answer, comment, reply)">
                            <svg class="action-icon" viewBox="0 0 16 16" fill="currentColor">
                              <path d="M8 4a.5.5 0 01.5.5v3h3a.5.5 0 010 1h-3v3a.5.5 0 01-1 0v-3h-3a.5.5 0 010-1h3v-3A.5.5 0 018 4z"/>
                            </svg>
                            {{ reply.likeCount || 0 }}
                          </button>
                          <button
                            v-if="reply.authorId === userStore.userInfo?.id"
                            class="comment-delete-btn"
                            @click="handleDeleteReply(answer, comment, reply)"
                            title="删除回复"
                          >
                            删除
                          </button>
                        </div>
                      </div>
                      <div class="reply-content">{{ reply.content }}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div v-else class="empty-answers">
            还没有回答，快来写下第一条回答吧。
          </div>
        </div>
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
import { ref, watch, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { useUserStore } from '@/stores/user'
import {
  getQuestionDetail,
  deleteQuestion,
  createAnswer,
  deleteAnswer,
  likeQuestion,
  favoriteQuestion,
  likeAnswer,
  createComment,
  deleteComment,
  likeComment,
  createReply,
  deleteReply,
  likeReply
} from '@/api/qa'
import { formatTime } from '@/utils/time'
import MessageToast from '@/components/MessageToast.vue'
import { useMessage } from '@/utils/message'

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()

// 消息提示
const { showToast, toastMessage, toastType, toastDuration, showSuccess, showError, hideMessage } = useMessage()

const props = defineProps({
  questionId: {
    type: String,
    required: true
  },
  answerId: {
    type: [Number, String],
    default: null
  }
})

const loading = ref(true)
const error = ref(null)
const question = ref(null)
const answerInput = ref('')
const answerIdToHighlight = ref(null)

// 评论相关状态
const activeCommentAnswerId = ref(null)
const commentInputs = ref({})
const activeReplyAnswerId = ref(null)
const activeReplyCommentId = ref(null)
const replyInputs = ref({})

// 获取问题详情
const fetchQuestionDetail = async () => {
  if (!props.questionId) return
  
  loading.value = true
  error.value = null
  
  try {
    const data = await getQuestionDetail(props.questionId)
    if (data) {
      question.value = data
      
      // 如果有answerId，高亮对应的回答
      if (props.answerId) {
        answerIdToHighlight.value = Number(props.answerId)
        // 滚动到对应回答
        setTimeout(() => {
          const element = document.querySelector('.answer-card.highlight')
          if (element) {
            element.scrollIntoView({ behavior: 'smooth', block: 'center' })
          }
        }, 300)
      }
    } else {
      error.value = '未找到对应问题'
    }
  } catch (err) {
    console.error('获取问题详情失败', err)
    error.value = '获取问题详情失败，请稍后重试'
  } finally {
    loading.value = false
  }
}

// 返回上一页
const goBack = () => {
  // 保存问题到localStorage（如果问题有更新）
  if (question.value) {
    try {
      const stored = localStorage.getItem('qa_question_list')
      let questionList = stored ? JSON.parse(stored) : []
      const idx = questionList.findIndex(q => q.questionId === question.value.questionId)
      if (idx >= 0) {
        questionList[idx] = {
          questionId: question.value.questionId,
          authorId: question.value.authorId,
          title: question.value.title,
          content: question.value.content,
          tags: question.value.tags || [],
          createdAt: question.value.createdAt,
          likeCount: question.value.likeCount || 0,
          favoriteCount: question.value.favoriteCount || 0,
          answerCount: question.value.answers?.length || 0,
          answers: question.value.answers || [],
          authorName: `用户 #${question.value.authorId}`
        }
        localStorage.setItem('qa_question_list', JSON.stringify(questionList))
      }
    } catch (err) {
      console.warn('保存问题更新失败:', err)
    }
  }
  
  // 检查是否来自搜索结果
  const fromTab = route.query.fromTab
  if (fromTab === 'search') {
    // 如果来自搜索结果，返回到搜索结果页面并保留搜索参数
    const keyword = route.query.keyword
    const searchType = route.query.searchType || 'qa'
    router.replace({
      path: route.path,
      query: {
        ...route.query,
        tab: 'search',
        fromTab: undefined,
        questionId: undefined,
        answerId: undefined,
        // 保留搜索参数 keyword 和 searchType
        keyword: keyword,
        searchType: searchType
      }
    })
  } else {
    // 其他来源，返回到问答圈
    router.replace({
      path: route.path,
      query: {
        ...route.query,
        tab: 'circle',
        questionId: undefined,
        answerId: undefined
      }
    })
  }
}

// 创建回答
const handleCreateAnswer = async () => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  const content = answerInput.value.trim()
  if (!content) {
    showError('回答内容不能为空')
    return
  }
  
  try {
    await createAnswer({
      questionId: props.questionId,
      authorId: userId,
      content
    })
    
    answerInput.value = ''
    // 重新加载问题详情
    await fetchQuestionDetail()
  } catch (err) {
    console.error('创建回答失败', err)
    showError('创建回答失败')
  }
}

// 点赞问题
const handleLikeQuestion = async () => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  try {
    await likeQuestion(userId, props.questionId)
    await fetchQuestionDetail()
  } catch (err) {
    console.error('点赞问题失败', err)
    showError('点赞失败')
  }
}

// 收藏问题
const handleFavoriteQuestion = async () => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  try {
    await favoriteQuestion(userId, props.questionId)
    await fetchQuestionDetail()
  } catch (err) {
    console.error('收藏问题失败', err)
    showError('收藏失败')
  }
}

// 点赞回答
const handleLikeAnswer = async (answer) => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  try {
    await likeAnswer(userId, props.questionId, answer.answerId)
    await fetchQuestionDetail()
  } catch (err) {
    console.error('点赞回答失败', err)
    showError('点赞失败')
  }
}

// 切换评论输入框显示
const toggleCommentInput = (answerId) => {
  if (activeCommentAnswerId.value === answerId) {
    activeCommentAnswerId.value = null
    commentInputs.value[answerId] = ''
  } else {
    activeCommentAnswerId.value = answerId
    if (!commentInputs.value[answerId]) {
      commentInputs.value[answerId] = ''
    }
  }
}

// 取消评论
const cancelComment = (answerId) => {
  activeCommentAnswerId.value = null
  commentInputs.value[answerId] = ''
}

// 创建评论
const handleCreateComment = async (answer) => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  const content = commentInputs.value[answer.answerId]?.trim()
  if (!content) {
    showError('评论内容不能为空')
    return
  }
  
  try {
    await createComment({
      questionId: props.questionId,
      answerId: answer.answerId,
      authorId: userId,
      content
    })
    
    commentInputs.value[answer.answerId] = ''
    activeCommentAnswerId.value = null
    // 重新加载问题详情
    await fetchQuestionDetail()
  } catch (err) {
    console.error('创建评论失败', err)
    showError('创建评论失败')
  }
}

// 点赞评论
const handleLikeComment = async (answer, comment) => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  try {
    await likeComment(userId, props.questionId, answer.answerId, comment.commentId)
    await fetchQuestionDetail()
  } catch (err) {
    console.error('点赞评论失败', err)
    showError('点赞失败')
  }
}

// 切换回复输入框显示
const toggleReplyInput = (answerId, commentId) => {
  if (activeReplyAnswerId.value === answerId && activeReplyCommentId.value === commentId) {
    activeReplyAnswerId.value = null
    activeReplyCommentId.value = null
    replyInputs.value[`${answerId}_${commentId}`] = ''
  } else {
    activeReplyAnswerId.value = answerId
    activeReplyCommentId.value = commentId
    const key = `${answerId}_${commentId}`
    if (!replyInputs.value[key]) {
      replyInputs.value[key] = ''
    }
  }
}

// 取消回复
const cancelReply = (answerId, commentId) => {
  activeReplyAnswerId.value = null
  activeReplyCommentId.value = null
  replyInputs.value[`${answerId}_${commentId}`] = ''
}

// 创建回复
const handleCreateReply = async (answer, comment) => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  const key = `${answer.answerId}_${comment.commentId}`
  const content = replyInputs.value[key]?.trim()
  if (!content) {
    showError('回复内容不能为空')
    return
  }
  
  try {
    await createReply({
      questionId: props.questionId,
      answerId: answer.answerId,
      commentId: comment.commentId,
      authorId: userId,
      content
    })
    
    replyInputs.value[key] = ''
    activeReplyAnswerId.value = null
    activeReplyCommentId.value = null
    // 重新加载问题详情
    await fetchQuestionDetail()
  } catch (err) {
    console.error('创建回复失败', err)
    showError('创建回复失败')
  }
}

// 点赞回复
const handleLikeReply = async (answer, comment, reply) => {
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  try {
    await likeReply(userId, props.questionId, answer.answerId, comment.commentId, reply.replyId)
    await fetchQuestionDetail()
  } catch (err) {
    console.error('点赞回复失败', err)
    showError('点赞失败')
  }
}

// 删除回答
const handleDeleteAnswer = async (answer) => {
  const confirmed = window.confirm('确定要删除这个回答吗？删除后无法恢复。')
  if (!confirmed) return
  
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  // 检查是否是作者
  if (answer.authorId !== userId) {
    showError('只能删除自己的回答')
    return
  }
  
  try {
    await deleteAnswer(props.questionId, answer.answerId)
    showSuccess('删除成功')
    // 重新加载问题详情
    await fetchQuestionDetail()
  } catch (err) {
    console.error('删除回答失败', err)
    showError('删除回答失败，请稍后重试')
  }
}

// 删除评论
const handleDeleteComment = async (answer, comment) => {
  const confirmed = window.confirm('确定要删除这条评论吗？删除后无法恢复。')
  if (!confirmed) return
  
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  // 检查是否是作者
  if (comment.authorId !== userId) {
    showError('只能删除自己的评论')
    return
  }
  
  try {
    await deleteComment(props.questionId, answer.answerId, comment.commentId)
    showSuccess('删除成功')
    // 重新加载问题详情
    await fetchQuestionDetail()
  } catch (err) {
    console.error('删除评论失败', err)
    showError('删除评论失败，请稍后重试')
  }
}

// 删除回复
const handleDeleteReply = async (answer, comment, reply) => {
  const confirmed = window.confirm('确定要删除这条回复吗？删除后无法恢复。')
  if (!confirmed) return
  
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  // 检查是否是作者
  if (reply.authorId !== userId) {
    showError('只能删除自己的回复')
    return
  }
  
  try {
    await deleteReply(props.questionId, answer.answerId, comment.commentId, reply.replyId)
    showSuccess('删除成功')
    // 重新加载问题详情
    await fetchQuestionDetail()
  } catch (err) {
    console.error('删除回复失败', err)
    showError('删除回复失败，请稍后重试')
  }
}

// 删除问题
const handleDeleteQuestion = async () => {
  if (!question.value) return
  
  const confirmed = window.confirm('确定要删除这个问题吗？删除后无法恢复。')
  if (!confirmed) return
  
  const userId = userStore.userInfo?.id
  if (!userId) {
    showError('请先登录后再进行此操作')
    return
  }
  
  // 检查是否是作者
  if (question.value.authorId !== userId) {
    showError('只能删除自己的问题')
    return
  }
  
  try {
    await deleteQuestion(props.questionId)
    showSuccess('删除成功')
    // 返回问答列表页
    goBack()
  } catch (err) {
    console.error('删除问题失败', err)
    showError('删除问题失败，请稍后重试')
  }
}

// 监听questionId变化
watch(() => props.questionId, () => {
  if (props.questionId) {
    fetchQuestionDetail()
  }
})

onMounted(() => {
  if (props.questionId) {
    fetchQuestionDetail()
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

.qa-detail-page {
  min-height: 100vh;
  background: var(--surface-muted);
  padding: 20px 24px 100px;
}

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

.back-button {
  padding: 8px 16px;
  background: var(--brand-primary);
  color: #fff;
  border: none;
  border-radius: 8px;
  cursor: pointer;
}

.detail-layout {
  width: min(1200px, 100%);
  margin: 0 auto;
}

.detail-container {
  background: var(--surface-base);
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  padding: 32px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
}

.back-header {
  margin-bottom: 24px;
}

.back-link-button {
  display: flex;
  align-items: center;
  gap: 8px;
  background: none;
  border: none;
  color: var(--text-secondary);
  cursor: pointer;
  font-size: 14px;
  padding: 4px 0;
}

.back-link-button:hover {
  color: var(--brand-primary);
}

.back-icon {
  width: 16px;
  height: 16px;
}

.question-section {
  margin-bottom: 32px;
  padding-bottom: 24px;
  border-bottom: 1px solid var(--line-soft);
}

.question-title {
  margin: 0 0 16px 0;
  font-size: 24px;
  font-weight: 600;
  color: var(--text-strong);
  line-height: 1.4;
}

.question-meta {
  display: flex;
  gap: 16px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.meta-item {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 13px;
  color: var(--text-muted);
}

.meta-icon {
  width: 14px;
  height: 14px;
}

.tag-list {
  display: flex;
  gap: 8px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}

.tag-chip {
  background: #eef2ff;
  color: #4338ca;
  padding: 4px 10px;
  border-radius: 6px;
  font-size: 12px;
}

.question-content {
  margin-bottom: 16px;
  font-size: 15px;
  line-height: 1.6;
  color: var(--text-strong);
  white-space: pre-wrap;
}

.question-actions {
  display: flex;
  gap: 12px;
}

.action-btn {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 16px;
  background: var(--surface-muted);
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  cursor: pointer;
  font-size: 14px;
  color: var(--text-strong);
  transition: all 0.2s;
}

.action-btn:hover {
  background: var(--line-soft);
  border-color: var(--brand-primary);
}

.delete-action-btn {
  color: #dc3545;
  border-color: rgba(220, 53, 69, 0.3);
}

.delete-action-btn:hover {
  background: rgba(220, 53, 69, 0.1);
  border-color: #dc3545;
  color: #c82333;
}

.action-icon {
  width: 16px;
  height: 16px;
}

.answer-editor-section {
  margin-bottom: 32px;
  padding-bottom: 24px;
  border-bottom: 1px solid var(--line-soft);
}

.section-title {
  margin: 0 0 16px 0;
  font-size: 18px;
  font-weight: 600;
  color: var(--text-strong);
}

.answer-textarea {
  width: 100%;
  padding: 12px;
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  font-size: 14px;
  font-family: inherit;
  resize: vertical;
  margin-bottom: 12px;
}

.editor-actions {
  display: flex;
  justify-content: flex-end;
}

.primary-btn {
  padding: 10px 20px;
  background: var(--brand-primary);
  color: #fff;
  border: none;
  border-radius: 8px;
  cursor: pointer;
  font-size: 14px;
  font-weight: 500;
}

.primary-btn:hover {
  opacity: 0.9;
}

.answers-section {
  margin-bottom: 24px;
}

.answers-list {
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.answer-card {
  padding: 20px;
  border: 1px solid var(--line-soft);
  border-radius: 8px;
  background: var(--surface-base);
  transition: all 0.2s;
}

.answer-card.highlight {
  border-color: var(--brand-primary);
  box-shadow: 0 0 0 3px rgba(0, 127, 255, 0.1);
  background: #f0f7ff;
}

.answer-meta {
  display: flex;
  gap: 16px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.answer-content-wrapper {
  position: relative;
  margin-bottom: 12px;
}

.answer-content {
  font-size: 15px;
  line-height: 1.6;
  color: var(--text-strong);
  white-space: pre-wrap;
  padding-right: 140px;
  min-height: 40px;
}

.answer-actions {
  position: absolute;
  bottom: 0;
  right: 0;
  display: flex;
  gap: 16px;
  align-items: center;
}

.text-action-btn {
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  font-size: 14px;
  color: var(--brand-primary);
  transition: all 0.2s;
  font-weight: 500;
  white-space: nowrap;
}

.text-action-btn:hover {
  color: var(--brand-primary);
  opacity: 0.8;
  text-decoration: underline;
}

.text-action-btn:active {
  opacity: 0.6;
}

.delete-text-btn {
  color: #dc3545;
}

.delete-text-btn:hover {
  color: #c82333;
  text-decoration: underline;
}

.comment-header-actions,
.reply-header-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.comment-delete-btn {
  background: none;
  border: none;
  padding: 4px 8px;
  cursor: pointer;
  font-size: 12px;
  color: #dc3545;
  transition: all 0.2s;
  border-radius: 4px;
}

.comment-delete-btn:hover {
  background: rgba(220, 53, 69, 0.1);
  color: #c82333;
}

.comments-section {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid var(--line-soft);
}

.comment-item {
  margin-bottom: 12px;
}

.comment-meta {
  display: flex;
  gap: 12px;
  margin-bottom: 6px;
  font-size: 12px;
  color: var(--text-muted);
}

.comment-content {
  font-size: 14px;
  color: var(--text-secondary);
  line-height: 1.5;
  margin-bottom: 8px;
}

.comment-input-section {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid var(--line-soft);
}

.comment-textarea {
  width: 100%;
  padding: 10px;
  border: 1px solid var(--line-soft);
  border-radius: 6px;
  font-size: 14px;
  font-family: inherit;
  resize: vertical;
  margin-bottom: 10px;
}

.comment-input-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.ghost-btn {
  padding: 8px 16px;
  background: transparent;
  color: var(--text-secondary);
  border: 1px solid var(--line-soft);
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
}

.ghost-btn:hover {
  background: var(--surface-muted);
  border-color: var(--text-muted);
}

.comment-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 6px;
}

.comment-author {
  font-weight: 500;
  color: var(--text-strong);
}

.comment-time {
  color: var(--text-muted);
}

.comment-action-btn {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 4px 8px;
  background: transparent;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 12px;
  color: var(--text-muted);
  transition: all 0.2s;
}

.comment-action-btn:hover {
  background: var(--surface-muted);
  color: var(--text-strong);
}

.comment-action-btn .action-icon {
  width: 12px;
  height: 12px;
}

.reply-btn {
  padding: 4px 8px;
  background: transparent;
  border: none;
  color: var(--brand-primary);
  cursor: pointer;
  font-size: 12px;
  margin-top: 4px;
}

.reply-btn:hover {
  text-decoration: underline;
}

.reply-input-section {
  margin-top: 12px;
  padding-left: 16px;
  border-left: 2px solid var(--line-soft);
}

.reply-textarea {
  width: 100%;
  padding: 8px;
  border: 1px solid var(--line-soft);
  border-radius: 6px;
  font-size: 13px;
  font-family: inherit;
  resize: vertical;
  margin-bottom: 8px;
}

.reply-input-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.replies-section {
  margin-top: 12px;
  padding-left: 16px;
  border-left: 2px solid var(--line-soft);
}

.reply-item {
  margin-bottom: 12px;
}

.reply-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 4px;
}

.reply-meta {
  display: flex;
  gap: 8px;
  font-size: 12px;
}

.reply-author {
  color: var(--text-secondary);
  font-weight: 500;
}

.reply-time {
  color: var(--text-muted);
}

.reply-content {
  font-size: 13px;
  color: var(--text-secondary);
  line-height: 1.5;
}

.empty-answers {
  padding: 40px 20px;
  text-align: center;
  color: var(--text-muted);
  font-size: 14px;
}

@media (max-width: 768px) {
  .qa-detail-page {
    padding: 16px;
  }

  .detail-container {
    padding: 20px;
  }

  .question-title {
    font-size: 20px;
  }

  .answer-content {
    padding-right: 0;
    margin-bottom: 8px;
  }

  .answer-actions {
    position: static;
    margin-top: 8px;
    justify-content: flex-end;
  }
}
</style>

