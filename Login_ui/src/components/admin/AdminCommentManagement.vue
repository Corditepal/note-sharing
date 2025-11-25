<template>
  <section class="module-card">
    <div class="module-head">
      <div>
        <p class="section-label">Comments</p>
        <h2>评论信息管理</h2>
      </div>
      <div class="module-actions multi">
        <div class="search-bar">
          <span aria-hidden="true">🗓</span>
          <input v-model="commentSearch.date" type="date" />
          <button type="button" class="text-link" @click="commentSearch.date = ''">重置</button>
        </div>
        <div class="search-bar">
          <span aria-hidden="true">👤</span>
          <input
            v-model="commentSearch.publisher"
            type="text"
            placeholder="输入发布人搜索"
          />
          <button type="button" class="text-link" @click="commentSearch.publisher = ''">清空</button>
        </div>
      </div>
    </div>

    <div class="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>评论ID</th>
            <th>评论正文</th>
            <th>评论用户</th>
            <th>被评论笔记</th>
            <th>父评论ID</th>
            <th>点赞数</th>
            <th>回复数</th>
            <th>评论时间</th>
            <th class="actions-col">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="comment in filteredComments" :key="comment.id">
            <td>{{ comment.id }}</td>
            <td class="prism-text">{{ comment.content }}</td>
            <td>{{ comment.username }}</td>
            <td>{{ comment.note }}</td>
            <td>{{ comment.parentId || '-' }}</td>
            <td>{{ comment.likes }}</td>
            <td>{{ comment.replies }}</td>
            <td>{{ comment.createdAt }}</td>
            <td class="actions-col">
              <button type="button" class="pill-btn danger" @click="openDeleteConfirm(comment)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="showDeleteModal" class="modal" @click.self="closeDeleteModal">
      <div class="modal-card">
        <h3>确认删除</h3>
        <p>该操作不可恢复，是否删除该评论？</p>
        <div class="modal-actions">
          <button type="button" class="ghost-btn" @click="closeDeleteModal">取消</button>
          <button type="button" class="pill-btn danger" @click="confirmDelete">确认删除</button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup>
import { computed, ref } from 'vue'

/**
 * API 占位：
 * - GET /api/admin/comments?date=YYYY-MM-DD&publisher=xxx
 * - DELETE /api/admin/comments/{id}
 */
const commentSearch = ref({ date: '', publisher: '' })
const comments = ref([
  {
    id: 'C001',
    content: '这篇笔记很有启发性，尤其是案例部分。',
    username: '陈明',
    note: '前端性能优化',
    parentId: '',
    likes: 12,
    replies: 3,
    createdAt: '2025-11-18'
  },
  {
    id: 'C002',
    content: '是否可以补充下数据库层的优化方案？',
    username: '高洁',
    note: '后端架构演进',
    parentId: 'C001',
    likes: 5,
    replies: 1,
    createdAt: '2025-11-17'
  }
])

const filteredComments = computed(() => {
  const sorted = [...comments.value].sort((a, b) => a.id.localeCompare(b.id))
  return sorted.filter((comment) => {
    const matchPublisher = commentSearch.value.publisher
      ? comment.username.includes(commentSearch.value.publisher.trim())
      : true
    const matchDate = commentSearch.value.date
      ? comment.createdAt === commentSearch.value.date
      : true
    return matchPublisher && matchDate
  })
})

const showDeleteModal = ref(false)
const pendingDeleteId = ref('')

const openDeleteConfirm = (comment) => {
  pendingDeleteId.value = comment.id
  showDeleteModal.value = true
}

const closeDeleteModal = () => {
  showDeleteModal.value = false
  pendingDeleteId.value = ''
}

const confirmDelete = () => {
  // TODO: 调用 DELETE /api/admin/comments/{id} 删除评论
  comments.value = comments.value.filter((item) => item.id !== pendingDeleteId.value)
  closeDeleteModal()
}
</script>

<style scoped>
@import './shared-admin.css';
</style>

