<template>
  <section class="module-card">
    <div class="module-head">
      <div>
        <p class="section-label">User</p>
        <h2>用户信息管理</h2>
      </div>
      <div class="module-actions">
        <div class="search-bar">
          <span aria-hidden="true">🔍</span>
          <input
            v-model="userSearch"
            type="text"
            placeholder="输入学号搜索用户"
          />
          <button type="button" class="text-link" @click="userSearch = ''">清空</button>
        </div>
        <button type="button" class="ghost-btn">导出用户</button>
      </div>
    </div>

    <div class="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>用户ID</th>
            <th>用户名</th>
            <th>学号</th>
            <th>邮箱</th>
            <th class="actions-col">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="user in filteredUsers" :key="user.id">
            <td>{{ user.id }}</td>
            <td>{{ user.username }}</td>
            <td>{{ user.studentId }}</td>
            <td>{{ user.email }}</td>
            <td class="actions-col">
              <button type="button" class="pill-btn ghost" @click="openUserModal(user)">修改</button>
              <button type="button" class="pill-btn danger" @click="openDeleteConfirm(user)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 修改弹窗 -->
    <div v-if="showUserModal" class="modal" @click.self="closeUserModal">
      <div class="modal-card">
        <h3>修改用户信息</h3>
        <p class="muted">用户ID：{{ editUserForm.id }}</p>
        <form @submit.prevent="saveUserEdit">
          <label>
            用户名
            <input v-model="editUserForm.username" type="text" required />
          </label>
          <label>
            学号
            <input v-model="editUserForm.studentId" type="text" required />
          </label>
          <label>
            邮箱
            <input v-model="editUserForm.email" type="email" required />
          </label>
          <div class="modal-actions">
            <button type="button" class="ghost-btn" @click="closeUserModal">取消</button>
            <button type="submit" class="pill-btn primary">保存修改</button>
          </div>
        </form>
      </div>
    </div>

    <!-- 删除弹窗 -->
    <div v-if="showDeleteModal" class="modal" @click.self="closeDeleteModal">
      <div class="modal-card">
        <h3>确认删除</h3>
        <p>该操作不可恢复，是否删除该用户？</p>
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
 * - GET /api/admin/users?studentId=xxx
 * - PUT /api/admin/users/{id}
 * - DELETE /api/admin/users/{id}
 */
const userSearch = ref('')
const users = ref([
  { id: '001', username: '李华', studentId: '2021001001', email: 'lihua@example.com' },
  { id: '002', username: '王婷', studentId: '2021001002', email: 'wangting@example.com' },
  { id: '003', username: '赵强', studentId: '2021001003', email: 'zhaoqiang@example.com' }
])

const filteredUsers = computed(() => {
  const keyword = userSearch.value.trim()
  const sorted = [...users.value].sort((a, b) => Number(a.id) - Number(b.id))
  if (!keyword) return sorted
  return sorted.filter((user) => user.studentId.includes(keyword))
})

const showUserModal = ref(false)
const showDeleteModal = ref(false)
const editUserForm = ref({ id: '', username: '', studentId: '', email: '' })
const pendingDeleteId = ref('')

const openUserModal = (user) => {
  editUserForm.value = { ...user }
  showUserModal.value = true
}

const closeUserModal = () => {
  showUserModal.value = false
}

const saveUserEdit = () => {
  // TODO: 调用 PUT /api/admin/users/{id} 更新用户
  const idx = users.value.findIndex((item) => item.id === editUserForm.value.id)
  if (idx !== -1) {
    users.value[idx] = { ...editUserForm.value }
  }
  closeUserModal()
}

const openDeleteConfirm = (user) => {
  pendingDeleteId.value = user.id
  showDeleteModal.value = true
}

const closeDeleteModal = () => {
  showDeleteModal.value = false
  pendingDeleteId.value = ''
}

const confirmDelete = () => {
  // TODO: 调用 DELETE /api/admin/users/{id} 删除用户
  users.value = users.value.filter((user) => user.id !== pendingDeleteId.value)
  closeDeleteModal()
}
</script>

<style scoped>
@import './shared-admin.css';
</style>

