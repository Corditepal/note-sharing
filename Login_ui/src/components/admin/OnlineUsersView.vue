<template>
  <div class="online-users-view">
    <div class="view-header">
      <h2 class="page-title">在线用户管理</h2>
      <button class="refresh-btn" @click="loadOnlineUsers">刷新</button>
    </div>
    
    <div class="stats-bar">
      <div class="stat-item">
        <span class="stat-label">当前在线人数：</span>
        <span class="stat-value">{{ onlineCount }}</span>
      </div>
    </div>

    <div class="table-container">
      <table class="data-table">
        <thead>
          <tr>
            <th>用户ID</th>
            <th>用户名</th>
            <th>邮箱</th>
            <th>学号</th>
            <th>上线时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading">
            <td colspan="5" class="loading-cell">加载中...</td>
          </tr>
          <tr v-else-if="onlineUsers.length === 0">
            <td colspan="5" class="empty-cell">暂无在线用户</td>
          </tr>
          <tr v-else v-for="user in onlineUsers" :key="user.id || user.userId">
            <td>{{ user.id || user.userId }}</td>
            <td>{{ user.username || '-' }}</td>
            <td>{{ user.email || '-' }}</td>
            <td>{{ user.studentNumber || '-' }}</td>
            <td>{{ formatTime(user.loginTime) }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { getOnlineUsers } from '../../api/admin'

const onlineCount = ref(0)
const onlineUsers = ref([])
const loading = ref(false)

const loadOnlineUsers = async () => {
  loading.value = true
  try {
    // 获取在线用户列表，使用列表长度作为在线人数统计
    const usersRes = await getOnlineUsers()
    
    // 处理在线用户列表 - 确保是数组类型
    let usersList = []
    if (usersRes) {
      // 如果 usersRes 有 data 属性，使用 data
      if (usersRes.data !== undefined) {
        usersList = usersRes.data
      } else if (Array.isArray(usersRes)) {
        // 如果 usersRes 本身就是数组，直接使用
        usersList = usersRes
      }
    }
    
    // 确保 usersList 是数组类型
    if (!Array.isArray(usersList)) {
      console.warn('在线用户数据格式异常，不是数组:', usersRes)
      usersList = []
    }
    
    // 设置用户列表
    onlineUsers.value = usersList
    
    // 使用用户列表长度作为在线人数统计
    onlineCount.value = usersList.length
  } catch (error) {
    console.error('加载在线用户失败:', error)
    onlineUsers.value = []
    onlineCount.value = 0
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

let refreshTimer = null

onMounted(() => {
  loadOnlineUsers()
  // 每10秒自动刷新
  refreshTimer = setInterval(loadOnlineUsers, 10000)
})

onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
})
</script>

<style scoped>
.online-users-view {
  max-width: 1200px;
  margin: 0 auto;
}

.view-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 20px;
}

.page-title {
  font-size: 24px;
  font-weight: 600;
  color: #333;
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

.stats-bar {
  background: white;
  padding: 16px 20px;
  border-radius: 8px;
  margin-bottom: 20px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
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
  font-size: 18px;
  font-weight: 600;
  color: #007FFF;
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

.loading-cell,
.empty-cell {
  text-align: center;
  color: #999;
  padding: 40px;
}

@media (max-width: 768px) {
  .data-table {
    font-size: 12px;
  }
  
  .data-table th,
  .data-table td {
    padding: 8px;
  }
}
</style>
