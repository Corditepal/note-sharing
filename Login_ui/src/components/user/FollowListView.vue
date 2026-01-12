<template>
  <div class="follow-list-page">
    <div class="follow-card">
      <header class="follow-header">
        <button 
          v-if="props.userId !== currentUserId" 
          class="back-button" 
          @click="goBack"
        >
          <svg class="back-icon" viewBox="0 0 16 16" fill="currentColor">
            <path fill-rule="evenodd" d="M15 8a.5.5 0 0 0-.5-.5H2.707l3.147-3.146a.5.5 0 1 0-.708-.708l-4 4a.5.5 0 0 0 0 .708l4 4a.5.5 0 0 0 .708-.708L2.707 8.5H14.5A.5.5 0 0 0 15 8z"/>
          </svg>
          <span>返回</span>
        </button>
        <h2 class="page-title">{{ pageTitle }}</h2>
      </header>

      <div class="tabs-section">
        <button 
          class="tab-button" 
          :class="{ active: activeTab === 'followings' }"
          @click="switchTab('followings')"
        >
          关注 ({{ followingsCount }})
        </button>
        <button 
          class="tab-button" 
          :class="{ active: activeTab === 'followers' }"
          @click="switchTab('followers')"
        >
          粉丝 ({{ followersCount }})
        </button>
        <button 
          v-if="props.userId === currentUserId"
          class="tab-button" 
          :class="{ active: activeTab === 'discover' }"
          @click="switchTab('discover')"
        >
          发现用户
        </button>
      </div>

      <div v-if="loading" class="loading-state">
        <div class="loader"></div>
        <p>加载中...</p>
      </div>

      <div v-else-if="error" class="error-state">
        <p>{{ error }}</p>
        <button class="retry-button" @click="loadData">重试</button>
      </div>

      <div v-else class="list-section">
        <!-- 发现用户标签页 -->
        <div v-if="activeTab === 'discover'" class="discover-section">
          <div class="search-user-box">
            <input
              v-model="searchUsername"
              type="text"
              class="user-id-input"
              placeholder="输入用户名，例如：zhangsan"
              @keyup.enter="handleSearchUser"
            />
            <button 
              class="search-user-btn"
              @click="handleSearchUser"
              :disabled="searchingUser"
            >
              <span v-if="searchingUser">搜索中...</span>
              <span v-else>搜索</span>
            </button>
          </div>
          
          <div v-if="searchedUser" class="searched-user-card">
            <div class="user-item">
              <div class="user-avatar">
                <img 
                  :src="getSearchedUserAvatar()" 
                  :alt="searchedUser.username || `用户 ${searchedUser.userId}`"
                  @error="handleAvatarError"
                />
              </div>
              <div class="user-info">
                <div class="user-name-row">
                  <span class="user-name">{{ searchedUser.username || `用户 ${searchedUser.userId}` }}</span>
                  <span 
                    v-if="currentUserId && searchedUser.userId !== currentUserId && isMutualFollowing(searchedUser.userId)" 
                    class="mutual-badge"
                  >
                    互相关注
                  </span>
                </div>
                <div v-if="searchedUser.studentNumber" class="user-username">学号：{{ searchedUser.studentNumber }}</div>
              </div>
              <div class="user-actions">
                <button 
                  v-if="searchedUser.userId !== currentUserId"
                  class="follow-btn"
                  :class="{ following: isFollowingUser(searchedUser.userId) }"
                  @click="toggleFollow(searchedUser.userId)"
                  :disabled="followLoading[searchedUser.userId]"
                >
                  <span v-if="followLoading[searchedUser.userId]">处理中...</span>
                  <span v-else>{{ isFollowingUser(searchedUser.userId) ? '已关注' : '关注' }}</span>
                </button>
              </div>
            </div>
          </div>
          
          <div v-else-if="searchError" class="search-error">
            <p>{{ searchError }}</p>
          </div>
          
          <div v-else class="discover-hint">
            <svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/>
            </svg>
            <p>输入用户名搜索并关注其他用户</p>
          </div>
        </div>
        
        <!-- 关注和粉丝列表 -->
        <div v-else>
          <div v-if="currentList.length === 0" class="empty-state">
            <svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor">
              <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z"/>
            </svg>
            <p>{{ activeTab === 'followings' ? '还没有关注任何人' : '还没有粉丝' }}</p>
          </div>

          <div v-else class="user-list">
            <div 
              v-for="item in currentList" 
              :key="item.userId" 
              class="user-item"
            >
              <div class="user-avatar">
                <img 
                  :src="getUserAvatar(item.userId)" 
                  :alt="`用户 ${item.userId}`"
                  @error="handleAvatarError"
                />
              </div>
              <div class="user-info">
                <div class="user-name-row">
                  <span class="user-name">用户 {{ item.userId }}</span>
                  <span 
                    v-if="currentUserId && item.userId !== currentUserId && isMutualFollowing(item.userId)" 
                    class="mutual-badge"
                  >
                    互相关注
                  </span>
                </div>
                <div class="follow-time">{{ formatTime(item.followTime) }}</div>
              </div>
              <div class="user-actions">
                <button 
                  v-if="activeTab === 'followers' && item.userId !== currentUserId"
                  class="follow-btn"
                  :class="{ following: isFollowingUser(item.userId) }"
                  @click="toggleFollow(item.userId)"
                  :disabled="followLoading[item.userId]"
                >
                  <span v-if="followLoading[item.userId]">处理中...</span>
                  <span v-else>{{ isFollowingUser(item.userId) ? '已关注' : '关注' }}</span>
                </button>
              </div>
            </div>
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
import { ref, computed, onMounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useUserStore } from '@/stores/user'
import { getFollowings, getFollowers, followUser, unfollowUser, isFollowing, isMutualFollow, getUserByUsername } from '@/api/follow'
import { formatTime } from '@/utils/time'
import MessageToast from '@/components/MessageToast.vue'
import { useMessage } from '@/utils/message'
import service from '@/api/request'

const router = useRouter()
const userStore = useUserStore()
const { userInfo } = storeToRefs(userStore)

// 消息提示
const { showToast, toastMessage, toastType, toastDuration, showSuccess, showError, hideMessage } = useMessage()

// Props
const props = defineProps({
  userId: {
    type: Number,
    required: true
  }
})

// 状态
const activeTab = ref('followings')
const loading = ref(false)
const error = ref('')
const followingsList = ref([])
const followersList = ref([])
const followStatusMap = ref({}) // 存储每个用户的关注状态
const mutualFollowMap = ref({}) // 存储每个用户的互相关注状态
const followLoading = ref({}) // 存储每个用户的关注操作加载状态
const searchUsername = ref('') // 搜索的用户名
const searchedUser = ref(null) // 搜索到的用户
const searchingUser = ref(false) // 是否正在搜索用户
const searchError = ref('') // 搜索错误信息

// 计算属性
const currentUserId = computed(() => userInfo.value?.id)

const pageTitle = computed(() => {
  if (props.userId === currentUserId.value) {
    return '我的关注'
  }
  return '关注列表'
})

const followingsCount = computed(() => followingsList.value.length)
const followersCount = computed(() => followersList.value.length)

const currentList = computed(() => {
  return activeTab.value === 'followings' ? followingsList.value : followersList.value
})

// 方法
const goBack = () => {
  router.back()
}

const switchTab = (tab) => {
  activeTab.value = tab
  loadData()
}

const getUserAvatar = (userId) => {
  // 暂时使用默认头像，后续可以根据 userId 获取用户信息
  return '/assets/avatars/avatar.png'
}

const getSearchedUserAvatar = () => {
  // 如果是搜索到的用户，使用其头像URL
  if (searchedUser.value && searchedUser.value.avatarUrl) {
    return searchedUser.value.avatarUrl
  }
  return '/assets/avatars/avatar.png'
}

const handleAvatarError = (event) => {
  event.target.src = '/assets/avatars/avatar.png'
}

const isFollowingUser = (targetUserId) => {
  return followStatusMap.value[targetUserId] === true
}

const isMutualFollowing = (targetUserId) => {
  return mutualFollowMap.value[targetUserId] === true
}

const loadData = async () => {
  if (!props.userId) {
    error.value = '用户ID无效'
    return
  }

  // 确保 userId 是数字类型
  const userId = Number(props.userId)
  if (isNaN(userId) || userId <= 0) {
    error.value = '用户ID无效'
    return
  }

  loading.value = true
  error.value = ''

  try {
    if (activeTab.value === 'followings') {
      const data = await getFollowings(userId)
      followingsList.value = data?.followings || []
      
      // 如果是查看自己的关注列表，检查每个用户的互相关注状态
      const currentId = Number(currentUserId.value)
      if (currentId && userId === currentId) {
        await checkFollowStatus(followingsList.value)
      } else if (currentId) {
        // 查看别人的关注列表时，也检查互相关注状态（如果当前用户也关注了这些人）
        // 这样可以显示"互相关注"标识
        await checkFollowStatus(followingsList.value)
      }
    } else {
      const data = await getFollowers(userId)
      followersList.value = data?.followers || []
      
      // 检查每个粉丝的关注状态（当前用户是否关注了他们）
      if (currentUserId.value) {
        await checkFollowStatus(followersList.value)
      }
    }
  } catch (err) {
    console.error('加载关注列表失败:', err)
    console.error('错误详情:', {
      message: err.message,
      response: err.response,
      status: err.response?.status,
      statusText: err.response?.statusText,
      data: err.response?.data,
      config: err.config,
      request: err.request
    })
    console.error('请求URL:', err.config?.url)
    console.error('请求参数:', err.config?.params)
    console.error('后端返回的错误数据:', JSON.stringify(err.response?.data, null, 2))
    
    // 更详细的错误信息
    let errorMsg = '加载失败，请稍后重试'
    if (err.response) {
      // 有响应但状态码不是 2xx
      if (err.response.status === 400) {
        errorMsg = err.response.data?.message || '请求参数错误，请检查用户ID'
      } else if (err.response.status === 401) {
        errorMsg = '未登录或登录已过期，请重新登录'
      } else if (err.response.status === 404) {
        errorMsg = '用户不存在'
      } else if (err.response.status >= 500) {
        errorMsg = '服务器错误，请稍后重试'
      } else {
        errorMsg = err.response.data?.message || err.response.data?.error || `请求失败 (${err.response.status})`
      }
    } else if (err.request) {
      // 请求已发出但没有收到响应
      errorMsg = '网络错误，请检查网络连接'
    } else {
      // 其他错误
      errorMsg = err.message || '加载失败，请稍后重试'
    }
    
    error.value = errorMsg
    showError(errorMsg)
  } finally {
    loading.value = false
  }
}

// 检查关注状态和互相关注状态
const checkFollowStatus = async (userList) => {
  if (!currentUserId.value) return

  const currentId = Number(currentUserId.value)
  if (!currentId || currentId <= 0) return

  for (const user of userList) {
    const targetId = Number(user.userId)
    if (!targetId || targetId <= 0) continue
    if (targetId === currentId) continue // 跳过自己
    
    try {
      // 检查是否已关注
      const status = await isFollowing(currentId, targetId)
      followStatusMap.value[targetId] = status
      
      // 检查是否互相关注（只有在已关注的情况下才检查）
      if (status) {
        const mutual = await isMutualFollow(currentId, targetId)
        mutualFollowMap.value[targetId] = mutual
      } else {
        mutualFollowMap.value[targetId] = false
      }
    } catch (err) {
      console.error(`检查用户 ${targetId} 的关注状态失败:`, err)
      followStatusMap.value[targetId] = false
      mutualFollowMap.value[targetId] = false
    }
  }
}

const toggleFollow = async (targetUserId) => {
  if (!currentUserId.value) {
    showError('请先登录')
    return
  }

  if (targetUserId === currentUserId.value) {
    showError('不能关注自己')
    return
  }

  const currentlyFollowing = isFollowingUser(targetUserId)
  followLoading.value[targetUserId] = true

  try {
    if (currentlyFollowing) {
      await unfollowUser(currentUserId.value, targetUserId)
      followStatusMap.value[targetUserId] = false
      mutualFollowMap.value[targetUserId] = false // 取消关注后肯定不是互相关注
      showSuccess('取消关注成功')
      // 如果是在发现页面，重新加载关注状态
      if (activeTab.value === 'discover' && searchedUser.value && searchedUser.value.userId === targetUserId) {
        await checkFollowStatus([searchedUser.value])
      }
    } else {
      await followUser(currentUserId.value, targetUserId)
      followStatusMap.value[targetUserId] = true
      // 检查是否互相关注
      try {
        const mutual = await isMutualFollow(currentUserId.value, targetUserId)
        mutualFollowMap.value[targetUserId] = mutual
      } catch (err) {
        console.error('检查互相关注状态失败:', err)
        mutualFollowMap.value[targetUserId] = false
      }
      showSuccess('关注成功')
      // 如果是在发现页面，重新加载关注状态
      if (activeTab.value === 'discover' && searchedUser.value && searchedUser.value.userId === targetUserId) {
        await checkFollowStatus([searchedUser.value])
      }
    }
  } catch (err) {
    const errorMsg = err.message || err.response?.data?.message || '操作失败，请稍后重试'
    showError(errorMsg)
  } finally {
    followLoading.value[targetUserId] = false
  }
}

// 搜索用户
const handleSearchUser = async () => {
  const username = searchUsername.value.trim()
  if (!username) {
    searchError.value = '请输入用户名'
    searchedUser.value = null
    return
  }

  searchingUser.value = true
  searchError.value = ''
  searchedUser.value = null

  try {
    // 调用后端API根据用户名获取用户信息
    const userData = await getUserByUsername(username)
    
    if (!userData || !userData.id) {
      searchError.value = '用户不存在'
      searchedUser.value = null
      return
    }

    // 检查是否是自己
    if (userData.id === currentUserId.value) {
      searchError.value = '不能搜索自己'
      searchedUser.value = null
      return
    }

    // 构造用户对象
    const user = {
      userId: userData.id,
      username: userData.username,
      email: userData.email,
      studentNumber: userData.studentNumber,
      avatarUrl: userData.avatarUrl
    }

    searchedUser.value = user

    // 检查关注状态
    if (currentUserId.value) {
      await checkFollowStatus([user])
    }
  } catch (err) {
    console.error('搜索用户失败:', err)
    const errorMsg = err.response?.data?.error || err.response?.data?.message || err.message || '搜索用户失败'
    if (err.response?.status === 404 || errorMsg.includes('不存在')) {
      searchError.value = '用户不存在'
    } else {
      searchError.value = errorMsg
    }
    searchedUser.value = null
  } finally {
    searchingUser.value = false
  }
}

// 监听 tab 切换
watch(activeTab, (newTab) => {
  if (newTab !== 'discover') {
    loadData()
  } else {
    // 切换到发现页面时，清空搜索状态
    searchUsername.value = ''
    searchedUser.value = null
    searchError.value = ''
  }
})

// 生命周期
onMounted(() => {
  loadData()
})
</script>

<style scoped>
.follow-list-page {
  min-height: 100vh;
  padding: 48px 20px 80px;
  background: var(--surface-muted, #f8faf9);
  display: flex;
  justify-content: center;
}

.follow-card {
  width: min(800px, 100%);
  background: var(--surface-base, #ffffff);
  border-radius: 32px;
  border: 1px solid var(--line-soft, #e8ecec);
  box-shadow: 0 20px 50px rgba(15, 23, 42, 0.08);
  overflow: hidden;
}

.follow-header {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 24px 32px;
  border-bottom: 1px solid var(--line-soft, #e8ecec);
}

.back-button {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border: none;
  background: transparent;
  color: var(--text-secondary, #4b5563);
  font-size: 14px;
  cursor: pointer;
  border-radius: 8px;
  transition: all 0.2s ease;
}

.back-button:hover {
  background: var(--surface-muted, #f8faf9);
  color: var(--brand-primary, #22ee99);
}

.back-icon {
  width: 16px;
  height: 16px;
}

.page-title {
  margin: 0;
  font-size: 24px;
  font-weight: 600;
  color: var(--text-strong, #1f2a37);
}

.tabs-section {
  display: flex;
  gap: 8px;
  padding: 0 32px;
  border-bottom: 1px solid var(--line-soft, #e8ecec);
}

.tab-button {
  padding: 16px 24px;
  border: none;
  background: transparent;
  color: var(--text-secondary, #4b5563);
  font-size: 15px;
  font-weight: 500;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  transition: all 0.2s ease;
  position: relative;
  top: 1px;
}

.tab-button:hover {
  color: var(--brand-primary, #22ee99);
}

.tab-button.active {
  color: var(--brand-primary, #22ee99);
  border-bottom-color: var(--brand-primary, #22ee99);
}

.loading-state,
.error-state,
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 80px 32px;
  text-align: center;
}

.loader {
  width: 40px;
  height: 40px;
  border: 4px solid var(--line-soft, #e8ecec);
  border-top-color: var(--brand-primary, #22ee99);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  margin-bottom: 16px;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.error-state p {
  color: var(--text-danger, #c6534c);
  margin-bottom: 16px;
}

.retry-button {
  padding: 10px 20px;
  border: 1px solid var(--line-soft, #e8ecec);
  background: transparent;
  color: var(--text-secondary, #4b5563);
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
}

.retry-button:hover {
  border-color: var(--brand-primary, #22ee99);
  color: var(--brand-primary, #22ee99);
}

.empty-icon {
  width: 64px;
  height: 64px;
  color: var(--text-muted, #8a9199);
  margin-bottom: 16px;
}

.empty-state p {
  color: var(--text-muted, #8a9199);
  font-size: 15px;
}

.list-section {
  padding: 24px 32px;
}

.user-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.user-item {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 16px;
  border-radius: 16px;
  border: 1px solid var(--line-soft, #e8ecec);
  transition: all 0.2s ease;
}

.user-item:hover {
  background: var(--surface-muted, #f8faf9);
  border-color: var(--brand-primary, #22ee99);
}

.user-avatar {
  width: 48px;
  height: 48px;
  border-radius: 12px;
  overflow: hidden;
  flex-shrink: 0;
  border: 1px solid var(--line-soft, #e8ecec);
}

.user-avatar img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.user-info {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.user-name-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.user-name {
  font-size: 16px;
  font-weight: 600;
  color: var(--text-strong, #1f2a37);
}

.mutual-badge {
  display: inline-flex;
  align-items: center;
  padding: 2px 8px;
  background: rgba(34, 238, 153, 0.1);
  color: var(--brand-primary, #22ee99);
  font-size: 12px;
  font-weight: 500;
  border-radius: 12px;
  border: 1px solid rgba(34, 238, 153, 0.3);
}

.follow-time {
  font-size: 13px;
  color: var(--text-muted, #8a9199);
}

.user-actions {
  flex-shrink: 0;
}

.follow-btn {
  padding: 8px 20px;
  border: 1px solid var(--brand-primary, #22ee99);
  background: var(--brand-primary, #22ee99);
  color: #0b1f14;
  font-size: 14px;
  font-weight: 500;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
  min-width: 80px;
}

.follow-btn:hover:not(:disabled) {
  filter: brightness(0.95);
}

.follow-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.follow-btn.following {
  background: transparent;
  color: var(--text-secondary, #4b5563);
  border-color: var(--line-soft, #e8ecec);
}

.follow-btn.following:hover:not(:disabled) {
  border-color: var(--text-danger, #c6534c);
  color: var(--text-danger, #c6534c);
}

/* 发现用户相关样式 */
.discover-section {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.search-user-box {
  display: flex;
  gap: 12px;
  align-items: center;
}

.user-id-input {
  flex: 1;
  padding: 12px 16px;
  border: 1px solid var(--line-soft, #e8ecec);
  border-radius: 8px;
  font-size: 14px;
  color: var(--text-strong, #1f2a37);
  transition: all 0.2s ease;
}

.user-id-input:focus {
  outline: none;
  border-color: var(--brand-primary, #22ee99);
  box-shadow: 0 0 0 3px rgba(34, 238, 153, 0.1);
}

.search-user-btn {
  padding: 12px 24px;
  border: none;
  background: var(--brand-primary, #22ee99);
  color: #0b1f14;
  font-size: 14px;
  font-weight: 500;
  border-radius: 8px;
  cursor: pointer;
  transition: all 0.2s ease;
  min-width: 80px;
}

.search-user-btn:hover:not(:disabled) {
  filter: brightness(0.95);
}

.search-user-btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.searched-user-card {
  margin-top: 8px;
}

.search-error {
  padding: 16px;
  background: rgba(198, 83, 76, 0.1);
  border: 1px solid rgba(198, 83, 76, 0.3);
  border-radius: 8px;
  color: var(--text-danger, #c6534c);
  text-align: center;
}

.discover-hint {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 80px 32px;
  text-align: center;
}

.discover-hint .empty-icon {
  width: 64px;
  height: 64px;
  color: var(--text-muted, #8a9199);
  margin-bottom: 16px;
}

.discover-hint p {
  color: var(--text-muted, #8a9199);
  font-size: 15px;
}

.user-username {
  font-size: 13px;
  color: var(--text-muted, #8a9199);
  margin-top: 4px;
}

@media (max-width: 640px) {
  .follow-card {
    border-radius: 24px;
  }

  .follow-header,
  .tabs-section,
  .list-section {
    padding: 16px 20px;
  }

  .page-title {
    font-size: 20px;
  }

  .tab-button {
    padding: 12px 16px;
    font-size: 14px;
  }
}
</style>
