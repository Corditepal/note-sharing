<template>
  <div class="main-shell">
    <header class="main-header app-layout">
      <div class="brand-logo-block">
        <span class="brand-logo-text">Folio</span>
      </div>

      <nav class="main-nav-links" aria-label="主要导航">
        <button
            v-for="tab in tabs"
            :key="tab.value"
            type="button"
            :class="['nav-link-item', { active: currentTab === tab.value }]"
            @click="currentTab = tab.value"
        >
          {{ tab.label }}
        </button>
      </nav>

      <div class="search-container">
        <input
            v-model="searchKeyword"
            type="text"
            class="search-input"
            placeholder="编译原理-词法分析器"
            aria-label="搜索框"
            @keyup.enter="handleSearch"
        />
        <button class="search-button" type="button" aria-label="搜索" @click="handleSearch">
          <span class="search-icon">🔍</span>
        </button>
      </div>

      <div class="header-actions">
      <button class="ask-button" type="button" @click="handleAskClick">
          <span class="icon">+</span> 提问
        </button>

        <div class="action-icon-wrapper message-wrapper" @click="togglePrivateMessagePanel">
          <img
              src="/assets/icons/icon-private-message.svg"
              alt="私信"
              class="action-image-icon private-message-icon-img"
          />
          <span v-if="privateMessageUnreadTotal > 0" class="badge">
            {{ privateMessageUnreadTotal > 99 ? '99+' : privateMessageUnreadTotal }}
          </span>
          <span class="action-text">私信</span>
        </div>

        <div class="action-icon-wrapper notification-wrapper" @click="toggleNotificationPanel">
          <img
              src="/assets/icons/icon-notification.svg"
              alt="消息"
              class="action-image-icon notification-icon-img"
          />
          <span v-if="notificationUnreadTotal > 0" class="badge">
            {{ notificationUnreadTotal > 99 ? '99+' : notificationUnreadTotal }}
          </span>
          <span class="action-text">消息</span>
        </div>

        <div class="user-avatar-block" @click="goToProfile">
          <img 
            :src="userAvatarUrl" 
            alt="用户头像" 
            class="user-avatar-img"
          />
        </div>
      </div>

    </header>

    <main class="main-content">
      <section v-if="editingNotebookId">
        <NoteEditorView
            v-if="editingNotebookId"

            :spaceId="editingSpaceId"
            :notebookId="editingNotebookId"
            :notebookName="editingNotebookName"
            :notebookList="editingNotebookList"
            :initialNoteId="editingNoteId"
            @close="handleCloseEditor"
            @note-selected="handleNoteSelected"
        />
      </section>

      <section v-else-if="currentTab === 'search'">
        <keep-alive>
          <SearchView 
            ref="searchViewRef"
            :initialKeyword="searchKeywordFromRoute" 
            @open-note-detail="handleOpenNoteDetail"
          />
        </keep-alive>
      </section>
      <section v-else-if="currentTab === 'recommend'">
        <keep-alive>
          <RecommendView 
            ref="recommendViewRef"
            @open-note-detail="handleOpenNoteDetail" 
          />
        </keep-alive>
      </section>
      <section v-else-if="currentTab === 'hot'">
        <keep-alive>
          <HotView 
            ref="hotViewRef"
            @open-note-detail="handleOpenNoteDetail" 
          />
        </keep-alive>
      </section>
      <section v-else-if="currentTab === 'note-detail' && viewingNoteId">
        <NoteDetailView 
          :noteId="viewingNoteId" 
          :initialStats="noteDetailStats"
          :initialTitle="noteDetailTitle"
          @stats-updated="handleStatsUpdated"
        />
      </section>
      <section v-else-if="currentTab === 'follow' && currentUserId">
        <FollowListView :userId="Number(currentUserId)" />
      </section>
      <section v-else-if="currentTab === 'circle'">
        <QACircleView ref="qaRef" />
      </section>
      <section v-else-if="currentTab === 'qa-detail' && route.query.questionId">
        <QADetailView 
          :questionId="route.query.questionId"
          :answerId="route.query.answerId"
          :commentId="route.query.commentId"
          :replyId="route.query.replyId"
        />
      </section>
      <section v-else-if="currentTab === 'workspace'">
        <WorkspaceView 
          :initialWorkspaceId="selectedWorkspaceId"
          @open-notebook="handleOpenNotebook"
          @workspace-selected="handleWorkspaceSelected"
        />
      </section>
      <section v-else-if="currentTab === 'favorites'">
        <FavoritesView />
      </section>
      <section v-else-if="currentTab === 'comments'">
        <CommentsView />
      </section>
      <section v-else-if="currentTab === 'follow-list' && route.query.userId">
        <FollowListView :userId="Number(route.query.userId)" />
      </section>
      <section v-else-if="currentTab === 'user-notes' && route.query.userId">
        <UserNotesView 
          :userId="Number(route.query.userId)"
          @open-note-detail="handleOpenNoteDetail"
        />
      </section>
      <section v-else>
        <ProfileView />
      </section>
    </main>

    <!-- 系统通知面板 -->
    <div v-if="showNotificationPanel" class="notification-panel">
      <div class="notification-panel-header">
        <span class="notification-title">消息中心</span>
        <button
          type="button"
          class="notification-mark-all"
          @click.stop="handleMarkAllNotificationsRead"
        >
          全部标记为已读
        </button>
      </div>
      <div class="notification-list" v-if="notifications.length">
        <button
          v-for="item in notifications"
          :key="item.id"
          type="button"
          class="notification-item"
          :class="{ 'is-unread': item.read === false }"
          @click.stop="handleNotificationClick(item)"
        >
          <div class="notification-avatar">
            <img
              :src="getNotificationAvatarUrl(item.actorId)"
              alt="用户头像"
              @error="handleNotificationAvatarError"
            />
          </div>
          <div class="notification-main">
            <div class="notification-message">
              {{ item.message }}
            </div>
            <div class="notification-meta">
              <span class="notification-time">
                {{ new Date(item.createdAt).toLocaleString() }}
              </span>
              <span v-if="item.read === false" class="notification-dot"></span>
            </div>
          </div>
        </button>
      </div>
      <div v-else class="notification-empty">
        暂无新的消息
      </div>
    </div>

    <PrivateMessagePanel
      v-model:visible="showPrivateMessagePanel"
      @unread-updated="handleUnreadUpdated"
    />
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onBeforeUnmount, nextTick, computed } from 'vue'
import SearchView from '../components/user/SearchView.vue'
import WorkspaceView from '../components/user/WorkspaceView.vue'
import ProfileView from '../components/user/ProfileView.vue'
import FavoritesView from '../components/user/FavoritesView.vue'
import CommentsView from '../components/user/CommentsView.vue'
import NoteEditorView from '../components/user/NoteEditorView.vue'
import NoteDetailView from '../components/user/NoteDetailView.vue'
import HotView from '../components/user/HotView.vue'
import RecommendView from '../components/user/RecommendView.vue'
import QACircleView from '../components/user/QACircleView.vue'
import QADetailView from '../components/user/QADetailView.vue'
import FollowListView from '../components/user/FollowListView.vue'
import UserNotesView from '../components/user/UserNotesView.vue'
import PrivateMessagePanel from '../components/user/PrivateMessagePanel.vue'
import { useRouter, useRoute } from 'vue-router'
import service from '../api/request'
import { useUserStore } from '@/stores/user'
import { storeToRefs } from 'pinia'
import { fetchUnreadTotal as fetchConversationUnreadTotal } from '@/api/conversation'
import {
  fetchNotifications,
  fetchNotificationUnreadTotal,
  markNotificationAsRead,
  markAllNotificationsAsRead
} from '@/api/notification'
import { getUserById } from '@/api/follow'

const router = useRouter()
const route = useRoute()
const userStore = useUserStore()
const { userInfo } = storeToRefs(userStore)
const BASE_PATH = "/noting"

// 计算用户头像URL
const userAvatarUrl = computed(() => {
  return userInfo.value.avatarUrl || '/assets/avatars/avatar.png'
})

// 当前用户ID
const currentUserId = computed(() => userInfo.value?.id)

// 私信面板控制与未读数
const showPrivateMessagePanel = ref(false)
const privateMessageUnreadTotal = ref(0)
let privateMessageTimer = null

// 通知面板 & 未读数
const showNotificationPanel = ref(false)
const notificationUnreadTotal = ref(0)
const notifications = ref([])
const notificationUserInfoMap = ref({}) // {actorId: { username, avatarUrl }}
let notificationTimer = null

const refreshPrivateMessageUnread = async () => {
  if (!currentUserId.value) {
    privateMessageUnreadTotal.value = 0
    return
  }
  try {
    const total = await fetchConversationUnreadTotal(currentUserId.value)
    privateMessageUnreadTotal.value = total || 0
  } catch (e) {
    console.error('加载私信未读数失败', e)
  }
}

const togglePrivateMessagePanel = async () => {
  showPrivateMessagePanel.value = !showPrivateMessagePanel.value
  if (showPrivateMessagePanel.value) {
    await refreshPrivateMessageUnread()
  }
}

const handleUnreadUpdated = (total) => {
  privateMessageUnreadTotal.value = total || 0
}

const getNotificationAvatarUrl = (actorId) => {
  if (!actorId) return '/assets/avatars/avatar.png'
  const info = notificationUserInfoMap.value[actorId]
  if (info && info.avatarUrl) return info.avatarUrl
  return '/assets/avatars/avatar.png'
}

const handleNotificationAvatarError = (event) => {
  event.target.src = '/assets/avatars/avatar.png'
}

const loadNotificationUserInfo = async (actorId, forceRefresh = false) => {
  if (!actorId) return
  if (!forceRefresh && notificationUserInfoMap.value[actorId]) return
  try {
    const res = await getUserById(actorId)
    const data = res.data || res
    if (data) {
      notificationUserInfoMap.value[actorId] = {
        username: data.username,
        avatarUrl: data.avatarUrl
      }
    }
  } catch (e) {
    console.error('加载通知用户信息失败', e)
  }
}

const refreshNotificationUnread = async () => {
  if (!currentUserId.value) {
    notificationUnreadTotal.value = 0
    return
  }
  try {
    const total = await fetchNotificationUnreadTotal(currentUserId.value)
    notificationUnreadTotal.value = total || 0
  } catch (e) {
    console.error('加载通知未读数失败', e)
  }
}

const loadNotifications = async (forceRefreshAvatars = false) => {
  if (!currentUserId.value) {
    notifications.value = []
    return
  }
  try {
    const list = await fetchNotifications(currentUserId.value)
    const arr = Array.isArray(list) ? list : []
    notifications.value = arr

    // 加载所有通知发送者的头像信息，如果强制刷新则忽略缓存
    const actorIds = Array.from(new Set(arr.map(n => n.actorId).filter(Boolean)))
    await Promise.all(actorIds.map(id => loadNotificationUserInfo(id, forceRefreshAvatars)))
  } catch (e) {
    console.error('加载通知列表失败', e)
  }
}

const toggleNotificationPanel = async () => {
  showNotificationPanel.value = !showNotificationPanel.value
  if (showNotificationPanel.value) {
    // 打开通知面板时，强制刷新所有用户的头像信息以显示最新头像
    await Promise.all([loadNotifications(true), refreshNotificationUnread()])
  }
}

const handleNotificationClick = async (item) => {
  if (!item) return

  // 根据 targetType 跳转到对应详情页
  if (item.targetType === 'NOTE' && item.targetId) {
    const noteId = Number(item.targetId)
    if (!Number.isNaN(noteId)) {
      handleOpenNoteDetail({ noteId, fromTab: currentTab.value || 'hot' })
    }
  } else if (item.targetType === 'QUESTION' && item.targetId) {
    // 问答：跳转到问题详情页，并携带回答 / 评论 / 回复 ID，便于自动滚动
    router.replace({
      path: route.path,
      query: {
        ...route.query,
        tab: 'qa-detail',
        questionId: item.targetId,
        answerId: item.answerId ?? undefined,
        commentId: item.commentId ?? undefined,
        replyId: item.replyId ?? undefined
      }
    })
    currentTab.value = 'qa-detail'
  }

  showNotificationPanel.value = false

  // 标记为已读并刷新未读数
  try {
    if (!item.read && item.id) {
      await markNotificationAsRead(item.id)
      await refreshNotificationUnread()
    }
  } catch (e) {
    console.error('标记通知已读失败', e)
  }
}

const handleMarkAllNotificationsRead = async () => {
  if (!currentUserId.value) return
  try {
    await markAllNotificationsAsRead(currentUserId.value)
    notificationUnreadTotal.value = 0
    // 本地列表全部置为已读
    notifications.value = notifications.value.map(n => ({
      ...n,
      read: true
    }))
  } catch (e) {
    console.error('全部标记通知已读失败', e)
  }
}

const tabs = [
  { value: 'follow', label: '关注', desc: 'Follow' },
  { value: 'favorites', label: '收藏', desc: 'Favorites' },
  { value: 'recommend', label: '推荐', desc: 'Recommend' },
  { value: 'hot', label: '热榜', desc: 'Hot' },
  { value: 'circle', label: '问答', desc: 'Q&A' },
  { value: 'workspace', label: '我的笔记', desc: 'WorkspaceView' }
]

// 搜索相关状态
const searchKeyword = ref('')
const searchKeywordFromRoute = ref('')

// 笔记详情页相关状态（需要在 restoreNoteDetailFromRoute 之前定义）
const viewingNoteId = ref(null) // 当前查看的笔记详情ID（用于note-detail tab）
const noteDetailStats = ref(null) // 笔记详情页的统计信息（从搜索结果传递过来）
const noteDetailTitle = ref(null) // 笔记详情页的标题（从搜索结果传递过来）

// workspace tab 相关状态（需要在 restoreWorkspaceFromRoute 之前定义）
const selectedWorkspaceId = ref(null) // 当前选中的笔记空间ID（在workspace tab时）

// 从 URL 查询参数中读取 tab，如果没有则使用默认值
const getTabFromRoute = () => {
  const tabFromQuery = route.query.tab
  // 验证 tab 值是否有效（包括search tab和note-detail tab）
  const validTabs = [...tabs.map(t => t.value), 'search', 'profile', 'note-detail', 'qa-detail', 'follow-list', 'user-notes']
  if (tabFromQuery && validTabs.includes(tabFromQuery)) {
    return tabFromQuery
  }
  return 'hot' // 默认值改为热榜
}

const currentTab = ref(getTabFromRoute())

// 当 currentTab 改变时，同步更新 URL 查询参数
watch(currentTab, (newTab, oldTab) => {
  console.log('[MainView] currentTab 变化:', oldTab, '->', newTab, '当前 route.query.tab:', route.query.tab)
  
  // 如果 URL 中的 tab 已经和 newTab 一致，不需要更新（避免循环）
  if (route.query.tab === newTab) {
    console.log('[MainView] URL tab 已经是', newTab, '，跳过更新')
    return
  }
  
  // 如果切换到 note-detail，并且 URL 中没有 fromTab，说明可能是通过 handleOpenNoteDetail 触发的
  // 这种情况下，让 handleOpenNoteDetail 来处理 URL 更新，避免覆盖 fromTab
  if (newTab === 'note-detail' && !route.query.fromTab) {
    console.log('[MainView] 切换到 note-detail 但没有 fromTab，等待 handleOpenNoteDetail 处理')
    return
  }
  
  const newQuery = { ...route.query, tab: newTab }
  
  // 如果切换到 note-detail，保留 fromTab（如果存在）
  if (newTab === 'note-detail' && route.query.fromTab) {
    newQuery.fromTab = route.query.fromTab
  }
  
  // 当切换到搜索 tab 时，如果没有 keyword，清除 searchType
  // 这样确保从主页搜索框搜索时，searchType 会被设置为 'notes'
  // 但如果是从详情页返回（有 keyword），则保留 searchType
  if (newTab === 'search' && !route.query.keyword) {
    delete newQuery.searchType
    // 清除 noteId，避免在搜索页时还保留旧的 noteId
    delete newQuery.noteId
    delete newQuery.title
    delete newQuery.fileType
  }
  // 如果是从详情页返回到搜索页，确保保留搜索参数
  else if (newTab === 'search' && oldTab === 'note-detail' && route.query.keyword) {
    // 保留 keyword 和 searchType，但清除 noteId 相关参数
    delete newQuery.noteId
    delete newQuery.title
    delete newQuery.fileType
  }
  
  // 如果是从详情页返回到其他页面，清除详情页相关参数
  if (oldTab === 'note-detail' && newTab !== 'note-detail' && newTab !== 'search') {
    delete newQuery.fromTab
    delete newQuery.noteId
    delete newQuery.title
    delete newQuery.fileType
  }
  
  // 当切换到 user-notes 或 follow-list 时，确保保留 userId 参数
  if ((newTab === 'user-notes' || newTab === 'follow-list') && route.query.userId) {
    newQuery.userId = route.query.userId
  }
  
  console.log('[MainView] 更新 URL，newQuery:', newQuery)
  router.replace({
    path: route.path,
    query: newQuery
  })
  // 当切换离开 note-detail tab 时，清除笔记详情相关状态
  if (oldTab === 'note-detail' && newTab !== 'note-detail') {
    viewingNoteId.value = null
    noteDetailStats.value = null
    noteDetailTitle.value = null
  }
  // 当切换到非 workspace tab 时，自动关闭编辑器
  if (newTab !== 'workspace' && editingNotebookId.value !== null) {
    // 保存笔记所在的空间ID，用于回到 workspace tab 时选中
    const spaceIdBeforeClose = editingSpaceId.value
    
    editingNotebookId.value = null
    editingSpaceId.value = null
    editingNotebookName.value = null
    editingNotebookList.value = []
    editingNoteId.value = null
    
    // 清除 URL 中的编辑器相关参数
    const newQuery = { ...route.query }
    delete newQuery.notebookId
    delete newQuery.spaceId
    delete newQuery.notebookName
    delete newQuery.noteId
    
    router.replace({
      path: route.path,
      query: newQuery
    })
  }
  // 当切换回 workspace tab 时，如果 URL 中有 notebookId，恢复编辑器状态
  else if (newTab === 'workspace' && editingNotebookId.value === null) {
    const notebookIdFromQuery = route.query.notebookId
    const spaceIdFromQuery = route.query.spaceId
    if (notebookIdFromQuery && spaceIdFromQuery) {
      // 异步恢复编辑器状态，但不设置 currentTab（因为已经是 workspace 了）
      restoreEditorFromRoute(false).catch(err => {
        console.error('恢复编辑器状态失败:', err)
      })
    }
  }
})

// 从 URL 恢复笔记详情页状态（需要在 watch 之前定义，因为 watch 使用了 immediate: true）
const restoreNoteDetailFromRoute = () => {
  if (currentTab.value === 'note-detail') {
    const noteIdFromQuery = route.query.noteId
    if (noteIdFromQuery) {
      const noteId = Number(noteIdFromQuery)
      if (!isNaN(noteId) && noteId > 0) {
        // 只有当 viewingNoteId 为空或与 URL 中的 noteId 不一致时才更新
        // 这样可以避免覆盖 handleOpenNoteDetail 中已经设置的值
        if (!viewingNoteId.value || viewingNoteId.value !== noteId) {
          viewingNoteId.value = noteId
          // 从 URL 恢复标题
          noteDetailTitle.value = route.query.title || null
        }
      }
    }
  }
}

// 从 URL 恢复 workspace tab 的选中空间（需要在 watch 之前定义，因为 watch 使用了 immediate: true）
const restoreWorkspaceFromRoute = () => {
  // 只有在 workspace tab 时才恢复空间ID
  if (currentTab.value === 'workspace') {
    const workspaceIdFromQuery = route.query.workspaceId
    if (workspaceIdFromQuery) {
      const workspaceId = Number(workspaceIdFromQuery)
      if (!isNaN(workspaceId)) {
        selectedWorkspaceId.value = workspaceId
      }
    }
  }
}

// 监听路由变化，从 URL 中恢复 tab 状态（处理浏览器前进/后退）
watch(() => route.query.tab, (newTab, oldTab) => {
  console.log('[MainView] 路由 tab 变化:', oldTab, '->', newTab, '当前 currentTab:', currentTab.value, '完整 query:', JSON.stringify(route.query))
  if (newTab) {
    const validTabs = [...tabs.map(t => t.value), 'search', 'profile', 'note-detail', 'qa-detail', 'follow-list', 'user-notes']
    if (validTabs.includes(newTab)) {
      // 强制更新 currentTab，确保与 URL 同步
      if (currentTab.value !== newTab) {
        console.log('[MainView] 更新 currentTab 从', currentTab.value, '到', newTab)
        currentTab.value = newTab
      } else {
        console.log('[MainView] currentTab 已经是', newTab, '，但强制检查是否需要更新')
        // 即使值相同，也确保状态正确
      }
      // 当切换到 workspace tab 时，恢复选中的空间
      if (newTab === 'workspace') {
        restoreWorkspaceFromRoute()
      }
      // 当切换到 note-detail tab 时，恢复笔记ID
      if (newTab === 'note-detail') {
        restoreNoteDetailFromRoute()
      }
    } else {
      console.warn('[MainView] 无效的 tab 值:', newTab)
    }
  } else {
    console.log('[MainView] URL 中没有 tab 参数')
  }
}, { immediate: true }) // 改为 immediate: true，确保初始化时也能触发

// 监听 workspaceId 变化，从 URL 中恢复空间状态
watch(() => route.query.workspaceId, (newWorkspaceId) => {
  if (currentTab.value === 'workspace' && newWorkspaceId) {
    const workspaceId = Number(newWorkspaceId)
    if (!isNaN(workspaceId)) {
      selectedWorkspaceId.value = workspaceId
    }
  }
})

// 监听 noteId 变化，确保 viewingNoteId 与 URL 同步
watch(() => route.query.noteId, (newNoteId, oldNoteId) => {
  // 只有在 note-detail tab 时才更新 viewingNoteId
  if (currentTab.value === 'note-detail' && newNoteId) {
    const noteId = Number(newNoteId)
    if (!isNaN(noteId) && noteId > 0) {
      // 只有当 noteId 真正变化时才更新，避免不必要的更新
      if (viewingNoteId.value !== noteId) {
        console.log('[MainView] noteId 变化，从', viewingNoteId.value, '到', noteId)
        viewingNoteId.value = noteId
        // 同时更新标题（如果 URL 中有）
        noteDetailTitle.value = route.query.title || null
      }
    }
  } else if (currentTab.value !== 'note-detail' && oldNoteId && !newNoteId) {
    // 当离开 note-detail tab 且 noteId 被清除时，也清除 viewingNoteId
    if (viewingNoteId.value) {
      console.log('[MainView] 清除 viewingNoteId，因为离开了 note-detail tab')
      viewingNoteId.value = null
      noteDetailTitle.value = null
      noteDetailStats.value = null
    }
  }
}, { immediate: true })

// --- 新增状态和方法来管理编辑器视图 ---

// 跟踪正在编辑的笔记ID。如果为null，则不显示编辑器。
const editingNotebookId = ref(null);
const editingSpaceId = ref(null);
const editingNotebookName = ref(null);
const editingNotebookList = ref([]); // 使用数组类型
const editingNoteId = ref(null); // 当前选中的笔记ID
const qaRef = ref(null); // 问答组件实例
const searchViewRef = ref(null); // 搜索视图组件实例
const recommendViewRef = ref(null); // 推荐视图组件实例
const hotViewRef = ref(null); // 热榜视图组件实例

// 获取标签名称的辅助函数
const getTagNameString = async (tag) => {
  try {
    if (tag === null || tag === undefined || tag === '') return null;
    const maybeId = Number(tag);
    if (!Number.isNaN(maybeId) && String(tag).trim() !== '') {
      const tagResp = await service.post(`${BASE_PATH}/tags/name`, { tagId: maybeId });
      if (tagResp?.data?.code === 200 && tagResp.data.data) {
        return tagResp.data.data.tagName || String(tag);
      }
    }
    return String(tag);
  } catch (err) {
    return String(tag);
  }
}

// 从 URL 查询参数中恢复编辑器状态
const restoreEditorFromRoute = async (shouldSetTab = true) => {
  const notebookIdFromQuery = route.query.notebookId
  const spaceIdFromQuery = route.query.spaceId
  
  if (notebookIdFromQuery && spaceIdFromQuery) {
    const notebookId = Number(notebookIdFromQuery)
    const spaceId = Number(spaceIdFromQuery)
    
    if (!isNaN(notebookId) && !isNaN(spaceId)) {
      // 恢复编辑器状态
      editingNotebookId.value = notebookId
      editingSpaceId.value = spaceId
      
      // 如果需要，设置 tab 为 workspace，确保显示正确的视图
      if (shouldSetTab) {
        currentTab.value = 'workspace'
      }
      
      // 从 URL 获取 notebookName
      editingNotebookName.value = route.query.notebookName || null
      
      // 从 URL 获取当前选中的笔记ID
      const noteIdFromQuery = route.query.noteId
      if (noteIdFromQuery) {
        const noteId = Number(noteIdFromQuery)
        if (!isNaN(noteId)) {
          editingNoteId.value = noteId
        }
      }
      
      // 尝试获取 notebookList
      try {
        const userId = userStore.userInfo.id
        if (userId) {
          const response = await service.post(`${BASE_PATH}/notebooks/by-space`, {
            spaceId,
            userId
          })
          
          if (response.data.code === 200 && Array.isArray(response.data.data)) {
            const notebooks = response.data.data
            // 处理标签名称
            const tasks = notebooks.map(async (nb) => {
              const tagId = nb.tagId ?? nb.tag;
              if (!tagId && tagId !== 0) {
                nb.tagName = null;
                return;
              }
              nb.tagName = await getTagNameString(tagId);
            });
            await Promise.all(tasks);
            editingNotebookList.value = notebooks
          } else {
            editingNotebookList.value = []
          }
        } else {
          editingNotebookList.value = []
        }
      } catch (error) {
        console.error('恢复笔记本列表失败:', error)
        editingNotebookList.value = []
      }
    }
  }
}

// 处理 WorkspaceView 发出的"打开笔记本"事件
const handleOpenNotebook = (payload) => {
  if (payload && typeof payload.notebookId !== 'undefined') {
    editingNotebookId.value = payload.notebookId;
    editingSpaceId.value = payload.spaceId;
    editingNotebookName.value = payload.notebookName;
    editingNotebookList.value = payload.notebookList;
    editingNoteId.value = null; // 打开新笔记本时，重置笔记ID

    // 将编辑器状态保存到 URL
    router.replace({
      path: route.path,
      query: {
        ...route.query,
        tab: 'workspace',
        notebookId: payload.notebookId,
        spaceId: payload.spaceId,
        notebookName: payload.notebookName || undefined
        // 注意：不包含 noteId，因为打开笔记本时还没有选中笔记
      }
    })
  } else {
    console.error("打开笔记本失败：事件载荷中缺少 notebookId 字段。");
    editingNotebookId.value = null;
    editingSpaceId.value = null;
    editingNotebookName.value = null;
    editingNotebookList.value = [];
    editingNoteId.value = null;
  }
}

// 处理 NoteEditorView 发出的"笔记选中"事件
const handleNoteSelected = (noteId) => {
  editingNoteId.value = noteId;
  
  // 将选中的笔记ID保存到 URL
  router.replace({
    path: route.path,
    query: {
      ...route.query,
      noteId: noteId
    }
  })
}

// 处理编辑器内"关闭"或"返回"操作
const handleCloseEditor = () => {
  // 保存笔记所在的空间ID，用于回到 workspace tab 时选中
  const spaceIdBeforeClose = editingSpaceId.value
  
  editingNotebookId.value = null
  editingSpaceId.value = null
  editingNotebookName.value = null
  editingNotebookList.value = []
  editingNoteId.value = null
  
  // 清除 URL 中的编辑器相关参数
  const newQuery = { ...route.query }
  delete newQuery.notebookId
  delete newQuery.spaceId
  delete newQuery.notebookName
  delete newQuery.noteId
  
  // 如果关闭编辑器后回到 workspace tab，使用笔记所在的空间ID
  if (spaceIdBeforeClose && currentTab.value === 'workspace') {
    selectedWorkspaceId.value = spaceIdBeforeClose
    newQuery.workspaceId = spaceIdBeforeClose
  }
  
  router.replace({
    path: route.path,
    query: newQuery
  })
  
  // 确保当前 tab 切换回 workspace 视图，以便用户返回时看到列表
  currentTab.value = 'workspace';
}

// 处理 WorkspaceView 发出的"空间选中"事件
const handleWorkspaceSelected = (workspaceId) => {
  selectedWorkspaceId.value = workspaceId
  
  // 将选中的空间ID保存到 URL（只在 workspace tab 时）
  if (currentTab.value === 'workspace') {
    router.replace({
      path: route.path,
      query: {
        ...route.query,
        workspaceId: workspaceId
      }
    })
  }
}

// 跳转到个人信息页面
const goToProfile = () => {
  currentTab.value = 'profile'
}

// 处理提问按钮，跳转问答并弹出提问框
const handleAskClick = () => {
  currentTab.value = 'circle'
  nextTick(() => {
    qaRef.value?.openAskDialog?.()
  })
}

// 处理搜索功能
const handleSearch = () => {
  const keyword = searchKeyword.value.trim()
  if (!keyword) return
  
  // 切换到搜索tab并传递关键词
  searchKeywordFromRoute.value = keyword
  currentTab.value = 'search'
  
  // 更新URL参数，重置搜索类型为默认的"笔记"
  router.replace({
    path: route.path,
    query: {
      ...route.query,
      tab: 'search',
      keyword: keyword,
      searchType: 'notes' // 从主页搜索框搜索时，默认搜索笔记
    }
  })
}

// 处理打开笔记详情页（从搜索结果点击）
const handleOpenNoteDetail = (payload) => {
  if (payload && payload.noteId) {
    viewingNoteId.value = payload.noteId
    const sourceTab = payload.fromTab || currentTab.value || 'hot'
    
    console.log('[MainView] handleOpenNoteDetail, payload.fromTab:', payload.fromTab, 'currentTab.value:', currentTab.value, 'sourceTab:', sourceTab)
    
    // 保存标题（如果从搜索结果传递过来）
    noteDetailTitle.value = payload.title || null
    
    // 保存统计信息（如果从搜索结果传递过来）
    if (payload.authorName !== undefined || payload.viewCount !== undefined) {
      noteDetailStats.value = {
        authorName: payload.authorName || '未知作者',
        views: payload.viewCount || 0,
        likes: payload.likeCount || 0,
        favorites: payload.favoriteCount || 0,
        comments: payload.commentCount || 0
      }
    } else {
      noteDetailStats.value = null // 如果没有传递统计信息，让组件自己获取
    }
    
    // 先更新URL参数，确保 fromTab 被正确设置，然后再更新 currentTab
    const newQuery = {
      ...route.query,
      tab: 'note-detail',
      fromTab: sourceTab, // 确保 fromTab 被设置
      noteId: payload.noteId,
      title: payload.title || undefined,
      fileType: payload.fileType || undefined
    }
    
    // 如果来自搜索结果，保留搜索参数
    if (sourceTab === 'search') {
      newQuery.keyword = route.query.keyword
      newQuery.searchType = route.query.searchType || 'notes'
    }
    
    console.log('[MainView] 更新 URL，newQuery:', newQuery)
    // 先更新 URL，再更新 currentTab，避免 watch 覆盖 fromTab
    router.replace({
      path: route.path,
      query: newQuery
    })
    // 同步更新 currentTab（watch 会检查 URL 中是否有 fromTab，如果有就跳过更新）
    currentTab.value = 'note-detail'
  }
}

// 处理统计信息更新（从 NoteDetailView 发出）
const handleStatsUpdated = (payload) => {
  if (!payload || !payload.noteId) return
  
  // 更新 noteDetailStats
  if (noteDetailStats.value) {
    if (payload.comments !== undefined) {
      noteDetailStats.value.comments = payload.comments
    }
    if (payload.likes !== undefined) {
      noteDetailStats.value.likes = payload.likes
    }
    if (payload.favorites !== undefined) {
      noteDetailStats.value.favorites = payload.favorites
    }
  }
  
  // 更新各个列表页面中对应笔记的统计数量
  if (searchViewRef.value) {
    if (typeof searchViewRef.value.updateCommentCount === 'function' && payload.comments !== undefined) {
      searchViewRef.value.updateCommentCount(payload.noteId, payload.comments)
    }
    if (typeof searchViewRef.value.updateLikeCount === 'function' && payload.likes !== undefined) {
      searchViewRef.value.updateLikeCount(payload.noteId, payload.likes)
    }
    if (typeof searchViewRef.value.updateFavoriteCount === 'function' && payload.favorites !== undefined) {
      searchViewRef.value.updateFavoriteCount(payload.noteId, payload.favorites)
    }
  }
  if (recommendViewRef.value) {
    if (typeof recommendViewRef.value.updateCommentCount === 'function' && payload.comments !== undefined) {
      recommendViewRef.value.updateCommentCount(payload.noteId, payload.comments)
    }
    if (typeof recommendViewRef.value.updateLikeCount === 'function' && payload.likes !== undefined) {
      recommendViewRef.value.updateLikeCount(payload.noteId, payload.likes)
    }
    if (typeof recommendViewRef.value.updateFavoriteCount === 'function' && payload.favorites !== undefined) {
      recommendViewRef.value.updateFavoriteCount(payload.noteId, payload.favorites)
    }
  }
  if (hotViewRef.value) {
    if (typeof hotViewRef.value.updateCommentCount === 'function' && payload.comments !== undefined) {
      hotViewRef.value.updateCommentCount(payload.noteId, payload.comments)
    }
    if (typeof hotViewRef.value.updateLikeCount === 'function' && payload.likes !== undefined) {
      hotViewRef.value.updateLikeCount(payload.noteId, payload.likes)
    }
    if (typeof hotViewRef.value.updateFavoriteCount === 'function' && payload.favorites !== undefined) {
      hotViewRef.value.updateFavoriteCount(payload.noteId, payload.favorites)
    }
  }
}

// 监听路由中的搜索关键词
watch(() => route.query.keyword, (newKeyword) => {
  if (newKeyword) {
    searchKeyword.value = newKeyword
    // 如果当前在搜索 tab，更新 searchKeywordFromRoute 以触发 SearchView 的搜索
    if (currentTab.value === 'search') {
      searchKeywordFromRoute.value = newKeyword
    }
  }
})

// 监听 tab 切换，当切换到 search 时，如果有 keyword，更新 searchKeywordFromRoute
watch(() => currentTab.value, (newTab) => {
  if (newTab === 'search' && route.query.keyword) {
    searchKeywordFromRoute.value = route.query.keyword
  }
})

// 监听用户头像更新，同步更新通知中的头像缓存
watch(() => userInfo.value.avatarUrl, async (newAvatarUrl, oldAvatarUrl) => {
  // 当头像更新时，更新通知用户信息映射中的当前用户头像
  if (currentUserId.value && newAvatarUrl && newAvatarUrl !== oldAvatarUrl) {
    // 直接更新缓存中的头像URL
    if (notificationUserInfoMap.value[currentUserId.value]) {
      notificationUserInfoMap.value[currentUserId.value].avatarUrl = newAvatarUrl
    }
    // 强制重新加载当前用户信息以确保同步
    await loadNotificationUserInfo(currentUserId.value, true)
    // 如果通知面板是打开的，重新加载通知列表以刷新显示
    if (showNotificationPanel.value) {
      await loadNotifications()
    }
  }
})

// 组件挂载时，确保 URL 中有 tab 参数，并尝试恢复编辑器状态
onMounted(async () => {
  if (!route.query.tab) {
    router.replace({
      path: route.path,
      query: { ...route.query, tab: currentTab.value }
    })
  }

  // 初始化私信未读数
  await refreshPrivateMessageUnread()
  // 初始化系统通知未读数
  await refreshNotificationUnread()

  // 周期性刷新私信未读数（即使未打开私信中心，也能收到新消息提示）
  privateMessageTimer = setInterval(() => {
    refreshPrivateMessageUnread()
  }, 10000) // 每 10 秒刷新一次

  // 周期性刷新通知未读数（无需点开消息面板也会更新小红点）
  notificationTimer = setInterval(() => {
    refreshNotificationUnread()
  }, 10000)
  
  // 恢复搜索关键词
  if (route.query.keyword) {
    searchKeyword.value = route.query.keyword
    searchKeywordFromRoute.value = route.query.keyword
  }
  
  // 恢复 workspace tab 的选中空间
  restoreWorkspaceFromRoute()
  
  // 恢复笔记详情页状态
  restoreNoteDetailFromRoute()
  
  // 尝试从 URL 恢复编辑器状态
  await restoreEditorFromRoute()
})

onBeforeUnmount(() => {
  if (privateMessageTimer) {
    clearInterval(privateMessageTimer)
    privateMessageTimer = null
  }
  if (notificationTimer) {
    clearInterval(notificationTimer)
    notificationTimer = null
  }
})

// --- 结束新增 ---
</script>

<style scoped>
/* 样式部分保持不变 */

.main-shell {
  min-height: 100vh;
  padding: 0;
  background: transparent;
  display: flex;
  flex-direction: column;
  gap: 0;
  position: relative;
  z-index: 1;
}

.main-header {
  display: flex;
  align-items: center;
  background: white;
  padding: 0 20px;
  height: 52px;
  border-bottom: 1px solid #ededed;
  gap: 24px;
  box-shadow: none;
  border-radius: 0;
  border: none;
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  width: 100%;
  z-index: 1000;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
}

/* --- Logo / Nav Links (保持不变) --- */

.brand-logo-block {
  margin-right: 16px;
}

.brand-logo-text {
  font-family: 'PingFang SC', 'Helvetica Neue', Arial, sans-serif;
  font-size: 22px;
  font-weight: 500;
  color: #0a0a0a;
  letter-spacing: 1px;
  text-transform: uppercase;
  user-select: none;
  cursor: pointer;
  transition: color 0.3s ease, opacity 0.3s ease;
  opacity: 0.9;
}

.brand-logo-text:hover {
  color: #007FFF;
  opacity: 1;
}

.main-nav-links {
  display: flex;
  align-items: center;
  gap: 35px;
  white-space: nowrap;
}

.nav-link-item {
  background: none;
  border: none;
  padding: 0 0 5px 0;
  color: #444;
  font-size: 15px;
  cursor: pointer;
  transition: color 0.2s, border-bottom-color 0.2s;
  position: relative;
  flex-shrink: 0;
}

.nav-link-item.active {
  color: #000;
  font-weight: bold;
}
.nav-link-item.active::after {
  content: '';
  position: absolute;
  left: 50%;
  bottom: 0;
  transform: translateX(-50%);
  width: 28px;
  height: 3px;
  background-color: #007FFF;
  border-radius: 2px;
}

.nav-link-item:hover {
  color: #000;
}


/* --- Search Bar (保持不变) --- */

.search-container {
  display: flex;
  flex: 1;
  max-width: 480px;
  height: 38px;
  background: var(--surface-base);
  border-radius: 8px;
  overflow: hidden;
  align-items: center;
  margin-left: auto;
  margin-right: 30px;
  border: 1px solid var(--line-soft);
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.04);
  transition: box-shadow 0.2s, border-color 0.2s;
}

.search-container:focus-within {
  border-color: var(--brand-primary);
  box-shadow: 0 2px 6px rgba(34, 191, 163, 0.12);
}

.search-input {
  flex: 1;
  border: none;
  background: none;
  padding: 0 12px;
  font-size: 14px;
  color: #333;
  height: 100%;
}

.search-input::placeholder {
  color: #999;
}

.search-button {
  width: 40px;
  height: 100%;
  border: none;
  background: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.search-icon {
  font-size: 16px;
  color: #8590a6;
}


/* -------------------------------------------------------------------------- */
/* 右侧功能按钮群样式 (图标部分已修改) */
/* -------------------------------------------------------------------------- */
.header-actions {
  display: flex;
  align-items: center;
  gap: 18px;
  flex-shrink: 0;
}

/* 提问按钮 */
.ask-button {
  display: flex;
  align-items: center;
  gap: 4px;
  background-color: #007FFF;
  color: white;
  border: none;
  border-radius: 20px;
  padding: 8px 16px;
  font-size: 15px;
  cursor: pointer;
  transition: background-color 0.2s;
  height: 38px;
}

.ask-button:hover {
  background-color: #006EDC;
}

.ask-button .icon {
  font-size: 20px;
  line-height: 1;
}

/* 消息/通知图标的通用样式 (保持一致性) */
.action-icon-wrapper {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 48px;
  height: 48px;
  padding-top: 5px;
  border-radius: 8px;
  background-color: transparent;
  cursor: pointer;
  transition: background-color 0.2s;
  flex-shrink: 0;
  box-sizing: border-box;
}

.action-icon-wrapper:hover {
  background-color: #f6f6f6;
}

/* 新增：图片图标的通用样式 */
.action-image-icon {
  width: 20px; /* 统一图标尺寸 */
  height: 20px;
  margin-bottom: 3px;
  object-fit: contain; /* 确保图片完整显示 */
  /* 替换之前的 .action-icon-wrapper .icon 样式 */
}

/* 移除不再需要的私信图标 SVG 样式 */
/* .private-message-icon { ... } */

.action-icon-wrapper .action-text {
  font-size: 12px;
  color: #8590a6;
  white-space: nowrap;
  line-height: 1;
}

.action-icon-wrapper .badge {
  position: absolute;
  top: 0px;
  right: 5px;
  background-color: #ff4d4f;
  color: white;
  font-size: 11px;
  padding: 0px 4px;
  border-radius: 10px;
  min-width: 12px;
  height: 18px;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  line-height: 1;
  box-sizing: border-box;
}

/* 用户头像 */
.user-avatar-block {
  width: 38px;
  height: 38px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 48px;
  cursor: pointer;
  border-radius: 50%;
  transition: background-color 0.2s;
}

.user-avatar-block:hover {
  background-color: #f6f6f6;
}

.user-avatar-block .avatar-placeholder {
  display: block;
  width: 100%;
  height: 100%;
  border-radius: 50%;
  background-color: #e0e0e0;
  border: none;
}

.user-avatar-block .user-avatar-img {
  display: block;
  width: 100%;
  height: 100%;
  border-radius: 50%;
  object-fit: cover;
  border: none;
}

/* --- Content and Media Queries (保持不变) --- */

.main-content {
  flex: 1;
  padding: 20px;
  margin-top: 52px;
}

.notification-panel {
  position: fixed;
  top: 64px;
  right: 32px;
  width: 360px;
  max-height: 480px;
  background: var(--surface-base);
  border-radius: var(--radius-md);
  box-shadow: 0 12px 30px rgba(15, 23, 42, 0.18);
  display: flex;
  flex-direction: column;
  overflow: hidden;
  z-index: 1500;
}

.notification-panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  border-bottom: 1px solid var(--line-soft);
  background: var(--surface-base);
}

.notification-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--text-strong);
}

.notification-mark-all {
  border: none;
  background: transparent;
  font-size: 12px;
  color: var(--text-muted);
  cursor: pointer;
}

.notification-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 4px 8px;
}

.notification-item {
  width: 100%;
  text-align: left;
  border: none;
  background: transparent;
  padding: 10px 12px;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: background 0.12s ease;
  display: flex;
  align-items: flex-start;
}

.notification-item:hover {
  background: var(--surface-soft);
}

.notification-item.is-unread {
  background: rgba(59, 130, 246, 0.06);
}

.notification-avatar {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  overflow: hidden;
  flex-shrink: 0;
  margin-right: 10px;
  background: var(--surface-soft);
  display: flex;
  align-items: center;
  justify-content: center;
}

.notification-avatar img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.notification-main {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.notification-message {
  font-size: 13px;
  color: var(--text-strong);
}

.notification-meta {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 11px;
  color: var(--text-muted);
}

.notification-dot {
  width: 8px;
  height: 8px;
  border-radius: 999px;
  background: var(--brand-primary);
}

.notification-empty {
  padding: 24px 16px;
  font-size: 13px;
  color: var(--text-muted);
  text-align: center;
}

@media (max-width: 960px) {
  .main-header {
    flex-wrap: wrap;
    height: auto;
    min-height: 52px;
    padding: 10px 20px;
    gap: 15px;
    justify-content: space-between;
  }

  .main-content {
    margin-top: 100px; /* 移动端导航栏高度更大，需要更多空间 */
  }

  .brand-logo-block {
    order: 1;
    margin-right: 0;
  }

  .main-nav-links {
    order: 3;
    width: 100%;
    justify-content: space-around;
    gap: 10px;
    border-top: 1px solid #ededed;
    padding-top: 10px;
    margin-top: 5px;
  }

  .search-container {
    order: 2;
    flex-grow: 1;
    max-width: none;
    margin-left: 0;
    margin-right: 15px;
  }

  .header-actions {
    order: 2;
    gap: 10px;
  }

  .ask-button {
    display: none;
  }

  /* 窄屏下图标容器样式 */
  .action-icon-wrapper {
    width: 32px;
    height: 32px;
    padding-top: 0;
    border-radius: 50%;
  }

  /* 窄屏下的图片图标尺寸 */
  .action-image-icon {
    width: 18px;
    height: 18px;
    margin-bottom: 0;
  }

  .action-icon-wrapper .action-text {
    display: none;
  }

  .action-icon-wrapper .badge {
    top: -2px;
    right: -2px;
    font-size: 10px;
    padding: 2px 5px;
    min-width: 10px;
    height: auto;
  }

  .user-avatar-block {
    width: 32px;
    height: 32px;
  }
}
</style>