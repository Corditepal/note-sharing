<template>
  <div class="pm-overlay" v-if="visible">
    <div class="pm-container">
      <header class="pm-header">
        <div class="pm-title">
          <span>私信中心</span>
          <span v-if="totalUnread > 0" class="pm-unread-badge">{{ totalUnread }}</span>
        </div>
        <button class="pm-close-btn" type="button" @click="handleClose">×</button>
      </header>

      <div class="pm-body">
        <!-- 会话列表 -->
        <aside class="pm-sidebar">
          <div class="pm-sidebar-header">
            <span class="pm-sidebar-title">会话列表</span>
            <span class="pm-sidebar-subtitle">仅显示互相关注的用户</span>
          </div>

          <div class="pm-conversation-list" v-if="conversations.length">
              <button
                v-for="item in sortedConversations"
                :key="item.conversationId"
                type="button"
                class="pm-conversation-item"
                :class="{ active: currentConversationId === item.conversationId }"
                @click="handleSelectConversation(item)"
              >
              <div class="pm-conv-avatar">
                <img 
                  :src="getConversationAvatarUrl(item.peerId)" 
                  :alt="item.peerName || ('用户 ' + item.peerId)"
                  @error="handleAvatarError"
                />
              </div>
              <div class="pm-conv-main">
                <div class="pm-conv-top">
                  <span class="pm-conv-name">
                    {{ getConversationPeerName(item.peerId) }}
                  </span>
                  <span class="pm-conv-time">
                    {{ formatTimeDisplay(item.lastTime) }}
                  </span>
                </div>
                <div class="pm-conv-bottom">
                  <span class="pm-conv-last" :title="item.lastMessage || ''">
                    {{ item.lastMessage || '暂无消息' }}
                  </span>
                  <span
                    v-if="unreadMap[item.conversationId] && unreadMap[item.conversationId] > 0"
                    class="pm-conv-unread"
                  >
                    {{ unreadMap[item.conversationId] }}
                  </span>
                </div>
              </div>
            </button>
          </div>

          <!-- 如果没有会话，显示互相关注的用户列表 -->
          <div v-else-if="mutualFollowUsers.length > 0" class="pm-mutual-list">
            <div class="pm-mutual-header">
              <span class="pm-mutual-title">互相关注的用户</span>
              <span class="pm-mutual-subtitle">点击发起私信</span>
            </div>
            <div class="pm-mutual-users">
              <button
                v-for="user in mutualFollowUsers"
                :key="user.userId"
                type="button"
                class="pm-mutual-item"
                :class="{ active: currentPeerId === user.userId && !currentConversationId }"
                @click="handleStartNewConversation(user)"
              >
                <div class="pm-mutual-avatar">
                  <img 
                    :src="getMutualUserAvatarUrl(user.userId)" 
                    :alt="user.username || ('用户 ' + user.userId)"
                    @error="handleAvatarError"
                  />
                </div>
                <div class="pm-mutual-info">
                  <div class="pm-mutual-name">
                    {{ user.username || ('用户 ' + user.userId) }}
                  </div>
                  <div class="pm-mutual-status">互相关注</div>
                </div>
              </button>
            </div>
          </div>

          <div v-else class="pm-empty">
            <p>还没有任何私信会话</p>
            <p class="pm-empty-tip">去关注一些同学，互相关注后即可开始私信。</p>
          </div>
        </aside>

        <!-- 聊天窗口 -->
        <section class="pm-chat" v-if="currentPeerId">
          <header class="pm-chat-header">
            <div class="pm-chat-peer">
              <div class="pm-peer-avatar">
                <img 
                  :src="getPeerAvatarUrl()" 
                  :alt="displayPeerName"
                  @error="handleAvatarError"
                />
              </div>
              <div class="pm-peer-info">
                <div class="pm-peer-name">
                  {{ displayPeerName }}
                </div>
                <div class="pm-peer-subtitle">
                  {{ isCurrentMutualFollow ? '互相关注 · 支持实时私信' : '已取消互相关注 · 仅可查看历史消息' }}
                </div>
              </div>
            </div>
          </header>

          <div 
            class="pm-messages" 
            ref="messageListRef"
            @wheel="handleMessageWheel"
            @touchmove.stop
          >
            <div v-if="loadingMessages" class="pm-loading">加载消息中...</div>
            <div v-else-if="messages.length === 0" class="pm-empty-messages">
              暂无消息，发送第一条打个招呼吧～
            </div>

            <div
              v-for="(msg, idx) in messages"
              :key="idx"
              class="pm-message-row"
              :class="{ 'is-me': msg.senderId === currentUserId }"
            >
              <div class="pm-message-bubble">
                <div class="pm-message-content">
                  {{ msg.content }}
                </div>
                <div class="pm-message-meta">
                  <span class="pm-message-time">
                    {{ formatFullTimeDisplay(msg.createdAt || msg.timestamp) }}
                  </span>
                </div>
              </div>
            </div>
          </div>

          <footer class="pm-input-bar">
            <textarea
              v-model="draftMessage"
              class="pm-input"
              rows="2"
              placeholder="按 Enter 发送，Shift+Enter 换行"
              @keydown.enter.prevent="handleEnterKey"
            />
            <div class="pm-input-actions">
              <span class="pm-input-tip">
                {{ sendTip }}
              </span>
              <button
                type="button"
                class="pm-send-btn"
                :disabled="sending || !draftMessage.trim() || !isCurrentMutualFollow"
                @click="handleSend"
              >
                {{ sending ? '发送中...' : '发送' }}
              </button>
            </div>
          </footer>
        </section>

        <section v-else class="pm-chat pm-chat-empty">
          <div class="pm-chat-placeholder">
            <p class="pm-chat-placeholder-title">选择左侧一个会话开始聊天</p>
            <p class="pm-chat-placeholder-subtitle">
              或者在用户主页点击“私信”入口开启新的会话。
            </p>
          </div>
        </section>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { useUserStore } from '@/stores/user'
import { storeToRefs } from 'pinia'
import { Client as StompClient } from '@stomp/stompjs'
import SockJS from 'sockjs-client'
import {
  fetchConversationList,
  fetchConversationDetail,
  fetchAllUnreadCounts,
  fetchUnreadTotal,
  markConversationAsRead
} from '@/api/conversation'
import { isMutualFollow, getFollowings, getFollowers, getUserById } from '@/api/follow'
import { formatTime, formatFullTime } from '@/utils/time'
import { useMessage } from '@/utils/message'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['close', 'update:visible', 'unread-updated'])

const userStore = useUserStore()
const { userInfo } = storeToRefs(userStore)
const currentUserId = computed(() => userInfo.value?.id)

const { showError } = useMessage()

const conversations = ref([])
const unreadMap = ref({})
const totalUnread = ref(0)
const mutualFollowUsers = ref([])
const loadingMutualFollow = ref(false)
const isCurrentMutualFollow = ref(true) // 当前会话是否仍为互相关注

const currentConversationId = ref(null)
const currentPeerId = ref(null)
const currentPeerName = ref(null)
const messages = ref([])
const loadingConversations = ref(false)
const loadingMessages = ref(false)
const sending = ref(false)
const draftMessage = ref('')
const messageListRef = ref(null)
const peerUserInfoMap = ref({}) // 存储对方用户的详细信息 {userId: {username, avatarUrl, ...}}

let stompClient = null
const subscriptionMap = new Map()
const pendingSends = new Map() // 记录待确认的发送消息

const sortedConversations = computed(() => {
  return [...conversations.value].sort((a, b) => {
    const t1 = a.lastTime ? new Date(a.lastTime).getTime() : 0
    const t2 = b.lastTime ? new Date(b.lastTime).getTime() : 0
    return t2 - t1
  })
})

const displayPeerName = computed(() => {
  if (currentPeerId.value) {
    const userInfo = peerUserInfoMap.value[currentPeerId.value]
    if (userInfo && userInfo.username) {
      return userInfo.username
    }
  }
  if (currentPeerName.value) return currentPeerName.value
  const conv = conversations.value.find(c => c.conversationId === currentConversationId.value)
  return conv?.peerName || (currentPeerId.value ? `用户 ${currentPeerId.value}` : '对方')
})

function getConversationPeerName(peerId) {
  if (!peerId) return '未知用户'
  const userInfo = peerUserInfoMap.value[peerId]
  if (userInfo && userInfo.username) {
    return userInfo.username
  }
  // 如果还没有加载用户信息，尝试从会话列表中找
  const conv = conversations.value.find(c => c.peerId === peerId)
  return conv?.peerName || `用户 ${peerId}`
}

const currentPeerInitial = computed(() => {
  const name = displayPeerName.value || ''
  return name.slice(0, 1).toUpperCase() || 'U'
})

function getUserInitial(nameOrId) {
  if (typeof nameOrId === 'string' && nameOrId.length > 0) {
    return nameOrId.slice(0, 1).toUpperCase()
  }
  return 'U'
}

function getPeerAvatarUrl() {
  if (!currentPeerId.value) return '/assets/avatars/avatar.png'
  const userInfo = peerUserInfoMap.value[currentPeerId.value]
  if (userInfo && userInfo.avatarUrl) {
    return userInfo.avatarUrl
  }
  return '/assets/avatars/avatar.png'
}

function getConversationAvatarUrl(peerId) {
  if (!peerId) return '/assets/avatars/avatar.png'
  const userInfo = peerUserInfoMap.value[peerId]
  if (userInfo && userInfo.avatarUrl) {
    return userInfo.avatarUrl
  }
  return '/assets/avatars/avatar.png'
}

function getMutualUserAvatarUrl(userId) {
  if (!userId) return '/assets/avatars/avatar.png'
  const userInfo = peerUserInfoMap.value[userId]
  if (userInfo && userInfo.avatarUrl) {
    return userInfo.avatarUrl
  }
  return '/assets/avatars/avatar.png'
}

function handleAvatarError(event) {
  event.target.src = '/assets/avatars/avatar.png'
}

async function loadPeerUserInfo(peerId, forceRefresh = false) {
  if (!peerId) return
  // 如果已经有完整的用户信息（包含用户名）且不需要强制刷新，直接返回
  if (!forceRefresh && peerUserInfoMap.value[peerId] && peerUserInfoMap.value[peerId].username) {
    return
  }
  try {
    const userData = await getUserById(peerId)
    const userInfo = userData.data || userData
    if (userInfo) {
      peerUserInfoMap.value[peerId] = {
        username: userInfo.username,
        avatarUrl: userInfo.avatarUrl
      }
    }
  } catch (e) {
    console.error('加载用户信息失败', e)
    // 即使失败也设置一个占位符，避免重复请求
    if (!peerUserInfoMap.value[peerId]) {
      peerUserInfoMap.value[peerId] = {
        username: null,
        avatarUrl: null
      }
    }
  }
}

const sendTip = computed(() => {
  if (!currentUserId.value) {
    return '请先登录后再发送私信'
  }
  if (!currentPeerId.value) {
    return '选择一个会话开始聊天'
  }
  if (!isCurrentMutualFollow.value) {
    return '双方已不再互相关注，只能查看历史消息，无法继续发送'
  }
  return '私信仅在互相关注的用户之间可用'
})

function formatTimeDisplay(time) {
  return formatTime(time)
}

function formatFullTimeDisplay(time) {
  return formatFullTime(time)
}

async function loadConversations(forceRefreshAvatars = false) {
  if (!currentUserId.value) return
  loadingConversations.value = true
  try {
    const listVO = await fetchConversationList(currentUserId.value)
    const list = (listVO?.conversations || []).map(item => ({
      ...item
    }))
    conversations.value = list
    
    // 加载所有会话对方的用户信息，如果强制刷新则忽略缓存
    const peerIds = list.map(item => item.peerId).filter(Boolean)
    await Promise.all(peerIds.map(peerId => loadPeerUserInfo(peerId, forceRefreshAvatars)))
  } catch (e) {
    console.error('加载会话列表失败', e)
    showError('加载会话列表失败，请稍后重试')
  } finally {
    loadingConversations.value = false
  }
}

async function loadUnreadStats() {
  if (!currentUserId.value) return
  try {
    const map = await fetchAllUnreadCounts(currentUserId.value)
    unreadMap.value = map || {}
    totalUnread.value = await fetchUnreadTotal(currentUserId.value)
    // 通知父组件更新未读数
    emit('unread-updated', totalUnread.value)
  } catch (e) {
    console.error('加载未读统计失败', e)
  }
}

async function loadMutualFollowUsers() {
  if (!currentUserId.value) return
  loadingMutualFollow.value = true
  try {
    // 获取关注列表和粉丝列表
    const [followingsRes, followersRes] = await Promise.all([
      getFollowings(currentUserId.value),
      getFollowers(currentUserId.value)
    ])
    
    const followings = followingsRes?.followings || []
    const followers = followersRes?.followers || []
    
    // 找出互相关注的用户（既在关注列表中，也在粉丝列表中）
    const followingIds = new Set(followings.map(f => f.userId))
    const mutualIds = followers
      .filter(f => followingIds.has(f.userId))
      .map(f => f.userId)
    
    // 获取这些用户的详细信息（强制刷新以获取最新头像）
    const userPromises = mutualIds.map(userId => 
      getUserById(userId).catch(() => null)
    )
    const users = await Promise.all(userPromises)
    
    // 过滤掉已经有会话的用户和获取失败的用户
    const existingPeerIds = new Set(conversations.value.map(c => c.peerId))
    mutualFollowUsers.value = users
      .filter(u => {
        if (!u) return false
        // getUserById 返回的是 res.data，可能是 StandardResponse 格式
        const userData = u.data || u
        const uid = userData?.id || userData?.userId
        return uid && !existingPeerIds.has(uid)
      })
      .map(u => {
        const userData = u.data || u
        const userId = userData.id || userData.userId
        // 保存用户信息到 map 中（强制更新以获取最新头像）
        if (userId) {
          peerUserInfoMap.value[userId] = {
            username: userData.username,
            avatarUrl: userData.avatarUrl
          }
        }
        return {
          userId: userId,
          username: userData.username || null
        }
      })
  } catch (e) {
    console.error('加载互相关注用户失败', e)
  } finally {
    loadingMutualFollow.value = false
  }
}

async function handleStartNewConversation(user) {
  if (!currentUserId.value || !user.userId) return
  
  // 检查是否真的互相关注
  try {
    const mutual = await isMutualFollow(currentUserId.value, user.userId)
    if (!mutual) {
      showError('该用户已取消互相关注，无法发送私信')
      await loadMutualFollowUsers() // 刷新列表
      return
    }
  } catch (e) {
    console.error('检查互相关注状态失败', e)
    showError('检查关注关系失败')
    return
  }
  
  // 加载用户信息（强制刷新以获取最新头像）
  await loadPeerUserInfo(user.userId, true)
  
  // 设置当前会话信息（但还没有 conversationId，需要发送第一条消息后才会创建）
  currentPeerId.value = user.userId
  const userInfo = peerUserInfoMap.value[user.userId]
  currentPeerName.value = userInfo?.username || user.username || `用户 ${user.userId}`
  currentConversationId.value = null // 新会话，还没有 conversationId
  messages.value = []
  
  // 确保 WebSocket 已连接
  ensureStompClient()
}

async function handleSelectConversation(item) {
  currentConversationId.value = item.conversationId
  currentPeerId.value = item.peerId
  // 先加载用户信息，确保显示真实用户名和最新头像
  await loadPeerUserInfo(item.peerId, true) // 强制刷新以获取最新头像
  const userInfo = peerUserInfoMap.value[item.peerId]
  currentPeerName.value = userInfo?.username || item.peerName || null

  // 选择会话时检查当前是否仍为互相关注，用于更新 UI 状态
  try {
    const mutual = await isMutualFollow(currentUserId.value, currentPeerId.value)
    isCurrentMutualFollow.value = !!mutual
  } catch (e) {
    console.error('检查互相关注状态失败', e)
    // 检查失败时，不强制修改原状态，避免误伤
  }

  await loadMessagesForConversation(item.conversationId)
  await markAsRead(item.conversationId)
  subscribeConversation(item.conversationId)
}

async function loadMessagesForConversation(conversationId) {
  if (!conversationId) return
  loadingMessages.value = true
  try {
    const detail = await fetchConversationDetail(conversationId)
    messages.value = detail?.messages || []
    // 等待 DOM 更新后再滚动到底部
    await nextTick()
    scrollToBottom()
  } catch (e) {
    console.error('加载会话消息失败', e)
    showError('加载会话消息失败，请稍后重试')
  } finally {
    loadingMessages.value = false
    // 确保在加载完成后也滚动到底部
    await nextTick()
    scrollToBottom()
  }
}

async function markAsRead(conversationId) {
  if (!currentUserId.value || !conversationId) return
  try {
    await markConversationAsRead(currentUserId.value, conversationId)
    unreadMap.value = {
      ...unreadMap.value,
      [conversationId]: 0
    }
    const total = await fetchUnreadTotal(currentUserId.value)
    totalUnread.value = total
    // 通知父组件更新未读数
    emit('unread-updated', total)
  } catch (e) {
    console.error('标记已读失败', e)
  }
}

function scrollToBottom() {
  nextTick(() => {
    requestAnimationFrame(() => {
      const el = messageListRef.value
      if (el) {
        el.scrollTop = el.scrollHeight
      }
    })
  })
}

function handleMessageWheel(event) {
  const el = messageListRef.value
  if (!el) return
  
  const { scrollTop, scrollHeight, clientHeight } = el
  const isAtTop = scrollTop === 0
  const isAtBottom = scrollTop + clientHeight >= scrollHeight - 1
  
  // 如果不在边界，或者滚动方向是向内的，阻止事件冒泡
  if (!isAtTop && !isAtBottom) {
    event.stopPropagation()
  } else if (isAtTop && event.deltaY < 0) {
    // 在顶部且向上滚动，阻止冒泡
    event.stopPropagation()
  } else if (isAtBottom && event.deltaY > 0) {
    // 在底部且向下滚动，阻止冒泡
    event.stopPropagation()
  }
}

function handleEnterKey(event) {
  if (event.shiftKey) {
    draftMessage.value += '\n'
    return
  }
  handleSend()
}

function ensureStompClient() {
  if (stompClient || !currentUserId.value || !userInfo.value?.token) return

  const socketUrl = 'http://localhost:8080/ws'
  const socket = new SockJS(socketUrl)

  stompClient = new StompClient({
    webSocketFactory: () => socket,
    reconnectDelay: 5000,
    debug: () => {},
    connectHeaders: {
      Authorization: `Bearer ${userInfo.value.token}`
    },
    onConnect: () => {
      if (currentConversationId.value) {
        subscribeConversation(currentConversationId.value)
      }
    },
    onStompError: (frame) => {
      console.error('STOMP 错误', frame)
    }
  })

  stompClient.activate()
}

function subscribeConversation(conversationId) {
  if (!stompClient || !stompClient.connected || !conversationId) return

  if (subscriptionMap.has(conversationId)) return

  const destination = `/user/queue/messages.${conversationId}`
  const sub = stompClient.subscribe(destination, async (message) => {
    try {
      const body = JSON.parse(message.body)
      const newMsg = {
        senderId: body.senderId,
        receiverId: body.receiverId,
        content: body.content,
        createdAt: body.timestamp
      }
      
      // 如果是自己发送的消息，清除对应的待确认记录
      if (body.senderId === currentUserId.value && body.content) {
        const key = `${body.content}_${body.senderId}`
        if (pendingSends.has(key)) {
          clearTimeout(pendingSends.get(key))
          pendingSends.delete(key)
          sending.value = false
        }
      }
      
      // 如果收到消息时还没有 conversationId，说明这是新会话的第一条消息，需要更新
      if (body.conversationId && !currentConversationId.value && body.senderId === currentUserId.value) {
        currentConversationId.value = body.conversationId
        // 刷新会话列表
        await loadConversations()
        // 订阅新会话
        subscribeConversation(body.conversationId)
      }
      
      messages.value.push(newMsg)
      scrollToBottom()

      // 如果是对方发送的消息，更新未读数
      if (body.senderId !== currentUserId.value && body.conversationId) {
        // 如果当前不在这个会话中，需要增加未读数
        if (currentConversationId.value !== body.conversationId) {
          const currentUnread = unreadMap.value[body.conversationId] || 0
          unreadMap.value = {
            ...unreadMap.value,
            [body.conversationId]: currentUnread + 1
          }
          totalUnread.value = (totalUnread.value || 0) + 1
          // 通知父组件更新未读数
          emit('unread-updated', totalUnread.value)
        } else {
          // 在当前会话中，刷新未读总数（可能其他会话有新消息）
          loadUnreadStats()
        }
      }
    } catch (e) {
      console.error('解析 WebSocket 消息失败', e)
    }
  })

  subscriptionMap.set(conversationId, sub)
}

function disconnectStomp() {
  subscriptionMap.forEach(sub => {
    if (sub && typeof sub.unsubscribe === 'function') {
      sub.unsubscribe()
    }
  })
  subscriptionMap.clear()

  // 清理所有待确认的发送记录
  pendingSends.forEach(timeoutId => {
    clearTimeout(timeoutId)
  })
  pendingSends.clear()

  if (stompClient) {
    stompClient.deactivate()
    stompClient = null
  }
}

async function handleSend() {
  if (!draftMessage.value.trim()) return
  if (!currentUserId.value) {
    showError('请先登录后再发送私信')
    return
  }
  if (!currentPeerId.value) {
    showError('请先选择一个会话')
    return
  }

  // 发送前先检查是否互相关注
  try {
    const mutual = await isMutualFollow(currentUserId.value, currentPeerId.value)
    isCurrentMutualFollow.value = !!mutual
    if (!isCurrentMutualFollow.value) {
      showError('双方已不再互相关注，只能查看历史消息，无法继续发送')
      return
    }
  } catch (e) {
    console.error('检查互相关注状态失败', e)
    showError('检查关注关系失败，请稍后重试')
    return
  }

  ensureStompClient()

  if (!stompClient || !stompClient.connected) {
    showError('连接服务器中，请稍后再试')
    return
  }

  const content = draftMessage.value.trim()
  sending.value = true
  
  // 设置超时检测：如果5秒内没有收到推送，认为发送失败
  const sendKey = `${content}_${currentUserId.value}`
  const timeoutId = setTimeout(() => {
    if (pendingSends.has(sendKey)) {
      showError('消息发送失败，可能是网络问题或对方已取消互相关注')
      sending.value = false
      pendingSends.delete(sendKey)
    }
  }, 5000)
  
  pendingSends.set(sendKey, timeoutId)

  try {
    const payload = {
      receiverId: currentPeerId.value,
      content
    }
    stompClient.publish({
      destination: '/app/chat.send',
      body: JSON.stringify(payload)
    })
    
    // 清空输入框
    draftMessage.value = ''
    
    // 如果是新会话（没有 conversationId），发送成功后需要刷新会话列表
    if (!currentConversationId.value) {
      // 等待一下让后端创建会话，然后刷新列表
      setTimeout(async () => {
        await loadConversations()
        // 刷新互相关注用户列表（因为该用户已经从"无会话"变成了"有会话"）
        await loadMutualFollowUsers()
        // 找到新创建的会话并选中
        const newConv = conversations.value.find(c => c.peerId === currentPeerId.value)
        if (newConv) {
          await handleSelectConversation(newConv)
        }
      }, 1000)
    }
    
  } catch (e) {
    if (pendingSends.has(sendKey)) {
      clearTimeout(pendingSends.get(sendKey))
      pendingSends.delete(sendKey)
    }
    console.error('发送消息失败', e)
    showError('发送失败，请检查网络后重试')
    sending.value = false
  }
}

function handleClose() {
  emit('update:visible', false)
  emit('close')
}

watch(
  () => props.visible,
  async (val) => {
    if (val) {
      // 打开私信面板时，强制刷新所有用户的头像信息以显示最新头像
      await Promise.all([loadConversations(true), loadUnreadStats()])
      // 如果会话列表为空，加载互相关注用户列表
      await loadMutualFollowUsers()
      ensureStompClient()
    } else {
      disconnectStomp()
      currentConversationId.value = null
      currentPeerId.value = null
      currentPeerName.value = null
      messages.value = []
      mutualFollowUsers.value = []
    }
  },
  { immediate: false }
)

// 监听用户头像更新，同步更新私信中的头像缓存
watch(() => userInfo.value.avatarUrl, async (newAvatarUrl, oldAvatarUrl) => {
  // 当头像更新时，更新私信用户信息映射中的当前用户头像
  if (currentUserId.value && newAvatarUrl && newAvatarUrl !== oldAvatarUrl) {
    // 直接更新缓存中的头像URL
    if (peerUserInfoMap.value[currentUserId.value]) {
      peerUserInfoMap.value[currentUserId.value].avatarUrl = newAvatarUrl
    }
    // 强制重新加载当前用户信息以确保同步
    await loadPeerUserInfo(currentUserId.value, true)
  }
})

onMounted(() => {
  if (props.visible) {
    loadConversations()
    loadUnreadStats()
    ensureStompClient()
  }
})

onBeforeUnmount(() => {
  disconnectStomp()
})
</script>

<style scoped>
.pm-overlay {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.3);
  display: flex;
  justify-content: flex-end;
  z-index: 2000;
}

.pm-container {
  width: 880px;
  max-width: 100%;
  height: 100%;
  background: var(--surface-base);
  color: var(--text-strong);
  display: flex;
  flex-direction: column;
  box-shadow: -6px 0 24px rgba(15, 23, 42, 0.15);
}

.pm-header {
  padding: 16px 20px;
  border-bottom: 1px solid var(--line-soft);
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: var(--surface-base);
}

.pm-title {
  font-size: 16px;
  font-weight: 600;
  color: var(--text-strong);
  display: flex;
  align-items: center;
  gap: 8px;
}

.pm-unread-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 22px;
  height: 20px;
  padding: 0 6px;
  border-radius: 999px;
  background: var(--feedback-danger);
  color: #fff;
  font-size: 11px;
  font-weight: 500;
}

.pm-close-btn {
  width: 32px;
  height: 32px;
  border-radius: var(--radius-sm);
  border: none;
  background: var(--surface-muted);
  color: var(--text-secondary);
  cursor: pointer;
  font-size: 20px;
  line-height: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background 0.15s ease, transform 0.1s ease;
}

.pm-close-btn:hover {
  background: var(--surface-soft);
  transform: translateY(-1px);
}

.pm-body {
  flex: 1;
  display: grid;
  grid-template-columns: 280px 1fr;
  overflow: hidden;
}

.pm-sidebar {
  border-right: 1px solid var(--line-soft);
  display: flex;
  flex-direction: column;
  background: var(--surface-muted);
}

.pm-sidebar-header {
  padding: 16px 16px 12px;
  background: var(--surface-base);
  border-bottom: 1px solid var(--line-soft);
}

.pm-sidebar-title {
  font-size: 14px;
  font-weight: 600;
  color: var(--text-strong);
}

.pm-sidebar-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: var(--text-muted);
}

.pm-conversation-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 6px 10px;
}

.pm-conversation-item {
  width: 100%;
  text-align: left;
  border: none;
  background: transparent;
  padding: 12px 14px;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: background 0.12s ease;
  color: inherit;
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 2px;
}

.pm-conversation-item:hover {
  background: var(--surface-soft);
}

.pm-conversation-item.active {
  background: var(--surface-base);
  box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
  border-left: 3px solid var(--brand-primary);
}

.pm-conv-avatar {
  width: 44px;
  height: 44px;
  border-radius: 50%;
  overflow: hidden;
  flex-shrink: 0;
  background: var(--surface-soft);
  display: flex;
  align-items: center;
  justify-content: center;
}

.pm-conv-avatar img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.pm-conv-main {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.pm-conv-top {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.pm-conv-name {
  font-size: 14px;
  font-weight: 600;
  color: var(--text-strong);
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}

.pm-conv-time {
  font-size: 11px;
  color: var(--text-muted);
}

.pm-conv-bottom {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 6px;
}

.pm-conv-last {
  font-size: 12px;
  color: var(--text-muted);
  max-width: 180px;
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}

.pm-conv-unread {
  min-width: 18px;
  height: 18px;
  padding: 0 6px;
  border-radius: 999px;
  background: var(--feedback-danger);
  color: #fff;
  font-size: 11px;
  font-weight: 500;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.pm-empty {
  padding: 40px 20px;
  text-align: center;
  font-size: 14px;
  color: var(--text-muted);
}

.pm-empty-tip {
  margin-top: 8px;
  font-size: 12px;
  color: var(--text-muted);
}

.pm-mutual-list {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.pm-mutual-header {
  padding: 14px 14px 10px;
  border-bottom: 1px solid rgba(148, 163, 184, 0.2);
}

.pm-mutual-title {
  font-size: 14px;
  font-weight: 600;
  display: block;
}

.pm-mutual-subtitle {
  margin-top: 4px;
  font-size: 12px;
  color: #9ca3af;
  display: block;
}

.pm-mutual-users {
  flex: 1;
  overflow-y: auto;
  padding: 8px 6px 10px;
}

.pm-mutual-item {
  width: 100%;
  text-align: left;
  border: none;
  background: transparent;
  padding: 12px 14px;
  border-radius: var(--radius-sm);
  cursor: pointer;
  transition: background 0.12s ease;
  color: inherit;
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 2px;
}

.pm-mutual-item:hover {
  background: var(--surface-soft);
}

.pm-mutual-item.active {
  background: var(--surface-base);
  box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
  border-left: 3px solid var(--brand-primary);
}

.pm-mutual-avatar {
  width: 44px;
  height: 44px;
  border-radius: 50%;
  overflow: hidden;
  background: var(--surface-soft);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.pm-mutual-avatar img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.pm-mutual-info {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.pm-mutual-name {
  font-size: 14px;
  font-weight: 600;
  color: var(--text-strong);
  white-space: nowrap;
  text-overflow: ellipsis;
  overflow: hidden;
}

.pm-mutual-status {
  font-size: 11px;
  color: var(--text-muted);
}

.pm-chat {
  display: flex;
  flex-direction: column;
  background: var(--surface-base);
  height: 100%;
  overflow: hidden;
}

.pm-chat-header {
  padding: 16px 20px;
  border-bottom: 1px solid var(--line-soft);
  display: flex;
  align-items: center;
  background: var(--surface-base);
  flex-shrink: 0;
}

.pm-chat-peer {
  display: flex;
  align-items: center;
  gap: 12px;
}

.pm-peer-avatar {
  width: 44px;
  height: 44px;
  border-radius: 50%;
  overflow: hidden;
  flex-shrink: 0;
  background: var(--surface-soft);
  display: flex;
  align-items: center;
  justify-content: center;
}

.pm-peer-avatar img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}

.pm-peer-info {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.pm-peer-name {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-strong);
}

.pm-peer-subtitle {
  font-size: 12px;
  color: var(--text-muted);
}

.pm-messages {
  flex: 1;
  min-height: 0;
  padding: 14px 16px 10px;
  overflow-y: auto;
  overflow-x: hidden;
  display: flex;
  flex-direction: column;
  gap: 8px;
  position: relative;
  -webkit-overflow-scrolling: touch;
  overscroll-behavior: contain;
}

.pm-loading {
  font-size: 14px;
  color: var(--text-muted);
  text-align: center;
  padding: 20px;
}

.pm-empty-messages {
  margin-top: 40px;
  font-size: 14px;
  color: var(--text-muted);
  text-align: center;
}

.pm-message-row {
  display: flex;
  margin-bottom: 4px;
}

.pm-message-row.is-me {
  justify-content: flex-end;
}

.pm-message-bubble {
  max-width: 68%;
  border-radius: var(--radius-md);
  padding: 10px 14px 8px;
  background: var(--surface-soft);
  color: var(--text-strong);
  box-shadow: 0 2px 8px rgba(15, 23, 42, 0.08);
}

.pm-message-row.is-me .pm-message-bubble {
  background: var(--brand-primary);
  color: #fff;
}

.pm-message-content {
  font-size: 13px;
  white-space: pre-wrap;
  word-break: break-word;
}

.pm-message-meta {
  margin-top: 2px;
  text-align: right;
}

.pm-message-time {
  font-size: 11px;
  color: var(--text-muted);
}

.pm-message-row.is-me .pm-message-time {
  color: rgba(255, 255, 255, 0.8);
}

.pm-input-bar {
  padding: 12px 16px 16px;
  border-top: 1px solid var(--line-soft);
  background: var(--surface-base);
  display: flex;
  flex-direction: column;
  gap: 8px;
  flex-shrink: 0;
}

.pm-input {
  width: 100%;
  resize: none;
  border-radius: var(--radius-sm);
  border: 1px solid var(--line-soft);
  padding: 10px 14px;
  background: var(--surface-muted);
  color: var(--text-strong);
  font-size: 14px;
  outline: none;
  transition: border 0.12s ease, box-shadow 0.12s ease;
  font-family: inherit;
}

.pm-input::placeholder {
  color: var(--text-muted);
}

.pm-input:focus {
  border-color: var(--brand-primary);
  box-shadow: 0 0 0 2px rgba(34, 191, 163, 0.1);
  background: var(--surface-base);
}

.pm-input-actions {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.pm-input-tip {
  font-size: 11px;
  color: var(--text-muted);
}

.pm-send-btn {
  padding: 8px 20px;
  border-radius: var(--radius-sm);
  border: none;
  background: var(--brand-primary);
  color: white;
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 4px;
  transition: background 0.15s ease, transform 0.08s ease;
}

.pm-send-btn:hover:not(:disabled) {
  background: var(--brand-secondary);
  transform: translateY(-1px);
}

.pm-send-btn:disabled {
  cursor: default;
  opacity: 0.5;
  background: var(--text-muted);
}

.pm-chat-empty {
  align-items: center;
  justify-content: center;
}

.pm-chat-placeholder {
  text-align: center;
  color: var(--text-muted);
  padding: 40px 20px;
}

.pm-chat-placeholder-title {
  font-size: 15px;
  font-weight: 600;
  color: var(--text-strong);
  margin-bottom: 8px;
}

.pm-chat-placeholder-subtitle {
  font-size: 13px;
  color: var(--text-muted);
}

/* 私信面板内滚动条样式（与页面滚动条明显区分） */
.pm-conversation-list,
.pm-mutual-users,
.pm-messages {
  scrollbar-width: thin; /* Firefox */
  scrollbar-color: var(--brand-primary) rgba(148, 163, 184, 0.18);
}

.pm-conversation-list::-webkit-scrollbar,
.pm-mutual-users::-webkit-scrollbar,
.pm-messages::-webkit-scrollbar {
  width: 8px;
}

.pm-conversation-list::-webkit-scrollbar-track,
.pm-mutual-users::-webkit-scrollbar-track,
.pm-messages::-webkit-scrollbar-track {
  background: rgba(15, 23, 42, 0.03);
  border-radius: 999px;
}

.pm-conversation-list::-webkit-scrollbar-thumb,
.pm-mutual-users::-webkit-scrollbar-thumb,
.pm-messages::-webkit-scrollbar-thumb {
  background: linear-gradient(
    180deg,
    var(--brand-primary),
    var(--brand-secondary)
  );
  border-radius: 999px;
}

.pm-conversation-list::-webkit-scrollbar-thumb:hover,
.pm-mutual-users::-webkit-scrollbar-thumb:hover,
.pm-messages::-webkit-scrollbar-thumb:hover {
  background: linear-gradient(
    180deg,
    var(--brand-secondary),
    var(--brand-primary)
  );
}

@media (max-width: 960px) {
  .pm-container {
    width: 100%;
  }
}
</style>

