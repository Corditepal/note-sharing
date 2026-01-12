<template>
  <button 
    class="follow-button"
    :class="{ 
      following: isFollowingValue, 
      loading: loading,
      small: size === 'small',
      large: size === 'large'
    }"
    @click="handleClick"
    :disabled="loading || !currentUserId || targetUserId === currentUserId"
  >
    <span v-if="loading" class="button-text">处理中...</span>
    <span v-else-if="isFollowingValue" class="button-text">已关注</span>
    <span v-else class="button-text">关注</span>
  </button>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useUserStore } from '@/stores/user'
import { followUser, unfollowUser, isFollowing } from '@/api/follow'
import { useMessage } from '@/utils/message'

const props = defineProps({
  targetUserId: {
    type: Number,
    required: true
  },
  size: {
    type: String,
    default: 'medium',
    validator: (value) => ['small', 'medium', 'large'].includes(value)
  },
  autoCheck: {
    type: Boolean,
    default: true
  }
})

const emit = defineEmits(['follow', 'unfollow', 'change'])

const userStore = useUserStore()
const { userInfo } = storeToRefs(userStore)
const { showSuccess, showError } = useMessage()

const currentUserId = computed(() => userInfo.value?.id)
const isFollowingValue = ref(false)
const loading = ref(false)

// 检查关注状态
const checkFollowStatus = async () => {
  if (!props.autoCheck || !currentUserId.value || props.targetUserId === currentUserId.value) {
    return
  }

  try {
    const status = await isFollowing(currentUserId.value, props.targetUserId)
    isFollowingValue.value = status
  } catch (err) {
    console.error('检查关注状态失败:', err)
    isFollowingValue.value = false
  }
}

// 处理点击
const handleClick = async () => {
  if (!currentUserId.value) {
    showError('请先登录')
    return
  }

  if (props.targetUserId === currentUserId.value) {
    showError('不能关注自己')
    return
  }

  loading.value = true

  try {
    if (isFollowingValue.value) {
      await unfollowUser(currentUserId.value, props.targetUserId)
      isFollowingValue.value = false
      showSuccess('取消关注成功')
      emit('unfollow', props.targetUserId)
    } else {
      await followUser(currentUserId.value, props.targetUserId)
      isFollowingValue.value = true
      showSuccess('关注成功')
      emit('follow', props.targetUserId)
    }
    emit('change', isFollowingValue.value)
  } catch (err) {
    const errorMsg = err.message || err.response?.data?.message || '操作失败，请稍后重试'
    showError(errorMsg)
  } finally {
    loading.value = false
  }
}

// 监听 targetUserId 变化，重新检查关注状态
watch(() => props.targetUserId, () => {
  if (props.autoCheck) {
    checkFollowStatus()
  }
})

// 监听登录状态变化
watch(() => currentUserId.value, () => {
  if (props.autoCheck) {
    checkFollowStatus()
  }
})

onMounted(() => {
  if (props.autoCheck) {
    checkFollowStatus()
  }
})

// 暴露方法供父组件调用
defineExpose({
  checkFollowStatus,
  setFollowing: (value) => {
    isFollowingValue.value = value
  },
  getFollowing: () => isFollowingValue.value
})
</script>

<style scoped>
.follow-button {
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
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.follow-button:hover:not(:disabled) {
  filter: brightness(0.95);
}

.follow-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.follow-button.following {
  background: transparent;
  color: var(--text-secondary, #4b5563);
  border-color: var(--line-soft, #e8ecec);
}

.follow-button.following:hover:not(:disabled) {
  border-color: var(--text-danger, #c6534c);
  color: var(--text-danger, #c6534c);
}

.follow-button.small {
  padding: 6px 16px;
  font-size: 12px;
  min-width: 70px;
}

.follow-button.large {
  padding: 12px 28px;
  font-size: 16px;
  min-width: 100px;
}

.follow-button.loading {
  cursor: wait;
}

.button-text {
  display: inline-block;
}
</style>
