<template>
  <Teleport to="body">
    <Transition name="toast">
      <div v-if="visible" class="message-toast-overlay" @click.self="handleClose">
        <div class="message-toast" :class="`toast-${type}`">
          <div class="toast-icon" :class="`icon-${type}`">
            <svg v-if="type === 'success'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M20 6L9 17l-5-5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <svg v-else-if="type === 'error'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M18 6L6 18M6 6l12 12" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <svg v-else viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12 16v-4M12 8h.01" stroke-linecap="round" stroke-linejoin="round"/>
              <circle cx="12" cy="12" r="10"/>
            </svg>
          </div>
          <div class="toast-content">
            <p class="toast-message">{{ message }}</p>
            <p v-if="countdown > 0 && redirectTo" class="toast-countdown">{{ countdown }}秒后自动跳转...</p>
          </div>
          <button v-if="showClose" class="toast-close" @click="handleClose">×</button>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'

const props = defineProps({
  message: {
    type: String,
    required: true
  },
  type: {
    type: String,
    default: 'success', // success, error, info
    validator: (value) => ['success', 'error', 'info'].includes(value)
  },
  duration: {
    type: Number,
    default: 2000 // 默认2秒后跳转
  },
  redirectTo: {
    type: [String, Object],
    default: null // 跳转路径，可以是字符串路径或路由对象
  },
  showClose: {
    type: Boolean,
    default: true // 是否显示关闭按钮
  },
  autoClose: {
    type: Boolean,
    default: true // 是否自动关闭并跳转
  }
})

const emit = defineEmits(['close', 'redirect'])

const visible = ref(false)
const countdown = ref(0)
const router = useRouter()
let countdownTimer = null
let closeTimer = null

const handleClose = () => {
  visible.value = false
  clearTimers()
  emit('close')
}

const clearTimers = () => {
  if (countdownTimer) {
    clearInterval(countdownTimer)
    countdownTimer = null
  }
  if (closeTimer) {
    clearTimeout(closeTimer)
    closeTimer = null
  }
}

const startCountdown = () => {
  if (!props.autoClose) return
  
  const duration = props.duration || 2000
  countdown.value = Math.ceil(duration / 1000)
  
  if (props.redirectTo) {
    // 有跳转路径时显示倒计时
    countdownTimer = setInterval(() => {
      countdown.value--
      if (countdown.value <= 0) {
        clearTimers()
        handleRedirect()
      }
    }, 1000)
  }
  
  // 无论是否有跳转，都设置自动关闭
  closeTimer = setTimeout(() => {
    handleRedirect()
  }, duration)
}

const handleRedirect = () => {
  if (props.redirectTo) {
    emit('redirect')
    if (typeof props.redirectTo === 'string') {
      router.push(props.redirectTo)
    } else {
      router.push(props.redirectTo)
    }
    handleClose()
  } else {
    handleClose()
  }
}

// 监听 visible 变化，显示时开始倒计时
watch(visible, (newVal) => {
  if (newVal) {
    startCountdown()
  } else {
    clearTimers()
  }
})

// 暴露方法供父组件调用
const show = () => {
  visible.value = true
}

const hide = () => {
  handleClose()
}

defineExpose({
  show,
  hide
})

onMounted(() => {
  // 组件挂载后自动显示
  visible.value = true
})

onUnmounted(() => {
  clearTimers()
})
</script>

<style scoped>
.message-toast-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.4);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 9999;
  backdrop-filter: blur(2px);
}

.message-toast {
  background: white;
  border-radius: 12px;
  padding: 24px 32px;
  min-width: 320px;
  max-width: 90%;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
  display: flex;
  align-items: center;
  gap: 16px;
  position: relative;
  animation: slideUp 0.3s ease-out;
}

.toast-icon {
  width: 40px;
  height: 40px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 50%;
}

.icon-success {
  background: #e6f7e6;
  color: #52c41a;
}

.icon-error {
  background: #fff1f0;
  color: #ff4d4f;
}

.icon-info {
  background: #e6f4ff;
  color: #1890ff;
}

.toast-icon svg {
  width: 24px;
  height: 24px;
}

.toast-content {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.toast-message {
  margin: 0;
  font-size: 16px;
  font-weight: 500;
  color: #333;
  line-height: 1.5;
}

.toast-countdown {
  margin: 0;
  font-size: 13px;
  color: #999;
  line-height: 1.4;
}

.toast-close {
  position: absolute;
  top: 12px;
  right: 12px;
  width: 24px;
  height: 24px;
  border: none;
  background: transparent;
  color: #999;
  font-size: 24px;
  line-height: 1;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
  transition: all 0.2s;
  padding: 0;
}

.toast-close:hover {
  background: #f5f5f5;
  color: #666;
}

@keyframes slideUp {
  from {
    opacity: 0;
    transform: translateY(20px) scale(0.95);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}

.toast-enter-active,
.toast-leave-active {
  transition: opacity 0.3s ease;
}

.toast-enter-from,
.toast-leave-to {
  opacity: 0;
}

.toast-enter-active .message-toast,
.toast-leave-active .message-toast {
  transition: transform 0.3s ease, opacity 0.3s ease;
}

.toast-enter-from .message-toast,
.toast-leave-to .message-toast {
  transform: translateY(20px) scale(0.95);
  opacity: 0;
}
</style>
