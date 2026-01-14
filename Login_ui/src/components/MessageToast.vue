<template>
  <Teleport to="body">
    <Transition name="toast">
      <div v-if="visible" class="message-toast-overlay">
        <div class="message-toast" :class="`toast-${type}`">
          <div class="toast-icon" :class="`icon-${type}`">
            <svg v-if="type === 'success'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M20 6L9 17l-5-5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <svg v-else-if="type === 'error'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M18 6L6 18M6 6l12 12" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <svg v-else-if="type === 'info'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M12 16v-4M12 8h.01" stroke-linecap="round" stroke-linejoin="round"/>
              <circle cx="12" cy="12" r="10"/>
            </svg>
            <svg v-else-if="type === 'confirm'" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M9 12l2 2 4-4" stroke-linecap="round" stroke-linejoin="round"/>
              <circle cx="12" cy="12" r="10"/>
            </svg>
          </div>
          <div class="toast-content">
            <p class="toast-message" v-html="formattedMessage"></p>
            <p v-if="countdown > 0 && redirectTo" class="toast-countdown">{{ countdown }}秒后自动跳转...</p>
          </div>
          <button v-if="showClose && type !== 'confirm'" class="toast-close" @click="handleClose">×</button>
          <div v-if="type === 'confirm'" class="toast-actions">
            <button class="toast-btn toast-btn-cancel" @click="handleCancel">取消</button>
            <button class="toast-btn toast-btn-confirm" @click="handleConfirm">确认</button>
          </div>
        </div>
      </div>
    </Transition>
  </Teleport>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted, computed } from 'vue'
import { useRouter } from 'vue-router'

const props = defineProps({
  message: {
    type: String,
    required: true
  },
  type: {
    type: String,
    default: 'success', // success, error, info, confirm
    validator: (value) => ['success', 'error', 'info', 'confirm'].includes(value)
  },
  duration: {
    type: Number,
    default: 1200 // 默认1.2秒后跳转
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
  },
  isConfirm: {
    type: Boolean,
    default: false // 是否为确认对话框
  }
})

const emit = defineEmits(['close', 'redirect', 'confirm', 'cancel'])

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

const handleConfirm = () => {
  visible.value = false
  clearTimers()
  emit('confirm')
  emit('close')
}

const handleCancel = () => {
  visible.value = false
  clearTimers()
  emit('cancel')
  emit('close')
}

// 格式化消息，将 \n 转换为 <br>
const formattedMessage = computed(() => {
  if (!props.message) return ''
  return props.message.replace(/\n/g, '<br>')
})

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
  // 确认对话框不自动关闭
  if (props.type === 'confirm' || !props.autoClose) return
  
  const duration = props.duration || 1200
  // 如果时间小于等于1.5秒，不显示倒计时文字
  if (duration > 1500) {
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
  background: rgba(0, 0, 0, 0.35);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 9999;
  backdrop-filter: blur(3px);
  transition: background 0.2s ease, backdrop-filter 0.2s ease;
}

.message-toast {
  background: var(--surface-base);
  border-radius: var(--radius-md);
  padding: 24px 32px;
  min-width: 320px;
  max-width: 90%;
  box-shadow: var(--shadow-lg);
  display: flex;
  align-items: center;
  gap: 16px;
  position: relative;
  animation: slideUp 0.25s cubic-bezier(0.34, 1.56, 0.64, 1);
  border: 1px solid var(--line-soft);
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
  background: rgba(34, 191, 163, 0.1);
  color: var(--brand-primary);
}

.icon-error {
  background: rgba(255, 59, 48, 0.1);
  color: var(--feedback-danger);
}

.icon-info {
  background: rgba(34, 191, 163, 0.1);
  color: var(--brand-primary);
}

.icon-confirm {
  background: rgba(255, 149, 0, 0.1);
  color: var(--feedback-warning);
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
  color: var(--text-strong);
  line-height: 1.5;
}

.toast-countdown {
  margin: 0;
  font-size: 13px;
  color: var(--text-muted);
  line-height: 1.4;
}

.toast-close {
  position: absolute;
  top: 12px;
  right: 12px;
  width: 28px;
  height: 28px;
  border: none;
  background: transparent;
  color: var(--text-muted);
  font-size: 20px;
  line-height: 1;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: var(--radius-xs);
  transition: all var(--transition-fast);
  padding: 0;
}

.toast-close:hover {
  background: var(--surface-soft);
  color: var(--text-primary);
}

.toast-actions {
  display: flex;
  gap: 12px;
  margin-top: 20px;
  justify-content: flex-end;
}

.toast-btn {
  padding: 10px 24px;
  border: none;
  border-radius: var(--radius-sm);
  font-size: 14px;
  font-weight: 500;
  cursor: pointer;
  transition: all var(--transition-base);
}

.toast-btn-cancel {
  background: var(--surface-soft);
  color: var(--text-secondary);
}

.toast-btn-cancel:hover {
  background: var(--line-soft);
  color: var(--text-primary);
}

.toast-btn-confirm {
  background: var(--brand-primary);
  color: white;
}

.toast-btn-confirm:hover {
  background: var(--brand-primary-hover);
}

.toast-confirm .toast-content {
  flex-direction: column;
  align-items: flex-start;
}

@keyframes slideUp {
  from {
    opacity: 0;
    transform: translateY(15px) scale(0.96);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}

.toast-enter-active,
.toast-leave-active {
  transition: opacity 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.toast-enter-from,
.toast-leave-to {
  opacity: 0;
}

.toast-enter-active .message-toast,
.toast-leave-active .message-toast {
  transition: transform 0.2s cubic-bezier(0.34, 1.56, 0.64, 1), opacity 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.toast-enter-from .message-toast {
  transform: translateY(15px) scale(0.96);
  opacity: 0;
}

.toast-leave-to .message-toast {
  transform: translateY(-10px) scale(0.98);
  opacity: 0;
}
</style>
