<template>
  <div class="profile-page">
    <div class="profile-card">
      <header class="profile-header">
        <div class="avatar-shell" @click="triggerFileInput" aria-hidden="true">
          <img 
            :src="avatarDisplayUrl" 
            alt="用户头像" 
            class="avatar-img"
            @error="handleImageError"
            @load="handleImageLoad"
          />
          <div class="avatar-overlay">
            <span class="avatar-upload-hint" v-if="!isUploadingAvatar">点击上传</span>
            <span class="avatar-upload-hint" v-else>上传中...</span>
          </div>
          <div v-if="isUploadingAvatar" class="avatar-uploading-indicator">
            <div class="upload-spinner"></div>
          </div>
          <input
            ref="fileInputRef"
            type="file"
            accept="image/*"
            style="display: none"
            @change="handleFileSelect"
          />
        </div>
        <div class="header-text">
          <p class="section-label">账户设置</p>
          <h2>个人信息</h2>
        </div>
      </header>

      <div class="profile-content">
        <div class="info-section">
          <div class="info-item">
            <label>用户名</label>
            <div class="info-value">{{ userInfo.username || 'N/A' }}</div>
          </div>

          <div class="info-item">
            <label>学号</label>
            <div class="info-value">{{ userInfo.studentNumber || '未填写' }}</div>
          </div>

          <div class="info-item">
            <label>邮箱</label>
            <div class="info-value">{{ userInfo.email || 'N/A' }}</div>
          </div>

          <p class="info-note">* 邮箱不可修改，重置密码验证码将发送到此邮箱</p>
        </div>

        <div class="actions-section">
          <button class="text-action" @click="showChangePasswordDialog = true">
            <span>重置密码</span>
            <span class="action-indicator" aria-hidden="true">↗</span>
          </button>
          <button class="text-action danger" @click="handleLogout">
            <span>退出登录</span>
            <span class="action-indicator" aria-hidden="true">↗</span>
          </button>
        </div>
      </div>
    </div>

    <div v-if="showChangePasswordDialog" class="modal" @click.self="closePasswordDialog">
      <div class="modal-content">
        <h3>重置密码</h3>
        <p v-if="userInfo.email">验证码将发送至您的注册邮箱：**{{ userInfo.email }}**</p>

        <div class="form-group code-group">
          <label>邮箱验证码</label>
          <div class="input-with-button">
            <input
                v-model="passwordForm.code"
                type="text"
                placeholder="请输入邮箱验证码"
            />
            <button
                :disabled="isSendingCode || codeCountdown > 0 || !userInfo.email"
                class="code-button"
                @click="sendResetCode"
            >
              {{ codeCountdown > 0 ? `${codeCountdown}s 后重发` : '发送验证码' }}
            </button>
          </div>
        </div>

        <div class="form-group">
          <label>新密码</label>
          <input
              v-model="passwordForm.newPassword"
              type="password"
              placeholder="请输入新密码 (至少6位)"
          />
        </div>

        <div class="form-group">
          <label>确认新密码</label>
          <input
              v-model="passwordForm.confirmPassword"
              type="password"
              placeholder="请再次输入新密码"
          />
        </div>

        <div v-if="passwordError" class="error-message">
          {{ passwordError }}
        </div>

        <div class="modal-actions">
          <button @click="closePasswordDialog">取消</button>
          <button class="primary" @click="handleResetPassword">确认重置</button>
        </div>
      </div>
    </div>

    <div v-if="showLogoutDialog" class="modal" @click.self="showLogoutDialog = false">
      <div class="modal-content">
        <h3>确认退出</h3>
        <p>确定要退出登录吗?</p>
        <div class="modal-actions">
          <button @click="showLogoutDialog = false">取消</button>
          <button class="primary danger" @click="confirmLogout">确认退出</button>
        </div>
      </div>
    </div>

    <!-- 图片裁剪对话框 -->
    <div v-if="showCropDialog" class="modal crop-modal" @click.self="closeCropDialog">
      <div class="modal-content crop-modal-content">
        <h3>裁剪头像</h3>
        <p class="crop-hint">拖动选择框调整裁剪区域，支持缩放和移动</p>
        <div class="crop-container">
          <img
            ref="cropperImageRef"
            :src="cropImageSrc"
            alt="裁剪图片"
            class="cropper-image"
          />
        </div>
        <div class="modal-actions">
          <button @click="closeCropDialog">取消</button>
          <button class="primary" @click="confirmCrop">确认裁剪</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick } from 'vue'
import { useRouter } from 'vue-router'
import { storeToRefs } from 'pinia'
import { useUserStore } from '@/stores/user'
import request from '@/api/request'
import Cropper from 'cropperjs'
import 'cropperjs/dist/cropper.css'

const router = useRouter()
const API_BASE_URL = '/auth'

// --- Pinia 整合 ---
const userStore = useUserStore()
const { userInfo, isLoggedIn } = storeToRefs(userStore)

// --- 本地状态 ---
const showChangePasswordDialog = ref(false)
const showLogoutDialog = ref(false)
const passwordError = ref('')
const fileInputRef = ref(null)
const isUploadingAvatar = ref(false)
const previewAvatarUrl = ref(null) // 上传前的预览URL
const showCropDialog = ref(false) // 是否显示裁剪对话框
const cropImageSrc = ref('') // 裁剪的图片源
const cropperImageRef = ref(null) // 裁剪图片元素引用
const cropperInstance = ref(null) // Cropper实例

const passwordForm = ref({
  code: '',
  newPassword: '',
  confirmPassword: ''
})

// 验证码发送相关状态
const isSendingCode = ref(false)
const codeCountdown = ref(0)
let timer = null

// ------------------------------------
// 1. 数据加载与同步
// ------------------------------------

// 计算头像显示URL（优先显示预览，然后是用户头像，最后是默认头像）
const avatarDisplayUrl = computed(() => {
  // 如果有预览URL（正在上传），显示预览
  if (previewAvatarUrl.value) {
    return previewAvatarUrl.value
  }
  // 否则显示用户头像或默认头像
  const url = userInfo.value.avatarUrl || '/assets/avatars/avatar.png'
  return url
})

// 图片加载错误处理
const handleImageError = (event) => {
  console.error('图片加载失败:', event.target.src)
  const failedUrl = event.target.src
  
  // 如果是预览URL失败，清除预览
  if (previewAvatarUrl.value && failedUrl === previewAvatarUrl.value) {
    URL.revokeObjectURL(previewAvatarUrl.value)
    previewAvatarUrl.value = null
    return
  }
  
  // 如果是服务器头像URL失败，尝试重试或回退
  if (userInfo.value.avatarUrl && failedUrl.includes(userInfo.value.avatarUrl)) {
    // 移除时间戳参数后重试
    const baseUrl = userInfo.value.avatarUrl.split('?')[0].split('&')[0]
    
    // 如果当前URL包含时间戳，尝试不带时间戳的URL
    if (failedUrl.includes('_t=') && failedUrl !== baseUrl) {
      console.log('重试加载图片（不带时间戳）:', baseUrl)
      event.target.src = baseUrl
      return
    }
    
    // 如果还是失败，等待一下再重试（可能是MinIO URL还没生效）
    if (failedUrl === baseUrl) {
      console.warn('图片加载失败，等待后重试')
      setTimeout(() => {
        if (event.target.src === baseUrl) {
          const retryUrl = baseUrl + (baseUrl.includes('?') ? '&' : '?') + '_retry=' + Date.now()
          event.target.src = retryUrl
        }
      }, 1000)
      return
    }
    
    // 最终回退到默认头像
    console.warn('自定义头像加载失败，使用默认头像')
    event.target.src = '/assets/avatars/avatar.png'
  }
}

// 图片加载成功
const handleImageLoad = (event) => {
  console.log('图片加载成功:', event.target.src)
}

const loadUserInfo = async () => {
  // 检查 Store 中是否有数据，如果没有且理论上已登录（有Token），则请求 /me
  if (!userInfo.value.email && isLoggedIn.value) {
    try {
      const res = await request.get('/auth/me');
      // 使用 Store Action 更新数据
      userStore.setUserData(res.data);
    } catch (error) {
      console.error('获取用户信息失败，执行登出:', error);
      userStore.clearUserData();
      router.push('/login');
    }
  } else if (!isLoggedIn.value) {
    // 如果没有 Token，确保跳转到登录页
    router.push('/login');
  }
}

// ------------------------------------
// 2. 发送验证码逻辑 (/api/v1/auth/password/send-code)
// ------------------------------------

const sendResetCode = async () => {
  const email = userInfo.value.email;

  if (!email || isSendingCode.value || codeCountdown.value > 0) {
    if (!email) passwordError.value = '无法获取用户邮箱地址。';
    return;
  }

  isSendingCode.value = true;
  codeCountdown.value = 60;
  passwordError.value = '';

  try {
    const res = await request.post(`${API_BASE_URL}/password/send-code`, { email: email });

    alert(res.message || '验证码已发送到您的邮箱。');
    startCountdown();

  } catch (error) {
    // 假设 request 库在错误时抛出包含后端错误信息的对象
    const errorMessage = error.response?.data?.error || error.response?.data?.message || '验证码发送失败，请稍后重试。';
    passwordError.value = errorMessage;
    codeCountdown.value = 0;
    isSendingCode.value = false;
  }
}

const startCountdown = () => {
  if (timer) clearInterval(timer);
  timer = setInterval(() => {
    if (codeCountdown.value > 0) {
      codeCountdown.value--;
    } else {
      clearInterval(timer);
      isSendingCode.value = false;
    }
  }, 1000);
}

// ------------------------------------
// 3. 重置密码逻辑 (/api/v1/auth/password/reset)
// ------------------------------------

const handleResetPassword = async () => {
  passwordError.value = ''

  // 1. 表单验证
  const { code, newPassword, confirmPassword } = passwordForm.value;

  if (!code) { passwordError.value = '请输入邮箱验证码'; return; }
  if (!newPassword || !confirmPassword) { passwordError.value = '新密码和确认密码不能为空'; return; }
  if (newPassword.length < 6) { passwordError.value = '新密码长度不能少于6位'; return; }
  if (newPassword !== confirmPassword) { passwordError.value = '两次输入的新密码不一致'; return; }

  // 2. 调用后端接口
  try {
    const payload = {
      email: userInfo.value.email,
      newPassword: newPassword,
      code: code
    }

    const res = await request.post(`${API_BASE_URL}/password/reset`, payload);

    alert(res.message || '密码重置成功，请使用新密码重新登录！');
    closePasswordDialog();
    confirmLogout();

  } catch (error) {
    const errorMessage = error.response?.data?.error || error.response?.data?.message || '重置密码失败,请稍后重试。';
    console.error('重置密码失败:', error);
    passwordError.value = errorMessage;
  }
}

const closePasswordDialog = () => {
  showChangePasswordDialog.value = false
  clearInterval(timer);
  codeCountdown.value = 0;
  isSendingCode.value = false;
  passwordForm.value = { code: '', newPassword: '', confirmPassword: '' }
  passwordError.value = ''
}

// ------------------------------------
// 4. 退出登录逻辑
// ------------------------------------

const handleLogout = () => {
  showLogoutDialog.value = true
}

const confirmLogout = () => {
  // 使用 Store Action 清除数据和 token
  userStore.clearUserData()
  router.push('/login')
}

// ------------------------------------
// 5. 头像上传逻辑
// ------------------------------------

const triggerFileInput = () => {
  fileInputRef.value?.click()
}

const handleFileSelect = async (event) => {
  const file = event.target.files?.[0]
  if (!file) return

  // 验证文件类型
  if (!file.type.startsWith('image/')) {
    alert('只能上传图片文件')
    return
  }

  // 验证文件大小（5MB）
  const maxSize = 5 * 1024 * 1024
  if (file.size > maxSize) {
    alert('图片大小不能超过5MB')
    return
  }

  // 1. 创建预览URL并打开裁剪对话框
  cropImageSrc.value = URL.createObjectURL(file)
  showCropDialog.value = true
  
  // 等待DOM更新后初始化Cropper
  nextTick(() => {
    if (cropperImageRef.value && !cropperInstance.value) {
      cropperInstance.value = new Cropper(cropperImageRef.value, {
        aspectRatio: 1,
        viewMode: 1,
        guides: true,
        background: true,
        autoCropArea: 0.8,
        dragMode: 'move',
        cropBoxMovable: true,
        cropBoxResizable: true,
        toggleDragModeOnDblclick: false,
      })
    }
  })
}

// 关闭裁剪对话框
const closeCropDialog = () => {
  showCropDialog.value = false
  // 销毁Cropper实例
  if (cropperInstance.value) {
    cropperInstance.value.destroy()
    cropperInstance.value = null
  }
  // 释放预览URL内存
  if (cropImageSrc.value) {
    URL.revokeObjectURL(cropImageSrc.value)
    cropImageSrc.value = ''
  }
  // 清空文件输入
  if (fileInputRef.value) {
    fileInputRef.value.value = ''
  }
}

// 确认裁剪并上传
const confirmCrop = async () => {
  if (!cropperInstance.value) {
    alert('裁剪器未准备好')
    return
  }

  try {
    // 获取裁剪后的Canvas
    const canvas = cropperInstance.value.getCroppedCanvas({
      width: 400,
      height: 400,
      imageSmoothingEnabled: true,
      imageSmoothingQuality: 'high'
    })

    if (!canvas) {
      alert('裁剪失败，请重试')
      return
    }

    // 将Canvas转换为Blob
    canvas.toBlob(async (blob) => {
      if (!blob) {
        alert('图片转换失败')
        return
      }

      // 创建预览URL
      previewAvatarUrl.value = URL.createObjectURL(blob)
      
      // 关闭裁剪对话框
      showCropDialog.value = false
      
      // 释放原始图片URL
      if (cropImageSrc.value) {
        URL.revokeObjectURL(cropImageSrc.value)
        cropImageSrc.value = ''
      }

      // 开始上传
      isUploadingAvatar.value = true

      try {
        const formData = new FormData()
        // 将Blob转换为File对象
        const croppedFile = new File([blob], 'avatar.jpg', { type: 'image/jpeg' })
        formData.append('file', croppedFile)

        console.log('准备上传裁剪后的头像')

        const res = await request.post('/auth/avatar', formData)

        console.log('上传响应:', res)
        
        // 检查响应结构
        const avatarUrl = res.data?.data?.avatarUrl || res.data?.avatarUrl
        
        if (avatarUrl) {
          console.log('获取到头像URL:', avatarUrl)
          
          // 更新store中的头像URL
          userInfo.value.avatarUrl = avatarUrl
          
          // 重新获取用户信息以确保数据同步
          await loadUserInfo()
          
          // 清除预览URL
          if (previewAvatarUrl.value) {
            URL.revokeObjectURL(previewAvatarUrl.value)
            previewAvatarUrl.value = null
          }
          
          alert('头像上传成功，页面将刷新')
          
          // 刷新页面以确保头像正常显示
          setTimeout(() => {
            window.location.reload()
          }, 500)
        } else {
          console.error('响应中没有找到avatarUrl:', res.data)
          // 清除预览URL
          if (previewAvatarUrl.value) {
            URL.revokeObjectURL(previewAvatarUrl.value)
            previewAvatarUrl.value = null
          }
          alert('头像上传失败：响应中没有找到头像URL')
        }
      } catch (error) {
        console.error('上传头像失败:', error)
        // 清除预览URL
        if (previewAvatarUrl.value) {
          URL.revokeObjectURL(previewAvatarUrl.value)
          previewAvatarUrl.value = null
        }
        const errorMessage = error.response?.data?.error || '上传头像失败，请稍后重试'
        alert(errorMessage)
      } finally {
        isUploadingAvatar.value = false
        // 清空文件输入
        if (fileInputRef.value) {
          fileInputRef.value.value = ''
        }
      }
    }, 'image/jpeg', 0.9) // 90%质量，JPEG格式

  } catch (error) {
    console.error('裁剪失败:', error)
    alert('裁剪失败，请重试')
  }
}

// ------------------------------------
// 6. 生命周期
// ------------------------------------

onMounted(() => {
  loadUserInfo()
})
</script>

<style scoped>
.profile-page {
  min-height: 100vh;
  padding: 48px 20px 80px;
  background: var(--surface-muted, #f8faf9);
  display: flex;
  justify-content: center;
}

.profile-card {
  width: min(760px, 100%);
  background: var(--surface-base, #ffffff);
  border-radius: 32px;
  border: 1px solid var(--line-soft, #e8ecec);
  box-shadow: 0 20px 50px rgba(15, 23, 42, 0.08);
  overflow: hidden;
}

.profile-header {
  display: flex;
  gap: 24px;
  align-items: center;
  padding: 36px 40px 24px;
  border-bottom: 1px solid var(--line-soft, #e8ecec);
}

.avatar-shell {
  width: 72px;
  height: 72px;
  border-radius: 18px;
  border: 1px solid var(--brand-primary, #22ee99);
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  overflow: hidden;
  cursor: pointer;
  transition: all 0.3s ease;
}

.avatar-shell:hover {
  border-color: var(--brand-primary, #22ee99);
  box-shadow: 0 0 0 3px rgba(34, 238, 153, 0.1);
}

.avatar-shell:hover .avatar-overlay {
  opacity: 1;
}

.avatar-icon {
  width: 60%;
  height: 60%;
  border-radius: 50%;
  border: 2px solid var(--brand-primary, #22ee99);
  position: relative;
}

.avatar-icon::after {
  content: '';
  position: absolute;
  bottom: -20%;
  left: 50%;
  transform: translateX(-50%);
  width: 120%;
  height: 50%;
  border: 2px solid var(--brand-primary, #22ee99);
  border-top: none;
  border-radius: 40% 40% 60% 60%;
}

.avatar-img {
  width: 100%;
  height: 100%;
  object-fit: cover;
  border-radius: 18px;
  transition: transform 0.3s ease;
}

.avatar-shell:hover .avatar-img {
  transform: scale(1.05);
}

.avatar-overlay {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 18px;
  opacity: 0;
  transition: opacity 0.3s ease;
}

.avatar-upload-hint {
  color: white;
  font-size: 12px;
  font-weight: 500;
  text-align: center;
}

/* 裁剪对话框样式 */
.crop-modal {
  z-index: 2000;
}

.crop-modal-content {
  width: min(600px, 90vw);
  max-width: 90vw;
  max-height: 90vh;
  overflow-y: auto;
}

.crop-hint {
  margin: 8px 0 16px;
  font-size: 14px;
  color: var(--text-muted, #8a9199);
  text-align: center;
}

.crop-container {
  margin: 20px 0;
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 400px;
  background: var(--surface-muted, #f8faf9);
  border-radius: 12px;
  overflow: hidden;
}

.crop-container .cropper-image {
  max-width: 100%;
  max-height: 500px;
  display: block;
}

.avatar-uploading-indicator {
  position: absolute;
  inset: 0;
  background: rgba(0, 0, 0, 0.3);
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 18px;
  z-index: 10;
}

.upload-spinner {
  width: 24px;
  height: 24px;
  border: 3px solid rgba(255, 255, 255, 0.3);
  border-top-color: white;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

.header-text h2 {
  margin: 4px 0 0;
  font-size: 28px;
  color: var(--text-strong, #1f2a37);
}

.section-label {
  font-size: 14px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-muted, #8a9199);
  margin: 0;
}

.profile-content {
  padding: 32px 40px 40px;
  display: flex;
  flex-direction: column;
  gap: 40px;
}

.info-section {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.info-item {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--line-soft, #e8ecec);
}

.info-item:last-of-type {
  border-bottom: none;
  padding-bottom: 0;
}

.info-item label {
  font-size: 14px;
  color: var(--text-muted, #8a9199);
  letter-spacing: 0.05em;
}

.info-value {
  font-size: 20px;
  color: var(--text-strong, #1f2a37);
  font-weight: 600;
}

.info-note {
  margin: -8px 0 0;
  font-size: 13px;
  color: var(--text-muted, #8a9199);
}

.actions-section {
  border-top: 1px solid var(--line-soft, #e8ecec);
  padding-top: 24px;
  display: flex;
  flex-direction: column;
}

.text-action {
  appearance: none;
  border: none;
  background: transparent;
  padding: 16px 0;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 16px;
  color: var(--text-secondary, #4b5563);
  cursor: pointer;
  transition: color 0.2s ease;
  border-bottom: 1px solid var(--line-soft, #e8ecec);
}

.text-action:last-child {
  border-bottom: none;
}

.text-action:hover,
.text-action:focus-visible {
  color: var(--brand-primary, #22ee99);
}

.text-action.danger {
  color: var(--text-danger, #c6534c);
}

.text-action.danger:hover,
.text-action.danger:focus-visible {
  color: var(--brand-primary, #22ee99);
}

.action-indicator {
  font-size: 18px;
  color: inherit;
}

.modal {
  position: fixed;
  inset: 0;
  background: rgba(15, 23, 42, 0.45);
  display: flex;
  justify-content: center;
  align-items: center;
  padding: 24px;
  z-index: 1000;
}

.modal-content {
  width: min(440px, 100%);
  background: var(--surface-base, #ffffff);
  border-radius: 28px;
  border: 1px solid var(--line-soft, #e8ecec);
  padding: 32px;
  box-shadow: 0 25px 60px rgba(15, 23, 42, 0.2);
}

.modal-content h3 {
  margin: 0 0 16px;
  font-size: 22px;
  color: var(--text-strong, #1f2a37);
}

.modal-content p {
  margin: 8px 0 24px;
  color: var(--text-secondary, #4b5563);
  font-size: 15px;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 18px;
}

.form-group label {
  font-size: 14px;
  color: var(--text-muted, #8a9199);
}

.form-group input {
  width: 100%;
  padding: 12px 14px;
  font-size: 15px;
  border-radius: 12px;
  border: 1px solid var(--line-soft, #e8ecec);
  background: var(--surface-muted, #f8faf9);
  transition: border-color 0.2s ease, background 0.2s ease;
}

.form-group input:focus {
  outline: none;
  border-color: var(--brand-primary, #22ee99);
  background: #fff;
}

.error-message {
  padding: 12px 16px;
  border-radius: 14px;
  background: rgba(198, 83, 76, 0.08);
  color: var(--text-danger, #c6534c);
  font-size: 14px;
  margin-bottom: 16px;
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  margin-top: 24px;
}

.modal-actions button {
  min-width: 110px;
  padding: 12px 18px;
  border-radius: 999px;
  border: 1px solid var(--line-soft, #e8ecec);
  background: transparent;
  font-size: 14px;
  color: var(--text-secondary, #4b5563);
  cursor: pointer;
  transition: all 0.2s ease;
}

.modal-actions button:hover,
.modal-actions button:focus-visible {
  color: var(--brand-primary, #22ee99);
  border-color: var(--brand-primary, #22ee99);
}

.modal-actions button.primary {
  background: var(--brand-primary, #22ee99);
  border-color: var(--brand-primary, #22ee99);
  color: #0b1f14;
  font-weight: 600;
}

.modal-actions button.primary:hover,
.modal-actions button.primary:focus-visible {
  filter: brightness(0.95);
}

.modal-actions button.primary.danger {
  background: rgba(198, 83, 76, 0.1);
  border-color: rgba(198, 83, 76, 0.4);
  color: var(--text-danger, #c6534c);
}

.modal-actions button.primary.danger:hover,
.modal-actions button.primary.danger:focus-visible {
  background: rgba(198, 83, 76, 0.18);
  border-color: var(--text-danger, #c6534c);
}

@media (max-width: 640px) {
  .profile-card {
    border-radius: 24px;
  }

  .profile-header,
  .profile-content {
    padding: 24px;
  }

  .profile-header {
    flex-direction: column;
    align-items: flex-start;
  }

  .avatar-shell {
    width: 64px;
    height: 64px;
  }
}

/* 验证码按钮和输入框的定制样式 */
.code-group .input-with-button {
  display: flex;
  gap: 10px;
}
.code-group input {
  flex-grow: 1;
}
.code-group .code-button {
  min-width: 110px;
  padding: 12px 14px;
  border-radius: 12px;
  border: 1px solid var(--line-soft, #e8ecec);
  background: var(--surface-muted, #f8faf9);
  font-size: 14px;
  color: var(--text-secondary, #4b5563);
  cursor: pointer;
  transition: all 0.2s ease;
}
.code-group .code-button:hover:not(:disabled) {
  background: #fff;
  border-color: var(--brand-primary, #22ee99);
  color: var(--brand-primary, #22ee99);
}
.code-group .code-button:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}
</style>