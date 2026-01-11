<template>
  <div class="form-container">
    <h2>重置密码</h2>

    <!-- 邮箱 -->
    <div class="input-group">
      <label>邮箱</label>
      <input v-model="email" type="email" placeholder="输入注册邮箱" />
    </div>

    <!-- 验证码输入 -->
    <div class="input-group verification-group">
      <label>验证码</label>
      <div class="input-with-button">
        <input v-model="code" type="text" placeholder="输入验证码" />
        <button
            @click="sendCode"
            :disabled="isCodeButtonDisabled"
            class="btn-code"
        >
          {{ codeButtonText }}
        </button>
      </div>
    </div>

    <!-- 新密码 -->
    <div class="input-group">
      <label>新密码</label>
      <input v-model="newPassword" type="password" placeholder="输入新密码" />
    </div>

    <!-- 确认新密码 -->
    <div class="input-group">
      <label>确认新密码</label>
      <input v-model="confirmPassword" type="password" placeholder="再次输入密码" />
    </div>

    <button class="btn" @click="resetPassword">重置密码</button>

    <div class="links">
      <router-link to="/login">返回登录</router-link>
    </div>

    <!-- 消息提示组件 -->
    <MessageToast
      v-if="showToast"
      :message="toastMessage"
      :type="toastType"
      :redirect-to="toastRedirect"
      :duration="toastRedirect ? 2000 : 1500"
      :auto-close="true"
      @close="showToast = false"
    />
  </div>
</template>

<script setup>
import { ref, computed, onUnmounted } from "vue";
import api from "../api/request";
import MessageToast from "./MessageToast.vue";

// 输入框数据
const email = ref("");
const code = ref("");
const newPassword = ref("");
const confirmPassword = ref("");

// 倒计时与按钮状态
const countdown = ref(0);
const isSending = ref(false);
let timer = null;

// 消息提示相关
const showToast = ref(false);
const toastMessage = ref("");
const toastType = ref("success");
const toastRedirect = ref(null);

// 显示消息提示
const showMessage = (message, type = "success", redirectTo = null) => {
  toastMessage.value = message;
  toastType.value = type;
  toastRedirect.value = redirectTo;
  showToast.value = true;
};

const isCodeButtonDisabled = computed(() => countdown.value > 0 || isSending.value || !email.value);
const codeButtonText = computed(() => {
  if (isSending.value) return "发送中...";
  if (countdown.value > 0) return `${countdown.value}s 后重发`;
  return "获取验证码";
});

// 发送验证码
const sendCode = async () => {
  if (!email.value) {
    showMessage("请输入邮箱！", "error");
    return;
  }
  isSending.value = true;
  try {
    await api.post("/auth/password/send-code", { email: email.value });
    showMessage("验证码已发送，请去邮箱查看", "success");

    countdown.value = 60;
    timer = setInterval(() => {
      countdown.value--;
      if (countdown.value <= 0) {
        clearInterval(timer);
        timer = null;
      }
    }, 1000);
  } catch (e) {
    showMessage("发送失败：" + (e.response?.data?.error || e.message), "error");
  } finally {
    isSending.value = false;
  }
};

// 重置密码
const resetPassword = async () => {
  if (!email.value || !code.value || !newPassword.value) {
    showMessage("请填写完整信息！", "error");
    return;
  }
  if (newPassword.value !== confirmPassword.value) {
    showMessage("两次密码不一致！", "error");
    return;
  }

  try {
    await api.post("/auth/password/reset", {
      email: email.value,
      newPassword: newPassword.value,
      code: code.value  // 改为 code 与后端匹配
    });

    // 显示成功消息并自动跳转到登录页
    showMessage("密码重置成功！", "success", "/login");
    // 重置输入框
    email.value = "";
    code.value = "";
    newPassword.value = "";
    confirmPassword.value = "";
  } catch (e) {
    showMessage("重置失败：" + (e.response?.data?.error || e.message), "error");
  }
};

// 页面卸载时清除定时器
onUnmounted(() => {
  if (timer) clearInterval(timer);
});
</script>

<style scoped>
.form-container {
  width: 100%;
  max-width: 400px;
  margin: 0 auto;
  display: flex;
  flex-direction: column;
}
h2 {
  text-align: center;
  margin-bottom: 20px;
}
.input-group {
  margin-bottom: 15px;
}
.input-group label {
  font-size: 14px;
  margin-bottom: 4px;
  display: block;
}
.input-group input {
  width: 100%;
  padding: 10px;
  border: 1px solid #ccc;
  border-radius: 8px;
  font-size: 15px;
}
.input-with-button {
  display: flex;
  gap: 8px;
}
.btn-code {
  flex-shrink: 0;
  padding: 10px 15px;
  border: none;
  background-color: #4e54c8;
  color: white;
  border-radius: 8px;
  cursor: pointer;
  font-size: 14px;
}
.btn-code:disabled {
  background-color: #999;
  cursor: not-allowed;
}
.btn {
  padding: 12px;
  margin-top: 10px;
  border: none;
  background-color: #4e54c8;
  color: white;
  border-radius: 8px;
  cursor: pointer;
  font-size: 16px;
}
.btn:hover {
  background-color: #3c40a8;
}
.links {
  display: flex;
  justify-content: center;
  margin-top: 10px;
}
.links a {
  color: #4e54c8;
  font-size: 14px;
}
</style>
