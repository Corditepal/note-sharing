<template>
  <div class="form-container">
    <h2>创建账号</h2>

    <div class="input-group">
      <label>用户名</label>
      <input v-model="username" type="text" placeholder="输入用户名" />
    </div>

    <div class="input-group">
      <label>邮箱</label>
      <input v-model="email" type="email" placeholder="输入邮箱" />
    </div>

    <div class="input-group">
      <label>学号</label>
      <input v-model="studentNumber" type="text" placeholder="输入学号" />
    </div>

    <div class="input-group">
      <label>密码</label>
      <input v-model="password" type="password" placeholder="输入密码" />
    </div>

    <div class="input-group">
      <label>确认密码</label>
      <input v-model="confirmPassword" type="password" placeholder="再次输入密码" />
    </div>

    <div class="input-group verification-group">
      <label>验证码</label>
      <div class="input-with-button">
        <input v-model="verificationCode" type="text" placeholder="输入6位验证码" />
        <button
            @click="sendCode"
            :disabled="isCodeButtonDisabled"
            class="btn-code"
        >
          {{ codeButtonText }}
        </button>
      </div>
    </div>

    <button class="btn" @click="register">注册</button>

    <div class="links">
      <router-link to="/login">已有账号？去登录</router-link>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onUnmounted } from "vue";
import { useRouter } from "vue-router";
import api from "../api/request";

const router = useRouter();

const username = ref("");
const email = ref("");
const studentNumber = ref("");
const password = ref("");
const confirmPassword = ref("");
const verificationCode = ref("");

// 倒计时逻辑
const countdown = ref(0);
const isSending = ref(false);
let timer = null;

const isCodeButtonDisabled = computed(() => countdown.value > 0 || isSending.value || !email.value);
const codeButtonText = computed(() => {
  if (isSending.value) return "发送中...";
  if (countdown.value > 0) return `${countdown.value}s 后重发`;
  return "获取验证码";
});

const sendCode = async () => {
  if (!email.value) return alert("请输入邮箱！");
  isSending.value = true;
  try {
    await api.post("/auth/register/send-code", { email: email.value });
    alert("验证码已发送到邮箱");
    countdown.value = 60;
    timer = setInterval(() => {
      countdown.value--;
      if (countdown.value <= 0) {
        clearInterval(timer);
        timer = null;
      }
    }, 1000);
  } catch (e) {
    alert("发送失败：" + (e.response?.data?.error || e.message));
  } finally {
    isSending.value = false;
  }
};

const register = async () => {
  if (!username.value || !email.value || !studentNumber.value || !password.value || !verificationCode.value) {
    return alert("请填写完整信息！");
  }
  if (password.value !== confirmPassword.value) return alert("两次密码不一致！");
  try {
    await api.post("/auth/register", {
      username: username.value,
      studentNumber: studentNumber.value,
      email: email.value,
      password: password.value,
      code: verificationCode.value,
    });
    alert("注册成功！");
    router.push("/login");
  } catch (e) {
    alert("注册失败：" + (e.response?.data?.error || e.message));
  }
};

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
