<template>
  <div class="form-container">
    <h2>管理员登录</h2>

    <div class="input-group">
      <label>邮箱</label>
      <input v-model="email" type="email" placeholder="输入管理员邮箱" />
    </div>

    <div class="input-group">
      <label>密码</label>
      <input v-model="password" type="password" placeholder="输入密码" />
    </div>

    <button class="btn" @click="login">登录</button>

    <div class="links">
      <router-link to="/login">返回用户登录</router-link>
    </div>

    <!-- 消息提示组件 -->
    <MessageToast
      v-if="showToast"
      :message="toastMessage"
      :type="toastType"
      :redirect-to="toastRedirect"
      :duration="1200"
      @close="showToast = false"
    />
  </div>
</template>

<script setup>
import { ref } from "vue";
import { useRouter } from "vue-router";
import { adminLogin } from "../../api/admin";
import MessageToast from "../MessageToast.vue";
import { useUserStore } from "@/stores/user";

// 输入框数据
const email = ref("");
const password = ref("");

const router = useRouter();
const userStore = useUserStore();

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

// 登录方法
const login = async () => {
  if (!email.value || !password.value) {
    showMessage("请填写完整信息！", "error");
    return;
  }

  try {
    const res = await adminLogin(email.value, password.value);

    console.log(res);

    // 保存 token
    if (res.token) {
      localStorage.setItem("token", res.token);
      
      // 设置登录入口类型为管理员
      userStore.setLoginType('admin');
      // 解码并设置 token 中的用户信息
      userStore.decodeAndSetToken(res.token, 'admin');
      
      // 显示成功消息并自动跳转
      showMessage("登录成功！", "success", "/admin/main");
    } else {
      showMessage("登录失败：未获取到 token", "error");
    }

  } catch (e) {
    showMessage("登录失败：" + (e.response?.data?.message || e.message), "error");
  }
};
</script>

<style scoped>
.form-container {
  width: 100%;
  display: flex;
  flex-direction: column;
}

h2 {
  text-align: center;
  margin-bottom: 32px;
  font-size: 28px;
  font-weight: 600;
  color: var(--text-strong);
  letter-spacing: -0.02em;
}

.input-group {
  margin-bottom: 20px;
}

.input-group label {
  font-size: 14px;
  font-weight: 500;
  margin-bottom: 8px;
  display: block;
  color: var(--text-primary);
}

.input-group input {
  width: 100%;
  padding: 14px 16px;
  border: 1.5px solid var(--line-soft);
  border-radius: var(--radius-sm);
  font-size: 15px;
  color: var(--text-primary);
  background: var(--surface-base);
  transition: all var(--transition-base);
  box-sizing: border-box;
}

.input-group input:focus {
  outline: none;
  border-color: var(--brand-primary);
  box-shadow: 0 0 0 3px rgba(34, 191, 163, 0.1);
}

.input-group input::placeholder {
  color: var(--text-muted);
}

.btn {
  padding: 14px 24px;
  margin-top: 8px;
  border: none;
  background: var(--brand-primary);
  color: white;
  border-radius: var(--radius-sm);
  cursor: pointer;
  font-size: 16px;
  font-weight: 500;
  transition: all var(--transition-base);
  box-shadow: var(--shadow-sm);
}

.btn:hover {
  background: var(--brand-primary-hover);
  box-shadow: var(--shadow-md);
  transform: translateY(-1px);
}

.btn:active {
  transform: translateY(0);
  box-shadow: var(--shadow-xs);
}

.links {
  display: flex;
  justify-content: space-between;
  margin-top: 24px;
  gap: 16px;
}

.links a {
  color: var(--brand-primary);
  font-size: 14px;
  font-weight: 500;
  transition: color var(--transition-fast);
  text-decoration: none;
}

.links a:hover {
  color: var(--brand-primary-hover);
  text-decoration: none;
}
</style>
