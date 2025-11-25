<template>
  <div class="form-container">
    <h2>用户登录</h2>

    <div class="input-group">
      <label>邮箱</label>
      <input v-model="email" type="email" placeholder="输入邮箱" />
    </div>

    <div class="input-group">
      <label>学号</label>
      <input v-model="studentNumber" type="text" placeholder="输入学号" />
    </div>

    <div class="input-group">
      <label>用户名</label>
      <input v-model="username" type="text" placeholder="输入用户名" />
    </div>

    <div class="input-group">
      <label>密码</label>
      <input v-model="password" type="password" placeholder="输入密码" />
    </div>

    <button class="btn" @click="login">登录</button>

    <div class="links">
      <router-link to="/register">没有账号？注册</router-link>
      <router-link to="/forgot-password">忘记密码？</router-link>
    </div>
  </div>
</template>

<script setup>
import { ref } from "vue";
import { useRouter } from "vue-router";
import api from "../api/request";

// 输入框数据
const email = ref("");
const studentNumber = ref("");
const username = ref("");
const password = ref("");

const router = useRouter();

// 登录方法
const login = async () => {
  if (!email.value || !studentNumber.value || !username.value || !password.value) {
    alert("请填写完整信息！");
    return;
  }

  try {
    const res = await api.post("/auth/login", {
      email: email.value,
      studentNumber: studentNumber.value,
      username: username.value,
      password: password.value
    });

    alert("登录成功！");
    console.log(res.data);

    // 假设后端返回 token：{ token: "xxxx" }
    localStorage.setItem("token", res.data.token);

    // 跳转到用户页面
    router.push("/main");

  } catch (e) {
    alert("登录失败：" + (e.response?.data?.error || e.message));
  }
};
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
  justify-content: space-between;
  margin-top: 10px;
}
.links a {
  color: #4e54c8;
  font-size: 14px;
}
</style>
