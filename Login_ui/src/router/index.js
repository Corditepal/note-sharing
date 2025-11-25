// src/router/index.js

import { createRouter, createWebHistory } from 'vue-router'
import request from '../api/request'
import AuthView from '../views/AuthView.vue'
import MainView from '../views/MainView.vue'
import AdminView from '../views/AdminView.vue'
import { useUserStore } from '@/stores/user' // 导入 Pinia Store

const routes = [
  { path: '/', redirect: '/login' },
  { path: '/login', name: 'Login', component: AuthView },
  { path: '/register', name: 'Register', component: AuthView },
  { path: '/forgot-password', name: 'ForgotPassword', component: AuthView },
  { path: '/main', name: 'Main', component: MainView, meta: { requiresAuth: true } },
  { path: '/admin', name: 'Admin', component: AdminView, meta: { requiresAuth: true, requiresAdmin: true } }
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

// 路由守卫
router.beforeEach(async (to, from, next) => {
  const token = localStorage.getItem('token')

  // 关键：获取 Store 实例
  const userStore = useUserStore()

  if (to.meta.requiresAuth) {
    // 1. 访问需要登录的页面
    if (!token) {
      // 没有 token，重定向到登录页
      return next('/login')
    }

    // 【优化点】：可以先尝试从本地解析 Token (如果 Store 中没有数据)
    // 检查 Store 中是否有用户信息，如果有且未过期，可跳过 /me 请求以提高性能
    if (!userStore.isLoggedIn) {
      // 如果 Store 中没有用户数据，尝试从 Token 中解码，并检查是否过期
      // 这一步虽然不验证签名，但能快速填充 UI 数据并检查 exp
      const isTokenValid = userStore.decodeAndSetToken(token);
      if (!isTokenValid) {
        // Token 格式错误或已过期
        userStore.clearUserData();
        return next('/login');
      }
    }


    try {
      // 验证 token 有效性，并获取用户数据
      // 注意：使用 request 库时，请确保它会自动添加 Authorization Header (Bearer Token)。
      const res = await request.get('/auth/me')

      // 核心修改 1: 验证成功，保存用户数据
      // 使用 setUserData 更新用户信息 (Token 已经在上面解码时设置过了)
      // 注意：res.data 应该是后端 /auth/me 接口返回的用户对象
      userStore.setUserData(res.data)

      // 核心修改 2: 检查是否需要管理员权限
      if (to.meta.requiresAdmin && res.data.role !== 'ADMIN') { // 假设权限角色字段为 role
        // 如果需要管理员权限但当前用户不是管理员，重定向到主页
        return next('/main')
      }

      next()  // 验证成功，继续
    } catch (err) {
      // token 无效、过期、或网络错误 (后端返回 401/403 等)
      console.error('Token 验证失败:', err)
      // 核心修改 3: 清除过期数据
      userStore.clearUserData() // 使用 Store 中的清除函数
      next('/login')
    }
  } else if ((to.path === '/login' || to.path === '/register' || to.path === '/forgot-password') && token) {
    // 2. 已登录用户尝试访问登录/注册页面
    next('/main')
  } else {
    // 3. 访问其他公共页面
    next()
  }
})

export default router