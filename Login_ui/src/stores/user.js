// src/stores/user.js

import { defineStore } from 'pinia';

// 辅助函数：安全地解码 JWT 的 Payload (从上次回答中复制过来)
function decodeJwtPayload(token) {
    if (!token) return null;
    try {
        const base64Url = token.split('.')[1];
        const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
        const jsonPayload = decodeURIComponent(
            atob(base64).split('').map(function(c) {
                return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
            }).join('')
        );
        return JSON.parse(jsonPayload);
    } catch (e) {
        console.error('JWT 解码失败:', e);
        return null;
    }
}


export const useUserStore = defineStore('user', {
    state: () => ({
        // 集中管理用户信息和 token
        userInfo: {
            id: null,
            username: null,
            email: null,
            studentNumber: null, // 与后端字段名保持一致
            token: localStorage.getItem('token') || null, // 保存 Token
        },
        isAuthenticated: !!localStorage.getItem('token'), // 根据 token 判断是否登录
    }),

    getters: {
        // 方便组件访问用户信息
        getCurrentUser: (state) => state.userInfo,
        // 检查登录状态
        isLoggedIn: (state) => state.isAuthenticated && !!state.userInfo.token,
    },

    actions: {

        /**
         * 【推荐】从 Token 解码并设置用户数据
         * 用于登录成功后或应用初始化时
         * @param {string} token - JWT Token 字符串
         */
        decodeAndSetToken(token) {
            const payload = decodeJwtPayload(token);

            if (payload) {
                // 1. 设置 Token
                this.userInfo.token = token;
                localStorage.setItem('token', token);

                // 2. 提取 Payload 中的信息 (注意字段映射)
                this.userInfo.id = payload.userId || payload.id;
                this.userInfo.username = payload.sub || payload.username;
                this.userInfo.email = payload.email;
                this.userInfo.studentNumber = payload.studentNumber;

                this.isAuthenticated = true;
                console.log('Pinia: Token 解析成功，用户数据已设置。');
                return true;
            }
            this.clearUserData(); // 如果解码失败，则清除数据
            return false;
        },

        /**
         * 【可选】直接设置用户数据到 Store (例如：通过 /me 接口获取后调用)
         * @param {Object} user - 包含用户信息的对象 (来自 /me 接口)
         */
        setUserData(user) {
            // 确保使用后端 /me 接口返回的字段名
            this.userInfo.id = user.id;
            this.userInfo.username = user.username;
            this.userInfo.email = user.email;
            this.userInfo.studentNumber = user.studentNumber;
            this.isAuthenticated = true;

            console.log('Pinia: 用户数据已设置:', user.username);
        },

        /**
         * 清除 Store 中的用户数据 (保留原函数名以匹配您组件中的调用)
         */
        clearUserData() {
            this.userInfo = {
                id: null,
                username: null,
                email: null,
                studentNumber: null,
                token: null
            };
            this.isAuthenticated = false;
            localStorage.removeItem('token');
            console.log('Pinia: 用户数据已清除');
        },
    },
});