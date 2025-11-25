import axios from 'axios'

const service = axios.create({
    baseURL: 'http://localhost:8080/api/v1', // 基础路径
    timeout: 5000
})

// Request 拦截器
service.interceptors.request.use(
    config => {
        // 从 localStorage 获取 token
        const token = localStorage.getItem('token')
        if (token) {
            // 这里的 Bearer 后面必须有空格
            config.headers['Authorization'] = 'Bearer ' + token
        }
        return config
    },
    error => {
        return Promise.reject(error)
    }
)

export default service