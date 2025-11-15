import axios from 'axios'

const request = axios.create({
baseURL: 'http://localhost:8080/api/v1', // 根据你的 Spring Boot 地址修改
timeout: 5000
})

export default request
