import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import { createPinia } from 'pinia'  // 引入 Pinia
import './style.css' // 全局样式

const app = createApp(App)

app.use(router)           // 注册路由
app.use(createPinia())    // 注册 Pinia

app.mount('#app')
