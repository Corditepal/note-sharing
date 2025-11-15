<template>
  <div class="dashboard">
    <header class="navbar">
      <div class="nav-left">
        <h1 class="logo" @click="currentView = 'home'">🎓 学霸笔记</h1>
        <div class="search-box" v-if="currentView !== 'upload'">
          <span class="search-icon">🔍</span>
          <input 
            type="text" 
            placeholder="搜索笔记、科目或上传者..." 
            v-model="searchQuery"
            @keyup.enter="handleSearch"
          >
        </div>
      </div>
      <div class="nav-right">
        <button class="upload-btn" @click="currentView = 'upload'">
          <span class="plus">+</span> 分享笔记
        </button>
        <div class="user-menu" @click="currentView = 'profile'">
          <img src="https://api.dicebear.com/7.x/avataaars/svg?seed=Felix" class="avatar" alt="用户头像">
          <span>同学你好</span>
        </div>
        <button class="logout-btn" @click="$emit('logout')">退出</button>
      </div>
    </header>

    <main class="main-content">
      
      <div v-if="currentView === 'home'" class="view-home">
        <div class="filter-bar">
          <div class="sort-tabs">
            <button :class="['sort-tab', { active: sortBy === 'time' }]" @click="sortBy = 'time'">最新上传</button>
            <button :class="['sort-tab', { active: sortBy === 'hot' }]" @click="sortBy = 'hot'">热门浏览</button>
          </div>
          <div class="category-tags">
            <span class="tag active">全部</span>
            <span class="tag">高等数学</span>
            <span class="tag">大学物理</span>
            <span class="tag">计算机基础</span>
          </div>
        </div>

        <div class="note-grid">
          <div class="note-card" v-for="i in 8" :key="i" @click="openNote(i)">
            <div class="note-cover">笔记封面预览</div>
            <div class="note-info">
              <h3 class="note-title">《软件工程》期末复习重点整理-第{{i}}章</h3>
              <div class="note-meta">
                <span>👤 张同学</span>
                <span>📅 2天前</span>
              </div>
              <div class="note-stats">
                <span>👁️ 12{{i}}</span>
                <span>⭐ 4{{i}}</span>
                <span>💬 1{{i}}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-if="currentView === 'read'" class="view-read">
        <div class="read-container card">
          <button class="back-btn" @click="currentView = 'home'">← 返回列表</button>
          <header class="read-header">
            <h2>《软件工程》期末复习重点整理</h2>
            <div class="read-actions">
              <button class="action-btn" @click="toggleBookmark">
                {{ isBookmarked ? '🏷️ 已加书签' : '🔖 添加书签' }}
              </button>
               <button class="action-btn" @click="toggleFavorite">
                {{ isFavorited ? '★ 已收藏' : '☆ 收藏' }}
              </button>
            </div>
          </header>
          
          <div class="read-content">
            <p class="placeholder-text">
              [此处为笔记内容预览区域]<br><br>
              第一章 软件工程概述<br>
              1.1 软件危机：在计算机软件开发和维护过程中所遇到的一系列严重问题。<br>
              1.2 软件工程：将系统化、规范化、可度量的方法应用于软件的开发、运行和维护的过程。<br>
              ...（此处省略5000字）...
            </p>
          </div>

          <div class="comment-section">
            <h3>评论 (12)</h3>
            <div class="comment-input">
              <input type="text" placeholder="写下你的想法..." v-model="newComment">
              <button class="submit-btn-small">发布</button>
            </div>
            <div class="comment-list">
              <div class="comment-item" v-for="j in 3" :key="j">
                <span class="comment-user">李同学:</span>
                <span class="comment-text">感谢分享，总结得非常到位！</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-if="currentView === 'upload'" class="view-upload">
        <div class="card upload-card">
          <h2 class="title">分享你的笔记</h2>
          <div class="form-group">
            <label>笔记标题</label>
            <input type="text" class="form-input" placeholder="例如：《数据结构》考研核心考点">
          </div>
          <div class="form-group">
            <label>所属科目</label>
            <select class="form-input">
              <option>请选择科目</option>
              <option>计算机科学</option>
              <option>数学</option>
            </select>
          </div>
          <div class="form-group">
            <label>笔记内容</label>
            <textarea class="form-input textarea" rows="10" placeholder="支持 Markdown 或直接粘贴内容..."></textarea>
          </div>
          <div class="upload-actions">
            <button class="cancel-btn" @click="currentView = 'home'">取消</button>
            <button class="submit-btn" @click="handleUpload">提交审核</button>
          </div>
        </div>
      </div>

      <div v-if="currentView === 'profile'" class="view-profile">
        <div class="profile-header card">
          <img src="https://api.dicebear.com/7.x/avataaars/svg?seed=Felix" class="profile-avatar" alt="用户头像">
          <div class="profile-info">
            <h2>张同学</h2>
            <p>软件工程 | 2023级</p>
             <button class="edit-profile-btn">编辑资料</button>
          </div>
        </div>

        <div class="profile-tabs">
          <button :class="['tab-btn', { active: profileTab === 'my-notes' }]" @click="profileTab = 'my-notes'">我的笔记</button>
          <button :class="['tab-btn', { active: profileTab === 'favorites' }]" @click="profileTab = 'favorites'">我的收藏</button>
          <button :class="['tab-btn', { active: profileTab === 'history' }]" @click="profileTab = 'history'">浏览记录</button>
        </div>

        <div class="profile-content">
           <div class="empty-state">
             📦 暂无相关内容
           </div>
        </div>
      </div>

    </main>
  </div>
</template>

<script>
export default {
  name: 'UserDashboard',
  emits: ['logout'],
  data() {
    return {
      currentView: 'home', // home, read, upload, profile
      sortBy: 'time', // time, hot
      searchQuery: '',
      profileTab: 'my-notes',
      isBookmarked: false,
      isFavorited: false,
      newComment: ''
    }
  },
  methods: {
    handleSearch() {
      if (this.searchQuery.trim()) {
        alert(`正在搜索：${this.searchQuery}`);
        // 实际应用中这里调用搜索API [cite: 15]
      }
    },
    openNote(id) {
      console.log('Opening note:', id);
      // 需求：若有浏览记录，跳转至上次阅读位置 
      this.currentView = 'read';
    },
    toggleBookmark() {
      this.isBookmarked = !this.isBookmarked;
      // 需求：书签作为重点记录和快速跳转工具 
    },
    toggleFavorite() {
      this.isFavorited = !this.isFavorited;
    },
    handleUpload() {
      alert('笔记已提交，请等待管理员审核！'); // 需求：经管理员审核 [cite: 20]
      this.currentView = 'home';
    }
  }
}
</script>

<style scoped>
/* 复用及扩展变量 */
:root {
  --primary-color: #667eea;
  --secondary-color: #764ba2;
  --bg-light: #f5f7fa;
  --text-color: #333;
}

.dashboard {
  min-height: 100vh;
  background-color: #f0f2f5;
}

/* 导航栏 */
.navbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0 30px;
  height: 64px;
  background: white;
  box-shadow: 0 2px 10px rgba(0,0,0,0.05);
  position: sticky;
  top: 0;
  z-index: 100;
}

.nav-left, .nav-right {
  display: flex;
  align-items: center;
  gap: 20px;
}

.logo {
  color: #667eea;
  font-size: 22px;
  cursor: pointer;
  margin: 0;
}

.search-box {
  position: relative;
  width: 300px;
}

.search-icon {
  position: absolute;
  left: 12px;
  top: 50%;
  transform: translateY(-50%);
  opacity: 0.5;
}

.search-box input {
  width: 100%;
  padding: 10px 10px 10px 36px;
  border: 1px solid #e0e0e0;
  border-radius: 20px;
  background: #f5f5f5;
  transition: all 0.3s;
}

.search-box input:focus {
  background: white;
  border-color: #667eea;
  outline: none;
}

.upload-btn {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  border: none;
  padding: 8px 20px;
  border-radius: 20px;
  font-weight: 600;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 5px;
  transition: transform 0.2s;
}

.upload-btn:hover {
  transform: translateY(-1px);
}

.user-menu {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
  padding: 5px 10px;
  border-radius: 8px;
  transition: background 0.2s;
}

.user-menu:hover {
  background: #f5f5f5;
}

.avatar {
  width: 32px;
  height: 32px;
  border-radius: 50%;
}

.logout-btn {
  border: none;
  background: none;
  color: #999;
  cursor: pointer;
}

/* 主内容区 */
.main-content {
  max-width: 1200px;
  margin: 20px auto;
  padding: 0 20px;
}

/* 通用卡片样式 (复用自登录页并微调) */
.card {
  background: white;
  border-radius: 16px;
  padding: 30px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.05);
}

/* 首页视图 */
.filter-bar {
  display: flex;
  justify-content: space-between;
  margin-bottom: 20px;
  align-items: center;
}

.sort-tabs {
  display: flex;
  gap: 15px;
}

.sort-tab {
  background: none;
  border: none;
  font-size: 16px;
  color: #999;
  cursor: pointer;
  padding-bottom: 5px;
  border-bottom: 2px solid transparent;
}

.sort-tab.active {
  color: #333;
  font-weight: 600;
  border-bottom-color: #667eea;
}

.category-tags {
  display: flex;
  gap: 10px;
}

.tag {
  padding: 4px 12px;
  background: white;
  border-radius: 15px;
  font-size: 14px;
  color: #666;
  cursor: pointer;
}

.tag.active {
  background: #667eea;
  color: white;
}

.note-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 20px;
}

.note-card {
  background: white;
  border-radius: 12px;
  overflow: hidden;
  cursor: pointer;
  transition: all 0.3s;
  border: 1px solid #eee;
}

.note-card:hover {
  transform: translateY(-5px);
  box-shadow: 0 10px 20px rgba(0,0,0,0.08);
}

.note-cover {
  height: 140px;
  background: #eef1f5;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #999;
  font-size: 14px;
}

.note-info {
  padding: 15px;
}

.note-title {
  font-size: 16px;
  margin: 0 0 10px 0;
  line-height: 1.4;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.note-meta {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  color: #999;
  margin-bottom: 10px;
}

.note-stats {
  display: flex;
  gap: 15px;
  font-size: 13px;
  color: #666;
}

/* 阅读视图 */
.read-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid #eee;
  padding-bottom: 20px;
  margin-bottom: 20px;
}

.read-actions {
  display: flex;
  gap: 10px;
}

.action-btn {
  padding: 8px 15px;
  border: 1px solid #eee;
  background: white;
  border-radius: 20px;
  cursor: pointer;
  transition: all 0.2s;
}

.action-btn:hover {
  border-color: #667eea;
  color: #667eea;
}

.read-content {
  min-height: 300px;
  line-height: 1.8;
  color: #444;
  padding: 20px;
  background: #f9f9f9;
  border-radius: 8px;
}

.back-btn {
  background: none;
  border: none;
  color: #667eea;
  cursor: pointer;
  margin-bottom: 15px;
}

.comment-section {
  margin-top: 40px;
  border-top: 1px solid #eee;
  padding-top: 20px;
}

.comment-input {
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
}

.comment-input input {
  flex: 1;
  padding: 10px;
  border: 1px solid #eee;
  border-radius: 8px;
}

.submit-btn-small {
  padding: 0 20px;
  background: #667eea;
  color: white;
  border: none;
  border-radius: 8px;
  cursor: pointer;
}

.comment-item {
  padding: 10px 0;
  border-bottom: 1px solid #f5f5f5;
}

.comment-user {
  font-weight: 600;
  margin-right: 10px;
}

/* 上传视图 */
.upload-card {
  max-width: 800px;
  margin: 0 auto;
}

.form-group {
  margin-bottom: 25px;
}

.form-group label {
  display: block;
  margin-bottom: 10px;
  font-weight: 500;
  color: #333;
}

.form-input {
  width: 100%;
  padding: 12px;
  border: 2px solid #e0e0e0;
  border-radius: 10px;
  font-size: 14px;
  transition: all 0.3s;
}

.form-input:focus {
  outline: none;
  border-color: #667eea;
}

.textarea {
  resize: vertical;
}

.upload-actions {
  display: flex;
  justify-content: flex-end;
  gap: 15px;
}

.cancel-btn {
  padding: 12px 30px;
  background: #f5f5f5;
  border: none;
  border-radius: 10px;
  cursor: pointer;
}

.submit-btn {
  padding: 12px 30px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  border: none;
  border-radius: 10px;
  font-weight: 600;
  cursor: pointer;
}

/* 个人中心视图 */
.profile-header {
  display: flex;
  align-items: center;
  gap: 20px;
  margin-bottom: 20px;
}

.profile-avatar {
  width: 80px;
  height: 80px;
  border-radius: 50%;
}

.edit-profile-btn {
  margin-top: 10px;
  padding: 6px 15px;
  border: 1px solid #667eea;
  background: white;
  color: #667eea;
  border-radius: 20px;
  cursor: pointer;
}

.profile-tabs {
  display: flex;
  gap: 10px;
  margin-bottom: 20px;
  border-bottom: 1px solid #eee;
}

.tab-btn {
  padding: 10px 20px;
  background: none;
  border: none;
  cursor: pointer;
  color: #666;
  position: relative;
}

.tab-btn.active {
  color: #667eea;
  font-weight: 600;
}

.tab-btn.active::after {
  content: '';
  position: absolute;
  bottom: -1px;
  left: 0;
  width: 100%;
  height: 3px;
  background: #667eea;
}

.empty-state {
  text-align: center;
  padding: 50px;
  color: #999;
  font-size: 18px;
}
</style>