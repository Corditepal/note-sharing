<template>
  <div class="main-shell">
    <header class="main-header app-layout">
      <div class="brand-logo-block">
        <span class="brand-logo-text">NoteFlow</span>
      </div>

      <nav class="main-nav-links" aria-label="主要导航">
        <button
            v-for="tab in tabs"
            :key="tab.value"
            type="button"
            :class="['nav-link-item', { active: currentTab === tab.value }]"
            @click="currentTab = tab.value"
        >
          {{ tab.label }}
        </button>
      </nav>

      <div class="search-container">
        <input
            type="text"
            class="search-input"
            placeholder="编译原理-词法分析器"
            aria-label="搜索框"
        />
        <button class="search-button" type="button" aria-label="搜索">
          <span class="search-icon">🔍</span>
        </button>
      </div>

      <div class="header-actions">
        <button class="ask-button" type="button">
          <span class="icon">+</span> 提问
        </button>

        <div class="action-icon-wrapper message-wrapper">
          <img
              src="/assets/icons/icon-private-message.svg"
              alt="私信"
              class="action-image-icon private-message-icon-img"
          />
          <span class="badge">16</span>
          <span class="action-text">私信</span>
        </div>

        <div class="action-icon-wrapper notification-wrapper">
          <img
              src="/assets/icons/icon-notification.svg"
              alt="消息"
              class="action-image-icon notification-icon-img"
          />
          <span class="badge">!</span>
          <span class="action-text">消息</span>
        </div>

        <div class="user-avatar-block">
          <span class="avatar-placeholder" role="img" aria-label="用户头像"></span>
        </div>
      </div>

    </header>

    <main class="main-content">
      <section v-if="editingNotebookId">
        <NoteEditorView
            v-if="editingNotebookId"

            :spaceId="editingSpaceId"
            :notebookId="editingNotebookId"
            :notebookName="editingNotebookName"
            :notebookList="editingNotebookList"
            @close="handleCloseEditor"
        />
      </section>

      <section v-else-if="currentTab === 'search'">
        <SearchView />
      </section>
      <section v-else-if="currentTab === 'workspace'">
        <WorkspaceView @open-notebook="handleOpenNotebook" />
      </section>
      <section v-else-if="currentTab === 'favorites'">
        <FavoritesView />
      </section>
      <section v-else-if="currentTab === 'comments'">
        <CommentsView />
      </section>
      <section v-else>
        <ProfileView />
      </section>
    </main>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue'
import SearchView from '../components/user/SearchView.vue'
import WorkspaceView from '../components/user/WorkspaceView.vue'
import ProfileView from '../components/user/ProfileView.vue'
import FavoritesView from '../components/user/FavoritesView.vue'
import CommentsView from '../components/user/CommentsView.vue'
import NoteEditorView from '../components/user/NoteEditorView.vue'
import { useRouter, useRoute } from 'vue-router'

const router = useRouter() // 【新增】
const route = useRoute()

const tabs = [
  { value: 'follow', label: '关注', desc: 'Follow' },
  { value: 'recommend', label: '推荐', desc: 'Recommend' },
  { value: 'hot', label: '热榜', desc: 'Hot' },
  { value: 'circle', label: '问答', desc: 'Q&A' },
  { value: 'workspace', label: '我的笔记', desc: 'WorkspaceView' }
]

const currentTab = ref('recommend')

// --- 新增状态和方法来管理编辑器视图 ---

// 跟踪正在编辑的笔记ID。如果为null，则不显示编辑器。
const editingNotebookId = ref(null);
const editingSpaceId = ref(null);
const editingNotebookName = ref(null);
const editingNotebookList = ref([]); // 使用数组类型

// 处理 WorkspaceView 发出的“打开笔记本”事件
const handleOpenNotebook = (payload) => {
  // 切换到编辑模式

  if (payload && typeof payload.notebookId !== 'undefined') {
    editingNotebookId.value = payload.notebookId;

    // 【修改点】：存储所有传入参数
    editingSpaceId.value = payload.spaceId;
    editingNotebookName.value = payload.notebookName;
    editingNotebookList.value = payload.notebookList;

  } else {

    console.error("打开笔记本失败：事件载荷中缺少 notebookId 字段。");
    editingNotebookId.value = null;
    editingSpaceId.value = null;
    editingNotebookName.value = null;
    editingNotebookList.value = [];
  }
}

// 处理编辑器内“关闭”或“返回”操作
const handleCloseEditor = () => {
  editingNotebookId.value = null
  // 确保当前 tab 切换回 workspace 视图，以便用户返回时看到列表
  currentTab.value = 'workspace';
}

// --- 结束新增 ---
</script>

<style scoped>
/* 样式部分保持不变 */

.main-shell {
  min-height: 100vh;
  padding: 0;
  background: #f6f6f6;
  display: flex;
  flex-direction: column;
  gap: 0;
}

.main-header {
  display: flex;
  align-items: center;
  background: white;
  padding: 0 20px;
  height: 52px;
  border-bottom: 1px solid #ededed;
  gap: 24px;
  box-shadow: none;
  border-radius: 0;
  border: none;
}

/* --- Logo / Nav Links (保持不变) --- */

.brand-logo-block {
  margin-right: 16px;
}

.brand-logo-text {
  font-family: 'PingFang SC', 'Helvetica Neue', Arial, sans-serif;
  font-size: 26px;
  font-weight: bold;
  color: #007FFF;
  user-select: none;
  cursor: pointer;
}

.main-nav-links {
  display: flex;
  align-items: center;
  gap: 35px;
  white-space: nowrap;
}

.nav-link-item {
  background: none;
  border: none;
  padding: 0 0 5px 0;
  color: #444;
  font-size: 15px;
  cursor: pointer;
  transition: color 0.2s, border-bottom-color 0.2s;
  position: relative;
  flex-shrink: 0;
}

.nav-link-item.active {
  color: #000;
  font-weight: bold;
}
.nav-link-item.active::after {
  content: '';
  position: absolute;
  left: 50%;
  bottom: 0;
  transform: translateX(-50%);
  width: 28px;
  height: 3px;
  background-color: #007FFF;
  border-radius: 2px;
}

.nav-link-item:hover {
  color: #000;
}


/* --- Search Bar (保持不变) --- */

.search-container {
  display: flex;
  flex: 1;
  max-width: 480px;
  height: 38px;
  background: #f6f6f6;
  border-radius: 8px;
  overflow: hidden;
  align-items: center;
  margin-left: auto;
  margin-right: 30px;
  border: 1px solid #e2e2e2;
}

.search-input {
  flex: 1;
  border: none;
  background: none;
  padding: 0 12px;
  font-size: 14px;
  color: #333;
  height: 100%;
}

.search-input::placeholder {
  color: #999;
}

.search-button {
  width: 40px;
  height: 100%;
  border: none;
  background: none;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
}

.search-icon {
  font-size: 16px;
  color: #8590a6;
}


/* -------------------------------------------------------------------------- */
/* 右侧功能按钮群样式 (图标部分已修改) */
/* -------------------------------------------------------------------------- */
.header-actions {
  display: flex;
  align-items: center;
  gap: 18px;
  flex-shrink: 0;
}

/* 提问按钮 */
.ask-button {
  display: flex;
  align-items: center;
  gap: 4px;
  background-color: #007FFF;
  color: white;
  border: none;
  border-radius: 20px;
  padding: 8px 16px;
  font-size: 15px;
  cursor: pointer;
  transition: background-color 0.2s;
  height: 38px;
}

.ask-button:hover {
  background-color: #006EDC;
}

.ask-button .icon {
  font-size: 20px;
  line-height: 1;
}

/* 消息/通知图标的通用样式 (保持一致性) */
.action-icon-wrapper {
  position: relative;
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 48px;
  height: 48px;
  padding-top: 5px;
  border-radius: 8px;
  background-color: transparent;
  cursor: pointer;
  transition: background-color 0.2s;
  flex-shrink: 0;
  box-sizing: border-box;
}

.action-icon-wrapper:hover {
  background-color: #f6f6f6;
}

/* 新增：图片图标的通用样式 */
.action-image-icon {
  width: 20px; /* 统一图标尺寸 */
  height: 20px;
  margin-bottom: 3px;
  object-fit: contain; /* 确保图片完整显示 */
  /* 替换之前的 .action-icon-wrapper .icon 样式 */
}

/* 移除不再需要的私信图标 SVG 样式 */
/* .private-message-icon { ... } */

.action-icon-wrapper .action-text {
  font-size: 12px;
  color: #8590a6;
  white-space: nowrap;
  line-height: 1;
}

.action-icon-wrapper .badge {
  position: absolute;
  top: 0px;
  right: 5px;
  background-color: #ff4d4f;
  color: white;
  font-size: 11px;
  padding: 0px 4px;
  border-radius: 10px;
  min-width: 12px;
  height: 18px;
  display: flex;
  align-items: center;
  justify-content: center;
  text-align: center;
  line-height: 1;
  box-sizing: border-box;
}

/* 用户头像 */
.user-avatar-block {
  width: 38px;
  height: 38px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  height: 48px;
}

.user-avatar-block .avatar-placeholder {
  display: block;
  width: 100%;
  height: 100%;
  border-radius: 50%;
  background-color: #e0e0e0;
  border: none;
}

/* --- Content and Media Queries (保持不变) --- */

.main-content {
  flex: 1;
  padding: 20px;
}

@media (max-width: 960px) {
  .main-header {
    flex-wrap: wrap;
    height: auto;
    padding: 10px 20px;
    gap: 15px;
    justify-content: space-between;
  }

  .brand-logo-block {
    order: 1;
    margin-right: 0;
  }

  .main-nav-links {
    order: 3;
    width: 100%;
    justify-content: space-around;
    gap: 10px;
    border-top: 1px solid #ededed;
    padding-top: 10px;
    margin-top: 5px;
  }

  .search-container {
    order: 2;
    flex-grow: 1;
    max-width: none;
    margin-left: 0;
    margin-right: 15px;
  }

  .header-actions {
    order: 2;
    gap: 10px;
  }

  .ask-button {
    display: none;
  }

  /* 窄屏下图标容器样式 */
  .action-icon-wrapper {
    width: 32px;
    height: 32px;
    padding-top: 0;
    border-radius: 50%;
  }

  /* 窄屏下的图片图标尺寸 */
  .action-image-icon {
    width: 18px;
    height: 18px;
    margin-bottom: 0;
  }

  .action-icon-wrapper .action-text {
    display: none;
  }

  .action-icon-wrapper .badge {
    top: -2px;
    right: -2px;
    font-size: 10px;
    padding: 2px 5px;
    min-width: 10px;
    height: auto;
  }

  .user-avatar-block {
    width: 32px;
    height: 32px;
  }
}
</style>