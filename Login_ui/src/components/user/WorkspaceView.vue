<template>
  <div class="workspace-page" @click="hideAllContextMenus">
    <aside class="sidebar">
      <div class="sidebar-header">
        <h3>笔记空间</h3>
        <button class="add-btn" @click="openCreateWorkspace">＋</button>
      </div>

      <ul class="workspace-list">
        <li
            v-for="ws in workspaceList"
            :key="ws.id"
            :class="{ active: ws.id === selectedWorkspace?.id }"
            @click="selectWorkspace(ws)"
            @contextmenu.prevent="showWorkspaceContext(ws, $event)"
        >
          <div class="workspace-item">
            <span class="icon">📁</span>
            <div class="ws-info">
              <p class="name">{{ ws.name }}</p>
              <p class="tags">标签：{{ ws.tagName || '无' }}</p>
              <p class="date">{{ ws.createdAt }}</p>
            </div>

            <span class="menu-btn" @click.stop="showWorkspaceContext(ws, $event)">⋯</span>
          </div>
        </li>
      </ul>
      <p v-if="workspaceList.length === 0" class="no-data">暂无笔记空间</p>
    </aside>

    <main class="main-panel">
      <div v-if="!selectedWorkspace" class="empty">
        请选择一个笔记空间
      </div>

      <div v-else class="notebook-section">
        <div class="section-header">
          <h2>当前空间：{{ selectedWorkspace.name }}</h2>
          <button class="create-btn" @click="openCreateNotebook">
            创建笔记本
          </button>
        </div>

        <ul class="notebook-list">
          <li
              v-for="nb in notebooks"
              :key="nb.id"
              @dblclick="openNotebook(nb)"
              @contextmenu.prevent="showNotebookContext(nb, $event)"
          >
            <div class="notebook-item">
              <span class="icon">📘</span>
              <div>
                <p class="name">{{ nb.name }}</p>
                <p class="tags-and-time">
                  标签：{{ nb.tagName || '无' }} ｜ 创建：{{ nb.createdAt }} ｜ 更新：{{ nb.updatedAt }}
                </p>
              </div>

              <span class="menu-btn" @click.stop="showNotebookContext(nb, $event)">⋯</span>
            </div>
          </li>
        </ul>
        <p v-if="notebooks.length === 0" class="no-data">该空间下暂无笔记本</p>
      </div>
    </main>

    <ul v-if="workspaceContextMenu.visible" class="context-menu"
        :style="{ top: workspaceContextMenu.y + 'px', left: workspaceContextMenu.x + 'px' }">
      <li @click="openCreateNotebook">创建笔记本</li>
      <li @click="openRenameWorkspace">重命名</li>
      <li @click="deleteWorkspace">删除</li>
    </ul>

    <ul v-if="notebookContextMenu.visible" class="context-menu"
        :style="{ top: notebookContextMenu.y + 'px', left: notebookContextMenu.x + 'px' }">
      <li @click="openNotebook(selectedNotebook)">打开</li>
      <li @click="openRenameNotebook">重命名</li>
      <li @click="openMoveNotebook">移动</li>
      <li @click="deleteNotebook">删除</li>
    </ul>

    <div v-if="dialog.visible" class="modal">
      <div class="modal-content">
        <h3>{{ dialog.title }}</h3>

        <template v-if="dialog.type !== 'move-notebook'">
          <input v-model="dialog.form.name" placeholder="名称" />
          <input v-model="dialog.form.tag" placeholder="标签（用逗号分隔，输入名称或ID）" />
        </template>

        <template v-else>
          <p>选择目标笔记空间：</p>
          <select v-model="dialog.form.targetNoteSpaceId" class="space-select">
            <option
                v-for="ws in workspaceList"
                :key="ws.id"
                :value="ws.id"
                :disabled="ws.id === selectedWorkspace.id"
            >
              {{ ws.name }}
              <template v-if="ws.id === selectedWorkspace.id">(当前)</template>
            </option>
          </select>
          <p v-if="workspaceList.length <= 1" class="warning-text">没有其他笔记空间可供移动。</p>
        </template>

        <div class="modal-actions">
          <button @click="confirmDialog" :disabled="isConfirmDisabled">确认</button>
          <button class="cancel" @click="closeDialog">取消</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, computed, watch } from "vue";
// 如果不再使用 router 进行跳转，可以移除 useRouter
import service from '../../api/request';
import { useUserStore } from '@/stores/user';

// --- 1. 必须显式定义 emit ---
const emit = defineEmits(['openNotebook']);

const userStore = useUserStore();

// 基础 API 地址
const BASE_PATH = "/noting";
const CURRENT_USER_ID = computed(() => userStore.userInfo.id);

/* ===============================
 数据
================================ */
const workspaceList = ref([]);
const selectedWorkspace = ref(null);
const selectedNotebook = ref(null);
const notebooks = ref([]);

/* ===============================
 右键菜单/上下文管理
================================ */
const workspaceContextMenu = ref({ visible: false, x: 0, y: 0, target: null });
const notebookContextMenu = ref({ visible: false, x: 0, y: 0, target: null });

const showWorkspaceContext = (ws, evt) => {
  hideAllContextMenus();
  workspaceContextMenu.value = { visible: true, x: evt.clientX, y: evt.clientY, target: ws };
  selectedWorkspace.value = ws;
};

const showNotebookContext = (nb, evt) => {
  hideAllContextMenus();
  notebookContextMenu.value = { visible: true, x: evt.clientX, y: evt.clientY, target: nb };
  selectedNotebook.value = nb;
};

const hideAllContextMenus = () => {
  if (workspaceContextMenu.value) workspaceContextMenu.value.visible = false;
  if (notebookContextMenu.value) notebookContextMenu.value.visible = false;
};

// --- 2. 修改 openNotebook 函数以传递所需参数 ---
function openNotebook(nb) {
  const target = nb || selectedNotebook.value;

  if (!target || !target.id) {
    console.error("无法打开: 目标笔记本无效");
    return;
  }

  if (!selectedWorkspace.value) {
    console.error("无法打开: 未选中笔记空间");
    return;
  }

  // 触发事件，传递参数：
  // 1. spaceId: 空间ID
  // 2. notebookId: 当前笔记本ID
  // 3. notebookName: 当前笔记本名称
  // 4. notebookList: 当前空间下的所有笔记本列表（包含其他笔记本ID）
  emit('openNotebook', {
    spaceId: selectedWorkspace.value.id,
    notebookId: target.id,
    notebookName: target.name,
    notebookList: notebooks.value
  });

  // 清理 UI 状态
  hideAllContextMenus();


  selectedNotebook.value = null;
}

/* ===============================
 弹窗数据与计算属性
================================ */
const dialog = ref({
  visible: false,
  type: "",
  title: "",
  form: {
    name: "",
    tag: "",
    targetNoteSpaceId: null,
  },
});

const isConfirmDisabled = computed(() => {
  const { type, form } = dialog.value;
  if (type === 'move-notebook') {
    return !form.targetNoteSpaceId;
  }
  const name = (form.name || '').toString();
  return !name.trim();
});

/* ===============================
 初始化加载 (onMounted)
================================ */

const fetchWorkspaces = async () => {
  const userId = CURRENT_USER_ID.value;

  if (!userId) {
    console.warn("User ID is null. Cannot fetch workspaces.");
    return;
  }

  try {
    const response = await service.post(`${BASE_PATH}/spaces/user`, { userId });

    if (response.data.code === 200 && Array.isArray(response.data.data)) {
      const workspaces = response.data.data;

      const tasks = workspaces.map(async (ws) => {
        if (!ws.tag && ws.tag !== 0) {
          ws.tagName = null;
          return;
        }
        ws.tagName = await getTagNameString(ws.tag);
      });

      await Promise.all(tasks);
      workspaceList.value = workspaces;

      if (workspaceList.value.length > 0 && !selectedWorkspace.value) {
        selectWorkspace(workspaceList.value[0]);
      }
    } else {
      workspaceList.value = [];
    }
  } catch (error) {
    console.error("加载笔记空间失败:", error);
    alert("加载笔记空间失败，请检查网络或登录状态。");
  }
};

onMounted(() => {
  if (CURRENT_USER_ID.value) {
    fetchWorkspaces();
  } else {
    const unwatch = watch(CURRENT_USER_ID, (newId) => {
      if (newId) {
        fetchWorkspaces();
        unwatch();
      }
    });
  }
});

/* ===============================
 帮助函数
================================ */
async function getTagNameString(tag) {
  try {
    if (tag === null || tag === undefined || tag === '') return null;
    const maybeId = Number(tag);
    if (!Number.isNaN(maybeId) && String(tag).trim() !== '') {
      const tagResp = await service.post(`${BASE_PATH}/tags/name`, { tagId: maybeId });
      if (tagResp?.data?.code === 200 && tagResp.data.data) {
        return tagResp.data.data.tagName || String(tag);
      }
    }
    return String(tag);
  } catch (err) {
    return String(tag);
  }
}

/* ===============================
 笔记空间操作 (CRUD)
================================ */

function selectWorkspace(ws) {
  selectedWorkspace.value = ws;
  selectedNotebook.value = null;
  hideAllContextMenus();
  loadNotebooks(ws.id);
}

async function deleteWorkspace() {
  if (!workspaceContextMenu.value.target || !CURRENT_USER_ID.value) return;

  const target = workspaceContextMenu.value.target;
  const targetId = target.id;
  const requestBody = { id: targetId, userId: CURRENT_USER_ID.value };

  try {
    await service.delete(`${BASE_PATH}/spaces`, { data: requestBody });

    console.log(`笔记空间 ID:${targetId} 删除成功`);
    await fetchWorkspaces();
    if (workspaceList.value.length === 0) {
      selectedWorkspace.value = null;
      notebooks.value = [];
    }

  } catch (error) {
    console.error("删除笔记空间失败:", error);
    alert("删除失败: " + (error.response?.data?.message || '网络或服务器错误'));
  } finally {
    hideAllContextMenus();
  }
}

/* ===============================
 笔记本操作 (CRUD)
================================ */

async function loadNotebooks(spaceId) {
  const userId = CURRENT_USER_ID.value;

  if (!spaceId || !userId) {
    notebooks.value = [];
    return;
  }

  try {
    const response = await service.post(`${BASE_PATH}/notebooks/by-space`, {
      spaceId,
      userId
    });

    if (response.data.code === 200 && Array.isArray(response.data.data)) {
      const notes = response.data.data;
      const tasks = notes.map(async (nb) => {
        const tagId = nb.tagId ?? nb.tag;
        if (!tagId && tagId !== 0) {
          nb.tagName = null;
          return;
        }
        nb.tagName = await getTagNameString(tagId);
      });

      await Promise.all(tasks);
      notebooks.value = notes;
    } else {
      notebooks.value = [];
    }
  } catch (error) {
    console.error("加载笔记本失败:", error);
    notebooks.value = [];
  }
}

async function deleteNotebook() {
  if (!selectedNotebook.value || !CURRENT_USER_ID.value) return;

  const targetId = selectedNotebook.value.id;
  const requestBody = { id: targetId, userId: CURRENT_USER_ID.value };

  try {
    await service.delete(`${BASE_PATH}/notebooks`, { data: requestBody });

    notebooks.value = notebooks.value.filter((nb) => nb.id !== targetId);
    selectedNotebook.value = null;

  } catch (error) {
    console.error("删除笔记本失败:", error);
    alert("删除失败: " + (error.response?.data?.message || '网络或服务器错误'));
  } finally {
    hideAllContextMenus();
  }
}

function openMoveNotebook() {
  hideAllContextMenus();
  if (!selectedNotebook.value || !selectedWorkspace.value) return;

  let defaultTargetId =
      workspaceList.value.find(ws => ws.id !== selectedWorkspace.value.id)?.id || null;

  dialog.value = {
    visible: true,
    title: `移动笔记本: ${selectedNotebook.value.name}`,
    type: "move-notebook",
    form: {
      targetNoteSpaceId: defaultTargetId,
      name: "",
      tag: ""
    },
  };
}

/* ===============================
 弹窗控制与确认 (CRUD)
================================ */
const openCreateWorkspace = () => {
  dialog.value = {
    visible: true,
    title: "创建笔记空间",
    type: "create-workspace",
    form: { name: "", tag: "", targetNoteSpaceId: null }
  };
};

const openCreateNotebook = () => {
  hideAllContextMenus();
  if (!selectedWorkspace.value) {
    alert("请先选择或创建一个笔记空间！");
    return;
  }
  dialog.value = {
    visible: true,
    title: "创建笔记本",
    type: "create-notebook",
    form: { name: "", tag: "", targetNoteSpaceId: null }
  };
};

function openRenameWorkspace() {
  hideAllContextMenus();
  const target = workspaceContextMenu.value.target;
  if (!target) return;
  dialog.value = {
    visible: true,
    title: "重命名笔记空间",
    type: "rename-workspace",
    form: { name: target.name, tag: target.tag ?? '', targetNoteSpaceId: null }
  };
}

function openRenameNotebook() {
  hideAllContextMenus();
  const target = selectedNotebook.value;
  if (!target) return;
  dialog.value = {
    visible: true,
    title: "重命名笔记本",
    type: "rename-notebook",
    form: {
      name: target.name,
      tag: target.tagId ?? target.tag ?? '',
      targetNoteSpaceId: null
    }
  };
}

const closeDialog = () => {
  dialog.value.visible = false;
};

const confirmDialog = async () => {
  const { type, form } = dialog.value;

  if (!CURRENT_USER_ID.value) {
    alert("用户认证信息缺失，请重新登录。");
    return;
  }

  if (type !== 'move-notebook' && !(form.name || '').toString().trim()) {
    alert("名称不能为空！");
    return;
  }

  try {
    if (type === "create-workspace") {
      const requestBody = { name: form.name, userId: CURRENT_USER_ID.value , tag: form.tag };
      const response = await service.post(`${BASE_PATH}/spaces`, requestBody);
      const newSpaceVO = response.data.data;
      newSpaceVO.tagName = await getTagNameString(newSpaceVO.tag);
      workspaceList.value.push(newSpaceVO);
      selectWorkspace(newSpaceVO);

    } else if (type === "rename-workspace") {
      const target = workspaceContextMenu.value.target;
      const requestBody = { id: target.id, name: form.name, tag: form.tag, userId: CURRENT_USER_ID.value };
      const response = await service.put(`${BASE_PATH}/spaces`, requestBody);
      const updatedSpaceVO = response.data.data;
      updatedSpaceVO.tagName = await getTagNameString(updatedSpaceVO.tag);
      Object.assign(target, updatedSpaceVO);

    } else if (type === "create-notebook") {
      const spaceId = selectedWorkspace.value?.id;
      const requestBody = { spaceId: spaceId, name: form.name, tag: form.tag, userId: CURRENT_USER_ID.value };
      const response = await service.post(`${BASE_PATH}/notebooks`, requestBody);
      const newNotebookVO = response.data.data;
      newNotebookVO.tagName = await getTagNameString(newNotebookVO.tag ?? newNotebookVO.tagId);
      notebooks.value.push(newNotebookVO);

    } else if (type === "rename-notebook") {
      const target = selectedNotebook.value;
      const requestBody = { id: target.id, name: form.name, tag: form.tag, userId: CURRENT_USER_ID.value };
      const response = await service.put(`${BASE_PATH}/notebooks`, requestBody);
      const updatedNotebookVO = response.data.data;
      updatedNotebookVO.tagName = await getTagNameString(updatedNotebookVO.tag ?? updatedNotebookVO.tagId);
      Object.assign(target, updatedNotebookVO);

    } else if (type === 'move-notebook') {
      const targetNotebook = selectedNotebook.value;
      const targetNoteSpaceId = form.targetNoteSpaceId;
      const requestBody = {
        id: targetNotebook.id,
        targetNoteSpaceId: targetNoteSpaceId,
        userId: CURRENT_USER_ID.value
      };
      await service.put(`${BASE_PATH}/notebooks/move-notebook`, requestBody);
      notebooks.value = notebooks.value.filter(nb => nb.id !== targetNotebook.id);
      selectedNotebook.value = null;
    }
  } catch (error) {
    alert(`${dialog.value.title} 失败: ` + (error.response?.data?.message || '网络或服务器错误'));
  }
  dialog.value.visible = false;
};
</script>

<style scoped>
/* 样式代码保持不变 */
.workspace-page { display: flex; height: 100vh; background: #fff; }
.sidebar { width: 260px; border-right: 1px solid #eee; padding: 16px; user-select: none; }
.sidebar-header { display: flex; justify-content: space-between; align-items: center; }
.add-btn { width: 32px; height: 32px; font-size: 20px; border: none; background: #2d5cf6; color: #fff; border-radius: 4px; cursor: pointer; }
.workspace-list li { list-style: none; padding: 10px; border-radius: 6px; cursor: pointer; margin-bottom: 4px; }
.workspace-list li.active, .workspace-list li:hover { background: #eef2ff; }
.workspace-item { display: flex; justify-content: space-between; align-items: center; }
.ws-info { flex: 1; margin-left: 8px; }
.ws-info .tags { font-size: 12px; color: #38761d; margin: 2px 0 2px 0; }
.ws-info .date { font-size: 12px; color: #999; }
.menu-btn { cursor: pointer; padding: 4px; font-size: 20px; line-height: 1; }
.main-panel { padding: 20px; flex: 1; }
.section-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
.create-btn { padding: 8px 16px; border: none; background: #2d5cf6; color: white; border-radius: 4px; cursor: pointer; }
.empty { text-align: center; color: #aaa; margin-top: 200px; }
.no-data { font-size: 14px; color: #888; text-align: center; margin-top: 10px; }
.notebook-list { list-style: none; padding: 0; margin: 0; }
.notebook-list li { padding: 10px 15px; border-radius: 6px; cursor: pointer; margin-bottom: 8px; background: #f8f8f8; transition: background 0.2s; }
.notebook-list li:hover { background: #eee; }
.notebook-item { display: flex; align-items: center; justify-content: space-between; }
.notebook-item > div { flex: 1; margin-left: 10px; }
.notebook-item .name { font-weight: bold; }
.notebook-item .tags-and-time { font-size: 12px; color: #666; margin-top: 4px; }
.context-menu { position: fixed; background: #fff; border: 1px solid #ddd; list-style: none; width: 140px; box-shadow: 0 2px 8px rgba(0,0,0,.15); z-index: 99; padding: 6px 0; }
.context-menu li { padding: 8px 14px; cursor: pointer; }
.context-menu li:hover { background: #f5f5f5; }
.modal { position: fixed; inset: 0; background: rgba(0,0,0,.4); display: flex; justify-content: center; align-items: center; z-index: 100; }
.modal-content { width: 360px; background: #fff; border-radius: 8px; padding: 20px; }
.modal-content input, .modal-content .space-select { width: 100%; padding: 8px 10px; margin-top: 10px; border-radius: 4px; border: 1px solid #ddd; box-sizing: border-box; }
.modal-content p { margin: 5px 0; }
.warning-text { font-size: 12px; color: #d32f2f; margin-top: 5px !important; }
.modal-actions { margin-top: 15px; text-align: right; }
.modal-actions button { padding: 6px 14px; border: none; border-radius: 4px; cursor: pointer; margin-left: 8px; }
.modal-actions button:first-child { background: #2d5cf6; color: white; }
.modal-actions button:disabled { background: #ccc; cursor: not-allowed; }
.cancel { background: #ddd; color: #333; }
</style>