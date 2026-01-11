<template>
  <div class="editor-layout" @click="closeAllDropdowns">
    <aside class="sidebar">
      <div class="notebook-header new-style">
        <div class="header-left">
          <button class="back-btn" @click="emit('close')" title="返回">
            <svg viewBox="0 0 24 24"><path fill="currentColor" d="M15.41 16.59L10.83 12l4.58-4.59L14 6l-6 6l6 6l1.41-1.41z"/></svg>
          </button>
          <h3 class="notebook-title" :title="notebookName">
            {{ notebookName || '数据结构' }}
          </h3>
        </div>

        <button class="btn-icon add-note-btn" @click.stop="showNewNoteModal = true" title="新建笔记">
          +
        </button>
      </div>

      <div class="note-list-container">
        <div v-if="isLoading" class="list-loading-state">
          正在加载笔记列表...
        </div>
        <div v-else-if="noteList.length === 0" class="list-empty-state">
          当前笔记本没有笔记。
        </div>
        <ul v-else class="note-list new-style">
          <li
              v-for="note in noteList"
              :key="note.id"
              :class="{ active: currentNote && currentNote.id === note.id }"
              @click="selectNote(note)"
          >
            <div class="note-item">
              <span class="file-icon" :title="note.type === 'pdf' ? 'PDF文件' : '富文本'">{{ note.type === 'pdf' ? '📄' : '📝' }}</span>
              <div class="note-info">
                <p class="note-title">{{ note.title || '无标题笔记' }}</p>
                <div class="note-meta-new-style">
                  <p class="meta-line">修改：{{ note.updatedAt }}</p>
                  <p class="meta-line">创建：{{ note.createdAt }}</p>
                  <p class="meta-line">类型：{{ note.fileType.toLowerCase() }}</p>
                </div>
              </div>

              <div class="relative menu-wrapper" @click.stop>
                <button class="btn-icon actions-menu-btn" title="更多操作" @click="toggleNoteMenu(note.id)">
                  ⋮
                </button>

                <div v-if="showNoteMenuId === note.id" class="dropdown-menu note-actions-menu">
                  <div class="menu-item" @click="handleAction('重命名', note.id)">重命名</div>
                  <div class="menu-item" @click="handleAction('移动到', note.id)">移动到</div>

                  <hr class="menu-divider">

                  <div class="menu-item" @click="handleAction('下载', note.id)">下载</div>

                  <hr class="menu-divider">

                  <div class="menu-item delete-item" @click="handleAction('删除', note.id)">删除</div>
                </div>
              </div>

            </div>
          </li>
        </ul>
      </div>

    </aside>

    <main class="editor-main">
      <div v-if="!currentNote" class="empty-state">
        <div class="empty-message">未选择文件</div>
        <p class="empty-tip">请在左侧列表选择一个笔记进行查看或编辑。</p>
      </div>

      <div v-else-if="currentNoteType === 'pdf'" class="file-preview-container">
        <header class="file-preview-header">
          <h4 class="file-title">PDF 预览: {{ currentNote.title }}</h4>
          <button class="download-btn" @click="handleAction('下载', currentNote.id)">下载文件</button>
        </header>
        <div class="file-content">
          <div v-if="pdfPreviewUrl" class="pdf-wrapper">
            <VuePdfEmbed
                :source="pdfPreviewUrl"
                class="pdf-embed-viewer"
                :width="700"
            />
          </div>
          <p v-else>正在加载 PDF 文件...</p>
        </div>
      </div>

      <div v-else-if="currentNoteType === 'md'" class="editor-container">
        <header class="editor-header">
          <input
              v-model="currentTitle"
              class="title-input"
              placeholder="无标题笔记"
              @blur="updateCurrentNoteTitle"
          />
          <div class="header-actions">
            <span class="save-status">☁️ 已保存</span>
            <button class="save-btn" @click="saveNoteContent">保存</button>
            <button class="publish-btn" @click="handlePublishNote" :disabled="!currentNote">发布</button>
          </div>
        </header>

        <div v-if="!editor" class="loading-state">编辑器加载中...</div>

        <div v-else class="tiptap-wrapper">
          <div class="tiptap-toolbar">
            <div class="toolbar-group">
              <button @click="editor.chain().focus().undo().run()" :disabled="!editor.can().undo()" title="撤销">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M12.5 8c-2.65 0-5.05.99-6.9 2.6L2 7v9h9l-3.62-3.62c1.39-1.16 3.16-1.88 5.12-1.88c3.54 0 6.55 2.31 7.6 5.5l2.37-.78C21.08 11.03 17.15 8 12.5 8z"/></svg>
              </button>
              <button @click="editor.chain().focus().redo().run()" :disabled="!editor.can().redo()" title="重做">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M18.4 10.6C16.55 9 14.15 8 11.5 8c-4.65 0-8.58 3.03-9.96 7.22L3.9 16a8.002 8.002 0 0 1 7.6-5.5c1.95 0 3.73.72 5.12 1.88L13 16h9V7l-3.6 3.6z"/></svg>
              </button>

              <button @click="editor.chain().focus().unsetAllMarks().run()" title="清除格式">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M19.89 18.48l-7.45-7.45l.95-2.26L15.1 5.3a1 1 0 0 1 1.59.67l2.09 9.17l1.11 3.34M6 19v-2.4l2.39-2.39l2.4 2.4H6m1.39-8.71l4.62-4.62a.993.993 0 0 1 1.41 0l2.83 2.83l-1.79.4L9.09 3.53L2.53 10.09C1.94 10.68 1.94 11.63 2.53 12.22l2.83 2.83L11 9.41L7.39 10.29z"/></svg>
              </button>
            </div>

            <div class="divider"></div>

            <div class="toolbar-group relative" @click.stop>
              <button class="insert-pill-btn" @click="toggleInsertMenu">
                <span class="plus-icon">＋</span> 插入 <span class="arrow-icon">▼</span>
              </button>

              <div v-if="showInsertMenu" class="dropdown-menu insert-menu" @click.stop="closeAllDropdowns">
                <div class="menu-item" @click="triggerImageUpload"><span class="emoji">🖼️</span> 图片</div>
                <div class="menu-item" @click="editor.chain().focus().toggleCodeBlock().run()"><span class="emoji">💻</span> 代码块</div>
                <div class="menu-item" @click="editor.chain().focus().setHorizontalRule().run()"><span class="emoji">―</span> 水平线</div>
              </div>
            </div>

            <div class="divider"></div>

            <div class="toolbar-group">
              <select @change="changeHeading($event)" class="toolbar-select" title="段落格式">
                <option value="0" :selected="editor.isActive('paragraph')">正文</option>
                <option value="1" :selected="editor.isActive('heading', { level: 1 })">标题 1</option>
                <option value="2" :selected="editor.isActive('heading', { level: 2 })">标题 2</option>
                <option value="3" :selected="editor.isActive('heading', { level: 3 })">标题 3</option>
              </select>
            </div>

            <div class="toolbar-group">
              <button @click="editor.chain().focus().toggleBold().run()" :class="{ 'is-active': editor.isActive('bold') }" title="加粗">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M15.6 10.79c.97-.67 1.65-1.77 1.65-2.79c0-2.26-1.75-4-4-4H7v14h7.04c2.09 0 3.71-1.7 3.71-3.79c0-1.52-.86-2.82-2.15-3.42zM10 6.5h3c.83 0 1.5.67 1.5 1.5s-.67 1.5-1.5 1.5h-3v-3zm3.5 9H10v-3h3.5c.83 0 1.5.67 1.5 1.5s-.67 1.5-1.5 1.5z"/></svg>
              </button>
              <button @click="editor.chain().focus().toggleUnderline().run()" :class="{ 'is-active': editor.isActive('underline') }" title="下划线">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M12 17c3.31 0 6-2.69 6-6V3h-2.5v8c0 1.93-1.57 3.5-3.5 3.5S8.5 12.93 8.5 11V3H6v8c0 3.31 2.69 6 6 6zm-7 2v2h14v-2H5z"/></svg>
              </button>
              <button @click="editor.chain().focus().toggleStrike().run()" :class="{ 'is-active': editor.isActive('strike') }" title="删除线">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M10 19h4v-3h-4v3zM5 4v3h5v3h4V7h5V4H5zM3 14h18v-2H3v2z"/></svg>
              </button>
            </div>

            <div class="toolbar-group">
              <div class="color-picker-wrapper">
                <input type="color" class="color-input" @input="editor.chain().focus().toggleHighlight({ color: $event.target.value }).run()" title="背景颜色">
                <svg viewBox="0 0 24 24" width="18" height="18" style="margin-top:2px"><path fill="currentColor" d="M18.5 1.15c-1.79-.63-3.74-.12-5.02 1.33l-1.53 1.74l5.5 5.5l1.74-1.53c1.45-1.27 1.96-3.23 1.33-5.02l-2.02 2.02l-2.02-2.02l2.02-2.02zM4.13 14.06L12.95 5.24l5.5 5.5L9.63 19.56c-1.26 1.26-3.16 1.55-4.72.72l3.33-3.33l-2.12-2.12l-3.33 3.33c-.83-1.56-.54-3.46.72-4.72l.62.62zM3 21.76L4.24 23l3.54-3.54l-2.12-2.12L3 21.76z"/></svg>
              </div>
            </div>

            <div class="divider"></div>

            <div class="toolbar-group">
              <button @click="editor.chain().focus().toggleTaskList().run()" :class="{ 'is-active': editor.isActive('taskList') }" title="待办事项">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M19 3H5c-1.11 0-2 .89-2 2v14c0 1.11.89 2 2 2h14c1.1 0 2-.89 2-2V5a2 2 0 0 0-2-2m-9 14l-5-5l1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg>
              </button>
              <button @click="editor.chain().focus().toggleBulletList().run()" :class="{ 'is-active': editor.isActive('bulletList') }" title="无序列表">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M7 5h14v2H7V5m0 8v-2h14v2H7M7 21v-2h14v2H7M3 6c0-.55.45-1 1-1s1 .45 1 1s-.45 1-1 1s-1-.45-1-1m0 8c0-.55.45-1 1-1s1 .45 1 1s-.45 1-1 1s-1-.45-1-1m0 8c0-.55.45-1 1-1s1 .45 1 1s-.45 1-1 1s-1-.45-1-1z"/></svg>
              </button>
              <button @click="editor.chain().focus().toggleOrderedList().run()" :class="{ 'is-active': editor.isActive('orderedList') }" title="有序列表">
                <svg viewBox="0 0 24 24"><path fill="currentColor" d="M7 13v-2h14v2H7m0 6v-2h14v2H7M7 7V5h14v2H7M3 8V5H2V4h2v4H3m-1 9v-1h3v4H2v-1h2v-.5H3v-1h2v-.5H2M2 14v-4h3v1H4v.5h1v1H4v.5h2v1H2z"/></svg>
              </button>
            </div>
          </div>
          <editor-content :editor="editor" class="tiptap-content" />
        </div>
      </div>
    </main>

    <div v-if="showNewNoteModal" class="modal-overlay" @click.self="showNewNoteModal = false">
      <div class="new-note-modal">
        <h4 class="modal-title">新建笔记</h4>
        <div class="modal-input-group">
          <label for="noteTitle">笔记名</label>
          <input id="noteTitle" v-model="newNoteTitle" placeholder="请输入笔记名称" class="modal-input" />
        </div>

        <div class="modal-input-group">
          <label>创建方式</label>
          <div class="creation-options">
            <button :class="['creation-btn', { active: newNoteType === 'online' }]" @click="newNoteType = 'online'">
              在线编辑 (富文本)
            </button>
            <button :class="['creation-btn', { active: newNoteType === 'upload' }]" @click="newNoteType = 'upload'">
              上传文件
            </button>
          </div>
        </div>

        <div class="modal-actions">
          <button class="modal-cancel-btn" @click="showNewNoteModal = false">取消</button>
          <button class="modal-confirm-btn" @click="handleNewNoteFromModal">确定</button>
        </div>
      </div>
    </div>

    <div v-if="renameDialog.visible" class="modal-overlay" @click.self="cancelRename">
      <div class="rename-dialog">
        <h4 class="modal-title">重命名笔记</h4>
        <div class="modal-input-group">
          <label for="renameTitle">新的笔记名称</label>
          <input
              id="renameTitle"
              type="text"
              v-model="renameDialog.newTitle"
              :placeholder="renameDialog.originalTitle"
              class="modal-input"
              @keyup.enter="confirmRename"
              ref="renameInputRef"
          />
        </div>

        <div class="modal-actions">
          <button class="modal-cancel-btn" @click="cancelRename">取消</button>
          <button
              class="modal-confirm-btn"
              @click="confirmRename"
              :disabled="!renameDialog.newTitle.trim() || renameDialog.newTitle.trim() === renameDialog.originalTitle"
          >
            确定
          </button>
        </div>
      </div>
    </div>

    <div v-if="deleteDialog.visible" class="modal-overlay" @click.self="cancelDelete">
      <div class="delete-dialog rename-dialog">
        <h4 class="modal-title">删除笔记</h4>
        <p class="delete-message">
          确定要删除笔记 《{{ deleteDialog.noteTitle }}》 吗? 此操作无法撤销。
        </p>

        <div class="modal-actions">
          <button class="modal-cancel-btn" @click="cancelDelete">取消</button>
          <button class="modal-confirm-btn delete-confirm-btn" @click="confirmDelete">
            确定删除
          </button>
        </div>
      </div>
    </div>

    <div v-if="moveToDialog.visible" class="modal-overlay" @click.self="cancelMoveTo">
      <div class="rename-dialog"> <h4 class="modal-title">移动笔记</h4>
        <p class="delete-message">
          请选择要将笔记 **"{{ moveToDialog.noteTitle }}"** 移动到的目标笔记本：
        </p>

        <div class="modal-input-group">
          <label for="targetNotebook">目标笔记本</label>
          <select id="targetNotebook" v-model="moveToDialog.targetNotebookId" class="modal-input">
            <option disabled :value="null">请选择笔记本</option>
            <option
                v-for="notebook in moveToDialog.notebookList"
                :key="notebook.id"
                :value="notebook.id"
                :disabled="notebook.id === notebookId"
            >
              {{ notebook.name }} <span v-if="notebook.id === notebookId">(当前)</span>
            </option>
          </select>
        </div>

        <div class="modal-actions">
          <button class="modal-cancel-btn" @click="cancelMoveTo">取消</button>
          <button
              class="modal-confirm-btn"
              @click="confirmMoveTo"
              :disabled="!moveToDialog.targetNotebookId || moveToDialog.targetNotebookId === notebookId"
          >
            确定移动
          </button>
        </div>
      </div>
    </div>

    <div v-if="downloadDialog.visible" class="modal-overlay" @click.self="cancelDownload">
      <div class="rename-dialog">
        <h4 class="modal-title">下载笔记文件</h4>
        <p class="delete-message">
          确定要下载笔记 **"{{ downloadDialog.noteTitle }}"** 吗? 文件将准备下载。
        </p>

        <div class="modal-actions">
          <button class="modal-cancel-btn" @click="cancelDownload">取消</button>
          <button class="modal-confirm-btn" @click="confirmDownload">
            确定下载
          </button>
        </div>
      </div>
    </div>

    <input
        type="file"
        ref="fileInput"
        accept="image/*"
        style="display:none"
        @change="handleImageUpload"
    />

    <input
        type="file"
        ref="uploadFileInput"
        accept="image/*, .doc, .docx, .pdf, .txt"
        style="display:none"
        @change="handleFileUpload"
    />

    <!-- 消息提示组件 -->
    <MessageToast
      v-if="showToast"
      :message="toastMessage"
      :type="toastType"
      :duration="toastDuration"
      @close="hideMessage"
    />
  </div>
</template>

<script setup>
import { ref, onBeforeUnmount, nextTick, onMounted } from 'vue';
import { useEditor, EditorContent } from '@tiptap/vue-3';

// 核心扩展导入
import StarterKit from '@tiptap/starter-kit';
import Underline from '@tiptap/extension-underline';
import TaskList from '@tiptap/extension-task-list';
import { TaskItem } from '@tiptap/extension-task-item';
import Highlight from '@tiptap/extension-highlight';
import Placeholder from '@tiptap/extension-placeholder';
import TurndownService from 'turndown';
import { debounce } from 'lodash-es';
import VuePdfEmbed from 'vue-pdf-embed'
import MarkdownIt from 'markdown-it';
import { ResizableImage } from '@/extensions/ResizableImage';


// 引入真实的 API 接口
import {
  fetchNotesByNotebook,
  createNote,
  uploadNote,
  updateNote,
  renameNote,
  deleteNote,
  moveNote,
  uploadImage,
  getFileUrl,
  publishNote
} from '@/api/note'; // 确保路径正确

import MessageToast from '@/components/MessageToast.vue'
import { useMessage } from '@/utils/message'

// ----------------- Props & Emits -----------------
const props = defineProps({
  spaceId: Number,
  notebookId: Number,
  notebookName: String,
  notebookList: Array,
  initialNoteId: Number  // 初始选中的笔记ID
});
const emit = defineEmits(['close', 'note-selected']);

// ----------------- 状态管理 -----------------
const showNoteMenuId = ref(null);
const showInsertMenu = ref(false);
const showNewNoteModal = ref(false);
const newNoteTitle = ref('新建笔记');
const newNoteType = ref('md');
const pdfPreviewUrl = ref(null);

const currentNote = ref(null);
const currentNoteType = ref(null);
const noteList = ref([]);
const currentTitle = ref('');
const fileInput = ref(null);
const uploadFileInput = ref(null);
const isLoading = ref(false);
const renameInputRef = ref(null);

// 消息提示
const { showToast, toastMessage, toastType, toastDuration, showSuccess, showError, showInfo, hideMessage } = useMessage()

// 1. 初始化 Markdown 解析器 (MD -> HTML)
const mdParser = new MarkdownIt({
  html: true, // 允许 HTML 标签
  linkify: true, // 自动识别链接
  breaks: true, // 换行符转为 <br>
});

// 2. 初始化 Turndown 服务 (HTML -> MD)
const turndownService = new TurndownService({
  headingStyle: 'atx',
  bulletListMarker: '-',
  codeBlockStyle: 'fenced'
});

// 确保 Turndown 保留图片（包括尺寸信息）
turndownService.addRule('keepImages', {
  filter: ['img'],
  replacement: function (content, node) {
    const alt = node.alt || '';
    const src = node.getAttribute('src') || '';
    const width = node.getAttribute('width');
    const height = node.getAttribute('height');
    const title = node.title || '';
    
    // 如果有尺寸信息，使用 HTML 格式保留
    if (width || height) {
      let htmlAttrs = '';
      if (width) htmlAttrs += ` width="${width}"`;
      if (height) htmlAttrs += ` height="${height}"`;
      if (title) htmlAttrs += ` title="${title}"`;
      return `<img src="${src}" alt="${alt}"${htmlAttrs}>`;
    }
    
    // 否则使用标准 Markdown 格式
    const titlePart = title ? ` "${title}"` : '';
    return `![${alt}](${src}${titlePart})`;
  }
});

// 重命名弹窗状态
const renameDialog = ref({
  visible: false,
  noteId: null,
  originalTitle: '',
  newTitle: '',
  resolve: null, // 用于解决 Promise
});

// 删除弹窗状态
const deleteDialog = ref({
  visible: false,
  noteId: null,
  noteTitle: '',
  resolve: null, // 用于解决 Promise
});

// 移动到弹窗状态
const moveToDialog = ref({
  visible: false,
  noteId: null,
  noteTitle: '',
  notebookList: [], // 目标笔记本列表
  targetNotebookId: null, // 选中的目标笔记本ID
  resolve: null,
});

// 下载弹窗状态
const downloadDialog = ref({
  visible: false,
  noteId: null,
  noteTitle: '',
  resolve: null,
});

// ----------------- TipTap Editor -----------------
// ... (TipTap Editor 配置和 debouncedUpdateNote 保持不变)

const debouncedUpdateNote = debounce(async (meta, file) => {
  // 检查 ID 是否存在，确保在有效笔记上操作
  if (!meta.id) return;

  try {
    isLoading.value = true;

    const updatedVo = await updateNote(meta, file);

    // 更新本地的 updatedAt 状态，给用户反馈
    if (currentNote.value && currentNote.value.id === updatedVo.id) {
      currentNote.value.updatedAt = updatedVo.updatedAt;
      // 同步更新 noteList 中对应笔记的信息
      const noteInList = noteList.value.find(n => n.id === updatedVo.id);
      if (noteInList) {
        Object.assign(noteInList, updatedVo);
      }
    }

    isLoading.value = false;
    console.log(`笔记 ${updatedVo.id} 自动保存成功.`);

  } catch (error) {
    isLoading.value = false;
    console.error('自动保存笔记失败:', error);
  }
}, 5000); // 5000ms = 1秒的延迟，可以根据需要调整

const editor = useEditor({
  content: '',
  extensions: [
    StarterKit, Underline, TaskList,
    TaskItem.configure({ nested: true }), Highlight.configure({ multicolor: true }),
    ResizableImage.configure({ 
      inline: true, 
      allowBase64: true,
      HTMLAttributes: {
        class: 'resizable-image',
      },
    }),
    Placeholder.configure({ placeholder: '输入内容，输入 / 唤起菜单...' }),
  ],
  editorProps: {
    attributes: {
      // 移除原有的 prose 类，使用自定义样式
      class: 'prose-container focus:outline-none',
    },
    handlePaste: (view, event, slice) => {
      // 处理粘贴事件
      const items = Array.from(event.clipboardData?.items || [])
      const imageItem = items.find(item => {
        const type = item.type || ''
        return type.indexOf('image') !== -1
      })
      
      if (imageItem) {
        event.preventDefault()
        event.stopPropagation()
        
        const file = imageItem.getAsFile()
        if (file && file.size > 0) {
          // 确保编辑器存在且当前笔记类型正确
          if (currentNoteType.value !== 'md') {
            showError('请先选择一个富文本笔记进行编辑。')
            return true
          }
          
          // 异步处理图片上传和插入
          handlePastedImage(file).catch(error => {
            console.error('粘贴图片失败:', error)
            showError('图片粘贴失败：' + (error.message || '请稍后重试'))
          })
          
          return true // 阻止默认粘贴行为
        } else {
          console.warn('粘贴的图片文件无效或为空')
        }
      }
      
      return false // 允许其他内容正常粘贴
    },
  },
  onUpdate: ({ editor }) => {
    // 【API调用点 A】: 内容变化时自动保存
    if (currentNote.value && currentNoteType.value === 'md') {

      const htmlContent = editor.getHTML();
      const markdownContent = turndownService.turndown(htmlContent);

      // 1. 本地状态同步
      currentNote.value.content = markdownContent;

      // 2. 构造 File 对象
      const blob = new Blob([markdownContent], { type: 'text/markdown' });
      const filename = `${currentTitle.value}.md`;
      const mdFile = new File([blob], filename, { type: 'text/markdown' });

      // 3. 构造 meta 对象
      const meta = {
        id: currentNote.value.id,
        title: currentTitle.value,
        notebookId: currentNote.value.notebookId,
        fileType: currentNoteType.value
      };

      // 4. 调用防抖函数，而不是直接调用 updateNote
      debouncedUpdateNote(meta, mdFile);
    }
  }
});

const saveNoteContent = async () => {
  // 保持检查不变，但确保逻辑严谨性
  if (!currentNote.value || currentNoteType.value !== 'md' || !editor.value) return;

  try {
    // 1. 获取 HTML 内容
    const htmlContent = editor.value.getHTML();

    // 2. 转换为 Markdown 字符串
    const markdownContent = turndownService.turndown(htmlContent);

    // 3. 构造 Blob/File 对象 (将 Markdown 字符串包装成文件)
    const blob = new Blob([markdownContent], { type: 'text/markdown' });
    const filename = `${currentTitle.value}.md`;
    const mdFile = new File([blob], filename, { type: 'text/markdown' });

    // 4. 构造 meta 对象（仅包含元数据，不包含 content）
    const meta = {
      id: currentNote.value.id,
      title: currentTitle.value, // 使用 .value
      notebookId: currentNote.value.notebookId, // 假设存在此字段
      fileType: currentNoteType.value // 使用 .value
    };

    // 【API调用点 B】: 手动保存笔记内容 (PUT /noting/notes/update)
    const updatedVo = await updateNote(meta, mdFile);

    if (updatedVo) {
      // 更新 currentNote
      if (updatedVo.updatedAt) {
        currentNote.value.updatedAt = updatedVo.updatedAt;
      } else {
        currentNote.value.updatedAt = new Date().toISOString();
      }
      // 同步更新 noteList 中对应笔记的信息
      const noteInList = noteList.value.find(n => n.id === updatedVo.id);
      if (noteInList) {
        Object.assign(noteInList, updatedVo);
      }
    }

    showSuccess('笔记内容保存成功！');

  } catch (error) {
    showError('保存笔记失败，请稍后重试。');
    console.error('Error saving note content:', error);
  }
};

/**
 * 发布笔记
 */
const handlePublishNote = async () => {
  if (!currentNote.value) {
    showError('请先选择一个笔记！');
    return;
  }

  // 确认发布
  if (!confirm('确定要发布这篇笔记吗？发布后笔记将对其他用户可见。')) {
    return;
  }

  try {
    isLoading.value = true;

    // 根据笔记类型处理内容
    let file = null;
    let meta = {};

    if (currentNoteType.value === 'md') {
      // 富文本笔记：获取HTML内容并转换为Markdown
      if (!editor.value) {
        showError('编辑器未初始化，无法发布。');
        return;
      }

      const htmlContent = editor.value.getHTML();
      const markdownContent = turndownService.turndown(htmlContent);

      // 构造 File 对象
      const blob = new Blob([markdownContent], { type: 'text/markdown' });
      const filename = `${currentTitle.value || currentNote.value.title}.md`;
      file = new File([blob], filename, { type: 'text/markdown' });

      // 构造 meta 对象（使用 NoteUpdateMeta 格式）
      meta = {
        id: currentNote.value.id,
        title: currentTitle.value || currentNote.value.title,
        notebookId: currentNote.value.notebookId,
        fileType: 'md'
      };
    } else {
      // PDF或其他文件类型：需要获取文件
      showError('文件类型笔记的发布功能需要先上传文件，请使用更新功能。');
      isLoading.value = false;
      return;
    }

    // 调用发布API
    const publishedVo = await publishNote(meta, file);

    if (publishedVo) {
      // 更新本地笔记信息
      if (publishedVo.updatedAt) {
        currentNote.value.updatedAt = publishedVo.updatedAt;
      }
      // 同步更新 noteList 中对应笔记的信息
      const noteInList = noteList.value.find(n => n.id === publishedVo.id);
      if (noteInList) {
        Object.assign(noteInList, publishedVo);
      }

      showSuccess('笔记发布成功！');
    }

  } catch (error) {
    showError('发布笔记失败：' + (error.response?.data?.message || error.message || '请稍后重试。'));
    console.error('Error publishing note:', error);
  } finally {
    isLoading.value = false;
  }
};


// ----------------- 核心数据操作 -----------------

/**
 * 获取笔记列表
 */
const fetchNotes = async (sortBy = 'updatedAt') => {
  // 确保 notebookId 存在且有效
  if (!props.notebookId) return;

  isLoading.value = true;
  try {
    // 【API调用点 C】: 获取笔记列表 (POST /noting/notes/by-notebook)
    const notes = await fetchNotesByNotebook(props.notebookId);
    noteList.value = notes;

    // 优先选中初始笔记ID，否则选中第一个笔记或保持现有选中状态
    if (props.initialNoteId) {
      const targetNote = noteList.value.find(n => n.id === props.initialNoteId);
      if (targetNote) {
        selectNote(targetNote);
      } else if (noteList.value.length > 0) {
        // 如果初始笔记ID不存在，选中第一个
        selectNote(noteList.value[0]);
      }
    } else if (!currentNote.value && noteList.value.length > 0) {
      selectNote(noteList.value[0]);
    } else if (currentNote.value) {
      const updatedNote = noteList.value.find(n => n.id === currentNote.value.id);
      if (updatedNote) currentNote.value = updatedNote;
      else {
        currentNote.value = null;
        currentNoteType.value = null;
        if (noteList.value.length > 0) selectNote(noteList.value[0]);
      }
    }
  } catch (error) {
    console.error('Failed to fetch notes:', error);
    showError('获取笔记列表失败，请稍后重试。');
  } finally {
    isLoading.value = false;
  }
};

// ----------------- 生命周期 -----------------
onMounted(() => {
  fetchNotes();
});

onBeforeUnmount(() => {
  editor.value?.destroy();
});

// ----------------- 【自定义弹窗函数】 -----------------

/**
 * 显示重命名弹窗并返回一个 Promise，用于替代原生的 prompt
 */
const showRenameDialog = (noteId, originalTitle) => {
  return new Promise((resolve) => {
    renameDialog.value = {
      visible: true,
      noteId,
      originalTitle,
      newTitle: originalTitle, // 初始值设为当前标题
      resolve,
    };
    // 确保弹窗显示后自动聚焦输入框
    nextTick(() => {
      // 使用可选链或条件判断确保引用存在
      renameInputRef.value?.focus();
    });
  });
};

// 确认重命名
const confirmRename = () => {
  const newTitle = renameDialog.value.newTitle.trim();
  if (!newTitle) return;

  // 解决 Promise，并传递新标题
  if (renameDialog.value.resolve) {
    renameDialog.value.resolve(newTitle);
  }
  // 关闭弹窗并重置状态
  renameDialog.value.visible = false;
  renameDialog.value.newTitle = '';
};

// 取消重命名
const cancelRename = () => {
  // 解决 Promise，并传递 null 表示取消
  if (renameDialog.value.resolve) {
    renameDialog.value.resolve(null);
  }
  // 关闭弹窗并重置状态
  renameDialog.value.visible = false;
  renameDialog.value.newTitle = '';
};

/**
 * 显示删除确认弹窗
 */
const showDeleteDialog = (noteId, noteTitle) => {
  return new Promise((resolve) => {
    deleteDialog.value = {
      visible: true,
      noteId,
      noteTitle,
      resolve,
    };
  });
};

// 确认删除
const confirmDelete = () => {
  // 解决 Promise，并传递 true 表示确定删除
  if (deleteDialog.value.resolve) {
    deleteDialog.value.resolve(true);
  }
  // 关闭弹窗并重置状态
  deleteDialog.value.visible = false;
  // 清除 noteTitle 和 noteId 状态，避免泄露
  deleteDialog.value.noteTitle = '';
  deleteDialog.value.noteId = null;
};

// 取消删除
const cancelDelete = () => {
  // 解决 Promise，并传递 false 表示取消
  if (deleteDialog.value.resolve) {
    deleteDialog.value.resolve(false);
  }
  // 关闭弹窗并重置状态
  deleteDialog.value.visible = false;
  deleteDialog.value.noteTitle = '';
  deleteDialog.value.noteId = null;
};


/**
 * 显示移动到弹窗
 * @param {string | number} noteId
 * @param {string} noteTitle
 * @param {Array} notebookList - 笔记本列表
 * @returns {Promise<number | null>} 返回目标笔记本ID或 null (如果取消)
 */
const showMoveToDialog = (noteId, noteTitle, notebookList) => {
  return new Promise((resolve) => {
    moveToDialog.value = {
      visible: true,
      noteId,
      noteTitle,
      notebookList,
      targetNotebookId: props.notebookId, // 默认选中当前笔记本
      resolve,
    };
  });
};

// 确认移动到
const confirmMoveTo = () => {
  const targetId = moveToDialog.value.targetNotebookId;
  // 仅在 targetId 有效且不是当前笔记本时解决 Promise
  if (targetId && targetId !== props.notebookId && moveToDialog.value.resolve) {
    moveToDialog.value.resolve(targetId);
  } else {
    // 视为取消或无效选择
    moveToDialog.value.resolve(null);
  }
  // 关闭弹窗并重置状态
  moveToDialog.value.visible = false;
  moveToDialog.value.targetNotebookId = null;
};

// 取消移动到
const cancelMoveTo = () => {
  if (moveToDialog.value.resolve) {
    moveToDialog.value.resolve(null);
  }
  moveToDialog.value.visible = false;
  moveToDialog.value.targetNotebookId = null;
};

/**
 * 显示下载弹窗
 */
const showDownloadDialog = (noteId, noteTitle) => {
  return new Promise((resolve) => {
    downloadDialog.value = {
      visible: true,
      noteId,
      noteTitle,
      resolve,
    };
  });
};

// 确认下载
const confirmDownload = () => {
  if (downloadDialog.value.resolve) {
    downloadDialog.value.resolve(true);
  }
  downloadDialog.value.visible = false;
  downloadDialog.value.noteTitle = '';
  downloadDialog.value.noteId = null;
};

// 取消下载
const cancelDownload = () => {
  if (downloadDialog.value.resolve) {
    downloadDialog.value.resolve(false);
  }
  downloadDialog.value.visible = false;
  downloadDialog.value.noteTitle = '';
  downloadDialog.value.noteId = null;
};

// ----------------- 逻辑函数 -----------------
const closeAllDropdowns = () => {
  showInsertMenu.value = false;
  showNoteMenuId.value = null;
};

const fetchFileContentByUrl = async (url) => {
  // 使用 fetch 的 cache 配置来强制浏览器发起网络请求
  const response = await fetch(url, {
    method: 'GET',
    cache: 'no-cache' // 'reload' 表示忽略本地缓存，强制从服务器获取
    // 或者使用 'no-store' / 'no-cache'
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch content from URL: ${url}, Status: ${response.status}`);
  }
  return response.text();
};

const selectNote = async (note) => {
  // 如果切换回同一个笔记，需要强制重新获取内容
  const isSameNote = currentNote.value && currentNote.value.id === note.id;
  
  currentNote.value = note;
  currentTitle.value = note.title;
  currentNoteType.value = note.fileType;
  pdfPreviewUrl.value = null;
  
  // 通知父组件当前选中的笔记ID
  emit('note-selected', note.id);

  // 1. 获取文件名 (假设 note 对象中包含文件名)
  const fileName = note.filename;
  if (!fileName) {
    console.error(`Note ${note.id} missing filename.`);
    // 强制清空编辑器/预览区
    editor.value?.commands.setContent('', false);
    return;
  }

  try {
    // 2. 获取 MinIO 文件 URL
    // 如果是同一个笔记，添加时间戳参数强制刷新缓存
    const fileUrl = await getFileUrl(fileName);
    if (!fileUrl) {
      throw new Error('Failed to get file URL.');
    }

    // 如果是同一个笔记，添加时间戳参数强制刷新缓存
    const urlWithCacheBuster = isSameNote 
      ? `${fileUrl}${fileUrl.includes('?') ? '&' : '?'}_t=${Date.now()}`
      : fileUrl;

    if (note.fileType === 'pdf') {
      // 3. 处理 PDF 预览
      // PDF 只需要 URL。您需要将这个 URL 传递给您集成的 PDF 预览组件。
      pdfPreviewUrl.value = urlWithCacheBuster;
      // 记得在模板中绑定这个 URL 到 PDF 预览组件
      console.log(`PDF Preview URL: ${urlWithCacheBuster}`);
    } else if (note.fileType === 'md' && editor.value) {
      // 4. 处理 Markdown 文件
      const markdownContent = await fetchFileContentByUrl(urlWithCacheBuster);
      const htmlContent = mdParser.render(markdownContent || '');
      editor.value.commands.setContent(htmlContent, false);
      nextTick(() => {
        editor.value.commands.focus('end');
      });
    }
  } catch (error) {
    console.error('Failed to load note content:', error);
    showError('加载笔记内容失败，请检查文件链接。');
    // 如果加载失败，清空编辑器/预览区
    editor.value?.commands.setContent('', false);
  }
};

const toggleNoteMenu = (noteId) => {
  showNoteMenuId.value = showNoteMenuId.value === noteId ? null : noteId;
};

const handleAction = async (action, noteId) => {
  closeAllDropdowns();
  const note = noteList.value.find(n => n.id === noteId);
  if (!note) return;

  try {
    if (action === '重命名') {
      // **调用自定义弹窗，并等待 Promise 结果**
      const newTitle = await showRenameDialog(noteId, note.title);

      if (newTitle && newTitle !== note.title) {
        // 【API调用点 D】: 重命名笔记 (PUT /noting/notes/rename)
        const updateResult = await renameNote(noteId, newTitle);
        note.title = newTitle;
        note.updatedAt = updateResult.updatedAt;

        if (currentNote.value && currentNote.value.id === noteId) {
          currentTitle.value = newTitle;
          currentNote.value.updatedAt = updateResult.updatedAt;
        }

        showSuccess(`笔记已重命名为 "${newTitle}"`);
      }
    } else if (action === '移动到') {
      const targetNotebookId = await showMoveToDialog(noteId, note.title, props.notebookList);

      if (targetNotebookId) {
        // 【API调用点 F】: 移动笔记 (PUT /noting/notes/move)
        // 假设 moveNote API 返回更新后的笔记对象或成功指示
        // 在实际应用中，您可能需要重新获取目标笔记本的笔记列表
        await moveNote(noteId, targetNotebookId);
        noteList.value = noteList.value.filter(n => n.id !== noteId);

        if (currentNote.value && currentNote.value.id === noteId) {
          currentNote.value = null;
          currentNoteType.value = null;
          if (noteList.value.length > 0) selectNote(noteList.value[0]);
        }

        showSuccess(`笔记 "${note.title}" 已成功移动到目标笔记本。`);
      }
    } else if (action === '下载') {
      const fileName = note.filename;
      if (!fileName) {
        showError(`笔记 "${note.title}" 缺少文件名信息，无法下载。`);
        return;
      }

      try {
        // 1. 获取 MinIO 下载链接
        const downloadUrl = await getFileUrl(fileName);
        if (!downloadUrl) {
          throw new Error('未能获取到下载链接。');
        }

        // 2. 构造下载的文件名
        const fileExtension = note.fileType ? `.${note.fileType.toLowerCase()}` : '';
        const downloadName = note.title.endsWith(fileExtension)
            ? note.title
            : `${note.title}${fileExtension}`;

        // --- 核心修改开始 ---

        // 3. 使用 fetch 请求文件流 (Blob)
        // 这会把文件内容下载到内存中，而不是让浏览器去导航
        const response = await fetch(downloadUrl);

        if (!response.ok) {
          throw new Error(`下载失败: ${response.statusText}`);
        }

        const blob = await response.blob();

        // 4. 创建一个指向内存中 Blob 的临时 URL
        const blobUrl = window.URL.createObjectURL(blob);

        // 5. 创建临时链接并触发下载
        const link = document.createElement('a');
        link.href = blobUrl;
        link.download = downloadName; // 这里设置文件名在 Blob 模式下一定生效

        document.body.appendChild(link);
        link.click();

        // 6. 清理资源
        document.body.removeChild(link);
        window.URL.revokeObjectURL(blobUrl); // 释放内存

        // --- 核心修改结束 ---

        console.log(`Note ${noteId} downloaded via Blob: ${downloadName}`);
        // alert(`笔记 "${note.title}" 下载已完成。`);

      } catch (error) {
        console.error('下载出错:', error);
        showError('下载失败，可能是跨域限制或网络问题，请检查控制台。');
      }
    } else if (action === '删除') {
      // **调用自定义弹窗，并等待 Promise 结果**
      const isConfirmed = await showDeleteDialog(noteId, note.title);

      // 检查 Promise 返回的布尔值
      if (isConfirmed) {
        // isConfirmed === true，表示用户点击了“确定删除”
        // 【API调用点 E】: 删除笔记 (DELETE /noting/notes)
        await deleteNote(noteId);
        const deletedId = noteId;
        noteList.value = noteList.value.filter(n => n.id !== noteId);

        if (currentNote.value && currentNote.value.id === deletedId) {
          currentNote.value = null;
          currentNoteType.value = null;
          // 删除后默认选中第一个
          if (noteList.value.length > 0) selectNote(noteList.value[0]);
        }
      }
    }
  } catch (error) {
    showError(`${action}操作失败，请稍后重试。`);
    console.error(`Error during ${action}:`, error);
  }
};

const handleNewNoteFromModal = async () => {
  const title = newNoteTitle.value.trim();
  if (!title) {
    showError('笔记名不能为空！');
    return;
  }
  const type = newNoteType.value === 'online' ? 'md' : 'pdf';
  showNewNoteModal.value = false;
  newNoteType.value = 'online';

  try {
    if (type === 'md') {
      // 1. 构造内容为空的 Blob 对象
      const emptyContent = '在此处编辑';
      const mimeType = 'text/markdown'; // 明确指定 MIME Type
      const blob = new Blob([emptyContent], { type: mimeType });
      // 2. 将 Blob 包装成 File 对象，并
      const file = new File([blob], `${title}.md`, { type: mimeType });

      const meta = {
        title: title,
        notebookId: props.notebookId,
        fileType: 'md'
      };

      // 【API调用点 G】: 创建新的 MD 笔记 (POST /noting/notes)
      const newNote = await createNote(meta, file);
      noteList.value.unshift(newNote); // 在列表前插入新笔记
      selectNote(newNote); // 选中新笔记
      newNoteTitle.value = '新建笔记'; // 重置
      showSuccess(`富文本笔记 "${title}" 创建成功。`);

    } else if (type === 'pdf') {
      // 【API调用点 H】: 触发文件上传
      uploadFileInput.value.accept = '.pdf';
      uploadFileInput.value.click();
    }
  } catch (error) {
    showError('创建笔记失败。');
    console.error('Error creating new note:', error);
  }
};

const handleFileUpload = async (e) => {
  const file = e.target.files[0];
  if (!file) return;

  const titleToUse = newNoteTitle.value.trim() || file.name.split('.').slice(0, -1).join('.');

  try {
    // 1. 确定文件类型
    let fileType = file.type;
    if (fileType.includes('/')) {
      fileType = fileType.split('/').pop().toLowerCase();
    } else {
      fileType = fileType.toLowerCase();
    }
    if (!fileType) fileType = 'unknown';

    const meta = {
      title: titleToUse,
      notebookId: props.notebookId,
      fileType: fileType,
    };

    const fileNote = await uploadNote(meta, file);
    noteList.value.unshift(fileNote);
    selectNote(fileNote);
    newNoteTitle.value = fileNote.title;

  } catch (error) {
    showError('文件上传和笔记创建失败。');
    console.error('Error uploading file/creating note:', error);
  }
};

const updateCurrentNoteTitle = async () => {
  if (!currentNote.value) return;

  // 标题不变动或为空则不进行 API 调用
  if (currentNote.value.title === currentTitle.value || currentTitle.value.trim() === '') return;

  try {
    const newTitle = currentTitle.value;
    const updateResult = await renameNote(currentNote.value.id, newTitle);
    currentNote.value.title = newTitle;
    currentNote.value.updatedAt = updateResult.updatedAt;
  } catch (error) {
    console.error('Error updating title:', error);
    currentTitle.value = currentNote.value.title;
  }
};

const triggerImageUpload = () => {
  if (currentNoteType.value === 'md') {
    fileInput.value.click();
  } else {
    showError('请先选择一个富文本笔记进行编辑。');
  }
  closeAllDropdowns();
};

const handleImageUpload = async (e) => {
  const file = e.target.files[0];
  if (!file || !editor.value) return;

  await insertImage(file);
  
  // 清空 input，防止无法连续上传同一张图
  e.target.value = null;
};

// 处理粘贴的图片
const handlePastedImage = async (file) => {
  if (!file) {
    console.warn('粘贴的文件无效')
    return;
  }
  
  if (!editor.value) {
    console.warn('编辑器未初始化')
    showError('编辑器未准备好，请稍后再试')
    return;
  }
  
  console.log('开始处理粘贴的图片，文件大小:', file.size, '文件类型:', file.type)
  await insertImage(file);
};

// 统一的图片插入函数
const insertImage = async (file) => {
  if (!editor.value) {
    console.warn('编辑器未初始化')
    return;
  }

  try {
    console.log('开始上传图片...')
    // 【API调用点 J】: 上传图片并获取 URL (POST /noting/notes/image)
    const imageUrl = await uploadImage(file);
    console.log('图片上传成功，URL:', imageUrl)

    if (!imageUrl) {
      throw new Error('图片上传失败，未返回 URL')
    }

    // 获取图片的原始尺寸（使用 Promise 包装）
    const getImageDimensions = (url) => {
      return new Promise((resolve) => {
        const img = new Image();
        img.crossOrigin = 'anonymous'; // 允许跨域加载
        let resolved = false;
        
        img.onload = () => {
          if (!resolved) {
            resolved = true;
            console.log('图片尺寸获取成功:', img.naturalWidth, 'x', img.naturalHeight)
            resolve({
              width: img.naturalWidth,
              height: img.naturalHeight
            });
          }
        };
        img.onerror = () => {
          if (!resolved) {
            resolved = true;
            // 如果无法加载图片尺寸，返回 null（不阻塞插入）
            console.warn('无法获取图片尺寸，将使用默认尺寸');
            resolve(null);
          }
        };
        // 设置超时，避免长时间等待
        setTimeout(() => {
          if (!resolved) {
            resolved = true;
            console.warn('获取图片尺寸超时');
            resolve(null);
          }
        }, 3000);
        img.src = url;
      });
    };

    // 等待图片尺寸加载完成（最多等待3秒）
    const dimensions = await getImageDimensions(imageUrl);
    
    // 插入图片，设置合理的默认尺寸
    const imageAttrs = { src: imageUrl };
    
    // 设置最大显示宽度（可以根据需要调整）
    const MAX_DISPLAY_WIDTH = 800; // 最大显示宽度 800px
    const MAX_DISPLAY_HEIGHT = 600; // 最大显示高度 600px
    
    if (dimensions && dimensions.width && dimensions.height) {
      // 计算缩放后的尺寸，保持宽高比
      let displayWidth = dimensions.width;
      let displayHeight = dimensions.height;
      
      // 如果宽度超过最大宽度，按比例缩放
      if (displayWidth > MAX_DISPLAY_WIDTH) {
        const ratio = MAX_DISPLAY_WIDTH / displayWidth;
        displayWidth = MAX_DISPLAY_WIDTH;
        displayHeight = Math.round(displayHeight * ratio);
      }
      
      // 如果高度仍然超过最大高度，再次按比例缩放
      if (displayHeight > MAX_DISPLAY_HEIGHT) {
        const ratio = MAX_DISPLAY_HEIGHT / displayHeight;
        displayHeight = MAX_DISPLAY_HEIGHT;
        displayWidth = Math.round(displayWidth * ratio);
      }
      
      imageAttrs.width = displayWidth;
      imageAttrs.height = displayHeight;
      
      console.log(`图片尺寸: 原始 ${dimensions.width}x${dimensions.height}, 显示 ${displayWidth}x${displayHeight}`)
    } else {
      // 如果无法获取尺寸，使用默认尺寸
      imageAttrs.width = MAX_DISPLAY_WIDTH;
      imageAttrs.height = MAX_DISPLAY_HEIGHT;
      console.log('使用默认图片尺寸:', MAX_DISPLAY_WIDTH, 'x', MAX_DISPLAY_HEIGHT)
    }
    
    console.log('准备插入图片，属性:', imageAttrs)
    // 插入图片到编辑器
    editor.value.chain().focus().setImage(imageAttrs).run();
    
    // 验证图片是否插入成功
    const htmlContent = editor.value.getHTML();
    const hasImage = htmlContent.includes(imageUrl);
    console.log('图片插入成功，HTML 中包含图片:', hasImage)
    console.log('当前编辑器 HTML 内容:', htmlContent.substring(0, 500))
      
  } catch (error) {
    console.error('Error uploading image:', error);
    showError('图片上传失败：' + (error.message || '请稍后重试'));
    throw error; // 重新抛出错误以便调用者处理
  }
};

// ... 其他编辑器/UI相关函数 ...
const changeHeading = (event) => {
  const level = parseInt(event.target.value);
  if (level === 0) {
    editor.value.chain().focus().setParagraph().run();
  } else {
    editor.value.chain().focus().toggleHeading({ level }).run();
  }
};

const toggleInsertMenu = () => showInsertMenu.value = !showInsertMenu.value;

</script>

<style scoped>
/* ================================================= */
/* ============= 布局和滚动容器样式 ============= */
/* ================================================= */
.editor-layout {
  display: flex;
  height: 100vh;
  width: 100vw;
  background-color: #fff; /* 整体白色背景 */
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  color: #333;
}

.sidebar {
  width: 300px; /* 侧边栏宽度略窄 */
  background-color: #f7f7f7; /* 侧边栏浅灰色背景 */
  border-right: 1px solid #e0e0e0;
  display: flex;
  flex-direction: column;
}

.editor-main {
  flex: 1; /* 占据剩余空间 */
  display: flex;
  flex-direction: column;
  background-color: #fff;
}

/* --- 侧边栏顶部 (笔记本标题) --- */

/* 删除了 .search-bar-wrapper 和 .search-input-box 相关的样式 */

.dropdown-menu {
  position: absolute;
  top: 100%;
  left: 0;
  margin-top: 6px;
  background: white;
  border: 1px solid #eee;
  border-radius: 8px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.1);
  z-index: 200;
  min-width: 140px;
  padding: 4px 0;
  overflow: hidden;
}

.dropdown-menu .menu-item {
  padding: 8px 12px;
  cursor: pointer;
  font-size: 14px;
  color: #333;
}

.dropdown-menu .menu-item:hover {
  background: #f0f0f0;
}

.notebook-header.new-style {
  padding: 10px 16px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid #e0e0e0;
  background: #fff; /* 笔记本标题区白色背景 */
}

.notebook-header.new-style .header-left {
  display: flex;
  align-items: center;
  min-width: 0;
}

.back-btn {
  background: transparent;
  border: none;
  cursor: pointer;
  color: #666;
  margin-right: 10px;
  width: 24px;
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.back-btn:hover {
  color: #333;
}

.notebook-title {
  font-size: 16px;
  font-weight: 600;
  margin: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  color: #333;
}

.add-note-btn {
  background: #4c7cff;
  border: none;
  border-radius: 50%;
  color: white;
  width: 30px;
  height: 30px;
  font-size: 20px;
  line-height: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  transition: background-color 0.2s;
}

.add-note-btn:hover {
  background: #3a68e0;
}

/* --- 笔记列表 --- */
.note-list-container {
  flex: 1; /* 占据剩余高度 */
  overflow-y: auto; /* 允许笔记列表独立滚动 */
  padding: 4px 0;
  min-height: 0; /* 【关键强化】防止 flex item 因内容过多而溢出 */
}

.list-loading-state, .list-empty-state {
  padding: 20px;
  text-align: center;
  color: #999;
  font-style: italic;
  font-size: 14px;
}

.note-list.new-style {
  list-style: none;
  padding: 0;
  margin: 0;
}

.note-list.new-style li {
  padding: 10px 16px;
  cursor: pointer;
  border-bottom: 1px solid #f0f0f0;
  transition: background-color 0.1s;
  min-height: 50px;
  box-sizing: border-box;
  display: flex;
  align-items: center;
  position: relative;
}

.note-list.new-style li:hover {
  background: #f0f0f0;
}

.note-list.new-style li.active {
  background: #eef2ff;
}

.note-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

/* 优化后的元数据样式（每行独占一行） */
.note-info {
  flex: 1;
  min-width: 0;
  padding-left: 12px;
}

.note-title {
  font-size: 15px;
  font-weight: 500;
  margin-bottom: 2px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.note-meta-new-style {
  font-size: 11px;
  color: #999;
}

.note-meta-new-style .meta-line {
  margin: 0;
  padding: 0;
  line-height: 1.2;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.note-meta {
  font-size: 12px;
  color: #999;
}

.file-icon {
  font-size: 18px;
  color: #4c7cff;
}

.menu-wrapper {
  position: relative;
}

.actions-menu-btn {
  opacity: 0;
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 4px;
}

.note-list.new-style li:hover .actions-menu-btn {
  opacity: 1;
}

.actions-menu-btn:hover {
  background: #d9e2ff;
  color: #4c7cff;
}

.note-actions-menu {
  right: 0;
  left: auto;
}

.note-actions-menu .delete-item {
  color: #e53e3e;
}

.note-actions-menu .delete-item:hover {
  background: #fbecec;
}

.menu-divider {
  border: none;
  border-top: 1px solid #eee;
  margin: 4px 0;
}

/* --- 右侧内容区 (预览和编辑) --- */
.empty-state {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: #999;
}

.empty-message {
  font-size: 24px;
  font-weight: 300;
  margin-bottom: 10px;
}

.empty-tip {
  font-size: 14px;
}

/* --- 编辑器头部 (标题和保存) --- */
.editor-container {
  display: flex;
  flex-direction: column;
  height: 100%; /* 确保占据 editor-main 的全部高度 */
}

.editor-header {
  padding: 15px 30px;
  border-bottom: 1px solid #eee;
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: #fff;
}

.title-input {
  flex: 1;
  border: none;
  font-size: 20px;
  font-weight: 700;
  outline: none;
  padding: 0;
  margin-right: 20px;
  color: #333;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 15px;
}

.save-status {
  font-size: 13px;
  color: #888;
}

.save-btn {
  padding: 8px 15px;
  background: #4c7cff;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: background-color 0.2s;
  margin-left: 8px;
}

.save-btn:hover {
  background: #3a68e0;
}

.publish-btn {
  padding: 8px 15px;
  background: #10b981;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: background-color 0.2s;
  margin-left: 8px;
}

.publish-btn:hover:not(:disabled) {
  background: #059669;
}

.publish-btn:disabled {
  background: #9ca3af;
  cursor: not-allowed;
  opacity: 0.6;
}

/* --- TipTap 工具栏 --- */
.tiptap-wrapper {
  flex: 1; /* 占据 header 以外的剩余高度 */
  display: flex;
  flex-direction: column;
  min-height: 0; /* 【关键强化】防止 flex item 因内容过多而溢出 */
}

/* 【样式优化点 2】: 工具栏样式 */
.tiptap-toolbar {
  display: flex;
  align-items: center;
  padding: 6px 12px;
  border-bottom: 1px solid #e0e0e0; /* 略深一点的边框 */
  background: #f5f5f5; /* 略微灰色背景 */
  flex-wrap: wrap;
  gap: 8px; /* 增加组间距 */
  position: sticky; /* 粘性定位，如果顶部有导航栏，可避免滚动时工具栏消失 */
  top: 0;
  z-index: 10;
}

.tiptap-toolbar button {
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 4px;
  border: 1px solid transparent;
  border-radius: 4px;
  background: transparent;
  cursor: pointer;
  color: #555;
  transition: all 0.2s;
}

.tiptap-toolbar button:hover {
  background: #e0e0e0;
  color: #333;
}

.tiptap-toolbar button.is-active {
  background: #d9e2ff; /* 浅蓝色背景 */
  border-color: #4c7cff;
  color: #4c7cff;
}

.toolbar-group {
  display: flex;
  gap: 4px;
  align-items: center;
}

.divider {
  width: 1px;
  height: 20px;
  background: #ccc;
  margin: 0 4px;
}

.toolbar-select {
  padding: 5px 8px;
  border: 1px solid #ddd;
  border-radius: 4px;
  background: #fff;
  font-size: 14px;
  cursor: pointer;
}

/* 【样式优化点 3】: 插入按钮样式 */
.insert-pill-btn {
  padding: 4px 10px;
  height: 30px;
  border: 1px solid #4c7cff;
  border-radius: 15px;
  background: #4c7cff;
  color: white;
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  font-size: 14px;
  transition: background-color 0.2s;
  writing-mode: horizontal-tb;
  text-orientation: mixed;
  direction: ltr;
  white-space: nowrap;
  min-width: fit-content;
}

.insert-pill-btn:hover {
  background: #3a68e0;
}

.insert-pill-btn .plus-icon {
  font-size: 14px;
  margin-right: 4px;
  line-height: 1;
}

.insert-pill-btn .arrow-icon {
  font-size: 8px;
  margin-left: 4px;
  transform: translateY(1px);
}

.insert-menu {
  left: 50%;
  transform: translateX(-50%);
  min-width: 120px;
}

.insert-menu .menu-item {
  writing-mode: horizontal-tb;
  text-orientation: mixed;
  direction: ltr;
}

.insert-menu .emoji {
  margin-right: 8px;
  font-size: 16px;
}

.color-picker-wrapper {
  position: relative;
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid transparent;
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s;
}

.color-picker-wrapper:hover {
  background: #e0e0e0;
}

.color-picker-wrapper .color-input {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  opacity: 0;
  cursor: pointer;
}

/* --- TipTap 内容区 --- */
.tiptap-content {
  flex: 1; /* 占据 toolbar 以外的剩余高度 */
  overflow-y: auto; /* 允许内容独立滚动 */
  padding: 30px 50px;
  outline: none;
  cursor: text;
  min-height: 0; /* 【关键强化】防止 flex item 因内容过多而溢出 */
  /* 【新增】确保内容居中，并限制最大宽度以提高可读性 */
  display: flex;
  justify-content: center;
}

:deep(.ProseMirror) {
  outline: none;
  min-height: 100%;
  cursor: text;
  /* 【新增】限制编辑内容的最大宽度，使其居中显示，提高美观度 */
  max-width: 100%; /* 优化可读性的标准宽度 */
  width: 100%; /* 允许在 max-width 内自适应 */
  padding-bottom: 50px; /* 底部留白 */
  line-height: 1.6; /* 提高行高 */
  font-size: 16px;
  color: #333;
}

/* TipTap 元素默认样式覆盖 */
:deep(h1) { font-size: 2em; margin-top: 1em; margin-bottom: 0.5em; }
:deep(h2) { font-size: 1.5em; margin-top: 1em; margin-bottom: 0.5em; }
:deep(h3) { font-size: 1.17em; margin-top: 1em; margin-bottom: 0.5em; }

:deep(ul), :deep(ol) { padding-left: 1.5em; margin-top: 0.5em; margin-bottom: 0.5em; }

:deep(pre) {
  background: #2d2d2d;
  color: #ccc;
  padding: 10px;
  border-radius: 6px;
  overflow-x: auto;
  font-size: 14px;
  margin: 1em 0;
  white-space: pre-wrap;
}

:deep(code) {
  font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
  padding: 2px 4px;
  background-color: #f0f0f0;
  border-radius: 4px;
  font-size: 0.9em;
}

:deep(pre code) {
  padding: 0;
  background: none;
  border-radius: 0;
}

:deep(li[data-type="taskItem"]) {
  display: flex;
  align-items: flex-start;
  margin-bottom: 6px;
}

:deep(li[data-type="taskItem"] label) {
  margin-right: 8px;
  user-select: none;
}

:deep(li[data-type="taskItem"] input[type="checkbox"]) {
  margin-top: 0.5em; /* 调整位置 */
  margin-right: 8px;
}

:deep(.ProseMirror img) {
  max-width: 100%;
  height: auto;
  border-radius: 6px;
  display: block;
  margin: 10px auto; /* 图片居中 */
  box-shadow: 0 2px 8px rgba(0,0,0,0.1);
}

/* 图片包装器样式 */
:deep(.image-wrapper) {
  display: inline-block;
  position: relative;
  max-width: 100%;
  margin: 10px auto;
  text-align: center;
}

:deep(.image-wrapper:hover .image-resize-handle) {
  opacity: 1;
}

/* 调整大小控制点样式 */
:deep(.image-resize-handle) {
  position: absolute;
  bottom: 0;
  right: 0;
  width: 16px;
  height: 16px;
  background: #4c7cff;
  border: 2px solid white;
  border-radius: 50%;
  cursor: nwse-resize;
  opacity: 0;
  transition: opacity 0.2s;
  z-index: 10;
  box-shadow: 0 2px 4px rgba(0,0,0,0.2);
}

:deep(.image-resize-handle:hover) {
  background: #3a68e0;
  transform: scale(1.2);
}

/* --- PDF 预览样式 --- */
.file-preview-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  height: 100%; /* 确保占据 editor-main 的全部高度 */
}

.file-preview-header {
  padding: 15px 30px;
  border-bottom: 1px solid #eee;
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: #fff;
}

.file-title {
  font-size: 20px;
  font-weight: 700;
  margin: 0;
  color: #333;
}

.download-btn {
  padding: 8px 15px;
  background: #4c7cff;
  color: white;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 14px;
  transition: background-color 0.2s;
}

.download-btn:hover {
  background: #3a68e0;
}

.file-content {
  flex: 1; /* 占据剩余高度 */
  overflow-y: auto; /* 允许文件内容独立滚动 */
  padding: 20px;
  background: #f0f0f0;
  text-align: center;
  color: #666;
  font-style: italic;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: flex-start; /* 从顶部开始对齐 */
  min-height: 0; /* 【关键强化】防止 flex item 因内容过多而溢出 */
  /* 确保PDF内容不被头部栏遮挡 */
  scroll-padding-top: 0;
}

.pdf-wrapper {
  width: 100%;
  display: flex;
  justify-content: center;
  align-items: flex-start;
  /* 确保PDF内容有足够的顶部间距，避免被头部栏遮挡 */
  padding-top: 0;
}

.pdf-embed-viewer {
  max-width: 100%;
  height: auto;
  margin: 0 auto;
  display: block;
  /* 确保PDF内容不被遮挡 */
  position: relative;
  z-index: 1;
}

/* ================================================= */
/* ============= 模态框/弹窗样式 ================= */
/* ================================================= */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}

.new-note-modal, .rename-dialog, .delete-dialog {
  background: white;
  padding: 30px;
  border-radius: 12px;
  box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
  width: 90%;
  max-width: 450px;
}

.modal-title {
  margin-top: 0;
  margin-bottom: 25px;
  font-size: 22px;
  font-weight: 600;
}

.modal-input-group {
  margin-bottom: 20px;
}

.modal-input-group label {
  display: block;
  font-size: 14px;
  color: #666;
  margin-bottom: 8px;
  font-weight: 500;
}

.creation-options {
  display: flex;
  gap: 10px;
}

.creation-btn {
  flex: 1;
  padding: 10px 15px;
  border: 1px solid #ddd;
  border-radius: 6px;
  background: #fff;
  cursor: pointer;
  font-size: 14px;
  transition: all 0.2s;
}

.creation-btn:hover {
  border-color: #a0a0a0;
}

.creation-btn.active {
  background: #4c7cff;
  color: white;
  border-color: #4c7cff;
}

.modal-input {
  width: 100%;
  padding: 10px 12px;
  box-sizing: border-box;
  border: 1px solid #ddd;
  border-radius: 6px;
  font-size: 15px;
  transition: border-color 0.2s;
}

.modal-input:focus {
  border-color: #4c7cff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(76, 124, 255, 0.1);
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
}

.modal-cancel-btn, .modal-confirm-btn {
  padding: 10px 20px;
  border: none;
  border-radius: 6px;
  cursor: pointer;
  font-size: 15px;
  transition: background-color 0.2s;
}

.modal-cancel-btn {
  background: #f0f0f0;
  color: #666;
}

.modal-cancel-btn:hover {
  background: #e0e0e0;
}

.modal-confirm-btn {
  background: #4c7cff;
  color: white;
}

.modal-confirm-btn:hover:not(:disabled) {
  background: #3a68e0;
}

.modal-confirm-btn:disabled {
  background: #ccc;
  cursor: not-allowed;
}

.delete-dialog .modal-confirm-btn {
  background: #e53e3e;
}

.delete-dialog .modal-confirm-btn:hover:not(:disabled) {
  background: #c53030;
}

.delete-message {
  font-size: 15px;
  line-height: 1.5;
  margin-bottom: 25px;
  color: #333;
}
</style>