<template>
  <section class="module-card">
    <div class="module-head">
      <div>
        <p class="section-label">Notes</p>
        <h2>笔记管理</h2>
      </div>
      <div class="module-actions multi">
        <div class="search-bar">
          <span aria-hidden="true">👤</span>
          <input
            v-model="noteSearch.user"
            type="text"
            placeholder="按笔记发布用户搜索"
          />
          <button type="button" class="text-link" @click="noteSearch.user = ''">清空</button>
        </div>
        <div class="search-bar">
          <span aria-hidden="true">#</span>
          <input
            v-model="noteSearch.tag"
            type="text"
            placeholder="按标签搜索"
          />
          <button type="button" class="text-link" @click="noteSearch.tag = ''">清空</button>
        </div>
      </div>
    </div>

    <div class="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>笔记ID</th>
            <th>发布用户</th>
            <th>所属空间 / 笔记本</th>
            <th>标签</th>
            <th>内容预览</th>
            <th class="actions-col">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="note in filteredNotes" :key="note.id">
            <td>{{ note.id }}</td>
            <td>{{ note.author }}</td>
            <td>{{ note.space }} / {{ note.notebook }}</td>
            <td>{{ note.tag }}</td>
            <td class="prism-text">{{ note.preview }}</td>
            <td class="actions-col">
              <button type="button" class="pill-btn primary" @click="viewNote(note)">查看</button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="showNotePreview" class="modal" @click.self="closeNotePreview">
      <div class="modal-card large">
        <h3>笔记预览 —— {{ activeNotePreview?.title }}</h3>
        <p class="muted">所属：{{ activeNotePreview?.space }} / {{ activeNotePreview?.notebook }}</p>
        <div class="note-preview">
          <p class="label">内容预览</p>
          <p>{{ activeNotePreview?.preview }}</p>
        </div>
        <div class="modal-actions">
          <button type="button" class="ghost-btn" @click="closeNotePreview">关闭</button>
          <button
            type="button"
            class="pill-btn primary"
            @click="simulateNavigateToNote"
          >
            跳转至笔记
          </button>
        </div>
      </div>
    </div>
  </section>
</template>

<script setup>
import { computed, ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()

/**
 * API 占位：
 * - GET /api/admin/notes?author=xxx&tag=xxx
 */
const noteSearch = ref({ user: '', tag: '' })
const notes = ref([
  {
    id: 'N001',
    title: '前端性能优化',
    author: '李华',
    space: '工作',
    notebook: '前端',
    tag: '性能',
    preview: '通过懒加载、资源分片与缓存策略提升应用体验……'
  },
  {
    id: 'N002',
    title: '后端架构演进',
    author: '王婷',
    space: '学习',
    notebook: '后端',
    tag: '架构',
    preview: '从单体应用到微服务，再到 Serverless 的演进过程总结……'
  }
])

const filteredNotes = computed(() => {
  const sorted = [...notes.value].sort((a, b) => a.id.localeCompare(b.id))
  return sorted.filter((note) => {
    const matchUser = noteSearch.value.user
      ? note.author.includes(noteSearch.value.user.trim())
      : true
    const matchTag = noteSearch.value.tag
      ? note.tag.includes(noteSearch.value.tag.trim())
      : true
    return matchUser && matchTag
  })
})

const showNotePreview = ref(false)
const activeNotePreview = ref(null)

const viewNote = (note) => {
  activeNotePreview.value = note
  showNotePreview.value = true
}

const closeNotePreview = () => {
  showNotePreview.value = false
  activeNotePreview.value = null
}

const simulateNavigateToNote = () => {
  // TODO: router.push({ path: '/main', query: { noteId: activeNotePreview.value?.id } })
  closeNotePreview()
}
</script>

<style scoped>
@import './shared-admin.css';
</style>

