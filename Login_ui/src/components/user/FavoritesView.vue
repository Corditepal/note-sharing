                                                                                                                                                                                                        <template>
  <div class="favorites-page">
    <section class="favorites-hero">
      <div>
        <p class="section-label">收藏中心</p>
        <div class="hero-title">
          <h2>我的收藏</h2>
          <span>Favorites</span>
        </div>
        <p class="hero-desc">
          汇总所有收藏的笔记，支持按照标题快速搜索、移动到不同收藏夹、移出收藏等操作，
          仅更新前端样式，不更改原有业务逻辑。
        </p>
      </div>
      <div class="hero-stats">
        <div class="stat-card">
          <p>收藏总数</p>
          <strong>{{ filteredFavorites.length }}</strong>
        </div>
        <div class="stat-card subtle">
          <p>收藏夹</p>
          <strong>{{ favoriteFolders.length }}</strong>
        </div>
      </div>
    </section>

    <div class="favorites-layout">
      <aside class="folder-panel">
        <header>
          <div>
            <p class="section-label">Folder</p>
            <h3>收藏夹</h3>
          </div>
          <button class="ghost-btn" type="button">+ 新建收藏夹</button>
        </header>

        <ul>
          <li>
            <button
              type="button"
              :class="['folder-item', { active: activeFolder === 'all' }]"
              @click="activeFolder = 'all'"
            >
              <div>
                <strong>全部收藏</strong>
                <p>{{ favorites.length }} 条笔记</p>
              </div>
              <span class="chip">ALL</span>
            </button>
          </li>
          <li v-for="folder in favoriteFolders" :key="folder.id">
            <button
              type="button"
              :class="['folder-item', { active: activeFolder === folder.id }]"
              @click="activeFolder = folder.id"
            >
              <div>
                <strong>{{ folder.name }}</strong>
                <p>{{ folder.count }} 条内容</p>
              </div>
              <span class="chip">{{ folder.short }}</span>
            </button>
          </li>
        </ul>
      </aside>

      <section class="favorites-panel">
        <div class="panel-toolbar">
          <div class="search-input">
            <span aria-hidden="true">🔍</span>
            <input
              v-model="searchKeyword"
              type="text"
              placeholder="按照笔记标题搜索..."
            />
            <button type="button" class="text-link">清空</button>
          </div>
          <div class="toolbar-actions">
            <button type="button" class="ghost-btn">批量操作</button>
            <button type="button" class="ghost-btn">导出收藏</button>
          </div>
        </div>

        <div v-if="filteredFavorites.length" class="favorite-list">
          <article
            v-for="favorite in filteredFavorites"
            :key="favorite.id"
            class="favorite-card"
          >
            <header>
              <div>
                <p class="favorite-type">{{ favorite.folderName }}</p>
                <h4>{{ favorite.title }}</h4>
              </div>
              <span class="badge">{{ favorite.tag }}</span>
            </header>

            <p class="favorite-desc">{{ favorite.preview }}</p>

            <footer>
              <div class="meta">
                <span>所属笔记本：{{ favorite.notebook }}</span>
                <span>创建者：{{ favorite.author }}</span>
                <span>收藏时间：{{ favorite.collectedAt }}</span>
              </div>
              <div class="actions">
                <label class="move-select">
                  <span>移动至</span>
                  <select v-model="favorite.targetFolder" @change="handleMoveFavorite(favorite)">
                    <option disabled value="">选择收藏夹</option>
                    <option
                      v-for="folder in favoriteFolders"
                      :key="folder.id"
                      :value="folder.id"
                    >
                      {{ folder.name }}
                    </option>
                  </select>
                </label>
                <button type="button" class="text-link" @click="handleRemoveFavorite(favorite.id)">
                  移出收藏
                </button>
              </div>
            </footer>
          </article>
        </div>

        <div v-else class="favorite-empty">
          <p>没有符合条件的收藏</p>
          <small>试试调整搜索关键词或切换收藏夹</small>
        </div>
      </section>
    </div>
  </div>
</template>

<script setup>
import { computed, ref } from 'vue'

/**
 * API 占位信息（仅前端展示）
 * - GET /api/favorites
 *   输入: 无
 *   输出: { code: number, data: Favorite[] }
 *   返回码: 200 成功 / 401 未登录 / 500 服务器异常
 * - PATCH /api/favorites/{id}
 *   输入: { folderId: string }
 *   输出: { code: number, message: string }
 *   返回码: 200 更新成功 / 400 参数错误 / 404 收藏不存在
 * - DELETE /api/favorites/{id}
 *   输入: 无
 *   输出: { code: number, message: string }
 *   返回码: 200 删除成功 / 404 收藏不存在
 */

const favoriteFolders = ref([
  { id: 'default', name: '默认收藏', short: 'DEF', count: 12 },
  { id: 'study', name: '学习沉淀', short: 'STU', count: 6 },
  { id: 'project', name: '项目灵感', short: 'PRJ', count: 4 },
  { id: 'share', name: '分享材料', short: 'SHR', count: 3 }
])

const favorites = ref([
  {
    id: 'fav-1',
    title: '高效学习方法论',
    folderId: 'study',
    folderName: '学习沉淀',
    tag: '学习',
    notebook: '知识卡片',
    author: '李四',
    collectedAt: '2025-11-02',
    preview: '包含拆解任务、刻意练习、复盘提升等步骤的学习指南。',
    targetFolder: ''
  },
  {
    id: 'fav-2',
    title: '系统设计十问',
    folderId: 'project',
    folderName: '项目灵感',
    tag: '系统',
    notebook: '后端架构',
    author: '张伟',
    collectedAt: '2025-10-18',
    preview: '覆盖扩展性、容错、缓存、存储等常见问题，适用于面试或方案讨论。',
    targetFolder: ''
  },
  {
    id: 'fav-3',
    title: 'Notion 快速排版模板',
    folderId: 'share',
    folderName: '分享材料',
    tag: '模板',
    notebook: '效率工具',
    author: '王晴',
    collectedAt: '2025-10-08',
    preview: '内置 6 套常用排版模块：项目追踪、会议纪要、知识沉淀……随取随用。',
    targetFolder: ''
  }
])

const searchKeyword = ref('')
const activeFolder = ref('all')

const filteredFavorites = computed(() => {
  return favorites.value.filter((fav) => {
    const matchFolder = activeFolder.value === 'all' || fav.folderId === activeFolder.value
    const matchKeyword = fav.title.toLowerCase().includes(searchKeyword.value.toLowerCase().trim())
    return matchFolder && matchKeyword
  })
})

const handleMoveFavorite = (favorite) => {
  if (!favorite.targetFolder) return
  // TODO: 调用 PATCH /api/favorites/{id} 更新收藏夹
  const folder = favoriteFolders.value.find((item) => item.id === favorite.targetFolder)
  if (folder) {
    favorite.folderName = folder.name
    favorite.folderId = folder.id
  }
  favorite.targetFolder = ''
}

const handleRemoveFavorite = (id) => {
  // TODO: 调用 DELETE /api/favorites/{id} 移出收藏
  const index = favorites.value.findIndex((fav) => fav.id === id)
  if (index !== -1) {
    favorites.value.splice(index, 1)
  }
}
</script>

<style scoped>
.favorites-page {
  display: flex;
  flex-direction: column;
  gap: 28px;
  padding: clamp(16px, 3vw, 36px);
}

.favorites-hero {
  background: var(--surface-base);
  border-radius: var(--radius-lg);
  padding: 32px clamp(20px, 4vw, 48px);
  border: 1px solid var(--line-soft);
  box-shadow: var(--shadow-card);
  display: flex;
  justify-content: space-between;
  gap: 24px;
  flex-wrap: wrap;
}

.hero-title {
  display: flex;
  align-items: center;
  gap: 16px;
  margin: 8px 0;
}

.hero-title span {
  font-size: 14px;
  letter-spacing: 0.4em;
  color: var(--text-muted);
  text-transform: uppercase;
}

.hero-desc {
  max-width: 620px;
}

.hero-stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
  flex: 1;
}

.stat-card {
  border-radius: var(--radius-md);
  border: 1px solid var(--line-soft);
  padding: 16px 18px;
  background: var(--surface-soft);
}

.stat-card p {
  font-size: 12px;
  letter-spacing: 0.2em;
  color: var(--text-muted);
  margin-bottom: 6px;
}

.stat-card strong {
  font-size: 26px;
}

.stat-card.subtle {
  background: rgba(47, 125, 255, 0.08);
  border-color: rgba(47, 125, 255, 0.2);
}

.favorites-layout {
  display: grid;
  grid-template-columns: 280px 1fr;
  gap: 24px;
}

.folder-panel,
.favorites-panel {
  background: var(--surface-base);
  border-radius: var(--radius-lg);
  border: 1px solid var(--line-soft);
  box-shadow: var(--shadow-soft);
}

.folder-panel {
  padding: 24px;
}

.folder-panel header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 18px;
  gap: 12px;
}

.folder-panel h3 {
  margin-top: 6px;
  font-size: 18px;
}

.folder-panel ul {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.folder-item {
  width: 100%;
  border: 1px solid transparent;
  border-radius: var(--radius-md);
  padding: 16px;
  background: var(--surface-soft);
  text-align: left;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  transition: border-color 0.2s ease, box-shadow 0.2s ease;
}

.folder-item strong {
  font-size: 15px;
  color: var(--text-strong);
}

.folder-item p {
  font-size: 13px;
  color: var(--text-muted);
}

.folder-item.active {
  border-color: var(--brand-primary);
  background: #fff;
  box-shadow: 0 12px 36px rgba(34, 191, 163, 0.18);
}

.chip {
  font-size: 11px;
  letter-spacing: 0.2em;
  color: var(--text-muted);
}

.favorites-panel {
  padding: 28px;
  display: flex;
  flex-direction: column;
  gap: 20px;
}

.panel-toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: center;
  justify-content: space-between;
}

.search-input {
  flex: 1;
  min-width: 220px;
  display: flex;
  align-items: center;
  gap: 10px;
  border: 1px solid var(--line-soft);
  border-radius: 999px;
  padding: 0 16px;
  background: var(--surface-soft);
}

.search-input input {
  flex: 1;
  border: none;
  background: transparent;
  padding: 12px 0;
  font-size: 15px;
  color: var(--text-strong);
}

.search-input input:focus {
  outline: none;
}

.toolbar-actions {
  display: flex;
  gap: 10px;
}

.favorite-list {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.favorite-card {
  border-radius: var(--radius-lg);
  border: 1px solid var(--line-soft);
  padding: 22px;
  background: var(--surface-base);
  box-shadow: 0 16px 40px rgba(15, 23, 42, 0.06);
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.favorite-card header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.favorite-type {
  font-size: 12px;
  letter-spacing: 0.3em;
  color: var(--text-muted);
  margin-bottom: 6px;
}

.favorite-card h4 {
  margin: 0;
  font-size: 20px;
}

.badge {
  padding: 6px 14px;
  border-radius: 999px;
  border: 1px solid var(--brand-primary);
  color: var(--brand-primary);
  font-size: 13px;
}

.favorite-desc {
  color: var(--text-secondary);
}

.favorite-card footer {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
}

.meta {
  display: flex;
  flex-wrap: wrap;
  gap: 10px 20px;
  font-size: 13px;
  color: var(--text-muted);
}

.actions {
  display: flex;
  align-items: center;
  gap: 14px;
}

.move-select {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 14px;
  color: var(--text-secondary);
}

.move-select select {
  border: 1px solid var(--line-soft);
  border-radius: 12px;
  padding: 6px 10px;
  background: var(--surface-soft);
  font-size: 14px;
}

.favorite-empty {
  border: 1px dashed var(--line-soft);
  border-radius: var(--radius-lg);
  padding: 40px;
  text-align: center;
  color: var(--text-secondary);
}

.favorite-empty p {
  font-size: 18px;
  margin-bottom: 10px;
}

.ghost-btn {
  border-radius: 999px;
  border: 1px solid var(--line-soft);
  padding: 8px 18px;
  background: transparent;
  color: var(--text-secondary);
}

.ghost-btn:hover,
.text-link:hover {
  color: var(--brand-primary);
  border-color: var(--brand-primary);
}

.text-link {
  border: none;
  background: none;
  color: var(--brand-secondary);
  font-weight: 600;
  padding: 0;
}

@media (max-width: 1024px) {
  .favorites-layout {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 640px) {
  .favorites-hero,
  .folder-panel,
  .favorites-panel {
    border-radius: 24px;
    padding: 24px;
  }

  .favorite-card footer {
    flex-direction: column;
    align-items: flex-start;
  }
}
</style>

