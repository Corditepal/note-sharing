<template>
  <div class="search-page">
    <section class="search-panel">
      <header class="panel-header">
        <div>
          <p class="section-label">全局检索</p>
          <div class="title-row">
            <h2>查询笔记</h2>
            <span class="active-indicator">Search</span>
          </div>
          <p class="panel-subtext">
            支持笔记、笔记本、空间、标签与用户的统一搜索，输入关键词即可快速定位内容。
          </p>
        </div>
      </header>

      <div class="search-bar">
        <input
          v-model="searchQuery"
          type="text"
          placeholder="可输入笔记、笔记本、空间、标签、用户查询"
          @keyup.enter="handleSearch"
        />
        <button class="text-action" @click="handleSearch">
          <span>开始搜索</span>
          <span aria-hidden="true">↗</span>
        </button>
      </div>

      <div class="hint-row">
        <span>快速提示：</span>
        <ul>
          <li># 笔记</li>
          <li># 笔记本</li>
          <li># 空间</li>
          <li># 标签</li>
          <li># 用户</li>
        </ul>
      </div>
    </section>

    <section class="results-panel">
      <div v-if="loading" class="state-card">
        <span class="loader" aria-hidden="true"></span>
        <p>搜索中...</p>
        <small>正在为你定位匹配内容</small>
      </div>

      <div v-else-if="searchResults.length > 0" class="results-list">
        <article
          v-for="result in searchResults"
          :key="result.id"
          class="result-card"
        >
          <header class="result-head">
            <span class="result-type">{{ result.type }}</span>
            <span class="result-indicator" aria-hidden="true">↗</span>
          </header>
          <h3 class="result-title">{{ result.title }}</h3>
          <p class="result-info">{{ result.info }}</p>
        </article>
      </div>

      <div v-else-if="hasSearched" class="state-card">
        <p>暂无搜索结果</p>
        <small>尝试更换关键词或缩短描述</small>
      </div>

      <div v-else class="state-card placeholder">
        <p>在上方输入关键词开始搜索</p>
        <small>支持搜索：笔记标题、笔记本名称、空间名称、标签、用户名</small>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const searchQuery = ref('')
const searchResults = ref([])
const loading = ref(false)
const hasSearched = ref(false)

/**
 * API: 搜索笔记
 * POST /api/search
 * 输入: {
 *   query: string,  // 搜索关键词
 *   type: string    // 可选: 'note', 'notebook', 'space', 'tag', 'user', 'all'
 * }
 * 输出: {
 *   code: number,
 *   data: [
 *     {
 *       id: string,
 *       type: string,      // 'note', 'notebook', 'space', 'tag', 'user'
 *       title: string,     // 标题/名称
 *       info: string,      // 附加信息(如笔记所属笔记本等)
 *       content: string    // 笔记内容预览(仅笔记类型)
 *     }
 *   ]
 * }
 */
const handleSearch = async () => {
  if (!searchQuery.value.trim()) return
  
  loading.value = true
  hasSearched.value = true
  
  try {
    // 调用搜索API
    // const response = await fetch('/api/search', {
    //   method: 'POST',
    //   headers: { 'Content-Type': 'application/json' },
    //   body: JSON.stringify({ query: searchQuery.value, type: 'all' })
    // })
    // const result = await response.json()
    // searchResults.value = result.data
    
    // 模拟数据
    setTimeout(() => {
      searchResults.value = [
        { id: '1', type: '笔记', title: '示例笔记', info: '所属: 示例笔记本 > 示例空间' },
        { id: '2', type: '笔记本', title: '示例笔记本', info: '所属空间: 示例空间' }
      ]
      loading.value = false
    }, 500)
  } catch (error) {
    console.error('搜索失败:', error)
    loading.value = false
  }
}
</script>

<style scoped>
:global(:root) {
  --brand-primary: #22ee99;
  --surface-base: #ffffff;
  --surface-muted: #f7faf8;
  --line-soft: #e6ece8;
  --text-strong: #111c17;
  --text-secondary: #4b5a53;
  --text-muted: #90a19b;
}

.search-page {
  min-height: 100vh;
  padding: 60px 24px 100px;
  background: var(--surface-muted);
  display: grid;
  gap: 32px;
}

.search-panel,
.results-panel {
  width: min(960px, 100%);
  margin: 0 auto;
  background: var(--surface-base);
  border: 1px solid var(--line-soft);
  border-radius: 36px;
  padding: 36px;
  box-shadow: 0 25px 80px rgba(17, 28, 23, 0.08);
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
}

.section-label {
  font-size: 14px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--text-muted);
  margin: 0;
}

.title-row {
  display: flex;
  align-items: center;
  gap: 12px;
}

.panel-header h2 {
  margin: 8px 0 4px;
  font-size: 36px;
  color: var(--text-strong);
}

.active-indicator {
  padding: 4px 16px;
  border-radius: 999px;
  border: 1px solid var(--brand-primary);
  color: var(--brand-primary);
  font-size: 13px;
  font-weight: 600;
}

.panel-subtext {
  margin: 0;
  font-size: 15px;
  color: var(--text-secondary);
}

.search-bar {
  margin-top: 32px;
  display: flex;
  gap: 16px;
  align-items: center;
  border: 1px solid var(--line-soft);
  border-radius: 999px;
  padding: 6px 6px 6px 22px;
  background: var(--surface-muted);
}

.search-bar input {
  flex: 1;
  border: none;
  background: transparent;
  font-size: 16px;
  color: var(--text-strong);
  padding: 12px 0;
}

.search-bar input:focus {
  outline: none;
}

.text-action {
  appearance: none;
  border: none;
  background: var(--surface-base);
  border-radius: 999px;
  padding: 12px 20px;
  display: flex;
  gap: 8px;
  align-items: center;
  font-size: 15px;
  color: var(--text-secondary);
  cursor: pointer;
  transition: color 0.2s ease, border-color 0.2s ease;
  border: 1px solid transparent;
}

.text-action:hover,
.text-action:focus-visible {
  color: var(--brand-primary);
  border-color: var(--brand-primary);
}

.hint-row {
  margin-top: 18px;
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 14px;
  color: var(--text-muted);
}

.hint-row ul {
  display: flex;
  gap: 10px;
  padding: 0;
  margin: 0;
  list-style: none;
}

.hint-row li {
  padding: 4px 10px;
  border-radius: 999px;
  border: 1px dashed var(--line-soft);
}

.results-panel {
  min-height: 420px;
}

.results-list {
  display: flex;
  flex-direction: column;
  gap: 18px;
}

.result-card {
  padding: 24px;
  border-radius: 28px;
  border: 1px solid var(--line-soft);
  background: var(--surface-base);
  transition: border-color 0.2s ease, transform 0.2s ease;
  cursor: pointer;
}

.result-card:hover {
  border-color: var(--brand-primary);
  transform: translateY(-2px);
}

.result-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.result-type {
  font-size: 13px;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--brand-primary);
}

.result-indicator {
  font-size: 18px;
  color: var(--text-muted);
}

.result-title {
  margin: 0 0 6px;
  font-size: 20px;
  color: var(--text-strong);
}

.result-info {
  margin: 0;
  font-size: 14px;
  color: var(--text-secondary);
}

.state-card {
  border-radius: 28px;
  border: 1px dashed var(--line-soft);
  padding: 48px 24px;
  text-align: center;
  color: var(--text-secondary);
  display: flex;
  flex-direction: column;
  gap: 10px;
  align-items: center;
}

.state-card p {
  margin: 0;
  font-size: 18px;
  color: var(--text-strong);
}

.state-card small {
  color: var(--text-muted);
}

.loader {
  width: 28px;
  height: 28px;
  border-radius: 50%;
  border: 2px solid var(--line-soft);
  border-top-color: var(--brand-primary);
  animation: spin 1s linear infinite;
}

.placeholder small {
  max-width: 320px;
  line-height: 1.6;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 640px) {
  .search-panel,
  .results-panel {
    padding: 28px;
    border-radius: 28px;
  }

  .search-bar {
    flex-direction: column;
    align-items: stretch;
    border-radius: 24px;
    padding: 16px;
  }

  .text-action {
    width: 100%;
    justify-content: center;
  }

  .hint-row {
    flex-wrap: wrap;
  }

  .title-row {
    flex-direction: column;
    align-items: flex-start;
  }
}
</style>