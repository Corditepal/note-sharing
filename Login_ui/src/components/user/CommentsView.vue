<template>
  <div class="comments-page">
    <section class="comments-hero">
      <div>
        <p class="section-label">评论管理</p>
        <div class="hero-title">
          <h2>我的评论</h2>
          <span>Comments</span>
        </div>
        <p class="hero-desc">
          分别查看「我的笔记收到的评论」与「我发送的评论」，支持删除操作并保持样式与交互一致。
        </p>
      </div>
      <div class="hero-meta">
        <div>
          <p>收到评论</p>
          <strong>{{ receivedComments.length }}</strong>
        </div>
        <div>
          <p>已发送</p>
          <strong>{{ sentComments.length }}</strong>
        </div>
      </div>
    </section>

    <div class="comments-layout">
      <section class="comment-panel">
        <header>
          <div>
            <p class="section-label">Received</p>
            <h3>我的笔记中的评论</h3>
          </div>
          <button class="ghost-btn" type="button">导出</button>
        </header>
        <ul>
          <li v-for="comment in receivedComments" :key="comment.id" class="comment-card">
            <div class="comment-bullet" :data-status="comment.status"></div>
            <div class="comment-body">
              <div class="comment-head">
                <strong>{{ comment.noteTitle }}</strong>
                <span>{{ comment.createdAt }}</span>
              </div>
              <p class="comment-content">{{ comment.content }}</p>
              <div class="comment-meta">
                <span>来自：{{ comment.from }}</span>
                <span>状态：{{ comment.status }}</span>
              </div>
              <div class="comment-actions">
                <button type="button" class="text-link">回复</button>
                <button type="button" class="text-link danger" @click="handleDelete('received', comment.id)">删除</button>
              </div>
            </div>
          </li>
        </ul>
      </section>

      <section class="comment-panel">
        <header>
          <div>
            <p class="section-label">Sent</p>
            <h3>我发送的评论</h3>
          </div>
          <button class="ghost-btn" type="button">批量删除</button>
        </header>
        <ul>
          <li v-for="comment in sentComments" :key="comment.id" class="comment-card">
            <div class="comment-bullet" data-status="sent"></div>
            <div class="comment-body">
              <div class="comment-head">
                <strong>目标笔记：{{ comment.noteTitle }}</strong>
                <span>{{ comment.createdAt }}</span>
              </div>
              <p class="comment-content">{{ comment.content }}</p>
              <div class="comment-meta">
                <span>对象：{{ comment.targetOwner }}</span>
                <span>所在空间：{{ comment.space }}</span>
              </div>
              <div class="comment-actions">
                <button type="button" class="text-link danger" @click="handleDelete('sent', comment.id)">删除评论</button>
              </div>
            </div>
          </li>
        </ul>
      </section>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'

/**
 * API 占位信息（仅限展示）
 * - GET /api/comments/received
 *   输入: 无
 *   输出: { code: number, data: Comment[] }
 *   返回码: 200 成功 / 401 未登录
 * - GET /api/comments/sent
 *   输入: 无
 *   输出: { code: number, data: Comment[] }
 *   返回码: 200 成功 / 401 未登录
 * - DELETE /api/comments/{id}
 *   输入: 无
 *   输出: { code: number, message: string }
 *   返回码: 200 删除成功 / 404 评论不存在
 */

const receivedComments = ref([
  {
    id: 'rc-1',
    noteTitle: 'Vue3 组件通信',
    content: '这段关于 provide/inject 的示例很有帮助，期待更多案例。',
    from: '林梓萱',
    status: '未读',
    createdAt: '2025-11-18 21:36'
  },
  {
    id: 'rc-2',
    noteTitle: '数据分析模板',
    content: '能分享下原始数据表结构吗？我想复用这套模板。',
    from: '张晨',
    status: '已读',
    createdAt: '2025-11-17 14:52'
  }
])

const sentComments = ref([
  {
    id: 'sc-1',
    noteTitle: 'OKR 复盘指引',
    content: '非常系统的拆解，已收藏～',
    targetOwner: '项目运营组',
    space: '组织管理',
    createdAt: '2025-11-15 09:25'
  },
  {
    id: 'sc-2',
    noteTitle: 'React 性能调优',
    content: '建议补充下关于 memo 的使用经验，会更完整。',
    targetOwner: '余洋',
    space: '前端技术',
    createdAt: '2025-11-11 16:40'
  }
])

const handleDelete = (type, id) => {
  // TODO: 调用 DELETE /api/comments/{id} 删除评论
  if (type === 'received') {
    receivedComments.value = receivedComments.value.filter((item) => item.id !== id)
  } else {
    sentComments.value = sentComments.value.filter((item) => item.id !== id)
  }
}
</script>

<style scoped>
.comments-page {
  display: flex;
  flex-direction: column;
  gap: 28px;
  padding: clamp(16px, 3vw, 36px);
}

.comments-hero {
  background: var(--surface-base);
  border-radius: var(--radius-lg);
  border: 1px solid var(--line-soft);
  padding: 32px clamp(20px, 4vw, 48px);
  box-shadow: var(--shadow-card);
  display: flex;
  justify-content: space-between;
  align-items: center;
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
  letter-spacing: 0.4em;
  color: var(--text-muted);
  text-transform: uppercase;
  font-size: 13px;
}

.hero-meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 12px;
}

.hero-meta div {
  border-radius: var(--radius-md);
  border: 1px solid var(--line-soft);
  background: var(--surface-soft);
  padding: 16px 18px;
}

.hero-meta p {
  font-size: 12px;
  letter-spacing: 0.2em;
  color: var(--text-muted);
  margin-bottom: 6px;
}

.hero-meta strong {
  font-size: 24px;
}

.comments-layout {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  gap: 24px;
}

.comment-panel {
  background: var(--surface-base);
  border-radius: var(--radius-lg);
  border: 1px solid var(--line-soft);
  padding: 28px;
  box-shadow: var(--shadow-soft);
}

.comment-panel header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 18px;
  gap: 12px;
}

.comment-panel h3 {
  margin-top: 6px;
  font-size: 20px;
}

.comment-panel ul {
  list-style: none;
  padding: 0;
  margin: 0;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.comment-card {
  display: grid;
  grid-template-columns: 12px 1fr;
  gap: 16px;
  padding: 16px 0;
  border-bottom: 1px solid var(--line-soft);
}

.comment-card:last-child {
  border-bottom: none;
}

.comment-bullet {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  margin-top: 8px;
  background: var(--brand-primary);
  position: relative;
}

.comment-bullet::after {
  content: '';
  position: absolute;
  left: 50%;
  top: 12px;
  width: 1px;
  height: calc(100% - 12px);
  background: var(--line-soft);
  transform: translateX(-50%);
}

.comment-card:last-child .comment-bullet::after {
  display: none;
}

.comment-bullet[data-status='未读'] {
  background: var(--feedback-warning);
}

.comment-body {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.comment-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
}

.comment-head span {
  font-size: 13px;
  color: var(--text-muted);
}

.comment-content {
  font-size: 15px;
  color: var(--text-secondary);
  line-height: 1.7;
}

.comment-meta {
  font-size: 13px;
  color: var(--text-muted);
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
}

.comment-actions {
  display: flex;
  gap: 16px;
}

.ghost-btn {
  border-radius: 999px;
  border: 1px solid var(--line-soft);
  padding: 8px 18px;
  background: transparent;
  color: var(--text-secondary);
}

.ghost-btn:hover {
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

.text-link.danger {
  color: var(--feedback-danger);
}

.text-link:hover {
  color: var(--brand-primary);
}

@media (max-width: 640px) {
  .comments-hero,
  .comment-panel {
    border-radius: 24px;
    padding: 24px;
  }

  .comment-head {
    flex-direction: column;
    align-items: flex-start;
  }
}
</style>

