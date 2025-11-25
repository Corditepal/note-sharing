<template>
  <section class="admin-hero">
    <div>
      <p class="section-label">管理员中心</p>
      <div class="hero-title">
        <h1>系统后台管理</h1>
        <span>Administration</span>
      </div>
      <p class="hero-desc">
        统一管理用户、笔记与评论数据。仅实现前端展示与交互，所有后端接口保留占位信息，方便后期接入。
      </p>
    </div>

    <div class="hero-metrics">
      <article v-for="metric in metrics" :key="metric.label" class="metric-card">
        <p>{{ metric.label }}</p>
        <strong>{{ metric.value }}</strong>
      </article>
    </div>
  </section>
</template>

<script setup>
const metrics = [
  { label: '今日新增用户', value: 8 },
  { label: '今日发布笔记', value: 23 },
  { label: '评论待审核', value: 5 }
]
</script>

<style scoped>
.admin-hero {
  background: var(--surface-base);
  border-radius: var(--radius-lg);
  border: 1px solid var(--line-soft);
  padding: 32px clamp(20px, 4vw, 48px);
  box-shadow: var(--shadow-card);
  display: flex;
  justify-content: space-between;
  gap: 24px;
  flex-wrap: wrap;
}

.hero-title {
  display: flex;
  align-items: baseline;
  gap: 16px;
  margin: 8px 0 6px;
}

.hero-title span {
  letter-spacing: 0.4em;
  color: var(--text-muted);
  text-transform: uppercase;
  font-size: 13px;
}

.hero-desc {
  max-width: 560px;
}

.hero-metrics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
  flex: 1;
}

.metric-card {
  border: 1px solid var(--line-soft);
  border-radius: var(--radius-md);
  padding: 18px;
  background: var(--surface-soft);
}

.metric-card p {
  font-size: 12px;
  letter-spacing: 0.2em;
  color: var(--text-muted);
  margin-bottom: 8px;
}

.metric-card strong {
  font-size: 26px;
  color: var(--text-strong);
}
@media (max-width: 960px) {
  .hero-metrics {
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  }
}

@media (max-width: 640px) {
  .admin-hero {
    border-radius: 24px;
    padding: 24px;
  }
}
</style>

