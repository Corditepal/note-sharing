<template>
  <div class="admin-page">
    <AdminHero />
    <AdminUserManagement />
    <AdminNoteManagement />
    <AdminCommentManagement />
  </div>
</template>

<script setup>
import AdminHero from '../components/admin/AdminHero.vue'
import AdminUserManagement from '../components/admin/AdminUserManagement.vue'
import AdminNoteManagement from '../components/admin/AdminNoteManagement.vue'
import AdminCommentManagement from '../components/admin/AdminCommentManagement.vue'
</script>

<style scoped>
.admin-page {
  padding: 32px clamp(20px, 4vw, 48px) 80px;
  background: var(--surface-muted);
  display: flex;
  flex-direction: column;
  gap: 28px;
}

@media (max-width: 640px) {
  .admin-page {
    padding: 24px 16px 80px;
  }
}
</style>

