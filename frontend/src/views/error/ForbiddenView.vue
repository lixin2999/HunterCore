<script setup lang="ts">
/** 403 无权限页（code=1002 语义：RBAC 或数据权限不足） */
import { useRouter } from 'vue-router'

const router = useRouter()
</script>

<template>
  <el-result icon="warning" title="无权限访问" sub-title="当前账号缺少该页面的权限编码（如 remote:read / ota:execute），请联系管理员分配角色。">
    <template #extra>
      <el-button type="primary" @click="router.push({ name: 'dashboard' })">返回看板</el-button>
    </template>
  </el-result>
</template>
