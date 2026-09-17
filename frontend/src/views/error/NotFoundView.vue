<script setup lang="ts">
/** 404 页面不存在 */
import { useRouter } from 'vue-router'

const router = useRouter()
</script>

<template>
  <el-result icon="info" title="页面不存在" sub-title="请检查地址是否正确，或返回运营看板。">
    <template #extra>
      <el-button type="primary" @click="router.push({ name: 'dashboard' })">返回看板</el-button>
    </template>
  </el-result>
</template>
