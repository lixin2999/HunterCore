<script setup lang="ts">
/**
 * 主布局：侧边菜单 + 顶栏 + 内容区
 *
 * 权限：菜单项按 meta.permission 过滤（服务端下发权限编码，见 constants/permissions.ts）；
 *      隐藏菜单仅影响展示，真实鉴权由网关 + 服务端 RBAC 执行。
 */
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessageBox } from 'element-plus'

import { APP_TITLE } from '@/constants'
import { PERMISSIONS, ROLE_LABELS } from '@/constants/permissions'
import { useUserStore } from '@/stores/user'

interface MenuItem {
  /** 路由名（子菜单首项） */
  name: string
  title: string
  icon: string
  permission: string
  children?: Array<{ name: string; title: string }>
}

const userStore = useUserStore()
const route = useRoute()
const router = useRouter()

/** 菜单定义（与 router 一一对应，顺序即展示顺序） */
const MENU: MenuItem[] = [
  { name: 'dashboard', title: '运营看板', icon: 'Monitor', permission: PERMISSIONS.analyticsRead },
  { name: 'scenes', title: '场景库', icon: 'Files', permission: PERMISSIONS.sceneRead },
  { name: 'analytics', title: '数据分析', icon: 'DataAnalysis', permission: PERMISSIONS.analyticsRead },
  {
    name: 'ota-versions',
    title: 'OTA 管理',
    icon: 'UploadFilled',
    permission: PERMISSIONS.otaRead,
    children: [
      { name: 'ota-versions', title: '版本仓库' },
      { name: 'ota-tasks', title: '升级任务' },
    ],
  },
  {
    name: 'remote-console',
    title: '远程操控',
    icon: 'VideoCamera',
    permission: PERMISSIONS.remoteRead,
    children: [
      { name: 'remote-console', title: '操控台' },
      { name: 'remote-history', title: '操控记录' },
    ],
  },
  { name: 'data-events', title: '事件与文件', icon: 'Warning', permission: PERMISSIONS.dataRead },
]

/** 按权限过滤后的菜单 */
const visibleMenu = computed<MenuItem[]>(() =>
  MENU.filter((item) => userStore.hasPermission(item.permission)),
)

const activeMenu = computed<string>(() => String(route.name ?? ''))

const currentTitle = computed<string>(() => route.meta.title ?? '')

const roleText = computed<string>(() =>
  userStore.roles.map((role) => ROLE_LABELS[role] ?? role).join(' / ') || '未分配角色',
)

/** 会话失效提示（请求层广播 → user store 暂存，挂载时消费一次） */
const expiredMessage = ref('')

onMounted(() => {
  expiredMessage.value = userStore.consumeExpiredMessage()
})

/** 登出（服务端幂等；本地会话由 store 清理） */
async function handleLogout(): Promise<void> {
  try {
    await ElMessageBox.confirm('确认退出登录？', '提示', { type: 'warning' })
  } catch {
    // 用户取消
    return
  }
  await userStore.logout()
  await router.push({ name: 'login' })
}
</script>

<template>
  <el-container class="layout">
    <el-aside width="220px" class="layout__aside">
      <div class="layout__brand">{{ APP_TITLE }}</div>
      <el-menu :default-active="activeMenu" router class="layout__menu">
        <template v-for="item in visibleMenu" :key="item.name">
          <el-sub-menu v-if="item.children?.length" :index="item.title">
            <template #title>
              <el-icon><component :is="item.icon" /></el-icon>
              <span>{{ item.title }}</span>
            </template>
            <el-menu-item v-for="child in item.children" :key="child.name" :index="child.name" :route="{ name: child.name }">
              {{ child.title }}
            </el-menu-item>
          </el-sub-menu>
          <el-menu-item v-else :index="item.name" :route="{ name: item.name }">
            <el-icon><component :is="item.icon" /></el-icon>
            <span>{{ item.title }}</span>
          </el-menu-item>
        </template>
      </el-menu>
    </el-aside>

    <el-container>
      <el-header class="layout__header">
        <div class="layout__breadcrumb">
          <el-breadcrumb separator="/">
            <el-breadcrumb-item :to="{ name: 'dashboard' }">首页</el-breadcrumb-item>
            <el-breadcrumb-item>{{ currentTitle }}</el-breadcrumb-item>
          </el-breadcrumb>
        </div>
        <div class="layout__user">
          <el-tag v-if="expiredMessage" type="danger" effect="dark" size="small">{{ expiredMessage }}</el-tag>
          <span class="layout__username">{{ userStore.displayName }}</span>
          <el-tag size="small" type="info">{{ roleText }}</el-tag>
          <el-button link type="primary" @click="handleLogout">退出</el-button>
        </div>
      </el-header>

      <el-main class="layout__main">
        <router-view v-slot="{ Component }">
          <keep-alive :max="3">
            <component :is="Component" />
          </keep-alive>
        </router-view>
      </el-main>
    </el-container>
  </el-container>
</template>

<style scoped>
.layout {
  height: 100vh;
}

.layout__aside {
  background: #1f2d3d;
  display: flex;
  flex-direction: column;
}

.layout__brand {
  color: #fff;
  font-weight: 600;
  font-size: 15px;
  padding: 18px 16px;
  letter-spacing: 0.5px;
}

.layout__menu {
  flex: 1;
  border-right: none;
  background: transparent;
  --el-menu-bg-color: transparent;
  --el-menu-text-color: #c7d0d9;
  --el-menu-active-color: #409eff;
  --el-menu-hover-bg-color: #2c3e50;
}

.layout__header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  border-bottom: 1px solid var(--el-border-color-light);
  background: #fff;
}

.layout__user {
  display: flex;
  align-items: center;
  gap: 8px;
}

.layout__username {
  font-size: 13px;
  color: var(--el-text-color-primary);
}

.layout__main {
  background: #f5f7fa;
  padding: 16px;
}
</style>
