/**
 * 路由配置与访问守卫
 *
 * 契约依据：
 * - 网关路由表（api-gateway.yaml / 系统约束）：/api/v1/scene|data|analytics|ota|remote|vehicle|user/**、
 *   /ws/remote/**；前端页面与之一一对应，禁止自行发明业务路径语义。
 * - RBAC 权限编码见 constants/permissions.ts（均可在各服务契约 RBAC 说明中溯源）。
 *
 * 守卫策略：
 * 1. 未登录访问受限页面 → 跳转登录页并携带 redirect；
 * 2. 已登录但缺少页面所需权限 → 跳转 403；
 * 3. 页面标题统一由 meta.title 渲染。
 */
import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'

import MainLayout from '@/layouts/MainLayout.vue'
import { PERMISSIONS } from '@/constants/permissions'
import { useUserStore } from '@/stores/user'

declare module 'vue-router' {
  interface RouteMeta {
    /** 页面标题（顶栏/面包屑/文档标题） */
    title: string
    /** 免登录页面 */
    public?: boolean
    /** 访问所需权限编码（服务端下发编码，见 constants/permissions.ts） */
    permission?: string
    /** 侧边菜单图标（Element Plus 图标组件名） */
    icon?: string
  }
}

const routes: RouteRecordRaw[] = [
  {
    path: '/login',
    name: 'login',
    component: () => import('@/views/LoginView.vue'),
    meta: { title: '登录', public: true },
  },
  {
    path: '/',
    component: MainLayout,
    redirect: { name: 'dashboard' },
    children: [
      {
        path: 'dashboard',
        name: 'dashboard',
        component: () => import('@/views/DashboardView.vue'),
        meta: { title: '运营看板', icon: 'Monitor', permission: PERMISSIONS.analyticsRead },
      },
      {
        path: 'scenes',
        name: 'scenes',
        component: () => import('@/views/scene/SceneListView.vue'),
        meta: { title: '场景库', icon: 'Files', permission: PERMISSIONS.sceneRead },
      },
      {
        path: 'scenes/:scene_id',
        name: 'scene-detail',
        component: () => import('@/views/scene/SceneDetailView.vue'),
        meta: { title: '场景详情', permission: PERMISSIONS.sceneRead },
      },
      {
        path: 'analytics',
        name: 'analytics',
        component: () => import('@/views/analytics/AnalyticsView.vue'),
        meta: { title: '数据分析', icon: 'DataAnalysis', permission: PERMISSIONS.analyticsRead },
      },
      {
        path: 'ota',
        name: 'ota',
        component: () => import('@/views/ota/OtaLayoutView.vue'),
        redirect: { name: 'ota-versions' },
        meta: { title: 'OTA 管理', icon: 'UploadFilled', permission: PERMISSIONS.otaRead },
        children: [
          {
            path: 'versions',
            name: 'ota-versions',
            component: () => import('@/views/ota/OtaVersionsView.vue'),
            meta: { title: '版本仓库', permission: PERMISSIONS.otaRead },
          },
          {
            path: 'tasks',
            name: 'ota-tasks',
            component: () => import('@/views/ota/OtaTasksView.vue'),
            meta: { title: '升级任务（灰度发布）', permission: PERMISSIONS.otaRead },
          },
          {
            path: 'tasks/:task_id',
            name: 'ota-task-detail',
            component: () => import('@/views/ota/OtaTaskDetailView.vue'),
            meta: { title: '任务详情', permission: PERMISSIONS.otaRead },
          },
        ],
      },
      {
        path: 'remote',
        name: 'remote',
        component: () => import('@/views/remote/RemoteLayoutView.vue'),
        redirect: { name: 'remote-console' },
        meta: { title: '远程操控', icon: 'VideoCamera', permission: PERMISSIONS.remoteRead },
        children: [
          {
            path: 'console',
            name: 'remote-console',
            component: () => import('@/views/remote/RemoteConsoleView.vue'),
            meta: { title: '操控台', permission: PERMISSIONS.remoteRead },
          },
          {
            path: 'history',
            name: 'remote-history',
            component: () => import('@/views/remote/RemoteHistoryView.vue'),
            meta: { title: '操控记录', permission: PERMISSIONS.remoteRead },
          },
        ],
      },
      {
        path: 'data',
        name: 'data-events',
        component: () => import('@/views/data/EventListView.vue'),
        meta: { title: '事件与文件', icon: 'Warning', permission: PERMISSIONS.dataRead },
      },
      {
        path: 'forbidden',
        name: 'forbidden',
        component: () => import('@/views/error/ForbiddenView.vue'),
        meta: { title: '无权限' },
      },
    ],
  },
  {
    path: '/:pathMatch(.*)*',
    name: 'not-found',
    component: () => import('@/views/error/NotFoundView.vue'),
    meta: { title: '页面不存在', public: true },
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
  scrollBehavior: () => ({ top: 0 }),
})

router.beforeEach(async (to) => {
  const userStore = useUserStore()
  document.title = to.meta.title ? `${to.meta.title} · HunterCore` : 'HunterCore'

  if (to.meta.public) {
    // 已登录用户访问登录页 → 回看板
    if (to.name === 'login' && userStore.isAuthenticated) {
      return { name: 'dashboard' }
    }
    return true
  }

  if (!userStore.isAuthenticated) {
    return { name: 'login', query: { redirect: to.fullPath } }
  }

  // 权限实时同步（角色/权限变更后刷新一次）
  if (userStore.permissions.length === 0) {
    try {
      await userStore.fetchProfile()
    } catch {
      // 拉取失败由请求层拦截器统一处理（会话失效 → 广播事件）
    }
  }

  const required = to.meta.permission
  if (required && !userStore.hasPermission(required)) {
    return { name: 'forbidden' }
  }
  return true
})

export default router
