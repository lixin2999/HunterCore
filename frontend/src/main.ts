/**
 * 应用入口
 *
 * 装配顺序：Pinia → 会话监听（请求层 401/1001/1003 广播）→ 路由守卫 → Element Plus → 权限指令 → 挂载。
 * 安全：Token 仅存于 localStorage（见 utils/storage.ts 折衷说明），日志不输出敏感信息。
 */
import { createApp } from 'vue'
import { createPinia } from 'pinia'
import ElementPlus from 'element-plus'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import zhCn from 'element-plus/es/locale/lang/zh-cn'
import 'element-plus/dist/index.css'

import App from './App.vue'
import { permissionDirective } from './directives/permission'
import router from './router'
import { useUserStore } from './stores/user'
import { APP_TITLE } from './constants'
import './styles/global.css'

const app = createApp(App)

app.use(createPinia())

// 注册全局会话失效监听：任一请求返回 1001/1003 且无法刷新时清理本地会话
useUserStore().bindSessionExpiredListener()

app.use(router)
app.use(ElementPlus, { locale: zhCn })

for (const [name, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(name, component)
}

// v-permission 指令（服务端下发 permission_code 成员判定）
app.directive('permission', permissionDirective)

document.title = APP_TITLE
app.mount('#app')
