// HunterCore 前端构建配置
// 说明：
// 1. 开发环境通过 Vite dev server 反向代理到 api-gateway(8080)，避免本地 CORS；
//    生产环境由 Ingress 按网关路由表前缀（/api/v1/**、/ws/remote/**）同源转发，
//    因此前端只使用相对基础路径，禁止在代码中硬编码后端主机地址（系统约束第 16 条）。
// 2. WebSocket 走同一代理（ws: true），网关契约要求 WSS + JWT 握手。
import { fileURLToPath, URL } from 'node:url'

import vue from '@vitejs/plugin-vue'
import { defineConfig, loadEnv } from 'vite'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), 'VITE_')
  const devPort = Number(env.VITE_DEV_PORT ?? 5173)
  const proxyTarget = env.VITE_PROXY_TARGET ?? 'http://localhost:8080'

  return {
    plugins: [vue()],
    resolve: {
      alias: {
        '@': fileURLToPath(new URL('./src', import.meta.url)),
      },
    },
    server: {
      host: true,
      port: devPort,
      proxy: {
        // 网关路由表：/api/v1/** → 各微服务（api-gateway 统一鉴权/限流）
        '/api': { target: proxyTarget, changeOrigin: true },
        // /ws/remote/** → remote-control（网关 x-hunter-websocket-routes）
        '/ws': { target: proxyTarget, changeOrigin: true, ws: true },
      },
    },
    build: {
      outDir: 'dist',
      sourcemap: false,
      chunkSizeWarningLimit: 1600,
      rollupOptions: {
        output: {
          // 大体积三方库拆分，保证首屏（看板）加载性能
          manualChunks: {
            vue: ['vue', 'vue-router', 'pinia'],
            'element-plus': ['element-plus', '@element-plus/icons-vue'],
            echarts: ['echarts'],
            three: ['three'],
          },
        },
      },
    },
  }
})
