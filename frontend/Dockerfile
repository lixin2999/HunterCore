# =====================================================================
# HunterCore web-portal 前端镜像（Vue3 + Vite 构建 → Nginx 静态托管）
# 构建上下文：./frontend（docker-compose.yml 中 web-portal.build.context = ./frontend）
# 构建：docker build -f frontend/Dockerfile --build-arg VERSION=0.1.0 -t hunter/web-portal:0.1.0 ./frontend
#
# 前端仅使用相对基础路径（VITE_API_BASE_URL=/api/v1、VITE_WS_BASE_URL=/ws，见 .env.production），
# 同源反代由 Nginx 完成：/api/ → api-gateway:8080，/ws/ → remote-control:8085。
# Nginx 配置【不烘焙进镜像】：由 compose 挂载 ./config/nginx.conf → /etc/nginx/nginx.conf:ro
#   （单一事实来源：infra/deploy/config/nginx.conf，避免镜像与部署包两处漂移）。
# =====================================================================

# ---------- 阶段 1：builder（Node 构建产物） ----------
FROM node:20-alpine AS builder
WORKDIR /app

# 先复制依赖清单，利用层缓存（package-lock.json 存在 → npm ci 可复现）
COPY package*.json ./
# 国内网络可加 --registry=https://registry.npmmirror.com
RUN npm ci --no-audit --no-fund

# 复制源码与构建配置后打包（type-check + vite build → /app/dist）
COPY . .
RUN npm run build

# ---------- 阶段 2：final（Nginx 托管 dist） ----------
FROM nginx:1.25-alpine
# 仅拷入静态产物；nginx.conf 通过卷挂载注入（见文件头说明）
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 80

# busybox wget 探活首页（nginx:alpine 自带 wget）
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost/ || exit 1

# 说明：nginx 官方镜像 master 以 root 启动、worker 以 nginx 用户运行。
# 若需完全非 root（80 端口 <1024 需 net.ipv4.ip_unprivileged_port_start=80 或改用 8080 + 上层映射），
# 请改用 nginxinc/nginx-unprivileged 镜像（监听 8080）。
CMD ["nginx", "-g", "daemon off;"]
