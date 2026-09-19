#!/usr/bin/env bash
# =====================================================================
# HunterCore MinIO Bucket 初始化（Bucket 名称与生命周期见设计文档，不可更改）
# 由 docker-compose 的 minio-init 一次性任务执行：bash /init-buckets.sh
# =====================================================================
set -euo pipefail

ALIAS="hunter-local"
ENDPOINT="${MINIO_ENDPOINT_INTERNAL:-http://minio:9000}"

# 等待 MinIO 就绪并配置 alias（最多 60s）
for i in $(seq 1 30); do
  if mc alias set "$ALIAS" "$ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; then
    break
  fi
  echo "[minio-init] waiting for minio... ($i/30)"
  sleep 2
done

create_bucket() {
  mc mb --ignore-existing "$ALIAS/$1"
  echo "[minio-init] bucket ready: $1"
}

# add_expiry <bucket> <days> [prefix]
add_expiry() {
  local bucket="$1" days="$2" prefix="${3:-}"
  if [ -n "$prefix" ]; then
    mc ilm rule add --expire-days "$days" --prefix "$prefix" "$ALIAS/$bucket" >/dev/null
  else
    mc ilm rule add --expire-days "$days" "$ALIAS/$bucket" >/dev/null
  fi
  echo "[minio-init] lifecycle: $bucket expire ${days}d${prefix:+ (prefix=$prefix)}"
}

# ---- 7 个 Bucket（名称与生命周期不可更改） ----
create_bucket "hunter-raw-data"       # 传感器原始数据（点云/图像）   30 天
create_bucket "hunter-rosbag"         # ROS Bag 文件                 事件永久/常规 30 天
create_bucket "hunter-video"          # 远程操控录像                  90 天
create_bucket "hunter-ota-packages"   # OTA 升级包                    永久
create_bucket "hunter-reports"        # 分析报告                      永久
create_bucket "hunter-logs"           # 系统日志                      30 天
create_bucket "hunter-scene-assets"   # 场景资源（地图/模型）          永久

# ---- 生命周期规则 ----
add_expiry "hunter-raw-data" 30
# rosbag 约定：常规数据存于 regular/ 前缀（30 天过期）；
# 事件数据存于 events/ 前缀（无过期规则 = 永久），由 data-collector 上传时遵循
add_expiry "hunter-rosbag" 30 "regular/"
add_expiry "hunter-video" 90
add_expiry "hunter-logs" 30
# hunter-ota-packages / hunter-reports / hunter-scene-assets：永久，不设置过期规则

echo "[minio-init] buckets:"
mc ls "$ALIAS"
