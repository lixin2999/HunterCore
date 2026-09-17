/**
 * 展示层格式化工具（纯函数）
 * 统一时间/时长/字节/数值/百分比格式，避免各组件重复实现。
 */
import dayjs from 'dayjs'

/** Unix epoch 秒 → 本地时间字符串 */
export function formatTime(epochSeconds?: number | null, template = 'YYYY-MM-DD HH:mm:ss'): string {
  if (epochSeconds === undefined || epochSeconds === null || Number.isNaN(epochSeconds)) {
    return '-'
  }
  return dayjs(epochSeconds * 1000).format(template)
}

/** Unix epoch 秒 → 到当前时刻的相对时间（如 3 分钟前） */
export function formatRelativeTime(epochSeconds?: number | null): string {
  if (!epochSeconds) {
    return '-'
  }
  const diffSeconds = Math.floor(Date.now() / 1000) - epochSeconds
  if (diffSeconds < 0) {
    return formatTime(epochSeconds)
  }
  if (diffSeconds < 60) {
    return `${diffSeconds} 秒前`
  }
  if (diffSeconds < 3600) {
    return `${Math.floor(diffSeconds / 60)} 分钟前`
  }
  if (diffSeconds < 86400) {
    return `${Math.floor(diffSeconds / 3600)} 小时前`
  }
  return `${Math.floor(diffSeconds / 86400)} 天前`
}

/** RFC3339 字符串（如场景 create_time）→ 本地时间字符串 */
export function formatDateTimeString(value?: string | null): string {
  if (!value) {
    return '-'
  }
  return dayjs(value).format('YYYY-MM-DD HH:mm:ss')
}

/** 秒 → 时长文案（如 1h 2m 3s） */
export function formatDuration(seconds?: number | null): string {
  if (seconds === undefined || seconds === null || seconds < 0) {
    return '-'
  }
  const total = Math.floor(seconds)
  const hours = Math.floor(total / 3600)
  const minutes = Math.floor((total % 3600) / 60)
  const secs = total % 60
  if (hours > 0) {
    return `${hours}h ${minutes}m ${secs}s`
  }
  if (minutes > 0) {
    return `${minutes}m ${secs}s`
  }
  return `${secs}s`
}

/** 字节 → 人类可读大小 */
export function formatBytes(bytes?: number | null, fractionDigits = 2): string {
  if (bytes === undefined || bytes === null || bytes < 0) {
    return '-'
  }
  if (bytes === 0) {
    return '0 B'
  }
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1)
  const value = bytes / 1024 ** exponent
  return `${value.toFixed(exponent === 0 ? 0 : fractionDigits)} ${units[exponent]}`
}

/** 数值保留小数（空值安全） */
export function formatNumber(value?: number | null, fractionDigits = 2): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return '-'
  }
  return value.toFixed(fractionDigits)
}

/** 比例（0–1）→ 百分比文案 */
export function formatPercent(value?: number | null, fractionDigits = 1): string {
  if (value === undefined || value === null || Number.isNaN(value)) {
    return '-'
  }
  return `${(value * 100).toFixed(fractionDigits)}%`
}

/** 时间范围（Unix 秒）→ 查询区间数组（供 el-date-picker 使用） */
export function toDateRange(startTime?: number, endTime?: number): [number, number] | undefined {
  if (!startTime || !endTime) {
    return undefined
  }
  return [startTime, endTime]
}
