/**
 * 通用轮询组合式函数
 *
 * 用途：看板/遥测/会话状态的周期刷新。
 * 约束：
 * 1. 间隔由调用方传入（来源于 constants POLL_INTERVALS ← 环境变量），不得在组件内硬编码；
 *    其下限受附录 D 限流约束（GET /api/v1/data/telemetry 单用户 20 QPS）。
 * 2. 页面隐藏（visibilitychange）时自动暂停，避免后台标签页空耗配额；
 * 3. 单次任务未完成时跳过本轮，防止慢请求堆积。
 */
import { onBeforeUnmount, ref, type Ref } from 'vue'

export interface PollingOptions {
  /** 立即执行一次（默认 true） */
  immediate?: boolean
  /** 页面隐藏时暂停（默认 true） */
  pauseWhenHidden?: boolean
  /** 异常回调（不中断轮询） */
  onError?: (error: unknown) => void
}

export interface PollingController {
  /** 是否运行中 */
  isRunning: Ref<boolean>
  /** 最近一次执行完成时间（毫秒时间戳，0 表示尚未执行） */
  lastRunAt: Ref<number>
  start: () => void
  stop: () => void
  /** 立即执行一次（不影响定时器） */
  trigger: () => Promise<void>
}

export function usePolling(
  task: () => Promise<void> | void,
  intervalMs: number,
  options: PollingOptions = {},
): PollingController {
  const { immediate = true, pauseWhenHidden = true, onError } = options
  const isRunning = ref(false)
  const lastRunAt = ref(0)

  let timer: number | null = null
  let executing = false

  async function trigger(): Promise<void> {
    if (executing) {
      return
    }
    executing = true
    try {
      await task()
      lastRunAt.value = Date.now()
    } catch (error) {
      onError?.(error)
    } finally {
      executing = false
    }
  }

  function start(): void {
    if (isRunning.value) {
      return
    }
    isRunning.value = true
    if (immediate) {
      void trigger()
    }
    timer = window.setInterval(() => {
      if (pauseWhenHidden && document.hidden) {
        return
      }
      void trigger()
    }, intervalMs)
  }

  function stop(): void {
    isRunning.value = false
    if (timer !== null) {
      window.clearInterval(timer)
      timer = null
    }
  }

  onBeforeUnmount(stop)

  return { isRunning, lastRunAt, start, stop, trigger }
}
