/**
 * 车辆读模型状态（Pinia）
 *
 * 契约：remote-control.yaml GET /api/v1/remote/vehicles
 * 数据源：Redis `vehicle:status:{vehicle_id}` + `vehicle:online:set`（服务端读模型，source=redis_read_model），
 *        非 TimescaleDB 直查；仅用于列表/看板展示，控制类动作仍需会话互斥校验。
 */
import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

import { listControllableVehicles } from '@/api/remote'
import type { ControllableVehicle, ControllableVehicleQuery } from '@/types/remote'
import { VEHICLE_ONLINE_STATUSES } from '@/constants'

export const useVehicleStore = defineStore('vehicle', () => {
  const vehicles = ref<ControllableVehicle[]>([])
  const total = ref(0)
  const loading = ref(false)
  const lastUpdatedAt = ref<number>(0)
  /** 查询条件（状态过滤 / 仅可操控） */
  const query = ref<ControllableVehicleQuery>({ page: 1, page_size: 200, controllable_only: false })

  /** 可操控车辆（controllable=true，用于远程操控入口） */
  const controllableVehicles = computed<ControllableVehicle[]>(() =>
    vehicles.value.filter((item) => item.controllable),
  )

  /** 在线车辆（offline 之外的 7 态，用于看板计数） */
  const onlineVehicles = computed<ControllableVehicle[]>(() =>
    vehicles.value.filter((item) => VEHICLE_ONLINE_STATUSES.includes(item.status)),
  )

  /** 车辆 ID 索引（事件/任务中的 vehicle_id 展示名称用） */
  const vehicleNameMap = computed<Record<string, string>>(() => {
    const map: Record<string, string> = {}
    for (const item of vehicles.value) {
      map[item.vehicle_id] = item.vehicle_name || item.vehicle_id
    }
    return map
  })

  /** 拉取车辆列表（读模型） */
  async function fetchVehicles(params?: ControllableVehicleQuery): Promise<void> {
    loading.value = true
    try {
      const effective: ControllableVehicleQuery = { ...query.value, ...params }
      const data = await listControllableVehicles(effective)
      vehicles.value = data.items
      total.value = data.total
      lastUpdatedAt.value = Date.now()
    } finally {
      loading.value = false
    }
  }

  /** 按 vehicle_id 取单车（未见则返回 undefined） */
  function findVehicle(vehicleId: string): ControllableVehicle | undefined {
    return vehicles.value.find((item) => item.vehicle_id === vehicleId)
  }

  /** 车辆展示名解析 */
  function resolveName(vehicleId: string): string {
    return vehicleNameMap.value[vehicleId] ?? vehicleId
  }

  return {
    vehicles,
    total,
    loading,
    lastUpdatedAt,
    query,
    controllableVehicles,
    onlineVehicles,
    vehicleNameMap,
    fetchVehicles,
    findVehicle,
    resolveName,
  }
})
