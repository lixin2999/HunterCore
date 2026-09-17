<script setup lang="ts">
/**
 * 场景 3D 预览（Three.js）
 *
 * 用途：场景详情页以三维视图核对场景配置（地图/自车/参与者/天气弱化展示）。
 * 坐标映射：场景配置沿用 Carla/ROS 右手坐标系（x 前、y 左、z 上，单位 m），
 *          Three.js 为 y-up，故 three(x, z, -y)。朝向 yaw 绕 Three.js 的 Y 轴取负。
 * 约束：不在此渲染任何业务阈值/文案，配置字段全部来自 SceneConfig（契约 scene-service.yaml）。
 */
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'
import * as THREE from 'three'
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls.js'

import type { ActorType, SceneConfig } from '@/types/scene'

const props = withDefaults(
  defineProps<{
    config?: SceneConfig | null
    height?: string
  }>(),
  { config: null, height: '460px' },
)

/** 参与者配色（与 enums.ts 的 ACTOR_TYPE_LABELS 一致的口径：vehicle/pedestrian/other） */
const ACTOR_COLORS: Record<ActorType, number> = {
  vehicle: 0x409eff,
  pedestrian: 0xe6a23c,
  other: 0x909399,
}

const EGO_COLOR = 0x67c23a

const container = ref<HTMLDivElement | null>(null)
let renderer: THREE.WebGLRenderer | null = null
let camera: THREE.PerspectiveCamera | null = null
let controls: OrbitControls | null = null
let scene: THREE.Scene | null = null
let actorGroup: THREE.Group | null = null
let resizeObserver: ResizeObserver | null = null
let animationFrame = 0

/** 释放组内几何体与材质，避免频繁重建导致显存泄漏 */
function clearGroup(group: THREE.Group): void {
  for (const child of [...group.children]) {
    group.remove(child)
    const mesh = child as THREE.Mesh
    mesh.geometry?.dispose()
    const material = mesh.material
    if (Array.isArray(material)) {
      material.forEach((item) => item.dispose())
    } else {
      material?.dispose()
    }
  }
}

/** 构建一个盒体参与者（尺寸按类型近似：车辆 4×2×1.6m，行人 0.6×0.6×1.7m，其他 2×2×1m） */
function createActorMesh(type: ActorType): THREE.Mesh {
  const size: [number, number, number] =
    type === 'vehicle' ? [4, 1.6, 2] : type === 'pedestrian' ? [0.6, 1.7, 0.6] : [2, 1, 2]
  const geometry = new THREE.BoxGeometry(size[0], size[1], size[2])
  const material = new THREE.MeshStandardMaterial({
    color: ACTOR_COLORS[type],
    metalness: 0.2,
    roughness: 0.6,
  })
  const mesh = new THREE.Mesh(geometry, material)
  mesh.position.y = size[1] / 2
  return mesh
}

/** 按当前配置重建场景对象 */
function rebuild(): void {
  if (!scene || !actorGroup) {
    return
  }
  clearGroup(actorGroup)
  const config = props.config
  if (!config) {
    return
  }

  const ego = config.ego_vehicle
  const spawn = config.map.spawn_point
  const egoMesh = createActorMesh('vehicle')
  ;(egoMesh.material as THREE.MeshStandardMaterial).color.setHex(EGO_COLOR)
  egoMesh.position.set(spawn.x, 0.8, -spawn.y)
  egoMesh.rotation.y = -spawn.yaw
  egoMesh.userData.label = `自车 ${ego.model}`
  actorGroup.add(egoMesh)

  for (const actor of config.actors) {
    const mesh = createActorMesh(actor.type)
    mesh.position.set(actor.spawn_point.x, mesh.position.y, -actor.spawn_point.y)
    mesh.rotation.y = -actor.spawn_point.yaw
    mesh.userData.label = actor.actor_id
    actorGroup.add(mesh)
  }

  // 相机聚焦自车
  if (controls && camera) {
    controls.target.set(spawn.x, 0, -spawn.y)
    camera.position.set(spawn.x - 18, 14, -spawn.y + 18)
    controls.update()
  }
}

function initScene(): void {
  if (!container.value) {
    return
  }
  const width = container.value.clientWidth
  const height = container.value.clientHeight

  scene = new THREE.Scene()
  scene.background = new THREE.Color(0x101820)

  camera = new THREE.PerspectiveCamera(55, width / height, 0.1, 2000)
  camera.position.set(-18, 14, 18)

  renderer = new THREE.WebGLRenderer({ antialias: true })
  renderer.setSize(width, height)
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
  container.value.appendChild(renderer.domElement)

  controls = new OrbitControls(camera, renderer.domElement)
  controls.enableDamping = true
  controls.maxPolarAngle = Math.PI / 2.1

  // 地面网格（10m 间距；园区/城区场景量级）
  const grid = new THREE.GridHelper(200, 20, 0x33485c, 0x22303c)
  scene.add(grid)

  const ground = new THREE.Mesh(
    new THREE.PlaneGeometry(200, 200),
    new THREE.MeshStandardMaterial({ color: 0x18222c, roughness: 1 }),
  )
  ground.rotation.x = -Math.PI / 2
  ground.position.y = -0.01
  scene.add(ground)

  scene.add(new THREE.AmbientLight(0xffffff, 0.9))
  const directional = new THREE.DirectionalLight(0xffffff, 0.6)
  directional.position.set(30, 60, 20)
  scene.add(directional)

  actorGroup = new THREE.Group()
  scene.add(actorGroup)

  rebuild()

  const animate = (): void => {
    animationFrame = window.requestAnimationFrame(animate)
    controls?.update()
    if (renderer && scene && camera) {
      renderer.render(scene, camera)
    }
  }
  animate()
}

function handleResize(): void {
  if (!container.value || !renderer || !camera) {
    return
  }
  const width = container.value.clientWidth
  const height = container.value.clientHeight
  camera.aspect = width / height
  camera.updateProjectionMatrix()
  renderer.setSize(width, height)
}

onMounted(() => {
  initScene()
  if (container.value) {
    resizeObserver = new ResizeObserver(handleResize)
    resizeObserver.observe(container.value)
  }
})

watch(() => props.config, rebuild, { deep: true })

onBeforeUnmount(() => {
  window.cancelAnimationFrame(animationFrame)
  resizeObserver?.disconnect()
  resizeObserver = null
  controls?.dispose()
  if (actorGroup) {
    clearGroup(actorGroup)
  }
  renderer?.dispose()
  renderer?.domElement.remove()
  renderer = null
  scene = null
  camera = null
})
</script>

<template>
  <div ref="container" class="scene-preview" :style="{ height }">
    <div v-if="!config" class="scene-preview__empty">暂无场景配置</div>
  </div>
</template>

<style scoped>
.scene-preview {
  position: relative;
  width: 100%;
  border-radius: 4px;
  overflow: hidden;
  background: #101820;
}

.scene-preview__empty {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #8c9aa8;
  font-size: 13px;
}
</style>
