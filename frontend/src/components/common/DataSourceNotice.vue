<script setup lang="ts">
/**
 * 数据来源与新鲜度提示（data-analytics 各评估响应共有字段）
 *
 * 契约：PerceptionEvalData / ControlEvalData / SceneCoverageData 均 required
 * `data_source` + `updated_at`，可选 `job_name`、`report_id`；
 * **不存在 available/reason 字段**（仅 dashboard 分块有）——数据不可用时服务端返回
 * 业务错误码（5001/3001），由请求层统一提示，因此本组件只做「来源 + 新鲜度」呈现。
 */
import { computed } from 'vue'

const props = withDefaults(
  defineProps<{
    /** 数据来源描述（契约 data_source） */
    dataSource?: string
    /** 离线作业名（契约 job_name） */
    jobName?: string
    /** 结果生成时间（Unix epoch 秒，契约 updated_at） */
    updatedAt?: number
    /** 超过该秒数视为陈旧（默认 24h = 86400s） */
    staleAfterSeconds?: number
  }>(),
  { dataSource: '', jobName: '', updatedAt: 0, staleAfterSeconds: 86_400 },
)

const isStale = computed<boolean>(
  () => props.updatedAt > 0 && Date.now() / 1000 - props.updatedAt > props.staleAfterSeconds,
)

const updatedText = computed<string>(() =>
  props.updatedAt > 0 ? new Date(props.updatedAt * 1000).toLocaleString() : '-',
)
</script>

<template>
  <div class="source">
    <el-tag v-if="dataSource" size="small" effect="plain">数据来源：{{ dataSource }}</el-tag>
    <el-tag v-if="jobName" size="small" type="info" effect="plain">作业：{{ jobName }}</el-tag>
    <el-tag :type="isStale ? 'warning' : 'success'" size="small" effect="plain">
      生成时间：{{ updatedText }}
    </el-tag>
    <el-tag v-if="isStale" size="small" type="warning" effect="dark">数据可能已陈旧</el-tag>
  </div>
</template>

<style scoped>
.source {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 12px;
}
</style>
