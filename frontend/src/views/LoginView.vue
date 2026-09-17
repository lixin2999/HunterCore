<script setup lang="ts">
/**
 * 登录页：用户名 + 密码（+ 可选 MFA(TOTP) 6 位动态码）
 *
 * 契约：POST /api/v1/user/login（api-gateway.yaml）
 * - 失败原因以 code 区分：1001（凭据或 MFA 错误）、2001/2002（参数错误）、5001（依赖不可用）
 * - 暴力破解防护由服务端登录失败限制实现（前端不做本地锁定，避免误导）
 */
import { computed, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'

import { APP_TITLE } from '@/constants'
import { useUserStore } from '@/stores/user'
import { HunterApiError } from '@/utils/error-code'
import type { LoginRequest } from '@/types/user'

const userStore = useUserStore()
const route = useRoute()
const router = useRouter()

const formRef = ref<FormInstance>()
const submitting = ref(false)
const showTotp = ref(false)

const form = reactive<LoginRequest>({
  username: '',
  password: '',
  totp_code: '',
})

const rules: FormRules<LoginRequest> = {
  username: [{ required: true, message: '请输入用户名', trigger: 'blur' }],
  password: [
    { required: true, message: '请输入密码', trigger: 'blur' },
    { min: 8, max: 128, message: '密码长度 8–128 字符', trigger: 'blur' },
  ],
  totp_code: [
    {
      validator: (_rule, value: string | null | undefined, callback: (error?: Error) => void) => {
        if (!value) {
          callback()
          return
        }
        callback(/^[0-9]{6}$/.test(value) ? undefined : new Error('MFA 动态码为 6 位数字'))
      },
      trigger: 'blur',
    },
  ],
}

const redirectTarget = computed<string>(() => (route.query.redirect as string) ?? '')

async function handleSubmit(): Promise<void> {
  if (!formRef.value) {
    return
  }
  const valid = await formRef.value.validate().catch(() => false)
  if (!valid) {
    return
  }
  submitting.value = true
  try {
    await userStore.login({
      username: form.username,
      password: form.password,
      totp_code: form.totp_code ? form.totp_code : null,
    })
    ElMessage.success('登录成功')
    await router.replace(redirectTarget.value || { name: 'dashboard' })
  } catch (error) {
    const apiError = error instanceof HunterApiError ? error : null
    if (apiError?.code === 1001 && !showTotp.value) {
      // 1001 可能因账号启用了 MFA：提示补充动态码（附录 A 无 MFA 专用错误码）
      showTotp.value = true
      ElMessage.warning('登录失败：请检查用户名/密码，若账号已启用 MFA 请填写 6 位动态码')
    } else {
      ElMessage.error(apiError?.message ?? '登录失败，请稍后重试')
    }
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <div class="login">
    <el-card class="login__card" shadow="always">
      <h2 class="login__title">{{ APP_TITLE }}</h2>
      <p class="login__subtitle">数据采集与分析系统 · 云端运营入口</p>

      <el-form ref="formRef" :model="form" :rules="rules" label-position="top" @submit.prevent="handleSubmit">
        <el-form-item label="用户名" prop="username">
          <el-input v-model="form.username" placeholder="请输入用户名" autocomplete="username" />
        </el-form-item>
        <el-form-item label="密码" prop="password">
          <el-input
            v-model="form.password"
            type="password"
            show-password
            placeholder="请输入密码"
            autocomplete="current-password"
          />
        </el-form-item>
        <el-form-item v-if="showTotp" label="MFA 动态码" prop="totp_code">
          <el-input v-model="form.totp_code" maxlength="6" placeholder="6 位数字（启用 MFA 时必填）" />
        </el-form-item>
        <el-button type="primary" class="login__submit" :loading="submitting" @click="handleSubmit">
          登录
        </el-button>
      </el-form>

      <el-alert
        class="login__hint"
        type="info"
        :closable="false"
        title="登录失败次数受限（暴力破解防护）：多次失败将被服务端临时锁定，请确认凭据后再试。"
      />
    </el-card>
  </div>
</template>

<style scoped>
.login {
  height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #1f2d3d 0%, #2c3e50 100%);
}

.login__card {
  width: 380px;
  padding: 8px 12px;
}

.login__title {
  margin: 0;
  font-size: 20px;
  color: var(--el-text-color-primary);
}

.login__subtitle {
  margin: 6px 0 20px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.login__submit {
  width: 100%;
  margin-top: 4px;
}

.login__hint {
  margin-top: 18px;
}
</style>
