/**
 * 文件摘要计算（上传前置完整性校验）
 *
 * 契约依据：
 * - ota-service.yaml：版本创建必须提交 `package_md5` + `package_sha256`（服务端 publish 时校验，
 *   失败返回 6001），前端在创建版本/确认上传前必须本地计算；
 * - data-collector.yaml：`POST /api/v1/data/files/complete` 需提交 md5 与 sha256。
 * 实现：MD5 用 spark-md5（浏览器无 WebCrypto MD5）；SHA-256 用 WebCrypto（需安全上下文 https/localhost）。
 */
import SparkMD5 from 'spark-md5'

/** 读取 Blob 为 ArrayBuffer */
async function readBuffer(blob: Blob): Promise<ArrayBuffer> {
  return blob.arrayBuffer()
}

/** 计算 SHA-256（十六进制小写） */
export async function computeSha256(blob: Blob): Promise<string> {
  const buffer = await readBuffer(blob)
  const digest = await globalThis.crypto.subtle.digest('SHA-256', buffer)
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, '0')).join('')
}

/** 计算 MD5（十六进制小写） */
export async function computeMd5(blob: Blob): Promise<string> {
  const buffer = await readBuffer(blob)
  const spark = new SparkMD5.ArrayBuffer()
  spark.append(buffer)
  return spark.end()
}

/** 同时计算 MD5 与 SHA-256（上传前一次性调用，避免重复读文件） */
export async function computeFileDigests(
  file: File,
): Promise<{ md5: string; sha256: string; sizeBytes: number }> {
  const [md5, sha256] = await Promise.all([computeMd5(file), computeSha256(file)])
  return { md5, sha256, sizeBytes: file.size }
}

/** 触发浏览器下载（导出场景 / 报告 / 录像等预签名 URL） */
export function downloadByUrl(url: string, fileName?: string): void {
  const anchor = document.createElement('a')
  anchor.href = url
  if (fileName) {
    anchor.download = fileName
  }
  anchor.rel = 'noopener'
  anchor.target = '_blank'
  document.body.appendChild(anchor)
  anchor.click()
  anchor.remove()
}
