/**
 * compute Worker：按函数 ID 从注册表取纯函数执行。
 * - 结果本地攒批，约每 8ms 回传一次，避免逐条 postMessage；
 * - cancel 只推进“采纳标记”adoptedRunId，不中断在途同步计算
 *   （同步 CPU 循环无法从外部抢占，其结果会带旧 runId，由主线程提交层丢弃）。
 */

import { buildSyntheticDomain } from '../lib/domain/functions'
import type { FnOutput, MainToWorker, NodeResult, WorkerToMain } from '../lib/scheduler/protocol'

const { registry } = buildSyntheticDomain()

/** 采纳标记：只采纳 runId >= adoptedRunId 的任务与结果。 */
let adoptedRunId = 0

let buffer: NodeResult[] = []
let flushTimer: number | null = null

const FLUSH_MS = 8

const ctx = self as unknown as {
  onmessage: ((event: MessageEvent<MainToWorker>) => void) | null
  postMessage(message: WorkerToMain, transfer?: Transferable[]): void
}

function scheduleFlush(): void {
  if (flushTimer !== null) return
  flushTimer = setTimeout(() => {
    flushTimer = null
    flush()
  }, FLUSH_MS)
}

function flush(): void {
  if (buffer.length === 0) return
  const batch = buffer
  buffer = []
  ctx.postMessage({ type: 'result', results: batch })
}

function postError(runId: number, nodeId: string, error: unknown): void {
  const message = error instanceof Error ? error.message : String(error)
  ctx.postMessage({ type: 'error', runId, nodeId, message })
}

ctx.onmessage = (event: MessageEvent<MainToWorker>) => {
  const message = event.data
  if (message.type === 'cancel') {
    // 只切换采纳标记；在途同步计算继续跑完，其结果由主线程按 runId 丢弃。
    adoptedRunId = Math.max(adoptedRunId, message.runId)
    return
  }
  if (message.runId < adoptedRunId) return // 旧轮任务，不启动
  const fn = registry.get(message.fnId)
  if (!fn) {
    postError(message.runId, message.nodeId, `unknown fn: ${message.fnId}`)
    return
  }
  const started = performance.now()
  try {
    const output: FnOutput = fn.run(message.input)
    buffer.push({
      nodeId: message.nodeId,
      runId: message.runId,
      output,
      computeMs: performance.now() - started,
    })
    scheduleFlush()
  } catch (error) {
    postError(message.runId, message.nodeId, error)
  }
}
