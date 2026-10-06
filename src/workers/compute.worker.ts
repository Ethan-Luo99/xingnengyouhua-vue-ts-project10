/**
 * Compute worker.
 *
 * Registered functions are the pure, isomorphic implementations in
 * `src/lib/domain/functions.ts`; this file never receives function closures.
 *
 * Cancellation semantics (hard constraint #4 and design doc §五.7):
 *  - a `cancel` message only flips an adoption flag per runId
 *  - the currently executing synchronous CPU loop is NOT interrupted
 *  - when it finishes, its buffer is discarded (no transfer) and a `stale`
 *    notice is sent instead so the pool can reuse the slot
 *
 * Results are buffered locally and flushed roughly every 8 ms.
 */

import { FUNCTIONS, runFunctionSync, type FunctionSpec } from '../lib/domain/functions'
import {
  ownBuffer,
  receiveTask,
  takeTransfers,
  type BatchOutput,
  type MainToWorker,
  type RunId,
  type StaleNotice,
  type TaskError,
  type TaskResult,
} from '../lib/scheduler/protocol'

/**
 * `lib.dom` is the configured TS lib for app code; redeclaring the narrow
 * worker global surface locally avoids a lib conflict while keeping full type
 * safety on the messages actually used.
 */
interface ComputeWorkerScope {
  readonly name: string
  postMessage(
    message: BatchOutput | { kind: 'ready'; workerId: number },
    transfer?: Transferable[],
  ): void
  addEventListener(type: 'message', listener: (event: MessageEvent) => void): void
}

const scope = globalThis as unknown as ComputeWorkerScope & {
  name?: string
}

const workerIdMatch = /(\d+)$/.exec(scope.name ?? '')
const workerId = workerIdMatch ? Number(workerIdMatch[1]) : 0

const registry = new Map<string, FunctionSpec>()
for (const spec of FUNCTIONS) registry.set(spec.fnId, spec)

const adoptedRuns = new Set<RunId>()
const resultBuffer: TaskResult[] = []
const errorBuffer: TaskError[] = []
const staleBuffer: StaleNotice[] = []
let flushTimer: ReturnType<typeof setTimeout> | null = null

const FLUSH_INTERVAL_MS = 8

function flush(): void {
  flushTimer = null
  if (resultBuffer.length === 0 && errorBuffer.length === 0 && staleBuffer.length === 0) {
    return
  }
  const message: BatchOutput = {
    kind: 'batch',
    results: resultBuffer.splice(0, resultBuffer.length),
    errors: errorBuffer.splice(0, errorBuffer.length),
    stale: staleBuffer.splice(0, staleBuffer.length),
  }
  ;(scope as ComputeWorkerScope).postMessage(message, takeTransfers(message))
}

function scheduleFlush(): void {
  if (flushTimer !== null) return
  flushTimer = setTimeout(flush, FLUSH_INTERVAL_MS)
}

function execute(message: MainToWorker): void {
  if (message.kind === 'shutdown') {
    flush()
    return
  }
  if (message.kind === 'cancel') {
    for (const runId of message.runIds) adoptedRuns.delete(runId)
    return
  }

  const task = receiveTask(message)
  adoptedRuns.add(task.runId)
  const spec = registry.get(task.fnId)
  const startedAt = performance.now()
  if (!spec) {
    errorBuffer.push({
      kind: 'error',
      runId: task.runId,
      nodeId: task.nodeId,
      error: { name: 'LookupError', message: `unregistered function ${task.fnId}` },
    })
    scheduleFlush()
    return
  }
  try {
    // Synchronous, non-preemptible compute. A cancel arriving during this call
    // cannot stop it; it only changes what happens to the result below.
    const output = runFunctionSync(spec, task.input)
    if (adoptedRuns.has(task.runId)) {
      resultBuffer.push({
        kind: 'result',
        runId: task.runId,
        nodeId: task.nodeId,
        output: ownBuffer(output),
        computeMs: performance.now() - startedAt,
      })
    } else {
      staleBuffer.push({ runId: task.runId, nodeId: task.nodeId })
    }
  } catch (error) {
    if (adoptedRuns.has(task.runId)) {
      errorBuffer.push({
        kind: 'error',
        runId: task.runId,
        nodeId: task.nodeId,
        error:
          error instanceof Error
            ? { name: error.name, message: error.message }
            : { name: 'UnknownError', message: String(error) },
      })
    } else {
      staleBuffer.push({ runId: task.runId, nodeId: task.nodeId })
    }
  }
  adoptedRuns.delete(task.runId)
  scheduleFlush()
}

scope.addEventListener('message', (event: MessageEvent<MainToWorker>) => {
  execute(event.data)
})

scope.postMessage({ kind: 'ready', workerId })
