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
  decodeResultBatch,
  encodeResultBatch,
  ownBuffer,
  receiveTask,
  takeTransfers,
  type BatchOutput,
  type BinaryBatchOutput,
  type MainToWorker,
  type RunId,
  type StaleNotice,
  type TaskError,
} from '../lib/scheduler/protocol'

/**
 * `lib.dom` is the configured TS lib for app code; redeclaring the narrow
 * worker global surface locally avoids a lib conflict while keeping full type
 * safety on the messages actually used.
 */
interface ComputeWorkerScope {
  readonly name: string
  postMessage(
    message:
      | BatchOutput
      | BinaryBatchOutput
      | { kind: 'ready'; workerId: number },
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
/**
 * Pending successes as plain decoded scalars. The per-task 8-byte output is
 * NOT retained: its f64 value is read immediately and the buffer discarded,
 * so at flush time the whole group can be encoded into ONE transferable
 * buffer (see {@link encodeResultBatch}).
 */
const resultBuffer: {
  readonly runId: RunId
  readonly nodeId: number
  readonly value: number
  readonly computeMs: number
}[] = []
const errorBuffer: TaskError[] = []
const staleBuffer: StaleNotice[] = []
let flushTimer: ReturnType<typeof setTimeout> | null = null

const FLUSH_INTERVAL_MS = 8

function flush(): void {
  flushTimer = null
  if (resultBuffer.length === 0 && errorBuffer.length === 0 && staleBuffer.length === 0) {
    return
  }
  const results = resultBuffer.splice(0, resultBuffer.length)
  const errors = errorBuffer.splice(0, errorBuffer.length)
  const stale = staleBuffer.splice(0, staleBuffer.length)

  // One envelope per run id: a run group with successes goes through the
  // zero-copy binary channel (single ArrayBuffer transfer); its errors and
  // stale notices ride alongside as plain DTOs. Groups without successes use
  // the legacy narrow-DTO envelope with an empty `results` array.
  const runIds: RunId[] = []
  const groups = new Map<RunId, typeof results>()
  for (const entry of results) {
    let group = groups.get(entry.runId)
    if (!group) {
      group = []
      groups.set(entry.runId, group)
      runIds.push(entry.runId)
    }
    group.push(entry)
  }
  for (const error of errors) {
    if (!groups.has(error.runId)) {
      groups.set(error.runId, [])
      runIds.push(error.runId)
    }
  }
  for (const notice of stale) {
    if (!groups.has(notice.runId)) {
      groups.set(notice.runId, [])
      runIds.push(notice.runId)
    }
  }

  for (const runId of runIds) {
    const groupResults = groups.get(runId) ?? []
    const groupErrors = errors.filter((error) => error.runId === runId)
    const groupStale = stale.filter((notice) => notice.runId === runId)
    if (groupResults.length > 0) {
      const payload = encodeResultBatch(
        runId,
        groupResults.map((entry) => ({
          nodeId: entry.nodeId,
          value: entry.value,
          computeMs: entry.computeMs,
        })),
      )
      // Self-test of the symmetry guaranteed by the layout contract: the
      // encoded buffer decodes back to the same run id and entry count.
      const decoded = decodeResultBatch(payload)
      if (decoded.runId !== runId || decoded.results.length !== groupResults.length) {
        throw new Error('binary batch encode/decode symmetry check failed')
      }
      const message: BinaryBatchOutput = {
        kind: 'binaryBatch',
        runId,
        payload: ownBuffer(payload),
        errors: groupErrors,
        stale: groupStale,
      }
      ;(scope as ComputeWorkerScope).postMessage(
        message,
        takeTransfers(message),
      )
    } else {
      const message: BatchOutput = {
        kind: 'batch',
        results: [],
        errors: groupErrors,
        stale: groupStale,
      }
      ;(scope as ComputeWorkerScope).postMessage(
        message,
        takeTransfers(message),
      )
    }
  }
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
        runId: task.runId,
        nodeId: task.nodeId,
        // Read the scalar now and drop the 8-byte buffer; the batch encoder
        // allocates the single transferable buffer at flush time.
        value: new Float64Array(output)[0] ?? 0,
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
