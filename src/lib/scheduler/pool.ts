/**
 * Resident compute-worker pool with a self-contained main-thread fallback.
 *
 *  - target size: min(3, max(1, hardwareConcurrency - 1)); on engines without
 *    hardwareConcurrency the conservative default of 2 is used
 *  - workers are module workers created via the Vite `new URL(...)` pattern and
 *    are warmed up during the first idle period
 *  - if a Worker cannot be constructed (e.g. CSP without worker-src), the pool
 *    flips to `fallback` mode and runs tasks on the main thread:
 *      fast   -> inline inside the coordinator's 4 ms slice (fast tasks are
 *                never routed here, so this only covers accidental routing)
 *      medium -> one chunked task per background-priority macrotask, admitted
 *                only while no recent input/scroll activity
 *      long   -> same chunked path, additionally gated behind an 80 ms quiet
 *                window so a 50–200 ms black box is never launched right before
 *                the next keystroke; cooperative ticks abort on cancel
 */

import {
  FUNCTIONS,
  type FunctionTier,
  makeInput,
  runFunctionChunked,
  runFunctionSync,
  ChunkAbortedError,
  type FunctionSpec,
} from '../domain/functions'
import {
  ownBuffer,
  receiveResult,
  takeTransfers,
  type BatchOutput,
  type MainToWorker,
  type NodeId,
  type RunId,
  type TaskError,
  type WorkerId,
} from './protocol'

export type PoolOutcomeKind = 'result' | 'error' | 'stale'

export interface PoolOutcome {
  readonly kind: PoolOutcomeKind
  readonly runId: RunId
  readonly nodeId: NodeId
  /** Fresh ArrayBuffer on `result` (main thread is the sole owner). */
  readonly output?: ArrayBuffer
  readonly computeMs?: number
  readonly error?: { readonly name: string; readonly message: string }
}

export type PoolOutcomeHandler = (outcome: PoolOutcome) => void

interface PendingEntry {
  readonly runId: RunId
  readonly nodeId: NodeId
}

interface WorkerSlot {
  readonly id: WorkerId
  readonly worker: Worker
  busy: boolean
  current: PendingEntry | null
}

const MAX_WORKERS = 3
const MEDIUM_ADMIT_QUIET_MS = 16
const LONG_ADMIT_QUIET_MS = 80

function targetPoolSize(): number {
  const concurrency = navigator.hardwareConcurrency
  if (typeof concurrency !== 'number' || concurrency <= 0) return 2
  return Math.min(MAX_WORKERS, Math.max(1, concurrency - 1))
}

function postBackgroundTask(callback: () => void): void {
  const schedulerWithPost = globalThis.scheduler as
    | { postTask?(cb: () => void): void }
    | undefined
  if (schedulerWithPost?.postTask) {
    schedulerWithPost.postTask(callback)
  } else {
    setTimeout(callback, 0)
  }
}

export class ComputePool {
  private readonly slots: WorkerSlot[] = []
  private readonly queued: PendingEntry[] = []
  private readonly cancelledRuns = new Set<RunId>()
  private readonly handlers = new Set<PoolOutcomeHandler>()
  private fallbackMode = false
  private fallbackDraining = false
  private lastInteractionAt = 0
  private warmStarted = false

  constructor(private readonly specs: readonly FunctionSpec[] = FUNCTIONS) {}

  /** Input/scroll activity notification used by fallback admission gating. */
  notifyInteraction(): void {
    this.lastInteractionAt = performance.now()
  }

  get isFallback(): boolean {
    return this.fallbackMode
  }

  get workerCount(): number {
    return this.fallbackMode ? 0 : this.slots.length
  }

  onOutcome(handler: PoolOutcomeHandler): () => void {
    this.handlers.add(handler)
    return () => this.handlers.delete(handler)
  }

  private emit(outcome: PoolOutcome): void {
    for (const handler of this.handlers) handler(outcome)
  }

  /** Construct all workers during idle time; failures flip fallback mode. */
  warmUp(): void {
    if (this.warmStarted) return
    this.warmStarted = true
    const start = () => {
      const target = targetPoolSize()
      for (let id = this.slots.length; id < target; id += 1) {
        if (!this.createWorker(id)) break
      }
    }
    const idle = globalThis.requestIdleCallback
    if (typeof idle === 'function') {
      idle(start, { timeout: 1000 })
    } else {
      setTimeout(start, 0)
    }
  }

  private createWorker(id: WorkerId): boolean {
    try {
      const worker = new Worker(new URL('../../workers/compute.worker.ts', import.meta.url), {
        type: 'module',
        name: `compute-${id}`,
      })
      worker.onmessage = (event: MessageEvent<BatchOutput | { kind: 'ready' }>) => {
        if (event.data.kind === 'ready') return
        this.handleBatch(event.data, id)
      }
      worker.onerror = () => this.killSlot(id)
      this.slots.push({ id, worker, busy: false, current: null })
      return true
    } catch {
      this.enterFallback()
      return false
    }
  }

  private killSlot(id: WorkerId): void {
    const slot = this.slots.find((entry) => entry.id === id)
    if (!slot) return
    try {
      slot.worker.terminate()
    } catch {
      // Termination is best effort; the slot is discarded either way.
    }
    const index = this.slots.indexOf(slot)
    if (index >= 0) this.slots.splice(index, 1)
    if (slot.current) {
      this.queued.push(slot.current)
      slot.current = null
    }
    if (this.slots.length === 0) this.enterFallback()
  }

  private enterFallback(): void {
    if (this.fallbackMode) return
    this.fallbackMode = true
    for (const slot of this.slots) {
      try {
        slot.worker.terminate()
      } catch {
        // ignore
      }
    }
    this.slots.length = 0
    this.drainFallbackQueue()
  }

  /** Dispatch a node; resolves to `false` only if the run was already dead. */
  dispatch(runId: RunId, nodeId: NodeId): boolean {
    if (this.cancelledRuns.has(runId)) return false
    if (this.fallbackMode) {
      this.queued.push({ runId, nodeId })
      this.drainFallbackQueue()
      return true
    }
    const idleSlot = this.slots.find((slot) => !slot.busy)
    if (idleSlot) {
      this.postTask(idleSlot, { runId, nodeId })
    } else if (this.slots.length < targetPoolSize()) {
      const id = this.slots.length
      if (this.createWorker(id)) {
        const slot = this.slots[id]
        if (slot) this.postTask(slot, { runId, nodeId })
        return true
      }
      // Construction failed: enterFallback() already flipped the mode.
      this.queued.push({ runId, nodeId })
      this.drainFallbackQueue()
    } else {
      this.queued.push({ runId, nodeId })
    }
    return true
  }

  private postTask(slot: WorkerSlot, entry: PendingEntry): void {
    const spec = this.specs[entry.nodeId]
    if (!spec) throw new Error(`unknown node ${entry.nodeId}`)
    slot.busy = true
    slot.current = entry
    const message: MainToWorker = {
      kind: 'task',
      runId: entry.runId,
      nodeId: entry.nodeId,
      fnId: spec.fnId,
      input: ownBuffer(makeInput(spec)),
    }
    slot.worker.postMessage(message, takeTransfers(message))
  }

  private freeSlot(slot: WorkerSlot): void {
    slot.busy = false
    slot.current = null
    while (this.queued.length > 0) {
      const entry = this.queued.shift()
      if (!entry) break
      if (this.cancelledRuns.has(entry.runId)) continue
      this.postTask(slot, entry)
      return
    }
  }

  private handleBatch(batch: BatchOutput, workerId: WorkerId): void {
    const slot = this.slots.find((entry) => entry.id === workerId)
    const touchedNodes = new Set<NodeId>()
    for (const result of batch.results) {
      touchedNodes.add(result.nodeId)
      if (this.cancelledRuns.has(result.runId)) continue
      const received = receiveResult(result)
      this.emit({
        kind: 'result',
        runId: received.runId,
        nodeId: received.nodeId,
        output: received.output,
        computeMs: received.computeMs,
      })
    }
    for (const error of batch.errors) {
      touchedNodes.add(error.nodeId)
      if (this.cancelledRuns.has(error.runId)) continue
      const dto: TaskError = error
      this.emit({
        kind: 'error',
        runId: dto.runId,
        nodeId: dto.nodeId,
        error: dto.error,
      })
    }
    for (const notice of batch.stale) {
      touchedNodes.add(notice.nodeId)
      this.emit({
        kind: 'stale',
        runId: notice.runId,
        nodeId: notice.nodeId,
      })
    }
    if (slot) {
      // One in-flight task per slot: every node mentioned in this batch
      // belongs to the slot's current task; release it once after dispatch.
      if (slot.current && touchedNodes.has(slot.current.nodeId)) {
        this.freeSlot(slot)
      }
    }
  }

  /** Best-effort cancel: flips adoption flags, never interrupts in-flight sync. */
  cancel(runId: RunId): void {
    this.cancelledRuns.add(runId)
    if (this.fallbackMode) return
    const liveWorkers = this.slots.filter((slot) =>
      slot.current ? slot.current.runId === runId : false,
    )
    if (liveWorkers.length === 0) return
    const message: MainToWorker = { kind: 'cancel', runIds: [runId] }
    for (const slot of liveWorkers) {
      slot.worker.postMessage(message, takeTransfers(message))
    }
  }

  /** Forget cancellation bookkeeping for a run whose retirement is complete. */
  purgeRun(runId: RunId): void {
    this.cancelledRuns.delete(runId)
  }

  /* ---------------- main-thread fallback ---------------- */

  private drainFallbackQueue(): void {
    if (this.fallbackDraining) return
    this.fallbackDraining = true
    postBackgroundTask(() => {
      this.fallbackDraining = false
      const entry = this.nextAdmittedEntry()
      if (!entry) {
        if (this.queued.some((queued) => !this.cancelledRuns.has(queued.runId))) {
          // Waiting for the interaction-quiet window; retry on a later frame.
          requestAnimationFrame(() => this.drainFallbackQueue())
        }
        return
      }
      void this.runFallback(entry)
    })
  }

  private nextAdmittedEntry(): PendingEntry | null {
    const now = performance.now()
    const quietFor = now - this.lastInteractionAt
    while (this.queued.length > 0) {
      const entry = this.queued.shift()
      if (!entry) return null
      if (this.cancelledRuns.has(entry.runId)) {
        this.emit({ kind: 'stale', runId: entry.runId, nodeId: entry.nodeId })
        continue
      }
      const tier: FunctionTier | undefined = this.specs[entry.nodeId]?.tier
      if (tier === 'long' && quietFor < LONG_ADMIT_QUIET_MS) {
        this.queued.unshift(entry)
        return null
      }
      if (tier === 'medium' && quietFor < MEDIUM_ADMIT_QUIET_MS) {
        this.queued.unshift(entry)
        return null
      }
      return entry
    }
    return null
  }

  private async runFallback(entry: PendingEntry): Promise<void> {
    const spec = this.specs[entry.nodeId]
    if (!spec) {
      this.emit({
        kind: 'error',
        runId: entry.runId,
        nodeId: entry.nodeId,
        error: { name: 'LookupError', message: `unknown node ${entry.nodeId}` },
      })
      this.drainFallbackQueue()
      return
    }
    const tick = (): void => {
      if (this.cancelledRuns.has(entry.runId)) throw new ChunkAbortedError()
    }
    const startedAt = performance.now()
    try {
      const input = makeInput(spec)
      let output: ArrayBuffer
      if (spec.tier === 'fast') {
        // Fast fallback work is short enough to finish inside the current slice.
        output = runFunctionSync(spec, input)
      } else {
        output = await runFunctionChunked(spec, input, tick)
      }
      this.emit({
        kind: 'result',
        runId: entry.runId,
        nodeId: entry.nodeId,
        output,
        computeMs: performance.now() - startedAt,
      })
    } catch (error) {
      if (error instanceof ChunkAbortedError) {
        this.emit({ kind: 'stale', runId: entry.runId, nodeId: entry.nodeId })
      } else {
        const normalized =
          error instanceof Error
            ? { name: error.name, message: error.message }
            : { name: 'UnknownError', message: String(error) }
        this.emit({
          kind: 'error',
          runId: entry.runId,
          nodeId: entry.nodeId,
          error: normalized,
        })
      }
    } finally {
      this.drainFallbackQueue()
    }
  }
}
