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
 *
 * Crash self-heal (v2 protocol upgrade):
 *  - `onerror`/`onmessageerror` (and an undecodable zero-copy batch) kill the
 *    slot: the worker is terminated and every entry that was in flight on it
 *    is pushed back onto the queue, then a fresh worker is constructed. Tasks
 *    are never lost and never abandoned silently.
 *  - delivery is idempotent at the pool boundary: a `runId:nodeId` pair that
 *    has already been emitted is dropped if it shows up again (the race where
 *    a dead worker's result was already queued on the main-thread event loop
 *    before the crash was observed), so retries never double-commit.
 *  - only {@link CRASH_DEGRADE_THRESHOLD} consecutive slot replacements flip
 *    the pool to the main-thread fallback; one healthy batch resets the
 *    streak, so a single flaky worker self-heals without degrading.
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
  BatchDecodeError,
  decodeResultBatch,
  ownBuffer,
  receiveBatchBinary,
  receiveResult,
  takeTransfers,
  type BatchBinaryOutput,
  type BatchOutput,
  type MainToWorker,
  type NodeId,
  type RunId,
  type TaskError,
  type WorkerToMain,
  type WorkerId,
} from './protocol'

export type PoolOutcomeKind = 'result' | 'error' | 'stale'

export interface PoolOutcome {
  readonly kind: PoolOutcomeKind
  readonly runId: RunId
  readonly nodeId: NodeId
  /** Fresh ArrayBuffer on legacy `batch` results (main thread sole owner). */
  readonly output?: ArrayBuffer
  /** Pre-decoded scalar on the zero-copy `batch-binary` results. */
  readonly value?: number
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
  worker: Worker
  /** Entries currently in flight on this worker, keyed `runId:nodeId`. */
  readonly inflight: Map<string, PendingEntry>
  /** Consecutive crashes/replacements without a healthy batch. */
  failureStreak: number
}

/** A slot must survive this many consecutive replacements before degrading. */
export const CRASH_DEGRADE_THRESHOLD = 3
const MAX_WORKERS = 3
const MEDIUM_ADMIT_QUIET_MS = 16
const LONG_ADMIT_QUIET_MS = 80

function inflightKey(runId: RunId, nodeId: NodeId): string {
  return `${runId}:${nodeId}`
}

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
  /**
   * `runId:nodeId` pairs already emitted to handlers. Crash replay can make a
   * result arrive twice (once from the doomed worker's queued batch, once
   * from the replacement); the second delivery is swallowed here, at the pool
   * boundary, so the coordinator ever sees one outcome per node per run.
   */
  private readonly delivered = new Set<string>()
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
      const slot: WorkerSlot = {
        id,
        worker,
        inflight: new Map(),
        failureStreak: 0,
      }
      worker.onmessage = (event: MessageEvent<WorkerToMain>) => {
        const data = event.data
        if (data.kind === 'ready') return
        this.handleSlotMessage(slot, data)
      }
      // onerror fires on worker crash / unhandled error; onmessageerror
      // covers a poisoned (non-deserializable) channel. Both self-heal.
      worker.onerror = () => this.replaceSlot(slot)
      worker.onmessageerror = () => this.replaceSlot(slot)
      this.slots.push(slot)
      this.pumpQueue()
      return true
    } catch {
      this.enterFallback()
      return false
    }
  }

  /**
   * Crash recovery: terminate the doomed worker, requeue every entry that was
   * in flight on it (canceled ones are filtered on dispatch), construct a
   * replacement with the SAME slot id, and let {@link pumpQueue} reload it.
   * Repeated consecutive failures at/over the threshold degrade wholesale.
   */
  private replaceSlot(slot: WorkerSlot): void {
    if (this.fallbackMode) return
    try {
      slot.worker.terminate()
    } catch {
      // Termination is best effort; the worker is discarded either way.
    }
    // Requeue in flight FIRST, before removing the slot, so an entry whose
    // result raced the crash can still be deduped by `delivered`.
    for (const entry of slot.inflight.values()) {
      if (!this.cancelledRuns.has(entry.runId)) this.queued.push(entry)
    }
    slot.inflight.clear()
    const index = this.slots.indexOf(slot)
    if (index >= 0) this.slots.splice(index, 1)

    const streak = slot.failureStreak + 1
    if (streak >= CRASH_DEGRADE_THRESHOLD) {
      // Threshold reached: stop replacing workers for this slot position.
      if (this.slots.length === 0) this.enterFallback()
      return
    }
    this.createWorkerWithStreak(slot.id, streak)
  }

  private createWorkerWithStreak(id: WorkerId, failureStreak: number): void {
    if (this.createWorker(id)) {
      const fresh = this.slots.find((entry) => entry.id === id)
      if (fresh) fresh.failureStreak = failureStreak
    }
  }

  /** Load as many queued entries as idle slots can take. */
  private pumpQueue(): void {
    while (this.queued.length > 0) {
      const slot = this.slots.find((entry) => entry.inflight.size === 0)
      if (!slot) return
      const entry = this.queued.shift()
      if (!entry) return
      if (this.cancelledRuns.has(entry.runId)) {
        this.emit({ kind: 'stale', runId: entry.runId, nodeId: entry.nodeId })
        continue
      }
      this.postTask(slot, entry)
    }
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
    // A slot may hold multiple in-flight entries only after crash replay has
    // requeued work while every slot was occupied; otherwise one task per
    // slot is posted, matching the 8 ms worker batching cadence.
    const idleSlot = this.slots.find((slot) => slot.inflight.size === 0)
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
    slot.inflight.set(inflightKey(entry.runId, entry.nodeId), entry)
    const message: MainToWorker = {
      kind: 'task',
      runId: entry.runId,
      nodeId: entry.nodeId,
      fnId: spec.fnId,
      input: ownBuffer(makeInput(spec)),
    }
    slot.worker.postMessage(message, takeTransfers(message))
  }

  /** Emit at most once per `runId:nodeId`; returns whether it was accepted. */
  private deliverOnce(outcome: PoolOutcome): boolean {
    const key = inflightKey(outcome.runId, outcome.nodeId)
    if (this.delivered.has(key)) return false
    this.delivered.add(key)
    // The dedupe set only needs live + recently-retired runs; trim entries
    // for runs whose bookkeeping is later purged via purgeRun().
    this.emit(outcome)
    return true
  }

  /** Resolve every batch-mentioned entry off the slot's in-flight map. */
  private settleSlot(slot: WorkerSlot, notices: readonly PendingEntry[]): void {
    for (const notice of notices) slot.inflight.delete(inflightKey(notice.runId, notice.nodeId))
    if (slot.inflight.size === 0) this.pumpQueue()
  }

  private handleSlotMessage(slot: WorkerSlot, message: BatchOutput | BatchBinaryOutput): void {
    // One healthy batch proves the worker is alive: reset the crash streak.
    slot.failureStreak = 0
    const notices: PendingEntry[] = []
    if (message.kind === 'batch') {
      for (const result of message.results) {
        notices.push({ runId: result.runId, nodeId: result.nodeId })
        if (this.cancelledRuns.has(result.runId)) continue
        const received = receiveResult(result)
        this.deliverOnce({
          kind: 'result',
          runId: received.runId,
          nodeId: received.nodeId,
          output: received.output,
          computeMs: received.computeMs,
        })
      }
      for (const error of message.errors) {
        notices.push({ runId: error.runId, nodeId: error.nodeId })
        if (this.cancelledRuns.has(error.runId)) continue
        const dto: TaskError = error
        this.deliverOnce({
          kind: 'error',
          runId: dto.runId,
          nodeId: dto.nodeId,
          error: dto.error,
        })
      }
      for (const notice of message.stale) {
        notices.push({ runId: notice.runId, nodeId: notice.nodeId })
        this.emit({ kind: 'stale', runId: notice.runId, nodeId: notice.nodeId })
      }
      this.settleSlot(slot, notices)
      return
    }

    // Zero-copy channel: take sole ownership of the one transferred buffer,
    // decode it with strict length validation, and release it immediately:
    // after decode, only plain numbers remain in scope — no DataView and no
    // reference to the ArrayBuffer survives this handler call.
    const received = receiveBatchBinary(message)
    const payload = received.payload
    let decoded: ReturnType<typeof decodeResultBatch>
    try {
      decoded = decodeResultBatch(payload)
    } catch (error) {
      if (error instanceof BatchDecodeError) {
        // Corrupt protocol from this worker: treat exactly like a crash so
        // its in-flight tasks replay on a fresh worker instead of vanishing.
        this.replaceSlot(slot)
        return
      }
      throw error
    }
    for (const entry of decoded.entries) {
      notices.push({ runId: decoded.runId, nodeId: entry.nodeId })
      if (this.cancelledRuns.has(decoded.runId)) continue
      this.deliverOnce({
        kind: 'result',
        runId: decoded.runId,
        nodeId: entry.nodeId,
        value: entry.value,
        computeMs: entry.computeMs,
      })
    }
    for (const error of received.errors) {
      notices.push({ runId: error.runId, nodeId: error.nodeId })
      if (this.cancelledRuns.has(error.runId)) continue
      this.deliverOnce({
        kind: 'error',
        runId: error.runId,
        nodeId: error.nodeId,
        error: error.error,
      })
    }
    for (const notice of received.stale) {
      notices.push({ runId: notice.runId, nodeId: notice.nodeId })
      this.emit({ kind: 'stale', runId: notice.runId, nodeId: notice.nodeId })
    }
    this.settleSlot(slot, notices)
  }

  /** Best-effort cancel: flips adoption flags, never interrupts in-flight sync. */
  cancel(runId: RunId): void {
    this.cancelledRuns.add(runId)
    if (this.fallbackMode) return
    const liveWorkers = this.slots.filter((slot) => {
      for (const entry of slot.inflight.values()) {
        if (entry.runId === runId) return true
      }
      return false
    })
    if (liveWorkers.length === 0) return
    const message: MainToWorker = { kind: 'cancel', runIds: [runId] }
    for (const slot of liveWorkers) {
      slot.worker.postMessage(message, takeTransfers(message))
    }
  }

  /** Forget cancellation/dedupe bookkeeping for a fully retired run. */
  purgeRun(runId: RunId): void {
    this.cancelledRuns.delete(runId)
    const prefix = `${runId}:`
    for (const key of this.delivered) {
      if (key.startsWith(prefix)) this.delivered.delete(key)
    }
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
