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
  BinaryProtocolError,
  decodeResultBatch,
  ownBuffer,
  takeTransfers,
  type BatchOutput,
  type BinaryBatchOutput,
  type MainToWorker,
  type NodeId,
  type RunId,
  type WorkerToMain,
  type WorkerId,
} from './protocol'

export type PoolOutcomeKind = 'result' | 'error' | 'stale'

export interface PoolOutcome {
  readonly kind: PoolOutcomeKind
  readonly runId: RunId
  readonly nodeId: NodeId
  /** Decoded scalar on `result` (the transfer buffer is already released). */
  readonly value?: number
  /** Kept for backwards-compatible callers; absent on the binary path. */
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
/**
 * Consecutive worker-death threshold: only after this many slot crashes in a
 * row (with no intervening healthy worker response) does the pool give up on
 * workers entirely and flip to main-thread fallback. Below the threshold a
 * dead worker is transparently replaced and its in-flight task re-enqueued.
 */
const CONSECUTIVE_CRASH_LIMIT = 3
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
  /**
   * Idempotency ledger: `${runId}:${nodeId}` of every terminal outcome this
   * pool has already emitted for the worker path. If a crashed worker's
   * result was in fact delivered right before the crash (the re-run race),
   * the replacement's duplicate outcome is swallowed here. Main-thread
   * fallback emits before a task is forgotten and stays a single executor, so
   * it cannot duplicate; the ledger therefore only guards worker deliveries.
   */
  private readonly deliveredKeys = new Set<string>()
  /** Worker deaths since the last healthy worker delivery. */
  private consecutiveCrashes = 0
  private runPriority: (runId: RunId) => number = () => 0

  constructor(private readonly specs: readonly FunctionSpec[] = FUNCTIONS) {}

  /** Input/scroll activity notification used by fallback admission gating. */
  notifyInteraction(): void {
    this.lastInteractionAt = performance.now()
  }

  /**
   * Wake the dispatch pump without submitting anything. Used when a paused
   * run is resumed: its worker tasks may all be sitting in the queue with no
   * slot-completion event imminent (e.g. a freshly replaced worker that
   * already said ready while the run was paused), so this closes the
   * otherwise-harmless lost-wakeup race.
   */
  kick(): void {
    if (this.fallbackMode) this.drainFallbackQueue()
    else this.pumpQueued()
  }

  /**
   * Live-run priority hook supplied by the coordinator: returns 0 for the
   * foreground run, 1 for a paused-but-live run, and >=2 for unknown/retired
   * runs. The FIFO queue is otherwise fair, but a foreground B must not be
   * stuck behind A tasks that were enqueued before the pause, and retired
   * runs' leftovers must not occupy slots at all. Lower wins; ties keep FIFO.
   */
  setRunPriority(probe: (runId: RunId) => number): void {
    this.runPriority = probe
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
      worker.onmessage = (event: MessageEvent<WorkerToMain>) => {
        if (event.data.kind === 'ready') {
          // A live replacement resetting the crash streak happens here, not
          // on construction, so a worker that dies instantly still counts.
          this.consecutiveCrashes = 0
          this.pumpQueued()
          return
        }
        if (event.data.kind === 'binaryBatch') {
          this.handleBinaryBatch(event.data, id)
        } else {
          this.handleBatch(event.data, id)
        }
      }
      // onerror covers an uncaught worker exception or a dead runtime; the
      // task's result is presumed lost, so self-heal by replacing the slot.
      worker.onerror = () => this.killAndReplaceSlot(id)
      this.slots.push({ id, worker, busy: false, current: null })
      return true
    } catch {
      this.enterFallback()
      return false
    }
  }

  /**
   * Tear down a dead slot, re-enqueue its in-flight task, and either spawn a
   * replacement (the normal self-heal path) or degrade wholesale once the
   * consecutive-crash threshold is exceeded.
   */
  private killAndReplaceSlot(id: WorkerId): void {
    const slot = this.slots.find((entry) => entry.id === id)
    if (!slot) return
    try {
      slot.worker.terminate()
    } catch {
      // Termination is best effort; the slot is discarded either way.
    }
    const index = this.slots.indexOf(slot)
    if (index >= 0) this.slots.splice(index, 1)
    this.consecutiveCrashes += 1
    if (slot.current) {
      // Re-run from the same coordinates: runId+nodeId are unchanged, which
      // is exactly what the idempotency ledger keys on. Duplicate delivery
      // is impossible if the original result arrives late (or already did);
      // only a genuinely undelivered task reaches the handler twice.
      this.queued.push(slot.current)
      slot.current = null
    }
    if (this.consecutiveCrashes >= CONSECUTIVE_CRASH_LIMIT) {
      this.enterFallback()
      return
    }
    // Same slot id: nothing in the coordinator ever identifies workers by
    // object identity, only by the stable index.
    if (!this.createWorker(id)) {
      // Construction failure flips fallback outright.
      return
    }
    this.pumpQueued()
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
      const id = this.missingSlotId()
      if (id === null) {
        this.queued.push({ runId, nodeId })
      } else if (this.createWorker(id)) {
        const slot = this.slots.find((entry) => entry.id === id)
        if (slot) this.postTask(slot, { runId, nodeId })
        else this.queued.push({ runId, nodeId })
      } else {
        // Construction failed: enterFallback() already flipped the mode.
        this.queued.push({ runId, nodeId })
        this.drainFallbackQueue()
      }
    } else {
      this.queued.push({ runId, nodeId })
    }
    return true
  }

  /** Lowest stable slot index without a live worker, or null when full. */
  private missingSlotId(): WorkerId | null {
    for (let id = 0; id < targetPoolSize(); id += 1) {
      if (!this.slots.some((slot) => slot.id === id)) return id
    }
    return null
  }

  /** Hand queued entries to every idle live slot. */
  private pumpQueued(): void {
    if (this.fallbackMode) return
    for (const slot of this.slots) {
      if (slot.busy) continue
      const entry = this.takeNextQueued()
      if (entry) this.postTask(slot, entry)
    }
  }

  /**
   * Remove and return the highest-priority queued entry. Retired runs are
   * dropped (stale notice emitted so their accounting closes); among the
   * remainder the foreground run wins over a paused live run, FIFO breaking
   * ties. Scanning the queue is bounded by its own length and a pump runs at
   * most once per freed slot, so total scan work stays proportional to
   * enqueue/dequeue volume.
   */
  private takeNextQueued(): PendingEntry | null {
    let bestIndex = -1
    let bestPriority = Number.POSITIVE_INFINITY
    for (let index = 0; index < this.queued.length; index += 1) {
      const entry = this.queued[index]
      if (!entry) continue
      if (this.cancelledRuns.has(entry.runId)) {
        this.queued.splice(index, 1)
        this.emit({ kind: 'stale', runId: entry.runId, nodeId: entry.nodeId })
        return this.takeNextQueued()
      }
      const priority = this.runPriority(entry.runId)
      if (priority < bestPriority) {
        bestPriority = priority
        bestIndex = index
      }
    }
    if (bestIndex < 0) return null
    if (bestPriority >= 2) {
      // Nothing live remains in the queue; retire every leftover entry so it
      // cannot pin a slot after a future kick.
      for (const dead of this.queued.splice(0)) {
        this.emit({ kind: 'stale', runId: dead.runId, nodeId: dead.nodeId })
      }
      return null
    }
    const [entry] = this.queued.splice(bestIndex, 1)
    return entry ?? null
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
    this.pumpQueued()
  }

  private handleBatch(batch: BatchOutput, workerId: WorkerId): void {
    const slot = this.slots.find((entry) => entry.id === workerId)
    const touchedNodes = new Set<NodeId>()
    for (const result of batch.results) {
      touchedNodes.add(result.nodeId)
      if (this.cancelledRuns.has(result.runId)) continue
      if (this.markDelivered(result.runId, result.nodeId)) {
        this.emit({
          kind: 'result',
          runId: result.runId,
          nodeId: result.nodeId,
          output: result.output as unknown as ArrayBuffer,
          computeMs: result.computeMs,
        })
      }
    }
    for (const error of batch.errors) {
      touchedNodes.add(error.nodeId)
      if (this.cancelledRuns.has(error.runId)) continue
      if (this.markDelivered(error.runId, error.nodeId)) {
        this.emit({
          kind: 'error',
          runId: error.runId,
          nodeId: error.nodeId,
          error: error.error,
        })
      }
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

  /**
   * Decode the single transferred payload, emit plain scalar outcomes, and
   * let the buffer go out of scope (ownership released immediately — it is
   * never retained, copied into a view, or re-posted). A malformed payload is
   * treated as a worker fault for this slot rather than risking half a batch.
   */
  private handleBinaryBatch(batch: BinaryBatchOutput, workerId: WorkerId): void {
    const slot = this.slots.find((entry) => entry.id === workerId)
    const touchedNodes = new Set<NodeId>()
    try {
      const decoded = decodeResultBatch(
        batch.payload as unknown as ArrayBuffer,
      )
      if (decoded.runId !== batch.runId) {
        throw new BinaryProtocolError(
          `envelope runId ${batch.runId} != header runId ${decoded.runId}`,
        )
      }
      for (const result of decoded.results) {
        touchedNodes.add(result.nodeId)
        if (this.cancelledRuns.has(decoded.runId)) continue
        if (this.markDelivered(decoded.runId, result.nodeId)) {
          this.emit({
            kind: 'result',
            runId: decoded.runId,
            nodeId: result.nodeId,
            value: result.value,
            computeMs: result.computeMs,
          })
        }
      }
    } catch (error) {
      if (error instanceof BinaryProtocolError) {
        // Structural corruption: abandon this slot's task entirely so no
        // partial batch can be committed; the slot is healed like a crash.
        if (slot?.current) touchedNodes.add(slot.current.nodeId)
        this.killAndReplaceSlot(workerId)
        return
      }
      throw error
    }
    for (const error of batch.errors) {
      touchedNodes.add(error.nodeId)
      if (this.cancelledRuns.has(error.runId)) continue
      if (this.markDelivered(error.runId, error.nodeId)) {
        this.emit({
          kind: 'error',
          runId: error.runId,
          nodeId: error.nodeId,
          error: error.error,
        })
      }
    }
    for (const notice of batch.stale) {
      touchedNodes.add(notice.nodeId)
      this.emit({
        kind: 'stale',
        runId: notice.runId,
        nodeId: notice.nodeId,
      })
    }
    if (
      slot &&
      slot.current &&
      touchedNodes.has(slot.current.nodeId)
    ) {
      this.freeSlot(slot)
    }
  }

  /**
   * First-terminal-outcome wins ledger. Returns false when this run+node was
   * already emitted, which is exactly the crash/re-run race: the original
   * task actually delivered before dying and the replacement's result is the
   * duplicate. Cancelled deliveries never record a key, so a genuine retry
   * under a NEW runId is never affected (keys include the runId).
   */
  private markDelivered(runId: RunId, nodeId: NodeId): boolean {
    const key = `${runId}:${nodeId}`
    if (this.deliveredKeys.has(key)) return false
    this.deliveredKeys.add(key)
    return true
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
    const prefix = `${runId}:`
    for (const key of this.deliveredKeys) {
      if (key.startsWith(prefix)) this.deliveredKeys.delete(key)
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
