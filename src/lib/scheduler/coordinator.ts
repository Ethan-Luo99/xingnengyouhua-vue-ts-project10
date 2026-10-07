/**
 * Run coordinator: owns runId gating, ready-set priority scheduling, the
 * main-thread slice loop for fast functions, and the non-reactive double
 * buffer with at most one requestAnimationFrame commit per frame.
 *
 * Result path (hard constraint #4):
 *   worker/pool outcome -> back buffer (plain Map) -> rAF flip ->
 *   front buffer snapshot handed to the registered commit sink -> Svelte state
 * Worker results are never written into reactive state one by one.
 */

import {
  CHUNK_SLICE_MS,
  FUNCTIONS,
  decodeValue,
  makeInput,
  runFunctionSync,
  type FunctionSpec,
} from '../domain/functions'
import {
  MutableDag,
  ReadyTracker,
  closureWithUpstreams,
  type PatchNodeInput,
  type PatchResult,
} from './graph'
import { ComputePool, type PoolOutcome } from './pool'
import type { NodeId, RunId } from './protocol'

export interface NodeOutcome {
  readonly nodeId: NodeId
  readonly status: 'done' | 'error'
  readonly value: number | null
  readonly error?: string
  readonly computeMs: number
}

export interface CommitPayload {
  readonly runId: RunId
  /** Outcomes accumulated since the previous frame for THIS run only. */
  readonly outcomes: readonly NodeOutcome[]
  readonly doneCount: number
  readonly totalCount: number
  readonly finished: boolean
}

export type CommitSink = (payload: CommitPayload) => void

export interface RunOptions {
  /** When omitted the whole graph runs (trigger A). */
  readonly selectedNodes?: readonly NodeId[]
  /**
   * Whether selected nodes pull in their transitive upstreams. Trigger B
   * needs the closure (upstream outputs feed the subset); a single-node
   * retry sets this to false so ONLY the errored node reruns.
   */
  readonly includeUpstreams?: boolean
}

interface RunRecord {
  readonly id: RunId
  readonly active: ReadonlySet<NodeId>
  readonly tracker: ReadyTracker
  /**
   * Lifecycle:
   *   running  - foreground; its fast slice owns the main thread
   *   paused   - a full (A) run parked behind a subset/retry foreground run;
   *              its heap and counters are frozen intact, worker results
   *              keep landing in the back buffer
   *   finished - all outcomes retired; the record lingers until replaced so
   *              late worker deliveries are still attributable
   */
  status: 'running' | 'paused' | 'finished'
  readonly kind: 'full' | 'subset'
  remaining: number
  done: number
  finished: boolean
  /** Idempotency guard for node completion (pool re-runs after crashes). */
  readonly completed: Set<NodeId>
  /** Frozen when the run is parked; resumed at exactly this point. */
  readonly heap: CriticalHeap
}

/** Binary min-heap keyed by -criticalDistance so critical-path nodes win. */
class CriticalHeap {
  private nodes: NodeId[] = []

  constructor(
    private readonly key: ReadonlyArrayLike<number>,
    private readonly runIdGate: () => RunId | null,
    private readonly ownerRunId: RunId,
  ) {}

  get size(): number {
    return this.nodes.length
  }

  push(id: NodeId): void {
    this.nodes.push(id)
    this.bubbleUp(this.nodes.length - 1)
  }

  /** Pop the highest-priority node still owned by the live run; else null. */
  pop(): NodeId | null {
    if (this.runIdGate() !== this.ownerRunId) return null
    if (this.nodes.length === 0) return null
    const top = this.nodes[0]
    const last = this.nodes.pop()
    if (this.nodes.length > 0 && last !== undefined) {
      this.nodes[0] = last
      this.bubbleDown(0)
    }
    return top ?? null
  }

  private bubbleUp(index: number): void {
    const id = this.nodes[index]
    if (id === undefined) return
    const weight = this.key[id] ?? 0
    let cursor = index
    while (cursor > 0) {
      const parent = (cursor - 1) >> 1
      const parentId = this.nodes[parent]
      if (parentId === undefined || (this.key[parentId] ?? 0) >= weight) break
      this.nodes[cursor] = parentId
      cursor = parent
    }
    this.nodes[cursor] = id
  }

  private bubbleDown(index: number): void {
    const length = this.nodes.length
    const id = this.nodes[index]
    if (id === undefined) return
    const weight = this.key[id] ?? 0
    let cursor = index
    for (;;) {
      const left = cursor * 2 + 1
      const right = left + 1
      let best = cursor
      let bestWeight = weight
      const leftId = this.nodes[left]
      if (left < length && leftId !== undefined && (this.key[leftId] ?? 0) > bestWeight) {
        best = left
        bestWeight = this.key[leftId] ?? 0
      }
      const rightId = this.nodes[right]
      if (right < length && rightId !== undefined && (this.key[rightId] ?? 0) > bestWeight) {
        best = right
      }
      if (best === cursor) break
      this.nodes[cursor] = this.nodes[best] as NodeId
      cursor = best
    }
    this.nodes[cursor] = id
  }
}

interface ReadonlyArrayLike<T> {
  readonly [index: number]: T | undefined
}

const FAST_SLICE_BUDGET_MS = CHUNK_SLICE_MS

function postMacroTask(callback: () => void): void {
  const schedulerWithYield = globalThis.scheduler as
    | { yield?(): Promise<void>; postTask?(cb: () => void): void }
    | undefined
  if (schedulerWithYield?.yield) {
    void schedulerWithYield.yield().then(callback)
  } else if (schedulerWithYield?.postTask) {
    schedulerWithYield.postTask(callback)
  } else {
    setTimeout(callback, 0)
  }
}

export class SchedulerCoordinator {
  private readonly dag = MutableDag.fromNodes(
    FUNCTIONS.map((spec) => ({
      id: spec.id,
      deps: spec.deps,
      hardEdges: spec.hardEdges,
      costMs: spec.costMs,
    })),
  )
  private readonly pool = new ComputePool(FUNCTIONS)
  private readonly specs: readonly FunctionSpec[] = FUNCTIONS

  private runSeq = 0
  private currentRun: RunRecord | null = null
  /** A full (A) run parked behind a subset/retry foreground run, at most one. */
  private pausedRun: RunRecord | null = null
  /** Every live/lingering run by id, so late outcomes route to their owner. */
  private readonly runsById = new Map<RunId, RunRecord>()
  private sliceQueued = false
  private sink: CommitSink | null = null

  /** Back buffer: plain (non-reactive) entries waiting for the frame flip. */
  private readonly backBuffer = new Map<
    NodeId,
    { runId: RunId; outcome: NodeOutcome }
  >()
  /** Front buffer: the last committed snapshot per node. */
  private readonly frontBuffer = new Map<NodeId, NodeOutcome>()
  private frameQueued = false
  private readonly retiredRuns: RunId[] = []
  private lastScrollAt = 0
  private lastScrollTop = 0

  constructor() {
    this.pool.onOutcome((outcome) => this.handlePoolOutcome(outcome))
    this.pool.setRunPriority((runId) => {
      if (this.currentRun?.id === runId) return 0
      if (this.pausedRun?.id === runId) return 1
      return 2
    })
    this.pool.warmUp()
  }

  get criticalPathMs(): number {
    return this.dag.snapshot().criticalPathMs
  }

  get usingFallback(): boolean {
    return this.pool.isFallback
  }

  setCommitSink(sink: CommitSink | null): void {
    this.sink = sink
  }

  /** Keystroke / click activity; sharpens fallback admission. */
  notifyInteraction(): void {
    this.pool.notifyInteraction()
  }

  /** Scroll activity only records a timestamp and position; no data recompute. */
  notifyScroll(scrollTop: number): void {
    this.lastScrollAt = performance.now()
    this.lastScrollTop = scrollTop
    this.pool.notifyInteraction()
  }

  /**
   * Start a run. Trigger A calls it with no selection (whole graph); trigger B
   * calls it per keystroke with a subset. Every call allocates a new monotonic
   * runId.
   *
   * Cooperative preemption:
   *   - a subset/retry run arriving while a FULL (A) run is foreground does
   *     NOT cancel it — the A run is PAUSED at its next yield point (the
   *     current slice finishes the node in flight, then stops popping) and
   *     automatically RESUMES, on the SAME runId from the frozen heap, once
   *     the interrupting run finishes
   *   - every other replacement (new A, or a second B while a B is live)
   *     retires the displaced run exactly as before: in-flight work is not
   *     killed, its adoption flag flips and late results are dropped
   */
  run(options: RunOptions = {}): RunId {
    const runId = this.runSeq + 1
    this.runSeq = runId
    const previous = this.currentRun
    const graphSnapshot = this.dag.snapshot()
    const includeUpstreams = options.includeUpstreams ?? true
    const active =
      options.selectedNodes === undefined
        ? new Set<NodeId>(graphSnapshot.order.filter((id) => this.dag.liveIds.has(id)))
        : includeUpstreams
          ? closureWithUpstreams(graphSnapshot, options.selectedNodes)
          : new Set<NodeId>(options.selectedNodes)
    const tracker = new ReadyTracker(graphSnapshot, active)
    const kind: RunRecord['kind'] =
      options.selectedNodes === undefined ? 'full' : 'subset'
    const heap = new CriticalHeap(
      graphSnapshot.criticalDistance,
      () => (this.currentRun?.id === runId ? runId : null),
      runId,
    )
    const record: RunRecord = {
      id: runId,
      active,
      tracker,
      status: 'running',
      kind,
      remaining: active.size,
      done: 0,
      finished: false,
      completed: new Set<NodeId>(),
      heap,
    }
    this.runsById.set(runId, record)
    this.currentRun = record
    this.enqueueReady(record, tracker.initialReady(active))

    if (previous) {
      const preemptsAsPause =
        kind === 'subset' &&
        previous.kind === 'full' &&
        !previous.finished &&
        this.pausedRun === null
      if (preemptsAsPause) {
        // Cooperative pause: not cancelled. Its queued/worker tasks are NOT
        // touched — they complete normally and are buffered under its runId;
        // the slice simply stops serving its heap (the heap's runId gate
        // makes pop() return null on the very next yield check).
        previous.status = 'paused'
        this.pausedRun = previous
      } else if (kind === 'subset' && this.pausedRun) {
        // Chained preemption: another subset/retry while a full run is
        // already parked. Only the immediately-previous foreground run is
        // retired; the paused A run stays paused for the whole subset chain
        // and is resumed once the final foreground run finishes. Retiring it
        // here would strand its heap and every task still in the pool queue.
        this.queueRetirement(previous)
      } else {
        // A new full run (or the very first run with no parked A): retire
        // the previous foreground and any paused run it was shadowing.
        if (this.pausedRun && this.pausedRun !== previous) {
          this.queueRetirement(this.pausedRun)
        }
        this.pausedRun = null
        this.queueRetirement(previous)
      }
    }
    this.scheduleSlice()
    return runId
  }

  /**
   * UI retry entry: rerun exactly one (errored) node through the SAME
   * scheduler, the SAME runId machinery and the SAME double-buffer commit —
   * no second submission path. Allocates a fresh monotonic runId (never
   * reusing or mutating the run that produced the error), so an in-flight B
   * run is preempted by the ordinary subset rules and cannot be polluted by
   * the retry's outcomes.
   */
  retryNode(nodeId: NodeId): RunId | null {
    if (!this.dag.liveIds.has(nodeId)) return null
    this.pool.notifyInteraction()
    return this.run({ selectedNodes: [nodeId], includeUpstreams: false })
  }

  /**
   * Incremental graph revision. Only allowed while no run is live: pending
   * ReadyTrackers and heaps reference the pre-patch snapshot, and wiring a
   * patch under in-flight outcomes would break their accounting.
   */
  applyGraphPatch(
    addNodes: readonly PatchNodeInput[],
    removeNodes: readonly NodeId[],
  ): PatchResult {
    if (this.currentRun && !this.currentRun.finished) {
      throw new Error('cannot patch the graph while a run is in progress')
    }
    if (this.pausedRun) throw new Error('cannot patch the graph while a run is paused')
    return this.dag.applyPatch(addNodes, removeNodes)
  }

  /**
   * Retire a run: flip the pool's adoption flag (in-flight work is not
   * killed) and age its bookkeeping out of the retirement queue.
   */
  private queueRetirement(record: RunRecord): void {
    record.status = 'finished'
    this.pool.cancel(record.id)
    this.retiredRuns.push(record.id)
    while (this.retiredRuns.length > 2) {
      const old = this.retiredRuns.shift()
      if (old !== undefined) {
        this.pool.purgeRun(old)
        this.runsById.delete(old)
      }
    }
  }

  private enqueueReady(run: RunRecord, ids: readonly NodeId[]): void {
    for (const id of ids) run.heap.push(id)
  }

  /* ---------------- main-thread fast slice ---------------- */

  private scheduleSlice(): void {
    if (this.sliceQueued) return
    this.sliceQueued = true
    postMacroTask(() => {
      this.sliceQueued = false
      this.runSlice()
    })
  }

  private runSlice(): void {
    const run = this.currentRun
    // The slice only ever serves the foreground run. A paused A run is not
    // cancelled: its heap and counters simply stop being serviced here until
    // it is promoted back to the foreground.
    if (!run || run.finished || run.status !== 'running') return
    const heap = run.heap
    // While scrolling the fast slice shrinks to ~1 ms so frames stay smooth.
    const scrolling = performance.now() - this.lastScrollAt < 150
    const budgetMs = scrolling ? 1 : FAST_SLICE_BUDGET_MS
    const deadline = performance.now() + budgetMs
    while (this.currentRun === run) {
      const nodeId = heap.pop()
      // The heap self-gates while the run is no longer foreground.
      if (nodeId === null) return
      const spec = this.specs[nodeId]
      if (!spec) continue
      if (spec.tier === 'fast') {
        // Admission: only start a task whose estimate fits the remaining slice.
        if (performance.now() + spec.costMs + 0.5 > deadline) {
          heap.push(nodeId)
          this.scheduleSlice()
          return
        }
        const startedAt = performance.now()
        let outcome: NodeOutcome
        try {
          const output = runFunctionSync(spec, makeInput(spec))
          outcome = {
            nodeId,
            status: 'done',
            value: decodeValue(output),
            computeMs: performance.now() - startedAt,
          }
        } catch (error) {
          outcome = {
            nodeId,
            status: 'error',
            value: null,
            error: error instanceof Error ? error.message : String(error),
            computeMs: performance.now() - startedAt,
          }
        }
        // Main-thread results take the same back buffer -> rAF path as the
        // worker results; nothing writes reactive state synchronously here.
        this.backBuffer.set(nodeId, { runId: run.id, outcome })
        if (this.complete(run, nodeId, outcome)) {
          // The foreground run may finish right here; promotion of a paused
          // run is handled after the frame flip below.
          this.afterRunProgress(run)
        }
      } else {
        // Medium and long-tail nodes always go to the worker pool (which may
        // itself be running in chunked main-thread fallback mode).
        this.pool.dispatch(run.id, nodeId)
      }
    }
  }

  private handlePoolOutcome(outcome: PoolOutcome): void {
    if (outcome.kind === 'stale') {
      // Slot bookkeeping only; cancelled runs never retire the new run.
      return
    }
    // Route by the PRODUCING run, not by the current foreground: a paused A
    // run keeps receiving worker outcomes (queued tasks were never cancelled)
    // and they must retire its counters while it is parked.
    const run = this.runsById.get(outcome.runId)
    if (!run) return
    // Everything lands in the non-reactive back buffer first, tagged with the
    // producing run id. The flip keeps entries of live foreground/paused runs
    // and drops entries belonging to retired runs.
    if (outcome.kind === 'error') {
      const nodeOutcome: NodeOutcome = {
        nodeId: outcome.nodeId,
        status: 'error',
        value: null,
        error: outcome.error?.message ?? 'unknown error',
        computeMs: 0,
      }
      this.backBuffer.set(outcome.nodeId, {
        runId: outcome.runId,
        outcome: nodeOutcome,
      })
      if (this.complete(run, outcome.nodeId, nodeOutcome)) {
        this.afterRunProgress(run)
      }
    } else {
      // The binary path hands the decoded scalar straight across; the legacy
      // DTO path still carries an 8-byte output buffer.
      const value =
        outcome.value ??
        (outcome.output ? decodeValue(outcome.output) : null)
      if (value === null) return
      const nodeOutcome: NodeOutcome = {
        nodeId: outcome.nodeId,
        status: 'done',
        value,
        computeMs: outcome.computeMs ?? 0,
      }
      this.backBuffer.set(outcome.nodeId, { runId: outcome.runId, outcome: nodeOutcome })
      if (this.complete(run, outcome.nodeId, nodeOutcome)) {
        this.afterRunProgress(run)
      }
    }
    this.scheduleFrame()
  }

  /**
   * Retire one node. Returns true if THIS call transitioned the run to
   * finished (the caller then runs foreground/pause promotion). The
   * completed-set makes the coordinator idempotent independently of the
   * pool's runId+nodeId ledger: a crash-driven re-run can only ever retire a
   * node once, and late deliveries from a retired run hit `runsById` first.
   */
  private complete(
    run: RunRecord,
    nodeId: NodeId,
    _outcome: NodeOutcome,
  ): boolean {
    if (run.finished || !run.active.has(nodeId)) return false
    if (run.completed.has(nodeId)) return false
    run.completed.add(nodeId)
    run.remaining -= 1
    run.done += 1
    const newlyReady = run.tracker.release(nodeId, run.active)
    this.enqueueReady(run, newlyReady)
    if (run.remaining === 0) {
      run.finished = true
      run.status = 'finished'
    }
    this.scheduleFrame()
    this.scheduleSlice()
    return run.finished
  }

  /**
   * Called whenever a run may have just finished. If it was the foreground
   * run, promotion happens after the frame flip (so its final outcomes are
   * committed under its own runId before anything else is shown). If a
   * background run finished while still paused it stays parked: the
   * foreground is what controls the slice, and it will be promoted on the
   * foreground's own completion.
   */
  private afterRunProgress(run: RunRecord): void {
    if (!run.finished) return
    if (run === this.currentRun) this.scheduleFrame()
  }

  /* ---------------- double buffer + frame commit ---------------- */

  private scheduleFrame(): void {
    if (this.frameQueued) return
    this.frameQueued = true
    requestAnimationFrame(() => {
      this.frameQueued = false
      this.flip()
    })
  }

  private flip(): void {
    const run = this.currentRun
    if (!run) {
      this.backBuffer.clear()
      return
    }
    const paused = this.pausedRun
    const accepted: NodeOutcome[] = []
    for (const [nodeId, entry] of this.backBuffer) {
      const belongsToForeground = entry.runId === run.id
      const belongsToPaused =
        paused !== null && entry.runId === paused.id
      if (belongsToForeground) {
        this.backBuffer.delete(nodeId)
        accepted.push(entry.outcome)
        this.frontBuffer.set(nodeId, entry.outcome)
        continue
      } else if (belongsToPaused) {
        // "Paused but live": the entry is KEPT in the back buffer (never
        // committed, never dropped) and flushed when the run is resumed and
        // becomes the foreground again. This is the distinction from a
        // STALE result: a stale/retired entry's runId matches neither the
        // foreground nor the paused run, so it falls through and is dropped.
        continue
      }
      this.backBuffer.delete(nodeId)
      // Entries from retired runs are dropped HERE, at the buffer boundary:
      // they never reach the commit sink and never touch reactive state.
    }
    if (this.sink && (accepted.length > 0 || run.finished)) {
      this.sink({
        runId: run.id,
        outcomes: accepted,
        doneCount: run.done,
        totalCount: run.active.size,
        finished: run.finished,
      })
    }
    if (run.finished) {
      // The foreground run's final frame is delivered above. Now resolve the
      // pause stack: resume the parked A run on its ORIGINAL runId (it was
      // never retired, so no id is allocated and the pool was never told to
      // cancel it), or retire the finished foreground record.
      if (paused && paused !== run) {
        const resumed = paused
        this.pausedRun = null
        this.currentRun = resumed
        resumed.status = 'running'
        // No catch-up burst for frames missed while parked: resume proceeds
        // through the ordinary one-commit-per-frame path; the kept back
        // buffer entries drain at their natural frame rate.
        this.scheduleFrame()
        this.scheduleSlice()
        // Close the lost-wakeup window for tasks parked in the pool queue.
        this.pool.kick()
        // The finished interrupting run lingers in runsById until the
        // retirement queue purges it; the pool cancel flag was flipped when
        // it naturally completed, so late deliveries simply no-op.
        this.queueRetirement(run)
      } else {
        // No parked run to resume: the finished record retires but is kept
        // attributable until the pool bookkeeping is purged.
        this.queueRetirement(run)
        this.currentRun = this.pausedRun
        if (this.currentRun) {
          this.currentRun.status = 'running'
          this.scheduleFrame()
          this.scheduleSlice()
        }
      }
    }
  }
}
