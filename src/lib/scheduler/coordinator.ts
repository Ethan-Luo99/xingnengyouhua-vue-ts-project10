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
import { ReadyTracker, closureWithUpstreams, compileGraph } from './graph'
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
}

interface RunRecord {
  readonly id: RunId
  readonly active: ReadonlySet<NodeId>
  readonly tracker: ReadyTracker
  remaining: number
  done: number
  finished: boolean
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
  private readonly graph = compileGraph(
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
  private readyHeap: CriticalHeap | null = null
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
    this.pool.warmUp()
  }

  get criticalPathMs(): number {
    return this.graph.criticalPathMs
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
   * runId and retires the previous run; this is cancellation, not debounce.
   */
  run(options: RunOptions = {}): RunId {
    const runId = this.runSeq + 1
    this.runSeq = runId
    const previous = this.currentRun
    const active =
      options.selectedNodes === undefined
        ? new Set<NodeId>(this.graph.order)
        : closureWithUpstreams(this.graph, options.selectedNodes)
    const tracker = new ReadyTracker(this.graph, active)
    const record: RunRecord = {
      id: runId,
      active,
      tracker,
      remaining: active.size,
      done: 0,
      finished: false,
    }
    this.currentRun = record
    this.readyHeap = new CriticalHeap(
      this.graph.criticalDistance,
      () => (this.currentRun?.id === runId ? runId : null),
      runId,
    )
    this.enqueueReady(tracker.initialReady(active))

    if (previous) {
      // Old results still in flight are NOT terminated; their adoption flags
      // flip and the commit flip drops anything that comes back late.
      this.pool.cancel(previous.id)
      this.retiredRuns.push(previous.id)
      while (this.retiredRuns.length > 2) {
        const old = this.retiredRuns.shift()
        if (old !== undefined) this.pool.purgeRun(old)
      }
    }
    this.scheduleSlice()
    return runId
  }

  private enqueueReady(ids: readonly NodeId[]): void {
    if (!this.readyHeap) return
    for (const id of ids) this.readyHeap.push(id)
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
    const heap = this.readyHeap
    if (!run || run.finished || !heap) return
    // While scrolling the fast slice shrinks to ~1 ms so frames stay smooth.
    const scrolling = performance.now() - this.lastScrollAt < 150
    const budgetMs = scrolling ? 1 : FAST_SLICE_BUDGET_MS
    const deadline = performance.now() + budgetMs
    while (this.currentRun === run) {
      const nodeId = heap.pop()
      if (nodeId === null) return // heap self-gates once the run is stale
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
        this.complete(run, nodeId, outcome)
      } else {
        // Medium and long-tail nodes always go to the worker pool (which may
        // itself be running in chunked main-thread fallback mode).
        this.pool.dispatch(run.id, nodeId)
      }
    }
  }

  private handlePoolOutcome(outcome: PoolOutcome): void {
    const run = this.currentRun
    if (!run) return
    if (outcome.kind === 'stale') {
      // Slot bookkeeping only; cancelled runs never retire the new run.
      return
    }
    // Everything lands in the non-reactive back buffer first, tagged with the
    // producing run id. Stale-run entries are discarded at the frame flip.
    if (outcome.kind === 'error') {
      this.backBuffer.set(outcome.nodeId, {
        runId: outcome.runId,
        outcome: {
          nodeId: outcome.nodeId,
          status: 'error',
          value: null,
          error: outcome.error?.message ?? 'unknown error',
          computeMs: 0,
        },
      })
      if (outcome.runId === run.id) {
        this.complete(run, outcome.nodeId, {
          nodeId: outcome.nodeId,
          status: 'error',
          value: null,
          error: outcome.error?.message ?? 'unknown error',
          computeMs: 0,
        })
      }
    } else if (outcome.output) {
      const nodeOutcome: NodeOutcome = {
        nodeId: outcome.nodeId,
        status: 'done',
        value: decodeValue(outcome.output),
        computeMs: outcome.computeMs ?? 0,
      }
      this.backBuffer.set(outcome.nodeId, { runId: outcome.runId, outcome: nodeOutcome })
      if (outcome.runId === run.id) this.complete(run, outcome.nodeId, nodeOutcome)
    }
    this.scheduleFrame()
  }

  private complete(run: RunRecord, nodeId: NodeId, outcome: NodeOutcome): void {
    if (run.finished || !run.active.has(nodeId)) return
    run.remaining -= 1
    run.done += 1
    const newlyReady = run.tracker.release(nodeId, run.active)
    this.enqueueReady(newlyReady)
    if (run.remaining === 0) {
      run.finished = true
    }
    this.scheduleFrame()
    this.scheduleSlice()
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
    const accepted: NodeOutcome[] = []
    for (const [nodeId, entry] of this.backBuffer) {
      this.backBuffer.delete(nodeId)
      if (entry.runId === run.id) {
        accepted.push(entry.outcome)
        this.frontBuffer.set(nodeId, entry.outcome)
      }
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
  }
}
