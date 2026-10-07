/**
 * Run coordinator: owns runId gating, ready-set priority scheduling, the
 * main-thread slice loop for fast functions, and the non-reactive double
 * buffer with at most one requestAnimationFrame commit per frame.
 *
 * Result path (hard constraint #4):
 *   worker/pool outcome -> back buffer (plain Map) -> rAF flip ->
 *   front buffer snapshot handed to the registered commit sink -> Svelte state
 * Worker results are never written into reactive state one by one.
 *
 * Cooperative priority preemption (protocol upgrade):
 *   - trigger B while an A full run is live PAUSES the A run instead of
 *     cancelling it: A keeps its original runId, its ready heap and all
 *     already-enqueued worker tasks; in-flight worker outcomes are "parked"
 *     in the back buffer (held, not dropped) until A resumes
 *   - the A fast slice stops at its NEXT yield point: slices are 4 ms
 *     macrotask chunks, and every chunk re-reads the foreground run, so a
 *     running sync slice is never interrupted mid-node but A never starts
 *     another node after B arrives
 *   - when the B subset finishes, A resumes under the SAME runId from its
 *     paused heap position; parked outcomes are flushed by the next frame
 *     flip together with any newly produced ones (still at most one commit
 *     per frame). Missed frame budgets are NOT replayed: resume starts a
 *     fresh 4 ms slice rather than accumulating slice debt
 *   - an error-cell retry starts a tiny `repair` sidecar run with its own
 *     fresh runId; it never pauses A/B and can never tag a foreground run's
 *     nodes with the wrong runId
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
  /**
   * Error-cell retry (trigger from DataTable): rerun exactly this node in a
   * dedicated repair sidecar run with a fresh runId. No upstream closure, no
   * preemption: it rides the same scheduler and the same commit path.
   */
  readonly repairNode?: NodeId
}

type RunKind = 'full' | 'subset' | 'repair'
type RunStatus = 'running' | 'paused' | 'finished'

interface RunRecord {
  readonly id: RunId
  readonly kind: RunKind
  readonly active: ReadonlySet<NodeId>
  readonly tracker: ReadyTracker
  readonly heap: CriticalHeap
  /** Non-fast nodes already handed to the pool for this run. */
  readonly dispatched: Set<NodeId>
  remaining: number
  done: number
  finished: boolean
  status: RunStatus
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

  /** Pop the highest-priority node still owned by a live record; else null. */
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
/**
 * Hard cap on fast nodes evaluated inside one macrotask slice, regardless of
 * how cheap each one is. This guarantees a real event-loop yield (and thus a
 * preemption checkpoint for an arriving B) even when every node finishes in
 * microseconds, where the wall-clock deadline alone could be postponed by
 * timer granularity for hundreds of nodes.
 */
const FAST_SLICE_NODE_CAP = 64

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
  /** The foreground run: a fresh A full run or the latest B subset. */
  private currentRun: RunRecord | null = null
  /** A full run parked behind a running B subset; at most one exists. */
  private pausedFull: RunRecord | null = null
  /** Error-cell repair sidecars; served on idle fast-slice gaps. */
  private readonly repairs: RunRecord[] = []
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
  /** Negative infinity until the first real scroll, so no phantom "scrolling". */
  private lastScrollAt = Number.NEGATIVE_INFINITY
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

  notifyInteraction(): void {
    this.pool.notifyInteraction()
  }

  notifyScroll(scrollTop: number): void {
    this.lastScrollAt = performance.now()
    this.lastScrollTop = scrollTop
    this.pool.notifyInteraction()
  }

  /**
   * Start a run.
   *  - A click (`repairNode`/`selectedNodes` absent): a full run. Any live
   *    full, subset or repair run is retired (the pre-existing semantics).
   *  - B keystroke (`selectedNodes`): while a full run is live it is PAUSED
   *    under the same runId and the subset becomes foreground; otherwise the
   *    previous foreground run is retired exactly as before.
   *  - error retry (`repairNode`): a one-node sidecar run with its own fresh
   *    runId; it never preempts and never retires anything.
   */
  run(options: RunOptions = {}): RunId {
    if (options.repairNode !== undefined) {
      return this.startRepair(options.repairNode)
    }
    const previous = this.currentRun
    let record: RunRecord
    if (options.selectedNodes === undefined) {
      record = this.startFull(previous)
    } else {
      record = this.startSubset(options.selectedNodes, previous)
    }
    this.currentRun = record
    this.enqueueReady(record, record.tracker.initialReady(record.active))
    this.scheduleSlice()
    return record.id
  }

  private nextRunId(): RunId {
    this.runSeq += 1
    return this.runSeq
  }

  private buildRecord(
    kind: RunKind,
    active: Set<NodeId>,
    gate: () => RunId | null,
  ): RunRecord {
    const id = this.nextRunId()
    return {
      id,
      kind,
      active,
      tracker: new ReadyTracker(this.graph, active),
      heap: new CriticalHeap(this.graph.criticalDistance, gate, id),
      dispatched: new Set<NodeId>(),
      remaining: active.size,
      done: 0,
      finished: false,
      status: 'running',
    }
  }

  /** Retire a record for good: flip its adoption flag and remember it. */
  private retire(record: RunRecord): void {
    record.status = 'finished'
    this.pool.cancel(record.id)
    this.retiredRuns.push(record.id)
    while (this.retiredRuns.length > 2) {
      const old = this.retiredRuns.shift()
      if (old !== undefined) this.pool.purgeRun(old)
    }
  }

  private startFull(previous: RunRecord | null): RunRecord {
    // A click abandons everything, including a paused A and all repairs.
    if (this.pausedFull) {
      this.retire(this.pausedFull)
      this.pausedFull = null
    }
    if (previous) this.retire(previous)
    for (const repair of this.repairs.splice(0, this.repairs.length)) this.retire(repair)
    const active = new Set<NodeId>(this.graph.order)
    const record = this.buildRecord(
      'full',
      active,
      () =>
        this.currentRun === record && record.status === 'running'
          ? record.id
          : null,
    )
    return record
  }

  private startSubset(
    selectedNodes: readonly NodeId[],
    previous: RunRecord | null,
  ): RunRecord {
    if (previous && previous.kind === 'full' && !previous.finished) {
      // Cooperative preemption, not cancellation: the A run keeps its runId,
      // heap and worker tasks; its late results are parked in the back buffer.
      previous.status = 'paused'
      this.pausedFull = previous
    } else if (previous && previous !== this.pausedFull) {
      this.retire(previous)
    }
    const active = closureWithUpstreams(this.graph, selectedNodes)
    const record = this.buildRecord('subset', active, () =>
      this.currentRun === record && record.status === 'running'
        ? record.id
        : null,
    )
    return record
  }

  private startRepair(nodeId: NodeId): RunId {
    if (this.specs[nodeId] === undefined) {
      throw new Error(`repair: unknown node ${nodeId}`)
    }
    const active = new Set<NodeId>([nodeId])
    const record = this.buildRecord(
      'repair',
      active,
      () => (this.repairs.includes(record) && record.status === 'running'
        ? record.id
        : null),
    )
    this.repairs.push(record)
    this.enqueueReady(record, [nodeId])
    this.scheduleSlice()
    return record.id
  }

  private enqueueReady(record: RunRecord, ids: readonly NodeId[]): void {
    for (const id of ids) record.heap.push(id)
  }

  /** B finished: wake the parked A run under its ORIGINAL runId. */
  private resumePausedFull(): void {
    const paused = this.pausedFull
    if (!paused || paused.finished) {
      this.pausedFull = null
      return
    }
    paused.status = 'running'
    this.pausedFull = null
    this.currentRun = paused
    // Parked back-buffer entries are simply left tagged with paused.id:
    // resume keeps the same runId, so the next flip accepts them. Slice debt
    // is deliberately NOT replayed — a fresh 4 ms budget starts now.
    this.scheduleSlice()
    this.scheduleFrame()
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

  /**
   * The live scheduling targets for one slice tick: the foreground run first;
   * repair sidecars only when the foreground run has no immediately runnable
   * fast node (they never delay a running A/B). A paused full run is skipped.
   */
  private sliceTargets(): RunRecord[] {
    const targets: RunRecord[] = []
    const foreground = this.currentRun
    if (foreground && !foreground.finished && foreground.status === 'running') {
      targets.push(foreground)
    }
    for (const repair of this.repairs) {
      if (!repair.finished && repair.status === 'running') targets.push(repair)
    }
    return targets
  }

  private runSlice(): void {
    const targets = this.sliceTargets()
    if (targets.length === 0) return
    // Foreground run as of entering THIS slice invocation. A preempting B
    // flips currentRun synchronously mid-slice; checking this token at each
    // iteration is the cooperative yield point (the already-started sync
    // node finishes, no further node is begun).
    const foregroundAtStart = this.currentRun
    const scrolling = performance.now() - this.lastScrollAt < 150
    const budgetMs = scrolling ? 1 : FAST_SLICE_BUDGET_MS
    for (const run of targets) {
      // A local deadline per live run: the slice is one 4 ms budget; a
      // scrolling frame shrinks it to 1 ms.
      const deadline = performance.now() + budgetMs
      let nodesThisSlice = 0
      // Non-fast heap tops are displaced into a per-slice side list so the
      // slice can keep scanning the critical heap for fast work beneath a
      // medium/long top; they are restored (heap-ordered) before leaving.
      const displaced: NodeId[] = []
      // The gate re-reads foreground state on EVERY pop: once a B arrives and
      // pauses A, A's heap refuses every further pop at this very yield point,
      // so the running fast slice pauses before starting another A node.
      while (run.status === 'running' && !run.finished) {
        if (
          run === foregroundAtStart &&
          this.currentRun !== foregroundAtStart
        ) {
          for (const id of displaced) run.heap.push(id)
          return
        }
        const nodeId = run.heap.pop()
        if (nodeId === null) {
          if (run.status !== 'running') break // gate closed: B preempted A
          break // heap drained for this target
        }
        const spec = this.specs[nodeId]
        if (!spec) continue
        if (spec.tier !== 'fast') {
          // Hand non-fast work to the pool exactly once per run; repeated
          // slice visits simply hold it aside so fast nodes beneath it run.
          if (!run.dispatched.has(nodeId)) {
            run.dispatched.add(nodeId)
            this.pool.dispatch(run.id, nodeId)
          }
          displaced.push(nodeId)
          continue
        }
        if (performance.now() + spec.costMs + 0.5 > deadline) {
          run.heap.push(nodeId)
          displaced.forEach((id) => run.heap.push(id))
          this.scheduleSlice()
          return
        }
        nodesThisSlice += 1
        const outcome = this.computeFast(run.id, nodeId, spec)
        this.backBuffer.set(nodeId, { runId: run.id, outcome })
        this.complete(run, nodeId, outcome)
        if (nodesThisSlice >= FAST_SLICE_NODE_CAP) {
          for (const id of displaced) run.heap.push(id)
          // complete() already queued another slice when nodes remain.
          return
        }
      }
      for (const id of displaced) run.heap.push(id)
    }
    // No blanket reschedule: completion events (fast `complete`, pool
    // outcomes) and new runs own the scheduling. A just-started B already
    // scheduled its own slice when it preempted A.
  }

  private computeFast(
    runId: RunId,
    nodeId: NodeId,
    spec: FunctionSpec,
  ): NodeOutcome {
    const startedAt = performance.now()
    try {
      const output = runFunctionSync(spec, makeInput(spec))
      return {
        nodeId,
        status: 'done',
        value: decodeValue(output),
        computeMs: performance.now() - startedAt,
      }
    } catch (error) {
      return {
        nodeId,
        status: 'error',
        value: null,
        error: error instanceof Error ? error.message : String(error),
        computeMs: performance.now() - startedAt,
      }
    }
  }

  /* ---------------- pool result routing ---------------- */

  /**
   * Find the live record that owns a worker outcome, or null. A paused A's
   * outcomes resolve here too (its record stays alive in `pausedFull`), so
   * they are parked in the back buffer instead of being mistaken for stale.
   */
  private ownerOf(runId: RunId): RunRecord | null {
    if (this.currentRun?.id === runId) return this.currentRun
    if (this.pausedFull?.id === runId) return this.pausedFull
    for (const repair of this.repairs) {
      if (repair.id === runId) return repair
    }
    return null
  }

  private handlePoolOutcome(outcome: PoolOutcome): void {
    if (outcome.kind === 'stale') {
      // Slot bookkeeping only; cancelled runs never retire the new run.
      return
    }
    const owner = this.ownerOf(outcome.runId)
    const nodeOutcome: NodeOutcome =
      outcome.kind === 'error'
        ? {
            nodeId: outcome.nodeId,
            status: 'error',
            value: null,
            error: outcome.error?.message ?? 'unknown error',
            computeMs: 0,
          }
        : {
            nodeId: outcome.nodeId,
            status: 'done',
            value: outcome.value ?? decodeValue(outcome.output ?? new ArrayBuffer(8)),
            computeMs: outcome.computeMs ?? 0,
          }
    // Everything lands in the non-reactive back buffer first, tagged with the
    // producing run id. The frame flip decides foreground vs parked vs stale.
    this.backBuffer.set(outcome.nodeId, {
      runId: outcome.runId,
      outcome: nodeOutcome,
    })
    if (owner) {
      this.complete(owner, outcome.nodeId, nodeOutcome)
    }
    this.scheduleFrame()
  }

  private complete(run: RunRecord, nodeId: NodeId, outcome: NodeOutcome): void {
    if (run.finished || !run.active.has(nodeId)) return
    run.remaining -= 1
    run.done += 1
    const newlyReady = run.tracker.release(nodeId, run.active)
    this.enqueueReady(run, newlyReady)
    if (run.remaining === 0) {
      run.finished = true
      run.status = 'finished'
    }
    this.scheduleFrame()
    if (!run.finished) {
      this.scheduleSlice()
      return
    }
    // Lifecycle transitions are deferred to AFTER the next frame flip:
    // B keeps foreground ownership until its final (possibly finished) commit
    // is sunk, and a repair sidecar stays in `repairs` until its outcomes are
    // accepted. Otherwise the resume/removal would make the flip treat the
    // last entries as stale and drop them.
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

  /** Run ids whose back-buffer entries the flip may surface right now. */
  private liveRunIds(): Set<RunId> {
    const live = new Set<RunId>()
    if (this.currentRun) live.add(this.currentRun.id)
    for (const repair of this.repairs) live.add(repair.id)
    // pausedFull is deliberately excluded: its results are PARKED, not stale.
    return live
  }

  /**
   * Flip the buffer.
   *  - entries of the foreground run and of live repair sidecars commit
   *    (repair cells ride the same sink — no new submission path)
   *  - entries belonging to the PAUSED full run stay buffered verbatim and
   *    commit after resume, because the runId never changes
   *  - entries from retired runs are dropped here at the buffer boundary;
   *    "paused-old" is therefore distinguished from "stale" purely by runId
   *    membership: paused A's runId is neither retired nor foreground
   */
  private flip(): void {
    const foreground = this.currentRun
    if (!foreground && !this.pausedFull && this.repairs.length === 0) {
      this.backBuffer.clear()
      return
    }
    const live = this.liveRunIds()
    const accepted: NodeOutcome[] = []
    for (const [nodeId, entry] of this.backBuffer) {
      if (entry.runId === this.pausedFull?.id) continue // parked; keep entry
      this.backBuffer.delete(nodeId)
      if (live.has(entry.runId)) {
        accepted.push(entry.outcome)
        this.frontBuffer.set(nodeId, entry.outcome)
      }
      // Retired-run entries die here, never reaching the sink or Svelte state.
    }
    // Exactly one sink call per frame: foreground progress counters, with any
    // finished repair-node outcomes merged onto the same commit. If only
    // repairs are live, the commit is attributed to the first of them.
    const commitRun =
      foreground ?? this.repairs.find((repair) => !repair.finished || true) ?? null
    const finished = commitRun?.finished ?? false
    if (this.sink && commitRun && (accepted.length > 0 || finished)) {
      this.sink({
        runId: commitRun.id,
        outcomes: accepted,
        doneCount: commitRun.done,
        totalCount: commitRun.active.size,
        finished,
      })
    }

    // Lifecycle transitions only AFTER the commit above has sunk, so a
    // finishing B's final outcomes are never mistaken for stale entries, and
    // the paused A is resumed only once B's last frame is committed.
    if (foreground?.finished) {
      if (foreground.kind === 'subset') {
        this.currentRun = null
        if (this.pausedFull) this.resumePausedFull()
      } else {
        this.currentRun = null
      }
    }
    for (let index = this.repairs.length - 1; index >= 0; index -= 1) {
      if (this.repairs[index]?.finished) this.repairs.splice(index, 1)
    }
  }
}
