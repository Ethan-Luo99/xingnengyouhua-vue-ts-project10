/**
 * DAG compilation and ready-set maintenance.
 *
 *  - cycle detection: Kahn remainder + an explicit node path in the error
 *  - topological layering: level(v) = 0 for roots, 1 + max(level(upstream))
 *  - critical distance: longest weighted path *to each node* (using the
 *    synthetic p95-ish cost estimates), i.e. the earliest that node can
 *    possibly finish along its critical path
 *  - ready set: {@link ReadyTracker} decrements downstream indegree once per
 *    completion and yields zero-alloc-ready nodes
 */

export interface DagEdge {
  readonly from: number
  readonly to: number
  readonly hard: boolean
}

/** Indexed number view backed by either a plain or typed array. */
export type NumberVector = readonly number[] | Int32Array | Float64Array

export interface CompiledGraph {
  readonly nodeCount: number
  readonly edges: readonly DagEdge[]
  /** Direct upstream node ids per node. */
  readonly upstream: readonly (readonly number[])[]
  /** Direct downstream node ids per node. */
  readonly downstream: readonly (readonly number[])[]
  /** Kahn topological order. */
  readonly order: readonly number[]
  /** Topological layer (parallel depth) of each node. */
  readonly levels: NumberVector
  /** Longest weighted path arriving at each node, in estimated milliseconds. */
  readonly criticalDistance: NumberVector
  /** Critical-path estimate for the whole graph. */
  readonly criticalPathMs: number
  /** Ids of nodes without any upstream dependency. */
  readonly roots: readonly number[]
}

export class DagCycleError extends Error {
  constructor(
    readonly cycleNodes: readonly number[],
    /** Ordered node path around the cycle (from -> ... -> from), when known. */
    readonly cyclePath?: readonly number[],
  ) {
    super(`DAG contains a cycle involving nodes: ${cycleNodes.join(' -> ')}`)
    this.name = 'DagCycleError'
  }

  /** Build from an ordered cycle path (first node repeated at the end). */
  static forPath(path: readonly number[]): DagCycleError {
    const unique = [...new Set(path)]
    return new DagCycleError(unique, path)
  }
}

export interface CompileNodeInput {
  readonly id: number
  readonly deps: readonly number[]
  readonly hardEdges: readonly boolean[]
  readonly costMs: number
}

export function compileGraph(nodes: readonly CompileNodeInput[]): CompiledGraph {
  const nodeCount = nodes.length
  const upstream: number[][] = nodes.map((node) => [...node.deps])
  const downstream: number[][] = Array.from({ length: nodeCount }, () => [])
  const edges: DagEdge[] = []

  for (const node of nodes) {
    node.deps.forEach((from, index) => {
      if (from < 0 || from >= nodeCount || from === node.id) {
        throw new Error(`invalid edge ${from} -> ${node.id}`)
      }
      edges.push({
        from,
        to: node.id,
        hard: node.hardEdges[index] ?? false,
      })
      downstream[from]?.push(node.id)
    })
  }

  // Kahn's algorithm doubles as cycle detection.
  const indegree = new Int32Array(nodeCount)
  for (const edge of edges) indegree[edge.to] += 1
  const queue: number[] = []
  for (let id = 0; id < nodeCount; id += 1) {
    if (indegree[id] === 0) queue.push(id)
  }
  const order: number[] = []
  let head = 0
  while (head < queue.length) {
    const id = queue[head]
    head += 1
    order.push(id)
    for (const next of downstream[id] ?? []) {
      indegree[next] -= 1
      if (indegree[next] === 0) queue.push(next)
    }
  }
  if (order.length !== nodeCount) {
    const remaining: number[] = []
    for (let id = 0; id < nodeCount; id += 1) {
      if (indegree[id] > 0) remaining.push(id)
    }
    throw new DagCycleError(remaining)
  }

  const levels = new Int32Array(nodeCount)
  const criticalDistance = new Float64Array(nodeCount)
  const roots: number[] = []
  let criticalPathMs = 0
  for (const id of order) {
    const node = nodes[id]
    if (!node) throw new Error(`missing node ${id}`)
    if (upstream[id]?.length === 0) {
      roots.push(id)
      levels[id] = 0
      criticalDistance[id] = node.costMs
    } else {
      let maxLevel = 0
      let maxDist = 0
      for (const from of upstream[id] ?? []) {
        if (levels[from] > maxLevel) maxLevel = levels[from]
        const candidate = criticalDistance[from] + node.costMs
        if (candidate > maxDist) maxDist = candidate
      }
      levels[id] = maxLevel + 1
      criticalDistance[id] = maxDist
    }
    if (criticalDistance[id] > criticalPathMs) {
      criticalPathMs = criticalDistance[id]
    }
  }

  return {
    nodeCount,
    edges,
    upstream,
    downstream,
    order,
    levels,
    criticalDistance,
    criticalPathMs,
    roots,
  }
}

/**
 * Per-run indegree tracker. Construction is O(V); each node is released once,
 * so total work per run is O(V + E). `release` returns the newly-ready
 * downstream nodes without touching Svelte or any reactive state.
 */
export class ReadyTracker {
  private readonly remaining: Int32Array

  constructor(
    private readonly graph: CompiledGraph,
    active: ReadonlySet<number>,
  ) {
    this.remaining = new Int32Array(graph.nodeCount)
    for (const id of active) {
      let count = 0
      for (const from of graph.upstream[id] ?? []) {
        if (active.has(from)) count += 1
      }
      this.remaining[id] = count
    }
  }

  /** Nodes in the active set whose active-upstreams are all satisfied. */
  initialReady(active: ReadonlySet<number>): number[] {
    const ready: number[] = []
    for (const id of active) {
      if (this.remaining[id] === 0) ready.push(id)
    }
    return ready
  }

  /** Mark `id` complete; return downstream nodes that just became ready. */
  release(id: number, active: ReadonlySet<number>): number[] {
    const newlyReady: number[] = []
    for (const next of this.graph.downstream[id] ?? []) {
      if (!active.has(next)) continue
      this.remaining[next] -= 1
      if (this.remaining[next] === 0) newlyReady.push(next)
    }
    return newlyReady
  }
}

/**
 * Transitive closure of a node selection: include every upstream node of
 * every selected node, because selected outputs cannot compute without them.
 * Returns a set suitable for {@link ReadyTracker}.
 */
export function closureWithUpstreams(
  graph: CompiledGraph,
  selected: readonly number[],
): Set<number> {
  const active = new Set<number>()
  const stack = [...selected]
  while (stack.length > 0) {
    const id = stack.pop()
    if (id === undefined || active.has(id)) continue
    active.add(id)
    for (const from of graph.upstream[id] ?? []) {
      if (!active.has(from)) stack.push(from)
    }
  }
  return active
}

/* ================================================================== */
/* Incremental revision                                                */
/* ================================================================== */

/**
 * Node descriptor accepted by {@link MutableDag.applyPatch}. Only NEW nodes
 * may be supplied; adding an id that already exists is a programmer error.
 */
export interface PatchNodeInput extends CompileNodeInput {}

export interface PatchResult {
  /** Nodes whose level / criticalDistance were (re)computed by this patch. */
  readonly recomputedNodes: readonly number[]
  /** Number of live nodes after the patch. */
  readonly nodeCount: number
}

/**
 * Transactional, incrementally revisable DAG view.
 *
 * Incremental guarantee
 * ---------------------
 * applyPatch never calls {@link compileGraph}: no Kahn pass, no whole-graph
 * level/distance sweep. Recomputation is confined to the affected subgraph:
 *   - add `x`: levels/distances are evaluated for `x` and for every node
 *     reachable DOWNSTREAM of `x` (a value can only rise via a new edge)
 *   - remove `x`: evaluations are confined to `x`'s former downstreams (a
 *     value can only fall when an upstream disappears), walked in order
 * The exact set touched is returned in {@link PatchResult.recomputedNodes}.
 *
 * Transactional guarantee
 * -----------------------
 * Validation (unknown deps, duplicate ids, would-be cycles) runs to
 * completion against PROSPECTIVE adjacency BEFORE any internal state changes.
 * Only after every check succeeds does the commit phase mutate; on failure a
 * DagCycleError (carrying the ordered cycle path) is thrown and the graph is
 * byte-for-byte unchanged. Deletions and insertions in one patch commit
 * together, so the "all or nothing" rule holds across both.
 *
 * Node indices are stable slots: removed ids keep their typed-array slot
 * (absent from `liveIds`), so adding a node never renumbers existing nodes.
 */
export class MutableDag {
  private upstreamArr: number[][]
  private downstreamArr: number[][]
  private edgeList: DagEdge[]
  private levelVec: Int32Array
  private distanceVec: Float64Array
  private orderArr: number[]
  private rootList: number[]
  private costs: Float64Array
  private readonly live = new Set<number>()
  private capacity: number
  private critical = 0
  private criticalOwner: number | null = null
  private cached: CompiledGraph | null = null

  private constructor(nodes: readonly CompileNodeInput[]) {
    const compiled = compileGraph(nodes)
    this.capacity =
      nodes.length === 0 ? 16 : Math.max(16, Math.ceil(nodes.length * 1.5))
    this.upstreamArr = compiled.upstream.map((list) => [...list])
    this.downstreamArr = compiled.downstream.map((list) => [...list])
    while (this.upstreamArr.length < this.capacity) {
      this.upstreamArr.push([])
      this.downstreamArr.push([])
    }
    this.edgeList = [...compiled.edges]
    this.levelVec = new Int32Array(this.capacity)
    this.levelVec.set(compiled.levels as Int32Array)
    this.distanceVec = new Float64Array(this.capacity)
    this.distanceVec.set(compiled.criticalDistance as Float64Array)
    this.orderArr = [...compiled.order]
    this.rootList = [...compiled.roots]
    this.costs = new Float64Array(this.capacity)
    nodes.forEach((node) => {
      this.costs[node.id] = node.costMs
      this.live.add(node.id)
      if (this.distanceVec[node.id] > this.critical) {
        this.critical = this.distanceVec[node.id]
        this.criticalOwner = node.id
      }
    })
  }

  /** Build a mutable view from node specs (compiles once, up front). */
  static fromNodes(nodes: readonly CompileNodeInput[]): MutableDag {
    return new MutableDag(nodes)
  }

  /** Immutable snapshot compatible with every existing CompiledGraph reader. */
  snapshot(): CompiledGraph {
    if (!this.cached) {
      const nodeCount = this.capacity
      this.cached = {
        nodeCount,
        edges: this.edgeList,
        upstream: this.upstreamArr,
        downstream: this.downstreamArr,
        order: this.orderArr,
        levels: this.levelVec,
        criticalDistance: this.distanceVec,
        criticalPathMs: this.critical,
        roots: this.rootList,
      }
    }
    return this.cached
  }

  /** Live node count (removed slots are not counted). */
  get liveNodeCount(): number {
    return this.live.size
  }

  get liveIds(): ReadonlySet<number> {
    return this.live
  }

  /**
   * Apply an incremental revision. See the class contract for the affected
   * subgraph and rollback semantics.
   */
  applyPatch(
    addNodes: readonly PatchNodeInput[] = [],
    removeNodes: readonly number[] = [],
  ): PatchResult {
    // Captured before edge detach in the commit phase (id -> old downstream).
    const removedDownstreams = new Map<number, number[]>()
    /* ---- phase 1: pure validation against prospective adjacency ---- */

    const addIds = new Set<number>()
    for (const node of addNodes) {
      if (addIds.has(node.id)) {
        throw new Error(`patch adds duplicate node ${node.id}`)
      }
      if (this.live.has(node.id)) {
        throw new Error(`patch node ${node.id} already exists`)
      }
      addIds.add(node.id)
    }
    const removeSet = new Set<number>()
    for (const id of removeNodes) {
      if (addIds.has(id)) {
        throw new Error(`node ${id} is both added and removed`)
      }
      if (!this.live.has(id)) {
        throw new Error(`patch removes unknown node ${id}`)
      }
      removeSet.add(id)
    }
    for (const node of addNodes) {
      for (const dep of node.deps) {
        if (dep === node.id) {
          throw DagCycleError.forPath([node.id, node.id])
        }
        if (dep < 0 || (!this.live.has(dep) && !addIds.has(dep))) {
          throw new Error(`node ${node.id} depends on unknown node ${dep}`)
        }
        if (removeSet.has(dep)) {
          throw new Error(`node ${node.id} depends on removed node ${dep}`)
        }
      }
    }

    // For the cycle search we need downstream edges OF both old and new nodes,
    // including edges that land on a new node (new nodes' reverse edges).
    const newUpstreams = new Map<number, number[]>()
    for (const node of addNodes) newUpstreams.set(node.id, [...node.deps])
    const downstreamOf = (id: number): number[] => {
      if (addIds.has(id)) {
        // New node -> its own deps are UPSTREAM; its prospective downstreams
        // are (a) other new nodes listing it as a dep, plus (b) existing
        // nodes (none can list a not-yet-existing dep).
        const result: number[] = []
        for (const [otherId, deps] of newUpstreams) {
          if (deps.includes(id)) result.push(otherId)
        }
        return result
      }
      const existing = this.downstreamArr[id] ?? []
      const result = existing.filter((next) => !removeSet.has(next))
      for (const [otherId, deps] of newUpstreams) {
        if (deps.includes(id)) result.push(otherId)
      }
      return result
    }

    // DFS cycle detection over the prospective graph, seeded only from new
    // nodes: an inserted cycle MUST pass through an inserted node (deletions
    // cannot create one). Color: 0 unseen, 1 on stack, 2 done.
    const color = new Map<number, number>()
    const stack: number[] = []
    const detectFrom = (seed: number): number[] | null => {
      color.set(seed, 1)
      stack.push(seed)
      for (const next of downstreamOf(seed)) {
        const state = color.get(next) ?? 0
        if (state === 1) {
          const start = stack.indexOf(next)
          return [...stack.slice(start), next]
        }
        if (state === 0) {
          const found = detectFrom(next)
          if (found) return found
        }
      }
      stack.pop()
      color.set(seed, 2)
      return null
    }
    for (const node of addNodes) {
      if ((color.get(node.id) ?? 0) === 0) {
        const cycle = detectFrom(node.id)
        if (cycle) throw DagCycleError.forPath(cycle)
      }
    }

    /* ---- phase 2: commit (validation has fully passed) ---- */

    this.cached = null

    // 2a. removals: detach edges, remove from order/roots/live.
    for (const id of removeSet) {
      // Capture the old downstream fan-out BEFORE edges are detached; phase
      // 3 needs it to seed the affected subgraph.
      const formerDownstream = [...(this.downstreamArr[id] ?? [])]
      removedDownstreams.set(id, formerDownstream)
      for (const up of this.upstreamArr[id] ?? []) {
        const list = this.downstreamArr[up]
        const at = list.indexOf(id)
        if (at >= 0) list.splice(at, 1)
      }
      for (const down of this.downstreamArr[id] ?? []) {
        const list = this.upstreamArr[down]
        const at = list.indexOf(id)
        if (at >= 0) list.splice(at, 1)
      }
      this.upstreamArr[id] = []
      this.downstreamArr[id] = []
      this.edgeList = this.edgeList.filter(
        (edge) => edge.from !== id && edge.to !== id,
      )
      this.live.delete(id)
      this.costs[id] = 0
      this.levelVec[id] = 0
      this.distanceVec[id] = 0
      const orderAt = this.orderArr.indexOf(id)
      if (orderAt >= 0) this.orderArr.splice(orderAt, 1)
      const rootAt = this.rootList.indexOf(id)
      if (rootAt >= 0) this.rootList.splice(rootAt, 1)
    }

    // 2b. capacity growth for new slots (amortized doubling).
    let maxId = this.capacity - 1
    for (const node of addNodes) if (node.id > maxId) maxId = node.id
    if (maxId >= this.capacity) {
      const grown = Math.max(this.capacity * 2, maxId + 1)
      const grow = (arr: number[][]): void => {
        while (arr.length < grown) arr.push([])
      }
      grow(this.upstreamArr)
      grow(this.downstreamArr)
      const levels = new Int32Array(grown)
      levels.set(this.levelVec)
      this.levelVec = levels
      const distances = new Float64Array(grown)
      distances.set(this.distanceVec)
      this.distanceVec = distances
      const costs = new Float64Array(grown)
      costs.set(this.costs)
      this.costs = costs
      this.capacity = grown
    }

    // 2c. insertions: wire edges. The new-node subgraph is Kahn-sorted
    // first so a forward reference between two added nodes still inserts in
    // topological order (the phase-1 DFS proved this sort cannot stall).
    const addedById = new Map(addNodes.map((node) => [node.id, node]))
    const addedIndegree = new Map<number, number>()
    for (const node of addNodes) {
      addedIndegree.set(
        node.id,
        node.deps.filter((dep) => addIds.has(dep)).length,
      )
    }
    const addedQueue = addNodes
      .filter((node) => (addedIndegree.get(node.id) ?? 0) === 0)
      .map((node) => node.id)
    const orderedAdds: PatchNodeInput[] = []
    let addedHead = 0
    while (addedHead < addedQueue.length) {
      const id = addedQueue[addedHead]
      addedHead += 1
      const node = addedById.get(id)
      if (!node) continue
      orderedAdds.push(node)
      for (const other of addNodes) {
        if (other.deps.includes(id)) {
          const next = (addedIndegree.get(other.id) ?? 1) - 1
          addedIndegree.set(other.id, next)
          if (next === 0) addedQueue.push(other.id)
        }
      }
    }
    for (const node of orderedAdds) {
      this.costs[node.id] = node.costMs
      this.live.add(node.id)
      this.upstreamArr[node.id] = [...node.deps]
      this.downstreamArr[node.id] = []
      node.deps.forEach((dep, index) => {
        this.downstreamArr[dep]?.push(node.id)
        this.edgeList.push({
          from: dep,
          to: node.id,
          hard: node.hardEdges[index] ?? false,
        })
      })
      // Topological insertion: place directly after the latest ordered
      // upstream (insertion order is acyclic by the phase-1 proof).
      let insertAt = this.orderArr.length
      for (const dep of node.deps) {
        const depAt = this.orderArr.indexOf(dep)
        if (depAt >= 0 && depAt + 1 > insertAt) insertAt = depAt + 1
      }
      this.orderArr.splice(insertAt, 0, node.id)
      if (node.deps.length === 0) this.rootList.push(node.id)
    }

    /* ---- phase 3: recompute ONLY the affected subgraph ---- */

    const affected = new Set<number>()
    const queue: number[] = []
    for (const id of removeSet) {
      for (const down of removedDownstreams.get(id) ?? []) {
        if (this.live.has(down) && !affected.has(down)) {
          affected.add(down)
          queue.push(down)
        }
      }
    }
    for (const node of addNodes) {
      affected.add(node.id)
      queue.push(node.id)
    }
    // New edges also reach every transitive downstream of every seed.
    let cursor = 0
    while (cursor < queue.length) {
      const id = queue[cursor]
      cursor += 1
      for (const down of this.downstreamArr[id] ?? []) {
        if (this.live.has(down) && !affected.has(down)) {
          affected.add(down)
          queue.push(down)
        }
      }
    }

    // Evaluate in topological order; a node whose upstream is untouched keeps
    // its previous value (the early-continue makes that explicit).
    const recomputed: number[] = []
    for (const id of this.orderArr) {
      if (!affected.has(id)) continue
      const ups = this.upstreamArr[id] ?? []
      if (ups.length === 0) {
        this.levelVec[id] = 0
        this.distanceVec[id] = this.costs[id]
      } else {
        let maxLevel = 0
        let maxDist = 0
        for (const up of ups) {
          if (!this.live.has(up)) continue
          if (this.levelVec[up] > maxLevel) maxLevel = this.levelVec[up]
          const candidate = this.distanceVec[up] + this.costs[id]
          if (candidate > maxDist) maxDist = candidate
        }
        this.levelVec[id] = maxLevel + 1
        this.distanceVec[id] = maxDist
      }
      recomputed.push(id)
    }

    // Roots among affected nodes: recompute root membership from upstreams,
    // still without scanning nodes outside the affected subgraph.
    for (const id of affected) {
      const hasLiveUpstream = (this.upstreamArr[id] ?? []).some((up) =>
        this.live.has(up),
      )
      const rootAt = this.rootList.indexOf(id)
      if (hasLiveUpstream && rootAt >= 0) this.rootList.splice(rootAt, 1)
      if (!hasLiveUpstream && rootAt < 0) this.rootList.push(id)
    }

    // Critical path: linear scan only if the previous owner disappeared or
    // changed; otherwise track the max over the recomputed subgraph alone.
    if (
      this.criticalOwner !== null &&
      (!this.live.has(this.criticalOwner) ||
        affected.has(this.criticalOwner))
    ) {
      let max = 0
      let owner: number | null = null
      for (const id of this.live) {
        if (this.distanceVec[id] > max) {
          max = this.distanceVec[id]
          owner = id
        }
      }
      this.critical = max
      this.criticalOwner = owner
    } else {
      for (const id of recomputed) {
        if (this.distanceVec[id] > this.critical) {
          this.critical = this.distanceVec[id]
          this.criticalOwner = id
        }
      }
    }

    return { recomputedNodes: recomputed, nodeCount: this.live.size }
  }
}
