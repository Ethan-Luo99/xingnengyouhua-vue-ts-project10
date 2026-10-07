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
 *
 * Incremental revision ({@link applyPatch}) mutates a compiled graph in place
 * inside one transaction: new nodes append at the high-water id, removed
 * nodes keep a tombstone slot (ids stay stable for run/pool bookkeeping), and
 * levels / critical distances are recomputed only for the added nodes plus the
 * transitive downstream closure of removed nodes — never for the whole graph.
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
  /** Per-node cost estimate used for weighted distance recompilation. */
  readonly costs: NumberVector
  /** True for live nodes; false slots are tombstones of removed nodes. */
  readonly alive: Uint8Array
  /** Critical-path estimate for the whole graph. */
  readonly criticalPathMs: number
  /** Ids of live nodes without any upstream dependency. */
  readonly roots: readonly number[]
}

export class DagCycleError extends Error {
  /**
   * @param cycleNodes nodes participating in the cycle
   * @param cyclePath explicit directed node path that closes the cycle, e.g.
   *        [a, b, c, a]; populated by {@link applyPatch} for an attempted
   *        cyclic insertion, `undefined` for the bulk compileGraph path
   */
  constructor(
    readonly cycleNodes: readonly number[],
    readonly cyclePath?: readonly number[],
  ) {
    const shown =
      cyclePath && cyclePath.length > 0
        ? cyclePath.join(' -> ')
        : cycleNodes.join(' -> ')
    super(`DAG contains a cycle involving nodes: ${shown}`)
    this.name = 'DagCycleError'
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
  const costs = new Float64Array(nodeCount)
  const alive = new Uint8Array(nodeCount).fill(1)
  for (const node of nodes) costs[node.id] = node.costMs
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
    costs,
    alive,
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

/* ====================================================================== */
/* Incremental revision                                                   */
/* ====================================================================== */

/**
 * Nodes to insert in one revision. Ids MUST be fresh: `>= graph.nodeCount`
 * (the current high-water mark), pairwise distinct within the patch. Arrays
 * are grown to `max(id) + 1`; removed ids are never reused.
 */
export interface AddNodeInput extends CompileNodeInput {}

/** Observed work done by one {@link applyPatch} call (proof of incrementality). */
export interface PatchReport {
  readonly added: readonly number[]
  readonly removed: readonly number[]
  /** Nodes whose level / criticalDistance were actually recomputed. */
  readonly recomputedNodes: readonly number[]
  /** Nodes visited while collecting the affected downstream closure. */
  readonly closureVisited: number
  /** True only for the one-time O(V) numeric critical-path fold. */
  readonly didCriticalPathFold: boolean
}

/** CompiledGraph fields kept mutable for the in-place transaction. */
interface MutableGraph {
  nodeCount: number
  edges: DagEdge[]
  upstream: number[][]
  downstream: number[][]
  order: number[]
  levels: Int32Array
  criticalDistance: Float64Array
  costs: Float64Array
  alive: Uint8Array
  criticalPathMs: number
  roots: number[]
}

function asMutable(graph: CompiledGraph): MutableGraph {
  return {
    nodeCount: graph.nodeCount,
    edges: [...graph.edges],
    upstream: graph.upstream as number[][],
    downstream: graph.downstream as number[][],
    order: [...graph.order],
    levels: graph.levels as Int32Array,
    criticalDistance: graph.criticalDistance as Float64Array,
    costs: graph.costs as Float64Array,
    alive: graph.alive,
    criticalPathMs: graph.criticalPathMs,
    roots: [...graph.roots],
  }
}

function commitMutable(graph: CompiledGraph, mutable: MutableGraph): void {
  const target = graph as unknown as MutableGraph
  target.nodeCount = mutable.nodeCount
  target.edges = mutable.edges
  target.upstream = mutable.upstream
  target.downstream = mutable.downstream
  target.order = mutable.order
  target.levels = mutable.levels
  target.criticalDistance = mutable.criticalDistance
  target.costs = mutable.costs
  target.alive = mutable.alive
  target.criticalPathMs = mutable.criticalPathMs
  target.roots = mutable.roots
}

/**
 * Validate a patch without touching the graph, returning normalized inputs.
 * Any failure here leaves the original graph exactly as it was.
 */
function validatePatch(
  graph: MutableGraph,
  addNodes: readonly AddNodeInput[],
  removeNodes: readonly number[],
): {
  addSet: Set<number>
  removeSet: Set<number>
  affected: Set<number>
  closureVisited: number
} {
  const addSet = new Set<number>()
  for (const node of addNodes) {
    if (node.id < graph.nodeCount) {
      throw new Error(
        `applyPatch: node id ${node.id} already exists (ids must be >= nodeCount ${graph.nodeCount})`,
      )
    }
    if (addSet.has(node.id)) {
      throw new Error(`applyPatch: duplicate added node id ${node.id}`)
    }
    if (node.hardEdges.length !== node.deps.length) {
      throw new Error(`applyPatch: hardEdges length mismatch for node ${node.id}`)
    }
    const seen = new Set<number>()
    for (const from of node.deps) {
      if (seen.has(from)) {
        throw new Error(`applyPatch: duplicate dependency ${from} of node ${node.id}`)
      }
      seen.add(from)
      // Unknown-id validation runs in the second pass once `addSet` covers
      // sibling nodes added in this same patch.
    }
    addSet.add(node.id)
  }

  const removeSet = new Set<number>()
  for (const id of removeNodes) {
    if (id < 0 || id >= graph.nodeCount || graph.alive[id] === 0) {
      throw new Error(`applyPatch: cannot remove non-live node ${id}`)
    }
    if (addSet.has(id)) {
      throw new Error(`applyPatch: node ${id} is both added and removed`)
    }
    if (removeSet.has(id)) {
      throw new Error(`applyPatch: duplicate removed node id ${id}`)
    }
    removeSet.add(id)
  }

  // Second dep pass now that addSet is complete: no dependency may vanish.
  for (const node of addNodes) {
    for (const from of node.deps) {
      if (removeSet.has(from)) {
        throw new Error(
          `applyPatch: node ${node.id} depends on node ${from} removed in the same patch`,
        )
      }
      const isOld = from < graph.nodeCount
      if (!isOld && !addSet.has(from)) {
        throw new Error(`applyPatch: node ${node.id} depends on unknown node ${from}`)
      }
    }
  }

  // Cycle check restricted to the induced subgraph of ADDED nodes. Every new
  // edge points INTO a new node, so no existing node can lie on a new cycle;
  // DFS follows new -> new upstream edges and reconstructs the closing path.
  const addById = new Map<number, AddNodeInput>()
  for (const node of addNodes) addById.set(node.id, node)
  const color = new Map<number, 0 | 1 | 2>()
  const stack: number[] = []
  const visit = (vertex: number): readonly number[] | null => {
    color.set(vertex, 1)
    stack.push(vertex)
    const spec = addById.get(vertex)
    for (const dep of spec?.deps ?? []) {
      if (!addSet.has(dep)) continue
      const depColor = color.get(dep) ?? 0
      if (depColor === 1) {
        const start = stack.indexOf(dep)
        return [...stack.slice(start), dep]
      }
      if (depColor === 0) {
        const cycle = visit(dep)
        if (cycle) return cycle
      }
    }
    stack.pop()
    color.set(vertex, 2)
    return null
  }
  for (const node of addNodes) {
    if ((color.get(node.id) ?? 0) === 0) {
      const cycle = visit(node.id)
      if (cycle) throw new DagCycleError([...cycle], cycle)
    }
  }

  // Affected closure: added nodes plus, for every removed node, its
  // transitive downstream closure in the graph as it stands BEFORE the patch,
  // excluding nodes that are themselves being removed.
  const affected = new Set<number>(addSet)
  let closureVisited = 0
  const work: number[] = []
  for (const id of removeSet) {
    for (const child of graph.downstream[id] ?? []) {
      if (!removeSet.has(child) && !affected.has(child)) work.push(child)
    }
  }
  while (work.length > 0) {
    const id = work.pop()
    if (id === undefined || affected.has(id)) continue
    affected.add(id)
    closureVisited += 1
    for (const child of graph.downstream[id] ?? []) {
      if (!removeSet.has(child) && !affected.has(child)) work.push(child)
    }
  }

  return { addSet, removeSet, affected, closureVisited }
}

/**
 * Apply one transactional revision.
 *
 * Either every structural edit and every affected recomputation takes effect,
 * or (on any validation failure, including {@link DagCycleError}) the graph
 * is byte-for-byte unchanged: validation and cycle detection run fully before
 * mutation begins.
 */
export function applyPatch(
  graph: CompiledGraph,
  addNodes: readonly AddNodeInput[],
  removeNodes: readonly number[] = [],
): PatchReport {
  const staged = asMutable(graph)
  const { addSet, removeSet, affected, closureVisited } = validatePatch(
    staged,
    addNodes,
    removeNodes,
  )

  /* ---- grow index space to the high-water id ---- */
  const highId = addNodes.reduce((max, node) => Math.max(max, node.id), staged.nodeCount - 1)
  const newSize = Math.max(staged.nodeCount, highId + 1)
  if (newSize > staged.nodeCount) {
    const grow = <T>(old: readonly T[], fill: () => T): T[] => {
      const grown = old.slice()
      while (grown.length < newSize) grown.push(fill())
      return grown
    }
    const growTyped = <A extends Int32Array | Float64Array | Uint8Array>(
      old: A,
      ctor: new (length: number) => A,
    ): A => {
      const grown = new ctor(newSize)
      grown.set(old as unknown as ArrayLike<number> & { length: number })
      return grown
    }
    staged.upstream = grow(staged.upstream, () => [])
    staged.downstream = grow(staged.downstream, () => [])
    staged.levels = growTyped(staged.levels, Int32Array)
    staged.criticalDistance = growTyped(staged.criticalDistance, Float64Array)
    staged.costs = growTyped(staged.costs, Float64Array)
    staged.alive = growTyped(staged.alive, Uint8Array)
  }

  /* ---- insert nodes (edges point INTO the new node) ---- */
  const addedInTopoOrder: number[] = []
  for (const node of addNodes) {
    staged.alive[node.id] = 1
    staged.costs[node.id] = node.costMs
    staged.upstream[node.id] = [...node.deps]
    node.deps.forEach((from, index) => {
      const edge: DagEdge = {
        from,
        to: node.id,
        hard: node.hardEdges[index] ?? false,
      }
      staged.edges.push(edge)
      staged.downstream[from]?.push(node.id)
    })
  }

  /* ---- remove nodes and their incident edges ---- */
  for (const id of removeSet) {
    for (const from of staged.upstream[id] ?? []) {
      const siblings = staged.downstream[from]
      if (siblings) {
        const at = siblings.indexOf(id)
        if (at >= 0) siblings.splice(at, 1)
      }
    }
    for (const child of staged.downstream[id] ?? []) {
      const parents = staged.upstream[child]
      if (parents) {
        const at = parents.indexOf(id)
        if (at >= 0) parents.splice(at, 1)
      }
    }
    staged.upstream[id] = []
    staged.downstream[id] = []
    staged.alive[id] = 0
    staged.levels[id] = 0
    staged.criticalDistance[id] = 0
  }
  // One linear pass over the flat edge list to drop dead edges. This is plain
  // array bookkeeping (no per-node topology math); levels/distances are never
  // recomputed from it here.
  staged.edges = staged.edges.filter(
    (edge) => staged.alive[edge.to] === 1 && staged.alive[edge.from] === 1,
  )

  /* ---- repair the topological order locally ----
   * Deleting edges/vertices cannot invalidate the old order among survivors.
   * New nodes are appended in the topo order of their induced subgraph
   * (Kahn over new-new edges only). */
  staged.order = staged.order.filter((id) => staged.alive[id] === 1)
  {
    const indegreeAmongNew = new Map<number, number>()
    for (const node of addNodes) {
      let count = 0
      for (const from of node.deps) if (addSet.has(from)) count += 1
      indegreeAmongNew.set(node.id, count)
    }
    const queue = addNodes
      .filter((node) => (indegreeAmongNew.get(node.id) ?? 0) === 0)
      .map((node) => node.id)
    let head = 0
    while (head < queue.length) {
      const id = queue[head]
      head += 1
      addedInTopoOrder.push(id)
      for (const child of staged.downstream[id] ?? []) {
        if (!addSet.has(child)) continue
        const next = (indegreeAmongNew.get(child) ?? 0) - 1
        indegreeAmongNew.set(child, next)
        if (next === 0) queue.push(child)
      }
    }
    for (const id of addedInTopoOrder) staged.order.push(id)
  }

  /* ---- repair the roots list locally ---- */
  staged.roots = staged.roots.filter((id) => staged.alive[id] === 1)
  for (const node of addNodes) {
    if ((staged.upstream[node.id]?.length ?? 0) === 0) staged.roots.push(node.id)
  }

  /* ---- recompute levels + critical distance ONLY for affected nodes ---- */
  const recomputed: number[] = []
  for (const id of staged.order) {
    if (!affected.has(id)) continue
    recomputed.push(id)
    const parents = staged.upstream[id] ?? []
    if (parents.length === 0) {
      staged.levels[id] = 0
      staged.criticalDistance[id] = staged.costs[id]
      continue
    }
    let maxLevel = -1
    let maxDist = 0
    for (const from of parents) {
      if (staged.levels[from] > maxLevel) maxLevel = staged.levels[from]
      const candidate = staged.criticalDistance[from] + staged.costs[id]
      if (candidate > maxDist) maxDist = candidate
    }
    staged.levels[id] = maxLevel + 1
    staged.criticalDistance[id] = maxDist
  }

  /* ---- critical-path fold: O(V) numeric max over one typed array ----
   * No adjacency is touched; this is a scalar aggregation, not graph
   * recompilation. Distances of unaffected nodes are provably unchanged. */
  let criticalPathMs = 0
  for (let id = 0; id < newSize; id += 1) {
    if (staged.alive[id] === 1 && staged.criticalDistance[id] > criticalPathMs) {
      criticalPathMs = staged.criticalDistance[id]
    }
  }
  staged.criticalPathMs = criticalPathMs
  staged.nodeCount = newSize

  commitMutable(graph, staged)
  return {
    added: addedInTopoOrder,
    removed: [...removeSet],
    recomputedNodes: recomputed,
    closureVisited,
    didCriticalPathFold: true,
  }
}
