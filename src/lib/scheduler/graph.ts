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
  constructor(readonly cycleNodes: readonly number[]) {
    super(`DAG contains a cycle involving nodes: ${cycleNodes.join(' -> ')}`)
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
