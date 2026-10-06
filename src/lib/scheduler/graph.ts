/**
 * 单向 DAG：校验（环检测）、拓扑分层、关键距离、就绪集合维护。
 *
 * 边分两种：
 * - 硬边（hardDeps）：调度约束，上游未完成时下游不就绪；
 * - 软边（softDeps）：仅数据流，输入取上游最近一次可用值，不阻塞调度。
 * 环检测对硬边+软边的全集执行（两种边都不允许成环）。
 */

export interface DagNode {
  readonly id: string
  readonly hardDeps: readonly string[]
  readonly softDeps: readonly string[]
  /** 预估耗时（ms），用于关键距离与路由。 */
  readonly costMs: number
}

export interface Dag {
  readonly nodes: ReadonlyMap<string, DagNode>
  /** 硬边下游：id -> 依赖它的节点列表。 */
  readonly hardDependents: ReadonlyMap<string, readonly string[]>
  /** 拓扑分层（Kahn），第 0 层为无依赖节点。 */
  readonly layers: readonly (readonly string[])[]
  /** 关键距离：从该节点出发到汇点的最长加权路径（含自身 costMs）。 */
  readonly criticalDistance: ReadonlyMap<string, number>
}

export function buildDag(nodes: readonly DagNode[]): Dag {
  const byId = new Map<string, DagNode>()
  for (const node of nodes) {
    if (byId.has(node.id)) throw new Error(`duplicate node id: ${node.id}`)
    byId.set(node.id, node)
  }
  for (const node of nodes) {
    for (const dep of [...node.hardDeps, ...node.softDeps]) {
      if (!byId.has(dep)) throw new Error(`unknown dep ${dep} of ${node.id}`)
    }
  }

  // Kahn 拓扑分层 + 环检测（对全部边）。
  const indegree = new Map<string, number>()
  const dependents = new Map<string, string[]>()
  for (const node of nodes) {
    const all = new Set([...node.hardDeps, ...node.softDeps])
    indegree.set(node.id, all.size)
    for (const dep of all) {
      const list = dependents.get(dep)
      if (list) list.push(node.id)
      else dependents.set(dep, [node.id])
    }
  }
  const layers: string[][] = []
  let frontier = nodes.filter((n) => indegree.get(n.id) === 0).map((n) => n.id)
  let visited = 0
  while (frontier.length > 0) {
    layers.push(frontier)
    visited += frontier.length
    const next: string[] = []
    for (const id of frontier) {
      for (const down of dependents.get(id) ?? []) {
        const d = (indegree.get(down) ?? 0) - 1
        indegree.set(down, d)
        if (d === 0) next.push(down)
      }
    }
    frontier = next
  }
  if (visited !== nodes.length) {
    const remaining = nodes.filter((n) => (indegree.get(n.id) ?? 0) > 0).map((n) => n.id)
    throw new Error(`cycle detected in DAG, involved nodes: ${remaining.slice(0, 10).join(', ')}`)
  }

  // 关键距离：逆拓扑序递推，cd(n) = cost(n) + max(cd(下游))。
  const critical = new Map<string, number>()
  for (let i = layers.length - 1; i >= 0; i--) {
    for (const id of layers[i]) {
      const node = byId.get(id)!
      let best = 0
      for (const down of dependents.get(id) ?? []) {
        const d = critical.get(down) ?? 0
        if (d > best) best = d
      }
      critical.set(id, node.costMs + best)
    }
  }

  // 硬边下游表（调度用）。
  const hardDependents = new Map<string, string[]>()
  for (const node of nodes) {
    for (const dep of node.hardDeps) {
      const list = hardDependents.get(dep)
      if (list) list.push(node.id)
      else hardDependents.set(dep, [node.id])
    }
  }

  return { nodes: byId, hardDependents, layers, criticalDistance: critical }
}

/** 就绪集合：只统计 run 子集内部的硬边入度；按关键距离大者优先出队。 */
export class ReadySet {
  private readonly indegree = new Map<string, number>()
  private readonly heap: string[] = []

  constructor(
    private readonly dag: Dag,
    subset: ReadonlySet<string>,
  ) {
    for (const id of subset) {
      const node = dag.nodes.get(id)
      if (!node) throw new Error(`unknown node in run subset: ${id}`)
      let degree = 0
      for (const dep of node.hardDeps) if (subset.has(dep)) degree++
      this.indegree.set(id, degree)
      if (degree === 0) this.push(id)
    }
  }

  get size(): number {
    return this.heap.length
  }

  /** 取关键距离最大的就绪节点。 */
  pop(): string | undefined {
    const heap = this.heap
    if (heap.length === 0) return undefined
    const top = heap[0]
    const last = heap.pop()!
    if (heap.length > 0) {
      heap[0] = last
      this.siftDown(0)
    }
    return top
  }

  /** 节点完成后释放其硬边下游，返回新就绪的节点 id。 */
  release(doneId: string): string[] {
    const newly: string[] = []
    for (const down of this.dag.hardDependents.get(doneId) ?? []) {
      if (!this.indegree.has(down)) continue
      const d = this.indegree.get(down)! - 1
      this.indegree.set(down, d)
      if (d === 0) {
        this.push(down)
        newly.push(down)
      }
    }
    return newly
  }

  private priority(id: string): number {
    return this.dag.criticalDistance.get(id) ?? 0
  }

  private push(id: string): void {
    const heap = this.heap
    heap.push(id)
    let i = heap.length - 1
    while (i > 0) {
      const parent = (i - 1) >> 1
      if (this.priority(heap[parent]) >= this.priority(heap[i])) break
      ;[heap[parent], heap[i]] = [heap[i], heap[parent]]
      i = parent
    }
  }

  private siftDown(start: number): void {
    const heap = this.heap
    let i = start
    for (;;) {
      const left = i * 2 + 1
      const right = left + 1
      let largest = i
      if (left < heap.length && this.priority(heap[left]) > this.priority(heap[largest])) largest = left
      if (right < heap.length && this.priority(heap[right]) > this.priority(heap[largest])) largest = right
      if (largest === i) return
      ;[heap[largest], heap[i]] = [heap[i], heap[largest]]
      i = largest
    }
  }
}
