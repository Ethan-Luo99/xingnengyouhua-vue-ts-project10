/**
 * 合成数据集注册表：510 个快函数（0~1ms）、60 个中函数（5~50ms）、
 * 3 个长尾函数（50~200ms，内部为同步 CPU 循环），以及带 30% 硬边的单向 DAG。
 *
 * 本模块是同构的：主线程与 compute.worker 都会 import，禁止引用 DOM/window。
 * 所有“随机”都来自确定性 LCG，保证每次构建的 DAG 与耗时分布一致。
 */

import { buildDag, type Dag, type DagNode } from '../scheduler/graph'
import type { FnInput, FnOutput } from '../scheduler/protocol'

export interface FnDef {
  readonly id: string
  readonly costMs: number
  /** 路由建议：<1ms 留主线程切片，其余进 Worker。 */
  readonly route: 'main' | 'worker'
  run(input: FnInput): FnOutput
  /**
   * 长尾函数专用：可恢复的分块执行器。
   * 主线程降级路径用它把 50~200ms 的同步循环切成 <=4ms 的片。
   */
  readonly stepper?: (input: FnInput) => Stepper
}

export interface Stepper {
  /** 在 budgetMs 内推进计算；返回是否完成。 */
  step(budgetMs: number): boolean
  /** step 返回 true 后读取结果。 */
  output(): FnOutput
}

export interface TableBinding {
  /** 该函数结果绑定的起始行（每个表绑定函数负责 25 行）。 */
  readonly rowStart: number
  /** 更新行内的哪一列。 */
  readonly column: 1 | 2 | 3
}

export interface Domain {
  readonly dag: Dag
  readonly registry: ReadonlyMap<string, FnDef>
  readonly allNodeIds: readonly string[]
  /** nodeId -> 表格绑定（200 个表绑定函数）。 */
  readonly tableBinding: ReadonlyMap<string, TableBinding>
}

export const FAST_COUNT = 510
export const MEDIUM_COUNT = 60
export const LONG_COUNT = 3
export const TABLE_BOUND_COUNT = 200
export const TABLE_ROWS = 5000
export const ROWS_PER_BINDING = TABLE_ROWS / TABLE_BOUND_COUNT // 25

/** 确定性 LCG，避免 Math.random 导致的不可复现。 */
function lcg(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    return state / 0x100000000
  }
}

/** 同步 CPU 空转约 ms 毫秒（长尾/中函数的“真实负载”）。 */
function spin(ms: number, salt: number): number {
  const end = performance.now() + ms
  let acc = salt
  while (performance.now() < end) {
    for (let i = 0; i < 1000; i++) acc = (acc + Math.sqrt(acc + i)) % 1e9
  }
  return acc
}

function makeInput(seed: number): FnInput {
  return { seed, values: [seed % 7, (seed * 3) % 11, (seed * 7) % 13] }
}

export function buildSyntheticDomain(): Domain {
  const rand = lcg(0x5eed)
  const registry = new Map<string, FnDef>()
  const nodes: DagNode[] = []

  // 510 个快函数：纯数值计算，单次 <1ms。
  for (let i = 0; i < FAST_COUNT; i++) {
    const id = `fast-${i}`
    const costMs = 0.1 + rand() * 0.9
    registry.set(id, {
      id,
      costMs,
      route: 'main',
      run(input) {
        let acc = input.seed
        for (const v of input.values) acc = (acc * 31 + v) % 1000003
        return { value: acc % 1000 }
      },
    })
    nodes.push({ id, hardDeps: [], softDeps: [], costMs })
  }

  // 60 个中函数：5~50ms 同步 CPU 循环。
  for (let i = 0; i < MEDIUM_COUNT; i++) {
    const id = `med-${i}`
    const costMs = 5 + rand() * 45
    registry.set(id, {
      id,
      costMs,
      route: 'worker',
      run(input) {
        return { value: spin(costMs, input.seed) % 1000 }
      },
    })
    nodes.push({ id, hardDeps: [], softDeps: [], costMs })
  }

  // 3 个长尾函数：50~200ms 同步 CPU 循环，附带可恢复 stepper 供主线程降级切片。
  for (let i = 0; i < LONG_COUNT; i++) {
    const id = `long-${i}`
    const costMs = 50 + rand() * 150
    registry.set(id, {
      id,
      costMs,
      route: 'worker',
      run(input) {
        return { value: spin(costMs, input.seed) % 1000 }
      },
      stepper(input) {
        const deadlineAt = performance.now() + costMs
        let acc = input.seed
        return {
          step(budgetMs: number): boolean {
            const sliceEnd = performance.now() + budgetMs
            while (performance.now() < sliceEnd) {
              for (let k = 0; k < 1000; k++) acc = (acc + Math.sqrt(acc + k)) % 1e9
              if (performance.now() >= deadlineAt) return true
            }
            return performance.now() >= deadlineAt
          },
          output(): FnOutput {
            return { value: acc % 1000 }
          },
        }
      },
    })
    nodes.push({ id, hardDeps: [], softDeps: [], costMs })
  }

  // 单向 DAG：每个节点只向更早的节点连边（天然无环），约 30% 的边为硬边。
  for (let i = 1; i < nodes.length; i++) {
    const edgeCount = 1 + Math.floor(rand() * 3)
    const hard: string[] = []
    const soft: string[] = []
    for (let e = 0; e < edgeCount; e++) {
      const target = Math.floor(rand() * i)
      const dep = nodes[target].id
      if (hard.includes(dep) || soft.includes(dep)) continue
      if (rand() < 0.3) hard.push(dep)
      else soft.push(dep)
    }
    nodes[i] = { ...nodes[i], hardDeps: hard, softDeps: soft }
  }

  // 前 200 个快函数的输出绑定到表格，每个负责连续 25 行的一列。
  const tableBinding = new Map<string, TableBinding>()
  for (let i = 0; i < TABLE_BOUND_COUNT; i++) {
    tableBinding.set(`fast-${i}`, {
      rowStart: i * ROWS_PER_BINDING,
      column: ((i % 3) + 1) as 1 | 2 | 3,
    })
  }

  return {
    dag: buildDag(nodes),
    registry,
    allNodeIds: nodes.map((n) => n.id),
    tableBinding,
  }
}

/** B 模式：每个字符触发一轮子集。子集由字符确定性地导出，含快/中函数，偶尔含长尾。 */
export function subsetForChar(charCode: number, allNodeIds: readonly string[]): string[] {
  const rand = lcg(charCode * 2654435761)
  const subset = new Set<string>()
  while (subset.size < 40) subset.add(`fast-${Math.floor(rand() * FAST_COUNT)}`)
  for (let i = 0; i < 5; i++) subset.add(`med-${Math.floor(rand() * MEDIUM_COUNT)}`)
  if (charCode % 7 === 0) subset.add(`long-${charCode % LONG_COUNT}`)
  // 保证子集都在 DAG 内。
  return [...subset].filter((id) => allNodeIds.includes(id))
}

export { makeInput }
