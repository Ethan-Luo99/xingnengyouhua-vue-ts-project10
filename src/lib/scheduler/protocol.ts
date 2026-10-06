/**
 * 主线程 <-> compute.worker 的通信协议。
 *
 * 全部消息都是可结构化克隆的窄 DTO：
 * - 只允许 string / number / boolean / 普通对象 / 普通数组 / ArrayBuffer；
 * - 禁止函数、DOM 节点、Svelte proxy、Symbol、Map/Set（此处未用到）跨边界。
 *
 * Transferable 约定：
 * - 消息上的 `transfer` 字段显式列出本次 postMessage 要“转移而非克隆”的 buffer；
 * - 只有 pool.ts 的 `post()` 被允许读取 `transfer` 并调用 worker.postMessage；
 * - 一旦发送，主线程不得再持有/读取被转移的 ArrayBuffer（见 DetachedBuffer）。
 */

export type NodeId = string
export type RunId = number

/** 函数输入：窄 DTO，数值与数值数组，可直接结构化克隆。 */
export interface FnInput {
  readonly seed: number
  readonly values: readonly number[]
}

/** 函数输出：窄 DTO。表格绑定函数把数值写进 value，由提交层映射到单元格。 */
export interface FnOutput {
  readonly value: number
}

/** 主线程 -> Worker：执行一个节点。 */
export interface RunTaskMessage {
  readonly type: 'run'
  readonly runId: RunId
  readonly nodeId: NodeId
  readonly fnId: string
  readonly input: FnInput
  /**
   * 本消息附带的 Transferable 列表（如 FnInput 内嵌的 ArrayBuffer）。
   * 列出即表示所有权在 postMessage 时转移给 Worker。
   */
  readonly transfer?: readonly Transferable[]
}

/** 主线程 -> Worker：把 runId 之前的所有 run 标记为不采纳。不中断在途同步计算。 */
export interface CancelMessage {
  readonly type: 'cancel'
  readonly runId: RunId
}

export type MainToWorker = RunTaskMessage | CancelMessage

/** 单个节点结果。runId 随结果走，提交层按它做过期丢弃。 */
export interface NodeResult {
  readonly nodeId: NodeId
  readonly runId: RunId
  readonly output: FnOutput
  readonly computeMs: number
}

/** Worker -> 主线程：攒批后的结果（Worker 侧约每 8ms _flush 一次）。 */
export interface ResultBatchMessage {
  readonly type: 'result'
  readonly results: readonly NodeResult[]
  /** 结果中附带的 Transferable 列表（所有权从 Worker 转回主线程）。 */
  readonly transfer?: readonly Transferable[]
}

/** Worker -> 主线程：单节点失败，不影响其他节点。 */
export interface NodeErrorMessage {
  readonly type: 'error'
  readonly runId: RunId
  readonly nodeId: NodeId
  readonly message: string
}

export type WorkerToMain = ResultBatchMessage | NodeErrorMessage

declare const detachedBrand: unique symbol

/**
 * 品牌类型：一个已经被 transfer 走的 ArrayBuffer。
 * TypeScript 无法在类型层面真正检测运行时 detachment，这里的防线是：
 * pool.post() 发送带 transfer 的消息后，调用方只能继续持有 DetachedBuffer，
 * 把它传给任何需要 ArrayBuffer 的 API 都会编译报错。
 */
export type DetachedBuffer = ArrayBuffer & { readonly [detachedBrand]: 'transferred' }

/** 标记一个 buffer 已被转移。只能在 pool.post 的 transfer 路径中调用。 */
export function asDetached(buffer: ArrayBuffer): DetachedBuffer {
  return buffer as DetachedBuffer
}
