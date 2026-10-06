/**
 * 调度协调器（主线程）。
 * - 单调递增 runId 门控：每次 startRun 生成新 runId，旧轮结果在双缓冲层丢弃；
 * - 就绪优先队列：ReadySet 按关键距离大者优先出队；
 * - 双缓冲提交：Worker/主线程结果先写非响应式 back buffer，
 *   requestAnimationFrame 每帧至多 swap 一次并回调 onCommit；
 * - 主线程切片：快函数（及降级时的全部函数）以 <=4ms 切片执行，
 *   长尾函数在降级时通过 stepper 分块，切片间用 MessageChannel 让出。
 */

import type { FnDef, Stepper } from '../domain/functions'
import type { Dag } from './graph'
import { ReadySet } from './graph'
import { WorkerPool } from './pool'
import type { FnInput, FnOutput, NodeId, RunId, WorkerToMain } from './protocol'

export interface Commit {
  readonly runId: RunId
  readonly results: ReadonlyMap<NodeId, FnOutput>
  readonly errors: ReadonlyMap<NodeId, string>
  /** 本轮 run 是否已全部完成。 */
  readonly done: boolean
}

export interface CoordinatorOptions {
  readonly dag: Dag
  readonly registry: ReadonlyMap<string, FnDef>
  readonly inputFor: (nodeId: NodeId, runId: RunId) => FnInput
  readonly onCommit: (commit: Commit) => void
}

/** 主线程单次切片预算（ms）。 */
const MAIN_SLICE_MS = 4

interface MainTask {
  readonly nodeId: NodeId
  readonly runId: RunId
  readonly fn: FnDef
  stepper?: Stepper
}

export class Coordinator {
  private currentRunId = 0
  private readonly pool: WorkerPool
  private ready: ReadySet | null = null
  private remaining = 0
  private runDone = false

  // 非响应式双缓冲：back 累积，rAF 时 swap 成 front 一次性提交。
  private back = new Map<NodeId, FnOutput>()
  private backErrors = new Map<NodeId, string>()
  private rafScheduled = false

  private readonly mainQueue: MainTask[] = []
  private mainScheduled = false
  private readonly yieldChannel = new MessageChannel()
  private readonly inFlightWorker = new Map<NodeId, RunId>()

  constructor(private readonly options: CoordinatorOptions) {
    this.pool = new WorkerPool({
      onMessage: (message) => this.handleWorkerMessage(message),
      onFallback: () => this.handlePoolFallback(),
    })
    this.pool.prewarmOnIdle()
    this.yieldChannel.port1.onmessage = () => this.pumpMainQueue()
  }

  get runId(): RunId {
    return this.currentRunId
  }

  /** 触发一轮执行。A=全量，B=子集；每个字符都会调用一次本方法（B 语义不防抖）。 */
  startRun(nodeIds: readonly NodeId[]): RunId {
    const runId = ++this.currentRunId
    this.runDone = false
    // 通知 Worker：runId 之前的轮次不再采纳（不中断在途同步计算）。
    this.pool.broadcast({ type: 'cancel', runId })
    // 双缓冲层丢弃旧轮残留：旧 run 的结果不允许进入下一轮提交。
    this.back.clear()
    this.backErrors.clear()
    this.inFlightWorker.clear()

    const subset = new Set(nodeIds)
    this.ready = new ReadySet(this.options.dag, subset)
    this.remaining = subset.size
    this.pool.start() // 预热未触发时兜底启动。
    this.dispatchReady()
    this.scheduleMainPump()
    return runId
  }

  private dispatchReady(): void {
    if (!this.ready) return
    for (;;) {
      const nodeId = this.ready.pop()
      if (nodeId === undefined) return
      const fn = this.options.registry.get(nodeId)
      if (!fn) {
        this.completeNode(nodeId, this.currentRunId)
        continue
      }
      if (fn.route === 'main' || this.pool.isFallback) {
        // 主线程切片通道；长尾在降级时携带 stepper 分块执行。
        this.mainQueue.push({ nodeId, runId: this.currentRunId, fn })
      } else {
        this.inFlightWorker.set(nodeId, this.currentRunId)
        this.pool.post({
          type: 'run',
          runId: this.currentRunId,
          nodeId,
          fnId: fn.id,
          input: this.options.inputFor(nodeId, this.currentRunId),
        })
      }
    }
  }

  private scheduleMainPump(): void {
    if (this.mainScheduled || this.mainQueue.length === 0) return
    this.mainScheduled = true
    this.yieldChannel.port2.postMessage(null)
  }

  private pumpMainQueue(): void {
    this.mainScheduled = false
    const deadline = performance.now() + MAIN_SLICE_MS
    while (this.mainQueue.length > 0) {
      const task = this.mainQueue[0]
      if (task.runId !== this.currentRunId) {
        // 旧轮主线程任务：直接丢弃，不执行。
        this.mainQueue.shift()
        continue
      }
      if (task.fn.stepper) {
        // 长尾降级：分块推进，单片不超过剩余预算。
        task.stepper ??= task.fn.stepper(this.options.inputFor(task.nodeId, task.runId))
        const done = task.stepper.step(Math.max(0.5, deadline - performance.now()))
        if (!done) {
          // 让出事件循环，下一片继续。
          this.scheduleMainPump()
          return
        }
        this.mainQueue.shift()
        this.recordResult(task.nodeId, task.runId, task.stepper.output(), 0)
      } else {
        const started = performance.now()
        if (started >= deadline) {
          // 预算耗尽，让出后再执行下一个。
          this.scheduleMainPump()
          return
        }
        this.mainQueue.shift()
        const output = task.fn.run(this.options.inputFor(task.nodeId, task.runId))
        this.recordResult(task.nodeId, task.runId, output, performance.now() - started)
      }
      if (performance.now() >= deadline) {
        this.scheduleMainPump()
        return
      }
    }
  }

  private handleWorkerMessage(message: WorkerToMain): void {
    if (message.type === 'result') {
      for (const result of message.results) {
        // 双缓冲层丢弃旧 runId：这是过期结果的唯一权威丢弃点。
        if (result.runId !== this.currentRunId) continue
        this.inFlightWorker.delete(result.nodeId)
        this.recordResult(result.nodeId, result.runId, result.output, result.computeMs)
      }
    } else {
      if (message.runId !== this.currentRunId) return
      this.inFlightWorker.delete(message.nodeId)
      this.backErrors.set(message.nodeId, message.message)
      this.completeNode(message.nodeId, message.runId)
    }
  }

  /** Worker 不可用：把在途任务迁回主线程切片队列。 */
  private handlePoolFallback(): void {
    for (const [nodeId, runId] of this.inFlightWorker) {
      if (runId !== this.currentRunId) continue
      const fn = this.options.registry.get(nodeId)
      if (fn) this.mainQueue.push({ nodeId, runId, fn })
    }
    this.inFlightWorker.clear()
    this.scheduleMainPump()
  }

  private recordResult(nodeId: NodeId, runId: RunId, output: FnOutput, _computeMs: number): void {
    if (runId !== this.currentRunId) return // 双缓冲层过期守卫
    this.back.set(nodeId, output)
    this.completeNode(nodeId, runId)
  }

  private completeNode(nodeId: NodeId, runId: RunId): void {
    if (runId !== this.currentRunId) return
    this.remaining--
    this.ready?.release(nodeId) // 新就绪节点已推回堆，由 dispatchReady 派发
    this.dispatchReady()
    if (this.remaining === 0) this.runDone = true
    this.scheduleCommit()
    this.scheduleMainPump()
  }

  /** rAF 提交：每帧至多一次，把 back buffer swap 出去。 */
  private scheduleCommit(): void {
    if (this.rafScheduled) return
    this.rafScheduled = true
    requestAnimationFrame(() => {
      this.rafScheduled = false
      if (this.back.size === 0 && this.backErrors.size === 0 && !this.runDone) return
      const results = this.back
      const errors = this.backErrors
      this.back = new Map()
      this.backErrors = new Map()
      const done = this.runDone
      this.options.onCommit({ runId: this.currentRunId, results, errors, done })
    })
  }

  terminate(): void {
    this.pool.terminate()
  }
}
