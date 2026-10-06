/**
 * 常驻 module Worker 池。
 * - 数量：min(navigator.hardwareConcurrency - 1, 3)，低端 4 核即 3 个；
 * - 空闲预热：requestIdleCallback 中提前构造，避免首次交互付 Worker 启动成本；
 * - 降级：构造抛错（同步 CSP 拦截）或首个 error 事件（异步 CSP 拦截）时
 *   标记 fallback，由协调器把后续节点路由到主线程切片执行。
 */

import type { MainToWorker, WorkerToMain } from './protocol'

export interface PoolCallbacks {
  onMessage(message: WorkerToMain): void
  /** 池在运行中判定 Worker 不可用（如 CSP worker-src 拦截）时回调一次。 */
  onFallback(): void
}

export class WorkerPool {
  private workers: Worker[] = []
  private cursor = 0
  private fallback = false
  private fallbackNotified = false

  constructor(private readonly callbacks: PoolCallbacks) {}

  get size(): number {
    return this.workers.length
  }

  get isFallback(): boolean {
    return this.fallback
  }

  /** 期望的池大小（不代表已构造成功）。 */
  static desiredSize(): number {
    const cores = navigator.hardwareConcurrency || 4
    return Math.max(1, Math.min(3, cores - 1))
  }

  /** 立即构造 Worker。构造失败进入降级模式，不抛错。 */
  start(): void {
    if (this.workers.length > 0 || this.fallback) return
    const desired = WorkerPool.desiredSize()
    try {
      for (let i = 0; i < desired; i++) {
        const worker = new Worker(new URL('../../workers/compute.worker.ts', import.meta.url), {
          type: 'module',
          name: `compute-${i}`,
        })
        worker.addEventListener('error', () => this.enterFallback())
        worker.addEventListener('message', (event: MessageEvent<WorkerToMain>) => {
          this.callbacks.onMessage(event.data)
        })
        this.workers.push(worker)
      }
    } catch {
      // 同步构造失败（部分浏览器对 CSP worker-src 违规直接抛 SecurityError）。
      this.enterFallback()
    }
  }

  /** 空闲预热：浏览器空闲时再付 Worker 启动成本；不支持 rIC 则退化为立即启动。 */
  prewarmOnIdle(): void {
    if (typeof requestIdleCallback === 'function') {
      requestIdleCallback(() => this.start(), { timeout: 2000 })
    } else {
      this.start()
    }
  }

  /** 轮询投递一个任务。fallback 时调用方应改走主线程切片，本方法为空操作。 */
  post(message: MainToWorker): void {
    if (this.fallback || this.workers.length === 0) return
    const worker = this.workers[this.cursor]
    this.cursor = (this.cursor + 1) % this.workers.length
    // transfer 列表在协议层显式声明；postMessage 后 buffer 所有权移出主线程。
    worker.postMessage(message, message.type === 'run' ? [...(message.transfer ?? [])] : [])
  }

  /** 广播（cancel 需要送达每个 Worker）。 */
  broadcast(message: MainToWorker): void {
    if (this.fallback) return
    for (const worker of this.workers) worker.postMessage(message)
  }

  terminate(): void {
    for (const worker of this.workers) worker.terminate()
    this.workers = []
  }

  private enterFallback(): void {
    for (const worker of this.workers) worker.terminate()
    this.workers = []
    this.fallback = true
    if (!this.fallbackNotified) {
      this.fallbackNotified = true
      this.callbacks.onFallback()
    }
  }
}
