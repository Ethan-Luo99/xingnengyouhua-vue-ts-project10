/**
 * Structured-clone message protocol between the coordinator and compute workers.
 *
 * Every variant is a narrow, plain DTO: no functions, no DOM nodes, no Svelte
 * proxies, no Error subclasses (errors are flattened to `{ name, message }`).
 *
 * Transferable discipline
 * ------------------------
 * The only transferred fields are:
 *   - TaskInput.input  : an OwnedArrayBuffer sent main -> worker
 *   - TaskResult.output: an OwnedArrayBuffer sent worker -> main (batched)
 * They are tagged with the opaque `OwnedArrayBuffer` brand so ordinary code
 * cannot read or re-send an ArrayBuffer that has already been (or is about to
 * be) transferred. Buffers enter the ownership system once via `ownBuffer()`
 * and leave it exactly once via `takeTransfers()`; the receiving side gets a
 * plain, freshly-owned `ArrayBuffer` back from `receive*()`.
 */

/** Protocol version; bump when the wire shape changes. */
export const PROTOCOL_VERSION = 1 as const

/** Monotonic coordinator run id (each click / each keystroke allocates one). */
export type RunId = number
/** Node identity within the DAG (its index in FUNCTIONS). */
export type NodeId = number
/** Stable worker slot index 0..N-1. */
export type WorkerId = number
/** Id of a registered pure function. */
export type FunctionId = string

declare const transferredBrand: unique symbol

/**
 * A buffer whose ownership is bound to a single postMessage transfer.
 * The type is opaque: callers cannot index it, read its bytes, or pass it
 * anywhere except {@link takeTransfers}. There is no runtime wrapper — the
 * brand exists only at the type level, exactly like the transfer list itself.
 */
export type OwnedArrayBuffer = ArrayBuffer & {
  readonly [transferredBrand]: 'transferred-once'
}

/** Move a freshly allocated, never-shared ArrayBuffer into owned/transfer mode. */
export function ownBuffer(buffer: ArrayBuffer): OwnedArrayBuffer {
  return buffer as OwnedArrayBuffer
}

/** Serialized error shape (Error instances are not portable across versions). */
export interface ErrorDto {
  readonly name: string
  readonly message: string
}

/* ---------- main -> worker ---------- */

export interface TaskInput {
  readonly kind: 'task'
  readonly runId: RunId
  readonly nodeId: NodeId
  readonly fnId: FunctionId
  /** TRANSFERRED main -> worker. Unreadable after the message is posted. */
  readonly input: OwnedArrayBuffer
}

export interface CancelInput {
  readonly kind: 'cancel'
  /** Ids the worker should stop adopting results for. */
  readonly runIds: readonly RunId[]
}

export interface ShutdownInput {
  readonly kind: 'shutdown'
}

export type MainToWorker = TaskInput | CancelInput | ShutdownInput

/* ---------- worker -> main ---------- */

export interface ReadyOutput {
  readonly kind: 'ready'
  readonly workerId: WorkerId
}

export interface TaskResult {
  readonly kind: 'result'
  readonly runId: RunId
  readonly nodeId: NodeId
  /** TRANSFERRED worker -> main. */
  readonly output: OwnedArrayBuffer
  readonly computeMs: number
}

export interface TaskError {
  readonly kind: 'error'
  readonly runId: RunId
  readonly nodeId: NodeId
  readonly error: ErrorDto
}

/**
 * Flush envelope used for ~8 ms local batching inside the worker. Only
 * `results` carry Transferables; errors are plain clones.
 */
export interface StaleNotice {
  readonly runId: RunId
  readonly nodeId: NodeId
}

export interface BatchOutput {
  readonly kind: 'batch'
  readonly results: readonly TaskResult[]
  readonly errors: readonly TaskError[]
  /**
   * Computations that finished after a cancel flipped the adoption flag.
   * No buffer is transferred for them; the notice exists only so the pool can
   * release the worker slot and the coordinator can retire the node quietly.
   */
  readonly stale: readonly StaleNotice[]
}

export type WorkerToMain = ReadyOutput | BatchOutput

/**
 * Extract the transfer list from an outbound message. Consuming the owned
 * buffers requires mapping the opaque type to `Transferable`; this is the
 * single sanctioned place where that conversion happens.
 */
export function takeTransfers(message: MainToWorker | BatchOutput): Transferable[] {
  if (message.kind === 'task') {
    return [message.input as unknown as Transferable]
  }
  if (message.kind === 'batch') {
    return message.results.map(
      (result) => result.output as unknown as Transferable,
    )
  }
  return []
}

/**
 * Worker-side: unwrap incoming inputs after structured clone. The receiver
 * obtains a plain ArrayBuffer (its new sole owner); the brand never crosses
 * the wire.
 */
export function receiveTask(message: TaskInput): {
  runId: RunId
  nodeId: NodeId
  fnId: FunctionId
  input: ArrayBuffer
} {
  const { runId, nodeId, fnId } = message
  return { runId, nodeId, fnId, input: message.input as unknown as ArrayBuffer }
}

/** Main-side: unwrap a batched result buffer after transfer. */
export function receiveResult(result: TaskResult): {
  runId: RunId
  nodeId: NodeId
  output: ArrayBuffer
  computeMs: number
} {
  const { runId, nodeId, computeMs } = result
  return {
    runId,
    nodeId,
    computeMs,
    output: result.output as unknown as ArrayBuffer,
  }
}
