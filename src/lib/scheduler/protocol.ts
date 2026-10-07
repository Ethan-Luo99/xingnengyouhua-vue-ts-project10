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
 *
 * Zero-copy batch channel (v1 binary layout, little-endian, fixed-width)
 * ---------------------------------------------------------------------
 * A whole batch of successful results is encoded by the worker into ONE
 * ArrayBuffer and transferred once (`BatchBinaryOutput.payload`) instead of
 * transferring one buffer per result. `errors` and `stale` remain narrow,
 * structured-cloned DTOs. Byte offsets:
 *
 *   header (16 bytes)
 *     0  u32  magic   = 0x44414731 (ASCII 'DAG1' in little-endian byte order)
 *     4  u32  protocolVersion (PROTOCOL_VERSION)
 *     8  u32  entryCount
 *     12 u32  runId           (one batch encodes a SINGLE run)
 *
 *   each entry (24 bytes, 8-byte aligned)
 *     0  u32  nodeId
 *     4  u8   status         (ENTRY_STATUS_DONE = 1 in v1; errors go via DTO)
 *     5  3 bytes reserved, MUST be zero
 *     8  f64  value          (the decoded scalar, little-endian IEEE-754)
 *     16 f64  computeMs      (little-endian IEEE-754)
 *
 * Total byte length MUST equal BINARY_HEADER_BYTES + entryCount *
 * BINARY_ENTRY_BYTES. Every multi-byte field is read/written with the
 * little-endian flag set; there is no endianness negotiation.
 */

/** Protocol version; bump when the wire shape changes. */
export const PROTOCOL_VERSION = 1 as const

/* ---- zero-copy batch wire constants (all numbers are little-endian) ---- */

/** Bytes 0..3 spell ASCII 'DAG1' when stored little-endian. */
export const BINARY_BATCH_MAGIC = 0x44414731 as const
/** Fixed header width in bytes. */
export const BINARY_HEADER_BYTES = 16 as const
/** Fixed per-entry width in bytes (8-byte aligned). */
export const BINARY_ENTRY_BYTES = 24 as const
/** Entry status byte: v1 only ever encodes successful, adopted results. */
export const ENTRY_STATUS_DONE = 1 as const

/** Thrown when a transferred batch buffer fails structural validation. */
export class BatchDecodeError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'BatchDecodeError'
  }
}

/** One decoded batch entry; only plain numbers survive the decode. */
export interface DecodedResultEntry {
  readonly nodeId: NodeId
  readonly status: typeof ENTRY_STATUS_DONE
  readonly value: number
  readonly computeMs: number
}

/** Worker-side input shape for {@link encodeResultBatch}. */
export interface EncodableResultEntry {
  readonly nodeId: NodeId
  readonly value: number
  readonly computeMs: number
}

/**
 * Encode one run's successful results into a single owned buffer. Pure and
 * isomorphic (usable from both worker and main thread); uses DataView with
 * explicit little-endian accessors and reserves padding bytes as zero.
 */
export function encodeResultBatch(
  runId: RunId,
  entries: readonly EncodableResultEntry[],
): OwnedArrayBuffer {
  const buffer = new ArrayBuffer(
    BINARY_HEADER_BYTES + entries.length * BINARY_ENTRY_BYTES,
  )
  const view = new DataView(buffer)
  view.setUint32(0, BINARY_BATCH_MAGIC, true)
  view.setUint32(4, PROTOCOL_VERSION, true)
  view.setUint32(8, entries.length, true)
  view.setUint32(12, runId, true)
  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index]
    if (!entry) continue
    const offset = BINARY_HEADER_BYTES + index * BINARY_ENTRY_BYTES
    view.setUint32(offset, entry.nodeId, true)
    view.setUint8(offset + 4, ENTRY_STATUS_DONE)
    view.setUint8(offset + 5, 0)
    view.setUint8(offset + 6, 0)
    view.setUint8(offset + 7, 0)
    view.setFloat64(offset + 8, entry.value, true)
    view.setFloat64(offset + 16, entry.computeMs, true)
  }
  return ownBuffer(buffer)
}

/**
 * Decode a transferred batch buffer into plain numbers and release it: no
 * view on the buffer is retained, so it becomes garbage immediately after the
 * call returns. Every structural assumption is validated up front against the
 * byte length, which makes all later DataView reads provably in-bounds; a
 * malformed buffer (bad magic/version/status/reserved or an entryCount that
 * disagrees with byteLength) raises {@link BatchDecodeError} and the caller is
 * expected to discard the buffer and treat the sender as poisoned.
 */
export function decodeResultBatch(buffer: ArrayBuffer): {
  readonly protocolVersion: number
  readonly runId: RunId
  readonly entries: readonly DecodedResultEntry[]
} {
  const byteLength = buffer.byteLength
  if (byteLength < BINARY_HEADER_BYTES) {
    throw new BatchDecodeError(
      `batch shorter than header: ${byteLength} < ${BINARY_HEADER_BYTES}`,
    )
  }
  const view = new DataView(buffer)
  const magic = view.getUint32(0, true)
  if (magic !== BINARY_BATCH_MAGIC) {
    throw new BatchDecodeError(`bad batch magic: 0x${magic.toString(16)}`)
  }
  const protocolVersion = view.getUint32(4, true)
  if (protocolVersion !== PROTOCOL_VERSION) {
    throw new BatchDecodeError(`unsupported batch protocol version: ${protocolVersion}`)
  }
  const entryCount = view.getUint32(8, true)
  const expectedLength =
    BINARY_HEADER_BYTES + entryCount * BINARY_ENTRY_BYTES
  // Exact equality: an entryCount inconsistent with the buffer length means a
  // truncated or corrupted transfer; reject before indexing any entry.
  if (byteLength !== expectedLength) {
    throw new BatchDecodeError(
      `batch length ${byteLength} disagrees with entryCount ${entryCount} (expected ${expectedLength})`,
    )
  }
  const runId = view.getUint32(12, true)
  const entries: DecodedResultEntry[] = []
  for (let index = 0; index < entryCount; index += 1) {
    const offset = BINARY_HEADER_BYTES + index * BINARY_ENTRY_BYTES
    const status = view.getUint8(offset + 4)
    if (status !== ENTRY_STATUS_DONE) {
      throw new BatchDecodeError(`unknown entry status byte ${status} at index ${index}`)
    }
    if (
      view.getUint8(offset + 5) !== 0 ||
      view.getUint8(offset + 6) !== 0 ||
      view.getUint8(offset + 7) !== 0
    ) {
      throw new BatchDecodeError(`non-zero reserved bytes at index ${index}`)
    }
    entries.push({
      nodeId: view.getUint32(offset, true),
      status: ENTRY_STATUS_DONE,
      value: view.getFloat64(offset + 8, true),
      computeMs: view.getFloat64(offset + 16, true),
    })
  }
  return { protocolVersion, runId, entries }
}

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

/**
 * Zero-copy batch envelope: all successful results of ONE run are encoded in
 * a single transferred ArrayBuffer (see file header for the binary layout).
 * `errors` and `stale` stay narrow plain DTOs.
 */
export interface BatchBinaryOutput {
  readonly kind: 'batch-binary'
  /** TRANSFERRED worker -> main; exactly one transfer per batch. */
  readonly payload: OwnedArrayBuffer
  readonly errors: readonly TaskError[]
  readonly stale: readonly StaleNotice[]
}

export type WorkerToMain = ReadyOutput | BatchOutput | BatchBinaryOutput

/**
 * Extract the transfer list from an outbound message. Consuming the owned
 * buffers requires mapping the opaque type to `Transferable`; this is the
 * single sanctioned place where that conversion happens.
 */
export function takeTransfers(
  message: MainToWorker | BatchOutput | BatchBinaryOutput,
): Transferable[] {
  if (message.kind === 'task') {
    return [message.input as unknown as Transferable]
  }
  if (message.kind === 'batch') {
    return message.results.map(
      (result) => result.output as unknown as Transferable,
    )
  }
  if (message.kind === 'batch-binary') {
    return [message.payload as unknown as Transferable]
  }
  return []
}

/**
 * Main-side: unwrap a transferred binary batch after the transfer. The
 * returned `payload` is a plain ArrayBuffer of which the receiver is the sole
 * owner; decode it once with {@link decodeResultBatch} and drop the reference
 * so ownership is released immediately.
 */
export function receiveBatchBinary(message: BatchBinaryOutput): {
  payload: ArrayBuffer
  errors: readonly TaskError[]
  stale: readonly StaleNotice[]
} {
  return {
    payload: message.payload as unknown as ArrayBuffer,
    errors: message.errors,
    stale: message.stale,
  }
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
