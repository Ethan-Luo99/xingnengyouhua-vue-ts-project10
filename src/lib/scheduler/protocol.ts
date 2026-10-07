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
 *   - BinaryBatchOutput.payload: one ArrayBuffer holding a whole result
 *     batch in the fixed little-endian layout (BINARY_LAYOUT_VERSION)
 * They are tagged with the opaque `OwnedArrayBuffer` brand so ordinary code
 * cannot read or re-send an ArrayBuffer that has already been (or is about to
 * be) transferred. Buffers enter the ownership system once via `ownBuffer()`
 * and leave it exactly once via `takeTransfers()`; the receiving side gets a
 * plain, freshly-owned `ArrayBuffer` back from `receive*()`.
 */

/** Protocol version; bump when the wire shape changes. */
export const PROTOCOL_VERSION = 1 as const

/**
 * Version of the fixed binary batch layout (independent of the DTO version).
 * Bump when the header/entry offsets or field types change.
 */
export const BINARY_LAYOUT_VERSION = 1 as const

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
 * Zero-copy fast path: a whole run's result batch is encoded into ONE
 * ArrayBuffer in the fixed layout below and transferred as a single
 * Transferable. Errors and stale notices never take this channel; when a
 * flush group has any of them they travel alongside in a plain DTO envelope
 * ({@link BinaryBatchOutput.errors} / `.stale`), and run groups that contain
 * only errors/stale still use the plain {@link BatchOutput} shape.
 *
 * Binary layout (all multi-byte numbers are EXPLICITLY little-endian; the
 * DataView getters/setters in this file pass `/* littleEndian * / true`, so
 * the wire bytes are host-endianness independent):
 *
 *   Header, BINARY_HEADER_BYTES = 24 bytes (8-byte aligned start/end):
 *     off  0  u32   magic            BINARY_MAGIC (0x52_42_4e_55)
 *     off  4  u32   layoutVersion    BINARY_LAYOUT_VERSION
 *     off  8  u32   entryCount       number of entries that follow
 *     off 12  u32   reserved         always 0
 *     off 16  f64   runId            producing run id
 *
 *   Each entry, BINARY_ENTRY_BYTES = 32 bytes, fixed length (no per-entry
 *   length prefix; entries never carry variable-width payloads):
 *     off  0  i32   nodeId
 *     off  4  u8    status          BINARY_STATUS_DONE (1)
 *     off  5  3 bytes padding       always 0
 *     off  8  f64   value           Float64 scalar result
 *     off 16  f64   computeMs
 *     off 24  8 bytes padding       always 0
 *
 * Total byte length is therefore exactly
 *   BINARY_HEADER_BYTES + entryCount * BINARY_ENTRY_BYTES
 * and the decoder rejects any buffer whose byteLength disagrees, which makes
 * out-of-bounds reads impossible (every DataView access is bounds-checked
 * against that exact expectation before the first read).
 */
export interface BinaryBatchOutput {
  readonly kind: 'binaryBatch'
  readonly runId: RunId
  /** TRANSFERRED worker -> main; sole owner after transfer. */
  readonly payload: OwnedArrayBuffer
  /** Narrow DTO channel; never encoded into the payload. */
  readonly errors: readonly TaskError[]
  /** Narrow DTO channel; never encoded into the payload. */
  readonly stale: readonly StaleNotice[]
}

export type WorkerToMain = ReadyOutput | BatchOutput | BinaryBatchOutput

/* ---------- fixed binary layout constants ---------- */

export const BINARY_MAGIC = 0x5242_4e55 as const
export const BINARY_HEADER_BYTES = 24 as const
export const BINARY_ENTRY_BYTES = 32 as const

export const BINARY_OFFSET_MAGIC = 0 as const
export const BINARY_OFFSET_VERSION = 4 as const
export const BINARY_OFFSET_COUNT = 8 as const
export const BINARY_OFFSET_RESERVED = 12 as const
export const BINARY_OFFSET_RUNID = 16 as const

export const BINARY_ENTRY_OFFSET_NODEID = 0 as const
export const BINARY_ENTRY_OFFSET_STATUS = 4 as const
export const BINARY_ENTRY_OFFSET_VALUE = 8 as const
export const BINARY_ENTRY_OFFSET_COMPUTEMS = 16 as const

/** Status codes stored in each entry's status byte. */
export const BINARY_STATUS_DONE = 1 as const

/** Hard sanity cap so a corrupt entryCount cannot drive a huge allocation. */
export const BINARY_MAX_ENTRIES = 1_000_000 as const

/** One encoded result entry before it is copied into the batch buffer. */
export interface BinaryResultEntry {
  readonly nodeId: NodeId
  readonly value: number
  readonly computeMs: number
}

/** Thrown when a received binary payload fails the structural validation. */
export class BinaryProtocolError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'BinaryProtocolError'
  }
}

/**
 * Encode one run group into a single transferable ArrayBuffer. The caller
 * owns the returned plain buffer and must move it through `ownBuffer()`
 * before posting. Layout is the mirror image of {@link decodeResultBatch}.
 */
export function encodeResultBatch(
  runId: RunId,
  entries: readonly BinaryResultEntry[],
): ArrayBuffer {
  const count = entries.length
  const total = BINARY_HEADER_BYTES + count * BINARY_ENTRY_BYTES
  const buffer = new ArrayBuffer(total)
  const view = new DataView(buffer)
  view.setUint32(BINARY_OFFSET_MAGIC, BINARY_MAGIC, true)
  view.setUint32(BINARY_OFFSET_VERSION, BINARY_LAYOUT_VERSION, true)
  view.setUint32(BINARY_OFFSET_COUNT, count, true)
  view.setUint32(BINARY_OFFSET_RESERVED, 0, true)
  view.setFloat64(BINARY_OFFSET_RUNID, runId, true)
  for (let index = 0; index < count; index += 1) {
    const entry = entries[index]
    if (!entry) throw new BinaryProtocolError(`missing entry at ${index}`)
    const base = BINARY_HEADER_BYTES + index * BINARY_ENTRY_BYTES
    view.setInt32(base + BINARY_ENTRY_OFFSET_NODEID, entry.nodeId, true)
    view.setUint8(base + BINARY_ENTRY_OFFSET_STATUS, BINARY_STATUS_DONE)
    view.setFloat64(base + BINARY_ENTRY_OFFSET_VALUE, entry.value, true)
    view.setFloat64(base + BINARY_ENTRY_OFFSET_COMPUTEMS, entry.computeMs, true)
  }
  return buffer
}

export interface DecodedResultBatch {
  readonly runId: RunId
  readonly results: {
    readonly nodeId: NodeId
    readonly status: typeof BINARY_STATUS_DONE
    readonly value: number
    readonly computeMs: number
  }[]
}

/**
 * Decode a received payload. Every structural expectation is validated BEFORE
 * any per-entry read:
 *   - byteLength must equal HEADER + count*ENTRY exactly (a count claiming
 *     more or fewer entries than the buffer holds is rejected wholesale — no
 *     partial decode, no out-of-bounds read)
 *   - magic, layout version, reserved bytes and per-entry status must match
 * All decoded numbers are copied out into fresh objects; the buffer is not
 * retained, i.e. the main thread releases its ownership immediately.
 */
export function decodeResultBatch(buffer: ArrayBuffer): DecodedResultBatch {
  const length = buffer.byteLength
  if (length < BINARY_HEADER_BYTES) {
    throw new BinaryProtocolError(
      `binary batch shorter than header: ${length} < ${BINARY_HEADER_BYTES}`,
    )
  }
  const view = new DataView(buffer)
  const magic = view.getUint32(BINARY_OFFSET_MAGIC, true)
  if (magic !== BINARY_MAGIC) {
    throw new BinaryProtocolError(`bad magic 0x${magic.toString(16)}`)
  }
  const version = view.getUint32(BINARY_OFFSET_VERSION, true)
  if (version !== BINARY_LAYOUT_VERSION) {
    throw new BinaryProtocolError(
      `unsupported binary layout version ${version} (want ${BINARY_LAYOUT_VERSION})`,
    )
  }
  const entryCount = view.getUint32(BINARY_OFFSET_COUNT, true)
  if (entryCount > BINARY_MAX_ENTRIES) {
    throw new BinaryProtocolError(`entryCount ${entryCount} exceeds cap`)
  }
  if (view.getUint32(BINARY_OFFSET_RESERVED, true) !== 0) {
    throw new BinaryProtocolError('nonzero reserved header word')
  }
  // The exact-length agreement is what rules out every OOB read below.
  const expectedLength =
    BINARY_HEADER_BYTES + entryCount * BINARY_ENTRY_BYTES
  if (length !== expectedLength) {
    throw new BinaryProtocolError(
      `entryCount ${entryCount} disagrees with byteLength ${length} ` +
        `(expected ${expectedLength})`,
    )
  }
  const runId = view.getFloat64(BINARY_OFFSET_RUNID, true)
  if (!Number.isFinite(runId) || runId <= 0 || Math.trunc(runId) !== runId) {
    throw new BinaryProtocolError(`invalid runId in header: ${runId}`)
  }
  const results: {
    nodeId: NodeId
    status: typeof BINARY_STATUS_DONE
    value: number
    computeMs: number
  }[] = []
  for (let index = 0; index < entryCount; index += 1) {
    const base = BINARY_HEADER_BYTES + index * BINARY_ENTRY_BYTES
    const nodeId = view.getInt32(base + BINARY_ENTRY_OFFSET_NODEID, true)
    const status = view.getUint8(base + BINARY_ENTRY_OFFSET_STATUS)
    if (status !== BINARY_STATUS_DONE) {
      throw new BinaryProtocolError(
        `entry ${index} has unknown status byte ${status}`,
      )
    }
    const value = view.getFloat64(base + BINARY_ENTRY_OFFSET_VALUE, true)
    const computeMs = view.getFloat64(base + BINARY_ENTRY_OFFSET_COMPUTEMS, true)
    results.push({ nodeId, status: BINARY_STATUS_DONE, value, computeMs })
  }
  return { runId: runId as RunId, results }
}

/**
 * Extract the transfer list from an outbound message. Consuming the owned
 * buffers requires mapping the opaque type to `Transferable`; this is the
 * single sanctioned place where that conversion happens.
 */
export function takeTransfers(
  message: MainToWorker | BatchOutput | BinaryBatchOutput,
): Transferable[] {
  if (message.kind === 'task') {
    return [message.input as unknown as Transferable]
  }
  if (message.kind === 'binaryBatch') {
    return [message.payload as unknown as Transferable]
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
