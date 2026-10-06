/**
 * Synthetic domain registry.
 *
 * This module is isomorphic: it is imported by both the main-thread scheduler
 * and `src/workers/compute.worker.ts`. It therefore contains no DOM, Svelte or
 * other non-structured-cloneable state — plain data plus pure compute only.
 *
 * Dataset (per docs/performance-architecture-plan.md §一):
 *  - 510 fast functions   0–1 ms
 *  -  60 medium functions 5–50 ms
 *  -   3 long-tail functions 50–200 ms containing a synchronous CPU loop
 *  - a single-direction DAG over 20 modules; ~30% of the edges are "hard"
 *    (ordering constraints that never allow parallel reordering past them)
 *  - 200 fast functions are bound one-to-one to cells of a 5000-row table
 */

export type FunctionTier = 'fast' | 'medium' | 'long'

/** A dependency of a function; `hard: true` marks a ~30% hard edge. */
export interface DepSpec {
  readonly from: number
  readonly hard: boolean
}

export interface FunctionSpec {
  readonly id: number
  readonly fnId: string
  readonly moduleId: number
  readonly tier: FunctionTier
  /** Synthetic p50/p95 duration estimate in milliseconds. */
  readonly costMs: number
  /** Upstream node ids; edges always point from a lower id to a higher id. */
  readonly deps: readonly number[]
  /** Hard-edge flags, aligned by index with `deps`. */
  readonly hardEdges: readonly boolean[]
  /** Seed for deterministic CPU work. */
  readonly seed: number
}

export interface TableRow {
  readonly rowId: string
  readonly moduleId: number
  readonly seed: number
  /** Business id of the function whose output is shown in column `value`. */
  readonly boundFnId: string | null
  /** Static value column; updated immutably when the bound function commits. */
  readonly value: number | null
  readonly status: 'pending' | 'done' | 'error'
}

export const MODULE_COUNT = 20
export const FAST_COUNT = 510
export const MEDIUM_COUNT = 60
export const LONG_COUNT = 3
export const TOTAL_FUNCS = FAST_COUNT + MEDIUM_COUNT + LONG_COUNT
export const TABLE_ROW_COUNT = 5000
export const BOUND_FN_COUNT = 200

const FAST_LIMIT_MS = 1
const MEDIUM_MIN_MS = 5
const MEDIUM_MAX_MS = 50
const LONG_MIN_MS = 50
const LONG_MAX_MS = 200

/** Deterministic 32-bit LCG so the dataset is stable across reloads. */
function lcg(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0
    return state / 0x100000000
  }
}

function tierOfIndex(index: number): FunctionTier {
  if (index < FAST_COUNT) return 'fast'
  if (index < FAST_COUNT + MEDIUM_COUNT) return 'medium'
  return 'long'
}

function buildFunctions(): readonly FunctionSpec[] {
  const rand = lcg(0x5eed1234)
  const specs: FunctionSpec[] = []
  for (let index = 0; index < TOTAL_FUNCS; index += 1) {
    const tier = tierOfIndex(index)
    const moduleId = Math.min(
      MODULE_COUNT - 1,
      Math.floor((index / TOTAL_FUNCS) * MODULE_COUNT),
    )
    let costMs: number
    if (tier === 'fast') {
      costMs = 0.2 + rand() * (FAST_LIMIT_MS - 0.2)
    } else if (tier === 'medium') {
      costMs = MEDIUM_MIN_MS + rand() * (MEDIUM_MAX_MS - MEDIUM_MIN_MS)
    } else {
      costMs = LONG_MIN_MS + rand() * (LONG_MAX_MS - LONG_MIN_MS)
    }

    const deps: number[] = []
    const hardEdges: boolean[] = []
    const addDep = (from: number) => {
      // ~30% of dependency edges are hard ordering constraints.
      const hard = rand() < 0.3
      if (!deps.includes(from)) {
        deps.push(from)
        hardEdges.push(hard)
      }
    }

    if (index > 0) {
      // Prefer an edge inside the same module to build a dependency spine.
      if (rand() < 0.8) {
        const moduleStart = Math.floor((moduleId / MODULE_COUNT) * TOTAL_FUNCS)
        const lower = Math.max(0, moduleStart)
        if (index > lower) {
          addDep(lower + Math.floor(rand() * (index - lower)))
        }
      }
      // Medium and long-tail nodes additionally depend on earlier fast/medium
      // nodes; long-tail nodes stay leaves-ish so they never fan out.
      const crossChance =
        tier === 'fast' ? 0.12 : tier === 'medium' ? 0.35 : 0.5
      if (rand() < crossChance) {
        const upperExclusive =
          tier === 'long' ? FAST_COUNT + MEDIUM_COUNT : index
        if (upperExclusive > 0) {
          addDep(Math.floor(rand() * upperExclusive))
        }
      }
    }

    specs.push({
      id: index,
      fnId: `fn-${index}`,
      moduleId,
      tier,
      costMs,
      deps,
      hardEdges,
      seed: (Math.imul(index + 1, 2654435761) ^ 0x9e3779b9) >>> 0,
    })
  }
  return specs
}

export const FUNCTIONS: readonly FunctionSpec[] = buildFunctions()

/**
 * Choose 200 distinct fast functions for the table binding. A coprime stride
 * over the fast range yields a deterministic permutation without allocating a
 * shuffled array.
 */
function buildBoundFastIds(): readonly number[] {
  const stride = 211 // prime, coprime-ish with 510
  const chosen = new Set<number>()
  let cursor = 0
  while (chosen.size < BOUND_FN_COUNT) {
    chosen.add(cursor % FAST_COUNT)
    cursor += stride
  }
  return [...chosen]
}

export const BOUND_FAST_IDS: readonly number[] = buildBoundFastIds()

/** Stable business row -> bound function map (4999 is prime => permutation). */
function buildBoundByRow(): ReadonlyMap<number, number> {
  const map = new Map<number, number>()
  for (let k = 0; k < BOUND_FAST_IDS.length; k += 1) {
    const rowIndex = (k * 4999) % TABLE_ROW_COUNT
    map.set(rowIndex, BOUND_FAST_IDS[k])
  }
  return map
}

export const BOUND_BY_ROW: ReadonlyMap<number, number> = buildBoundByRow()
export const BOUND_ROW_BY_FN: ReadonlyMap<number, number> = (() => {
  const map = new Map<number, number>()
  for (const [rowIndex, fn] of BOUND_BY_ROW) map.set(fn, rowIndex)
  return map
})()

export function buildInitialRows(): readonly TableRow[] {
  const rows: TableRow[] = new Array(TABLE_ROW_COUNT)
  for (let rowIndex = 0; rowIndex < TABLE_ROW_COUNT; rowIndex += 1) {
    const boundFn = BOUND_BY_ROW.get(rowIndex)
    rows[rowIndex] = {
      rowId: `row-${rowIndex}`,
      moduleId: FUNCTIONS[(rowIndex * 13) % TOTAL_FUNCS].moduleId,
      seed: rowIndex,
      boundFnId: boundFn === undefined ? null : FUNCTIONS[boundFn].fnId,
      value: null,
      status: 'pending',
    }
  }
  return rows
}

/* ------------------------------------------------------------------ */
/* Pure compute (also imported by the worker)                          */
/* ------------------------------------------------------------------ */

export const CHUNK_SLICE_MS = 4

/**
 * Fuse two u32 words into the running FNV-1a-like checksum. Integer math only
 * so worker and main-thread results are bit-identical.
 */
function mixWord(checksum: number, word: number): number {
  let h = Math.imul(checksum ^ (word & 0xff), 0x01000193)
  h = Math.imul(h ^ ((word >>> 8) & 0xff), 0x01000193)
  h = Math.imul(h ^ ((word >>> 16) & 0xff), 0x01000193)
  return Math.imul(h ^ ((word >>> 24) & 0xff), 0x01000193)
}

/** Calibrated burn iterations per millisecond for the synthetic device. */
const ITERS_PER_MS = 14000

function iterationsFor(costMs: number): number {
  return Math.max(1, Math.round(costMs * ITERS_PER_MS))
}

/**
 * Synchronous CPU loop. This is intentionally non-preemptible, mirroring the
 * design doc's "already-started 200 ms black box cannot be stopped from the
 * outside" constraint. Never call it on the main thread for long-tail nodes.
 */
function burnSync(seed: number, costMs: number, input: Uint32Array): number {
  let acc = seed >>> 0
  const iterations = iterationsFor(costMs)
  for (let i = 0; i < iterations; i += 1) {
    acc = Math.imul(acc ^ input[i % input.length], 2654435761)
    acc = (acc + Math.imul(i, 2246822519)) >>> 0
  }
  return acc >>> 0
}

/** Thrown by `tick` to abandon a cooperatively chunked fallback run. */
export class ChunkAbortedError extends Error {
  constructor() {
    super('chunked-compute-aborted')
    this.name = 'ChunkAbortedError'
  }
}

export type ChunkTick = () => void

/**
 * Cooperative-chunked variant of the same deterministic compute. Used only on
 * the main-thread fallback path when Workers cannot be constructed. Every
 * `CHUNK_SLICE_MS` it awaits a macrotask and calls `tick`; an aborting tick
 * throws {@link ChunkAbortedError} so the coordinator can retire the run.
 */
export async function burnChunked(
  seed: number,
  costMs: number,
  input: Uint32Array,
  tick: ChunkTick,
): Promise<number> {
  let acc = seed >>> 0
  const iterations = iterationsFor(costMs)
  let sliceStart = performance.now()
  for (let i = 0; i < iterations; i += 1) {
    acc = Math.imul(acc ^ input[i % input.length], 2654435761)
    acc = (acc + Math.imul(i, 2246822519)) >>> 0
    if (i % 2048 === 0) {
      if (performance.now() - sliceStart >= CHUNK_SLICE_MS) {
        tick()
        await new Promise<void>((resolve) => {
          // Prefer background-priority yielding when available.
          const schedulerWithPost = globalThis.scheduler as
            | { postTask?(cb: () => void): void }
            | undefined
          if (schedulerWithPost?.postTask) {
            schedulerWithPost.postTask(resolve)
          } else {
            setTimeout(resolve, 0)
          }
        })
        tick()
        sliceStart = performance.now()
      }
    }
  }
  return acc >>> 0
}

function inputWords(input: ArrayBuffer): Uint32Array {
  return new Uint32Array(input)
}

/** Deterministic output value (float64) derived from the checksum word. */
function outputValue(checksum: number, seed: number): number {
  const bits = (checksum >>> 0) ^ (seed >>> 0)
  // Normalize to a 4-decimal value in [0, 1000).
  return ((bits % 1_000_000) / 1000) % 1000
}

/**
 * Run a registered function synchronously (worker path + fast/medium fallback
 * tasks whose admission check passed). Returns a fresh 8-byte Float64Array
 * ArrayBuffer so the caller can transfer it.
 */
export function runFunctionSync(
  spec: FunctionSpec,
  input: ArrayBuffer,
): ArrayBuffer {
  const words = inputWords(input)
  const checksum = burnSync(spec.seed, spec.costMs, words)
  const output = new ArrayBuffer(8)
  new Float64Array(output)[0] = outputValue(checksum, spec.seed)
  return output
}

/**
 * Run a registered function in cooperative chunks (main-thread fallback only).
 */
export async function runFunctionChunked(
  spec: FunctionSpec,
  input: ArrayBuffer,
  tick: ChunkTick,
): Promise<ArrayBuffer> {
  const words = inputWords(input)
  const checksum = await burnChunked(spec.seed, spec.costMs, words, tick)
  const output = new ArrayBuffer(8)
  new Float64Array(output)[0] = outputValue(checksum, spec.seed)
  return output
}

/** Construct the per-task input payload (64 bytes) for a node. */
export function makeInput(spec: FunctionSpec): ArrayBuffer {
  const buf = new ArrayBuffer(64)
  const words = new Uint32Array(buf)
  for (let i = 0; i < words.length; i += 1) {
    words[i] = Math.imul(spec.seed ^ i, 40503) >>> 0
  }
  return buf
}

/** Read the scalar value out of an 8-byte Float64 result buffer. */
export function decodeValue(output: ArrayBuffer): number {
  return new Float64Array(output)[0] ?? 0
}
