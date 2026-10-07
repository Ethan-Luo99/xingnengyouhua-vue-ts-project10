<script lang="ts">
  import DataTable from './lib/DataTable.svelte'
  import {
    BOUND_FN_COUNT,
    BOUND_FAST_IDS,
    TOTAL_FUNCS,
    buildInitialRows,
  } from './lib/domain/functions'
  import { SchedulerCoordinator } from './lib/scheduler/coordinator'

  const initialRows = buildInitialRows()
  const coordinator = new SchedulerCoordinator()

  let table: DataTable
  let text = $state('')
  let composing = $state(false)
  let progress = $state({ runId: 0, done: 0, total: 0, finished: false })

  coordinator.setCommitSink((payload) => {
    table?.applyCommit(payload)
    progress = {
      runId: payload.runId,
      done: payload.doneCount,
      total: payload.totalCount,
      finished: payload.finished,
    }
  })

  /** Trigger A: one click runs the entire graph. */
  function runAll(): void {
    coordinator.notifyInteraction()
    coordinator.run()
  }

  /**
   * Deterministic per-character subset for trigger B: a string hash selects a
   * rotating slice of the 200 table-bound fast functions. Each keystroke runs
   * exactly one scheduling round — there is no debounce.
   */
  function subsetFor(value: string): number[] {
    let hash = 2166136261
    for (let i = 0; i < value.length; i += 1) {
      hash = Math.imul(hash ^ value.charCodeAt(i), 16777619)
    }
    const size = 40 + (Math.abs(hash) % 80)
    const start = Math.abs(Math.imul(hash, 48271)) % BOUND_FN_COUNT
    const nodes: number[] = []
    for (let offset = 0; offset < size; offset += 1) {
      const node = BOUND_FAST_IDS[(start + offset) % BOUND_FN_COUNT]
      if (node !== undefined) nodes.push(node)
    }
    return nodes
  }

  function triggerB(value: string): void {
    coordinator.notifyInteraction()
    coordinator.run({ selectedNodes: subsetFor(value) })
  }

  function handleInput(event: Event): void {
    const target = event.currentTarget as HTMLInputElement
    text = target.value
    // While an IME composition is active, intermediate glyphs are suppressed;
    // the single round fires at compositionend (see handleCompositionEnd).
    if (!composing) triggerB(text)
  }

  function handleCompositionStart(): void {
    composing = true
  }

  function handleCompositionEnd(event: CompositionEvent): void {
    composing = false
    text = (event.currentTarget as HTMLInputElement).value
    triggerB(text)
  }

  function handleScroll(scrollTop: number): void {
    coordinator.notifyScroll(scrollTop)
  }

  /**
   * Error-cell retry: goes through the same coordinator run path as B (a
   * single-node subset run under a fresh runId), so it can pause an A run
   * cooperatively but never mutates an in-flight run's node set.
   */
  function handleRetryNode(nodeId: number): void {
    coordinator.retryNode(nodeId)
  }
</script>

<main class="page">
  <header>
    <h1>DAG compute scheduler</h1>
    <p>
      {TOTAL_FUNCS} synthetic functions across 20 modules · 3 resident module
      workers · 5000-row table
    </p>
  </header>

  <section class="controls">
    <button type="button" onclick={runAll}>A — run all functions</button>
    <input
      type="text"
      value={text}
      placeholder="B — each character runs a subset"
      oninput={handleInput}
      oncompositionstart={handleCompositionStart}
      oncompositionend={handleCompositionEnd}
    />
    <span class="progress">
      {#if progress.runId > 0}
        run #{progress.runId}: {progress.done}/{progress.total}
        {progress.finished ? 'finished' : 'running'}
      {/if}
    </span>
  </section>

  <DataTable
    bind:this={table}
    rows={initialRows}
    onScroll={handleScroll}
    onRetryNode={handleRetryNode}
  />
</main>

<style>
  .page {
    max-width: 960px;
    margin: 0 auto;
    padding: 16px;
    text-align: left;
  }

  h1 {
    font-size: 22px;
    margin: 0 0 4px;
  }

  header p {
    font-size: 13px;
    color: #6b7280;
    margin: 0 0 12px;
  }

  .controls {
    display: flex;
    gap: 10px;
    align-items: center;
    margin-bottom: 12px;
  }

  .controls input {
    flex: 1;
    padding: 6px 10px;
    font: inherit;
    font-size: 14px;
  }

  .controls button {
    padding: 6px 12px;
    font: inherit;
    font-size: 14px;
    cursor: pointer;
  }

  .progress {
    font-size: 12px;
    font-family: ui-monospace, Consolas, monospace;
    min-width: 150px;
  }
</style>
