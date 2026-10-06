<script lang="ts">
  import {
    BOUND_ROW_BY_FN,
    FUNCTIONS,
    type TableRow,
  } from './domain/functions'
  import type { CommitPayload } from './scheduler/coordinator'

  interface Props {
    rows: readonly TableRow[]
    onScroll?: (scrollTop: number) => void
  }

  import { untrack } from 'svelte'

  let { rows, onScroll }: Props = $props()

  /**
   * $state.raw holds the 5000 rows as immutable snapshots: Svelte never
   * deep-proxies them; updates arrive as replacement row references.
   */
  let tableRows: readonly TableRow[] = $state.raw(untrack(() => rows))

  const fnIndexToRow = new Map<number, number>(BOUND_ROW_BY_FN)

  /**
   * Apply one frame commit. Only row objects whose node produced an outcome
   * are replaced; every other row reference stays identical, so keyed-each
   * reconciliation leaves its DOM untouched.
   */
  export function applyCommit(payload: CommitPayload): void {
    const next = tableRows.slice()
    let touched = 0
    for (const outcome of payload.outcomes) {
      const rowIndex = fnIndexToRow.get(outcome.nodeId)
      if (rowIndex === undefined) continue
      const previous = next[rowIndex]
      if (!previous) continue
      next[rowIndex] = {
        ...previous,
        value: outcome.value,
        status: outcome.status === 'done' ? 'done' : 'error',
      }
      touched += 1
    }
    if (touched > 0) tableRows = next
  }

  $effect(() => {
    if (rows !== tableRows) tableRows = rows
  })

  let scroller: HTMLDivElement
  function handleScroll(event: Event): void {
    // Position reporting only. No DAG run, filter, sort, or row recompute
    // happens on the scroll path, and no Svelte state is written.
    const target = event.currentTarget as HTMLDivElement
    onScroll?.(target.scrollTop)
  }
</script>

<div class="table-scroller" bind:this={scroller} onscroll={handleScroll}>
  <table class="data-table">
    <colgroup>
      <col style="width: 14%" />
      <col style="width: 26%" />
      <col style="width: 12%" />
      <col style="width: 18%" />
      <col style="width: 14%" />
      <col style="width: 16%" />
    </colgroup>
    <thead>
      <tr>
        <th>Row ID</th>
        <th>Bound function</th>
        <th>Module</th>
        <th>Status</th>
        <th>Value</th>
        <th>Tier</th>
      </tr>
    </thead>
    <tbody>
      {#each tableRows as row (row.rowId)}
        <tr
          class="data-row"
          class:done={row.status === 'done'}
          class:error={row.status === 'error'}
        >
          <td class="mono">{row.rowId}</td>
          <td class="mono">{row.boundFnId ?? '—'}</td>
          <td>m{row.moduleId}</td>
          <td>{row.status}</td>
          <td class="mono value">
            {row.value === null ? '—' : row.value.toFixed(3)}
          </td>
          <td>
            {row.boundFnId === null
              ? 'unbound'
              : FUNCTIONS[Number(row.boundFnId.slice(3))]?.tier}
          </td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .table-scroller {
    height: 420px;
    overflow-y: auto;
    contain: strict;
    border: 1px solid var(--border, #e5e4e7);
    border-radius: 6px;
  }

  .data-table {
    width: 100%;
    border-collapse: collapse;
    table-layout: fixed;
    font-size: 12px;
    line-height: 1.4;
  }

  thead th {
    position: sticky;
    top: 0;
    z-index: 1;
    background: #f4f3ec;
    text-align: left;
    padding: 6px 8px;
    font-weight: 600;
    border-bottom: 1px solid var(--border, #e5e4e7);
  }

  .data-row {
    /*
     * Skip layout/paint for the thousands of off-screen rows while keeping
     * them in the DOM. The intrinsic size reserves one row height so the
     * scrollbar stays stable without measuring hidden row contents.
     */
    content-visibility: auto;
    contain-intrinsic-size: auto 22px;
  }

  .data-row td {
    padding: 3px 8px;
    border-bottom: 1px solid rgba(0, 0, 0, 0.06);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .data-row.done .value {
    color: #16794c;
  }

  .data-row.error {
    color: #b42318;
  }

  .mono {
    font-family: ui-monospace, Consolas, monospace;
  }
</style>
