<script lang="ts">
  import DataTable, { type Row } from './lib/DataTable.svelte'
  import {
    buildSyntheticDomain,
    makeInput,
    subsetForChar,
    TABLE_ROWS,
    type TableBinding,
  } from './lib/domain/functions'
  import { Coordinator, type Commit } from './lib/scheduler/coordinator'
  import type { FnInput } from './lib/scheduler/protocol'

  const domain = buildSyntheticDomain()

  function buildRows(): Row[] {
    const rows: Row[] = []
    for (let i = 0; i < TABLE_ROWS; i++) {
      rows.push({ id: `row-${i}`, c0: '', c1: '—', c2: '—', c3: '—' })
    }
    return rows
  }

  // $state.raw：5000 行不可变快照，不做深代理；更新 = 替换引用。
  let rows = $state.raw<readonly Row[]>(buildRows())
  let lastRunId = $state(0)
  let lastCommitCount = $state(0)
  let inputText = $state('')

  const coordinator = new Coordinator({
    dag: domain.dag,
    registry: domain.registry,
    inputFor(nodeId: string, runId: number): FnInput {
      let hash = runId
      for (let i = 0; i < nodeId.length; i++) hash = (hash * 31 + nodeId.charCodeAt(i)) >>> 0
      return makeInput(hash)
    },
    onCommit(commit: Commit) {
      lastRunId = commit.runId
      lastCommitCount = commit.results.size
      if (commit.results.size === 0) return
      // 最小不可变更新：只替换受影响行的快照，其余行保持原引用。
      const next = rows.slice()
      for (const [nodeId, output] of commit.results) {
        const binding: TableBinding | undefined = domain.tableBinding.get(nodeId)
        if (!binding) continue
        for (let r = 0; r < 25; r++) {
          const index = binding.rowStart + r
          const row = next[index]
          const value = String((output.value + r) % 1000)
          next[index] =
            binding.column === 1
              ? { ...row, c1: value }
              : binding.column === 2
                ? { ...row, c2: value }
                : { ...row, c3: value }
        }
      }
      rows = next
    },
  })

  // 触发方式 A：点击一次，全量执行。
  function triggerA(): void {
    coordinator.startRun(domain.allNodeIds)
  }

  // 触发方式 B：每个字符触发一轮子集（不防抖、不跳过）。
  function triggerB(text: string): void {
    const code = text.length > 0 ? text.charCodeAt(text.length - 1) : 0
    coordinator.startRun(subsetForChar(code, domain.allNodeIds))
  }

  // IME 组合期：缓冲策略——组合期内不触发 run，compositionend 时补一轮。
  let composing = false

  function handleCompositionStart(): void {
    composing = true
  }

  function handleCompositionEnd(): void {
    composing = false
    triggerB(inputText)
  }

  function handleInput(event: Event): void {
    const native = event as InputEvent
    if (composing || native.isComposing) return // 组合期缓冲，不触发
    triggerB(inputText)
  }
</script>

<main>
  <h1>DAG 调度表演示</h1>
  <div class="controls">
    <button onclick={triggerA}>A：全量执行（573 函数）</button>
    <input
      bind:value={inputText}
      oninput={handleInput}
      oncompositionstart={handleCompositionStart}
      oncompositionend={handleCompositionEnd}
      placeholder="B：每字符触发一轮子集"
    />
    <span class="status">run #{lastRunId} · 本帧提交 {lastCommitCount} 项</span>
  </div>
  <DataTable {rows} />
</main>

<style>
  main {
    max-width: 880px;
    margin: 0 auto;
    padding: 24px 16px;
    font-family: system-ui, sans-serif;
  }
  .controls {
    display: flex;
    gap: 12px;
    align-items: center;
    margin-bottom: 16px;
  }
  input {
    flex: 1;
    padding: 6px 10px;
  }
  button {
    padding: 6px 14px;
  }
  .status {
    font-size: 12px;
    color: #777;
    white-space: nowrap;
  }
</style>
