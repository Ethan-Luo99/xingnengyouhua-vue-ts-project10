<script lang="ts">
  /**
   * 5000 行实时表格。
   * - rows 由父组件以 $state.raw 持有：不可变快照，整数组替换引用；
   * - keyed each 用稳定业务 ID（row.id），禁止用数组索引；
   * - content-visibility: auto + contain-intrinsic-size 让屏外行跳过渲染，
   *   table-layout: fixed 让列宽不随内容重算；
   * - 滚动回调只记录 scrollTop，不做任何数据重算/过滤/排序。
   */
  export interface Row {
    readonly id: string
    readonly c0: string
    readonly c1: string
    readonly c2: string
    readonly c3: string
  }

  let { rows }: { rows: readonly Row[] } = $props()

  let scrollTop = $state.raw(0)

  function handleScroll(event: Event): void {
    // 只读滚动位置；数据重算、DAG、过滤排序一律禁止进入此回调。
    scrollTop = (event.currentTarget as HTMLElement).scrollTop
  }
</script>

<div class="viewport" onscroll={handleScroll}>
  <div class="scroll-indicator">{Math.round(scrollTop)}px</div>
  <table>
    <thead>
      <tr>
        <th>ID</th>
        <th>指标 A</th>
        <th>指标 B</th>
        <th>指标 C</th>
      </tr>
    </thead>
    <tbody>
      {#each rows as row (row.id)}
        <tr>
          <td>{row.id}</td>
          <td>{row.c1}</td>
          <td>{row.c2}</td>
          <td>{row.c3}</td>
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  .viewport {
    position: relative;
    height: 480px;
    overflow: auto;
    border: 1px solid #d0d0d0;
    border-radius: 6px;
    contain: strict;
  }
  .scroll-indicator {
    position: sticky;
    top: 0;
    float: right;
    padding: 2px 8px;
    font-size: 11px;
    color: #888;
    background: transparent;
    pointer-events: none;
  }
  table {
    width: 100%;
    table-layout: fixed;
    border-collapse: collapse;
    font-size: 13px;
  }
  thead th {
    position: sticky;
    top: 0;
    background: #f6f6f6;
    text-align: left;
    padding: 6px 10px;
    border-bottom: 1px solid #ddd;
  }
  tbody tr {
    content-visibility: auto;
    contain-intrinsic-size: auto 32px;
  }
  td {
    padding: 6px 10px;
    border-bottom: 1px solid #eee;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
