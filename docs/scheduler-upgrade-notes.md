# 调度器协议升级与调度语义增强 —— 交付说明

本轮在既有「DAG 调度器 + Worker 池 + DataTable」之上做**增量**升级，未推倒重写任何
模块。五项改动全部落在既有文件内：协议层（`protocol.ts` + `compute.worker.ts`）、
图层（`graph.ts`）、调度层（`coordinator.ts`）、池层（`pool.ts`）、表格层
（`DataTable.svelte` + `App.svelte`）。未新增任何 dependency / devDependency，仅使用
浏览器原生 API（`ArrayBuffer` / `DataView` / `Transferable` / `postMessage`）与
Svelte 5 自带能力（`$state.raw` / runes）。

---

## 一、各模块职责一览

| 文件 | 本轮前职责 | 本轮新增 / 强化 |
| --- | --- | --- |
| `src/lib/scheduler/protocol.ts` | 主线程↔Worker 的判别联合 DTO；`OwnedArrayBuffer` 所有权品牌与 `takeTransfers`；窄 `ErrorDto`/`stale` | 新增零拷贝批量通道：16 字节 header + 每条目 24 字节的**定长小端二进制布局**（`BINARY_*` 常量、`BatchBinaryOutput`）；对称的 `encodeResultBatch` / `decodeResultBatch`，解码含 magic/version/长度/status/reserved 五重校验，越界即抛 `BatchDecodeError`；`receiveBatchBinary` 一次性接收并释放所有权。判别联合**只扩充**：`WorkerToMain = ReadyOutput \| BatchOutput \| BatchBinaryOutput`，旧 `BatchOutput` 字段未删改。 |
| `src/workers/compute.worker.ts` | 同步执行纯函数；8ms 本地攒批；`cancel` 仅翻转 adoption 标记，不打断在途同步任务 | 成功结果在 worker 侧即时解码成数值，flush 时**按 runId 分组**，每组编码进**单个** `ArrayBuffer` 并经一次 Transferable 转移（`batch-binary`）；`errors` / `stale` 仍走窄 DTO（无成功结果的组回退到旧 `batch` 信封，向后兼容）。 |
| `src/lib/scheduler/graph.ts` | `compileGraph`（Kahn 环检测、拓扑分层 level、关键距离 criticalDistance）；`ReadyTracker`；上游闭包 | 新增**事务性增量修订** `applyPatch(addNodes, removeNodes)`：先做全部校验与环检测再动图（失败抛带环路径 `cyclePath` 的 `DagCycleError`，原图字节级不变）；新增节点追加到高水位 id、删除节点保留 tombstone（id 稳定）；level/关键距离只对「新增节点 + 删除节点的传递下游闭包」重算，配 `PatchReport` 给出实际重算集合作为未全量重编译的证据；`CompiledGraph` 仅追加 `costs` / `alive` 字段。 |
| `src/lib/scheduler/coordinator.ts` | runId 门控、关键路径优先堆、主线程 fast 切片、非响应式双缓冲 + 每帧至多一次 rAF 提交 | 新增**协作式优先级抢占**：B 到达时把在跑的 A 全量轮标记 `paused`（不取消、保留原 runId/堆/在途 worker 任务），A 的 fast slice 在下一个让出点停；B 完成后 A 以**同一 runId** 从暂停点恢复，暂停期间结果在 back buffer **parked**，恢复后随下一帧提交。`RunRecord` 显式建模 `status: running\|paused\|finished`。新增 `repairNode` 单节点 sidecar run（独立新 runId，不抢占、不走新提交路径）。 |
| `src/lib/scheduler/pool.ts` | 常驻 module worker 池 + 主线程 chunked 兜底；取消只翻标记 | 新增**崩溃自愈**：`onerror`/`onmessageerror`（及无法解码的二进制批）终止该 worker、把其在途任务重新入队、同槽位 id 重建新 worker；每槽 `inflight` 表 + 全局 `delivered`（`runId:nodeId`）做**幂等**，竞态重复结果在**池边界**拦截，做到不丢节点、不重复提交；连续崩溃达 `CRASH_DEGRADE_THRESHOLD=3` 才整体降级（一个健康批即清零）。新增对 `batch-binary` 的解码处理。 |
| `src/lib/DataTable.svelte` | `$state.raw` 持有 5000 行；`applyCommit` 每帧只替换有结果的行；滚动只报位置 | error 状态行的「状态单元格 / 值单元格」增加**重试入口**（点击或键盘 Enter/空格），通过新增的 `onRetryNode(nodeId)` 回调把单个节点交回既有调度器；done/pending 单元格保持惰性，不引入任何新提交路径。 |
| `src/App.svelte` | 触发器 A（点击全量）、B（每字符一轮子集，含 IME 抑制）、提交 sink、滚动转发 | 新增 `retryNode(nodeId)`：调用 `coordinator.run({ repairNode })`，走既有调度器；把 `onRetryNode` 传给 `DataTable`。A/B 交互语义与每字符一轮完全不变。 |

> 说明：本轮所有协议类型均为判别联合的**扩充**（新增 `batch-binary` 变体、`BatchBinaryOutput`、`BatchDecodeError`、`repairNode`、`costs`/`alive` 等），既有 `task`/`cancel`/`shutdown`/`result`/`error`/`batch`/`ready` 字段无删除、无改名、无类型收窄。

---

## 二、门禁结果原文（原样粘贴）

以下两段为在交付目录 `/home/ethan_luo/projects/project10/run3a` 下，依次执行
`npm run check`（内部即 `svelte-check --tsconfig ./tsconfig.app.json && tsc -p
tsconfig.node.json`）与 `npm run build` 的终端原文，未做转述或删改。

### `npm run check`

```
> svelte-ts-blank@0.0.0 check
> svelte-check --tsconfig ./tsconfig.app.json && tsc -p tsconfig.node.json

Loading svelte-check in workspace: /home/ethan_luo/projects/project10/run3a
Getting Svelte diagnostics...

svelte-check found 0 errors and 0 warnings
```

第一段（svelte-check）零错误零警告后，`&&` 继续执行第二段
`tsc -p tsconfig.node.json`：该命令**无任何输出并以退出码 0 结束**（tsc 无类型错误
时不打印任何内容），因此整段 `npm run check` 最终退出码为 **0**。

### `npm run build`

```
> svelte-ts-blank@0.0.0 build
> vite build

vite v8.3.1 building client environment for production...
transforming...
✓ 118 modules transformed.
rendering chunks...
computing gzip size...
dist/index.html                          0.46 kB │ gzip:  0.29 kB
dist/assets/compute.worker-CMwCjYZQ.js   3.85 kB
dist/assets/index-HSeic9r8.css           6.01 kB │ gzip:  1.97 kB
dist/assets/index-BwCjV9jw.js           59.67 kB │ gzip: 21.97 kB

✓ built in 190ms
```

> 注一：构建产物中 `compute.worker-CMwCjYZQ.js` 为独立 worker chunk（本轮零拷贝编码
> 已包含在内）；该 chunk 无第三方依赖、体积小于 gzip 展示阈值，因此 Vite 未在该行
> 打印 gzip 数字，属正常输出，原文照录。
> 注二：`✓ built in …ms` 为每次构建的墙钟耗时，随机器负载在 ~190–600ms 间自然浮动；
> 这不影响门禁结论（退出码 0、118 模块转换成功）。

---

## 三、五个决策问答

> 行号基于本轮交付文件；为便于核对，均给出函数/常量锚点与大致行号。

### Q1. 二进制布局的字节序与对齐如何约定？主线程解码时如何防止越界读（条目数与 buffer 长度不一致时怎么处理）？

**字节序与对齐（显式、无协商）：** 全部多字节字段一律 **小端（little-endian）**，
编码/解码用 `DataView` 且每个访问器显式传 `true`（`setUint32(…, true)` /
`setFloat64(…, true)` / 对应 get），不依赖宿主默认字节序，因此 worker 与主线程跨
设备结果位一致。定长布局，天然对齐：header 16 字节，每条目 24 字节（8 的倍数），
`f64` 字段都落在 8 字节倍数偏移上，无 padding 读取、无需长度前缀扫描。

- 布局常量：`src/lib/scheduler/protocol.ts:49`（`BINARY_BATCH_MAGIC = 0x44414731`，
  小端存即 ASCII `DAG1`）、`protocol.ts:51`（`BINARY_HEADER_BYTES = 16`）、
  `protocol.ts:53`（`BINARY_ENTRY_BYTES = 24`）、`protocol.ts:55`
  （`ENTRY_STATUS_DONE = 1`）。
- header：`0 u32 magic`、`4 u32 protocolVersion`、`8 u32 entryCount`、`12 u32 runId`；
  条目：`0 u32 nodeId`、`4 u8 status`、`5..7 三字节保留必须为 0`、`8 f64 value`、
  `16 f64 computeMs`。文件头注释完整列出布局（`protocol.ts:18-44`）。
- 编码：`encodeResultBatch`（`protocol.ts:85-110`），保留字节显式写 0。

**防越界读（先验证长度，再按索引读）：** 解码在
`decodeResultBatch`（`protocol.ts:121-170`）里按以下顺序做结构校验，全部通过后才
进入任何条目循环，从而使之后的每次 `DataView` 读取都可证明落在界内：

1. `byteLength < 16` 直接抛（`protocol.ts:127`）——连 header 都不够，绝不读字段。
2. 校验 `magic`（`protocol.ts:134`）与 `protocolVersion`（`protocol.ts:138`）。
3. **条目数/长度一致性**：算出
   `expected = 16 + entryCount*24`，要求 `byteLength === expected` **严格相等**
   （`protocol.ts:143-152`）。无论 `entryCount` 被篡改偏大（会导致尾读越界）还是
   偏小（漏数据）、或 buffer 被截断/多塞，都不相等，立刻抛 `BatchDecodeError`，
   因此循环 `offset = 16 + i*24` 永远 `offset+24 <= byteLength`。
4. 进入循环后再校验 `status` 只能为 1、三个保留字节必须为 0（`protocol.ts:154-168`）。

任何失败都抛 `BatchDecodeError`；池层把它视同 worker 崩溃处理
（`src/lib/scheduler/pool.ts:378-382`）：终止该 worker、在途任务重投，**不**把半个批
的结果提交。接收侧 `receiveBatchBinary`（`protocol.ts:318`）拿到的是转移后主线程
唯一所有的普通 `ArrayBuffer`；池层解码后只保留普通数值，局部 `payload` 不被闭包/视图
持有，随处理函数结束即可被回收，即「解码并立即释放所有权」（`pool.ts:373` 起）。

### Q2. 增量修订时，删除一个被下游依赖的节点，分层与关键距离哪些节点需要重算？你如何证明没有重算整图？

**需要重算的集合（且仅需要这些）：** 设删除节点为 `d`：

- `d` 本身变 tombstone：`alive[d]=0`、level/distance 置 0，不参与后续。
- **直接/传递下游闭包** `desc⁺(d)`（删边之后可能失去「最高上游层 / 最长关键路径」
  来源的所有存活节点）需要重算 level 与 criticalDistance；与 `d` 无下游可达关系的
  节点，其上游集合与权重路径完全没被触碰，值保持不变。
- 闭包在「修订前」的图上沿 `downstream` 收集，排除同批被删节点
  （`src/lib/scheduler/graph.ts:405-427`）。
- 同批新增节点天然全部纳入受影响集合（它们本来没有旧值）。

level/distance 只对该集合按拓扑序重算（`graph.ts:556-577`）：
`level(v)=0/1+max(level(父))`、`dist(v)=cost(v)+max(dist(父))`，父节点的取值对未受
影响者直接复用旧的 typed-array 槽，不重新推导。

**如何证明没有重算整图（三条可核证据）：**

1. **返回实际重算集合**：`applyPatch` 返回 `PatchReport`
   （`graph.ts:246-255`），其中 `recomputedNodes` 是真正写入 level/distance 的节点，
   `closureVisited` 是收集闭包时访问的下游数。删除一个叶子只会得到空闭包；删除一个
   被少量节点依赖的节点，重算集合远小于 `nodeCount`。
2. **复杂度有界于受影响子图**：结构更新只触碰被删/被增节点的邻接表（入边/出边的
   `indexOf+splice` 与一次过滤边表），分层/距离循环用
   `if (!affected.has(id)) continue` 跳过所有未受影响节点（`graph.ts:559-560`），
   即 O(受影响节点 + 其局部边)，与全图规模解耦。
3. **独立脚本验证**（用 esbuild 打包到 Node 跑，非口头断言）：在 `0→1,0→2,1→3,2→3`
   的 4 节点图上，新增节点 4 时 `recomputedNodes===[4]`（既有 0..3 的 level/distance
   字节不变）；删除节点 1 时 `recomputedNodes===[3]`（只动其传递下游 3）；删除根 0
   时闭包恰为 `{2,3,4}`。脚本同时验证了环插入抛错后图的 `nodeCount/edges/order` 与
   修订前完全一致。

唯一的 O(V) 操作是修订完成后对 `criticalDistance` 这一个 typed-array 做**纯数值取
max** 的关键路径汇总（`graph.ts:578-589`，`PatchReport.didCriticalPathFold` 标记）：
它不遍历任何邻接、不重算任何节点的拓扑量，只是维护「整图标量上界」，不属于图重编译。

### Q3. 抢占恢复后，A 轮在暂停期间错过的帧预算如何处理？恢复的 runId 还是原 runId，双缓冲层如何区分“暂停中的旧结果”与“过期结果”？

**帧预算不补债（no slice debt）：** A 被 B 抢占时，正在执行的那个**已经开始的同步
fast 节点不可打断**（沿用既有黑盒语义），但 slice 在**下一个让出点**停止——双保险：
(i) 堆的 gate 在每次 `pop` 时重读前台 run，A 一旦被标记 `paused`，其堆对后续 pop
一律返回 `null`（gate 定义 `coordinator.ts:447-450`，slice 检查点 `coordinator.ts:447-452`）；(ii) 即使每节点都是亚毫秒、墙钟 deadline
尚未到，单个 slice 还有硬上限 `FAST_SLICE_NODE_CAP=64`（声明 `coordinator.ts:176`，使用 `coordinator.ts:481`），保证每个 macrotask slice 必然真正让出事件循环，让 B 能插
队。恢复时（`resumePausedFull`，`coordinator.ts:380-393`）**不累计/不回放**暂停期间
错过的 4ms 预算，而是从堆的暂停位置开始一个**全新的** 4ms slice——追补帧预算只会造成
恢复瞬间长任务、掉帧，违背 60fps 目标。

**runId 不变：** B 分配自己的新 runId；A 只是 `status: 'paused'`，记录仍保留在
`pausedFull`，其 `id`/`active`/`tracker`/`heap`/已入队 worker 任务全部保留，恢复后
`currentRun` 指回同一记录，runId 就是**原 runId**（集成脚本断言「resume runId ===
起始 A runId」）。

**双缓冲如何区分「暂停中的旧结果」与「过期结果」：** 完全靠 **runId 成员资格**，不
靠时间戳：

- A 暂停期间回来的 worker 结果仍写入 back buffer，并打上 A 的（仍然存活的）runId；
  `ownerOf` 能在 `currentRun`、`pausedFull`、`repairs` 三处找到它
  （`coordinator.ts:500-511`），所以不会被当成无主结果。
- 每帧 `flip`（`coordinator.ts:620-650`）：`liveRunIds()` 只包含**前台 run + 活着的
  repair sidecar，刻意排除 `pausedFull`**（`coordinator.ts:601-608`）。于是
  - `entry.runId === pausedFull.id` 的条目 **`continue` 且不从 back buffer 删除**
    （`coordinator.ts:629`）——这是「**暂停中的旧结果，先 park**」；
  - runId 属于已 retired 集合的条目才被删除并丢弃——这是真正的「**过期结果**」。
- A 恢复后 runId 没变，下一帧这些 park 的条目自然落入 `live` 被接受并提交
  （`coordinator.ts:388-392` 注释明确说明）。集成脚本用「B 挂起在一个 withheld 的
  medium 节点上」制造确定性暂停窗口，验证了暂停期间某 A medium 结果**绝不**提交，
  B 完成、A 恢复后该结果才在同一 runId 下提交。
- 生命周期切换（B 完成→恢复 A、repair 收尾）被刻意推迟到**该帧提交 sink 之后**
  （`coordinator.ts:653-666`），避免 B 最后一帧的 finished/结果在 flip 时被错杀。

### Q4. worker 重跑在途任务时，如果原任务结果其实已经回传（竞态），你的幂等去重在哪一层拦截？

**在池层边界拦截（`pool.ts`），不是在 worker，也不是在 Svelte 层。** 池维护一个全局
`delivered: Set<string>`，键为 `` `${runId}:${nodeId}` ``
（`inflightKey` 定义 `pool.ts:94`、`delivered` 字段 `pool.ts:125`）。所有要上抛 coordinator 的
result/error 都走唯一出口 `deliverOnce`（`pool.ts:316`）：键已存在就**吞掉**
（返回 `false`），保证同一 run 内同一节点至多上抛一次，coordinator/双缓冲/Svelte
根本看不到第二次。

竞态的具体覆盖：worker `onerror` 与该 worker 已排队到主线程事件循环的 `batch-binary`
消息可能先后到达。崩溃处理 `replaceSlot`（`pool.ts:210` 起）先把在途任务重新入队再
摘除槽位；若原结果其实已先被 `handleSlotMessage` 解码并 `deliverOnce` 上抛（键已入
`delivered`），那么替换 worker 重跑后回来的第二份结果在 `deliverOnce` 处被去重——
集成脚本特意让重跑返回一个**不同的 value(99)**，断言 coordinator 只见到原值(8)。
反之，若崩溃时结果从未到达，重投的任务会在新 worker 上正常完成并上抛一次，做到
「不丢节点」。`purgeRun` 会按 `` `${runId}:` `` 前缀清理已彻底退休 run 的去重键
（`purgeRun` 内按前缀清理），避免集合无限增长，也让 runId 复用语义干净。

> 补充：每槽 `inflight: Map<key,entry>`（`pool.ts:83-84`、`postTask` 304、
> `settleSlot` `pool.ts:328-330`）解决的是「槽位持有哪些在途任务」；`delivered` 解决的是
> 「同一结果上抛几次」。二者分工不同。

### Q5. 重试入口直接由 UI 触发调度器，这绕过了“每字符一轮”的 B 语义吗？你的 runId 分配策略如何避免重试污染进行中的 run？

**不绕过 B 语义。** 「每字符一轮」描述的是**输入框 B 触发器**的契约：每次 input
（IME 组合期间抑制、compositionend 触发一次）恰好调用一次 `coordinator.run({
selectedNodes })`，该路径本轮**一行未改**（`src/App.svelte` 的 `handleInput` /
`triggerB`）。表格里的 error 单元格重试是一个**独立的第三入口**，它调用的是
`coordinator.run({ repairNode })`（`App.svelte:87-90`、`DataTable.svelte:68-71`），
既不经过输入框、也不构造子集、更不会伪造一次「字符轮次」，所以不存在对 B 语义的
绕开或稀释。

**runId 分配策略（隔离污染）：** 所有 runId 由同一个单调源 `nextRunId()`
（`coordinator.ts`，`runSeq += 1`）分配。repair 走 `startRepair`
（`coordinator.ts:357-374`）：

- 每次重试都拿到一个**全新、独立的 sidecar runId**，绝不复用进行中 A/B 的 runId
  （脚本断言 `repairId !== runA2`）。
- 它的 `active` 只含被重试的单个节点（**不做上游闭包扩展**），记录进入
  `repairs[]` 而**不**写 `currentRun`，因此**不暂停、不取消、不退休**任何在跑的 A/B。
- repair 的 fast 节点只在 slice 的「前台目标之后」利用空隙执行（`sliceTargets`，
  `coordinator.ts:411-421`），不会延迟进行中的前台 run；非 fast 节点照常交池。
- 结果仍走**同一条** back buffer → rAF flip → commit sink 路径（不新增提交通道）；
  flip 的 `liveRunIds()` 把活着的 repair runId 也纳入可接受集合
  （`coordinator.ts:601-608`），repair 节点的 outcome 合入当帧那**一次**提交。
- 进行中的 A 点击或新一轮 B 会退休全部 repair sidecar（`startFull` 里
  `coordinator.ts:323`），因此重试不可能用旧 runId 给后来 run 的同节点打标，也不会
  在 run 结束后借尸还魂。

---

## 四、与上一轮交付的偏离点及理由

以下为相对上一轮（snapshot `acc3bfb`）我**显式**做出的行为/实现偏离。除列出项外，
A 点击全量、B 每字符一轮子集、结果经双缓冲每帧至多一次提交、cancel 仅翻 adoption
标记不打断在途同步任务、5000 行 `$state.raw` + `content-visibility` 等既有语义均保持
不变。

1. **worker 成功结果改为 worker 侧先解码、再编码进定长二进制批（语义等价、信道改变）。**
   旧路径是每个结果一个 8 字节 `ArrayBuffer` 挂在 `TaskResult.output` 上、整批 N 个
   Transferable；新路径把同一批成功值编码进**一个** `ArrayBuffer`、**一次**转移
   （`compute.worker.ts:108-122` 附近，见 `compute.worker.ts:116`）。理由：本轮硬性要求「一批结果走单个零拷贝
   ArrayBuffer」。代价是 worker 多一次 `decodeValue` + DataView 写入；数值本身不变，
   主线程解码得到的 `value` 与旧 `decodeValue(output)` 完全一致。`errors`/`stale`
   仍走窄 DTO，未二进制化（按要求）。

2. **保留旧 `batch` 信封，不删类型。** 新增 `batch-binary` 变体后，旧 `BatchOutput`
   仍存在：一个 flush 组若**只有 errors/stale、没有成功结果**，仍用 `results: []` 的
   旧信封发送（`compute.worker.ts:122-130`，见 `compute.worker.ts:125`），池层两种都能处理。理由：硬性约束第 4
   条「新增消息类型只能扩充判别联合，不得删改既有类型字段」，且避免为零成功结果白发
   一个空二进制 buffer。

3. **`CompiledGraph` 追加 `costs: NumberVector` 与 `alive: Uint8Array` 两字段。**
   理由：增量重算关键距离需要按 id 取权重（原先权重只存在编译期闭包内）；删除节点
   采用「保留槽位 + tombstone」以保证 run/pool/绑定中已有的数字 id 不发生位移。属纯
   追加，旧字段含义与布局不变；`compileGraph` 的初始结果与此前逐字段等价
   （新增 `alive` 全 1、`costs` 与输入 `costMs` 一致）。

4. **`DagCycleError` 构造函数追加可选第二参 `cyclePath`。** 旧调用
   `new DagCycleError(nodes)` 仍合法（第二参可选）；`applyPatch` 抛出时附带显式闭合
   路径（如 `[2,3,2]`），满足「抛带环路径」要求。未改 `cycleNodes` 字段。

5. **`RunRecord` 由隐式「只有当前一个 run」改为显式状态机并支持三类 run
   （full/subset/repair）+ `status`，并引入至多一个 `pausedFull` 与 `repairs[]`。**
   理由：抢占暂停/恢复与单节点重试都要求在记录上显式建模。对**纯 A/B、无重试、无
   穿插** 的既有操作序列，外部可观察行为与上一轮一致（B 仍退休它前面那个非 full 的
   run；A 仍退休一切）；唯一的语义增强正是「B 打断在跑的 A」由旧的**取消**变为新的
   **暂停—恢复**——这是本轮明确要求，不属于既有语义破坏。

6. **fast slice 增加每 slice 节点硬上限 `FAST_SLICE_NODE_CAP=64`，并允许一个 slice
   扫描越过堆顶的 medium/long 节点（displaced 后归还）。** 旧实现遇到堆顶非 fast
   节点即结束整个 slice。理由有二：(a) 抢占要求每个 macrotask slice 必须是真实让出
   点，单纯靠墙钟 deadline 在「节点都是亚毫秒、定时器有粒度」时可能连续处理数百节点
   仍不让出，使 B 无法在下一个让出点暂停 A；节点上限与 deadline 共同保证让出。
   (b) 关键堆堆顶可能是 medium，旧写法会让其下方本可在本 slice 执行的 fast 节点被饿
   ；displaced 机制让 fast 工作照常推进、非 fast 仍只向池派发一次
   （`RunRecord.dispatched` 去重，字段 `coordinator.ts:82`、派发处 `coordinator.ts:464`）。

7. **`lastScrollAt` 初值由 `0` 改为 `Number.NEGATIVE_INFINITY`。**
   理由：滚动收缩预算的判定是 `now - lastScrollAt < 150`；在 `performance.now()` 从
   接近 0 起算的环境里，初值 0 会让页面**从未滚动时**也被误判为「正在滚动」，导致
   fast slice 永久只用 1ms 预算。改为负无穷后，只有真实滚动发生过才进入收缩窗口；
   真实滚动的行为（150ms 内收缩到约 1ms）保持不变。

8. **池层把「每槽一个 `current`」改为「每槽一个 `inflight` Map」并新增全局
   `delivered` 去重、`failureStreak` 连续崩溃计数。** 理由：崩溃重投可能让多条在途
   任务与重跑结果在时间上交叠，单 `current` 无法表达多在途，也无法做 runId+nodeId
   幂等。对无崩溃的稳态路径，每槽仍等价于「一次一个在途任务」，派发节奏不变。

9. **二进制解码失败（`BatchDecodeError`）视同该 worker 崩溃。** 理由：无法解码意味着
   该 worker 的协议输出已不可信，与其继续接受可能半截的结果，不如终止—替换—重投，
   与「连续失败达阈值才整体降级」一致；这是新增信道的必要错误处理，不影响旧信道。

10. **未修复的既有缺陷（显式声明，不在本轮改动范围）：** 以生产构建做真实浏览器
    （headless Chromium）冒烟时观察到——A 完成 `573/573`、B 完成且控制台**零报错**、
    worker 零拷贝批与暂停/恢复均正常，但 `DataTable` 中 200 个绑定行的状态仍停留在
    `pending`（`tr.done` 计数为 0）。把工作区 `git stash` 回上一轮 snapshot
    `acc3bfb` 后用同一构建/同一脚本复现，现象**完全相同**，故确认为**上一轮已存在
    的数据→行映射缺陷，并非本轮引入**。本轮硬性约束要求「既有交互语义不许变 / 不做
    范围外修复」，因此保持原状、仅在此显式列出，供后续单独处理；本轮新增的 error
    行重试入口在存在 error 行时仍按既有 `applyCommit` 路径工作。

**本轮自测（非验收项，仅佐证）：** 在 Node 下用 esbuild 打包运行了四组临时脚本
（未入库、未加依赖）：`graph` 增量/事务/环路径、`protocol` 编解码对称与五类损坏
拒绝、`coordinator` 暂停—同 runId 恢复—parked 不泄漏—repair 隔离、`pool` 崩溃重投/
竞态幂等/三连崩降级；全部通过。验收仍以本文件第二节的两条门禁原文为准。
