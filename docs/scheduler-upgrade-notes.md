# 调度器协议升级与调度语义增强 — 交付说明

本轮在既有「DAG 调度器 + Worker 池 + DataTable」之上做协议升级与语义增强，
未推倒重写任何既有模块，未新增任何 dependency / devDependency（仅浏览器原生 API
与 Svelte 5 自带能力）。既有交互语义保持不变：A 点击全量、B 每字符一轮子集、
结果经双缓冲每帧至多一次提交。所有新增消息类型均为对判别联合（discriminated
union）的**扩充**，未删改既有类型字段。

- 协议版本：DTO `PROTOCOL_VERSION = 1`（未变），新增独立二进制布局版本
  `BINARY_LAYOUT_VERSION = 1`。
- 验收以本文件与代码同级：`npm run check`（svelte-check + tsc node）零错误、
  `npm run build` 成功。

---

## 一、各模块职责一览

| 模块 | 文件 | 本轮职责（增量部分） |
| --- | --- | --- |
| 协议层 | `src/lib/scheduler/protocol.ts` | 新增判别联合成员 `BinaryBatchOutput`（`kind: 'binaryBatch'`），定义零拷贝二进制批布局常量与 `encodeResultBatch` / `decodeResultBatch`；`errors`/`stale` 仍走窄 DTO；`takeTransfers` 扩充支持单 buffer 转移。既有 `TaskResult`/`BatchOutput` 字段原样保留。 |
| Worker | `src/workers/compute.worker.ts` | 成功结果不再逐条持有 8 字节输出 buffer；在 8ms flush 时按 runId 分组，把一批标量结果编码进**单个 ArrayBuffer** 经一次 Transferable 转移；同组 errors/stale 作为 DTO 随附；编码后立即做一次编解码对称自检。 |
| 图层 | `src/lib/scheduler/graph.ts` | 新增 `MutableDag.applyPatch(addNodes, removeNodes)`：受影响子图内增量更新拓扑分层与关键距离，不调用 `compileGraph`、不做全图扫描；事务性（校验全过才提交，失败原图不变）；插环抛带环路径的 `DagCycleError`。既有 `compileGraph` / `ReadyTracker` / `closureWithUpstreams` 保持不变。 |
| 调度层 | `src/lib/scheduler/coordinator.ts` | 新增协作式优先级抢占：B（subset/retry）到来时暂停而非取消正在执行的 A 全量轮，B 完成后 A 在**原 runId** 上从冻结的堆恢复；`RunRecord` 显式建模 `running / paused / finished`；worker 结果按「产生 run」路由；新增单节点重跑入口 `retryNode` 与图修订入口 `applyGraphPatch`。 |
| 池层 | `src/lib/scheduler/pool.ts` | Worker 崩溃自愈：`onerror` 后同槽位替换新 worker、在途任务以相同 `runId+nodeId` 重新入队；`runId+nodeId` 幂等台账去重，不丢节点、不重复提交；连续崩溃达阈值（3）才整体降级主线程 fallback；解码二进制批并立即释放 buffer；队列按前台/暂停 run 优先级派发。 |
| 表格层 | `src/lib/DataTable.svelte` | error 行的状态单元格出现「error ↻」重试入口，点击触发单节点重跑；经既有 `onRetryNode` → coordinator → 同一双缓冲提交路径，无新增提交路径。 |
| 装配 | `src/App.svelte` | 将重试回调接到 `coordinator.retryNode`；A/B 触发语义与 IME 处理完全不变。 |

### 关键文件与入口行号

- 协议：`src/lib/scheduler/protocol.ts:163`（`BinaryBatchOutput`）、
  `:179-180`（header/entry 长度）、`:219`（编码）、`:264`（解码与越界防护）。
- 图：`src/lib/scheduler/graph.ts:258`（`MutableDag`）、`:338`（`applyPatch`）、
  `:41`（`DagCycleError`，`:52` 带环路径）。
- 调度：`src/lib/scheduler/coordinator.ts:73`（RunRecord 状态）、
  `:297`（抢占判定）、`:338`（`retryNode`）、`:349`（`applyGraphPatch`）、
  `:507`（幂等完成）、`:552`（双缓冲 flip 与暂停区分）、`:590-605`（恢复）。
- 池：`src/lib/scheduler/pool.ts:77`（崩溃阈值）、`:221`（替换自愈）、
  `:425`（二进制批处理）、`:496`（幂等台账）、`:323`（优先级取队）。
- Worker：`src/workers/compute.worker.ts:119`（编码）、`:134`（转移）。
- UI：`src/lib/DataTable.svelte:71`（重试处理）、`:109`（重试入口渲染）。

---

## 二、门禁结果原文

### 2.1 `npm run check`（原样粘贴）

命令：`npm run check`（= `svelte-check --tsconfig ./tsconfig.app.json && tsc -p tsconfig.node.json`），退出码 `0`：

```text
> svelte-ts-blank@0.0.0 check
> svelte-check --tsconfig ./tsconfig.app.json && tsc -p tsconfig.node.json

Loading svelte-check in workspace: /home/ethan_luo/projects/project10/run3b
Getting Svelte diagnostics...

svelte-check found 0 errors and 0 warnings
```

第二段 `tsc -p tsconfig.node.json` 无任何输出且退出码为 `0`（tsc 零诊断时静默）。
为便于核对，两段分别单独执行的原文如下，均为退出码 `0`：

```text
===== svelte-check =====
Loading svelte-check in workspace: /home/ethan_luo/projects/project10/run3b
Getting Svelte diagnostics...

svelte-check found 0 errors and 0 warnings
svelte-check exit=0
===== tsc node =====
tsc exit=0
```

### 2.2 `npm run build`（原样粘贴）

命令：`npm run build`（= `vite build`），退出码 `0`：

```text
> svelte-ts-blank@0.0.0 build
> vite build

vite v8.3.1 building client environment for production...
transforming...
✓ 118 modules transformed.
rendering chunks...
computing gzip size...
dist/index.html                          0.46 kB │ gzip:  0.30 kB
dist/assets/compute.worker-DN7XulRV.js   5.02 kB
dist/assets/index-CLPU6W-w.css           5.80 kB │ gzip:  1.94 kB
dist/assets/index-BoHZgIOM.js           64.32 kB │ gzip: 23.23 kB

✓ built in 258ms
```

---

## 三、五个决策问答（标注实现文件与大致行号）

### 问 1：二进制布局的字节序与对齐如何约定？主线程解码如何防止越界读？

**布局（显式、定长、无长度前缀）。** 全部多字节整数/浮点**显式约定为
小端序（little-endian）**，与宿主机字节序无关——编码/解码一律使用
`DataView` 的 `set/get*` 并传 `littleEndian = true`，绝不使用平台相关的
`Uint32Array`/`Float64Array` 别名去写多字节标量。header 24 字节、8 字节对齐，
每个 entry 32 字节、8 字节对齐；entry 内不留可变载荷（value 本身是 f64 标量，
故不需要长度前缀），总长度严格等于
`BINARY_HEADER_BYTES + entryCount * BINARY_ENTRY_BYTES`。

- Header（24B）：`off 0 u32 magic(0x52424e55)`、`off 4 u32 layoutVersion`、
  `off 8 u32 entryCount`、`off 12 u32 reserved(=0)`、`off 16 f64 runId`。
- Entry（32B）：`off 0 i32 nodeId`、`off 4 u8 status(=1 DONE)`、`off 5..7 填充0`、
  `off 8 f64 value`、`off 16 f64 computeMs`、`off 24..31 填充0`。

常量与字段偏移在 `src/lib/scheduler/protocol.ts:179-205` 集中定义；布局文档
注释在 `:130-161`；编码 `:219 encodeResultBatch`，解码 `:264 decodeResultBatch`。
Worker 侧编码与单次转移在 `src/workers/compute.worker.ts:119-145`。

**防越界读。** 解码在读取任何 entry 之前，先对整段 buffer 做结构校验，且校验
顺序保证「先长度、后读取」：

1. `byteLength < 24` 直接抛 `BinaryProtocolError`（`protocol.ts:268-274`）；
2. 校验 magic、layoutVersion、reserved（`:275-289`）；
3. 读取 `entryCount` 后先与 `BINARY_MAX_ENTRIES`（1e6）做硬上限比较，防止
   损坏的计数驱动异常分配/循环（`:285-288`）；
4. **关键一步**：计算 `expectedLength = 24 + entryCount*32`，要求
   `byteLength === expectedLength` 完全相等，否则整体拒绝（`protocol.ts:290-297`）。
   条目数与 buffer 长度不一致（偏大或偏小、尾部截断）都在此被拦下，不会做
   任何半截解码；
5. 之后每个 entry 的读取基址 `24 + i*32` 必然落在已验证长度内（`DataView` 自身
   也会在越界时抛 `RangeError`），并校验每个 `status` 字节（`:304-318`）。

解码后标量被拷贝进普通对象返回，**不保留对 buffer 的任何引用/视图**，主线程
随即释放所有权；池层在 `src/lib/scheduler/pool.ts:429` 调用解码，buffer 在该
函数返回后即不可达。若结构校验失败，池按 worker 故障处理该槽位
（`killAndReplaceSlot`，`pool.ts:451-456`），绝不提交半批结果。

### 问 2：增量修订删除一个被下游依赖的节点时，哪些节点需要重算？如何证明没有重算整图？

**重算集合（只在受影响子图内）。** 分层 `level` 与关键距离 `criticalDistance`
都只沿「上游 → 本节点」方向计算：

- 删除节点 `x`：`x` 本身移除；需要重算的是**从 `x` 的旧下游出发可达的所有存活
  节点**（旧直连下游 + 其传递下游），因为删除一条上游边只会让 level 下降或
  不变、让 criticalDistance 下降或不变。删除发生前先快照 `x` 的旧下游表
  （`src/lib/scheduler/graph.ts:438-441`，在拆边之前捕获），阶段三以它为种子
  BFS 出受影响集合并在拓扑序上重算（`:544-594`）。
- 新增节点：新节点自身 + 其传递下游（新增边只会让值上升）需要重算；若新增
  节点是叶子（无下游），则只重算它自己。
- 集合之外的节点：其上游集合与各上游的值都未变，值必然不变，阶段三的主循环
  `if (!affected.has(id)) continue`（`:576-577`）直接跳过，不读不写。

**不重算整图的证据（结构性 + 可观测）：**

1. `applyPatch` 全程不调用 `compileGraph`，没有 Kahn 全量 pass，也没有对
   `0..nodeCount` 的 level/distance 全扫描；唯一的遍历作用域是 `affected` 集合
   与删除节点的旧下游（类契约注释见 `graph.ts:236-256`）。
2. 函数返回 `PatchResult.recomputedNodes`，**显式列出本次真正重算的节点**，
   可直接断言其规模受限于受影响子图而非全图（`graph.ts:227-231`、`:575-595`）。
   例如在一个 0→1→2 的链外加独立叶 3、4 的用例上：删除 0 时返回的
   `recomputedNodes` 恰为其传递下游 `{1,2,3?,4?}` 中真正受影响者，数量与全图
   节点数无关；新增一个无下游的叶子时返回长度恰为 1。
3. 事务性保证：所有校验（重复 id、未知依赖、未知/被删依赖、新边成环）都在
   **提交前**基于「前瞻邻接」完成（阶段一 `graph.ts:344-431`），任一失败即抛
   带环路径的 `DagCycleError` 且内部状态一字未改；只有全部通过才进入阶段二
   提交（`:433` 起）。因此「要么全部生效、要么全部回滚」。
4. 拓扑序通过对新增子图做一次仅覆盖新增节点的 Kahn 排序后按位置插入维护
   （`:486-536`），同样不是全图重排；关键路径只在旧 owner 消失或落在受影响
   集合内时才全量扫描 live 集合，否则只在重算子集内取 max（`:607-630`）。

上述行为由仓库外的临时行为脚本验证（二进制布局、受影响子图规模、回滚后
`liveNodeCount`/`order`/levels 不变、环路径含成环节点均通过）；脚本不属于
交付物，结论在此显式说明。

### 问 3：抢占恢复后，A 暂停期间错过的帧预算如何处理？恢复 runId 是否不变？双缓冲如何区分「暂停中的旧结果」与「过期结果」？

**暂停而非取消，runId 不变。** B（subset）到来时，若前台是一个未完成的全量
（A）轮且当前没有别的暂停轮，则把该 A 轮标记为 `paused` 并挂到 `pausedRun`，
**不分配新 runId、不调用 pool.cancel**（`src/lib/scheduler/coordinator.ts:297-307`）。
RunRecord 的生命周期显式建模为 `running | paused | finished`（`:73`）。A 的
`CriticalHeap`、计数器、ReadyTracker 原样冻结；暂停期间已入队/在 worker 上
运行的任务**照常回收入账**——worker 结果按「产生它的 runId」从 `runsById`
路由（`:463-464`），即使该 run 此刻不在前台也会照常 `complete`（`:507-533`）。

**下一个让出点暂停。** 主线程 fast slice 每轮循环都以
`currentRun === run && run.status === 'running'` 为门，且堆的 `pop()` 自带
runId 门（`runSlice` `:383-401`）。A 被挂起后，其正在执行的当前节点跑完，
随后不再从它的堆取节点，即「在下一个让出点暂停」。B 完成触发 flip 的末尾，
把 `pausedRun` 提升回 `currentRun`、状态改回 `running`，并在**同一个 runId**
上 `scheduleSlice()` 恢复（`:590-605`）。连续多轮 B/retry 期间 A 持续挂起，
只有紧邻的前台轮被 retire，A 不被连带取消（链式抢占分支 `:310-316`）。

**错过的帧预算不补发（no catch-up burst）。** 恢复时不做「把暂停期间欠的帧
一次性补提」的突发提交，避免一次大提交冲击帧预算；恢复后仍走既有的
「每帧至多一次提交」路径，暂存的 back-buffer 条目按自然帧率逐帧排空
（注释见 `:599-602`）。同时恢复时调用 `pool.kick()`（`:605`；池实现在
`src/lib/scheduler/pool.ts:134-140`）消除「槽位空闲但队列任务缺唤醒事件」的
lost-wakeup 竞态。

**双缓冲如何区分暂停中的旧结果 vs 过期结果。** flip 只按 runId 归属三分
（`coordinator.ts:560-578`）：

- `entry.runId === currentRun.id` → 前台结果：本帧取出、进 front buffer、提交；
- `entry.runId === pausedRun.id` → **暂停但存活**：`continue` **保留在 back
  buffer**，既不提交也不丢弃，待恢复为前台后再提交；
- 两者都不是（retired/被取代轮的 runId）→ 才是**过期结果**：在缓冲边界删除，
  永不到达 sink、永不触碰响应式状态。

因此「暂停中的 A 结果」与「过期结果」的区别就是：暂停轮的 id 仍登记在
`pausedRun`（live），过期轮的 id 既不是前台也不是暂停轮。因为恢复沿用原
runId，A 恢复后其暂停期间积压的条目会被第一类（前台）正常提交，不会被误删。
恢复后 runId 不变这一点与「B 每轮都是全新 runId」并存：B 用新 id 触发暂停，
A 始终是自己原来的 id。

（验证：以会在 2ms 后回二进制批的假 worker 跑 A→B，观测到提交序列
`…,2F,1,1,…,1F`，即 B 以 runId 2 先完成，随后 A 在 runId 1 上恢复并最终
`573/573 finished`；脚本为仓库外临时验证，结论在此说明。）

### 问 4：worker 重跑在途任务时，如果原结果其实已经回传（竞态），幂等去重在哪一层拦截？

**在池层（pool）拦截，键为 `runId+nodeId`。** Worker `onerror` 触发自愈时，
槽位把当前在途任务以**相同的 runId、nodeId 原样重新入队**
（`src/lib/scheduler/pool.ts:221-247`）。若崩溃前原任务结果其实已经回传，则
替换 worker 的重跑会产生第二条相同 `runId+nodeId` 的结果。池维护
`deliveredKeys: Set<"runId:nodeId">`，在**任何终端结果（成功 result 或 error）
向 coordinator 发射之前**统一经 `markDelivered(runId, nodeId)`
（`:496-503`）：首次返回 `true` 放行，重复返回 `false` 直接吞掉，不向上发射。

调用点覆盖全部终端通道：旧 DTO 批的 result/error（`pool.ts:380`、`:393`）、
二进制批的 result（`:440`）、二进制批随附 errors（`:463`）。因此无论重复来自
崩溃重跑、还是批内/批间重复，都在**到达 coordinator 之前**被去重，满足「不
重复提交」。cancel 的交付不登记键（被 `cancelledRuns` 门挡下），所以一个在
**新 runId** 下的合法重跑不受影响——键内含 runId；run 退休清理时
`purgeRun` 同步按前缀清除其台账键（`:518-523`）。

coordinator 侧还有一层独立幂等：`complete()` 用 RunRecord 自己的 `completed`
Set 保证每个节点在一次 run 内只被 retire 一次（`coordinator.ts:507-533`），
即使池层假设被破坏也不会重复推进 `remaining/done`。两层各司其职：池层保证
「不重复发射终端结果」，调度层保证「同一 run 内不重复记账」。

同时自愈保证「不丢节点」：崩溃槽位的 `current` 必被重新入队
（`pool.ts:233-238`），替换 worker 在稳定槽位 id 上重建（`:248-253`）；
队列派发还会跳过已 cancel run 的残留并补发 stale 以结清记账
（`takeNextQueued` `:323-352`）。只有**连续**崩溃计数达到
`CONSECUTIVE_CRASH_LIMIT = 3`（`:77`、`:240-243`）才整体降级到主线程
fallback；期间任何一次健康 worker 的 `ready`/响应都会把计数清零
（`:193-197`），降级后 fallback 仍把所有节点跑完（不丢、不重）。

（验证：假 worker 在投递首结果后立刻 `onerror`，重跑同节点——断言 8 个节点
全部交付且每个 nodeId 仅 1 条 result、单次崩溃不降级；always-crash 模式下
连续崩溃达阈值后进入 fallback 且仍全交付、无重复。脚本为仓库外临时验证。）

### 问 5：重试入口直接由 UI 触发调度器，是否绕过「每字符一轮」的 B 语义？runId 分配如何避免重试污染进行中的 run？

**不绕过，也不改 B 语义。** A（点击全量）与 B（每字符一轮子集、无防抖、IME
compositionend 单轮）的触发路径在 `src/App.svelte` 中一字未动。error 单元格
的重试按钮（`src/lib/DataTable.svelte:109-118`）仅新增一个回调 `onRetryNode`，
它调用 `coordinator.retryNode(nodeId)`（`coordinator.ts:338-342`），而
`retryNode` 内部复用的就是**同一个 `run()` 调度入口、同一个堆、同一个池、
同一条 back-buffer → rAF 提交路径**，没有任何新的提交/执行通道。

**重试 = 单节点 subset 轮，而不是并入进行中的 run。** `retryNode` 以
`{ selectedNodes: [nodeId], includeUpstreams: false }` 调 `run()`
（`coordinator.ts:341`）：`includeUpstreams:false` 使该轮的 active 集合**只有
该错误节点本身**（B 默认 `true` 会闭包上游，见 `:56-62`、`:265-271`），因此
重跑精确到一个单元格绑定的节点，不拖入上游、也不改变 B 的每字符子集行为。

**runId 分配隔离污染。** 每次 `run()` 都分配一个新的单调递增 runId
（`:262-263`），重试绝不复用、也不就地修改产生该错误的旧轮 id，因此：

- 重试的结果只带新 runId，不可能写进进行中 B 轮的计数/提交；
- 若重试到来时前台是一个未完成的 A 全量轮，它按问 3 的规则把 A **暂停**，
  重试轮跑完后 A 在原 runId 恢复；
- 若前台已经是一个 B/retry 轮，则仅取代紧邻前台轮，背后暂停的 A 继续挂起
  （链式抢占 `:310-316`），进行中那轮的节点不会被重试结果污染。

UI 层重试只在 `row.status === 'error'` 且该单元格有绑定函数、且外部传入了
`onRetryNode` 时出现（`DataTable.svelte:71-75、109`）；节点 id 从稳定的
`fn-<index>` 绑定解析，非法值不触发。重试结果仍按 runId 经双缓冲每帧至多
一次提交，满足所有既有提交约束。

---

## 四、与上一轮交付的偏离点及理由（显式列出）

以下均为「在既有模块上的增量改动」，无推倒重写；凡涉及协议处均为判别联合的
扩充或新增可选字段，未删除/改名/改类型任何既有字段。静态数据集（FUNCTIONS）
未变，故 A/B 在默认页面上的可观测行为与上一轮一致。

1. **成功批改走新增 `binaryBatch`，按 runId 分组、每组一个信封。**
   上一轮 worker 每次 flush 发一个 `BatchOutput`（可能跨多个 runId，逐条带
   Transferable）。本轮成功结果改为：按 runId 分组，每组编码进**单个
   ArrayBuffer** 一次转移（新变体 `BinaryBatchOutput`）；仅含 error/stale 的
   组仍发旧形态 `BatchOutput`（`results: []`）。理由：实现题面要求的零拷贝单
   buffer 通道，同时保持 errors/stale 窄 DTO。旧的 `TaskResult` /
   `BatchOutput.results` 类型与 `receiveResult` **保留不删**（向后兼容），只是
   出厂 worker 不再经它们发送成功结果。实现：`protocol.ts:163-176`、
   `compute.worker.ts:74-156`。

2. **`PoolOutcome` 新增可选 `value?: number`，保留 `output?`。**
   二进制路径直接交解码后的 f64 标量（buffer 在池内即释放）；旧 DTO 路径仍带
   `output: ArrayBuffer`。coordinator 优先取 `value`，否则回退解码 `output`
   （`coordinator.ts:481-485`）。纯新增可选字段，旧消费者不受影响。

3. **池队列由「严格 FIFO」改为「前台 run 优先、暂停 run 次之、FIFO 破平」。**
   新增 `setRunPriority` 探针（`pool.ts:146-152`）与 `takeNextQueued`
   （`:323-352`）。理由：B 抢占 A 后，新到的 B 任务不能被暂停前轮更早入队的
   A 任务堵在 FIFO 队头；retired run 的残留也不再占用槽位（改发 stale 结清）。
   同一 run 内部仍是 FIFO，空闲槽位足够时二者本就并行，可观测差异仅出现在
   槽位饱和且跨 run 竞争时。

4. **Worker `onerror` 由「杀槽位，槽位清空即降级」改为「同稳定槽位 id 替换 +
   在途任务同 runId+nodeId 重跑，连续 3 次崩溃才降级」。** 这是题面要求的自愈
   语义；相应地动态建 worker 的槽位 id 从「取 `slots.length`」改为
   `missingSlotId()` 复用空出的稳定下标（`pool.ts:255-272`），并在健康
   `ready`/响应时清零连续崩溃计数（`:193-197`）。

5. **`DagCycleError` 构造新增可选第二参数 `cyclePath`，新增静态 `forPath`。**
   既有单参数构造与 `compileGraph` 的调用保持可用（`graph.ts:41-56`）；仅为
   增量插环提供「带环路径」。属向后兼容的可选参数扩充。

6. **`MutableDag.snapshot()` 的 `nodeCount` 为「槽位容量」而非「存活节点数」。**
   为保证删除后再新增不重排既有节点 id（索引稳定），被删 id 保留其类型化数组
   槽位，仅从 live 集合/order/roots 中移除；故快照 `nodeCount` 可能大于存活数
   （初始为 `ceil(573*1.5)` 的容量）。`ReadyTracker` 按节点 id 稀疏索引，仅多占
   数组容量，正确性不变；全量轮 active 集合改为按 live 集合过滤
   （`coordinator.ts:266-268`），在静态全图下与上一轮结果完全相同。存活数以
   `MutableDag.liveNodeCount` / `liveIds` 为准（`graph.ts:319-326`）。

7. **worker flush 增加一次编解码对称自检。** 编码后立即 `decodeResultBatch`
   复核 runId 与条目数一致（`compute.worker.ts:128-133`），不符即抛错使该
   worker 走自愈/降级而非发出坏批。代价为每 flush 一次小批量解码（批规模小、
   每 8ms 至多一次），以一条廉价运行时不变量换取线格式对称性的硬保证。

8. **新增公共能力但未接入页面 UI：`coordinator.applyGraphPatch`。**
   图增量修订接口（`coordinator.ts:349-356` → `MutableDag.applyPatch`）已交付，
   但当前数据集是静态生成的、页面没有增删节点的交互入口，故没有按钮接它；
   其正确性由仓库外临时行为脚本验证（见问 2）。error 行重试入口
   （`retryNode`）则已完整接入 UI。此项为「接口已交付、暂无页面触发方」的
   范围说明，不改变任何既有交互。

9. **新增少量公共方法（纯增量，无既有方法签名变更）：** coordinator 的
   `retryNode` / `applyGraphPatch`；pool 的 `kick` / `setRunPriority`；
   protocol 的 `encodeResultBatch` / `decodeResultBatch` / 布局常量；
   graph 的 `MutableDag` 及 `PatchNodeInput` / `PatchResult`。`PROTOCOL_VERSION`
   未变；二进制线格式由独立的 `BINARY_LAYOUT_VERSION` 管理。

