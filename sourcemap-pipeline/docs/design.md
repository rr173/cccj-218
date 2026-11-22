# 设计决策与可复查依据

## 1. 为什么有两份 VLQ 解码器

`src/smpipe/vlq.py` 是生产解码器，`tests/reference_decoder.py` 用另一种
扁平字符循环风格独立实现，两者不共享任何代码。测试对每一张手写段表同时用
两边解码并要求逐元组相等，从而证明生产解码器覆盖：正负值、多字节 VLQ、
空生成行（`;;`）、行边界寄存器重置、段字段数为 1/4/5、非法字符、悬空分隔符、
超长续位。生产解码器另外强制：负绝对坐标拒绝、生成列不得回退；同列段交由
存储层区分“完全相同可折叠”与“同坐标冲突映射”。

## 2. 段定位规则（不跨段回退、不跨行借值）

`Store.lookup_segment` 只在**同一生成行**内取 `gen_col <= 查询列` 的最近段：

* 命中段无原位置（单字段段）→ 直接 `unmapped / segment_without_original_position`，
  不会继续向更早的段回退；
* 查询行上一个段都没有 → `unmapped / no_segment_on_generated_line`，
  不会借上一行的段；
* 命中段给出的原行列是**显式坐标**，下一层直接使用它，绝不把生成列差值
  追加到原列。

## 3. 列单位与代理对

外部帧 1-based，单位为 UTF-16 代码单元或 Unicode 码点；Source Map 内部一律
零基 UTF-16。`LineIndex` 缓存行起点与（仅非 BMP 行的）非 BMP 跨度表：

* 纯 BMP 行两条单位轴重合，O(1)；
* 非 BMP 行通过二分跨度表定位，落在低代理半段的列以
  `inside_surrogate_pair` 拒绝，两种单位输入指向同一真实字符时归一到同一
  UTF-16 列，因此最终位置一致。

## 4. sections 展平与拒绝项

* 仅接受内嵌 `map`；任何 `url` 形式的外部 section 抛
  `external_section_not_supported`，不静默跳过、不抓取；
* section offset 必须严格递增，相同起点判重叠；展平后若两 section 产生同一
  全局生成坐标也判 `section_overlap`；
* 列偏移只作用于 section 的首个生成行，后续行保留自身列号；
* 嵌套 section 设深度上限 64，防止解析循环；version 非 3 直接拒绝。

## 5. 绑定、循环与身份

下一跳只能来自清单 `source_bindings`，且目标必须是同一 release 内已登记的
artifact。富化时对 `(artifact_id, map_digest)` 签名做 visited 检测，重复
访问即 `cycle / binding_cycle`，并设有 32 层硬上限。

## 6. 存储与有界缓存

* 段表唯一索引 `(map,line,col,src,line,col,name)` 折叠完全相同映射；
  `conflicting_coordinates` 用相关子查询找同坐标异映射；
* 稀疏行走 2 MiB LRU 行缓存，密集行（默认 >64 段）走单条 B-tree 点查，
  避免把超长行整体拉进 Python；
* map names 与正文按摘要做有界 memo；生成正文用 `LineIndex` 而非每帧
  `splitlines()`；
* 所有写入在关闭时 commit 并 `wal_checkpoint(TRUNCATE)`，删除输入目录后
  重开库仍能 explain 出完整归源依据；
* metrics 表记录 `segment_lookup_queries`、`sql_point_lookups`、
  `line_loads`、`line_cache_hits`、`blob_reads` 等访问量，作为“不重新解码
  整图”的量化证据。

## 7. 失败也是有效观测

富化永远产出结构化结果而不是丢帧：缺 URL 绑定、坏 map、循环、代理对列、缺
正文但有原位置等都有独立状态与稳定 reason，并进入带 release/产物身份的分组。

## 8. 曝光首错事实表（增量维护，不重建）

灰度场景下同一浏览器会话会在新旧发布间反复切换。事实表回答：每条曝光
（某会话在某一刻生效的发布）在其生效区间内，**第一条归源到该发布本身的
已归源错误**是哪一条；至今没有合格错误的曝光也保留一行、首错字段为空。

### 8.1 两个库、只读边界

* **归源库（sealed）**：由现有 `enrich` 产出，封盘后字节不再变化。事实表
  只用 `immutable=1` 只读连接读它的 `frames` 表（主键、`release_id` 与
  `result_json.status`），不读构建目录、不重新归源、绝不写入；首次绑定计算
  整库 SHA-256 指纹写入工作库 `fact_meta`，重开后指纹不一致直接拒绝。归源库
  若仍带非空 `-wal`（未 checkpoint 封盘）也拒绝，避免读到不完整视图。
* **工作库（新）**：`exposures`、`event_locs`、`exposure_facts`、`batches`
  台账与 `fact_metrics` 计数，全部在一个独立 SQLite 文件里。

### 8.2 时间轴与归属规则

* 每会话按 `start_ms` 排序，曝光区间是左闭右开
  `[start_ms, 下一条 start_ms)`；首条之前无曝光，最后一条 `end_ms` 为空。
  事件时刻恰好等于某曝光起点时，归属该曝光（起点包含）。
* 同会话 `(session_id, start_ms)` 最终状态必须唯一；校验针对**整批投影后的
  最终状态**。落库时先删除本批涉及的曝光再按最终状态插入，因此同批合法换位
  不会因中间态撞唯一索引而失败，而真正的最终冲突仍被拒绝。
* 事件合格条件：在该事件的帧里存在 `release_id` 等于曝光发布 **且**
  `status='mapped'` 的结果；多帧取最小 `frame_index`。不合格就不计，**绝不回
  找前一条发布**。未匹配到曝光、或没有合格帧的事件都不是坏输入。
* 每曝光在合格事件中取最小 `(occurred_ms, event_id)`，`event_id` 按字符串
  比较、与导入顺序无关；延迟 = `occurred_ms - start_ms`。

### 8.3 局部增量、受影响范围的精确界定

导入只重算可能变化的曝光，作用域与记录移动的距离无关：

* 变更的曝光：在变更前、后两条时间轴上各标记自身及直接前后邻（只有邻接
  曝光的 `end_ms` 可能变）；
* 变更的事件定位：只标记旧时刻在旧时间轴上的承载曝光、与新时刻在新时间轴
  上的承载曝光各一条。事件从时间轴一端跳到另一端只触碰两条事实，不遍历中间
  互不相交的区间；
* 被删除的曝光只删其事实行；不在受影响集合内的事实行一律不写。

事件检索走 `idx_event_window(session_id, occurred_ms, event_id)` 的有界区间
索引读；归源判定走归源库 `frames` 主键的 `SEARCH`（EXPLAIN QUERY PLAN 可见，
不是 `SCAN`）。`fact_metrics` 暴露
`frame_qualification_queries`、`fact_rows_updated/inserted/deleted/unchanged`
作为“没有全量遍历、没有成批重写”的量化证据。

### 8.4 原子性与批次台账

定位、时间轴、事实与批次进度在同一事务内提交；结构非法（同 ID 一批重复、
撤回不存在的 ID、最终时间轴冲突、字段类型错误等）在任何写入前就被拒绝，坏
批次不留下部分变化。`batches` 记录每序号的原始字节 SHA-256 与长度：同序号
同字节重放是空操作，同序号异内容或跳号拒绝；规则持久化在工作库里，重开
不变。导出按 `(session_id, start_ms)` 稳定排序，直接读已物化的事实表，
不从历史批次现场重建。

## 9. 源码区间反查索引（区间计算 + 共享点复用）

`enrich` 是「已知生成坐标找源码」；反查索引是「已知源码区间找全部生成
区间」，面向至今没有报错的代码位置，因此覆盖清单只能来自整批构建资料，
不能来自已有错误分组。

### 9.1 两个库、自包含

* **构建库（sealed）**：`ingest-build` 的产物，封盘后只读（`immutable=1`
  打开、非空 `-wal` 拒绝）。反查只需要 `artifacts` / `url_aliases` /
  `maps` / `map_sources` / `bindings` / `segments` / `blobs`，**不需要
  `frames`**，也不读原构建目录；建索引时把整库 SHA-256 写进索引 `rev_meta`
  作为来源指纹。
* **索引库（新）**：`entries`（每个登记过 URL 的
  `(release_id, artifact_id)` 一行，别名去重）与 `hits`（物化命中），
  索引 `idx_hits_source(source_identity, src_line, src_col)`。建好后
  `state=complete` 才接受查询；查询只带这一个文件即可重开。

### 9.2 覆盖区间而不是逐字符归源

入口行上相邻的已入库段决定各自覆盖：段 i 覆盖 `[col_i, col_{i+1})`，末段
覆盖到行尾（按 UTF-16 行长截断），首段之前不归任何段，无原位置段同样
截断前段——与 `lookup_segment` 的「同行走最近 ≤ 列」语义逐点等价。索引
对象只是真实字符起点：不含行尾之后，不含代理对后半个单元（低代理位是
空洞）。每段只做**一次**归源，覆盖区间内连续有效列且最终源码坐标相同
就合成一段输出；空洞两侧、不同源码点之间绝不合并。200 万列的长行因此
只产生常数次解析与常数行命中记录（演练计数器
`generated_columns_indexed` vs `hit_rows` 为证）。

### 9.3 一跳点记忆化与循环的路径相关性

后续层只查上层段给出的固定原坐标，不加列差。大量入口段会汇入同一中间
坐标，因此按 `(release, artifact, map, line, col)` 记忆化一跳点的解析
结果。记忆项携带该链访问过的 `(artifact, map)` 签名集与消耗层数，复用
前必须满足：签名集与当前路径的 visited 不相交、且剩余层数预算足够
（`depth + layers_used <= 32`）。**循环与超层结果从不写入缓存**——它们
依赖入口路径，不能当成通用结果；演练里 `cyc-a`（经共享中间点命中）与
`cyc-b`（同一中间点但循环回自身）证明两条路径互不污染
（`hop_cache_rejected_path` 计数可见拒绝重算）。

### 9.4 命中语义与查询定位

断链、无映射、坏 map、循环分支只让自己的覆盖列无命中，不吞同一产物的
其他有效分支；最终源码正文缺失不影响精确坐标命中（索引根本不读源码
正文）。查询按 `(source_identity, src_line, src_col)` 索引定位行带，
起止为坐标对字典序的半开区间（可跨行；倒置或相等报
`invalid_source_range`），不扫描其他源码的生成区间、不现场重走映射链；
输出按 `(release_id, artifact_id, gen_line, gen_col_start)` 稳定排序，
每行含发布、入口产物 ID 及摘要、生成行、生成起止列、最终源码身份及
精确行列。演练用按字符枚举的独立参考实现对小发布逐行对拍，并用
`EXPLAIN QUERY PLAN` 展示窄查询的 `SEARCH ... USING INDEX
idx_hits_source` 访问形态。

## 10. 触发操作血缘（离线逐事件导出，不在线维护）

归源帧回答「哪段代码报错」；血缘回答另一个运维问题：这条错误是从哪次
实际用户操作，经哪些异步工作一路传下来的。它只描述**观察到的触发链**，
不诊断代码错误，因此既不读 Source Map、也不读曝光事实表与反查索引。

### 10.1 两个输入、只读边界与磁盘暂存

* **封盘归源库（sealed）**：沿用 `immutable=1` 只读连接与非空 `-wal`
  拒绝规则；只对 `frames` 做一次 `SELECT DISTINCT event_id WHERE
  result_json IS NOT NULL` 取事件全集，多帧事件天然一行，帧状态
  （含归源失败）不影响纳入。归源库字节在导出前后不变、无 WAL 侧车。
* **无序遥测 JSONL**：`action` / `work` / `error` 三类记录。节点身份是
  `(run_id, node_id)`，父边只在同运行解析；`parents` 按集合归一化
  （去重 + 排序后存规范签名），因此输入行序、父列表次序与等价重送都不
  改变结果。
* 一次导出可能覆盖大量彼此独立的运行。解析后的节点、父边、事件绑定、
  事件全集与结果全部写入临时目录里的暂存 SQLite（`WITHOUT ROWID` 主键
  表），进程退出即删；内存中任一时刻只装一个运行的图，不把所有运行的
  关系与所有事件路径同时留在内存。

### 10.2 冲突拒绝与采样缺口的分界

* **输入冲突**（`lineage_input_conflict`，拒绝整份遥测）：同 `(run,node)`
  出现 `action`/`work` 类型冲突、同一 work 节点两份不同父集合、同一
  `event_id` 绑定到两个不同 `(run,node)`、或 error 的 `event_id` 不在
  归源帧事件全集内。结构非法（字段类型、空 ID、空 `parents`、未知字段、
  坏 JSON）报 `invalid_telemetry` 并带行号。
* **采样缺口**（不拒绝）：父 ID 或上报节点在本运行无定义记录。缺口只
  影响从该节点出发的可达性标记；绑定到未定义上报节点的事件是
  `unresolved` 且 `reaches_missing=true`，而不是冲突。
* 所有校验在写出前完成；输出先走同目录临时文件，成功后 `os.replace`
  原子替换，尾部坏记录保留原有导出。

### 10.3 O(V+E) 的共享血缘计算

一个运行里大量 error 常共享同一片上游关系，因此逐节点血缘只计算一次，
与 error 数量无关：

1. 迭代式 Tarjan（显式栈，不受递归深度限制）在排序节点与排序邻接上求
   强连通分量：多节点分量与自环都判为有向环；分量按汇点优先（缩点 DAG
   的逆拓扑）顺序产出。
2. 在缩点 DAG 上按该顺序传播三个量：可达缺失标记、可达环标记、可达
   action 集合。action 集合在每个节点上**始终封顶为 ID 最小的两个**
   （`merge_top_actions` 只做小集合归并），所以传播代价与可达操作总数、
   路径组合数无关；环分量即使另有出口也标环，出口后的 action 照常
   收集。
3. 复查路径只为最终入选的候选重建：每个不同上报节点至多跑一次 BFS，
   邻接有序保证同距离按节点 ID 序列字典序稳定选定，结果按节点记忆化，
   共享同一上报点的多个 error 复用同一张路径表。计数器
   `path_bfs_runs` / `path_lookups` / `path_cache_hits` 是复用证据；
   演练中 20 与 400 个共享上报点的错误相比，`nodes_defined`、
   `parent_edges_stored`、`path_bfs_runs` 完全持平。

### 10.4 行状态与稳定输出

每行字段固定为 `event_id`、`run_id`、`node_id`（未绑定为 `null`）、
`reaches_missing`、`reaches_cycle`、`status`、`candidates`（每项含
`action_id` 与两端包含的 `path` 节点序列），按 `event_id` 字符串升序、
键名排序写出。状态机：未绑定 → `no_anchor`；已绑定且候选 ≥2 →
`ambiguous`；恰一个候选且无缺口无环 → `attributed`；其余已绑定 →
`unresolved`。测试侧另有不共享任何图代码的朴素 DFS/染色/BFS 参考实现
（`tests/reference_lineage.py`），在 120 组随机图与完整导出上逐值对拍。

