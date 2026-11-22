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

