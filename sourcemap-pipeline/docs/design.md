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
