# smpipe — 离线错误事实富化管道

把观测仓收到的前端压缩产物报错，**离线、可复查地**还原到真正的源码坐标。

* 不执行任何用户脚本，不修复网页错误；
* 不访问网络，不依赖在线 symbol 服务，所有资料来自本地构建清单；
* 产物以 `release_id` + `artifact_id` + SHA-256 固定，同一 URL 在不同发布中
  天然隔离；
* Source Map 严格按 ECMA-426 / v3 解析：普通 `mappings`、内嵌 `sections`、
  `sourceRoot`、`names`、无原位置段、section 行列偏移；外部 section URL
  明确拒绝；
* 落盘 SQLite 段索引 + 有界缓存，百万段 map 在 256 MiB 预算内可重开查询，
  每帧只走一次索引点查，不重新解码整张 map。

## 目录结构

```
src/smpipe/
  errors.py      稳定错误码（bad_vlq / cycle / digest_mismatch ...）
  vlq.py         生产用 base64 VLQ 编解码器（严格校验）
  columns.py     UTF-16 代码单元 / Unicode 码点转换、代理对防护、LineIndex
  sourcemap.py   v3 map 解析、sections 展平、sourceRoot、冲突/顺序校验
  store.py       可重开 SQLite 存储、段 B-tree、有界 LRU、访问计数
  pipeline.py    ingest-build / enrich / explain / groups 主逻辑
  facts.py       曝光首错事实表：批次台账、局部增量维护、只读归源绑定
  reverse.py     源码区间反查索引：建索引 / 查索引（可重开、自包含）
  lineage.py     触发操作血缘：只读归源库 + 异步遥测的离线逐事件导出
  cli.py         命令行入口
tools/
  generate_fixtures.py   手写两层 map 等全部演练资料生成器
  stress_test.py         百万段 map + 五万重复帧压力测试与访问量记录
  facts_walkthrough.py   曝光首错事实表端到端复跑演练（含参考实现对拍）
  reverse_walkthrough.py 反查索引端到端复跑演练（含按字符枚举对拍）
  lineage_walkthrough.py 触发操作血缘导出端到端复跑演练（含独立参考实现对拍）
tests/
  reference_decoder.py  独立风格的第二份 VLQ 解码器（仅测试交叉核对）
  reference_facts.py    只保留最终输入、每次从零重算的事实表参考实现
  reference_lineage.py  朴素 DFS/BFS 从零重算的血缘参考实现（不共享图代码）
  test_pipeline.py      42 条实际断言
  test_facts.py         曝光首错事实表行为/增量/对拍/局部性断言
  test_reverse.py       反查索引行为/区间合并/循环差异/重开断言
  test_lineage.py       血缘 SCC/封顶传播/路径/冲突/原子性/复用/对拍断言
  run_tests.py          python -I 隔离模式测试入口
```

## 快速开始（从零重建）

```bash
cd sourcemap-pipeline

# 1. 生成手写演练资料（小资料，四个 release + 六类坏资料）
python3 -I tools/generate_fixtures.py --out ./demo-data

# 2. 建库并摄入四个 release（可重复执行，按摘要幂等）
for r in r1 r2 r3 r4; do
  python3 -I smpipe.py ingest-build \
    --store ./run/store.db \
    --manifest ./demo-data/$r/manifest.json
done

# 3. 富化规范化堆栈帧（JSONL）
python3 -I smpipe.py enrich \
  --store ./run/store.db \
  --frames ./demo-data/frames/frames.jsonl \
  --out ./run/results.jsonl

# 4. 逐帧复查归源依据
python3 -I smpipe.py explain-frame --store ./run/store.db \
  --event-id E1 --frame-index 0

# 5. 可复查错误分组
python3 -I smpipe.py groups --store ./run/store.db --out ./run/groups.json

# 6. 访问量/缓存命中证据
python3 -I smpipe.py metrics --store ./run/store.db
```

一条命令完成以上全部步骤（含测试与压力测试）：

```bash
bash rebuild.sh
```

所有 Python 调用均带 `-I`（隔离模式）：不把当前目录、脚本目录、`PYTHONPATH`
与用户 site-packages 纳入导入路径，防止资料目录里被放置的同名模块（例如
`json.py`）被解释器加载。

## 主演练预期结果

| 输入 | 预期 |
| --- | --- |
| `E1/0` R1 (1,1) | 两层映射：压缩 map → 中间 `(0,4)` → 转译 map → 源码 `(2,1)`，外部展示**第 3 行第 2 列**；第一层原列 4 不会泄漏为最终列 |
| `E1/1` R1 (1,9) | 同行第 8 列是**无原位置段**，查第 9 列必须 `unmapped`，不回退到第 0 列段、不借上一行 |
| `E3`(R2) / `E4`(R1) | 同 URL 不同发布的 unmapped，分组按 release + 产物摘要隔离 |
| `E5/0` UTF-16 列 6 与 `E5/1` 码点列 5 | 同一非 BMP 字符 E，最终位置完全一致 `(0,4)` |
| `E6/0` | 落在代理对内部，`invalid_frame / inside_surrogate_pair` |
| `E7/*` R4 sections | 首行加列偏移、后续行不加列偏移、第二 section 行偏移全部正确 |
| `E1` 两帧同位置 + `E2` | 同 event 多帧命中同一最终位置只计一次 event；该组 event_count=2 |

## 状态语义

`mapped` / `unmapped` / `missing_artifact` / `invalid_map` / `cycle`，外加
输入级 `invalid_frame`。注意：

* **缺 map 与缺源码正文是两回事**：没有下一层绑定、也没有正文时，仍可得到
  正确的原位置，状态为 `mapped`、原因 `mapped_source_body_missing`，绝不编造
  代码片段；
* `sourcesContent` 若与清单固定的同身份源码摘要不一致，摄入期直接拒绝；
* 同一生成坐标出现不一致映射 → 拒绝该 map（`inconsistent_mapping`），完全
  相同的映射在唯一索引中折叠；
* `(event_id, frame_index)` 重复且内容不同 → 资料错误 `duplicate_frame_conflict`；
  完全相同的重复输入幂等跳过。

## 分组键

* mapped 组：显式源码身份 + 源码正文摘要 + 最终零基行列 + 业务错误类别；
* 未映射/失败组：release、原始 URL、`artifact_id` 与产物摘要、原始生成坐标，
  因此不同发布的同名 URL 不会混作一个问题。

## 曝光首错事实表（`import-facts` / `export-facts`）

灰度发布中同一浏览器会话可能先看到新版又被切回旧版。本功能在一个**独立的
SQLite 工作库**里持续维护「每条曝光的第一条已归源错误」，归源依据只读一个
已经封盘、永不再变的 `enrich` 归源库（只读 `frames`，不读构建目录、不重新
归源、不写入归源库）。

### 批次格式

一批是一个 JSON 对象，`seq` 从 1 开始连续递增，包含两类变更，均支持整条
写入（默认 `op` 可省略）或按 ID 撤回；所有时间都是非负整数毫秒：

```json
{
  "seq": 3,
  "exposure_changes": [
    {"exposure_id": "EXP-7", "session_id": "sess-1",
     "release_id": "R2", "start_ms": 1000},
    {"exposure_id": "EXP-2", "op": "delete"}
  ],
  "event_changes": [
    {"event_id": "E1", "session_id": "sess-1", "occurred_ms": 1250},
    {"event_id": "E9", "op": "delete"}
  ]
}
```

事件定位可以先于所属曝光到达；匹配不到曝光或没有合格帧都不是坏输入。
撤回事件定位不会删除归源库里的帧。

### 规则要点

* 曝光区间左闭右开 `[start_ms, 下一条 start_ms)`，事件恰在起点归属新曝光；
  同会话起点时刻最终状态唯一，同批合法换位按最终状态生效，不会因中间态失败；
* 事件只在某帧 `release_id` 等于曝光发布且 `status=mapped` 时合格，多帧取
  最小 `frame_index`；不合格不回找前一条发布；
* 每曝光取最小 `(occurred_ms, event_id)`（ID 按字符串比较），事实行含曝光、
  会话、发布、生效/结束时刻、首错事件、帧索引、发生时刻与距开始毫秒数，
  无首错时后四项为空；
* 导入只重算局部受影响曝光，不扫描整库、不全量重建、不重写无关事实行；
  定位/时间轴/事实/批次进度同一事务提交，坏批次不留部分变化；
* 同序号同字节重放为空操作，同序号异内容或跳号拒绝，重开工作库规则不变。

### 复跑步骤（从已有归源库开始）

```bash
cd sourcemap-pipeline

# 0. 归源库需先封盘（checkpoint 掉 WAL），随后视为只读
python3 -I -c "import sqlite3;c=sqlite3.connect('./run/store.db');\
c.execute('PRAGMA wal_checkpoint(TRUNCATE)');c.close()"

# 1. 导入第 1 批（首次需用 --enrich-db 绑定封盘归源库，记录其 SHA-256）
python3 -I smpipe.py import-facts --work-db ./run/facts/work.db \
  --enrich-db ./run/store.db --batch ./path/batch-1.json

# 2. 继续补报/更正；重开后可不传 --enrich-db，复用已记录的绑定
python3 -I smpipe.py import-facts --work-db ./run/facts/work.db \
  --batch ./path/batch-2.json

# 3. 以 JSONL 导出当前事实（按 session、start_ms 稳定排序）
python3 -I smpipe.py export-facts --work-db ./run/facts/work.db \
  --out ./run/facts/facts.jsonl

# 4. 维护计数与查询计划（证明有界索引读、无全表扫描）
python3 -I smpipe.py facts-metrics --work-db ./run/facts/work.db \
  --plan-event E1 --plan-release R1
```

一键小型可复跑演练（自建迷你封盘归源库，覆盖边界事件、补插使首错因发布不
匹配消失、撤回/移动与候补接任、同毫秒多候选、多帧最小索引、同批换位、整批
拒绝、重开重送，并逐批与从零重算的参考实现对拍）：

```bash
python3 -I tools/facts_walkthrough.py --out ./run/facts-demo
```

生成的数据只落在 `run/` 下，不入库到 Git。

## 源码区间反查索引（`build-reverse-index` / `query-reverse-index`）

`enrich` 回答「这个生成坐标对应哪段源码」；反查索引回答相反的问题：
拿到一份源码修复清单后，**所有已发布入口产物里有哪些生成区间**对应某段
源码——哪怕那里至今没有报错。覆盖范围来自整批构建资料，与已有错误分组
无关。

* **入口**：每个登记过 URL 的 `(release_id, artifact_id)`；多个别名只算
  一次，没有别名的中间产物不单独列出（但仍作为跳转目标被遍历）；
* **索引对象**：生成正文里的真实字符起点（不含行尾之后、不含代理对后半
  单元）；同一生成行上连续有效列且最终源码坐标相同就合成一个半开区间，
  不跨空洞、不混不同源码点；
* **归源语义与 `enrich` 完全一致**：入口行上相邻已入库段决定各自覆盖
  （首段之前不归任何段，无原位置段同样截断前段）；后续层只查上层段给出
  的固定原坐标，不加列差；沿用构建库展开后的段、同一发布的显式绑定、
  相同的循环与 32 层上限判定。断链/无映射/循环分支只让自己无命中，不吞
  同产物其他分支；最终源码正文缺失不影响精确坐标命中；
* **区间计算，不按字符**：长压缩行只被少量映射段覆盖时，工作量按覆盖
  区间与共享中间点增长；汇入同一中间坐标的一跳段匹配只解析一次（记忆化
  携带链上签名集与层数，路径相关或超层数预算的结果绝不复用，循环结果
  从不入缓存）；
* **窄查询不扫全量**：命中表以 `(source_identity, src_line, src_col)`
  建索引，查询只触达该源码在该行带内的行（`EXPLAIN QUERY PLAN` 可见
  `SEARCH ... USING INDEX idx_hits_source`），不现场重走映射链。

### 复跑步骤（从 `ingest-build` 到移走原库、重开反查）

```bash
cd sourcemap-pipeline

# 1. 照常摄入构建资料（frames 不是必需的，不必先跑 enrich）
python3 -I smpipe.py ingest-build --store ./run/store.db \
  --manifest ./demo-data/r1/manifest.json

# 2. 封盘（checkpoint 掉 WAL），随后构建库视为只读
python3 -I -c "import sqlite3;c=sqlite3.connect('./run/store.db');\
c.execute('PRAGMA wal_checkpoint(TRUNCATE)');c.close()"

# 3. 从封盘构建库建反查索引（只读读取，不读原构建目录）
python3 -I smpipe.py build-reverse-index \
  --store ./run/store.db --index ./run/reverse.idx

# 4. 查询：精确 source_identity + 源码半开范围（零基行, 零基 UTF-16 列），
#    起止按坐标对先后比较，可跨行；倒置或相等报错 invalid_source_range
python3 -I smpipe.py query-reverse-index --index ./run/reverse.idx \
  --source src/app.ts --start-line 2 --start-col 0 --end-line 2 --end-col 10

# 5. 移走原库与构建目录后，只带索引文件重开反查，结果不变
mv ./run/store.db ./run/store.db.gone
python3 -I smpipe.py query-reverse-index --index ./run/reverse.idx \
  --source src/app.ts --start-line 2 --start-col 0 --end-line 2 --end-col 10

# 6. 需要查询计划/索引计数证据时加 --explain（写到 stderr）
python3 -I smpipe.py query-reverse-index --index ./run/reverse.idx \
  --source src/app.ts --start-line 2 --start-col 0 --end-line 2 --end-col 10 \
  --explain
```

输出为 JSONL（按发布、产物 ID、生成行、起始列稳定排序），每行含发布、
入口产物 ID 及摘要、生成行、生成起止列、最终源码身份及精确行列；范围内
没有命中时输出为空。

一键可复跑演练（手写小构建资料：emoji 行区间合并、多入口汇合、跨发布
绑定、别名去重、入口不同导致的循环差异、断链/无映射分支隔离、缺正文仍
命中；另加运行时生成的 200 万列长行与 4000 个共享中间点的入口段验证
区间级工作量；并与按字符枚举的独立参考实现逐行对拍；最后移走原库重开）：

```bash
python3 -I tools/reverse_walkthrough.py --out ./run/reverse-demo
```

生成的数据只落在 `run/` 下，不入库到 Git。

## 触发操作血缘（`export-lineage`）

一次点击可能启动多项异步工作，若干工作汇合后才上报一条错误。归源帧只能
指出哪段代码报错，回答不了「这条错误是从哪次页面操作一路传下来的」。本
命令把**封盘的 `enrich` 归源库**与同批浏览器异步遥测接起来，离线导出一张
逐事件的触发操作血缘表。血缘只描述观察到的触发链，**不诊断代码错误**；
不读 Source Map、不读曝光事实表、不读反查索引，归源库以 `immutable=1`
只读打开，仅取 `frames` 里已有结果帧的 `event_id` 作为事件全集（一个事件
多帧只算一行，帧未能归源不影响纳入）。

### 遥测格式（无序 JSONL，每行一条）

```json
{"type": "action", "run_id": "r1", "node_id": "click-save"}
{"type": "work", "run_id": "r1", "node_id": "job-2",
 "parents": ["click-save", "job-1"]}
{"type": "error", "event_id": "E7", "run_id": "r1", "node_id": "job-2"}
```

* 节点完整身份是 `(run_id, node_id)`；父边只在同一次运行内解析，**绝不从
  另一运行借同名节点**；
* `parents` 是非空数组、按集合理解：次序无关、重复折叠，等价重送幂等；
* 输入冲突（拒绝整份遥测、不替换输出）：同一节点出现不同定义（含类型
  冲突）、同一事件绑定到不同节点、`error` 指向事件全集之外的 `event_id`；
* 采样缺口（不拒绝，落到行上的缺陷标记）：父节点或上报节点没有定义记录。

### 输出行语义（JSONL，按 `event_id` 字符串升序）

每行含 `event_id`、上报节点的 `run_id`/`node_id`（未绑定为 `null`）、
`reaches_missing`（能否沿父边到达缺失节点）、`reaches_cycle`（能否到达
有向环；环另有出口也照样标记并继续收集可达操作）、`status` 与
`candidates`：可达操作中按 ID 字符串升序的**前两个不同操作**，每个附从
上报节点到该操作、两端都包含的节点 ID 序列（边数最少；同长度取字典序
最小的一条）。同一操作经多个分支到达只算一次。

状态：`no_anchor`（没有 error 记录绑定）；`ambiguous`（已绑定且至少两个
不同候选操作）；`attributed`（恰好一个候选且无缺口、无环）；其余已绑定
事件为 `unresolved`。

### 工作量与原子性

* 一次导出可覆盖大量彼此独立的运行：解析后的遥测、事件全集与结果全部
  暂存在临时 SQLite（退出即删），内存中一次只保留一个运行的图；
* 同一运行里大量错误共享同一片上游关系时，逐节点血缘只算一次：迭代
  Tarjan SCC（O(V+E)）定位有向环，再在缩点 DAG 上按逆拓扑传播，每个节点
  的可达操作集合始终封顶为两个最小 ID——节点及边的处理量随图大小增长，
  不随错误数、可达操作总数或路径组合数成倍增长；复查路径按不同上报节点
  记忆化 BFS 复用；
* 整份输入检查与全部运行分析成功后才经同目录临时文件 + 原子替换写目标，
  尾部坏记录保留原有导出。

```bash
# 归源库需先封盘（checkpoint 掉 WAL），随后视为只读
python3 -I -c "import sqlite3;c=sqlite3.connect('./run/store.db');\
c.execute('PRAGMA wal_checkpoint(TRUNCATE)');c.close()"

python3 -I smpipe.py export-lineage \
  --enrich-db ./run/store.db \
  --telemetry ./path/async-telemetry.jsonl \
  --out ./run/lineage.jsonl
```

一键可复跑演练（迷你封盘归源库 + 手写遥测：双分支汇合仍为单一候选、
等长路径稳定选定、更近但 ID 排第三的操作不挤占前两名、带出口的环与
缺失父节点并存、另一运行同名节点隔离、无绑定/多帧/尾行冲突/重排重送、
逐行与朴素 DFS/BFS 独立参考实现对拍；另用运行时生成的共享上游扇入
证明图处理计数器不随错误数增长）：

```bash
python3 -I tools/lineage_walkthrough.py --out ./run/lineage-demo
```

生成的数据只落在 `run/` 下，不入库到 Git。

## 性能与内存（实测，见 `rebuild.sh` 输出）

* 1,000,000 段 map：约 11 秒入库，库文件约 270 MiB；
* 5,000 次随机点查约 5 秒（~1k qps），每次一条索引 SQL，不整表解码；
* 峰值 RSS 约 150 MiB，低于 256 MiB 预算；
* 50,000 条完全重复帧：1 条新增、49,999 条幂等跳过；
* 稀疏小 map 行走有界 LRU 行缓存（命中计数见 metrics），密集大 map 行走
  B-tree 点查，避免把超长行整体物化为 Python 对象。

## 清单格式

见 `demo-data/r1/manifest.json`。要点：

* `artifacts[].sha256` 固定产物字节，`url_aliases` 显式列出 URL 别名，
  `source_map` 以路径 + 摘要绑定 map；
* 中间 source 的下一跳必须写在 `source_bindings`，按 map 内解析后的
  `source_identity`（或 source_index）显式绑定到**同一 release** 的下一个
  artifact，禁止凭同名路径猜测；
* `sources[]` 固定终态源码正文摘要，用于片段展示与 sourcesContent 核对。
