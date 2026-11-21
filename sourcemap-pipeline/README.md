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
  cli.py         命令行入口
tools/
  generate_fixtures.py  手写两层 map 等全部演练资料生成器
  stress_test.py        百万段 map + 五万重复帧压力测试与访问量记录
tests/
  reference_decoder.py  独立风格的第二份 VLQ 解码器（仅测试交叉核对）
  test_pipeline.py      42 条实际断言
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
