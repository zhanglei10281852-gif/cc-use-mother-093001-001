# 地下管线权威底账协同服务

面向市级测绘中心的地下管线**权威底账（authoritative ledger）**：各权属单位（供水、燃气、
通信）按来源批次提交资产与勘测记录，系统留存不可变原始版本，对**重叠线段、时间倒置、
属性矛盾**给出可解释的冲突，由有权限的审核人完成**采信 / 驳回 / 合并**；任何更正都形成
新的生效版本，历史查询可还原当时采用的资料、证据与责任链。

仅依赖 Python 3.10+ 标准库（SQLite 持久化、`http.server` 提供 HTTP）。

## 核心保证

| 需求 | 实现方式 |
| --- | --- |
| 原始资料不可变 | `source_batches.raw_payload/raw_hash` 与 `raw_records` 只增不改；每条记录内容做 SHA-256，跨批次完全重复的记录被 `UNIQUE(content_hash)` 拒绝 |
| 重复批次不产生两份资产 | 批次号唯一；批内资产唯一；同内容记录唯一；跨资产重叠经采信后落败方版本退出 |
| 可解释冲突 | 检测三类：`spatial_overlap`（同类型同道路线段重叠）、`attr_contradiction`（埋深超 0.10m 容差 / 投运状态矛盾 / 同资产分段不一致）、`time_inversion`（同址记录勘测时间与“拟建→在役→退役”生命周期矛盾）。每个 `detail.reason` 给出双方实测值与判定依据 |
| 有权限审核 | `reviewers` 白名单，非授权人处置返回 403 |
| 并发不覆盖较新决定 | 冲突带 `row_version`，处置以 `expected_version` 乐观锁 + `WHERE status='open' AND row_version=?` 条件更新，败者整体回滚 |
| 更正形成新生效版本 | 采信/合并都向 `effective_versions` 追加版本（`version_no` 递增、旧版本 `superseded_at` 失效），从不原地改 |
| 历史还原 | 双时态：按 `effective_from <= as_of < superseded_at` 还原任一时点有效管线；`history` 输出来源批次原文哈希、原始记录、冲突处置、提交人/审核人责任链 |
| 重启后可继续处理 | 全部状态在 SQLite（WAL），未决 `open` 冲突与版本链持久化，重开服务/进程即可继续处置 |
| 责任链防篡改 | `audit_log` 为 SHA-256 哈希链（含 `prev_hash`），`verify` 可检出任何行被改动 |

## 目录结构

```
src/pipeline_ledger/
  contracts.py   领域契约：来源批次、原始记录、冲突/决定枚举
  geometry.py    道路桩号分段解析与重叠/容差几何
  store.py       SQLite 存储 + 冲突检测 + 版本/乐观锁/审计哈希链（核心）
  service.py     HTTP 与 CLI 共用的用例编排
  http_api.py    HTTP 接口（标准库 ThreadingHTTPServer）
  cli.py         命令行
  serve.py       HTTP 服务启动入口
examples/        供水/燃气/通信三方批次与合并决定示例
tests/           34 个单元 + HTTP 端到端测试
```

## 运行

```bash
# 测试
python -m unittest discover -s tests -v
# 编译检查
python -m compileall -q src tests run_cli.py
# 脚手架冒烟
python run_cli.py
```

### CLI 快速上手

```bash
export PYTHONPATH=src
python -m pipeline_ledger.cli init-db                       # 初始化（含两名默认审核人）
python -m pipeline_ledger.cli submit examples/batch_water.json
python -m pipeline_ledger.cli submit examples/batch_gas.json
python -m pipeline_ledger.cli submit examples/batch_comm.json

python -m pipeline_ledger.cli conflicts --status open       # 看未决冲突（含解释）
python -m pipeline_ledger.cli conflict attr_contradiction:1-3

# 合并：给出三方复核值；--expected-version 为读取到的冲突 row_version
python -m pipeline_ledger.cli decide attr_contradiction:1-3 merge \
  --reviewer reviewer-li --rationale "现场开挖复核，取2.38m" \
  --expected-version 1 --merged examples/merge_decision.json
# 采信 / 驳回
python -m pipeline_ledger.cli decide spatial_overlap:1-3 accept \
  --reviewer reviewer-li --rationale "位置以复核为准"
python -m pipeline_ledger.cli decide spatial_overlap:1-5 reject \
  --reviewer reviewer-wang --rationale "重复资料不另立版本"

python -m pipeline_ledger.cli effective ROAD-8                       # 当前有效管线
python -m pipeline_ledger.cli effective ROAD-8 --as-of 2026-09-02T12:00:00Z
python -m pipeline_ledger.cli history   ROAD-8                       # 证据+责任链还原
python -m pipeline_ledger.cli versions  W-101                         # 版本时间线
python -m pipeline_ledger.cli diff      W-101 --from 1 --to 2        # 版本差异
python -m pipeline_ledger.cli audit --limit 50
python -m pipeline_ledger.cli verify                                  # 校验审计哈希链
```

数据库路径可用 `--db` 或环境变量 `LEDGER_DB` 指定；任何命令加 `--json` 输出机器可读 JSON
（`--json` 放在子命令前后均可）。

### HTTP 接口

```bash
python -m pipeline_ledger.serve --host 127.0.0.1 --port 8080
```

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches` | 提交来源批次 |
| GET  | `/batches/{batch_id}` | 批次原文、记录与哈希 |
| GET  | `/conflicts?status=&road=` | 冲突清单 |
| GET  | `/conflicts/{id}` | 冲突详情（含处置决定） |
| POST | `/conflicts/{id}/decisions` | `accept`/`reject`/`merge`，体含 `reviewer`、`rationale`、`expected_version`、`merged` |
| GET  | `/roads/{road}/effective?as_of=` | 某时点有效管线 |
| GET  | `/roads/{road}/history?as_of=` | 当时资料、证据与责任链 |
| GET  | `/assets/{id}/versions` | 资产版本时间线 |
| GET  | `/assets/{id}/diff?from=&to=` | 版本字段差异 |
| GET  | `/audit?limit=` | 审计哈希链 |
| POST | `/audit/verify` | 校验链完整性 |
| GET  | `/health` | 健康检查 |

错误语义：400 业务/格式错误，403 无权限审核人，404 实体不存在，409 重复批次或冲突状态/
乐观锁冲突。响应统一为 `{"ok": bool, "data"|"error": ...}`。

合并请求示例：

```json
POST /conflicts/attr_contradiction:1-3/decisions
{
  "action": "merge",
  "reviewer": "reviewer-li",
  "rationale": "现场开挖复核，结合三方资料取 2.38m",
  "expected_version": 1,
  "merged": {"segment": "ROAD-8:20-80", "burial_depth_m": 2.38,
             "status": "in_service", "surveyed_at": "2026-09-04T00:00:00Z"}
}
```

## 数据模型要点

- **source_batches**：批次元数据 + 原始 JSON 原文 + 原文哈希。
- **raw_records**：规范化的不可变原始记录，含道路桩号、埋深、投运状态、勘测时点、
  扩展属性、备注与内容哈希。
- **effective_versions**：每资产至多一个 `active=1` 版本（部分唯一索引保证）；每次采信/
  合并追加新版本并作废旧版本；`origin_record_ids/origin_batch_ids` 指向采信证据，合并会
  继承上一版本的全部来源。
- **conflicts**：冲突双方记录、类型、人类可读 `detail`、状态、`row_version`。
- **decisions**：处置动作、审核人、理由、合并值快照、产生的版本、提交时的期望行版本。
- **audit_log**：哈希链，串联提交、入账、生效、建冲突、处置等事件。

默认审核人（仅演示用，可在 `service.DEFAULT_REVIEWERS` 调整）：`reviewer-li`、
`reviewer-wang`。
