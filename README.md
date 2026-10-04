# 地下管线权威底账服务

面向市级测绘中心的地下管线**权威底账（system of record）**：各权属单位（供水、燃气、
通信等）按**来源批次**提交资产与勘测记录，系统保留不可变原始版本，对重叠线段、时间
倒置、属性矛盾给出可解释冲突，由有权限的审核人采信/驳回/合并；任何更正形成新生效版本，
历史查询可还原某一时点采用的资料与责任链。

仅依赖 Python 3.10+ 标准库（SQLite + http.server），无第三方依赖。

## 核心能力与对应实现

| 需求 | 实现 |
| --- | --- |
| 按来源批次提交，原始版本不可变 | `source_batches` 保存原始载荷与哈希；`asset_versions` 数据列只插入、永不更新 |
| 重复批次不产生两份资产 | 资产身份 = `权属|提交方资产编号`；同批次重发幂等；跨批次同内容复用同一版本链 |
| 重叠/时间倒置/属性矛盾可解释 | `detect.findings_for_pair`，每条冲突附实际取值（重叠米数、埋深差、互斥字段） |
| 审核人采信/驳回/合并 | Bearer 令牌 + `reviewer/admin` 角色；`POST /conflicts/{id}/resolve` |
| 更正形成新生效版本 | 同 `asset_key` 新版本 `version_no+1`，`supersedes_version_id` 链接旧版，待审核后生效 |
| 审核并发不覆盖较新决定 | 冲突带 `revision` 乐观锁 + `BEGIN IMMEDIATE`，过期修订返回 409 |
| 重启后未决冲突可继续处理 | 冲突、版本、审计全部持久化于 SQLite |
| 时点回放 + 证据 + 处置 + 差异 | `GET /roads/{road}?at=`、`GET /versions/{a}/diff/{b}`、哈希责任链 |
| 责任链 | 版本哈希链（`prev_record_hash`）与审计日志哈希链（`chain_hash`），`/verify` 自检 |

## 运行

```bash
# 测试
python -m unittest discover -s tests -v        # 25 个测试
python -m compileall -q src tests run_cli.py
python run_cli.py                              # 冒烟

# HTTP 服务
PYTHONPATH=src python -m pipeline_ledger.httpapi --db ledger.db --port 8080

# CLI（先种入供水/燃气/通信冲突演示数据）
PYTHONPATH=src python -m pipeline_ledger.cli --db ledger.db seed --with-reviewer
```

## HTTP 接口

```
GET  /health
POST /reviewers                       {"reviewer_id","display_name",role?,token?} -> 一次性令牌
GET  /reviewers/me                    Bearer 令牌自检
POST /batches                         {"batch_id","owner","records":[...]} 幂等
GET  /batches / /batches/{id}         批次清单 / 含不可变原始载荷
GET  /conflicts?status=open|all&road=
GET  /conflicts/{id}                  候选版本、判定细节、事件链
POST /conflicts/{id}/resolve          Bearer；{action,expected_revision,winner_version_id?,merged_fields?,note?}
GET  /roads/{road}?at=ISO             时点有效管线 + 来源证据 + 冲突处置
GET  /assets/by-label/{label}
GET  /assets/{asset_key}/history
GET  /versions/{id} / /versions/{a}/diff/{b}
GET  /audit  ·  GET /verify
```

记录字段：`asset_id`、`segment_ref`（`道路:起-止` 米，也可用 `road/from_m/to_m`）、
`burial_depth_m`、`status`（in_service/out_of_service/planned/unknown，支持中文别名）、
`operated_from/operated_to`、`material`、`diameter_mm`、`attributes`。

## 冲突规则

- **overlap（重叠）**：同道路里程重叠 ≥1m。不同权属且埋深差 ≤0.30m 判为空间碰撞；
  同权属重叠直接成立。
- **attribute（属性矛盾）**：同权属重叠且埋深差 >0.30m、材质/管径不一致、
  或在役/退役/规划状态互斥。
- **time_inversion（时间倒置）**：记录自身投运起始晚于终止（原始照存、挂起待审），
  或互斥状态双方主张的寿命窗口互不衔接。
- **correction（更正待审）**：同一资产出现新版本，需审核决定新版本生效或驳回；
  采信后整条祖先链失效（旧版保留、标 superseded 并记 effective_to）。

审核动作：`accept`（指定 `winner_version_id`）、`reject`、`merge`（以
`merged_fields` 生成新的权威版本，`attributes._merged_from` 留存双方出处）。
候选失效的未决冲突由系统自动标记 `obsoleted`。

## CLI 示例

```bash
python -m pipeline_ledger.cli --db ledger.db road ROAD-8 --at 2026-09-01T00:00:00Z
python -m pipeline_ledger.cli --db ledger.db conflicts -v
python -m pipeline_ledger.cli --db ledger.db resolve C-xxxx accept --revision 1 --winner 5 --token TOK
python -m pipeline_ledger.cli --db ledger.db resolve C-yyyy merge  --revision 1 --token TOK \
    --merge-json '{"burial_depth_m":1.5,"segment_ref":"ROAD-8:20-60"}'
python -m pipeline_ledger.cli --db ledger.db history water|WP-101
python -m pipeline_ledger.cli --db ledger.db diff 1 4
python -m pipeline_ledger.cli --db ledger.db verify
```
