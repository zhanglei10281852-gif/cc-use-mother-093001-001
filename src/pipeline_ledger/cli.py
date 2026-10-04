"""命令行查看/处置工具。

示例：
  python -m pipeline_ledger.cli init-db
  python -m pipeline_ledger.cli submit FILE            # FILE 为批次 JSON
  python -m pipeline_ledger.cli conflicts [--status open] [--road ROAD-8]
  python -m pipeline_ledger.cli conflict CONFLICT_ID
  python -m pipeline_ledger.cli decide CONFLICT_ID accept|reject|merge \
      --reviewer reviewer-li --rationale "..." [--expected-version N] [--merged FILE]
  python -m pipeline_ledger.cli effective ROAD-8 [--as-of 2026-09-20T00:00:00Z]
  python -m pipeline_ledger.cli history ROAD-8 [--as-of ...]
  python -m pipeline_ledger.cli versions A-17
  python -m pipeline_ledger.cli diff A-17 --from 1 --to 2
  python -m pipeline_ledger.cli audit [--limit 30]
  python -m pipeline_ledger.cli verify

环境变量 LEDGER_DB 指定数据库路径（默认 ledger.db）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .service import (
    DEFAULT_REVIEWERS,
    ConflictStateError,
    DuplicateBatchError,
    LedgerError,
    LedgerService,
    NotFoundError,
    PermissionDeniedError,
)
from .store import LedgerStore


def open_service(db_path: str) -> LedgerService:
    fresh = not Path(db_path).exists()
    store = LedgerStore(db_path, bootstrap_reviewers=DEFAULT_REVIEWERS if fresh else None)
    return LedgerService(store)


def print_json(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _status_cn(status: str) -> str:
    return {
        "open": "未决",
        "accepted": "已采信",
        "rejected": "已驳回",
        "merged": "已合并",
        "spatial_overlap": "重叠线段",
        "time_inversion": "时间倒置",
        "attr_contradiction": "属性矛盾",
    }.get(status, status)


def render_effective(data: dict) -> str:
    lines = [f"道路 {data['road']} 截至 {data['as_of']} 的有效管线："]
    if not data["pipelines"]:
        lines.append("  （无）")
    for p in data["pipelines"]:
        lines.append(
            f"  - [{p['utility_type']}] {p['asset_id']} v{p['version_no']} "
            f"{p['segment']} 埋深 {p['burial_depth_m']}m 状态 {p['status']} "
            f"生效于 {p['effective_from']}"
        )
        lines.append(f"      来源批次：{', '.join(p['origin_batch_ids'])}")
    return "\n".join(lines)


def render_history(data: dict) -> str:
    lines = [f"道路 {data['road']} 截至 {data['as_of']} 的底账还原："]
    if not data["pipelines"]:
        lines.append("  （无）")
    for item in data["pipelines"]:
        v = item["effective_version"]
        lines.append(
            f"  ● [{v['utility_type']}] {v['asset_id']} v{v['version_no']} "
            f"{v['segment']} 埋深 {v['burial_depth_m']}m {v['status']}"
        )
        lines.append("    证据：")
        for ev in item["evidence_chain"]:
            r, b = ev["raw_record"], ev["source_batch"]
            lines.append(
                f"      - 记录#{r['record_id']} 来自批次 {b['batch_id']}"
                f"（{b['owner']}，提交 {b['submitted_at']}，hash {b['raw_hash'][:12]}…）"
            )
            lines.append(
                f"        勘测 {r['surveyed_at']}｜{r['segment']}｜"
                f"埋深 {r['burial_depth_m']}m｜{r['status']}"
            )
            for c in ev["conflicts"]:
                tail = ""
                if c["decision"]:
                    d = c["decision"]
                    tail = (
                        f" → {_status_cn(d['action'])} by {d['reviewer']} "
                        f"at {d['decided_at']}（{d['rationale']}）"
                    )
                lines.append(
                    f"        冲突 {c['conflict_id']} [{_status_cn(c['type'])}] "
                    f"状态 {_status_cn(c['status'])}{tail}"
                )
        lines.append("    责任链：")
        for step in item["responsibility_chain"]:
            lines.append(
                f"      - {step['at']} {step['stage']}：{step['actor']} "
                f"（{step['reference']}）{step['note']}"
            )
    return "\n".join(lines)


def render_conflicts(items: list[dict]) -> str:
    if not items:
        return "没有符合条件的冲突。"
    lines = []
    for c in items:
        lines.append(
            f"{c['conflict_id']}  [{_status_cn(c['conflict_type'])}] "
            f"状态={_status_cn(c['status'])} 版本号={c['row_version']} 道路={c['road']}"
        )
        lines.append(f"    {c['detail']['reason']}")
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    # 子解析器里的同名选项默认 SUPPRESS：只在用户于子命令后再次给出时覆盖，
    # 否则保留主解析器上 --db/--json 的值（兼容两种书写位置）。
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=os.environ.get("LEDGER_DB", "ledger.db"))
    common.add_argument("--json", action="store_true", help="以 JSON 输出")

    sub_common = argparse.ArgumentParser(add_help=False)
    sub_common.add_argument("--db", default=argparse.SUPPRESS)
    sub_common.add_argument("--json", action="store_true", default=argparse.SUPPRESS,
                           help="以 JSON 输出")

    parser = argparse.ArgumentParser(prog="pipeline-ledger", description="地下管线权威底账 CLI",
                                     parents=[common])
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db", parents=[sub_common], help="初始化数据库（含默认审核人）")

    p = sub.add_parser("submit", parents=[sub_common], help="提交来源批次 JSON 文件（- 表示 stdin）")
    p.add_argument("file")

    p = sub.add_parser("conflicts", parents=[sub_common], help="列出冲突")
    p.add_argument("--status", choices=["open", "accepted", "rejected", "merged"])
    p.add_argument("--road")

    p = sub.add_parser("conflict", parents=[sub_common], help="冲突详情")
    p.add_argument("conflict_id")

    p = sub.add_parser("decide", parents=[sub_common], help="采信/驳回/合并冲突")
    p.add_argument("conflict_id")
    p.add_argument("action", choices=["accept", "reject", "merge"])
    p.add_argument("--reviewer", required=True)
    p.add_argument("--rationale", required=True)
    p.add_argument("--expected-version", type=int)
    p.add_argument("--merged", help="合并值 JSON 文件（merge 时必填）")
    p.add_argument("--decided-at")

    p = sub.add_parser("effective", parents=[sub_common], help="道路在某时点的有效管线")
    p.add_argument("road")
    p.add_argument("--as-of")

    p = sub.add_parser("history", parents=[sub_common], help="道路在某时点的资料/证据/责任链还原")
    p.add_argument("road")
    p.add_argument("--as-of")

    p = sub.add_parser("versions", parents=[sub_common], help="资产版本时间线")
    p.add_argument("asset_id")

    p = sub.add_parser("diff", parents=[sub_common], help="资产版本差异")
    p.add_argument("asset_id")
    p.add_argument("--from", dest="v_from", type=int, required=True)
    p.add_argument("--to", dest="v_to", type=int, required=True)

    p = sub.add_parser("audit", parents=[sub_common], help="审计哈希链")
    p.add_argument("--limit", type=int, default=50)

    sub.add_parser("verify", parents=[sub_common], help="校验审计链完整性")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    service = open_service(args.db)
    try:
        return _run(args, service)
    except (NotFoundError, DuplicateBatchError, ConflictStateError,
            PermissionDeniedError, LedgerError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 1


def _run(args: argparse.Namespace, service: LedgerService) -> int:
    cmd = args.cmd
    if cmd == "init-db":
        print(f"数据库已就绪：{args.db}（审核人：{', '.join(DEFAULT_REVIEWERS)}）")
        return 0

    if cmd == "submit":
        raw = sys.stdin.read() if args.file == "-" else Path(args.file).read_text(encoding="utf-8")
        payload = json.loads(raw)
        result = service.submit_batch(payload)
        if args.json:
            print_json(result)
        else:
            print(
                f"批次 {result['batch_id']}（{result['owner']}）已入账，"
                f"{len(result['records'])} 条原始记录已不可变留存；"
                f"发现 {len(result['new_conflicts'])} 个新冲突。"
            )
            for c in result["new_conflicts"]:
                print(f"  ! {c['conflict_id']} [{_status_cn(c['conflict_type'])}]")
        return 0

    if cmd == "conflicts":
        items = service.conflicts(args.status, args.road)
        print_json({"conflicts": items}) if args.json else print(render_conflicts(items))
        return 0

    if cmd == "conflict":
        print_json(service.conflict(args.conflict_id))
        return 0

    if cmd == "decide":
        merged = None
        if args.merged:
            merged = json.loads(Path(args.merged).read_text(encoding="utf-8"))
        result = service.decide(
            args.conflict_id,
            action=args.action,
            reviewer=args.reviewer,
            rationale=args.rationale,
            expected_version=args.expected_version,
            merged=merged,
            decided_at=args.decided_at,
        )
        print_json(result) if args.json else print(
            f"决定 {result['decision_id']} 已提交：{_status_cn(result['conflict_status'])}，"
            f"冲突行版本推进至 {result['conflict_row_version']}"
            + (f"，新生效版本 #{result['resulting_version_id']}"
               if result["resulting_version_id"] else "，未产生新版本（维持现行有效版本）")
        )
        return 0

    if cmd == "effective":
        data = service.effective(args.road, args.as_of)
        print_json(data) if args.json else print(render_effective(data))
        return 0

    if cmd == "history":
        data = service.history(args.road, args.as_of)
        print_json(data) if args.json else print(render_history(data))
        return 0

    if cmd == "versions":
        print_json({"versions": service.asset_versions(args.asset_id)})
        return 0

    if cmd == "diff":
        data = service.version_diff(args.asset_id, args.v_from, args.v_to)
        if args.json:
            print_json(data)
        elif not data["changes"]:
            print(f"{args.asset_id} v{args.v_from} 与 v{args.v_to} 无字段差异。")
        else:
            for ch in data["changes"]:
                print(f"{ch['label']}（{ch['field']}）：{ch['from']} → {ch['to']}")
        return 0

    if cmd == "audit":
        print_json({"entries": service.audit(args.limit)})
        return 0

    if cmd == "verify":
        result = service.verify()
        print_json(result)
        return 0 if result["ok"] else 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
