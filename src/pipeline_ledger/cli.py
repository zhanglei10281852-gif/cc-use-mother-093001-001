"""命令行：查看道路时点视图、冲突、审核决定、版本差异、批次提交等。

示例：
  python -m pipeline_ledger.cli --db ledger.db seed
  python -m pipeline_ledger.cli --db ledger.db road ROAD-8 --at 2026-09-10T00:00:00Z
  python -m pipeline_ledger.cli --db ledger.db conflicts
  python -m pipeline_ledger.cli --db ledger.db resolve C-xxxx accept --winner 5 --revision 1 --token TOK
  python -m pipeline_ledger.cli --db ledger.db diff 5 6
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from .errors import LedgerError
from .service import LedgerService


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _version_line(v: dict[str, Any]) -> str:
    return (
        f"  #{v['version_id']:<4} {v['utility']:<8} {v['road']}:{v['seg_start']}-{v['seg_end']}m"
        f"  埋深{v['burial_depth_m']}m  状态={v['status']}  处置={v['outcome']}"
        f"  批次={v['batch_id'] or '(合并)'}"
    )


def cmd_road(svc: LedgerService, args: argparse.Namespace) -> None:
    view = svc.road_view(args.road, args.at)
    print(f"道路 {view['road']} 在 {view['at']} 的有效管线：")
    if not view["effective_pipelines"]:
        print("  （无）")
    for p in view["effective_pipelines"]:
        print(_version_line(p))
        b = view["source_evidence"].get(p["batch_id"] or "", {})
        print(f"        证据: 批次={p['batch_id'] or '(合并产物)'} 权属={b.get('owner', '')}"
              f" 提交于={b.get('submitted_at', '')} 提交人={b.get('submitted_by', '')}"
              f" 载荷哈希={b.get('payload_hash', '')[:16]}")
        print(f"        记录哈希={p['record_hash'][:24]}… 前序={p['prev_record_hash'][:24]}…")
    print(f"\n冲突处置（{len(view['conflicts'])}）：")
    for c in view["conflicts"]:
        state = c["status"].upper()
        who = f" 决定人={c['decided_by']} 动作={c['action']}" if c["status"] == "resolved" else ""
        print(f"  [{state}] {c['conflict_id']} r{c['revision']} 类型={','.join(c['kinds'])}"
              f" 候选={c['candidate_ids']}{who}")
        for d in c["details"]:
            print(f"      - {json.dumps(d, ensure_ascii=False)}")


def cmd_conflicts(svc: LedgerService, args: argparse.Namespace) -> None:
    items = svc.list_conflicts(status=None if args.all else "open", road=args.road)
    for c in items:
        print(f"{c['conflict_id']}  [{c['status']}] r{c['revision']}  道路={c['road']}"
              f"  类型={','.join(c['kinds'])}  候选={c['candidate_ids']}")
        if args.verbose:
            print(json.dumps(c["details"], ensure_ascii=False, indent=2))
    if not items:
        print("（无未决冲突）" if not args.all else "（无冲突）")


def cmd_conflict(svc: LedgerService, args: argparse.Namespace) -> None:
    _print(svc.get_conflict(args.conflict_id))


def cmd_resolve(svc: LedgerService, args: argparse.Namespace) -> None:
    actor = svc.authenticate(args.token)
    merged = None
    if args.merge_json:
        merged = json.loads(args.merge_json)
    result = svc.resolve_conflict(
        conflict_id=args.conflict_id,
        actor=actor,
        action=args.action,
        expected_revision=args.revision,
        winner_version_id=args.winner,
        merged_fields=merged,
        note=args.note,
    )
    print(f"冲突 {result['conflict_id']} 已处置 -> r{result['revision']} {result['action']} by {result['decided_by']}")


def cmd_history(svc: LedgerService, args: argparse.Namespace) -> None:
    keys = svc.find_asset_key(args.label) if args.by_label else [args.asset_key]
    for key in keys:
        h = svc.asset_history(key)
        print(f"资产 {key} 版本链：")
        for v in h["versions"]:
            print(_version_line(v))
            print(f"      生效={v['effective_from']} ~ {v['effective_to'] or '至今'}"
                  f" 冲突={v['decision_conflict_id'] or '-'} 更正人={v['corrected_by'] or '-'}")
        for d in h["successive_diffs"]:
            print(f"  v{d['from_version']} -> v{d['to_version']} 差异：")
            for ch in d["changes"]:
                print(f"      {ch['field']}: {ch['from']!r} -> {ch['to']!r}")


def cmd_diff(svc: LedgerService, args: argparse.Namespace) -> None:
    d = svc.diff_versions(args.a, args.b)
    print(f"版本 {args.a} -> {args.b} 差异：")
    for ch in d["changes"]:
        print(f"  {ch['field']}: {ch['from']!r} -> {ch['to']!r}")
    print(f"哈希: {d['record_hashes']['from'][:20]}… -> {d['record_hashes']['to'][:20]}…")


def cmd_version(svc: LedgerService, args: argparse.Namespace) -> None:
    _print(svc.get_version(args.version_id))


def cmd_batches(svc: LedgerService, args: argparse.Namespace) -> None:
    for b in svc.list_batches():
        print(f"{b['batch_id']}  权属={b['owner']}  提交于={b['submitted_at']}"
              f"  提交人={b['submitted_by']}  hash={b['payload_hash'][:16]}")


def cmd_submit(svc: LedgerService, args: argparse.Namespace) -> None:
    with open(args.file, encoding="utf-8") as f:
        body = json.load(f)
    actor = None
    if args.token:
        actor = svc.authenticate(args.token)
    result = svc.submit_batch(
        batch_id=body["batch_id"],
        owner=body.get("owner", "unknown"),
        records=body["records"],
        submitted_by=actor["reviewer_id"] if actor else body.get("submitted_by", "cli"),
    )
    _print(result)


def cmd_register(svc: LedgerService, args: argparse.Namespace) -> None:
    _print(svc.register_reviewer(args.reviewer_id, args.display_name, args.role))


def cmd_audit(svc: LedgerService, args: argparse.Namespace) -> None:
    for e in svc.audit_trail(limit=args.limit):
        print(f"{e['seq']:>4} {e['at']} {e['actor']:<12} {e['action']:<22} {e['target']}")


def cmd_verify(svc: LedgerService, args: argparse.Namespace) -> None:
    result = svc.verify_chain()
    _print(result)
    if not result["audit_chain_intact"] or result["broken_version_ids"]:
        sys.exit(1)


def cmd_seed(svc: LedgerService, args: argparse.Namespace) -> None:
    """内置演示场景：供水/燃气/通信三方在 ROAD-8 上互相冲突。"""
    demo = [
        ("SURVEY-2026-09", "water", [
            {"asset_id": "WP-101", "segment_ref": "ROAD-8:0-60", "burial_depth_m": 1.20,
             "status": "in_service", "operated_from": "2018-05-01T00:00:00Z",
             "material": "PE", "diameter_mm": 300},
        ]),
        ("GAS-2026-09", "gas", [
            {"asset_id": "GP-77", "segment_ref": "ROAD-8:20-80", "burial_depth_m": 1.30,
             "status": "in_service", "operated_from": "2020-01-01T00:00:00Z",
             "material": "steel", "diameter_mm": 200},
        ]),
        ("TELECOM-2026-09", "telecom", [
            {"asset_id": "TC-55", "segment_ref": "ROAD-8:10-50", "burial_depth_m": 0.80,
             "status": "planned", "material": "PVC", "diameter_mm": 100},
        ]),
        ("WATER-CORRECTION-10", "water", [
            # 供水单位更正：埋深 1.80、退役时间与现状矛盾 => 时间倒置/属性冲突
            {"asset_id": "WP-101", "segment_ref": "ROAD-8:0-60", "burial_depth_m": 1.80,
             "status": "out_of_service", "operated_from": "2018-05-01T00:00:00Z",
             "operated_to": "2017-01-01T00:00:00Z",
             "material": "PE", "diameter_mm": 300},
        ]),
    ]
    created: dict[str, str] = {}
    for bid, owner, records in demo:
        r = svc.submit_batch(bid, owner, records, submitted_by="seed")
        print(f"批次 {bid}: 新版本 {r['new_versions']} 复用 {r['reused_versions']} 去重={r['deduplicated']}")
    if args.with_reviewer:
        rev = svc.register_reviewer("reviewer-1", "张工（审核）", "reviewer")
        created["reviewer_token"] = rev["token"]
        print(f"审核人令牌（仅显示一次）: {rev['token']}")
    _print(created if created else {"seeded": True})


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pipeline_ledger", description="地下管线权威底账 CLI")
    p.add_argument("--db", default="ledger.db", help="SQLite 数据库路径")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("seed", help="写入演示数据")
    sp.add_argument("--with-reviewer", action="store_true")
    sp.set_defaults(func=cmd_seed)

    sp = sub.add_parser("register", help="注册审核人")
    sp.add_argument("reviewer_id")
    sp.add_argument("display_name")
    sp.add_argument("--role", default="reviewer")
    sp.set_defaults(func=cmd_register)

    sp = sub.add_parser("submit", help="从 JSON 文件提交批次")
    sp.add_argument("file")
    sp.add_argument("--token")
    sp.set_defaults(func=cmd_submit)

    sp = sub.add_parser("batches", help="批次清单")
    sp.set_defaults(func=cmd_batches)

    sp = sub.add_parser("conflicts", help="冲突清单")
    sp.add_argument("--all", action="store_true")
    sp.add_argument("--road")
    sp.add_argument("-v", "--verbose", action="store_true")
    sp.set_defaults(func=cmd_conflicts)

    sp = sub.add_parser("conflict", help="冲突详情")
    sp.add_argument("conflict_id")
    sp.set_defaults(func=cmd_conflict)

    sp = sub.add_parser("resolve", help="审核：accept/reject/merge")
    sp.add_argument("conflict_id")
    sp.add_argument("action", choices=["accept", "reject", "merge"])
    sp.add_argument("--revision", type=int, required=True, help="客户端所见的 conflict.revision")
    sp.add_argument("--winner", type=int, help="accept 时采信的候选版本 id")
    sp.add_argument("--merge-json", help="merge 时覆盖字段的 JSON 字符串/文件路径(@file)")
    sp.add_argument("--note")
    sp.add_argument("--token", required=True)
    sp.set_defaults(func=cmd_resolve)

    sp = sub.add_parser("road", help="指定道路某一时点视图")
    sp.add_argument("road")
    sp.add_argument("--at", help="ISO8601 时点，默认当前")
    sp.set_defaults(func=cmd_road)

    sp = sub.add_parser("history", help="资产版本链与差异")
    sp.add_argument("asset_key")
    sp.add_argument("--by-label", action="store_true", help="参数为提交方资产编号而非 asset_key")
    sp.set_defaults(func=cmd_history)

    sp = sub.add_parser("version", help="版本详情")
    sp.add_argument("version_id", type=int)
    sp.set_defaults(func=cmd_version)

    sp = sub.add_parser("diff", help="两版本差异")
    sp.add_argument("a", type=int)
    sp.add_argument("b", type=int)
    sp.set_defaults(func=cmd_diff)

    sp = sub.add_parser("audit", help="审计责任链")
    sp.add_argument("--limit", type=int, default=100)
    sp.set_defaults(func=cmd_audit)

    sp = sub.add_parser("verify", help="哈希链自检")
    sp.set_defaults(func=cmd_verify)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    svc = LedgerService(args.db)
    try:
        args.func(svc, args)
    except LedgerError as exc:
        print(f"错误[{type(exc).__name__}]: {exc}", file=sys.stderr)
        return 2
    finally:
        svc.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
