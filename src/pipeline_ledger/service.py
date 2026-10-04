"""地下管线权威底账核心服务。

设计要点：
- 原始记录不可变：asset_versions 的数据列只插入、永不更新；
  outcome/effective_* 是生命周期列，记录“何时起/止被采信”。
- 哈希责任链：每条记录版本带 record_hash（含上一版本哈希），审计日志带 chain_hash。
- 去重：资产身份为 ``权属|资产编号``；同批次重复提交、不同批次同内容，均不产生第二份资产。
- 冲突持久化在 SQLite 中，服务重启后 open 冲突可继续处置。
- 审核用 conflict.revision 乐观锁 + BEGIN IMMEDIATE，并发决定不会覆盖较新的决定。
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from typing import Any, Iterable

from . import detect
from .contracts import (
    ACTION_ACCEPT,
    ACTION_MERGE,
    ACTION_REJECT,
    REVIEW_ACTIONS,
    make_segment_ref,
    normalize_status,
    normalize_utility,
    parse_segment_ref,
)
from .errors import (
    AuthenticationError,
    ConflictStateError,
    DuplicateBatchError,
    NotFoundError,
    PermissionDeniedError,
    RevisionStaleError,
    ValidationError,
)
from .storage import connect, init_schema, row_to_dict, transaction
from .util import iso, parse_ts, utcnow_iso

GENESIS = "GENESIS"
MERGED_BATCH = None  # 合并版本不隶属于任何提交批次，出处记录在 attributes.sources


def canonical(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def token_hash(token: str) -> str:
    return sha("pipeline-ledger-token:" + token)


class LedgerService:
    def __init__(self, db_path: str | sqlite3.Connection):
        if isinstance(db_path, sqlite3.Connection):
            self.conn = db_path
        else:
            self.conn = connect(db_path)
        init_schema(self.conn)
        self._ensure_genesis()

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ 人员

    def register_reviewer(
        self, reviewer_id: str, display_name: str, role: str = "reviewer", token: str | None = None
    ) -> dict[str, str]:
        if not reviewer_id or not display_name:
            raise ValidationError("reviewer_id 与 display_name 必填")
        if role not in ("reviewer", "admin"):
            raise ValidationError("role 只能是 reviewer 或 admin")
        token = token or secrets.token_urlsafe(18)
        now = utcnow_iso()
        try:
            with transaction(self.conn):
                self.conn.execute(
                    "INSERT INTO reviewers(reviewer_id, display_name, token_hash, role, created_at)"
                    " VALUES(?,?,?,?,?)",
                    (reviewer_id, display_name, token_hash(token), role, now),
                )
                self._audit(reviewer_id, "reviewer_registered", f"reviewer:{reviewer_id}",
                            {"role": role, "display_name": display_name})
        except sqlite3.IntegrityError as exc:
            raise DuplicateBatchError(f"审核人已存在: {reviewer_id}") from exc
        return {"reviewer_id": reviewer_id, "display_name": display_name, "role": role, "token": token}

    def authenticate(self, token: str | None) -> dict[str, Any]:
        if not token:
            raise AuthenticationError("缺少 Bearer 令牌")
        row = self.conn.execute(
            "SELECT reviewer_id, display_name, role, created_at FROM reviewers WHERE token_hash=?",
            (token_hash(token),),
        ).fetchone()
        if row is None:
            raise AuthenticationError("令牌无效")
        return dict(row)

    @staticmethod
    def require_reviewer(actor: dict[str, Any]) -> None:
        if actor.get("role") not in ("reviewer", "admin"):
            raise PermissionDeniedError("需要审核权限")

    # ------------------------------------------------------------------ 批次

    def submit_batch(
        self,
        batch_id: str,
        owner: str,
        records: list[dict[str, Any]],
        submitted_by: str = "anonymous",
    ) -> dict[str, Any]:
        if not batch_id or not str(batch_id).strip():
            raise ValidationError("batch_id 必填")
        if not records:
            raise ValidationError("批次至少包含一条记录")
        owner_norm = normalize_utility(owner)

        norm_records: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for i, rec in enumerate(records):
            nr = self._normalize_record(rec, owner_norm, i)
            if nr["asset_key"] in seen_keys:
                raise ValidationError(f"批次内资产编号重复: {nr['asset_id_label']}")
            seen_keys.add(nr["asset_key"])
            norm_records.append(nr)

        payload_hash = sha(canonical({"owner": owner_norm, "records": [
            {k: r[k] for k in r if k != "asset_key"} for r in norm_records
        ]}))
        now = utcnow_iso()

        with transaction(self.conn):
            existing = self.conn.execute(
                "SELECT batch_id, payload_hash FROM source_batches WHERE batch_id=?",
                (batch_id,),
            ).fetchone()
            if existing is not None:
                if existing["payload_hash"] != payload_hash:
                    raise DuplicateBatchError(
                        f"批次 {batch_id} 已以不同内容提交，原始批次不可变更；请使用新批次更正"
                    )
                # 完全相同的重复提交：幂等返回，不产生第二份资产
                rows = self.conn.execute(
                    "SELECT version_id FROM asset_versions WHERE batch_id=? ORDER BY version_id",
                    (batch_id,),
                ).fetchall()
                self._audit(submitted_by, "batch_resubmit_ignored", f"batch:{batch_id}",
                            {"payload_hash": payload_hash})
                return {
                    "batch_id": batch_id,
                    "owner": owner_norm,
                    "payload_hash": payload_hash,
                    "deduplicated": True,
                    "asset_versions": [r["version_id"] for r in rows],
                }

            self.conn.execute(
                "INSERT INTO source_batches(batch_id, owner, submitted_at, submitted_by,"
                " payload_hash, payload_json) VALUES(?,?,?,?,?,?)",
                (batch_id, owner_norm, now, submitted_by, payload_hash,
                 json.dumps(records, ensure_ascii=False)),
            )

            inserted: list[int] = []
            reused: list[int] = []
            for nr in norm_records:
                vid, is_new = self._upsert_version(nr, batch_id, now, submitted_by)
                (inserted if is_new else reused).append(vid)
                self.conn.execute(
                    "INSERT OR IGNORE INTO batch_version_refs(batch_id, version_id) VALUES(?,?)",
                    (batch_id, vid),
                )

            candidate_version_ids = self._scan_conflicts(now, submitted_by)
            self._auto_adopt(inserted + reused, candidate_version_ids, now, submitted_by)
            self._audit(submitted_by, "batch_submitted", f"batch:{batch_id}", {
                "owner": owner_norm,
                "payload_hash": payload_hash,
                "records": len(norm_records),
                "new_versions": inserted,
                "reused_versions": reused,
            })

        return {
            "batch_id": batch_id,
            "owner": owner_norm,
            "payload_hash": payload_hash,
            "deduplicated": False,
            "asset_versions": inserted + reused,
            "new_versions": inserted,
            "reused_versions": reused,
        }

    def _normalize_record(self, rec: dict[str, Any], owner_norm: str, index: int) -> dict[str, Any]:
        prefix = f"第 {index + 1} 条记录"
        try:
            label = str(rec["asset_id"]).strip()
            if not label:
                raise ValidationError(f"{prefix}: asset_id 必填")

            seg = rec.get("segment_ref")
            if isinstance(seg, dict):
                road = str(seg["road"])
                seg_ref = make_segment_ref(road, float(seg["from_m"]), float(seg["to_m"]))
            elif seg:
                seg_ref = str(seg)
            else:
                road = str(rec["road"])
                seg_ref = make_segment_ref(road, float(rec["from_m"]), float(rec["to_m"]))
            road, start, end = parse_segment_ref(seg_ref)

            depth = float(rec["burial_depth_m"])
            if depth <= 0:
                raise ValidationError(f"{prefix}: 埋深必须大于零")

            status = normalize_status(rec.get("status"))
            utility = normalize_utility(rec.get("utility") or owner_norm)

            operated_from = rec.get("operated_from")
            operated_to = rec.get("operated_to")
            df = parse_ts(operated_from, "operated_from")
            dt = parse_ts(operated_to, "operated_to")
            # 注意：from 晚于 to 不在此拒收——原始资料必须原样留存，
            # 由冲突检测以 time_inversion 给出可解释结果。
            diameter = rec.get("diameter_mm")
            if diameter is not None:
                diameter = float(diameter)
                if diameter <= 0:
                    raise ValidationError(f"{prefix}: diameter_mm 必须大于零")
            attrs = rec.get("attributes") or {}
            if not isinstance(attrs, dict):
                raise ValidationError(f"{prefix}: attributes 必须是对象")

            return {
                "asset_key": f"{owner_norm}|{label}",
                "asset_id_label": label,
                "road": road,
                "seg_start": start,
                "seg_end": end,
                "utility": utility,
                "burial_depth_m": depth,
                "status": status,
                "operated_from": iso(df) if df else None,
                "operated_to": iso(dt) if dt else None,
                "material": str(rec["material"]) if rec.get("material") else None,
                "diameter_mm": diameter,
                "attributes": attrs,
            }
        except KeyError as exc:
            raise ValidationError(f"{prefix}: 缺少字段 {exc.args[0]}") from exc
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ValidationError):
                raise
            raise ValidationError(f"{prefix}: {exc}") from exc

    def _upsert_version(
        self, nr: dict[str, Any], batch_id: str, now: str, submitted_by: str
    ) -> tuple[int, bool]:
        """返回 (version_id, is_new)。内容与最新版本一致则复用，不产生新版本。"""
        latest = self.conn.execute(
            "SELECT * FROM asset_versions WHERE asset_key=? ORDER BY version_no DESC LIMIT 1",
            (nr["asset_key"],),
        ).fetchone()

        payload = {
            "asset_key": nr["asset_key"],
            "asset_id_label": nr["asset_id_label"],
            "road": nr["road"],
            "seg_start": nr["seg_start"],
            "seg_end": nr["seg_end"],
            "utility": nr["utility"],
            "burial_depth_m": nr["burial_depth_m"],
            "status": nr["status"],
            "operated_from": nr["operated_from"],
            "operated_to": nr["operated_to"],
            "material": nr["material"],
            "diameter_mm": nr["diameter_mm"],
            "attributes": nr["attributes"],
        }

        if latest is not None:
            latest_payload = self._version_payload(latest)
            if canonical(latest_payload) == canonical(payload):
                return latest["version_id"], False
            supersedes = latest["version_id"]
            version_no = latest["version_no"] + 1
            prev_hash = latest["record_hash"]
        else:
            supersedes = None
            version_no = 1
            prev_hash = GENESIS

        record_hash = sha(prev_hash + "|" + canonical(payload))
        cur = self.conn.execute(
            "INSERT INTO asset_versions(asset_key, asset_id_label, batch_id, version_no,"
            " supersedes_version_id, road, seg_start, seg_end, utility, burial_depth_m,"
            " status, operated_from, operated_to, material, diameter_mm, attributes_json,"
            " record_hash, prev_record_hash, outcome, submitted_at, submitted_by)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'pending', ?, ?)",
            (
                nr["asset_key"], nr["asset_id_label"], batch_id, version_no, supersedes,
                nr["road"], nr["seg_start"], nr["seg_end"], nr["utility"], nr["burial_depth_m"],
                nr["status"], nr["operated_from"], nr["operated_to"], nr["material"],
                nr["diameter_mm"], json.dumps(nr["attributes"], ensure_ascii=False),
                record_hash, prev_hash, now, submitted_by,
            ),
        )
        vid = int(cur.lastrowid)
        if supersedes is not None:
            # 旧版本数据列不动；它被新版本取代的事实由 supersedes_version_id 表达。
            self._audit(submitted_by, "correction_submitted", f"version:{vid}", {
                "asset_key": nr["asset_key"], "supersedes": supersedes, "record_hash": record_hash,
            })
        return vid, True

    @staticmethod
    def _version_payload(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "asset_key": row["asset_key"],
            "asset_id_label": row["asset_id_label"],
            "road": row["road"],
            "seg_start": row["seg_start"],
            "seg_end": row["seg_end"],
            "utility": row["utility"],
            "burial_depth_m": row["burial_depth_m"],
            "status": row["status"],
            "operated_from": row["operated_from"],
            "operated_to": row["operated_to"],
            "material": row["material"],
            "diameter_mm": row["diameter_mm"],
            "attributes": json.loads(row["attributes_json"]),
        }

    # ------------------------------------------------------------------ 冲突

    def _scan_conflicts(self, now: str, actor: str) -> set[int]:
        """检测当前所有 pending/accepted 版本的冲突，登记或作废冲突。

        - 同一 asset_key 的版本链（更正链）彼此不构成冲突；
        - 单条记录自身时间倒置登记为单候选冲突；
        - 候选已失效（被更正/驳回）的未决冲突标记为 obsoleted。
        返回当前处于未决冲突中的版本集合。
        """
        rows = self.conn.execute(
            "SELECT * FROM asset_versions WHERE outcome IN ('pending','accepted') ORDER BY version_id"
        ).fetchall()
        active_ids = {r["version_id"] for r in rows}
        by_road: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            by_road.setdefault(row["road"], []).append(row)

        candidates: set[int] = set()

        def register(pair_key: str, conflict_id: str, road: str,
                     result: dict, cids: list[int]) -> None:
            if self.conn.execute(
                "SELECT 1 FROM conflicts WHERE pair_key=?", (pair_key,)
            ).fetchone() is not None:
                candidates.update(cids)
                return
            candidates.update(cids)
            self.conn.execute(
                "INSERT INTO conflicts(conflict_id, pair_key, road, kinds_json, details_json,"
                " candidate_ids_json, status, revision, created_at, created_by)"
                " VALUES(?,?,?,?,?,?,'open',1,?,?)",
                (conflict_id, pair_key, road,
                 json.dumps(result["kinds"], ensure_ascii=False),
                 json.dumps(result["details"], ensure_ascii=False),
                 json.dumps(cids), now, actor),
            )
            self._event(conflict_id, actor, "conflict_detected", 0, 1, now,
                        note=f"检测到冲突: {','.join(result['kinds'])}",
                        payload={"kinds": result["kinds"], "details": result["details"],
                                 "pair_key": pair_key})
            self._audit(actor, "conflict_detected", f"conflict:{conflict_id}", {
                "pair_key": pair_key, "road": road, "kinds": result["kinds"],
            })

        # 单条记录自身的时间倒置
        for r in rows:
            sf = detect.self_findings(dict(r))
            if sf:
                register(f"self:{r['version_id']}",
                         "C-" + sha(f"self:{r['version_id']}")[:16], r["road"], sf,
                         [r["version_id"]])

        # 更正版本待审：任何带前序版本的 pending 新版本，需审核人决定生效或驳回
        for r in rows:
            if r["outcome"] != "pending" or r["supersedes_version_id"] is None:
                continue
            register(
                f"correction:{r['version_id']}",
                "C-" + sha(f"correction:{r['version_id']}")[:16],
                r["road"],
                {"kinds": ["correction"], "details": [{
                    "type": "correction_pending_review",
                    "new_version_id": r["version_id"],
                    "supersedes_version_id": r["supersedes_version_id"],
                }]},
                [r["version_id"]],
            )

        # 候选已失效的旧未决冲突 -> obsoleted
        for c in self.conn.execute("SELECT * FROM conflicts WHERE status='open'").fetchall():
            cids = json.loads(c["candidate_ids_json"])
            dead = [v for v in cids if v not in active_ids]
            if dead:
                self._obsolete(c, f"候选版本 {dead} 已被更正或处置", now, actor)

        # 两两检测
        for road, group in by_road.items():
            for i in range(len(group)):
                for j in range(i + 1, len(group)):
                    a, b = group[i], group[j]
                    if a["asset_key"] == b["asset_key"]:
                        continue  # 同一资产的新旧版本：由版本链表达，不是冲突
                    result = detect.findings_for_pair(dict(a), dict(b))
                    if not result["kinds"]:
                        continue
                    pair_key = f"{min(a['version_id'], b['version_id'])}:{max(a['version_id'], b['version_id'])}"
                    conflict_id = "C-" + sha("pair:" + pair_key)[:16]
                    register(pair_key, conflict_id, road, result,
                             [a["version_id"], b["version_id"]])
        return candidates

    def _obsolete(self, conflict_row: sqlite3.Row, reason: str, now: str, actor: str) -> None:
        cid = conflict_row["conflict_id"]
        old_rev = conflict_row["revision"]
        self.conn.execute(
            "UPDATE conflicts SET status='resolved', revision=revision+1, resolved_at=?,"
            " action='obsoleted', decided_by='system', decision_note=? WHERE conflict_id=?",
            (now, reason, cid),
        )
        self._event(cid, "system", "obsoleted", old_rev, old_rev + 1, now, note=reason,
                    payload={"reason": reason})
        self._audit(actor, "conflict_obsoleted", f"conflict:{cid}", {"reason": reason})

    def _lineage_ancestors(self, version_id: int) -> list[int]:
        out: list[int] = []
        seen = {version_id}
        cur = self.conn.execute(
            "SELECT supersedes_version_id FROM asset_versions WHERE version_id=?", (version_id,)
        ).fetchone()
        while cur and cur["supersedes_version_id"]:
            parent = cur["supersedes_version_id"]
            if parent in seen:
                break
            seen.add(parent)
            out.append(parent)
            cur = self.conn.execute(
                "SELECT supersedes_version_id FROM asset_versions WHERE version_id=?", (parent,)
            ).fetchone()
        return out

    def _supersede_lineage(self, winner_id: int, now: str, conflict_id: str) -> list[int]:
        """采信某更正版本后，其版本链祖先全部失效。"""
        changed: list[int] = []
        for aid in self._lineage_ancestors(winner_id):
            row = self.conn.execute(
                "SELECT outcome FROM asset_versions WHERE version_id=?", (aid,)
            ).fetchone()
            if row is None or row["outcome"] not in ("pending", "accepted"):
                continue
            if row["outcome"] == "accepted":
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='superseded', effective_to=?,"
                    " decision_conflict_id=? WHERE version_id=?",
                    (now, conflict_id, aid),
                )
            else:
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='rejected', decision_conflict_id=?"
                    " WHERE version_id=?",
                    (conflict_id, aid),
                )
            changed.append(aid)
        return changed

    def _auto_adopt(
        self, touched: Iterable[int], candidate_ids: set[int], now: str, actor: str
    ) -> None:
        for vid in dict.fromkeys(touched):
            if vid in candidate_ids:
                continue
            row = self.conn.execute(
                "SELECT outcome FROM asset_versions WHERE version_id=?", (vid,)
            ).fetchone()
            if row is None or row["outcome"] != "pending":
                continue
            self.conn.execute(
                "UPDATE asset_versions SET outcome='accepted', effective_from=? WHERE version_id=?",
                (now, vid),
            )
            self._audit(actor, "auto_adopted", f"version:{vid}", {"effective_from": now})

    def list_conflicts(self, status: str | None = "open", road: str | None = None) -> list[dict]:
        sql = "SELECT * FROM conflicts WHERE 1=1"
        args: list[Any] = []
        if status:
            sql += " AND status=?"
            args.append(status)
        if road:
            sql += " AND road=?"
            args.append(road)
        sql += " ORDER BY created_at, conflict_id"
        return [self._conflict_summary(r) for r in self.conn.execute(sql, args).fetchall()]

    def get_conflict(self, conflict_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM conflicts WHERE conflict_id=?", (conflict_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"冲突不存在: {conflict_id}")
        data = self._conflict_summary(row)
        data["candidates"] = [
            self.version_brief(vid) for vid in json.loads(row["candidate_ids_json"])
        ]
        data["events"] = [
            dict(r) for r in self.conn.execute(
                "SELECT event_id, at, actor, action, from_revision, to_revision,"
                " winner_version_id, merged_version_id, note, parent_conflict_id"
                " FROM conflict_events WHERE conflict_id=? ORDER BY event_id",
                (conflict_id,),
            ).fetchall()
        ]
        if row["winner_version_id"]:
            data["winner"] = self.version_brief(row["winner_version_id"])
        if row["merged_version_id"]:
            data["merged"] = self.version_brief(row["merged_version_id"])
        return data

    def resolve_conflict(
        self,
        conflict_id: str,
        actor: dict[str, Any],
        action: str,
        expected_revision: int,
        winner_version_id: int | None = None,
        merged_fields: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        self.require_reviewer(actor)
        if action not in REVIEW_ACTIONS:
            raise ValidationError(f"action 必须是 {REVIEW_ACTIONS} 之一")
        try:
            expected_revision = int(expected_revision)
        except (TypeError, ValueError) as exc:
            raise ValidationError("expected_revision 必须是整数") from exc

        with transaction(self.conn):
            row = self.conn.execute(
                "SELECT * FROM conflicts WHERE conflict_id=?", (conflict_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError(f"冲突不存在: {conflict_id}")
            if row["revision"] != expected_revision:
                raise RevisionStaleError(
                    f"冲突修订号已过期：客户端基于 r{expected_revision}，当前为 r{row['revision']}，"
                    "请重新拉取后再决定"
                )
            if row["status"] == "resolved":
                raise ConflictStateError(
                    f"冲突 {conflict_id} 已由 {row['decided_by']} 处置（{row['action']}），不可重复决定"
                )

            candidate_ids = json.loads(row["candidate_ids_json"])
            candidates = [
                cr for cr in (
                    self.conn.execute(
                        "SELECT * FROM asset_versions WHERE version_id=?", (vid,)
                    ).fetchone()
                    for vid in candidate_ids
                ) if cr is not None
            ]
            if not candidates:
                raise ConflictStateError("冲突候选版本均不存在，数据异常")
            stale = [c["version_id"] for c in candidates if c["outcome"] not in ("pending", "accepted")]
            if stale:
                raise ConflictStateError(
                    f"候选版本 {stale} 已被更正或处置，请处理其更正产生的新冲突"
                )

            now = utcnow_iso()
            actor_id = actor["reviewer_id"]
            merged_vid = None

            if action == ACTION_ACCEPT:
                if winner_version_id is None:
                    raise ValidationError("采信(accept)必须指定 winner_version_id")
                if int(winner_version_id) not in [c["version_id"] for c in candidates]:
                    raise ValidationError("winner_version_id 必须是冲突候选版本之一")
                self._apply_accept(candidates, int(winner_version_id), now, conflict_id)
                # 采信更正版本时，其整条祖先链失效
                self._supersede_lineage(int(winner_version_id), now, conflict_id)
                winner_vid = int(winner_version_id)
            elif action == ACTION_REJECT:
                self._apply_reject(candidates, now, conflict_id)
                winner_vid = None
            else:  # merge
                merged_fields = merged_fields or {}
                winner_vid, merged_vid = self._apply_merge(
                    candidates, merged_fields, now, actor_id, conflict_id, note
                )

            self.conn.execute(
                "UPDATE conflicts SET status='resolved', revision=revision+1, resolved_at=?,"
                " action=?, winner_version_id=?, merged_version_id=?, decided_by=?, decision_note=?"
                " WHERE conflict_id=?",
                (now, action, winner_vid, merged_vid, actor_id, note, conflict_id),
            )
            self._event(conflict_id, actor_id, action, expected_revision,
                        expected_revision + 1, now, winner_vid, merged_vid, note,
                        payload={"action": action, "winner_version_id": winner_vid,
                                 "merged_version_id": merged_vid, "note": note})
            self._audit(actor_id, f"conflict_{action}", f"conflict:{conflict_id}", {
                "revision": expected_revision + 1,
                "winner_version_id": winner_vid,
                "merged_version_id": merged_vid,
                "candidate_ids": candidate_ids,
                "note": note,
            })
            # 处置改变了版本生效状态：作废候选失效的其他未决冲突，并让合并产物参与新检测
            self._scan_conflicts(now, actor_id)

        return self.get_conflict(conflict_id)

    def _apply_accept(
        self, candidates: list[sqlite3.Row], winner_id: int, now: str, conflict_id: str
    ) -> None:
        for c in candidates:
            if c["version_id"] == winner_id:
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='accepted',"
                    " effective_from=COALESCE(effective_from, ?), effective_to=NULL,"
                    " decision_conflict_id=? WHERE version_id=?",
                    (now, conflict_id, winner_id),
                )
            elif c["outcome"] == "accepted":
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='superseded', effective_to=?,"
                    " decision_conflict_id=? WHERE version_id=?",
                    (now, conflict_id, c["version_id"]),
                )
            else:
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='rejected', decision_conflict_id=?"
                    " WHERE version_id=?",
                    (conflict_id, c["version_id"]),
                )

    def _apply_reject(self, candidates: list[sqlite3.Row], now: str, conflict_id: str) -> None:
        for c in candidates:
            if c["outcome"] == "accepted":
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='rejected', effective_to=?,"
                    " decision_conflict_id=? WHERE version_id=?",
                    (now, conflict_id, c["version_id"]),
                )
            else:
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='rejected', decision_conflict_id=?"
                    " WHERE version_id=?",
                    (conflict_id, c["version_id"]),
                )

    def _apply_merge(
        self,
        candidates: list[sqlite3.Row],
        fields: dict[str, Any],
        now: str,
        actor_id: str,
        conflict_id: str,
        note: str | None,
    ) -> tuple[int, int]:
        base = dict(candidates[0])
        sources = [
            {"version_id": c["version_id"], "asset_key": c["asset_key"], "batch_id": c["batch_id"]}
            for c in candidates
        ]

        def pick(name: str, default: Any) -> Any:
            return fields[name] if name in fields else default

        seg_ref = fields.get("segment_ref")
        if seg_ref:
            road, start, end = parse_segment_ref(str(seg_ref))
        elif "road" in fields:
            road = str(fields["road"])
            start = float(pick("seg_start", base["seg_start"]))
            end = float(pick("seg_end", base["seg_end"]))
        else:
            road, start, end = base["road"], base["seg_start"], base["seg_end"]
        if start >= end:
            raise ValidationError("合并分段起点必须小于终点")

        depth = float(pick("burial_depth_m", base["burial_depth_m"]))
        if depth <= 0:
            raise ValidationError("合并埋深必须大于零")
        status = normalize_status(pick("status", base["status"]))
        diameter = fields.get("diameter_mm", base["diameter_mm"])
        diameter = float(diameter) if diameter is not None else None
        attrs = dict(json.loads(base["attributes_json"]))
        attrs.update(fields.get("attributes") or {})
        attrs["_merged_from"] = sources
        attrs["_merged_by"] = actor_id
        attrs["_merge_note"] = note

        asset_key = f"merged|{conflict_id}"
        payload = {
            "asset_key": asset_key,
            "asset_id_label": f"MERGED-{conflict_id}",
            "road": road,
            "seg_start": start,
            "seg_end": end,
            "utility": normalize_utility(pick("utility", base["utility"])),
            "burial_depth_m": depth,
            "status": status,
            "operated_from": pick("operated_from", base["operated_from"]),
            "operated_to": pick("operated_to", base["operated_to"]),
            "material": pick("material", base["material"]),
            "diameter_mm": diameter,
            "attributes": attrs,
        }
        record_hash = sha(GENESIS + "|" + canonical(payload))
        cur = self.conn.execute(
            "INSERT INTO asset_versions(asset_key, asset_id_label, batch_id, version_no,"
            " supersedes_version_id, road, seg_start, seg_end, utility, burial_depth_m,"
            " status, operated_from, operated_to, material, diameter_mm, attributes_json,"
            " record_hash, prev_record_hash, outcome, effective_from, decision_conflict_id,"
            " submitted_at, submitted_by, corrected_by, correction_reason)"
            " VALUES(?,?,NULL,1,NULL,?,?,?,?,?,?,?,?,?,?,?,?,?,'accepted',?,?,?,?,'merge',?)",
            (
                asset_key, payload["asset_id_label"], road, start, end, payload["utility"],
                depth, status, payload["operated_from"], payload["operated_to"],
                payload["material"], diameter,
                json.dumps(attrs, ensure_ascii=False), record_hash, GENESIS,
                now, conflict_id, now, actor_id, note,
            ),
        )
        merged_vid = int(cur.lastrowid)
        for c in candidates:
            if c["outcome"] == "accepted":
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='superseded', effective_to=?,"
                    " decision_conflict_id=? WHERE version_id=?",
                    (now, conflict_id, c["version_id"]),
                )
            else:
                self.conn.execute(
                    "UPDATE asset_versions SET outcome='rejected', decision_conflict_id=?"
                    " WHERE version_id=?",
                    (conflict_id, c["version_id"]),
                )
        return merged_vid, merged_vid

    # ------------------------------------------------------------------ 查询

    def road_view(self, road: str, at: str | None = None) -> dict[str, Any]:
        """指定道路在某一时点的：有效管线 + 来源证据 + 冲突处置。"""
        at_dt = parse_ts(at, "at") if at else parse_ts(utcnow_iso())
        at_iso = iso(at_dt)

        rows = self.conn.execute(
            "SELECT * FROM asset_versions v WHERE road=? AND "
            " v.effective_from IS NOT NULL AND v.effective_from<=? AND "
            " (v.effective_to IS NULL OR v.effective_to>?) "
            " ORDER BY seg_start, version_id",
            (road, at_iso, at_iso),
        ).fetchall()
        # 生效与否只由生效区间决定，不看 outcome 现值：
        # 一条曾被采信、之后被驳回/取代的记录，在历史时点仍应如实还原为当时有效。
        # 从未生效的版本 effective_from 为 NULL；失效时 _apply_* 会写 effective_to。
        pipelines = [self._version_full(r) for r in rows]

        batch_ids = {p["batch_id"] for p in pipelines if p["batch_id"]}
        # 合并产物无提交批次：沿 attributes._merged_from 回溯原始批次证据
        for p in pipelines:
            for src in p["attributes"].get("_merged_from", []) or []:
                if src.get("batch_id"):
                    batch_ids.add(src["batch_id"])
        evidence = {}
        for bid in batch_ids:
            b = self.conn.execute(
                "SELECT batch_id, owner, submitted_at, submitted_by, payload_hash"
                " FROM source_batches WHERE batch_id=?",
                (bid,),
            ).fetchone()
            if b:
                evidence[bid] = dict(b)

        conflicts = self.conn.execute(
            "SELECT * FROM conflicts WHERE road=? ORDER BY created_at", (road,)
        ).fetchall()
        conflict_out = []
        effective_ids = {p["version_id"] for p in pipelines}
        for c in conflicts:
            if c["created_at"] > at_iso:
                continue  # 该时点冲突尚未产生
            cids = json.loads(c["candidate_ids_json"])
            summary = self._conflict_summary(c)
            # 还原该时点的冲突状态：处置发生在 at 之后 => 当时仍未决
            if c["resolved_at"] and c["resolved_at"] > at_iso:
                summary["status_as_of_at"] = "open"
            else:
                summary["status_as_of_at"] = c["status"]
            # 未决冲突始终提示（当前可操作的警告）；
            # 已处置冲突须产出当前有效管线（候选、采信或合并产物之一在生效集）
            produced = set(cids) | {c["winner_version_id"], c["merged_version_id"]}
            if summary["status_as_of_at"] == "open" or any(
                v in effective_ids for v in produced if v
            ):
                conflict_out.append(summary)

        return {
            "road": road,
            "at": at_iso,
            "effective_pipelines": pipelines,
            "source_evidence": evidence,
            "conflicts": conflict_out,
        }

    def asset_history(self, asset_key: str) -> dict[str, Any]:
        rows = self.conn.execute(
            "SELECT * FROM asset_versions WHERE asset_key=? ORDER BY version_no", (asset_key,)
        ).fetchall()
        if not rows:
            raise NotFoundError(f"资产不存在: {asset_key}")
        versions = [self._version_full(r) for r in rows]
        diffs = []
        for a, b in zip(versions, versions[1:]):
            diffs.append(self.diff_versions(a["version_id"], b["version_id"]))
        return {"asset_key": asset_key, "versions": versions, "successive_diffs": diffs}

    def find_asset_key(self, label: str) -> list[str]:
        return [
            r["asset_key"]
            for r in self.conn.execute(
                "SELECT DISTINCT asset_key FROM asset_versions WHERE asset_id_label=? ORDER BY asset_key",
                (label,),
            ).fetchall()
        ]

    def get_version(self, version_id: int) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM asset_versions WHERE version_id=?", (version_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"版本不存在: {version_id}")
        return self._version_full(row)

    def diff_versions(self, a_id: int, b_id: int) -> dict[str, Any]:
        a, b = self.get_version(a_id), self.get_version(b_id)
        fields = [
            "asset_id_label", "road", "seg_start", "seg_end", "utility", "burial_depth_m",
            "status", "operated_from", "operated_to", "material", "diameter_mm",
            "outcome", "effective_from", "effective_to", "batch_id",
        ]
        changes = [
            {"field": f, "from": a.get(f), "to": b.get(f)}
            for f in fields
            if a.get(f) != b.get(f)
        ]
        if a.get("attributes") != b.get("attributes"):
            changes.append({
                "field": "attributes",
                "from": a.get("attributes"),
                "to": b.get("attributes"),
            })
        return {
            "from_version": a_id,
            "to_version": b_id,
            "from_identity": {"asset_key": a["asset_key"], "version_no": a["version_no"]},
            "to_identity": {"asset_key": b["asset_key"], "version_no": b["version_no"]},
            "changes": changes,
            "record_hashes": {"from": a["record_hash"], "to": b["record_hash"]},
        }

    def list_batches(self) -> list[dict]:
        return [
            dict(r)
            for r in self.conn.execute(
                "SELECT batch_id, owner, submitted_at, submitted_by, payload_hash"
                " FROM source_batches ORDER BY submitted_at"
            ).fetchall()
        ]

    def get_batch(self, batch_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM source_batches WHERE batch_id=?", (batch_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"批次不存在: {batch_id}")
        data = dict(row)
        data["versions"] = [
            self.version_brief(r["version_id"])
            for r in self.conn.execute(
                "SELECT version_id FROM asset_versions WHERE batch_id=? ORDER BY version_id",
                (batch_id,),
            ).fetchall()
        ]
        return data

    # ------------------------------------------------------------------ 序列化

    def version_brief(self, version_id: int) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT version_id, asset_key, asset_id_label, version_no, batch_id, road,"
            " seg_start, seg_end, utility, burial_depth_m, status, outcome,"
            " effective_from, effective_to, decision_conflict_id"
            " FROM asset_versions WHERE version_id=?",
            (version_id,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"版本不存在: {version_id}")
        return dict(row)

    def _version_full(self, row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        data["attributes"] = json.loads(data.pop("attributes_json"))
        return data

    def _conflict_summary(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "conflict_id": row["conflict_id"],
            "pair_key": row["pair_key"],
            "road": row["road"],
            "kinds": json.loads(row["kinds_json"]),
            "details": json.loads(row["details_json"]),
            "candidate_ids": json.loads(row["candidate_ids_json"]),
            "status": row["status"],
            "revision": row["revision"],
            "created_at": row["created_at"],
            "created_by": row["created_by"],
            "resolved_at": row["resolved_at"],
            "action": row["action"],
            "winner_version_id": row["winner_version_id"],
            "merged_version_id": row["merged_version_id"],
            "decided_by": row["decided_by"],
            "decision_note": row["decision_note"],
            "parent_conflict_id": row["parent_conflict_id"],
        }

    # ------------------------------------------------------------------ 内部

    def _event(
        self, conflict_id: str, actor: str, action: str, from_rev: int, to_rev: int, at: str,
        winner_version_id: int | None = None, merged_version_id: int | None = None,
        note: str | None = None, payload: dict[str, Any] | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT INTO conflict_events(conflict_id, at, actor, action, from_revision,"
            " to_revision, winner_version_id, merged_version_id, note, parent_conflict_id,"
            " payload_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (conflict_id, at, actor, action, from_rev, to_rev, winner_version_id,
             merged_version_id, note, None,
             json.dumps(payload or {}, ensure_ascii=False)),
        )

    def _audit(self, actor: str, action: str, target: str, detail: dict[str, Any]) -> None:
        prev = self.conn.execute(
            "SELECT chain_hash FROM audit_log ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        prev_hash = prev["chain_hash"] if prev else GENESIS
        at = utcnow_iso()
        payload = canonical({"at": at, "actor": actor, "action": action,
                             "target": target, "detail": detail})
        chain = sha(prev_hash + "|" + payload)
        self.conn.execute(
            "INSERT INTO audit_log(at, actor, action, target, detail_json, chain_hash,"
            " prev_chain_hash) VALUES(?,?,?,?,?,?,?)",
            (at, actor, action, target, json.dumps(detail, ensure_ascii=False), chain, prev_hash),
        )

    def _ensure_genesis(self) -> None:
        row = self.conn.execute("SELECT seq FROM audit_log ORDER BY seq LIMIT 1").fetchone()
        if row is None:
            at = utcnow_iso()
            payload = canonical({"at": at, "actor": "system", "action": "genesis",
                                 "target": "ledger", "detail": {}})
            chain = sha(GENESIS + "|" + payload)
            self.conn.execute(
                "INSERT INTO audit_log(at, actor, action, target, detail_json, chain_hash,"
                " prev_chain_hash) VALUES(?,?,?,?,?,?,?)",
                (at, "system", "genesis", "ledger", "{}", chain, GENESIS),
            )

    def audit_trail(self, limit: int = 100) -> list[dict]:
        rows = self.conn.execute(
            "SELECT seq, at, actor, action, target, detail_json, chain_hash, prev_chain_hash"
            " FROM audit_log ORDER BY seq DESC LIMIT ?", (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def verify_chain(self) -> dict[str, Any]:
        """重算审计哈希链与版本哈希链，用于自检/演示责任链完整性。"""
        prev_hash = GENESIS
        audit_ok = True
        for r in self.conn.execute(
            "SELECT at, actor, action, target, detail_json, chain_hash, prev_chain_hash"
            " FROM audit_log ORDER BY seq"
        ).fetchall():
            if r["prev_chain_hash"] != prev_hash:
                audit_ok = False
                break
            payload = canonical({
                "at": r["at"], "actor": r["actor"], "action": r["action"],
                "target": r["target"], "detail": json.loads(r["detail_json"]),
            })
            if sha(prev_hash + "|" + payload) != r["chain_hash"]:
                audit_ok = False
                break
            prev_hash = r["chain_hash"]

        broken_versions: list[int] = []
        rows = self.conn.execute(
            "SELECT * FROM asset_versions ORDER BY asset_key, version_no"
        ).fetchall()
        last_by_key: dict[str, sqlite3.Row] = {}
        for r in rows:
            expected_prev = GENESIS if r["version_no"] == 1 else last_by_key[r["asset_key"]]["record_hash"]
            payload = canonical(self._version_payload(r))
            if r["prev_record_hash"] != expected_prev or r["record_hash"] != sha(expected_prev + "|" + payload):
                broken_versions.append(r["version_id"])
            last_by_key[r["asset_key"]] = r

        return {"audit_chain_intact": audit_ok, "broken_version_ids": broken_versions}
