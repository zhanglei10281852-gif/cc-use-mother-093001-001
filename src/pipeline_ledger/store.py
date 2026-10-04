"""权威底账存储与领域服务。

设计要点：
- 原始批次与原始记录只增不改（immutable），逐份留存原文哈希；
- 生效版本（effective_versions）每次采信/合并都新增版本，旧版本置为失效，
  从而支持某一时点（as-of）的还原与版本差异；
- 冲突在行级带 row_version，审核决定以乐观锁提交，并发决定不会覆盖较新结论；
- audit_log 为哈希链，串联提交、生效、建冲突、处置等事件，形成责任链；
- 所有状态持久化在 SQLite，服务重启后未决冲突可继续处理。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from typing import Any, Optional

from .contracts import (
    AssetStatus,
    ConflictStatus,
    ConflictType,
    DecisionAction,
    UtilityType,
    build_record,
)
from .geometry import (
    DEPTH_TOLERANCE_M,
    Segment,
    fmt_num,
    overlaps,
    parse_segment,
    same_span,
)

# 状态生命周期序号：拟建 -> 在役 -> 退役
_LIFECYCLE_ORDER = {
    AssetStatus.PLANNED: 0,
    AssetStatus.IN_SERVICE: 1,
    AssetStatus.DECOMMISSIONED: 2,
}


class LedgerError(Exception):
    """业务错误基类。"""


class DuplicateBatchError(LedgerError):
    pass


class NotFoundError(LedgerError):
    pass


class ConflictStateError(LedgerError):
    """冲突已处置或已被他人更新（并发冲突）。"""


class PermissionDeniedError(LedgerError):
    pass


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_ts(value: Optional[str]) -> str:
    """把输入时间统一为带时区的 UTC ISO 字符串，保证可按字符串比较。"""
    if not value:
        return utcnow_iso()
    text = value.strip().replace("Z", "+00:00") if value.endswith("Z") else value.strip()
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise LedgerError(f"时间格式无法解析：{value!r}（需 ISO8601）") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def canonical_hash(payload: Any) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _record_content_hash(rec: dict) -> str:
    return canonical_hash(
        {
            "asset_id": rec["asset_id"],
            "utility_type": rec["utility_type"],
            "segment": rec["segment"],
            "burial_depth_m": round(float(rec["burial_depth_m"]), 4),
            "status": rec["status"],
            "surveyed_at": rec["surveyed_at"],
            "attributes": rec.get("attributes") or {},
            "note": rec.get("note"),
        }
    )


SCHEMA = """
CREATE TABLE IF NOT EXISTS source_batches (
    batch_id      TEXT PRIMARY KEY,
    owner         TEXT NOT NULL,
    submitted_at  TEXT NOT NULL,
    record_count  INTEGER NOT NULL,
    raw_payload   TEXT NOT NULL,
    raw_hash      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_records (
    record_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id        TEXT NOT NULL,
    utility_type    TEXT NOT NULL,
    road            TEXT NOT NULL,
    span_start      REAL NOT NULL,
    span_end        REAL NOT NULL,
    segment_ref     TEXT NOT NULL,
    burial_depth    REAL NOT NULL,
    status          TEXT NOT NULL,
    surveyed_at     TEXT NOT NULL,
    batch_id        TEXT NOT NULL REFERENCES source_batches(batch_id),
    attributes_json TEXT NOT NULL DEFAULT '{}',
    note            TEXT,
    ingested_at     TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    UNIQUE(batch_id, asset_id),
    UNIQUE(content_hash)
);
CREATE TABLE IF NOT EXISTS effective_versions (
    version_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id                TEXT NOT NULL,
    version_no              INTEGER NOT NULL,
    utility_type            TEXT NOT NULL,
    road                    TEXT NOT NULL,
    span_start              REAL NOT NULL,
    span_end                REAL NOT NULL,
    segment_ref             TEXT NOT NULL,
    burial_depth            REAL NOT NULL,
    status                  TEXT NOT NULL,
    surveyed_at             TEXT NOT NULL,
    origin_record_ids       TEXT NOT NULL,
    origin_batch_ids        TEXT NOT NULL,
    effective_from          TEXT NOT NULL,
    superseded_at           TEXT,
    supersedes_version_id   INTEGER,
    conflict_id             TEXT,
    decision_id             TEXT,
    active                  INTEGER NOT NULL DEFAULT 1,
    UNIQUE(asset_id, version_no)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_version
    ON effective_versions(asset_id) WHERE active = 1;
CREATE TABLE IF NOT EXISTS conflicts (
    conflict_id   TEXT PRIMARY KEY,
    conflict_type TEXT NOT NULL,
    road          TEXT NOT NULL,
    record_a      INTEGER NOT NULL REFERENCES raw_records(record_id),
    record_b      INTEGER NOT NULL REFERENCES raw_records(record_id),
    detail_json   TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open',
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    resolved_by_decision_id TEXT,
    row_version   INTEGER NOT NULL DEFAULT 1,
    UNIQUE(conflict_type, record_a, record_b)
);
CREATE TABLE IF NOT EXISTS decisions (
    decision_id          TEXT PRIMARY KEY,
    conflict_id          TEXT NOT NULL REFERENCES conflicts(conflict_id),
    action               TEXT NOT NULL,
    reviewer             TEXT NOT NULL,
    rationale            TEXT NOT NULL,
    decided_at           TEXT NOT NULL,
    merged_json          TEXT,
    resulting_version_id INTEGER,
    expected_row_version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reviewers (
    reviewer_id  TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    added_at     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_log (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    actor       TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    prev_hash   TEXT,
    entry_hash  TEXT NOT NULL
);
"""


class LedgerStore:
    """底账存储。默认持久化到文件；``path=":memory:"`` 仅供单连接测试。"""

    def __init__(self, path: str = "ledger.db", bootstrap_reviewers: Optional[dict] = None):
        self.path = path
        self._writer_lock = threading.Lock()
        # :memory: 下所有连接必须共享同一个内存库，否则每个连接看到不同的库
        self._memory = path == ":memory:"
        self._shared_conn: Optional[sqlite3.Connection] = None
        init_conn = self._connect()
        init_conn.row_factory = sqlite3.Row
        self._apply_pragmas(init_conn)
        init_conn.executescript(SCHEMA)
        init_conn.commit()
        if bootstrap_reviewers:
            for rid, name in bootstrap_reviewers.items():
                self.add_reviewer(rid, name)
        # 文件库的初始化连接不再使用，即时释放；内存库它就是共享连接，需保留
        if not self._memory:
            init_conn.close()

    # ---- 基础连接 -------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        if self._memory:
            if self._shared_conn is None:
                conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA foreign_keys = ON")
                self._shared_conn = conn
            return self._shared_conn
        conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn

    @contextmanager
    def _write(self):
        """串行化写事务：取写锁 -> BEGIN IMMEDIATE -> 提交/回滚 -> 关闭连接。"""
        conn = self._connect()
        try:
            with self._writer_lock:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    yield conn
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
        finally:
            if not self._memory:
                conn.close()

    @contextmanager
    def _read(self):
        conn = self._connect()
        # 内存库只有一个共享连接，读也需与写互斥
        lock = self._writer_lock if self._memory else nullcontext()
        try:
            with lock:
                yield conn
        finally:
            if not self._memory:
                conn.close()

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")

    def close(self) -> None:
        if self._memory and self._shared_conn is not None:
            self._shared_conn.close()
            self._shared_conn = None

    # ---- 审核人 ---------------------------------------------------------

    def add_reviewer(self, reviewer_id: str, display_name: str) -> None:
        if not reviewer_id:
            raise LedgerError("审核人标识不能为空")
        with self._write() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO reviewers(reviewer_id, display_name, added_at) "
                "VALUES (?,?,?)",
                (reviewer_id, display_name or reviewer_id, utcnow_iso()),
            )

    def list_reviewers(self) -> list[dict]:
        with self._read() as conn:
            rows = conn.execute("SELECT * FROM reviewers ORDER BY reviewer_id").fetchall()
            return [dict(r) for r in rows]

    def _require_reviewer(self, conn: sqlite3.Connection, reviewer_id: str) -> None:
        row = conn.execute(
            "SELECT 1 FROM reviewers WHERE reviewer_id = ?", (reviewer_id,)
        ).fetchone()
        if not row:
            raise PermissionDeniedError(f"审核人 {reviewer_id!r} 无权限或不存在")

    # ---- 批次提交 -------------------------------------------------------

    def submit_batch(
        self,
        *,
        batch_id: str,
        owner: str,
        records: list[dict],
        submitted_at: Optional[str] = None,
        actor: Optional[str] = None,
    ) -> dict:
        """提交一个来源批次；返回批次、入库记录与新发现冲突的汇总。

        重复批次（同 batch_id）会被拒绝；批次内重复资产、与历史完全相同的
        重复记录也不会产生第二份资产。
        """
        if not batch_id or not owner:
            raise LedgerError("batch_id 与 owner 不能为空")
        if not records:
            raise LedgerError("批次至少包含一条记录")

        submitted = normalize_ts(submitted_at)
        actor = actor or owner
        raw_payload = {"batch_id": batch_id, "owner": owner, "records": records}
        raw_hash = canonical_hash(raw_payload)

        # 预解析，提前暴露格式错误，避免写入半成品
        parsed = []
        seen_assets: set[str] = set()
        for item in records:
            rec = build_record(
                asset_id=item["asset_id"],
                utility_type=item["utility_type"],
                segment=item["segment"],
                burial_depth_m=item["burial_depth_m"],
                status=item["status"],
                surveyed_at=normalize_ts(item["surveyed_at"]),
                source_batch_id=batch_id,
                attributes=item.get("attributes"),
                note=item.get("note"),
            )
            if rec.asset_id in seen_assets:
                raise DuplicateBatchError(
                    f"批次 {batch_id} 内资产 {rec.asset_id} 重复，禁止生成两份资产"
                )
            seen_assets.add(rec.asset_id)
            parsed.append(rec)

        with self._write() as conn:
            exists = conn.execute(
                "SELECT 1 FROM source_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if exists:
                raise DuplicateBatchError(f"批次 {batch_id} 已存在，重复批次禁止再次入账")

            conn.execute(
                "INSERT INTO source_batches(batch_id, owner, submitted_at, record_count, "
                "raw_payload, raw_hash) VALUES (?,?,?,?,?,?)",
                (
                    batch_id,
                    owner,
                    submitted,
                    len(parsed),
                    json.dumps(raw_payload, ensure_ascii=False),
                    raw_hash,
                ),
            )
            self._audit(
                conn,
                event_type="batch_submitted",
                entity_type="batch",
                entity_id=batch_id,
                actor=actor,
                occurred_at=submitted,
                payload={"owner": owner, "record_count": len(parsed), "raw_hash": raw_hash},
            )

            ingested: list[dict] = []
            new_conflicts: list[dict] = []
            for rec in parsed:
                try:
                    stored = self._insert_record(conn, rec, submitted, actor)
                except sqlite3.IntegrityError as exc:
                    raise DuplicateBatchError(
                        f"资产 {rec.asset_id} 的记录与既有记录完全重复，已保留原始版本，"
                        f"不产生第二份资产"
                    ) from exc
                conflicts = self._detect_conflicts(conn, stored, submitted, actor)
                new_conflicts.extend(conflicts)
                if not conflicts:
                    # 无任何冲突：成为新生效版本（自动接替同资产旧版本）
                    self._adopt(
                        conn,
                        stored,
                        effective_from=submitted,
                        conflict_id=None,
                        decision_id=None,
                        actor=actor,
                    )
                ingested.append(self._record_json(stored))

        return {
            "batch_id": batch_id,
            "owner": owner,
            "submitted_at": submitted,
            "raw_hash": raw_hash,
            "records": ingested,
            "new_conflicts": new_conflicts,
        }

    def _insert_record(
        self, conn: sqlite3.Connection, rec, ingested_at: str, actor: str
    ) -> sqlite3.Row:
        attrs = dict(rec.attributes)
        content_hash = _record_content_hash(
            {
                "asset_id": rec.asset_id,
                "utility_type": rec.utility_type.value,
                "segment": rec.segment.as_ref(),
                "burial_depth_m": rec.burial_depth_m,
                "status": rec.status.value,
                "surveyed_at": rec.surveyed_at,
                "attributes": attrs,
                "note": rec.note,
            }
        )
        cur = conn.execute(
            "INSERT INTO raw_records(asset_id, utility_type, road, span_start, span_end, "
            "segment_ref, burial_depth, status, surveyed_at, batch_id, attributes_json, "
            "note, ingested_at, content_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                rec.asset_id,
                rec.utility_type.value,
                rec.segment.road,
                rec.segment.start_m,
                rec.segment.end_m,
                rec.segment.as_ref(),
                rec.burial_depth_m,
                rec.status.value,
                rec.surveyed_at,
                rec.source_batch_id,
                json.dumps(attrs, ensure_ascii=False),
                rec.note,
                ingested_at,
                content_hash,
            ),
        )
        row = conn.execute(
            "SELECT * FROM raw_records WHERE record_id = ?", (cur.lastrowid,)
        ).fetchone()
        self._audit(
            conn,
            event_type="record_ingested",
            entity_type="raw_record",
            entity_id=str(row["record_id"]),
            actor=actor,
            occurred_at=ingested_at,
            payload={
                "asset_id": rec.asset_id,
                "batch_id": rec.source_batch_id,
                "segment": rec.segment.as_ref(),
                "content_hash": content_hash,
            },
        )
        return row

    # ---- 冲突检测 -------------------------------------------------------

    def _detect_conflicts(
        self, conn: sqlite3.Connection, new_row: sqlite3.Row, now: str, actor: str
    ) -> list[dict]:
        others = conn.execute(
            "SELECT * FROM raw_records WHERE utility_type = ? AND road = ? "
            "AND record_id <> ?",
            (new_row["utility_type"], new_row["road"], new_row["record_id"]),
        ).fetchall()
        # 同资产但换了道路写法（不应常见）也纳入属性比对
        others += conn.execute(
            "SELECT * FROM raw_records WHERE asset_id = ? AND record_id NOT IN "
            "(SELECT record_id FROM raw_records WHERE utility_type = ? AND road = ?) "
            "AND record_id <> ?",
            (
                new_row["asset_id"],
                new_row["utility_type"],
                new_row["road"],
                new_row["record_id"],
            ),
        ).fetchall()

        found: list[dict] = []
        for other in others:
            # lo/hi 按入库先后排序，保证冲突双方与 conflict_id 稳定
            lo, hi = sorted([new_row, other], key=lambda r: r["record_id"])
            lo_seg, hi_seg = self._row_segment(lo), self._row_segment(hi)

            same_asset = lo["asset_id"] == hi["asset_id"]
            seg_overlap = lo["road"] == hi["road"] and overlaps(lo_seg, hi_seg)
            depth_diff = abs(lo["burial_depth"] - hi["burial_depth"])
            depth_clash = depth_diff > DEPTH_TOLERANCE_M
            status_clash = lo["status"] != hi["status"]

            if seg_overlap:
                found.append(
                    self._raise_conflict(
                        conn,
                        ConflictType.SPATIAL_OVERLAP,
                        lo,
                        hi,
                        lo_seg,
                        hi_seg,
                        now,
                        actor,
                        reason=(
                            f"同一资产 {lo['asset_id']} 被不同资料重复申报于重叠分段："
                            f"{lo_seg.as_ref()}（{lo['batch_id']}）与 "
                            f"{hi_seg.as_ref()}（{hi['batch_id']}）"
                            if same_asset
                            else f"{lo['utility_type']} 管线在 {lo['road']} 上空间重叠："
                            f"{lo['asset_id']} {lo_seg.as_ref()} 与 "
                            f"{hi['asset_id']} {hi_seg.as_ref()}"
                        ),
                        extra={"depth_a": lo["burial_depth"], "depth_b": hi["burial_depth"]},
                    )
                )
                if depth_clash or status_clash:
                    reasons = []
                    if depth_clash:
                        reasons.append(
                            f"埋深 {lo['burial_depth']}m vs {hi['burial_depth']}m"
                            f"（差 {fmt_num(depth_diff)}m，超过容差 "
                            f"{fmt_num(DEPTH_TOLERANCE_M)}m）"
                        )
                    if status_clash:
                        reasons.append(
                            f"投运状态 {lo['status']} vs {hi['status']}"
                        )
                    found.append(
                        self._raise_conflict(
                            conn,
                            ConflictType.ATTR_CONTRADICTION,
                            lo,
                            hi,
                            lo_seg,
                            hi_seg,
                            now,
                            actor,
                            reason="重叠线段属性矛盾：" + "；".join(reasons),
                            extra={
                                "depth_a": lo["burial_depth"],
                                "depth_b": hi["burial_depth"],
                                "status_a": lo["status"],
                                "status_b": hi["status"],
                            },
                        )
                    )
            elif same_asset and (depth_clash or status_clash or not same_span(lo_seg, hi_seg)):
                reasons = []
                if depth_clash:
                    reasons.append(
                        f"埋深 {lo['burial_depth']}m vs {hi['burial_depth']}m"
                    )
                if status_clash:
                    reasons.append(f"投运状态 {lo['status']} vs {hi['status']}")
                if not same_span(lo_seg, hi_seg):
                    reasons.append(f"分段 {lo_seg.as_ref()} vs {hi_seg.as_ref()}")
                found.append(
                    self._raise_conflict(
                        conn,
                        ConflictType.ATTR_CONTRADICTION,
                        lo,
                        hi,
                        lo_seg,
                        hi_seg,
                        now,
                        actor,
                        reason=f"同一资产 {lo['asset_id']} 属性矛盾：" + "；".join(reasons),
                        extra={
                            "depth_a": lo["burial_depth"],
                            "depth_b": hi["burial_depth"],
                            "status_a": lo["status"],
                            "status_b": hi["status"],
                        },
                    )
                )

            # 时间倒置：同址（重叠或同一资产）记录的勘测时间先后与物理生命周期矛盾
            order_a = _LIFECYCLE_ORDER[AssetStatus(lo["status"])]
            order_b = _LIFECYCLE_ORDER[AssetStatus(hi["status"])]
            if (
                (seg_overlap or same_asset)
                and lo["surveyed_at"] != hi["surveyed_at"]
                and order_a != order_b
            ):
                earlier, later = (
                    (lo, hi) if lo["surveyed_at"] < hi["surveyed_at"] else (hi, lo)
                )
                earlier_order = _LIFECYCLE_ORDER[AssetStatus(earlier["status"])]
                later_order = _LIFECYCLE_ORDER[AssetStatus(later["status"])]
                if later_order < earlier_order:
                    found.append(
                        self._raise_conflict(
                            conn,
                            ConflictType.TIME_INVERSION,
                            lo,
                            hi,
                            lo_seg,
                            hi_seg,
                            now,
                            actor,
                            reason=(
                                f"时间倒置：{earlier['asset_id']} 在 {earlier['surveyed_at']} "
                                f"勘测为 {earlier['status']}，而 {later['asset_id']} 在 "
                                f"{later['surveyed_at']}（更晚）反而为 {later['status']}，"
                                f"与生命周期 拟建→在役→退役 矛盾"
                            ),
                            extra={
                                "surveyed_a": lo["surveyed_at"],
                                "surveyed_b": hi["surveyed_at"],
                                "status_a": lo["status"],
                                "status_b": hi["status"],
                            },
                        )
                    )
        return [f for f in found if f is not None]

    def _raise_conflict(
        self,
        conn: sqlite3.Connection,
        ctype: ConflictType,
        row_a: sqlite3.Row,
        row_b: sqlite3.Row,
        seg_a: Segment,
        seg_b: Segment,
        now: str,
        actor: str,
        *,
        reason: str,
        extra: dict,
    ) -> Optional[dict]:
        conflict_id = f"{ctype.value}:{row_a['record_id']}-{row_b['record_id']}"
        existing = conn.execute(
            "SELECT * FROM conflicts WHERE conflict_id = ?", (conflict_id,)
        ).fetchone()
        if existing:
            return None
        detail = {
            "reason": reason,
            "record_a": self._record_json(row_a),
            "record_b": self._record_json(row_b),
            **extra,
        }
        conn.execute(
            "INSERT INTO conflicts(conflict_id, conflict_type, road, record_a, record_b, "
            "detail_json, status, created_at, updated_at, row_version) "
            "VALUES (?,?,?,?,?,?,?,?,?,1)",
            (
                conflict_id,
                ctype.value,
                row_a["road"],
                row_a["record_id"],
                row_b["record_id"],
                json.dumps(detail, ensure_ascii=False),
                ConflictStatus.OPEN.value,
                now,
                now,
            ),
        )
        self._audit(
            conn,
            event_type="conflict_raised",
            entity_type="conflict",
            entity_id=conflict_id,
            actor=actor,
            occurred_at=now,
            payload={"type": ctype.value, "reason": reason},
        )
        return {
            "conflict_id": conflict_id,
            "conflict_type": ctype.value,
            "status": ConflictStatus.OPEN.value,
            "detail": detail,
        }

    # ---- 生效版本 -------------------------------------------------------

    def _adopt(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        effective_from: str,
        conflict_id: Optional[str],
        decision_id: Optional[str],
        actor: str,
        segment: Optional[Segment] = None,
        burial_depth: Optional[float] = None,
        status: Optional[AssetStatus] = None,
        surveyed_at: Optional[str] = None,
        origin_record_ids: Optional[list[int]] = None,
    ) -> sqlite3.Row:
        seg = segment or self._row_segment(row)
        depth = float(burial_depth if burial_depth is not None else row["burial_depth"])
        st = (
            status
            if isinstance(status, AssetStatus)
            else AssetStatus.parse(status if status is not None else row["status"])
        )
        svy = surveyed_at or row["surveyed_at"]
        origins = set(origin_record_ids or [row["record_id"]])
        # 合并会接替已有版本：继承其来源，保证证据链跨多次处置完整可溯
        prior = conn.execute(
            "SELECT * FROM effective_versions WHERE asset_id = ? AND active = 1",
            (row["asset_id"],),
        ).fetchone()
        if prior is not None:
            origins |= set(json.loads(prior["origin_record_ids"]))
        origins = sorted(origins)

        current = prior
        next_no = (
            conn.execute(
                "SELECT COALESCE(MAX(version_no), 0) + 1 FROM effective_versions "
                "WHERE asset_id = ?",
                (row["asset_id"],),
            ).fetchone()[0]
        )
        origin_batches = [
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT batch_id FROM raw_records WHERE record_id IN "
                f"({','.join('?' * len(origins))})",
                origins,
            ).fetchall()
        ]
        cur_conn = conn
        # 必须先让旧版本退出 active，再插入新版本，否则与“每资产至多一个 active
        # 版本”的部分唯一索引冲突；两步处于同一事务，失败会整体回滚。
        if current:
            cur_conn.execute(
                "UPDATE effective_versions SET active = 0, superseded_at = ? "
                "WHERE version_id = ?",
                (effective_from, current["version_id"]),
            )
        cur = cur_conn.execute(
            "INSERT INTO effective_versions(asset_id, version_no, utility_type, road, "
            "span_start, span_end, segment_ref, burial_depth, status, surveyed_at, "
            "origin_record_ids, origin_batch_ids, effective_from, superseded_at, "
            "supersedes_version_id, conflict_id, decision_id, active) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
            (
                row["asset_id"],
                next_no,
                row["utility_type"],
                seg.road,
                seg.start_m,
                seg.end_m,
                seg.as_ref(),
                depth,
                st.value,
                svy,
                json.dumps(origins),
                json.dumps(origin_batches),
                effective_from,
                None,
                current["version_id"] if current else None,
                conflict_id,
                decision_id,
            ),
        )
        new_row = cur_conn.execute(
            "SELECT * FROM effective_versions WHERE version_id = ?", (cur.lastrowid,)
        ).fetchone()
        self._audit(
            conn,
            event_type="version_effective",
            entity_type="asset",
            entity_id=row["asset_id"],
            actor=actor,
            occurred_at=effective_from,
            payload={
                "version_id": new_row["version_id"],
                "version_no": next_no,
                "supersedes": current["version_id"] if current else None,
                "conflict_id": conflict_id,
                "decision_id": decision_id,
            },
        )
        return new_row

    def _retire_other_asset(
        self,
        conn: sqlite3.Connection,
        other: sqlite3.Row,
        *,
        effective_from: str,
        conflict_id: str,
        decision_id: str,
        actor: str,
    ) -> None:
        """跨资产重叠处置后，落败一方的生效版本退出（不删除，保留历史）。"""
        if other["asset_id"] is None:
            return
        current = conn.execute(
            "SELECT * FROM effective_versions WHERE asset_id = ? AND active = 1",
            (other["asset_id"],),
        ).fetchone()
        if not current:
            return
        conn.execute(
            "UPDATE effective_versions SET active = 0, superseded_at = ? "
            "WHERE version_id = ?",
            (effective_from, current["version_id"]),
        )
        self._audit(
            conn,
            event_type="version_superseded_by_decision",
            entity_type="asset",
            entity_id=other["asset_id"],
            actor=actor,
            occurred_at=effective_from,
            payload={
                "version_id": current["version_id"],
                "conflict_id": conflict_id,
                "decision_id": decision_id,
            },
        )

    # ---- 审核决定（乐观锁） ---------------------------------------------

    def _active_matches_record(
        self,
        conn: sqlite3.Connection,
        rec: sqlite3.Row,
        *,
        segment: Optional[Segment] = None,
        burial_depth: Optional[float] = None,
        status: Optional[AssetStatus] = None,
        surveyed_at: Optional[str] = None,
        origin_record_ids: Optional[list[int]] = None,
    ) -> Optional[sqlite3.Row]:
        """若当前生效版本已体现该记录/合并值，则返回该行（避免重复造版本）。"""
        current = conn.execute(
            "SELECT * FROM effective_versions WHERE asset_id = ? AND active = 1",
            (rec["asset_id"],),
        ).fetchone()
        if current is None:
            return None
        seg = segment or self._row_segment(rec)
        depth = float(burial_depth if burial_depth is not None else rec["burial_depth"])
        st = (
            status
            if isinstance(status, AssetStatus)
            else AssetStatus.parse(status if status is not None else rec["status"])
        )
        svy = surveyed_at or rec["surveyed_at"]
        origins = origin_record_ids or [rec["record_id"]]
        if (
            current["segment_ref"] == seg.as_ref()
            and abs(current["burial_depth"] - depth) < 1e-9
            and current["status"] == st.value
            and current["surveyed_at"] == svy
            and json.loads(current["origin_record_ids"]) == origins
        ):
            return current
        return None

    def _active_resolves_pair(
        self, conn: sqlite3.Connection, asset_id: str, record_a_id: int, record_b_id: int
    ) -> Optional[sqlite3.Row]:
        """若该记录对已被现有生效版本裁决（采信/合并），返回该版本。

        同一对记录可能产生多个冲突（重叠、属性矛盾、时间倒置），分别处置时，
        已生效的裁决（尤其是合并值）不应被后续“采信”用原始候选值覆盖。
        """
        current = conn.execute(
            "SELECT * FROM effective_versions WHERE asset_id = ? AND active = 1",
            (asset_id,),
        ).fetchone()
        if current is None:
            return None
        origins = set(json.loads(current["origin_record_ids"]))
        if record_b_id in origins and (
            record_a_id in origins or current["decision_id"] is not None
        ):
            return current
        return None

    def decide_conflict(
        self,
        conflict_id: str,
        *,
        action: str,
        reviewer: str,
        rationale: str,
        expected_version: Optional[int] = None,
        merged: Optional[dict] = None,
        decided_at: Optional[str] = None,
    ) -> dict:
        """对冲突做 采信/驳回/合并 处置。

        - 必须由有权限的审核人执行；
        - 仅 open 冲突可处置；
        - ``expected_version`` 为调用方读取到的 row_version，提交时若已被其他
          审核人推进，则整体失败（不覆盖较新决定）。
        """
        try:
            act = DecisionAction(action)
        except ValueError:
            raise LedgerError(f"未知处置动作 {action!r}，允许 accept/reject/merge")
        if not rationale:
            raise LedgerError("处置必须填写理由 rationale")
        at = normalize_ts(decided_at)

        with self._write() as conn:
            self._require_reviewer(conn, reviewer)
            conflict = conn.execute(
                "SELECT * FROM conflicts WHERE conflict_id = ?", (conflict_id,)
            ).fetchone()
            if not conflict:
                raise NotFoundError(f"冲突 {conflict_id} 不存在")
            if conflict["status"] != ConflictStatus.OPEN.value:
                raise ConflictStateError(
                    f"冲突 {conflict_id} 已由决定 {conflict['resolved_by_decision_id']} "
                    f"处置为 {conflict['status']}，不能重复处置"
                )
            current_rv = conflict["row_version"]
            if expected_version is not None and expected_version != current_rv:
                raise ConflictStateError(
                    f"冲突已被他人更新：客户端版本 {expected_version}，"
                    f"当前版本 {current_rv}；请刷新后重试"
                )

            rec_b = conn.execute(
                "SELECT * FROM raw_records WHERE record_id = ?", (conflict["record_b"],)
            ).fetchone()
            rec_a = conn.execute(
                "SELECT * FROM raw_records WHERE record_id = ?", (conflict["record_a"],)
            ).fetchone()

            decision_id = f"D-{conflict_id}-{current_rv + 1}"
            merged_json = None
            resulting_version = None

            if act is DecisionAction.ACCEPT:
                existing = self._active_matches_record(conn, rec_b)
                if existing is None:
                    existing = self._active_resolves_pair(
                        conn, rec_b["asset_id"], rec_a["record_id"], rec_b["record_id"]
                    )
                if existing is not None:
                    new_row = existing
                else:
                    new_row = self._adopt(
                        conn,
                        rec_b,
                        effective_from=at,
                        conflict_id=conflict_id,
                        decision_id=decision_id,
                        actor=reviewer,
                    )
                resulting_version = new_row["version_id"]
                if rec_a["asset_id"] != rec_b["asset_id"]:
                    self._retire_other_asset(
                        conn, rec_a, effective_from=at,
                        conflict_id=conflict_id, decision_id=decision_id, actor=reviewer,
                    )
                new_status = ConflictStatus.ACCEPTED
            elif act is DecisionAction.REJECT:
                # 驳回候选记录：维持既有生效版本不变，候选永不自动生效
                new_status = ConflictStatus.REJECTED
            else:  # MERGE
                if not merged:
                    raise LedgerError("merge 处置必须提供 merged 合并值")
                seg = parse_segment(
                    merged.get("segment", rec_b["segment_ref"])
                )
                merged_json = json.dumps(merged, ensure_ascii=False)
                new_row = self._adopt(
                    conn,
                    rec_b,
                    effective_from=at,
                    conflict_id=conflict_id,
                    decision_id=decision_id,
                    actor=reviewer,
                    segment=seg,
                    burial_depth=float(
                        merged.get("burial_depth_m", rec_b["burial_depth"])
                    ),
                    status=AssetStatus.parse(
                        merged.get("status", rec_b["status"])
                    ),
                    surveyed_at=normalize_ts(
                        merged.get("surveyed_at", rec_b["surveyed_at"])
                    ),
                    origin_record_ids=[rec_a["record_id"], rec_b["record_id"]],
                )
                resulting_version = new_row["version_id"]
                if rec_a["asset_id"] != rec_b["asset_id"]:
                    self._retire_other_asset(
                        conn, rec_a, effective_from=at,
                        conflict_id=conflict_id, decision_id=decision_id, actor=reviewer,
                    )
                new_status = ConflictStatus.MERGED

            updated = conn.execute(
                "UPDATE conflicts SET status = ?, updated_at = ?, "
                "resolved_by_decision_id = ?, row_version = row_version + 1 "
                "WHERE conflict_id = ? AND status = 'open' AND row_version = ?",
                (
                    new_status.value,
                    at,
                    decision_id,
                    conflict_id,
                    current_rv,
                ),
            )
            if updated.rowcount != 1:
                # 并发下另一决定抢先提交：放弃本次写入
                raise ConflictStateError(
                    f"冲突 {conflict_id} 在处置期间已被其他审核人更新，本次决定未生效"
                )

            conn.execute(
                "INSERT INTO decisions(decision_id, conflict_id, action, reviewer, "
                "rationale, decided_at, merged_json, resulting_version_id, "
                "expected_row_version) VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    decision_id,
                    conflict_id,
                    act.value,
                    reviewer,
                    rationale,
                    at,
                    merged_json,
                    resulting_version,
                    current_rv,
                ),
            )
            self._audit(
                conn,
                event_type="conflict_decided",
                entity_type="conflict",
                entity_id=conflict_id,
                actor=reviewer,
                occurred_at=at,
                payload={
                    "action": act.value,
                    "decision_id": decision_id,
                    "resulting_version_id": resulting_version,
                    "rationale": rationale,
                },
            )

        return {
            "decision_id": decision_id,
            "conflict_id": conflict_id,
            "action": act.value,
            "reviewer": reviewer,
            "rationale": rationale,
            "decided_at": at,
            "resulting_version_id": resulting_version,
            "conflict_row_version": current_rv + 1,
            "conflict_status": new_status.value,
        }

    # ---- 查询 -----------------------------------------------------------

    def get_batch(self, batch_id: str) -> dict:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM source_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if not row:
                raise NotFoundError(f"批次 {batch_id} 不存在")
            records = conn.execute(
                "SELECT * FROM raw_records WHERE batch_id = ? ORDER BY record_id",
                (batch_id,),
            ).fetchall()
            return {
                "batch_id": row["batch_id"],
                "owner": row["owner"],
                "submitted_at": row["submitted_at"],
                "record_count": row["record_count"],
                "raw_hash": row["raw_hash"],
                "records": [self._record_json(r) for r in records],
            }

    def list_conflicts(
        self, status: Optional[str] = None, road: Optional[str] = None
    ) -> list[dict]:
        sql = "SELECT * FROM conflicts WHERE 1=1"
        params: list = []
        if status:
            sql += " AND status = ?"
            params.append(status)
        if road:
            sql += " AND road = ?"
            params.append(road)
        sql += " ORDER BY created_at, conflict_id"
        with self._read() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [self._conflict_json(conn, r) for r in rows]

    def get_conflict(self, conflict_id: str) -> dict:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM conflicts WHERE conflict_id = ?", (conflict_id,)
            ).fetchone()
            if not row:
                raise NotFoundError(f"冲突 {conflict_id} 不存在")
            result = self._conflict_json(conn, row)
            if row["resolved_by_decision_id"]:
                d = conn.execute(
                    "SELECT * FROM decisions WHERE decision_id = ?",
                    (row["resolved_by_decision_id"],),
                ).fetchone()
                result["resolution"] = dict(d)
            return result

    def effective_pipelines(self, road: str, as_of: Optional[str] = None) -> dict:
        """某道路在指定时点（默认当前）的有效管线。"""
        point = normalize_ts(as_of)
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM effective_versions WHERE road = ? "
                "AND effective_from <= ? "
                "AND (superseded_at IS NULL OR superseded_at > ?) "
                "ORDER BY span_start, asset_id, version_no",
                (road, point, point),
            ).fetchall()
            return {
                "road": road,
                "as_of": point,
                "pipelines": [self._version_json(r) for r in rows],
            }

    def asset_versions(self, asset_id: str) -> list[dict]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM effective_versions WHERE asset_id = ? ORDER BY version_no",
                (asset_id,),
            ).fetchall()
            if not rows:
                raise NotFoundError(f"资产 {asset_id} 没有生效版本")
            return [self._version_json(r) for r in rows]

    def version_diff(self, asset_id: str, from_version_no: int, to_version_no: int) -> dict:
        with self._read() as conn:
            a = conn.execute(
                "SELECT * FROM effective_versions WHERE asset_id = ? AND version_no = ?",
                (asset_id, from_version_no),
            ).fetchone()
            b = conn.execute(
                "SELECT * FROM effective_versions WHERE asset_id = ? AND version_no = ?",
                (asset_id, to_version_no),
            ).fetchone()
            if not a or not b:
                raise NotFoundError(
                    f"资产 {asset_id} 的版本 v{from_version_no}/v{to_version_no} 不存在"
                )
            fields = [
                ("segment_ref", "分段"),
                ("burial_depth", "埋深(m)"),
                ("status", "投运状态"),
                ("surveyed_at", "勘测时点"),
                ("utility_type", "管线类型"),
            ]
            changes = []
            for key, label in fields:
                if a[key] != b[key]:
                    changes.append(
                        {
                            "field": key,
                            "label": label,
                            "from": a[key],
                            "to": b[key],
                        }
                    )
            return {
                "asset_id": asset_id,
                "from_version": self._version_json(a),
                "to_version": self._version_json(b),
                "changes": changes,
            }

    def road_history(self, road: str, as_of: Optional[str] = None) -> dict:
        """还原某道路在某一时点采用的资料、冲突处置与责任链。"""
        point = normalize_ts(as_of)
        with self._read() as conn:
            versions = conn.execute(
                "SELECT * FROM effective_versions WHERE road = ? "
                "AND effective_from <= ? "
                "AND (superseded_at IS NULL OR superseded_at > ?) "
                "ORDER BY span_start, asset_id",
                (road, point, point),
            ).fetchall()
            pipelines = []
            for v in versions:
                origin_ids = json.loads(v["origin_record_ids"])
                evidence = []
                for rid in origin_ids:
                    r = conn.execute(
                        "SELECT * FROM raw_records WHERE record_id = ?", (rid,)
                    ).fetchone()
                    if not r:
                        continue
                    batch = conn.execute(
                        "SELECT batch_id, owner, submitted_at, raw_hash FROM source_batches "
                        "WHERE batch_id = ?",
                        (r["batch_id"],),
                    ).fetchone()
                    related = conn.execute(
                        "SELECT c.*, d.action, d.reviewer, d.rationale, d.decided_at, "
                        "d.decision_id FROM conflicts c LEFT JOIN decisions d "
                        "ON c.resolved_by_decision_id = d.decision_id "
                        "WHERE (c.record_a = ? OR c.record_b = ?) "
                        "AND c.created_at <= ? ORDER BY c.created_at",
                        (rid, rid, point),
                    ).fetchall()
                    evidence.append(
                        {
                            "raw_record": self._record_json(r),
                            "source_batch": dict(batch),
                            "conflicts": [
                                {
                                    "conflict_id": c["conflict_id"],
                                    "type": c["conflict_type"],
                                    "status": c["status"],
                                    "detail": json.loads(c["detail_json"]),
                                    "decision": (
                                        {
                                            "decision_id": c["decision_id"],
                                            "action": c["action"],
                                            "reviewer": c["reviewer"],
                                            "rationale": c["rationale"],
                                            "decided_at": c["decided_at"],
                                        }
                                        if c["decision_id"]
                                        and c["decided_at"] is not None
                                        and c["decided_at"] <= point
                                        else None
                                    ),
                                }
                                for c in related
                            ],
                        }
                    )
                decision_chain = []
                if v["decision_id"]:
                    d = conn.execute(
                        "SELECT * FROM decisions WHERE decision_id = ?",
                        (v["decision_id"],),
                    ).fetchone()
                    if d:
                        decision_chain.append(
                            {
                                "decision_id": d["decision_id"],
                                "action": d["action"],
                                "reviewer": d["reviewer"],
                                "rationale": d["rationale"],
                                "decided_at": d["decided_at"],
                                "conflict_id": d["conflict_id"],
                            }
                        )
                pipelines.append(
                    {
                        "effective_version": self._version_json(v),
                        "evidence_chain": evidence,
                        "responsibility_chain": self._responsibility_chain(conn, v, point),
                    }
                )
            return {"road": road, "as_of": point, "pipelines": pipelines}

    def _responsibility_chain(
        self, conn: sqlite3.Connection, version: sqlite3.Row, point: str
    ) -> list[dict]:
        """沿 version -> decision/reviewer -> 来源批次 -> 提交人 构建责任链。"""
        chain: list[dict] = []
        if version["decision_id"]:
            d = conn.execute(
                "SELECT * FROM decisions WHERE decision_id = ?",
                (version["decision_id"],),
            ).fetchone()
            if d:
                chain.append(
                    {
                        "stage": "审核处置",
                        "actor": d["reviewer"],
                        "action": d["action"],
                        "at": d["decided_at"],
                        "reference": d["decision_id"],
                        "note": d["rationale"],
                    }
                )
        for bid in json.loads(version["origin_batch_ids"]):
            b = conn.execute(
                "SELECT * FROM source_batches WHERE batch_id = ?", (bid,)
            ).fetchone()
            if b:
                chain.append(
                    {
                        "stage": "资料提交",
                        "actor": b["owner"],
                        "action": "submit_batch",
                        "at": b["submitted_at"],
                        "reference": b["batch_id"],
                        "note": f"原始资料哈希 {b['raw_hash'][:16]}…",
                    }
                )
        chain.sort(key=lambda x: x["at"])
        return chain

    def audit_tail(self, limit: int = 50) -> list[dict]:
        with self._read() as conn:
            rows = conn.execute(
                "SELECT seq, event_type, entity_type, entity_id, actor, occurred_at, "
                "payload_json, prev_hash, entry_hash FROM audit_log "
                "ORDER BY seq DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [
                {
                    "seq": r["seq"],
                    "event_type": r["event_type"],
                    "entity_type": r["entity_type"],
                    "entity_id": r["entity_id"],
                    "actor": r["actor"],
                    "occurred_at": r["occurred_at"],
                    "payload": json.loads(r["payload_json"]),
                    "prev_hash": r["prev_hash"],
                    "entry_hash": r["entry_hash"],
                }
                for r in reversed(rows)
            ]

    def verify_audit_chain(self) -> dict:
        """校验审计哈希链的完整性。"""
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM audit_log ORDER BY seq"
            ).fetchall()
        prev_hash = None
        for r in rows:
            if r["prev_hash"] != prev_hash:
                return {"ok": False, "broken_at_seq": r["seq"], "reason": "prev_hash 不衔接"}
            expect = self._hash_entry(r, prev_hash)
            if expect != r["entry_hash"]:
                return {"ok": False, "broken_at_seq": r["seq"], "reason": "entry_hash 校验失败"}
            prev_hash = r["entry_hash"]
        return {"ok": True, "entries": len(rows), "head_hash": prev_hash}

    # ---- 审计哈希链 -----------------------------------------------------

    def _audit(
        self,
        conn: sqlite3.Connection,
        *,
        event_type: str,
        entity_type: str,
        entity_id: str,
        actor: str,
        occurred_at: str,
        payload: dict,
    ) -> None:
        last = conn.execute("SELECT entry_hash FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
        prev_hash = last["entry_hash"] if last else None
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        entry_hash = canonical_hash(
            {
                "prev": prev_hash,
                "event_type": event_type,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "actor": actor,
                "occurred_at": occurred_at,
                "payload_json": payload_json,
            }
        )
        conn.execute(
            "INSERT INTO audit_log(event_type, entity_type, entity_id, actor, occurred_at, "
            "payload_json, prev_hash, entry_hash) VALUES (?,?,?,?,?,?,?,?)",
            (
                event_type,
                entity_type,
                entity_id,
                actor,
                occurred_at,
                payload_json,
                prev_hash,
                entry_hash,
            ),
        )

    @staticmethod
    def _hash_entry(row, prev_hash: Optional[str], *, is_row: bool = True) -> str:
        if is_row:
            parts = {
                "event_type": row["event_type"],
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "actor": row["actor"],
                "occurred_at": row["occurred_at"],
                "payload_json": row["payload_json"],
            }
        else:
            parts = row
        return canonical_hash({"prev": prev_hash, **parts})

    # ---- JSON 视图 ------------------------------------------------------

    @staticmethod
    def _row_segment(row: sqlite3.Row) -> Segment:
        return Segment(row["road"], float(row["span_start"]), float(row["span_end"]))

    @staticmethod
    def _record_json(row: sqlite3.Row) -> dict:
        return {
            "record_id": row["record_id"],
            "asset_id": row["asset_id"],
            "utility_type": row["utility_type"],
            "segment": row["segment_ref"],
            "burial_depth_m": row["burial_depth"],
            "status": row["status"],
            "surveyed_at": row["surveyed_at"],
            "source_batch_id": row["batch_id"],
            "attributes": json.loads(row["attributes_json"]),
            "note": row["note"],
            "ingested_at": row["ingested_at"],
            "content_hash": row["content_hash"],
        }

    @staticmethod
    def _version_json(row: sqlite3.Row) -> dict:
        return {
            "version_id": row["version_id"],
            "asset_id": row["asset_id"],
            "version_no": row["version_no"],
            "utility_type": row["utility_type"],
            "segment": row["segment_ref"],
            "burial_depth_m": row["burial_depth"],
            "status": row["status"],
            "surveyed_at": row["surveyed_at"],
            "effective_from": row["effective_from"],
            "superseded_at": row["superseded_at"],
            "supersedes_version_id": row["supersedes_version_id"],
            "origin_record_ids": json.loads(row["origin_record_ids"]),
            "origin_batch_ids": json.loads(row["origin_batch_ids"]),
            "conflict_id": row["conflict_id"],
            "decision_id": row["decision_id"],
            "active": bool(row["active"]),
        }

    def _conflict_json(self, conn: sqlite3.Connection, row: sqlite3.Row) -> dict:
        return {
            "conflict_id": row["conflict_id"],
            "conflict_type": row["conflict_type"],
            "road": row["road"],
            "status": row["status"],
            "row_version": row["row_version"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "resolved_by_decision_id": row["resolved_by_decision_id"],
            "record_a_id": row["record_a"],
            "record_b_id": row["record_b"],
            "detail": json.loads(row["detail_json"]),
        }
