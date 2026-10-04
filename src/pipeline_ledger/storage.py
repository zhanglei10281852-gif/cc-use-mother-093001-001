"""SQLite 持久化层：只负责建表、连接与低层读写，业务规则在 service 层。"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviewers (
    reviewer_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    role TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source_batches (
    batch_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_versions (
    version_id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_key TEXT NOT NULL,              -- 去重身份：batch_id + 提交方资产编号
    asset_id_label TEXT NOT NULL,
    batch_id TEXT REFERENCES source_batches(batch_id),  -- 合并产物无提交批次，出处见 attributes._merged_from
    version_no INTEGER NOT NULL,
    supersedes_version_id INTEGER,
    road TEXT NOT NULL,
    seg_start REAL NOT NULL,
    seg_end REAL NOT NULL,
    utility TEXT NOT NULL,
    burial_depth_m REAL NOT NULL,
    status TEXT NOT NULL,
    operated_from TEXT,
    operated_to TEXT,
    material TEXT,
    diameter_mm REAL,
    attributes_json TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    prev_record_hash TEXT NOT NULL,
    outcome TEXT NOT NULL DEFAULT 'pending',  -- pending|accepted|rejected|superseded
    effective_from TEXT,
    effective_to TEXT,
    decision_conflict_id TEXT,
    submitted_at TEXT NOT NULL,
    submitted_by TEXT NOT NULL,
    corrected_by TEXT,
    correction_reason TEXT,
    UNIQUE(asset_key, version_no)
);
CREATE INDEX IF NOT EXISTS ix_av_road ON asset_versions(road);
CREATE INDEX IF NOT EXISTS ix_av_outcome ON asset_versions(outcome);
CREATE INDEX IF NOT EXISTS ix_av_batch ON asset_versions(batch_id);

CREATE TABLE IF NOT EXISTS batch_version_refs (
    batch_id TEXT NOT NULL REFERENCES source_batches(batch_id),
    version_id INTEGER NOT NULL REFERENCES asset_versions(version_id),
    PRIMARY KEY (batch_id, version_id)
);

CREATE TABLE IF NOT EXISTS conflicts (
    conflict_id TEXT PRIMARY KEY,
    pair_key TEXT NOT NULL UNIQUE,
    road TEXT NOT NULL,
    kinds_json TEXT NOT NULL,
    details_json TEXT NOT NULL,
    candidate_ids_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',   -- open|resolved
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    resolved_at TEXT,
    action TEXT,
    winner_version_id INTEGER,
    merged_version_id INTEGER,
    decided_by TEXT,
    decision_note TEXT,
    parent_conflict_id TEXT
);
CREATE INDEX IF NOT EXISTS ix_conf_status ON conflicts(status);
CREATE INDEX IF NOT EXISTS ix_conf_road ON conflicts(road);

CREATE TABLE IF NOT EXISTS conflict_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    conflict_id TEXT NOT NULL,
    at TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    from_revision INTEGER NOT NULL,
    to_revision INTEGER NOT NULL,
    winner_version_id INTEGER,
    merged_version_id INTEGER,
    note TEXT,
    parent_conflict_id TEXT,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    chain_hash TEXT NOT NULL,
    prev_chain_hash TEXT NOT NULL
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    if path.parent != Path(""):
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?)", (str(SCHEMA_VERSION),)
        )


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """BEGIN IMMEDIATE：保证审核并发时串行化，不会互相覆盖。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None
