"""应用服务层：HTTP 与 CLI 共用的用例编排。"""
from __future__ import annotations

from typing import Optional

from .store import (
    ConflictStateError,
    DuplicateBatchError,
    LedgerError,
    LedgerStore,
    NotFoundError,
    PermissionDeniedError,
    normalize_ts,
)

DEFAULT_REVIEWERS = {
    "reviewer-li": "李审核（市级测绘中心）",
    "reviewer-wang": "王审核（市级测绘中心）",
}


class LedgerService:
    def __init__(self, store: LedgerStore):
        self.store = store

    # ---- 写 ----

    def submit_batch(self, payload: dict) -> dict:
        return self.store.submit_batch(
            batch_id=payload["batch_id"],
            owner=payload["owner"],
            records=payload["records"],
            submitted_at=payload.get("submitted_at"),
            actor=payload.get("actor"),
        )

    def decide(
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
        return self.store.decide_conflict(
            conflict_id,
            action=action,
            reviewer=reviewer,
            rationale=rationale,
            expected_version=expected_version,
            merged=merged,
            decided_at=decided_at,
        )

    # ---- 读 ----

    def effective(self, road: str, as_of: Optional[str] = None) -> dict:
        return self.store.effective_pipelines(road, as_of)

    def history(self, road: str, as_of: Optional[str] = None) -> dict:
        return self.store.road_history(road, as_of)

    def conflicts(self, status: Optional[str] = None, road: Optional[str] = None) -> list[dict]:
        return self.store.list_conflicts(status, road)

    def conflict(self, conflict_id: str) -> dict:
        return self.store.get_conflict(conflict_id)

    def batch(self, batch_id: str) -> dict:
        return self.store.get_batch(batch_id)

    def asset_versions(self, asset_id: str) -> list[dict]:
        return self.store.asset_versions(asset_id)

    def version_diff(self, asset_id: str, v_from: int, v_to: int) -> dict:
        return self.store.version_diff(asset_id, v_from, v_to)

    def audit(self, limit: int = 50) -> list[dict]:
        return self.store.audit_tail(limit)

    def verify(self) -> dict:
        return self.store.verify_audit_chain()


__all__ = [
    "LedgerService",
    "DEFAULT_REVIEWERS",
    "LedgerError",
    "NotFoundError",
    "DuplicateBatchError",
    "ConflictStateError",
    "PermissionDeniedError",
    "normalize_ts",
]
