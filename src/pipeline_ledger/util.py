"""时间与几何小工具。"""
from __future__ import annotations

from datetime import datetime, timezone


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_ts(value: str | None, field: str = "时间") -> datetime | None:
    """解析 ISO8601；朴素时间按 UTC 处理。返回带时区的 datetime。"""
    if value in (None, ""):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field}不是合法的 ISO8601 时间: {value}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).astimezone(timezone.utc).isoformat()


def intervals_overlap(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    """半开区间 [start, end) 是否相交（端点相接不算冲突）。"""
    return a_start < b_end and b_start < a_end


def overlap_length(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))
