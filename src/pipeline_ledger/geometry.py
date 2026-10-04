"""道路分段与几何比较工具。

分段引用形如 ``ROAD-8:10-20``，表示 ROAD-8 道路桩号 10m 至 20m。
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# 埋深比对容差（米）：差异不超过该值视为一致
DEPTH_TOLERANCE_M = 0.10
# 桩号比对容差（米）
SPAN_TOLERANCE_M = 0.05

_SEGMENT_RE = re.compile(
    r"^\s*(?P<road>[^:]+?)\s*:\s*(?P<start>\d+(?:\.\d+)?)\s*[-~]\s*(?P<end>\d+(?:\.\d+)?)\s*$"
)


@dataclass(frozen=True)
class Segment:
    road: str
    start_m: float
    end_m: float

    def as_ref(self) -> str:
        return f"{self.road}:{fmt_num(self.start_m)}-{fmt_num(self.end_m)}"


def fmt_num(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def parse_segment(ref: str) -> Segment:
    """解析 ``道路:起-止`` 形式的分段引用。"""
    if not isinstance(ref, str):
        raise ValueError("分段引用必须是字符串，例如 ROAD-8:10-20")
    match = _SEGMENT_RE.match(ref)
    if not match:
        raise ValueError(f"无法解析分段引用：{ref!r}（应为 道路:桩号起-桩号止）")
    road = match.group("road").strip()
    start = float(match.group("start"))
    end = float(match.group("end"))
    if not road:
        raise ValueError("道路名称不能为空")
    if end <= start:
        raise ValueError(f"分段 {ref!r} 的终止桩号必须大于起始桩号")
    return Segment(road, start, end)


def make_segment(road: str, start_m: float, end_m: float) -> Segment:
    road = (road or "").strip()
    if not road:
        raise ValueError("道路名称不能为空")
    start = float(start_m)
    end = float(end_m)
    if end <= start:
        raise ValueError(f"分段 {road}:{start}-{end} 的终止桩号必须大于起始桩号")
    return Segment(road, start, end)


def overlaps(a: Segment, b: Segment) -> bool:
    """两个分段是否存在实际重叠（仅端点相接不算重叠）。"""
    if a.road != b.road:
        return False
    return a.start_m < b.end_m - SPAN_TOLERANCE_M and b.start_m < a.end_m - SPAN_TOLERANCE_M


def same_span(a: Segment, b: Segment) -> bool:
    return (
        a.road == b.road
        and abs(a.start_m - b.start_m) <= SPAN_TOLERANCE_M
        and abs(a.end_m - b.end_m) <= SPAN_TOLERANCE_M
    )
