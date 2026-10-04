"""地下管线资产、来源批次与审核决定的基础领域契约。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict

# 管线权属 / 专业类型
UTILITY_TYPES = ("water", "gas", "telecom", "power", "drainage", "unknown")

# 投运状态
STATUS_IN_SERVICE = "in_service"
STATUS_OUT_OF_SERVICE = "out_of_service"
STATUS_PLANNED = "planned"
STATUS_UNKNOWN = "unknown"
OPERATIONAL_STATUSES = (
    STATUS_IN_SERVICE,
    STATUS_OUT_OF_SERVICE,
    STATUS_PLANNED,
    STATUS_UNKNOWN,
)

# 冲突类型
CONFLICT_OVERLAP = "overlap"              # 重叠线段
CONFLICT_TIME_INVERSION = "time_inversion"  # 时间倒置
CONFLICT_ATTRIBUTE = "attribute"          # 属性矛盾
CONFLICT_CORRECTION = "correction"        # 更正待审（新版本待采信/驳回）
CONFLICT_KINDS = (CONFLICT_OVERLAP, CONFLICT_TIME_INVERSION, CONFLICT_ATTRIBUTE, CONFLICT_CORRECTION)

# 审核动作
ACTION_ADOPT = "adopt"      # 系统自动采信（无冲突）
ACTION_ACCEPT = "accept"    # 审核人采信
ACTION_REJECT = "reject"    # 审核人驳回
ACTION_MERGE = "merge"      # 审核人合并
ACTION_OBSOLETED = "obsoleted"  # 系统：旧冲突被更正版本取代
REVIEW_ACTIONS = (ACTION_ACCEPT, ACTION_REJECT, ACTION_MERGE)


def make_segment_ref(road: str, from_m: float, to_m: float) -> str:
    """生成道路分段引用，形如 ``ROAD-8:10-20``（单位：米）。"""
    return f"{road}:{_num(from_m)}-{_num(to_m)}"


def parse_segment_ref(segment_ref: str) -> tuple[str, float, float]:
    """解析道路分段引用 ``ROAD:起-止``，道路名本身可包含 ``-``。"""
    if ":" not in segment_ref:
        raise ValueError(f"分段引用缺少道路分隔符 ':': {segment_ref}")
    road, span = segment_ref.rsplit(":", 1)
    if "-" not in span:
        raise ValueError(f"分段引用缺少里程分隔符 '-': {segment_ref}")
    start, end = span.split("-", 1)
    try:
        from_m, to_m = float(start), float(end)
    except ValueError as exc:
        raise ValueError(f"分段里程不是数字: {segment_ref}") from exc
    if not road or from_m >= to_m:
        raise ValueError(f"分段引用无效: {segment_ref}")
    return road, from_m, to_m


def normalize_status(value: Any) -> str:
    if value in (None, ""):
        return STATUS_UNKNOWN
    text = str(value).strip().lower()
    aliases = {
        "in_service": STATUS_IN_SERVICE, "active": STATUS_IN_SERVICE,
        "运行": STATUS_IN_SERVICE, "在运": STATUS_IN_SERVICE, "投运": STATUS_IN_SERVICE,
        "out_of_service": STATUS_OUT_OF_SERVICE, "retired": STATUS_OUT_OF_SERVICE,
        "停运": STATUS_OUT_OF_SERVICE, "退役": STATUS_OUT_OF_SERVICE, "废弃": STATUS_OUT_OF_SERVICE,
        "planned": STATUS_PLANNED, "规划": STATUS_PLANNED, "待建": STATUS_PLANNED,
        "unknown": STATUS_UNKNOWN, "未知": STATUS_UNKNOWN,
    }
    if text not in aliases:
        raise ValueError(f"未知投运状态: {value}")
    return aliases[text]


def normalize_utility(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    text = str(value).strip().lower()
    aliases = {
        "water": "water", "供水": "water", "自来水": "water",
        "gas": "gas", "燃气": "gas", "天然气": "gas",
        "telecom": "telecom", "通信": "telecom", "通讯": "telecom",
        "power": "power", "电力": "power",
        "drainage": "drainage", "排水": "drainage",
        "unknown": "unknown",
    }
    if text not in aliases:
        raise ValueError(f"未知权属/专业类型: {value}")
    return aliases[text]


def _num(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else str(x)


@dataclass(frozen=True)
class SourceBatch:
    batch_id: str
    owner: str
    record_count: int

    def __post_init__(self) -> None:
        if not self.batch_id or not self.owner or self.record_count < 1:
            raise ValueError("来源批次信息不完整")


@dataclass(frozen=True)
class AssetRecord:
    asset_id: str
    segment_ref: str
    burial_depth_m: float
    source_batch_id: str
    status: str = STATUS_UNKNOWN
    operated_from: str | None = None
    operated_to: str | None = None
    material: str | None = None
    diameter_mm: float | None = None
    attributes: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.asset_id or not self.segment_ref or not self.source_batch_id:
            raise ValueError("资产标识、道路分段和来源不能为空")
        if self.burial_depth_m <= 0:
            raise ValueError("埋深必须大于零")
        if self.status not in OPERATIONAL_STATUSES:
            raise ValueError(f"未知投运状态: {self.status}")
