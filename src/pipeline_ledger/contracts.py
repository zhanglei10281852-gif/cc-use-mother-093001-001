"""地下管线资产、来源批次与审核决定的基础领域契约。

这些类型描述进入底账的*不可变原始信息*：来源批次与原始记录一旦提交
即不得修改；任何更正都以新记录、新版本的形式追加。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from .geometry import Segment, parse_segment


class UtilityType(str, Enum):
    WATER = "water"
    GAS = "gas"
    COMM = "comm"
    OTHER = "other"


class AssetStatus(str, Enum):
    """管线投运状态。"""

    IN_SERVICE = "in_service"   # 在役
    PLANNED = "planned"        # 拟建/在建
    DECOMMISSIONED = "decommissioned"  # 退役

    @classmethod
    def parse(cls, value: str) -> "AssetStatus":
        try:
            return cls(value)
        except ValueError:
            allowed = ", ".join(s.value for s in cls)
            raise ValueError(f"未知投运状态 {value!r}，允许：{allowed}") from None


class ConflictType(str, Enum):
    SPATIAL_OVERLAP = "spatial_overlap"        # 重叠线段
    TIME_INVERSION = "time_inversion"          # 时间倒置
    ATTR_CONTRADICTION = "attr_contradiction"  # 属性矛盾（埋深/状态等）


class ConflictStatus(str, Enum):
    OPEN = "open"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    MERGED = "merged"


class DecisionAction(str, Enum):
    ACCEPT = "accept"  # 采信候选记录
    REJECT = "reject"  # 驳回候选记录
    MERGE = "merge"    # 合并为新生效版本


@dataclass(frozen=True)
class SourceBatch:
    """一个权属单位按批次提交的资料包（不可变）。"""

    batch_id: str
    owner: str
    record_count: int

    def __post_init__(self) -> None:
        if not self.batch_id or not self.owner or self.record_count < 1:
            raise ValueError("来源批次信息不完整")


@dataclass(frozen=True)
class AssetRecord:
    """轻量资产记录视图，保留脚手架期的基础契约。"""

    asset_id: str
    segment_ref: str
    burial_depth_m: float
    source_batch_id: str

    def __post_init__(self) -> None:
        if not self.asset_id or not self.segment_ref or not self.source_batch_id:
            raise ValueError("资产标识、道路分段和来源不能为空")
        if self.burial_depth_m <= 0:
            raise ValueError("埋深必须大于零")


@dataclass(frozen=True)
class RawPipelineRecord:
    """批次中的一条原始管线记录（不可变，逐字节留痕）。"""

    asset_id: str
    utility_type: UtilityType
    segment: Segment
    burial_depth_m: float
    status: AssetStatus
    surveyed_at: str                 # ISO8601 勘测/资料时点
    source_batch_id: str
    attributes: tuple = field(default_factory=tuple)  # ((k, v), ...) 扩展属性
    note: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.asset_id or not self.source_batch_id:
            raise ValueError("资产标识和来源批次不能为空")
        if self.burial_depth_m <= 0:
            raise ValueError("埋深必须大于零")
        if not self.surveyed_at:
            raise ValueError("勘测时点 surveyed_at 不能为空")

    @property
    def segment_ref(self) -> str:
        return self.segment.as_ref()

    def to_asset_record(self) -> AssetRecord:
        return AssetRecord(
            self.asset_id, self.segment_ref, self.burial_depth_m, self.source_batch_id
        )


def build_record(
    *,
    asset_id: str,
    utility_type: str,
    segment: str,
    burial_depth_m: float,
    status: str,
    surveyed_at: str,
    source_batch_id: str,
    attributes=None,
    note: Optional[str] = None,
) -> RawPipelineRecord:
    """从 JSON 友好的标量构造一条原始记录。"""
    attrs = tuple(sorted((attributes or {}).items()))
    return RawPipelineRecord(
        asset_id=asset_id,
        utility_type=UtilityType(utility_type) if isinstance(utility_type, str) else utility_type,
        segment=parse_segment(segment),
        burial_depth_m=float(burial_depth_m),
        status=AssetStatus.parse(status) if isinstance(status, str) else status,
        surveyed_at=surveyed_at,
        source_batch_id=source_batch_id,
        attributes=attrs,
        note=note,
    )
