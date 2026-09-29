"""地下管线资产和来源批次的基础契约。"""
from dataclasses import dataclass


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

    def __post_init__(self) -> None:
        if not self.asset_id or not self.segment_ref or not self.source_batch_id:
            raise ValueError("资产标识、道路分段和来源不能为空")
        if self.burial_depth_m <= 0:
            raise ValueError("埋深必须大于零")
