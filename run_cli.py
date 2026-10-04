#!/usr/bin/env python3
"""命令行冒烟：建库、提交、查重、查冲突、查道路视图。"""
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from pipeline_ledger.service import LedgerService

tmp = tempfile.TemporaryDirectory()
svc = LedgerService(Path(tmp.name) / "smoke.db")
r1 = svc.submit_batch("SURVEY-2026-09", "water", [{
    "asset_id": "A-17", "segment_ref": "ROAD-8:10-20", "burial_depth_m": 1.8,
    "status": "in_service",
}], "water-unit")
r2 = svc.submit_batch("SURVEY-2026-09", "water", [{
    "asset_id": "A-17", "segment_ref": "ROAD-8:10-20", "burial_depth_m": 1.8,
    "status": "in_service",
}], "water-unit")
view = svc.road_view("ROAD-8")
print(json.dumps({
    "first_submit": r1["new_versions"],
    "duplicate_submit_deduplicated": r2["deduplicated"],
    "effective_pipelines": len(view["effective_pipelines"]),
    "chain": svc.verify_chain(),
}, ensure_ascii=False))
svc.close()
