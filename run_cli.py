import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))
from pipeline_ledger.contracts import AssetRecord, SourceBatch

batch = SourceBatch("SURVEY-2026-09", "water", 3)
asset = AssetRecord("A-17", "ROAD-8:10-20", 1.8, batch.batch_id)
print(json.dumps({"asset_id": asset.asset_id, "segment": asset.segment_ref, "source": batch.owner}, ensure_ascii=False))
