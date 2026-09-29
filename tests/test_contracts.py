import sys, unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from pipeline_ledger.contracts import AssetRecord, SourceBatch


class LedgerContractTests(unittest.TestCase):
    def test_asset_keeps_source_reference(self):
        batch = SourceBatch("B-1", "gas", 2)
        asset = AssetRecord("A-1", "S-1", 2.4, batch.batch_id)
        self.assertEqual(asset.source_batch_id, "B-1")

    def test_invalid_depth_is_rejected(self):
        with self.assertRaises(ValueError):
            AssetRecord("A-2", "S-2", 0, "B-2")


if __name__ == "__main__":
    unittest.main()
