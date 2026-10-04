"""核心服务端到端测试：批次去重、不可变版本、冲突、审核并发、时点回放。"""
import os
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline_ledger.errors import (  # noqa: E402
    ConflictStateError,
    DuplicateBatchError,
    PermissionDeniedError,
    RevisionStaleError,
)
from pipeline_ledger.service import LedgerService  # noqa: E402


def water_record(asset_id="WP-1", segment="ROAD-8:0-60", depth=1.2, **extra):
    rec = {"asset_id": asset_id, "segment_ref": segment, "burial_depth_m": depth,
           "status": "in_service", "operated_from": "2018-05-01T00:00:00Z"}
    rec.update(extra)
    return rec


class ServiceTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "ledger.db")
        self.svc = LedgerService(self.db)
        self.reviewer = self.svc.register_reviewer("r1", "审核员甲", token="tok-r1")
        self.actor = self.svc.authenticate("tok-r1")

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()


class BatchDedupTests(ServiceTestBase):
    def test_identical_resubmit_does_not_create_second_asset(self):
        r1 = self.svc.submit_batch("B-1", "water", [water_record()], "unit-a")
        r2 = self.svc.submit_batch("B-1", "water", [water_record()], "unit-a")
        self.assertTrue(r2["deduplicated"])
        self.assertEqual(r1["new_versions"], r2["asset_versions"])
        self.assertEqual(1, self.svc.conn.execute(
            "SELECT COUNT(*) c FROM asset_versions").fetchone()["c"])

    def test_same_batch_id_with_different_payload_is_rejected(self):
        self.svc.submit_batch("B-1", "water", [water_record()], "unit-a")
        with self.assertRaises(DuplicateBatchError):
            self.svc.submit_batch("B-1", "water",
                                  [water_record(depth=2.0)], "unit-a")

    def test_same_asset_in_different_batch_reuses_chain_not_duplicate_asset(self):
        r1 = self.svc.submit_batch("B-1", "water", [water_record()], "unit-a")
        r2 = self.svc.submit_batch("B-2", "water", [water_record()], "unit-a")
        self.assertEqual(r1["new_versions"], r2["reused_versions"])
        keys = self.svc.find_asset_key("WP-1")
        self.assertEqual(["water|WP-1"], keys)

    def test_duplicate_asset_id_within_one_batch_rejected(self):
        with self.assertRaises(Exception):
            self.svc.submit_batch("B-9", "water",
                                  [water_record(), water_record()], "u")


class ConflictDetectionTests(ServiceTestBase):
    def test_cross_utility_overlap_at_similar_depth_flags_conflict(self):
        self.svc.submit_batch("WB", "water", [water_record()], "water-unit")
        self.svc.submit_batch("GB", "gas", [{
            "asset_id": "GP-1", "segment_ref": "ROAD-8:20-80", "burial_depth_m": 1.35,
            "status": "in_service", "operated_from": "2020-01-01T00:00:00Z"}], "gas-unit")
        conflicts = self.svc.list_conflicts()
        self.assertEqual(1, len(conflicts))
        self.assertIn("overlap", conflicts[0]["kinds"])
        self.assertEqual("cross_utility_collision", conflicts[0]["details"][0]["type"])

    def test_distant_depths_do_not_collide(self):
        self.svc.submit_batch("WB", "water", [water_record(depth=1.2)], "w")
        self.svc.submit_batch("TB", "telecom", [{
            "asset_id": "TC-1", "segment_ref": "ROAD-8:0-60", "burial_depth_m": 0.7,
            "status": "planned"}], "t")
        self.assertEqual([], self.svc.list_conflicts())

    def test_same_utility_overlap_with_attribute_contradiction(self):
        self.svc.submit_batch("B1", "water",
                              [water_record("WP-1", depth=1.2, material="PE",
                                            diameter_mm=300)], "w1")
        self.svc.submit_batch("B2", "water",
                              [water_record("WP-2", depth=2.0, material="steel",
                                            diameter_mm=500)], "w2")
        conflicts = self.svc.list_conflicts()
        kinds = {k for c in conflicts for k in c["kinds"]}
        self.assertIn("overlap", kinds)
        self.assertIn("attribute", kinds)

    def test_self_time_inversion_is_recorded_but_raw_data_kept(self):
        self.svc.submit_batch("B1", "water", [water_record(
            status="out_of_service",
            operated_from="2020-01-01T00:00:00Z",
            operated_to="2019-01-01T00:00:00Z")], "w")
        conflicts = self.svc.list_conflicts()
        self.assertTrue(any("time_inversion" in c["kinds"] for c in conflicts))
        v = self.svc.get_version(1)
        self.assertEqual("2020-01-01T00:00:00+00:00", v["operated_from"])
        self.assertEqual("pending", v["outcome"])


class ReviewTests(ServiceTestBase):
    def _pair_conflict(self):
        self.svc.submit_batch("WB", "water", [water_record()], "w")
        self.svc.submit_batch("GB", "gas", [{
            "asset_id": "GP-1", "segment_ref": "ROAD-8:20-80", "burial_depth_m": 1.3,
            "status": "in_service", "operated_from": "2020-01-01T00:00:00Z"}], "g")
        return self.svc.list_conflicts()[0]

    def test_accept_picks_winner_and_rejects_other(self):
        c = self._pair_conflict()
        water_vid, gas_vid = c["candidate_ids"]
        result = self.svc.resolve_conflict(
            c["conflict_id"], self.actor, "accept", 1, winner_version_id=water_vid,
            note="供水资料与竣工图一致")
        self.assertEqual("resolved", result["status"])
        self.assertEqual(2, result["revision"])
        self.assertEqual("accepted", self.svc.get_version(water_vid)["outcome"])
        self.assertEqual("rejected", self.svc.get_version(gas_vid)["outcome"])

    def test_stale_revision_is_rejected_under_concurrency(self):
        c = self._pair_conflict()
        a, b = c["candidate_ids"]
        # 第一个审核人成功
        self.svc.resolve_conflict(c["conflict_id"], self.actor, "accept", 1,
                                  winner_version_id=a)
        # 第二个审核人仍基于 r1 决定 -> 409，不能覆盖较新的决定
        with self.assertRaises(RevisionStaleError):
            self.svc.resolve_conflict(c["conflict_id"], self.actor, "reject", 1)

    def test_parallel_reviews_only_one_wins(self):
        c = self._pair_conflict()
        a, b = c["candidate_ids"]

        def decide(winner):
            svc = LedgerService(self.db)
            actor = svc.authenticate("tok-r1")
            try:
                svc.resolve_conflict(c["conflict_id"], actor, "accept", 1,
                                     winner_version_id=winner)
                return "ok"
            except RevisionStaleError:
                return "stale"
            finally:
                svc.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(decide, [a, b]))
        self.assertEqual(sorted(results), ["ok", "stale"])
        final = self.svc.get_conflict(c["conflict_id"])
        self.assertEqual("resolved", final["status"])

    def test_cannot_decide_twice(self):
        c = self._pair_conflict()
        a, _ = c["candidate_ids"]
        self.svc.resolve_conflict(c["conflict_id"], self.actor, "accept", 1, winner_version_id=a)
        with self.assertRaises(ConflictStateError):
            self.svc.resolve_conflict(c["conflict_id"], self.actor, "reject", 2)

    def test_non_reviewer_forbidden(self):
        self.svc.register_reviewer("viewer", "只读", role="reviewer")
        c = self._pair_conflict()
        # 直接构造无权限角色
        with self.assertRaises(PermissionDeniedError):
            self.svc.resolve_conflict(c["conflict_id"], {"reviewer_id": "x", "role": "guest"},
                                      "reject", 1)

    def test_merge_creates_new_effective_version_with_both_sources(self):
        c = self._pair_conflict()
        result = self.svc.resolve_conflict(
            c["conflict_id"], self.actor, "merge", 1,
            merged_fields={"burial_depth_m": 1.5, "material": "steel",
                           "segment_ref": "ROAD-8:20-60"},
            note="按现场物探取折中埋深")
        merged = self.svc.get_version(result["merged_version_id"])
        self.assertEqual("accepted", merged["outcome"])
        self.assertEqual(1.5, merged["burial_depth_m"])
        sources = merged["attributes"]["_merged_from"]
        self.assertEqual(2, len(sources))
        for vid in c["candidate_ids"]:
            self.assertIn(self.svc.get_version(vid)["outcome"], ("rejected", "superseded"))


class CorrectionAndHistoryTests(ServiceTestBase):
    def test_correction_creates_new_version_and_chain(self):
        self.svc.submit_batch("B1", "water", [water_record(depth=1.2)], "w")
        # 初版无冲突，自动采信
        v1 = self.svc.get_version(1)
        self.assertEqual("accepted", v1["outcome"])

        self.svc.submit_batch("B2", "water", [water_record(depth=1.8)], "w")
        history = self.svc.asset_history("water|WP-1")
        self.assertEqual(2, len(history["versions"]))
        v2 = history["versions"][1]
        self.assertEqual(2, v2["version_no"])
        self.assertEqual(v1["record_hash"], v2["prev_record_hash"])
        self.assertEqual("pending", v2["outcome"])

        diff = history["successive_diffs"][0]
        fields = {ch["field"]: ch for ch in diff["changes"]}
        self.assertEqual(1.2, fields["burial_depth_m"]["from"])
        self.assertEqual(1.8, fields["burial_depth_m"]["to"])

        # 更正待审冲突存在
        open_c = [c for c in self.svc.list_conflicts() if "correction" in c["kinds"]]
        self.assertEqual(1, len(open_c))
        self.svc.resolve_conflict(open_c[0]["conflict_id"], self.actor, "accept", 1,
                                  winner_version_id=v2["version_id"])
        self.assertEqual("superseded", self.svc.get_version(1)["outcome"])
        self.assertIsNotNone(self.svc.get_version(1)["effective_to"])
        self.assertEqual("accepted", self.svc.get_version(v2["version_id"])["outcome"])

    def test_point_in_time_reconstruction(self):
        self.svc.submit_batch("B1", "water", [water_record(depth=1.2)], "w")
        self.svc.submit_batch("B2", "water", [water_record(depth=1.8)], "w")
        correction = [c for c in self.svc.list_conflicts() if "correction" in c["kinds"]][0]
        self.svc.resolve_conflict(correction["conflict_id"], self.actor, "accept", 1,
                                  winner_version_id=2)
        old = self.svc.road_view("ROAD-8", at="2026-10-04T00:00:00+00:00")
        # 所有操作都发生在“现在”附近；用极早/极晚时点验证生效窗口
        future = self.svc.road_view("ROAD-8", at="2099-01-01T00:00:00+00:00")
        self.assertEqual(1, len(future["effective_pipelines"]))
        self.assertEqual(1.8, future["effective_pipelines"][0]["burial_depth_m"])
        ancient = self.svc.road_view("ROAD-8", at="2000-01-01T00:00:00+00:00")
        self.assertEqual(0, len(ancient["effective_pipelines"]))
        # 证据批次随结果返回
        self.assertIn("B2", future["source_evidence"])
        self.assertEqual("water", future["source_evidence"]["B2"]["owner"])


    def test_superseded_record_still_visible_at_historical_point(self):
        self.svc.submit_batch("B1", "water", [water_record(depth=1.2)], "w")
        self.svc.submit_batch("B2", "water", [water_record(depth=1.8)], "w")
        correction = [c for c in self.svc.list_conflicts() if "correction" in c["kinds"]][0]
        # 把 v1 生效时间拨到 2025 年，v2 生效于“现在(2026-10)”，制造清晰历史窗口
        self.svc.conn.execute(
            "UPDATE asset_versions SET effective_from='2025-01-01T00:00:00+00:00'"
            " WHERE version_id=1")
        self.svc.resolve_conflict(correction["conflict_id"], self.actor, "accept", 1,
                                  winner_version_id=2)
        # 旧时点：v1 有效（即便现在已是 superseded）
        old = self.svc.road_view("ROAD-8", at="2025-06-01T00:00:00+00:00")
        self.assertEqual([1.2], [p["burial_depth_m"] for p in old["effective_pipelines"]])
        # 新时点：v2 有效，v1 因 effective_to 退出
        new = self.svc.road_view("ROAD-8", at="2027-06-01T00:00:00+00:00")
        self.assertEqual([1.8], [p["burial_depth_m"] for p in new["effective_pipelines"]])


class RestartPersistenceTests(ServiceTestBase):
    def test_open_conflict_survives_restart(self):
        self.svc.submit_batch("WB", "water", [water_record()], "w")
        self.svc.submit_batch("GB", "gas", [{
            "asset_id": "GP-1", "segment_ref": "ROAD-8:20-80", "burial_depth_m": 1.25,
            "status": "in_service", "operated_from": "2020-01-01T00:00:00Z"}], "g")
        open_id = self.svc.list_conflicts()[0]["conflict_id"]
        self.svc.close()

        svc2 = LedgerService(self.db)
        reopened = svc2.list_conflicts()
        self.assertEqual(open_id, reopened[0]["conflict_id"])
        actor = svc2.authenticate("tok-r1")
        svc2.resolve_conflict(open_id, actor, "accept", 1,
                              winner_version_id=reopened[0]["candidate_ids"][0])
        self.assertEqual("resolved", svc2.get_conflict(open_id)["status"])
        svc2.close()


class ChainIntegrityTests(ServiceTestBase):
    def test_verify_chain_ok(self):
        self.svc.submit_batch("B1", "water", [water_record()], "w")
        self.svc.submit_batch("B2", "water", [water_record(depth=1.9)], "w")
        result = self.svc.verify_chain()
        self.assertTrue(result["audit_chain_intact"])
        self.assertEqual([], result["broken_version_ids"])

    def test_immutable_raw_row_cannot_change_semantics(self):
        self.svc.submit_batch("B1", "water", [water_record(depth=1.2)], "w")
        self.svc.submit_batch("B2", "water", [water_record(depth=1.9)], "w")
        v1 = self.svc.get_version(1)
        self.assertEqual(1.2, v1["burial_depth_m"])  # 原始版本即便被更正也不改动


if __name__ == "__main__":
    unittest.main()
