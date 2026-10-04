"""底账核心领域与存储测试。"""
import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from pipeline_ledger.service import DEFAULT_REVIEWERS, LedgerService
from pipeline_ledger.store import (
    ConflictStateError,
    DuplicateBatchError,
    LedgerStore,
    NotFoundError,
    PermissionDeniedError,
)

REVIEWER = next(iter(DEFAULT_REVIEWERS))
REVIEWER2 = list(DEFAULT_REVIEWERS)[1]


def rec(asset_id, segment, depth, status="in_service", surveyed_at="2026-09-01T00:00:00+00:00",
        utility="water", **kw):
    d = {
        "asset_id": asset_id,
        "utility_type": utility,
        "segment": segment,
        "burial_depth_m": depth,
        "status": status,
        "surveyed_at": surveyed_at,
    }
    d.update(kw)
    return d


class ServiceTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "ledger.db")
        self.service = LedgerService(LedgerStore(self.db, DEFAULT_REVIEWERS))

    def tearDown(self):
        self.service.store.close()
        self.tmp.cleanup()


class SubmissionTests(ServiceTestBase):
    def test_batch_ingested_and_auto_adopted(self):
        r = self.service.submit_batch({
            "batch_id": "B-W1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:0-50", 1.5)],
        })
        self.assertEqual(r["new_conflicts"], [])
        eff = self.service.effective("ROAD-8")["pipelines"]
        self.assertEqual(len(eff), 1)
        self.assertEqual(eff[0]["asset_id"], "W-1")
        self.assertEqual(eff[0]["version_no"], 1)
        self.assertEqual(eff[0]["origin_batch_ids"], ["B-W1"])

    def test_duplicate_batch_id_rejected(self):
        payload = {
            "batch_id": "B-DUP", "owner": "water-company",
            "records": [rec("W-9", "ROAD-8:0-10", 1.5)],
        }
        self.service.submit_batch(payload)
        with self.assertRaises(DuplicateBatchError):
            self.service.submit_batch(payload)

    def test_duplicate_asset_inside_one_batch_rejected(self):
        with self.assertRaises(DuplicateBatchError):
            self.service.submit_batch({
                "batch_id": "B-DUP2", "owner": "water-company",
                "records": [
                    rec("W-2", "ROAD-8:0-10", 1.5),
                    rec("W-2", "ROAD-8:20-30", 1.6),
                ],
            })

    def test_exact_duplicate_record_across_batches_rejected(self):
        payload = {
            "batch_id": "B-A", "owner": "water-company",
            "records": [rec("W-3", "ROAD-8:0-10", 1.5)],
        }
        self.service.submit_batch(payload)
        dup = dict(payload, batch_id="B-B")
        with self.assertRaises(DuplicateBatchError):
            self.service.submit_batch(dup)
        # 仍然只有一份资产
        self.assertEqual(len(self.service.asset_versions("W-3")), 1)


class ConflictDetectionTests(ServiceTestBase):
    def _seed_overlap(self):
        self.service.submit_batch({
            "batch_id": "B-WATER", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8, surveyed_at="2026-08-20T00:00:00Z")],
        })
        r = self.service.submit_batch({
            "batch_id": "B-SURVEY", "owner": "survey-center",
            "submitted_at": "2026-09-05T08:00:00Z",
            "records": [
                rec("W-1", "ROAD-8:10-30", 2.6, status="in_service",
                    surveyed_at="2026-09-03T00:00:00Z"),
                rec("G-7", "ROAD-8:15-25", 1.2, utility="gas",
                    surveyed_at="2026-09-03T00:00:00Z"),
            ],
        })
        return r

    def test_overlap_and_attribute_conflicts_detected(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        r = self.service.submit_batch({
            "batch_id": "B2", "owner": "water-company",
            "submitted_at": "2026-09-02T08:00:00Z",
            "records": [rec("W-2", "ROAD-8:12-28", 1.8)],
        })
        types = {c["conflict_type"] for c in r["new_conflicts"]}
        self.assertIn("spatial_overlap", types)
        # 埋深一致时不报属性矛盾
        self.assertNotIn("attr_contradiction", types)
        # 冲突未决前，新记录不生效
        self.assertEqual(
            [p["asset_id"] for p in self.service.effective("ROAD-8")["pipelines"]],
            ["W-1"],
        )

    def test_depth_contradiction_reported_explainably(self):
        r = self._seed_overlap()
        # W-1 v.s. 复测 W-1（埋深矛盾）以及 G-7 与谁？gas 只有一条
        attr = [c for c in r["new_conflicts"] if c["conflict_type"] == "attr_contradiction"]
        self.assertTrue(attr)
        self.assertIn("埋深", attr[0]["detail"]["reason"])

    def test_time_inversion_detected(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "gas-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("G-1", "ROAD-1:0-20", 1.0, status="in_service",
                            surveyed_at="2026-08-01T00:00:00Z", utility="gas")],
        })
        r = self.service.submit_batch({
            "batch_id": "B2", "owner": "gas-company",
            "submitted_at": "2026-09-10T08:00:00Z",
            "records": [rec("G-1", "ROAD-1:0-20", 1.0, status="planned",
                            surveyed_at="2026-09-05T00:00:00Z", utility="gas")],
        })
        types = {c["conflict_type"] for c in r["new_conflicts"]}
        self.assertIn("time_inversion", types)


class DecisionTests(ServiceTestBase):
    def _conflicting_pair(self, **submit2):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        defaults = {
            "batch_id": "B2", "owner": "survey-center",
            "submitted_at": "2026-09-05T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 2.6)],
        }
        defaults.update(submit2)
        r = self.service.submit_batch(defaults)
        return r["new_conflicts"]

    def test_accept_creates_new_effective_version(self):
        conflicts = self._conflicting_pair()
        cid = next(c["conflict_id"] for c in conflicts if c["conflict_type"] == "spatial_overlap")
        d = self.service.decide(cid, action="accept", reviewer=REVIEWER,
                                rationale="复测资料经现场核实，采信新埋深",
                                decided_at="2026-09-06T09:00:00Z")
        self.assertEqual(d["conflict_status"], "accepted")
        versions = self.service.asset_versions("W-1")
        self.assertEqual([v["version_no"] for v in versions], [1, 2])
        self.assertFalse(versions[0]["active"])
        self.assertTrue(versions[1]["active"])
        self.assertEqual(versions[1]["burial_depth_m"], 2.6)
        eff = self.service.effective("ROAD-8")["pipelines"]
        self.assertEqual(eff[0]["burial_depth_m"], 2.6)
        self.assertEqual(eff[0]["decision_id"], d["decision_id"])

    def test_reject_keeps_old_version(self):
        conflicts = self._conflicting_pair()
        cid = conflicts[0]["conflict_id"]
        self.service.decide(cid, action="reject", reviewer=REVIEWER,
                            rationale="新资料仪器未校准，驳回",
                            decided_at="2026-09-06T09:00:00Z")
        eff = self.service.effective("ROAD-8")["pipelines"]
        self.assertEqual(len(eff), 1)
        self.assertEqual(eff[0]["burial_depth_m"], 1.8)

    def test_merge_creates_version_with_both_origins(self):
        conflicts = self._conflicting_pair()
        cid = conflicts[0]["conflict_id"]
        self.service.decide(cid, action="merge", reviewer=REVIEWER,
                            rationale="结合两份资料取现场复核值",
                            merged={"segment": "ROAD-8:10-30", "burial_depth_m": 2.2,
                                    "status": "in_service"},
                            decided_at="2026-09-06T09:00:00Z")
        v = self.service.asset_versions("W-1")[-1]
        self.assertEqual(v["burial_depth_m"], 2.2)
        self.assertEqual(sorted(v["origin_batch_ids"]), ["B1", "B2"])
        self.assertEqual(len(v["origin_record_ids"]), 2)

    def test_unknown_reviewer_denied(self):
        conflicts = self._conflicting_pair()
        with self.assertRaises(PermissionDeniedError):
            self.service.decide(conflicts[0]["conflict_id"], action="accept",
                                reviewer="intruder", rationale="无权限")

    def test_cannot_decide_twice(self):
        conflicts = self._conflicting_pair()
        cid = conflicts[0]["conflict_id"]
        self.service.decide(cid, action="accept", reviewer=REVIEWER, rationale="首次处置")
        with self.assertRaises(ConflictStateError):
            self.service.decide(cid, action="reject", reviewer=REVIEWER2, rationale="重复处置")

    def test_stale_expected_version_rejected(self):
        conflicts = self._conflicting_pair()
        cid = conflicts[0]["conflict_id"]
        # 客户端拿着过期的版本号提交
        with self.assertRaises(ConflictStateError):
            self.service.decide(cid, action="accept", reviewer=REVIEWER,
                                rationale="过期版本", expected_version=99)

    def test_concurrent_decisions_only_one_wins(self):
        conflicts = self._conflicting_pair()
        cid = conflicts[0]["conflict_id"]
        errors = []
        results = []

        def vote(action, reviewer):
            try:
                results.append(self.service.decide(
                    cid, action=action, reviewer=reviewer, rationale=f"并发{action}"))
            except ConflictStateError as exc:
                errors.append(str(exc))

        t1 = threading.Thread(target=vote, args=("accept", REVIEWER))
        t2 = threading.Thread(target=vote, args=("reject", REVIEWER2))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        final = self.service.conflict(cid)
        self.assertEqual(final["row_version"], 2)

    def test_cross_asset_overlap_accept_retires_loser(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        r = self.service.submit_batch({
            "batch_id": "B2", "owner": "water-company",
            "submitted_at": "2026-09-05T08:00:00Z",
            "records": [rec("W-2", "ROAD-8:10-30", 2.6)],
        })
        cid = next(c["conflict_id"] for c in r["new_conflicts"]
                   if c["conflict_type"] == "spatial_overlap")
        self.service.decide(cid, action="accept", reviewer=REVIEWER,
                            rationale="实为同一物理管线，采信 W-2 资料")
        ids = sorted(p["asset_id"] for p in self.service.effective("ROAD-8")["pipelines"])
        self.assertEqual(ids, ["W-2"])


class HistoryAndVersionTests(ServiceTestBase):
    def test_as_of_restores_past_state(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        self.service.submit_batch({
            "batch_id": "B2", "owner": "water-company",
            "submitted_at": "2026-09-10T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 2.6)],
        })
        cid = self.service.conflicts(status="open")[0]["conflict_id"]
        self.service.decide(cid, action="accept", reviewer=REVIEWER,
                            rationale="采信复测", decided_at="2026-09-11T08:00:00Z")

        past = self.service.effective("ROAD-8", as_of="2026-09-05T00:00:00Z")["pipelines"]
        self.assertEqual(len(past), 1)
        self.assertEqual(past[0]["burial_depth_m"], 1.8)
        self.assertEqual(past[0]["version_no"], 1)

        now = self.service.effective("ROAD-8", as_of="2026-09-12T00:00:00Z")["pipelines"]
        self.assertEqual(now[0]["burial_depth_m"], 2.6)

    def test_version_diff_lists_changed_fields(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        self.service.submit_batch({
            "batch_id": "B2", "owner": "water-company",
            "submitted_at": "2026-09-10T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-32", 2.6)],
        })
        cid = self.service.conflicts(status="open")[0]["conflict_id"]
        self.service.decide(cid, action="accept", reviewer=REVIEWER, rationale="采信")
        diff = self.service.version_diff("W-1", 1, 2)
        fields = {c["field"] for c in diff["changes"]}
        self.assertIn("burial_depth", fields)
        self.assertIn("segment_ref", fields)

    def test_history_contains_evidence_and_responsibility_chain(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        self.service.submit_batch({
            "batch_id": "B2", "owner": "survey-center",
            "submitted_at": "2026-09-05T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 2.6)],
        })
        cid = self.service.conflicts(status="open")[0]["conflict_id"]
        self.service.decide(cid, action="merge", reviewer=REVIEWER,
                            rationale="合并采信", merged={"burial_depth_m": 2.1},
                            decided_at="2026-09-06T08:00:00Z")
        hist = self.service.history("ROAD-8", as_of="2026-09-10T00:00:00Z")
        p = hist["pipelines"][0]
        batches = {ev["source_batch"]["batch_id"] for ev in p["evidence_chain"]}
        self.assertEqual(batches, {"B1", "B2"})
        chain_actors = {s["actor"] for s in p["responsibility_chain"]}
        self.assertIn("water-company", chain_actors)
        self.assertIn(REVIEWER, chain_actors)

    def test_history_excludes_future_decisions(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        self.service.submit_batch({
            "batch_id": "B2", "owner": "water-company",
            "submitted_at": "2026-09-10T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 2.6)],
        })
        cid = self.service.conflicts(status="open")[0]["conflict_id"]
        self.service.decide(cid, action="accept", reviewer=REVIEWER,
                            rationale="未来才决定", decided_at="2026-09-20T08:00:00Z")
        hist = self.service.history("ROAD-8", as_of="2026-09-15T00:00:00Z")
        # 决定尚未生效：仍是 v1，证据中的决定为 None
        self.assertEqual(hist["pipelines"][0]["effective_version"]["version_no"], 1)
        decisions = [
            c["decision"]
            for ev in hist["pipelines"][0]["evidence_chain"]
            for c in ev["conflicts"]
        ]
        self.assertTrue(decisions and all(d is None for d in decisions))


class PersistenceTests(ServiceTestBase):
    def test_open_conflicts_survive_restart(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        self.service.submit_batch({
            "batch_id": "B2", "owner": "water-company",
            "submitted_at": "2026-09-02T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 2.6)],
        })
        self.service.store.close()

        reopened = LedgerService(LedgerStore(self.db))
        open_conflicts = reopened.conflicts(status="open")
        self.assertTrue(open_conflicts)
        cid = open_conflicts[0]["conflict_id"]
        d = reopened.decide(cid, action="accept", reviewer=REVIEWER,
                            rationale="重启后继续处置")
        self.assertEqual(d["conflict_status"], "accepted")
        self.assertEqual(reopened.effective("ROAD-8")["pipelines"][0]["burial_depth_m"], 2.6)

    def test_raw_batch_payload_retained_verbatim(self):
        payload = {
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8, note="原件备注")],
        }
        self.service.submit_batch(payload)
        got = self.service.batch("B1")
        self.assertEqual(got["record_count"], 1)
        self.assertTrue(got["raw_hash"])
        self.assertEqual(got["records"][0]["note"], "原件备注")

    def test_missing_entities_raise_not_found(self):
        with self.assertRaises(NotFoundError):
            self.service.conflict("nope:1-2")
        with self.assertRaises(NotFoundError):
            self.service.batch("nope")


class AuditChainTests(ServiceTestBase):
    def test_chain_verifies(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        result = self.service.verify()
        self.assertTrue(result["ok"])
        self.assertGreaterEqual(result["entries"], 2)

    def test_tampering_detected(self):
        self.service.submit_batch({
            "batch_id": "B1", "owner": "water-company",
            "records": [rec("W-1", "ROAD-8:10-30", 1.8)],
        })
        import sqlite3
        conn = sqlite3.connect(self.db)
        conn.execute("UPDATE audit_log SET actor = 'forged' WHERE seq = 1")
        conn.commit()
        conn.close()
        self.assertFalse(self.service.verify()["ok"])


if __name__ == "__main__":
    unittest.main()
