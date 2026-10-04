"""HTTP 接口端到端测试。"""
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from pipeline_ledger.http_api import make_server
from pipeline_ledger.service import DEFAULT_REVIEWERS, LedgerService
from pipeline_ledger.store import LedgerStore

REVIEWER = next(iter(DEFAULT_REVIEWERS))


class HttpTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "ledger.db")
        service = LedgerService(LedgerStore(self.db, DEFAULT_REVIEWERS))
        self.server = make_server(service, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def request(self, method, path, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())

    def make_conflict(self):
        self.request("POST", "/batches", {
            "batch_id": "B1", "owner": "water-company",
            "submitted_at": "2026-09-01T08:00:00Z",
            "records": [{
                "asset_id": "W-1", "utility_type": "water", "segment": "ROAD-8:10-30",
                "burial_depth_m": 1.8, "status": "in_service",
                "surveyed_at": "2026-08-20T00:00:00Z",
            }],
        })
        status, resp = self.request("POST", "/batches", {
            "batch_id": "B2", "owner": "survey-center",
            "submitted_at": "2026-09-05T08:00:00Z",
            "records": [{
                "asset_id": "W-1", "utility_type": "water", "segment": "ROAD-8:10-30",
                "burial_depth_m": 2.6, "status": "in_service",
                "surveyed_at": "2026-09-03T00:00:00Z",
            }],
        })
        self.assertEqual(status, 200)
        return resp["data"]["new_conflicts"]


class HttpApiTests(HttpTestBase):
    def test_health(self):
        status, resp = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertTrue(resp["ok"])

    def test_full_conflict_lifecycle_over_http(self):
        conflicts = self.make_conflict()
        self.assertTrue(conflicts)
        cid = conflicts[0]["conflict_id"]

        status, resp = self.request("GET", f"/conflicts?status=open&road=ROAD-8")
        self.assertEqual(status, 200)
        self.assertEqual(len(resp["data"]["conflicts"]), len(conflicts))
        self.assertEqual(resp["data"]["conflicts"][0]["row_version"], 1)

        # 未决：有效管线仍只有 v1
        status, resp = self.request("GET", "/roads/ROAD-8/effective")
        self.assertEqual(resp["data"]["pipelines"][0]["burial_depth_m"], 1.8)

        # 过期版本号 -> 409
        status, resp = self.request("POST", f"/conflicts/{cid}/decisions", {
            "action": "accept", "reviewer": REVIEWER, "rationale": "x",
            "expected_version": 5,
        })
        self.assertEqual(status, 409)
        self.assertFalse(resp["ok"])

        # 正确提交
        status, resp = self.request("POST", f"/conflicts/{cid}/decisions", {
            "action": "accept", "reviewer": REVIEWER,
            "rationale": "复测可信，采信", "expected_version": 1,
        })
        self.assertEqual(status, 200)
        self.assertEqual(resp["data"]["conflict_row_version"], 2)

        # 重复处置 -> 409
        status, resp = self.request("POST", f"/conflicts/{cid}/decisions", {
            "action": "reject", "reviewer": REVIEWER, "rationale": "y",
        })
        self.assertEqual(status, 409)

        # 生效版本已切换
        status, resp = self.request("GET", "/roads/ROAD-8/effective")
        self.assertEqual(resp["data"]["pipelines"][0]["burial_depth_m"], 2.6)

        # 历史还原含证据与责任链
        status, resp = self.request("GET", "/roads/ROAD-8/history")
        p = resp["data"]["pipelines"][0]
        self.assertTrue(p["responsibility_chain"])
        # 采信后新版本继承双方原始记录作为证据
        evidence_batches = {ev["source_batch"]["batch_id"] for ev in p["evidence_chain"]}
        self.assertEqual(evidence_batches, {"B1", "B2"})

        # 版本差异
        status, resp = self.request("GET", "/assets/W-1/diff?from=1&to=2")
        self.assertEqual(status, 200)
        changed = {c["field"] for c in resp["data"]["changes"]}
        self.assertIn("burial_depth", changed)

    def test_duplicate_batch_409(self):
        body = {
            "batch_id": "B1", "owner": "water-company",
            "records": [{
                "asset_id": "W-1", "utility_type": "water", "segment": "ROAD-8:0-10",
                "burial_depth_m": 1.8, "status": "in_service",
                "surveyed_at": "2026-08-20T00:00:00Z",
            }],
        }
        self.request("POST", "/batches", body)
        status, resp = self.request("POST", "/batches", body)
        self.assertEqual(status, 409)
        self.assertIn("重复", resp["error"])

    def test_forbidden_reviewer(self):
        cid = self.make_conflict()[0]["conflict_id"]
        status, resp = self.request("POST", f"/conflicts/{cid}/decisions", {
            "action": "accept", "reviewer": "nobody", "rationale": "x",
        })
        self.assertEqual(status, 403)

    def test_bad_json_400(self):
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/batches",
            data=b"{not json", method="POST",
        )
        req.add_header("Content-Type", "application/json")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req)
        self.assertEqual(cm.exception.code, 400)

    def test_as_of_query(self):
        self.make_conflict()
        status, resp = self.request(
            "GET", "/roads/ROAD-8/effective?as_of=2026-08-01T00:00:00Z"
        )
        self.assertEqual(status, 200)
        self.assertEqual(resp["data"]["pipelines"], [])

    def test_audit_verify(self):
        self.make_conflict()
        status, resp = self.request("POST", "/audit/verify")
        self.assertEqual(status, 200)
        self.assertTrue(resp["data"]["ok"])
        status, resp = self.request("GET", "/audit?limit=5")
        self.assertEqual(status, 200)
        self.assertLessEqual(len(resp["data"]["entries"]), 5)

    def test_unknown_route_404(self):
        status, _ = self.request("GET", "/nope")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
