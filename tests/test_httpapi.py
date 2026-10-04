"""HTTP 接口端到端测试：启动真实端口，用 urllib 调用。"""
import json
import os
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline_ledger.httpapi import _Handler  # noqa: E402
from pipeline_ledger.service import LedgerService  # noqa: E402


class HttpTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "ledger.db")
        self.service = LedgerService(self.db)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), type(
            "BoundHandler", (_Handler,), {"service": self.service}))
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)
        self.service.close()
        self.tmp.cleanup()

    def url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def request(self, method, path, body=None, token=None, expect_error=False):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url(path), data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            payload = json.loads(exc.read().decode())
            if expect_error:
                return exc.code, payload
            raise AssertionError(f"{method} {path} 意外 {exc.code}: {payload}") from exc


class HttpApiTests(HttpTestBase):
    def test_health(self):
        status, body = self.request("GET", "/health")
        self.assertEqual(200, status)
        self.assertEqual("ok", body["status"])

    def test_full_workflow_over_http(self):
        # 注册审核人
        _, reg = self.request("POST", "/reviewers", {
            "reviewer_id": "rv", "display_name": "李工", "token": "secret-token"})
        self.assertEqual("secret-token", reg["token"])

        # 未带令牌访问受保护端点 -> 401
        code, err = self.request("GET", "/reviewers/me", expect_error=True)
        self.assertEqual(401, code)
        # 错误令牌 -> 401
        req = urllib.request.Request(self.url("/reviewers/me"))
        req.add_header("Authorization", "Bearer wrong")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(401, cm.exception.code)

        # 三方提交，供水 vs 燃气重叠同深
        wb = {"batch_id": "W1", "owner": "water", "records": [{
            "asset_id": "WP-1", "segment_ref": "ROAD-8:0-60", "burial_depth_m": 1.2,
            "status": "in_service", "operated_from": "2018-05-01T00:00:00Z"}]}
        gb = {"batch_id": "G1", "owner": "gas", "records": [{
            "asset_id": "GP-1", "segment_ref": "ROAD-8:20-80", "burial_depth_m": 1.3,
            "status": "in_service", "operated_from": "2020-01-01T00:00:00Z"}]}
        self.request("POST", "/batches", wb)
        self.request("POST", "/batches", gb)

        # 重复批次幂等
        _, resub = self.request("POST", "/batches", wb)
        self.assertTrue(resub["deduplicated"])

        _, open_list = self.request("GET", "/conflicts?status=open")
        self.assertEqual(1, len(open_list))
        conflict = open_list[0]
        self.assertEqual(1, conflict["revision"])
        self.assertIn("overlap", conflict["kinds"])

        # 未审核人（无令牌）决定 -> 401
        code, _ = self.request("POST", f"/conflicts/{conflict['conflict_id']}/resolve", {
            "action": "reject", "expected_revision": 1}, expect_error=True)
        self.assertEqual(401, code)

        # 过期修订号：伪造 r99 -> 409
        code, err = self.request("POST", f"/conflicts/{conflict['conflict_id']}/resolve", {
            "action": "accept", "expected_revision": 99,
            "winner_version_id": conflict["candidate_ids"][0]},
            token="secret-token", expect_error=True)
        self.assertEqual(409, code)
        self.assertEqual("RevisionStaleError", err["error"])

        # 正常采信
        _, resolved = self.request("POST", f"/conflicts/{conflict['conflict_id']}/resolve", {
            "action": "accept", "expected_revision": 1,
            "winner_version_id": conflict["candidate_ids"][0],
            "note": "供水竣工图与物探一致"}, token="secret-token")
        self.assertEqual(2, resolved["revision"])
        self.assertEqual("rv", resolved["decided_by"])

        # 道路视图含证据与处置
        _, view = self.request("GET", "/roads/ROAD-8")
        self.assertEqual(1, len(view["effective_pipelines"]))
        self.assertIn("W1", view["source_evidence"])
        self.assertTrue(any(
            c["conflict_id"] == conflict["conflict_id"] for c in view["conflicts"]))

        # 历史与差异
        _, labels = self.request("GET", "/assets/by-label/WP-1")
        _, history = self.request("GET", f"/assets/{labels['asset_keys'][0]}/history")
        self.assertGreaterEqual(len(history["versions"]), 1)

        # 审计链自检
        _, verify = self.request("GET", "/verify")
        self.assertTrue(verify["audit_chain_intact"])
        self.assertEqual([], verify["broken_version_ids"])

        # 批次详情保留不可变原始载荷
        _, batch = self.request("GET", "/batches/W1")
        self.assertEqual("W1", batch["batch_id"])
        self.assertIn("payload_json", batch)

    def test_404_route(self):
        code, _ = self.request("GET", "/nope", expect_error=True)
        self.assertEqual(404, code)

    def test_bad_json_400(self):
        req = urllib.request.Request(self.url("/batches"),
                                     data=b"{not json", method="POST")
        req.add_header("Content-Type", "application/json")
        with self.assertRaises(urllib.error.HTTPError) as cm:
            urllib.request.urlopen(req, timeout=5)
        self.assertEqual(400, cm.exception.code)


if __name__ == "__main__":
    unittest.main()
