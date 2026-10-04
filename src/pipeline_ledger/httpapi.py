"""HTTP 接口（仅标准库 http.server）。

路由：
  GET  /health
  POST /reviewers                       注册审核人，返回一次性令牌
  GET  /reviewers/me                    Bearer 令牌自检
  POST /batches                         提交来源批次（幂等去重）
  GET  /batches                         批次清单
  GET  /batches/{batch_id}              批次与原始载荷
  GET  /conflicts?status=&road=         冲突清单
  GET  /conflicts/{id}                  冲突详情（候选与事件链）
  POST /conflicts/{id}/resolve          审核决定（需 Bearer；乐观锁 expected_revision）
  GET  /roads/{road}?at=ISO             某时点有效管线+证据+冲突处置
  GET  /assets/by-label/{label}         资产编号 -> asset_key
  GET  /assets/{asset_key}/history      版本链与相邻版本差异
  GET  /versions/{id}                   版本详情（含不可变哈希）
  GET  /versions/{a}/diff/{b}           任意两版本差异
  GET  /audit?limit=                    责任链审计日志
  GET  /verify                          哈希链完整性自检
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlsplit

from .errors import LedgerError
from .service import LedgerService


def _json_default(obj: Any) -> Any:
    return str(obj)


class _Handler(BaseHTTPRequestHandler):
    service: LedgerService

    # ---- 框架 ----------------------------------------------------------

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    def _send(self, status: int, body: Any) -> None:
        data = json.dumps(body, ensure_ascii=False, indent=2, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status: int, code: str, message: str) -> None:
        self._send(status, {"error": code, "message": message})

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise LedgerError(f"请求体不是合法 JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise LedgerError("请求体必须是 JSON 对象")
        return data

    def _actor(self, required: bool = False) -> dict[str, Any] | None:
        auth = self.headers.get("Authorization", "")
        token = auth[7:].strip() if auth.startswith("Bearer ") else None
        if not token and required:
            self.service.authenticate(None)  # 抛出 401
        if not token:
            return None
        return self.service.authenticate(token)

    def _query(self) -> dict[str, str]:
        q = parse_qs(urlsplit(self.path).query)
        return {k: v[-1] for k, v in q.items()}

    # ---- 路由 ----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        try:
            path = urlsplit(self.path).path.rstrip("/") or "/"
            q = self._query()
            for pattern, verbs, fn in ROUTES:
                m = re.fullmatch(pattern, path)
                if m and method in verbs:
                    return fn(self, q, **m.groupdict())
            self._error(404, "not_found", f"没有 {method} {path} 路由")
        except LedgerError as exc:
            self._error(exc.http_status, type(exc).__name__, str(exc))
        except Exception as exc:  # 最后防线，不向调用方泄漏栈
            self._error(500, "internal_error", f"服务器内部错误: {type(exc).__name__}: {exc}")

    # ---- 端点 ----------------------------------------------------------

    def health(self, q: dict[str, str]) -> None:
        self._send(200, {"status": "ok"})

    def register(self, q: dict[str, str]) -> None:
        body = self._read_json()
        result = self.service.register_reviewer(
            reviewer_id=str(body.get("reviewer_id", "")).strip(),
            display_name=str(body.get("display_name", "")).strip(),
            role=str(body.get("role", "reviewer")),
            token=body.get("token"),
        )
        self._send(201, result)

    def me(self, q: dict[str, str]) -> None:
        self._send(200, self._actor(required=True))

    def submit_batch(self, q: dict[str, str]) -> None:
        body = self._read_json()
        actor = None
        try:
            actor = self._actor(required=False)
        except LedgerError:
            actor = None
        submitted_by = actor["reviewer_id"] if actor else str(body.get("submitted_by", "anonymous"))
        result = self.service.submit_batch(
            batch_id=str(body.get("batch_id", "")).strip(),
            owner=str(body.get("owner", "")).strip(),
            records=body.get("records") or [],
            submitted_by=submitted_by,
        )
        self._send(200, result)

    def list_batches(self, q: dict[str, str]) -> None:
        self._send(200, self.service.list_batches())

    def get_batch(self, q: dict[str, str], batch_id: str) -> None:
        self._send(200, self.service.get_batch(batch_id))

    def list_conflicts(self, q: dict[str, str]) -> None:
        status = q.get("status", "open")
        self._send(200, self.service.list_conflicts(
            status=status if status != "all" else None, road=q.get("road")
        ))

    def get_conflict(self, q: dict[str, str], conflict_id: str) -> None:
        self._send(200, self.service.get_conflict(conflict_id))

    def resolve_conflict(self, q: dict[str, str], conflict_id: str) -> None:
        actor = self._actor(required=True)
        body = self._read_json()
        if "expected_revision" not in body:
            raise LedgerError("缺少 expected_revision（乐观锁）")
        result = self.service.resolve_conflict(
            conflict_id=conflict_id,
            actor=actor,
            action=str(body["action"]),
            expected_revision=int(body["expected_revision"]),
            winner_version_id=body.get("winner_version_id"),
            merged_fields=body.get("merged_fields"),
            note=body.get("note"),
        )
        self._send(200, result)

    def road_view(self, q: dict[str, str], road: str) -> None:
        self._send(200, self.service.road_view(road, q.get("at")))

    def find_by_label(self, q: dict[str, str], label: str) -> None:
        keys = self.service.find_asset_key(label)
        self._send(200, {"asset_id_label": label, "asset_keys": keys})

    def asset_history(self, q: dict[str, str], asset_key: str) -> None:
        self._send(200, self.service.asset_history(asset_key))

    def get_version(self, q: dict[str, str], version_id: str) -> None:
        self._send(200, self.service.get_version(int(version_id)))

    def diff_versions(self, q: dict[str, str], a: str, b: str) -> None:
        self._send(200, self.service.diff_versions(int(a), int(b)))

    def audit(self, q: dict[str, str]) -> None:
        self._send(200, self.service.audit_trail(limit=int(q.get("limit", 100))))

    def verify(self, q: dict[str, str]) -> None:
        self._send(200, self.service.verify_chain())


ROUTES: list[tuple[str, tuple[str, ...], Callable[..., None]]] = [
    (r"/health", ("GET",), _Handler.health),
    (r"/reviewers", ("POST",), _Handler.register),
    (r"/reviewers/me", ("GET",), _Handler.me),
    (r"/batches", ("POST",), _Handler.submit_batch),
    (r"/batches", ("GET",), _Handler.list_batches),
    (r"/batches/(?P<batch_id>[^/]+)", ("GET",), _Handler.get_batch),
    (r"/conflicts", ("GET",), _Handler.list_conflicts),
    (r"/conflicts/(?P<conflict_id>[^/]+)/resolve", ("POST",), _Handler.resolve_conflict),
    (r"/conflicts/(?P<conflict_id>[^/]+)", ("GET",), _Handler.get_conflict),
    (r"/roads/(?P<road>[^/]+)", ("GET",), _Handler.road_view),
    (r"/assets/by-label/(?P<label>[^/]+)", ("GET",), _Handler.find_by_label),
    (r"/assets/(?P<asset_key>.+)/history", ("GET",), _Handler.asset_history),
    (r"/versions/(?P<version_id>\d+)", ("GET",), _Handler.get_version),
    (r"/versions/(?P<a>\d+)/diff/(?P<b>\d+)", ("GET",), _Handler.diff_versions),
    (r"/audit", ("GET",), _Handler.audit),
    (r"/verify", ("GET",), _Handler.verify),
]


def create_server(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    service = LedgerService(db_path)

    class BoundHandler(_Handler):
        pass

    BoundHandler.service = service
    httpd = ThreadingHTTPServer((host, port), BoundHandler)
    httpd.service = service  # type: ignore[attr-defined]
    return httpd


def serve(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> None:
    httpd = create_server(db_path, host, port)
    print(f"地下管线权威底账服务已启动: http://{host}:{port}  (db={db_path})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.service.close()  # type: ignore[attr-defined]


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="地下管线权威底账 HTTP 服务")
    parser.add_argument("--db", default="ledger.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    serve(args.db, args.host, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
