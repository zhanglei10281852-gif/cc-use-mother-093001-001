"""HTTP 接口（标准库实现，无第三方依赖）。

路由：
  POST /batches                           提交来源批次
  GET  /batches/{batch_id}                查询批次原文与记录
  GET  /conflicts?status=&road=           冲突清单
  GET  /conflicts/{conflict_id}           冲突详情（含处置）
  POST /conflicts/{conflict_id}/decisions 采信/驳回/合并（乐观锁 expected_version）
  GET  /roads/{road}/effective?as_of=     某时点有效管线
  GET  /roads/{road}/history?as_of=       当时资料、证据与责任链
  GET  /assets/{asset_id}/versions        资产版本时间线
  GET  /assets/{asset_id}/diff?from=&to=  版本差异
  GET  /audit?limit=                      审计哈希链
  POST /audit/verify                      校验审计链完整性
  GET  /health                            健康检查
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, unquote, urlsplit

from .service import (
    ConflictStateError,
    DuplicateBatchError,
    LedgerError,
    LedgerService,
    NotFoundError,
    PermissionDeniedError,
)


def _json_default(obj: Any) -> Any:
    return obj.value if hasattr(obj, "value") else str(obj)


class HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


Route = tuple[str, re.Pattern, Callable[..., Any]]


class LedgerHttpHandler(BaseHTTPRequestHandler):
    service: LedgerService  # 由 make_server 注入到类属性

    server_version = "PipelineLedger/1.0"

    # ---- 框架 -----------------------------------------------------------

    def _send(self, status: int, body: Any) -> None:
        data = json.dumps(body, ensure_ascii=False, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise HttpError(400, "请求体必须是 JSON")
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HttpError(400, f"JSON 解析失败：{exc}") from exc
        if not isinstance(payload, dict):
            raise HttpError(400, "请求体必须是 JSON 对象")
        return payload

    def _query(self, key: str, default: Optional[str] = None) -> Optional[str]:
        query = parse_qs(urlsplit(self.path).query)
        values = query.get(key)
        return values[0] if values else default

    def _dispatch(self, method: str) -> None:
        parts = urlsplit(self.path)
        path = parts.path.rstrip("/") or "/"
        try:
            for route_method, pattern, handler in self.server.routes:  # type: ignore[attr-defined]
                if route_method != method:
                    continue
                match = pattern.fullmatch(path)
                if match:
                    body = handler(self, **match.groupdict())
                    self._send(200, {"ok": True, "data": body})
                    return
            self._send(404, {"ok": False, "error": f"无此路由：{method} {path}"})
        except HttpError as exc:
            self._send(exc.status, {"ok": False, "error": exc.message})
        except NotFoundError as exc:
            self._send(404, {"ok": False, "error": str(exc)})
        except (DuplicateBatchError, ConflictStateError) as exc:
            self._send(409, {"ok": False, "error": str(exc)})
        except PermissionDeniedError as exc:
            self._send(403, {"ok": False, "error": str(exc)})
        except (LedgerError, ValueError) as exc:
            self._send(400, {"ok": False, "error": str(exc)})

    def do_GET(self) -> None:  # noqa: N802
        self._dispatch("GET")

    def do_POST(self) -> None:  # noqa: N802
        self._dispatch("POST")

    def log_message(self, fmt: str, *args: Any) -> None:  # 精简访问日志
        self.server.log_callback(f"{self.command} {self.path} -> {args[1] if len(args) > 1 else ''}")  # type: ignore[attr-defined]

    # ---- 处理函数 -------------------------------------------------------

    def h_health(self) -> dict:
        return {"status": "ok", "service": "pipeline-ledger"}

    def h_submit_batch(self) -> dict:
        return self.service.submit_batch(self._read_json())

    def h_get_batch(self, batch_id: str) -> dict:
        return self.service.batch(unquote(batch_id))

    def h_list_conflicts(self) -> dict:
        return {
            "conflicts": self.service.conflicts(
                status=self._query("status"), road=self._query("road")
            )
        }

    def h_get_conflict(self, conflict_id: str) -> dict:
        return self.service.conflict(unquote(conflict_id))

    def h_decide(self, conflict_id: str) -> dict:
        payload = self._read_json()
        try:
            expected_raw = payload.get("expected_version")
            expected = int(expected_raw) if expected_raw is not None else None
        except (TypeError, ValueError):
            raise HttpError(400, "expected_version 必须是整数") from None
        return self.service.decide(
            unquote(conflict_id),
            action=payload.get("action", ""),
            reviewer=payload.get("reviewer", ""),
            rationale=payload.get("rationale", ""),
            expected_version=expected,
            merged=payload.get("merged"),
            decided_at=payload.get("decided_at"),
        )

    def h_effective(self, road: str) -> dict:
        return self.service.effective(unquote(road), self._query("as_of"))

    def h_history(self, road: str) -> dict:
        return self.service.history(unquote(road), self._query("as_of"))

    def h_asset_versions(self, asset_id: str) -> dict:
        return {"versions": self.service.asset_versions(unquote(asset_id))}

    def h_asset_diff(self, asset_id: str) -> dict:
        try:
            v_from = int(self._query("from", ""))
            v_to = int(self._query("to", ""))
        except (TypeError, ValueError):
            raise HttpError(400, "必须提供整数查询参数 from 与 to") from None
        return self.service.version_diff(unquote(asset_id), v_from, v_to)

    def h_audit(self) -> dict:
        try:
            limit = int(self._query("limit", "50"))
        except (TypeError, ValueError):
            raise HttpError(400, "limit 必须是整数") from None
        return {"entries": self.service.audit(limit)}

    def h_verify(self) -> dict:
        return self.service.verify()


def _routes() -> list[Route]:
    return [
        ("GET", re.compile(r"/health"), LedgerHttpHandler.h_health),
        ("POST", re.compile(r"/batches"), LedgerHttpHandler.h_submit_batch),
        ("GET", re.compile(r"/batches/(?P<batch_id>[^/]+)"), LedgerHttpHandler.h_get_batch),
        ("GET", re.compile(r"/conflicts"), LedgerHttpHandler.h_list_conflicts),
        ("GET", re.compile(r"/conflicts/(?P<conflict_id>[^/]+)"), LedgerHttpHandler.h_get_conflict),
        ("POST", re.compile(r"/conflicts/(?P<conflict_id>[^/]+)/decisions"), LedgerHttpHandler.h_decide),
        ("GET", re.compile(r"/roads/(?P<road>[^/]+)/effective"), LedgerHttpHandler.h_effective),
        ("GET", re.compile(r"/roads/(?P<road>[^/]+)/history"), LedgerHttpHandler.h_history),
        ("GET", re.compile(r"/assets/(?P<asset_id>[^/]+)/versions"), LedgerHttpHandler.h_asset_versions),
        ("GET", re.compile(r"/assets/(?P<asset_id>[^/]+)/diff"), LedgerHttpHandler.h_asset_diff),
        ("GET", re.compile(r"/audit"), LedgerHttpHandler.h_audit),
        ("POST", re.compile(r"/audit/verify"), LedgerHttpHandler.h_verify),
    ]


def make_server(
    service: LedgerService, host: str = "127.0.0.1", port: int = 8080,
    log_callback: Callable[[str], None] = lambda _m: None,
) -> ThreadingHTTPServer:
    handler = LedgerHttpHandler
    handler.service = service
    server = ThreadingHTTPServer((host, port), handler)
    server.routes = _routes()  # type: ignore[attr-defined]
    server.log_callback = log_callback  # type: ignore[attr-defined]
    return server


def serve(service: LedgerService, host: str = "127.0.0.1", port: int = 8080) -> None:
    import sys

    def log(msg: str) -> None:
        print(msg, file=sys.stderr)

    server = make_server(service, host, port, log)
    print(f"地下管线权威底账服务已启动：http://{host}:{port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
