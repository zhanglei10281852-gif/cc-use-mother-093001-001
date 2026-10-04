"""服务启动入口：python -m pipeline_ledger.serve [--db ledger.db] [--port 8080]"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from .service import DEFAULT_REVIEWERS, LedgerService
from .store import LedgerStore
from .http_api import serve


def main() -> None:
    parser = argparse.ArgumentParser(prog="pipeline-ledger-server")
    parser.add_argument("--db", default=os.environ.get("LEDGER_DB", "ledger.db"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    fresh = not Path(args.db).exists()
    store = LedgerStore(args.db, bootstrap_reviewers=DEFAULT_REVIEWERS if fresh else None)
    serve(LedgerService(store), args.host, args.port)


if __name__ == "__main__":
    main()
