"""领域错误类型，服务层抛出，HTTP/CLI 层负责翻译。"""
from __future__ import annotations


class LedgerError(Exception):
    """所有底账错误的基类。"""

    http_status = 400


class ValidationError(LedgerError):
    http_status = 400


class NotFoundError(LedgerError):
    http_status = 404


class DuplicateBatchError(LedgerError):
    """同一 batch_id 以不同内容重复提交。"""

    http_status = 409


class RevisionStaleError(LedgerError):
    """审核决定基于过期的冲突修订号（并发覆盖保护）。"""

    http_status = 409


class ConflictStateError(LedgerError):
    """冲突当前状态不允许该操作（例如候选记录已被处置）。"""

    http_status = 409


class AuthenticationError(LedgerError):
    http_status = 401


class PermissionDeniedError(LedgerError):
    http_status = 403
