"""冲突检测纯逻辑：两条管线记录主张之间的重叠、时间倒置与属性矛盾。

规则全部显式、可解释，输出附带每个判断用到的实际取值。
"""
from __future__ import annotations

from typing import Any

from .contracts import (
    STATUS_IN_SERVICE,
    STATUS_OUT_OF_SERVICE,
    STATUS_PLANNED,
    STATUS_UNKNOWN,
)
from .util import overlap_length, parse_ts

# 里程重叠达到 1 米才视为同一标的物的主张重叠
OVERLAP_MIN_M = 1.0
# 埋深差小于等于 0.30 米视为近似同深（不同权属管线碰撞风险）；
# 同权属记录埋深差大于该阈值视为属性矛盾
DEPTH_TOL_M = 0.30
DIAMETER_TOL_MM = 10.0

# 直接互斥的投运状态组合（unknown 不参与）
_STATUS_CLASSES = {
    STATUS_IN_SERVICE: "live",
    STATUS_OUT_OF_SERVICE: "dead",
    STATUS_PLANNED: "future",
}


def statuses_contradict(a: str, b: str) -> bool:
    if a == STATUS_UNKNOWN or b == STATUS_UNKNOWN or a == b:
        return False
    ca, cb = _STATUS_CLASSES.get(a), _STATUS_CLASSES.get(b)
    return ca is not None and cb is not None and ca != cb


def findings_for_pair(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """返回 {"kinds": [...], "details": [...]}；无冲突时 kinds 为空。"""
    kinds: list[str] = []
    details: list[dict[str, Any]] = []

    if a["road"] != b["road"]:
        return {"kinds": kinds, "details": details}

    ov = overlap_length(a["seg_start"], a["seg_end"], b["seg_start"], b["seg_end"])
    if ov < OVERLAP_MIN_M:
        return {"kinds": kinds, "details": details}

    same_utility = (
        a["utility"] == b["utility"] or a["utility"] == "unknown" or b["utility"] == "unknown"
    )

    if not same_utility:
        # 不同权属管线在重叠位置近似同深 => 空间碰撞
        depth_diff = abs(a["burial_depth_m"] - b["burial_depth_m"])
        if depth_diff <= DEPTH_TOL_M:
            kinds.append("overlap")
            details.append(
                {
                    "type": "cross_utility_collision",
                    "overlap_m": round(ov, 3),
                    "depth_diff_m": round(depth_diff, 3),
                    "utilities": [a["utility"], b["utility"]],
                }
            )
        return {"kinds": kinds, "details": details}

    # 同权属（或未知）：两个来源对同一管段主张重叠
    kinds.append("overlap")
    details.append(
        {
            "type": "same_utility_overlap",
            "overlap_m": round(ov, 3),
            "segments": {
                str(a["version_id"]): [a["seg_start"], a["seg_end"]],
                str(b["version_id"]): [b["seg_start"], b["seg_end"]],
            },
        }
    )

    attr_values: list[dict[str, Any]] = []

    depth_diff = abs(a["burial_depth_m"] - b["burial_depth_m"])
    if depth_diff > DEPTH_TOL_M:
        attr_values.append(
            {
                "field": "burial_depth_m",
                "values": {
                    str(a["version_id"]): a["burial_depth_m"],
                    str(b["version_id"]): b["burial_depth_m"],
                },
                "difference_m": round(depth_diff, 3),
            }
        )

    if a.get("material") and b.get("material") and a["material"] != b["material"]:
        attr_values.append(
            {
                "field": "material",
                "values": {str(a["version_id"]): a["material"], str(b["version_id"]): b["material"]},
            }
        )

    if a.get("diameter_mm") is not None and b.get("diameter_mm") is not None:
        dd = abs(a["diameter_mm"] - b["diameter_mm"])
        if dd > DIAMETER_TOL_MM:
            attr_values.append(
                {
                    "field": "diameter_mm",
                    "values": {
                        str(a["version_id"]): a["diameter_mm"],
                        str(b["version_id"]): b["diameter_mm"],
                    },
                    "difference_mm": round(dd, 3),
                }
            )

    if statuses_contradict(a["status"], b["status"]):
        attr_values.append(
            {
                "field": "status",
                "values": {str(a["version_id"]): a["status"], str(b["version_id"]): b["status"]},
            }
        )

    if attr_values:
        kinds.append("attribute")
        details.append({"type": "attribute_contradictions", "contradictions": attr_values})

    # 时间倒置：
    # 1) 单条记录自身投运起止倒置（from 晚于 to，原始照存但标记冲突）；
    # 2) 双方互斥状态（在役/退役/规划）且主张寿命窗口互不衔接。
    inversion = _time_inversion(a, b)
    if inversion is not None:
        kinds.append("time_inversion")
        details.append({"type": "time_inversion", **inversion})

    return {"kinds": kinds, "details": details}


def _time_inversion(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any] | None:
    windows: dict[str, list[Any]] = {}
    internal: list[str] = []
    parsed: dict[str, tuple] = {}
    for r in (a, b):
        try:
            f = parse_ts(r.get("operated_from"), "operated_from")
            t = parse_ts(r.get("operated_to"), "operated_to")
        except ValueError:
            return None
        parsed[str(r["version_id"])] = (f, t)
        windows[str(r["version_id"])] = [r.get("operated_from"), r.get("operated_to")]
        if f and t and f > t:
            internal.append(str(r["version_id"]))

    if internal:
        return {"reason": "operated_from_after_operated_to",
                "internally_inverted_version_ids": internal, "windows": windows}

    if not statuses_contradict(a["status"], b["status"]):
        return None
    af, at_ = parsed[str(a["version_id"])]
    bf, bt = parsed[str(b["version_id"])]
    if not (af and at_ and bf and bt):
        return None
    latest_start = max(af, bf)
    earliest_end = min(at_, bt)
    if latest_start > earliest_end:
        return {
            "reason": "disjoint_lifetimes_under_contradictory_status",
            "latest_operated_from": latest_start.date().isoformat(),
            "earliest_operated_to": earliest_end.date().isoformat(),
            "windows": windows,
        }
    return None


def self_findings(r: dict[str, Any]) -> dict[str, Any] | None:
    """单条记录自身的时间倒置（operated_from 晚于 operated_to）。"""
    try:
        f = parse_ts(r.get("operated_from"), "operated_from")
        t = parse_ts(r.get("operated_to"), "operated_to")
    except ValueError:
        return None
    if f and t and f > t:
        return {
            "kinds": ["time_inversion"],
            "details": [{
                "type": "time_inversion",
                "reason": "operated_from_after_operated_to",
                "internally_inverted_version_ids": [str(r["version_id"])],
                "windows": {str(r["version_id"]): [r.get("operated_from"), r.get("operated_to")]},
            }],
        }
    return None


def correction_findings(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """更正版本与当前生效版本之间的差异；至少有字段变化，否则上层拒绝。"""
    result = findings_for_pair(old, new)
    changed: list[dict[str, Any]] = []
    for field_name in (
        "seg_start",
        "seg_end",
        "burial_depth_m",
        "status",
        "operated_from",
        "operated_to",
        "material",
        "diameter_mm",
    ):
        ov, nv = old.get(field_name), new.get(field_name)
        if ov != nv:
            changed.append({"field": field_name, "from": ov, "to": nv})
    if old.get("attributes") != new.get("attributes"):
        changed.append({"field": "attributes", "from": old.get("attributes"), "to": new.get("attributes")})
    if changed:
        if "attribute" not in result["kinds"]:
            result["kinds"].append("attribute")
        result["details"].append({"type": "correction_changes", "changes": changed})
    return result
