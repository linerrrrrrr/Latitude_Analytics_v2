"""恢复第 02 轮 v1 已完成请求的内存报告，不重复调用任何聚宽 API。

必须紧接序列化 TypeError 后在同一个未重启的 Notebook 内核中执行。
若 report 或比较用索引变量已经丢失，本单元格会明确失败；此时改跑 round_02_v2。
"""

import builtins
import datetime
import json


RECOVERY_VERSION = "joinquant_financial_futures_round_02_v1_recovery_v1"
required_global_names = [
    "report",
    "morning_index",
    "afternoon_index",
    "full_day_index",
    "chunk_left_index",
    "chunk_right_index",
]
missing_global_names = [
    name
    for name in required_global_names
    if name not in globals()
]
if missing_global_names:
    raise RuntimeError(
        "当前内核已缺少 v1 报告状态，请改跑 round_02_v2；缺少: {}".format(
            ", ".join(missing_global_names)
        )
    )

global_all_value = globals().get("all")
global_all_description = {
    "present": global_all_value is not None,
    "python_type": (
        builtins.type(global_all_value).__name__
        if global_all_value is not None
        else None
    ),
    "declared_module": (
        builtins.str(getattr(global_all_value, "__module__", ""))
        if global_all_value is not None
        else None
    ),
    "repr": (
        builtins.repr(global_all_value)[:1000]
        if global_all_value is not None
        else None
    ),
    "is_builtin_all": global_all_value is builtins.all,
}

session_union_available = (
    morning_index is not None
    and afternoon_index is not None
    and full_day_index is not None
)
shared_boundary_chunks_available = (
    chunk_left_index is not None
    and chunk_right_index is not None
    and morning_index is not None
)

report["comparisons"] = {
    "session_union": {
        "available": session_union_available,
        "morning_afternoon_overlap": (
            builtins.sorted(morning_index & afternoon_index)
            if morning_index is not None and afternoon_index is not None
            else None
        ),
        "union_equals_full_day": (
            (morning_index | afternoon_index) == full_day_index
            if session_union_available
            else None
        ),
    },
    "shared_boundary_chunks": {
        "available": shared_boundary_chunks_available,
        "overlap": (
            builtins.sorted(chunk_left_index & chunk_right_index)
            if chunk_left_index is not None and chunk_right_index is not None
            else None
        ),
        "unique_union_count": (
            builtins.len(chunk_left_index | chunk_right_index)
            if chunk_left_index is not None and chunk_right_index is not None
            else None
        ),
        "union_equals_morning": (
            (chunk_left_index | chunk_right_index) == morning_index
            if shared_boundary_chunks_available
            else None
        ),
    },
}
report["serialization_recovery"] = {
    "recovery_version": RECOVERY_VERSION,
    "recovered_at": datetime.datetime.now().astimezone().isoformat(),
    "repeated_api_requests": False,
    "cause": "unqualified all(generator_expression) did not resolve to builtins.all",
    "global_all": global_all_description,
    "recomputed_fields": [
        "comparisons.session_union",
        "comparisons.shared_boundary_chunks",
    ],
}

report_json = json.dumps(
    report,
    ensure_ascii=False,
    indent=2,
    sort_keys=True,
)
print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_02_BEGIN")
print(report_json)
print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_02_END")
