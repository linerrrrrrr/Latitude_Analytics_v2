"""聚宽研究 Notebook 的金融期货取数协议第 03 轮 v1 容量探针。

将本文件全文复制到聚宽“投资研究”的一个全新 Python 3 Notebook 单元格中执行。
本轮用 IF2409 全生命周期分钟请求对照九个不重叠逐月请求，逐键逐值检查静默截顶，
同时记录耗时、DataFrame 内存、进程内存高水位以及无效参数的实际异常类型。
本探针只读取有界历史行情，不写文件、不安装依赖、不修改研究目录或本地数据库。
"""

import builtins as py_builtins
import datetime
import gc
import hashlib
import importlib
import json
import math
import platform
import sys
import time
import warnings

import numpy as np
import pandas as pd


PROBE_VERSION = "joinquant_financial_futures_round_03_v1"
RUN_STARTED_AT = datetime.datetime.now().astimezone().isoformat()
probe_started_datetime = datetime.datetime.now().astimezone()

TARGET_CONTRACT = "IF2409.CCFX"
TARGET_START_AT = "2024-01-22 00:00:00"
TARGET_END_AT = "2024-09-20 23:59:59"

MONTH_WINDOWS = [
    ("2024-01", "2024-01-22 00:00:00", "2024-01-31 23:59:59"),
    ("2024-02", "2024-02-01 00:00:00", "2024-02-29 23:59:59"),
    ("2024-03", "2024-03-01 00:00:00", "2024-03-31 23:59:59"),
    ("2024-04", "2024-04-01 00:00:00", "2024-04-30 23:59:59"),
    ("2024-05", "2024-05-01 00:00:00", "2024-05-31 23:59:59"),
    ("2024-06", "2024-06-01 00:00:00", "2024-06-30 23:59:59"),
    ("2024-07", "2024-07-01 00:00:00", "2024-07-31 23:59:59"),
    ("2024-08", "2024-08-01 00:00:00", "2024-08-31 23:59:59"),
    ("2024-09", "2024-09-01 00:00:00", "2024-09-20 23:59:59"),
]

MINUTE_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "open_interest",
]


try:
    jqresearch_api = importlib.import_module("jqresearch.api")
    jqresearch_import_error = None
except Exception as error:
    jqresearch_api = None
    jqresearch_import_error = {
        "error_type": type(error).__name__,
        "error_message": str(error)[:2000],
    }

try:
    resource_module = importlib.import_module("resource")
    resource_import_error = None
except Exception as error:
    resource_module = None
    resource_import_error = {
        "error_type": type(error).__name__,
        "error_message": str(error)[:2000],
    }


def json_value(value):
    if value is None:
        return None
    if py_builtins.isinstance(value, np.generic):
        return json_value(value.item())
    if py_builtins.isinstance(
        value,
        (datetime.datetime, datetime.date, datetime.time, pd.Timestamp),
    ):
        return value.isoformat()
    if py_builtins.isinstance(value, tuple):
        return [json_value(item) for item in value]
    if py_builtins.isinstance(value, (str, bool, int)):
        return value
    if py_builtins.isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def process_peak_rss():
    if resource_module is None:
        return None
    try:
        usage = resource_module.getrusage(resource_module.RUSAGE_SELF)
        return json_value(usage.ru_maxrss)
    except Exception:
        return None


def index_values_as_text(frame):
    return [json_value(value) for value in frame.index.tolist()]


def canonical_frame_digest(frame):
    ordered_frame = frame.sort_index(kind="mergesort")
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "columns": [str(column) for column in ordered_frame.columns],
                "dtypes": [str(dtype) for dtype in ordered_frame.dtypes],
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    )
    row_hashes = pd.util.hash_pandas_object(
        ordered_frame,
        index=True,
    ).values
    digest.update(row_hashes.tobytes())
    return digest.hexdigest()


def summarize_dataframe(frame):
    index_values = index_values_as_text(frame)
    index_payload = "\n".join(
        "" if value is None else str(value)
        for value in index_values
    ).encode("utf-8")

    preview_positions = list(range(py_builtins.min(2, py_builtins.len(frame))))
    preview_positions.extend(
        range(
            py_builtins.max(2, py_builtins.len(frame) - 2),
            py_builtins.len(frame),
        )
    )
    preview_rows = []
    for position in py_builtins.sorted(py_builtins.set(preview_positions)):
        row = frame.iloc[position]
        preview_row = {"__index__": json_value(frame.index[position])}
        for column in frame.columns:
            preview_row[str(column)] = json_value(row[column])
        preview_rows.append(preview_row)

    return {
        "python_type": type(frame).__name__,
        "shape": [int(frame.shape[0]), int(frame.shape[1])],
        "columns": [str(column) for column in frame.columns],
        "dtypes": {
            str(column): str(dtype)
            for column, dtype in frame.dtypes.items()
        },
        "dataframe_memory_bytes": int(
            frame.memory_usage(index=True, deep=True).sum()
        ),
        "canonical_frame_sha256": canonical_frame_digest(frame),
        "index": {
            "python_type": type(frame.index).__name__,
            "names": [json_value(name) for name in frame.index.names],
            "dtype": str(getattr(frame.index, "dtype", None)),
            "timezone": str(getattr(frame.index, "tz", None)),
            "count": py_builtins.len(index_values),
            "first": index_values[0] if index_values else None,
            "last": index_values[-1] if index_values else None,
            "duplicate_count": int(frame.index.duplicated().sum()),
            "sha256": hashlib.sha256(index_payload).hexdigest(),
            "values": index_values if py_builtins.len(index_values) <= 20 else None,
            "values_omitted": py_builtins.len(index_values) > 20,
        },
        "null_counts": {
            str(column): int(count)
            for column, count in frame.isna().sum().items()
        },
        "preview": preview_rows,
    }


case_value_by_name = {}


def run_case(case_name, callback):
    started_at = time.perf_counter()
    peak_rss_before = process_peak_rss()
    caught_warnings = []
    try:
        with warnings.catch_warnings(record=True) as warning_records:
            warnings.simplefilter("always")
            value = callback()
            caught_warnings = [
                {
                    "category": warning.category.__name__,
                    "message": str(warning.message)[:2000],
                }
                for warning in warning_records
            ]
        case_value_by_name[case_name] = value
        if py_builtins.isinstance(value, pd.DataFrame):
            result = summarize_dataframe(value)
        else:
            result = {
                "python_type": type(value).__name__,
                "repr": repr(value)[:2000],
            }
        return {
            "case": case_name,
            "status": "ok",
            "elapsed_seconds": round(time.perf_counter() - started_at, 6),
            "peak_rss_before": peak_rss_before,
            "peak_rss_after": process_peak_rss(),
            "warnings": caught_warnings,
            "result": result,
        }
    except Exception as error:
        return {
            "case": case_name,
            "status": "error",
            "elapsed_seconds": round(time.perf_counter() - started_at, 6),
            "peak_rss_before": peak_rss_before,
            "peak_rss_after": process_peak_rss(),
            "warnings": caught_warnings,
            "error_type": type(error).__name__,
            "error_message": str(error)[:2000],
        }


def call_api(api_name, *args, **kwargs):
    if jqresearch_api is None:
        raise RuntimeError(
            "jqresearch.api 导入失败: {}".format(jqresearch_import_error)
        )
    value = getattr(jqresearch_api, api_name, None)
    if not py_builtins.callable(value):
        raise AttributeError(
            "jqresearch.api 没有 callable {!r}".format(api_name)
        )
    return value(*args, **kwargs)


def get_minute_table(contract_code, start_at, end_at, fields=None, frequency="1m"):
    requested_fields = MINUTE_FIELDS if fields is None else fields
    return call_api(
        "get_price",
        contract_code,
        start_date=start_at,
        end_date=end_at,
        frequency=frequency,
        fields=requested_fields,
        skip_paused=True,
        fq=None,
        panel=False,
        fill_paused=False,
        round=False,
    )


quota_counter_candidates = {}
for candidate_name in [
    "get_query_count",
    "get_api_usage",
    "get_data_usage",
]:
    candidate_value = (
        getattr(jqresearch_api, candidate_name, None)
        if jqresearch_api is not None
        else None
    )
    quota_counter_candidates[candidate_name] = {
        "available": py_builtins.callable(candidate_value),
        "python_type": type(candidate_value).__name__,
    }


report = {
    "probe_version": PROBE_VERSION,
    "run_started_at": RUN_STARTED_AT,
    "environment": {
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "local_timezone": str(probe_started_datetime.tzinfo),
        "pandas_version": pd.__version__,
        "numpy_version": np.__version__,
        "jqresearch_api_available": jqresearch_api is not None,
        "jqresearch_import_error": jqresearch_import_error,
        "resource_module_available": resource_module is not None,
        "resource_import_error": resource_import_error,
        "peak_rss_unit": "KiB on Linux; platform-defined otherwise",
        "peak_rss_at_start": process_peak_rss(),
    },
    "coordinates": {
        "target_contract": TARGET_CONTRACT,
        "target_start_at": TARGET_START_AT,
        "target_end_at": TARGET_END_AT,
        "minute_fields": MINUTE_FIELDS,
        "month_windows": [
            {
                "label": label,
                "start_at": start_at,
                "end_at": end_at,
            }
            for label, start_at, end_at in MONTH_WINDOWS
        ],
    },
    "quota_counter_candidates": quota_counter_candidates,
    "cases": [],
}


def append_case(case_name, callback):
    report["cases"].append(run_case(case_name, callback))


append_case(
    "lifecycle_single_request",
    lambda: get_minute_table(
        TARGET_CONTRACT,
        TARGET_START_AT,
        TARGET_END_AT,
    ),
)

month_case_names = []
for month_label, month_start_at, month_end_at in MONTH_WINDOWS:
    month_case_name = "month_{}".format(month_label.replace("-", "_"))
    month_case_names.append(month_case_name)
    append_case(
        month_case_name,
        lambda start_at=month_start_at, end_at=month_end_at: get_minute_table(
            TARGET_CONTRACT,
            start_at,
            end_at,
        ),
    )

append_case(
    "invalid_contract_code",
    lambda: get_minute_table(
        "NOT_A_REAL_CONTRACT.CCFX",
        "2024-06-28 09:31:00",
        "2024-06-28 09:32:00",
    ),
)
append_case(
    "invalid_field",
    lambda: get_minute_table(
        TARGET_CONTRACT,
        "2024-06-28 09:31:00",
        "2024-06-28 09:32:00",
        fields=["definitely_not_a_field"],
    ),
)
append_case(
    "invalid_frequency",
    lambda: get_minute_table(
        TARGET_CONTRACT,
        "2024-06-28 09:31:00",
        "2024-06-28 09:32:00",
        frequency="not_a_frequency",
    ),
)
append_case(
    "reversed_interval",
    lambda: get_minute_table(
        TARGET_CONTRACT,
        "2024-06-28 09:32:00",
        "2024-06-28 09:31:00",
    ),
)


try:
    whole_frame = case_value_by_name.get("lifecycle_single_request")
    chunk_frames = []
    chunks_available = True
    for month_case_name in month_case_names:
        month_frame = case_value_by_name.get(month_case_name)
        if not py_builtins.isinstance(month_frame, pd.DataFrame):
            chunks_available = False
            break
        chunk_frames.append(month_frame)

    comparison_available = (
        py_builtins.isinstance(whole_frame, pd.DataFrame)
        and chunks_available
        and py_builtins.len(chunk_frames) == py_builtins.len(MONTH_WINDOWS)
    )

    if comparison_available:
        ordered_whole_frame = whole_frame.sort_index(kind="mergesort")
        ordered_chunked_frame = pd.concat(chunk_frames, axis=0).sort_index(
            kind="mergesort"
        )
        whole_index_values = py_builtins.set(index_values_as_text(ordered_whole_frame))
        chunked_index_values = py_builtins.set(
            index_values_as_text(ordered_chunked_frame)
        )
        indexes_equal = ordered_whole_frame.index.equals(
            ordered_chunked_frame.index
        )
        values_equal = ordered_whole_frame.equals(ordered_chunked_frame)
        whole_duplicate_count = int(
            ordered_whole_frame.index.duplicated().sum()
        )
        chunked_duplicate_count = int(
            ordered_chunked_frame.index.duplicated().sum()
        )
        returned_rows_by_month = {
            label: int(frame.shape[0])
            for (label, _, _), frame in zip(MONTH_WINDOWS, chunk_frames)
        }
        report["whole_vs_month_chunks"] = {
            "available": True,
            "whole_row_count": int(ordered_whole_frame.shape[0]),
            "chunked_row_count": int(ordered_chunked_frame.shape[0]),
            "whole_duplicate_count": whole_duplicate_count,
            "chunked_duplicate_count": chunked_duplicate_count,
            "indexes_equal_in_order": bool(indexes_equal),
            "all_values_equal": bool(values_equal),
            "whole_canonical_sha256": canonical_frame_digest(ordered_whole_frame),
            "chunked_canonical_sha256": canonical_frame_digest(ordered_chunked_frame),
            "whole_only_keys_first_20": py_builtins.sorted(
                whole_index_values - chunked_index_values
            )[:20],
            "chunked_only_keys_first_20": py_builtins.sorted(
                chunked_index_values - whole_index_values
            )[:20],
            "returned_rows_by_month": returned_rows_by_month,
            "largest_month_row_count": py_builtins.max(
                returned_rows_by_month.values()
            ),
            "silent_truncation_not_observed_in_tested_range": bool(
                indexes_equal
                and values_equal
                and whole_duplicate_count == 0
                and chunked_duplicate_count == 0
            ),
            "scope_note": (
                "This proves only the tested IF2409 lifecycle request against "
                "the nine non-overlapping month requests; it is not a provider-wide maximum."
            ),
        }
    else:
        report["whole_vs_month_chunks"] = {
            "available": False,
            "reason": "whole request or at least one month request did not return a DataFrame",
        }
except Exception as error:
    report["whole_vs_month_chunks"] = {
        "available": False,
        "comparison_error_type": type(error).__name__,
        "comparison_error_message": str(error)[:2000],
    }


report["request_attempt_count"] = py_builtins.len(report["cases"])
report["resource_after_requests"] = {
    "peak_rss": process_peak_rss(),
    "retained_dataframe_count": py_builtins.len(case_value_by_name),
    "retained_dataframe_memory_bytes": int(
        sum(
            value.memory_usage(index=True, deep=True).sum()
            for value in case_value_by_name.values()
            if py_builtins.isinstance(value, pd.DataFrame)
        )
    ),
}

case_value_by_name.clear()
report["resource_after_cleanup"] = {
    "garbage_collected_object_count": int(gc.collect()),
    "peak_rss": process_peak_rss(),
}


print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_03_BEGIN")
print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_03_END")
