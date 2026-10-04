"""聚宽研究 Notebook 的金融期货取数协议第 02 轮 v2 边界探针。

将本文件全文复制到聚宽“投资研究”的一个 Python 3 Notebook 单元格中执行。
本版验证分钟 Session 与切块边界、早期 TF 空响应范围，以及当前数据可见性。
它只读取少量历史行情，不写文件、不安装依赖、不修改研究目录或本地数据库。
"""

import datetime


PROBE_VERSION = "joinquant_financial_futures_round_02_v2"
RUN_STARTED_AT = datetime.datetime.now().astimezone().isoformat()
probe_started_datetime = datetime.datetime.now().astimezone()
probe_run_date = probe_started_datetime.date()
previous_calendar_date = probe_run_date - datetime.timedelta(days=1)

import hashlib
import importlib
import json
import math
import platform
import sys
import time

import numpy as np
import pandas as pd


RECENT_CONTRACT = "IF2409.CCFX"
RECENT_TRADING_DATE = "2024-06-28"
EARLY_TF_CONTRACT = "TF1303.CCFX"
EARLY_TF_START_DATE = "2012-06-11"
EARLY_TF_END_DATE = "2013-03-08"
FIRST_LIVE_TF_CANDIDATE = "TF1312.CCFX"
FIRST_LIVE_TF_CANDIDATE_DATE = "2013-09-06"
CURRENT_CONTRACT = "IF2609.CCFX"
LOCAL_CALENDAR_WATERMARK_DATE = "2026-08-28"

DAILY_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "pre_close",
    "open_interest",
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


def json_value(value):
    if value is None:
        return None
    if isinstance(value, np.generic):
        return json_value(value.item())
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, tuple):
        return [json_value(item) for item in value]
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return str(value)


def index_values_as_text(frame):
    return [json_value(value) for value in frame.index.tolist()]


def summarize_dataframe(frame):
    index_values = index_values_as_text(frame)
    index_payload = "\n".join(
        "" if value is None else str(value)
        for value in index_values
    ).encode("utf-8")

    preview_positions = list(range(min(2, len(frame))))
    preview_positions.extend(range(max(2, len(frame) - 2), len(frame)))
    preview_rows = []
    for position in sorted(set(preview_positions)):
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
        "index": {
            "python_type": type(frame.index).__name__,
            "names": [json_value(name) for name in frame.index.names],
            "dtype": str(getattr(frame.index, "dtype", None)),
            "timezone": str(getattr(frame.index, "tz", None)),
            "count": len(index_values),
            "first": index_values[0] if index_values else None,
            "last": index_values[-1] if index_values else None,
            "duplicate_count": int(frame.index.duplicated().sum()),
            "sha256": hashlib.sha256(index_payload).hexdigest(),
            "values": index_values if len(index_values) <= 500 else None,
            "values_omitted": len(index_values) > 500,
        },
        "null_counts": {
            str(column): int(count)
            for column, count in frame.isna().sum().items()
        },
        "preview": preview_rows,
    }


def summarize_security_info(value):
    if value is None:
        return {"python_type": "NoneType", "value": None}
    attributes = {}
    for attribute_name in [
        "code",
        "display_name",
        "name",
        "start_date",
        "end_date",
        "type",
        "parent",
    ]:
        if hasattr(value, attribute_name):
            attributes[attribute_name] = json_value(
                getattr(value, attribute_name)
            )
    return {
        "python_type": type(value).__name__,
        "attributes": attributes,
        "repr": repr(value)[:1000],
    }


case_value_by_name = {}


def run_case(case_name, callback, result_kind="dataframe"):
    started_at = time.perf_counter()
    try:
        value = callback()
        case_value_by_name[case_name] = value
        if result_kind == "security_info":
            result = summarize_security_info(value)
        elif isinstance(value, pd.DataFrame):
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
            "result": result,
        }
    except Exception as error:
        return {
            "case": case_name,
            "status": "error",
            "elapsed_seconds": round(time.perf_counter() - started_at, 6),
            "error_type": type(error).__name__,
            "error_message": str(error)[:2000],
        }


def call_api(api_name, *args, **kwargs):
    if jqresearch_api is None:
        raise RuntimeError(
            "jqresearch.api 导入失败: {}".format(jqresearch_import_error)
        )
    value = getattr(jqresearch_api, api_name, None)
    if not callable(value):
        raise AttributeError(
            "jqresearch.api 没有 callable {!r}".format(api_name)
        )
    return value(*args, **kwargs)


def get_price_table(contract_code, start_at, end_at, frequency, fields):
    return call_api(
        "get_price",
        contract_code,
        start_date=start_at,
        end_date=end_at,
        frequency=frequency,
        fields=fields,
        skip_paused=True,
        fq=None,
        panel=False,
        fill_paused=False,
        round=False,
    )


report = {
    "probe_version": PROBE_VERSION,
    "run_started_at": RUN_STARTED_AT,
    "environment": {
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "local_timezone": str(probe_started_datetime.tzinfo),
        "jqresearch_api_available": jqresearch_api is not None,
        "jqresearch_import_error": jqresearch_import_error,
    },
    "coordinates": {
        "recent_contract": RECENT_CONTRACT,
        "recent_trading_date": RECENT_TRADING_DATE,
        "early_tf_contract": EARLY_TF_CONTRACT,
        "early_tf_lifecycle": [
            EARLY_TF_START_DATE,
            EARLY_TF_END_DATE,
        ],
        "first_live_tf_candidate": FIRST_LIVE_TF_CANDIDATE,
        "first_live_tf_candidate_date": FIRST_LIVE_TF_CANDIDATE_DATE,
        "current_contract": CURRENT_CONTRACT,
        "local_calendar_watermark_date": LOCAL_CALENDAR_WATERMARK_DATE,
        "probe_run_date": probe_run_date.isoformat(),
        "previous_calendar_date": previous_calendar_date.isoformat(),
    },
    "cases": [],
}


def append_case(case_name, callback, result_kind="dataframe"):
    report["cases"].append(
        run_case(case_name, callback, result_kind=result_kind)
    )


append_case(
    "early_tf_1303_security_info",
    lambda: call_api("get_security_info", EARLY_TF_CONTRACT),
    result_kind="security_info",
)
append_case(
    "first_live_candidate_tf_1312_security_info",
    lambda: call_api("get_security_info", FIRST_LIVE_TF_CANDIDATE),
    result_kind="security_info",
)

append_case(
    "recent_morning_exact",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 09:30:00",
        "2024-06-28 11:30:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "recent_afternoon_exact",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 13:00:00",
        "2024-06-28 15:00:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "recent_full_day",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 09:30:00",
        "2024-06-28 15:00:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "recent_lunch_gap",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 11:31:00",
        "2024-06-28 13:00:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "recent_single_bar_0931",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 09:31:00",
        "2024-06-28 09:31:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "recent_two_bars_0931_0932",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 09:31:00",
        "2024-06-28 09:32:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "recent_chunk_left",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 09:30:00",
        "2024-06-28 10:30:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "recent_chunk_right_shared_boundary",
    lambda: get_price_table(
        RECENT_CONTRACT,
        "2024-06-28 10:30:00",
        "2024-06-28 11:30:00",
        "1m",
        MINUTE_FIELDS,
    ),
)

append_case(
    "early_tf_1303_lifecycle_daily",
    lambda: get_price_table(
        EARLY_TF_CONTRACT,
        EARLY_TF_START_DATE,
        EARLY_TF_END_DATE,
        "1d",
        DAILY_FIELDS,
    ),
)
append_case(
    "first_live_candidate_tf_1312_daily",
    lambda: get_price_table(
        FIRST_LIVE_TF_CANDIDATE,
        FIRST_LIVE_TF_CANDIDATE_DATE,
        FIRST_LIVE_TF_CANDIDATE_DATE,
        "1d",
        DAILY_FIELDS,
    ),
)
append_case(
    "first_live_candidate_tf_1312_minute_full_day",
    lambda: get_price_table(
        FIRST_LIVE_TF_CANDIDATE,
        "2013-09-06 09:15:00",
        "2013-09-06 15:15:00",
        "1m",
        MINUTE_FIELDS,
    ),
)

append_case(
    "local_watermark_if2609_daily",
    lambda: get_price_table(
        CURRENT_CONTRACT,
        LOCAL_CALENDAR_WATERMARK_DATE,
        LOCAL_CALENDAR_WATERMARK_DATE,
        "1d",
        DAILY_FIELDS,
    ),
)
append_case(
    "previous_calendar_day_if2609_daily",
    lambda: get_price_table(
        CURRENT_CONTRACT,
        previous_calendar_date.isoformat(),
        previous_calendar_date.isoformat(),
        "1d",
        DAILY_FIELDS,
    ),
)
append_case(
    "probe_run_day_if2609_daily",
    lambda: get_price_table(
        CURRENT_CONTRACT,
        probe_run_date.isoformat(),
        probe_run_date.isoformat(),
        "1d",
        DAILY_FIELDS,
    ),
)
append_case(
    "previous_calendar_day_if2609_full_day_minute",
    lambda: get_price_table(
        CURRENT_CONTRACT,
        previous_calendar_date.isoformat() + " 09:30:00",
        previous_calendar_date.isoformat() + " 15:00:00",
        "1m",
        MINUTE_FIELDS,
    ),
)
append_case(
    "probe_run_day_if2609_morning_minute",
    lambda: get_price_table(
        CURRENT_CONTRACT,
        probe_run_date.isoformat() + " 09:30:00",
        probe_run_date.isoformat() + " 11:30:00",
        "1m",
        MINUTE_FIELDS,
    ),
)


def successful_index_set(case_name):
    value = case_value_by_name.get(case_name)
    if not isinstance(value, pd.DataFrame):
        return None
    return set(index_values_as_text(value))


morning_index = successful_index_set("recent_morning_exact")
afternoon_index = successful_index_set("recent_afternoon_exact")
full_day_index = successful_index_set("recent_full_day")
chunk_left_index = successful_index_set("recent_chunk_left")
chunk_right_index = successful_index_set(
    "recent_chunk_right_shared_boundary"
)

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
            sorted(morning_index & afternoon_index)
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
            sorted(chunk_left_index & chunk_right_index)
            if chunk_left_index is not None and chunk_right_index is not None
            else None
        ),
        "unique_union_count": (
            len(chunk_left_index | chunk_right_index)
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

print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_02_BEGIN")
print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_02_END")
