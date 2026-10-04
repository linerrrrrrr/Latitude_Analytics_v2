"""聚宽研究 Notebook 的金融期货取数协议第 01 轮探针。

将本文件全文复制到聚宽“投资研究”的一个空白 Python 3 Notebook 单元格中执行。
本探针只读取少量历史行情并打印结构化结果，不写文件、不安装依赖、不修改研究目录。
执行完成后，请复制 BEGIN/END 标记之间的完整 JSON 返回本地项目继续分析。
"""

API_NAMES_TO_PROBE = (
    "get_price",
    "get_extras",
    "get_security_info",
    "get_all_securities",
    "get_query_count",
)
preimport_api_by_name = dict(
    (api_name, globals().get(api_name)) for api_name in API_NAMES_TO_PROBE
)
try:
    from jqdata import *
    jqdata_star_import_error = None
except Exception as error:
    jqdata_star_import_error = "{}: {}".format(type(error).__name__, str(error))

import datetime
import importlib
import inspect
import json
import math
import platform
import sys
import time

import jqdata as jqdata_module
import numpy as np
import pandas as pd


PROBE_VERSION = "joinquant_financial_futures_round_01_v2"
RECENT_CONTRACT = "IF2409.CCFX"
RECENT_COMPARISON_CONTRACT = "IH2409.CCFX"
RECENT_TRADING_DATE = "2024-06-28"
EARLY_TF_CONTRACT = "TF1303.CCFX"
EARLY_TF_TRADING_DATE = "2012-06-11"

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


def dataframe_preview(frame, row_count=2):
    preview_rows = []
    if frame.empty:
        return preview_rows

    positions = list(range(min(row_count, len(frame))))
    tail_start = max(row_count, len(frame) - row_count)
    positions.extend(range(tail_start, len(frame)))
    positions = sorted(set(positions))

    for position in positions:
        row = frame.iloc[position]
        preview_row = {"__index__": json_value(frame.index[position])}
        for column in frame.columns:
            preview_row[str(column)] = json_value(row[column])
        preview_rows.append(preview_row)
    return preview_rows


def summarize_dataframe(frame):
    index = frame.index
    summary = {
        "python_type": type(frame).__name__,
        "shape": [int(frame.shape[0]), int(frame.shape[1])],
        "columns": [str(column) for column in frame.columns],
        "dtypes": {str(column): str(dtype) for column, dtype in frame.dtypes.items()},
        "index": {
            "python_type": type(index).__name__,
            "name": json_value(index.name),
            "dtype": str(getattr(index, "dtype", None)),
            "timezone": str(getattr(index, "tz", None)),
            "first": json_value(index[0]) if len(index) else None,
            "last": json_value(index[-1]) if len(index) else None,
            "duplicate_count": int(index.duplicated().sum()),
        },
        "null_counts": {
            str(column): int(count) for column, count in frame.isna().sum().items()
        },
        "preview": dataframe_preview(frame),
    }
    if "code" in frame.columns:
        summary["returned_codes"] = sorted(
            {str(value) for value in frame["code"].dropna().tolist()}
        )
    return summary


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
            attributes[attribute_name] = json_value(getattr(value, attribute_name))
    return {
        "python_type": type(value).__name__,
        "attributes": attributes,
        "repr": repr(value)[:1000],
    }


def summarize_result(value, result_kind):
    if result_kind == "dataframe":
        if not isinstance(value, pd.DataFrame):
            return {
                "python_type": type(value).__name__,
                "unexpected_value": repr(value)[:1000],
            }
        return summarize_dataframe(value)
    if result_kind == "security_info":
        return summarize_security_info(value)
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    return {"python_type": type(value).__name__, "value": json_value(value)}


def run_case(case_name, callback, result_kind="dataframe"):
    started_at = time.perf_counter()
    try:
        value = callback()
        return {
            "case": case_name,
            "status": "ok",
            "elapsed_seconds": round(time.perf_counter() - started_at, 6),
            "result": summarize_result(value, result_kind),
        }
    except Exception as error:
        return {
            "case": case_name,
            "status": "error",
            "elapsed_seconds": round(time.perf_counter() - started_at, 6),
            "error_type": type(error).__name__,
            "error_message": str(error)[:2000],
        }


def optional_module_version(module_name):
    try:
        module = importlib.import_module(module_name)
    except Exception as error:
        return {
            "available": False,
            "error_type": type(error).__name__,
            "error_message": str(error)[:500],
        }
    return {
        "available": True,
        "version": str(getattr(module, "__version__", "unknown")),
    }


def resolve_api_callable(callable_name):
    preimport_value = preimport_api_by_name.get(callable_name)
    if callable(preimport_value):
        return preimport_value, "platform_global_preimport"

    imported_value = globals().get(callable_name)
    if callable(imported_value):
        return imported_value, "jqdata_star_import_global"

    module_value = getattr(jqdata_module, callable_name, None)
    if callable(module_value):
        return module_value, "jqdata_module_attribute"
    return None, None


def call_api(callable_name, *args, **kwargs):
    value, resolution = resolve_api_callable(callable_name)
    if value is None:
        raise AttributeError(
            "jqdata API {!r} 既未由 import * 暴露，也不是模块属性".format(
                callable_name
            )
        )
    return value(*args, **kwargs)


def callable_signature(callable_name):
    value, resolution = resolve_api_callable(callable_name)
    preimport_value = preimport_api_by_name.get(callable_name)
    imported_value = globals().get(callable_name)
    module_value = getattr(jqdata_module, callable_name, None)
    resolution_details = {
        "preimport_global_callable": callable(preimport_value),
        "post_import_global_callable": callable(imported_value),
        "module_attribute_callable": callable(module_value),
        "resolution": resolution,
    }
    if value is None:
        resolution_details["available"] = False
        return resolution_details
    try:
        signature = str(inspect.signature(value))
    except Exception as error:
        signature = "unavailable: {}: {}".format(type(error).__name__, str(error))
    resolution_details["available"] = True
    resolution_details["signature"] = signature
    resolution_details["selected_repr"] = repr(value)[:1000]
    return resolution_details


report = {
    "probe_version": PROBE_VERSION,
    "executed_at": datetime.datetime.now().astimezone().isoformat(),
    "jqdata_star_import_error": jqdata_star_import_error,
    "environment": {
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "local_timezone": str(datetime.datetime.now().astimezone().tzinfo),
        "modules": {
            "jqdata": optional_module_version("jqdata"),
            "numpy": optional_module_version("numpy"),
            "pandas": optional_module_version("pandas"),
            "pyarrow": optional_module_version("pyarrow"),
        },
    },
    "callables": {
        name: callable_signature(name)
        for name in API_NAMES_TO_PROBE
    },
    "local_calendar_expectations": {
        "recent": {
            "contract": RECENT_CONTRACT,
            "trading_date": RECENT_TRADING_DATE,
            "daily_expected_rows": 1,
            "minute_sessions": [
                {
                    "session_number": 1,
                    "interval": "(2024-06-28 09:30:00, 2024-06-28 11:30:00]",
                    "expected_rows": 120,
                },
                {
                    "session_number": 2,
                    "interval": "(2024-06-28 13:00:00, 2024-06-28 15:00:00]",
                    "expected_rows": 120,
                },
            ],
        },
        "early_tf": {
            "contract": EARLY_TF_CONTRACT,
            "trading_date": EARLY_TF_TRADING_DATE,
            "daily_expected_rows": 1,
            "minute_sessions": [
                {
                    "session_number": 1,
                    "interval": "(2012-06-11 09:15:00, 2012-06-11 11:30:00]",
                    "expected_rows": 135,
                },
                {
                    "session_number": 2,
                    "interval": "(2012-06-11 13:00:00, 2012-06-11 15:15:00]",
                    "expected_rows": 135,
                },
            ],
        },
    },
    "cases": [],
}

report["cases"].append(
    run_case(
        "recent_security_info",
        lambda: call_api("get_security_info", RECENT_CONTRACT),
        result_kind="security_info",
    )
)
report["cases"].append(
    run_case(
        "early_tf_security_info",
        lambda: call_api("get_security_info", EARLY_TF_CONTRACT),
        result_kind="security_info",
    )
)
report["cases"].append(
    run_case(
        "recent_daily_baseline",
        lambda: call_api(
            "get_price",
            RECENT_CONTRACT,
            start_date=RECENT_TRADING_DATE,
            end_date=RECENT_TRADING_DATE,
            frequency="1d",
            fields=DAILY_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        ),
    )
)
report["cases"].append(
    run_case(
        "recent_daily_extended_kwargs",
        lambda: call_api(
            "get_price",
            RECENT_CONTRACT,
            start_date=RECENT_TRADING_DATE,
            end_date=RECENT_TRADING_DATE,
            frequency="1d",
            fields=DAILY_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
            fill_paused=False,
            round=False,
        ),
    )
)
report["cases"].append(
    run_case(
        "recent_daily_multi_panel_false",
        lambda: call_api(
            "get_price",
            [RECENT_CONTRACT, RECENT_COMPARISON_CONTRACT],
            start_date=RECENT_TRADING_DATE,
            end_date=RECENT_TRADING_DATE,
            frequency="1d",
            fields=DAILY_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        ),
    )
)
report["cases"].append(
    run_case(
        "recent_settlement",
        lambda: call_api(
            "get_extras",
            "futures_sett_price",
            [RECENT_CONTRACT, RECENT_COMPARISON_CONTRACT],
            start_date=RECENT_TRADING_DATE,
            end_date=RECENT_TRADING_DATE,
            df=True,
        ),
    )
)
report["cases"].append(
    run_case(
        "recent_positions",
        lambda: call_api(
            "get_extras",
            "futures_positions",
            [RECENT_CONTRACT, RECENT_COMPARISON_CONTRACT],
            start_date=RECENT_TRADING_DATE,
            end_date=RECENT_TRADING_DATE,
            df=True,
        ),
    )
)
report["cases"].append(
    run_case(
        "recent_minute_exact_morning_session",
        lambda: call_api(
            "get_price",
            RECENT_CONTRACT,
            start_date="2024-06-28 09:30:00",
            end_date="2024-06-28 11:30:00",
            frequency="1m",
            fields=MINUTE_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        ),
    )
)
report["cases"].append(
    run_case(
        "recent_minute_widened_morning_session",
        lambda: call_api(
            "get_price",
            RECENT_CONTRACT,
            start_date="2024-06-28 09:29:00",
            end_date="2024-06-28 11:31:00",
            frequency="1m",
            fields=MINUTE_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        ),
    )
)
report["cases"].append(
    run_case(
        "recent_minute_date_only",
        lambda: call_api(
            "get_price",
            RECENT_CONTRACT,
            start_date=RECENT_TRADING_DATE,
            end_date=RECENT_TRADING_DATE,
            frequency="1m",
            fields=MINUTE_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        ),
    )
)
report["cases"].append(
    run_case(
        "early_tf_daily",
        lambda: call_api(
            "get_price",
            EARLY_TF_CONTRACT,
            start_date=EARLY_TF_TRADING_DATE,
            end_date=EARLY_TF_TRADING_DATE,
            frequency="1d",
            fields=DAILY_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        ),
    )
)
report["cases"].append(
    run_case(
        "early_tf_minute_all_sessions",
        lambda: call_api(
            "get_price",
            EARLY_TF_CONTRACT,
            start_date="2012-06-11 09:15:00",
            end_date="2012-06-11 15:15:00",
            frequency="1m",
            fields=MINUTE_FIELDS,
            skip_paused=True,
            fq=None,
            panel=False,
        ),
    )
)

if resolve_api_callable("get_query_count")[0] is not None:
    report["cases"].append(
        run_case(
            "query_count",
            lambda: call_api("get_query_count"),
            result_kind="other",
        )
    )

print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_01_BEGIN")
print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_01_END")
