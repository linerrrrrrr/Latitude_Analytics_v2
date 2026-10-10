"""聚宽研究 Notebook 的金融期货取数协议第 01 轮命名空间诊断探针。

将本文件全文复制到聚宽“投资研究”的一个空白 Python 3 Notebook 单元格中执行。
本版只检查行情 API 实际暴露在哪个命名空间，不请求行情、不写文件、不安装依赖。
执行完成后，请复制 BEGIN/END 标记之间的完整 JSON 返回本地项目继续分析。
"""

import datetime


PROBE_VERSION = "joinquant_financial_futures_round_01_v3"
RUN_STARTED_AT = datetime.datetime.now().astimezone().isoformat()
CANDIDATE_API_NAMES = (
    "get_price",
    "get_bars",
    "get_ticks",
    "history",
    "attribute_history",
    "get_current_data",
    "get_extras",
    "get_all_securities",
    "get_security_info",
    "get_query_count",
)
NAME_KEYWORDS = (
    "price",
    "bar",
    "tick",
    "history",
    "extra",
    "security",
    "future",
    "query",
)

preimport_value_by_name = dict(
    (api_name, globals().get(api_name)) for api_name in CANDIDATE_API_NAMES
)
preimport_global_names = sorted(
    name
    for name in globals()
    if not name.startswith("_")
)

jqdata_star_import_error = None
global_names_before_star_import = set(globals()) | {
    "global_names_before_star_import"
}
try:
    from jqdata import *
except Exception as error:
    jqdata_star_import_error = {
        "error_type": type(error).__name__,
        "error_message": str(error)[:2000],
    }

post_import_global_names = sorted(
    name
    for name in globals()
    if not name.startswith("_")
)

import inspect
import json
import pkgutil
import platform
import sys
import types

import jqdata as jqdata_module


def safe_repr(value, limit=1000):
    try:
        return repr(value)[:limit]
    except Exception as error:
        return "<repr failed: {}: {}>".format(
            type(error).__name__,
            str(error)[:500],
        )


def callable_description(value):
    if not callable(value):
        return {
            "callable": False,
            "python_type": type(value).__name__,
            "repr": safe_repr(value),
        }

    try:
        signature = str(inspect.signature(value))
    except Exception as error:
        signature = "unavailable: {}: {}".format(
            type(error).__name__,
            str(error)[:500],
        )

    try:
        inspected_module = inspect.getmodule(value)
        inspected_module_name = (
            getattr(inspected_module, "__name__", None)
            if inspected_module is not None
            else None
        )
    except Exception:
        inspected_module_name = None

    return {
        "callable": True,
        "python_type": type(value).__name__,
        "name": str(getattr(value, "__name__", "")),
        "qualname": str(getattr(value, "__qualname__", "")),
        "declared_module": str(getattr(value, "__module__", "")),
        "inspected_module": inspected_module_name,
        "signature": signature,
        "repr": safe_repr(value),
    }


def bounded_name_list(names, limit=500):
    sorted_names = sorted(set(str(name) for name in names))
    return {
        "count": len(sorted_names),
        "limit": limit,
        "truncated": len(sorted_names) > limit,
        "names": sorted_names[:limit],
    }


def matching_public_names(namespace):
    matched_names = []
    for name in namespace:
        if name.startswith("_"):
            continue
        lowered_name = name.lower()
        if any(keyword in lowered_name for keyword in NAME_KEYWORDS):
            matched_names.append(name)
    return sorted(set(matched_names))


def module_candidate_report(module):
    candidate_report = {}
    for api_name in CANDIDATE_API_NAMES:
        try:
            value = getattr(module, api_name)
        except AttributeError:
            candidate_report[api_name] = {"present": False}
        except Exception as error:
            candidate_report[api_name] = {
                "present": "attribute_error",
                "error_type": type(error).__name__,
                "error_message": str(error)[:1000],
            }
        else:
            candidate_report[api_name] = {
                "present": True,
                "description": callable_description(value),
            }
    return candidate_report


callable_resolution = {}
owner_module_names = set()
for api_name in CANDIDATE_API_NAMES:
    preimport_value = preimport_value_by_name.get(api_name)
    post_import_value = globals().get(api_name)
    module_value = getattr(jqdata_module, api_name, None)

    location_values = {
        "platform_global_preimport": preimport_value,
        "post_import_global": post_import_value,
        "jqdata_module_attribute": module_value,
    }
    callable_resolution[api_name] = {}
    for location_name, value in location_values.items():
        if value is None:
            callable_resolution[api_name][location_name] = {"present": False}
            continue
        description = callable_description(value)
        callable_resolution[api_name][location_name] = {
            "present": True,
            "description": description,
        }
        if description.get("callable"):
            for module_name_key in ["declared_module", "inspected_module"]:
                module_name = description.get(module_name_key)
                if module_name:
                    owner_module_names.add(module_name)

jqdata_public_attribute_names = [
    name for name in dir(jqdata_module) if not name.startswith("_")
]
jqdata_all_value = getattr(jqdata_module, "__all__", None)
if jqdata_all_value is None:
    jqdata_all_report = {
        "present": False,
        "value_type": None,
        "names": None,
    }
else:
    try:
        jqdata_all_names = [str(name) for name in jqdata_all_value]
    except Exception as error:
        jqdata_all_report = {
            "present": True,
            "value_type": type(jqdata_all_value).__name__,
            "error_type": type(error).__name__,
            "error_message": str(error)[:1000],
        }
    else:
        jqdata_all_report = {
            "present": True,
            "value_type": type(jqdata_all_value).__name__,
            "names": bounded_name_list(jqdata_all_names),
        }

jqdata_package_path = getattr(jqdata_module, "__path__", None)
discovered_submodules = []
submodule_discovery_error = None
if jqdata_package_path is not None:
    try:
        discovered_submodules = sorted(
            module_info.name
            for module_info in pkgutil.iter_modules(
                jqdata_package_path,
                prefix="jqdata.",
            )
        )
    except Exception as error:
        submodule_discovery_error = {
            "error_type": type(error).__name__,
            "error_message": str(error)[:2000],
        }

loaded_jqdata_module_names = sorted(
    module_name
    for module_name, module in sys.modules.items()
    if (
        module is not None
        and (
            module_name == "jqdata"
            or module_name.startswith("jqdata.")
        )
    )
)

module_attribute_module_names = set()
for attribute_name in jqdata_public_attribute_names:
    try:
        attribute_value = getattr(jqdata_module, attribute_name)
    except Exception:
        continue
    if isinstance(attribute_value, types.ModuleType):
        module_attribute_module_names.add(attribute_value.__name__)

modules_to_inspect = sorted(
    set(["jqdata"])
    | set(loaded_jqdata_module_names)
    | owner_module_names
    | module_attribute_module_names
)
module_reports = {}
for module_name in modules_to_inspect:
    module = sys.modules.get(module_name)
    if module is None:
        module_reports[module_name] = {
            "loaded": False,
            "candidate_apis": None,
        }
        continue

    try:
        module_names = dir(module)
    except Exception as error:
        module_reports[module_name] = {
            "loaded": True,
            "dir_error_type": type(error).__name__,
            "dir_error_message": str(error)[:1000],
        }
        continue

    matched_names = matching_public_names(module_names)
    matched_callable_descriptions = {}
    for name in matched_names:
        try:
            value = getattr(module, name)
        except Exception as error:
            matched_callable_descriptions[name] = {
                "attribute_error_type": type(error).__name__,
                "attribute_error_message": str(error)[:500],
            }
            continue
        if callable(value):
            matched_callable_descriptions[name] = callable_description(value)

    module_reports[module_name] = {
        "loaded": True,
        "file": str(getattr(module, "__file__", None)),
        "package": str(getattr(module, "__package__", None)),
        "public_names": bounded_name_list(
            name for name in module_names if not name.startswith("_")
        ),
        "matching_names": matched_names,
        "matching_callables": matched_callable_descriptions,
        "candidate_apis": module_candidate_report(module),
    }

new_global_names_from_star_import = sorted(
    set(post_import_global_names) - global_names_before_star_import
)
matching_preimport_global_names = matching_public_names(preimport_global_names)
matching_post_import_global_names = matching_public_names(post_import_global_names)

report = {
    "probe_version": PROBE_VERSION,
    "run_started_at": RUN_STARTED_AT,
    "environment": {
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "local_timezone": str(datetime.datetime.now().astimezone().tzinfo),
        "jqdata_version": str(getattr(jqdata_module, "__version__", "unknown")),
    },
    "jqdata_star_import_error": jqdata_star_import_error,
    "callable_resolution": callable_resolution,
    "globals": {
        "preimport": {
            "names": bounded_name_list(preimport_global_names),
            "matching_names": matching_preimport_global_names,
        },
        "post_import": {
            "names": bounded_name_list(post_import_global_names),
            "matching_names": matching_post_import_global_names,
        },
        "new_names_from_star_import": bounded_name_list(
            new_global_names_from_star_import
        ),
    },
    "jqdata_module": {
        "file": str(getattr(jqdata_module, "__file__", None)),
        "package": str(getattr(jqdata_module, "__package__", None)),
        "path": (
            [str(path) for path in jqdata_package_path]
            if jqdata_package_path is not None
            else None
        ),
        "all": jqdata_all_report,
        "public_attribute_names": bounded_name_list(
            jqdata_public_attribute_names
        ),
        "matching_public_names": matching_public_names(
            jqdata_public_attribute_names
        ),
        "candidate_apis": module_candidate_report(jqdata_module),
        "attribute_module_names": sorted(module_attribute_module_names),
    },
    "module_discovery": {
        "discovered_submodules": bounded_name_list(discovered_submodules),
        "discovery_error": submodule_discovery_error,
        "loaded_jqdata_modules": bounded_name_list(
            loaded_jqdata_module_names
        ),
        "known_callable_owner_modules": sorted(owner_module_names),
        "inspected_modules": module_reports,
    },
}

print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_01_BEGIN")
print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_01_END")
