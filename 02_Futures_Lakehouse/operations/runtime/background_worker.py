"""Shared control plane for one explicitly authorized formal collection batch.

The worker has no schedule, daemon loop, resume path, or business retry.  Every
caller supplies an immutable stage manifest and a fresh run directory.  A
separate visible console is mandatory for the whole batch.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
import pathlib
import queue
import re
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psutil


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


PROTOCOL_VERSION = 1
OPERATIONS_ROOT = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "operations"
)
COLLECTION_ROOT = (
    PROJECT_ROOT / "02_Futures_Lakehouse"
)
RUN_HISTORY_ROOT = OPERATIONS_ROOT / "run_history"
GLOBAL_LOCK_PATH = RUN_HISTORY_ROOT / ".active_formal_run"
HEARTBEAT_SECONDS = 2.0
OUTPUT_SLICE_SECONDS = 0.05
OUTPUT_SLICE_LINES = 256
PROGRESS_SCOPE_LIMIT = 128
ERROR_SUMMARY_LIMIT = 100
PROGRESS_EVENT_PREFIX = "progress_event:"
MONITOR_START_TIMEOUT_SECONDS = 60.0
STATUS_REPLACE_ATTEMPTS = 50
STATUS_REPLACE_RETRY_SECONDS = 0.1
TERMINATION_TIMEOUT_SECONDS = 10.0
DEFAULT_PROGRESS_PREFIXES = (
    "planning_progress:",
    "auto_plan:",
    "explicit_plan:",
    "partition_plan:",
    "request_batch:",
    "partition_start:",
    "partition_committed:",
    "partition_timing:",
    "performance_gate_passed:",
    "performance_gate_failed:",
    "policy_committed:",
    "api_result:",
    "api_success:",
    "up_to_date:",
    "committed:",
    "quota_stop:",
    "reconciliation_plan:",
    "state_repair_plan:",
    "http_success:",
    "fetch_progress:",
    "pagination_progress:",
    "special_case_source_valid:",
    "special_case_existing_valid:",
    "special_case_committed:",
)
TERMINAL_STATES = frozenset(
    {"succeeded", "failed", "quota_stopped", "interrupted"}
)


class ActiveFormalRunError(RuntimeError):
    """Another formal operation owns the atomic directory lock."""


class ManualInterruption(RuntimeError):
    """The operator created the run's durable interruption request."""


@dataclass(frozen=True, slots=True)
class StageSpec:
    """One immutable, single-attempt Python process specification."""

    name: str
    entrypoint_path: pathlib.Path
    arguments: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("阶段名称不得为空。")
        if not isinstance(self.entrypoint_path, pathlib.Path):
            raise TypeError("entrypoint_path 必须是 pathlib.Path。")
        if not isinstance(self.arguments, tuple) or not all(
            isinstance(argument, str) for argument in self.arguments
        ):
            raise TypeError("arguments 必须是不可变的 str tuple。")

    def command(self) -> tuple[str, ...]:
        return (
            sys.executable,
            str(self.entrypoint_path.resolve()),
            *self.arguments,
        )


DEFAULT_PREFLIGHT_STAGES = (
    StageSpec(
        "preflight/01_lakehouse_runtime",
        PROJECT_ROOT / "02_Futures_Lakehouse" / "a00_01_verify_runtime.py",
    ),
    StageSpec(
        "preflight/02_operations_runtime",
        OPERATIONS_ROOT / "runtime" / "verify_operations_runtime.py",
    ),
    StageSpec(
        "preflight/03_notebook_export_sync",
        COLLECTION_ROOT / "a00_02_sync_notebook_exports.py",
        ("--check", "--check-level", "code"),
    ),
)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json_shared(path: pathlib.Path) -> dict[str, Any]:
    """Read an atomic state snapshot without preventing Windows replacement."""
    if os.name != "nt":
        return json.loads(path.read_text(encoding="utf-8-sig"))
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                           wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create_file.restype = wintypes.HANDLE
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL
    handle = create_file(str(path.resolve()), 0x80000000, 7, None, 3, 0x80, None)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        descriptor = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        close_handle(handle)
        raise
    with os.fdopen(descriptor, "r", encoding="utf-8-sig") as stream:
        return json.load(stream)


def monitor_is_healthy(run_root: pathlib.Path, monitor_pid: int | None) -> bool:
    """Console runs require a fresh UI-loop lease, not just a live process."""
    if not process_is_alive(monitor_pid):
        return False
    if not (run_root / "request.json").exists():
        # Read-only compatibility with the original monitor protocol/tests.
        return True
    try:
        lease = read_json_shared(run_root / "monitor.json")
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(lease["heartbeat_at"])).total_seconds()
        return (
            lease["pid"] == monitor_pid
            and abs(psutil.Process(monitor_pid).create_time() - lease["process_created_at"]) < 0.1
            and -2 <= age <= 12
        )
    except (OSError, ValueError, TypeError, KeyError, psutil.Error):
        return False


def atomic_write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    """Publish complete JSON; retry only Windows status-file replace locks."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.flush()
            os.fsync(handle.fileno())

        for attempt in range(1, STATUS_REPLACE_ATTEMPTS + 1):
            try:
                os.replace(temporary_path, path)
                return
            except PermissionError as error:
                if (
                    getattr(error, "winerror", None) not in {5, 32}
                    or attempt == STATUS_REPLACE_ATTEMPTS
                ):
                    raise
                time.sleep(STATUS_REPLACE_RETRY_SECONDS)
    finally:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass


def atomic_write_text(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass


def process_is_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        process = psutil.Process(pid)
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except (psutil.Error, ValueError):
        return False


def tracked_process_is_alive(process: psutil.Process) -> bool:
    try:
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def discover_descendants(
    direct_process: subprocess.Popen[str],
    tracked_processes: dict[int, psutil.Process],
) -> None:
    """Remember descendants while their parent relation still exists."""

    try:
        parent_process = psutil.Process(direct_process.pid)
        tracked_processes.setdefault(parent_process.pid, parent_process)
        for descendant_process in parent_process.children(recursive=True):
            tracked_processes.setdefault(
                descendant_process.pid,
                descendant_process,
            )
    except psutil.Error:
        pass

    # On Windows a process keeps its creation parent PID after that parent
    # exits.  Scan once the direct child is gone so an extremely short-lived
    # launcher cannot orphan a grandchild between the regular tree polls.
    if direct_process.poll() is not None:
        known_parent_pids = {direct_process.pid, *tracked_processes}
        found_descendant = True
        while found_descendant:
            found_descendant = False
            for candidate_process in psutil.process_iter(("pid", "ppid")):
                try:
                    candidate_pid = int(candidate_process.info["pid"])
                    candidate_parent_pid = int(candidate_process.info["ppid"])
                except (KeyError, TypeError, ValueError, psutil.Error):
                    continue
                if (
                    candidate_pid == os.getpid()
                    or candidate_pid in known_parent_pids
                    or candidate_parent_pid not in known_parent_pids
                ):
                    continue
                tracked_processes[candidate_pid] = candidate_process
                known_parent_pids.add(candidate_pid)
                found_descendant = True


def stop_tracked_processes(
    direct_process: subprocess.Popen[str] | None,
    tracked_processes: dict[int, psutil.Process],
) -> bool:
    """Recursively terminate known descendants, including re-parented children."""

    if direct_process is not None:
        discover_descendants(direct_process, tracked_processes)

    alive_processes = [
        process
        for process in tracked_processes.values()
        if tracked_process_is_alive(process)
    ]
    direct_pid = direct_process.pid if direct_process is not None else None
    alive_processes.sort(key=lambda process: process.pid == direct_pid)

    for process in alive_processes:
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
        except psutil.Error:
            continue

    if alive_processes:
        _, alive_processes = psutil.wait_procs(
            alive_processes,
            timeout=TERMINATION_TIMEOUT_SECONDS,
        )
    for process in alive_processes:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass
        except psutil.Error:
            continue
    if alive_processes:
        psutil.wait_procs(
            alive_processes,
            timeout=TERMINATION_TIMEOUT_SECONDS,
        )

    if direct_process is not None and direct_process.poll() is None:
        try:
            direct_process.terminate()
            direct_process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            direct_process.kill()
            direct_process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        except OSError:
            pass

    all_stopped = (
        (direct_process is None or direct_process.poll() is not None)
        and not any(
            tracked_process_is_alive(process)
            for process in tracked_processes.values()
        )
    )
    if all_stopped:
        tracked_processes.clear()
    return all_stopped


def monitor_pid_from_file(run_root: pathlib.Path) -> int | None:
    monitor_pid_path = run_root / "monitor.pid"
    if not monitor_pid_path.is_file():
        return None
    try:
        return int(monitor_pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def raise_if_manual_interruption(run_root: pathlib.Path) -> None:
    if (run_root / "interrupt.request").is_file():
        raise ManualInterruption(
            "interrupt.request 已存在；人工中断请求保留为现场证据。"
        )


def wait_for_required_monitor(
    run_root: pathlib.Path,
    status_path: pathlib.Path,
    status: dict[str, Any],
) -> int:
    deadline = time.monotonic() + MONITOR_START_TIMEOUT_SECONDS
    status.update(
        {
            "state": "waiting_for_monitor",
            "phase": "waiting_for_monitor",
            "stage_name": "等待总控台监控就绪",
            "progress": "monitor=waiting",
        }
    )

    while time.monotonic() < deadline:
        raise_if_manual_interruption(run_root)
        monitor_pid = monitor_pid_from_file(run_root)
        status["heartbeat_at"] = utc_now_text()
        status["monitor_pid"] = monitor_pid
        atomic_write_json(status_path, status)
        if monitor_is_healthy(run_root, monitor_pid):
            return int(monitor_pid)
        time.sleep(1.0)

    raise RuntimeError(
        "monitor_unavailable: 未在限定时间内发现存活的用户可见监控进程。"
    )


def classify_stage_result(return_code: int, saw_quota_stop: bool) -> str:
    if saw_quota_stop:
        return "quota_stopped"
    if return_code == 130:
        return "interrupted"
    if return_code != 0:
        return "failed"
    return "succeeded"


def parse_progress_event(line: str) -> dict[str, Any]:
    """Validate the small public event protocol; never infer business completion."""
    if not line.startswith(PROGRESS_EVENT_PREFIX):
        raise ValueError("不是 progress_event 行。")
    if len(line) > 65536:
        raise ValueError("进度事件超过 64 KiB。")
    event = json.loads(line[len(PROGRESS_EVENT_PREFIX):])
    if not isinstance(event, dict) or type(event.get("event_version")) is not int or event["event_version"] != 1:
        raise ValueError("不支持的进度事件版本。")
    if event.get("event") not in {"plan", "phase", "request", "response", "progress", "transaction", "rollback", "failure"}:
        raise ValueError("未知进度事件类型。")
    if event.get("state") not in {"started", "running", "completed", "failed", "skipped"}:
        raise ValueError("未知进度事件状态。")
    for name in ("phase", "scope_id"):
        if not isinstance(event.get(name), str) or not event[name].strip() or len(event[name]) > 256:
            raise ValueError(f"进度 {name} 必须是非空短文本。")
    parent_scope_id = event.get("parent_scope_id")
    if parent_scope_id is not None and (
        not isinstance(parent_scope_id, str) or not parent_scope_id.strip()
        or len(parent_scope_id) > 256 or parent_scope_id == event["scope_id"]
    ):
        raise ValueError("进度父作用域无效。")
    for name in ("operation", "outcome", "transaction_id", "message", "label"):
        if name in event and (not isinstance(event[name], str) or len(event[name]) > 2048):
            raise ValueError(f"进度 {name} 必须是短文本。")
    if "emitted_at" in event:
        if not isinstance(event["emitted_at"], str) or datetime.fromisoformat(event["emitted_at"]).tzinfo is None:
            raise ValueError("进度来源时间必须含时区。")
    if "persisted" in event and type(event["persisted"]) is not bool:
        raise ValueError("persisted 必须是布尔值。")
    event_object = event.get("object", {})
    if not isinstance(event_object, dict) or len(event_object) > 32:
        raise ValueError("进度对象必须是有界映射。")
    for name, value in event_object.items():
        if not isinstance(name, str) or len(name) > 128 or not isinstance(value, (str, int, float, bool, type(None))):
            raise ValueError("进度对象只允许公开标识的标量值。")
        if isinstance(value, str) and len(value) > 2048 or isinstance(value, float) and not math.isfinite(value):
            raise ValueError("进度对象值无效或过长。")
    counters = event.get("counters", {})
    if not isinstance(counters, dict) or len(counters) > 32:
        raise ValueError("进度计数必须是有界映射。")
    for name, counter in counters.items():
        if not isinstance(name, str) or not name or len(name) > 128 or not isinstance(counter, dict):
            raise ValueError("进度计数条目无效。")
        if set(counter) != {"completed", "total", "unit"}:
            raise ValueError("计数必须明确 completed、total 和 unit。")
        if not isinstance(counter["unit"], str) or not counter["unit"].strip() or len(counter["unit"]) > 64:
            raise ValueError("计数单位无效。")
        for field in ("completed", "total"):
            value = counter[field]
            if field == "total" and value is None:
                continue
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError("计数必须是非负有限数；未知总量使用 null。")
        if counter["total"] is not None and counter["completed"] > counter["total"]:
            raise ValueError("计数已完成量不得超过已知总量。")
    # Source payload cannot replace worker-owned sequence, identity or provenance.
    allowed_fields = {
        "event_version", "event", "emitted_at", "operation", "phase", "scope_id",
        "parent_scope_id", "state", "outcome", "object", "counters", "persisted",
        "transaction_id", "message", "label",
    }
    if set(event) - allowed_fields:
        raise ValueError("进度事件包含未知字段。")
    event["object"] = event_object
    event["counters"] = counters
    event["parent_scope_id"] = parent_scope_id
    event["source"] = "structured"
    # Escaped lone surrogates are JSON-decodable but cannot enter UTF-8 evidence.
    json.dumps(event, ensure_ascii=False, allow_nan=False).encode("utf-8")
    return event


def parse_legacy_progress(line: str) -> dict[str, Any] | None:
    """Read explicit local counters from existing logs, without guessing totals."""
    if len(line) > 65536:
        return None
    prefix, separator, payload = line.partition(":")
    if not separator or prefix not in {
        "planning_progress", "request_batch", "fetch_progress", "pagination_progress",
        "reconciliation_plan", "auto_plan", "explicit_plan", "partition_plan", "partition_start",
        "api_result", "partition_committed", "committed", "up_to_date",
    }:
        return None
    fields = {}
    for token in payload.split(";"):
        name, equals, value = token.strip().partition("=")
        if equals and re.fullmatch(r"[a-z_]+", name):
            fields[name] = value.strip()
    function = fields.get("function", prefix)[:256]
    phase = fields.get("phase", prefix)[:256]
    # These four calendar producers have audited log units and transaction boundaries.
    # A stable scope retains the latest occurrence of a local operation, never sums it.
    calendar_functions = {
        "dim_trade_calendar": {"main", "collect", "commit_partitions"},
        "dim_futures_variety_calendar": {"main", "collect", "commit_partitions"},
        "dim_futures_contract_calendar": {"main", "collect", "build_partition", "commit_partition", "write_processing_watermark"},
        "dim_futures_bar_calendar": {"main", "open_contract_dataset", "partition_keys_from_files", "read_partition",
                                     "build_structural_partitions", "build_fresh_partitions", "read_existing_partition_leaf", "commit_partition"},
    }
    table = fields.get("table")
    calendar_function = fields.get("function", "main").removesuffix("()")
    if table in calendar_functions and calendar_function in calendar_functions[table]:
        if prefix in {"partition_start", "partition_committed"} and "phase" not in fields:
            phase = "install_partitions"
        elif prefix == "committed" and "phase" not in fields:
            phase = "transaction_complete"
        elif prefix == "reconciliation_plan" and "phase" not in fields:
            phase = "reconcile"
        elif prefix in {"auto_plan", "explicit_plan"} and "phase" not in fields:
            phase = "plan"
        elif prefix == "up_to_date" and "phase" not in fields:
            phase = "run"
        if prefix == "partition_plan" and phase in {"reconcile", "plan"} and "partition" in fields:
            phase = "partition_detail"
        event_object = {name: value[:2048] for name, value in fields.items() if name in {
            "table", "partition", "key", "trading_date", "start_date", "end_date", "mode", "write", "outcome",
            "run_id", "bar_frequency", "failed_phase", "error", "source", "automatic_tail_processed_through",
            "exchange_code", "year", "month",
            "valid_start", "valid_end", "valid_end_date", "existing_rows",
            "latest_calendar_date", "latest_trading_date", "batch",
            "rows", "replacement_rows", "validated_rows", "trading_day_count", "trading_dates", "fixed_contracts",
            "contracts", "info_batches", "requested_contracts", "returned_contracts", "requested_variety_dates",
            "requested_variety_date_count", "expected_session_count", "missing_session_count", "changed_session_count",
            "extra_session_count", "no_valid_rule_contract_day_count", "touched_partition_count", "partitions",
            "valid_grid_count", "complete_grid_count", "missing_or_revised_grid_count", "upstream_grid_count",
            "missing_or_incomplete_grid_count", "replacement_row_count", "new_trading_date_count", "range_count",
            "upstream_files", "target_files", "upstream_partitions", "target_partitions", "upstream_rows", "target_rows",
            "candidate_partitions", "dirty_partitions", "minute_rows", "daily_rows", "skipped_clean_partitions",
            "expected_rows", "missing", "changed", "extra", "quality_error", "api_calls",
            "missing_grid_count", "changed_grid_count", "extra_grid_count", "clean_partition_count",
        }}
        counters = {}
        if table == "dim_trade_calendar" and phase == "collect":
            if fields.get("rows", "").isdecimal() and fields.get("status") == "completed":
                counters["natural_days"] = {"completed": int(fields["rows"]), "total": int(fields["rows"]), "unit": "自然日"}
            elif fields.get("start_date") and fields.get("end_date"):
                try:
                    day_count = (datetime.fromisoformat(fields["end_date"]) - datetime.fromisoformat(fields["start_date"])).days + 1
                    if day_count >= 0:
                        counters["natural_days"] = {"completed": 0, "total": day_count, "unit": "自然日"}
                except ValueError:
                    pass
        counter_spec = {
            "dim_trade_calendar": {"install_partitions": ("installed_leaves", "叶（待事务确认）")},
            "dim_futures_variety_calendar": {"expand": ("expanded_dates", "交易日"), "merge": ("merged_leaves", "叶"),
                                              "install_partitions": ("installed_leaves", "叶（待事务确认）")},
            "dim_futures_contract_calendar": {"contract_info": ("info_batches", "信息批次"), "reconcile": ("compared_partitions", "分区"),
                                               "build": ("variety_dates", "品种日（当前分区）"), "expand": ("variety_dates", "品种日（当前分区）"),
                                               "commit_batch": ("committed_leaves", "已提交叶")},
            "dim_futures_bar_calendar": {"watermark": ("watermark_groups", "水位读取分组"), "plan": ("planned_partitions", "基础分区"),
                                          "build_structure": ("frequencies", "频率（当前分区）"), "build_fresh": ("frequencies", "频率（当前分区）"),
                                          "commit_batch": ("processed_leaves", "已处理叶（含跳过）")},
        }[table].get(phase)
        completed, total = fields.get("completed", ""), fields.get("total", "")
        if counter_spec and completed.isdecimal() and total.isdecimal() and int(completed) <= int(total):
            counters[counter_spec[0]] = {"completed": int(completed), "total": int(total), "unit": counter_spec[1]}
        state = fields.get("status", "running")
        if prefix == "partition_committed" and phase == "install_partitions":
            state = "running"  # b01/b02 log inside their common transaction.
        if prefix == "up_to_date":
            event_object["outcome"] = "up_to_date"
            state = "completed"
        if counter_spec and counters and int(completed) < int(total) and state == "completed":
            state = "running"  # A completed API response is not the completed batch loop.
        if state not in {"started", "running", "completed", "failed", "skipped"}:
            state = "running"
        return {
            "event_version": 1, "event": "phase", "source": "legacy",
            "scope_id": f"calendar:{table}:{calendar_function}:{phase}", "parent_scope_id": None,
            "phase": phase, "function": calendar_function, "state": state,
            "object": event_object, "counters": counters, "counter_scope": "latest_operation",
            "raw_line": line[:4096],
        }
    if prefix in {"api_result", "partition_committed", "committed", "up_to_date"}:
        return None
    # Values remain text: in particular grid reprs are never evaluated.
    event_object = {name: fields[name][:2048] for name in (
        "table", "artifact", "exchange", "exchange_code", "underlying", "underlying_code",
        "contract", "contract_code", "trading_date", "date", "dates", "start_date", "end_date",
        "session", "session_name", "frequency", "dataset", "dataset_name", "source_api",
        "series", "series_code", "report", "indicator", "indicator_code", "grid", "key",
        "partition", "case_id", "case_index", "page", "request", "request_dates",
        "batch", "start", "end", "api", "range", "sessions",
    ) if name in fields}
    # Existing plan quantities are context, not a claim of current-batch completion.
    plan_context_names = {
        "pending_sessions", "complete_sessions", "selected_sessions", "pending_partitions",
        "expected_pending_rows", "pending_grid_count", "planned_grid_count",
        "completed_required_count", "request_batch_count", "pending_grids", "required_grids",
    }
    event_object.update({name: fields[name] for name in plan_context_names
                         if name in fields and fields[name].isdecimal()})
    counters = {}
    local_counter_units = {
        "processed_dates": "日期", "checked_dates": "日期", "completed_dates": "日期",
        "processed_series": "系列", "compared_partitions": "分区", "checked_partitions": "分区",
        "processed_sessions": "Session", "checked_sessions": "Session", "completed_sessions": "Session",
        "processed_upstream": "上游格点", "checked_grids": "格点", "processed_grids": "格点",
        "visited_rows": "行", "checked_rows": "行", "processed_rows": "行",
        "accepted_pages": "页", "completed_pages": "页", "checked_files": "文件",
        "read_files": "文件", "written_files": "文件",
        "processed_partitions": "分区", "completed_fact_partitions": "事实叶",
    }
    explicit_total_fields = {
        "processed_dates": "total_dates", "checked_dates": "total_dates", "completed_dates": "total_dates",
        "processed_series": "total_series", "compared_partitions": "total_partitions",
        "checked_partitions": "total_partitions", "processed_sessions": "total_sessions",
        "checked_sessions": "total_sessions", "completed_sessions": "total_sessions",
        "processed_upstream": "upstream_rows", "checked_grids": "total_grids",
        "processed_grids": "total_grids", "visited_rows": "source_rows",
        "checked_rows": "total_rows", "processed_rows": "total_rows",
        "accepted_pages": "total_pages", "completed_pages": "total_pages",
        "checked_files": "total_files", "read_files": "total_files", "written_files": "total_files",
        "processed_partitions": "total_partitions", "completed_fact_partitions": "total_fact_partitions",
    }
    for name, unit in local_counter_units.items():
        value = fields.get(name, "")
        ratio = re.fullmatch(r"(\d+)\s*/\s*(\d+)", value)
        total_value = fields.get(explicit_total_fields[name], "")
        if name == "processed_sessions" and not total_value:
            total_value = fields.get("planned_sessions", "")
        if ratio:
            completed, total = map(int, ratio.groups())
        elif value.isdecimal() and total_value.isdecimal():
            completed, total = int(value), int(total_value)
        else:
            continue
        if completed <= total:
            counters[name] = {"completed": completed, "total": total, "unit": unit}
    # b06 reports these generic names at audited calendar-leaf commit boundaries.
    if function == "commit_calendar_updates" and fields.get("table") == "dim_futures_bar_calendar":
        completed_field = {
            "build_calendar_leaf": "completed", "calendar_commit": "committed_partitions",
        }.get(phase)
        completed_value = fields.get(completed_field, "") if completed_field else ""
        total_value = fields.get("total", "")
        if completed_value.isdecimal() and total_value.isdecimal() and int(completed_value) <= int(total_value):
            counters["committed_calendar_leaves"] = {
                "completed": int(completed_value), "total": int(total_value), "unit": "日历叶",
            }
    # Start ordinals such as request, case_index and partition_start are context only.
    scope_object = {name: value for name, value in event_object.items()
                    if name not in {"case_index", "page", "request", "request_dates", "batch", "sessions", "api"}
                    and name not in plan_context_names}
    scope_digest = hashlib.sha256(json.dumps(
        [function, phase, scope_object], ensure_ascii=False, sort_keys=True,
    ).encode("utf-8")).hexdigest()[:20]
    state = fields.get("status", "running")
    if state not in {"started", "running", "completed", "failed", "skipped"}:
        state = "running"
    return {
        "event_version": 1, "event": "phase", "source": "legacy",
        "scope_id": f"legacy:{scope_digest}", "parent_scope_id": None,
        "phase": phase, "function": function, "state": state,
        "object": event_object, "counters": counters, "counter_scope": "local_step",
        "raw_line": line[:4096],
    }


def run_stage(
    *,
    stage: StageSpec,
    phase: str,
    stage_index: int,
    stage_total: int,
    run_root: pathlib.Path,
    status_path: pathlib.Path,
    status: dict[str, Any],
    monitor_pid: int,
    progress_prefixes: tuple[str, ...],
    tracked_processes: dict[int, psutil.Process],
) -> str:
    """Run one child process exactly once and leave no descendant behind."""

    raise_if_manual_interruption(run_root)
    if not monitor_is_healthy(run_root, monitor_pid):
        raise RuntimeError(
            "monitor_unavailable: 阶段启动前可见监控进程已退出。"
        )
    command = list(stage.command())
    safe_stage_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", stage.name)
    log_path = (
        run_root
        / "logs"
        / f"{phase}_{stage_index:02d}_{safe_stage_name}.log"
    )
    log_path.parent.mkdir(parents=True, exist_ok=True)

    child_environment = os.environ.copy()
    child_environment["PYTHONUTF8"] = "1"
    child_environment["PYTHONIOENCODING"] = "utf-8"
    child_environment["PYTHONUNBUFFERED"] = "1"
    child_environment.pop("LATITUDE_B01_PREVIEW_PATH", None)
    if stage.name == "a01/b01_trade_calendar":
        child_environment["LATITUDE_B01_PREVIEW_PATH"] = str(run_root / "artifacts" / "b01_generated.parquet")
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    direct_process: subprocess.Popen[str] | None = None
    reader: threading.Thread | None = None
    stop_reading = threading.Event()
    reader_errors: list[Exception] = []
    recent_lines: deque[str] = deque(maxlen=40)
    saw_quota_stop = False
    leaked_descendants = False

    status.update(
        {
            "state": "running",
            "phase": phase,
            "stage_index": stage_index,
            "stage_total": stage_total,
            "stage_name": stage.name,
            "stage_started_at": utc_now_text(),
            "stage_exit_code": None,
            "child_pid": None,
            "progress": f"{phase}={stage_index}/{stage_total}",
            "last_line": "",
            "last_output_at": None,
            "progress_at": None,
            "recent_lines": [],
            "error": None,
            "log_path": str(log_path),
            "progress_path": str(run_root / "progress.jsonl"),
            "progress_scopes": {},
            "current_scope_id": None,
            "progress_source": "text",
            "progress_parse_error": None,
            "progress_scopes_omitted": 0,
        }
    )
    status.setdefault("progress_event_count", 0)
    status.setdefault("progress_parse_errors", 0)
    status.setdefault("error_summary", [])
    status.setdefault("error_summary_omitted", 0)
    atomic_write_json(status_path, status)

    try:
        with (
            log_path.open("w", encoding="utf-8", newline="\n") as log_handle,
            (run_root / "progress.jsonl").open("a", encoding="utf-8", newline="\n") as progress_handle,
        ):
            direct_process = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                env=child_environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creation_flags,
            )
            discover_descendants(direct_process, tracked_processes)
            status["child_pid"] = direct_process.pid
            atomic_write_json(status_path, status)

            output_queue: queue.Queue[str] = queue.Queue(maxsize=512)

            def read_output() -> None:
                assert direct_process is not None
                assert direct_process.stdout is not None
                try:
                    for raw_line in direct_process.stdout:
                        while not stop_reading.is_set():
                            try:
                                output_queue.put(raw_line, timeout=0.1)
                                break
                            except queue.Full:
                                continue
                        if stop_reading.is_set():
                            break
                except Exception as error:
                    reader_errors.append(error)
                finally:
                    direct_process.stdout.close()

            reader = threading.Thread(target=read_output, daemon=True)
            reader.start()
            last_heartbeat = 0.0

            while (
                direct_process.poll() is None
                or reader.is_alive()
                or not output_queue.empty()
            ):
                discover_descendants(direct_process, tracked_processes)
                raise_if_manual_interruption(run_root)
                if reader_errors:
                    raise RuntimeError(f"child_output_read_failed: {reader_errors[0]}") from reader_errors[0]
                if direct_process.poll() is not None:
                    has_live_descendants = any(
                        process.pid != direct_process.pid
                        and tracked_process_is_alive(process)
                        for process in tracked_processes.values()
                    )
                    if has_live_descendants:
                        leaked_descendants = True
                        if not stop_tracked_processes(
                            direct_process,
                            tracked_processes,
                        ):
                            raise RuntimeError(
                                "child_tree_cleanup_failed: 入口退出后仍有"
                                "子进程存活；全局锁保留。"
                            )
                slice_deadline = time.monotonic() + OUTPUT_SLICE_SECONDS
                consumed_lines = 0
                while consumed_lines < OUTPUT_SLICE_LINES and time.monotonic() < slice_deadline:
                    try:
                        raw_line = output_queue.get_nowait()
                    except queue.Empty:
                        break

                    line = raw_line.rstrip("\r\n")
                    consumed_lines += 1
                    log_handle.write(raw_line)
                    log_handle.flush()
                    recent_lines.append(line)
                    if line:
                        status["last_line"] = line
                        status["last_output_at"] = utc_now_text()
                    if line.startswith(progress_prefixes) and status["progress_source"] != "structured":
                        status["progress"] = line
                        status["progress_at"] = utc_now_text()
                    if "quota_stop:" in line:
                        saw_quota_stop = True

                    received_at = utc_now_text()
                    event = None
                    parse_error = None
                    if line.startswith(PROGRESS_EVENT_PREFIX):
                        try:
                            event = parse_progress_event(line)
                            # Reject parent cycles before changing the current snapshot.
                            ancestor = event["parent_scope_id"]
                            seen = {event["scope_id"]}
                            while ancestor is not None:
                                if ancestor in seen:
                                    raise ValueError("进度父作用域形成循环。")
                                seen.add(ancestor)
                                ancestor = status["progress_scopes"].get(ancestor, {}).get("parent_scope_id")
                        except (ValueError, TypeError, OverflowError, RecursionError) as error:
                            event = None
                            status["progress_parse_errors"] += 1
                            parse_error = f"进度不可解析：{error}"
                            status["progress_parse_error"] = parse_error
                            status["progress"] = line[:4096]
                    elif status["progress_source"] != "structured":
                        event = parse_legacy_progress(line)

                    if event is not None:
                        event.update({
                            "sequence": status["progress_event_count"] + 1,
                            "received_at": received_at, "batch_id": run_root.name,
                            "stage_name": stage.name, "stage_index": stage_index,
                            "worker_phase": phase, "log_path": str(log_path),
                        })
                        # Preserve every accepted event before updating the small status view.
                        progress_handle.write(json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n")
                        progress_handle.flush()
                        status["progress_event_count"] = event["sequence"]
                        if event["source"] == "structured" and status["progress_source"] != "structured":
                            status["progress_scopes"] = {}
                        status["progress_source"] = event["source"]
                        status["progress_at"] = received_at
                        scope_id = event["scope_id"]
                        previous_scope = status["progress_scopes"].get(scope_id, {})
                        scope = {**previous_scope, **event}
                        new_scope_cycle = event["state"] == "started" and (
                            previous_scope.get("state") in {"completed", "failed", "skipped"}
                            or event.get("counter_scope") == "latest_operation")
                        reset_calendar_object = new_scope_cycle and event.get("counter_scope") == "latest_operation"
                        scope["object"] = {**({} if reset_calendar_object else previous_scope.get("object", {})), **event["object"]}
                        scope["counters"] = {**({} if new_scope_cycle else previous_scope.get("counters", {})), **event["counters"]}
                        if new_scope_cycle or not previous_scope:
                            scope["started_at"] = received_at
                        scope["first_seen_at"] = previous_scope.get("first_seen_at", received_at)
                        scope["updated_at"] = received_at
                        status["progress_scopes"][scope_id] = scope
                        status["current_scope_id"] = scope_id
                        if event["source"] == "structured":
                            status["progress"] = event.get("message") or f"{event['phase']}: {event['state']}"
                        # Keep parent chains and recent scopes; full history remains in JSONL.
                        while len(status["progress_scopes"]) > PROGRESS_SCOPE_LIMIT:
                            protected = {scope_id}
                            ancestor = scope.get("parent_scope_id")
                            while ancestor in status["progress_scopes"] and ancestor not in protected:
                                protected.add(ancestor)
                                ancestor = status["progress_scopes"][ancestor].get("parent_scope_id")
                            candidates = [key for key in status["progress_scopes"] if key not in protected]
                            if not candidates:
                                raise RuntimeError("progress_scope_depth_exceeded: 进度层级超过控制面上限。")
                            expired_scope = min(candidates, key=lambda key: (
                                status["progress_scopes"][key].get("phase") in {"plan", "planning", "run", "collect_batch", "commit_batch"}
                                and not any(name in status["progress_scopes"][key].get("object", {})
                                            for name in ("key", "partition", "grid", "contract", "contract_code", "trading_date")),
                                status["progress_scopes"][key].get("state") not in {"completed", "failed", "skipped"},
                                status["progress_scopes"][key]["sequence"],
                            ))
                            del status["progress_scopes"][expired_scope]
                            status["progress_scopes_omitted"] += 1

                    is_error_line = (
                        line.startswith(("ERROR:", "Error:", "FATAL:", "Traceback (", "failure:", "performance_gate_failed:"))
                        or bool(re.match(r"^(?:\[ERROR\]|\[FAIL\]|[A-Za-z_.]*(?:Error|Exception):)", line))
                        or "; status=failed" in line
                        or bool(event and event.get("state") == "failed")
                    )
                    if is_error_line or parse_error:
                        message = (parse_error or line)[:4096]
                        matching_error = next((item for item in status["error_summary"]
                                               if item["stage_name"] == stage.name and item["message"] == message), None)
                        if matching_error is not None:
                            matching_error["count"] += 1
                            matching_error["last_seen_at"] = received_at
                        elif len(status["error_summary"]) < ERROR_SUMMARY_LIMIT:
                            status["error_summary"].append({
                                "stage_name": stage.name, "phase": phase, "message": message,
                                "kind": "progress_parse_error" if parse_error else "output_error",
                                "count": 1, "first_seen_at": received_at, "last_seen_at": received_at,
                                "log_path": str(log_path),
                            })
                        else:
                            status["error_summary_omitted"] += 1

                now = time.monotonic()
                if now - last_heartbeat >= HEARTBEAT_SECONDS:
                    if not monitor_is_healthy(run_root, monitor_pid):
                        raise RuntimeError(
                            "monitor_unavailable: 可见监控进程已退出。"
                        )
                    status["heartbeat_at"] = utc_now_text()
                    status["monitor_pid"] = monitor_pid
                    status["recent_lines"] = list(recent_lines)[-12:]
                    progress_handle.flush()
                    os.fsync(progress_handle.fileno())
                    atomic_write_json(status_path, status)
                    last_heartbeat = now

                if output_queue.empty():
                    time.sleep(0.05)

            discover_descendants(direct_process, tracked_processes)
            leaked_descendants = leaked_descendants or any(
                process.pid != direct_process.pid
                and tracked_process_is_alive(process)
                for process in tracked_processes.values()
            )
            reader.join(timeout=5)
            if reader_errors:
                raise RuntimeError(f"child_output_read_failed: {reader_errors[0]}") from reader_errors[0]
            log_handle.flush()
            os.fsync(log_handle.fileno())
            progress_handle.flush()
            os.fsync(progress_handle.fileno())
    except BaseException as error:
        stop_reading.set()
        status["recent_lines"] = list(recent_lines)[-12:]
        if not stop_tracked_processes(direct_process, tracked_processes):
            raise RuntimeError(
                "child_tree_cleanup_failed: 子进程树未能全部停止；全局锁保留。"
            )
        status.setdefault("stage_history", []).append({
            "phase": phase, "index": stage_index, "name": stage.name,
            "state": "interrupted" if isinstance(error, (ManualInterruption, KeyboardInterrupt)) else "failed",
            "started_at": status["stage_started_at"], "finished_at": utc_now_text(),
            "exit_code": direct_process.poll() if direct_process is not None else None,
            "log_path": str(log_path), "error": f"{type(error).__name__}: {error}",
            "progress_scopes": copy.deepcopy(status["progress_scopes"]),
            "current_scope_id": status["current_scope_id"],
            "progress_source": status["progress_source"],
            "progress_scopes_omitted": status["progress_scopes_omitted"],
            "error_summary": copy.deepcopy([item for item in status["error_summary"] if item["stage_name"] == stage.name]),
        })
        raise

    assert direct_process is not None
    return_code = int(direct_process.returncode)
    if not stop_tracked_processes(direct_process, tracked_processes):
        raise RuntimeError(
            "child_tree_cleanup_failed: 子进程树未能全部停止；全局锁保留。"
        )
    status.update(
        {
            "child_pid": None,
            "stage_exit_code": return_code,
            "heartbeat_at": utc_now_text(),
            "recent_lines": list(recent_lines)[-12:],
        }
    )
    raise_if_manual_interruption(run_root)
    if not monitor_is_healthy(run_root, monitor_pid):
        raise RuntimeError(
            "monitor_unavailable: 阶段结束时可见监控进程已退出。"
        )

    result = classify_stage_result(return_code, saw_quota_stop)
    if leaked_descendants and result == "succeeded":
        result = "failed"
        status["last_line"] = "child_tree_leak: 已停止入口遗留的子进程。"
    status.setdefault("stage_history", []).append({
        "phase": phase, "index": stage_index, "name": stage.name,
        "state": result, "started_at": status["stage_started_at"],
        "finished_at": utc_now_text(), "exit_code": return_code,
        "log_path": str(log_path),
        "progress_scopes": copy.deepcopy(status["progress_scopes"]),
        "current_scope_id": status["current_scope_id"],
        "progress_source": status["progress_source"],
        "progress_scopes_omitted": status["progress_scopes_omitted"],
        "error_summary": copy.deepcopy([item for item in status["error_summary"] if item["stage_name"] == stage.name]),
    })
    atomic_write_json(status_path, status)
    return result


def validate_stage_specs(stages: tuple[StageSpec, ...], context: str) -> None:
    if not isinstance(stages, tuple) or not stages:
        raise ValueError(f"{context}阶段必须是非空不可变 tuple。")
    if not all(isinstance(stage, StageSpec) for stage in stages):
        raise TypeError(f"{context}阶段必须全部是 StageSpec。")
    stage_names = [stage.name for stage in stages]
    if len(stage_names) != len(set(stage_names)):
        raise ValueError(f"{context}阶段名称不得重复。")

def validate_stage_entrypoints(
    stages: tuple[StageSpec, ...],
    context: str,
) -> None:
    for stage in stages:
        if not stage.entrypoint_path.is_file():
            raise FileNotFoundError(
                f"{context}入口不存在：{stage.entrypoint_path}"
            )


def validate_progress_prefixes(progress_prefixes: tuple[str, ...]) -> None:
    if (
        not isinstance(progress_prefixes, tuple)
        or not progress_prefixes
        or not all(
            isinstance(prefix, str) and prefix
            for prefix in progress_prefixes
        )
    ):
        raise TypeError("progress_prefixes 必须是不可变的非空 str tuple。")


def validate_run_root(run_root: pathlib.Path) -> pathlib.Path:
    if not isinstance(run_root, pathlib.Path):
        raise TypeError("run_root 必须是 pathlib.Path。")
    resolved_run_root = run_root.resolve()
    allowed_root = RUN_HISTORY_ROOT.resolve()
    if resolved_run_root == allowed_root or not resolved_run_root.is_relative_to(
        allowed_root
    ):
        raise ValueError(f"运行目录必须是 {allowed_root} 下的独立批次目录。")

    relative_parts = resolved_run_root.relative_to(allowed_root).parts
    reserved_names = {"legacy_imports", ".active_formal_run"}
    if any(part.lower() in reserved_names for part in relative_parts):
        raise ValueError("run_root 不得使用 legacy_imports 或全局锁目录。")
    return resolved_run_root


def ensure_fresh_or_monitor_only_run_root(run_root: pathlib.Path) -> None:
    if not run_root.exists():
        return
    if not run_root.is_dir():
        raise NotADirectoryError(f"运行目录不是目录：{run_root}")
    entries = {path.name: path for path in run_root.iterdir()}
    allowed_control_files = {"monitor.pid", "monitor.json", "interrupt.request", "request.json", "bootstrap.log"}
    unexpected_entries = set(entries) - allowed_control_files
    invalid_control_entries = {
        name
        for name, path in entries.items()
        if name in allowed_control_files and not path.is_file()
    }
    if unexpected_entries or invalid_control_entries:
        raise FileExistsError(
            "运行目录含既有现场，禁止覆盖或自动恢复："
            f"{sorted(unexpected_entries | invalid_control_entries)}"
        )


def acquire_global_lock(operation_name: str, run_root: pathlib.Path) -> None:
    global_lock_owner_path = GLOBAL_LOCK_PATH / "owner.json"
    GLOBAL_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        GLOBAL_LOCK_PATH.mkdir()
    except FileExistsError as error:
        owner_text = ""
        try:
            owner_text = global_lock_owner_path.read_text(encoding="utf-8")
        except OSError:
            pass
        detail = f"；owner={owner_text}" if owner_text else ""
        raise ActiveFormalRunError(
            f"已有正式 operations batch 持有全局锁{detail}"
        ) from error

    # If owner publication fails, retain the directory lock for manual review.
    atomic_write_json(
        global_lock_owner_path,
        {
            "protocol_version": PROTOCOL_VERSION,
            "operation_name": operation_name,
            "run_root": str(run_root),
            "worker_pid": os.getpid(),
            "acquired_at": utc_now_text(),
        },
    )


def release_global_lock(operation_name: str, run_root: pathlib.Path) -> None:
    global_lock_owner_path = GLOBAL_LOCK_PATH / "owner.json"
    owner = json.loads(global_lock_owner_path.read_text(encoding="utf-8"))
    expected_owner = (
        owner.get("worker_pid") == os.getpid()
        and owner.get("operation_name") == operation_name
        and pathlib.Path(owner.get("run_root", "")).resolve() == run_root
    )
    if not expected_owner:
        raise RuntimeError("全局锁 owner 已变化，拒绝释放。")
    unexpected_entries = {
        path.name for path in GLOBAL_LOCK_PATH.iterdir()
    } - {"owner.json"}
    if unexpected_entries:
        raise RuntimeError(
            f"全局锁目录含未知内容，拒绝释放：{sorted(unexpected_entries)}"
        )
    global_lock_owner_path.unlink()
    GLOBAL_LOCK_PATH.rmdir()


def terminal_exit_code(state: str) -> int:
    return {
        "succeeded": 0,
        "failed": 1,
        "quota_stopped": 3,
        "interrupted": 130,
    }[state]


def record_terminal_state(
    *,
    run_root: pathlib.Path,
    status_path: pathlib.Path,
    status: dict[str, Any],
    state: str,
    error: str | None,
) -> int:
    if state not in TERMINAL_STATES:
        raise ValueError(f"未知终态：{state}")
    exit_code = terminal_exit_code(state)
    terminal_status = {
        **status,
        "state": state,
        "phase": "terminal",
        "finished_at": utc_now_text(),
        "heartbeat_at": utc_now_text(),
        "child_pid": None,
        "error": error,
        "exit_code": exit_code,
    }
    if state != "succeeded":
        atomic_write_json(
            run_root / "failure.json",
            {
                "protocol_version": PROTOCOL_VERSION,
                "operation_name": terminal_status["operation_name"],
                "state": state,
                "phase": "terminal",
                "exit_code": exit_code,
                "stage_index": terminal_status["stage_index"],
                "stage_name": terminal_status["stage_name"],
                "stage_exit_code": terminal_status["stage_exit_code"],
                "error": error,
                "recent_lines": terminal_status["recent_lines"],
                "recorded_at": utc_now_text(),
            },
        )
    atomic_write_json(status_path, terminal_status)
    status.clear()
    status.update(terminal_status)
    try:
        print(
            f"job_state={state}; operation={status['operation_name']}; "
            f"exit_code={exit_code}",
            flush=True,
        )
    except OSError:
        pass
    return exit_code


def finalize_owned_run(
    *,
    operation_name: str,
    run_root: pathlib.Path,
    status_path: pathlib.Path,
    status: dict[str, Any],
    state: str,
    error: str | None,
) -> int:
    """Publish the desired terminal state, then visibly account for unlock."""

    original_exit_code = record_terminal_state(
        run_root=run_root,
        status_path=status_path,
        status=status,
        state=state,
        error=error,
    )
    owner_path = GLOBAL_LOCK_PATH / "owner.json"
    owner_text = None
    owner_payload: Any = None
    try:
        owner_text = owner_path.read_text(encoding="utf-8")
        try:
            owner_payload = json.loads(owner_text)
        except json.JSONDecodeError:
            owner_payload = None
    except OSError as owner_error:
        owner_text = f"owner_read_failed: {owner_error}"

    try:
        release_global_lock(operation_name, run_root)
    except Exception as release_error:
        control_error = f"{type(release_error).__name__}: {release_error}"
        control_failure = {
            "protocol_version": PROTOCOL_VERSION,
            "operation_name": operation_name,
            "mode": "formal",
            "original_terminal_state": state,
            "original_exit_code": original_exit_code,
            "original_terminal_error": error,
            "owner": owner_payload,
            "owner_text": owner_text,
            "lock_path": str(GLOBAL_LOCK_PATH),
            "error": control_error,
            "recorded_at": utc_now_text(),
        }
        control_failure_path = run_root / "control_failure.json"
        try:
            atomic_write_json(control_failure_path, control_failure)
        except Exception as control_write_error:
            control_error += (
                "; control_failure_write_failed: "
                f"{type(control_write_error).__name__}: {control_write_error}"
            )

        visible_error = (
            "global_lock_release_failed: "
            f"{control_error}；原终态={state}/{original_exit_code}；"
            "锁保留，请人工核查。"
        )
        try:
            return record_terminal_state(
                run_root=run_root,
                status_path=status_path,
                status=status,
                state="failed",
                error=visible_error,
            )
        except Exception as second_status_error:
            try:
                print(
                    "lock_release_failure_status_publish_failed: "
                    f"{second_status_error}; control_failure="
                    f"{control_failure_path}",
                    flush=True,
                )
            except OSError:
                pass
            return 1
    return original_exit_code


def record_bootstrap_terminal_state(
    *,
    operation_name: str,
    run_root: pathlib.Path,
    status_path: pathlib.Path,
    stage_total: int,
    state: str,
    stage_name: str,
    progress: str,
    error: str,
    release_owned_lock: bool = False,
) -> int:
    """Make a pre-child failure visible without touching any global lock."""

    bootstrap_time = utc_now_text()
    bootstrap_status = {
        "protocol_version": PROTOCOL_VERSION,
        "operation_name": operation_name,
        "mode": "formal",
        "state": "starting",
        "phase": "bootstrap",
        "started_at": bootstrap_time,
        "finished_at": None,
        "heartbeat_at": bootstrap_time,
        "worker_pid": os.getpid(),
        "child_pid": None,
        "monitor_pid": monitor_pid_from_file(run_root),
        "stage_index": 0,
        "stage_total": stage_total,
        "stage_name": stage_name,
        "stage_started_at": None,
        "stage_exit_code": None,
        "progress": progress,
        "last_line": "",
        "recent_lines": [],
        "error": None,
        "exit_code": None,
    }
    if release_owned_lock:
        return finalize_owned_run(
            operation_name=operation_name,
            run_root=run_root,
            status_path=status_path,
            status=bootstrap_status,
            state=state,
            error=error,
        )
    return record_terminal_state(
        run_root=run_root,
        status_path=status_path,
        status=bootstrap_status,
        state=state,
        error=error,
    )


def run_batch(
    *,
    operation_name: str,
    run_root: pathlib.Path,
    stages: tuple[StageSpec, ...],
    progress_prefixes: tuple[str, ...] = DEFAULT_PROGRESS_PREFIXES,
    preflight_stages: tuple[StageSpec, ...] | None = None,
) -> int:
    """Run one batch; trusted maintenance callers may explicitly omit preflights.

    Business request payloads must never be forwarded to preflight_stages.  The
    console owns the fixed maintenance allowlist; None retains all three checks.
    """

    if not operation_name.strip():
        raise ValueError("operation_name 不得为空。")
    if preflight_stages is None:
        preflight_stages = DEFAULT_PREFLIGHT_STAGES
    if not isinstance(preflight_stages, tuple):
        raise TypeError("preflight 阶段必须是不可变 tuple 或 None。")
    if preflight_stages:
        validate_stage_specs(preflight_stages, "preflight")
    validate_stage_specs(stages, "业务")
    resolved_run_root = validate_run_root(run_root)
    ensure_fresh_or_monitor_only_run_root(resolved_run_root)

    lock_acquired = False
    child_tree_stopped = True
    tracked_processes: dict[int, psutil.Process] = {}
    status_path = resolved_run_root / "status.json"
    status: dict[str, Any] | None = None
    exit_code = 1

    try:
        acquire_global_lock(operation_name, resolved_run_root)
        lock_acquired = True
        ensure_fresh_or_monitor_only_run_root(resolved_run_root)
        resolved_run_root.mkdir(parents=True, exist_ok=True)

        atomic_write_json(
            resolved_run_root / "command_manifest.json",
            {
                "protocol_version": PROTOCOL_VERSION,
                "operation_name": operation_name,
                "mode": "formal",
                "created_at": utc_now_text(),
                "project_root": str(PROJECT_ROOT),
                "run_root": str(resolved_run_root),
                "preflight_total": len(preflight_stages),
                "preflights": [
                    {
                        "index": index,
                        "name": stage.name,
                        "command": list(stage.command()),
                    }
                    for index, stage in enumerate(preflight_stages, start=1)
                ],
                "stage_total": len(stages),
                "stages": [
                    {
                        "index": index,
                        "name": stage.name,
                        "command": list(stage.command()),
                    }
                    for index, stage in enumerate(stages, start=1)
                ],
            },
        )
        atomic_write_text(
            resolved_run_root / "worker.pid",
            f"{os.getpid()}\n",
        )

        status = {
            "protocol_version": PROTOCOL_VERSION,
            "operation_name": operation_name,
            "mode": "formal",
            "state": "starting",
            "phase": "initializing",
            "started_at": utc_now_text(),
            "finished_at": None,
            "heartbeat_at": utc_now_text(),
            "worker_pid": os.getpid(),
            "child_pid": None,
            "monitor_pid": None,
            "stage_index": 0,
            "stage_total": len(stages),
            "stage_name": "初始化",
            "stage_started_at": None,
            "stage_exit_code": None,
            "progress": "initializing",
            "last_line": "",
            "recent_lines": [],
            "error": None,
            "exit_code": None,
        }
        atomic_write_json(status_path, status)
        monitor_pid = wait_for_required_monitor(
            resolved_run_root,
            status_path,
            status,
        )
        validate_stage_entrypoints(preflight_stages, "preflight")
        validate_stage_entrypoints(stages, "业务")
        validate_progress_prefixes(progress_prefixes)

        for preflight_index, preflight_stage in enumerate(
            preflight_stages,
            start=1,
        ):
            result = run_stage(
                stage=preflight_stage,
                phase="preflight",
                stage_index=preflight_index,
                stage_total=len(preflight_stages),
                run_root=resolved_run_root,
                status_path=status_path,
                status=status,
                monitor_pid=monitor_pid,
                progress_prefixes=progress_prefixes,
                tracked_processes=tracked_processes,
            )
            child_tree_stopped = not tracked_processes
            raise_if_manual_interruption(resolved_run_root)
            if result != "succeeded":
                terminal_state = (
                    "interrupted" if result == "interrupted" else "failed"
                )
                exit_code = finalize_owned_run(
                    operation_name=operation_name,
                    run_root=resolved_run_root,
                    status_path=status_path,
                    status=status,
                    state=terminal_state,
                    error=f"自动 preflight {preflight_stage.name} 未通过。",
                )
                return exit_code

        for stage_index, stage in enumerate(stages, start=1):
            result = run_stage(
                stage=stage,
                phase="business",
                stage_index=stage_index,
                stage_total=len(stages),
                run_root=resolved_run_root,
                status_path=status_path,
                status=status,
                monitor_pid=monitor_pid,
                progress_prefixes=progress_prefixes,
                tracked_processes=tracked_processes,
            )
            child_tree_stopped = not tracked_processes
            raise_if_manual_interruption(resolved_run_root)
            if result != "succeeded":
                if result == "quota_stopped":
                    error = "业务来源配额停止；未自动重试或恢复。"
                elif result == "interrupted":
                    error = f"阶段 {stage.name} 被中断。"
                else:
                    error = f"阶段 {stage.name} 执行失败。"
                exit_code = finalize_owned_run(
                    operation_name=operation_name,
                    run_root=resolved_run_root,
                    status_path=status_path,
                    status=status,
                    state=result,
                    error=error,
                )
                return exit_code

        raise_if_manual_interruption(resolved_run_root)
        if not monitor_is_healthy(resolved_run_root, monitor_pid):
            raise RuntimeError(
                "monitor_unavailable: 成功终态发布前可见监控进程已退出。"
            )
        status.update(
            {
                "stage_index": len(stages),
                "stage_total": len(stages),
                "stage_name": "全部阶段完成",
                "progress": f"business={len(stages)}/{len(stages)}",
            }
        )
        exit_code = finalize_owned_run(
            operation_name=operation_name,
            run_root=resolved_run_root,
            status_path=status_path,
            status=status,
            state="succeeded",
            error=None,
        )
        child_tree_stopped = not tracked_processes
        return exit_code
    except ActiveFormalRunError as error:
        # This worker never owned the existing lock.  Publish only into the
        # losing run's fresh directory so its visible monitor reaches a
        # terminal state; do not touch the active lock or its owner bytes.
        try:
            ensure_fresh_or_monitor_only_run_root(resolved_run_root)
            resolved_run_root.mkdir(parents=True, exist_ok=True)
            return record_bootstrap_terminal_state(
                operation_name=operation_name,
                run_root=resolved_run_root,
                status_path=status_path,
                stage_total=len(stages),
                state="failed",
                stage_name="全局 active formal run 锁门禁",
                progress="lock=blocked",
                error=(
                    f"active_formal_run: {error}；不得自动清锁或续跑，"
                    "请人工核查。"
                ),
            )
        except Exception as publication_error:
            try:
                print(
                    "active_lock_failure_publish_failed: "
                    f"{publication_error}; existing_lock=untouched",
                    flush=True,
                )
            except OSError:
                pass
            return 1
    except ManualInterruption as error:
        child_tree_stopped = stop_tracked_processes(None, tracked_processes)
        interruption_error = f"manual_interruption: {error}"
        if status is None and child_tree_stopped:
            try:
                if not lock_acquired:
                    ensure_fresh_or_monitor_only_run_root(resolved_run_root)
                resolved_run_root.mkdir(parents=True, exist_ok=True)
                return record_bootstrap_terminal_state(
                    operation_name=operation_name,
                    run_root=resolved_run_root,
                    status_path=status_path,
                    stage_total=len(stages),
                    state="interrupted",
                    stage_name="人工中断",
                    progress="interrupt=requested",
                    error=interruption_error,
                    release_owned_lock=lock_acquired,
                )
            except Exception:
                return 130
        if status is None or not child_tree_stopped:
            return 130
        try:
            return finalize_owned_run(
                operation_name=operation_name,
                run_root=resolved_run_root,
                status_path=status_path,
                status=status,
                state="interrupted",
                error=interruption_error,
            )
        except Exception as terminal_error:
            try:
                print(
                    f"terminal_publish_failed: {terminal_error}; "
                    "global_lock=retained",
                    flush=True,
                )
            except OSError:
                pass
            return 130
    except KeyboardInterrupt:
        child_tree_stopped = stop_tracked_processes(None, tracked_processes)
        if status is None and child_tree_stopped:
            try:
                if not lock_acquired:
                    ensure_fresh_or_monitor_only_run_root(resolved_run_root)
                resolved_run_root.mkdir(parents=True, exist_ok=True)
                exit_code = record_bootstrap_terminal_state(
                    operation_name=operation_name,
                    run_root=resolved_run_root,
                    status_path=status_path,
                    stage_total=len(stages),
                    state="interrupted",
                    stage_name="控制面启动中断",
                    progress="bootstrap=interrupted",
                    error="worker 在初始 status 前收到 KeyboardInterrupt。",
                    release_owned_lock=lock_acquired,
                )
                return exit_code
            except Exception:
                pass
        if status is None or not child_tree_stopped:
            try:
                print(
                    f"job_state=interrupted; operation={operation_name}; "
                    "exit_code=130; global_lock=retained",
                    flush=True,
                )
            except OSError:
                pass
            return 130
        try:
            exit_code = finalize_owned_run(
                operation_name=operation_name,
                run_root=resolved_run_root,
                status_path=status_path,
                status=status,
                state="interrupted",
                error="worker 收到 KeyboardInterrupt；未自动恢复。",
            )
            return exit_code
        except Exception as terminal_error:
            try:
                print(
                    f"terminal_publish_failed: {terminal_error}; "
                    "global_lock=retained",
                    flush=True,
                )
            except OSError:
                pass
            return 130
    except Exception as error:
        child_tree_stopped = stop_tracked_processes(None, tracked_processes)
        error_text = f"{type(error).__name__}: {error}"
        if status is None and child_tree_stopped:
            try:
                if not lock_acquired:
                    ensure_fresh_or_monitor_only_run_root(resolved_run_root)
                resolved_run_root.mkdir(parents=True, exist_ok=True)
                exit_code = record_bootstrap_terminal_state(
                    operation_name=operation_name,
                    run_root=resolved_run_root,
                    status_path=status_path,
                    stage_total=len(stages),
                    state="failed",
                    stage_name="控制面启动失败",
                    progress="bootstrap=failed",
                    error=error_text,
                    release_owned_lock=lock_acquired,
                )
                return exit_code
            except Exception as publication_error:
                error_text += (
                    "; bootstrap_failure_publish_failed: "
                    f"{publication_error}"
                )
        if status is None or not child_tree_stopped:
            try:
                print(
                    f"job_state=failed; operation={operation_name}; "
                    f"exit_code=1; error={error_text}; global_lock=retained",
                    flush=True,
                )
            except OSError:
                pass
            return 1
        try:
            exit_code = finalize_owned_run(
                operation_name=operation_name,
                run_root=resolved_run_root,
                status_path=status_path,
                status=status,
                state="failed",
                error=error_text,
            )
            return exit_code
        except Exception as terminal_error:
            try:
                print(
                    f"terminal_publish_failed: {terminal_error}; "
                    "global_lock=retained",
                    flush=True,
                )
            except OSError:
                pass
            return 1
