"""Shared control plane for one explicitly authorized formal collection batch.

The worker has no schedule, daemon loop, resume path, or business retry.  Every
caller supplies an immutable stage manifest and a fresh run directory.  A
separate visible PowerShell monitor is mandatory for the whole batch.
"""

from __future__ import annotations

import json
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
    / "a02_Data_Collection_Operations"
)
COLLECTION_ROOT = (
    PROJECT_ROOT / "02_Futures_Lakehouse" / "a01_Data_Collection"
)
RUN_HISTORY_ROOT = OPERATIONS_ROOT / "run_history"
GLOBAL_LOCK_PATH = RUN_HISTORY_ROOT / ".active_formal_run"
HEARTBEAT_SECONDS = 2.0
MONITOR_START_TIMEOUT_SECONDS = 60.0
STATUS_REPLACE_ATTEMPTS = 50
STATUS_REPLACE_RETRY_SECONDS = 0.1
TERMINATION_TIMEOUT_SECONDS = 10.0
DEFAULT_PROGRESS_PREFIXES = (
    "planning_progress:",
    "auto_plan:",
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
        PROJECT_ROOT / "02_Futures_Lakehouse" / "verify_runtime.py",
    ),
    StageSpec(
        "preflight/02_operations_runtime",
        OPERATIONS_ROOT / "verify_operations_runtime.py",
    ),
    StageSpec(
        "preflight/03_notebook_export_sync",
        COLLECTION_ROOT / "b00_sync_notebook_exports.py",
        ("--check",),
    ),
)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


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
            "stage_name": "等待用户可见 Terminal monitor",
            "progress": "monitor=waiting",
        }
    )

    while time.monotonic() < deadline:
        raise_if_manual_interruption(run_root)
        monitor_pid = monitor_pid_from_file(run_root)
        status["heartbeat_at"] = utc_now_text()
        status["monitor_pid"] = monitor_pid
        atomic_write_json(status_path, status)
        if process_is_alive(monitor_pid):
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
    if not process_is_alive(monitor_pid):
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
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    direct_process: subprocess.Popen[str] | None = None
    reader: threading.Thread | None = None
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
            "recent_lines": [],
            "error": None,
        }
    )
    atomic_write_json(status_path, status)

    try:
        with log_path.open("w", encoding="utf-8", newline="\n") as log_handle:
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

            output_queue: queue.Queue[str] = queue.Queue()

            def read_output() -> None:
                assert direct_process is not None
                assert direct_process.stdout is not None
                for raw_line in direct_process.stdout:
                    output_queue.put(raw_line)
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
                while True:
                    try:
                        raw_line = output_queue.get_nowait()
                    except queue.Empty:
                        break

                    line = raw_line.rstrip("\r\n")
                    log_handle.write(raw_line)
                    log_handle.flush()
                    recent_lines.append(line)
                    if line:
                        status["last_line"] = line
                    if line.startswith(progress_prefixes):
                        status["progress"] = line
                    if "quota_stop:" in line:
                        saw_quota_stop = True

                now = time.monotonic()
                if now - last_heartbeat >= HEARTBEAT_SECONDS:
                    if not process_is_alive(monitor_pid):
                        raise RuntimeError(
                            "monitor_unavailable: 可见监控进程已退出。"
                        )
                    status["heartbeat_at"] = utc_now_text()
                    status["monitor_pid"] = monitor_pid
                    status["recent_lines"] = list(recent_lines)[-12:]
                    atomic_write_json(status_path, status)
                    last_heartbeat = now

                time.sleep(0.1)

            discover_descendants(direct_process, tracked_processes)
            leaked_descendants = leaked_descendants or any(
                process.pid != direct_process.pid
                and tracked_process_is_alive(process)
                for process in tracked_processes.values()
            )
            reader.join(timeout=5)
            log_handle.flush()
            os.fsync(log_handle.fileno())
    except BaseException:
        if not stop_tracked_processes(direct_process, tracked_processes):
            raise RuntimeError(
                "child_tree_cleanup_failed: 子进程树未能全部停止；全局锁保留。"
            )
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
    if not process_is_alive(monitor_pid):
        raise RuntimeError(
            "monitor_unavailable: 阶段结束时可见监控进程已退出。"
        )

    result = classify_stage_result(return_code, saw_quota_stop)
    if leaked_descendants and result == "succeeded":
        result = "failed"
        status["last_line"] = "child_tree_leak: 已停止入口遗留的子进程。"
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

    for stage in stages:
        stage_identity = " ".join(
            (stage.name, str(stage.entrypoint_path), *stage.arguments)
        )
        if "c08" in stage_identity.casefold():
            raise ValueError("c08 不得进入任何 operations batch。")


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
    allowed_control_files = {"monitor.pid", "interrupt.request"}
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
) -> int:
    """Run one explicit formal batch, with no retry or implicit stage selection."""

    if not operation_name.strip():
        raise ValueError("operation_name 不得为空。")
    preflight_stages = DEFAULT_PREFLIGHT_STAGES
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
        if not process_is_alive(monitor_pid):
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
