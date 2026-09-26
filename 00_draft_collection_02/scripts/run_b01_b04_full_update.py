"""顺序执行 b01-b04 的 18 个日常正式阶段，并为独立 Terminal 提供状态与心跳。

这是经用户授权的单批草稿 worker，不是定时任务、守护服务或自动恢复器。
它从不重试；普通失败、quota_stop 或正式模式 monitor 消失都会停止后续阶段。
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import queue
import subprocess
import sys
import threading
import time
from collections import deque
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


COLLECTION_ROOT = (
    PROJECT_ROOT / "02_Futures_Lakehouse"
)
B01_ROOT = COLLECTION_ROOT / "a01_Futures_Market_Data"
B02_ROOT = COLLECTION_ROOT / "a02_Futures_Exchange_Reports"
B03_ROOT = COLLECTION_ROOT / "a03_External_Market_Data"
B04_ROOT = COLLECTION_ROOT / "a04_Macro_And_Interest_Rates"
ALLOWED_RUN_ROOT = PROJECT_ROOT / "00_draft_collection_02" / "run_status"
HEARTBEAT_SECONDS = 2.0
MONITOR_START_TIMEOUT_SECONDS = 60.0
STATUS_REPLACE_ATTEMPTS = 50
STATUS_REPLACE_RETRY_SECONDS = 0.1
PROGRESS_PREFIXES = (
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

STAGES = (
    ("a01/b01_trade_calendar", B01_ROOT / "b01_trade_calendar.py", True, ()),
    ("a01/b02_futures_variety_calendar", B01_ROOT / "b02_futures_variety_calendar.py", True, ()),
    ("a01/b03_futures_contract_calendar", B01_ROOT / "b03_futures_contract_calendar.py", True, ()),
    ("a01/b04_futures_bar_calendar", B01_ROOT / "b04_futures_bar_calendar.py", True, ()),
    ("a01/b05_futures_daily", B01_ROOT / "b05_futures_daily.py", True, ()),
    ("a01/b06_futures_minute", B01_ROOT / "b06_futures_minute.py", True, ()),
    ("a01/b07_suspected_session_reconciliation", B01_ROOT / "b07_suspected_session_reconciliation.py", True, ()),
    ("a02/b01_exchange_report_calendar", B02_ROOT / "b01_exchange_report_calendar.py", True, ()),
    (
        "a02/b01a_position_rank_special_case_calibration",
        B02_ROOT / "b01a_position_rank_special_case_calibration.py",
        False,
        (),
    ),
    ("a02/b02_futures_holding_reports", B02_ROOT / "b02_futures_holding_reports.py", True, ()),
    ("a02/b03_warehouse_receipt", B02_ROOT / "b03_warehouse_receipt.py", True, ()),
    ("a03/b01_external_market_calendar", B03_ROOT / "b01_external_market_calendar.py", True, ()),
    ("a03/b02_domestic_spot_basis", B03_ROOT / "b02_domestic_spot_basis.py", True, ()),
    ("a03/b03_overseas_futures", B03_ROOT / "b03_overseas_futures.py", True, ()),
    ("a03/b04_external_index", B03_ROOT / "b04_external_index.py", True, ()),
    ("a04/b01_macro_release_calendar", B04_ROOT / "b01_macro_release_calendar.py", True, ()),
    ("a04/b02_interest_rate", B04_ROOT / "b02_interest_rate.py", True, ()),
    ("a04/b03_macro_release", B04_ROOT / "b03_macro_release.py", True, ()),
)


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    """同目录写临时文件后原子替换，monitor 永远只读取完整 JSON。"""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    # Windows 上 monitor、杀毒软件或索引器可能短暂占用目标文件。
    # 这里只重试状态文件的原子发布，不重试任何业务请求或业务阶段。
    for attempt in range(1, STATUS_REPLACE_ATTEMPTS + 1):
        try:
            os.replace(temporary_path, path)
            break
        except PermissionError as error:
            if (
                getattr(error, "winerror", None) not in {5, 32}
                or attempt == STATUS_REPLACE_ATTEMPTS
            ):
                raise
            time.sleep(STATUS_REPLACE_RETRY_SECONDS)


def build_stage_commands(
    mode: str,
    run_root: pathlib.Path,
    sample_start: str | None,
    sample_end: str | None,
    sample_lake_root: pathlib.Path | None = None,
) -> list[dict[str, Any]]:
    commands = []
    sample_lake = sample_lake_root or (run_root / "sample_lake")

    for stage_index, (
        stage_name,
        script_path,
        supports_dates,
        extra_arguments,
    ) in enumerate(STAGES, start=1):
        command = [sys.executable, str(script_path)]
        if mode == "sample":
            if sample_start is None or sample_end is None:
                raise ValueError("sample 模式必须同时提供起止日期。")
            command.extend(["--lake-root", str(sample_lake)])
            if supports_dates:
                command.extend([
                    "--start-date",
                    sample_start,
                    "--end-date",
                    sample_end,
                ])
        command.extend(extra_arguments)
        command.append("--write")
        commands.append({
            "index": stage_index,
            "name": stage_name,
            "command": command,
        })

    return commands


def process_is_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        process = psutil.Process(pid)
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except (psutil.Error, ValueError):
        return False


def monitor_pid_from_file(run_root: pathlib.Path) -> int | None:
    monitor_pid_path = run_root / "monitor.pid"
    if not monitor_pid_path.is_file():
        return None
    try:
        return int(monitor_pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def wait_for_required_monitor(
    run_root: pathlib.Path,
    status_path: pathlib.Path,
    status: dict[str, Any],
) -> int:
    deadline = time.monotonic() + MONITOR_START_TIMEOUT_SECONDS
    status["state"] = "waiting_for_monitor"
    status["stage_name"] = "等待用户可见 Terminal monitor"

    while time.monotonic() < deadline:
        monitor_pid = monitor_pid_from_file(run_root)
        status["heartbeat_at"] = utc_now_text()
        status["monitor_pid"] = monitor_pid
        atomic_write_json(status_path, status)
        if process_is_alive(monitor_pid):
            return int(monitor_pid)
        time.sleep(1.0)

    raise RuntimeError("monitor_unavailable: 60 秒内未发现存活的可见监控进程。")


def terminate_child(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def classify_stage_result(return_code: int, saw_quota_stop: bool) -> str:
    if saw_quota_stop:
        return "quota_stopped"
    if return_code != 0:
        return "failed"
    return "succeeded"


def run_stage(
    stage: dict[str, Any],
    stage_index: int,
    stage_total: int,
    run_root: pathlib.Path,
    status_path: pathlib.Path,
    status: dict[str, Any],
    monitor_pid: int | None,
) -> str:
    """运行一个业务子进程；主线程持续写心跳并消费输出。"""

    stage_name = str(stage["name"])
    command = [str(part) for part in stage["command"]]
    safe_stage_name = stage_name.replace("/", "_")
    log_path = run_root / "logs" / f"{stage_index:02d}_{safe_stage_name}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    child_environment = os.environ.copy()
    child_environment["PYTHONUTF8"] = "1"
    child_environment["PYTHONIOENCODING"] = "utf-8"
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    with log_path.open("w", encoding="utf-8", newline="\n") as log_handle:
        process = subprocess.Popen(
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

        output_queue: queue.Queue[str] = queue.Queue()

        def read_output() -> None:
            assert process.stdout is not None
            for raw_line in process.stdout:
                output_queue.put(raw_line)
            process.stdout.close()

        reader = threading.Thread(target=read_output, daemon=True)
        reader.start()

        recent_lines: deque[str] = deque(maxlen=40)
        saw_quota_stop = False
        last_heartbeat = 0.0
        status.update({
            "state": "running",
            "stage_index": stage_index,
            "stage_total": stage_total,
            "stage_name": stage_name,
            "stage_started_at": utc_now_text(),
            "child_pid": process.pid,
            "progress": f"stage={stage_index}/{stage_total}",
            "last_line": "",
            "error": None,
        })

        while (
            process.poll() is None
            or reader.is_alive()
            or not output_queue.empty()
        ):
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
                if line.startswith(PROGRESS_PREFIXES):
                    status["progress"] = line
                if "quota_stop:" in line:
                    saw_quota_stop = True

            now = time.monotonic()
            if now - last_heartbeat >= HEARTBEAT_SECONDS:
                if monitor_pid is not None and not process_is_alive(monitor_pid):
                    terminate_child(process)
                    raise RuntimeError(
                        "monitor_unavailable: 可见监控进程已退出，业务子进程已停止。"
                    )
                status["heartbeat_at"] = utc_now_text()
                status["monitor_pid"] = monitor_pid
                status["recent_lines"] = list(recent_lines)[-12:]
                try:
                    atomic_write_json(status_path, status)
                except Exception:
                    # 状态发布本身失效时也必须明确停止业务子进程。
                    terminate_child(process)
                    raise
                last_heartbeat = now

            time.sleep(0.2)

        reader.join(timeout=5)
        log_handle.flush()
        os.fsync(log_handle.fileno())

    return_code = int(process.returncode)
    result = classify_stage_result(return_code, saw_quota_stop)
    status["child_pid"] = None
    status["stage_exit_code"] = return_code
    status["heartbeat_at"] = utc_now_text()
    status["recent_lines"] = list(recent_lines)[-12:]
    atomic_write_json(status_path, status)
    return result


def run_job(
    mode: str,
    run_root: pathlib.Path,
    sample_start: str | None,
    sample_end: str | None,
    start_stage: int = 1,
    sample_lake_root: pathlib.Path | None = None,
    require_monitor: bool = False,
) -> int:
    run_root = run_root.resolve()
    allowed_root = ALLOWED_RUN_ROOT.resolve()
    if not run_root.is_relative_to(allowed_root):
        raise ValueError(f"运行目录必须位于 {allowed_root}。")

    run_root.mkdir(parents=True, exist_ok=True)
    status_path = run_root / "status.json"
    if status_path.exists():
        raise FileExistsError(f"运行状态已经存在，禁止覆盖或自动恢复：{status_path}")

    if start_stage < 1 or start_stage > len(STAGES):
        raise ValueError(f"start_stage 必须位于 1..{len(STAGES)}。")
    if mode == "formal" and (start_stage != 1 or sample_lake_root is not None):
        raise ValueError("正式模式禁止跳过阶段或指定测试湖。")

    resolved_sample_lake = None
    if sample_lake_root is not None:
        resolved_sample_lake = sample_lake_root.resolve()
        if not resolved_sample_lake.is_relative_to(allowed_root):
            raise ValueError("续跑测试湖必须位于 00_draft_collection_02/run_status。")
        if not resolved_sample_lake.is_dir():
            raise FileNotFoundError(f"续跑测试湖不存在：{resolved_sample_lake}")

    all_commands = build_stage_commands(
        mode,
        run_root,
        sample_start,
        sample_end,
        resolved_sample_lake,
    )
    commands = [stage for stage in all_commands if stage["index"] >= start_stage]
    manifest = {
        "mode": mode,
        "created_at": utc_now_text(),
        "project_root": str(PROJECT_ROOT),
        "run_root": str(run_root),
        "sample_start": sample_start,
        "sample_end": sample_end,
        "sample_lake_root": str(resolved_sample_lake) if resolved_sample_lake else None,
        "start_stage": start_stage,
        "require_monitor": require_monitor,
        "stage_total": len(STAGES),
        "stages": commands,
    }
    atomic_write_json(run_root / "command_manifest.json", manifest)
    (run_root / "worker.pid").write_text(str(os.getpid()), encoding="utf-8")

    status: dict[str, Any] = {
        "mode": mode,
        "state": "starting",
        "started_at": utc_now_text(),
        "finished_at": None,
        "heartbeat_at": utc_now_text(),
        "worker_pid": os.getpid(),
        "child_pid": None,
        "monitor_pid": None,
        "stage_index": 0,
        "stage_total": len(STAGES),
        "stage_name": "初始化",
        "stage_started_at": None,
        "stage_exit_code": None,
        "progress": f"stage={start_stage - 1}/{len(STAGES)}",
        "last_line": "",
        "recent_lines": [],
        "error": None,
    }
    atomic_write_json(status_path, status)

    monitor_pid = None
    try:
        if mode == "formal" or require_monitor:
            monitor_pid = wait_for_required_monitor(run_root, status_path, status)

        for stage in commands:
            stage_index = int(stage["index"])
            result = run_stage(
                stage,
                stage_index,
                len(STAGES),
                run_root,
                status_path,
                status,
                monitor_pid,
            )
            if result != "succeeded":
                status["state"] = result
                status["finished_at"] = utc_now_text()
                status["error"] = (
                    "JQData 配额不足；未自动恢复。"
                    if result == "quota_stopped"
                    else f"阶段 {stage['name']} 执行失败。"
                )
                status["heartbeat_at"] = utc_now_text()
                atomic_write_json(status_path, status)
                atomic_write_json(
                    run_root / "failure.json",
                    {
                        "state": result,
                        "stage_index": stage_index,
                        "stage_name": stage["name"],
                        "stage_exit_code": status["stage_exit_code"],
                        "error": status["error"],
                        "recent_lines": status["recent_lines"],
                        "recorded_at": utc_now_text(),
                    },
                )
                print(f"job_state={result}; stage={stage['name']}", flush=True)
                return 3 if result == "quota_stopped" else 1

        status.update({
            "state": "succeeded",
            "finished_at": utc_now_text(),
            "heartbeat_at": utc_now_text(),
            "child_pid": None,
            "stage_index": len(STAGES),
            "stage_name": "全部阶段完成",
            "progress": f"stage={len(STAGES)}/{len(STAGES)}",
        })
        atomic_write_json(status_path, status)
        print(f"job_state=succeeded; stages={len(STAGES)}/{len(STAGES)}", flush=True)
        return 0
    except Exception as error:
        status.update({
            "state": "failed",
            "finished_at": utc_now_text(),
            "heartbeat_at": utc_now_text(),
            "child_pid": None,
            "error": f"{type(error).__name__}: {error}",
        })
        atomic_write_json(status_path, status)
        atomic_write_json(
            run_root / "failure.json",
            {
                "state": "failed",
                "stage_index": status["stage_index"],
                "stage_name": status["stage_name"],
                "error": status["error"],
                "recent_lines": status["recent_lines"],
                "recorded_at": utc_now_text(),
            },
        )
        print(f"job_state=failed; error={status['error']}", flush=True)
        return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("sample", "formal"), required=True)
    parser.add_argument("--run-root", type=pathlib.Path, required=True)
    parser.add_argument("--sample-start")
    parser.add_argument("--sample-end")
    parser.add_argument("--start-stage", type=int, default=1)
    parser.add_argument("--sample-lake-root", type=pathlib.Path)
    parser.add_argument("--require-monitor", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return run_job(
        args.mode,
        args.run_root,
        args.sample_start,
        args.sample_end,
        args.start_stage,
        args.sample_lake_root,
        args.require_monitor,
    )


if __name__ == "__main__":
    raise SystemExit(main())
