from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
import pathlib
import threading
import time
import traceback

import nbformat
from nbclient import NotebookClient
import psutil


if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class AtomicStatusPublisher:
    def __init__(self, status_path: pathlib.Path, monitor_pid_path: pathlib.Path):
        self.status_path = status_path
        self.monitor_pid_path = monitor_pid_path
        self.started_at = time.monotonic()
        self.lock = threading.Lock()
        self.payload = {
            "protocol_version": 1,
            "operation_name": "item09_rolling_variance_forecasts",
            "phase": "starting",
            "worker_pid": os.getpid(),
            "completed_streams": 0,
            "total_streams": 120,
            "completed_selected_fits": 0,
            "total_selected_fits": 31_200,
            "reused_streams": 0,
            "newly_committed_streams": 0,
            "current_cell_index": None,
            "last_series_id": None,
            "last_model_id": None,
            "failure": None,
        }

    def monitor_pid(self) -> int | None:
        try:
            return int(self.monitor_pid_path.read_text(encoding="utf-8").strip())
        except (FileNotFoundError, ValueError, OSError):
            return None

    def monitor_is_alive(self) -> bool:
        monitor_pid = self.monitor_pid()
        if monitor_pid is None or not psutil.pid_exists(monitor_pid):
            return False
        try:
            monitor_process = psutil.Process(monitor_pid)
            if not monitor_process.is_running():
                return False
            monitor_command_line = " ".join(monitor_process.cmdline()).casefold()
            expected_monitor_path = str(
                pathlib.Path(__file__).with_name("monitor.ps1").resolve()
            ).casefold()
            return (
                expected_monitor_path in monitor_command_line
                and str(self.status_path).casefold() in monitor_command_line
                and str(self.monitor_pid_path).casefold() in monitor_command_line
            )
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False

    def publish(self, **updates) -> None:
        with self.lock:
            self.payload.update(updates)
            self.payload["monitor_pid"] = self.monitor_pid()
            self.payload["elapsed_seconds"] = round(
                time.monotonic() - self.started_at,
                3,
            )
            self.payload["heartbeat_at"] = datetime.now(timezone.utc).isoformat()
            serialized = json.dumps(
                self.payload,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            temporary_path = self.status_path.with_name(
                f".{self.status_path.name}.{os.getpid()}.tmp"
            )
            with temporary_path.open("w", encoding="utf-8", newline="\n") as stream:
                stream.write(serialized)
                stream.flush()
                os.fsync(stream.fileno())
            for attempt in range(20):
                try:
                    os.replace(temporary_path, self.status_path)
                    return
                except OSError as error:
                    if getattr(error, "winerror", None) not in {5, 32}:
                        raise
                    if attempt == 19:
                        raise
                    time.sleep(0.05 * (attempt + 1))


def terminate_descendants() -> None:
    try:
        worker_process = psutil.Process(os.getpid())
        descendants = worker_process.children(recursive=True)
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        return
    for descendant in reversed(descendants):
        try:
            descendant.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    _, alive = psutil.wait_procs(descendants, timeout=10)
    for descendant in alive:
        try:
            descendant.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    psutil.wait_procs(alive, timeout=5)


class ProgressNotebookClient(NotebookClient):
    def __init__(self, *args, status_publisher: AtomicStatusPublisher, **kwargs):
        self.status_publisher = status_publisher
        super().__init__(*args, **kwargs)

    def process_message(self, msg, cell, cell_index):
        if msg.get("msg_type") == "stream":
            text = msg.get("content", {}).get("text", "")
            for line in text.splitlines():
                if line.startswith("ITEM09_PROGRESS "):
                    progress = json.loads(line.removeprefix("ITEM09_PROGRESS "))
                    self.status_publisher.publish(
                        phase="rolling_forecast_batch",
                        current_cell_index=cell_index,
                        **progress,
                    )
        return super().process_message(msg, cell, cell_index)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--notebook", type=pathlib.Path, required=True)
    parser.add_argument("--status", type=pathlib.Path, required=True)
    parser.add_argument("--monitor-pid", type=pathlib.Path, required=True)
    parser.add_argument("--partial-notebook", type=pathlib.Path, required=True)
    args = parser.parse_args()

    args.notebook = args.notebook.resolve()
    args.status = args.status.resolve()
    args.monitor_pid = args.monitor_pid.resolve()
    args.partial_notebook = args.partial_notebook.resolve()
    args.status.parent.mkdir(parents=True, exist_ok=False) if not args.status.parent.exists() else None

    status_publisher = AtomicStatusPublisher(args.status, args.monitor_pid)
    status_publisher.publish(phase="waiting_for_visible_monitor")
    monitor_deadline = time.monotonic() + 60
    while not status_publisher.monitor_is_alive():
        if time.monotonic() >= monitor_deadline:
            status_publisher.publish(
                phase="failed",
                failure="visible monitor did not become live within 60 seconds",
            )
            return 1
        time.sleep(0.5)

    notebook = nbformat.read(args.notebook, as_version=4)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.execution_count = None
            cell.outputs = []

    client_holder = {"client": None}
    stop_heartbeat = threading.Event()

    def heartbeat_loop():
        while not stop_heartbeat.wait(10):
            try:
                if not status_publisher.monitor_is_alive():
                    status_publisher.publish(
                        phase="monitor_failed",
                        failure="visible monitor process is no longer alive",
                    )
                    terminate_descendants()
                    return
                status_publisher.publish()
            except Exception:
                terminate_descendants()
                os._exit(1)

    def write_partial_notebook():
        temporary_partial = args.partial_notebook.with_name(
            f".{args.partial_notebook.name}.tmp"
        )
        nbformat.write(notebook, temporary_partial)
        os.replace(temporary_partial, args.partial_notebook)

    def on_cell_start(cell, cell_index):
        status_publisher.publish(
            phase="executing_notebook_cell",
            current_cell_index=cell_index,
        )

    def on_cell_complete(cell, cell_index):
        write_partial_notebook()
        status_publisher.publish(
            phase="cell_completed",
            current_cell_index=cell_index,
        )

    heartbeat_thread = threading.Thread(
        target=heartbeat_loop,
        name="item09-status-heartbeat",
        daemon=True,
    )
    heartbeat_thread.start()

    try:
        status_publisher.publish(phase="starting_clean_latitude_kernel")
        client = ProgressNotebookClient(
            notebook,
            timeout=86_400,
            kernel_name="latitude",
            resources={"metadata": {"path": str(args.notebook.parent)}},
            allow_errors=False,
            record_timing=True,
            status_publisher=status_publisher,
            on_cell_start=on_cell_start,
            on_cell_complete=on_cell_complete,
        )
        client_holder["client"] = client
        client.execute()
        nbformat.validate(notebook)
        code_cells = [cell for cell in notebook.cells if cell.cell_type == "code"]
        expected_counts = list(range(1, len(code_cells) + 1))
        actual_counts = [cell.execution_count for cell in code_cells]
        if actual_counts != expected_counts:
            raise RuntimeError(
                f"execution counts are not continuous: {actual_counts}"
            )
        errors = [
            output
            for cell in code_cells
            for output in cell.get("outputs", [])
            if output.get("output_type") == "error"
        ]
        if errors:
            raise RuntimeError(f"executed notebook contains {len(errors)} errors")

        temporary_output = args.notebook.with_name(
            f".{args.notebook.name}.executed.tmp"
        )
        nbformat.write(notebook, temporary_output)
        os.replace(temporary_output, args.notebook)
        status_publisher.publish(
            phase="succeeded",
            current_cell_index=len(notebook.cells) - 1,
            output_notebook=str(args.notebook),
            code_cells=len(code_cells),
        )
        return 0
    except Exception as error:
        terminate_descendants()
        try:
            write_partial_notebook()
        except Exception:
            pass
        failure_text = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        status_publisher.publish(
            phase="failed",
            failure=failure_text[-12_000:],
        )
        return 1
    finally:
        stop_heartbeat.set()
        heartbeat_thread.join(timeout=5)
        terminate_descendants()


if __name__ == "__main__":
    raise SystemExit(main())
