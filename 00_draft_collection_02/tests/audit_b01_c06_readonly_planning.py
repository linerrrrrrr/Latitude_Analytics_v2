from __future__ import annotations

import importlib.util
import pathlib
import sys
import threading
import time
from unittest import mock

import psutil
from click.testing import CliRunner


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


MODULE_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Futures_Market_Data"
    / "b06_futures_minute.py"
)


def main() -> int:
    spec = importlib.util.spec_from_file_location(
        "audit_b06_futures_minute",
        MODULE_PATH,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块：{MODULE_PATH}")
    minute = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(minute)

    process = psutil.Process()
    peak_rss = [process.memory_info().rss]
    stop_event = threading.Event()

    def monitor_memory() -> None:
        while not stop_event.wait(0.05):
            peak_rss[0] = max(peak_rss[0], process.memory_info().rss)

    monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
    monitor_thread.start()
    started_at = time.perf_counter()
    try:
        with mock.patch(
            "config.jqdata_connection.authenticate_jqdata",
            side_effect=RuntimeError("只读规划禁止认证或调用 API。"),
        ):
            result = CliRunner().invoke(minute.main, [])
    finally:
        elapsed_seconds = time.perf_counter() - started_at
        stop_event.set()
        monitor_thread.join(timeout=1)
        peak_rss[0] = max(peak_rss[0], process.memory_info().rss)

    print(result.output, end="")
    print(f"audit_elapsed_seconds={elapsed_seconds:.3f}")
    print(f"audit_peak_rss_gib={peak_rss[0] / 1024 ** 3:.3f}")
    if (
        isinstance(result.exception, RuntimeError)
        and str(result.exception) == "只读规划禁止认证或调用 API。"
    ):
        print("audit_api_guard_triggered=true")
        return 0
    if result.exception is not None:
        print(f"audit_exception={result.exception!r}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
