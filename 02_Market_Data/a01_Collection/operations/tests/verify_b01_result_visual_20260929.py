"""Render the result-first b01 page using explicitly synthetic, temporary calendars."""
import os
from pathlib import Path
import sys
import tempfile
from unittest import mock

os.environ["QT_QPA_PLATFORM"] = "offscreen"
project_markers = [".git", ".env", "config/settings.py"]
current_path = Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
sys.path.insert(0, str(ROOT / "02_Market_Data/a01_Collection/operations/tests"))
from test_b01_result_table import console, calendar_fixture, NAME, evidence, date, pq
from PySide6.QtCore import QSettings
from PySide6.QtGui import QFontDatabase
from PySide6.QtWidgets import QApplication
from PySide6.QtTest import QTest

app = QApplication.instance() or QApplication([])
app.setQuitOnLastWindowClosed(False)
for path in ("C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/msyhbd.ttc"):
    QFontDatabase.addApplicationFont(path)
visual_history_root = ROOT / "02_Market_Data/a01_Collection/operations/run_history"
visual_history_root.mkdir(parents=True, exist_ok=True)
output = Path(tempfile.mkdtemp(prefix="visual_b01_result_visual_20260929_", dir=visual_history_root))
with tempfile.TemporaryDirectory(dir=ROOT / "02_Market_Data/a01_Collection/operations/tests") as directory:
    root = Path(directory)
    lake_root = root / "lake"
    pq.write_to_dataset(calendar_fixture(date(2020, 1, 1), 2463), lake_root / "silver" / console.TRADE_CALENDAR_TABLE_NAME,
                        partition_cols=console.TRADE_CALENDAR_PARTITIONS)
    contracts = console.discover_contracts()
    with mock.patch.object(console, "discover_contracts", return_value=contracts):
        window = console.OperationsConsole(QSettings(str(root / "test.ini"), QSettings.Format.IniFormat))
        window.contracts_future.result(timeout=5)
        window.finish_loading()
    window.monitor_timer.stop()
    window.loading_timer.stop()
    window.resize(1440, 1020)
    window.parameter_values[NAME]["lake_root"] = str(lake_root)
    window.show()
    for scenario in ("noop", "readonly"):
        run_root = root / scenario
        run_root.mkdir()
        if scenario == "noop":
            lines = ["planning_progress: phase=run; status=started; mode=automatic_tail; valid_end=2026-09-28",
                     "up_to_date: mode=automatic_tail; latest_calendar_date=2026-09-28; api_calls=0"]
        else:
            (run_root / "artifacts").mkdir()
            pq.write_table(calendar_fixture(date(2026, 9, 29), 2), run_root / "artifacts/b01_generated.parquet")
            lines = ["planning_progress: phase=collect; status=completed; start_date=2026-09-29; end_date=2026-09-30; rows=2; trading_day_count=2",
                     "planning_progress: phase=run; status=completed; mode=automatic_tail; write=false; validated_rows=2"]
        lines = [line.replace(": ", f": table={console.TRADE_CALENDAR_TABLE_NAME}; ", 1) for line in lines]
        status = evidence(NAME, lines, "succeeded")
        log_path = run_root / "preview.log"
        log_path.write_text("界面验收样例，未运行采集。\n" + "\n".join(lines), encoding="utf-8")
        status["log_path"] = str(log_path)
        console.worker.atomic_write_json(run_root / "status.json", status)
        window.calendar_runs[NAME] = run_root
        window.calendar_requests[NAME] = {"effective_parameters": {"lake_root": str(lake_root), "write": False}, "arguments": []}
        window.entry_tree.setCurrentItem(window.entry_items[NAME])
        for _ in range(3):
            window.refresh_calendar_dashboard()
            if window.trade_table_future:
                window.trade_table_future[1].result(timeout=5)
        window.refresh_calendar_dashboard()
        for theme in ("light", "dark"):
            window.apply_theme(theme)
            window.page_trade_calendar(0)
            window.active_batch_banner.setText("界面预览 · 临时模拟日历与状态 · 未执行业务采集")
            window.active_batch_banner.show()
            app.processEvents()
            QTest.qWait(50)
            window.grab().save(str(output / f"{theme}_{scenario}.png"))
            window.business_detail.grab().save(str(output / f"{theme}_{scenario}_detail.png"))
        if scenario == "noop":
            window.resize(1100, 820)
            app.processEvents()
            QTest.qWait(50)
            window.grab().save(str(output / "dark_noop_compact.png"))
            print(f"compact table={window.trade_table.width()}x{window.trade_table.height()}, source_height={window.trade_source.height()}, window_hint={window.minimumSizeHint().height()}")
            window.resize(1440, 1020)
    window.worker_process = None
    window.io_pool.shutdown(wait=True, cancel_futures=True)
    window.trade_table_pool.shutdown(wait=True, cancel_futures=True)
    window.close()
print(output)
