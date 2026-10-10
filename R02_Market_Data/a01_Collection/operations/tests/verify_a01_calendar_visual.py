"""Offscreen visual fixtures. No collection or export synchronization is launched."""
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
sys.path.insert(0, str(ROOT / "R02_Market_Data/a01_Collection/operations/tests"))
from test_a01_calendar_dashboards import console, evidence, NAMES, TABLES
from PySide6.QtCore import QSettings
from PySide6.QtGui import QFontDatabase
from PySide6.QtWidgets import QApplication

app = QApplication.instance() or QApplication([])
app.setQuitOnLastWindowClosed(False)
for font_path in ("C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/msyhbd.ttc"):
    QFontDatabase.addApplicationFont(font_path)
visual_history_root = ROOT / "R02_Market_Data/a01_Collection/operations/run_history"
visual_history_root.mkdir(parents=True, exist_ok=True)
output_dir = Path(tempfile.mkdtemp(prefix="visual_a01_calendar_visual_", dir=visual_history_root))
contracts = console.discover_contracts()
table_reader_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={
    "rows": [], "exists": False, "path": "offline-fixture", "read_at": console.worker.utc_now_text()})
table_reader_patch.start()
fixtures = [
    ["planning_progress: phase=run; status=started; mode=automatic_tail; write=true",
     "planning_progress: phase=collect; status=completed; start_date=2026-09-01; end_date=2026-09-30; rows=30; trading_day_count=22",
     "partition_plan: phase=merge_validate; status=completed; rows=30; partitions=2; run_id=preview",
     "partition_committed: partition=year=2026; completed=1; total=2; rows=30; run_id=preview"],
    ["planning_progress: phase=run; status=started; mode=automatic_tail; write=true",
     "planning_progress: phase=upstream_reuse; status=completed; trading_dates=750",
     "planning_progress: phase=catalog_validation; status=completed; fixed_contracts=12340",
     "planning_progress: phase=expand; status=running; completed=250; total=750; trading_date=2026-09-01; rows=20500"],
    ["planning_progress: phase=run; status=started; mode=full; write=true",
     "planning_progress: phase=candidate_selection; status=completed; contracts=240; info_batches=3",
     "api_result: phase=contract_info; status=completed; completed=3; total=3; returned_contracts=40",
     "planning_progress: phase=reconcile; status=running; completed=6; total=18; partition=('SHFE',2026,9)",
     "planning_progress: phase=expand; status=running; completed=250; total=1000; trading_date=2026-09-28; partition=('SHFE',2026,9); rows=38500"],
    ["planning_progress: phase=run; status=started; mode=automatic_tail; write=true",
     "planning_progress: phase=discovery; status=completed; upstream_files=520; target_files=1038",
     "planning_progress: phase=watermark; status=completed; completed=12; total=12",
     "planning_progress: phase=plan; status=running; completed=4; total=20; key=('SHFE',2026,9)",
     "planning_progress: function=build_structural_partitions; phase=build_structure; status=running; completed=1; total=2; bar_frequency=1m; rows=25000"],
]
with tempfile.TemporaryDirectory(dir=ROOT / "R02_Market_Data/a01_Collection/operations/tests") as directory:
    root = Path(directory)
    with mock.patch.object(console, "discover_contracts", return_value=contracts):
        window = console.OperationsConsole(QSettings(str(root / "theme.ini"), QSettings.Format.IniFormat))
        window.contracts_future.result(timeout=5)
        window.finish_loading()
    window.monitor_timer.stop()
    window.loading_timer.stop()
    window.resize(1440, 1120)
    window.show()
    for name, table, fixture in zip(NAMES, TABLES, fixtures):
        run_root = root / name.split("/")[-1]
        run_root.mkdir()
        lines = [line.replace(": ", f": table={table}; ", 1) for line in fixture]
        status = evidence(name, lines)
        now = console.worker.utc_now_text()
        status.update({"heartbeat_at": now, "progress_at": now, "started_at": now, "stage_started_at": now})
        log_path = run_root / "fixture.log"
        log_path.write_text("界面验收样例 · 未执行采集\n" + "\n".join(lines) + "\n", encoding="utf-8")
        status["log_path"] = str(log_path)
        console.worker.atomic_write_json(run_root / "status.json", status)
        window.calendar_runs[name] = run_root
        window.calendar_requests[name] = {"effective_parameters": {"write": True}, "arguments": ["--write"]}
    for theme in ("light", "dark"):
        window.apply_theme(theme)
        for name in NAMES:
            window.entry_tree.setCurrentItem(window.entry_items[name])
            if window.calendar_log_future:
                window.calendar_log_future[1].result(timeout=5)
                window.refresh_calendar_dashboard()
            window.active_batch_banner.setText("界面预览 · 测试状态与日志 · 未启动采集")
            window.active_batch_banner.show()
            app.processEvents()
            filename = name.split("/")[-1]
            window.grab().save(str(output_dir / f"{theme}_{filename}.png"))
            window.calendar_page.currentWidget().widget().grab().save(str(output_dir / f"{theme}_{filename}_body.png"))
        window.resize(900, 640)
        app.processEvents()
        window.grab().save(str(output_dir / f"{theme}_compact.png"))
        print(f"{theme}: compact={window.width()}x{window.height()}, hint={window.minimumSizeHint().width()}x{window.minimumSizeHint().height()}")
        window.resize(1440, 1120)
    window.worker_process = None
    window.io_pool.shutdown(wait=True, cancel_futures=True)
    window.trade_table_pool.shutdown(wait=True, cancel_futures=True)
    window.close()
table_reader_patch.stop()
print(output_dir)
