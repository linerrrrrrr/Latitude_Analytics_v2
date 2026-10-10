"""Offline Qt visual checks: synthetic progress, temporary history, no collection."""
import json
import os
from pathlib import Path
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ["QT_QPA_PLATFORM"] = "offscreen"
project_markers = [".git", ".env", "config/settings.py"]
current_path = Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        project_dir = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
import console
from PySide6.QtCore import QSettings
from PySide6.QtGui import QFont, QFontDatabase
from PySide6.QtWidgets import QApplication

application = QApplication.instance() or QApplication([])
for font_path in ("C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/msyhbd.ttc", "C:/Windows/Fonts/arial.ttf"):
    QFontDatabase.addApplicationFont(font_path)
application.setFont(QFont("Microsoft YaHei UI", 10))
application.setQuitOnLastWindowClosed(False)
contracts = console.discover_contracts()
visual_history_root = project_dir / "R02_Market_Data/a01_Collection/operations/run_history"
visual_history_root.mkdir(parents=True, exist_ok=True)
output_dir = Path(tempfile.mkdtemp(prefix="visual_operations_single_tools_visual_", dir=visual_history_root))

with tempfile.TemporaryDirectory() as temporary:
    temporary_dir = Path(temporary)
    preferences = QSettings(str(temporary_dir / "appearance.ini"), QSettings.Format.IniFormat)
    with mock.patch.object(console, "discover_contracts", return_value=contracts), mock.patch.object(console.worker, "RUN_HISTORY_ROOT", temporary_dir):
        window = console.OperationsConsole(preferences)
        window.contracts_future.result(timeout=5)
        window.finish_loading()
        window.monitor_timer.stop()
        window.loading_timer.stop()
        window.resize(1280, 900)
        window.show()
        window.entry_tree.setCurrentItem(window.entry_items["b01/c06_futures_minute"])
        root = temporary_dir / "offline-preview"
        (root / "logs").mkdir(parents=True)
        name = "b01/c06_futures_minute"
        log_path = root / "logs/business_01_a01_b06_futures_minute.log"
        log_path.write_text("离线界面验收样例；未运行采集。\nrequest_batch: contract=CU2610.XSGE; trading_date=2026-09-28\n等待 get_price(1m) 响应…\n", encoding="utf-8")
        (root / "command_manifest.json").write_text(json.dumps({"preflights": [], "stages": [{"index": 1, "name": name}]}), encoding="utf-8")
        now = datetime.now(timezone.utc)
        scope = {"source": "legacy", "counter_scope": "local_step", "scope_id": "sample", "phase": "query", "state": "running",
                 "started_at": (now - timedelta(seconds=4)).isoformat(), "updated_at": now.isoformat(),
                 "object": {"contract": "CU2610.XSGE", "trading_date": "2026-09-28", "api": "get_price(1m)"},
                 "counters": {"sessions": {"completed": 128, "total": 400, "unit": "Session"}}}
        status = {"state": "running", "phase": "business", "stage_name": name, "stage_index": 1, "stage_total": 1,
                  "worker_pid": os.getpid(), "started_at": (now - timedelta(seconds=50)).isoformat(), "heartbeat_at": now.isoformat(),
                  "last_output_at": now.isoformat(), "progress_at": now.isoformat(), "stage_history": [], "log_path": str(log_path),
                  "progress_scopes": {"sample": scope}, "current_scope_id": "sample", "progress": "离线进度样例"}
        window.view_run_root = root
        for theme in ("light", "dark"):
            window.apply_theme(theme)
            for label, page in (("configuration", window.configure_tab), ("maintenance", window.configure_tab), ("monitor", window.monitor_tab)):
                window.tabs.setCurrentWidget(page)
                if label == "maintenance":
                    window.entry_tree.setCurrentItem(window.b00_items["b00_01"])
                elif label == "configuration":
                    window.entry_tree.setCurrentItem(window.entry_items["b01/c06_futures_minute"])
                if label == "monitor":
                    window.render_status(status)
                    if window.log_future is not None:
                        window.log_future[1].result(timeout=5)
                    window.refresh_stage_details()
                application.processEvents()
                window.grab().save(str(output_dir / f"{theme}_{label}.png"))
        window.resize(900, 640)
        window.tabs.setCurrentWidget(window.configure_tab)
        window.entry_tree.setCurrentItem(window.b00_items["b00_01"])
        application.processEvents()
        window.grab().save(str(output_dir / "dark_maintenance_compact.png"))
        window.worker_process = None
        window.io_pool.shutdown(wait=True, cancel_futures=True)
        window.close()
        window.deleteLater()
        application.processEvents()
print(output_dir)
