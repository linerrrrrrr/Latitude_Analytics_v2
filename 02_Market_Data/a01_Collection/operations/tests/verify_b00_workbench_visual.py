"""Render synthetic b00 evidence into offscreen Qt screenshots; never launch jobs."""
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
        ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
sys.path.insert(0, str(ROOT / "02_Market_Data/a01_Collection/operations"))
import console
from PySide6.QtCore import QSettings
from PySide6.QtGui import QFontDatabase
from PySide6.QtWidgets import QApplication

app = QApplication.instance() or QApplication([])
app.setQuitOnLastWindowClosed(False)
for font_path in ("C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/msyhbd.ttc"):
    QFontDatabase.addApplicationFont(font_path)
visual_history_root = ROOT / "02_Market_Data/a01_Collection/operations/run_history"
visual_history_root.mkdir(parents=True, exist_ok=True)
output_dir = Path(tempfile.mkdtemp(prefix="visual_b00_workbench_visual_", dir=visual_history_root))
contracts = console.discover_contracts()
with tempfile.TemporaryDirectory(dir=ROOT / "02_Market_Data/a01_Collection/operations/tests") as directory:
    temporary_dir = Path(directory)
    with mock.patch.object(console, "discover_contracts", return_value=contracts):
        window = console.OperationsConsole(QSettings(str(temporary_dir / "theme.ini"), QSettings.Format.IniFormat))
        window.contracts_future.result(timeout=5)
        window.finish_loading()
    window.monitor_timer.stop()
    window.loading_timer.stop()
    window.resize(1440, 1000)
    window.show()
    now = datetime.now(timezone.utc)
    for tool_id in console.MAINTENANCE_TOOLS:
        run_root = temporary_dir / tool_id
        run_root.mkdir()
        scopes = {}
        phases = console.B00_VIEWS[tool_id][3]
        for phase, title in phases:
            total = 19 if tool_id in {"check_code", "check_full", "sync_exports"} else 5 if phase == "packages" else 1
            completed = 7 if phase == "check" else 3 if phase == "packages" else total
            scopes[phase] = {"phase": phase, "scope_id": phase, "state": "completed" if total == completed else "running",
                             "counters": {"items": {"completed": completed, "total": total, "unit": "份文件" if total == 19 else "项检查"}}}
            if total == 19:
                for index, contract in enumerate(contracts):
                    notebook = str(contract.path.with_suffix(".ipynb").relative_to(console.LAKEHOUSE_ROOT))
                    if index < completed:
                        scopes[phase + ":" + notebook] = {"phase": phase, "parent_scope_id": phase, "state": "completed", "object": {"notebook": notebook}}
                    elif index == completed:
                        scopes[phase]["object"] = {"notebook": notebook}
            elif phase == "packages":
                for package, version in (("numpy", "2.4.3"), ("pandas", "3.0.1"), ("polars", "1.35.2")):
                    scopes["packages:" + package] = {"phase": phase, "parent_scope_id": phase, "state": "completed", "object": {"package": package, "version": version}}
            elif phase == "environment":
                scopes[phase]["object"] = {"python": sys.executable, "environment": "latitude"}
            else:
                scopes[phase]["object"] = {"check": title}
        log_path = run_root / "stage.log"
        log_path.write_text("界面验收样例：本次没有运行采集或修改正式导出。\n[07/19] 校验完成，正在处理下一份文件。\n各步骤实际计数分别呈现，当前页保留真实流程位置。\n", encoding="utf-8")
        console.worker.atomic_write_json(run_root / "status.json", {
            "state": "running", "phase": "business", "stage_name": "maintenance/" + tool_id,
            "progress_scopes": scopes, "current_scope_id": phases[-1][0], "log_path": str(log_path),
            "started_at": (now - timedelta(seconds=32)).isoformat(), "heartbeat_at": now.isoformat(), "progress_at": now.isoformat(),
        })
        window.b00_runs[tool_id] = run_root
    for theme in ("light", "dark"):
        window.apply_theme(theme)
        for tool_id in console.B00_VIEWS:
            page_id = next(page_id for page_id, (_, modes) in console.B00_PAGES.items() if tool_id in dict(modes))
            window.entry_tree.setCurrentItem(window.b00_items[page_id])
            window.show_b00_page(page_id, tool_id)
            window.active_batch_banner.setText("界面预览 · 使用测试状态展示布局，未启动采集")
            window.active_batch_banner.show()
            if window.b00_log_future:
                window.b00_log_future[1].result(timeout=5)
                window.refresh_b00_dashboard()
            app.processEvents()
            window.grab().save(str(output_dir / f"{theme}_{tool_id}.png"))
        window.entry_tree.setCurrentItem(window.b00_items["b00_02"])
        window.show_b00_page("b00_02", "sync_exports")
        window.resize(900, 640)
        app.processEvents()
        window.grab().save(str(output_dir / f"{theme}_compact.png"))
        print(f"compact_{theme}: actual={window.width()}x{window.height()}; minimum={window.minimumSizeHint().width()}x{window.minimumSizeHint().height()}")
        window.resize(1440, 1000)
    window.worker_process = None
    window.io_pool.shutdown(wait=True, cancel_futures=True)
    window.close()
print(output_dir)
