"""b00 dashboards: real event counts, isolation, failures and read-only navigation."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
from PySide6.QtCore import QSettings, Qt
from PySide6.QtWidgets import QApplication

project_markers = [".git", ".env", "config/settings.py"]
current_path = Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
sys.path.insert(0, str(ROOT / "R02_Market_Data/a01_Collection/operations"))
import console


class B00DashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt = QApplication.instance() or QApplication([])
        cls.qt.setQuitOnLastWindowClosed(False)
        cls.contracts = console.discover_contracts()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=ROOT / "R02_Market_Data/a01_Collection/operations/tests")
        self.run_root = Path(self.temporary.name)
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": str(self.run_root), "read_at": console.worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": str(self.run_root), "read_at": console.worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        with mock.patch.object(console, "discover_contracts", return_value=self.contracts):
            self.window = console.OperationsConsole(QSettings(str(self.run_root / "test.ini"), QSettings.Format.IniFormat))
            self.window.contracts_future.result(timeout=5)
            self.window.finish_loading()

    def tearDown(self):
        self.window.worker_process = None
        self.window.io_pool.shutdown(wait=True, cancel_futures=True)
        self.window.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.window.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.window.close()
        self.window.deleteLater()
        self.qt.processEvents()
        self.temporary.cleanup()

    def test_unified_navigation_keeps_b00_out_of_business_batches(self):
        window = self.window
        self.assertEqual([button.text() for button in window.navigation_buttons], ["采集工作台", "运行监控", "历史批次"])
        self.assertEqual(window.b00_tool_id, "verify_runtime")
        self.assertEqual(list(window.b00_items), ["b00_01", "b00_02", "b00_03", "b00_04"])
        self.assertEqual(window.b00_items["b00_01"].parent().childCount(), 4)
        window.select_daily()
        before = window.current_request()
        for page_id, item in window.b00_items.items():
            self.assertFalse(item.flags() & Qt.ItemFlag.ItemIsUserCheckable)
            window.entry_tree.setCurrentItem(item)
            self.assertEqual(window.b00_page_id, page_id)
        self.assertEqual(window.current_request()["tasks"], before["tasks"])
        window.filter_entries("b00 同步")
        self.assertFalse(window.b00_items["b00_02"].isHidden())
        self.assertEqual(len(window.current_request()["tasks"]), 18)
        self.assertTrue(window.run_tool_button.isHidden())

    def test_phase_counts_and_failure_do_not_claim_success_or_erase_location(self):
        window = self.window
        notebook = str(self.contracts[0].path.with_suffix(".ipynb").relative_to(console.LAKEHOUSE_ROOT))
        scopes = {
            "export": {"state": "completed", "phase": "export", "counters": {"files": {"completed": 19, "total": 19, "unit": "份"}}},
            "check": {"state": "running", "phase": "check", "counters": {"files": {"completed": 0, "total": 19, "unit": "份"}}},
            "check:" + notebook: {"parent_scope_id": "check", "phase": "check", "state": "failed", "object": {"notebook": notebook}, "message": "AST mismatch"},
        }
        status = {"state": "failed", "progress_scopes": scopes, "current_scope_id": "check:" + notebook,
                  "error_summary": ["AST mismatch"], "started_at": "2026-09-29T00:00:00+00:00", "finished_at": "2026-09-29T00:00:10+00:00"}
        console.worker.atomic_write_json(self.run_root / "status.json", status)
        window.b00_runs["check_code"] = self.run_root
        window.show_b00_page("b00_02", "check_code")
        self.assertIn("第 2 / 2 步", window.b00_position.text())
        self.assertEqual(window.b00_metrics["completed"].text(), "1")
        self.assertEqual(window.b00_metrics["remaining"].text(), "18")
        self.assertEqual(window.b00_metrics["failed"].text(), "1")
        self.assertEqual(window.b00_phase_widgets["check"][1].text(), "失败")
        self.assertEqual(window.b00_state.text(), "失败")
        self.assertEqual(window.b00_results.topLevelItem(0).text(2), "失败")
        self.assertIn("10", window.b00_health.text())
        window.show_b00_page("b00_01", "verify_control")
        self.assertEqual(window.b00_metrics["total"].text(), "—")
        self.assertEqual(window.b00_results.topLevelItemCount(), 0)
        self.assertEqual(window.b00_error.text(), "")

    def test_missing_or_old_events_stay_unknown_and_frozen_history_is_not_rewritten(self):
        status_path = self.run_root / "status.json"
        console.worker.atomic_write_json(status_path, {"state": "succeeded"})
        before = status_path.read_bytes()
        self.window.b00_runs["verify_runtime"] = self.run_root
        self.window.show_b00_page("b00_01", "verify_runtime")
        self.assertEqual(self.window.b00_metrics["completed"].text(), "—")
        self.assertEqual(self.window.b00_phase_widgets["packages"][2].value(), 0)
        self.assertEqual(status_path.read_bytes(), before)

    def test_late_log_result_does_not_leak_across_tool_pages(self):
        from concurrent.futures import Future
        log_path = self.run_root / "stage.log"
        log_path.write_text("old tool only", encoding="utf-8")
        console.worker.atomic_write_json(self.run_root / "status.json", {"state": "running", "log_path": str(log_path)})
        self.window.b00_runs["verify_runtime"] = self.run_root
        pending = Future()
        with mock.patch.object(self.window.io_pool, "submit", return_value=pending):
            self.window.show_b00_page("b00_01", "verify_runtime")
            self.window.show_b00_page("b00_01", "verify_control")
            pending.set_result((13, b"old tool only", False))
            self.window.refresh_b00_dashboard()
        self.assertEqual(self.window.b00_logs.toPlainText(), "")

    def test_maintenance_launch_stays_on_its_dashboard(self):
        process = mock.Mock()
        process.poll.return_value = None
        with mock.patch.object(console, "start_detached_batch", return_value=(self.run_root, process)), mock.patch.object(self.window, "refresh_monitor"):
            for tool_id in console.MAINTENANCE_TOOLS:
                self.window.worker_process = None
                self.window.launch(console.prepare_maintenance_request(tool_id))
                self.assertIs(self.window.tabs.currentWidget(), self.window.configure_tab)
                page_id = "b00_01" if tool_id in {"verify_runtime", "verify_control"} else "b00_02"
                self.assertIs(self.window.entry_tree.currentItem(), self.window.b00_items[page_id])
                self.assertEqual(self.window.b00_tool_id, tool_id)
                self.assertEqual(self.window.b00_runs[tool_id], self.run_root)

    def test_page_modes_are_local_remembered_and_do_not_execute(self):
        window = self.window
        with mock.patch.object(window, "launch") as launch, mock.patch.object(window, "preview") as preview:
            window.entry_tree.setCurrentItem(window.b00_items["b00_02"])
            self.assertEqual([window.b00_mode_tabs.tabText(index) for index in range(3)], ["代码检查", "完整检查", "导出同步"])
            window.b00_mode_tabs.setCurrentIndex(2)
            self.assertEqual(window.b00_tool_id, "sync_exports")
            self.assertIn("预览并同步", window.run_tool_button.text())
            self.assertEqual(set(window.b00_phase_widgets), {"export", "write", "check"})
            window.entry_tree.setCurrentItem(window.b00_items["b00_01"])
            window.b00_mode_tabs.setCurrentIndex(1)
            self.assertEqual(window.b00_tool_id, "verify_control")
            self.assertIn("operations/runtime/verify_operations_runtime.py", window.b00_script_origin.text())
            self.assertFalse(window.b00_script_origin.isHidden())
            self.assertIn("verify_operations_runtime.py", window.maintenance_command.text())
            window.entry_tree.setCurrentItem(window.b00_items["b00_02"])
            self.assertEqual(window.b00_tool_id, "sync_exports")
            self.assertEqual(window.b00_mode_tabs.currentIndex(), 2)
            with mock.patch.object(console, "discover_contracts", return_value=self.contracts):
                window.reload_catalog()
                window.contracts_future.result(timeout=5)
                window.finish_loading()
            self.assertEqual(list(window.b00_items), ["b00_01", "b00_02", "b00_03", "b00_04"])
            self.assertEqual(window.b00_tool_id, "sync_exports")
            window.filter_entries("Windows")
            self.assertFalse(window.b00_items["b00_01"].isHidden())
            launch.assert_not_called()
            preview.assert_not_called()


class B00SourceEventTests(unittest.TestCase):
    def test_runtime_missing_dependency_reports_failure_without_losing_other_results(self):
        spec = importlib.util.spec_from_file_location("b00_runtime_test", console.LAKEHOUSE_ROOT / "b00_01_verify_runtime.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        def version(package):
            if package == "pandas":
                raise module.PackageNotFoundError(package)
            return "test-version"
        output = io.StringIO()
        with mock.patch.object(module, "version", side_effect=version), contextlib.redirect_stdout(output), self.assertRaises(SystemExit):
            module.main()
        events = [console.worker.parse_progress_event(line) for line in output.getvalue().splitlines() if line.startswith("progress_event:")]
        self.assertEqual(events[-1]["state"], "failed")
        self.assertEqual(events[-1]["counters"]["checks"]["completed"], 5)
        self.assertEqual(sum(event["scope_id"].startswith("packages:") for event in events), 5)

    def test_export_write_and_recheck_events_only_use_temporary_notebooks(self):
        import nbformat
        spec = importlib.util.spec_from_file_location("b00_export_test", console.LAKEHOUSE_ROOT / "b00_02_sync_notebook_exports.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory(dir=ROOT / "R02_Market_Data/a01_Collection/operations/tests") as folder:
            notebook_root = Path(folder)
            for index in range(19):
                nbformat.write(nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell("value = 1")]), notebook_root / f"c{index:02}_sample.ipynb")
            module.PROJECT_DIR = notebook_root
            module.WORKFLOW_DIRS = (notebook_root,)
            output = io.StringIO()
            with mock.patch.object(sys, "argv", ["sync", "--write"]), contextlib.redirect_stdout(output):
                self.assertEqual(module.main(), 0)
            events = [console.worker.parse_progress_event(line) for line in output.getvalue().splitlines() if line.startswith("progress_event:")]
            for phase in ("export", "write", "check"):
                items = [event for event in events if event.get("parent_scope_id") == phase]
                self.assertEqual(len(items), 19)
                self.assertTrue(all(event["state"] == "completed" for event in items))
            (notebook_root / "c00_sample.py").write_text("value = 2", encoding="utf-8")
            output = io.StringIO()
            with mock.patch.object(sys, "argv", ["sync", "--check", "--check-level", "code"]), contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(module.main(), 1)
            events = [console.worker.parse_progress_event(line) for line in output.getvalue().splitlines() if line.startswith("progress_event:")]
            self.assertEqual(sum(event["state"] == "failed" for event in events if event.get("parent_scope_id") == "check"), 1)


if __name__ == "__main__":
    unittest.main()
