from __future__ import annotations

import copy
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
from PySide6.QtCore import Qt, QSettings, QTimer
from PySide6.QtGui import QColor, QPalette
from PySide6.QtTest import QTest
from PySide6.QtWidgets import QApplication, QDialog, QMessageBox, QPlainTextEdit, QPushButton

import click
import psutil

OPERATIONS_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPERATIONS_ROOT))
sys.path.insert(0, str(OPERATIONS_ROOT / "runtime"))
import console
import invoke_exported_click_entrypoint as invoker
import background_worker as worker


class ConsoleContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = console.discover_contracts()
        cls.by_name = {contract.name: contract for contract in cls.contracts}

    def test_all_nineteen_entries_support_single_selection_without_hidden_write(self):
        self.assertEqual(len(self.contracts), 19)
        for contract in self.contracts:
            with self.subTest(entry=contract.name):
                arguments = ["--confirm-full-quality"] if contract.path.stem == "b08_full_minute_quality" else []
                request = console.prepare_request(self.contracts, {contract.name: arguments})
                stages = console.stages_from_request(request)
                self.assertEqual(len(stages), 1)
                self.assertNotIn("--write", stages[0].arguments)
                self.assertFalse(request["tasks"][0]["effective_parameters"]["write"])
                self.assertEqual(stages[0].arguments[-len(arguments):], tuple(arguments)) if arguments else None

    def test_source_defaults_b08_confirmation_and_original_types(self):
        minute = "a01/b06_futures_minute"
        request = console.prepare_request(self.contracts, {minute: []})
        self.assertEqual(request["tasks"][0]["effective_parameters"]["quota_reserve"], 5_000_000)
        with self.assertRaises(click.MissingParameter):
            console.prepare_request(self.contracts, {"a01/b08_full_minute_quality": []})
        for arguments in (["--quota-reserve", "-1"], ["--start-date", "bad"], ["--invented"]):
            with self.subTest(arguments=arguments), self.assertRaises(click.ClickException):
                console.prepare_request(self.contracts, {minute: arguments})

    def test_repeated_options_spaces_and_full_are_passed_as_tokens(self):
        name = "a01/b07_suspected_session_reconciliation"
        args = ["--lake-root", "E:/临时 数据湖", "--contract-code", "IM2609.CCFX", "--contract-code", "IF2609.CCFX", "--force", "--write"]
        request = console.prepare_request(self.contracts, {name: args})
        self.assertEqual(request["tasks"][0]["arguments"], args)
        self.assertEqual(request["tasks"][0]["effective_parameters"]["contract_code"], ["IM2609.CCFX", "IF2609.CCFX"])
        full = console.prepare_request(self.contracts, {"a01/b01_trade_calendar": ["--full", "--write"]})
        self.assertTrue(full["tasks"][0]["effective_parameters"]["full_refresh"])

    def test_order_is_fixed_and_tampered_requests_rejected(self):
        request = console.prepare_request(self.contracts, {"a04/b03_macro_release": [], "a01/b01_trade_calendar": []})
        self.assertEqual(request["tasks"][0]["name"], "a01/b01_trade_calendar")
        for change in ("hash", "path", "duplicate", "order", "effective"):
            modified = copy.deepcopy(request)
            if change == "hash":
                modified["tasks"][0]["source_sha256"] = "0" * 64
            elif change == "path":
                modified["tasks"][0]["entrypoint"] = "../../outside.py"
            elif change == "duplicate":
                modified["tasks"].append(modified["tasks"][0])
            elif change == "order":
                modified["tasks"].reverse()
            else:
                modified["tasks"][0]["effective_parameters"]["write"] = True
            with self.subTest(change=change), self.assertRaises(ValueError):
                console.stages_from_request(modified)

    def test_static_reader_does_not_execute_top_level_or_callback(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            source = directory / "b01_example.py"
            source.with_suffix(".ipynb").write_text("{}", encoding="utf-8")
            source.write_text('import click\nraise RuntimeError("must not import")\nDEFAULT_LIMIT = 17\n@click.command()\n@click.option("--limit", type=click.IntRange(min=1), default=DEFAULT_LIMIT)\ndef main(limit):\n    raise RuntimeError("must not run")\n', encoding="utf-8")
            with mock.patch.object(invoker, "COLLECTION_DIRS", (directory,)):
                contract = invoker.read_cli_contract(source)
                with contract.command.make_context("test", []) as context:
                    self.assertEqual(context.params["limit"], 17)
                source.write_text(source.read_text().replace("default=DEFAULT_LIMIT", "default=__import__('os').getcwd()"))
                with self.assertRaises(ValueError):
                    invoker.read_cli_contract(source)

    def test_invoker_restricts_paths_but_accepts_b08(self):
        path = self.by_name["a01/b08_full_minute_quality"].path
        arguments = ["invoker.py", "--entrypoint-path", str(path), "--confirm-full-quality"]
        self.assertEqual(invoker.selected_entrypoint(arguments), path)
        self.assertEqual(arguments, ["invoker.py", "--confirm-full-quality"])
        with self.assertRaises(ValueError):
            invoker.selected_entrypoint(["invoker.py", "--entrypoint-path", str(OPERATIONS_ROOT / "console.py")])

    def test_source_hash_checked_before_runpy(self):
        path = self.contracts[0].path
        arguments = ["invoker.py", "--entrypoint-path", str(path), "--source-sha256", "0" * 64]
        with mock.patch.object(sys, "argv", arguments), mock.patch.object(invoker.runpy, "run_path") as run_path:
            with self.assertRaisesRegex(RuntimeError, "变化"):
                invoker.main()
            run_path.assert_not_called()

    def test_detached_launch_persists_exact_request_and_never_executes_in_ui(self):
        request = console.prepare_request(self.contracts, {self.contracts[0].name: []})
        with tempfile.TemporaryDirectory() as temporary:
            history = pathlib.Path(temporary) / "history"
            with mock.patch.object(worker, "RUN_HISTORY_ROOT", history), mock.patch.object(worker, "GLOBAL_LOCK_PATH", history / ".lock"), mock.patch.object(console.subprocess, "Popen") as popen:
                run_root, process = console.start_detached_batch(request)
                self.assertEqual(worker.read_json_shared(run_root / "request.json"), request)
                self.assertTrue(worker.monitor_is_healthy(run_root, os.getpid()))
                self.assertIn("--execute-request", popen.call_args.args[0])
                self.assertEqual(popen.call_args.kwargs["stdin"], subprocess.DEVNULL)
                if os.name == "nt":
                    self.assertTrue(popen.call_args.kwargs["creationflags"] & subprocess.DETACHED_PROCESS)
                worker.ensure_fresh_or_monitor_only_run_root(run_root)
                self.assertIs(process, popen.return_value)

    def test_worker_entrypoint_uses_explicit_request_without_changing_parameters(self):
        request = console.prepare_request(self.contracts, {"a01/b08_full_minute_quality": ["--confirm-full-quality"]})
        with tempfile.TemporaryDirectory() as temporary:
            history = pathlib.Path(temporary)
            run_root = history / "run"
            run_root.mkdir()
            worker.atomic_write_json(run_root / "request.json", request)
            with mock.patch.object(worker, "RUN_HISTORY_ROOT", history), mock.patch.object(worker, "run_batch", return_value=3) as run_batch:
                self.assertEqual(console.main(["--execute-request", str(run_root / "request.json")]), 3)
                stages = run_batch.call_args.kwargs["stages"]
                self.assertEqual(len(stages), 1)
                self.assertEqual(stages[0].name, "a01/b08_full_minute_quality")
                self.assertNotIn("--write", stages[0].arguments)

    def test_console_lease_expiry_identity_and_interrupt_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_root = pathlib.Path(temporary)
            worker.atomic_write_json(run_root / "request.json", {})
            lease = {"pid": os.getpid(), "process_created_at": psutil.Process().create_time(), "heartbeat_at": worker.utc_now_text()}
            worker.atomic_write_json(run_root / "monitor.json", lease)
            self.assertTrue(worker.monitor_is_healthy(run_root, os.getpid()))
            worker.atomic_write_json(run_root / "monitor.json", {**lease, "process_created_at": 0})
            self.assertFalse(worker.monitor_is_healthy(run_root, os.getpid()))
            worker.atomic_write_json(run_root / "monitor.json", {**lease, "heartbeat_at": (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat()})
            self.assertFalse(worker.monitor_is_healthy(run_root, os.getpid()))
            console.request_interruption(run_root)
            path = run_root / "interrupt.request"
            before = (path.read_bytes(), path.stat().st_mtime_ns)
            console.request_interruption(run_root)
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)

    def test_history_includes_incomplete_batches_once_and_keeps_evidence_read_only(self):
        with tempfile.TemporaryDirectory() as temporary:
            history_root = pathlib.Path(temporary)
            fixtures = {
                "legacy_imports/old/status.json": {"started_at": "2026-08-01T00:00:00+00:00", "state": "succeeded", "stage_name": "old"},
                "new/request.json": {"created_at": "2026-09-28T21:30:57+00:00", "tasks": []},
                "new/status.json": {"started_at": "2026-09-28T21:30:57+00:00", "state": "failed", "stage_name": "preflight/03_notebook_export_sync"},
                "new/launch_failure.json": {"at": "2026-09-28T21:30:58+00:00", "error": "less authoritative"},
                "request_only/request.json": {"created_at": "2026-09-29T00:00:00+00:00", "tasks": []},
                "launch_only/launch_failure.json": {"at": "2026-09-29T01:00:00+00:00", "error": "child did not launch"},
            }
            for relative_path, payload in fixtures.items():
                path = history_root / relative_path
                path.parent.mkdir(parents=True, exist_ok=True)
                worker.atomic_write_json(path, payload)
            # Archiving can update an old directory later than a new batch.
            os.utime(history_root / "legacy_imports/old", (2_000_000_000, 2_000_000_000))
            before = {path: (path.read_bytes(), path.stat().st_mtime_ns)
                      for path in history_root.rglob("*") if path.is_file()}
            with mock.patch.object(worker, "RUN_HISTORY_ROOT", history_root), mock.patch.object(worker, "GLOBAL_LOCK_PATH", history_root / ".lock"):
                rows, _ = console.read_history()
            by_name = {path.name: values for path, values in rows}
            self.assertEqual(len(rows), 4)
            self.assertEqual(by_name["new"][1:3], ["失败", "preflight/03_notebook_export_sync"])
            self.assertEqual(by_name["request_only"][1], "未发布状态")
            self.assertEqual(by_name["launch_only"][1], "启动失败")
            names = [path.name for path, _ in rows]
            self.assertLess(names.index("request_only"), names.index("new"))
            self.assertLess(names.index("new"), names.index("old"))
            self.assertEqual({path: (path.read_bytes(), path.stat().st_mtime_ns)
                              for path in history_root.rglob("*") if path.is_file()}, before)

    def test_history_bad_json_and_missing_time_do_not_hide_other_batches(self):
        with tempfile.TemporaryDirectory() as temporary:
            history_root = pathlib.Path(temporary)
            for name, payload in (("valid", {"started_at": "2026-09-28T21:30:57+00:00", "state": "succeeded"}),
                                  ("null_time", {"started_at": None, "state": "failed"})):
                (history_root / name).mkdir()
                worker.atomic_write_json(history_root / name / "status.json", payload)
            broken_root = history_root / "broken"
            broken_root.mkdir()
            (broken_root / "status.json").write_text("{incomplete", encoding="utf-8")
            with mock.patch.object(worker, "RUN_HISTORY_ROOT", history_root), mock.patch.object(worker, "GLOBAL_LOCK_PATH", history_root / ".lock"):
                rows, _ = console.read_history()
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0][0].name, "valid")
            self.assertEqual(next(values[1] for path, values in rows if path == broken_root), "错误")


class ConsoleWidgetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt_application = QApplication.instance() or QApplication([])
        cls.qt_application.setQuitOnLastWindowClosed(False)
        cls.contracts = console.discover_contracts()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": self.temporary.name, "read_at": worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": self.temporary.name, "read_at": worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        self.history_patch = mock.patch.object(worker, "RUN_HISTORY_ROOT", pathlib.Path(self.temporary.name))
        self.history_patch.start()
        self.lock_patch = mock.patch.object(worker, "GLOBAL_LOCK_PATH", pathlib.Path(self.temporary.name) / ".lock")
        self.lock_patch.start()
        self.preferences = QSettings(str(pathlib.Path(self.temporary.name) / "appearance.ini"), QSettings.Format.IniFormat)
        with mock.patch.object(console, "discover_contracts", return_value=self.contracts):
            self.app = console.OperationsConsole(self.preferences)
            self.app.contracts_future.result(timeout=5)
            self.app.finish_loading()
            # Existing business-form tests begin on the first business entry;
            # the workbench itself now opens on a00.
            self.app.entry_tree.setCurrentItem(self.app.entry_items[self.contracts[0].name])

    def tearDown(self):
        self.app.worker_process = None
        self.app.io_pool.shutdown(wait=True, cancel_futures=True)
        self.app.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.app.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.app.close()
        self.app.deleteLater()
        self.qt_application.processEvents()
        self.history_patch.stop()
        self.lock_patch.stop()
        self.temporary.cleanup()

    def test_defaults_daily_selection_and_explicit_b08(self):
        app = self.app
        with self.assertRaisesRegex(ValueError, "至少"):
            app.current_request()
        app.daily_button.click()
        self.assertEqual(len(app.current_request()["tasks"]), 18)
        self.assertEqual(app.entry_items["a01/b08_full_minute_quality"].checkState(0), Qt.CheckState.Unchecked)
        self.assertTrue(all(task["effective_parameters"]["write"] and "--write" in task["arguments"]
                            for task in app.current_request()["tasks"]))
        self.assertTrue(app.parameter_widgets["a01/b01_trade_calendar"]["write"].isChecked())
        lazy_name = "a01/b06_futures_minute"
        app.entry_tree.setCurrentItem(app.entry_items[lazy_name])
        self.assertTrue(app.parameter_widgets[lazy_name]["write"].isChecked())
        app.parameter_widgets[lazy_name]["write"].click()
        self.assertFalse(next(task for task in app.current_request()["tasks"] if task["name"] == lazy_name)["effective_parameters"]["write"])
        app.parameter_widgets[lazy_name]["quota_reserve"].setText("12345")
        app.daily_button.click()
        self.assertTrue(app.parameter_widgets[lazy_name]["write"].isChecked())
        self.assertEqual(app.parameter_widgets[lazy_name]["quota_reserve"].text(), "12345")
        name = "a01/b08_full_minute_quality"
        app.clear_selection()
        app.entry_items[name].setCheckState(0, Qt.CheckState.Checked)
        app.entry_tree.setCurrentItem(app.entry_items[name])
        self.assertFalse(app.parameter_widgets[name]["confirm_full_quality"].isChecked())
        with self.assertRaises(click.MissingParameter):
            app.current_request()
        app.parameter_widgets[name]["confirm_full_quality"].click()
        self.assertEqual(app.current_request()["tasks"][0]["arguments"], ["--confirm-full-quality"])

    def test_parameter_edits_survive_switching_and_do_not_modify_frozen_request(self):
        app = self.app
        name = "a01/b06_futures_minute"
        app.entry_items[name].setCheckState(0, Qt.CheckState.Checked)
        app.entry_tree.setCurrentItem(app.entry_items[name])
        app.parameter_widgets[name]["quota_reserve"].setText("12345")
        request = app.current_request()
        original_field = app.parameter_widgets[name]["quota_reserve"]
        app.entry_tree.setCurrentItem(app.entry_items["a01/b01_trade_calendar"])
        app.entry_tree.setCurrentItem(app.entry_items[name])
        self.assertIs(app.parameter_widgets[name]["quota_reserve"], original_field)
        self.assertEqual(original_field.text(), "12345")
        original_field.setText("67890")
        self.assertEqual(request["tasks"][0]["effective_parameters"]["quota_reserve"], 12345)
        self.assertEqual(app.current_request()["tasks"][0]["effective_parameters"]["quota_reserve"], 67890)

    def test_history_render_does_not_write_and_terminal_elapsed_is_frozen(self):
        run_root = pathlib.Path(self.temporary.name) / "old"
        run_root.mkdir()
        status = {"state": "succeeded", "started_at": "2026-09-01T00:00:00+00:00", "finished_at": "2026-09-01T00:01:02+00:00", "recent_lines": ["done"]}
        worker.atomic_write_json(run_root / "status.json", status)
        path = run_root / "status.json"
        before = (path.read_bytes(), path.stat().st_mtime_ns)
        self.app.view_run_root = run_root
        self.app.render_status(status)
        self.assertEqual(self.app.elapsed_label.text(), "00:01:02")
        self.assertEqual(self.app.heartbeat_label.text(), "已结束")
        self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)
        self.assertFalse((run_root / "monitor.pid").exists())

    def test_history_refresh_discovers_new_batch_and_displays_beijing_time(self):
        history_root = pathlib.Path(self.temporary.name)
        old_root = history_root / "old"
        old_root.mkdir()
        worker.atomic_write_json(old_root / "status.json", {"state": "succeeded", "started_at": "2026-09-01T00:00:00+00:00"})
        self.app.history_button.click()
        self.app.history_future.result(timeout=5)
        self.app.finish_loading()
        self.assertEqual(self.app.history_tree.topLevelItemCount(), 1)
        latest_root = history_root / "latest"
        latest_root.mkdir()
        timestamp = "2026-09-28T21:30:57+00:00"
        worker.atomic_write_json(latest_root / "status.json", {"state": "failed", "started_at": timestamp})
        before = {path: (path.read_bytes(), path.stat().st_mtime_ns)
                  for path in history_root.rglob("status.json")}
        self.app.history_button.click()
        self.assertFalse(self.app.history_button.isEnabled())
        self.app.history_future.result(timeout=5)
        self.app.finish_loading()
        self.assertTrue(self.app.history_button.isEnabled())
        self.assertEqual(self.app.history_tree.topLevelItemCount(), 2)
        self.assertEqual(self.app.history_tree.headerItem().text(0), "启动时间（北京时间）")
        first = self.app.history_tree.topLevelItem(0)
        self.assertEqual(first.data(0, Qt.ItemDataRole.UserRole), latest_root)
        self.assertIn("2026-09-29", first.text(0))
        self.assertIn("05:30:57", first.text(0))
        self.assertIn(timestamp, first.toolTip(0))
        summary = self.app.history_status_label.text()
        self.assertIn("2", summary)
        self.assertIn("刷新", summary)
        self.assertIn("2026-09-29", summary)
        self.assertIn("05:30:57", summary)
        self.assertEqual({path: (path.read_bytes(), path.stat().st_mtime_ns)
                          for path in history_root.rglob("status.json")}, before)

    def test_history_ui_handles_null_start_time_without_aborting_refresh(self):
        history_root = pathlib.Path(self.temporary.name)
        for name, started_at in (("valid", "2026-09-28T21:30:57+00:00"), ("missing_time", None)):
            (history_root / name).mkdir()
            worker.atomic_write_json(history_root / name / "status.json", {"state": "failed", "started_at": started_at})
        self.app.history_button.click()
        self.app.history_future.result(timeout=5)
        self.app.finish_loading()
        self.assertEqual(self.app.history_tree.topLevelItemCount(), 2)
        self.assertNotIn("历史读取失败", self.app.lock_label.text())
        self.assertTrue(self.app.history_button.isEnabled())

    def test_missing_status_clears_previous_monitor_values_and_uses_only_its_bootstrap(self):
        history_root = pathlib.Path(self.temporary.name)
        finished_root = history_root / "finished"
        finished_root.mkdir()
        worker.atomic_write_json(finished_root / "command_manifest.json", {"stages": [{"name": "example", "index": 1}]})
        completed = {"state": "succeeded", "started_at": "2026-09-01T00:00:00+00:00", "finished_at": "2026-09-01T00:01:02+00:00",
                     "stage_name": "example", "recent_lines": ["old output"], "progress": "old progress"}
        worker.atomic_write_json(finished_root / "status.json", completed)
        self.app.view_run_root = finished_root
        self.app.refresh_monitor()
        self.assertEqual(self.app.stage_tree.topLevelItemCount(), 1)
        incomplete_root = history_root / "incomplete"
        incomplete_root.mkdir()
        bootstrap_path = incomplete_root / "bootstrap.log"
        bootstrap_path.write_text("new bootstrap only", encoding="utf-8")
        before = (bootstrap_path.read_bytes(), bootstrap_path.stat().st_mtime_ns)
        self.app.view_run_root = incomplete_root
        self.app.refresh_monitor()
        self.assertEqual(self.app.run_title.text(), "未发布状态")
        self.assertEqual(self.app.elapsed_label.text(), "—")
        self.assertEqual(self.app.heartbeat_label.text(), "—")
        self.assertEqual(self.app.stage_tree.topLevelItemCount(), 0)
        self.assertEqual(self.app.stage_items, {})
        self.assertNotIn("old progress", self.app.progress_label.text())
        self.assertIn("尚未发布", self.app.progress_label.text())
        self.assertEqual(self.app.logs.toPlainText(), "new bootstrap only")
        self.assertEqual((bootstrap_path.read_bytes(), bootstrap_path.stat().st_mtime_ns), before)
        empty_root = history_root / "launch_failure"
        empty_root.mkdir()
        worker.atomic_write_json(empty_root / "launch_failure.json", {"error": "cannot launch", "at": "2026-09-29T00:00:00+00:00"})
        self.app.view_run_root = empty_root
        self.app.refresh_monitor()
        self.assertEqual(self.app.run_title.text(), "启动失败")
        self.assertEqual(self.app.logs.toPlainText(), "")
        self.assertIn("cannot launch", self.app.error_label.text())
        self.assertFalse((incomplete_root / "monitor.json").exists())
        self.assertFalse((empty_root / "monitor.json").exists())

    def test_return_button_targets_only_batch_started_in_this_window(self):
        self.assertEqual(self.app.current_button.text(), "返回本窗口批次")
        self.assertFalse(self.app.current_button.isEnabled())
        self.assertTrue(self.app.current_button.toolTip())
        history_root = pathlib.Path(self.temporary.name)
        own_root, other_root = history_root / "own", history_root / "historical"
        for run_root in (own_root, other_root):
            run_root.mkdir()
            worker.atomic_write_json(run_root / "status.json", {"state": "succeeded"})
        self.app.view_run_root = other_root
        self.app.refresh_monitor()
        self.assertFalse(self.app.current_button.isEnabled())
        self.app.active_run_root = own_root
        self.app.refresh_monitor()
        self.assertTrue(self.app.current_button.isEnabled())
        self.app.current_button.click()
        self.assertEqual(self.app.view_run_root, own_root)
        self.assertIs(self.app.tabs.currentWidget(), self.app.monitor_tab)
        self.assertFalse(self.app.current_button.isEnabled())
        self.app.view_run_root = other_root
        self.app.refresh_monitor()
        self.assertTrue(self.app.current_button.isEnabled())
        self.app.current_button.click()
        self.assertEqual(self.app.view_run_root, own_root)

    def test_viewing_history_keeps_lease_and_interruption_on_this_windows_batch(self):
        history_root = pathlib.Path(self.temporary.name)
        active_root, historical_root = history_root / "active", history_root / "historical"
        active_root.mkdir()
        historical_root.mkdir()
        worker.atomic_write_json(active_root / "status.json", {
            "state": "running", "stage_name": "a01/b06_futures_minute", "progress": "fixture_count=7",
            "started_at": worker.utc_now_text(), "heartbeat_at": worker.utc_now_text(), "worker_pid": os.getpid(),
        })
        worker.atomic_write_json(active_root / "monitor.json", {"heartbeat_at": "old"})
        worker.atomic_write_json(historical_root / "request.json", {"created_at": "2026-09-01T00:00:00+00:00"})
        worker.atomic_write_json(historical_root / "monitor.json", {"heartbeat_at": "historical"})
        before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in historical_root.iterdir()}
        self.app.active_run_root = active_root
        self.app.view_run_root = historical_root
        self.app.worker_process = mock.Mock()
        self.app.worker_process.poll.return_value = None
        self.app.refresh_monitor()
        self.assertEqual(self.app.run_title.text(), "未发布状态")
        self.assertIn("历史", self.app.view_context_label.text())
        self.assertIn("fixture_count=7", self.app.view_context_label.text())
        self.assertNotEqual(worker.read_json_shared(active_root / "monitor.json")["heartbeat_at"], "old")
        self.app.interrupt()
        self.assertTrue((active_root / "interrupt.request").exists())
        self.assertEqual({path: (path.read_bytes(), path.stat().st_mtime_ns) for path in historical_root.iterdir()}, before)
        self.assertFalse((historical_root / "monitor.pid").exists())

    def test_entry_intro_is_visible_with_parameters_and_full_notes_remain_available(self):
        self.assertEqual(self.app.detail_tabs.currentWidget(), self.app.calendar_page)
        self.app.detail_tabs.setCurrentIndex(0)
        self.assertFalse(self.app.entry_summary.isHidden())
        self.assertIn("dim_trade_calendar", self.app.entry_summary.text())
        self.assertIn("每个自然日保留一行", self.app.entry_summary.text())
        name = "a01/b08_full_minute_quality"
        self.app.entry_tree.setCurrentItem(self.app.entry_items[name])
        self.assertEqual(self.app.detail_tabs.currentIndex(), 0)
        self.assertIn("fact_futures_missing_bar", self.app.entry_summary.text())
        self.assertIn("不调用外部 API", self.app.entry_summary.text())
        self.assertIn("后续读取", self.app.notes.toPlainText())
        self.assertFalse(self.app.parameter_widgets[name]["confirm_full_quality"].isChecked())
        self.assertFalse(self.app.parameter_widgets[name]["write"].isChecked())

    def test_failed_render_does_not_renew_monitor_lease(self):
        run_root = pathlib.Path(self.temporary.name) / "active"
        run_root.mkdir()
        worker.atomic_write_json(run_root / "status.json", {"state": "running"})
        worker.atomic_write_json(run_root / "monitor.json", {"heartbeat_at": "old"})
        self.app.active_run_root = self.app.view_run_root = run_root
        self.app.worker_process = mock.Mock()
        self.app.worker_process.poll.return_value = None
        self.app.monitor_timer.stop()
        with mock.patch.object(self.app, "render_status", side_effect=RuntimeError("broken rendering")):
            self.app.refresh_monitor()
        self.assertEqual(worker.read_json_shared(run_root / "monitor.json"), {"heartbeat_at": "old"})
        self.assertIn("broken rendering", self.app.error_label.text())
        self.app.worker_process = None

    def test_preview_is_read_only_and_freezes_reviewed_arguments(self):
        name = "a01/b06_futures_minute"
        self.app.entry_items[name].setCheckState(0, Qt.CheckState.Checked)
        self.app.entry_tree.setCurrentItem(self.app.entry_items[name])
        self.app.parameter_widgets[name]["quota_reserve"].setText("12345")
        with mock.patch.object(console, "start_detached_batch") as start_batch:
            self.app.preview()
            start_batch.assert_not_called()
        dialog = self.app.findChild(QDialog)
        self.assertIn("12345", dialog.findChild(QPlainTextEdit).toPlainText())
        self.app.parameter_widgets[name]["quota_reserve"].setText("67890")
        button = next(button for button in dialog.findChildren(QPushButton) if button.text() == "启动本批次")
        with mock.patch.object(self.app, "launch") as launch:
            button.click()
            self.assertEqual(launch.call_args.args[0]["tasks"][0]["effective_parameters"]["quota_reserve"], 12345)
        dialog.reject()

    def test_background_catalog_read_does_not_block_qt_events(self):
        release = threading.Event()
        entered = threading.Event()
        def delayed_catalog():
            entered.set()
            release.wait(5)
            return self.contracts
        with mock.patch.object(console, "discover_contracts", side_effect=delayed_catalog):
            window = console.OperationsConsole(self.preferences)
            try:
                self.assertTrue(entered.wait(1))
                ticks = []
                QTimer.singleShot(0, lambda: ticks.append(True))
                QTest.qWait(30)
                self.assertEqual(ticks, [True])
                self.assertFalse(window.preview_button.isEnabled())
                release.set()
                window.contracts_future.result(timeout=5)
                window.finish_loading()
                self.assertEqual(len(window.entry_items), 19)
            finally:
                release.set()
                window.io_pool.shutdown(wait=True)
                window.close()
                window.deleteLater()

    def test_close_active_batch_requests_interrupt_and_waits_for_exit(self):
        run_root = pathlib.Path(self.temporary.name) / "active"
        run_root.mkdir()
        self.app.active_run_root = run_root
        self.app.worker_process = mock.Mock()
        self.app.worker_process.poll.return_value = None
        with mock.patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.No):
            self.assertFalse(self.app.close())
        self.assertFalse((run_root / "interrupt.request").exists())
        with mock.patch.object(QMessageBox, "question", return_value=QMessageBox.StandardButton.Yes):
            self.assertFalse(self.app.close())
        self.assertTrue((run_root / "interrupt.request").exists())
        self.assertTrue(self.app.monitor_timer.isActive())
        self.app.worker_process.poll.return_value = 130
        self.app.refresh_monitor()
        self.assertFalse(self.app.monitor_timer.isActive())

    def test_theme_overrides_host_palette_and_covers_existing_dialogs(self):
        host_palette = QPalette()
        host_palette.setColor(QPalette.ColorRole.Text, QColor("white"))
        host_palette.setColor(QPalette.ColorRole.WindowText, QColor("white"))
        host_palette.setColor(QPalette.ColorRole.Base, QColor("black"))
        self.qt_application.setPalette(host_palette)
        name = "a01/b06_futures_minute"
        self.app.entry_items[name].setCheckState(0, Qt.CheckState.Checked)
        self.app.entry_tree.setCurrentItem(self.app.entry_items[name])
        self.app.parameter_widgets[name]["quota_reserve"].setText("12345")
        before = self.app.current_request()["tasks"]
        self.app.preview()
        dialog = self.app.findChild(QDialog)
        preview_text = dialog.findChild(QPlainTextEdit)
        reviewed_text = preview_text.toPlainText()
        for theme in ("light", "dark", "light"):
            with self.subTest(theme=theme):
                self.app.apply_theme(theme)
                self.qt_application.processEvents()
                colors = console.THEMES[theme]
                for group in (QPalette.ColorGroup.Active, QPalette.ColorGroup.Inactive):
                    for widget in (self.app.entry_tree, self.app.notes, self.app.logs, preview_text):
                        widget.ensurePolished()
                        self.assertEqual(widget.palette().color(group, QPalette.ColorRole.Text).name(), colors["text"])
                        background = "surface" if widget is self.app.entry_tree else "field"
                        self.assertEqual(widget.palette().color(group, QPalette.ColorRole.Base).name(), colors[background])
                self.assertEqual(self.app.current_request()["tasks"], before)
                self.assertEqual(preview_text.toPlainText(), reviewed_text)
                self.assertIs(self.app.entry_tree.currentItem(), self.app.entry_items[name])
        dialog.reject()

    def test_theme_preference_is_saved_and_restored_without_business_parameters(self):
        self.assertEqual(self.app.theme_name, "light")
        self.assertEqual(self.app.theme_button.text(), "")
        self.assertEqual(self.app.theme_button.accessibleName(), "切换到暗色主题")
        self.app.theme_button.click()
        self.assertEqual(self.app.theme_button.accessibleName(), "切换到亮色主题")
        self.assertEqual(self.preferences.allKeys(), ["appearance/theme"])
        self.assertEqual(self.preferences.value("appearance/theme"), "dark")
        with mock.patch.object(console, "discover_contracts", return_value=self.contracts):
            restored = console.OperationsConsole(self.preferences)
        try:
            self.assertEqual(restored.theme_name, "dark")
            self.assertEqual(restored.theme_button.toolTip(), "切换到亮色主题")
            restored.theme_button.click()
            self.assertEqual(restored.theme_name, "light")
            self.assertEqual(self.preferences.value("appearance/theme"), "light")
        finally:
            restored.io_pool.shutdown(wait=True)
            restored.close()
            restored.deleteLater()

    def test_search_and_navigation_preserve_hidden_batch_selection(self):
        self.app.select_daily()
        before = self.app.current_request()["tasks"]
        self.app.entry_search.setText("a02 排名")
        visible = [name for name, item in self.app.entry_items.items() if not item.isHidden()]
        self.assertEqual(visible, ["a02/b01a_position_rank_special_case_calibration", "a02/b02_futures_holding_reports"])
        self.assertEqual(self.app.current_request()["tasks"], before)
        self.app.entry_search.setText("no matching entry")
        self.assertFalse(self.app.search_empty.isHidden())
        self.assertTrue(self.app.preview_button.isEnabled())
        self.app.entry_search.clear()
        self.assertTrue(all(not item.isHidden() for item in self.app.entry_items.values()))
        self.app.detail_tabs.setCurrentIndex(1)
        self.assertIn("b01_trade_calendar", self.app.notes.toPlainText())
        self.app.navigation_buttons[1].click()
        self.assertIs(self.app.tabs.currentWidget(), self.app.monitor_tab)
        self.app.tabs.setCurrentWidget(self.app.configure_tab)
        self.assertTrue(self.app.navigation_buttons[0].isChecked())
        self.app.clear_selection()
        self.assertFalse(self.app.preview_button.isEnabled())

    def test_application_and_theme_icons_render_at_small_and_large_sizes(self):
        self.assertFalse(self.app.windowIcon().isNull())
        for symbol in ("latitude", "sun", "moon"):
            for size in (16, 32, 64, 128):
                with self.subTest(symbol=symbol, size=size):
                    pixels = console.console_icon(symbol, "#123456").pixmap(size, size).toImage()
                    self.assertFalse(pixels.isNull())
                    self.assertTrue(any(pixels.pixelColor(x, y).alpha() > 0 for x in range(pixels.width()) for y in range(pixels.height())))

    def test_both_themes_have_readable_text_and_status_contrast(self):
        def luminance(hex_color):
            channels = [int(hex_color[index:index + 2], 16) / 255 for index in (1, 3, 5)]
            channels = [value / 12.92 if value <= 0.04045 else ((value + 0.055) / 1.055) ** 2.4 for value in channels]
            return sum(value * weight for value, weight in zip(channels, (0.2126, 0.7152, 0.0722)))
        for theme, colors in console.THEMES.items():
            pairs = [(text, background) for text in ("text", "muted", "danger", "success", "info")
                     for background in ("canvas", "surface", "field", "alternate")]
            pairs.extend((("selection_text", "selection"), ("accent_text", "accent")))
            for text, background in pairs:
                with self.subTest(theme=theme, text=text, background=background):
                    values = sorted((luminance(colors[text]), luminance(colors[background])))
                    self.assertGreaterEqual((values[1] + 0.05) / (values[0] + 0.05), 4.5)


if __name__ == "__main__":
    unittest.main()
