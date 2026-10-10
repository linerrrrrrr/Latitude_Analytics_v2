"""单环节启动、固定维护命令和有界日志读取；不启动实际 worker。"""

from __future__ import annotations

import copy
from concurrent.futures import Future
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
from PySide6.QtCore import QSettings, Qt
from PySide6.QtWidgets import QApplication, QMessageBox

import click

project_markers = [".git", ".env", "config/settings.py"]
current_path = Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
OPERATIONS_ROOT = PROJECT_ROOT / "R02_Market_Data/a01_Collection" / "operations"
sys.path.insert(0, str(OPERATIONS_ROOT))
sys.path.insert(0, str(OPERATIONS_ROOT / "runtime"))
import console
import background_worker as worker


CONTRACTS = console.discover_contracts()


class MaintenanceRequestTests(unittest.TestCase):
    def test_all_five_tools_freeze_one_allowlisted_command(self):
        expected_arguments = {
            "verify_runtime": [],
            "verify_control": [],
            "check_code": ["--check", "--check-level", "code"],
            "check_full": ["--check", "--check-level", "full"],
            "sync_exports": ["--write"],
        }
        for tool_id, arguments in expected_arguments.items():
            with self.subTest(tool=tool_id):
                request = console.prepare_maintenance_request(tool_id)
                self.assertEqual(request["kind"], "maintenance")
                self.assertEqual(request["tool"], tool_id)
                self.assertEqual(len(request["tasks"]), 1)
                task = request["tasks"][0]
                self.assertEqual(task["name"], "maintenance/" + tool_id)
                self.assertEqual(task["arguments"], arguments)
                source_path = PROJECT_ROOT / task["entrypoint"]
                self.assertEqual(task["source_sha256"], hashlib.sha256(source_path.read_bytes()).hexdigest())
                stages = console.stages_from_request(request)
                self.assertEqual(len(stages), 1)
                self.assertEqual(stages[0].name, task["name"])
                self.assertEqual(stages[0].arguments, ("--execute-tool", tool_id, task["source_sha256"]))

    def test_unknown_tools_and_tampered_maintenance_requests_are_rejected(self):
        with self.assertRaises(ValueError):
            console.prepare_maintenance_request("arbitrary.py")
        request = console.prepare_maintenance_request("check_code")
        for modification in ("arguments", "entrypoint", "source_sha256", "effective_parameters", "extra_task", "unknown_tool", "tool_mismatch"):
            modified = copy.deepcopy(request)
            if modification == "arguments":
                modified["tasks"][0]["arguments"] = ["--write"]
            elif modification == "entrypoint":
                modified["tasks"][0]["entrypoint"] = "../../outside.py"
            elif modification == "source_sha256":
                modified["tasks"][0]["source_sha256"] = "0" * 64
            elif modification == "effective_parameters":
                modified["tasks"][0]["effective_parameters"] = {"action": "sync_exports"}
            elif modification == "extra_task":
                modified["tasks"].append(copy.deepcopy(modified["tasks"][0]))
            elif modification == "unknown_tool":
                modified["tool"] = "arbitrary.py"
            else:
                modified["tool"] = "sync_exports"
            with self.subTest(change=modification), self.assertRaises(ValueError):
                console.stages_from_request(modified)

    def test_collection_request_cannot_execute_maintenance_task(self):
        maintenance_task = console.prepare_maintenance_request("sync_exports")["tasks"][0]
        request = console.prepare_request(CONTRACTS, {CONTRACTS[0].name: []})
        for tasks in ([maintenance_task], [request["tasks"][0], maintenance_task]):
            modified = {**request, "tasks": tasks}
            with self.subTest(task_count=len(tasks)), mock.patch.object(console, "discover_contracts", return_value=CONTRACTS):
                with self.assertRaises(ValueError):
                    console.stages_from_request(modified)

    def test_source_changed_after_request_is_rejected_without_running_tool(self):
        request = console.prepare_maintenance_request("verify_runtime")
        original_read_bytes = Path.read_bytes
        tool_path = PROJECT_ROOT / request["tasks"][0]["entrypoint"]

        def changed_source(path):
            return b"changed source" if path == tool_path else original_read_bytes(path)

        with mock.patch.object(Path, "read_bytes", changed_source):
            with self.assertRaises(ValueError):
                console.stages_from_request(request)


class SingleEntryAndToolsWidgetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt_application = QApplication.instance() or QApplication([])
        cls.qt_application.setQuitOnLastWindowClosed(False)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "R02_Market_Data/a01_Collection/operations/tests")
        self.addCleanup(self.temporary.cleanup)
        history_root = Path(self.temporary.name)
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": str(history_root), "read_at": worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": str(history_root), "read_at": worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        self.history_patch = mock.patch.object(worker, "RUN_HISTORY_ROOT", history_root)
        self.lock_patch = mock.patch.object(worker, "GLOBAL_LOCK_PATH", history_root / ".lock")
        self.history_patch.start()
        self.lock_patch.start()
        self.addCleanup(self.history_patch.stop)
        self.addCleanup(self.lock_patch.stop)
        self.preferences = QSettings(str(history_root / "appearance.ini"), QSettings.Format.IniFormat)
        with mock.patch.object(console, "discover_contracts", return_value=CONTRACTS):
            self.app = console.OperationsConsole(self.preferences)
            self.app.contracts_future.result(timeout=5)
            self.app.finish_loading()

    def tearDown(self):
        self.app.worker_process = None
        self.app.io_pool.shutdown(wait=True, cancel_futures=True)
        self.app.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.app.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.app.close()
        self.app.deleteLater()
        self.qt_application.processEvents()

    def test_single_request_uses_current_form_without_changing_batch_selection(self):
        app = self.app
        checked_name = "b01/c01_trade_calendar"
        current_name = "b01/c06_futures_minute"
        app.entry_items[checked_name].setCheckState(0, Qt.CheckState.Checked)
        app.entry_tree.setCurrentItem(app.entry_items[current_name])
        app.parameter_widgets[current_name]["quota_reserve"].setText("4321")
        app.parameter_widgets[current_name]["write"].setChecked(True)
        selected_before = {name: item.checkState(0) for name, item in app.entry_items.items()}
        request = app.current_request(current_name)
        self.assertEqual([task["name"] for task in request["tasks"]], [current_name])
        self.assertEqual(request["tasks"][0]["effective_parameters"]["quota_reserve"], 4321)
        self.assertTrue(request["tasks"][0]["effective_parameters"]["write"])
        self.assertEqual({name: item.checkState(0) for name, item in app.entry_items.items()}, selected_before)
        self.assertEqual([task["name"] for task in app.current_request()["tasks"]], [checked_name])

    def test_run_entry_button_launches_one_current_entry_without_preview(self):
        app = self.app
        name = "b01/c06_futures_minute"
        app.daily_button.click()
        app.entry_tree.setCurrentItem(app.entry_items[name])
        app.parameter_widgets[name]["quota_reserve"].setText("7890")
        with mock.patch.object(app, "launch") as launch, mock.patch.object(app, "preview") as preview:
            app.run_entry_button.click()
        preview.assert_not_called()
        launch.assert_called_once()
        request, dialog = launch.call_args.args
        self.assertIsNone(dialog)
        self.assertEqual([task["name"] for task in request["tasks"]], [name])
        self.assertEqual(request["tasks"][0]["effective_parameters"]["quota_reserve"], 7890)
        self.assertTrue(request["tasks"][0]["effective_parameters"]["write"])
        self.assertEqual(len(app.current_request()["tasks"]), 18)

    def test_invalid_parameters_and_unconfirmed_b08_never_launch(self):
        app = self.app
        name = "b01/c06_futures_minute"
        app.entry_tree.setCurrentItem(app.entry_items[name])
        app.parameter_widgets[name]["quota_reserve"].setText("-1")
        with mock.patch.object(app, "launch") as launch, mock.patch.object(QMessageBox, "warning") as warning:
            app.run_entry_button.click()
            launch.assert_not_called()
            warning.assert_called_once()
        b08 = "b01/c08_full_minute_quality"
        app.entry_tree.setCurrentItem(app.entry_items[b08])
        with self.assertRaises(click.MissingParameter):
            app.current_request(b08)
        with mock.patch.object(app, "launch") as launch, mock.patch.object(QMessageBox, "warning") as warning:
            app.run_entry_button.click()
            launch.assert_not_called()
            warning.assert_called_once()
        app.parameter_widgets[b08]["confirm_full_quality"].setChecked(True)
        with mock.patch.object(app, "launch") as launch:
            app.run_entry_button.click()
        self.assertEqual(launch.call_args.args[0]["tasks"][0]["arguments"], ["--confirm-full-quality"])

    def test_check_tools_launch_directly_but_full_sync_requires_preview(self):
        app = self.app
        for tool_id in ("verify_runtime", "verify_control", "check_code", "check_full", "sync_exports"):
            with self.subTest(tool=tool_id):
                page_id = "b00_01" if tool_id in {"verify_runtime", "verify_control"} else "b00_02"
                app.entry_tree.setCurrentItem(app.b00_items[page_id])
                app.b00_mode_tabs.setCurrentIndex(next(index for index in range(app.b00_mode_tabs.count()) if app.b00_mode_tabs.tabData(index) == tool_id))
                with mock.patch.object(app, "launch") as launch, mock.patch.object(app, "preview") as preview:
                    app.run_maintenance()
                if tool_id == "sync_exports":
                    launch.assert_not_called()
                    preview.assert_called_once()
                    request = preview.call_args.args[0]
                else:
                    preview.assert_not_called()
                    launch.assert_called_once()
                    request, dialog = launch.call_args.args
                    self.assertIsNone(dialog)
                self.assertEqual(request["tool"], tool_id)
                self.assertEqual(len(request["tasks"]), 1)

    def test_write_toggle_immediately_refreshes_current_entry_hint(self):
        app = self.app
        name = "b01/c06_futures_minute"
        app.entry_tree.setCurrentItem(app.entry_items[name])
        write_field = app.parameter_widgets[name]["write"]
        write_field.setChecked(True)
        self.assertIn("将写入业务结果", app.entry_run_hint.text())
        write_field.setChecked(False)
        self.assertIn("只读运行", app.entry_run_hint.text())

    def test_pending_catalog_reload_blocks_maintenance_launch_and_preview(self):
        app = self.app
        with mock.patch.object(app, "contracts_future", Future()), mock.patch.object(app, "launch") as launch, mock.patch.object(
            app, "preview"
        ) as preview, mock.patch.object(console, "prepare_maintenance_request") as prepare:
            app.update_selection()
            self.assertFalse(app.run_tool_button.isEnabled())
            for tool_id in ("check_code", "sync_exports"):
                app.entry_tree.setCurrentItem(app.b00_items["b00_02"])
                app.b00_mode_tabs.setCurrentIndex(next(index for index in range(app.b00_mode_tabs.count()) if app.b00_mode_tabs.tabData(index) == tool_id))
                app.run_maintenance()
            self.assertIn("正在读取环节参数", app.maintenance_description.text())
            prepare.assert_not_called()
            launch.assert_not_called()
            preview.assert_not_called()

    def test_completed_sync_reloads_once_and_preserves_forms_and_selection(self):
        app = self.app
        name = "b01/c06_futures_minute"
        app.daily_button.click()
        app.entry_tree.setCurrentItem(app.entry_items[name])
        app.parameter_widgets[name]["quota_reserve"].setText("2468")
        app.parameter_widgets[name]["write"].setChecked(False)
        before_request = app.current_request()
        app.active_request = console.prepare_maintenance_request("sync_exports")
        app.active_run_root = Path(self.temporary.name) / "completed-sync"
        app.worker_process = mock.Mock()
        app.worker_process.poll.return_value = None
        with mock.patch.object(console, "discover_contracts", return_value=CONTRACTS) as discover:
            with mock.patch.object(worker, "atomic_write_json"):
                app.refresh_monitor()
            discover.assert_not_called()
            self.assertEqual(app.current_request()["tasks"], before_request["tasks"])
            app.worker_process.poll.return_value = 0
            app.refresh_monitor()
            app.contracts_future.result(timeout=5)
            app.finish_loading()
            app.refresh_monitor()
            discover.assert_called_once()
        self.assertEqual(app.entry_tree.currentItem().data(0, Qt.ItemDataRole.UserRole), name)
        self.assertEqual(app.parameter_widgets[name]["quota_reserve"].text(), "2468")
        self.assertFalse(app.parameter_widgets[name]["write"].isChecked())
        self.assertEqual(app.current_request()["tasks"], before_request["tasks"])
        self.assertEqual(app.catalog_reloaded_run, app.active_run_root)

    def test_late_log_read_cannot_mix_old_and_new_batches(self):
        app = self.app
        first_root = Path(self.temporary.name) / "first-batch"
        second_root = Path(self.temporary.name) / "second-batch"
        first_root.mkdir()
        second_root.mkdir()
        first_log = first_root / "stage.log"
        second_log = second_root / "stage.log"
        first_log.write_bytes(b"first batch only\n")
        second_log.write_bytes("第二批\n".encode("utf-8"))
        first_future, second_future, next_future = Future(), Future(), Future()
        stage_status = {"phase": "collection", "stage_name": "b01/c06_futures_minute", "state": "running"}
        with mock.patch.object(app.io_pool, "submit", side_effect=[first_future, second_future, next_future]) as submit:
            app.view_run_root = first_root
            app.view_status = {**stage_status, "log_path": str(first_log)}
            app.refresh_stage_details()
            submit.assert_called_once_with(console.read_log_chunk, first_log, None)
            app.view_run_root = second_root
            app.view_status = {**stage_status, "log_path": str(second_log)}
            app.refresh_stage_details()
            self.assertEqual(app.logs.toPlainText(), "")
            self.assertEqual(submit.call_count, 1)
            first_future.set_result(console.read_log_chunk(first_log, None))
            app.refresh_stage_details()
            self.assertEqual(app.logs.toPlainText(), "")
            submit.assert_called_with(console.read_log_chunk, second_log, None)
            second_future.set_result(console.read_log_chunk(second_log, None))
            app.refresh_stage_details()
            self.assertEqual(app.logs.toPlainText(), "第二批\n")
            self.assertNotIn("first batch", app.logs.toPlainText())
            submit.assert_called_with(console.read_log_chunk, second_log, second_log.stat().st_size)


class InternalToolCliTests(unittest.TestCase):
    def test_execute_tool_hash_gate_uses_fixed_arguments(self):
        for tool_id in console.MAINTENANCE_TOOLS:
            with self.subTest(tool=tool_id):
                request = console.prepare_maintenance_request(tool_id)
                task = request["tasks"][0]
                path = PROJECT_ROOT / task["entrypoint"]
                observed_argv = []
                with mock.patch.object(sys, "argv", ["console.py"]), mock.patch.object(
                    console.runpy, "run_path", side_effect=lambda *args, **kwargs: observed_argv.append(sys.argv.copy())
                ) as run_path:
                    exit_code = console.main(["--execute-tool", tool_id, task["source_sha256"]])
                self.assertEqual(exit_code, 0)
                run_path.assert_called_once_with(str(path), run_name="__main__")
                self.assertEqual(observed_argv, [[str(path), *task["arguments"]]])

    def test_unknown_hash_mismatch_and_extra_cli_arguments_never_execute(self):
        request = console.prepare_maintenance_request("check_code")
        digest = request["tasks"][0]["source_sha256"]
        invalid_arguments = [
            ["--execute-tool", "arbitrary.py", digest],
            ["--execute-tool", "check_code", "0" * 64],
            ["--execute-tool", "check_code", digest, "--write"],
            ["--execute-tool", "check_code"],
        ]
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments), mock.patch.object(console.runpy, "run_path") as run_path:
                with self.assertRaises((ValueError, RuntimeError)):
                    console.main(arguments)
                run_path.assert_not_called()
        with mock.patch.object(sys, "argv", ["console.py"]), mock.patch.object(
            console.runpy, "run_path", side_effect=SystemExit(7)
        ):
            with self.assertRaises(SystemExit) as failure:
                console.main(["--execute-tool", "check_code", digest])
            self.assertEqual(failure.exception.code, 7)

    def test_maintenance_skips_business_preflight_but_collection_keeps_it(self):
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "R02_Market_Data/a01_Collection/operations/tests") as temporary:
            history_root = Path(temporary)
            maintenance_request = console.prepare_maintenance_request("sync_exports")
            collection_request = console.prepare_request(CONTRACTS, {CONTRACTS[0].name: []})
            for request in (maintenance_request, collection_request):
                kind = request.get("kind", "collection")
                run_root = history_root / kind
                run_root.mkdir()
                worker.atomic_write_json(run_root / "request.json", request)
                with self.subTest(kind=kind), mock.patch.object(worker, "RUN_HISTORY_ROOT", history_root), mock.patch.object(
                    console, "discover_contracts", return_value=CONTRACTS
                ), mock.patch.object(worker, "run_batch", return_value=17) as run_batch:
                    exit_code = console.main(["--execute-request", str(run_root / "request.json")])
                self.assertEqual(exit_code, 17)
                run_batch.assert_called_once()
                self.assertEqual(run_batch.call_args.kwargs["run_root"], run_root)
                self.assertEqual(run_batch.call_args.kwargs["operation_name"], "interactive_" + kind)
                self.assertEqual(run_batch.call_args.kwargs["preflight_stages"], () if kind == "maintenance" else None)


class LogChunkTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "R02_Market_Data/a01_Collection/operations/tests")
        self.addCleanup(self.temporary.cleanup)
        self.log_path = Path(self.temporary.name) / "stage.log"

    def test_initial_tail_and_incremental_reads_are_bounded(self):
        initial_bytes = b"old\n" * 100_000
        self.log_path.write_bytes(initial_bytes)
        offset, content, reset = console.read_log_chunk(self.log_path, None)
        self.assertTrue(reset)
        self.assertEqual(len(content), 262144)
        self.assertEqual(content, initial_bytes[-262144:])
        self.assertEqual(offset, len(initial_bytes))
        with self.log_path.open("ab") as stream:
            stream.write("新增日志\n".encode("utf-8"))
        next_offset, content, reset = console.read_log_chunk(self.log_path, offset)
        self.assertFalse(reset)
        self.assertEqual(content, "新增日志\n".encode("utf-8"))
        self.assertEqual(next_offset, self.log_path.stat().st_size)
        self.assertEqual(console.read_log_chunk(self.log_path, next_offset), (next_offset, b"", False))

    def test_chunk_boundaries_preserve_raw_utf8_bytes(self):
        expected_bytes = b"a" * 262143 + "合约\n".encode("utf-8") * 70_000
        self.log_path.write_bytes(expected_bytes)
        offset = 0
        chunks = []
        while offset < len(expected_bytes):
            offset, content, reset = console.read_log_chunk(self.log_path, offset)
            self.assertFalse(reset)
            self.assertLessEqual(len(content), 262144)
            self.assertTrue(content)
            chunks.append(content)
        self.assertEqual(b"".join(chunks), expected_bytes)

    def test_truncated_logs_reset_and_handles_are_released(self):
        self.log_path.write_bytes(b"old log\n" * 100)
        offset, _, _ = console.read_log_chunk(self.log_path, None)
        self.log_path.write_bytes(b"replacement\n")
        new_offset, content, reset = console.read_log_chunk(self.log_path, offset)
        self.assertEqual((new_offset, content, reset), (12, b"replacement\n", True))
        renamed_path = self.log_path.with_name("renamed.log")
        self.log_path.rename(renamed_path)
        self.assertTrue(renamed_path.is_file())
        with self.assertRaises(OSError):
            console.read_log_chunk(self.log_path, None)


if __name__ == "__main__":
    unittest.main()
