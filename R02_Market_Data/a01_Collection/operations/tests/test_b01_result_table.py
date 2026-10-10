"""b01 result-first page, using only temporary calendars and a mocked source API."""
from datetime import date, datetime, time, timedelta, timezone
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
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
from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QApplication
import pyarrow as pa
import pyarrow.parquet as pq
import console
from test_a01_calendar_dashboards import evidence

NAME = "b01/c01_trade_calendar"


def calendar_fixture(start=date(2026, 8, 1), days=59):
    """Synthetic weekday flags; never represents an exchange's real trading calendar."""
    rows = []
    for offset in range(days):
        current_date = start + timedelta(days=offset)
        rows.append({"calendar_date": current_date, "date_key": current_date.strftime("%Y%m%d"),
                     "is_trading_day": current_date.weekday() < 5, "weekday": current_date.weekday() + 1,
                     "is_weekend": current_date.weekday() >= 5, "source": "JQData_get_trade_days",
                     "calendar_name": "CN_FUTURES_MARKET", "calendar_timezone": "Asia/Shanghai",
                     "effective_after": time(20, 0), "updated_at": datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc),
                     "year": current_date.year})
    return pa.Table.from_pylist(rows, schema=console.TRADE_CALENDAR_SCHEMA)


class TradeCalendarTableTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt = QApplication.instance() or QApplication([])
        cls.qt.setQuitOnLastWindowClosed(False)
        cls.contracts = console.discover_contracts()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=ROOT / "R02_Market_Data/a01_Collection/operations/tests")
        self.root = Path(self.temporary.name)
        self.lake_root = self.root / "lake"
        self.table_path = self.lake_root / "silver" / console.TRADE_CALENDAR_TABLE_NAME
        pq.write_to_dataset(calendar_fixture(), self.table_path, partition_cols=console.TRADE_CALENDAR_PARTITIONS)
        with mock.patch.object(console, "discover_contracts", return_value=self.contracts):
            self.window = console.OperationsConsole(QSettings(str(self.root / "test.ini"), QSettings.Format.IniFormat))
            self.window.contracts_future.result(timeout=5)
            self.window.finish_loading()
        self.window.monitor_timer.stop()
        self.window.loading_timer.stop()
        self.window.parameter_values[NAME]["lake_root"] = str(self.lake_root)

    def tearDown(self):
        self.window.worker_process = None
        self.window.io_pool.shutdown(wait=True, cancel_futures=True)
        self.window.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.window.close()
        self.window.deleteLater()
        self.qt.processEvents()
        self.temporary.cleanup()

    def display(self, lines=(), state="succeeded", write=False):
        run_root = self.root / "run"
        run_root.mkdir(exist_ok=True)
        status = evidence(NAME, [line.replace(": ", f": table={console.TRADE_CALENDAR_TABLE_NAME}; ", 1) for line in lines], state)
        console.worker.atomic_write_json(run_root / "status.json", status)
        self.window.calendar_runs[NAME] = run_root
        self.window.calendar_requests[NAME] = {"effective_parameters": {"lake_root": str(self.lake_root), "write": write}, "arguments": []}
        self.window.entry_tree.setCurrentItem(self.window.entry_items[NAME])
        self.settle()
        return run_root

    def settle(self):
        for _ in range(3):
            self.window.refresh_calendar_dashboard()
            if self.window.trade_table_future:
                self.window.trade_table_future[1].result(timeout=5)
        self.window.refresh_calendar_dashboard()

    def test_noop_shows_zero_new_rows_real_table_and_explicit_completion(self):
        self.display(["planning_progress: function=main(); phase=run; status=started; mode=automatic_tail; valid_end=2026-09-28",
                      "up_to_date: mode=automatic_tail; latest_calendar_date=2026-09-28; api_calls=0"])
        window = self.window
        self.assertIs(window.calendar_page.currentWidget(), window.trade_result_scroll)
        self.assertEqual(window.trade_outcome.text(), "已是最新 · 本次无需更新")
        self.assertIn("新增 / 修订 0 行", window.trade_run_summary.text())
        self.assertIn("API 请求 0 次", window.trade_run_summary.text())
        self.assertEqual(window.trade_progress.value(), 1000)
        self.assertIn("59 行", window.trade_table_summary.text())
        self.assertEqual(window.trade_table.topLevelItemCount(), 28)
        self.assertEqual(window.trade_table.topLevelItem(0).text(0), "2026-09-28")
        self.assertIn("已落盘日历", window.trade_table_note.text())

    def test_filters_pagination_all_columns_and_timezone_preserve_source_rows(self):
        self.display(["up_to_date: latest_calendar_date=2026-09-28; api_calls=0"])
        window = self.window
        window.trade_day_filter.setCurrentIndex(1)
        self.assertTrue(all(window.trade_table.topLevelItem(index).text(2) == "● 交易日" for index in range(window.trade_table.topLevelItemCount())))
        window.trade_month.setCurrentIndex(8)
        self.assertEqual(window.trade_table.topLevelItemCount(), 21)
        window.trade_all_fields.setChecked(True)
        self.assertTrue(all(not window.trade_table.isColumnHidden(index) for index in range(11)))
        updated_index = console.TRADE_CALENDAR_SCHEMA.get_field_index("updated_at")
        self.assertEqual(window.trade_table.topLevelItem(0).text(updated_index), "2026-09-28 20:00:00")
        self.assertEqual(len(window.trade_table_snapshot["rows"]), 59)
        window.apply_theme("dark")
        self.assertEqual(window.trade_table.topLevelItem(0).foreground(2).color().name(), console.THEMES["dark"]["success"])
        self.assertEqual(console.read_trade_calendar_result(str(self.lake_root))["rows"][0]["calendar_date"], date(2026, 9, 28))

    def test_readonly_generated_result_is_not_confused_with_persisted_calendar(self):
        run_root = self.root / "run"
        (run_root / "artifacts").mkdir(parents=True)
        pq.write_table(calendar_fixture(date(2026, 9, 29), 2), run_root / "artifacts/b01_generated.parquet")
        self.display(["planning_progress: phase=collect; status=completed; start_date=2026-09-29; end_date=2026-09-30; rows=2; trading_day_count=2",
                      "planning_progress: phase=run; status=completed; write=false; validated_rows=2"], write=False)
        window = self.window
        self.assertEqual(window.trade_source.currentData(), "generated")
        self.assertIn("只读检查完成", window.trade_outcome.text())
        self.assertIn("尚未提交", window.trade_table_note.text())
        self.assertEqual(window.trade_table.topLevelItem(0).text(0), "2026-09-30")
        window.trade_source.setCurrentIndex(0)
        window.change_trade_source()
        self.settle()
        self.assertEqual(window.trade_table.topLevelItem(0).text(0), "2026-09-28")
        self.assertIn("已落盘日历", window.trade_table_note.text())

    def test_failed_transaction_does_not_claim_preview_was_committed(self):
        run_root = self.root / "run"
        (run_root / "artifacts").mkdir(parents=True)
        pq.write_table(calendar_fixture(date(2026, 9, 29), 2), run_root / "artifacts/b01_generated.parquet")
        self.display(["planning_progress: phase=collect; status=completed; rows=2",
                      "partition_committed: partition=year=2026; completed=1; total=1; run_id=fixture"], state="failed", write=True)
        self.assertIn("失败", self.window.trade_outcome.text())
        self.assertIn("未确认", self.window.trade_run_summary.text())
        self.assertIn("尚未提交", self.window.trade_table_note.text())

    def test_steady_monitor_refresh_does_not_rescan_lake(self):
        with mock.patch.object(console, "read_trade_calendar_result", wraps=console.read_trade_calendar_result) as read:
            self.display(["up_to_date: latest_calendar_date=2026-09-28; api_calls=0"])
            initial_calls = read.call_count
            for _ in range(15):
                self.window.refresh_calendar_dashboard()
            self.assertEqual(read.call_count, initial_calls)
            self.window.reload_trade_calendar()
            self.settle()
            self.assertEqual(read.call_count, initial_calls + 1)

    def test_loading_failures_and_empty_lake_are_explicit_and_retryable(self):
        with mock.patch.object(console, "read_trade_calendar_result", side_effect=OSError("fixture read failed")):
            self.window.entry_tree.setCurrentItem(self.window.entry_items[NAME])
            try:
                self.window.trade_table_future[1].result(timeout=5)
            except OSError:
                pass
            self.window.refresh_calendar_dashboard()
            self.assertIn("读取失败", self.window.trade_table_summary.text())
            self.assertIn("fixture read failed", self.window.trade_table_note.text())
        self.window.reload_trade_calendar()
        self.settle()
        self.assertIn("59 行", self.window.trade_table_summary.text())
        missing = console.read_trade_calendar_result(str(self.root / "missing"))
        self.assertFalse(missing["exists"])
        self.assertEqual(missing["rows"], [])

    def test_result_reading_is_independent_of_monitor_log_reader(self):
        import threading
        gate = threading.Event()
        original = console.read_trade_calendar_result
        def delayed(*args):
            gate.wait(5)
            return original(*args)
        with mock.patch.object(console, "read_trade_calendar_result", side_effect=delayed):
            try:
                self.window.entry_tree.setCurrentItem(self.window.entry_items[NAME])
                self.assertEqual(self.window.io_pool.submit(lambda: "logs responsive").result(timeout=1), "logs responsive")
            finally:
                gate.set()
                self.settle()

    def test_late_table_result_cannot_replace_a_new_lake_selection(self):
        import threading
        second_lake = self.root / "second_lake"
        pq.write_to_dataset(calendar_fixture(date(2026, 9, 29), 2), second_lake / "silver" / console.TRADE_CALENDAR_TABLE_NAME,
                            partition_cols=console.TRADE_CALENDAR_PARTITIONS)
        gate = threading.Event()
        original = console.read_trade_calendar_result
        def delayed(lake_root, preview_path=None):
            if lake_root == str(self.lake_root):
                gate.wait(5)
            return original(lake_root, preview_path)
        with mock.patch.object(console, "read_trade_calendar_result", side_effect=delayed):
            try:
                self.window.entry_tree.setCurrentItem(self.window.entry_items[NAME])
                self.window.parameter_widgets[NAME]["lake_root"].setText(str(second_lake))
                self.window.reload_trade_calendar()
            finally:
                gate.set()
            self.settle()
        self.assertEqual(len(self.window.trade_table_snapshot["rows"]), 2)
        self.assertEqual(self.window.trade_table.topLevelItem(0).text(0), "2026-09-30")


class PreviewPublicationTests(unittest.TestCase):
    def test_collect_publishes_validated_preview_without_writing_lake(self):
        source_path = ROOT / "R02_Market_Data/a01_Collection/b01_Futures_Market_Data/c01_trade_calendar.py"
        spec = importlib.util.spec_from_file_location("b01_preview_fixture", source_path)
        workflow = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(workflow)
        fake_client = SimpleNamespace(get_trade_days=mock.Mock(return_value=[date(2026, 9, 28)]))
        fake_connection = SimpleNamespace(authenticate_jqdata=mock.Mock(return_value=fake_client))
        with tempfile.TemporaryDirectory() as directory:
            preview_path = Path(directory) / "artifacts/b01_generated.parquet"
            with mock.patch.dict(sys.modules, {"config.jqdata_connection": fake_connection}), mock.patch.dict(os.environ,
                    {"LATITUDE_B01_PREVIEW_PATH": str(preview_path)}), mock.patch.object(workflow, "settings", SimpleNamespace(jqdata_id="fixture", jqdata_secret="fixture")):
                frame = workflow.collect(date(2026, 9, 27), date(2026, 9, 28))
            self.assertEqual(len(frame), 2)
            self.assertEqual(pq.read_table(preview_path).column("is_trading_day").to_pylist(), [False, True])
            self.assertFalse((Path(directory) / "silver").exists())
            self.assertFalse(preview_path.with_suffix(".parquet.tmp").exists())
            failing_path = Path(directory) / "artifacts/failed.parquet"
            with mock.patch.dict(sys.modules, {"config.jqdata_connection": fake_connection}), mock.patch.dict(os.environ,
                    {"LATITUDE_B01_PREVIEW_PATH": str(failing_path)}), mock.patch.object(workflow, "settings", SimpleNamespace(jqdata_id="fixture", jqdata_secret="fixture")), mock.patch("os.replace", side_effect=PermissionError("fixture preview failure")):
                frame = workflow.collect(date(2026, 9, 27), date(2026, 9, 28))
            self.assertEqual(len(frame), 2)
            self.assertFalse(failing_path.exists())

    def test_worker_scopes_preview_environment_to_b01_only(self):
        sys.path.insert(0, str(ROOT / "R02_Market_Data/a01_Collection/operations/tests"))
        import test_background_worker
        fixture = test_background_worker.BackgroundWorkerTests(methodName="runTest")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with fixture.isolated_worker(root) as (history, _), mock.patch.dict(os.environ, {"LATITUDE_B01_PREVIEW_PATH": "stale-parent-value"}):
                script = fixture.make_script(root, "env.py", "import os; print(os.environ.get('LATITUDE_B01_PREVIEW_PATH', 'absent'))")
                run_root = fixture.monitored_run_root(history, "preview_env")
                self.assertEqual(console.worker.run_batch(operation_name="preview_env", run_root=run_root, preflight_stages=(),
                    stages=(console.worker.StageSpec(NAME, script), console.worker.StageSpec("b01/c02_futures_variety_calendar", script))), 0)
                history = console.worker.read_json_shared(run_root / "status.json")["stage_history"]
                self.assertEqual(Path(history[0]["log_path"]).read_text().strip(), str(run_root / "artifacts/b01_generated.parquet"))
                self.assertEqual(Path(history[1]["log_path"]).read_text().strip(), "absent")


if __name__ == "__main__":
    unittest.main()
