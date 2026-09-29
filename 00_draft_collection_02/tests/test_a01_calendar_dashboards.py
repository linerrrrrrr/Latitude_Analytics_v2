"""Calendar dashboards consume audited logs only; all runs below are temporary fixtures."""
import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QApplication

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "02_Futures_Lakehouse/operations"))
import console

NAMES = tuple(console.CALENDAR_VIEWS)
TABLES = tuple(view["table"] for view in console.CALENDAR_VIEWS.values())


def evidence(name, lines, state="running"):
    """Build display fixtures from the producer's existing log format."""
    scopes = {}
    for sequence, line in enumerate(lines, 1):
        event = console.worker.parse_legacy_progress(line)
        if event is None:
            raise AssertionError(line)
        event["sequence"] = sequence
        previous = scopes.get(event["scope_id"], {})
        scopes[event["scope_id"]] = {**event,
            "object": {**previous.get("object", {}), **event["object"]},
            "counters": {**previous.get("counters", {}), **event["counters"]}}
    return {"state": state, "phase": "business", "stage_name": name,
            "progress_scopes": scopes, "current_scope_id": event["scope_id"] if lines else None,
            "stage_started_at": "2026-09-29T00:00:00+00:00", "started_at": "2026-09-29T00:00:00+00:00",
            "finished_at": "2026-09-29T00:00:20+00:00" if state != "running" else None}


class CalendarDashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt = QApplication.instance() or QApplication([])
        cls.qt.setQuitOnLastWindowClosed(False)
        cls.contracts = console.discover_contracts()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=ROOT / "00_draft_collection_02")
        self.root = Path(self.temporary.name)
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": str(self.root), "read_at": console.worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        self.table_patch = mock.patch.object(console, "read_trade_calendar_result", return_value={"rows": [], "exists": False,
            "path": str(self.root), "read_at": console.worker.utc_now_text()})
        self.table_patch.start()
        self.addCleanup(self.table_patch.stop)
        with mock.patch.object(console, "discover_contracts", return_value=self.contracts):
            self.window = console.OperationsConsole(QSettings(str(self.root / "test.ini"), QSettings.Format.IniFormat))
            self.window.contracts_future.result(timeout=5)
            self.window.finish_loading()
        self.window.monitor_timer.stop()
        self.window.loading_timer.stop()

    def tearDown(self):
        self.window.worker_process = None
        self.window.io_pool.shutdown(wait=True, cancel_futures=True)
        self.window.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.window.trade_table_pool.shutdown(wait=True, cancel_futures=True)
        self.window.close()
        self.window.deleteLater()
        self.qt.processEvents()
        self.temporary.cleanup()

    def show(self, name, status, write=True):
        run_root = self.root / name.split("/")[-1]
        run_root.mkdir(exist_ok=True)
        console.worker.atomic_write_json(run_root / "status.json", status)
        self.window.calendar_runs[name] = run_root
        self.window.calendar_requests[name] = {"effective_parameters": {"write": write}, "arguments": ["--write"] if write else []}
        self.window.entry_tree.setCurrentItem(self.window.entry_items[name])
        self.window.refresh_calendar_dashboard()
        return run_root

    def test_four_views_default_to_dashboard_and_keep_parameters_and_selection(self):
        window = self.window
        window.select_daily()
        before = copy.deepcopy(window.current_request())
        for name in NAMES:
            window.entry_tree.setCurrentItem(window.entry_items[name])
            self.assertIs(window.detail_tabs.currentWidget(), window.calendar_page)
            self.assertEqual(window.calendar_name, name)
            self.assertTrue(window.parameter_widgets[name]["write"].isChecked())
            self.assertIn("—", window.calendar_bar_widgets[0][1].text())
        window.entry_tree.setCurrentItem(window.entry_items["a01/b05_futures_daily"])
        self.assertFalse(window.detail_tabs.isTabVisible(2))
        self.assertEqual(window.detail_tabs.currentIndex(), 0)
        self.assertEqual(window.current_request()["tasks"], before["tasks"])

    def test_b01_natural_days_and_common_transaction_are_separate(self):
        table = TABLES[0]
        lines = [f"planning_progress: table={table}; phase=collect; status=started; start_date=2026-09-01; end_date=2026-09-30",
                 f"planning_progress: table={table}; phase=collect; status=completed; rows=30; trading_day_count=22",
                 f"partition_committed: table={table}; completed=1; total=1; partition=year=2026; rows=30; run_id=first"]
        self.show(NAMES[0], evidence(NAMES[0], lines, "failed"))
        self.assertIn("30 / 30 自然日", self.window.calendar_bar_widgets[0][2].format())
        self.assertIn("待事务确认", self.window.calendar_bar_widgets[1][2].format())
        self.assertIn("尚无成功退出", self.window.calendar_commit.text())
        self.assertEqual(self.window.calendar_state.text(), "失败")
        lines.append(f"committed: table={table}; status=completed; rows=30; partitions=1; run_id=first")
        self.show(NAMES[0], evidence(NAMES[0], lines, "succeeded"))
        self.assertIn("共同事务已确认", self.window.calendar_commit.text())

    def test_b02_later_transaction_clears_old_install_claims_and_bars(self):
        table = TABLES[1]
        lines = [f"planning_progress: table={table}; phase=expand; completed=250; total=500; rows=12000",
                 f"partition_committed: table={table}; completed=2; total=2; run_id=first",
                 f"committed: table={table}; status=completed; partitions=2; rows=12000; run_id=first",
                 f"partition_plan: table={table}; phase=commit; status=started; rows=100"]
        self.show(NAMES[1], evidence(NAMES[1], lines))
        self.assertIn("剩余 250", self.window.calendar_bar_widgets[0][1].text())
        self.assertEqual(self.window.calendar_bar_widgets[2][2].format(), "尚无可量化记录")
        self.assertNotIn("共同事务已确认", self.window.calendar_commit.text())

    def test_b03_information_responses_partition_comparison_and_watermark(self):
        table = TABLES[2]
        lines = [f"planning_progress: table={table}; phase=candidate_selection; status=completed; contracts=240; info_batches=3",
                 f"api_result: table={table}; phase=contract_info; status=completed; completed=1; total=3; returned_contracts=100",
                 f"planning_progress: table={table}; phase=reconcile; status=running; completed=3; total=12; partition=('SHFE',2026,9)",
                 f"planning_progress: table={table}; phase=expand; status=running; completed=250; total=1000; partition=('SHFE',2026,9)"]
        response = console.worker.parse_legacy_progress(lines[1])
        self.assertEqual(response["state"], "running")
        self.show(NAMES[2], evidence(NAMES[2], lines))
        self.assertEqual(self.window.calendar_metric_widgets[0][1].text(), "240")
        self.assertIn("剩余 2", self.window.calendar_bar_widgets[0][1].text())
        self.assertIn("剩余 9", self.window.calendar_bar_widgets[1][1].text())
        self.assertIn("当前分区", self.window.calendar_bar_widgets[2][2].format())
        lines.extend([f"planning_progress: table={table}; phase=commit_batch; status=running; completed=2; total=2",
                      f"planning_progress: table={table}; phase=watermark; status=failed; automatic_tail_processed_through=2026-09-29"])
        self.show(NAMES[2], evidence(NAMES[2], lines, "failed"))
        self.assertIn("已正式提交 2 叶", self.window.calendar_commit.text())
        self.assertIn("尚无本次水位提交确认", self.window.calendar_commit.text())

    def test_b04_frequencies_do_not_become_whole_batch_completion(self):
        table = TABLES[3]
        lines = [f"planning_progress: table={table}; phase=plan; status=running; completed=4; total=20",
                 f"planning_progress: table={table}; function=build_structural_partitions; phase=build_structure; status=completed; completed=2; total=2; minute_rows=400; daily_rows=200"]
        self.show(NAMES[3], evidence(NAMES[3], lines))
        self.assertIn("4 / 20", self.window.calendar_bar_widgets[1][2].format())
        self.assertIn("2 / 2 频率（当前分区）", self.window.calendar_bar_widgets[2][2].format())
        self.assertIn("水位步骤读取已有目标", self.window.calendar_boundary.text())
        lines.append(f"committed: table={table}; phase=commit_batch; status=completed; completed=5; total=5; skipped_clean_partitions=2; rows=400; partitions=3")
        self.show(NAMES[3], evidence(NAMES[3], lines, "succeeded"))
        self.assertIn("已处理 5 叶", self.window.calendar_commit.text())
        self.assertIn("正式提交 3 叶", self.window.calendar_commit.text())

    def test_preflight_and_previous_business_stage_never_supply_calendar_counts(self):
        status = evidence(NAMES[0], [f"planning_progress: table={TABLES[0]}; phase=collect; status=completed; rows=30"])
        status["phase"] = "preflight"
        status["stage_name"] = "verify_runtime"
        status["error_summary"] = ["missing dependency"]
        self.show(NAMES[2], status)
        self.assertIn("等待本环节", self.window.calendar_state.text())
        self.assertTrue(all(value.text() == "—" for _, value in self.window.calendar_metric_widgets))
        self.assertIn("missing dependency", self.window.calendar_error.text())
        status["phase"] = "business"
        status["stage_name"] = NAMES[0]
        status.pop("error_summary")
        self.show(NAMES[2], status)
        self.assertTrue(all(value.text() == "—" for _, value in self.window.calendar_metric_widgets))

    def test_stage_history_and_readonly_noop_keep_truthful_state(self):
        stage = evidence(NAMES[0], [f"up_to_date: table={TABLES[0]}; phase=run; status=completed; api_calls=0"], "succeeded")
        stage["name"] = stage.pop("stage_name")
        status = {"state": "running", "stage_name": NAMES[1], "stage_history": [stage],
                  "error_summary": [{"stage_name": NAMES[1], "message": "other entry error"}], "error": "other entry failure"}
        run_root = self.show(NAMES[0], status, write=False)
        before = (run_root / "status.json").read_bytes()
        self.window.refresh_calendar_dashboard()
        self.assertEqual(self.window.calendar_state.text(), "完成")
        self.assertEqual(self.window.calendar_error.text(), "")
        self.assertIn("只读运行", self.window.calendar_commit.text())
        self.assertIn("无需更新", self.window.calendar_commit.text())
        self.assertEqual((run_root / "status.json").read_bytes(), before)
        self.assertFalse((run_root / "monitor.json").exists())

    def test_single_launch_stays_on_own_dashboard_and_ignores_batch_selection(self):
        window = self.window
        window.select_daily()
        window.entry_tree.setCurrentItem(window.entry_items[NAMES[1]])
        before = copy.deepcopy(window.current_request())
        process = mock.Mock()
        process.poll.return_value = None
        with mock.patch.object(console, "start_detached_batch", return_value=(self.root, process)) as launch, mock.patch.object(window, "refresh_monitor"):
            window.run_current_entry()
        request = launch.call_args.args[0]
        self.assertEqual([task["name"] for task in request["tasks"]], [NAMES[1]])
        self.assertIs(window.tabs.currentWidget(), window.configure_tab)
        self.assertIs(window.detail_tabs.currentWidget(), window.calendar_page)
        self.assertEqual(window.current_request()["tasks"], before["tasks"])

    def test_async_log_results_cannot_cross_pages(self):
        status = evidence(NAMES[0], [])
        run_root = self.show(NAMES[0], status)
        log_path = run_root / "log.txt"
        log_path.write_text("b01 only\n", encoding="utf-8")
        status["log_path"] = str(log_path)
        self.show(NAMES[0], status)
        self.window.calendar_log_future[1].result(timeout=5)
        self.window.entry_tree.setCurrentItem(self.window.entry_items[NAMES[1]])
        self.window.refresh_calendar_dashboard()
        self.assertEqual(self.window.calendar_logs.toPlainText(), "")
        self.window.entry_tree.setCurrentItem(self.window.entry_items[NAMES[0]])
        self.window.calendar_log_future[1].result(timeout=5)
        self.window.refresh_calendar_dashboard()
        self.assertEqual(self.window.calendar_logs.toPlainText(), "b01 only\n")


class CalendarWorkerTests(unittest.TestCase):
    def test_real_worker_preserves_summaries_and_resets_repeated_local_work(self):
        sys.path.insert(0, str(ROOT / "02_Futures_Lakehouse/operations/tests"))
        import test_background_worker
        fixture = test_background_worker.BackgroundWorkerTests(methodName="runTest")
        table = TABLES[3]
        lines = [f"planning_progress: table={table}; phase=plan; status=running; completed=4; total=20",
                 f"planning_progress: table={table}; function=build_structural_partitions; phase=build_structure; status=completed; completed=2; total=2; minute_rows=100",
                 f"planning_progress: table={table}; function=build_structural_partitions; phase=build_structure; status=started; completed=0; total=2; upstream_rows=5"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with fixture.isolated_worker(root) as (history, _):
                script = fixture.make_script(root, "events.py", "\n".join(f"print({line!r})" for line in lines))
                run_root = fixture.monitored_run_root(history, "calendars")
                self.assertEqual(console.worker.run_batch(operation_name="fixture", run_root=run_root,
                    stages=(console.worker.StageSpec(NAMES[3], script),), preflight_stages=()), 0)
                scopes = console.worker.read_json_shared(run_root / "status.json")["progress_scopes"]
                build = next(scope for scope in scopes.values() if scope["phase"] == "build_structure")
                self.assertEqual(build["counters"]["frequencies"]["completed"], 0)
                self.assertNotIn("minute_rows", build["object"])
                self.assertEqual(len(scopes), 2)


if __name__ == "__main__":
    unittest.main()
