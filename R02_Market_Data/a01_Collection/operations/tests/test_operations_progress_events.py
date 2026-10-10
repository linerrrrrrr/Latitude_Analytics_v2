"""Offline worker protocol checks; only temporary scripts and run directories."""
from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
import test_background_worker as worker_tests

worker = worker_tests.worker


def event_line(**overrides):
    event = {
        "event_version": 1, "event": "progress", "phase": "collect",
        "scope_id": "batch-work", "parent_scope_id": None, "state": "running",
        "object": {"exchange": "SHFE"},
        "counters": {"variety_days": {"completed": 3, "total": 10, "unit": "品种日"}},
    }
    event.update(overrides)
    return "progress_event: " + json.dumps(event, ensure_ascii=False)


class ProgressProtocolTests(unittest.TestCase):
    def setUp(self):
        self.fixture = worker_tests.BackgroundWorkerTests(methodName="runTest")

    def test_protocol_rejects_guessed_or_invalid_counters_and_versions(self):
        accepted = worker.parse_progress_event(event_line())
        self.assertEqual(accepted["source"], "structured")
        self.assertEqual(accepted["counters"]["variety_days"]["total"], 10)
        self.assertIsNone(worker.parse_progress_event(event_line(counters={
            "pages": {"completed": 1, "total": None, "unit": "页"},
        }))["counters"]["pages"]["total"])
        for bad_line in (
            event_line(event_version=2), event_line(event_version=True),
            event_line(scope_id=""), event_line(parent_scope_id="batch-work"),
            event_line(persisted="true"), event_line(counters={"days": "3/10"}),
            event_line(counters={"days": {"completed": True, "total": 10, "unit": "天"}}),
            event_line(counters={"days": {"completed": 11, "total": 10, "unit": "天"}}),
            event_line(counters={"days": {"completed": 1, "total": float("nan"), "unit": "天"}}),
            event_line(object={"payload": {"nested": "not an identifier"}}),
            event_line(sequence=999),
        ):
            with self.subTest(bad_line=bad_line):
                with self.assertRaises(ValueError):
                    worker.parse_progress_event(bad_line)

    def test_legacy_counters_are_local_and_start_ordinals_are_not_completed(self):
        event = worker.parse_legacy_progress(
            "planning_progress: table=calendar; function=compare; phase=compare; status=running; "
            "partition=('SHFE', 2026, 9); checked_partitions=4/10; case_index=7/9; persisted=true"
        )
        self.assertEqual(event["counter_scope"], "local_step")
        self.assertEqual(event["counters"], {"checked_partitions": {"completed": 4, "total": 10, "unit": "分区"}})
        self.assertEqual(event["object"]["case_index"], "7/9")
        self.assertNotIn("persisted", event)
        for line in (
            "partition_start: 4/10; key=('SHFE', 'CU'); grids=12",
            "request_batch: function=query; phase=query; request=9; dates=2026-01-01..2026-01-02",
        ):
            self.assertEqual(worker.parse_legacy_progress(line)["counters"], {})
        self.assertIsNone(worker.parse_legacy_progress("api_success: grids=4/10; persisted=true"))
        self.assertIsNone(worker.parse_legacy_progress("partition_committed: partitions=4/10; persisted=true"))
        self.assertIsNone(worker.parse_legacy_progress("anything: completed_dates=4/10"))
        event = worker.parse_legacy_progress(
            "fetch_progress: function=query; phase=query; completed_dates=4; total_dates=10"
        )
        self.assertEqual(event["counters"]["completed_dates"]["total"], 10)

    def test_hierarchy_final_summary_json_precedence_and_raw_log_are_preserved(self):
        lines = [
            "planning_progress: function=legacy; phase=plan; checked_dates=1/9",
            event_line(state="started"),
            event_line(event="request", scope_id="request:CU", parent_scope_id="batch-work",
                       phase="request", state="started", object={"contract": "CU2610"}, counters={}),
            event_line(state="started"),
            "planning_progress: function=legacy; phase=plan; checked_dates=8/9",
            event_line(scope_id="request:CU", parent_scope_id="batch-work", phase="request",
                       state="completed", object={}, counters={}),
            event_line(state="completed", counters={"variety_days": {"completed": 10, "total": 10, "unit": "品种日"}}),
        ]
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.fixture.isolated_worker(temporary_root) as (run_history, _):
                script = self.fixture.make_script(temporary_root, "events.py", "\n".join(f"print({line!r})" for line in lines))
                run_root = self.fixture.monitored_run_root(run_history, "structured")
                exit_code = worker.run_batch(operation_name="test_events", run_root=run_root,
                    stages=(worker.StageSpec("events", script),), preflight_stages=())
                self.assertEqual(exit_code, 0)
                status = worker.read_json_shared(run_root / "status.json")
                scopes = status["progress_scopes"]
                self.assertEqual(set(scopes), {"batch-work", "request:CU"})
                self.assertEqual(scopes["batch-work"]["counters"]["variety_days"]["completed"], 10)
                self.assertEqual(scopes["request:CU"]["object"]["contract"], "CU2610")
                self.assertEqual(scopes["request:CU"]["parent_scope_id"], "batch-work")
                self.assertEqual(status["stage_history"][-1]["progress_scopes"], scopes)
                journal = [json.loads(line) for line in (run_root / "progress.jsonl").read_text(encoding="utf-8").splitlines()]
                self.assertEqual([event["sequence"] for event in journal], list(range(1, len(journal) + 1)))
                self.assertEqual(len(journal), 6)
                self.assertEqual(scopes["batch-work"]["started_at"], journal[1]["received_at"])
                self.assertEqual(pathlib.Path(status["log_path"]).read_text(encoding="utf-8").splitlines(), lines)
                self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())

    def test_audited_minute_local_counts_and_request_context(self):
        collected = worker.parse_legacy_progress(
            "planning_progress: table=fact_futures_minute; function=main; phase=collect_batch; status=running; "
            "processed_partitions=2; total_partitions=10; processed_sessions=48; planned_sessions=300"
        )
        self.assertEqual(collected["counters"], {
            "processed_partitions": {"completed": 2, "total": 10, "unit": "分区"},
            "processed_sessions": {"completed": 48, "total": 300, "unit": "Session"},
        })
        committed = worker.parse_legacy_progress(
            "planning_progress: table=dim_futures_bar_calendar; function=commit_calendar_updates; "
            "phase=build_calendar_leaf; status=started; partition=('1m','SHFE',2026,9); completed=2; total=10; persisted=false"
        )
        self.assertEqual(committed["counters"], {
            "committed_calendar_leaves": {"completed": 2, "total": 10, "unit": "日历叶"},
        })
        unknown = worker.parse_legacy_progress(
            "planning_progress: table=unknown; function=unknown; phase=build_calendar_leaf; completed=2; total=10"
        )
        self.assertEqual(unknown["counters"], {})
        request = worker.parse_legacy_progress(
            "request_batch: batch=2/10; contract=CU2610; trading_date=2026-09-28; sessions=2; "
            "start=2026-09-25 21:00; end=2026-09-28 15:00; "
            "table=fact_futures_minute; function=collect_partition; phase=api_request; status=started"
        )
        self.assertEqual(request["object"]["contract"], "CU2610")
        self.assertEqual(request["object"]["start"], "2026-09-25 21:00")
        self.assertEqual(request["counters"], {})

    def test_plan_quantities_are_context_and_do_not_change_scope_identity(self):
        self.assertIn("explicit_plan:", worker.DEFAULT_PROGRESS_PREFIXES)
        minute = worker.parse_legacy_progress(
            "auto_plan: table=fact_futures_minute; function=main; phase=planning; status=completed; "
            "pending_sessions=300; complete_sessions=700; selected_sessions=1000; "
            "pending_partitions=10; expected_pending_rows=12000"
        )
        revised = worker.parse_legacy_progress(
            "explicit_plan: table=fact_futures_minute; function=main; phase=planning; status=completed; "
            "pending_sessions=200; complete_sessions=800; selected_sessions=1000; "
            "pending_partitions=8; expected_pending_rows=9000"
        )
        self.assertEqual(minute["scope_id"], revised["scope_id"])
        self.assertEqual(minute["object"]["pending_sessions"], "300")
        self.assertEqual(revised["object"]["expected_pending_rows"], "9000")
        self.assertEqual(minute["counters"], {})
        self.assertEqual(revised["counters"], {})
        daily = worker.parse_legacy_progress(
            "explicit_plan: table=fact_futures_daily; function=main; phase=planning; status=completed; "
            "planned_grid_count=1000; completed_required_count=500; pending_grid_count=20; request_batch_count=3"
        )
        self.assertEqual(daily["object"]["request_batch_count"], "3")
        self.assertEqual(daily["object"]["completed_required_count"], "500")
        self.assertEqual(daily["counters"], {})
        self.assertNotIn("persisted", daily)

    def test_early_errors_survive_tail_replacement_and_invalid_event_falls_back(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.fixture.isolated_worker(temporary_root) as (run_history, _):
                script = self.fixture.make_script(temporary_root, "errors.py", "\n".join([
                    "import os,sys", "print('ERROR: first notebook mismatch')",
                    "print('ERROR: second notebook mismatch')",
                    "print('progress_event: {not json}')",
                    "print('unbuffered=' + os.environ.get('PYTHONUNBUFFERED', ''))",
                    "for number in range(100): print('OK: ' + str(number))", "sys.exit(1)",
                ]))
                run_root = self.fixture.monitored_run_root(run_history, "errors")
                exit_code = worker.run_batch(operation_name="test_errors", run_root=run_root,
                    stages=(worker.StageSpec("errors", script),), preflight_stages=())
                self.assertEqual(exit_code, 1)
                status = worker.read_json_shared(run_root / "status.json")
                self.assertEqual(status["progress_parse_errors"], 1)
                self.assertEqual(len(status["error_summary"]), 3)
                self.assertNotIn("ERROR: first notebook mismatch", status["recent_lines"])
                self.assertIn("first notebook mismatch", status["error_summary"][0]["message"])
                self.assertEqual(status["stage_history"][-1]["error_summary"], status["error_summary"])
                self.assertIn("unbuffered=1", pathlib.Path(status["log_path"]).read_text(encoding="utf-8"))

    def test_scope_snapshot_is_bounded_but_event_history_and_parent_are_retained(self):
        lines = [event_line(state="started")]
        for number in range(8):
            lines.append(event_line(scope_id=f"request:{number}", parent_scope_id="batch-work",
                                   event="response", phase="request", state="completed", counters={}))
        lines.append(event_line(parent_scope_id="request:7"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.fixture.isolated_worker(temporary_root) as (run_history, _):
                script = self.fixture.make_script(temporary_root, "scopes.py", "\n".join(f"print({line!r})" for line in lines))
                run_root = self.fixture.monitored_run_root(run_history, "scopes")
                with mock.patch.object(worker, "PROGRESS_SCOPE_LIMIT", 4):
                    self.assertEqual(worker.run_batch(operation_name="test_scopes", run_root=run_root,
                        stages=(worker.StageSpec("scopes", script),), preflight_stages=()), 0)
                status = worker.read_json_shared(run_root / "status.json")
                self.assertEqual(len(status["progress_scopes"]), 4)
                self.assertEqual(status["progress_scopes_omitted"], 5)
                self.assertIn("batch-work", status["progress_scopes"])
                self.assertEqual(status["progress_scopes"]["request:7"]["parent_scope_id"], "batch-work")
                self.assertEqual(status["progress_parse_errors"], 1)
                self.assertEqual(len((run_root / "progress.jsonl").read_text(encoding="utf-8").splitlines()), 9)

    def test_explicit_empty_preflight_is_recorded_and_default_checks_remain(self):
        self.assertEqual(worker.DEFAULT_PREFLIGHT_STAGES[-1].arguments, ("--check", "--check-level", "code"))
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.fixture.isolated_worker(temporary_root) as (run_history, order_path):
                script = self.fixture.make_script(temporary_root, "noop.py", "print('maintenance done')")
                for name, optional_arguments in (("maintenance", {"preflight_stages": ()}), ("default", {})):
                    run_root = self.fixture.monitored_run_root(run_history, name)
                    self.assertEqual(worker.run_batch(operation_name=name, run_root=run_root,
                        stages=(worker.StageSpec("noop", script),), **optional_arguments), 0)
                    manifest = worker.read_json_shared(run_root / "command_manifest.json")
                    self.assertEqual(manifest["preflight_total"], 0 if name == "maintenance" else 3)
                self.assertEqual(order_path.read_text(encoding="utf-8").splitlines(), ["1", "2", "3"])
                with self.assertRaises(TypeError):
                    worker.run_batch(operation_name="invalid", run_root=run_history / "invalid",
                        stages=(worker.StageSpec("noop", script),), preflight_stages=[])

    def test_progress_write_failure_stops_child_and_next_stage(self):
        original_open = pathlib.Path.open
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.fixture.isolated_worker(temporary_root) as (run_history, _):
                script = self.fixture.make_script(temporary_root, "running.py", "\n".join([
                    "import os,time", "print('child_pid=' + str(os.getpid()), flush=True)",
                    f"print({event_line()!r}, flush=True)", "time.sleep(30)",
                ]))
                marker_path = temporary_root / "forbidden.txt"
                forbidden = self.fixture.make_script(temporary_root, "forbidden.py",
                    f"import pathlib; pathlib.Path({str(marker_path)!r}).write_text('ran')")
                run_root = self.fixture.monitored_run_root(run_history, "disk_failure")

                def failing_progress_open(path, *args, **kwargs):
                    handle = original_open(path, *args, **kwargs)
                    if path.name == "progress.jsonl":
                        wrapper = mock.MagicMock(wraps=handle)
                        wrapper.__enter__.return_value = wrapper
                        wrapper.__exit__.side_effect = lambda *ignored: handle.close()
                        wrapper.write.side_effect = OSError("simulated event disk failure")
                        return wrapper
                    return handle

                with mock.patch.object(pathlib.Path, "open", failing_progress_open):
                    exit_code = worker.run_batch(operation_name="test_failure", run_root=run_root,
                        stages=(worker.StageSpec("running", script), worker.StageSpec("forbidden", forbidden)),
                        preflight_stages=())
                self.assertEqual(exit_code, 1)
                status = worker.read_json_shared(run_root / "status.json")
                self.assertIn("simulated event disk failure", status["error"])
                self.assertFalse(marker_path.exists())
                log_text = pathlib.Path(status["log_path"]).read_text(encoding="utf-8")
                child_pid = int(log_text.splitlines()[0].split("=", 1)[1])
                self.assertFalse(worker.process_is_alive(child_pid))

    def test_output_flood_cannot_starve_interruption(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.fixture.isolated_worker(temporary_root) as (run_history, _):
                script = self.fixture.make_script(temporary_root, "flood.py", "while True: print('ordinary output ' * 20)")
                run_root = self.fixture.monitored_run_root(run_history, "flood")
                timer = threading.Timer(0.3, lambda: (run_root / "interrupt.request").write_text("stop", encoding="utf-8"))
                timer.start()
                started_at = time.monotonic()
                try:
                    exit_code = worker.run_batch(operation_name="test_flood", run_root=run_root,
                        stages=(worker.StageSpec("flood", script),), preflight_stages=())
                finally:
                    timer.cancel()
                self.assertEqual(exit_code, 130)
                self.assertLess(time.monotonic() - started_at, 4)


if __name__ == "__main__":
    unittest.main()
