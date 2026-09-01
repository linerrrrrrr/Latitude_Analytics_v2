from __future__ import annotations

import dataclasses
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import ExitStack, contextmanager
from unittest import mock

import psutil


OPERATIONS_ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPERATIONS_ROOT))

import background_worker as worker  # noqa: E402


class BackgroundWorkerTests(unittest.TestCase):
    def make_script(
        self,
        directory: pathlib.Path,
        name: str,
        source: str,
    ) -> pathlib.Path:
        script_path = directory / name
        script_path.write_text(source, encoding="utf-8")
        return script_path

    @contextmanager
    def isolated_worker(self, temporary_root: pathlib.Path):
        run_history_root = temporary_root / "run_history"
        run_history_root.mkdir()
        preflight_order_path = temporary_root / "preflight_order.txt"
        preflight_script = self.make_script(
            temporary_root,
            "preflight.py",
            """
import pathlib
import sys

with pathlib.Path(sys.argv[1]).open("a", encoding="utf-8") as handle:
    handle.write(sys.argv[2] + "\\n")
""".lstrip(),
        )
        preflight_stages = tuple(
            worker.StageSpec(
                f"preflight/{index}",
                preflight_script,
                (str(preflight_order_path), str(index)),
            )
            for index in range(1, 4)
        )
        with ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(worker, "RUN_HISTORY_ROOT", run_history_root)
            )
            stack.enter_context(
                mock.patch.object(
                    worker,
                    "GLOBAL_LOCK_PATH",
                    run_history_root / ".active_formal_run",
                )
            )
            stack.enter_context(
                mock.patch.object(
                    worker,
                    "DEFAULT_PREFLIGHT_STAGES",
                    preflight_stages,
                )
            )
            stack.enter_context(
                mock.patch.object(worker, "HEARTBEAT_SECONDS", 0.02)
            )
            stack.enter_context(
                mock.patch.object(worker, "MONITOR_START_TIMEOUT_SECONDS", 0.2)
            )
            stack.enter_context(
                mock.patch.object(worker, "TERMINATION_TIMEOUT_SECONDS", 1.0)
            )
            yield run_history_root, preflight_order_path

    def monitored_run_root(
        self,
        run_history_root: pathlib.Path,
        name: str,
    ) -> pathlib.Path:
        run_root = run_history_root / name
        run_root.mkdir()
        (run_root / "monitor.pid").write_text(
            f"{os.getpid()}\n",
            encoding="utf-8",
        )
        return run_root

    def test_stage_spec_is_frozen_and_requires_immutable_arguments(self) -> None:
        stage = worker.StageSpec("stage", worker.__file__ and pathlib.Path(worker.__file__))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            stage.name = "changed"  # type: ignore[misc]
        with self.assertRaises(TypeError):
            worker.StageSpec(
                "stage",
                pathlib.Path(worker.__file__),
                ["--write"],  # type: ignore[arg-type]
            )

    def test_fixed_preflight_order_and_general_c08_rejection(self) -> None:
        self.assertEqual(
            [stage.name for stage in worker.DEFAULT_PREFLIGHT_STAGES],
            [
                "preflight/01_lakehouse_runtime",
                "preflight/02_operations_runtime",
                "preflight/03_notebook_export_sync",
            ],
        )
        self.assertEqual(
            worker.DEFAULT_PREFLIGHT_STAGES[0].entrypoint_path,
            worker.PROJECT_ROOT / "02_Futures_Lakehouse" / "verify_runtime.py",
        )
        self.assertEqual(
            worker.DEFAULT_PREFLIGHT_STAGES[1].entrypoint_path,
            worker.OPERATIONS_ROOT / "verify_operations_runtime.py",
        )
        self.assertEqual(
            worker.DEFAULT_PREFLIGHT_STAGES[2].arguments,
            ("--check",),
        )
        self.assertEqual(
            worker.DEFAULT_PREFLIGHT_STAGES[2].entrypoint_path,
            worker.COLLECTION_ROOT / "b00_sync_notebook_exports.py",
        )
        forbidden_stage = worker.StageSpec(
            "quality/C08-audit",
            pathlib.Path(worker.__file__),
        )
        with self.assertRaisesRegex(ValueError, "c08"):
            worker.validate_stage_specs((forbidden_stage,), "业务")

        hidden_in_arguments = worker.StageSpec(
            "apparently-safe",
            pathlib.Path(worker.__file__),
            ("--entrypoint-path", "somewhere/c08.py"),
        )
        with self.assertRaisesRegex(ValueError, "c08"):
            worker.validate_stage_specs((hidden_in_arguments,), "业务")

    def test_c08_rejection_happens_before_lock_or_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(run_history, "c08_rejected")
                forbidden_stage = worker.StageSpec(
                    "b01/c08_full_quality",
                    pathlib.Path(worker.__file__),
                )
                with self.assertRaisesRegex(ValueError, "c08"):
                    worker.run_batch(
                        operation_name="unit_c08_rejected",
                        run_root=run_root,
                        stages=(forbidden_stage,),
                    )
                self.assertFalse((run_root / "command_manifest.json").exists())
                self.assertFalse((run_root / "status.json").exists())
                self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())

    def test_run_root_rejects_legacy_and_lock_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            with self.isolated_worker(temporary_root) as (run_history, _):
                with self.assertRaises(ValueError):
                    worker.validate_run_root(run_history / "legacy_imports" / "x")
                with self.assertRaises(ValueError):
                    worker.validate_run_root(
                        run_history / ".active_formal_run" / "x"
                    )
                with self.assertRaises(ValueError):
                    worker.validate_run_root(run_history)

    def test_atomic_json_retries_only_replace_sharing_error(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            status_path = pathlib.Path(temporary_directory) / "status.json"
            real_replace = worker.os.replace
            calls = 0
            sharing_error = PermissionError("sharing violation")
            sharing_error.winerror = 5  # type: ignore[attr-defined]

            def replace_once_locked(source: object, destination: object) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise sharing_error
                real_replace(source, destination)

            with mock.patch.object(
                worker.os,
                "replace",
                side_effect=replace_once_locked,
            ), mock.patch.object(worker, "STATUS_REPLACE_RETRY_SECONDS", 0):
                worker.atomic_write_json(status_path, {"state": "complete"})
            self.assertEqual(calls, 2)
            self.assertEqual(
                json.loads(status_path.read_text(encoding="utf-8")),
                {"state": "complete"},
            )

    def test_success_runs_three_preflights_then_business_and_releases_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            business_order_path = temporary_root / "business_order.txt"
            business_script = self.make_script(
                temporary_root,
                "success.py",
                """
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text("business\\n", encoding="utf-8")
print("planning_progress: complete", flush=True)
""".lstrip(),
            )
            with self.isolated_worker(temporary_root) as (
                run_history,
                preflight_order,
            ):
                run_root = self.monitored_run_root(run_history, "success")
                exit_code = worker.run_batch(
                    operation_name="unit_success",
                    run_root=run_root,
                    stages=(
                        worker.StageSpec(
                            "business/success",
                            business_script,
                            (str(business_order_path),),
                        ),
                    ),
                )

                self.assertEqual(exit_code, 0)
                self.assertEqual(
                    preflight_order.read_text(encoding="utf-8").splitlines(),
                    ["1", "2", "3"],
                )
                self.assertEqual(
                    business_order_path.read_text(encoding="utf-8"),
                    "business\n",
                )
                status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(status["protocol_version"], 1)
                self.assertEqual(status["operation_name"], "unit_success")
                self.assertEqual(status["mode"], "formal")
                self.assertEqual(status["phase"], "terminal")
                self.assertEqual(status["state"], "succeeded")
                self.assertEqual(status["exit_code"], 0)
                self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())
                manifest = json.loads(
                    (run_root / "command_manifest.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(manifest["mode"], "formal")
                self.assertEqual(manifest["preflight_total"], 3)
                self.assertEqual(
                    len(list((run_root / "logs").glob("preflight_*.log"))),
                    3,
                )
                self.assertEqual(
                    len(list((run_root / "logs").glob("business_*.log"))),
                    1,
                )

    def test_failure_is_single_attempt_and_recursively_stops_descendant(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            child_pid_path = temporary_root / "child.pid"
            attempt_path = temporary_root / "attempts.txt"
            sentinel_path = temporary_root / "must_not_run.txt"
            failing_script = self.make_script(
                temporary_root,
                "spawn_then_fail.py",
                """
import pathlib
import subprocess
import sys
import time

attempt_path = pathlib.Path(sys.argv[1])
with attempt_path.open("a", encoding="utf-8") as handle:
    handle.write("attempt\\n")
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
pathlib.Path(sys.argv[2]).write_text(str(child.pid), encoding="utf-8")
raise SystemExit(7)
""".lstrip(),
            )
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(run_history, "failure")
                sentinel_script = self.make_script(
                    temporary_root,
                    "sentinel.py",
                    """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
                )
                exit_code = worker.run_batch(
                    operation_name="unit_failure",
                    run_root=run_root,
                    stages=(
                        worker.StageSpec(
                            "business/failure",
                            failing_script,
                            (str(attempt_path), str(child_pid_path)),
                        ),
                        worker.StageSpec(
                            "business/must_not_run",
                            sentinel_script,
                            (str(sentinel_path),),
                        ),
                    ),
                )
                self.assertEqual(exit_code, 1)
                self.assertEqual(
                    attempt_path.read_text(encoding="utf-8").splitlines(),
                    ["attempt"],
                )
                child_pid = int(child_pid_path.read_text(encoding="utf-8"))
                deadline = time.monotonic() + 3
                while worker.process_is_alive(child_pid) and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertFalse(worker.process_is_alive(child_pid))
                self.assertFalse(sentinel_path.exists())
                self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())
                status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(status["state"], "failed")
                self.assertEqual(status["exit_code"], 1)

    def test_quota_and_interruption_have_frozen_exit_codes(self) -> None:
        cases = (
            ("quota.py", 'print("quota_stop: test", flush=True)\n', "quota_stopped", 3),
            ("interrupt.py", "raise SystemExit(130)\n", "interrupted", 130),
        )
        for script_name, source, expected_state, expected_code in cases:
            with self.subTest(expected_state=expected_state):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    temporary_root = pathlib.Path(temporary_directory)
                    stage_script = self.make_script(
                        temporary_root,
                        script_name,
                        source,
                    )
                    with self.isolated_worker(temporary_root) as (run_history, _):
                        sentinel_path = temporary_root / "must_not_run.txt"
                        sentinel_script = self.make_script(
                            temporary_root,
                            "sentinel.py",
                            """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
                        )
                        run_root = self.monitored_run_root(
                            run_history,
                            expected_state,
                        )
                        exit_code = worker.run_batch(
                            operation_name=f"unit_{expected_state}",
                            run_root=run_root,
                            stages=(
                                worker.StageSpec(
                                    f"business/{expected_state}",
                                    stage_script,
                                ),
                                worker.StageSpec(
                                    "business/must_not_run",
                                    sentinel_script,
                                    (str(sentinel_path),),
                                ),
                            ),
                        )
                        self.assertEqual(exit_code, expected_code)
                        status = json.loads(
                            (run_root / "status.json").read_text(
                                encoding="utf-8"
                            )
                        )
                        self.assertEqual(status["state"], expected_state)
                        self.assertEqual(status["exit_code"], expected_code)
                        self.assertFalse(sentinel_path.exists())
                        self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())

    def test_monitor_exit_during_stage_stops_tree_and_later_stage(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            direct_pid_path = temporary_root / "direct.pid"
            descendant_pid_path = temporary_root / "descendant.pid"
            sentinel_path = temporary_root / "must_not_run.txt"
            long_stage = self.make_script(
                temporary_root,
                "long_stage.py",
                """
import os
import pathlib
import subprocess
import sys
import time

pathlib.Path(sys.argv[1]).write_text(str(os.getpid()), encoding="utf-8")
descendant = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
pathlib.Path(sys.argv[2]).write_text(str(descendant.pid), encoding="utf-8")
time.sleep(60)
""".lstrip(),
            )
            sentinel_stage = self.make_script(
                temporary_root,
                "sentinel.py",
                """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
            )
            monitor_process = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    (
                        "import pathlib,sys,time; "
                        "p=pathlib.Path(sys.argv[1]); "
                        "deadline=time.monotonic()+15; "
                        "\nwhile not p.exists() and time.monotonic()<deadline: "
                        "time.sleep(0.02)\n"
                        "time.sleep(0.2)"
                    ),
                    str(descendant_pid_path),
                ],
            )
            try:
                with self.isolated_worker(temporary_root) as (run_history, _):
                    run_root = run_history / "monitor_exit"
                    run_root.mkdir()
                    (run_root / "monitor.pid").write_text(
                        f"{monitor_process.pid}\n",
                        encoding="utf-8",
                    )
                    exit_code = worker.run_batch(
                        operation_name="unit_monitor_exit",
                        run_root=run_root,
                        stages=(
                            worker.StageSpec(
                                "business/long",
                                long_stage,
                                (str(direct_pid_path), str(descendant_pid_path)),
                            ),
                            worker.StageSpec(
                                "business/must_not_run",
                                sentinel_stage,
                                (str(sentinel_path),),
                            ),
                        ),
                    )
                    self.assertEqual(exit_code, 1)
                    self.assertFalse(sentinel_path.exists())
                    self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())
                    for pid_path in (direct_pid_path, descendant_pid_path):
                        pid = int(pid_path.read_text(encoding="utf-8"))
                        self.assertFalse(worker.process_is_alive(pid))
                    status = json.loads(
                        (run_root / "status.json").read_text(encoding="utf-8")
                    )
                    self.assertEqual(status["state"], "failed")
                    self.assertIn("monitor_unavailable", status["error"])
            finally:
                if monitor_process.poll() is None:
                    monitor_process.kill()
                monitor_process.wait(timeout=5)

    def test_monitor_exit_with_fast_final_stage_cannot_publish_success(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            exit_signal_path = temporary_root / "exit_monitor.signal"
            sentinel_path = temporary_root / "must_not_run.txt"
            fast_stage = self.make_script(
                temporary_root,
                "fast_stage.py",
                """
import pathlib
import psutil
import sys
import time

time.sleep(0.2)
pathlib.Path(sys.argv[1]).write_text("exit", encoding="utf-8")
monitor_pid = int(sys.argv[2])
deadline = time.monotonic() + 5
while time.monotonic() < deadline:
    try:
        monitor = psutil.Process(monitor_pid)
        if not monitor.is_running() or monitor.status() == psutil.STATUS_ZOMBIE:
            break
    except psutil.Error:
        break
    time.sleep(0.01)
""".lstrip(),
            )
            sentinel_stage = self.make_script(
                temporary_root,
                "sentinel.py",
                """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
            )
            monitor_process = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    (
                        "import pathlib,sys,time; "
                        "p=pathlib.Path(sys.argv[1]); "
                        "deadline=time.monotonic()+15; "
                        "\nwhile not p.exists() and time.monotonic()<deadline: "
                        "time.sleep(0.01)"
                    ),
                    str(exit_signal_path),
                ],
            )
            try:
                with self.isolated_worker(temporary_root) as (run_history, _), mock.patch.object(
                    worker,
                    "HEARTBEAT_SECONDS",
                    60.0,
                ):
                    run_root = run_history / "fast_monitor_exit"
                    run_root.mkdir()
                    (run_root / "monitor.pid").write_text(
                        f"{monitor_process.pid}\n",
                        encoding="utf-8",
                    )
                    exit_code = worker.run_batch(
                        operation_name="unit_fast_monitor_exit",
                        run_root=run_root,
                        stages=(
                            worker.StageSpec(
                                "business/fast",
                                fast_stage,
                                (str(exit_signal_path), str(monitor_process.pid)),
                            ),
                            worker.StageSpec(
                                "business/must_not_run",
                                sentinel_stage,
                                (str(sentinel_path),),
                            ),
                        ),
                    )
                    self.assertEqual(exit_code, 1)
                    self.assertFalse(sentinel_path.exists())
                    status = json.loads(
                        (run_root / "status.json").read_text(encoding="utf-8")
                    )
                    self.assertEqual(status["state"], "failed")
                    self.assertIn("monitor_unavailable", status["error"])
                    self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())
            finally:
                if monitor_process.poll() is None:
                    monitor_process.kill()
                monitor_process.wait(timeout=5)

    def test_manual_interrupt_request_stops_descendants_and_returns_130(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            direct_pid_path = temporary_root / "direct.pid"
            descendant_pid_path = temporary_root / "descendant.pid"
            sentinel_path = temporary_root / "must_not_run.txt"
            long_stage = self.make_script(
                temporary_root,
                "interruptible_stage.py",
                """
import os
import pathlib
import subprocess
import sys
import time

pathlib.Path(sys.argv[1]).write_text(str(os.getpid()), encoding="utf-8")
descendant = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
pathlib.Path(sys.argv[2]).write_text(str(descendant.pid), encoding="utf-8")
time.sleep(60)
""".lstrip(),
            )
            sentinel_stage = self.make_script(
                temporary_root,
                "sentinel.py",
                """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
            )
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(run_history, "manual_interrupt")
                interrupt_path = run_root / "interrupt.request"

                def request_interruption() -> None:
                    deadline = time.monotonic() + 15
                    while (
                        not descendant_pid_path.exists()
                        and time.monotonic() < deadline
                    ):
                        time.sleep(0.02)
                    if descendant_pid_path.exists():
                        interrupt_path.write_text(
                            "requested_by=unit_test\n",
                            encoding="utf-8",
                        )

                requester = threading.Thread(
                    target=request_interruption,
                    daemon=True,
                )
                requester.start()
                exit_code = worker.run_batch(
                    operation_name="unit_manual_interrupt",
                    run_root=run_root,
                    stages=(
                        worker.StageSpec(
                            "business/interruptible",
                            long_stage,
                            (str(direct_pid_path), str(descendant_pid_path)),
                        ),
                        worker.StageSpec(
                            "business/must_not_run",
                            sentinel_stage,
                            (str(sentinel_path),),
                        ),
                    ),
                )
                requester.join(timeout=5)
                self.assertEqual(exit_code, 130)
                self.assertEqual(
                    interrupt_path.read_text(encoding="utf-8"),
                    "requested_by=unit_test\n",
                )
                self.assertFalse(sentinel_path.exists())
                for pid_path in (direct_pid_path, descendant_pid_path):
                    pid = int(pid_path.read_text(encoding="utf-8"))
                    self.assertFalse(worker.process_is_alive(pid))
                status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(status["state"], "interrupted")
                self.assertEqual(status["phase"], "terminal")
                self.assertEqual(status["exit_code"], 130)
                self.assertIn("interrupt.request", status["error"])
                self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())

    def test_heartbeat_status_publish_failure_stops_child_and_retains_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            long_stage = self.make_script(
                temporary_root,
                "long_stage.py",
                """
import time

time.sleep(60)
""".lstrip(),
            )
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(run_history, "status_failure")
                real_atomic_write_json = worker.atomic_write_json
                running_child_publications = 0
                injected_failure = False
                direct_child_pid: int | None = None

                def fail_status_after_child_started(
                    path: pathlib.Path,
                    payload: dict[str, object],
                ) -> None:
                    nonlocal running_child_publications, injected_failure
                    nonlocal direct_child_pid
                    if path.name == "status.json" and injected_failure:
                        raise OSError("injected persistent status failure")
                    if (
                        path.name == "status.json"
                        and payload.get("phase") == "business"
                        and payload.get("child_pid") is not None
                    ):
                        running_child_publications += 1
                        direct_child_pid = int(payload["child_pid"])
                        if running_child_publications >= 2:
                            injected_failure = True
                            raise OSError("injected persistent status failure")
                    real_atomic_write_json(path, payload)

                with mock.patch.object(
                    worker,
                    "atomic_write_json",
                    side_effect=fail_status_after_child_started,
                ):
                    exit_code = worker.run_batch(
                        operation_name="unit_status_failure",
                        run_root=run_root,
                        stages=(
                            worker.StageSpec(
                                "business/long",
                                long_stage,
                            ),
                        ),
                    )
                self.assertEqual(exit_code, 1)
                self.assertTrue(injected_failure)
                self.assertIsNotNone(direct_child_pid)
                self.assertFalse(worker.process_is_alive(direct_child_pid))
                self.assertTrue(worker.GLOBAL_LOCK_PATH.is_dir())

    def test_monitor_gate_is_mandatory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            business_script = self.make_script(
                temporary_root,
                "must_not_run.py",
                "raise RuntimeError('must not run')\n",
            )
            with self.isolated_worker(temporary_root) as (run_history, _), mock.patch.object(
                worker,
                "MONITOR_START_TIMEOUT_SECONDS",
                0,
            ):
                run_root = run_history / "no_monitor"
                exit_code = worker.run_batch(
                    operation_name="unit_no_monitor",
                    run_root=run_root,
                    stages=(
                        worker.StageSpec("business/blocked", business_script),
                    ),
                )
                self.assertEqual(exit_code, 1)
                status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(status["state"], "failed")
                self.assertEqual(status["phase"], "terminal")
                self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())

    def test_missing_entrypoint_is_visible_before_any_child_runs(self) -> None:
        cases = ("preflight", "business")
        for missing_context in cases:
            with self.subTest(missing_context=missing_context):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    temporary_root = pathlib.Path(temporary_directory)
                    sentinel_path = temporary_root / "must_not_run.txt"
                    sentinel_script = self.make_script(
                        temporary_root,
                        "sentinel.py",
                        """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
                    )
                    missing_script = temporary_root / "missing.py"
                    with self.isolated_worker(temporary_root) as (run_history, _):
                        run_root = self.monitored_run_root(
                            run_history,
                            f"missing_{missing_context}",
                        )
                        business_stages = (
                            worker.StageSpec(
                                "business/must_not_run",
                                sentinel_script,
                                (str(sentinel_path),),
                            ),
                        )
                        nested_patch = ExitStack()
                        if missing_context == "preflight":
                            nested_patch.enter_context(
                                mock.patch.object(
                                    worker,
                                    "DEFAULT_PREFLIGHT_STAGES",
                                    (
                                        worker.StageSpec(
                                            "preflight/missing",
                                            missing_script,
                                        ),
                                    ),
                                )
                            )
                        else:
                            business_stages = (
                                *business_stages,
                                worker.StageSpec(
                                    "business/missing",
                                    missing_script,
                                ),
                            )
                        with nested_patch:
                            exit_code = worker.run_batch(
                                operation_name=f"unit_missing_{missing_context}",
                                run_root=run_root,
                                stages=business_stages,
                            )
                        self.assertEqual(exit_code, 1)
                        self.assertFalse(sentinel_path.exists())
                        status = json.loads(
                            (run_root / "status.json").read_text(
                                encoding="utf-8"
                            )
                        )
                        self.assertEqual(status["state"], "failed")
                        self.assertEqual(status["phase"], "terminal")
                        self.assertIn("FileNotFoundError", status["error"])
                        self.assertIn("入口不存在", status["error"])
                        self.assertTrue((run_root / "failure.json").is_file())
                        self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())

    def test_owner_publish_failure_is_visible_and_retains_new_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            sentinel_path = temporary_root / "must_not_run.txt"
            sentinel_script = self.make_script(
                temporary_root,
                "sentinel.py",
                """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
            )
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(
                    run_history,
                    "owner_publish_failure",
                )
                real_atomic_write_json = worker.atomic_write_json

                def fail_owner_publish(
                    path: pathlib.Path,
                    payload: dict[str, object],
                ) -> None:
                    if path == worker.GLOBAL_LOCK_PATH / "owner.json":
                        raise OSError("injected owner publish failure")
                    real_atomic_write_json(path, payload)

                with mock.patch.object(
                    worker,
                    "atomic_write_json",
                    side_effect=fail_owner_publish,
                ):
                    exit_code = worker.run_batch(
                        operation_name="unit_owner_publish_failure",
                        run_root=run_root,
                        stages=(
                            worker.StageSpec(
                                "business/must_not_run",
                                sentinel_script,
                                (str(sentinel_path),),
                            ),
                        ),
                    )
                self.assertEqual(exit_code, 1)
                self.assertFalse(sentinel_path.exists())
                self.assertTrue(worker.GLOBAL_LOCK_PATH.is_dir())
                self.assertFalse(
                    (worker.GLOBAL_LOCK_PATH / "owner.json").exists()
                )
                status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(status["state"], "failed")
                self.assertEqual(status["phase"], "terminal")
                self.assertIn("owner publish failure", status["error"])

    def test_invalid_progress_protocol_is_visible_before_preflight(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            sentinel_path = temporary_root / "must_not_run.txt"
            sentinel_script = self.make_script(
                temporary_root,
                "sentinel.py",
                """
import pathlib
import sys
pathlib.Path(sys.argv[1]).write_text("ran", encoding="utf-8")
""".lstrip(),
            )
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(
                    run_history,
                    "invalid_progress",
                )
                exit_code = worker.run_batch(
                    operation_name="unit_invalid_progress",
                    run_root=run_root,
                    stages=(
                        worker.StageSpec(
                            "business/must_not_run",
                            sentinel_script,
                            (str(sentinel_path),),
                        ),
                    ),
                    progress_prefixes=(),
                )
                self.assertEqual(exit_code, 1)
                self.assertFalse(sentinel_path.exists())
                status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(status["state"], "failed")
                self.assertIn("progress_prefixes", status["error"])
                self.assertFalse(worker.GLOBAL_LOCK_PATH.exists())

    def test_lock_collision_and_terminal_publish_failure_retain_lock(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            business_script = self.make_script(
                temporary_root,
                "success.py",
                "print('ok', flush=True)\n",
            )
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(run_history, "collision")
                worker.GLOBAL_LOCK_PATH.mkdir()
                owner_path = worker.GLOBAL_LOCK_PATH / "owner.json"
                owner_bytes = b'{"operation_name":"other","sentinel":"unchanged"}'
                owner_path.write_bytes(owner_bytes)
                owner_mtime_ns = owner_path.stat().st_mtime_ns
                lock_mtime_ns = worker.GLOBAL_LOCK_PATH.stat().st_mtime_ns
                exit_code = worker.run_batch(
                    operation_name="unit_collision",
                    run_root=run_root,
                    stages=(worker.StageSpec("business/ok", business_script),),
                )
                self.assertEqual(exit_code, 1)
                self.assertTrue(worker.GLOBAL_LOCK_PATH.is_dir())
                self.assertEqual(owner_path.read_bytes(), owner_bytes)
                self.assertEqual(owner_path.stat().st_mtime_ns, owner_mtime_ns)
                self.assertEqual(
                    worker.GLOBAL_LOCK_PATH.stat().st_mtime_ns,
                    lock_mtime_ns,
                )
                self.assertEqual(
                    [path.name for path in worker.GLOBAL_LOCK_PATH.iterdir()],
                    ["owner.json"],
                )
                collision_status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(collision_status["state"], "failed")
                self.assertEqual(collision_status["phase"], "terminal")
                self.assertEqual(collision_status["exit_code"], 1)
                self.assertIsNone(collision_status["child_pid"])
                self.assertIn("active_formal_run", collision_status["error"])
                self.assertIn("人工核查", collision_status["error"])
                collision_failure = json.loads(
                    (run_root / "failure.json").read_text(encoding="utf-8")
                )
                self.assertEqual(collision_failure["state"], "failed")
                self.assertEqual(collision_failure["exit_code"], 1)

            second_root = temporary_root / "second"
            second_root.mkdir()
            with self.isolated_worker(second_root) as (run_history, _):
                run_root = self.monitored_run_root(run_history, "publish_failure")
                real_atomic_write_json = worker.atomic_write_json

                def fail_terminal_status(
                    path: pathlib.Path,
                    payload: dict[str, object],
                ) -> None:
                    if (
                        path.name == "status.json"
                        and payload.get("phase") == "terminal"
                    ):
                        raise OSError("injected terminal publish failure")
                    real_atomic_write_json(path, payload)

                with mock.patch.object(
                    worker,
                    "atomic_write_json",
                    side_effect=fail_terminal_status,
                ):
                    exit_code = worker.run_batch(
                        operation_name="unit_publish_failure",
                        run_root=run_root,
                        stages=(
                            worker.StageSpec("business/ok", business_script),
                        ),
                    )
                self.assertEqual(exit_code, 1)
                self.assertTrue(worker.GLOBAL_LOCK_PATH.is_dir())
                owner = json.loads(
                    (worker.GLOBAL_LOCK_PATH / "owner.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(owner["operation_name"], "unit_publish_failure")

    def test_owner_mismatch_on_release_downgrades_success_to_failed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_root = pathlib.Path(temporary_directory)
            mutate_owner_script = self.make_script(
                temporary_root,
                "mutate_owner.py",
                """
import json
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(
    json.dumps({"worker_pid": -1, "operation_name": "different"}),
    encoding="utf-8",
)
""".lstrip(),
            )
            with self.isolated_worker(temporary_root) as (run_history, _):
                run_root = self.monitored_run_root(
                    run_history,
                    "release_owner_mismatch",
                )
                exit_code = worker.run_batch(
                    operation_name="unit_release_owner_mismatch",
                    run_root=run_root,
                    stages=(
                        worker.StageSpec(
                            "business/mutate_owner",
                            mutate_owner_script,
                            (str(worker.GLOBAL_LOCK_PATH / "owner.json"),),
                        ),
                    ),
                )
                self.assertEqual(exit_code, 1)
                self.assertTrue(worker.GLOBAL_LOCK_PATH.is_dir())
                control_failure = json.loads(
                    (run_root / "control_failure.json").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertEqual(
                    control_failure["original_terminal_state"],
                    "succeeded",
                )
                self.assertEqual(control_failure["original_exit_code"], 0)
                self.assertEqual(control_failure["owner"]["worker_pid"], -1)
                self.assertIn("owner 已变化", control_failure["error"])
                status = json.loads(
                    (run_root / "status.json").read_text(encoding="utf-8")
                )
                self.assertEqual(status["state"], "failed")
                self.assertEqual(status["exit_code"], 1)
                self.assertIn("global_lock_release_failed", status["error"])
                self.assertTrue((run_root / "failure.json").is_file())


if __name__ == "__main__":
    unittest.main()
