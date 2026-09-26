from __future__ import annotations

import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_ROOT = PROJECT_ROOT / "00_draft_collection_02" / "scripts"
RUN_STATUS_ROOT = PROJECT_ROOT / "00_draft_collection_02" / "run_status"
WORKER_PATH = SCRIPT_ROOT / "run_b01_b04_full_update.py"
MONITOR_PATH = SCRIPT_ROOT / "watch_b01_b04_full_update.ps1"


def load_worker_module():
    specification = importlib.util.spec_from_file_location(
        "run_b01_b04_full_update",
        WORKER_PATH,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("无法加载 b01-b04 全链路 worker。")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


class BackgroundWorkerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        RUN_STATUS_ROOT.mkdir(parents=True, exist_ok=True)
        cls.worker = load_worker_module()

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(dir=RUN_STATUS_ROOT)
        self.temporary_root = pathlib.Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_stage(self, name: str, body: str) -> pathlib.Path:
        path = self.temporary_root / f"{name}.py"
        path.write_text(body, encoding="utf-8")
        return path

    def stage_tuple(self, paths: list[pathlib.Path]):
        return tuple(
            (f"stage_{index}", path, True, ())
            for index, path in enumerate(paths, 1)
        )

    def run_sample(self, stage_paths: list[pathlib.Path], run_name: str = "run") -> tuple[int, pathlib.Path]:
        run_root = self.temporary_root / run_name
        with mock.patch.object(self.worker, "STAGES", self.stage_tuple(stage_paths)), mock.patch.object(
            self.worker,
            "HEARTBEAT_SECONDS",
            0.05,
        ):
            result = self.worker.run_job(
                "sample",
                run_root,
                "2026-07-21",
                "2026-08-20",
            )
        return result, run_root

    def test_success_runs_all_eighteen_stages_and_updates_heartbeat(self) -> None:
        scripts = [
            self.write_stage(
                f"success_{index}",
                "import time\n"
                "time.sleep(0.08)\n"
                f"print('planning_progress: stage={index}', flush=True)\n",
            )
            for index in range(1, 19)
        ]

        result, run_root = self.run_sample(scripts)

        self.assertEqual(result, 0)
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["state"], "succeeded")
        self.assertEqual(status["stage_index"], 18)
        self.assertGreater(status["heartbeat_at"], status["started_at"])
        manifest = json.loads(
            (run_root / "command_manifest.json").read_text(encoding="utf-8")
        )
        for stage in manifest["stages"]:
            command = stage["command"]
            self.assertIn("--lake-root", command)
            self.assertIn("--start-date", command)
            self.assertIn("--end-date", command)
            self.assertEqual(command[-1], "--write")

    def test_real_stage_manifest_covers_b01_b04_without_manual_c08(self) -> None:
        sample_commands = self.worker.build_stage_commands(
            "sample",
            self.temporary_root,
            "2026-07-21",
            "2026-08-20",
        )
        formal_commands = self.worker.build_stage_commands(
            "formal",
            self.temporary_root,
            None,
            None,
        )

        self.assertEqual(len(sample_commands), 18)
        self.assertEqual(len(formal_commands), 18)
        self.assertEqual(sample_commands[0]["name"], "a01/b01_trade_calendar")
        self.assertEqual(formal_commands[-1]["name"], "a04/b03_macro_release")
        self.assertEqual(
            formal_commands[8]["name"],
            "a02/b01a_position_rank_special_case_calibration",
        )
        self.assertNotIn(
            "a01/b08_full_minute_quality",
            {stage["name"] for stage in sample_commands},
        )

        for stage in formal_commands:
            self.assertNotIn("--start-date", stage["command"])
            self.assertNotIn("--end-date", stage["command"])
            self.assertEqual(stage["command"][-1], "--write")

    def test_status_replace_retries_only_transient_windows_file_lock(self) -> None:
        target = self.temporary_root / "status.json"
        real_replace = self.worker.os.replace
        transient = PermissionError(13, "locked")
        transient.winerror = 5
        calls = 0

        def replace_once_locked(source, destination):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise transient
            return real_replace(source, destination)

        with mock.patch.object(
            self.worker.os,
            "replace",
            side_effect=replace_once_locked,
        ), mock.patch.object(
            self.worker,
            "STATUS_REPLACE_RETRY_SECONDS",
            0,
        ):
            self.worker.atomic_write_json(target, {"state": "running"})

        self.assertEqual(calls, 2)
        self.assertEqual(
            json.loads(target.read_text(encoding="utf-8")),
            {"state": "running"},
        )

    def test_explicit_sample_continuation_uses_existing_lake_and_stage_six(self) -> None:
        existing_lake = self.temporary_root / "existing-sample-lake"
        existing_lake.mkdir()
        commands = self.worker.build_stage_commands(
            "sample",
            self.temporary_root,
            "2026-07-21",
            "2026-08-20",
            existing_lake,
        )

        self.assertEqual(commands[5]["index"], 6)
        self.assertEqual(commands[5]["name"], "a01/b06_futures_minute")
        lake_argument = commands[5]["command"].index("--lake-root") + 1
        self.assertEqual(
            pathlib.Path(commands[5]["command"][lake_argument]),
            existing_lake,
        )

    def test_ordinary_failure_stops_following_stage_and_does_not_retry(self) -> None:
        counter_path = self.temporary_root / "counter.txt"
        marker_path = self.temporary_root / "following-stage.txt"
        failure = self.write_stage(
            "failure",
            "import pathlib\n"
            f"path = pathlib.Path({str(counter_path)!r})\n"
            "count = int(path.read_text() or '0') if path.exists() else 0\n"
            "path.write_text(str(count + 1))\n"
            "print('ordinary failure', flush=True)\n"
            "raise SystemExit(2)\n",
        )
        following = self.write_stage(
            "following",
            "import pathlib\n"
            f"pathlib.Path({str(marker_path)!r}).write_text('ran')\n",
        )

        result, run_root = self.run_sample([failure, following])

        self.assertEqual(result, 1)
        self.assertEqual(counter_path.read_text(encoding="utf-8"), "1")
        self.assertFalse(marker_path.exists())
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["state"], "failed")
        self.assertTrue((run_root / "failure.json").is_file())

    def test_c05_nonzero_quota_stop_is_terminal(self) -> None:
        success = self.write_stage("success", "print('ok', flush=True)\n")
        quota = self.write_stage(
            "quota_nonzero",
            "print('quota_stop: daily quota exhausted', flush=True)\n"
            "raise SystemExit(1)\n",
        )
        following_marker = self.temporary_root / "after-c05.txt"
        following = self.write_stage(
            "after_c05",
            "import pathlib\n"
            f"pathlib.Path({str(following_marker)!r}).write_text('ran')\n",
        )

        result, run_root = self.run_sample(
            [success, success, success, success, quota, following]
        )

        self.assertEqual(result, 3)
        self.assertFalse(following_marker.exists())
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["state"], "quota_stopped")
        self.assertEqual(status["stage_index"], 5)

    def test_c06_zero_exit_quota_stop_is_still_terminal(self) -> None:
        success = self.write_stage("success", "print('ok', flush=True)\n")
        quota = self.write_stage(
            "quota_zero",
            "print('quota_stop: minute quota exhausted', flush=True)\n",
        )

        result, run_root = self.run_sample(
            [success, success, success, success, success, quota]
        )

        self.assertEqual(result, 3)
        status = json.loads((run_root / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(status["state"], "quota_stopped")
        self.assertEqual(status["stage_exit_code"], 0)

    def test_monitor_once_renders_stage_progress_and_heartbeat(self) -> None:
        run_root = self.temporary_root / "monitor"
        run_root.mkdir()
        now = self.worker.utc_now_text()
        self.worker.atomic_write_json(
            run_root / "status.json",
            {
                "state": "running",
                "started_at": now,
                "heartbeat_at": now,
                "worker_pid": 999999,
                "child_pid": None,
                "stage_index": 3,
                "stage_total": 6,
                "stage_name": "b03_futures_contract_calendar",
                "progress": "planning_progress: current=25; total=100",
                "recent_lines": ["planning_progress: current=25; total=100"],
                "error": None,
            },
        )

        completed = subprocess.run(
            [
                "pwsh.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(MONITOR_PATH),
                "-RunRoot",
                str(run_root),
                "-Once",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("b03_futures_contract_calendar", completed.stdout)
        self.assertIn("planning_progress: current=25; total=100", completed.stdout)


if __name__ == "__main__":
    unittest.main()
