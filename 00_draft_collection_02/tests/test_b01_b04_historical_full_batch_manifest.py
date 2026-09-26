from __future__ import annotations

import importlib.util
import pathlib
import sys
import unittest
from unittest import mock


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")

SCRIPTS_ROOT = PROJECT_ROOT / "00_draft_collection_02" / "scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))
MODULE_PATH = SCRIPTS_ROOT / "run_b01_b04_historical_full_batch.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "test_historical_full_batch_module",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"无法加载模块：{MODULE_PATH}")
historical_batch = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = historical_batch
MODULE_SPEC.loader.exec_module(historical_batch)


class HistoricalFullBatchManifestTests(unittest.TestCase):
    def test_exact_formal_manifest(self) -> None:
        worker = historical_batch.worker
        original_stages = worker.STAGES
        try:
            worker.STAGES = historical_batch.FULL_BATCH_STAGES
            commands = worker.build_stage_commands(
                "formal",
                PROJECT_ROOT / "00_draft_collection_02" / "run_status" / "test",
                None,
                None,
            )
        finally:
            worker.STAGES = original_stages

        self.assertEqual(
            tuple(stage["name"] for stage in commands),
            historical_batch.EXPECTED_STAGE_NAMES,
        )
        self.assertEqual(len(commands), 18)
        for index, stage in enumerate(commands, start=1):
            command = stage["command"]
            self.assertEqual(command[-1], "--write")
            self.assertEqual("--full" in command, index <= 4)
            for forbidden_argument in (
                "--lake-root",
                "--start-date",
                "--end-date",
                "--force",
                "--confirm-full-quality",
            ):
                self.assertNotIn(forbidden_argument, command)
        self.assertEqual(
            commands[6]["name"],
            "b01/c07_suspected_session_reconciliation",
        )
        self.assertNotIn(
            "b01/c08_full_minute_quality",
            {stage["name"] for stage in commands},
        )

    def test_main_hard_codes_formal_mode_and_required_monitor(self) -> None:
        run_root = (
            PROJECT_ROOT
            / "00_draft_collection_02"
            / "run_status"
            / "test-main"
        )
        original_stages = historical_batch.worker.STAGES
        try:
            with (
                mock.patch.object(
                    sys,
                    "argv",
                    ["run_b01_b04_historical_full_batch.py", "--run-root", str(run_root)],
                ),
                mock.patch.object(
                    historical_batch.worker,
                    "run_job",
                    return_value=0,
                ) as run_job,
            ):
                self.assertEqual(historical_batch.main(), 0)
        finally:
            historical_batch.worker.STAGES = original_stages

        run_job.assert_called_once_with(
            "formal",
            run_root,
            None,
            None,
            require_monitor=True,
        )


if __name__ == "__main__":
    unittest.main()
