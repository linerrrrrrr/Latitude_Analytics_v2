"""生产 runner 的 a01 可选 b07 与 groups 过滤边界。"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from unittest import mock


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_ROOT = PROJECT_ROOT / "00_draft_collection_02" / "scripts"
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import run_b01_b04_production_update as production  # noqa: E402


class ProductionRunnerManifestTest(unittest.TestCase):
    def invoke(self, *arguments: str) -> list[str]:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        run_root = pathlib.Path(temporary_directory.name) / "run"
        captured_names: list[str] = []

        def capture_job(*args: object, **kwargs: object) -> int:
            captured_names.extend(stage[0] for stage in production.worker.STAGES)
            return 0

        original_stages = production.worker.STAGES
        self.addCleanup(setattr, production.worker, "STAGES", original_stages)
        argv = [
            "run_b01_b04_production_update.py",
            "--mode",
            "formal",
            "--run-root",
            str(run_root),
            *arguments,
        ]
        with mock.patch.object(sys, "argv", argv), mock.patch.object(
            production.worker,
            "run_job",
            side_effect=capture_job,
        ):
            self.assertEqual(production.main(), 0)
        return captured_names

    def test_b01_default_runs_c01_through_c07_and_never_c08(self) -> None:
        names = self.invoke("--groups", "a01")
        self.assertEqual(
            names,
            [
                "a01/b01_trade_calendar",
                "a01/b02_futures_variety_calendar",
                "a01/b03_futures_contract_calendar",
                "a01/b04_futures_bar_calendar",
                "a01/b05_futures_daily",
                "a01/b06_futures_minute",
                "a01/b07_suspected_session_reconciliation",
            ],
        )

    def test_skip_optional_quality_removes_only_c07(self) -> None:
        names = self.invoke(
            "--groups",
            "a01",
            "--skip-optional-quality",
        )
        self.assertEqual(len(names), 6)
        self.assertEqual(names[-1], "a01/b06_futures_minute")

    def test_skip_flag_does_not_restore_unselected_groups(self) -> None:
        names = self.invoke(
            "--groups",
            "a02",
            "a03",
            "--skip-optional-quality",
        )
        self.assertEqual(len(names), 8)
        self.assertTrue(all(name.startswith(("a02/", "a03/")) for name in names))
        self.assertEqual(
            names[:4],
            [
                "a02/b01_exchange_report_calendar",
                "a02/b01a_position_rank_special_case_calibration",
                "a02/b02_futures_holding_reports",
                "a02/b03_warehouse_receipt",
            ],
        )


if __name__ == "__main__":
    unittest.main()
