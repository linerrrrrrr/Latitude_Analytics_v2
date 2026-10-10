from __future__ import annotations

import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
from datetime import date
from unittest import mock

from click.testing import CliRunner


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


MODULE_PATH = (
    PROJECT_ROOT
    / "R02_Market_Data/a01_Collection"
    / "b02_Futures_Exchange_Reports"
    / "c01a_position_rank_special_case_calibration.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "test_position_rank_special_case_calibration_module",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"无法加载模块：{MODULE_PATH}")
calibration = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = calibration
MODULE_SPEC.loader.exec_module(calibration)


class FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code


class PositionRankSpecialCaseCalibrationTests(unittest.TestCase):
    def official_rows(self) -> tuple[tuple[int, str, int, int], ...]:
        return tuple(
            (rank, f"会员{rank}", 1000 - rank, rank - 10)
            for rank in range(1, 21)
        )

    def response_content(self) -> bytes:
        source_rows = [{
            "INSTRUMENTID": "au1306                        ",
            "RANK": rank,
            "PARTICIPANTABBR1": member_name + "  ",
            "CJ1": indicator,
            "CJ1_CHG": increase,
        } for rank, member_name, indicator, increase in self.official_rows()]
        source_rows.append({
            "INSTRUMENTID": "cu1305",
            "RANK": 1,
            "PARTICIPANTABBR1": "无关会员",
            "CJ1": 1,
            "CJ1_CHG": 0,
        })
        return json.dumps(
            {"o_cursor": source_rows},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

    def special_case(self) -> dict[str, object]:
        response_content = self.response_content()
        return {
            "case_id": "test_case",
            "raw_relative_path": "shfe/position_rank_special_cases/test_case",
            "trading_date": date(2013, 4, 18),
            "exchange_code": "XSGE",
            "underlying_code": "AU",
            "source_symbol": "AU1306.XSGE",
            "official_instrument_id": "AU1306",
            "rank_type_id": 501001,
            "rank_type": "成交量排名",
            "official_url": "https://www.shfe.com.cn/test.dat",
            "official_response_sha256": hashlib.sha256(response_content).hexdigest(),
            "expected_jqdata_rows": (),
            "official_rows": self.official_rows(),
        }

    def test_response_requires_exact_hash_and_top_twenty(self) -> None:
        special_case = self.special_case()
        manifest = calibration.validate_response(
            self.response_content(),
            special_case,
        )
        self.assertEqual(manifest["case_id"], "test_case")
        self.assertEqual(len(manifest["official_rows"]), 20)

        drifted_case = {**special_case, "official_response_sha256": "0" * 64}
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            calibration.validate_response(self.response_content(), drifted_case)

    def test_write_installs_and_reopens_complete_artifact_directory(self) -> None:
        special_case = self.special_case()
        response = FakeResponse(self.response_content())
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            calibration,
            "POSITION_RANK_SPECIAL_CASES",
            (special_case,),
        ), mock.patch.object(
            calibration.requests,
            "get",
            return_value=response,
        ) as request_get:
            result = runner.invoke(
                calibration.main,
                ["--lake-root", temp_dir, "--write"],
            )
            self.assertEqual(result.exit_code, 0, result.output)
            artifact_path = (
                pathlib.Path(temp_dir)
                / "raw"
                / str(special_case["raw_relative_path"])
            )
            calibration.verify_artifacts(artifact_path, special_case)
            self.assertEqual(request_get.call_count, 1)

            with mock.patch.object(
                calibration.requests,
                "get",
                side_effect=AssertionError("已有正式证据时不得再次请求"),
            ):
                second_result = runner.invoke(
                    calibration.main,
                    ["--lake-root", temp_dir, "--write"],
                )
            self.assertEqual(second_result.exit_code, 0, second_result.output)
            self.assertIn("special_case_existing_valid", second_result.output)

            (artifact_path / "unexpected").mkdir()
            with self.assertRaisesRegex(ValueError, "文件集合"):
                calibration.verify_artifacts(artifact_path, special_case)

    def test_non_200_response_fails_without_writing(self) -> None:
        special_case = self.special_case()
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            calibration,
            "POSITION_RANK_SPECIAL_CASES",
            (special_case,),
        ), mock.patch.object(
            calibration.requests,
            "get",
            return_value=FakeResponse(b"blocked", status_code=503),
        ):
            result = runner.invoke(
                calibration.main,
                ["--lake-root", temp_dir, "--write"],
            )
            self.assertNotEqual(result.exit_code, 0)
            artifact_path = (
                pathlib.Path(temp_dir)
                / "raw"
                / str(special_case["raw_relative_path"])
            )
            self.assertFalse(artifact_path.exists())


if __name__ == "__main__":
    unittest.main()
