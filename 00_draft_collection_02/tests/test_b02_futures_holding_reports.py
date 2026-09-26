from __future__ import annotations

import hashlib
import importlib.util
import inspect
import json
import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import pandas as pd


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
    / "02_Futures_Lakehouse"
    / "a02_Futures_Exchange_Reports"
    / "b02_futures_holding_reports.py"
)
MODULE_SPEC = importlib.util.spec_from_file_location(
    "test_b02_futures_holding_reports_module",
    MODULE_PATH,
)
if MODULE_SPEC is None or MODULE_SPEC.loader is None:
    raise RuntimeError(f"无法加载模块：{MODULE_PATH}")
holding_reports = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = holding_reports
MODULE_SPEC.loader.exec_module(holding_reports)


GRID = {
    "exchange_code": "XDCE",
    "underlying_code": "BB",
    "trading_date": date(2014, 4, 28),
}
UPDATED_AT = datetime(2026, 8, 23, tzinfo=timezone.utc)


def source_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "day": GRID["trading_date"],
        "code": "BB1501.XDCE",
        "exchange": "XDCE",
        "underlying_code": "BB",
        "rank_type_ID": 501001,
        "rank_type": "成交量排名",
        "rank": 11,
        "member_name": "光大期货",
        "indicator": 6.0,
        "indicator_increase": -3.0,
    }
    row.update(overrides)
    return row


def query_jqdata(*responses: object) -> mock.MagicMock:
    jqdata = mock.MagicMock()
    jqdata.finance.run_query.side_effect = list(responses)
    return jqdata


def calendar_row(
    dataset_name: str,
    underlying_code: str,
    trading_date: date,
    *,
    completed: bool,
    actual_record_count: int = 1,
) -> dict[str, object]:
    if completed and actual_record_count:
        fetch_result_status = "success"
        is_data_missing = False
        quality_status = "passed"
        quality_reason = "测试正式事实已完成复读。"
    elif completed:
        fetch_result_status = "empty_confirmed"
        is_data_missing = True
        quality_status = "warning"
        quality_reason = "测试正式事实已确认为空。"
    else:
        fetch_result_status = "pending"
        is_data_missing = False
        quality_status = "pending"
        quality_reason = "等待测试采集。"

    return {
        "dataset_name": dataset_name,
        "exchange_code": GRID["exchange_code"],
        "underlying_code": underlying_code,
        "trading_date": trading_date,
        "is_fetch_required": True,
        "requirement_reason": "测试 required 格点。",
        "is_fetch_completed": completed,
        "fetch_result_status": fetch_result_status,
        "is_data_missing": is_data_missing,
        "expected_record_count": 1,
        "actual_record_count": actual_record_count if completed else 0,
        "quality_status": quality_status,
        "quality_reason": quality_reason,
        "fetch_run_id": "seed-batch" if completed else None,
        "fetch_completed_at": UPDATED_AT if completed else None,
        "quality_checked_at": UPDATED_AT if completed else None,
        "updated_at": UPDATED_AT,
        "year": trading_date.year,
        "month": trading_date.month,
    }


def normalized_fact_frames(
    underlying_code: str,
    trading_date: date,
    position_volume: float,
    member_volume: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    grid = {
        "exchange_code": GRID["exchange_code"],
        "underlying_code": underlying_code,
        "trading_date": trading_date,
    }
    raw_df = pd.DataFrame([
        source_row(
            day=trading_date,
            code=f"{underlying_code}1501.XDCE",
            underlying_code=underlying_code,
            indicator=position_volume,
        ),
        source_row(
            day=trading_date,
            code=f"{underlying_code}1501.XDCE",
            underlying_code=underlying_code,
            rank=1,
            member_name="期货公司",
            indicator=member_volume,
        ),
    ])
    position_df, member_df, _warnings = holding_reports.normalize_rank_response(
        raw_df,
        grid,
        UPDATED_AT,
    )
    return position_df, member_df


def parquet_hashes(root: pathlib.Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*.parquet"))
    }


def partition_file(
    table_name: str,
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> str:
    partition_path = "/".join(
        f"{column}={value}"
        for column, value in zip(partition_columns, partition_key, strict=True)
    )
    return f"{table_name}/{partition_path}/part-0.parquet"


class RankDateBatchQueryTests(unittest.TestCase):
    def test_twenty_dates_use_one_source_call(self) -> None:
        pending_dates = [
            GRID["trading_date"] + timedelta(days=offset)
            for offset in range(20)
        ]
        raw_df = pd.DataFrame([
            source_row(day=trading_date)
            for trading_date in pending_dates
        ])
        jqdata = query_jqdata(raw_df)

        raw_frames_by_date, source_call_count, split_count = (
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                pending_dates,
            )
        )

        self.assertEqual(set(raw_frames_by_date), set(pending_dates))
        self.assertTrue(all(
            len(raw_frames_by_date[value]) == 1
            for value in pending_dates
        ))
        self.assertEqual(source_call_count, 1)
        self.assertEqual(split_count, 0)
        self.assertEqual(jqdata.finance.run_query.call_count, 1)
        requested_date_batches = [
            list(call.args[0])
            for call in (
                jqdata.finance.FUT_MEMBER_POSITION_RANK.day.in_.call_args_list
            )
        ]
        self.assertEqual(requested_date_batches, [pending_dates])

    def test_response_at_limit_is_bisected_by_date(self) -> None:
        pending_dates = [
            GRID["trading_date"] + timedelta(days=offset)
            for offset in range(4)
        ]
        capped_df = pd.DataFrame([
            source_row(day=pending_dates[0])
            for _ in range(5000)
        ])
        left_df = pd.DataFrame([
            source_row(day=pending_dates[0]),
            source_row(day=pending_dates[1]),
        ])
        right_df = pd.DataFrame([
            source_row(day=pending_dates[2]),
            source_row(day=pending_dates[3]),
        ])
        jqdata = query_jqdata(capped_df, left_df, right_df)

        raw_frames_by_date, source_call_count, split_count = (
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                list(reversed(pending_dates)),
            )
        )

        self.assertEqual(set(raw_frames_by_date), set(pending_dates))
        self.assertTrue(all(
            len(raw_frames_by_date[value]) == 1
            for value in pending_dates
        ))
        self.assertEqual(source_call_count, 3)
        self.assertEqual(split_count, 1)
        self.assertEqual(jqdata.finance.run_query.call_count, 3)
        requested_date_batches = [
            list(call.args[0])
            for call in (
                jqdata.finance.FUT_MEMBER_POSITION_RANK.day.in_.call_args_list
            )
        ]
        self.assertEqual(
            requested_date_batches,
            [pending_dates, pending_dates[:2], pending_dates[2:]],
        )

    def test_single_date_at_limit_fails(self) -> None:
        capped_df = pd.DataFrame([
            source_row()
            for _ in range(5000)
        ])
        jqdata = query_jqdata(capped_df)

        with self.assertRaisesRegex(ValueError, "单日.*上限|单日.*5000"):
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                [GRID["trading_date"]],
            )

        self.assertEqual(jqdata.finance.run_query.call_count, 1)

    def test_none_response_fails_without_retry(self) -> None:
        jqdata = query_jqdata(None)

        with self.assertRaisesRegex(RuntimeError, "返回 None"):
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                [GRID["trading_date"]],
            )

        self.assertEqual(jqdata.finance.run_query.call_count, 1)

    def test_missing_source_column_fails(self) -> None:
        jqdata = query_jqdata(pd.DataFrame([source_row()]).drop(columns=["rank"]))

        with self.assertRaisesRegex(ValueError, "缺列.*rank"):
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                [GRID["trading_date"]],
            )

    def test_empty_response_still_requires_source_columns(self) -> None:
        jqdata = query_jqdata(pd.DataFrame(columns=["day"]))

        with self.assertRaisesRegex(ValueError, "缺列"):
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                [GRID["trading_date"]],
            )

    def test_valid_exchange_mixed_with_null_fails(self) -> None:
        jqdata = query_jqdata(pd.DataFrame([
            source_row(),
            source_row(exchange=None, code="BB1505.XDCE", member_name="中信期货"),
        ]))

        with self.assertRaisesRegex(ValueError, "交易所"):
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                [GRID["trading_date"]],
            )

    def test_response_cannot_escape_requested_grid(self) -> None:
        invalid_responses = (
            (
                pd.DataFrame([source_row(day=GRID["trading_date"] + timedelta(days=1))]),
                "交易日",
            ),
            (
                pd.DataFrame([source_row(underlying_code="A")]),
                "品种",
            ),
            (
                pd.DataFrame([source_row(exchange="XSGE")]),
                "交易所",
            ),
        )

        for raw_df, error_pattern in invalid_responses:
            with self.subTest(error_pattern=error_pattern):
                jqdata = query_jqdata(raw_df)
                with self.assertRaisesRegex(ValueError, error_pattern):
                    holding_reports.query_rank_date_batch(
                        jqdata,
                        GRID["exchange_code"],
                        GRID["underlying_code"],
                        [GRID["trading_date"]],
                    )

    def test_missing_dates_are_returned_as_empty_frames(self) -> None:
        pending_dates = [
            GRID["trading_date"] + timedelta(days=offset)
            for offset in range(3)
        ]
        jqdata = query_jqdata(pd.DataFrame([
            source_row(day=pending_dates[0]),
        ]))

        raw_frames_by_date, source_call_count, split_count = (
            holding_reports.query_rank_date_batch(
                jqdata,
                GRID["exchange_code"],
                GRID["underlying_code"],
                pending_dates,
            )
        )

        self.assertEqual(len(raw_frames_by_date[pending_dates[0]]), 1)
        self.assertTrue(raw_frames_by_date[pending_dates[1]].empty)
        self.assertTrue(raw_frames_by_date[pending_dates[2]].empty)
        self.assertEqual(
            list(raw_frames_by_date[pending_dates[1]].columns),
            holding_reports.JQDATA_FIELDS,
        )
        self.assertEqual(source_call_count, 1)
        self.assertEqual(split_count, 0)


class RankResponseDuplicateTests(unittest.TestCase):
    def test_top_twenty_rank_boundaries_are_accepted(self) -> None:
        for rank_value in (1, 20):
            with self.subTest(rank_value=rank_value):
                position_df, member_df, warnings = (
                    holding_reports.normalize_rank_response(
                        pd.DataFrame([source_row(rank=rank_value)]),
                        GRID,
                        UPDATED_AT,
                    )
                )

                self.assertEqual(position_df.iloc[0]["volume_rank"], rank_value)
                self.assertTrue(member_df.empty)
                self.assertEqual(warnings["position_rank"], [])

    def test_rank_outside_top_twenty_fails(self) -> None:
        for rank_value in (0, 21):
            with self.subTest(rank_value=rank_value):
                with self.assertRaisesRegex(ValueError, "1.*20|前 ?20"):
                    holding_reports.normalize_rank_response(
                        pd.DataFrame([source_row(rank=rank_value)]),
                        GRID,
                        UPDATED_AT,
                    )

    def test_equal_source_payload_uses_best_rank_and_returns_warning(self) -> None:
        raw_df = pd.DataFrame([
            source_row(rank=11),
            source_row(rank=12),
        ])

        position_df, member_df, warnings = holding_reports.normalize_rank_response(
            raw_df,
            GRID,
            UPDATED_AT,
        )

        self.assertEqual(len(position_df), 1)
        self.assertTrue(member_df.empty)
        row = position_df.iloc[0]
        self.assertEqual(row["volume_rank"], 11)
        self.assertEqual(row["volume"], 6.0)
        self.assertEqual(row["volume_change"], -3.0)
        self.assertEqual(len(warnings["position_rank"]), 1)
        self.assertIn("原名次=[11, 12]", warnings["position_rank"][0])
        self.assertEqual(warnings["member_position"], [])

    def test_conflicting_source_payload_still_fails(self) -> None:
        conflicting_overrides = (
            {"indicator": 7.0},
            {"indicator_increase": -2.0},
            {"rank_type_ID": 501099},
            {"rank_type": "成交量"},
        )

        for overrides in conflicting_overrides:
            with self.subTest(overrides=overrides):
                raw_df = pd.DataFrame([
                    source_row(rank=11),
                    source_row(rank=12, **overrides),
                ])
                with self.assertRaisesRegex(ValueError, "重复行存在.*冲突"):
                    holding_reports.normalize_rank_response(
                        raw_df,
                        GRID,
                        UPDATED_AT,
                    )

    def test_participant_summary_duplicate_is_coalesced_separately(self) -> None:
        raw_df = pd.DataFrame([
            source_row(member_name="期货公司", rank=1, indicator=20.0),
            source_row(member_name="期货公司", rank=2, indicator=20.0),
        ])

        position_df, member_df, warnings = holding_reports.normalize_rank_response(
            raw_df,
            GRID,
            UPDATED_AT,
        )

        self.assertTrue(position_df.empty)
        self.assertEqual(len(member_df), 1)
        self.assertEqual(member_df.iloc[0]["volume"], 20.0)
        self.assertEqual(warnings["position_rank"], [])
        self.assertEqual(len(warnings["member_position"]), 1)


class PositionRankSpecialCaseTests(unittest.TestCase):
    def special_case(self) -> dict[str, object]:
        response_content = b"frozen official response"
        bad_rows = tuple(
            (rank, f"坏会员{rank}", 1000 - rank, rank)
            for rank in range(1, 21)
        )
        official_rows = tuple(
            (rank, f"官方会员{rank}", 2000 - rank, -rank)
            for rank in range(1, 21)
        )
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
            "expected_jqdata_rows": bad_rows,
            "official_rows": official_rows,
        }

    def raw_frame(
        self,
        rows: tuple[tuple[int, str, int, int], ...],
    ) -> pd.DataFrame:
        return pd.DataFrame([
            source_row(
                day=date(2013, 4, 18),
                code="AU1306.XSGE",
                exchange="XSGE",
                underlying_code="AU",
                rank_type_ID=501001,
                rank_type="成交量排名",
                rank=rank,
                member_name=member_name,
                indicator=indicator,
                indicator_increase=increase,
            )
            for rank, member_name, indicator, increase in rows
        ])

    def write_artifacts(
        self,
        lake_root: pathlib.Path,
        special_case: dict[str, object],
    ) -> None:
        artifact_path = (
            lake_root / "raw" / str(special_case["raw_relative_path"])
        )
        artifact_path.mkdir(parents=True)
        response_content = b"frozen official response"
        (artifact_path / "response.dat").write_bytes(response_content)
        (artifact_path / "response.sha256").write_text(
            str(special_case["official_response_sha256"]) + "\n",
            encoding="ascii",
        )
        manifest = {
            "case_id": special_case["case_id"],
            "trading_date": special_case["trading_date"].isoformat(),
            "exchange_code": special_case["exchange_code"],
            "underlying_code": special_case["underlying_code"],
            "source_symbol": special_case["source_symbol"],
            "rank_type_id": special_case["rank_type_id"],
            "rank_type": special_case["rank_type"],
            "official_url": special_case["official_url"],
            "official_response_sha256": special_case["official_response_sha256"],
            "official_rows": [list(row) for row in special_case["official_rows"]],
        }
        (artifact_path / "calibration.json").write_text(
            json.dumps(manifest, ensure_ascii=False),
            encoding="utf-8",
        )

    def test_known_bad_top_twenty_is_replaced_as_a_complete_group(self) -> None:
        special_case = self.special_case()
        grid = {
            "exchange_code": "XSGE",
            "underlying_code": "AU",
            "trading_date": date(2013, 4, 18),
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            lake_root = pathlib.Path(temp_dir)
            self.write_artifacts(lake_root, special_case)
            with mock.patch.object(
                holding_reports,
                "POSITION_RANK_SPECIAL_CASES",
                (special_case,),
            ):
                calibrated_df, warnings = (
                    holding_reports.apply_position_rank_special_cases(
                        self.raw_frame(special_case["expected_jqdata_rows"]),
                        grid,
                        lake_root,
                    )
                )

        calibrated_rows = tuple(sorted(
            (
                int(row["rank"]),
                str(row["member_name"]),
                int(row["indicator"]),
                int(row["indicator_increase"]),
            )
            for row in calibrated_df.to_dict("records")
        ))
        self.assertEqual(calibrated_rows, special_case["official_rows"])
        self.assertEqual(len(warnings), 1)
        self.assertTrue(
            warnings[0].startswith(
                holding_reports.SOURCE_SPECIAL_CASE_REASON_MARKER
            )
        )

    def test_already_official_source_needs_no_artifact_or_warning(self) -> None:
        special_case = self.special_case()
        grid = {
            "exchange_code": "XSGE",
            "underlying_code": "AU",
            "trading_date": date(2013, 4, 18),
        }
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            holding_reports,
            "POSITION_RANK_SPECIAL_CASES",
            (special_case,),
        ):
            official_df = self.raw_frame(special_case["official_rows"])
            calibrated_df, warnings = (
                holding_reports.apply_position_rank_special_cases(
                    official_df,
                    grid,
                    pathlib.Path(temp_dir),
                )
            )
        pd.testing.assert_frame_equal(calibrated_df, official_df)
        self.assertEqual(warnings, [])

    def test_third_payload_is_rejected(self) -> None:
        special_case = self.special_case()
        third_rows = list(special_case["expected_jqdata_rows"])
        third_rows[0] = (1, "未知会员", 9999, 0)
        grid = {
            "exchange_code": "XSGE",
            "underlying_code": "AU",
            "trading_date": date(2013, 4, 18),
        }
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            holding_reports,
            "POSITION_RANK_SPECIAL_CASES",
            (special_case,),
        ):
            with self.assertRaisesRegex(ValueError, "既不匹配冻结坏指纹"):
                holding_reports.apply_position_rank_special_cases(
                    self.raw_frame(tuple(third_rows)),
                    grid,
                    pathlib.Path(temp_dir),
                )

    def test_known_bad_payload_requires_formal_artifacts(self) -> None:
        special_case = self.special_case()
        grid = {
            "exchange_code": "XSGE",
            "underlying_code": "AU",
            "trading_date": date(2013, 4, 18),
        }
        with tempfile.TemporaryDirectory() as temp_dir, mock.patch.object(
            holding_reports,
            "POSITION_RANK_SPECIAL_CASES",
            (special_case,),
        ):
            with self.assertRaisesRegex(FileNotFoundError, "请先运行"):
                holding_reports.apply_position_rank_special_cases(
                    self.raw_frame(special_case["expected_jqdata_rows"]),
                    grid,
                    pathlib.Path(temp_dir),
                )


class CompletionProofTests(unittest.TestCase):
    def calendar_df(self, completed: bool) -> pd.DataFrame:
        return pd.DataFrame([
            calendar_row(
                dataset_name,
                GRID["underlying_code"],
                GRID["trading_date"],
                completed=completed,
            )
            for dataset_name in holding_reports.DATASET_NAMES
        ])

    def test_facts_without_calendar_cannot_prove_completion(self) -> None:
        grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)

        pending_df, complete_count = holding_reports.pending_report_grids(
            self.calendar_df(completed=False),
            {grid_key: 1},
            {grid_key: 1},
            None,
            None,
        )

        self.assertEqual(len(pending_df), 1)
        self.assertEqual(complete_count, 0)

    def test_calendar_without_facts_cannot_prove_completion(self) -> None:
        pending_df, complete_count = holding_reports.pending_report_grids(
            self.calendar_df(completed=True),
            {},
            {},
            None,
            None,
        )

        self.assertEqual(len(pending_df), 1)
        self.assertEqual(complete_count, 0)

    def test_both_fact_counts_and_calendar_are_required(self) -> None:
        grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)
        completed_calendar_df = self.calendar_df(completed=True)

        for position_counts, member_counts in (
            ({grid_key: 1}, {}),
            ({}, {grid_key: 1}),
        ):
            with self.subTest(
                position_counts=position_counts,
                member_counts=member_counts,
            ):
                pending_df, complete_count = holding_reports.pending_report_grids(
                    completed_calendar_df,
                    position_counts,
                    member_counts,
                    None,
                    None,
                )
                self.assertEqual(len(pending_df), 1)
                self.assertEqual(complete_count, 0)

        pending_df, complete_count = holding_reports.pending_report_grids(
            completed_calendar_df,
            {grid_key: 1},
            {grid_key: 1},
            None,
            None,
        )
        self.assertTrue(pending_df.empty)
        self.assertEqual(complete_count, 1)

    def test_calendar_validator_accepts_completed_grid_outside_current_policy(self) -> None:
        historical_row = calendar_row(
            "warehouse_receipt",
            GRID["underlying_code"],
            GRID["trading_date"],
            completed=True,
        )
        historical_row["is_fetch_required"] = False
        historical_row["requirement_reason"] = "当前政策排除，保留历史完成凭证。"
        historical_row["is_data_missing"] = False

        validated_df = holding_reports.validate_calendar_frame(
            pd.DataFrame([historical_row]),
            "测试",
        )
        self.assertFalse(validated_df.loc[0, "is_fetch_required"])
        self.assertTrue(validated_df.loc[0, "is_fetch_completed"])

        invalid_df = pd.DataFrame([historical_row])
        invalid_df.loc[0, "is_fetch_completed"] = False
        with self.assertRaisesRegex(ValueError, "not_required"):
            holding_reports.validate_calendar_frame(invalid_df, "测试")

    def test_reader_accepts_descriptive_calendar_metadata_drift(self) -> None:
        current_schema = holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
        historical_metadata = dict(current_schema.metadata or {})
        historical_metadata[b"update_mode_zh"] = "历史更新说明。".encode("utf-8")
        historical_schema = holding_reports.pa.schema(
            list(current_schema),
            metadata=historical_metadata,
        )

        self.assertTrue(
            holding_reports.physically_and_identity_compatible(
                historical_schema,
                current_schema,
            )
        )


class EmptyLeafMarkerTests(unittest.TestCase):
    def file_schema(self) -> object:
        return holding_reports.pa.schema(
            [
                field
                for field in holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA
                if field.name not in holding_reports.POSITION_PARTITION_COLUMNS
            ],
            metadata=(
                holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA.metadata
            ),
        )

    def read_leaf(self, table_path: pathlib.Path) -> pd.DataFrame:
        return holding_reports.read_complete_partition(
            table_path,
            holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA,
            holding_reports.POSITION_PARTITION_COLUMNS,
            (GRID["exchange_code"], GRID["underlying_code"], 2014, 4),
            holding_reports.validate_position_frame,
            "测试空排名事实",
        )

    def test_correct_zero_row_marker_returns_contract_empty_frame(self) -> None:
        with tempfile.TemporaryDirectory(prefix="holding-empty-leaf-") as temp_dir:
            table_path = pathlib.Path(temp_dir) / "position"
            table_path.mkdir()
            holding_reports.pq.write_table(
                holding_reports.pa.Table.from_batches([], schema=self.file_schema()),
                table_path / "schema.parquet",
            )

            empty_df = self.read_leaf(table_path)

            self.assertTrue(empty_df.empty)
            self.assertEqual(
                list(empty_df.columns),
                holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA.names,
            )

    def test_nonzero_marker_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="holding-nonzero-marker-") as temp_dir:
            table_path = pathlib.Path(temp_dir) / "position"
            table_path.mkdir()
            position_df, _member_df = normalized_fact_frames(
                GRID["underlying_code"],
                GRID["trading_date"],
                6.0,
                20.0,
            )
            complete_table = holding_reports.pandas_to_arrow(
                position_df,
                holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA,
            )
            physical_table = complete_table.select([
                name
                for name in complete_table.column_names
                if name not in holding_reports.POSITION_PARTITION_COLUMNS
            ])
            holding_reports.pq.write_table(
                physical_table,
                table_path / "schema.parquet",
            )

            with self.assertRaisesRegex(TypeError, "schema.parquet"):
                self.read_leaf(table_path)

    def test_wrong_schema_marker_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="holding-wrong-marker-") as temp_dir:
            table_path = pathlib.Path(temp_dir) / "position"
            table_path.mkdir()
            wrong_schema = holding_reports.pa.schema([
                holding_reports.pa.field("unexpected", holding_reports.pa.string()),
            ])
            holding_reports.pq.write_table(
                holding_reports.pa.Table.from_batches([], schema=wrong_schema),
                table_path / "schema.parquet",
            )

            with self.assertRaisesRegex(TypeError, "schema.parquet"):
                self.read_leaf(table_path)


class LeafIsolationTests(unittest.TestCase):
    def commit_fact_leaf(
        self,
        lake_root: pathlib.Path,
        position_df: pd.DataFrame,
        member_df: pd.DataFrame,
        partition_key: tuple[object, ...],
    ) -> None:
        holding_reports.commit_complete_partition(
            position_df,
            lake_root,
            holding_reports.POSITION_TABLE_NAME,
            holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA,
            holding_reports.POSITION_PARTITION_COLUMNS,
            holding_reports.POSITION_PARTITIONING,
            partition_key,
            holding_reports.validate_position_frame,
        )
        holding_reports.commit_complete_partition(
            member_df,
            lake_root,
            holding_reports.MEMBER_TABLE_NAME,
            holding_reports.FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
            holding_reports.MEMBER_PARTITION_COLUMNS,
            holding_reports.MEMBER_PARTITIONING,
            partition_key,
            holding_reports.validate_member_frame,
        )

    def commit_calendar_leaf(
        self,
        lake_root: pathlib.Path,
        dataset_name: str,
        rows: list[dict[str, object]],
        year: int,
        month: int,
    ) -> None:
        calendar_df = pd.DataFrame(
            rows,
            columns=holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
        )
        holding_reports.commit_complete_partition(
            calendar_df,
            lake_root,
            holding_reports.CALENDAR_TABLE_NAME,
            holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            holding_reports.CALENDAR_PARTITION_COLUMNS,
            holding_reports.CALENDAR_PARTITIONING,
            (dataset_name, GRID["exchange_code"], year, month),
            holding_reports.validate_calendar_frame,
        )

    def read_fact_leaf(
        self,
        lake_root: pathlib.Path,
        table_name: str,
        schema: object,
        partition_columns: list[str],
        partition_key: tuple[object, ...],
        validator: object,
    ) -> pd.DataFrame:
        return holding_reports.read_complete_partition(
            lake_root / "silver" / table_name,
            schema,
            partition_columns,
            partition_key,
            validator,
            f"测试 {table_name}",
        )

    def read_calendar_month(
        self,
        lake_root: pathlib.Path,
        year: int,
        month: int,
    ) -> pd.DataFrame:
        frames = []
        for dataset_name in holding_reports.DATASET_NAMES:
            frames.append(holding_reports.read_complete_partition(
                lake_root / "silver" / holding_reports.CALENDAR_TABLE_NAME,
                holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                holding_reports.CALENDAR_PARTITION_COLUMNS,
                (dataset_name, GRID["exchange_code"], year, month),
                holding_reports.validate_calendar_frame,
                f"测试报告日历 {dataset_name}",
            ))
        return holding_reports.validate_calendar_frame(
            pd.concat(frames, ignore_index=True),
            "测试合并后的报告日历叶",
        )

    def test_commits_only_target_fact_and_calendar_leaves(self) -> None:
        target_date = GRID["trading_date"]
        retained_date = date(2014, 4, 25)
        unrelated_month_date = date(2014, 5, 5)
        target_partition_key = (GRID["exchange_code"], "BB", 2014, 4)
        unrelated_partition_key = (GRID["exchange_code"], "A", 2014, 4)

        with tempfile.TemporaryDirectory(prefix="holding-report-test-") as temp_dir:
            lake_root = pathlib.Path(temp_dir)
            old_target_position_df, old_target_member_df = normalized_fact_frames(
                "BB",
                target_date,
                6.0,
                20.0,
            )
            retained_position_df, retained_member_df = normalized_fact_frames(
                "BB",
                retained_date,
                8.0,
                22.0,
            )
            unrelated_position_df, unrelated_member_df = normalized_fact_frames(
                "A",
                target_date,
                9.0,
                24.0,
            )
            self.commit_fact_leaf(
                lake_root,
                pd.concat(
                    [old_target_position_df, retained_position_df],
                    ignore_index=True,
                ),
                pd.concat(
                    [old_target_member_df, retained_member_df],
                    ignore_index=True,
                ),
                target_partition_key,
            )
            self.commit_fact_leaf(
                lake_root,
                unrelated_position_df,
                unrelated_member_df,
                unrelated_partition_key,
            )

            for dataset_name in holding_reports.DATASET_NAMES:
                self.commit_calendar_leaf(
                    lake_root,
                    dataset_name,
                    [
                        calendar_row(
                            dataset_name,
                            "BB",
                            target_date,
                            completed=False,
                        ),
                        calendar_row(
                            dataset_name,
                            "A",
                            target_date,
                            completed=True,
                        ),
                    ],
                    2014,
                    4,
                )
                self.commit_calendar_leaf(
                    lake_root,
                    dataset_name,
                    [calendar_row(
                        dataset_name,
                        "BB",
                        unrelated_month_date,
                        completed=False,
                    )],
                    2014,
                    5,
                )

            calendar_before_df = self.read_calendar_month(lake_root, 2014, 4)
            unrelated_calendar_before_df = calendar_before_df.loc[
                calendar_before_df["underlying_code"].eq("A")
            ].reset_index(drop=True)
            silver_root = lake_root / "silver"
            hashes_before = parquet_hashes(silver_root)

            incoming_position_df, incoming_member_df = normalized_fact_frames(
                "BB",
                target_date,
                70.0,
                80.0,
            )
            existing_position_df = self.read_fact_leaf(
                lake_root,
                holding_reports.POSITION_TABLE_NAME,
                holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA,
                holding_reports.POSITION_PARTITION_COLUMNS,
                target_partition_key,
                holding_reports.validate_position_frame,
            )
            existing_member_df = self.read_fact_leaf(
                lake_root,
                holding_reports.MEMBER_TABLE_NAME,
                holding_reports.FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
                holding_reports.MEMBER_PARTITION_COLUMNS,
                target_partition_key,
                holding_reports.validate_member_frame,
            )
            retained_position_before_df = existing_position_df.loc[
                existing_position_df["trading_date"].eq(retained_date)
            ].reset_index(drop=True)
            retained_member_before_df = existing_member_df.loc[
                existing_member_df["trading_date"].eq(retained_date)
            ].reset_index(drop=True)

            complete_position_df = holding_reports.full_fact_partition(
                existing_position_df,
                incoming_position_df,
                {target_date},
                holding_reports.POSITION_PARTITION_COLUMNS,
                target_partition_key,
                holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA,
                holding_reports.validate_position_frame,
            )
            complete_member_df = holding_reports.full_fact_partition(
                existing_member_df,
                incoming_member_df,
                {target_date},
                holding_reports.MEMBER_PARTITION_COLUMNS,
                target_partition_key,
                holding_reports.FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
                holding_reports.validate_member_frame,
            )
            self.commit_fact_leaf(
                lake_root,
                complete_position_df,
                complete_member_df,
                target_partition_key,
            )

            committed_position_df = self.read_fact_leaf(
                lake_root,
                holding_reports.POSITION_TABLE_NAME,
                holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA,
                holding_reports.POSITION_PARTITION_COLUMNS,
                target_partition_key,
                holding_reports.validate_position_frame,
            )
            committed_member_df = self.read_fact_leaf(
                lake_root,
                holding_reports.MEMBER_TABLE_NAME,
                holding_reports.FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
                holding_reports.MEMBER_PARTITION_COLUMNS,
                target_partition_key,
                holding_reports.validate_member_frame,
            )
            grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)
            grid_counts = {
                grid_key: (
                    holding_reports.grid_count_map(committed_position_df)[grid_key],
                    holding_reports.grid_count_map(committed_member_df)[grid_key],
                )
            }
            completed_calendar_df = holding_reports.apply_calendar_completion(
                calendar_before_df,
                grid_counts,
                {},
                "target-batch",
                UPDATED_AT,
            )
            holding_reports.commit_calendar_partitions(
                completed_calendar_df,
                grid_counts,
                lake_root,
            )

            hashes_after = parquet_hashes(silver_root)
            changed_files = {
                path
                for path in hashes_before.keys() | hashes_after.keys()
                if hashes_before.get(path) != hashes_after.get(path)
            }
            expected_changed_files = {
                partition_file(
                    holding_reports.POSITION_TABLE_NAME,
                    holding_reports.POSITION_PARTITION_COLUMNS,
                    target_partition_key,
                ),
                partition_file(
                    holding_reports.MEMBER_TABLE_NAME,
                    holding_reports.MEMBER_PARTITION_COLUMNS,
                    target_partition_key,
                ),
                *{
                    partition_file(
                        holding_reports.CALENDAR_TABLE_NAME,
                        holding_reports.CALENDAR_PARTITION_COLUMNS,
                        (dataset_name, GRID["exchange_code"], 2014, 4),
                    )
                    for dataset_name in holding_reports.DATASET_NAMES
                },
            }
            self.assertEqual(changed_files, expected_changed_files)

            target_position_row = committed_position_df.loc[
                committed_position_df["trading_date"].eq(target_date)
            ].iloc[0]
            target_member_row = committed_member_df.loc[
                committed_member_df["trading_date"].eq(target_date)
            ].iloc[0]
            self.assertEqual(target_position_row["volume"], 70.0)
            self.assertEqual(target_member_row["volume"], 80.0)
            pd.testing.assert_frame_equal(
                retained_position_before_df,
                committed_position_df.loc[
                    committed_position_df["trading_date"].eq(retained_date)
                ].reset_index(drop=True),
            )
            pd.testing.assert_frame_equal(
                retained_member_before_df,
                committed_member_df.loc[
                    committed_member_df["trading_date"].eq(retained_date)
                ].reset_index(drop=True),
            )

            calendar_after_df = self.read_calendar_month(lake_root, 2014, 4)
            pd.testing.assert_frame_equal(
                unrelated_calendar_before_df,
                calendar_after_df.loc[
                    calendar_after_df["underlying_code"].eq("A")
                ].reset_index(drop=True),
            )
            target_calendar_df = calendar_after_df.loc[
                calendar_after_df["underlying_code"].eq("BB")
            ]
            self.assertTrue(target_calendar_df["is_fetch_completed"].all())
            self.assertEqual(
                set(target_calendar_df["fetch_result_status"]),
                {"success"},
            )


class RecoverySemanticsTests(unittest.TestCase):
    def pending_calendar_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                calendar_row(
                    dataset_name,
                    GRID["underlying_code"],
                    GRID["trading_date"],
                    completed=False,
                )
                for dataset_name in holding_reports.DATASET_NAMES
            ],
            columns=holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
        )

    def commit_fact(
        self,
        lake_root: pathlib.Path,
        frame: pd.DataFrame,
        *,
        position: bool,
    ) -> pd.DataFrame:
        if position:
            table_name = holding_reports.POSITION_TABLE_NAME
            schema = holding_reports.FUTURES_POSITION_RANK_DAILY_SCHEMA
            partition_columns = holding_reports.POSITION_PARTITION_COLUMNS
            partitioning = holding_reports.POSITION_PARTITIONING
            validator = holding_reports.validate_position_frame
        else:
            table_name = holding_reports.MEMBER_TABLE_NAME
            schema = holding_reports.FUTURES_MEMBER_POSITION_DAILY_SCHEMA
            partition_columns = holding_reports.MEMBER_PARTITION_COLUMNS
            partitioning = holding_reports.MEMBER_PARTITIONING
            validator = holding_reports.validate_member_frame
        return holding_reports.commit_complete_partition(
            frame,
            lake_root,
            table_name,
            schema,
            partition_columns,
            partitioning,
            (GRID["exchange_code"], GRID["underlying_code"], 2014, 4),
            validator,
        )

    def seed_pending_calendar(self, lake_root: pathlib.Path) -> None:
        for dataset_name in holding_reports.DATASET_NAMES:
            frame = pd.DataFrame(
                [calendar_row(
                    dataset_name,
                    GRID["underlying_code"],
                    GRID["trading_date"],
                    completed=False,
                )],
                columns=(
                    holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
                ),
            )
            holding_reports.commit_complete_partition(
                frame,
                lake_root,
                holding_reports.CALENDAR_TABLE_NAME,
                holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                holding_reports.CALENDAR_PARTITION_COLUMNS,
                holding_reports.CALENDAR_PARTITIONING,
                (dataset_name, GRID["exchange_code"], 2014, 4),
                holding_reports.validate_calendar_frame,
            )

    def read_calendar(self, lake_root: pathlib.Path) -> pd.DataFrame:
        frames = []
        calendar_path = (
            lake_root / "silver" / holding_reports.CALENDAR_TABLE_NAME
        )
        for dataset_name in holding_reports.DATASET_NAMES:
            frames.append(holding_reports.read_complete_partition(
                calendar_path,
                holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                holding_reports.CALENDAR_PARTITION_COLUMNS,
                (dataset_name, GRID["exchange_code"], 2014, 4),
                holding_reports.validate_calendar_frame,
                f"恢复测试报告日历 {dataset_name}",
            ))
        return pd.concat(frames, ignore_index=True)

    def test_second_fact_failure_leaves_grid_pending(self) -> None:
        position_df, member_df = normalized_fact_frames(
            GRID["underlying_code"],
            GRID["trading_date"],
            6.0,
            20.0,
        )
        grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)

        with tempfile.TemporaryDirectory(prefix="holding-fact-failure-") as temp_dir:
            lake_root = pathlib.Path(temp_dir)
            committed_position_df = self.commit_fact(
                lake_root,
                position_df,
                position=True,
            )
            with mock.patch.object(
                holding_reports,
                "commit_complete_partition",
                side_effect=RuntimeError("模拟第二张事实提交失败"),
            ):
                with self.assertRaisesRegex(RuntimeError, "第二张事实"):
                    self.commit_fact(
                        lake_root,
                        member_df,
                        position=False,
                    )

            pending_df, complete_count = holding_reports.pending_report_grids(
                self.pending_calendar_df(),
                holding_reports.grid_count_map(committed_position_df),
                {},
                None,
                None,
            )

            self.assertEqual(
                holding_reports.grid_count_map(committed_position_df),
                {grid_key: 1},
            )
            self.assertEqual(len(pending_df), 1)
            self.assertEqual(complete_count, 0)

    def test_second_calendar_leaf_failure_leaves_grid_pending(self) -> None:
        position_df, member_df = normalized_fact_frames(
            GRID["underlying_code"],
            GRID["trading_date"],
            6.0,
            20.0,
        )
        grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)
        grid_counts = {grid_key: (1, 1)}

        with tempfile.TemporaryDirectory(prefix="holding-calendar-failure-") as temp_dir:
            lake_root = pathlib.Path(temp_dir)
            committed_position_df = self.commit_fact(
                lake_root,
                position_df,
                position=True,
            )
            committed_member_df = self.commit_fact(
                lake_root,
                member_df,
                position=False,
            )
            self.seed_pending_calendar(lake_root)
            completed_calendar_df = holding_reports.apply_calendar_completion(
                self.read_calendar(lake_root),
                grid_counts,
                {},
                "calendar-failure-batch",
                UPDATED_AT,
            )

            original_commit = holding_reports.commit_complete_partition
            commit_count = 0

            def fail_second_calendar_leaf(*args: object, **kwargs: object) -> object:
                nonlocal commit_count
                commit_count += 1
                if commit_count == 2:
                    raise RuntimeError("模拟第二个日历叶提交失败")
                return original_commit(*args, **kwargs)

            with mock.patch.object(
                holding_reports,
                "commit_complete_partition",
                side_effect=fail_second_calendar_leaf,
            ):
                with self.assertRaisesRegex(RuntimeError, "第二个日历叶"):
                    holding_reports.commit_calendar_partitions(
                        completed_calendar_df,
                        grid_counts,
                        lake_root,
                    )

            partially_committed_calendar_df = self.read_calendar(lake_root)
            pending_df, complete_count = holding_reports.pending_report_grids(
                partially_committed_calendar_df,
                holding_reports.grid_count_map(committed_position_df),
                holding_reports.grid_count_map(committed_member_df),
                None,
                None,
            )

            self.assertEqual(
                int(partially_committed_calendar_df["is_fetch_completed"].sum()),
                1,
            )
            self.assertEqual(len(pending_df), 1)
            self.assertEqual(complete_count, 0)

    def test_existing_source_duplicate_warning_survives_clean_reapply(self) -> None:
        calendar_df = pd.DataFrame(
            [
                calendar_row(
                    dataset_name,
                    GRID["underlying_code"],
                    GRID["trading_date"],
                    completed=True,
                )
                for dataset_name in holding_reports.DATASET_NAMES
            ],
            columns=holding_reports.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
        )
        position_mask = calendar_df["dataset_name"].eq("position_rank")
        calendar_df.loc[position_mask, "quality_status"] = "warning"
        calendar_df.loc[position_mask, "quality_reason"] = (
            "JQData 排名响应已转换并从正式事实表复读 1 行；"
            "来源重复归一化：历史重复证据。"
        )
        grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)

        reapplied_df = holding_reports.apply_calendar_completion(
            calendar_df,
            {grid_key: (1, 1)},
            {},
            "clean-reapply-batch",
            UPDATED_AT,
        )
        position_row = reapplied_df.loc[
            reapplied_df["dataset_name"].eq("position_rank")
        ].iloc[0]

        self.assertEqual(position_row["quality_status"], "warning")
        self.assertIn("来源重复归一化", position_row["quality_reason"])
        self.assertIn("历史重复证据", position_row["quality_reason"])


class CalendarWarningTests(unittest.TestCase):
    def calendar_df(self) -> pd.DataFrame:
        rows = []
        for dataset_name in holding_reports.DATASET_NAMES:
            rows.append({
                "dataset_name": dataset_name,
                **GRID,
                "is_fetch_required": True,
                "is_fetch_completed": False,
                "fetch_result_status": "pending",
                "is_data_missing": False,
                "expected_record_count": 1,
                "actual_record_count": 0,
                "quality_status": "pending",
                "quality_reason": "等待采集。",
                "fetch_run_id": None,
                "fetch_completed_at": None,
                "quality_checked_at": None,
                "updated_at": UPDATED_AT,
            })
        return pd.DataFrame(rows)

    def apply_completion(
        self,
        quality_warnings_by_grid_dataset: dict[tuple[object, ...], list[str]],
    ) -> pd.DataFrame:
        with mock.patch.object(
            holding_reports,
            "validate_calendar_frame",
            side_effect=lambda frame, _context: frame,
        ):
            return holding_reports.apply_calendar_completion(
                self.calendar_df(),
                {(GRID["exchange_code"], GRID["underlying_code"], GRID["trading_date"]): (1, 0)},
                quality_warnings_by_grid_dataset,
                "batch-id",
                UPDATED_AT,
            )

    def test_source_duplicate_warning_is_persistent_and_complete(self) -> None:
        grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)
        completed_df = self.apply_completion({
            (*grid_key, "position_rank"): ["来源重复测试"],
        })
        position_row = completed_df.loc[
            completed_df["dataset_name"].eq("position_rank")
        ].iloc[0].to_dict()

        self.assertEqual(position_row["fetch_result_status"], "success")
        self.assertEqual(position_row["quality_status"], "warning")
        self.assertIn("来源重复归一化", position_row["quality_reason"])
        self.assertTrue(
            holding_reports.calendar_grid_is_complete(position_row, 1)
        )

    def test_ordinary_nonempty_response_remains_passed(self) -> None:
        completed_df = self.apply_completion({})
        position_row = completed_df.loc[
            completed_df["dataset_name"].eq("position_rank")
        ].iloc[0].to_dict()

        self.assertEqual(position_row["quality_status"], "passed")
        self.assertTrue(
            holding_reports.calendar_grid_is_complete(position_row, 1)
        )

    def test_special_case_calibration_is_a_persistent_warning(self) -> None:
        grid_key = tuple(GRID[column] for column in holding_reports.GRID_COLUMNS)
        completed_df = self.apply_completion({
            (*grid_key, "position_rank"): [
                holding_reports.SOURCE_SPECIAL_CASE_REASON_MARKER
                + "test_case：整组 Top 20 已按官方文件校准"
            ],
        })
        position_row = completed_df.loc[
            completed_df["dataset_name"].eq("position_rank")
        ].iloc[0].to_dict()

        self.assertEqual(position_row["quality_status"], "warning")
        self.assertIn("来源特殊校准", position_row["quality_reason"])
        self.assertIn("test_case", position_row["quality_reason"])
        self.assertTrue(
            holding_reports.calendar_grid_is_complete(position_row, 1)
        )


class PerformanceStructureTests(unittest.TestCase):
    def test_month_loop_has_no_table_root_discovery_or_full_materialization(self) -> None:
        main_source = inspect.getsource(holding_reports.main.callback)
        month_loop_source = main_source.split("for group_number", maxsplit=1)[1]

        self.assertNotIn("open_exact_dataset(", month_loop_source)
        self.assertNotIn("open_optional_exact_dataset(", month_loop_source)
        self.assertNotIn(".to_table(", month_loop_source)
        self.assertNotIn(".rglob(", month_loop_source)
        self.assertNotIn("query_rank_grid(", month_loop_source)
        self.assertIn("query_rank_date_batch(", month_loop_source)

    def test_leaf_reader_only_enumerates_the_target_leaf(self) -> None:
        leaf_reader_source = inspect.getsource(holding_reports.read_complete_partition)

        self.assertIn('leaf_path.glob("*.parquet")', leaf_reader_source)
        self.assertNotIn(".rglob(", leaf_reader_source)


if __name__ == "__main__":
    unittest.main()
