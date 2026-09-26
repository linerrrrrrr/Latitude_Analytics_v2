from __future__ import annotations

import importlib.util
import pathlib
import shutil
import tempfile
import unittest
from datetime import date, datetime, timezone
from unittest import mock

import nbformat
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKFLOW_ROOT = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a04_Macro_And_Interest_Rates"
)
C01_PATH = WORKFLOW_ROOT / "b01_macro_release_calendar.py"
C03_PATH = WORKFLOW_ROOT / "b03_macro_release.py"
C03_NOTEBOOK_PATH = C03_PATH.with_suffix(".ipynb")


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


C01 = load_module("test_b04_c01_for_c03", C01_PATH)
C03 = load_module("test_b04_c03", C03_PATH)


class FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self.payload


class FakeEastmoneySession:
    def __init__(
        self,
        rows_by_report: dict[str, list[dict[str, object]]],
    ) -> None:
        self.rows_by_report = rows_by_report
        self.calls: list[dict[str, object]] = []
        self.closed = False

    def get(self, _url, *, params, timeout):
        self.calls.append({"params": dict(params), "timeout": timeout})
        rows = self.rows_by_report[str(params["reportName"])]
        payload = {
            "success": True,
            "result": {
                "pages": 1 if rows else 0,
                "count": len(rows),
                "data": rows,
            },
        }
        return FakeResponse(payload)

    def close(self) -> None:
        self.closed = True


def source_row(
    report_name: str,
    source_report_date: date,
    *,
    missing_column: str | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {
        "REPORT_DATE": f"{source_report_date.isoformat()} 00:00:00",
    }
    for offset, series in enumerate(
        C03.SERIES_BY_REPORT[report_name],
        start=1,
    ):
        value = 100.0 + offset if series.source_value_offset == -100.0 else float(offset)
        row[series.source_column] = value
    if missing_column is not None:
        row[missing_column] = None
    return row


def all_report_rows(source_report_date: date):
    return {
        report_name: [source_row(report_name, source_report_date)]
        for report_name in C03.SERIES_BY_REPORT
    }


def build_calendar(
    lake_root: pathlib.Path,
    report_date: date,
) -> None:
    result = CliRunner().invoke(
        C01.main,
        [
            "--lake-root",
            str(lake_root),
            "--start-date",
            report_date.isoformat(),
            "--end-date",
            report_date.isoformat(),
            "--write",
        ],
    )
    if result.exit_code != 0:
        raise AssertionError(result.output) from result.exception


def run_c03(
    lake_root: pathlib.Path,
    report_date: date | None,
    session: FakeEastmoneySession | None,
):
    arguments = ["--lake-root", str(lake_root)]
    if report_date is not None:
        arguments.extend([
            "--start-date",
            report_date.isoformat(),
            "--end-date",
            report_date.isoformat(),
        ])
    arguments.append("--write")

    replacement = (
        mock.Mock(return_value=session)
        if session is not None
        else mock.Mock(side_effect=AssertionError("不应创建 Eastmoney 会话"))
    )
    with mock.patch.object(C03, "create_eastmoney_session", replacement):
        return CliRunner().invoke(C03.main, arguments)


class MacroReleaseNotebookTests(unittest.TestCase):
    def test_notebook_is_split_commented_and_exportable(self) -> None:
        notebook = nbformat.read(C03_NOTEBOOK_PATH, as_version=4)
        nbformat.validate(notebook)
        source, _ = PythonExporter().from_notebook_node(notebook)
        compile(source, str(C03_NOTEBOOK_PATH), "exec")

        self.assertGreaterEqual(len(notebook.cells), 20)
        self.assertGreaterEqual(
            sum(cell.cell_type == "code" for cell in notebook.cells),
            10,
        )
        self.assertIn("# Eastmoney", source)

    def test_shared_mapping_uses_real_ppi_column_and_cumulative_offsets(self) -> None:
        self.assertEqual(C03.SERIES_BY_CODE["PPI_YOY"].source_column, "BASE_SAME")
        offset_codes = {
            series.series_code
            for series in C03.MACRO_SERIES
            if series.source_value_offset == -100.0
        }
        self.assertEqual(
            offset_codes,
            {
                "CPI_NATIONAL_YTD",
                "CPI_CITY_YTD",
                "CPI_RURAL_YTD",
                "PPI_YTD",
            },
        )

    def test_normalize_maps_source_month_start_and_applies_offsets(self) -> None:
        canonical_date = date(2026, 7, 31)
        source_date = date(2026, 7, 1)
        calendar_df = C01.build_expected_calendar(
            canonical_date,
            canonical_date,
            C01.empty_pandas(C01.MACRO_RELEASE_CALENDAR_SCHEMA),
            date(2026, 8, 17),
            datetime.now(timezone.utc),
        )
        pending_df = calendar_df.loc[
            calendar_df["dataset_name"].eq(C03.DATASET_NAME)
            & calendar_df["series_code"].isin({"PPI_YOY", "PPI_YTD"})
        ]
        row = source_row("RPT_ECONOMY_PPI", source_date)
        row["BASE_SAME"] = 3.5
        row["BASE_ACCUMULATE"] = 101.8

        fact_df, counts = C03.normalize_macro_release_response(
            [row],
            "RPT_ECONOMY_PPI",
            pending_df,
            source_date,
            source_date,
            datetime.now(timezone.utc),
        )

        values = fact_df.set_index("series_code")["value"].to_dict()
        self.assertAlmostEqual(values["PPI_YOY"], 3.5)
        self.assertAlmostEqual(values["PPI_YTD"], 1.8)
        self.assertEqual(set(fact_df["report_date"]), {canonical_date})
        self.assertEqual(sum(counts.values()), 2)

    def test_normalize_confirms_missing_but_rejects_invalid_source(self) -> None:
        canonical_date = date(2026, 7, 31)
        source_date = date(2026, 7, 1)
        calendar_df = C01.build_expected_calendar(
            canonical_date,
            canonical_date,
            C01.empty_pandas(C01.MACRO_RELEASE_CALENDAR_SCHEMA),
            date(2026, 8, 17),
            datetime.now(timezone.utc),
        )
        pending_df = calendar_df.loc[
            calendar_df["dataset_name"].eq(C03.DATASET_NAME)
            & calendar_df["series_code"].isin({"PPI_YOY", "PPI_YTD"})
        ]
        missing_row = source_row(
            "RPT_ECONOMY_PPI",
            source_date,
            missing_column="BASE_SAME",
        )
        fact_df, counts = C03.normalize_macro_release_response(
            [missing_row],
            "RPT_ECONOMY_PPI",
            pending_df,
            source_date,
            source_date,
            datetime.now(timezone.utc),
        )
        self.assertEqual(len(fact_df), 1)
        self.assertEqual(counts[("PPI_YOY", canonical_date)], 0)

        invalid_row = source_row("RPT_ECONOMY_PPI", source_date)
        invalid_row["BASE_SAME"] = np.inf
        with self.assertRaisesRegex(ValueError, "NaN/Inf"):
            C03.normalize_macro_release_response(
                [invalid_row],
                "RPT_ECONOMY_PPI",
                pending_df,
                source_date,
                source_date,
                datetime.now(timezone.utc),
            )

        wrong_date_row = source_row(
            "RPT_ECONOMY_PPI",
            date(2026, 7, 2),
        )
        with self.assertRaisesRegex(ValueError, "报告月 1 日"):
            C03.normalize_macro_release_response(
                [wrong_date_row],
                "RPT_ECONOMY_PPI",
                pending_df,
                date(2026, 7, 2),
                date(2026, 7, 2),
                datetime.now(timezone.utc),
            )

    def test_gdp_source_month_start_maps_to_quarter_end(self) -> None:
        canonical_date = date(2026, 6, 30)
        source_date = date(2026, 6, 1)
        calendar_df = C01.build_expected_calendar(
            canonical_date,
            canonical_date,
            C01.empty_pandas(C01.MACRO_RELEASE_CALENDAR_SCHEMA),
            date(2026, 7, 16),
            datetime.now(timezone.utc),
        )
        pending_df = calendar_df.loc[
            calendar_df["dataset_name"].eq(C03.DATASET_NAME)
            & calendar_df["series_code"].str.startswith("GDP_")
        ]

        fact_df, counts = C03.normalize_macro_release_response(
            [source_row("RPT_ECONOMY_GDP", source_date)],
            "RPT_ECONOMY_GDP",
            pending_df,
            source_date,
            source_date,
            datetime.now(timezone.utc),
        )
        self.assertEqual(len(fact_df), 4)
        self.assertEqual(sum(counts.values()), 4)
        self.assertEqual(set(fact_df["report_date"]), {canonical_date})

        invalid_source_date = date(2026, 5, 1)
        with self.assertRaisesRegex(ValueError, "不是季度末月份"):
            C03.normalize_macro_release_response(
                [source_row("RPT_ECONOMY_GDP", invalid_source_date)],
                "RPT_ECONOMY_GDP",
                pending_df,
                invalid_source_date,
                invalid_source_date,
                datetime.now(timezone.utc),
            )

    def test_query_rejects_pagination_drift(self) -> None:
        report_name = "RPT_ECONOMY_PPI"
        rows = [
            source_row(report_name, date(2026, 6, 1)),
            source_row(report_name, date(2026, 7, 1)),
        ]
        first_payload = {
            "success": True,
            "result": {"pages": 2, "count": 3, "data": rows},
        }
        second_payload = {
            "success": True,
            "result": {"pages": 2, "count": 4, "data": [rows[0], rows[1]]},
        }
        fake_session = mock.Mock()
        fake_session.get.side_effect = [
            FakeResponse(first_payload),
            FakeResponse(second_payload),
        ]

        with mock.patch.object(C03, "EASTMONEY_PAGE_SIZE", 2):
            with self.assertRaisesRegex(
                C03.MacroReleaseRequestError,
                "元数据在请求期间漂移",
            ):
                C03.query_eastmoney_report_range(
                    fake_session,
                    report_name,
                    date(2026, 5, 1),
                    date(2026, 7, 1),
                )

    def test_empty_lake_real_shape_and_idempotent_no_api(self) -> None:
        canonical_date = date(2026, 7, 31)
        source_date = date(2026, 7, 1)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, canonical_date)

            session = FakeEastmoneySession(all_report_rows(source_date))
            result = run_c03(lake_root, canonical_date, session)
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertTrue(session.closed)
            self.assertEqual(len(session.calls), 3)
            for call in session.calls:
                self.assertIn("REPORT_DATE>='2026-07-01'", call["params"]["filter"])

            fact_df, exact = C03.read_optional_fact(
                lake_root / "silver" / C03.TABLE_NAME
            )
            calendar_df = C03.read_macro_calendar(
                lake_root / "silver" / C03.CALENDAR_TABLE_NAME
            )
            self.assertTrue(exact)
            self.assertEqual(len(fact_df), 13)
            self.assertTrue(calendar_df["fetch_result_status"].eq("success").all())
            self.assertTrue(calendar_df["quality_status"].eq("passed").all())

            second_result = run_c03(lake_root, canonical_date, None)
            self.assertEqual(second_result.exit_code, 0, second_result.output)
            self.assertIn("session not created", second_result.output)

    def test_existing_fact_repairs_stale_calendar_without_api(self) -> None:
        canonical_date = date(2026, 7, 31)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, canonical_date)
            first_result = run_c03(
                lake_root,
                canonical_date,
                FakeEastmoneySession(all_report_rows(date(2026, 7, 1))),
            )
            self.assertEqual(first_result.exit_code, 0, first_result.output)

            calendar_path = lake_root / "silver" / C03.CALENDAR_TABLE_NAME
            calendar_df = C03.read_macro_calendar(calendar_path)
            stale_index = calendar_df.index[
                calendar_df["series_code"].eq("PPI_YOY")
            ][0]
            calendar_df.at[stale_index, "is_fetch_completed"] = False
            calendar_df.at[stale_index, "fetch_result_status"] = "pending"
            calendar_df.at[stale_index, "is_data_missing"] = False
            calendar_df.at[stale_index, "actual_record_count"] = 0
            calendar_df.at[stale_index, "quality_status"] = "pending"
            calendar_df.at[stale_index, "quality_reason"] = "等待事实采集。"
            calendar_df.at[stale_index, "fetch_run_id"] = None
            calendar_df.at[stale_index, "fetch_completed_at"] = None
            calendar_df.at[stale_index, "quality_checked_at"] = None
            calendar_df.at[stale_index, "updated_at"] = datetime.now(timezone.utc)
            C03.commit_calendar_partition(
                calendar_df,
                lake_root,
                (C03.DATASET_NAME, 2026, 7),
            )

            repair_result = run_c03(lake_root, canonical_date, None)
            self.assertEqual(repair_result.exit_code, 0, repair_result.output)
            self.assertIn("state_repaired: rows=1", repair_result.output)

    def test_missing_series_is_empty_confirmed_after_formal_reread(self) -> None:
        canonical_date = date(2026, 7, 31)
        rows_by_report = all_report_rows(date(2026, 7, 1))
        rows_by_report["RPT_ECONOMY_PPI"][0]["BASE_SAME"] = None

        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, canonical_date)
            result = run_c03(
                lake_root,
                canonical_date,
                FakeEastmoneySession(rows_by_report),
            )
            self.assertEqual(result.exit_code, 0, result.output)

            fact_df, _ = C03.read_optional_fact(
                lake_root / "silver" / C03.TABLE_NAME
            )
            calendar_df = C03.read_macro_calendar(
                lake_root / "silver" / C03.CALENDAR_TABLE_NAME
            )
            ppi_row = calendar_df.loc[
                calendar_df["series_code"].eq("PPI_YOY")
            ].iloc[0]
            self.assertEqual(len(fact_df), 12)
            self.assertEqual(ppi_row["fetch_result_status"], "empty_confirmed")
            self.assertEqual(ppi_row["quality_status"], "warning")
            self.assertTrue(ppi_row["is_data_missing"])
            self.assertEqual(ppi_row["actual_record_count"], 0)

    def test_explicit_formal_write_gate_precedes_lake_and_api(self) -> None:
        sentinel = mock.Mock(side_effect=AssertionError("不得触发 I/O"))
        with mock.patch.object(C03, "read_macro_calendar", sentinel), mock.patch.object(
            C03,
            "create_eastmoney_session",
            sentinel,
        ):
            result = CliRunner().invoke(
                C03.main,
                [
                    "--lake-root",
                    str(C03.settings.futures_lake_root.resolve()),
                    "--start-date",
                    "2026-07-31",
                    "--end-date",
                    "2026-07-31",
                    "--write",
                ],
            )

        self.assertEqual(result.exit_code, 2, result.output)
        self.assertIn("禁止写入", result.output)
        sentinel.assert_not_called()

    def test_fact_second_move_failure_restores_old_leaf(self) -> None:
        canonical_date = date(2026, 7, 31)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, canonical_date)
            result = run_c03(
                lake_root,
                canonical_date,
                FakeEastmoneySession(all_report_rows(date(2026, 7, 1))),
            )
            self.assertEqual(result.exit_code, 0, result.output)

            fact_path = lake_root / "silver" / C03.TABLE_NAME
            before_df, _ = C03.read_optional_fact(fact_path)
            changed_df = before_df.copy()
            changed_df.loc[
                changed_df["series_code"].eq("PPI_YOY"),
                "value",
            ] = 99.0
            original_move = shutil.move

            def fail_staging_install(source, destination, *args, **kwargs):
                if ".staging-" in str(source) and str(destination).endswith("month=7"):
                    raise OSError("injected second move failure")
                return original_move(source, destination, *args, **kwargs)

            with mock.patch.object(C03.shutil, "move", side_effect=fail_staging_install):
                with self.assertRaisesRegex(OSError, "injected"):
                    C03.commit_complete_fact_partition(
                        changed_df,
                        lake_root,
                        (2026, 7),
                    )

            after_df, exact = C03.read_optional_fact(fact_path)
            self.assertTrue(exact)
            self.assertEqual(
                C03.table_digest(before_df, C03.MACRO_RELEASE_SCHEMA, C03.PRIMARY_KEY),
                C03.table_digest(after_df, C03.MACRO_RELEASE_SCHEMA, C03.PRIMARY_KEY),
            )
            residues = [
                path
                for path in (lake_root / "silver").iterdir()
                if path.name.startswith(f".{C03.TABLE_NAME}.")
            ]
            self.assertEqual(residues, [])

    def test_fact_staging_failure_leaves_no_residue(self) -> None:
        series = C03.SERIES_BY_CODE["PPI_YOY"]
        fact_df = pd.DataFrame([{
            "series_code": series.series_code,
            "report_date": date(2026, 7, 31),
            "available_date": date(2026, 8, 10),
            "value": 3.5,
            "source": f"Eastmoney_{series.source_api}",
            "updated_at": datetime.now(timezone.utc),
            "year": 2026,
            "month": 7,
        }])
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            with mock.patch.object(
                C03.ds,
                "write_dataset",
                side_effect=OSError("injected staging failure"),
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    C03.commit_complete_fact_partition(
                        fact_df,
                        lake_root,
                        (2026, 7),
                    )

            silver_root = lake_root / "silver"
            residues = (
                list(silver_root.glob(f".{C03.TABLE_NAME}.*"))
                if silver_root.is_dir()
                else []
            )
            self.assertEqual(residues, [])

    def test_compatible_old_metadata_upgrades_without_api(self) -> None:
        canonical_date = date(2026, 7, 31)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, canonical_date)
            result = run_c03(
                lake_root,
                canonical_date,
                FakeEastmoneySession(all_report_rows(date(2026, 7, 1))),
            )
            self.assertEqual(result.exit_code, 0, result.output)

            fact_path = lake_root / "silver" / C03.TABLE_NAME
            fact_df, _ = C03.read_optional_fact(fact_path)
            old_metadata = dict(C03.MACRO_RELEASE_SCHEMA.metadata)
            old_metadata[b"schema_version"] = b"1.0.0"
            old_schema = C03.MACRO_RELEASE_SCHEMA.with_metadata(old_metadata)

            shutil.rmtree(fact_path)
            fact_path.mkdir(parents=True)
            old_file_schema = pa.schema(
                [
                    field
                    for field in old_schema
                    if field.name not in C03.PARTITION_COLUMNS
                ],
                metadata=old_schema.metadata,
            )
            pq.write_table(
                pa.Table.from_batches([], schema=old_file_schema),
                fact_path / "schema.parquet",
            )
            old_table = pa.Table.from_pylist(
                C03.pandas_to_arrow(
                    fact_df,
                    C03.MACRO_RELEASE_SCHEMA,
                ).to_pylist(),
                schema=old_schema,
            )
            ds.write_dataset(
                old_table,
                fact_path,
                format="parquet",
                partitioning=C03.FACT_PARTITIONING,
                existing_data_behavior="delete_matching",
            )

            _, exact_before = C03.read_optional_fact(fact_path)
            self.assertFalse(exact_before)
            upgrade_result = run_c03(lake_root, None, None)
            self.assertEqual(upgrade_result.exit_code, 0, upgrade_result.output)
            self.assertIn("metadata_upgraded", upgrade_result.output)
            upgraded_df, exact_after = C03.read_optional_fact(fact_path)
            self.assertTrue(exact_after)
            self.assertEqual(len(upgraded_df), 13)


if __name__ == "__main__":
    unittest.main()
