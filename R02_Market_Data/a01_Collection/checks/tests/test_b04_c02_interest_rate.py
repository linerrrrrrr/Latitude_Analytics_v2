from __future__ import annotations

import hashlib
import importlib.util
import pathlib
import sys
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


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
C01_PATH = (
    PROJECT_ROOT
    / "R02_Market_Data/a01_Collection"
    / "b04_Macro_And_Interest_Rates"
    / "c01_macro_release_calendar.py"
)
C02_PATH = C01_PATH.with_name("c02_interest_rate.py")
C02_NOTEBOOK_PATH = C02_PATH.with_suffix(".ipynb")


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


C01 = load_module("test_b04_c01_for_c02", C01_PATH)
C02 = load_module("test_b04_c02", C02_PATH)
TRANSACTION = sys.modules[C02.StagedPathTransaction.__module__]


def frame_digest(frame, schema, primary_key):
    ordered_df = frame.sort_values(primary_key).reset_index(drop=True)
    table = C02.pandas_to_arrow(ordered_df.loc[:, schema.names], schema)
    stable_table = pa.Table.from_pylist(table.to_pylist(), schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(stable_table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


class FakeTushareClient:
    def __init__(self, response: pd.DataFrame) -> None:
        self.response = response
        self.calls: list[dict[str, str]] = []

    def shibor(self, **kwargs) -> pd.DataFrame:
        self.calls.append(dict(kwargs))
        return self.response.copy()


def shibor_response(
    observation_date: date,
    *,
    missing_column: str | None = None,
) -> pd.DataFrame:
    row: dict[str, object] = {
        "date": observation_date.strftime("%Y%m%d"),
    }
    for offset, source_column in enumerate(
        C02.SOURCE_COLUMN_TO_SERIES,
        start=1,
    ):
        row[source_column] = float(offset)
    if missing_column is not None:
        row[missing_column] = np.nan
    return pd.DataFrame([row])


def build_calendar(
    lake_root: pathlib.Path,
    start_date: date,
    end_date: date,
) -> None:
    runner = CliRunner()
    result = runner.invoke(
        C01.main,
        [
            "--lake-root",
            str(lake_root),
            "--start-date",
            start_date.isoformat(),
            "--end-date",
            end_date.isoformat(),
            "--write",
        ],
    )
    if result.exit_code != 0:
        raise AssertionError(result.output) from result.exception


def run_c02(
    lake_root: pathlib.Path,
    start_date: date | None,
    end_date: date | None,
    client: FakeTushareClient | None,
) -> object:
    arguments = ["--lake-root", str(lake_root)]
    if start_date is not None:
        arguments.extend([
            "--start-date",
            start_date.isoformat(),
            "--end-date",
            end_date.isoformat(),
        ])
    arguments.append("--write")

    if client is None:
        replacement = mock.Mock(side_effect=AssertionError("不应创建 Tushare 客户端"))
    else:
        replacement = mock.Mock(return_value=client)

    with mock.patch.object(C02, "create_tushare_client", replacement):
        result = CliRunner().invoke(C02.main, arguments)
    return result


class InterestRateNotebookTests(unittest.TestCase):
    def test_notebook_is_readable_split_and_exportable(self) -> None:
        notebook = nbformat.read(C02_NOTEBOOK_PATH, as_version=4)
        nbformat.validate(notebook)
        source, _ = PythonExporter().from_notebook_node(notebook)
        compile(source, str(C02_NOTEBOOK_PATH), "exec")

        self.assertGreaterEqual(len(notebook.cells), 20)
        self.assertGreaterEqual(
            sum(cell.cell_type == "code" for cell in notebook.cells),
            10,
        )
        self.assertIn("#", source)

    def test_normalize_keeps_exact_grids_and_confirms_one_missing_tenor(self) -> None:
        observation_date = date(2026, 7, 17)
        calendar_df = C01.build_expected_calendar(
            observation_date,
            observation_date,
            C01.empty_pandas(C01.MACRO_RELEASE_CALENDAR_SCHEMA),
            observation_date,
            datetime.now(timezone.utc),
        )
        pending_df = calendar_df.loc[
            calendar_df["dataset_name"].eq("interest_rate")
        ]
        raw_df = shibor_response(
            observation_date,
            missing_column="2w",
        )
        # 范围内额外返回日必须通过来源校验，但不能成为非待办事实。
        extra_df = shibor_response(date(2026, 7, 16))
        raw_df = pd.concat([raw_df, extra_df], ignore_index=True)

        fact_df, counts = C02.normalize_shibor_response(
            raw_df,
            pending_df,
            date(2026, 7, 16),
            observation_date,
            datetime.now(timezone.utc),
        )

        self.assertEqual(len(fact_df), 7)
        self.assertEqual(
            counts[("SHIBOR_2W", observation_date)],
            0,
        )
        self.assertEqual(sum(counts.values()), 7)
        self.assertEqual(set(fact_df["observation_date"]), {observation_date})

        empty_fact_df, empty_counts = C02.normalize_shibor_response(
            pd.DataFrame(columns=C02.SHIBOR_FIELDS),
            pending_df,
            observation_date,
            observation_date,
            datetime.now(timezone.utc),
        )
        self.assertTrue(empty_fact_df.empty)
        self.assertEqual(sum(empty_counts.values()), 0)

        with self.assertRaisesRegex(ValueError, "缺少必需列"):
            C02.normalize_shibor_response(
                pd.DataFrame(),
                pending_df,
                observation_date,
                observation_date,
                datetime.now(timezone.utc),
            )

    def test_normalize_rejects_duplicate_out_of_range_and_nonfinite_values(self) -> None:
        observation_date = date(2026, 7, 17)
        calendar_df = C01.build_expected_calendar(
            observation_date,
            observation_date,
            C01.empty_pandas(C01.MACRO_RELEASE_CALENDAR_SCHEMA),
            observation_date,
            datetime.now(timezone.utc),
        )
        pending_df = calendar_df.loc[
            calendar_df["dataset_name"].eq("interest_rate")
        ]
        valid_df = shibor_response(observation_date)

        with self.assertRaisesRegex(ValueError, "重复日期"):
            C02.normalize_shibor_response(
                pd.concat([valid_df, valid_df], ignore_index=True),
                pending_df,
                observation_date,
                observation_date,
                datetime.now(timezone.utc),
            )

        with self.assertRaisesRegex(ValueError, "越出请求范围"):
            C02.normalize_shibor_response(
                shibor_response(date(2026, 7, 16)),
                pending_df,
                observation_date,
                observation_date,
                datetime.now(timezone.utc),
            )

        invalid_df = valid_df.copy()
        invalid_df.loc[0, "on"] = np.inf
        with self.assertRaisesRegex(ValueError, "NaN/Inf"):
            C02.normalize_shibor_response(
                invalid_df,
                pending_df,
                observation_date,
                observation_date,
                datetime.now(timezone.utc),
            )

        boolean_df = valid_df.copy()
        boolean_df["on"] = boolean_df["on"].astype("object")
        boolean_df.loc[0, "on"] = True
        with self.assertRaisesRegex(ValueError, "布尔值"):
            C02.normalize_shibor_response(
                boolean_df,
                pending_df,
                observation_date,
                observation_date,
                datetime.now(timezone.utc),
            )

    def test_empty_lake_partial_month_and_idempotent_state(self) -> None:
        first_date = date(2026, 7, 17)
        second_date = date(2026, 7, 20)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, first_date, second_date)

            first_client = FakeTushareClient(shibor_response(first_date))
            first_result = run_c02(
                lake_root,
                first_date,
                first_date,
                first_client,
            )
            self.assertEqual(first_result.exit_code, 0, first_result.output)
            self.assertEqual(len(first_client.calls), 1)
            self.assertEqual(first_client.calls[0]["fields"], ",".join(C02.SHIBOR_FIELDS))

            second_client = FakeTushareClient(shibor_response(second_date))
            second_result = run_c02(
                lake_root,
                second_date,
                second_date,
                second_client,
            )
            self.assertEqual(second_result.exit_code, 0, second_result.output)

            fact_df, exact = C02.read_optional_fact(
                lake_root / "silver" / C02.TABLE_NAME
            )
            self.assertTrue(exact)
            self.assertEqual(len(fact_df), 16)
            self.assertEqual(
                set(fact_df["observation_date"]),
                {first_date, second_date},
            )

            # 已完整范围不得重新创建客户端或访问 API。
            third_result = run_c02(
                lake_root,
                first_date,
                second_date,
                None,
            )
            self.assertEqual(third_result.exit_code, 0, third_result.output)
            self.assertIn("Tushare client not created", third_result.output)

    def test_missing_tenor_is_empty_confirmed_after_fact_reread(self) -> None:
        observation_date = date(2026, 7, 17)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, observation_date, observation_date)
            client = FakeTushareClient(
                shibor_response(observation_date, missing_column="2w")
            )
            result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                client,
            )
            self.assertEqual(result.exit_code, 0, result.output)

            fact_df, _ = C02.read_optional_fact(
                lake_root / "silver" / C02.TABLE_NAME
            )
            calendar_df = C02.read_interest_calendar(
                lake_root / "silver" / C02.CALENDAR_TABLE_NAME
            )
            missing_row = calendar_df.loc[
                calendar_df["series_code"].eq("SHIBOR_2W")
            ].iloc[0]

            self.assertEqual(len(fact_df), 7)
            self.assertEqual(missing_row["fetch_result_status"], "empty_confirmed")
            self.assertEqual(missing_row["quality_status"], "warning")
            self.assertTrue(missing_row["is_data_missing"])
            self.assertEqual(missing_row["actual_record_count"], 0)

    def test_invalid_response_records_failure_then_valid_retry_completes(self) -> None:
        observation_date = date(2026, 7, 17)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, observation_date, observation_date)

            invalid_df = shibor_response(observation_date)
            invalid_df.loc[0, "on"] = np.inf
            failure_result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                FakeTushareClient(invalid_df),
            )
            self.assertEqual(failure_result.exit_code, 1, failure_result.output)
            calendar_path = lake_root / "silver" / C02.CALENDAR_TABLE_NAME
            failed_calendar_df = C02.read_interest_calendar(calendar_path)
            self.assertTrue(
                failed_calendar_df["fetch_result_status"]
                .eq("permanent_error")
                .all()
            )
            self.assertTrue(failed_calendar_df["quality_status"].eq("failed").all())

            retry_result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                FakeTushareClient(shibor_response(observation_date)),
            )
            self.assertEqual(retry_result.exit_code, 0, retry_result.output)
            repaired_calendar_df = C02.read_interest_calendar(calendar_path)
            self.assertTrue(
                repaired_calendar_df["fetch_result_status"].eq("success").all()
            )
            self.assertTrue(repaired_calendar_df["quality_status"].eq("passed").all())

    def test_existing_fact_repairs_stale_calendar_without_api(self) -> None:
        observation_date = date(2026, 7, 17)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, observation_date, observation_date)
            initial_result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                FakeTushareClient(shibor_response(observation_date)),
            )
            self.assertEqual(initial_result.exit_code, 0, initial_result.output)

            calendar_path = lake_root / "silver" / C02.CALENDAR_TABLE_NAME
            calendar_df = C02.read_interest_calendar(calendar_path)
            stale_index = calendar_df.index[
                calendar_df["series_code"].eq("SHIBOR_ON")
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
            C02.commit_calendar_partition(
                calendar_df,
                lake_root,
                (C02.DATASET_NAME, 2026, 7),
            )

            repair_result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                None,
            )
            self.assertEqual(repair_result.exit_code, 0, repair_result.output)
            self.assertIn("state_repaired: rows=1", repair_result.output)

            repaired_df = C02.read_interest_calendar(calendar_path)
            repaired_row = repaired_df.loc[
                repaired_df["series_code"].eq("SHIBOR_ON")
            ].iloc[0]
            self.assertEqual(repaired_row["quality_reason"], C02.STATE_REPAIR_REASON)
            self.assertEqual(repaired_row["actual_record_count"], 1)

    def test_explicit_formal_write_gate_runs_before_lake_or_api_io(self) -> None:
        sentinel = mock.Mock(side_effect=AssertionError("不得读取正式湖"))
        with mock.patch.object(C02, "read_interest_calendar", sentinel), mock.patch.object(
            C02,
            "create_tushare_client",
            sentinel,
        ):
            result = CliRunner().invoke(
                C02.main,
                [
                    "--lake-root",
                    str(C02.settings.futures_lake_root.resolve()),
                    "--start-date",
                    "2026-07-17",
                    "--end-date",
                    "2026-07-17",
                    "--write",
                ],
            )

        self.assertEqual(result.exit_code, 2, result.output)
        self.assertIn("禁止写入", result.output)
        sentinel.assert_not_called()

    def test_fact_leaf_second_move_failure_restores_old_partition(self) -> None:
        observation_date = date(2026, 7, 17)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, observation_date, observation_date)
            result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                FakeTushareClient(shibor_response(observation_date)),
            )
            self.assertEqual(result.exit_code, 0, result.output)

            fact_path = lake_root / "silver" / C02.TABLE_NAME
            before_df, _ = C02.read_optional_fact(fact_path)
            changed_df = before_df.copy()
            changed_df.loc[
                changed_df["series_code"].eq("SHIBOR_ON"),
                "rate",
            ] = 99.0
            original_move = TRANSACTION.os.replace

            def fail_staging_install(source, destination, *args, **kwargs):
                source_text = str(source)
                destination_text = str(destination)
                if ".staging-" in source_text and destination_text.endswith("month=7"):
                    raise OSError("injected second move failure")
                return original_move(source, destination, *args, **kwargs)

            with mock.patch.object(TRANSACTION.os, "replace", side_effect=fail_staging_install):
                with self.assertRaisesRegex(OSError, "injected"):
                    C02.commit_complete_fact_partition(
                        changed_df,
                        lake_root,
                        (2026, 7),
                    )

            after_df, exact = C02.read_optional_fact(fact_path)
            self.assertTrue(exact)
            self.assertEqual(
                frame_digest(before_df, C02.INTEREST_RATE_DAILY_SCHEMA, C02.PRIMARY_KEY),
                frame_digest(after_df, C02.INTEREST_RATE_DAILY_SCHEMA, C02.PRIMARY_KEY),
            )
            residues = [
                path
                for path in (lake_root / "silver").iterdir()
                if path.name.startswith(f".{C02.TABLE_NAME}.")
            ]
            self.assertEqual(residues, [])

    def test_fact_leaf_first_move_failure_keeps_old_partition(self) -> None:
        observation_date = date(2026, 7, 17)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, observation_date, observation_date)
            result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                FakeTushareClient(shibor_response(observation_date)),
            )
            self.assertEqual(result.exit_code, 0, result.output)

            fact_path = lake_root / "silver" / C02.TABLE_NAME
            before_df, _ = C02.read_optional_fact(fact_path)
            original_move = TRANSACTION.os.replace

            def fail_old_partition_move(source, destination, *args, **kwargs):
                if ".backup-" in str(destination) and str(source).endswith("month=7"):
                    raise OSError("injected first move failure")
                return original_move(source, destination, *args, **kwargs)

            with mock.patch.object(TRANSACTION.os, "replace", side_effect=fail_old_partition_move):
                with self.assertRaisesRegex(OSError, "injected"):
                    C02.commit_complete_fact_partition(
                        before_df,
                        lake_root,
                        (2026, 7),
                    )

            after_df, exact = C02.read_optional_fact(fact_path)
            self.assertTrue(exact)
            self.assertEqual(
                frame_digest(before_df, C02.INTEREST_RATE_DAILY_SCHEMA, C02.PRIMARY_KEY),
                frame_digest(after_df, C02.INTEREST_RATE_DAILY_SCHEMA, C02.PRIMARY_KEY),
            )

    def test_calendar_leaf_second_move_failure_restores_old_partition(self) -> None:
        observation_date = date(2026, 7, 17)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, observation_date, observation_date)
            calendar_path = lake_root / "silver" / C02.CALENDAR_TABLE_NAME
            before_df = C02.read_interest_calendar(calendar_path)
            changed_df = before_df.copy()
            changed_df.loc[:, "quality_reason"] = "事务回滚注入测试。"
            changed_df.loc[:, "updated_at"] = datetime.now(timezone.utc)
            original_move = TRANSACTION.os.replace

            def fail_staging_install(source, destination, *args, **kwargs):
                if ".staging-" in str(source) and str(destination).endswith("month=7"):
                    raise OSError("injected calendar second move failure")
                return original_move(source, destination, *args, **kwargs)

            with mock.patch.object(TRANSACTION.os, "replace", side_effect=fail_staging_install):
                with self.assertRaisesRegex(OSError, "injected"):
                    C02.commit_calendar_partition(
                        changed_df,
                        lake_root,
                        (C02.DATASET_NAME, 2026, 7),
                    )

            after_df = C02.read_interest_calendar(calendar_path)
            self.assertEqual(
                frame_digest(before_df, C02.MACRO_RELEASE_CALENDAR_SCHEMA, C02.CALENDAR_PRIMARY_KEY),
                frame_digest(after_df, C02.MACRO_RELEASE_CALENDAR_SCHEMA, C02.CALENDAR_PRIMARY_KEY),
            )
            residues = [
                path
                for path in (lake_root / "silver").iterdir()
                if path.name.startswith(f".{C02.CALENDAR_TABLE_NAME}.")
            ]
            self.assertEqual(residues, [])

    def test_fact_staging_write_failure_leaves_no_residue(self) -> None:
        observation_date = date(2026, 7, 17)
        fact_df = pd.DataFrame([
            {
                "series_code": "SHIBOR_ON",
                "observation_date": observation_date,
                "rate": 1.5,
                "source": C02.SOURCE_NAME,
                "updated_at": datetime.now(timezone.utc),
                "year": 2026,
                "month": 7,
            }
        ])
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            with mock.patch.object(
                C02.ds,
                "write_dataset",
                side_effect=OSError("injected staging write failure"),
            ):
                with self.assertRaisesRegex(OSError, "injected"):
                    C02.commit_complete_fact_partition(
                        fact_df,
                        lake_root,
                        (2026, 7),
                    )

            silver_root = lake_root / "silver"
            residues = (
                [
                    path
                    for path in silver_root.iterdir()
                    if path.name.startswith(f".{C02.TABLE_NAME}.")
                ]
                if silver_root.is_dir()
                else []
            )
            self.assertEqual(residues, [])

    def test_compatible_old_fact_metadata_is_upgraded_without_api(self) -> None:
        observation_date = date(2026, 7, 17)
        with tempfile.TemporaryDirectory() as temporary_dir:
            lake_root = pathlib.Path(temporary_dir) / "lake"
            build_calendar(lake_root, observation_date, observation_date)
            result = run_c02(
                lake_root,
                observation_date,
                observation_date,
                FakeTushareClient(shibor_response(observation_date)),
            )
            self.assertEqual(result.exit_code, 0, result.output)

            fact_path = lake_root / "silver" / C02.TABLE_NAME
            fact_df, _ = C02.read_optional_fact(fact_path)
            old_metadata = dict(C02.INTEREST_RATE_DAILY_SCHEMA.metadata)
            old_metadata[b"schema_version"] = b"1.0.0"
            old_schema = C02.INTEREST_RATE_DAILY_SCHEMA.with_metadata(old_metadata)

            shutil.rmtree(fact_path)
            fact_path.mkdir(parents=True)
            old_file_schema = pa.schema(
                [
                    field
                    for field in old_schema
                    if field.name not in C02.PARTITION_COLUMNS
                ],
                metadata=old_schema.metadata,
            )
            pq.write_table(
                pa.Table.from_batches([], schema=old_file_schema),
                fact_path / "schema.parquet",
            )
            old_table = pa.Table.from_pylist(
                C02.pandas_to_arrow(
                    fact_df,
                    C02.INTEREST_RATE_DAILY_SCHEMA,
                ).to_pylist(),
                schema=old_schema,
            )
            ds.write_dataset(
                old_table,
                fact_path,
                format="parquet",
                partitioning=C02.FACT_PARTITIONING,
                existing_data_behavior="delete_matching",
            )

            _, exact_before = C02.read_optional_fact(fact_path)
            self.assertFalse(exact_before)
            upgrade_result = run_c02(lake_root, None, None, None)
            self.assertEqual(upgrade_result.exit_code, 0, upgrade_result.output)
            self.assertIn("metadata_upgraded", upgrade_result.output)
            upgraded_df, exact_after = C02.read_optional_fact(fact_path)
            self.assertTrue(exact_after)
            self.assertEqual(len(upgraded_df), 8)


if __name__ == "__main__":
    unittest.main()
