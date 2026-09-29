from __future__ import annotations

import hashlib
import pathlib
import sys
import tempfile
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest import mock

import nbformat
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from nbconvert.exporters import PythonExporter


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a03_External_Market_Data"
    / "b02_domestic_spot_basis.ipynb"
)


def load_notebook_module() -> tuple[types.ModuleType, str]:
    notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
    nbformat.validate(notebook)
    source, _ = PythonExporter().from_notebook_node(notebook)
    compile(source, str(NOTEBOOK_PATH), "exec")

    module = types.ModuleType("b02_domestic_spot_basis_test_module")
    module.__file__ = str(NOTEBOOK_PATH.with_suffix(".py"))
    exec(compile(source, str(NOTEBOOK_PATH), "exec"), module.__dict__)
    return module, source


MODULE, EXPORTED_SOURCE = load_notebook_module()
TRANSACTION_MODULE = sys.modules[MODULE.StagedPathTransaction.__module__]


def pending_calendar_frame(observation_date: date) -> pd.DataFrame:
    updated_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    row = {
        "dataset_name": MODULE.DATASET_NAME,
        "entity_code": MODULE.ENTITY_CODE,
        "observation_date": observation_date,
        "is_fetch_required": True,
        "requirement_reason": "中国期货交易日，归档生意社原始日页面。",
        "is_fetch_completed": False,
        "fetch_result_status": "pending",
        "is_data_missing": False,
        "actual_record_count": 0,
        "quality_status": "pending",
        "quality_reason": "等待归档生意社 HTTP 原始响应。",
        "fetch_run_id": None,
        "fetch_completed_at": None,
        "quality_checked_at": None,
        "updated_at": updated_at,
        "year": observation_date.year,
        "month": observation_date.month,
    }
    table = MODULE.pandas_to_arrow(
        pd.DataFrame([row]),
        MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )
    return MODULE.arrow_to_pandas(
        table,
        MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA,
    )


def write_exact_calendar(lake_root: pathlib.Path, frame: pd.DataFrame) -> None:
    table_path = lake_root / "silver" / MODULE.CALENDAR_TABLE_NAME
    table_path.mkdir(parents=True, exist_ok=True)
    file_schema = pa.schema(
        [
            field
            for field in MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA
            if field.name not in MODULE.CALENDAR_PARTITION_COLUMNS
        ],
        metadata=MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA.metadata,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        table_path / "schema.parquet",
    )
    ds.write_dataset(
        MODULE.pandas_to_arrow(
            frame,
            MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ),
        table_path,
        format="parquet",
        partitioning=MODULE.CALENDAR_PARTITIONING,
        existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet",
    )


class FakeResponse:
    def __init__(self, status_code: int, content: bytes) -> None:
        self.status_code = status_code
        self.content = content


class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.get_count = 0
        self.closed = False

    def get(self, _url: str, timeout: int) -> FakeResponse:
        self.get_count += 1
        if timeout != MODULE.REQUEST_TIMEOUT_SECONDS:
            raise AssertionError("请求超时参数被意外改变。")
        return self.response

    def close(self) -> None:
        self.closed = True


class RawArchiveLifecycleTests(unittest.TestCase):
    def test_notebook_has_no_html_parser_or_structured_fact_schema(self) -> None:
        self.assertNotIn("BeautifulSoup", EXPORTED_SOURCE)
        self.assertNotIn("parse_spot_basis", EXPORTED_SOURCE)
        self.assertNotIn("DOMESTIC_SPOT_BASIS_DAILY_SCHEMA", EXPORTED_SOURCE)
        self.assertNotIn("fact_domestic_spot_basis_daily", EXPORTED_SOURCE)

    def test_empty_http_200_bytes_are_valid_raw_content(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            raw_root = pathlib.Path(temporary_directory) / "raw"
            observation_date = date(2026, 8, 14)

            evidence = MODULE.commit_raw_response(
                raw_root,
                observation_date,
                b"",
            )
            leaf_path = MODULE.raw_leaf_path(raw_root, observation_date)

            self.assertEqual((leaf_path / "response.html").read_bytes(), b"")
            self.assertEqual(evidence["byte_count"], 0)
            self.assertEqual(
                evidence["sha256"],
                hashlib.sha256(b"").hexdigest(),
            )
            self.assertEqual(MODULE.inspect_raw_leaf(leaf_path), evidence)

    def test_corrupt_sha_reenters_api_pending(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            raw_root = pathlib.Path(temporary_directory) / "raw"
            observation_date = date(2026, 8, 14)
            MODULE.commit_raw_response(raw_root, observation_date, b"source-bytes")
            leaf_path = MODULE.raw_leaf_path(raw_root, observation_date)
            (leaf_path / "response.sha256").write_text("0" * 64, encoding="utf-8")

            pending_df, repair_df, complete_count = MODULE.plan_raw_grids(
                pending_calendar_frame(observation_date),
                raw_root,
                None,
                None,
            )

            self.assertEqual(len(pending_df), 1)
            self.assertTrue(repair_df.empty)
            self.assertEqual(complete_count, 0)

    def test_first_swap_failure_keeps_old_raw_tree(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            raw_root = pathlib.Path(temporary_directory) / "raw"
            observation_date = date(2026, 8, 14)
            MODULE.commit_raw_response(raw_root, observation_date, b"old-source")
            leaf_path = MODULE.raw_leaf_path(raw_root, observation_date)
            original_move = TRANSACTION_MODULE.os.replace

            def fail_old_target_move(source: str, destination: str):
                if pathlib.Path(source) == leaf_path:
                    raise OSError("injected first move failure")
                return original_move(source, destination)

            with mock.patch.object(
                TRANSACTION_MODULE.os,
                "replace",
                side_effect=fail_old_target_move,
            ):
                with self.assertRaisesRegex(OSError, "first move failure"):
                    MODULE.commit_raw_response(
                        raw_root,
                        observation_date,
                        b"new-source",
                    )

            self.assertEqual((leaf_path / "response.html").read_bytes(), b"old-source")
            self.assertIsNotNone(MODULE.inspect_raw_leaf(leaf_path))
            residues = [
                path.name
                for path in raw_root.iterdir()
                if path.name.startswith((".staging-", ".backup-", ".quarantine-"))
            ]
            self.assertEqual(residues, [])

    def test_http_failure_body_is_preserved_only_as_hidden_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            raw_root = pathlib.Path(temporary_directory) / "raw"
            observation_date = date(2026, 8, 14)
            session = FakeSession(FakeResponse(404, b"not-found-body"))

            with self.assertRaises(MODULE.RawRequestError) as context:
                MODULE.fetch_raw_response(session, observation_date)
            self.assertEqual(context.exception.response_content, b"not-found-body")

            failed_leaf = MODULE.preserve_failed_response(
                raw_root,
                "batch",
                observation_date,
                context.exception.response_content,
            )
            self.assertEqual(
                (failed_leaf / "response.html").read_bytes(),
                b"not-found-body",
            )
            self.assertFalse(
                MODULE.raw_leaf_path(raw_root, observation_date).exists()
            )

    def test_existing_raw_repairs_stale_calendar_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            observation_date = date(2026, 8, 14)
            write_exact_calendar(
                lake_root,
                pending_calendar_frame(observation_date),
            )
            MODULE.commit_raw_response(
                lake_root / MODULE.RAW_RELATIVE_ROOT,
                observation_date,
                b"already-archived",
            )

            with mock.patch.object(
                MODULE,
                "create_http_session",
                side_effect=AssertionError("状态修复不得创建 HTTP Session"),
            ):
                MODULE.main.callback(
                    lake_root=lake_root,
                    start_date=None,
                    end_date=None,
                    write=True,
                )

            calendar_dataset = MODULE.open_exact_dataset(
                lake_root / "silver" / MODULE.CALENDAR_TABLE_NAME,
                MODULE.CALENDAR_PARTITIONING,
                MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "状态修复测试日历",
            )
            repaired_df = MODULE.arrow_to_pandas(
                calendar_dataset.to_table(
                    columns=MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                ),
                MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA,
            )
            self.assertTrue(repaired_df.iloc[0]["is_fetch_completed"])
            self.assertEqual(repaired_df.iloc[0]["actual_record_count"], 1)

    def test_main_archives_exact_bytes_then_is_idempotent_without_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            lake_root = pathlib.Path(temporary_directory)
            observation_date = date(2026, 8, 14)
            write_exact_calendar(
                lake_root,
                pending_calendar_frame(observation_date),
            )
            response_bytes = b"\x00\xffcurrent-page-content"
            fake_session = FakeSession(FakeResponse(200, response_bytes))

            with mock.patch.object(
                MODULE,
                "create_http_session",
                return_value=fake_session,
            ):
                MODULE.main.callback(
                    lake_root=lake_root,
                    start_date=None,
                    end_date=None,
                    write=True,
                )

            leaf_path = MODULE.raw_leaf_path(
                lake_root / MODULE.RAW_RELATIVE_ROOT,
                observation_date,
            )
            self.assertEqual(
                (leaf_path / "response.html").read_bytes(),
                response_bytes,
            )
            self.assertEqual(fake_session.get_count, 1)
            self.assertTrue(fake_session.closed)

            with mock.patch.object(
                MODULE,
                "create_http_session",
                side_effect=AssertionError("幂等运行不应创建 HTTP Session"),
            ):
                MODULE.main.callback(
                    lake_root=lake_root,
                    start_date=None,
                    end_date=None,
                    write=True,
                )

            calendar_dataset = MODULE.open_exact_dataset(
                lake_root / "silver" / MODULE.CALENDAR_TABLE_NAME,
                MODULE.CALENDAR_PARTITIONING,
                MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "测试日历",
            )
            calendar_df = MODULE.arrow_to_pandas(
                calendar_dataset.to_table(
                    columns=MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                ),
                MODULE.EXTERNAL_MARKET_CALENDAR_SCHEMA,
            )
            row = calendar_df.iloc[0]
            self.assertTrue(row["is_fetch_completed"])
            self.assertEqual(row["fetch_result_status"], "success")
            self.assertEqual(row["actual_record_count"], 1)
            self.assertEqual(row["quality_status"], "passed")
            self.assertIn("未解析 HTML", row["quality_reason"])


if __name__ == "__main__":
    unittest.main()
