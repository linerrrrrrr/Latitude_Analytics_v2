"""隔离验证 a03/b03 只从正式事实修复陈旧 OHLC 日历状态。"""

from __future__ import annotations

import hashlib
import json
import pathlib
import tempfile
import types
import unittest
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from click.testing import CliRunner


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
NOTEBOOK_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a03_External_Market_Data"
    / "b03_overseas_futures.ipynb"
)
NORMAL_DATE = date(2026, 7, 16)
ANOMALOUS_DATE = date(2026, 7, 17)


def load_notebook_module() -> types.ModuleType:
    """直接加载 Notebook 权威源，不依赖尚待统一导出的 .py。"""
    notebook = json.loads(NOTEBOOK_PATH.read_text(encoding="utf-8"))
    source = "\n\n".join(
        "".join(cell["source"])
        for cell in notebook["cells"]
        if cell["cell_type"] == "code"
        and cell["id"] not in {"b03-c03-05", "b03-c03-21"}
    )

    module = types.ModuleType("test_b03_c03_notebook_source")
    module.__file__ = str(NOTEBOOK_PATH)
    exec(compile(source, str(NOTEBOOK_PATH), "exec"), module.__dict__)
    return module


def write_partitioned_table(
    module: types.ModuleType,
    frame: pd.DataFrame,
    schema: pa.Schema,
    partition_columns: list[str],
    table_path: pathlib.Path,
) -> None:
    """按生产表相同的 Hive 目录和根级 0 行 marker 写隔离输入。"""
    table_path.mkdir(parents=True)
    file_schema = pa.schema(
        [field for field in schema if field.name not in partition_columns],
        metadata=schema.metadata,
    )
    pq.write_table(
        pa.Table.from_batches([], schema=file_schema),
        table_path / "schema.parquet",
    )

    partitioning = ds.partitioning(
        pa.schema([schema.field(name) for name in partition_columns]),
        flavor="hive",
    )
    ds.write_dataset(
        module.pandas_to_arrow(frame.loc[:, schema.names], schema),
        table_path,
        format="parquet",
        partitioning=partitioning,
        existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet",
    )


def parquet_hashes(table_path: pathlib.Path) -> dict[str, str]:
    """记录事实文件逐文件摘要，证明状态修复没有重写或改值。"""
    return {
        str(path.relative_to(table_path)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(table_path.rglob("*.parquet"))
    }


class OverseasStateRepairTests(unittest.TestCase):
    def test_stale_warning_and_passed_are_repaired_without_api(self) -> None:
        module = load_notebook_module()
        checked_at = datetime.now(timezone.utc) - timedelta(seconds=2)

        normal_raw = pd.DataFrame([{
            "id": "normal-source-id",
            "code": "NORMAL",
            "name": "正常境外期货",
            "day": NORMAL_DATE,
            "open": 100.0,
            "close": 101.0,
            "low": 99.0,
            "high": 102.0,
            "volume": 10.0,
            "change_pct": float("nan"),
            "amplitude": float("nan"),
            "pre_close": 100.0,
        }])
        anomalous_raw = pd.DataFrame([{
            "id": "anomalous-source-id",
            "code": "ANOMALOUS",
            "name": "异常境外期货",
            "day": ANOMALOUS_DATE,
            "open": 326.2,
            "close": 322.5,
            "low": 322.0,
            "high": 325.0,
            "volume": 12.0,
            "change_pct": -1.0,
            "amplitude": 2.0,
            "pre_close": None,
        }])
        fact_df = pd.concat(
            [
                module.normalize_overseas_futures_response(
                    normal_raw,
                    NORMAL_DATE,
                    checked_at,
                ),
                module.normalize_overseas_futures_response(
                    anomalous_raw,
                    ANOMALOUS_DATE,
                    checked_at,
                ),
            ],
            ignore_index=True,
        )
        fact_df = module.validate_overseas_futures_frame(
            fact_df,
            "隔离测试事实",
        )

        # 正常事实故意保留陈旧 warning；异常事实故意保留陈旧 passed。
        calendar_rows = []
        for observation_date, quality_status, quality_reason in [
            (NORMAL_DATE, "warning", "陈旧 warning，不应继续保留。"),
            (ANOMALOUS_DATE, "passed", "陈旧 passed，遗漏来源 OHLC 异常。"),
        ]:
            calendar_rows.append({
                "dataset_name": module.DATASET_NAME,
                "entity_code": module.ENTITY_CODE,
                "observation_date": observation_date,
                "is_fetch_required": True,
                "requirement_reason": "隔离测试 required 格点",
                "is_fetch_completed": True,
                "fetch_result_status": "success",
                "is_data_missing": False,
                "actual_record_count": 1,
                "quality_status": quality_status,
                "quality_reason": quality_reason,
                "fetch_run_id": "stale-run",
                "fetch_completed_at": checked_at,
                "quality_checked_at": checked_at,
                "updated_at": checked_at,
                "year": observation_date.year,
                "month": observation_date.month,
            })
        calendar_df = pd.DataFrame(calendar_rows)

        with tempfile.TemporaryDirectory(prefix="b03-c03-repair-") as directory:
            lake_root = pathlib.Path(directory)
            silver_root = lake_root / "silver"
            fact_path = silver_root / module.TABLE_NAME
            calendar_path = silver_root / module.CALENDAR_TABLE_NAME
            write_partitioned_table(
                module,
                fact_df,
                module.OVERSEAS_FUTURES_DAILY_SCHEMA,
                module.PARTITION_COLUMNS,
                fact_path,
            )
            write_partitioned_table(
                module,
                calendar_df,
                module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                module.CALENDAR_PARTITION_COLUMNS,
                calendar_path,
            )
            fact_hashes_before = parquet_hashes(fact_path)

            api_call_count = 0

            def forbidden_authenticate(*args: object, **kwargs: object) -> None:
                nonlocal api_call_count
                del args, kwargs
                api_call_count += 1
                raise AssertionError("状态修复不得认证或访问 JQData。")

            module.authenticate_jqdata = forbidden_authenticate
            result = CliRunner().invoke(
                module.main,
                (
                    "--lake-root",
                    str(lake_root),
                    "--start-date",
                    NORMAL_DATE.isoformat(),
                    "--end-date",
                    ANOMALOUS_DATE.isoformat(),
                    "--write",
                ),
            )

            self.assertEqual(
                result.exit_code,
                0,
                msg=f"output={result.output!r}; exception={result.exception!r}",
            )
            self.assertEqual(api_call_count, 0)
            self.assertIn("state_repaired: grids=2", result.output)
            self.assertIn("api_requests=0", result.output)
            self.assertEqual(parquet_hashes(fact_path), fact_hashes_before)

            repaired_calendar_dataset = module.open_exact_dataset(
                calendar_path,
                module.CALENDAR_PARTITIONING,
                module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                "隔离测试修复后日历",
            )
            repaired_calendar_df = module.validate_calendar_frame(
                module.arrow_to_pandas(
                    repaired_calendar_dataset.to_table(
                        columns=module.EXTERNAL_MARKET_CALENDAR_SCHEMA.names
                    ),
                    module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
                ),
                "隔离测试修复后",
            ).set_index("observation_date")

            normal_row = repaired_calendar_df.loc[NORMAL_DATE]
            self.assertEqual(normal_row["quality_status"], "passed")
            self.assertEqual(
                normal_row["quality_reason"],
                module.calendar_completion_result(1, None)[3],
            )

            warning_reason = module.ohlc_relation_warning_map(fact_df)[ANOMALOUS_DATE]
            anomalous_row = repaired_calendar_df.loc[ANOMALOUS_DATE]
            self.assertEqual(anomalous_row["quality_status"], "warning")
            self.assertEqual(
                anomalous_row["quality_reason"],
                module.calendar_completion_result(1, warning_reason)[3],
            )

            # 事实中的供应商原值仍保持 open > high，没有被状态修复改写。
            fact_after = module.read_optional_fact(fact_path).set_index("snapshot_date")
            self.assertTrue(pd.isna(fact_after.loc[NORMAL_DATE, "change_pct"]))
            self.assertTrue(pd.isna(fact_after.loc[NORMAL_DATE, "amplitude"]))
            self.assertEqual(float(fact_after.loc[ANOMALOUS_DATE, "open"]), 326.2)
            self.assertEqual(float(fact_after.loc[ANOMALOUS_DATE, "high"]), 325.0)


if __name__ == "__main__":
    unittest.main()
