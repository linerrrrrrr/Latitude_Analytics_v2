"""验证品种日历由 date=None 完整合约目录构建及 c02 的上游消费边界。

本测试只验证 c02 行为，不重新定义 c01 的完整表级契约。
"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, time, timezone
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

PROJECT_ROOT = candidate_root
COLLECTION_DIR = PROJECT_ROOT / "02_Futures_Lakehouse"
MARKET_WORKFLOW_DIR = COLLECTION_DIR / "a01_Futures_Market_Data"
sys.path.insert(0, str(COLLECTION_DIR))
sys.path.insert(0, str(MARKET_WORKFLOW_DIR))

from config.data_contracts import (  # noqa: E402
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    pandas_to_arrow,
)
import b02_futures_variety_calendar as variety_calendar  # noqa: E402
from config import jqdata_connection  # noqa: E402


TRADING_DATE = date(2024, 1, 3)
UPDATED_AT = datetime(2026, 8, 10, tzinfo=timezone.utc)


class FakeJQData:
    def __init__(self) -> None:
        self.calls: list[tuple[list[str], object]] = []

    def get_all_securities(
        self,
        types: list[str],
        *,
        date: object,
    ) -> pd.DataFrame:
        self.calls.append((types, date))
        codes = [
            "RB2405.XSGE",
            "A2405.XDCE",
            "AP2405.XZCE",
            "IF2403.CCFX",
            "RB8888.XSGE",
            "A9998.XDCE",
            "AP9999.XZCE",
            "RB.XSGE",
        ]
        return pd.DataFrame(
            {
                "start_date": [pd.Timestamp("2023-01-01").date()] * len(codes),
                "end_date": [pd.Timestamp("2024-05-15").date()] * len(codes),
            },
            index=pd.Index(codes, name=None),
        )


class FuturesVarietyCalendarCompleteCatalogTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.lake_root = pathlib.Path(temporary_directory.name)
        silver_root = self.lake_root / "silver"
        # 这些未被 c02 使用的 c01 专属字段刻意采用非正式值，用来确认消费者
        # 只确认物理 Schema，并信任生产者已经正式提交的主键、水位和业务语义。
        trade_frame = pd.DataFrame(
            [
                {
                    "calendar_date": TRADING_DATE,
                    "date_key": "20240103",
                    "is_trading_day": True,
                    "weekday": 3,
                    "is_weekend": False,
                    "source": "test_fixture",
                    "calendar_name": "China Futures",
                    "calendar_timezone": "Asia/Shanghai",
                    "effective_after": time(15, 0),
                    "updated_at": UPDATED_AT,
                    "year": 2024,
                }
            ],
            columns=TRADE_CALENDAR_SCHEMA.names,
        )
        trade_partitioning = ds.partitioning(
            pa.schema([TRADE_CALENDAR_SCHEMA.field("year")]),
            flavor="hive",
        )
        ds.write_dataset(
            pandas_to_arrow(trade_frame, TRADE_CALENDAR_SCHEMA),
            silver_root / "dim_trade_calendar",
            format="parquet",
            partitioning=trade_partitioning,
        )

    def test_complete_catalog_build_and_commit_do_not_use_fact_whitelist(self) -> None:
        fake_jqdata = FakeJQData()
        with mock.patch.object(
            jqdata_connection,
            "authenticate_jqdata",
            return_value=fake_jqdata,
        ):
            frame = variety_calendar.collect(
                self.lake_root,
                TRADING_DATE,
                TRADING_DATE,
            )

        self.assertEqual(fake_jqdata.calls, [(["futures"], None)])
        expected_pairs = {
            ("XSGE", "RB"),
            ("XDCE", "A"),
            ("XZCE", "AP"),
            ("CCFX", "IF"),
        }
        self.assertEqual(
            set(
                frame[["exchange_code", "underlying_code"]].itertuples(
                    index=False,
                    name=None,
                )
            ),
            expected_pairs,
        )
        self.assertTrue(frame["active_contract_count"].eq(1).all())

        committed_rows = variety_calendar.commit_partitions(
            frame,
            self.lake_root,
            TRADING_DATE,
            TRADING_DATE,
        )
        self.assertEqual(committed_rows, 4)
        partitioning = ds.partitioning(
            pa.schema(
                [
                    FUTURES_VARIETY_CALENDAR_SCHEMA.field(name)
                    for name in ["exchange_code", "year", "month"]
                ]
            ),
            flavor="hive",
        )
        committed = ds.dataset(
            self.lake_root / "silver" / "dim_futures_variety_calendar",
            format="parquet",
            partitioning=partitioning,
        ).to_table(columns=FUTURES_VARIETY_CALENDAR_SCHEMA.names).to_pandas()
        self.assertEqual(
            set(
                committed[["exchange_code", "underlying_code"]].itertuples(
                    index=False,
                    name=None,
                )
            ),
            expected_pairs,
        )

    def test_collect_does_not_recheck_upstream_natural_date_coverage(self) -> None:
        fake_jqdata = FakeJQData()
        with mock.patch.object(
            jqdata_connection,
            "authenticate_jqdata",
            return_value=fake_jqdata,
        ):
            frame = variety_calendar.collect(
                self.lake_root,
                date(2024, 1, 2),
                date(2024, 1, 4),
            )

        # 测试湖只含 1 月 3 日；c02 直接消费该正式上游结果，不重建并比较逐日列表。
        self.assertEqual(fake_jqdata.calls, [(["futures"], None)])
        self.assertEqual(set(frame["trading_date"]), {TRADING_DATE})
        self.assertEqual(len(frame), 4)


if __name__ == "__main__":
    unittest.main()
