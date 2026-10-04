"""审计 b01、b02、b03 隔离运行湖的 silver 契约与 raw 原文完整性。

这是本轮主动验证使用的草稿审计入口，不属于正式生产链路。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
sys.path.insert(0, str(PROJECT_ROOT))

from config.data_contracts import (  # noqa: E402
    EXTERNAL_INDEX_DAILY_SCHEMA,
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_MISSING_BAR_SCHEMA,
    FUTURES_POSITION_RANK_DAILY_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    OVERSEAS_FUTURES_DAILY_SCHEMA,
    TRADE_CALENDAR_SCHEMA,
    validate_arrow_table,
)


SCHEMAS = (
    TRADE_CALENDAR_SCHEMA,
    FUTURES_VARIETY_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    FUTURES_MISSING_BAR_SCHEMA,
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    FUTURES_POSITION_RANK_DAILY_SCHEMA,
    FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
    FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    OVERSEAS_FUTURES_DAILY_SCHEMA,
    EXTERNAL_INDEX_DAILY_SCHEMA,
)


def metadata_text(schema: pa.Schema, key: str) -> str:
    value = (schema.metadata or {}).get(key.encode("utf-8"))
    if value is None:
        raise KeyError(f"Schema metadata 缺少 {key!r}。")
    return value.decode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake-root", type=pathlib.Path, required=True)
    args = parser.parse_args()

    lake_root = args.lake_root.resolve()
    silver_root = lake_root / "silver"
    domestic_raw_root = lake_root / "raw" / "100ppi" / "domestic_spot_basis"
    failures: list[str] = []
    opened_datasets: dict[str, ds.Dataset] = {}

    for schema in SCHEMAS:
        table_name = metadata_text(schema, "table_name")
        table_path = silver_root / table_name
        parquet_files = list(table_path.rglob("*.parquet")) if table_path.is_dir() else []
        if not parquet_files:
            print(json.dumps({"table": table_name, "status": "absent"}, ensure_ascii=False))
            continue

        partition_columns = metadata_text(schema, "partition_columns").split(",")
        primary_key = metadata_text(schema, "primary_key").split(",")
        partition_schema = pa.schema([schema.field(name) for name in partition_columns])
        partitioning = ds.partitioning(partition_schema, flavor="hive")
        dataset = ds.dataset(table_path, format="parquet", partitioning=partitioning)
        opened_datasets[table_name] = dataset

        reconstructed = pa.schema(
            [dataset.schema.field(name) for name in schema.names],
            metadata=dataset.schema.metadata,
        )
        if not reconstructed.equals(schema, check_metadata=True):
            failures.append(f"{table_name}: Schema/metadata 不一致")
            continue

        row_count = 0
        for batch in dataset.scanner(columns=schema.names).to_batches():
            validate_arrow_table(pa.Table.from_batches([batch]), schema)
            row_count += batch.num_rows

        key_result = (
            pl.scan_pyarrow_dataset(dataset)
            .select(
                pl.len().alias("rows"),
                pl.struct(primary_key).n_unique().alias("unique_keys"),
            )
            .collect()
            .row(0, named=True)
        )
        if key_result["rows"] != key_result["unique_keys"]:
            failures.append(
                f"{table_name}: 主键不唯一 rows={key_result['rows']} "
                f"unique_keys={key_result['unique_keys']}"
            )

        date_column = next(
            (
                name
                for name in (
                    "calendar_date",
                    "trading_date",
                    "observation_date",
                    "report_date",
                    "bar_at",
                    "expected_bar_at",
                )
                if name in schema.names
            ),
            None,
        )
        date_bounds = None
        if date_column is not None and row_count:
            date_bounds = (
                pl.scan_pyarrow_dataset(dataset)
                .select(
                    pl.col(date_column).min().alias("minimum"),
                    pl.col(date_column).max().alias("maximum"),
                )
                .collect()
                .row(0, named=True)
            )
            date_bounds = {key: str(value) for key, value in date_bounds.items()}

        print(
            json.dumps(
                {
                    "table": table_name,
                    "status": "passed",
                    "rows": row_count,
                    "parquet_files": len(parquet_files),
                    "date_bounds": date_bounds,
                },
                ensure_ascii=False,
            )
        )

    # 事实完成计数必须来自正式路径复读结果，不能只相信日历中的自报状态。
    def grouped_count_map(
        table_name: str,
        key_columns: list[str],
    ) -> dict[tuple[object, ...], int]:
        dataset = opened_datasets.get(table_name)
        if dataset is None:
            return {}
        frame = pl.from_arrow(dataset.to_table(columns=key_columns))
        if frame.is_empty():
            return {}
        return {
            tuple(row[column] for column in key_columns): row["count"]
            for row in frame.group_by(key_columns).len(name="count").to_dicts()
        }

    bar_dataset = opened_datasets.get("dim_futures_bar_calendar")
    if bar_dataset is not None:
        bar_frame = pl.from_arrow(bar_dataset.to_table())
        daily_counts = grouped_count_map(
            "fact_futures_daily",
            ["contract_code", "trading_date"],
        )
        minute_counts = grouped_count_map(
            "fact_futures_minute",
            ["contract_code", "trading_date", "session_number"],
        )
        bar_mismatches = 0
        required_incomplete = 0
        for row in bar_frame.filter(pl.col("is_fetch_required")).iter_rows(named=True):
            if not row["is_fetch_completed"]:
                required_incomplete += 1
                continue
            if row["bar_frequency"] == "1d":
                actual = daily_counts.get(
                    (row["contract_code"], row["trading_date"]),
                    0,
                )
            else:
                actual = minute_counts.get(
                    (
                        row["contract_code"],
                        row["trading_date"],
                        row["session_number"],
                    ),
                    0,
                )
            if actual != row["actual_bar_count"]:
                bar_mismatches += 1
        print(
            json.dumps(
                {
                    "reconciliation": "futures_bar_calendar",
                    "required_incomplete": required_incomplete,
                    "completed_count_mismatches": bar_mismatches,
                },
                ensure_ascii=False,
            )
        )
        if bar_mismatches:
            failures.append(f"行情日历与正式事实计数不一致：{bar_mismatches}")

    report_dataset = opened_datasets.get("dim_futures_exchange_report_calendar")
    if report_dataset is not None:
        report_frame = pl.from_arrow(report_dataset.to_table())
        report_fact_tables = {
            "position_rank": "fact_futures_position_rank_daily",
            "member_position": "fact_futures_member_position_daily",
            "warehouse_receipt": "fact_futures_warehouse_receipt_daily",
        }
        report_mismatches = 0
        report_required_incomplete = 0
        for dataset_name, fact_table_name in report_fact_tables.items():
            fact_counts = grouped_count_map(
                fact_table_name,
                ["exchange_code", "underlying_code", "trading_date"],
            )
            rows = report_frame.filter(
                (pl.col("dataset_name") == dataset_name)
                & pl.col("is_fetch_required")
            )
            for row in rows.iter_rows(named=True):
                if not row["is_fetch_completed"]:
                    report_required_incomplete += 1
                    continue
                actual = fact_counts.get(
                    (
                        row["exchange_code"],
                        row["underlying_code"],
                        row["trading_date"],
                    ),
                    0,
                )
                if actual != row["actual_record_count"]:
                    report_mismatches += 1
        print(
            json.dumps(
                {
                    "reconciliation": "exchange_reports",
                    "required_incomplete": report_required_incomplete,
                    "completed_count_mismatches": report_mismatches,
                },
                ensure_ascii=False,
            )
        )
        if report_required_incomplete or report_mismatches:
            failures.append(
                "交易所报告日历与正式事实未完整一致："
                f"incomplete={report_required_incomplete}, mismatches={report_mismatches}"
            )

    external_dataset = opened_datasets.get("dim_external_market_calendar")
    if external_dataset is not None:
        external_frame = pl.from_arrow(external_dataset.to_table())
        status_summary = (
            external_frame.group_by(
                "dataset_name",
                "is_fetch_required",
                "is_fetch_completed",
                "fetch_result_status",
                "quality_status",
            )
            .len(name="count")
            .sort(
                "dataset_name",
                "is_fetch_required",
                "is_fetch_completed",
                "fetch_result_status",
                "quality_status",
            )
            .to_dicts()
        )
        print(
            json.dumps(
                {"reconciliation": "external_market_status", "groups": status_summary},
                ensure_ascii=False,
                default=str,
            )
        )

        # 生意社链路只归档原始响应字节。正式完成必须由规范 raw 路径、
        # SHA-256 sidecar 和外部市场日历状态共同证明，不再依赖结构化基差事实。
        raw_root = domestic_raw_root
        raw_dates: set[str] = set()
        raw_failures: list[str] = []
        if raw_root.is_dir():
            for response_path in raw_root.glob(
                "year=*/month=*/observation_date=*/response.html"
            ):
                date_part = response_path.parent.name
                month_part = response_path.parent.parent.name
                year_part = response_path.parent.parent.parent.name
                if not date_part.startswith("observation_date="):
                    raw_failures.append(f"非法日期目录：{response_path.parent}")
                    continue

                date_text = date_part.split("=", 1)[1]
                if (
                    year_part != f"year={date_text[:4]}"
                    or month_part != f"month={date_text[5:7]}"
                ):
                    raw_failures.append(f"raw Hive 路径与日期不一致：{response_path}")
                    continue

                sidecar_path = response_path.with_name("response.sha256")
                if not sidecar_path.is_file():
                    raw_failures.append(f"缺少 SHA-256 sidecar：{response_path.parent}")
                    continue

                response_bytes = response_path.read_bytes()
                expected_digest = sidecar_path.read_text(encoding="utf-8").strip()
                actual_digest = hashlib.sha256(response_bytes).hexdigest()
                if expected_digest != actual_digest:
                    raw_failures.append(f"正式原始响应摘要不一致：{response_path}")
                    continue

                official_files = sorted(
                    path.name
                    for path in response_path.parent.iterdir()
                    if path.is_file()
                )
                if official_files != ["response.html", "response.sha256"]:
                    raw_failures.append(
                        f"正式 raw 日期目录包含非契约文件：{response_path.parent}"
                    )
                    continue

                if date_text in raw_dates:
                    raw_failures.append(f"raw 日期重复：{date_text}")
                    continue
                raw_dates.add(date_text)

        domestic_mismatches = 0
        domestic_incomplete = 0
        domestic_rows = external_frame.filter(
            (pl.col("dataset_name") == "domestic_spot_basis")
            & pl.col("is_fetch_required")
        )
        for row in domestic_rows.iter_rows(named=True):
            date_text = str(row["observation_date"])
            if not row["is_fetch_completed"]:
                domestic_incomplete += 1
                continue
            if (
                date_text not in raw_dates
                or row["fetch_result_status"] != "success"
                or row["quality_status"] != "passed"
                or row["actual_record_count"] != 1
                or row["is_data_missing"]
            ):
                domestic_mismatches += 1

        print(
            json.dumps(
                {
                    "reconciliation": "domestic_spot_basis_raw",
                    "official_raw_dates": len(raw_dates),
                    "required_incomplete": domestic_incomplete,
                    "completed_count_mismatches": domestic_mismatches,
                    "raw_contract_failures": len(raw_failures),
                },
                ensure_ascii=False,
            )
        )
        if raw_failures or domestic_incomplete or domestic_mismatches:
            failures.append(
                "生意社原始页面与外部市场日历未完整一致："
                f"incomplete={domestic_incomplete}, "
                f"mismatches={domestic_mismatches}, "
                f"raw_failures={raw_failures}"
            )

        index_counts = grouped_count_map(
            "fact_external_index_daily",
            ["source_indicator_id", "observation_date"],
        )
        index_mismatches = 0
        index_incomplete = 0
        index_rows = external_frame.filter(
            (pl.col("dataset_name") == "external_index")
            & pl.col("is_fetch_required")
        )
        for row in index_rows.iter_rows(named=True):
            if not row["is_fetch_completed"]:
                index_incomplete += 1
                continue
            actual = index_counts.get((row["entity_code"], row["observation_date"]), 0)
            if actual != row["actual_record_count"]:
                index_mismatches += 1
        print(
            json.dumps(
                {
                    "reconciliation": "external_index",
                    "required_incomplete": index_incomplete,
                    "completed_count_mismatches": index_mismatches,
                },
                ensure_ascii=False,
            )
        )
        if index_incomplete or index_mismatches:
            failures.append(
                "外部指数日历与正式事实未完整一致："
                f"incomplete={index_incomplete}, mismatches={index_mismatches}"
            )

    transient_directories = sorted(
        str(path.relative_to(silver_root))
        for path in silver_root.iterdir()
        if path.is_dir()
        and ".raw-failed-" not in path.name
        and any(marker in path.name for marker in (".staging-", ".backup-", ".failed-"))
    )
    if transient_directories:
        failures.append(f"残留事务目录：{transient_directories}")

    raw_transient_directories = sorted(
        str(path.relative_to(domestic_raw_root))
        for path in domestic_raw_root.iterdir()
        if path.is_dir()
        and any(
            marker in path.name
            for marker in (".staging-", ".backup-", ".quarantine-")
        )
    ) if domestic_raw_root.is_dir() else []
    if raw_transient_directories:
        failures.append(f"生意社 raw 残留事务目录：{raw_transient_directories}")

    if failures:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    main()
