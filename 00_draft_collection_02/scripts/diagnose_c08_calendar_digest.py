from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import os
import pathlib
import shutil
import sys

import pyarrow as pa


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

MIGRATION_PATH = (
    PROJECT_ROOT
    / "00_draft_collection_02"
    / "scripts"
    / "migrate_pre_rebuild_futures_lake.py"
)
C08_PATH = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Futures_Market_Data"
    / "b08_full_minute_quality.py"
)
TRANSACTION_ROOT = (
    PROJECT_ROOT
    / "03_Futures_Database"
    / ".legacy-migration-20260817T232818Z-13588d1e"
)
SOURCE_SILVER = TRANSACTION_ROOT / "candidate_lake" / "silver"
OLD_MINUTE = (
    PROJECT_ROOT
    / "05_Old_Projects"
    / "futures_lake_pre_rebuild_20260810"
    / "silver"
    / "fact_futures_minute"
)


def load_module(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块：{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def link_partition(source_root: pathlib.Path, target_root: pathlib.Path, relative: pathlib.Path) -> None:
    source_leaf = source_root / relative
    files = sorted(source_leaf.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(source_leaf)
    target_leaf = target_root / relative
    target_leaf.mkdir(parents=True, exist_ok=False)
    for source in files:
        os.link(source, target_leaf / source.name)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("exchange_code")
    parser.add_argument("year", type=int)
    parser.add_argument("month", type=int)
    parser.add_argument("--output-root", type=pathlib.Path, required=True)
    args = parser.parse_args()

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    silver_root = output_root / "silver"
    silver_root.mkdir()

    migration = load_module("migration_diag", MIGRATION_PATH)
    c08 = load_module("c08_diag", C08_PATH)

    calendar_relative = pathlib.Path(
        "bar_frequency=1m"
    ) / f"exchange_code={args.exchange_code}" / f"year={args.year}" / f"month={args.month}"
    contract_relative = pathlib.Path(
        f"exchange_code={args.exchange_code}"
    ) / f"year={args.year}" / f"month={args.month}"
    link_partition(
        SOURCE_SILVER / c08.CALENDAR_TABLE_NAME,
        silver_root / c08.CALENDAR_TABLE_NAME,
        calendar_relative,
    )
    link_partition(
        SOURCE_SILVER / c08.CONTRACT_TABLE_NAME,
        silver_root / c08.CONTRACT_TABLE_NAME,
        contract_relative,
    )

    calendar_dataset = c08.open_exact_dataset(
        silver_root / c08.CALENDAR_TABLE_NAME,
        c08.CALENDAR_PARTITIONING,
        c08.CALENDAR_SCHEMA,
        "诊断行情日历",
    )
    calendar_key = ("1m", args.exchange_code, args.year, args.month)
    calendar_table = calendar_dataset.to_table(
        columns=c08.CALENDAR_SCHEMA.names,
        filter=c08.partition_expression(c08.CALENDAR_PARTITION_COLUMNS, calendar_key),
    )
    calendar_df = c08.arrow_to_pandas(calendar_table, c08.CALENDAR_SCHEMA)
    underlyings = sorted(
        calendar_df.loc[
            calendar_df["bar_frequency"].eq("1m")
            & calendar_df["is_fetch_required"].eq(True),
            "underlying_code",
        ].unique()
    )
    if not underlyings:
        raise RuntimeError("目标分区没有需审计品种。")

    for table_name in (c08.DAILY_TABLE_NAME, c08.MINUTE_TABLE_NAME):
        (silver_root / table_name).mkdir()
    for underlying in underlyings:
        fact_relative = pathlib.Path(
            f"exchange_code={args.exchange_code}"
        ) / f"underlying_code={underlying}" / f"year={args.year}" / f"month={args.month}"
        link_partition(
            SOURCE_SILVER / c08.DAILY_TABLE_NAME,
            silver_root / c08.DAILY_TABLE_NAME,
            fact_relative,
        )

        target_leaf = silver_root / c08.MINUTE_TABLE_NAME / fact_relative
        target_leaf.mkdir(parents=True, exist_ok=False)
        candidate_leaf = SOURCE_SILVER / c08.MINUTE_TABLE_NAME / fact_relative
        old_leaf = OLD_MINUTE / fact_relative
        candidate_files = sorted(candidate_leaf.glob("*.parquet"))
        old_files = sorted(old_leaf.glob("*.parquet"))
        if bool(candidate_files) == bool(old_files):
            raise RuntimeError(f"minute 叶无法唯一定位：{fact_relative}")
        if candidate_files:
            for source in candidate_files:
                os.link(source, target_leaf / source.name)
        else:
            for source in old_files:
                target = target_leaf / source.name
                shutil.copy2(source, target)
                migration.patch_parquet_footer(target, migration.FUTURES_MINUTE_SCHEMA)

    captured: list[tuple[str, object, pa.Table]] = []
    original_table_digest = c08.table_digest

    def capturing_digest(frame, schema, sort_columns):
        digest = original_table_digest(frame, schema, sort_columns)
        sorted_df = frame.sort_values(sort_columns).reset_index(drop=True)
        table = c08.pandas_to_arrow(sorted_df.loc[:, schema.names], schema)
        captured.append((digest, list(sort_columns), table))
        return digest

    c08.table_digest = capturing_digest
    missing_staging = silver_root / ".diag-missing-staging"
    calendar_staging = silver_root / ".diag-calendar-staging"
    try:
        result = c08.build_full_audit_staging(
            output_root,
            missing_staging,
            calendar_staging,
            datetime(2026, 8, 18, 2, 56, 15, 123456, tzinfo=timezone.utc),
        )
        print(f"RESULT={result}")
    except BaseException as error:
        print(f"ERROR={type(error).__name__}: {error}")
        if len(captured) >= 2:
            left_digest, left_sort, left = captured[-2]
            right_digest, right_sort, right = captured[-1]
            print(f"LEFT_DIGEST={left_digest}")
            print(f"RIGHT_DIGEST={right_digest}")
            print(f"SORT_COLUMNS={left_sort}|{right_sort}")
            print(f"ROWS={left.num_rows}|{right.num_rows}")
            print(f"SCHEMA_EQUAL={left.schema.equals(right.schema, check_metadata=True)}")
            unequal_columns = []
            for name in left.schema.names:
                left_array = left[name].combine_chunks()
                right_array = right[name].combine_chunks()
                left_buffers = [
                    None if buffer is None else hashlib.sha256(buffer.to_pybytes()).hexdigest()
                    for buffer in left_array.buffers()
                ]
                right_buffers = [
                    None if buffer is None else hashlib.sha256(buffer.to_pybytes()).hexdigest()
                    for buffer in right_array.buffers()
                ]
                if left_buffers != right_buffers:
                    print(
                        f"PHYSICAL_COLUMN={name} "
                        f"LEFT_BUFFERS={left_buffers} RIGHT_BUFFERS={right_buffers}"
                    )
                if not left[name].equals(right[name]):
                    unequal_columns.append(name)
                    left_values = left[name].to_pylist()
                    right_values = right[name].to_pylist()
                    examples = []
                    for index, (left_value, right_value) in enumerate(zip(left_values, right_values, strict=True)):
                        if left_value != right_value:
                            examples.append((index, left_value, right_value))
                            if len(examples) == 5:
                                break
                    print(f"COLUMN={name} EXAMPLES={examples}")
            print(f"UNEQUAL_COLUMNS={unequal_columns}")
        raise


if __name__ == "__main__":
    main()
