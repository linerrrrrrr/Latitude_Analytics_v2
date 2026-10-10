"""Build independent minute observations from the project's frozen raw-price input."""

import hashlib
import json
import pathlib
import platform
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_contracts import INPUT_SNAPSHOT_SCHEMA, MINUTE_OBSERVATION_SCHEMA


def main():
    project_dir = pathlib.Path(__file__).resolve().parent
    input_path = project_dir / "data/input/im_minute_snapshot.parquet"
    input_table = pq.read_table(input_path).cast(INPUT_SNAPSHOT_SCHEMA)
    input_df = input_table.to_pandas().sort_values(["trading_date", "minute_index"])
    if input_df.duplicated(["trading_date", "minute_index"]).any():
        raise ValueError("Duplicate day/slot keys")
    if input_df[INPUT_SNAPSHOT_SCHEMA.names].isna().any().any():
        raise ValueError("Required input field is null")
    if not np.isfinite(input_df[["open", "close", "volume", "money"]]).all().all():
        raise ValueError("Nonfinite price or activity value")
    if (input_df[["open", "close"]] <= 0).any().any():
        raise ValueError("Nonpositive raw price")
    if not (input_df["signal_trading_date"] < input_df["trading_date"]).all():
        raise ValueError("Main-contract selection signal is not strictly lagged")

    session_groups = input_df.groupby(
        ["trading_date", "contract_code", "session_number"], sort=False
    )
    log_close = np.log(input_df["close"])
    return_series = log_close.groupby([
        input_df["trading_date"], input_df["contract_code"], input_df["session_number"]
    ]).diff()
    return_series = return_series.where(
        session_groups["bar_at"].diff().eq(pd.Timedelta(minutes=1))
    )
    first_minute_mask = input_df["minute_index"].isin([0, 120])
    return_series.loc[first_minute_mask] = np.log(
        input_df.loc[first_minute_mask, "close"] / input_df.loc[first_minute_mask, "open"]
    )
    previous_return = return_series.groupby([
        input_df["trading_date"], input_df["contract_code"], input_df["session_number"]
    ]).shift(1)
    observations_df = input_df[MINUTE_OBSERVATION_SCHEMA.names[:5]].copy()
    observations_df["log_return"] = return_series
    observations_df["rv_contribution"] = return_series**2
    observations_df["bpv_contribution"] = np.pi / 2 * np.abs(return_series * previous_return)
    observations_df["mixed_contribution"] = observations_df["bpv_contribution"].where(
        ~first_minute_mask, observations_df["rv_contribution"]
    )
    observations_table = pa.Table.from_pandas(
        observations_df, schema=MINUTE_OBSERVATION_SCHEMA, preserve_index=False
    ).replace_schema_metadata(MINUTE_OBSERVATION_SCHEMA.metadata)
    output_path = project_dir / "data/minute_observations.parquet"
    temporary_path = output_path.with_suffix(".parquet.tmp")
    pq.write_table(observations_table, temporary_path, compression="zstd")
    installed_table = pq.read_table(temporary_path)
    if not installed_table.schema.equals(MINUTE_OBSERVATION_SCHEMA, check_metadata=True):
        raise ValueError("Observation schema round-trip mismatch")
    if not installed_table.equals(observations_table):
        raise ValueError("Observation values round-trip mismatch")
    temporary_path.replace(output_path)

    daily_coverage_df = observations_df.groupby("trading_date").agg(
        minute_count=("minute_index", "size"),
        return_count=("log_return", "count"),
        bpv_pair_count=("bpv_contribution", "count"),
        contract_count=("contract_code", "nunique"),
        daily_rv=("rv_contribution", "sum"),
        daily_bpv_unscaled=("bpv_contribution", "sum"),
    )
    results_dir = project_dir / "results/data"
    results_dir.mkdir(parents=True, exist_ok=True)
    daily_coverage_df.to_csv(results_dir / "daily_coverage.csv", encoding="utf-8-sig")
    if not daily_coverage_df[["minute_count", "return_count", "bpv_pair_count"]].eq(
        [240, 240, 238]
    ).all().all():
        raise ValueError("The initial experiment requires complete 240-slot days")
    manifest = {
        "python": sys.executable,
        "platform": platform.platform(),
        "input_sha256": hashlib.sha256(input_path.read_bytes()).hexdigest(),
        "observation_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        "rows": len(observations_df), "days": len(daily_coverage_df),
        "first_date": str(observations_df.trading_date.min()),
        "last_date": str(observations_df.trading_date.max()),
        "missing_returns": int(observations_df.log_return.isna().sum()),
        "zero_returns": int(observations_df.log_return.eq(0).sum()),
        "expected_missing_bpv": int(first_minute_mask.sum()),
        "signal_is_strictly_lagged": True,
        "source_prices": "Raw open and close only; no bidirectional adjustment",
        "upstream_limit": "Signal-date ordering checked; original contract-selection algorithm not reconstructed",
        "split": {"training_end": "2024-12-31", "validation_end": "2025-12-31", "retrospective_evaluation_start": "2026-01-01"},
        "versions": {"numpy": np.__version__, "pandas": pd.__version__, "pyarrow": pa.__version__},
    }
    (results_dir / "input_validation.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
