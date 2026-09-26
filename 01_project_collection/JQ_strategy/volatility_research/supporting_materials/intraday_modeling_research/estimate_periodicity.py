"""Estimate the complete frozen grid of causal daily periodicity candidates."""

import json
import pathlib
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_contracts import MINUTE_OBSERVATION_SCHEMA, PERIODICITY_FACTOR_SCHEMA
from estimators import estimate_variance_factor


def main():
    project_dir = pathlib.Path(__file__).resolve().parent
    observation_table = pq.read_table(
        project_dir / "data/minute_observations.parquet"
    ).cast(MINUTE_OBSERVATION_SCHEMA)
    observation_df = observation_table.to_pandas()
    return_matrix = observation_df.pivot(
        index="trading_date", columns="minute_index", values="log_return"
    ).sort_index()
    trading_dates = return_matrix.index
    return_values = return_matrix.to_numpy()
    bpv_matrix = observation_df.pivot(
        index="trading_date", columns="minute_index", values="bpv_contribution"
    ).reindex(trading_dates)
    mixed_matrix = observation_df.pivot(
        index="trading_date", columns="minute_index", values="mixed_contribution"
    ).reindex(trading_dates)
    daily_bpv_scale = np.sqrt(np.nanmean(bpv_matrix.to_numpy(), axis=1))
    standardized_returns = return_values / daily_bpv_scale[:, None]
    daily_rv = np.mean(return_values**2, axis=1)
    lagged_state = np.concatenate([[np.nan], np.log(daily_rv[:-1])])
    mixed_shape_values = mixed_matrix.to_numpy() / mixed_matrix.mean(axis=1).to_numpy()[:, None]
    day_count, slot_count = return_values.shape
    candidate_grid = [("none", 0)] + [
        (method, lookback)
        for method in ("mean_rv", "median_rv", "median_mixed", "boudt_wsd", "fff_wsd")
        for lookback in (30, 60, 120, 360)
    ] + [("state_wsd", lookback) for lookback in (120, 360)]

    results_dir = project_dir / "results"
    results_dir.mkdir(exist_ok=True)
    output_path = results_dir / "periodicity_factors.parquet"
    temporary_path = output_path.with_suffix(".parquet.tmp")
    coverage_records = []
    start_time = time.monotonic()
    with pq.ParquetWriter(temporary_path, PERIODICITY_FACTOR_SCHEMA, compression="zstd") as writer:
        for method, lookback in candidate_grid:
            variance_factors = np.full_like(return_values, np.nan)
            fit_dates = np.full(day_count, None, dtype=object)
            state_fallback_days = 0
            degenerate_days = 0
            if method == "none":
                variance_factors[:] = 1.0
            else:
                for day_index in range(lookback, day_count):
                    historical_returns = standardized_returns[day_index - lookback:day_index]
                    if method == "median_mixed":
                        shape_factor = np.median(
                            mixed_shape_values[day_index - lookback:day_index], axis=0
                        )
                        if np.isfinite(shape_factor).all() and np.all(shape_factor > 0):
                            variance_factors[day_index] = shape_factor / shape_factor.mean()
                    elif method == "state_wsd":
                        historical_state = lagged_state[day_index - lookback:day_index]
                        finite_state = np.isfinite(historical_state)
                        thresholds = np.quantile(historical_state[finite_state], [1/3, 2/3])
                        target_regime = np.searchsorted(thresholds, lagged_state[day_index])
                        historical_regime = np.searchsorted(thresholds, historical_state)
                        retained_days = finite_state & (historical_regime == target_regime)
                        if retained_days.sum() >= 30:
                            historical_returns = historical_returns[retained_days]
                        else:
                            state_fallback_days += 1
                        variance_factors[day_index] = estimate_variance_factor(
                            historical_returns, "boudt_wsd"
                        )
                    else:
                        variance_factors[day_index] = estimate_variance_factor(
                            historical_returns, method
                        )
                    fit_dates[day_index] = trading_dates[day_index - 1]
                    if not np.isfinite(variance_factors[day_index]).all():
                        degenerate_days += 1

            contribution_factors = variance_factors.copy()
            if method != "median_mixed":
                for session_start in (0, 120):
                    session_end = session_start + 120
                    contribution_factors[:, session_start + 1:session_end] = np.sqrt(
                        variance_factors[:, session_start + 1:session_end]
                        * variance_factors[:, session_start:session_end - 1]
                    )
            finite_days = np.isfinite(variance_factors).all(axis=1)
            if not np.allclose(variance_factors[finite_days].mean(axis=1), 1.0, atol=1e-12):
                raise ValueError("Mean-one factor identity failed")
            factor_df = pd.DataFrame({
                "trading_date": np.repeat(trading_dates.to_numpy(), slot_count),
                "minute_index": np.tile(np.arange(slot_count), day_count),
                "method": method,
                "lookback_days": lookback,
                "variance_factor": variance_factors.ravel(),
                "contribution_factor": contribution_factors.ravel(),
                "factor_fit_end_date": np.repeat(fit_dates, slot_count),
            })
            factor_table = pa.Table.from_pandas(
                factor_df, schema=PERIODICITY_FACTOR_SCHEMA, preserve_index=False
            ).replace_schema_metadata(PERIODICITY_FACTOR_SCHEMA.metadata)
            writer.write_table(factor_table)
            coverage_records.append({
                "method": method, "lookback_days": lookback,
                "total_days": day_count, "valid_days": int(finite_days.sum()),
                "warmup_days": lookback, "degenerate_days": degenerate_days,
                "state_pooled_fallback_days": state_fallback_days,
            })
            print(f"{method}/{lookback}: {finite_days.sum()}/{day_count} days; elapsed={time.monotonic()-start_time:.1f}s", flush=True)

    expected_rows = day_count * slot_count * len(candidate_grid)
    with pq.ParquetFile(temporary_path) as reopened_file:
        if not reopened_file.schema_arrow.equals(PERIODICITY_FACTOR_SCHEMA, check_metadata=True):
            raise ValueError("Factor schema round-trip failed")
        if reopened_file.metadata.num_rows != expected_rows:
            raise ValueError("Factor row count mismatch")
    temporary_path.replace(output_path)
    pd.DataFrame(coverage_records).to_csv(
        results_dir / "factor_coverage.csv", index=False, encoding="utf-8-sig"
    )
    (results_dir / "factor_specification.json").write_text(json.dumps({
        "candidate_grid": candidate_grid,
        "daily_standardization": "r/sqrt(mean of 238 within-session BPV contributions)",
        "boudt_core": "signed ShortH: floor(n/2)+1; 0.741; squared cutoff6.635; WSD second moment constant1.081",
        "degenerate_policy": "entire daily factor is null when any scale is zero/nonfinite or retained count is zero; no silent filling",
        "fff_adaptation": "log WSD variance, separate session intercept/trend, harmonics1..3, first2 and last slot dummies",
        "state_adaptation": "lagged log daily RV, history-only terciles, at least30 matching days or pooled history",
        "causal": "day d uses only historical dates d-L through d-1; all parameters fixed before target day",
        "rows": expected_rows, "elapsed_seconds": time.monotonic()-start_time,
    }, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
