"""Run causal, daily-refitted intraday forecasts on this project's frozen inputs.

This is a local research experiment. It does not connect to JQData or write to a lake.
All period filters, model fitting and window selection are explicit below.
"""

from __future__ import annotations

import hashlib
import json
import sys
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_contracts import MINUTE_OBSERVATION_SCHEMA, PERIODICITY_FACTOR_SCHEMA


def rolling_ridge_predictions(
    feature_values: np.ndarray,
    target_values: np.ndarray,
    *,
    last_label_delay: int = 1,
    training_days: int = 252,
    minimum_training_days: int = 126,
    ridge_fraction: float = 0.01,
) -> tuple[np.ndarray, np.ndarray]:
    """Fit complete prior-day labels using rolling sufficient statistics.

Arrays are day x origin x feature (or day x origin for targets). The intercept
and feature standardization use precisely the same training rows as the fit.
last_label_delay=h excludes incomplete h-day labels in the daily HAR extension.
"""
    day_count, origin_count, feature_count = feature_values.shape
    valid_rows = np.isfinite(feature_values).all(axis=2) & np.isfinite(target_values)
    feature_clean = np.where(valid_rows[..., None], feature_values, 0.0)
    target_clean = np.where(valid_rows, target_values, 0.0)
    row_counts = valid_rows.sum(axis=1)
    feature_sums = feature_clean.sum(axis=1)
    target_sums = target_clean.sum(axis=1)
    feature_products = np.einsum("doi,doj->dij", feature_clean, feature_clean)
    feature_target_products = np.einsum("doi,do->di", feature_clean, target_clean)
    daily_statistics = [
        row_counts, feature_sums, target_sums, feature_products, feature_target_products
    ]
    cumulative_statistics = [
        np.concatenate([np.zeros_like(values[:1]), values.cumsum(axis=0)], axis=0)
        for values in daily_statistics
    ]
    cumulative_valid_days = np.r_[0, (row_counts > 0).cumsum()]
    predictions = np.full((day_count, origin_count), np.nan)
    fit_day_counts = np.zeros(day_count, dtype=np.int16)
    for day_index in range(day_count):
        training_end = day_index - last_label_delay + 1
        training_start = max(0, training_end - training_days)
        if training_end <= 0:
            continue
        fit_days = int(
            cumulative_valid_days[training_end] - cumulative_valid_days[training_start]
        )
        fit_day_counts[day_index] = fit_days
        if fit_days < minimum_training_days:
            continue
        count, feature_sum, target_sum, products, target_products = [
            values[training_end] - values[training_start]
            for values in cumulative_statistics
        ]
        feature_mean = feature_sum / count
        target_mean = target_sum / count
        centered_products = products - count * np.outer(feature_mean, feature_mean)
        feature_scale = np.sqrt(
            np.maximum(np.diag(centered_products) / count, np.finfo(float).tiny)
        )
        standardized_products = centered_products / np.outer(feature_scale, feature_scale)
        standardized_target_products = (
            target_products - count * feature_mean * target_mean
        ) / feature_scale
        coefficients = np.linalg.solve(
            standardized_products + ridge_fraction * count * np.eye(feature_count),
            standardized_target_products,
        )
        current_prediction = (
            (feature_values[day_index] - feature_mean) / feature_scale
        ) @ coefficients + target_mean
        # This floor is learned from historical targets; no current/future label is used.
        predictions[day_index] = np.maximum(current_prediction, max(target_mean * 1e-6, 1e-16))
    return predictions, fit_day_counts


def paired_day_block_intervals(
    paired_differences: np.ndarray, *, seed: int, replications: int = 200, block_days: int = 5
) -> tuple[float, float]:
    """Moving-block bootstrap of paired, equally weighted daily loss differences."""
    paired_differences = paired_differences[np.isfinite(paired_differences)]
    day_count = len(paired_differences)
    if day_count < 2:
        return float("nan"), float("nan")
    block_days = min(block_days, day_count)
    generator = np.random.default_rng(seed)
    starts = generator.integers(
        0, day_count - block_days + 1,
        size=(replications, int(np.ceil(day_count / block_days))),
    )
    sampled_indices = (
        starts[..., None] + np.arange(block_days)[None, None, :]
    ).reshape(replications, -1)[:, :day_count]
    bootstrap_means = paired_differences[sampled_indices].mean(axis=1)
    return tuple(np.quantile(bootstrap_means, [0.025, 0.975]).tolist())


def compare_selected_methods(
    daily_losses_df: pd.DataFrame, output_dir: Path
) -> pd.DataFrame:
    """Compare locked method choices with the existing mixed-shape compatibility baseline."""
    selected_evaluation_df = daily_losses_df[
        (daily_losses_df.year == 2026) & daily_losses_df.selected_on_2025
    ]
    comparison_records = []
    for (target_name, model_name), evaluation_df in selected_evaluation_df.groupby(
        ["target", "model"]
    ):
        baseline_df = evaluation_df[evaluation_df.method == "median_mixed"].set_index(
            "trading_date"
        ).sort_index()
        if baseline_df.empty or baseline_df.index.duplicated().any():
            raise ValueError("Each selected-method comparison needs one mixed baseline per day")
        for (method, window), candidate_df in evaluation_df.groupby(["method", "lookback_days"]):
            candidate_df = candidate_df.set_index("trading_date").sort_index()
            if not candidate_df.index.equals(baseline_df.index):
                raise ValueError("Selected methods do not share identical retrospective days")
            qlike_difference = (candidate_df.qlike - baseline_df.qlike).to_numpy()
            mse_difference = (candidate_df.mse - baseline_df.mse).to_numpy()
            qlike_low, qlike_high = paired_day_block_intervals(qlike_difference, seed=20260907)
            mse_low, mse_high = paired_day_block_intervals(mse_difference, seed=20260907)
            comparison_records.append({
                "year": 2026, "target": target_name, "model": model_name,
                "method": method, "lookback_days": int(window),
                "baseline_method": "median_mixed",
                "baseline_lookback_days": int(baseline_df.lookback_days.iloc[0]),
                "selection_period": "2025", "days": len(candidate_df),
                "origins": int(candidate_df.origins.sum()),
                "qlike_difference_vs_median_mixed": float(qlike_difference.mean()),
                "qlike_difference_ci_low": qlike_low,
                "qlike_difference_ci_high": qlike_high,
                "mse_difference_vs_median_mixed": float(mse_difference.mean()),
                "mse_difference_ci_low": mse_low, "mse_difference_ci_high": mse_high,
                "mse_ratio_vs_median_mixed": float(candidate_df.mse.mean() / baseline_df.mse.mean()),
            })
    comparison_df = pd.DataFrame(comparison_records)
    comparison_df.to_csv(
        output_dir / "selected_method_comparison.csv", index=False, encoding="utf-8-sig"
    )
    return comparison_df


def run_daily_har(
    observation_dates: pd.DatetimeIndex,
    daily_rv: np.ndarray,
    filtered_daily_rv: np.ndarray,
    output_dir: Path,
) -> pd.DataFrame:
    """A deliberately small 252-day HAR/HARP adaptation, not the paper's replication."""
    records = []
    predictions_frames = []
    years = observation_dates.year.to_numpy()
    for horizon_days in (1, 5, 22):
        future_daily_target = (
            pd.Series(daily_rv)[::-1]
            .rolling(horizon_days, min_periods=horizon_days).mean()[::-1]
            .to_numpy()
        )
        for model_name, predictor_rv in (
            ("HAR_ridge_252", daily_rv), ("HARP_WSD120_ridge_252", filtered_daily_rv)
        ):
            daily_predictors = pd.DataFrame({
                "lag1": pd.Series(predictor_rv).shift(1),
                "lag5": pd.Series(predictor_rv).shift(1).rolling(5).mean(),
                "lag22": pd.Series(predictor_rv).shift(1).rolling(22).mean(),
            }).to_numpy()
            predicted_daily_rv, training_counts = rolling_ridge_predictions(
                daily_predictors[:, None, :], future_daily_target[:, None],
                last_label_delay=horizon_days,
            )
            valid_days = (
                (years >= 2025) & np.isfinite(predicted_daily_rv[:, 0])
                & np.isfinite(future_daily_target)
            )
            label_end_indices = np.minimum(
                np.arange(len(observation_dates)) + horizon_days - 1, len(observation_dates) - 1
            )
            # Validation labels ending in 2026 are purged, so calendar split labels do
            # not overlap between the reported validation and retrospective periods.
            valid_days &= years == years[label_end_indices]
            frame = pd.DataFrame({
                "trading_date": observation_dates[valid_days].date,
                "year": years[valid_days],
                "model": model_name,
                "horizon_days": horizon_days,
                "target": future_daily_target[valid_days],
                "prediction": predicted_daily_rv[valid_days, 0],
                "training_days": training_counts[valid_days],
                "training_label_end_date": observation_dates[np.flatnonzero(valid_days) - 1].date,
                "target_end_date": observation_dates[
                    np.flatnonzero(valid_days) + horizon_days - 1
                ].date,
            })
            if not (frame.training_label_end_date < frame.trading_date).all():
                raise ValueError("Daily HAR training contains an incomplete future label")
            predictions_frames.append(frame)
    predictions_df = pd.concat(predictions_frames, ignore_index=True)
    # Align both models on identical dates for every horizon and evaluation period.
    for horizon_days in (1, 5, 22):
        horizon_df = predictions_df[predictions_df.horizon_days == horizon_days].copy()
        common_dates = horizon_df.groupby("trading_date").model.nunique()
        horizon_df = horizon_df[
            horizon_df.trading_date.isin(common_dates[common_dates == 2].index)
        ]
        for year, year_df in horizon_df.groupby("year"):
            baseline = year_df[year_df.model == "HAR_ridge_252"].set_index("trading_date")
            baseline_loss = np.log(baseline.prediction * 1e8) + baseline.target / baseline.prediction
            for model_name, model_df in year_df.groupby("model"):
                model_df = model_df.set_index("trading_date").loc[baseline.index]
                qlike = np.log(model_df.prediction * 1e8) + model_df.target / model_df.prediction
                difference = (qlike - baseline_loss).to_numpy()
                ci_low, ci_high = paired_day_block_intervals(
                    difference, seed=20260907 + horizon_days + int(year),
                    block_days=max(5, horizon_days),
                )
                records.append({
                    "year": int(year), "model": model_name, "horizon_days": horizon_days,
                    "days": len(model_df), "qlike": float(qlike.mean()),
                    "mse": float(np.mean((model_df.target - model_df.prediction) ** 2)),
                    "qlike_difference_vs_har": float(difference.mean()),
                    "qlike_difference_ci_low": ci_low, "qlike_difference_ci_high": ci_high,
                })
    daily_prediction_schema = pa.schema([
        pa.field("trading_date", pa.date32(), False),
        pa.field("year", pa.int16(), False), pa.field("model", pa.string(), False),
        pa.field("horizon_days", pa.int16(), False),
        pa.field("target", pa.float64(), False), pa.field("prediction", pa.float64(), False),
        pa.field("training_days", pa.int16(), False),
        pa.field("training_label_end_date", pa.date32(), False),
        pa.field("target_end_date", pa.date32(), False),
    ], metadata={b"research_table": b"daily_har_predictions", b"schema_version": b"1"})
    predictions_table = pa.Table.from_pandas(
        predictions_df, schema=daily_prediction_schema, preserve_index=False
    ).replace_schema_metadata(daily_prediction_schema.metadata)
    pq.write_table(predictions_table, output_dir / "daily_har_predictions.parquet", compression="zstd")
    summary_df = pd.DataFrame(records)
    summary_df.to_csv(output_dir / "daily_har_summary.csv", index=False, encoding="utf-8-sig")
    return summary_df


def main() -> None:
    started_at = time.perf_counter()
    research_dir = Path(__file__).resolve().parent
    output_dir = research_dir / "results" / "forecasting"
    output_dir.mkdir(parents=True, exist_ok=True)
    observation_path = research_dir / "data" / "minute_observations.parquet"
    factor_path = research_dir / "results" / "periodicity_factors.parquet"
    print(f"Python: {sys.executable}", flush=True)
    observation_table = pq.read_table(observation_path)
    factor_table = pq.read_table(factor_path)
    if not observation_table.schema.equals(MINUTE_OBSERVATION_SCHEMA, check_metadata=False):
        raise ValueError("Minute observation schema differs from the local executable contract")
    if not factor_table.schema.equals(PERIODICITY_FACTOR_SCHEMA, check_metadata=False):
        raise ValueError("Periodicity factor schema differs from the local executable contract")
    observations_df = observation_table.to_pandas().sort_values(["trading_date", "minute_index"])
    factors_df = factor_table.to_pandas(categories=["method"])
    del observation_table, factor_table
    observations_df["trading_date"] = pd.to_datetime(observations_df["trading_date"])
    factors_df["trading_date"] = pd.to_datetime(factors_df["trading_date"])
    factors_df["factor_fit_end_date"] = pd.to_datetime(factors_df["factor_fit_end_date"])
    dated_factors = factors_df.factor_fit_end_date.notna()
    if (
        factors_df.loc[dated_factors, "factor_fit_end_date"]
        >= factors_df.loc[dated_factors, "trading_date"]
    ).any():
        raise ValueError("A factor uses its current or a future trading day")
    if observations_df.duplicated(["trading_date", "minute_index"]).any():
        raise ValueError("Duplicate minute observations")
    observation_dates = pd.DatetimeIndex(sorted(observations_df.trading_date.unique()))
    minute_indices = np.arange(240)
    session_numbers = observations_df.pivot(
        index="trading_date", columns="minute_index", values="session_number"
    ).reindex(index=observation_dates, columns=minute_indices).to_numpy()
    rv_values = observations_df.pivot(
        index="trading_date", columns="minute_index", values="rv_contribution"
    ).reindex(index=observation_dates, columns=minute_indices).to_numpy()
    bpv_values = observations_df.pivot(
        index="trading_date", columns="minute_index", values="bpv_contribution"
    ).reindex(index=observation_dates, columns=minute_indices).to_numpy()
    complete_day_mask = (
        np.isfinite(rv_values).all(axis=1)
        & np.isfinite(session_numbers).all(axis=1)
        & (rv_values >= 0).all(axis=1)
    )
    if not complete_day_mask.all():
        raise ValueError("This experiment requires complete nonnegative 240-minute RV days")
    if not (session_numbers == session_numbers[:1]).all():
        raise ValueError("Session grid changes within the frozen sample")
    # Origin = after this minute's bar. The 15 targets finish in the same session.
    origin_indices = np.array([
        minute for minute in range(14, 225)
        if session_numbers[0, minute - 14] == session_numbers[0, minute]
        and session_numbers[0, minute + 15] == session_numbers[0, minute]
    ])
    future_indices = origin_indices[:, None] + np.arange(1, 16)[None, :]
    past5_indices = origin_indices[:, None] - np.arange(4, -1, -1)[None, :]
    past15_indices = origin_indices[:, None] - np.arange(14, -1, -1)[None, :]
    if future_indices.shape[1] != 15 or not np.all(
        future_indices[:, -1] - future_indices[:, 0] + 1 == 15
    ):
        raise ValueError("A forecast label does not contain exactly 15 contiguous minutes")
    if not np.all(past15_indices.max(axis=1) <= origin_indices):
        raise ValueError("A recent-return feature uses a bar after its forecast origin")
    if not np.all(
        session_numbers[0, future_indices] == session_numbers[0, origin_indices, None]
    ):
        raise ValueError("A forecast label crosses a session boundary")
    bar_times_df = observations_df.pivot(
        index="trading_date", columns="minute_index", values="bar_at"
    ).reindex(index=observation_dates, columns=minute_indices)
    session_starts = np.r_[0, np.flatnonzero(np.diff(session_numbers[0])) + 1]
    origin_session_starts = np.array([
        session_starts[session_starts <= minute][-1] for minute in origin_indices
    ])
    target_values_by_name = {
        "RV15": rv_values[:, future_indices].sum(axis=2),
        "BPV15": bpv_values[:, future_indices].sum(axis=2),
    }
    if any(not np.isfinite(values).all() for values in target_values_by_name.values()):
        raise ValueError("Missing interval label after same-session origin construction")
    day_count = len(observation_dates)
    daily_rv = rv_values.sum(axis=1)
    prior_day_rv = pd.Series(daily_rv).shift(1)
    historical_low = prior_day_rv.rolling(252, min_periods=126).quantile(1 / 3)
    historical_high = prior_day_rv.rolling(252, min_periods=126).quantile(2 / 3)
    state_values = np.where(
        prior_day_rv.to_numpy() < historical_low.to_numpy(), "low",
        np.where(prior_day_rv.to_numpy() > historical_high.to_numpy(), "high", "middle"),
    )
    years = observation_dates.year.to_numpy()
    evaluation_mask = years >= 2025
    candidate_keys = sorted(
        (str(method), int(window))
        for method, window in factors_df[["method", "lookback_days"]].drop_duplicates().itertuples(index=False)
    )
    supported_methods = {
        "none", "mean_rv", "median_rv", "median_mixed", "boudt_wsd", "fff_wsd", "state_wsd"
    }
    candidate_keys = [key for key in candidate_keys if key[0] in supported_methods]
    if ("none", 0) not in candidate_keys:
        raise ValueError("The unfiltered none/0 benchmark is missing")
    fixed_keys = {
        ("none", 0), ("mean_rv", 120), ("median_mixed", 120),
        ("boudt_wsd", 120), ("fff_wsd", 120), ("state_wsd", 360),
    }
    if not fixed_keys.issubset(candidate_keys):
        raise ValueError(f"Missing prespecified candidates: {fixed_keys - set(candidate_keys)}")
    forecast_payloads = {}
    daily_filtered_wsd = None
    coverage_records = []
    for candidate_index, (method, window) in enumerate(candidate_keys, 1):
        candidate_df = factors_df[
            (factors_df.method == method) & (factors_df.lookback_days == window)
        ]
        candidate_fit_dates = candidate_df.groupby("trading_date").factor_fit_end_date.max().reindex(
            observation_dates
        )
        if candidate_df.duplicated(["trading_date", "minute_index"]).any():
            raise ValueError(f"Duplicate periodicity factor keys: {method}/{window}")
        variance_factors = candidate_df.pivot(
            index="trading_date", columns="minute_index", values="variance_factor"
        ).reindex(index=observation_dates, columns=minute_indices).to_numpy(dtype=float, copy=True)
        contribution_factors = candidate_df.pivot(
            index="trading_date", columns="minute_index", values="contribution_factor"
        ).reindex(index=observation_dates, columns=minute_indices).to_numpy(dtype=float, copy=True)
        valid_factor_day = (
            np.isfinite(variance_factors).all(axis=1) & (variance_factors > 0).all(axis=1)
        )
        variance_factors[~valid_factor_day] = np.nan
        contribution_factors[~valid_factor_day] = np.nan
        filtered_rv_values = rv_values / variance_factors
        filtered_daily_mean = np.mean(filtered_rv_values, axis=1)
        if (method, window) == ("boudt_wsd", 120):
            daily_filtered_wsd = filtered_daily_mean * 240
        previous_rates = pd.DataFrame({
            "lag1": pd.Series(filtered_daily_mean).shift(1),
            "lag5": pd.Series(filtered_daily_mean).shift(1).rolling(5).mean(),
            "lag22": pd.Series(filtered_daily_mean).shift(1).rolling(22).mean(),
        }).to_numpy()
        recent5 = filtered_rv_values[:, past5_indices].mean(axis=2)
        recent15 = filtered_rv_values[:, past15_indices].mean(axis=2)
        session_prefix_sum = np.c_[np.zeros(day_count), np.cumsum(filtered_rv_values, axis=1)]
        session_rate = (
            session_prefix_sum[:, origin_indices + 1] - session_prefix_sum[:, origin_session_starts]
        ) / (origin_indices - origin_session_starts + 1)[None, :]
        feature_values = np.stack([
            recent5, recent15, session_rate,
            np.repeat(previous_rates[:, 0, None], len(origin_indices), axis=1),
            np.repeat(previous_rates[:, 1, None], len(origin_indices), axis=1),
            np.repeat(previous_rates[:, 2, None], len(origin_indices), axis=1),
        ], axis=2)
        previous20_rate = (
            pd.Series(filtered_daily_mean).shift(1).rolling(20).mean().to_numpy()[:, None]
        )
        # Fixed equal blend: recent realized activity + historical daily scale.
        blend_rate = 0.5 * recent15 + 0.5 * previous20_rate
        for target_name, interval_target in target_values_by_name.items():
            exposure = (
                variance_factors if target_name == "RV15" else contribution_factors
            )[:, future_indices].sum(axis=2)
            normalized_target = interval_target / exposure
            ridge_rate, training_counts = rolling_ridge_predictions(feature_values, normalized_target)
            for model_name, prediction in (
                ("scale_exposure", np.maximum(blend_rate, 1e-16) * exposure),
                ("ridge_intraday", ridge_rate * exposure),
            ):
                configuration = (method, window, target_name, model_name)
                forecast_payloads[configuration] = {
                    "prediction": prediction,
                    "training_days": training_counts if model_name == "ridge_intraday" else np.zeros(day_count),
                    "factor_fit_end_date": candidate_fit_dates.to_numpy(),
                }
                coverage_records.append({
                    "method": method, "lookback_days": window,
                    "target": target_name, "model": model_name,
                    "eligible_origins": int(evaluation_mask.sum() * len(origin_indices)),
                    "predicted_origins": int(np.isfinite(prediction[evaluation_mask]).sum()),
                    "factor_days": int(valid_factor_day.sum()),
                    "first_factor_date": (
                        observation_dates[np.flatnonzero(valid_factor_day)[0]].date().isoformat()
                        if valid_factor_day.any() else ""
                    ),
                })
        print(
            f"Forecast candidates {candidate_index}/{len(candidate_keys)}: {method}/{window}; "
            f"elapsed {time.perf_counter() - started_at:.1f}s", flush=True
        )
    del factors_df
    # One mask across every factor, window, target and model prevents favorable sample changes.
    common_origin_mask = np.logical_and.reduce([
        np.isfinite(payload["prediction"]) & (payload["prediction"] > 0)
        for payload in forecast_payloads.values()
    ])
    common_origin_mask &= evaluation_mask[:, None]
    if not common_origin_mask[years == 2025].any() or not common_origin_mask[years == 2026].any():
        raise ValueError("No common 2025 validation or 2026 retrospective evaluation sample")
    daily_loss_records = []
    for (method, window, target_name, model_name), payload in forecast_payloads.items():
        target = target_values_by_name[target_name]
        prediction = payload["prediction"]
        for day_index in np.flatnonzero(common_origin_mask.any(axis=1)):
            valid_origins = common_origin_mask[day_index]
            actual = target[day_index, valid_origins]
            forecast = prediction[day_index, valid_origins]
            daily_loss_records.append({
                "trading_date": observation_dates[day_index].date(),
                "year": int(years[day_index]), "method": method, "lookback_days": window,
                "target": target_name, "model": model_name, "state": state_values[day_index],
                "origins": int(valid_origins.sum()), "zero_targets": int((actual == 0).sum()),
                "qlike": float(np.mean(np.log(forecast * 1e8) + actual / forecast)),
                "mse": float(np.mean((actual - forecast) ** 2)),
                "training_days": int(payload["training_days"][day_index]),
                "training_label_end_date": observation_dates[day_index - 1].date(),
            })
    daily_losses_df = pd.DataFrame(daily_loss_records)
    validation_summary = (
        daily_losses_df[daily_losses_df.year == 2025]
        .groupby(["method", "lookback_days", "target", "model"], observed=True)
        .agg(days=("trading_date", "size"), qlike=("qlike", "mean"), mse=("mse", "mean"))
        .reset_index()
    )
    selected_indices = validation_summary.groupby(["method", "target", "model"]).qlike.idxmin()
    selected_windows_df = validation_summary.loc[selected_indices].sort_values(
        ["target", "model", "method"]
    )
    selected_configurations = {
        (row.method, int(row.lookback_days), row.target, row.model)
        for row in selected_windows_df.itertuples()
    }
    kept_configurations = selected_configurations | {
        (method, window, target, model)
        for method, window in fixed_keys
        for target in target_values_by_name for model in ("scale_exposure", "ridge_intraday")
    }
    daily_losses_df["selected_on_2025"] = [
        (row.method, row.lookback_days, row.target, row.model) in selected_configurations
        for row in daily_losses_df.itertuples()
    ]
    daily_losses_df["prespecified_primary"] = [
        (row.method, row.lookback_days) in fixed_keys for row in daily_losses_df.itertuples()
    ]
    summary_records = []
    for (year, target_name, model_name), comparison_df in daily_losses_df.groupby(
        ["year", "target", "model"]
    ):
        baseline_df = comparison_df[
            (comparison_df.method == "none") & (comparison_df.lookback_days == 0)
        ].set_index("trading_date")
        for (method, window), candidate_df in comparison_df.groupby(["method", "lookback_days"]):
            configuration = (method, int(window), target_name, model_name)
            if configuration not in kept_configurations:
                continue
            candidate_df = candidate_df.set_index("trading_date").loc[baseline_df.index]
            for state in ("all", "low", "middle", "high"):
                state_mask = np.ones(len(candidate_df), dtype=bool) if state == "all" else (
                    candidate_df.state.to_numpy() == state
                )
                if not state_mask.any():
                    continue
                qlike_difference = (
                    candidate_df.qlike.to_numpy() - baseline_df.qlike.to_numpy()
                )[state_mask]
                mse_difference = (
                    candidate_df.mse.to_numpy() - baseline_df.mse.to_numpy()
                )[state_mask]
                # State-specific intervals are descriptive; avoid pretending discontinuous
                # state-filtered dates form contiguous 5-day blocks.
                if state == "all":
                    qlike_low, qlike_high = paired_day_block_intervals(qlike_difference, seed=20260907)
                    mse_low, mse_high = paired_day_block_intervals(mse_difference, seed=20260907)
                else:
                    qlike_low = qlike_high = mse_low = mse_high = float("nan")
                candidate_mse = float(candidate_df.mse.to_numpy()[state_mask].mean())
                baseline_mse = float(baseline_df.mse.to_numpy()[state_mask].mean())
                summary_records.append({
                    "year": int(year), "target": target_name, "model": model_name,
                    "method": method, "lookback_days": int(window), "state": state,
                    "days": int(state_mask.sum()),
                    "origins": int(candidate_df.origins.to_numpy()[state_mask].sum()),
                    "selected_on_2025": configuration in selected_configurations,
                    "prespecified_primary": (method, int(window)) in fixed_keys,
                    "qlike": float(candidate_df.qlike.to_numpy()[state_mask].mean()),
                    "qlike_difference_vs_none": float(qlike_difference.mean()),
                    "qlike_difference_ci_low": qlike_low, "qlike_difference_ci_high": qlike_high,
                    "mse": candidate_mse,
                    "mse_ratio_vs_none": candidate_mse / baseline_mse if baseline_mse > 0 else float("nan"),
                    "mse_difference_ci_low": mse_low, "mse_difference_ci_high": mse_high,
                })
    summary_df = pd.DataFrame(summary_records)
    selected_method_comparison = compare_selected_methods(daily_losses_df, output_dir)
    daily_loss_schema = pa.schema([
        pa.field("trading_date", pa.date32(), False), pa.field("year", pa.int16(), False),
        pa.field("method", pa.string(), False), pa.field("lookback_days", pa.int16(), False),
        pa.field("target", pa.string(), False), pa.field("model", pa.string(), False),
        pa.field("state", pa.string(), False), pa.field("origins", pa.int16(), False),
        pa.field("zero_targets", pa.int16(), False), pa.field("qlike", pa.float64(), False),
        pa.field("mse", pa.float64(), False), pa.field("training_days", pa.int16(), False),
        pa.field("training_label_end_date", pa.date32(), False),
        pa.field("selected_on_2025", pa.bool_(), False),
        pa.field("prespecified_primary", pa.bool_(), False),
    ], metadata={b"research_table": b"intraday_daily_losses", b"schema_version": b"1"})
    daily_loss_table = pa.Table.from_pandas(
        daily_losses_df, schema=daily_loss_schema, preserve_index=False
    ).replace_schema_metadata(daily_loss_schema.metadata)
    pq.write_table(daily_loss_table, output_dir / "daily_losses.parquet", compression="zstd")
    daily_losses_df.to_csv(output_dir / "daily_losses.csv", index=False, encoding="utf-8-sig")
    validation_summary.to_csv(output_dir / "validation_candidates.csv", index=False, encoding="utf-8-sig")
    selected_windows_df.to_csv(output_dir / "windows_selected_on_2025.csv", index=False, encoding="utf-8-sig")
    summary_df.to_csv(output_dir / "forecast_summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(coverage_records).to_csv(output_dir / "coverage.csv", index=False, encoding="utf-8-sig")
    forecast_schema = pa.schema([
        pa.field("trading_date", pa.date32(), False), pa.field("origin_minute_index", pa.int16(), False),
        pa.field("target_end_minute_index", pa.int16(), False),
        pa.field("target_start_minute_index", pa.int16(), False),
        pa.field("label_minute_count", pa.int8(), False),
        pa.field("session_number", pa.int8(), False),
        pa.field("origin_bar_at", pa.timestamp("us", tz="Asia/Shanghai"), False),
        pa.field("last_feature_bar_at", pa.timestamp("us", tz="Asia/Shanghai"), False),
        pa.field("training_label_end_date", pa.date32(), False),
        pa.field("factor_fit_end_date", pa.date32(), True),
        pa.field("method", pa.string(), False), pa.field("lookback_days", pa.int16(), False),
        pa.field("target_name", pa.string(), False), pa.field("model", pa.string(), False),
        pa.field("target", pa.float64(), False), pa.field("prediction", pa.float64(), False),
    ], metadata={b"research_table": b"intraday_selected_forecasts", b"schema_version": b"1"})
    common_day_indices, common_origin_indices = np.nonzero(common_origin_mask)
    with pq.ParquetWriter(
        output_dir / "minute_forecasts.parquet", forecast_schema, compression="zstd"
    ) as forecast_writer:
        for configuration in sorted(kept_configurations):
            method, window, target_name, model_name = configuration
            output_forecasts_df = pd.DataFrame({
                "trading_date": observation_dates[common_day_indices].date,
                "origin_minute_index": origin_indices[common_origin_indices],
                "target_end_minute_index": origin_indices[common_origin_indices] + 15,
                "target_start_minute_index": origin_indices[common_origin_indices] + 1,
                "label_minute_count": 15,
                "session_number": session_numbers[common_day_indices, origin_indices[common_origin_indices]],
                "origin_bar_at": bar_times_df.to_numpy()[common_day_indices, origin_indices[common_origin_indices]],
                "last_feature_bar_at": bar_times_df.to_numpy()[common_day_indices, origin_indices[common_origin_indices]],
                "training_label_end_date": observation_dates[common_day_indices - 1].date,
                "factor_fit_end_date": pd.to_datetime(
                    forecast_payloads[configuration]["factor_fit_end_date"][common_day_indices]
                ).date,
                "method": method, "lookback_days": window,
                "target_name": target_name, "model": model_name,
                "target": target_values_by_name[target_name][common_origin_mask],
                "prediction": forecast_payloads[configuration]["prediction"][common_origin_mask],
            })
            if not (
                (output_forecasts_df.last_feature_bar_at <= output_forecasts_df.origin_bar_at).all()
                and (output_forecasts_df.training_label_end_date < output_forecasts_df.trading_date).all()
            ):
                raise ValueError("Forecast cutoff evidence failed before writing")
            forecast_writer.write_table(pa.Table.from_pandas(
                output_forecasts_df, schema=forecast_schema, preserve_index=False
            ).replace_schema_metadata(forecast_schema.metadata))
    daily_har_summary = run_daily_har(
        observation_dates, daily_rv, daily_filtered_wsd, output_dir
    )
    plot_df = summary_df[
        (summary_df.year == 2026) & (summary_df.state == "all")
        & (summary_df.target == "RV15") & summary_df.selected_on_2025
    ].copy()
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), constrained_layout=True)
    for axis, model_name in zip(axes, ("scale_exposure", "ridge_intraday")):
        model_df = plot_df[plot_df.model == model_name].sort_values("qlike_difference_vs_none")
        positions = np.arange(len(model_df))
        axis.barh(positions, model_df.qlike_difference_vs_none, color="#24798a")
        axis.hlines(
            positions, model_df.qlike_difference_ci_low, model_df.qlike_difference_ci_high,
            color="#30343a", linewidth=1.7,
        )
        axis.set_yticks(positions, [
            f"{row.method}/{row.lookback_days}" for row in model_df.itertuples()
        ])
        axis.axvline(0, color="#555555", linewidth=0.8)
        axis.set_xlabel("Daily QLIKE difference vs unfiltered (lower is better)")
        axis.set_title(model_name + " | 2026 retrospective")
        axis.grid(axis="x", alpha=0.2)
    fig.suptitle("15-minute RV forecasts | windows selected on 2025 | paired 5-day bootstrap")
    fig.savefig(output_dir / "forecast_qlike_comparison.png", dpi=170)
    plt.close(fig)
    selected_2026 = summary_df[
        (summary_df.year == 2026) & (summary_df.state == "all")
        & summary_df.selected_on_2025 & (summary_df.method != "none")
    ]
    best_rows = selected_2026.sort_values("qlike_difference_vs_none").groupby(
        ["target", "model"], sort=False
    ).head(1)
    lines = [
        "# 日内预测实验报告", "",
        "这是在已探索历史上的回顾评估。窗口只按 2025 年验证集选择并固定用于 2026 年，"
        "没有把 2026 年称为新的未触碰测试集。", "",
        f"输入 {day_count} 个交易日；每个完整交易日 {len(origin_indices)} 个共同候选预测时点；"
        "时点位于 bar 收盘后，预测随后 15 个分钟 bar，严格保持同一 session。", "",
        "模型一将过去 15 分钟去周期方差速率与过去 20 日平均速率各赋权一半，"
        "乘回未来区间周期暴露；模型二以过去 5/15 分钟、当前 session 已经过分钟、"
        "过去 1/5/22 日的去周期速率做 ridge 回归，预测区间去周期速率后乘回暴露。"
        "回归每日只拟合以前 252 个交易日，至少 126 个有完整训练数据的交易日；"
        "标准化、截距和正预测下限均来自同一历史训练集。固定 ridge 强度为样本数的 0.01，未用评估结果调参。", "",
        "RV 标签是未来 15 分钟平方收益之和；BPV 标签为这 15 个右端点的相邻收益绝对乘积乘 π/2，"
        "第一个乘积使用已观察的 origin 收益，全部在同一 session 内。"
        "因此 BPV15 共有 15 项，并非只在未来 15 个收益内部计算的 14 对乘积。"
        "BPV 暴露采用输入 contribution_factor；median_mixed 是原有混合贡献曲线兼容对照，"
        "不是已证明的标准差周期估计，也不把它等同 WSD。", "",
        "所有方法、窗口、模型、标签共享同一有效预测时点掩码。每日先平均分钟损失，"
        "再对交易日等权平均；全体状态的区间来自 200 次、5 个连续交易日移动块配对 bootstrap。"
        "状态按前一日 RV 相对于此前 252 日三分位确定，状态表仅作描述，不为非连续子样本伪造连续块区间。", "",
        "QLIKE 使用 log(预测×10^8)+实际/预测，允许零标签且不替换标签；"
        "与正标签常用 y/p−log(y/p)−1 的模型排序和配对差相同。"
        "损失绝对水平依赖计量单位，因此只解释同样本配对差，不解释 QLIKE 比率。"
        "零预测由历史目标均值×10^-6（最低10^-16）的下限避免。"
        "因重叠预测和多候选选择，bootstrap 区间仅是描述性不确定性范围，未作多重比较校正。", "",
        "## 2026 年验证集选窗后的结果", "",
        "| 标签 | 模型 | 最低 QLIKE 差的方法 | 窗口 | QLIKE 差 | 95% 配对区间 | MSE 比率 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in best_rows.itertuples():
        lines.append(
            f"| {row.target} | {row.model} | {row.method} | {row.lookback_days} | "
            f"{row.qlike_difference_vs_none:.6f} | "
            f"[{row.qlike_difference_ci_low:.6f}, {row.qlike_difference_ci_high:.6f}] | "
            f"{row.mse_ratio_vs_none:.4f} |"
        )
    lines.extend([
        "", "表中的“最低”是结果描述，不是另一次部署选择；全部候选和预先指定对照见 CSV。"
        "若区间跨零，应表述为证据不足；曲线更平坦本身不是预测有效性证据。", "",
        "## 相对现有混合贡献曲线的直接比较", "",
        "优于完全不去周期化，不能推出优于既有 median_mixed。以下对双方都使用只按 2025 年选定的窗口，"
        "在同一 2026 年交易日配对比较。负差表示候选损失更低；区间跨零时，没有明确的替换依据。"
        "完整方法对照见 selected_method_comparison.csv；区间仍为 200 次五交易日块 bootstrap，未作多重比较校正。", "",
        "| 标签 | 模型 | 方法/窗口 | median_mixed 窗口 | QLIKE 差 | 95% 配对区间 | MSE 比率 |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ])
    for row in selected_method_comparison[
        selected_method_comparison.method.isin(["boudt_wsd", "fff_wsd", "state_wsd"])
    ].itertuples():
        lines.append(
            f"| {row.target} | {row.model} | {row.method}/{row.lookback_days} | "
            f"{row.baseline_lookback_days} | {row.qlike_difference_vs_median_mixed:.6f} | "
            f"[{row.qlike_difference_ci_low:.6f}, {row.qlike_difference_ci_high:.6f}] | "
            f"{row.mse_ratio_vs_median_mixed:.4f} |"
        )
    lines.extend([
        "",
        "## 日级 HAR/HARP 改编", "",
        "另比较原始 RV 预测变量与 WSD/120 滤波 RV 预测变量，目标都为原尺度未来 1/5/22 日平均 RV。"
        "使用 252 日训练和同样的 ridge 正预测处理，至少 126 日，"
        "训练标签必须在当前预测日之前完整结束；多日预测的 bootstrap 块长为 max(5,h)。"
        "跨入次年才结束的验证标签已剔除，避免 2025 和 2026 标签区间重叠。"
        "这是适配不足千日样本的实验，不是 Dumitru 等人 1000 日滚动训练的精确复现。"
        "当前数据不含隔夜方差，因此目标也是日内交易时段 RV。", "",
        "## 输出", "",
        "- forecast_summary.csv：固定对照与验证选窗后的等权日损失、状态分层、配对区间。",
        "- validation_candidates.csv、windows_selected_on_2025.csv：选窗依据和固定结果。",
        "- selected_method_comparison.csv：验证选窗后，相对既有 median_mixed 的直接配对比较。",
        "- daily_losses.parquet / CSV：全部候选的共同样本逐日损失，可复核汇总。",
        "- minute_forecasts.parquet：固定对照和验证选中方案的逐预测时点原尺度预测。",
        "- coverage.csv：完整日、因素热身和有效预测覆盖。",
        "- daily_har_summary.csv、daily_har_predictions.parquet：日级改编对照。",
        "- forecast_qlike_comparison.png：RV15 结果与配对区间。",
        "", "方法来源：[Dumitru 等阅读笔记](../../literature/notes/06_dumitru_hizmeri_izzeldin_2025.md)、"
        "[Dette 等阅读笔记](../../literature/notes/05_dette_2022.md)。"
        "本实验没有修改既有 Notebook 或正式湖，也没有产生已获未来收益验证的交易策略。", "",
    ])
    (output_dir / "REPORT.md").write_text("\n".join(lines), encoding="utf-8")
    provenance = {
        "python_executable": sys.executable,
        "runtime_seconds": round(time.perf_counter() - started_at, 3),
        "data_start": observation_dates[0].date().isoformat(),
        "data_end": observation_dates[-1].date().isoformat(),
        "day_count": day_count, "origins_per_complete_day": len(origin_indices),
        "common_validation_origins": int(common_origin_mask[years == 2025].sum()),
        "common_retrospective_origins": int(common_origin_mask[years == 2026].sum()),
        "zero_rv15_labels": int((target_values_by_name["RV15"][common_origin_mask] == 0).sum()),
        "zero_bpv15_labels": int((target_values_by_name["BPV15"][common_origin_mask] == 0).sum()),
        "candidate_count": len(candidate_keys),
        "bootstrap_replications": 200, "bootstrap_seed": 20260907,
        "bootstrap_block_days": 5,
        "input_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (observation_path, factor_path)
        },
        "causality_checks": {
            "factor_fit_end_before_target_day": True,
            "feature_last_minute_at_origin": True,
            "target_same_session": True,
            "ridge_fit_uses_only_complete_prior_days": True,
            "window_selection_uses_2025_only": True,
            "all_candidates_share_origin_mask": True,
        },
        "daily_har_summary_rows": len(daily_har_summary),
    }
    (output_dir / "provenance.json").write_text(
        json.dumps(provenance, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    for path, schema in (
        (output_dir / "daily_losses.parquet", daily_loss_schema),
        (output_dir / "minute_forecasts.parquet", forecast_schema),
    ):
        if not pq.read_schema(path).equals(schema, check_metadata=True):
            raise ValueError(f"Output schema failed readback: {path}")
    print(json.dumps(provenance, ensure_ascii=False, indent=2), flush=True)


if __name__ == "__main__":
    main()
