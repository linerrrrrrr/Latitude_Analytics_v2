"""Completed clock-event features -> a fixed future RV15 label.

The three clock settings are frozen before evaluation. No future event boundary is
predicted or interpolated. Only this project's artifacts are read; output is local.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.optimize import nnls

from data_contracts import MINUTE_OBSERVATION_SCHEMA, PERIODICITY_FACTOR_SCHEMA
from run_forecasting import rolling_ridge_predictions, paired_day_block_intervals


def rolling_nonnegative_ridge_predictions(feature_values, target_values, training_days=252,
                                         minimum_training_days=126, ridge_fraction=0.01):
    """历史RMS缩放的非负ridge：探索性修正，保留原尺度真实零标签。"""
    day_count, origin_count, feature_count = feature_values.shape
    valid_rows = np.isfinite(feature_values).all(axis=2) & np.isfinite(target_values)
    if (feature_values[valid_rows] < 0).any() or (target_values[valid_rows] < 0).any():
        raise ValueError("The nonnegative-rate model requires nonnegative features and targets")
    design = np.concatenate([np.ones((day_count, origin_count, 1)), feature_values], axis=2)
    design = np.where(valid_rows[:, :, None], design, 0.0)
    target_clean = np.where(valid_rows, target_values, 0.0)
    daily_counts = valid_rows.sum(axis=1)
    daily_sums = design.sum(axis=1)
    daily_gram = np.einsum("doi,doj->dij", design, design)
    daily_cross = np.einsum("doi,do->di", design, target_clean)
    cumulative_statistics = [np.concatenate([np.zeros_like(values[:1]), np.cumsum(values, axis=0)])
                             for values in [daily_counts, daily_sums, daily_gram, daily_cross]]
    cumulative_valid_days = np.r_[0, (daily_counts > 0).cumsum()]
    predictions = np.full((day_count, origin_count), np.nan)
    fit_counts = np.zeros(day_count, dtype=np.int16)
    historical_floors = np.full(day_count, np.nan)
    for day_number in range(1, day_count):
        training_start = max(0, day_number - training_days)
        fit_counts[day_number] = cumulative_valid_days[day_number] - cumulative_valid_days[training_start]
        if fit_counts[day_number] < minimum_training_days:
            continue
        count, column_sum, gram, cross = [values[day_number] - values[training_start] for values in cumulative_statistics]
        historical_target_mean = cross[0] / count
        intercept_lower_bound = max(historical_target_mean * 1e-6, 1e-16)
        historical_floors[day_number] = intercept_lower_bound
        column_scale = np.sqrt(np.maximum(np.diag(gram) / count, 0.0))
        column_scale[column_scale == 0] = 1.0
        standardized_gram = gram / np.outer(column_scale, column_scale)
        standardized_cross = (cross - intercept_lower_bound * column_sum) / column_scale
        penalty = ridge_fraction * count * np.eye(feature_count + 1)
        penalty[0, 0] = 0.0
        cholesky_lower = np.linalg.cholesky(standardized_gram + penalty)
        # ||L.T beta - solve(L, X.T y)||² has the exact penalized Gram objective.
        coefficients, _ = nnls(cholesky_lower.T, np.linalg.solve(cholesky_lower, standardized_cross))
        current_design = np.column_stack([np.ones(origin_count), feature_values[day_number]])
        predictions[day_number] = intercept_lower_bound + (current_design / column_scale) @ coefficients
    return predictions, fit_counts, historical_floors


def main():
    started = time.perf_counter()
    project_dir = Path(__file__).resolve().parent
    output_dir = project_dir / "results" / "clock_forecasting"
    output_dir.mkdir(parents=True, exist_ok=True)
    observation_path = project_dir / "data" / "minute_observations.parquet"
    factor_path = project_dir / "results" / "periodicity_factors.parquet"
    events_path = project_dir / "results" / "clocks" / "events.csv"
    threshold_path = project_dir / "results" / "clocks" / "training_thresholds.csv"
    minute_forecast_path = project_dir / "results" / "forecasting" / "minute_forecasts.parquet"
    observation_table = pq.read_table(observation_path).cast(MINUTE_OBSERVATION_SCHEMA)
    observation_df = observation_table.to_pandas().sort_values(["trading_date", "minute_index"]).reset_index(drop=True)
    factor_table = pq.read_table(factor_path, filters=[("method", "=", "boudt_wsd"), ("lookback_days", "=", 120)]).cast(PERIODICITY_FACTOR_SCHEMA)
    factor_df = factor_table.to_pandas()
    dates = pd.DatetimeIndex(sorted(pd.to_datetime(observation_df.trading_date.unique())))
    date_values = np.array(dates.date)
    day_lookup = {date: index for index, date in enumerate(date_values)}
    day_count = len(dates)
    if len(observation_df) != day_count * 240 or not np.array_equal(observation_df.minute_index, np.tile(np.arange(240), day_count)):
        raise ValueError("A complete ordered 240-slot grid is required")
    returns = observation_df.log_return.to_numpy(dtype=float).reshape(day_count, 240)
    rv_values = observation_df.rv_contribution.to_numpy(dtype=float).reshape(day_count, 240)
    bar_timestamps = observation_df.bar_at.to_numpy().reshape(day_count, 240)
    if not np.isfinite(returns).all() or not np.isfinite(rv_values).all():
        raise ValueError("Event return construction requires finite retained returns and RV")
    if (factor_df.factor_fit_end_date >= factor_df.trading_date).fillna(False).any():
        raise ValueError("Periodicity is not known before its target day")
    variance_factors = factor_df.pivot(index="trading_date", columns="minute_index", values="variance_factor").reindex(index=date_values, columns=np.arange(240)).to_numpy(dtype=float)
    factor_fit_dates = factor_df.groupby("trading_date").factor_fit_end_date.max().reindex(date_values).to_numpy()
    origin_indices = np.r_[14:105, 134:225]
    if len(origin_indices) != 182:
        raise AssertionError("Unexpected origin count")
    future_indices = origin_indices[:, None] + np.arange(1, 16)
    targets = rv_values[:, future_indices].sum(axis=2)
    future_exposure = variance_factors[:, future_indices].sum(axis=2)
    normalized_targets = targets / future_exposure
    absolute_origin_slots = np.arange(day_count)[:, None] * 240 + origin_indices[None, :]
    prior_daily_rv_rate = np.r_[np.nan, rv_values.sum(axis=1)[:-1] / 240.0]
    return_prefix = np.c_[np.zeros(day_count), np.cumsum(returns, axis=1)]
    factor_prefix = np.c_[np.zeros(day_count), np.cumsum(variance_factors, axis=1)]

    clock_specs = [
        ("time15", "time:none:0"),
        ("ex_ante_wsd120", "ex_ante_periodic_variance:boudt_wsd:120"),
        ("realized_bpv_wsd120", "realized_deseasonal_bpv:boudt_wsd:120"),
    ]
    threshold_df = pd.read_csv(threshold_path)
    selected_thresholds = threshold_df[threshold_df.clock_id.isin([clock_id for _, clock_id in clock_specs])]
    if len(selected_thresholds) != 3 or not (pd.to_datetime(selected_thresholds.fit_end_date) <= pd.Timestamp("2024-12-31")).all():
        raise ValueError("All three clock thresholds must be calibrated by the training end")
    all_events_df = pd.read_csv(events_path)
    predictions_by_clock, features_by_clock, event_evidence_by_clock, coverage_rows = {}, {}, {}, []
    training_counts_by_clock = {}
    prediction_at_floor_by_clock = {}
    for clock_label, clock_id in clock_specs:
        event_df = all_events_df[all_events_df.clock_id == clock_id].copy()
        event_df["trading_date"] = pd.to_datetime(event_df.trading_date).dt.date
        event_df = event_df.sort_values(["trading_date", "minute_index"]).reset_index(drop=True)
        if event_df.empty or event_df.duplicated(["trading_date", "minute_index"]).any():
            raise ValueError(f"Missing or repeated events: {clock_id}")
        event_day_indices = event_df.trading_date.map(day_lookup).to_numpy(dtype=int)
        event_end_slots = event_df.minute_index.to_numpy(dtype=int)
        prior_event_slot = event_df.groupby(["trading_date", "session_number"]).minute_index.shift(1)
        first_session_slot = (event_end_slots // 120) * 120
        event_start_slots = np.where(prior_event_slot.notna(), prior_event_slot.fillna(0).to_numpy() + 1, first_session_slot).astype(int)
        if not ((event_start_slots // 120) == (event_end_slots // 120)).all():
            raise ValueError("An event return crosses a session")
        interval_returns = return_prefix[event_day_indices, event_end_slots + 1] - return_prefix[event_day_indices, event_start_slots]
        interval_exposures = factor_prefix[event_day_indices, event_end_slots + 1] - factor_prefix[event_day_indices, event_start_slots]
        if not (np.isfinite(interval_exposures) & (interval_exposures > 0)).all():
            raise ValueError("Event normalization has missing or nonpositive historical exposure")
        # All clocks use the same historical WSD120 curve to isolate event timing.
        event_variance_rates = interval_returns**2 / interval_exposures
        event_durations = event_end_slots - event_start_slots + 1
        if not np.array_equal(event_durations, event_df.duration_minutes.to_numpy()):
            raise ValueError("Event interval disagrees with the clock's recorded duration")
        event_absolute_slots = event_day_indices * 240 + event_end_slots
        last_event_indices = np.searchsorted(event_absolute_slots, absolute_origin_slots, side="right") - 1
        history_available = last_event_indices >= 11
        clipped_last = np.maximum(last_event_indices, 0)
        rate_prefix = np.r_[0.0, np.cumsum(event_variance_rates)]
        event_features = np.full((day_count, len(origin_indices), 7), np.nan)
        for feature_number, event_window in enumerate([1, 3, 12]):
            mean_event_rate = (rate_prefix[clipped_last + 1] - rate_prefix[np.maximum(clipped_last + 1 - event_window, 0)]) / event_window
            event_features[:, :, feature_number] = np.where(history_available, mean_event_rate, np.nan)
        event_features[:, :, 3] = np.where(history_available, event_durations[clipped_last], np.nan)
        event_age = absolute_origin_slots - event_absolute_slots[clipped_last]
        event_features[:, :, 4] = np.where(history_available, event_age, np.nan)
        event_features[:, :, 5] = prior_daily_rv_rate[:, None]
        same_session = (event_day_indices[clipped_last] == np.arange(day_count)[:, None]) & (event_end_slots[clipped_last] // 120 == origin_indices[None, :] // 120)
        event_features[:, :, 6] = np.where(history_available, same_session.astype(float), np.nan)
        if not (event_absolute_slots[clipped_last][history_available] <= absolute_origin_slots[history_available]).all():
            raise AssertionError("An unfinished future event entered a feature")
        rate_predictions, training_counts = rolling_ridge_predictions(event_features, normalized_targets,
                                                                      last_label_delay=1, training_days=252,
                                                                      minimum_training_days=126, ridge_fraction=0.01)
        predictions_by_clock[clock_label] = rate_predictions * future_exposure
        # Reconstruct the shared regression's historical floor without changing it.
        valid_training_rows = np.isfinite(event_features).all(axis=2) & np.isfinite(normalized_targets)
        row_count_prefix = np.r_[0, valid_training_rows.sum(axis=1).cumsum()]
        target_sum_prefix = np.r_[0.0, np.where(valid_training_rows, normalized_targets, 0.0).sum(axis=1).cumsum()]
        prediction_floors = np.full(day_count, np.nan)
        for day_number in range(1, day_count):
            training_start = max(0, day_number - 252)
            row_count = row_count_prefix[day_number] - row_count_prefix[training_start]
            if row_count > 0:
                historical_target_mean = (target_sum_prefix[day_number] - target_sum_prefix[training_start]) / row_count
                prediction_floors[day_number] = max(historical_target_mean * 1e-6, 1e-16)
        prediction_at_floor_by_clock[clock_label] = np.isfinite(rate_predictions) & np.isclose(rate_predictions, prediction_floors[:, None], rtol=1e-10, atol=0.0)
        features_by_clock[clock_label] = event_features
        training_counts_by_clock[clock_label] = training_counts
        event_evidence_by_clock[clock_label] = {
            "last_event_indices": clipped_last, "event_day_indices": event_day_indices,
            "event_start_slots": event_start_slots, "event_end_slots": event_end_slots,
            "interval_returns": interval_returns, "interval_exposures": interval_exposures,
            "event_variance_rates": event_variance_rates, "event_durations": event_durations,
        }
        nnls_label = clock_label + "_nnls"
        nnls_rates, nnls_counts, nnls_floors = rolling_nonnegative_ridge_predictions(event_features, normalized_targets)
        predictions_by_clock[nnls_label] = nnls_rates * future_exposure
        features_by_clock[nnls_label] = event_features
        training_counts_by_clock[nnls_label] = nnls_counts
        event_evidence_by_clock[nnls_label] = event_evidence_by_clock[clock_label]
        prediction_at_floor_by_clock[nnls_label] = np.isfinite(nnls_rates) & np.isclose(nnls_rates, nnls_floors[:, None], rtol=1e-10, atol=0.0)
        for year in [2025, 2026]:
            year_mask = dates.year == year
            for evaluated_model in [clock_label, nnls_label]:
                available_predictions = np.isfinite(predictions_by_clock[evaluated_model]) & (predictions_by_clock[evaluated_model] > 0)
                coverage_rows.append({"clock": clock_label, "model": evaluated_model, "year": year,
                                      "candidate_origins": int(year_mask.sum()) * 182,
                                      "twelve_completed_event_origins": int(history_available[year_mask].sum()),
                                      "available_model_origins": int(available_predictions[year_mask].sum()),
                                      "model_days": int(available_predictions[year_mask].any(axis=1).sum())})
        print(f"[clock forecast] {clock_label}: {len(event_df)} completed events; elapsed={time.perf_counter()-started:.1f}s", flush=True)

    # The existing minute-ridge baseline is re-scored on this exact common origin mask.
    baseline_table = pq.read_table(minute_forecast_path,
                                  filters=[("method", "=", "none"), ("lookback_days", "=", 0),
                                           ("target_name", "=", "RV15"), ("model", "=", "ridge_intraday")],
                                  columns=["trading_date", "origin_minute_index", "target", "prediction"])
    baseline_df = baseline_table.to_pandas()
    if baseline_df.duplicated(["trading_date", "origin_minute_index"]).any():
        raise ValueError("Repeated baseline forecasts")
    baseline_predictions = baseline_df.pivot(index="trading_date", columns="origin_minute_index", values="prediction").reindex(index=date_values, columns=origin_indices).to_numpy(dtype=float)
    baseline_targets = baseline_df.pivot(index="trading_date", columns="origin_minute_index", values="target").reindex(index=date_values, columns=origin_indices).to_numpy(dtype=float)
    shared_origins = (dates.year.to_numpy() >= 2025)[:, None] & np.isfinite(baseline_predictions) & (baseline_predictions > 0)
    for forecast_values in predictions_by_clock.values():
        shared_origins &= np.isfinite(forecast_values) & (forecast_values > 0)
    shared_origins &= np.isfinite(targets)
    if not shared_origins[dates.year == 2025].any() or not shared_origins[dates.year == 2026].any():
        raise ValueError("No common validation or retrospective origins")
    if not np.allclose(targets[shared_origins], baseline_targets[shared_origins], rtol=1e-12, atol=1e-18):
        raise ValueError("The baseline does not use identical raw RV15 targets")
    for row in coverage_rows:
        row["common_origins"] = int(shared_origins[dates.year == row["year"]].sum())
    pd.DataFrame(coverage_rows).to_csv(output_dir / "coverage.csv", index=False)
    all_predictions = {**predictions_by_clock, "minute_none_ridge": baseline_predictions}
    daily_rows = []
    floor_rows = []
    for model_name, prediction_values in all_predictions.items():
        for day_number in np.flatnonzero(shared_origins.any(axis=1)):
            valid = shared_origins[day_number]
            actual, prediction = targets[day_number, valid], prediction_values[day_number, valid]
            origin_qlike = np.log(prediction * 1e8) + actual / prediction
            daily_rows.append({"trading_date": date_values[day_number], "year": int(dates.year[day_number]),
                               "model": model_name, "origins": int(valid.sum()),
                               "qlike": float(np.mean(origin_qlike)),
                               "mse": float(np.mean((actual - prediction)**2))})
        if model_name in prediction_at_floor_by_clock:
            for year in [2025, 2026]:
                year_origins = shared_origins & (dates.year.to_numpy() == year)[:, None]
                floor_mask = prediction_at_floor_by_clock[model_name][year_origins]
                prediction = prediction_values[year_origins]
                actual = targets[year_origins]
                origin_qlike = np.log(prediction * 1e8) + actual / prediction
                floor_rows.append({"year": year, "model": model_name, "origins": int(year_origins.sum()),
                                   "at_historical_floor_count": int(floor_mask.sum()),
                                   "at_historical_floor_rate": float(np.mean(floor_mask)),
                                   "mean_qlike": float(origin_qlike.mean()),
                                   "mean_qlike_at_floor": float(origin_qlike[floor_mask].mean()) if floor_mask.any() else np.nan,
                                   "mean_qlike_above_floor": float(origin_qlike[~floor_mask].mean()),
                                   "max_origin_qlike": float(origin_qlike.max()),
                                   "minimum_prediction": float(prediction.min())})
    daily_loss_df = pd.DataFrame(daily_rows)
    daily_loss_df.to_csv(output_dir / "daily_losses.csv", index=False)
    floor_df = pd.DataFrame(floor_rows)
    floor_df.to_csv(output_dir / "floor_diagnostics.csv", index=False)
    summary_rows = []
    for year, year_df in daily_loss_df.groupby("year"):
        for baseline_name in ["time15", "time15_nnls", "minute_none_ridge"]:
            baseline_losses = year_df[year_df.model == baseline_name].set_index("trading_date")
            for model_name, model_df in year_df.groupby("model"):
                model_losses = model_df.set_index("trading_date").loc[baseline_losses.index]
                qlike_differences = model_losses.qlike.to_numpy() - baseline_losses.qlike.to_numpy()
                mse_differences = model_losses.mse.to_numpy() - baseline_losses.mse.to_numpy()
                q_low, q_high = paired_day_block_intervals(qlike_differences, seed=20260907, replications=200, block_days=5)
                m_low, m_high = paired_day_block_intervals(mse_differences, seed=20260907, replications=200, block_days=5)
                summary_rows.append({"year": int(year), "model": model_name, "baseline": baseline_name,
                                     "days": len(model_losses), "origins": int(model_losses.origins.sum()),
                                     "qlike": float(model_losses.qlike.mean()),
                                     "qlike_difference": float(qlike_differences.mean()),
                                     "qlike_difference_ci_low": q_low, "qlike_difference_ci_high": q_high,
                                     "mse": float(model_losses.mse.mean()),
                                     "mse_ratio": float(model_losses.mse.mean()/baseline_losses.mse.mean()),
                                     "mse_difference_ci_low": m_low, "mse_difference_ci_high": m_high})
    summary_df = pd.DataFrame(summary_rows)
    summary_df["analysis_context"] = np.where(summary_df.model.str.endswith("_nnls"),
                                               "exploratory_correction_after_floor_failure",
                                               np.where(summary_df.model == "minute_none_ridge", "existing_minute_baseline", "initial_clock_feature_comparison"))
    summary_df.to_csv(output_dir / "summary.csv", index=False)

    forecast_schema = pa.schema([
        pa.field("trading_date", pa.date32(), False), pa.field("origin_minute_index", pa.int16(), False),
        pa.field("origin_at", pa.timestamp("us", tz="Asia/Shanghai"), False),
        pa.field("target_end_minute_index", pa.int16(), False), pa.field("model", pa.string(), False),
        pa.field("model_family", pa.string(), False), pa.field("analysis_context", pa.string(), False),
        pa.field("target_rv15", pa.float64(), False), pa.field("prediction_rv15", pa.float64(), False),
        pa.field("prediction_at_historical_floor", pa.bool_(), False),
        pa.field("minute_none_prediction_rv15", pa.float64(), False),
        pa.field("future_variance_exposure", pa.float64(), False),
        pa.field("last_event_at", pa.timestamp("us", tz="Asia/Shanghai"), False),
        pa.field("last_event_start_minute_index", pa.int16(), False),
        pa.field("last_event_end_minute_index", pa.int16(), False),
        pa.field("last_event_squared_return", pa.float64(), False),
        pa.field("last_event_variance_exposure", pa.float64(), False),
        pa.field("event_rate_lag1", pa.float64(), False), pa.field("event_rate_mean3", pa.float64(), False),
        pa.field("event_rate_mean12", pa.float64(), False), pa.field("last_event_duration_minutes", pa.float64(), False),
        pa.field("event_age_trading_minutes", pa.float64(), False), pa.field("lagged_daily_rv_rate", pa.float64(), False),
        pa.field("last_event_same_session", pa.bool_(), False), pa.field("training_days", pa.int16(), False),
        pa.field("training_label_end_date", pa.date32(), False), pa.field("factor_fit_end_date", pa.date32(), False),
    ], metadata={b"research_table": b"clock_event_rv15_forecasts", b"schema_version": b"1",
                 b"target": b"15 future same-session minute RV contributions",
                 b"normalization": b"all clocks use historical boudt_wsd120 variance factors"})
    day_indices, origin_positions = np.nonzero(shared_origins)
    event_checks = []
    with pq.ParquetWriter(output_dir / "clock_forecasts.parquet", forecast_schema, compression="zstd") as writer:
        for clock_label in predictions_by_clock:
            features = features_by_clock[clock_label][shared_origins]
            evidence = event_evidence_by_clock[clock_label]
            last_events = evidence["last_event_indices"][shared_origins]
            last_event_days = evidence["event_day_indices"][last_events]
            last_event_ends = evidence["event_end_slots"][last_events]
            forecast_df = pd.DataFrame({
                "trading_date": date_values[day_indices], "origin_minute_index": origin_indices[origin_positions],
                "origin_at": bar_timestamps[day_indices, origin_indices[origin_positions]],
                "target_end_minute_index": origin_indices[origin_positions] + 15, "model": clock_label,
                "model_family": "nonnegative_ridge" if clock_label.endswith("_nnls") else "linear_ridge_floor",
                "analysis_context": "exploratory_correction_after_floor_failure" if clock_label.endswith("_nnls") else "initial_clock_feature_comparison",
                "target_rv15": targets[shared_origins], "prediction_rv15": predictions_by_clock[clock_label][shared_origins],
                "prediction_at_historical_floor": prediction_at_floor_by_clock[clock_label][shared_origins],
                "minute_none_prediction_rv15": baseline_predictions[shared_origins],
                "future_variance_exposure": future_exposure[shared_origins],
                "last_event_at": bar_timestamps[last_event_days, last_event_ends],
                "last_event_start_minute_index": evidence["event_start_slots"][last_events],
                "last_event_end_minute_index": last_event_ends,
                "last_event_squared_return": evidence["interval_returns"][last_events]**2,
                "last_event_variance_exposure": evidence["interval_exposures"][last_events],
                "event_rate_lag1": features[:, 0], "event_rate_mean3": features[:, 1], "event_rate_mean12": features[:, 2],
                "last_event_duration_minutes": features[:, 3], "event_age_trading_minutes": features[:, 4],
                "lagged_daily_rv_rate": features[:, 5], "last_event_same_session": features[:, 6].astype(bool),
                "training_days": training_counts_by_clock[clock_label][day_indices],
                "training_label_end_date": date_values[day_indices - 1], "factor_fit_end_date": factor_fit_dates[day_indices],
            })
            if not ((forecast_df.last_event_at <= forecast_df.origin_at).all()
                    and (forecast_df.training_label_end_date < forecast_df.trading_date).all()
                    and (forecast_df.factor_fit_end_date < forecast_df.trading_date).all()):
                raise ValueError("Clock forecast cutoff check failed")
            writer.write_table(pa.Table.from_pandas(forecast_df, schema=forecast_schema, preserve_index=False).replace_schema_metadata(forecast_schema.metadata))
            # Independent direct sums for a bounded, deterministic sample of exported rows.
            for row in forecast_df.sample(n=min(100, len(forecast_df)), random_state=21).itertuples():
                event_day = day_lookup[row.last_event_at.date()]
                event_sum = np.sum(returns[event_day, row.last_event_start_minute_index:row.last_event_end_minute_index + 1])
                assert np.isclose(event_sum**2, row.last_event_squared_return, rtol=1e-10, atol=1e-18)
                target_day = day_lookup[row.trading_date]
                target_sum = np.sum(rv_values[target_day, row.origin_minute_index + 1:row.origin_minute_index + 16])
                assert np.isclose(target_sum, row.target_rv15, rtol=1e-12, atol=1e-18)
            event_checks.append({"clock": clock_label, "direct_return_and_target_checks": min(100, len(forecast_df))})
    if not pq.read_schema(output_dir / "clock_forecasts.parquet").equals(forecast_schema, check_metadata=True):
        raise AssertionError("Clock forecast Arrow schema failed roundtrip")
    expected_rows = int(shared_origins.sum()) * len(predictions_by_clock)
    if pq.ParquetFile(output_dir / "clock_forecasts.parquet").metadata.num_rows != expected_rows:
        raise AssertionError("Clock forecast row count mismatch")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True)
    for axis, baseline_name in zip(axes, ["time15_nnls", "minute_none_ridge"]):
        plot_df = summary_df[(summary_df.year == 2026) & (summary_df.baseline == baseline_name) & summary_df.model.str.endswith("_nnls")]
        axis.barh(plot_df.model, plot_df.qlike_difference)
        axis.hlines(np.arange(len(plot_df)), plot_df.qlike_difference_ci_low, plot_df.qlike_difference_ci_high, color="black")
        axis.axvline(0, color="grey", linewidth=0.8)
        axis.set(title=f"Exploratory NNLS vs {baseline_name}", xlabel="Paired daily QLIKE difference (lower is better)")
        axis.tick_params(axis="y", labelsize=9)
        axis.grid(axis="x", alpha=0.2)
    fig.suptitle("Completed clock-event features -> fixed RV15: 2026 retrospective")
    fig.savefig(output_dir / "clock_forecast_qlike_2026.png", dpi=160)
    plt.close(fig)
    floor_plot = floor_df[floor_df.year == 2026].copy()
    floor_plot["clock"] = floor_plot.model.str.replace("_nnls", "", regex=False)
    floor_plot["family"] = np.where(floor_plot.model.str.endswith("_nnls"), "NNLS", "Initial linear+floor")
    floor_pivot = floor_plot.pivot(index="clock", columns="family", values="at_historical_floor_rate")
    fig, axis = plt.subplots(figsize=(10, 4), constrained_layout=True)
    positions = np.arange(len(floor_pivot))
    for family_index, family in enumerate(["Initial linear+floor", "NNLS"]):
        axis.barh(positions + (family_index - 0.5) * 0.32, floor_pivot[family] * 100, height=0.3, label=family)
    axis.set_yticks(positions, floor_pivot.index)
    axis.set(title="2026 historical-floor incidence: failure retained, NNLS added", xlabel="Forecasts at the historical numerical floor (%)")
    axis.legend()
    axis.grid(axis="x", alpha=0.2)
    fig.savefig(output_dir / "linear_floor_rates_2026.png", dpi=160)
    plt.close(fig)
    report_lines = [
        "# 完整事件特征到固定 RV15 的预测闭环", "",
        "这是直接固定 15 分钟 horizon 回归；没有预测未来事件边界、没有随机插值，也没有实现 GARCH 整事件预测向固定时钟 horizon 的转换。", "",
        "三种主时钟在评估前冻结：15分钟时间钟、事前 WSD120 相对方差暴露钟、已实现去周期 BPV/WSD120 活动钟。三者一律用事件发生日事前可知的同一 WSD120 方差曲线归一化，以隔离事件取样时点的区别。", "",
        "## 特征与标签", "",
        "每个完整事件的收益为该 session 开始或前一完整事件之后，直到本事件末端的分钟 log_return 之和；特征原子是这段累计收益的平方除以相同区间的历史 WSD120 方差暴露。它不是把分钟 RV 加总后除以暴露，两者统计对象不同。",
        "使用最近1个、3个、12个完整事件的速率均值、上个事件时长、距上个事件的交易分钟年龄、上一交易日原始 RV/240，另加入末事件是否与当前 origin 属于同一 session 的标记。历史事件 lags 可以跨 session；任何单事件收益都不跨 session。没有把当前尚未完成事件的终价提前纳入。年龄按交易分钟计，午休/隔夜不另加墙钟分钟。",
        "origin 为14..104与134..224，每日182个候选点；要求过去15分钟和未来15分钟都在同一 session，因此不同于旧208点口径。事件仅允许末端<=origin；同bar末新完成的事件可用。",
        "原尺度标签为origin之后15根同session RV之和。回归目标=原标签/当天事前WSD120未来区间暴露，再乘回相同暴露得到原尺度预测；未来价格只进入标签。",
        "每日252日回归、至少126个完整训练日，截距、特征标准化、ridge强度和正值下限复用已审查的 rolling_ridge_predictions。最后训练标签日严格早于预测日。三种时钟不使用2026选择配置。", "",
        "追加的 *_nnls 是在观察到初始线性模型负预测/触底机制之后增加的探索性结构修正，不与初始对照混同为事前注册的成功结果。仍使用同样7个非负特征、同样252/126训练日和0.01×训练行数的ridge强度；仅按历史RMS缩放、不减均值，斜率约束非负，intercept不惩罚且下界沿用历史目标均值×10^-6（最低10^-16）。",
        "NNLS通过8维Gram的Cholesky等价最小二乘实现；其目标仍是原尺度平方误差下的条件均值拟合。所有真实零标签保留，不对真实标签加常数；求解中的常数offset仅表示intercept下界。未使用2026损失调整超参数。非负系数假设限制负方向效应，其比较为敏感性实验。", "",
        "## 训练阈值与回顾边界", "",
        "时钟阈值只用截至2024-12-31的训练段标定。训练段事件是训练结束后按冻结阈值回顾重构，可作为训练数据，不声称阈值在每个训练日已知；仅2025和2026评估时点使用已知阈值。2026仍是已探索历史上的回顾评估，不称为新的未触及测试集。",
        "只评估三种时钟×两个模型结构（共六个事件预测方案）和一个既有 none/ridge 分钟基线共同可用的origin。既有分钟预测在该共同样本重新计算损失，不复用不一致样本的汇总数。每日期先平均origin损失再日等权；200次连续5交易日配对块bootstrap，逐比较描述性95%区间，未作多重比较校正。", "",
        "## 2026 共同样本结果", "",
        "| 模型 | 比较基准 | 交易日 | origins | QLIKE差 | 95%描述性区间 | MSE比 |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary_df[summary_df.year == 2026].itertuples():
        report_lines.append(f"| {row.model} | {row.baseline} | {row.days} | {row.origins} | {row.qlike_difference:.6f} | [{row.qlike_difference_ci_low:.6f}, {row.qlike_difference_ci_high:.6f}] | {row.mse_ratio:.4f} |")
    report_lines += ["", "## 正预测下限与失败机制", "",
                     "线性ridge没有强制未截断拟合值为正。沿用共享模型的历史目标均值×10^-6（最低10^-16）下限后，极低预测遇到正的真实方差会产生很大的QLIKE。以下保留全部这些损失，没有按2026结果删除、截尾或替换模型。触底诊断仅重新计算同一历史下限，不改变预测。", "",
                     "| 模型 | 2026触底origins | 比例 | 最大单origin QLIKE |", "|---|---:|---:|---:|"]
    for row in floor_df[floor_df.year == 2026].itertuples():
        report_lines.append(f"| {row.model} | {row.at_historical_floor_count} | {row.at_historical_floor_rate:.4%} | {row.max_origin_qlike:.2f} |")
    report_lines += ["", "因此巨大的QLIKE差首先是事件特征配合初始线性正值处理的失败证据，不能泛化为活动时钟本身一定无效；追加NNLS结果与失败结果均完整保留。*_nnls应优先与同结构time15_nnls比较，再与既有minute_none_ridge比较，后者同时改变了特征和约束。", "",
                     "正的QLIKE差表示比基准差。区间跨零不等于证明模型相等；样本外回顾优势也不等于交易收益。时钟生成更多事件本身不是更好预测的证据。", "",
                     "## 文件", "", "- summary.csv：初始time15、探索性time15_nnls、既有分钟none/ridge三个基准下的共同样本损失与区间。", "- daily_losses.csv：六个事件预测方案及一个分钟基线的共同origin逐日损失。",
                     "- clock_forecasts.parquet：预测、最后完整事件、事件区间与特征、因子和训练截止证据。", "- coverage.csv：事件历史充分性及共同origin覆盖。",
                     "- floor_diagnostics.csv：历史正值下限触底次数及损失放大诊断。",
                     "- clock_forecast_qlike_2026.png：同NNLS结构时钟互比及NNLS对分钟基线；linear_floor_rates_2026.png：单列初始失败与修正后触底率。",
                     "- run_manifest.json：输入指纹、固定设置和独立有界复算结果。", ""]
    (output_dir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    manifest = {"created_at": dt.datetime.now(dt.timezone.utc).isoformat(), "runtime_seconds": time.perf_counter() - started,
                "clock_specifications": clock_specs, "normalization_for_all_clocks": "boudt_wsd120 historical variance_factor",
                "target": "future same-session RV15", "candidate_origins_per_day": 182,
                "common_validation_origins": int(shared_origins[dates.year == 2025].sum()),
                "common_retrospective_origins": int(shared_origins[dates.year == 2026].sum()),
                "exported_prediction_rows": expected_rows, "bootstrap_replicates": 200, "bootstrap_block_days": 5,
                "bootstrap_seed": 20260907, "training_days": 252, "minimum_training_days": 126,
                "clock_thresholds_fit_end": "2024-12-31", "training_events_context": "retrospectively reconstructed after training threshold calibration",
                "nonnegative_ridge_context": "exploratory structural correction added after observing linear floor failure; no 2026 hyperparameter tuning",
                "nonnegative_ridge_features": "historical RMS only; no centering; nonnegative slopes; unpenalized intercept with historical numerical lower bound",
                "future_event_boundaries_predicted": False, "whole_event_garch_horizon_conversion": False,
                "checks": {"last_event_at_or_before_origin": True, "raw_rv15_matches_minute_baseline": True,
                           "all_models_same_origins": True, "positive_predictions": True,
                           "prior_day_fit_dates": True, "arrow_schema_roundtrip": True, "direct_sums": event_checks},
                "input_sha256": {str(path.relative_to(project_dir)): hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in [observation_path, factor_path, events_path, threshold_path, minute_forecast_path]}}
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[complete] clock forecasting: {expected_rows} rows; elapsed={manifest['runtime_seconds']:.1f}s", flush=True)


if __name__ == "__main__":
    main()
