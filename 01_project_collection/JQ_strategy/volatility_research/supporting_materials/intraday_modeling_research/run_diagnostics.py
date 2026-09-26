"""日内分布诊断与因果时钟对比；只消费本项目的两个研究 Parquet。

运行：latitude python run_diagnostics.py。输出只进入 results/diagnostics 与 results/clocks。
这里的日块 bootstrap 是描述性区间，不是 ATT/CHP 原文检验。
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import time
import warnings
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from data_contracts import MINUTE_OBSERVATION_SCHEMA, PERIODICITY_FACTOR_SCHEMA


TRAIN_END = dt.date(2024, 12, 31)
VALIDATION_END = dt.date(2025, 12, 31)
WINDOW_PAIRS = [("open_close", np.arange(1, 30), np.arange(210, 240)),
                ("prebreak_reopen", np.arange(90, 120), np.arange(121, 150))]


def moving_day_bootstrap_indices(day_count: int, replicate_count: int, seed: int,
                                 block_length: int = 5) -> np.ndarray:
    """抽取连续五交易日块，块内及同日内分钟顺序保持不变。"""
    if day_count < block_length:
        raise ValueError("日块 bootstrap 至少需要五个可用交易日")
    rng = np.random.default_rng(seed)
    block_count = int(np.ceil(day_count / block_length))
    starts = rng.integers(0, day_count - block_length + 1,
                          size=(replicate_count, block_count))
    return (starts[:, :, None] + np.arange(block_length)).reshape(replicate_count, -1)[:, :day_count]


def run_distribution_diagnostics(observation_df, dates, factor_arrays, split_by_day,
                                state_by_day, output_dir, bootstrap_count, seed):
    minute_count = 240
    return_matrix = observation_df["log_return"].to_numpy(dtype=float).reshape(-1, minute_count)
    rv_matrix = observation_df["rv_contribution"].to_numpy(dtype=float).reshape(-1, minute_count)
    bpv_matrix = observation_df["bpv_contribution"].to_numpy(dtype=float).reshape(-1, minute_count)
    mixed_matrix = observation_df["mixed_contribution"].to_numpy(dtype=float).reshape(-1, minute_count)
    profile_rows, summary_rows, coverage_rows, factor_rows, bootstrap_rows, state_profile_rows = [], [], [], [], [], []
    valid_factor_days = {key: np.isfinite(values[0]).all(axis=1) &
                         np.isfinite(values[1][:, np.r_[1:120, 121:240]]).all(axis=1)
                         for key, values in factor_arrays.items()}
    common_days = np.logical_and.reduce(list(valid_factor_days.values()))
    pd.DataFrame({"trading_date": dates, "split": split_by_day, "lagged_state": state_by_day,
                  "all_method_common_day": common_days}).to_csv(output_dir / "common_dates.csv", index=False)
    zero_rows = []
    for split_name in ["train", "validation_2025", "retrospective_2026"]:
        for state_name in ["all", "low", "middle", "high", "unavailable"]:
            day_mask = split_by_day == split_name
            if state_name != "all":
                day_mask &= state_by_day == state_name
            if not day_mask.any():
                continue
            returns = return_matrix[day_mask]
            for slot in range(minute_count):
                finite_returns = returns[:, slot][np.isfinite(returns[:, slot])]
                zero_rows.append({"split": split_name, "lagged_state": state_name,
                                  "minute_index": slot, "day_count": int(day_mask.sum()),
                                  "return_count": len(finite_returns),
                                  "zero_return_rate": float(np.mean(finite_returns == 0)) if len(finite_returns) else np.nan})
    pd.DataFrame(zero_rows).to_csv(output_dir / "zero_return_rate.csv", index=False)

    for factor_number, ((method, lookback), (variance_factor, contribution_factor)) in enumerate(factor_arrays.items()):
        print(f"[diagnostics] {factor_number + 1}/{len(factor_arrays)} {method}/{lookback}", flush=True)
        eligible_days = valid_factor_days[(method, lookback)]
        if not eligible_days.any():
            continue
        with np.errstate(invalid="ignore", divide="ignore"):
            normalized_return = return_matrix / np.sqrt(variance_factor)
            normalized_bpv = bpv_matrix / contribution_factor
            mixed_divisor = contribution_factor.copy()
            mixed_divisor[:, [0, 120]] = variance_factor[:, [0, 120]]
            normalized_mixed = mixed_matrix / mixed_divisor
            normalized_rv = rv_matrix / variance_factor
            factor_logs = np.log(variance_factor)
        within_session_differences = np.diff(factor_logs, axis=1)[:, np.r_[0:119, 120:239]]
        factor_drift = np.full(len(dates), np.nan)
        factor_drift[1:] = np.nanmean(np.abs(np.diff(factor_logs, axis=0)), axis=1)
        for day_number in np.flatnonzero(eligible_days):
            factor_rows.append({"trading_date": dates[day_number], "split": split_by_day[day_number],
                                "method": method, "lookback_days": lookback,
                                "log_factor_roughness": float(np.mean(np.abs(within_session_differences[day_number]))),
                                "log_factor_day_drift": factor_drift[day_number],
                                "factor_mean": float(np.mean(variance_factor[day_number])),
                                "factor_min": float(np.min(variance_factor[day_number])),
                                "factor_max": float(np.max(variance_factor[day_number]))})
        typed_matrices = {"log_return": normalized_return, "abs_return": np.abs(normalized_return),
                          "rv_contribution": normalized_rv, "bpv_contribution": normalized_bpv,
                          "mixed_contribution": normalized_mixed}
        for split_name in ["train", "validation_2025", "retrospective_2026"]:
            split_days = split_by_day == split_name
            coverage_rows.append({"method": method, "lookback_days": lookback, "split": split_name,
                                  "total_days": int(split_days.sum()),
                                  "factor_available_days": int((split_days & eligible_days).sum()),
                                  "all_method_common_days": int((split_days & common_days).sum())})
            for state_name in ["low", "middle", "high"]:
                state_days = split_days & common_days & (state_by_day == state_name)
                if state_days.sum() < 5:
                    continue
                state_bpv = normalized_bpv[state_days]
                state_quantiles = np.nanquantile(state_bpv, [0.25, 0.5, 0.75, 0.95], axis=0)
                state_mean = np.nanmean(state_bpv, axis=0)
                for slot in range(minute_count):
                    q25, median, q75, q95 = state_quantiles[:, slot]
                    state_profile_rows.append({"method": method, "lookback_days": lookback,
                                               "split": split_name, "lagged_state": state_name,
                                               "minute_index": slot, "day_count": int(state_days.sum()),
                                               "mean": state_mean[slot], "q25": q25, "median": median,
                                               "q75": q75, "q95": q95, "iqr": q75 - q25,
                                               "q95_median_ratio": q95 / median if median > 0 else np.nan})
            for sample_name, day_mask in [("available", split_days & eligible_days),
                                         ("all_method_common", split_days & common_days)]:
                if day_mask.sum() < 5:
                    continue
                for variable, value_matrix in typed_matrices.items():
                    sampled_values = value_matrix[day_mask]
                    quantiles = np.nanquantile(sampled_values, [0.05, 0.25, 0.5, 0.75, 0.95], axis=0)
                    means = np.nanmean(sampled_values, axis=0)
                    for slot in range(minute_count):
                        q05, q25, median, q75, q95 = quantiles[:, slot]
                        profile_rows.append({"method": method, "lookback_days": lookback,
                                             "split": split_name, "sample": sample_name, "variable": variable,
                                             "minute_index": slot, "session_number": 1 + slot // 120,
                                             "day_count": int(day_mask.sum()),
                                             "value_count": int(np.isfinite(sampled_values[:, slot]).sum()),
                                             "mean": means[slot], "q05": q05, "q25": q25, "median": median,
                                             "q75": q75, "q95": q95, "iqr": q75 - q25,
                                             "q95_median_ratio": q95 / median if median > 0 else np.nan})
                    active = np.r_[1:120, 121:240] if variable == "bpv_contribution" else np.arange(minute_count)
                    mean_profile, median_profile, upper_profile = means[active], quantiles[2, active], quantiles[4, active]
                    pooled_values = sampled_values[np.isfinite(sampled_values)]
                    pooled_quantiles = np.quantile(pooled_values, [0.25, 0.5, 0.75, 0.95])
                    summary_rows.append({"method": method, "lookback_days": lookback,
                                         "split": split_name, "sample": sample_name, "variable": variable,
                                         "day_count": int(day_mask.sum()), "value_count": len(pooled_values),
                                         "pooled_median": pooled_quantiles[1],
                                         "pooled_iqr": pooled_quantiles[2] - pooled_quantiles[0],
                                         "pooled_q95": pooled_quantiles[3],
                                         "mean_profile_cv": np.nanstd(mean_profile) / np.nanmean(mean_profile) if np.nanmean(mean_profile) > 0 else np.nan,
                                         "median_profile_cv": np.nanstd(median_profile) / np.nanmean(median_profile) if np.nanmean(median_profile) > 0 else np.nan,
                                         "q95_profile_cv": np.nanstd(upper_profile) / np.nanmean(upper_profile) if np.nanmean(upper_profile) > 0 else np.nan})

        # 预先固定时段；CDF 阈值仅用共同训练日期校准。按日配对后抽五日块。
        training_days = common_days & (split_by_day == "train")
        if training_days.sum() < 5:
            continue
        training_values = normalized_bpv[training_days]
        training_values = training_values[np.isfinite(training_values)]
        cdf_thresholds = np.quantile(training_values, [0.1, 0.25, 0.5, 0.75, 0.9])
        log_scale = float(np.median(training_values[training_values > 0]))
        for split_name in ["train", "validation_2025", "retrospective_2026"]:
            for state_name in ["all", "low", "middle", "high"]:
                sampled_days = common_days & (split_by_day == split_name)
                conditional_weights = np.ones(int(sampled_days.sum()), dtype=float)
                if state_name != "all":
                    conditional_weights = (state_by_day[sampled_days] == state_name).astype(float)
                sampled_day_count = int(conditional_weights.sum())
                if sampled_day_count < 10:
                    continue
                selected_bpv = normalized_bpv[sampled_days]
                # 先对原始日期时间线抽连续块，再按状态取条件均值；不把不相邻的状态日拼接。
                bootstrap_indices = moving_day_bootstrap_indices(int(sampled_days.sum()), bootstrap_count, seed)
                for pair_name, left_slots, right_slots in WINDOW_PAIRS:
                    left_bpv, right_bpv = selected_bpv[:, left_slots], selected_bpv[:, right_slots]
                    left_cdf = np.mean(left_bpv[:, :, None] <= cdf_thresholds, axis=1)
                    right_cdf = np.mean(right_bpv[:, :, None] <= cdf_thresholds, axis=1)
                    cdf_differences = left_cdf - right_cdf
                    log_differences = np.mean(np.log1p(left_bpv / log_scale), axis=1) - np.mean(np.log1p(right_bpv / log_scale), axis=1)
                    daily_statistics = np.column_stack([cdf_differences, log_differences])
                    bootstrap_weights = conditional_weights[bootstrap_indices]
                    bootstrap_denominators = bootstrap_weights.sum(axis=1)
                    weighted_statistics = daily_statistics[bootstrap_indices] * bootstrap_weights[:, :, None]
                    bootstrapped_means = weighted_statistics.sum(axis=1)[bootstrap_denominators > 0] / bootstrap_denominators[bootstrap_denominators > 0, None]
                    observed_means = np.sum(daily_statistics * conditional_weights[:, None], axis=0) / sampled_day_count
                    for metric_index, metric_name in enumerate(["cdf_q10", "cdf_q25", "cdf_q50", "cdf_q75", "cdf_q90", "mean_log1p"]):
                        ci_low, ci_high = np.quantile(bootstrapped_means[:, metric_index], [0.025, 0.975])
                        bootstrap_rows.append({"method": method, "lookback_days": lookback,
                                               "split": split_name, "lagged_state": state_name,
                                               "window_pair": pair_name, "metric": metric_name,
                                               "day_count": sampled_day_count, "estimate": observed_means[metric_index],
                                               "ci_low": ci_low, "ci_high": ci_high,
                                               "training_threshold": cdf_thresholds[metric_index] if metric_index < 5 else log_scale,
                                               "bootstrap_replicates": bootstrap_count, "block_days": 5,
                                               "seed": seed, "interval_kind": "descriptive_pointwise_percentile"})

    profile_df = pd.DataFrame(profile_rows)
    summary_df = pd.DataFrame(summary_rows)
    factor_df = pd.DataFrame(factor_rows)
    bootstrap_df = pd.DataFrame(bootstrap_rows)
    coverage_df = pd.DataFrame(coverage_rows)
    state_profile_df = pd.DataFrame(state_profile_rows)
    for output_df in [profile_df, summary_df, factor_df, bootstrap_df, coverage_df, state_profile_df]:
        fixed_method = output_df.method == "fixed_train_median_rv"
        output_df["fit_context"] = np.where(fixed_method & (output_df.split == "train"),
                                            "in_sample_training_end_curve",
                                            np.where(fixed_method, "frozen_after_training",
                                                     np.where(output_df.method == "none", "raw", "strictly_prior_rolling")))
        output_df["causal_usage_allowed"] = ~(fixed_method & (output_df.split == "train"))
    profile_df.to_csv(output_dir / "minute_distribution_profiles.csv", index=False)
    summary_df.to_csv(output_dir / "distribution_summary.csv", index=False)
    factor_df.to_csv(output_dir / "factor_roughness_drift.csv", index=False)
    bootstrap_df.to_csv(output_dir / "day_block_bootstrap.csv", index=False)
    coverage_df.to_csv(output_dir / "coverage.csv", index=False)
    state_profile_df.to_csv(output_dir / "lagged_state_bpv_profiles.csv", index=False)
    shortlist = [("none", 0), ("mean_rv", 120), ("boudt_wsd", 120), ("fff_wsd", 120), ("state_wsd", 360), ("fixed_train_median_rv", 0)]
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), constrained_layout=True)
    for axis, variable, metric in [(axes[0, 0], "bpv_contribution", "median"),
                                  (axes[0, 1], "bpv_contribution", "q95"),
                                  (axes[1, 0], "rv_contribution", "mean"),
                                  (axes[1, 1], "abs_return", "q95_median_ratio")]:
        for method, lookback in shortlist:
            selected = profile_df[(profile_df.method == method) & (profile_df.lookback_days == lookback) &
                                  (profile_df.split == "retrospective_2026") &
                                  (profile_df["sample"] == "all_method_common") & (profile_df.variable == variable)]
            if len(selected):
                values = selected[metric].to_numpy()
                if metric != "q95_median_ratio":
                    values = values / np.nanmean(values)
                axis.plot(selected.minute_index, values, label=f"{method}/{lookback}", linewidth=1)
        axis.axvline(119.5, color="grey", linestyle="--", linewidth=0.8)
        axis.set(title=f"2026 common dates: {variable} {metric}", xlabel="Trading minute index", ylabel="Relative profile" if metric != "q95_median_ratio" else "Q95 / median")
        axis.grid(alpha=0.2)
    axes[0, 0].legend(fontsize=7)
    fig.savefig(output_dir / "distribution_profiles_2026.png", dpi=150)
    plt.close(fig)
    factor_plot = factor_df[factor_df.split == "retrospective_2026"].groupby(["method", "lookback_days"])[["log_factor_roughness", "log_factor_day_drift"]].mean().reset_index().sort_values("log_factor_roughness")
    factor_labels = factor_plot.method + "/" + factor_plot.lookback_days.astype(str)
    fig, axes = plt.subplots(1, 2, figsize=(14, 9), constrained_layout=True, sharey=True)
    axes[0].barh(factor_labels, factor_plot.log_factor_roughness)
    axes[1].barh(factor_labels, factor_plot.log_factor_day_drift)
    axes[0].set(xlabel="Mean absolute adjacent log-factor change", title="2026: within-session factor roughness")
    axes[1].set(xlabel="Mean absolute log-factor day-to-day change", title="2026: temporal factor drift")
    for axis in axes:
        axis.tick_params(axis="y", labelsize=8)
        axis.grid(axis="x", alpha=0.2)
    fig.savefig(output_dir / "factor_roughness_drift.png", dpi=150)
    plt.close(fig)
    fig, axes = plt.subplots(2, 2, figsize=(13, 8), constrained_layout=True)
    for axis, (method, lookback) in zip(axes.flat, [("none", 0), ("boudt_wsd", 120), ("fff_wsd", 120), ("state_wsd", 360)]):
        for state_name in ["low", "middle", "high"]:
            selected = state_profile_df[(state_profile_df.method == method) & (state_profile_df.lookback_days == lookback) &
                                        (state_profile_df.split == "retrospective_2026") & (state_profile_df.lagged_state == state_name)]
            if len(selected):
                axis.plot(selected.minute_index, selected["mean"] / selected["mean"].mean(), label=f"{state_name} (n={int(selected.day_count.iloc[0])})", linewidth=1)
        axis.set(title=f"{method}/{lookback}: relative mean BPV", xlabel="Trading minute index", ylabel="State profile / own mean")
        axis.axvline(119.5, color="grey", linestyle="--", linewidth=0.8)
        axis.legend(fontsize=8)
        axis.grid(alpha=0.2)
    fig.savefig(output_dir / "lagged_state_profiles_2026.png", dpi=150)
    plt.close(fig)
    return summary_df, bootstrap_df, common_days


def run_clock_comparison(observation_df, dates, factor_arrays, split_by_day, common_days,
                         output_dir, bootstrap_count, seed):
    rv_matrix = observation_df.rv_contribution.to_numpy(dtype=float).reshape(-1, 240)
    bpv_matrix = observation_df.bpv_contribution.to_numpy(dtype=float).reshape(-1, 240)
    timestamps = observation_df.bar_at.to_numpy().reshape(-1, 240)
    clock_specs = [("time", "none", 0, np.ones_like(rv_matrix))]
    for method, lookback in [("none", 0), ("boudt_wsd", 120), ("fff_wsd", 120), ("state_wsd", 360)]:
        if (method, lookback) not in factor_arrays:
            continue
        variance_factor, contribution_factor = factor_arrays[(method, lookback)]
        if method != "none":
            clock_specs.append(("ex_ante_periodic_variance", method, lookback, variance_factor))
        clock_specs.append(("realized_deseasonal_bpv", method, lookback, bpv_matrix / contribution_factor))
    training_days = common_days & (split_by_day == "train")
    if training_days.sum() < 10:
        raise ValueError("共同训练样本不足，不能校准时钟阈值")
    event_rows, session_rows, threshold_rows = [], [], []
    for clock_name, method, lookback, exposure_matrix in clock_specs:
        threshold = 15.0 if clock_name == "time" else float(np.nanmean(np.nansum(exposure_matrix[training_days], axis=1)) / 16.0)
        if not np.isfinite(threshold) or threshold <= 0:
            raise ValueError(f"Invalid threshold: {clock_name}/{method}/{lookback}")
        clock_id = f"{clock_name}:{method}:{lookback}"
        threshold_rows.append({"clock_id": clock_id, "clock": clock_name, "method": method,
                               "lookback_days": lookback, "threshold": threshold,
                               "training_day_count": int(training_days.sum()), "fit_end_date": TRAIN_END,
                               "calibration": "15 trading minutes" if clock_name == "time" else "mean training daily exposure / 16"})
        print(f"[clocks] {clock_id} threshold={threshold:.8g}", flush=True)
        for day_number in np.flatnonzero(common_days):
            for session_number, (start_slot, end_slot) in enumerate([(0, 120), (120, 240)], start=1):
                cumulative_exposure, prior_threshold_count, exposure_since_event = 0.0, 0, 0.0
                previous_event_slot, event_count = start_slot - 1, 0
                for slot in range(start_slot, end_slot):
                    minute_exposure = exposure_matrix[day_number, slot]
                    if not np.isfinite(minute_exposure):
                        if clock_name == "realized_deseasonal_bpv" and slot == start_slot:
                            minute_exposure = 0.0
                        else:
                            raise ValueError(f"Unexpected missing clock exposure {clock_id} {dates[day_number]} {slot}")
                    if minute_exposure < 0:
                        raise ValueError("Clock exposure cannot be negative")
                    cumulative_exposure += minute_exposure
                    exposure_since_event += minute_exposure
                    reached_threshold_count = int(np.floor(cumulative_exposure / threshold + 1e-12))
                    thresholds_crossed = reached_threshold_count - prior_threshold_count
                    if thresholds_crossed <= 0:
                        continue
                    event_count += 1
                    # 多阈值在同一分钟合并为一个时点；余量保留在累计值中，绝不插值。
                    overshoot = max(0.0, cumulative_exposure - (prior_threshold_count + 1) * threshold)
                    future_values = rv_matrix[day_number, slot + 1:slot + 16]
                    future_available = slot + 15 < end_slot and len(future_values) == 15 and np.isfinite(future_values).all()
                    event_rows.append({"clock_id": clock_id, "clock": clock_name, "method": method,
                                       "lookback_days": lookback, "trading_date": dates[day_number],
                                       "split": split_by_day[day_number], "session_number": session_number,
                                       "minute_index": slot, "event_at": timestamps[day_number, slot],
                                       "duration_minutes": slot - previous_event_slot,
                                       "thresholds_crossed": thresholds_crossed,
                                       "exposure_since_previous_event": exposure_since_event,
                                       "overshoot": overshoot, "overshoot_threshold_ratio": overshoot / threshold,
                                       "carry_remainder": cumulative_exposure - reached_threshold_count * threshold,
                                       "future15m_available": future_available,
                                       "future15m_rv": float(np.sum(future_values)) if future_available else np.nan})
                    previous_event_slot, prior_threshold_count, exposure_since_event = slot, reached_threshold_count, 0.0
                session_rows.append({"clock_id": clock_id, "clock": clock_name, "method": method,
                                     "lookback_days": lookback, "trading_date": dates[day_number],
                                     "split": split_by_day[day_number], "session_number": session_number,
                                     "event_count": event_count, "threshold_crossing_count": prior_threshold_count,
                                     "total_exposure": cumulative_exposure,
                                     "end_session_remainder": cumulative_exposure - prior_threshold_count * threshold,
                                     "tail_minutes_without_event": end_slot - 1 - previous_event_slot})
    event_df, session_df = pd.DataFrame(event_rows), pd.DataFrame(session_rows)
    if (event_df.duration_minutes <= 0).any() or event_df.duplicated(["clock_id", "trading_date", "minute_index"]).any():
        raise AssertionError("Clock generated a repeated or non-positive-duration event")
    event_df.to_csv(output_dir / "events.csv", index=False)
    session_df.to_csv(output_dir / "session_summary.csv", index=False)
    pd.DataFrame(threshold_rows).to_csv(output_dir / "training_thresholds.csv", index=False)
    summary_rows = []
    for (clock_id, split_name), sampled_sessions in session_df.groupby(["clock_id", "split"], sort=False):
        sampled_events = event_df[(event_df.clock_id == clock_id) & (event_df.split == split_name)]
        daily_counts = sampled_sessions.groupby("trading_date").event_count.sum().to_numpy()
        bootstrap_indices = moving_day_bootstrap_indices(len(daily_counts), bootstrap_count, seed)
        bootstrapped_counts = np.mean(daily_counts[bootstrap_indices], axis=1)
        ci_low, ci_high = np.quantile(bootstrapped_counts, [0.025, 0.975])
        row = sampled_sessions.iloc[0]
        summary_rows.append({"clock_id": clock_id, "clock": row.clock, "method": row.method,
                             "lookback_days": int(row.lookback_days), "split": split_name,
                             "day_count": len(daily_counts), "event_count": len(sampled_events),
                             "daily_event_count_mean": np.mean(daily_counts),
                             "daily_event_count_sd": np.std(daily_counts),
                             "daily_event_count_ci_low": ci_low, "daily_event_count_ci_high": ci_high,
                             "duration_q25": sampled_events.duration_minutes.quantile(0.25),
                             "duration_median": sampled_events.duration_minutes.median(),
                             "duration_q95": sampled_events.duration_minutes.quantile(0.95),
                             "overshoot_ratio_mean": sampled_events.overshoot_threshold_ratio.mean(),
                             "overshoot_ratio_q95": sampled_events.overshoot_threshold_ratio.quantile(0.95),
                             "multi_threshold_event_rate": (sampled_events.thresholds_crossed > 1).mean(),
                             "future15m_available_rate": sampled_events.future15m_available.mean(),
                             "mean_session_tail_minutes": sampled_sessions.tail_minutes_without_event.mean()})
    clock_summary_df = pd.DataFrame(summary_rows)
    clock_summary_df.to_csv(output_dir / "clock_summary.csv", index=False)
    selected_clocks = clock_summary_df[clock_summary_df.split == "retrospective_2026"]
    labels = selected_clocks.clock_id.str.replace("ex_ante_periodic_variance", "periodic", regex=False).str.replace("realized_deseasonal_bpv", "realized BPV", regex=False)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), constrained_layout=True)
    axes[0].barh(labels, selected_clocks.daily_event_count_mean)
    axes[1].barh(labels, selected_clocks.duration_median)
    axes[0].set(xlabel="Mean events per day", title="2026: event count at training-fixed thresholds")
    axes[1].set(xlabel="Median trading-minute duration", title="2026: no interpolation, session resets")
    for axis in axes:
        axis.tick_params(axis="y", labelsize=7)
        axis.grid(axis="x", alpha=0.2)
    fig.savefig(output_dir / "clock_comparison_2026.png", dpi=150)
    plt.close(fig)
    return clock_summary_df


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-replicates", type=int, default=200)
    parser.add_argument("--seed", type=int, default=20260907)
    args = parser.parse_args()
    if not 100 <= args.bootstrap_replicates <= 1000:
        raise ValueError("Bootstrap replicate count must be between 100 and 1000")
    started = time.perf_counter()
    project_dir = Path(__file__).resolve().parent
    diagnostics_dir, clocks_dir = project_dir / "results" / "diagnostics", project_dir / "results" / "clocks"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    clocks_dir.mkdir(parents=True, exist_ok=True)
    observation_path = project_dir / "data" / "minute_observations.parquet"
    factor_path = project_dir / "results" / "periodicity_factors.parquet"
    observation_table = ds.dataset(observation_path, format="parquet").to_table()
    factor_table = ds.dataset(factor_path, format="parquet").to_table()
    for loaded_table, expected_schema in [(observation_table, MINUTE_OBSERVATION_SCHEMA), (factor_table, PERIODICITY_FACTOR_SCHEMA)]:
        for field in expected_schema:
            if loaded_table.schema.field(field.name).type != field.type:
                raise TypeError(f"Input physical type mismatch: {field.name}")
    observation_df = observation_table.cast(MINUTE_OBSERVATION_SCHEMA).to_pandas().sort_values(["trading_date", "minute_index"]).reset_index(drop=True)
    factor_df = factor_table.cast(PERIODICITY_FACTOR_SCHEMA).to_pandas()
    dates = np.array(sorted(observation_df.trading_date.unique()))
    if len(observation_df) != len(dates) * 240 or not np.array_equal(observation_df.minute_index.to_numpy(), np.tile(np.arange(240), len(dates))):
        raise ValueError("This experiment requires exactly 240 ordered slots per retained trading day")
    fit_dates = factor_df.factor_fit_end_date
    if ((fit_dates.notna()) & (fit_dates >= factor_df.trading_date)).any():
        raise ValueError("Factor fit end date must be strictly earlier than target trading day")
    split_by_day = np.where(dates <= TRAIN_END, "train", np.where(dates <= VALIDATION_END, "validation_2025", "retrospective_2026"))
    rv_matrix = observation_df.rv_contribution.to_numpy(dtype=float).reshape(-1, 240)
    daily_rv = np.nansum(rv_matrix, axis=1)
    lagged_daily_rv = np.r_[np.nan, daily_rv[:-1]]
    training_state_values = lagged_daily_rv[(split_by_day == "train") & np.isfinite(lagged_daily_rv)]
    state_cutoffs = np.quantile(training_state_values, [1/3, 2/3])
    state_by_day = np.where(~np.isfinite(lagged_daily_rv), "unavailable", np.where(lagged_daily_rv <= state_cutoffs[0], "low", np.where(lagged_daily_rv <= state_cutoffs[1], "middle", "high")))
    pd.DataFrame({"trading_date": dates, "split": split_by_day, "lagged_daily_rv": lagged_daily_rv,
                  "lagged_state": state_by_day}).to_csv(diagnostics_dir / "lagged_daily_states.csv", index=False)
    factor_arrays = {}
    for (method, lookback), factor_group in factor_df.groupby(["method", "lookback_days"], sort=True):
        variance_factor = factor_group.pivot(index="trading_date", columns="minute_index", values="variance_factor").reindex(index=dates, columns=np.arange(240)).to_numpy(dtype=float)
        contribution_factor = factor_group.pivot(index="trading_date", columns="minute_index", values="contribution_factor").reindex(index=dates, columns=np.arange(240)).to_numpy(dtype=float)
        if (variance_factor[np.isfinite(variance_factor)] <= 0).any() or (contribution_factor[np.isfinite(contribution_factor)] <= 0).any():
            raise ValueError("Finite factors must be strictly positive")
        factor_arrays[(str(method), int(lookback))] = (variance_factor, contribution_factor)
    factor_arrays.setdefault(("none", 0), (np.ones_like(rv_matrix), np.ones_like(rv_matrix)))
    # 一个训练段固定基准：训练期为回顾拟合，验证和2026使用冻结曲线。
    fixed_variance = np.nanmedian(rv_matrix[split_by_day == "train"], axis=0)
    positive_fixed = fixed_variance[np.isfinite(fixed_variance) & (fixed_variance > 0)]
    fixed_variance = np.where(np.isfinite(fixed_variance) & (fixed_variance > 0), fixed_variance, np.median(positive_fixed))
    fixed_variance /= fixed_variance.mean()
    fixed_contribution = np.sqrt(fixed_variance * np.r_[fixed_variance[0], fixed_variance[:-1]])
    fixed_contribution[[0, 120]] = fixed_variance[[0, 120]]
    factor_arrays[("fixed_train_median_rv", 0)] = (np.tile(fixed_variance, (len(dates), 1)), np.tile(fixed_contribution, (len(dates), 1)))
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*All-NaN slice encountered.*", category=RuntimeWarning)
        warnings.filterwarnings("ignore", message=".*Mean of empty slice.*", category=RuntimeWarning)
        summary_df, bootstrap_df, common_days = run_distribution_diagnostics(observation_df, dates, factor_arrays,
                                                                           split_by_day, state_by_day,
                                                                           diagnostics_dir, args.bootstrap_replicates, args.seed)
    clock_summary_df = run_clock_comparison(observation_df, dates, factor_arrays, split_by_day, common_days,
                                            clocks_dir, args.bootstrap_replicates, args.seed)
    selected_summary = summary_df[(summary_df.split == "retrospective_2026") & (summary_df["sample"] == "all_method_common") & (summary_df.variable == "bpv_contribution")].sort_values("median_profile_cv")
    ranking_lines = [f"| {row.method} | {int(row.lookback_days)} | {row.median_profile_cv:.4f} | {row.q95_profile_cv:.4f} | {row.mean_profile_cv:.4f} |" for _, row in selected_summary.iterrows()]
    selected_bootstrap = bootstrap_df[(bootstrap_df.split == "retrospective_2026") & (bootstrap_df.lagged_state == "all") & (bootstrap_df.window_pair == "open_close") & (bootstrap_df.metric == "cdf_q50")]
    bootstrap_lines = [f"| {row.method}/{int(row.lookback_days)} | {row.estimate:.4f} | [{row.ci_low:.4f}, {row.ci_high:.4f}] |" for _, row in selected_bootstrap.iterrows()]
    report = "\n".join([
        "# 日内分布与时钟实验结果", "", f"输入为 {len(dates)} 个交易日、{len(observation_df):,} 个分钟格点；日期 {dates[0]} 至 {dates[-1]}。",
        "训练段截至 2024-12-31；2025 为验证段；2026 为回顾评估，不称为未触及测试集。", "",
        "## 口径与覆盖", "",
        f"共比较 {len(factor_arrays)} 个设置（含原始和训练段冻结基准）。跨设置共同日期共 {int(common_days.sum())} 日。逐设置原生可用样本和共同样本均独立输出。",
        "log_return/abs_return 用方差因子的平方根去尺度，RV 用方差因子，BPV 用相邻周期尺度，mixed 的每个 session 首分钟用方差因子。median_mixed 是历史归一化对照，其因子不具有真实瞬时方差曲线解释。",
        "fixed_train_median_rv 在 2024-12-31 训练结束后才可用：训练段展示标记 fit_context=in_sample_training_end_curve、causal_usage_allowed=false，不能作为训练期逐日因果预测输入；2025/2026 冻结不更新并标为 frozen_after_training。状态分组用上一交易日 RV，三分位阈值只来自训练段。",
        "分位数/零值保留，不给零收益添加扰动。BPV 每个 session 首分钟缺失；不把午休当连续收益。quantile ratio 的分母为零时记缺失。", "",
        "## 2026 共同日期的 BPV 日内曲线", "",
        "CV 越低只表示对应边际摘要越平，不代表收益可预测或全分布相同；以下排序是描述性回顾，不能替代验证段模型选择。", "",
        "| 方法 | 窗口日 | 中位曲线 CV | Q95 曲线 CV | 均值曲线 CV |", "|---|---:|---:|---:|---:|", *ranking_lines, "",
        "## 预先指定的时段差异与日块 bootstrap", "",
        f"比较 open_close（分钟 1–29 减 210–239）与 prebreak_reopen（90–119 减 121–149）。对每日期的时段经验 CDF 差异和 log1p 均值差做连续五日块重采样，{args.bootstrap_replicates} 次，seed={args.seed}。CDF 阈值/正值尺度只用共同训练日期确定。",
        "区间是逐指标描述性的 95% percentile 区间，未作多重比较校正；不输出伪装成 ATT/CHP 的 p 值。条件状态结果先在原始共同日期时间线上抽连续五日块，再按状态取加权条件均值，不把不相邻的状态日期拼成连续交易日。", "",
        "下表为 2026 开盘减收盘的训练中位数阈值 CDF 差异；正值表示开盘落在低值区间的频率更高。", "",
        "| 方法/窗口 | CDF 差异 | 描述性 95% 区间 |", "|---|---:|---:|", *bootstrap_lines, "",
        "均值或中位曲线变平而 Q95/CDF 差异仍大，支持继续研究分布形状；差异区间覆盖零不证明所有时变周期不存在。同边际分布也不意味着独立同分布。", "",
        "## 时钟构造", "",
        "time 每 15 个交易分钟触达。ex_ante_periodic_variance 累加事前已拟合的 a[k]，是预期相对方差暴露钟，不包含当日最终总方差或未来日波动水平。realized_deseasonal_bpv 累加当前已完成分钟的 BPV/相邻周期尺度，是事后逐分钟可观测活动钟，不能当作事前时间表。",
        "活动阈值为共同训练段日总暴露均值/16，随后冻结；这仅校准训练期平均活动量，不用目标日最终总量决定事件数。训练段时钟统计本身包含阈值校准，结论以之后两段为主。",
        "在分钟末首次观察到跨越时产生事件，不插值。累计余量保留；一分钟跨多个阈值时合并为一个分钟末事件，并记录 thresholds_crossed，因此不产生多个零时长事件。overshoot 是相对本分钟跨越的第一个阈值的超额，可大于一个阈值。",
        "每个 session 独立重置，末端不足阈值不强制造事件；尾段长度和余量另存。future15m_rv 仅使用事件之后同 session 的完整 15 分钟，跨午休/收盘记 unavailable。该标签只供后续评估，不参与事件生成。", "",
        "详见 ../clocks/clock_summary.csv、events.csv、session_summary.csv 和 training_thresholds.csv。", "",
        "## 产出与限制", "",
        "minute_distribution_profiles.csv：五类变量分时中位数/IQR/Q95及分位比；lagged_state_bpv_profiles.csv：按日初滞后状态分组的BPV均值及分位曲线；distribution_summary.csv：池化及曲线摘要；coverage.csv/common_dates.csv：公平比较日期；zero_return_rate.csv：逐分钟零收益；factor_roughness_drift.csv：相邻分钟粗糙度与跨日漂移；day_block_bootstrap.csv：两组时段、各状态、逐阈值差异。",
        "这里没有估计原文全部噪声/跳跃结构，也没有声称复现 ATT/CHP 正式检验。文献结论、描述性诊断、研究假说分开；因子稳定性不等于预测收益，预测实验由独立入口负责。", ""])
    (diagnostics_dir / "report.md").write_text(report, encoding="utf-8")
    clock_lines = ["# 时钟对比结果", "", "完整定义和泄漏边界见 ../diagnostics/report.md；所有阈值只在训练期标定。", "",
                   "| 时钟/方法 | 评估段 | 日事件均值 | 时长中位数 | Q95时长 | 多阈值事件率 | future15m可用率 |", "|---|---|---:|---:|---:|---:|---:|"]
    for _, row in clock_summary_df.iterrows():
        clock_lines.append(f"| {row.clock_id} | {row.split} | {row.daily_event_count_mean:.2f} | {row.duration_median:.1f} | {row.duration_q95:.1f} | {row.multi_threshold_event_rate:.2%} | {row.future15m_available_rate:.2%} |")
    clock_lines += ["", "事件数区间与超额、尾段统计见 clock_summary.csv；这组实验测量时钟差异，不把更多事件或更短时长直接解释成交易优势。", ""]
    (clocks_dir / "report.md").write_text("\n".join(clock_lines), encoding="utf-8")
    manifest = {"created_at": dt.datetime.now(dt.timezone.utc).isoformat(), "duration_seconds": time.perf_counter() - started,
                "input_sha256": {str(p.relative_to(project_dir)): hashlib.sha256(p.read_bytes()).hexdigest() for p in [observation_path, factor_path]},
                "trading_days": len(dates), "common_days": int(common_days.sum()), "factor_settings": len(factor_arrays),
                "train_end": str(TRAIN_END), "validation_end": str(VALIDATION_END), "state_rv_cutoffs": state_cutoffs.tolist(),
                "bootstrap_replicates": args.bootstrap_replicates, "seed": args.seed,
                "formal_att_chp_replication": False, "clock_interpolation": False,
                "clock_remainder_carried": True, "same_minute_threshold_crossings_merged": True}
    (diagnostics_dir / "run_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[complete] diagnostics/clocks completed in {manifest['duration_seconds']:.1f}s", flush=True)


if __name__ == "__main__":
    main()
