"""Run controlled simulations for intraday variance-factor estimation.

This experiment reads no market data and writes only results/simulations.
The two-session BPV identities below are explicit project derivations.
"""

from __future__ import annotations

import argparse
import json
import platform
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from estimators import estimate_variance_factor


def generate_return_panel(
    rng: np.random.Generator,
    scenario: str,
    training_days: int,
    test_days: int,
    slots: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Generate observed returns, latent day scales and population factor targets."""
    slot_index = np.arange(slots)
    session_start = slots // 2
    base_variance_factor = (
        0.35
        + 1.20 * ((slot_index - 102) / 137) ** 2
        + 0.55 * np.exp(-slot_index / 7)
        + 0.42 * np.exp(-np.abs(slot_index - session_start) / 9)
    )
    base_variance_factor /= base_variance_factor.mean()
    days = training_days + test_days
    day_scale = np.exp(0.35 * rng.standard_normal(days) - 0.5 * 0.35**2)
    population_variance_factor = np.broadcast_to(
        base_variance_factor, (days, slots)
    ).copy()

    if scenario == "session_boundary":
        population_variance_factor[:, 0] *= 5
        population_variance_factor[:, session_start] *= 7
        population_variance_factor[:, session_start - 1] *= 0.3
        population_variance_factor[:, -3:] *= 3
        population_variance_factor /= population_variance_factor.mean(
            axis=1, keepdims=True
        )
    elif scenario == "state_change":
        late_session_loading = 0.25 + 2.1 * (slot_index / (slots - 1)) ** 4
        high_state_factor = base_variance_factor * late_session_loading
        high_state_factor /= high_state_factor.mean()
        high_state_probability = np.full(days, 0.15)
        high_state_probability[training_days - 60 : training_days] = 0.65
        high_state_probability[training_days:] = 1.0
        high_state = rng.random(days) < high_state_probability
        population_variance_factor[high_state] = high_state_factor

    efficient_target = population_variance_factor[training_days:].mean(axis=0)
    observed_target = efficient_target.copy()
    local_variance_factor = population_variance_factor.copy()
    if scenario == "stochastic_intraday":
        log_variance = rng.normal(0, 0.70, size=(days, slots))
        for slot in range(1, slots):
            log_variance[:, slot] = (
                0.96 * log_variance[:, slot - 1]
                + np.sqrt(1 - 0.96**2) * log_variance[:, slot]
            )
        local_variance_factor *= np.exp(log_variance - 0.5 * 0.70**2)

    efficient_returns = (
        day_scale[:, None]
        / np.sqrt(slots)
        * np.sqrt(local_variance_factor)
        * rng.standard_normal((days, slots))
    )
    observed_returns = efficient_returns.copy()
    if scenario == "periodic_jumps":
        jump_probability = np.full(slots, 0.001)
        jump_probability[:8] = 0.06
        jump_probability[-16:] = 0.08
        jump_occurrence = rng.random((days, slots)) < jump_probability
        jump_sign = rng.choice(np.array([-1.0, 1.0]), size=(days, slots))
        observed_returns += (
            jump_occurrence
            * jump_sign
            * 8
            * day_scale[:, None]
            / np.sqrt(slots)
            * np.sqrt(population_variance_factor)
        )
        observed_target = efficient_target * (1 + 64 * jump_probability)
        observed_target /= observed_target.mean()
    elif scenario == "microstructure_zeros":
        noise_ratio = 0.45
        price_noise = (
            noise_ratio
            * day_scale[:, None]
            / np.sqrt(slots)
            * rng.standard_normal((days, slots + 1))
        )
        observed_returns += np.diff(price_noise, axis=1)
        zero_probability = 0.03 + 0.30 * (
            1 - efficient_target / efficient_target.max()
        )
        observed_returns[
            rng.random((days, slots)) < zero_probability
        ] = 0
        observed_target = (efficient_target + 2 * noise_ratio**2) * (
            1 - zero_probability
        )
        observed_target /= observed_target.mean()
    return (
        observed_returns,
        efficient_returns,
        day_scale,
        efficient_target,
        observed_target,
    )


def check_bpv_identities(slots: int) -> pd.DataFrame:
    """Verify exact Gaussian expectations and session-boundary denominator algebra."""
    pair_slots = np.setdiff1d(np.arange(1, slots), [slots // 2])
    singleton_slots = np.array([0, slots // 2])
    variance_factor = np.ones(slots)
    variance_factor[slots // 2] = 20
    variance_factor[slots // 2 - 1] = 0.05
    variance_factor[-1] = 10
    variance_factor /= variance_factor.mean()
    pair_factor = np.sqrt(
        variance_factor[pair_slots] * variance_factor[pair_slots - 1]
    )
    valid_pairs = pair_slots.size
    periodic_bpv_constant = pair_factor.mean()
    expected_raw_hybrid = (
        variance_factor[singleton_slots].sum() + pair_factor.sum()
    ) / slots
    expected_direct_division = (
        singleton_slots.size
        + np.sum(pair_factor / variance_factor[pair_slots])
    ) / slots
    expected_pair_division = (singleton_slots.size + valid_pairs) / slots
    expected_scalar_corrected_bpv = (
        periodic_bpv_constant / periodic_bpv_constant
    )
    np.testing.assert_allclose(expected_pair_division, 1, atol=1e-14)
    np.testing.assert_allclose(expected_scalar_corrected_bpv, 1, atol=1e-14)
    assert abs(expected_direct_division - 1) > 0.01
    single_session_pair_factor = np.sqrt(
        variance_factor[1:] * variance_factor[:-1]
    )
    single_session_constant = single_session_pair_factor.sum() / (slots - 1)
    np.testing.assert_allclose(
        1 / single_session_constant,
        (slots - 1) / single_session_pair_factor.sum(),
        atol=1e-14,
    )
    flat_factor = np.ones(slots)
    np.testing.assert_allclose(
        np.sqrt(flat_factor[pair_slots] * flat_factor[pair_slots - 1]).mean(),
        1,
        atol=1e-14,
    )
    # Independently verify the expectations by simulated returns, and verify the
    # contribution transformation against recomputing it from filtered returns.
    identity_rng = np.random.default_rng(2026090701)
    sample_values = {
        "raw_hybrid_expectation": [],
        "direct_division_expectation": [],
        "pair_division_expectation": [],
        "scalar_corrected_bpv_expectation": [],
    }
    hybrid_denominator = variance_factor.copy()
    hybrid_denominator[pair_slots] = pair_factor
    for _ in range(20):
        gaussian_returns = identity_rng.standard_normal((5000, slots)) * np.sqrt(
            variance_factor / slots
        )
        raw_contribution = gaussian_returns**2
        raw_contribution[:, pair_slots] = (
            np.pi
            / 2
            * np.abs(
                gaussian_returns[:, pair_slots]
                * gaussian_returns[:, pair_slots - 1]
            )
        )
        filtered_returns = gaussian_returns / np.sqrt(variance_factor)
        recomputed_contribution = filtered_returns**2
        recomputed_contribution[:, pair_slots] = (
            np.pi
            / 2
            * np.abs(
                filtered_returns[:, pair_slots]
                * filtered_returns[:, pair_slots - 1]
            )
        )
        np.testing.assert_allclose(
            raw_contribution / hybrid_denominator,
            recomputed_contribution,
            rtol=1e-12,
            atol=1e-14,
        )
        sample_values["raw_hybrid_expectation"].append(raw_contribution.sum(axis=1))
        sample_values["direct_division_expectation"].append(
            (raw_contribution / variance_factor).sum(axis=1)
        )
        sample_values["pair_division_expectation"].append(
            (raw_contribution / hybrid_denominator).sum(axis=1)
        )
        sample_values["scalar_corrected_bpv_expectation"].append(
            slots
            / valid_pairs
            * raw_contribution[:, pair_slots].sum(axis=1)
            / periodic_bpv_constant
        )
    identity_rows = [
        {
            "quantity": "two_session_valid_pairs",
            "value": float(valid_pairs),
            "meaning": "Excludes each session's first return from adjacent pairs",
        },
        {
            "quantity": "bpv_periodic_constant",
            "value": periodic_bpv_constant,
            "meaning": "E[N/K * raw_BPV] / daily_variance",
        },
    ]
    for quantity, expectation in [
        ("raw_hybrid_expectation", expected_raw_hybrid),
        ("direct_division_expectation", expected_direct_division),
        ("pair_division_expectation", expected_pair_division),
        ("scalar_corrected_bpv_expectation", expected_scalar_corrected_bpv),
    ]:
        metric_values = np.concatenate(sample_values[quantity])
        sample_mean = metric_values.mean()
        sample_mean_se = metric_values.std(ddof=1) / np.sqrt(metric_values.size)
        standard_error_distance = (sample_mean - expectation) / sample_mean_se
        assert abs(standard_error_distance) <= 5
        identity_rows.append(
            {
                "quantity": quantity,
                "value": expectation,
                "simulated_mean": sample_mean,
                "mc_standard_error": sample_mean_se,
                "error_in_standard_errors": standard_error_distance,
                "simulated_days": metric_values.size,
                "meaning": "Independent Gaussian Monte Carlo verification",
            }
        )
    return pd.DataFrame(identity_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--replicates", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20260907)
    parser.add_argument("--test-days", type=int, default=64)
    args = parser.parse_args()
    if args.replicates < 2 or args.test_days < 2:
        parser.error("replicates and test-days must each be at least 2")
    started = time.perf_counter()
    output_dir = Path(__file__).resolve().parent / "results" / "simulations"
    output_dir.mkdir(parents=True, exist_ok=True)
    slots = 240
    windows = [30, 60, 120, 360]
    training_days = max(windows)
    methods = ["mean_rv", "median_rv", "boudt_wsd", "fff_wsd"]
    scenarios = [
        "gaussian_static",
        "periodic_jumps",
        "state_change",
        "stochastic_intraday",
        "microstructure_zeros",
        "session_boundary",
    ]
    pair_slots = np.setdiff1d(np.arange(1, slots), [slots // 2])
    singleton_slots = np.array([0, slots // 2])
    identities_df = check_bpv_identities(slots)
    identities_df.to_csv(output_dir / "algebra_checks.csv", index=False)
    rng = np.random.default_rng(args.seed)
    simulation_rows = []
    factor_example_rows = []
    error_rows = []

    for scenario in scenarios:
        scenario_started = time.perf_counter()
        for replicate in range(args.replicates):
            (
                observed_returns,
                efficient_returns,
                day_scale,
                efficient_target,
                observed_target,
            ) = generate_return_panel(
                rng, scenario, training_days, args.test_days, slots
            )
            oracle_standardized_returns = (
                observed_returns * np.sqrt(slots) / day_scale[:, None]
            )
            bpv_day_scale = np.sqrt(
                np.pi
                / 2
                * np.mean(
                    np.abs(
                        observed_returns[:, pair_slots]
                        * observed_returns[:, pair_slots - 1]
                    ),
                    axis=1,
                )
            )
            if np.any(~np.isfinite(bpv_day_scale) | (bpv_day_scale <= 0)):
                raise ValueError("Generated invalid daily BPV scale")
            bpv_standardized_returns = observed_returns / bpv_day_scale[:, None]
            test_returns = oracle_standardized_returns[training_days:]
            test_contribution = test_returns**2
            test_contribution[:, pair_slots] = (
                np.pi
                / 2
                * np.abs(
                    test_returns[:, pair_slots]
                    * test_returns[:, pair_slots - 1]
                )
            )
            for normalization, standardized_returns in [
                ("oracle_day_scale", oracle_standardized_returns),
                ("realized_bpv_day_scale", bpv_standardized_returns),
            ]:
                for window in windows:
                    training_returns = standardized_returns[
                        training_days - window : training_days
                    ]
                    for method in methods:
                        try:
                            estimated_factor = np.asarray(
                                estimate_variance_factor(
                                    training_returns,
                                    method,
                                    fourier_order=3,
                                ),
                                dtype=np.float64,
                            )
                            if (
                                estimated_factor.shape != (slots,)
                                or np.any(~np.isfinite(estimated_factor))
                                or np.any(estimated_factor <= 0)
                            ):
                                raise ValueError("Estimator returned an invalid factor")
                            np.testing.assert_allclose(
                                estimated_factor.mean(), 1, atol=1e-10
                            )
                        except Exception as error:
                            error_rows.append(
                                {
                                    "scenario": scenario,
                                    "replicate": replicate,
                                    "normalization": normalization,
                                    "window": window,
                                    "method": method,
                                    "error": f"{type(error).__name__}: {error}",
                                }
                            )
                            continue

                        pair_denominator = estimated_factor.copy()
                        pair_denominator[pair_slots] = np.sqrt(
                            estimated_factor[pair_slots]
                            * estimated_factor[pair_slots - 1]
                        )
                        expected_pair_factor = np.sqrt(
                            efficient_target[pair_slots]
                            * efficient_target[pair_slots - 1]
                        )
                        expected_hybrid_direct = (
                            np.sum(
                                efficient_target[singleton_slots]
                                / estimated_factor[singleton_slots]
                            )
                            + np.sum(
                                expected_pair_factor / estimated_factor[pair_slots]
                            )
                        ) / slots
                        expected_hybrid_pair = (
                            np.sum(
                                efficient_target[singleton_slots]
                                / estimated_factor[singleton_slots]
                            )
                            + np.sum(
                                expected_pair_factor / pair_denominator[pair_slots]
                            )
                        ) / slots
                        test_adjusted_second_moment = np.mean(
                            test_returns**2 / estimated_factor, axis=0
                        )
                        test_adjusted_second_moment /= (
                            test_adjusted_second_moment.mean()
                        )
                        simulation_rows.append(
                            {
                                "scenario": scenario,
                                "replicate": replicate,
                                "normalization": normalization,
                                "window": window,
                                "method": method,
                                "factor_rmse": np.sqrt(
                                    np.mean((estimated_factor - efficient_target) ** 2)
                                ),
                                "factor_relative_rmse": np.sqrt(
                                    np.mean(
                                        (estimated_factor / efficient_target - 1) ** 2
                                    )
                                ),
                                "observed_factor_rmse": np.sqrt(
                                    np.mean((estimated_factor - observed_target) ** 2)
                                ),
                                "test_second_moment_slot_rmse": np.sqrt(
                                    np.mean((test_adjusted_second_moment - 1) ** 2)
                                ),
                                "gaussian_plugin_hybrid_direct_bias":
                                    expected_hybrid_direct - 1,
                                "gaussian_plugin_hybrid_pair_bias":
                                    expected_hybrid_pair - 1,
                                "test_hybrid_direct_mean": np.mean(
                                    test_contribution / estimated_factor
                                ),
                                "test_hybrid_pair_mean": np.mean(
                                    test_contribution / pair_denominator
                                ),
                            }
                        )
                        if replicate == 0 and window in (30, 360):
                            for slot in range(slots):
                                factor_example_rows.append(
                                    {
                                        "scenario": scenario,
                                        "normalization": normalization,
                                        "window": window,
                                        "method": method,
                                        "slot": slot,
                                        "efficient_target": efficient_target[slot],
                                        "observed_target": observed_target[slot],
                                        "estimated_factor": estimated_factor[slot],
                                    }
                                )
        print(
            f"{scenario}: {args.replicates} replicates; "
            f"{time.perf_counter() - scenario_started:.1f}s; "
            f"total {time.perf_counter() - started:.1f}s; "
            f"estimator failures {len(error_rows)}",
            flush=True,
        )

    simulations_df = pd.DataFrame(simulation_rows)
    if simulations_df.empty:
        raise RuntimeError(f"All estimators failed: {error_rows[:2]}")
    metric_columns = [
        column
        for column in simulations_df.columns
        if column not in ["scenario", "replicate", "normalization", "window", "method"]
    ]
    summary_df = (
        simulations_df.groupby(
            ["scenario", "normalization", "window", "method"],
        )[metric_columns]
        .agg(["mean", "std", "count"])
        .reset_index()
    )
    summary_df.columns = [
        "_".join(str(part) for part in column if part)
        if isinstance(column, tuple)
        else column
        for column in summary_df.columns
    ]
    for metric in metric_columns:
        summary_df[f"{metric}_mc_se"] = (
            summary_df[f"{metric}_std"]
            / np.sqrt(summary_df[f"{metric}_count"])
        )
    simulations_df.to_csv(output_dir / "replicate_metrics.csv", index=False)
    summary_df.to_csv(output_dir / "summary.csv", index=False)
    pd.DataFrame(factor_example_rows).to_csv(
        output_dir / "factor_examples.csv", index=False
    )
    pd.DataFrame(
        error_rows,
        columns=["scenario", "replicate", "normalization", "window", "method", "error"],
    ).to_csv(output_dir / "estimator_failures.csv", index=False)

    for normalization in ["oracle_day_scale", "realized_bpv_day_scale"]:
        figure, axes = plt.subplots(2, 3, figsize=(15, 8), constrained_layout=True)
        for axis, scenario in zip(axes.flat, scenarios):
            for method in methods:
                chart_df = summary_df.loc[
                    (summary_df["scenario"] == scenario)
                    & (summary_df["normalization"] == normalization)
                    & (summary_df["method"] == method)
                ].sort_values("window")
                axis.errorbar(
                    chart_df["window"],
                    chart_df["factor_rmse_mean"],
                    yerr=1.96 * chart_df["factor_rmse_mc_se"],
                    marker="o",
                    label=method,
                    linewidth=1.4,
                    capsize=2,
                )
            axis.set_title(scenario.replace("_", " "))
            axis.set_xscale("log")
            axis.set_xticks(windows, [str(window) for window in windows])
            axis.set_xlabel("training days")
            axis.set_ylabel("RMSE to efficient variance factor")
            axis.grid(alpha=0.25)
        handles, labels = axes.flat[0].get_legend_handles_labels()
        figure.legend(handles, labels, loc="outside lower center", ncol=len(methods))
        figure.suptitle(f"{normalization}: Monte Carlo mean and 1.96 x MC SE")
        figure.savefig(output_dir / f"factor_rmse_{normalization}.png", dpi=150)
        plt.close(figure)

    metadata = {
        "seed": args.seed,
        "replicates": args.replicates,
        "test_days": args.test_days,
        "slots": slots,
        "sessions": [[0, 119], [120, 239]],
        "windows": windows,
        "methods": methods,
        "scenarios": scenarios,
        "normalizations": ["oracle_day_scale", "realized_bpv_day_scale"],
        "elapsed_seconds": time.perf_counter() - started,
        "metric_rows": len(simulations_df),
        "estimator_failures": len(error_rows),
        "python": platform.python_version(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
        "matplotlib": matplotlib.__version__,
        "scope": "Controlled synthetic experiments; no market data or lake writes.",
        "uncertainty": "Monte Carlo SE measures simulation error, not market confidence.",
    }
    (output_dir / "run_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    ranking_df = summary_df.loc[
        summary_df["normalization"] == "realized_bpv_day_scale",
        ["scenario", "window", "method", "factor_rmse_mean"],
    ]
    best_df = ranking_df.loc[
        ranking_df.groupby(["scenario", "window"])["factor_rmse_mean"].idxmin()
    ].sort_values(["scenario", "window"])
    best_lines = [
        "| 情景 | 窗口 | 本次最低平均RMSE方法 | RMSE |",
        "| --- | ---: | --- | ---: |",
    ]
    for row in best_df.itertuples(index=False):
        best_lines.append(
            f"| {row.scenario} | {row.window} | {row.method} "
            f"| {row.factor_rmse_mean:.4f} |"
        )
    report = f"""# 日内周期受控模拟实验

符号与字段映射见[统一符号约定](../../research/03_method_derivations.md)。本报告区分标准差因子与方差因子，表格沿用既有输出字段名。

运行：seed={args.seed}；每情景{args.replicates}次独立重复；每次360日训练池、{args.test_days}日测试；
240个slot、两段各120个slot；窗口30/60/120/360日。耗时{metadata["elapsed_seconds"]:.1f}秒。
共产生{len(simulations_df)}条方法评估，显式记录{len(error_rows)}次估计器失败。

## 模拟机制和评估对象

- gaussian_static：固定方差周期、独立正态创新、独立随机日尺度。
- periodic_jumps：开收盘附近增加独立稀疏跳跃；主要恢复目标仍是连续价格方差形状，另保存包含跳跃的总方差形状误差。
- state_change：训练早期高状态概率0.15，末60日0.65，测试全为高状态；高状态提高尾盘相对方差。固定历史估计器可能因状态分布变化而偏离测试目标。
- stochastic_intraday：周期之外叠加平稳AR(1)对数方差，系数0.96、标准差0.70；期望乘子为1，日内路径仍随机。
- microstructure_zeros：收益加入相邻独立价格噪声的差分，并随机置零；分别记录对潜在有效价格形状和观测方差形状的误差。该机制故意违反无噪声独立收益假设。
- session_boundary：放大两个session首格和末三格，压低午间闭市前一格，检查尖锐形状及邻接项。

oracle_day_scale使用模拟生成的真实日尺度，仅作可识别性的对照；
realized_bpv_day_scale使用每个已完成训练日的有效相邻收益BPV均方尺度。
测试数据从不进入训练。测试日真实尺度仅用于模拟评估，不能视为实盘可用量。

所有估计器由同一estimators.py实现；比较mean_rv、median_rv、boudt_wsd、fff_wsd。
主指标factor_rmse比较均值为1的估计方差曲线 `hat f_i²` 与测试日连续价格方差目标；
带帽记号表示估计值，下述已知曲线代数中的 `f_i` 不带帽。
表中最小值是本次模拟结果，不构成市场方法选择或显著性结论。
图误差条为1.96倍Monte Carlo均值标准误，不是中国期货市场的置信区间。

## 有限样本与边界代数检查

令 `N=240` 为全日格点数（对应主文档的 `M`），`I` 为排除两段首格的有效配对右端点集合，
`K=|I|=238`。标准差周期因子为 `f_i`，方差因子为 `f_i²`，且 `mean(f_i²)=1`。
在给定日方差 `sigma²`、独立零均值正态收益下，
未经边界修正的 `BV=(pi/2)*sum_I |r_i*r_(i-1)|`；本实验使用 `BV_bc=(N/K)*BV`。
`BV_bc` 的期望除以 `sigma²` 为 `C_pair=mean_I f_i*f_(i-1)`；
因此 `BV_bc/C_pair` 恢复日方差期望。`BV_bc` 是主文档明确标出的边界修正版，不能简称原始BV。
单段 `K=N-1` 时恢复Dette等有限样本常数；两段是本项目的显式扩展。

当前混合贡献在两段首格用 `r_i²`，其余用 `(pi/2)*|r_i*r_(i-1)|`。
逐格除 `f_i²` 后的混合日总量，其期望除以 `sigma²` 一般不等于1；
首格除 `f_i²`、配对格除 `f_i*f_(i-1)` 后，该日总量的相应期望比率才等于1。
这只比较同一给定收益周期因子下的代数；另行估计的 `median_mixed` 贡献形状记为 `hat g_mix`，
对应主文档的带帽混合贡献因子，其直接除数是 `hat g_mix`，本模拟未评估该基线。
algebra_checks.csv使用尖锐已知曲线独立检查平坦边界、单段公式、两段pair数及混合期望。
其中所有声称为1的代数项均通过绝对误差1e-14检查。另以独立种子生成100000个正态交易日，
核对四种度量的样本均值都在理论值的5倍Monte Carlo标准误内，并逐格核对
“原贡献除正确因子”与“先过滤收益再重算混合贡献”在1e-12相对容差内一致。

gaussian_plugin_hybrid_*仅是把估计曲线和连续价格目标代入独立正态公式所得指标；
在随机日内方差、跳跃或噪声情景，不能把它当作该情景的真实无条件期望。
test_hybrid_*则是实际测试样本矩；两类列刻意分开。

## 已完成日BPV尺度下的结果

{chr(10).join(best_lines)}

估计器失败未填补；均值是有效重复的条件均值，factor_rmse_count保留分母。
若退化次数不同，不能只比较幸存样本的最小RMSE。噪声和零收益也是估计器适用边界的一部分。

## 结果文件与边界

- summary.csv：每种配置的均值、跨重复标准差、重复数、Monte Carlo标准误。
- replicate_metrics.csv：每次重复的全部度量，可作配对比较。
- factor_examples.csv：固定第0次重复、30/360日窗口的曲线实例，非挑选最佳图。
- estimator_failures.csv：失败显式留存；空文件表头表示本次无失败。
- factor_rmse_*.png：六情景的窗口敏感性。
- run_metadata.json：随机种子、版本、用时和规模。

未实现或声称复现CHP/ATT的正式统计检验；没有收益预测回测，也没有交易收益结论。
有限样本常数参考Metrika论文式(11)–(12)，DOI:10.1007/s00184-022-00875-0。
具体版本差异及Boudt算法见literature/notes；fff_wsd是本项目候选平滑法，应按estimators.py定义解释。
"""
    (output_dir / "REPORT.md").write_text(report, encoding="utf-8")
    print(
        f"Finished: {len(simulations_df)} rows, {len(error_rows)} estimator failures, "
        f"{metadata['elapsed_seconds']:.1f}s. {output_dir}",
        flush=True,
    )


if __name__ == "__main__":
    main()
