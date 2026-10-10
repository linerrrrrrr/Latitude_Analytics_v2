"""Compare daily realized measures at one and five minutes without a true-IV label.

Read-only inputs: data/minute_observations.parquet and results/periodicity_factors.parquet.
Outputs are restricted to results/measures. No estimator is selected by this comparison.
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
import pyarrow.parquet as pq


def main() -> None:
    started = time.perf_counter()
    project_dir = Path(__file__).resolve().parent
    output_dir = project_dir / "results" / "measures"
    output_dir.mkdir(parents=True, exist_ok=True)
    observation_path = project_dir / "data" / "minute_observations.parquet"
    factor_path = project_dir / "results" / "periodicity_factors.parquet"
    observation_df = pq.read_table(
        observation_path,
        columns=["trading_date", "minute_index", "log_return"],
    ).to_pandas()
    return_df = observation_df.pivot(
        index="trading_date", columns="minute_index", values="log_return"
    ).sort_index()
    if not np.array_equal(return_df.columns.to_numpy(), np.arange(240)):
        raise ValueError("Expected 240 ordered one-minute slots per day")
    return_values = return_df.to_numpy(dtype=float)
    if not np.isfinite(return_values).all():
        raise ValueError("The comparison requires complete finite returns")
    dates = return_df.index.to_numpy()
    split = np.where(
        dates <= dt.date(2024, 12, 31),
        "train",
        np.where(
            dates <= dt.date(2025, 12, 31),
            "validation_2025",
            "retrospective_2026",
        ),
    )
    factor_df = pq.read_table(
        factor_path,
        columns=[
            "trading_date", "minute_index", "method", "lookback_days",
            "variance_factor", "factor_fit_end_date",
        ],
    ).to_pandas()
    finite_fit = factor_df.factor_fit_end_date.notna()
    if (
        factor_df.loc[finite_fit, "factor_fit_end_date"]
        >= factor_df.loc[finite_fit, "trading_date"]
    ).any():
        raise ValueError("A factor contains same-day or future fitted information")
    factor_arrays = {}
    coverage_rows = []
    for (method, lookback), group_df in factor_df.groupby(
        ["method", "lookback_days"], sort=True
    ):
        if method == "median_mixed":
            continue
        variance_factor = group_df.pivot(
            index="trading_date", columns="minute_index", values="variance_factor"
        ).reindex(index=dates, columns=np.arange(240)).to_numpy(dtype=float)
        valid_days = (
            np.isfinite(variance_factor).all(axis=1)
            & (variance_factor > 0).all(axis=1)
        )
        np.testing.assert_allclose(
            variance_factor[valid_days].mean(axis=1), 1, atol=1e-12
        )
        factor_arrays[(str(method), int(lookback))] = variance_factor
        coverage_rows.append(
            {
                "method": str(method),
                "lookback_days": int(lookback),
                "input_days": len(dates),
                "available_factor_days": int(valid_days.sum()),
            }
        )
    common_days = np.logical_and.reduce(
        [
            np.isfinite(factor).all(axis=1) & (factor > 0).all(axis=1)
            for factor in factor_arrays.values()
        ]
    )
    if not common_days.any():
        raise ValueError("No common dates for the return-factor methods")
    pd.DataFrame(
        {"trading_date": dates, "split": split, "common_day": common_days}
    ).to_csv(output_dir / "common_dates.csv", index=False)
    pd.DataFrame(coverage_rows).to_csv(
        output_dir / "factor_coverage.csv", index=False
    )
    retained_dates = dates[common_days]
    retained_split = split[common_days]
    retained_returns = return_values[common_days]
    daily_frames = []
    measure_columns = [
        "rv_raw",
        "rv_filtered",
        "bpv_raw_finite",
        "bpv_scalar_corrected",
        "bpv_return_filtered",
        "mixed_raw",
        "mixed_direct_division",
        "mixed_pair_division",
    ]

    for (method, lookback), minute_factor in factor_arrays.items():
        minute_factor = minute_factor[common_days]
        for frequency in (1, 5):
            slot_count = 240 // frequency
            # Both 120-slot sessions divide exactly into nonoverlapping blocks.
            frequency_returns = retained_returns.reshape(
                len(retained_dates), slot_count, frequency
            ).sum(axis=2)
            variance_factor = minute_factor.reshape(
                len(retained_dates), slot_count, frequency
            ).mean(axis=2)
            np.testing.assert_allclose(
                variance_factor.mean(axis=1), 1, atol=1e-12
            )
            session_starts = np.array([0, slot_count // 2])
            pair_slots = np.setdiff1d(
                np.arange(1, slot_count), [slot_count // 2]
            )
            pair_count = len(pair_slots)
            assert pair_count == slot_count - len(session_starts)
            pair_factor = np.sqrt(
                variance_factor[:, pair_slots]
                * variance_factor[:, pair_slots - 1]
            )
            raw_squared_returns = frequency_returns**2
            raw_pair_contribution = (
                np.pi
                / 2
                * np.abs(
                    frequency_returns[:, pair_slots]
                    * frequency_returns[:, pair_slots - 1]
                )
            )
            raw_bpv = slot_count / pair_count * raw_pair_contribution.sum(axis=1)
            periodic_scalar = pair_factor.mean(axis=1)
            filtered_returns = frequency_returns / np.sqrt(variance_factor)
            filtered_pair_contribution = (
                np.pi
                / 2
                * np.abs(
                    filtered_returns[:, pair_slots]
                    * filtered_returns[:, pair_slots - 1]
                )
            )
            np.testing.assert_allclose(
                raw_pair_contribution / pair_factor,
                filtered_pair_contribution,
                rtol=1e-12,
                atol=1e-16,
            )
            raw_mixed = (
                raw_squared_returns[:, session_starts].sum(axis=1)
                + raw_pair_contribution.sum(axis=1)
            )
            direct_mixed = (
                (
                    raw_squared_returns[:, session_starts]
                    / variance_factor[:, session_starts]
                ).sum(axis=1)
                + (
                    raw_pair_contribution
                    / variance_factor[:, pair_slots]
                ).sum(axis=1)
            )
            pair_mixed = (
                (
                    raw_squared_returns[:, session_starts]
                    / variance_factor[:, session_starts]
                ).sum(axis=1)
                + filtered_pair_contribution.sum(axis=1)
            )
            np.testing.assert_allclose(
                pair_mixed,
                (filtered_returns[:, session_starts] ** 2).sum(axis=1)
                + filtered_pair_contribution.sum(axis=1),
                rtol=1e-12,
                atol=1e-16,
            )
            daily_df = pd.DataFrame(
                {
                    "trading_date": retained_dates,
                    "split": retained_split,
                    "method": method,
                    "lookback_days": lookback,
                    "sampling_minutes": frequency,
                    "slot_count": slot_count,
                    "valid_pair_count": pair_count,
                    "periodic_scalar": periodic_scalar,
                    "finite_sample_multiplier": slot_count / pair_count,
                    "rv_raw": raw_squared_returns.sum(axis=1),
                    "rv_filtered": (filtered_returns**2).sum(axis=1),
                    "bpv_raw_finite": raw_bpv,
                    "bpv_scalar_corrected": raw_bpv / periodic_scalar,
                    "bpv_return_filtered":
                        slot_count / pair_count
                        * filtered_pair_contribution.sum(axis=1),
                    "mixed_raw": raw_mixed,
                    "mixed_direct_division": direct_mixed,
                    "mixed_pair_division": pair_mixed,
                }
            )
            for measure in measure_columns:
                daily_df[f"{measure}_rel_to_same_frequency_rv"] = np.divide(
                    daily_df[measure],
                    daily_df.rv_raw,
                    out=np.full(len(daily_df), np.nan),
                    where=daily_df.rv_raw.to_numpy() > 0,
                ) - 1
            daily_df["local_minus_scalar_bpv_rel"] = np.divide(
                daily_df.bpv_return_filtered - daily_df.bpv_scalar_corrected,
                daily_df.bpv_scalar_corrected,
                out=np.full(len(daily_df), np.nan),
                where=daily_df.bpv_scalar_corrected.to_numpy() > 0,
            )
            daily_df["direct_minus_pair_mixed_rel"] = np.divide(
                daily_df.mixed_direct_division - daily_df.mixed_pair_division,
                daily_df.mixed_pair_division,
                out=np.full(len(daily_df), np.nan),
                where=daily_df.mixed_pair_division.to_numpy() > 0,
            )
            if method == "none":
                np.testing.assert_allclose(
                    daily_df.bpv_raw_finite,
                    daily_df.bpv_scalar_corrected,
                    rtol=1e-12,
                )
                np.testing.assert_allclose(
                    daily_df.bpv_raw_finite,
                    daily_df.bpv_return_filtered,
                    rtol=1e-12,
                )
                np.testing.assert_allclose(
                    daily_df.mixed_direct_division,
                    daily_df.mixed_pair_division,
                    rtol=1e-12,
                )
            daily_frames.append(daily_df)

    daily_measures = pd.concat(daily_frames, ignore_index=True)
    daily_measures.to_csv(output_dir / "daily_measures.csv", index=False)
    metrics = [
        "periodic_scalar",
        *measure_columns,
        *[f"{measure}_rel_to_same_frequency_rv" for measure in measure_columns],
        "local_minus_scalar_bpv_rel",
        "direct_minus_pair_mixed_rel",
    ]
    summary_rows = []
    for keys, group_df in daily_measures.groupby(
        ["split", "method", "lookback_days", "sampling_minutes"], sort=True
    ):
        for metric in metrics:
            values = group_df[metric].dropna().to_numpy()
            summary_rows.append(
                {
                    "split": keys[0],
                    "method": keys[1],
                    "lookback_days": keys[2],
                    "sampling_minutes": keys[3],
                    "metric": metric,
                    "day_count": len(values),
                    "mean": values.mean() if len(values) else np.nan,
                    "median": np.median(values) if len(values) else np.nan,
                    "q05": np.quantile(values, 0.05) if len(values) else np.nan,
                    "q95": np.quantile(values, 0.95) if len(values) else np.nan,
                }
            )
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(output_dir / "split_summary.csv", index=False)
    merge_keys = ["trading_date", "split", "method", "lookback_days"]
    one_minute = daily_measures.loc[
        daily_measures.sampling_minutes == 1, merge_keys + measure_columns
    ]
    five_minute = daily_measures.loc[
        daily_measures.sampling_minutes == 5, merge_keys + measure_columns
    ]
    frequency_df = one_minute.merge(
        five_minute, on=merge_keys, suffixes=("_1m", "_5m"), validate="one_to_one"
    )
    for measure in measure_columns:
        denominator = frequency_df[f"{measure}_1m"].to_numpy()
        frequency_df[f"{measure}_5m_rel_to_1m"] = np.divide(
            frequency_df[f"{measure}_5m"],
            denominator,
            out=np.full(len(frequency_df), np.nan),
            where=denominator > 0,
        ) - 1
    frequency_df.to_csv(output_dir / "frequency_sensitivity.csv", index=False)
    frequency_metrics = [
        f"{measure}_5m_rel_to_1m" for measure in measure_columns
    ]
    frequency_summary = frequency_df.groupby(
        ["split", "method", "lookback_days"]
    )[frequency_metrics].agg(["mean", "median", "count"]).reset_index()
    frequency_summary.columns = [
        "_".join(str(part) for part in column if part)
        if isinstance(column, tuple) else column
        for column in frequency_summary.columns
    ]
    frequency_summary.to_csv(
        output_dir / "frequency_split_summary.csv", index=False
    )

    selected_methods = [
        ("none", 0), ("mean_rv", 120), ("median_rv", 120),
        ("boudt_wsd", 120), ("fff_wsd", 120), ("state_wsd", 360),
    ]
    selected_labels = [f"{method}/{window}" for method, window in selected_methods]
    comparison_measures = [
        "bpv_raw_finite",
        "bpv_scalar_corrected",
        "bpv_return_filtered",
        "mixed_direct_division",
        "mixed_pair_division",
    ]
    figure, axes = plt.subplots(1, 2, figsize=(15, 6), constrained_layout=True)
    offsets = np.linspace(-0.30, 0.30, len(comparison_measures))
    for axis, frequency in zip(axes, (1, 5)):
        for offset, measure in zip(offsets, comparison_measures):
            values = []
            for method, window in selected_methods:
                selected = summary_df.loc[
                    (summary_df.split == "retrospective_2026")
                    & (summary_df.method == method)
                    & (summary_df.lookback_days == window)
                    & (summary_df.sampling_minutes == frequency)
                    & (
                        summary_df.metric
                        == f"{measure}_rel_to_same_frequency_rv"
                    )
                ]
                values.append(float(selected["median"].iloc[0]))
            axis.barh(
                np.arange(len(selected_methods)) + offset,
                values, height=0.13, label=measure,
            )
        axis.axvline(0, color="black", linewidth=0.7)
        axis.set_yticks(np.arange(len(selected_methods)), selected_labels)
        axis.set_title(f"2026: {frequency}-minute measures")
        axis.set_xlabel("Median daily relative difference to same-frequency RV")
        axis.grid(axis="x", alpha=0.2)
    figure.legend(
        *axes[0].get_legend_handles_labels(),
        loc="outside lower center", ncol=3,
    )
    figure.suptitle("Differences in measures; distance to RV is not an accuracy score")
    figure.savefig(output_dir / "relative_measure_differences_2026.png", dpi=150)
    plt.close(figure)

    table_lines = [
        "| 方法/窗口 | 分钟频率 | 周期标量中位数 | 局部BPV相对标量BPV中位差 | 混合直接除数相对配对除数中位差 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for method, window in selected_methods:
        for frequency in (1, 5):
            selected = daily_measures.loc[
                (daily_measures.split == "retrospective_2026")
                & (daily_measures.method == method)
                & (daily_measures.lookback_days == window)
                & (daily_measures.sampling_minutes == frequency)
            ]
            table_lines.append(
                f"| {method}/{window} | {frequency} "
                f"| {selected.periodic_scalar.median():.6f} "
                f"| {selected.local_minus_scalar_bpv_rel.median():.2%} "
                f"| {selected.direct_minus_pair_mixed_rel.median():.2%} |"
            )
    manifest = {
        "common_days": int(common_days.sum()),
        "input_days": len(dates),
        "return_factor_settings": len(factor_arrays),
        "daily_rows": len(daily_measures),
        "frequencies_minutes": [1, 5],
        "slot_pair_counts": {"1": [240, 238], "5": [48, 46]},
        "excluded_methods": {
            "median_mixed": "Contribution-shape baseline; not a return variance factor"
        },
        "five_minute_aggregation": "Sum 5 nonoverlapping within-session log returns",
        "five_minute_factor": "Mean of corresponding 5 one-minute variance factors",
        "input_sha256": {
            str(path.relative_to(project_dir)):
                hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (observation_path, factor_path)
        },
        "elapsed_seconds": time.perf_counter() - started,
        "true_integrated_variance_available": False,
        "selected_winner": None,
    }
    (output_dir / "run_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    report = f"""# E06：实证实现波动度量与采样频率比较

符号与字段映射见[统一符号约定](../../research/03_method_derivations.md)。本报告区分标准差因子与方差因子，表格沿用既有输出字段名。

已使用{int(common_days.sum())}个共同有效日期、{len(factor_arrays)}种收益因子设置，
比较1分钟和5分钟频率；共{len(daily_measures):,}条日级记录。
训练截至2024-12-31，2025为验证段，2026为回顾评估，汇总分段保存。
因子只来自目标日前数据；本入口不重新估计参数，也不按评估结果选方法。

## 明确的统计量

令 `r_i` 为当前频率的收益，`f_i` 为对应标准差周期因子，`f_i²` 为方差因子；
日内 `mean(f_i²)=1`。本报告省略交易日下标和估计帽，实证因子均为事前估计值。
两段交易分别重置，`I` 是同 Session 有效配对的右端点集合；`K=|I|`。
本报告以 `N` 表示当前频率的全日格点数，对应主文档的 `M`：
1分钟 `N=240、K=238`，5分钟 `N=48、K=46`。每段首格不属于配对集合。

- RV：`sum r_i²`；过滤RV：`sum r_i²/f_i²`。
- 未经边界修正的BV：`BV=(pi/2)*sum_I |r_i*r_(i-1)|`。
  输出字段 `bpv_raw_finite` 实际是 `BV_bc=(N/K)*BV`，即主文档的边界修正版，不能省略下标称为原始BV。
- 两段周期标量：`C_pair=mean_I f_i*f_(i-1)`；标量校正为 `BV_bc/C_pair`。
- 局部收益过滤BPV：先计算 `r_i/f_i`，再按同样 `N/K` 计算边界修正BPV；
  等价于每个配对贡献除 `f_i*f_(i-1)`，已经逐格复算验证。
- 混合原始度量：两段首格 `r_i²` 加其余配对的 `(pi/2)|r_i*r_(i-1)|`，不额外乘 `N/K`。
- 混合直接除数：每个混合格点统一除从收益估计的 `f_i²`。
- 混合配对除数：首格除 `f_i²`，其余配对除 `f_i*f_(i-1)`；
  与先过滤收益再重新计算混合度量逐日一致。

单段标量常数对应Dette等Metrika式(11)–(12)，DOI:10.1007/s00184-022-00875-0。
用 `I` 和 `K` 处理两段是本项目的显式扩展；不是声称原文已经证明全部中国期货设定。
局部过滤改变了被测量的收益过程，`BV_bc/C_pair` 保留每日标量操作；
两者不应被解释为必须数值相同的两种计算实现。
有限样本 `C_pair` 的日方差期望解释还需要条件独立、日尺度与固定周期形状等模型条件。

5分钟收益通过各Session内不重叠的5个1分钟log收益求和，日内共48格。
5分钟方差因子 `f_i²` 取相应5个1分钟方差因子的均值，平方根才是该频率的标准差因子。
聚合后方差因子仍为日均1。这是从同一事前曲线作频率聚合，不是重新用5分钟数据拟合周期估计器。
若收益存在序列相关，简单方差因子聚合不能代表全部条件协方差；敏感性结果包含这一局限。
未跨午休配对、未使用目标日最终波动估计事前因子。

`median_mixed` 估计的是贡献形状 `hat g_mix`，对应主文档的带帽混合贡献因子；
它的直接除数是 `hat g_mix`，不能自动解释成 `f_i²`。本实验明确排除该方法，
所以这里的“混合直接除数”只指从收益估计的 `f_i²`；旧基线的兼容性诊断仍在其他研究输出中。

## 2026回顾结果示例

{chr(10).join(table_lines)}

上述差异只说明标量/局部操作、首格处理和采样频率确实会改变实证度量。
未观测真实IV，不以“接近RV”表示更准确，也不以差异较小选出胜者。
图上的同频率RV仅是便于阅读的参照分母。

## 文件与校验

- daily_measures.csv：所有方法、窗口、日期、频率的原始度量、标量及相对差异。
- split_summary.csv：训练/验证/回顾分段的均值、中位数、5%和95%分位数；分位区间是日分布范围，不是置信区间。
- frequency_sensitivity.csv：同一天同设置的5分钟相对1分钟差异；frequency_split_summary.csv为分段汇总。
- common_dates.csv、factor_coverage.csv：公共日期和各方法原生覆盖；共同日期不根据度量结果筛选。
- relative_measure_differences_2026.png：部分事先固定代表设置的同频率RV相对差异。
- run_manifest.json：输入哈希、频率、排除方法、规模和用时。

已核验因子严格早于目标日、方差因子日均为1、两段pair数、局部BPV代数、
混合贡献重算及none因子恒等关系。没有价格/API/正式湖写入，也没有真实IV标签或交易收益结论。
"""
    (output_dir / "REPORT.md").write_text(report, encoding="utf-8")
    print(
        f"Measure comparison complete: {len(daily_measures)} rows, "
        f"{int(common_days.sum())} common days, "
        f"{manifest['elapsed_seconds']:.2f}s; {output_dir}",
        flush=True,
    )


if __name__ == "__main__":
    main()
