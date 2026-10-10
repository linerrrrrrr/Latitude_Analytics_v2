"""Check how selected-method comparisons depend on daily bootstrap block length."""

import json
import pathlib

import numpy as np
import pandas as pd


def main():
    project_dir = pathlib.Path(__file__).resolve().parent
    output_dir = project_dir / "results/inference_robustness"
    output_dir.mkdir(parents=True, exist_ok=True)
    daily_losses_df = pd.read_csv(project_dir / "results/forecasting/daily_losses.csv")
    selected_losses_df = daily_losses_df[
        (daily_losses_df.year == 2026) & daily_losses_df.selected_on_2025
    ].sort_values("trading_date")
    sensitivity_records = []
    for (target, model), comparison_df in selected_losses_df.groupby(["target", "model"]):
        baseline_df = comparison_df[comparison_df.method == "median_mixed"].set_index("trading_date")
        for method, candidate_df in comparison_df.groupby("method"):
            if method in ("median_mixed", "none"):
                continue
            candidate_df = candidate_df.set_index("trading_date").loc[baseline_df.index]
            assert candidate_df.index.equals(baseline_df.index)
            for metric in ("qlike", "mse"):
                differences = (candidate_df[metric] - baseline_df[metric]).to_numpy()
                assert np.isfinite(differences).all()
                for block_days in (5, 10, 20):
                    generator = np.random.default_rng(20260907 + block_days)
                    starts = generator.integers(
                        0, len(differences) - block_days + 1,
                        size=(2000, int(np.ceil(len(differences) / block_days))),
                    )
                    indices = (starts[..., None] + np.arange(block_days)).reshape(2000, -1)[:, :len(differences)]
                    bootstrap_means = differences[indices].mean(axis=1)
                    lower, upper = np.quantile(bootstrap_means, [0.025, 0.975])
                    sensitivity_records.append({
                        "target": target, "model": model, "method": method,
                        "lookback_days": int(candidate_df.lookback_days.iloc[0]),
                        "baseline_method": "median_mixed",
                        "baseline_lookback_days": int(baseline_df.lookback_days.iloc[0]),
                        "metric": metric, "block_days": block_days, "replications": 2000,
                        "days": len(differences), "mean_difference": float(differences.mean()),
                        "ci_lower": float(lower), "ci_upper": float(upper),
                        "interval_contains_zero": bool(lower <= 0 <= upper),
                    })
    sensitivity_df = pd.DataFrame(sensitivity_records)
    sensitivity_df.to_csv(output_dir / "block_length_sensitivity.csv", index=False, encoding="utf-8-sig")
    representative_df = sensitivity_df[
        (sensitivity_df.target == "RV15") & (sensitivity_df.model == "ridge_intraday")
        & sensitivity_df.method.isin(["boudt_wsd", "fff_wsd"])
    ][["method", "metric", "block_days", "mean_difference", "ci_lower", "ci_upper", "interval_contains_zero"]]
    report = [
        "# 新旧估计器差异的区间敏感性", "",
        "这是在观察到小幅方法差异后追加的探索性稳健性分析，没有参与2025选窗，也不重新选择2026赢家。比较双方的窗口都已由2025锁定。",
        "",
        "用相同2026共同交易日的配对损失差，分别以5/10/20连续交易日块、2000次重采样计算逐项95% percentile区间。差值为候选减去现有median_mixed，负值较好。区间仍条件于既定数据、方法和已完成选窗；未覆盖所有模型筛选的不确定性，未作多重比较校正。",
        "",
        representative_df.to_markdown(index=False, floatfmt=".9g"), "",
        "这份结果把‘相对无周期处理有改善’与‘相对已有周期处理仍有增量’分开。QLIKE和MSE可以给出不同证据；小的平均差异也不等于经济上显著的收益。所有方法、两个目标和两个预测器的结果都保留在CSV，不能只引用有利的单一块长。",
    ]
    (output_dir / "REPORT.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    (output_dir / "run_manifest.json").write_text(json.dumps({
        "rows": len(sensitivity_df), "bootstrap_replications": 2000,
        "block_days": [5, 10, 20], "base_seed": 20260907,
        "analysis_status": "post-hoc sensitivity; no retuning; no multiplicity adjustment",
    }, indent=2), encoding="utf-8")
    print(f"Computed {len(sensitivity_df)} paired block-length sensitivity results", flush=True)


if __name__ == "__main__":
    main()
