"""Verify artifact identities, time boundaries and full-pipeline future perturbation."""

import hashlib
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from data_contracts import MINUTE_OBSERVATION_SCHEMA, PERIODICITY_FACTOR_SCHEMA


def main():
    project_dir = pathlib.Path(__file__).resolve().parent
    check_records = []
    started_at = time.monotonic()
    source_manifest = json.loads((project_dir / "literature/source_manifest.json").read_text(encoding="utf-8"))
    for source_record in source_manifest["records"]:
        pdf_path = project_dir / source_record["local_pdf"]
        assert pdf_path.read_bytes().startswith(b"%PDF"), pdf_path
        assert pdf_path.stat().st_size == source_record["bytes"], pdf_path
        assert hashlib.sha256(pdf_path.read_bytes()).hexdigest() == source_record["sha256"], pdf_path
    check_records.append({"check": "literature_pdf_hashes", "passed": True, "files": len(source_manifest["records"])})

    observations_table = pq.read_table(project_dir / "data/minute_observations.parquet")
    assert observations_table.schema.equals(MINUTE_OBSERVATION_SCHEMA, check_metadata=True)
    observations_df = observations_table.to_pandas()
    observation_key = ["trading_date", "minute_index"]
    assert not observations_df.duplicated(observation_key).any()
    trading_dates = np.sort(observations_df.trading_date.unique())
    observed_returns = observations_df.log_return.to_numpy().reshape(-1, 240)
    observed_bpv = observations_df.bpv_contribution.to_numpy().reshape(-1, 240)
    assert np.isnan(observed_bpv[:, [0, 120]]).all()
    assert np.isfinite(observed_returns).all()
    assert np.allclose(observations_df.rv_contribution, observations_df.log_return**2)
    for first_slot in (0, 120):
        expected_bpv = np.pi / 2 * np.abs(
            observed_returns[:, first_slot + 1:first_slot + 120]
            * observed_returns[:, first_slot:first_slot + 119]
        )
        np.testing.assert_allclose(observed_bpv[:, first_slot + 1:first_slot + 120], expected_bpv)
    check_records.append({"check": "observation_schema_keys_and_session_pairs", "passed": True, "rows": len(observations_df)})

    factor_path = project_dir / "results/periodicity_factors.parquet"
    factor_keys = []
    with pq.ParquetFile(factor_path) as factor_file:
        assert factor_file.schema_arrow.equals(PERIODICITY_FACTOR_SCHEMA, check_metadata=True)
        for row_group in range(factor_file.num_row_groups):
            factor_df = factor_file.read_row_group(row_group).to_pandas()
            candidate_key = (factor_df.method.iloc[0], int(factor_df.lookback_days.iloc[0]))
            assert not factor_df.duplicated(observation_key).any()
            assert len(factor_df) == len(observations_df)
            factor_keys.append(candidate_key)
            a_values = factor_df.variance_factor.to_numpy().reshape(-1, 240)
            g_values = factor_df.contribution_factor.to_numpy().reshape(-1, 240)
            finite_days = np.isfinite(a_values).all(axis=1)
            assert (np.isfinite(a_values).sum(axis=1) == finite_days.astype(int) * 240).all()
            assert (a_values[finite_days] > 0).all()
            np.testing.assert_allclose(a_values[finite_days].mean(axis=1), 1, atol=1e-12)
            if candidate_key[0] == "none":
                assert factor_df.factor_fit_end_date.isna().all()
            else:
                fitted_rows = factor_df.factor_fit_end_date.notna()
                assert (factor_df.loc[fitted_rows, "factor_fit_end_date"] < factor_df.loc[fitted_rows, "trading_date"]).all()
            if candidate_key[0] == "median_mixed":
                np.testing.assert_allclose(a_values, g_values, equal_nan=True)
            else:
                for first_slot in (0, 120):
                    np.testing.assert_allclose(g_values[:, first_slot], a_values[:, first_slot], equal_nan=True)
                    np.testing.assert_allclose(
                        g_values[:, first_slot + 1:first_slot + 120],
                        np.sqrt(a_values[:, first_slot + 1:first_slot + 120] * a_values[:, first_slot:first_slot + 119]),
                        equal_nan=True,
                    )
    assert len(factor_keys) == len(set(factor_keys)) == 23
    check_records.append({"check": "factor_schema_cutoffs_normalization_and_pair_scale", "passed": True, "candidates": len(factor_keys)})

    # This is an end-to-end leakage check: change all observations starting on a
    # target day, rerun the actual estimator entry point, and compare its prefix.
    # Checking only recorded cutoff columns would not establish this property.
    perturbation_date = trading_dates[-50]
    altered_df = observations_df.copy()
    future_mask = altered_df.trading_date >= perturbation_date
    slot_multiplier = np.where(altered_df.minute_index.to_numpy() % 7 < 3, 3.0, 0.4)
    altered_df.loc[future_mask, "log_return"] *= slot_multiplier[future_mask]
    altered_df["rv_contribution"] = altered_df.log_return**2
    previous_returns = altered_df.groupby(["trading_date", "session_number"])["log_return"].shift(1)
    altered_df["bpv_contribution"] = np.pi / 2 * np.abs(altered_df.log_return * previous_returns)
    altered_df["mixed_contribution"] = altered_df.bpv_contribution.where(
        ~altered_df.minute_index.isin([0, 120]), altered_df.rv_contribution
    )
    temp_parent = pathlib.Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(prefix="intraday_causality_", dir=temp_parent) as temp_name:
        perturbation_dir = pathlib.Path(temp_name).resolve()
        assert perturbation_dir.parent == temp_parent
        assert perturbation_dir.name.startswith("intraday_causality_")
        (perturbation_dir / "data").mkdir()
        for module_name in ("data_contracts.py", "estimators.py", "estimate_periodicity.py"):
            shutil.copyfile(project_dir / module_name, perturbation_dir / module_name)
        altered_table = pa.Table.from_pandas(
            altered_df, schema=MINUTE_OBSERVATION_SCHEMA, preserve_index=False
        ).replace_schema_metadata(MINUTE_OBSERVATION_SCHEMA.metadata)
        pq.write_table(altered_table, perturbation_dir / "data/minute_observations.parquet")
        probe_run = subprocess.run(
            [sys.executable, "-X", "utf8", str(perturbation_dir / "estimate_periodicity.py")],
            cwd=perturbation_dir, capture_output=True, text=True, encoding="utf-8", timeout=180,
        )
        if probe_run.returncode:
            raise RuntimeError(probe_run.stdout + probe_run.stderr)
        changed_future_values = 0
        with pq.ParquetFile(factor_path) as original_file, pq.ParquetFile(
            perturbation_dir / "results/periodicity_factors.parquet"
        ) as altered_file:
            assert original_file.num_row_groups == altered_file.num_row_groups
            for row_group in range(original_file.num_row_groups):
                original_factor_df = original_file.read_row_group(row_group).to_pandas()
                altered_factor_df = altered_file.read_row_group(row_group).to_pandas()
                prefix_mask = original_factor_df.trading_date <= perturbation_date
                np.testing.assert_allclose(
                    original_factor_df.loc[prefix_mask, ["variance_factor", "contribution_factor"]],
                    altered_factor_df.loc[prefix_mask, ["variance_factor", "contribution_factor"]],
                    rtol=0, atol=0, equal_nan=True,
                )
                suffix_mask = original_factor_df.trading_date > perturbation_date
                changed_future_values += int(np.sum(~np.isclose(
                    original_factor_df.loc[suffix_mask, "variance_factor"],
                    altered_factor_df.loc[suffix_mask, "variance_factor"], equal_nan=True,
                )))
        assert changed_future_values > 0
    check_records.append({
        "check": "future_observation_perturbation_full_estimator_rerun", "passed": True,
        "perturbed_from": str(perturbation_date), "prefix_including_target_day_unchanged": True,
        "later_factor_values_changed": changed_future_values,
    })

    # Reopen the actual prediction artifact on every complete reproduction run.
    # Timing assertions plus source-label recomputation complement source review.
    prediction_path = project_dir / "results/forecasting/minute_forecasts.parquet"
    observation_lookup = observations_df.set_index(observation_key)
    checked_forecasts = 0
    checked_labels = 0
    with pq.ParquetFile(prediction_path) as prediction_file:
        for row_group in range(prediction_file.num_row_groups):
            prediction_df = prediction_file.read_row_group(row_group).to_pandas()
            checked_forecasts += len(prediction_df)
            assert (prediction_df.label_minute_count == 15).all()
            assert (prediction_df.target_start_minute_index == prediction_df.origin_minute_index + 1).all()
            assert (prediction_df.target_end_minute_index == prediction_df.origin_minute_index + 15).all()
            assert (prediction_df.last_feature_bar_at <= prediction_df.origin_bar_at).all()
            assert (prediction_df.training_label_end_date < prediction_df.trading_date).all()
            fitted_rows = prediction_df.factor_fit_end_date.notna()
            assert (prediction_df.loc[fitted_rows, "factor_fit_end_date"] < prediction_df.loc[fitted_rows, "trading_date"]).all()
            assert np.isfinite(prediction_df[["prediction", "target"]]).all().all()
            assert (prediction_df.prediction > 0).all() and (prediction_df.target >= 0).all()
            assert (prediction_df.origin_minute_index // 120 == prediction_df.target_end_minute_index // 120).all()
            for sampled_index in np.linspace(0, len(prediction_df) - 1, 5, dtype=int):
                prediction_row = prediction_df.iloc[sampled_index]
                label_key = pd.MultiIndex.from_product([
                    [prediction_row.trading_date],
                    range(prediction_row.target_start_minute_index, prediction_row.target_end_minute_index + 1),
                ], names=observation_key)
                target_column = "rv_contribution" if prediction_row.target_name == "RV15" else "bpv_contribution"
                recomputed_target = observation_lookup.loc[label_key, target_column].sum()
                np.testing.assert_allclose(recomputed_target, prediction_row.target, rtol=1e-12, atol=1e-20)
                checked_labels += 1
    check_records.append({
        "check": "reopened_forecast_cutoffs_and_source_labels", "passed": True,
        "forecast_rows": checked_forecasts, "independent_labels_recomputed": checked_labels,
    })

    daily_forecasts_df = pq.read_table(project_dir / "results/forecasting/daily_har_predictions.parquet").to_pandas()
    assert (daily_forecasts_df.training_label_end_date < daily_forecasts_df.trading_date).all()
    assert (daily_forecasts_df.target_end_date >= daily_forecasts_df.trading_date).all()
    assert all(start.year == end.year for start, end in zip(daily_forecasts_df.trading_date, daily_forecasts_df.target_end_date))
    check_records.append({"check": "daily_multihorizon_training_and_split_purge", "passed": True, "rows": len(daily_forecasts_df)})

    from run_forecasting import rolling_ridge_predictions
    generator = np.random.default_rng(20260907)
    synthetic_features = np.exp(generator.normal(size=(380, 5, 4)))
    synthetic_targets = 0.2 + synthetic_features @ np.array([0.3, 0.5, 0.2, 0.1])
    synthetic_targets += generator.uniform(0.0, 0.1, size=(380, 5))
    forecast_day = 300
    for label_delay in (1, 5, 22):
        baseline_prediction, fit_counts = rolling_ridge_predictions(
            synthetic_features, synthetic_targets, last_label_delay=label_delay
        )
        assert fit_counts[forecast_day] == 252
        assert np.isfinite(baseline_prediction[forecast_day]).all()
        future_targets = synthetic_targets.copy()
        future_targets[forecast_day:] *= 100
        future_prediction, _ = rolling_ridge_predictions(
            synthetic_features, future_targets, last_label_delay=label_delay
        )
        np.testing.assert_allclose(baseline_prediction[:forecast_day + 1], future_prediction[:forecast_day + 1], rtol=0, atol=0, equal_nan=True)
        future_features = synthetic_features.copy()
        future_features[forecast_day + 1:] *= 100
        future_prediction, _ = rolling_ridge_predictions(
            future_features, synthetic_targets, last_label_delay=label_delay
        )
        np.testing.assert_allclose(baseline_prediction[:forecast_day + 1], future_prediction[:forecast_day + 1], rtol=0, atol=0, equal_nan=True)
        unavailable_targets = synthetic_targets.copy()
        unavailable_targets[forecast_day - label_delay + 1] *= 100
        boundary_prediction, _ = rolling_ridge_predictions(
            synthetic_features, unavailable_targets, last_label_delay=label_delay
        )
        np.testing.assert_array_equal(baseline_prediction[forecast_day], boundary_prediction[forecast_day])
        assert not np.array_equal(baseline_prediction[forecast_day + 1], boundary_prediction[forecast_day + 1])
    check_records.append({"check": "rolling_regression_future_targets_features_and_label_maturity", "passed": True, "label_delays": [1, 5, 22]})
    (project_dir / "results/verification.json").write_text(json.dumps({
        "checks": check_records, "elapsed_seconds": time.monotonic() - started_at,
        "scope": "Independent snapshot, periodicity future perturbation and reopened prediction verification",
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(check_records, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
