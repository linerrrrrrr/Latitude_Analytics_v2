"""Generate the frozen 40 IM volatility-proxy features for XGBoost.

The feature set is frozen from the validation-only selection recorded in this
research directory.  At every source-minute close this program re-anchors the
time, volume and money clocks, reconstructs the preceding event sequences,
refits the selected models and forecasts the next event on each clock.

The program is a batch CLI suitable for an Alibaba Cloud PAI command job.  PAI
inputs should be mounted as local paths.  Outputs are committed one trading day
at a time so date shards can run independently and a stopped job can resume.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import pathlib
import time
import warnings

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from arch.univariate import FIGARCH, GARCH, Normal, ZeroMean
from arch.univariate.recursions import figarch_weights
from scipy.optimize import minimize, nnls
from scipy.signal import lfilter
from scipy.special import logsumexp


TIMEZONE = "Asia/Shanghai"
FEATURE_SET_VERSION = "im_xgb_volatility_40_reanchored_v1"
MODEL_MEMORY = 1000
MIN_NATIVE_OBSERVATIONS = 500
MIN_HAR_OBSERVATIONS = 100
COORDINATE_TOLERANCE = 1e-8
SUPPORTED_GRANULARITIES = (5, 10, 15)
SUPPORTED_SEASONALITY_LOOKBACKS = (0, 60, 120)


@dataclasses.dataclass(frozen=True)
class FeatureSpec:
    name: str
    clock: str
    equivalent_minutes: int
    model: str
    target: str
    seasonality_lookback: int
    fit_window_months: int


# Frozen after ranking only validation_2025h1, validation_2025h2 and
# validation_2026h1.  final_2026 was used only for the post-freeze audit.
FEATURE_SPECS = (
    FeatureSpec("vol_money_05m_arfima_bpv_s000_w06", "money", 5, "ARFIMA-logBPV", "bpv", 0, 6),
    FeatureSpec("vol_money_05m_har_bpv_s000_w06", "money", 5, "HAR", "bpv", 0, 6),
    FeatureSpec("vol_money_05m_arfima_rv_s120_w06", "money", 5, "ARFIMA-logRV", "rv", 120, 6),
    FeatureSpec("vol_money_05m_har_rv_s000_w06", "money", 5, "HAR", "rv", 0, 6),
    FeatureSpec("vol_money_10m_arfima_bpv_s120_w06", "money", 10, "ARFIMA-logBPV", "bpv", 120, 6),
    FeatureSpec("vol_money_10m_har_bpv_s000_w06", "money", 10, "HAR", "bpv", 0, 6),
    FeatureSpec("vol_money_10m_arfima_rv_s000_w12", "money", 10, "ARFIMA-logRV", "rv", 0, 12),
    FeatureSpec("vol_money_10m_har_rv_s000_w06", "money", 10, "HAR", "rv", 0, 6),
    FeatureSpec("vol_money_15m_arfima_bpv_s000_w06", "money", 15, "ARFIMA-logBPV", "bpv", 0, 6),
    FeatureSpec("vol_money_15m_har_bpv_s000_w06", "money", 15, "HAR", "bpv", 0, 6),
    FeatureSpec("vol_money_15m_arfima_rv_s000_w06", "money", 15, "ARFIMA-logRV", "rv", 0, 6),
    FeatureSpec("vol_money_15m_har_rv_s000_w06", "money", 15, "HAR", "rv", 0, 6),
    FeatureSpec("vol_time_05m_arfima_bpv_s060_w06", "time", 5, "ARFIMA-logBPV", "bpv", 60, 6),
    FeatureSpec("vol_time_05m_har_bpv_s120_w06", "time", 5, "HAR", "bpv", 120, 6),
    FeatureSpec("vol_time_05m_arfima_rv_s120_w06", "time", 5, "ARFIMA-logRV", "rv", 120, 6),
    FeatureSpec("vol_time_05m_har_rv_s120_w06", "time", 5, "HAR", "rv", 120, 6),
    FeatureSpec("vol_time_10m_arfima_bpv_s120_w06", "time", 10, "ARFIMA-logBPV", "bpv", 120, 6),
    FeatureSpec("vol_time_10m_har_bpv_s120_w06", "time", 10, "HAR", "bpv", 120, 6),
    FeatureSpec("vol_time_10m_arfima_rv_s120_w06", "time", 10, "ARFIMA-logRV", "rv", 120, 6),
    FeatureSpec("vol_time_10m_har_rv_s120_w12", "time", 10, "HAR", "rv", 120, 12),
    FeatureSpec("vol_time_15m_arfima_bpv_s060_w06", "time", 15, "ARFIMA-logBPV", "bpv", 60, 6),
    FeatureSpec("vol_time_15m_har_bpv_s120_w12", "time", 15, "HAR", "bpv", 120, 12),
    FeatureSpec("vol_time_15m_arfima_rv_s060_w06", "time", 15, "ARFIMA-logRV", "rv", 60, 6),
    FeatureSpec("vol_time_15m_har_rv_s120_w12", "time", 15, "HAR", "rv", 120, 12),
    FeatureSpec("vol_volume_05m_arfima_bpv_s000_w12", "volume", 5, "ARFIMA-logBPV", "bpv", 0, 12),
    FeatureSpec("vol_volume_05m_har_bpv_s000_w06", "volume", 5, "HAR", "bpv", 0, 6),
    FeatureSpec("vol_volume_05m_arfima_rv_s000_w06", "volume", 5, "ARFIMA-logRV", "rv", 0, 6),
    FeatureSpec("vol_volume_05m_har_rv_s000_w06", "volume", 5, "HAR", "rv", 0, 6),
    FeatureSpec("vol_volume_10m_arfima_bpv_s000_w12", "volume", 10, "ARFIMA-logBPV", "bpv", 0, 12),
    FeatureSpec("vol_volume_10m_har_bpv_s000_w06", "volume", 10, "HAR", "bpv", 0, 6),
    FeatureSpec("vol_volume_10m_arfima_rv_s000_w06", "volume", 10, "ARFIMA-logRV", "rv", 0, 6),
    FeatureSpec("vol_volume_10m_har_rv_s000_w06", "volume", 10, "HAR", "rv", 0, 6),
    FeatureSpec("vol_volume_15m_arfima_bpv_s000_w06", "volume", 15, "ARFIMA-logBPV", "bpv", 0, 6),
    FeatureSpec("vol_volume_15m_har_bpv_s000_w06", "volume", 15, "HAR", "bpv", 0, 6),
    FeatureSpec("vol_volume_15m_arfima_rv_s000_w06", "volume", 15, "ARFIMA-logRV", "rv", 0, 6),
    FeatureSpec("vol_volume_15m_har_rv_s000_w06", "volume", 15, "HAR", "rv", 0, 6),
    FeatureSpec("vol_volume_15m_figarch_rv_s000_w12", "volume", 15, "FIGARCH", "rv", 0, 12),
    FeatureSpec("vol_time_05m_figarch_rv_s120_w12", "time", 5, "FIGARCH", "rv", 120, 12),
    FeatureSpec("vol_volume_10m_garch_rv_s000_w12", "volume", 10, "GARCH", "rv", 0, 12),
    FeatureSpec("vol_money_15m_garch_rv_s000_w12", "money", 15, "GARCH", "rv", 0, 12),
)

if len(FEATURE_SPECS) != 40 or len({spec.name for spec in FEATURE_SPECS}) != 40:
    raise RuntimeError("The frozen feature manifest must contain 40 unique feature names")
if any(spec.seasonality_lookback not in SUPPORTED_SEASONALITY_LOOKBACKS for spec in FEATURE_SPECS):
    raise RuntimeError("The frozen manifest may use only seasonality 0, 60 and 120")


@dataclasses.dataclass
class MinuteIntegral:
    value_prefix: np.ndarray
    missing_prefix: np.ndarray
    clean_values: np.ndarray
    missing_values: np.ndarray

    @classmethod
    def from_values(cls, values: np.ndarray) -> "MinuteIntegral":
        values = np.asarray(values, dtype=np.float64)
        finite = np.isfinite(values)
        clean = np.where(finite, values, 0.0)
        missing = ~finite
        return cls(
            value_prefix=np.r_[0.0, np.cumsum(clean)],
            missing_prefix=np.r_[0.0, np.cumsum(missing.astype(np.float64))],
            clean_values=clean,
            missing_values=missing,
        )

    def integrate(self, start_coordinates: np.ndarray, end_coordinates: np.ndarray) -> np.ndarray:
        start_coordinates = np.asarray(start_coordinates, dtype=np.float64)
        end_coordinates = np.asarray(end_coordinates, dtype=np.float64)
        source_count = len(self.clean_values)
        coordinates = np.r_[start_coordinates, end_coordinates]
        valid_coordinate = (
            np.isfinite(coordinates) & (coordinates >= 0) & (coordinates <= source_count)
        )
        safe_coordinate = np.where(valid_coordinate, coordinates, 0.0)
        rounded = np.rint(safe_coordinate)
        safe_coordinate = np.where(
            np.abs(safe_coordinate - rounded) <= COORDINATE_TOLERANCE,
            rounded,
            safe_coordinate,
        )
        integer_coordinate = np.floor(safe_coordinate).astype(np.int64)
        fraction = safe_coordinate - integer_coordinate
        bounded_index = np.minimum(integer_coordinate, source_count - 1)
        integral = (
            self.value_prefix[integer_coordinate]
            + fraction * self.clean_values[bounded_index]
        )
        missing_integral = (
            self.missing_prefix[integer_coordinate]
            + fraction * self.missing_values[bounded_index]
        )
        interval_count = len(start_coordinates)
        interval_values = integral[interval_count:] - integral[:interval_count]
        missing_coverage = missing_integral[interval_count:] - missing_integral[:interval_count]
        usable = (
            valid_coordinate[:interval_count]
            & valid_coordinate[interval_count:]
            & (end_coordinates > start_coordinates)
            & (missing_coverage <= COORDINATE_TOLERANCE)
        )
        interval_values[~usable] = np.nan
        return interval_values


@dataclasses.dataclass
class PreparedSeries:
    values: np.ndarray
    previous_sample_ns: np.ndarray
    sample_ns: np.ndarray
    features: np.ndarray
    next_targets: np.ndarray
    next_target_ns: np.ndarray
    next_feature_start_ns: np.ndarray
    adjacent_pair: np.ndarray


@dataclasses.dataclass
class FeatureInputs:
    source_df: pd.DataFrame
    bar_ns: np.ndarray
    minute_start_ns: np.ndarray
    contract_code: np.ndarray
    trading_date: np.ndarray
    session_number: np.ndarray
    source_session_id: np.ndarray
    event_log_price: np.ndarray
    activity_volume: np.ndarray
    activity_money: np.ndarray
    volume_prefix: np.ndarray
    money_prefix: np.ndarray
    seasonality: dict[int, np.ndarray]
    rv_integral: MinuteIntegral
    bpv_integral: MinuteIntegral
    input_snapshot_id: str
    event_price_column: str


def _to_shanghai(values: pd.Series | pd.Index) -> pd.DatetimeIndex:
    timestamps = pd.DatetimeIndex(pd.to_datetime(values, utc=True))
    return timestamps.tz_convert(TIMEZONE)


def _read_parquet_table(path: pathlib.Path, columns: list[str]) -> pa.Table:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_dir():
        return ds.dataset(path, format="parquet").to_table(columns=columns)
    return pq.read_table(path, columns=columns)


def _load_activity(
    source_bar_at: pd.DatetimeIndex,
    activity_input: pathlib.Path | None,
    activity_duckdb: pathlib.Path | None,
) -> pd.DataFrame:
    if activity_input is not None:
        activity_df = _read_parquet_table(
            activity_input,
            ["bar_at", "all_contract_volume", "all_contract_money"],
        ).to_pandas()
    else:
        if activity_duckdb is None or not activity_duckdb.exists():
            raise FileNotFoundError(
                "Provide --activity-input, or provide an existing --activity-duckdb"
            )
        import duckdb

        connection = duckdb.connect(str(activity_duckdb), read_only=True)
        try:
            activity_df = connection.execute(
                """
                SELECT
                    bar_at,
                    sum(volume)::DOUBLE AS all_contract_volume,
                    sum(money)::DOUBLE AS all_contract_money
                FROM futures_minute
                WHERE regexp_full_match(contract_code, 'IM[0-9]{4}[.]CCFX')
                  AND bar_at >= ?
                  AND bar_at <= ?
                GROUP BY bar_at
                ORDER BY bar_at
                """,
                [source_bar_at[0].to_pydatetime(), source_bar_at[-1].to_pydatetime()],
            ).fetchdf()
        finally:
            connection.close()

    activity_df = activity_df.copy()
    activity_df["bar_at"] = _to_shanghai(activity_df["bar_at"])
    if activity_df["bar_at"].duplicated().any():
        raise ValueError("Activity input contains duplicate bar_at values")
    activity_df = activity_df.set_index("bar_at").reindex(source_bar_at)
    if activity_df[["all_contract_volume", "all_contract_money"]].isna().any().any():
        missing_count = int(
            activity_df[["all_contract_volume", "all_contract_money"]].isna().any(axis=1).sum()
        )
        raise ValueError(f"Activity input is missing {missing_count} source minutes")
    activity_df = activity_df.reset_index(names="bar_at")
    for column in ["all_contract_volume", "all_contract_money"]:
        values = activity_df[column].to_numpy(dtype=np.float64)
        if not np.isfinite(values).all() or np.any(values < 0):
            raise ValueError(f"{column} must be finite and nonnegative")
    return activity_df


def _load_seasonality(
    source_df: pd.DataFrame,
    seasonality_path: pathlib.Path,
    lookback: int,
    raw_log_return: np.ndarray,
    raw_bpv: np.ndarray,
) -> np.ndarray:
    factor_df = _read_parquet_table(
        seasonality_path,
        [
            "contract_code",
            "bar_at",
            "lookback_trading_days",
            "log_return",
            "bpv_contribution",
            "bpv_seasonality",
            "history_end_date",
        ],
    ).to_pandas()
    if not factor_df["lookback_trading_days"].eq(lookback).all():
        raise ValueError(f"{seasonality_path} does not contain only {lookback}-day factors")
    factor_df["bar_at"] = _to_shanghai(factor_df["bar_at"])
    factor_keys = pd.MultiIndex.from_frame(factor_df[["contract_code", "bar_at"]])
    source_keys = pd.MultiIndex.from_frame(source_df[["contract_code", "bar_at"]])
    if not factor_keys.is_unique:
        raise ValueError(f"{seasonality_path} contains duplicate business keys")
    locations = factor_keys.get_indexer(source_keys)
    if np.any(locations < 0) or len(factor_df) != len(source_df):
        raise ValueError(f"{seasonality_path} does not exactly cover the minute source")
    aligned = factor_df.iloc[locations].reset_index(drop=True)
    if not np.allclose(
        aligned["log_return"].to_numpy(dtype=np.float64),
        raw_log_return,
        rtol=0,
        atol=1e-14,
        equal_nan=True,
    ):
        raise ValueError(f"{seasonality_path} raw returns do not match the minute source")
    if not np.allclose(
        aligned["bpv_contribution"].to_numpy(dtype=np.float64),
        raw_bpv,
        rtol=0,
        atol=1e-18,
        equal_nan=True,
    ):
        raise ValueError(f"{seasonality_path} BPV contributions do not match the minute source")
    history_end = pd.to_datetime(aligned["history_end_date"])
    source_dates = pd.to_datetime(source_df["trading_date"])
    if not ((history_end < source_dates) | history_end.isna()).all():
        raise ValueError(f"{seasonality_path} contains a same-day or future seasonal estimate")

    factors = aligned["bpv_seasonality"].to_numpy(dtype=np.float64).copy()
    slot_number = source_df["bar_at"].dt.hour.to_numpy() * 60 + source_df["bar_at"].dt.minute.to_numpy()
    reference_slot = np.where(
        np.isin(slot_number, [571, 572]),
        573,
        np.where(np.isin(slot_number, [781, 782]), 783, slot_number),
    )
    factor_lookup = pd.Series(
        factors,
        index=pd.MultiIndex.from_arrays([source_df["trading_date"], slot_number]),
    )
    structural = np.isin(slot_number, [571, 572, 781, 782]) & ~np.isfinite(factors)
    reference_index = pd.MultiIndex.from_arrays([source_df["trading_date"], reference_slot])
    reference_factors = factor_lookup.reindex(reference_index).to_numpy(dtype=np.float64)
    factors[structural] = reference_factors[structural]
    return factors


def load_feature_inputs(
    minute_input: pathlib.Path,
    activity_input: pathlib.Path | None,
    activity_duckdb: pathlib.Path | None,
    seasonality_60_input: pathlib.Path,
    seasonality_120_input: pathlib.Path,
    event_price_column: str,
) -> FeatureInputs:
    source_columns = [
        "contract_code",
        "trading_date",
        "session_number",
        "bar_at",
        "close",
        "price_adjustment_available_at",
    ]
    if event_price_column != "close":
        source_columns.append(event_price_column)
    source_df = _read_parquet_table(minute_input, source_columns).to_pandas()
    source_df["bar_at"] = _to_shanghai(source_df["bar_at"])
    source_df["price_adjustment_available_at"] = _to_shanghai(
        source_df["price_adjustment_available_at"]
    )
    source_df = source_df.sort_values("bar_at", kind="stable").reset_index(drop=True)
    if source_df["bar_at"].duplicated().any():
        raise ValueError("Minute source contains duplicate bar_at values")
    if len(source_df) == 0:
        raise ValueError("Minute source is empty")
    if not source_df["close"].gt(0).all():
        raise ValueError("Minute close must be positive")
    if event_price_column not in source_df or not source_df[event_price_column].gt(0).all():
        raise ValueError(f"{event_price_column} must exist and be positive")

    source_groups = source_df.groupby(
        ["trading_date", "contract_code", "session_number"], sort=False
    )
    consecutive_minute = source_groups["bar_at"].diff().eq(pd.Timedelta(minutes=1))
    raw_log_close = np.log(source_df["close"].to_numpy(dtype=np.float64))
    raw_log_return = pd.Series(raw_log_close, index=source_df.index).groupby(
        [source_df["trading_date"], source_df["contract_code"], source_df["session_number"]],
        sort=False,
    ).diff().where(consecutive_minute)
    previous_absolute_return = raw_log_return.abs().groupby(
        [source_df["trading_date"], source_df["contract_code"], source_df["session_number"]],
        sort=False,
    ).shift(1)
    raw_rv = raw_log_return.to_numpy(dtype=np.float64) ** 2
    raw_bpv = (
        (np.pi / 2)
        * raw_log_return.abs().to_numpy(dtype=np.float64)
        * previous_absolute_return.to_numpy(dtype=np.float64)
    )

    source_bar_at = pd.DatetimeIndex(source_df["bar_at"])
    activity_df = _load_activity(source_bar_at, activity_input, activity_duckdb)
    seasonality = {
        60: _load_seasonality(
            source_df,
            seasonality_60_input,
            60,
            raw_log_return.to_numpy(dtype=np.float64),
            raw_bpv,
        ),
        120: _load_seasonality(
            source_df,
            seasonality_120_input,
            120,
            raw_log_return.to_numpy(dtype=np.float64),
            raw_bpv,
        ),
    }
    activity_volume = activity_df["all_contract_volume"].to_numpy(dtype=np.float64)
    activity_money = activity_df["all_contract_money"].to_numpy(dtype=np.float64)
    source_session_keys = pd.MultiIndex.from_frame(
        source_df[["trading_date", "contract_code", "session_number"]]
    )
    source_session_id = pd.factorize(source_session_keys, sort=False)[0]
    event_log_price = np.log(source_df[event_price_column].to_numpy(dtype=np.float64))

    snapshot_hasher = hashlib.sha256()
    snapshot_hasher.update(FEATURE_SET_VERSION.encode())
    snapshot_hasher.update(event_price_column.encode())
    for values in [
        source_bar_at.as_unit("ns").asi8,
        pd.DatetimeIndex(source_df["price_adjustment_available_at"]).as_unit("ns").asi8,
        source_df["session_number"].to_numpy(dtype=np.int8),
        source_df["close"].to_numpy(dtype="<f8"),
        event_log_price.astype("<f8", copy=False),
        activity_volume.astype("<f8", copy=False),
        activity_money.astype("<f8", copy=False),
        seasonality[60].astype("<f8", copy=False),
        seasonality[120].astype("<f8", copy=False),
    ]:
        snapshot_hasher.update(np.ascontiguousarray(values).tobytes())
    snapshot_hasher.update("\n".join(source_df["contract_code"].astype(str)).encode())
    snapshot_hasher.update("\n".join(source_df["trading_date"].astype(str)).encode())

    return FeatureInputs(
        source_df=source_df,
        bar_ns=source_bar_at.as_unit("ns").asi8,
        minute_start_ns=source_bar_at.as_unit("ns").asi8 - 60_000_000_000,
        contract_code=source_df["contract_code"].astype(str).to_numpy(),
        trading_date=source_df["trading_date"].to_numpy(),
        session_number=source_df["session_number"].to_numpy(dtype=np.int8),
        source_session_id=source_session_id,
        event_log_price=event_log_price,
        activity_volume=activity_volume,
        activity_money=activity_money,
        volume_prefix=np.r_[0.0, np.cumsum(activity_volume)],
        money_prefix=np.r_[0.0, np.cumsum(activity_money)],
        seasonality=seasonality,
        rv_integral=MinuteIntegral.from_values(raw_rv),
        bpv_integral=MinuteIntegral.from_values(raw_bpv),
        input_snapshot_id=snapshot_hasher.hexdigest(),
        event_price_column=event_price_column,
    )


def _locate_backward_sampling_points(
    directed_source_index: np.ndarray,
    directed_increment: np.ndarray,
    directed_cumulative: np.ndarray,
    sampling_unit: float,
) -> tuple[np.ndarray, np.ndarray]:
    if not np.isfinite(sampling_unit) or sampling_unit <= 0:
        raise ValueError("Sampling unit must be finite and positive")
    clock_quotient = directed_cumulative / sampling_unit
    if not np.isfinite(clock_quotient).all() or np.any(directed_increment < 0):
        raise ValueError("Clock increments must be finite and nonnegative")
    tolerance = 32 * np.finfo(np.float64).eps * max(1.0, clock_quotient[-1])
    rounded = np.rint(clock_quotient)
    clock_quotient = np.where(
        np.abs(clock_quotient - rounded) <= tolerance,
        rounded,
        clock_quotient,
    )
    quotient_before = np.r_[0.0, clock_quotient[:-1]]
    quotient_increment = clock_quotient - quotient_before
    positive_minute_position = np.flatnonzero(directed_increment > 0)
    if np.any(quotient_increment[positive_minute_position] <= 0):
        raise ValueError("Clock cumulative precision cannot distinguish positive increments")
    sample_steps = np.arange(1, int(np.floor(clock_quotient[-1])) + 1, dtype=np.int64)
    hit_positions = positive_minute_position[
        np.searchsorted(clock_quotient[positive_minute_position], sample_steps, side="left")
    ]
    directed_fraction = (
        (sample_steps - quotient_before[hit_positions])
        / quotient_increment[hit_positions]
    ).clip(0, 1)
    source_index = np.r_[directed_source_index[0], directed_source_index[hit_positions]][::-1]
    minute_fraction = np.r_[1.0, 1.0 - directed_fraction][::-1]
    return source_index.astype(np.int64, copy=False), minute_fraction.astype(np.float64, copy=False)


def _build_event_frame(
    inputs: FeatureInputs,
    source_index: np.ndarray,
    minute_fraction: np.ndarray,
) -> pd.DataFrame:
    if len(source_index) < 2:
        raise ValueError("Clock history contains fewer than two sampling points")
    sample_ns = inputs.minute_start_ns[source_index] + np.rint(
        minute_fraction * 60_000_000_000
    ).astype(np.int64)
    left_source_index = np.maximum(source_index - 1, 0)
    left_log_price = inputs.event_log_price[left_source_index].copy()
    left_log_price[source_index == 0] = inputs.event_log_price[0]
    sample_log_price = left_log_price + minute_fraction * (
        inputs.event_log_price[source_index] - left_log_price
    )

    previous_source_index = source_index[:-1]
    current_source_index = source_index[1:]
    previous_fraction = minute_fraction[:-1]
    current_fraction = minute_fraction[1:]
    start_coordinate = previous_source_index + previous_fraction
    end_coordinate = current_source_index + current_fraction
    for coordinates in [start_coordinate, end_coordinate]:
        rounded = np.rint(coordinates)
        snap = np.abs(coordinates - rounded) <= COORDINATE_TOLERANCE
        coordinates[snap] = rounded[snap]

    has_interval = end_coordinate > start_coordinate
    first_covered = np.floor(start_coordinate).astype(np.int64).clip(0, len(inputs.bar_ns) - 1)
    last_covered = (
        np.ceil(end_coordinate).astype(np.int64) - 1
    ).clip(0, len(inputs.bar_ns) - 1)
    sample_at = pd.to_datetime(sample_ns[1:], unit="ns", utc=True).tz_convert(TIMEZONE)
    previous_sample_at = pd.to_datetime(sample_ns[:-1], unit="ns", utc=True).tz_convert(TIMEZONE)
    current_minute = sample_at.hour * 60 + sample_at.minute
    previous_minute = previous_sample_at.hour * 60 + previous_sample_at.minute
    current_session = np.where(current_minute <= 690, 1, 2)
    previous_session = np.where(previous_minute <= 690, 1, 2)
    crosses_day = np.asarray(previous_sample_at.date != sample_at.date)
    crosses_session = (
        (previous_session != current_session)
        | (inputs.session_number[first_covered] != inputs.session_number[last_covered])
    )
    left_endpoint_index = np.floor(
        np.nextafter(start_coordinate, -np.inf)
    ).astype(np.int64).clip(0, len(inputs.bar_ns) - 1)
    crosses_contract = (
        (inputs.contract_code[first_covered] != inputs.contract_code[last_covered])
        | (inputs.contract_code[left_endpoint_index] != inputs.contract_code[last_covered])
    )
    same_source_session = (
        inputs.source_session_id[first_covered] == inputs.source_session_id[last_covered]
    )
    rv = inputs.rv_integral.integrate(start_coordinate, end_coordinate)
    bpv = inputs.bpv_integral.integrate(start_coordinate, end_coordinate)
    log_return = np.diff(sample_log_price)
    eligible = (
        has_interval
        & ~crosses_day
        & ~crosses_session
        & ~crosses_contract
        & same_source_session
        & np.isfinite(rv)
        & np.isfinite(bpv)
        & np.isfinite(log_return)
    )

    factor_index = previous_source_index.copy()
    zero_fraction = np.abs(previous_fraction) <= COORDINATE_TOLERANCE
    prior_factor_index = np.maximum(factor_index - 1, 0)
    use_previous_slot = (
        zero_fraction
        & (factor_index > 0)
        & (inputs.source_session_id[factor_index] == inputs.source_session_id[prior_factor_index])
    )
    factor_index[use_previous_slot] -= 1
    return pd.DataFrame(
        {
            "previous_sample_at": previous_sample_at,
            "sample_at": sample_at,
            "previous_sample_ns": sample_ns[:-1],
            "sample_ns": sample_ns[1:],
            "log_return": log_return,
            "rv": rv,
            "bpv": bpv,
            "eligible": eligible,
            "factor_index": factor_index,
            "trading_duration_minutes": end_coordinate - start_coordinate,
        }
    )


def build_reanchored_event_frames(
    inputs: FeatureInputs,
    endpoint_index: int,
) -> dict[tuple[str, int], pd.DataFrame]:
    endpoint_at = pd.Timestamp(inputs.source_df["bar_at"].iloc[endpoint_index])
    history_start_at = endpoint_at - pd.DateOffset(months=12)
    history_start_index = int(np.searchsorted(inputs.bar_ns, history_start_at.value, side="left"))
    directed_source_index = np.arange(history_start_index, endpoint_index + 1, dtype=np.int64)[::-1]
    if len(directed_source_index) < max(SUPPORTED_GRANULARITIES):
        raise ValueError("Endpoint has insufficient source history")

    directed_increment_by_clock = {
        "time": np.ones(len(directed_source_index), dtype=np.float64),
        "volume": inputs.activity_volume[directed_source_index],
        "money": inputs.activity_money[directed_source_index],
    }
    directed_cumulative_by_clock = {
        clock: np.cumsum(increment)
        for clock, increment in directed_increment_by_clock.items()
    }
    total_history_minutes = endpoint_index + 1
    event_frames: dict[tuple[str, int], pd.DataFrame] = {}
    for clock in ["time", "volume", "money"]:
        full_history_total = {
            "time": float(total_history_minutes),
            "volume": float(inputs.volume_prefix[endpoint_index + 1]),
            "money": float(inputs.money_prefix[endpoint_index + 1]),
        }[clock]
        for equivalent_minutes in SUPPORTED_GRANULARITIES:
            training_interval_count = total_history_minutes // equivalent_minutes
            if training_interval_count < 1:
                raise ValueError("Endpoint has insufficient calibration history")
            sampling_unit = (
                float(equivalent_minutes)
                if clock == "time"
                else full_history_total / training_interval_count
            )
            source_index, minute_fraction = _locate_backward_sampling_points(
                directed_source_index,
                directed_increment_by_clock[clock],
                directed_cumulative_by_clock[clock],
                sampling_unit,
            )
            event_frames[(clock, equivalent_minutes)] = _build_event_frame(
                inputs, source_index, minute_fraction
            )
    return event_frames


def har_next_features(values: np.ndarray, equivalent_minutes: int) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    if not np.isfinite(values).all():
        raise ValueError("HAR feature history must be finite")
    features = np.full((len(values), 3), np.nan)
    cumulative = np.r_[0.0, np.cumsum(values)]
    widths = (1, int(np.ceil(60 / equivalent_minutes)), int(np.ceil(240 / equivalent_minutes)))
    for column, width in enumerate(widths):
        if len(values) >= width:
            features[width - 1 :, column] = (
                cumulative[width:] - cumulative[:-width]
            ) / width
    return features


def fit_har_model(features: np.ndarray, targets: np.ndarray) -> dict[str, object]:
    features = np.asarray(features, dtype=np.float64)
    targets = np.asarray(targets, dtype=np.float64)
    if features.shape != (len(targets), 3):
        raise ValueError("HAR requires exactly three features")
    if not np.isfinite(features).all() or not np.isfinite(targets).all() or np.any(targets < 0):
        raise ValueError("HAR inputs must be finite and targets nonnegative")
    if not np.any(targets > 0):
        raise ValueError("HAR targets are all zero")
    feature_scales = np.mean(features, axis=0)
    feature_scales = np.where(feature_scales > 0, feature_scales, 1.0)
    target_scale = float(np.mean(targets[targets > 0]))
    design = np.column_stack((np.ones(len(targets)), features / feature_scales))
    coefficients, residual_norm = nnls(design, targets / target_scale, maxiter=1000)
    return {
        "model": "HAR",
        "coefficients": coefficients.tolist(),
        "feature_scales": feature_scales.tolist(),
        "target_scale": target_scale,
        "status": "success",
        "objective": float(residual_norm**2 / len(targets)),
    }


def predict_har_model(payload: dict[str, object], features: np.ndarray) -> float:
    features = np.asarray(features, dtype=np.float64)
    design = np.r_[1.0, features / np.asarray(payload["feature_scales"])]
    forecast = (
        float(design @ np.asarray(payload["coefficients"]))
        * float(payload["target_scale"])
    )
    return max(forecast, float(payload["target_scale"]) * 1e-12)


def fit_native_model(model: str, values: np.ndarray) -> dict[str, object]:
    values = np.asarray(values, dtype=np.float64)
    if values.ndim != 1 or len(values) < 100 or not np.isfinite(values).all():
        raise ValueError("Native fit requires at least 100 finite observations")
    if model in ("GARCH", "FIGARCH"):
        numeric_scale = float(np.sqrt(np.mean(values**2)))
        if numeric_scale <= 0:
            raise ValueError("Return history is all zero")
        scaled_values = values / numeric_scale
        volatility = (
            GARCH(p=1, o=0, q=1)
            if model == "GARCH"
            else FIGARCH(p=1, q=1, truncation=MODEL_MEMORY)
        )
        arch_model = ZeroMean(
            scaled_values,
            volatility=volatility,
            distribution=Normal(),
            rescale=False,
        )
        with warnings.catch_warnings(record=True) as fit_warnings:
            warnings.simplefilter("always")
            fitted_model = arch_model.fit(
                disp="off",
                show_warning=False,
                options={"maxiter": 300, "ftol": 1e-8},
            )
        optimization = fitted_model.optimization_result
        parameters = {str(key): float(value) for key, value in fitted_model.params.items()}
        success = int(fitted_model.convergence_flag) == 0 and np.isfinite(
            list(parameters.values())
        ).all()
        return {
            "model": model,
            "params": parameters,
            "numeric_scale": numeric_scale,
            "memory": MODEL_MEMORY,
            "status": "success" if success else "failed",
            "message": str(optimization.message)
            + (
                "; " + "; ".join(str(warning.message) for warning in fit_warnings)
                if fit_warnings
                else ""
            ),
        }

    if model not in ("ARFIMA-logRV", "ARFIMA-logBPV"):
        raise ValueError(f"Unknown native model: {model}")
    if np.any(values < 0) or not np.any(values > 0):
        raise ValueError("ARFIMA proxy values must be nonnegative with at least one positive value")
    log_offset = float(1e-6 * np.mean(values[values > 0]))
    log_values = np.log(values + log_offset)
    log_mean = float(np.mean(log_values))
    centered_values = log_values - log_mean
    frequencies = 2 * np.pi * np.fft.rfftfreq(len(values))[1:]
    periodogram = np.abs(np.fft.rfft(centered_values)[1:]) ** 2 / len(values)
    if len(values) % 2 == 0:
        frequencies = frequencies[:-1]
        periodogram = periodogram[:-1]
    cosine = np.cos(frequencies)
    fractional_log = np.log(2 * np.sin(frequencies / 2))
    if np.mean(periodogram) <= 0:
        raise ValueError("ARFIMA periodogram is degenerate")

    def whittle_objective(parameters: np.ndarray) -> tuple[float, np.ndarray]:
        phi, theta, fractional_d = parameters
        numerator = 1 + theta**2 + 2 * theta * cosine
        denominator = 1 + phi**2 - 2 * phi * cosine
        log_spectrum = (
            np.log(numerator)
            - np.log(denominator)
            - 2 * fractional_d * fractional_log
        )
        weighted_periodogram = periodogram * np.exp(-log_spectrum)
        average_ratio = np.mean(weighted_periodogram)
        objective = float(np.mean(log_spectrum) + np.log(average_ratio))
        derivatives = np.column_stack(
            (
                (2 * cosine - 2 * phi) / denominator,
                (2 * theta + 2 * cosine) / numerator,
                -2 * fractional_log,
            )
        )
        gradient = np.mean(derivatives, axis=0) - (
            weighted_periodogram[:, None] * derivatives
        ).sum(axis=0) / weighted_periodogram.sum()
        return objective, gradient

    optimization = minimize(
        whittle_objective,
        [0.2, 0.05, 0.2],
        jac=True,
        method="L-BFGS-B",
        bounds=[(-0.98, 0.98), (-0.98, 0.98), (-0.45, 0.49)],
        options={"maxiter": 300, "ftol": 1e-10, "gtol": 1e-7},
    )
    phi, theta, fractional_d = map(float, optimization.x)
    return {
        "model": model,
        "params": {"phi": phi, "theta": theta, "d": fractional_d},
        "numeric_scale": 1.0,
        "log_offset": log_offset,
        "log_mean": log_mean,
        "training_n": len(values),
        "memory": MODEL_MEMORY,
        "status": "success" if optimization.success else "failed",
        "message": str(optimization.message),
    }


def restore_native_state(
    model: str,
    payload: dict[str, object],
    values: np.ndarray,
) -> dict[str, object]:
    values = np.asarray(values, dtype=np.float64)
    parameters = payload["params"]
    state: dict[str, object] = {"model": model, "payload": payload}
    if model == "GARCH":
        scaled_values = values / float(payload["numeric_scale"])
        volatility = GARCH(p=1, o=0, q=1)
        parameter_array = np.array(
            [parameters["omega"], parameters["alpha[1]"], parameters["beta[1]"]]
        )
        variances = np.empty(len(values))
        volatility.compute_variance(
            parameter_array,
            scaled_values,
            variances,
            volatility.backcast(scaled_values),
            volatility.variance_bounds(scaled_values),
        )
        state.update(
            last_squared=float(scaled_values[-1] ** 2),
            last_variance=float(variances[-1]),
        )
    elif model == "FIGARCH":
        scaled_values = values / float(payload["numeric_scale"])
        weights = np.asarray(
            figarch_weights(
                np.array([parameters["phi"], parameters["d"], parameters["beta"]]),
                1,
                1,
                int(payload["memory"]),
            )
        )
        volatility = FIGARCH(p=1, q=1, truncation=int(payload["memory"]))
        history = np.full(int(payload["memory"]), float(volatility.backcast(scaled_values)))
        count = min(len(values), len(history))
        history[-count:] = scaled_values[-count:] ** 2
        state.update(
            weights=weights,
            squared_history=history,
            omega_tilde=parameters["omega"] / (1 - parameters["beta"]),
        )
    else:
        centered_values = (
            np.log(values + float(payload["log_offset"])) - float(payload["log_mean"])
        )
        memory = int(payload["memory"])
        fractional_weights = np.empty(memory + 1)
        fractional_weights[0] = 1.0
        for lag in range(1, memory + 1):
            fractional_weights[lag] = (
                fractional_weights[lag - 1]
                * (lag - 1 - parameters["d"])
                / lag
            )
        innovation_weights = lfilter(
            [1.0, -parameters["phi"]],
            [1.0, parameters["theta"]],
            fractional_weights,
        )
        innovations = lfilter(innovation_weights, [1.0], centered_values)
        burn = min(memory, max(25, int(payload.get("training_n", len(values))) // 5))
        innovation_sample = innovations[burn : int(payload.get("training_n", len(values)))]
        innovation_sample = innovation_sample - np.mean(innovation_sample)
        history = np.zeros(memory)
        count = min(memory, len(values))
        history[-count:] = centered_values[-count:]
        state.update(
            innovation_weights=innovation_weights,
            log_history=history,
            innovation_sample=innovation_sample,
        )
    return state


def forecast_native(state: dict[str, object]) -> float:
    model = state["model"]
    payload = state["payload"]
    parameters = payload["params"]
    if model == "GARCH":
        forecast = (
            parameters["omega"]
            + parameters["alpha[1]"] * state["last_squared"]
            + parameters["beta[1]"] * state["last_variance"]
        ) * float(payload["numeric_scale"]) ** 2
    elif model == "FIGARCH":
        forecast = (
            state["omega_tilde"]
            + np.dot(state["weights"], state["squared_history"][::-1])
        ) * float(payload["numeric_scale"]) ** 2
    else:
        log_forecast = float(
            -np.dot(state["innovation_weights"][1:], state["log_history"][::-1])
        )
        innovation_sample = state["innovation_sample"]
        log_smearing = float(logsumexp(innovation_sample) - np.log(len(innovation_sample)))
        exponent = float(payload["log_mean"]) + log_forecast + log_smearing
        if exponent > 700:
            raise FloatingPointError("ARFIMA level restoration overflow")
        forecast = max(
            float(np.exp(exponent) - float(payload["log_offset"])),
            np.finfo(float).tiny,
        )
    if not np.isfinite(forecast) or forecast <= 0:
        raise FloatingPointError("Model produced a nonpositive or nonfinite forecast")
    return float(forecast)


def _prepare_proxy_series(
    inputs: FeatureInputs,
    events: pd.DataFrame,
    equivalent_minutes: int,
    seasonality_lookback: int,
    target: str,
) -> PreparedSeries:
    factors = (
        np.ones(len(events), dtype=np.float64)
        if seasonality_lookback == 0
        else inputs.seasonality[seasonality_lookback][
            events["factor_index"].to_numpy(dtype=np.int64)
        ]
    )
    proxy = events[target].to_numpy(dtype=np.float64)
    valid = (
        events["eligible"].to_numpy(dtype=bool)
        & np.isfinite(factors)
        & (factors > 0)
        & np.isfinite(proxy)
        & (proxy >= 0)
    )
    original_positions = np.flatnonzero(valid)
    values = proxy[valid] / factors[valid]
    previous_sample_ns = events["previous_sample_ns"].to_numpy(dtype=np.int64)[valid]
    sample_ns = events["sample_ns"].to_numpy(dtype=np.int64)[valid]
    features = har_next_features(values, equivalent_minutes)
    next_targets = np.r_[values[1:], np.nan]
    next_target_ns = np.r_[sample_ns[1:], np.iinfo(np.int64).min]
    adjacent_pair = np.r_[np.diff(original_positions) == 1, False]
    longest_width = int(np.ceil(240 / equivalent_minutes))
    next_feature_start_ns = np.full(len(values), np.iinfo(np.int64).min, dtype=np.int64)
    if len(values) >= longest_width:
        next_feature_start_ns[longest_width - 1 :] = previous_sample_ns[
            : len(values) - longest_width + 1
        ]
    return PreparedSeries(
        values=values,
        previous_sample_ns=previous_sample_ns,
        sample_ns=sample_ns,
        features=features,
        next_targets=next_targets,
        next_target_ns=next_target_ns,
        next_feature_start_ns=next_feature_start_ns,
        adjacent_pair=adjacent_pair,
    )


def compute_endpoint_features(
    inputs: FeatureInputs,
    endpoint_index: int,
) -> tuple[dict[str, float], dict[str, str], float]:
    started = time.perf_counter()
    endpoint_at = pd.Timestamp(inputs.source_df["bar_at"].iloc[endpoint_index])
    endpoint_ns = endpoint_at.value
    event_frames = build_reanchored_event_frames(inputs, endpoint_index)
    series_cache: dict[tuple[str, int, int, str], PreparedSeries] = {}
    features: dict[str, float] = {}
    failures: dict[str, str] = {}

    for spec in FEATURE_SPECS:
        try:
            events = event_frames[(spec.clock, spec.equivalent_minutes)]
            window_start_ns = (endpoint_at - pd.DateOffset(months=spec.fit_window_months)).value
            forecast_factor = (
                1.0
                if spec.seasonality_lookback == 0
                else float(inputs.seasonality[spec.seasonality_lookback][endpoint_index])
            )
            if not np.isfinite(forecast_factor) or forecast_factor <= 0:
                raise ValueError("Forecast-origin seasonal factor is unavailable")

            if spec.model in ("GARCH", "FIGARCH"):
                event_factors = (
                    np.ones(len(events), dtype=np.float64)
                    if spec.seasonality_lookback == 0
                    else inputs.seasonality[spec.seasonality_lookback][
                        events["factor_index"].to_numpy(dtype=np.int64)
                    ]
                )
                observed_returns = events["log_return"].to_numpy(dtype=np.float64)
                mask = (
                    events["eligible"].to_numpy(dtype=bool)
                    & np.isfinite(event_factors)
                    & (event_factors > 0)
                    & np.isfinite(observed_returns)
                    & (events["previous_sample_ns"].to_numpy(dtype=np.int64) >= window_start_ns)
                    & (events["sample_ns"].to_numpy(dtype=np.int64) <= endpoint_ns)
                )
                training_values = observed_returns[mask] / np.sqrt(event_factors[mask])
                if len(training_values) < MIN_NATIVE_OBSERVATIONS:
                    raise ValueError(
                        f"Effective observations {len(training_values)} < {MIN_NATIVE_OBSERVATIONS}"
                    )
                payload = fit_native_model(spec.model, training_values)
                if payload["status"] != "success":
                    raise RuntimeError(f"Optimizer failed: {payload['message']}")
                prediction = (
                    forecast_native(restore_native_state(spec.model, payload, training_values))
                    * forecast_factor
                )
            else:
                series_key = (
                    spec.clock,
                    spec.equivalent_minutes,
                    spec.seasonality_lookback,
                    spec.target,
                )
                if series_key not in series_cache:
                    series_cache[series_key] = _prepare_proxy_series(
                        inputs,
                        events,
                        spec.equivalent_minutes,
                        spec.seasonality_lookback,
                        spec.target,
                    )
                series = series_cache[series_key]
                if spec.model == "HAR":
                    mask = (
                        series.adjacent_pair
                        & np.isfinite(series.features).all(axis=1)
                        & np.isfinite(series.next_targets)
                        & (series.next_feature_start_ns >= window_start_ns)
                        & (series.next_target_ns <= endpoint_ns)
                    )
                    training_features = series.features[mask]
                    training_values = series.next_targets[mask]
                    if len(training_values) < MIN_HAR_OBSERVATIONS:
                        raise ValueError(
                            f"Effective observations {len(training_values)} < {MIN_HAR_OBSERVATIONS}"
                        )
                    if len(series.features) == 0 or not np.isfinite(series.features[-1]).all():
                        raise ValueError("HAR forecast history is incomplete")
                    payload = fit_har_model(training_features, training_values)
                    prediction = predict_har_model(payload, series.features[-1]) * forecast_factor
                else:
                    mask = (
                        (series.previous_sample_ns >= window_start_ns)
                        & (series.sample_ns <= endpoint_ns)
                    )
                    training_values = series.values[mask]
                    if len(training_values) < MIN_NATIVE_OBSERVATIONS:
                        raise ValueError(
                            f"Effective observations {len(training_values)} < {MIN_NATIVE_OBSERVATIONS}"
                        )
                    payload = fit_native_model(spec.model, training_values)
                    if payload["status"] != "success":
                        raise RuntimeError(f"Optimizer failed: {payload['message']}")
                    prediction = (
                        forecast_native(restore_native_state(spec.model, payload, training_values))
                        * forecast_factor
                    )

            if not np.isfinite(prediction) or prediction <= 0:
                raise FloatingPointError("Final feature is nonpositive or nonfinite")
            features[spec.name] = float(prediction)
        except (ValueError, FloatingPointError, RuntimeError, np.linalg.LinAlgError) as exception:
            features[spec.name] = np.nan
            failures[spec.name] = f"{type(exception).__name__}: {exception}"[:500]
    return features, failures, time.perf_counter() - started


def _feature_manifest_json() -> str:
    return json.dumps(
        [dataclasses.asdict(spec) for spec in FEATURE_SPECS],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def output_schema(inputs: FeatureInputs) -> pa.Schema:
    fields = [
        pa.field("as_of_at", pa.timestamp("us", tz=TIMEZONE), False),
        pa.field("trading_date", pa.date32(), False),
        pa.field("contract_code", pa.string(), False),
        pa.field("session_number", pa.int8(), False),
        pa.field("source_minute_index", pa.int64(), False),
        *[pa.field(spec.name, pa.float64()) for spec in FEATURE_SPECS],
        pa.field("valid_feature_count", pa.int16(), False),
        pa.field("feature_failures_json", pa.string(), False),
        pa.field("compute_seconds", pa.float64(), False),
    ]
    metadata = {
        b"table_name": b"im_xgb_40_volatility_features",
        b"feature_set_version": FEATURE_SET_VERSION.encode(),
        b"primary_key": b"as_of_at",
        b"feature_count": b"40",
        b"feature_manifest": _feature_manifest_json().encode(),
        b"input_snapshot_id": inputs.input_snapshot_id.encode(),
        b"event_price_column": inputs.event_price_column.encode(),
        b"forecast_horizon": b"one next event on each feature's own clock",
        b"anchor_policy": b"re-anchor backward at every source-minute close",
        b"clock_calibration": b"all source history through endpoint; map history limited to preceding 12 calendar months",
        b"seasonality_lookbacks": b"0,60,120; 30 and 360 excluded",
        b"har_horizon": b"next_step only",
        b"availability": b"usable after as_of_at minute close and source input availability checks",
    }
    return pa.schema(fields, metadata=metadata)


def _write_output_table(
    output_path: pathlib.Path,
    records: list[dict[str, object]],
    schema: pa.Schema,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_table = pa.Table.from_pylist(records, schema=schema)
    temporary_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        with temporary_path.open("wb") as output_file:
            pq.write_table(
                output_table,
                output_file,
                compression="zstd",
                row_group_size=max(1, len(records)),
            )
            output_file.flush()
            os.fsync(output_file.fileno())
        written_schema = pq.read_schema(temporary_path)
        written_metadata = pq.read_metadata(temporary_path)
        if not written_schema.equals(schema, check_metadata=True):
            raise RuntimeError("Written output schema or metadata did not round-trip")
        if written_metadata.num_rows != len(records):
            raise RuntimeError("Written output row count did not round-trip")
        os.replace(temporary_path, output_path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _existing_output_is_complete(
    output_path: pathlib.Path,
    expected_rows: int,
    schema: pa.Schema,
) -> bool:
    if not output_path.exists():
        return False
    parquet_file = pq.ParquetFile(output_path)
    if not parquet_file.schema_arrow.equals(schema, check_metadata=True):
        raise RuntimeError(f"Existing output has another contract: {output_path}")
    if parquet_file.metadata.num_rows != expected_rows:
        raise RuntimeError(f"Existing output is incomplete: {output_path}")
    return True


def _record_for_endpoint(
    inputs: FeatureInputs,
    endpoint_index: int,
    require_all_features: bool,
) -> dict[str, object]:
    feature_values, failures, compute_seconds = compute_endpoint_features(
        inputs, endpoint_index
    )
    valid_feature_count = sum(np.isfinite(value) for value in feature_values.values())
    if require_all_features and valid_feature_count != len(FEATURE_SPECS):
        raise RuntimeError(
            f"Endpoint {inputs.source_df['bar_at'].iloc[endpoint_index]} produced "
            f"{valid_feature_count}/40 features: {json.dumps(failures, ensure_ascii=False)}"
        )
    return {
        "as_of_at": inputs.source_df["bar_at"].iloc[endpoint_index],
        "trading_date": inputs.source_df["trading_date"].iloc[endpoint_index],
        "contract_code": inputs.contract_code[endpoint_index],
        "session_number": int(inputs.session_number[endpoint_index]),
        "source_minute_index": int(endpoint_index),
        **feature_values,
        "valid_feature_count": int(valid_feature_count),
        "feature_failures_json": json.dumps(
            failures, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
        "compute_seconds": float(compute_seconds),
    }


def _parse_arguments() -> argparse.Namespace:
    script_dir = pathlib.Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description="Refit and generate the frozen 40 IM next-event volatility features"
    )
    parser.add_argument(
        "--minute-input",
        type=pathlib.Path,
        default=script_dir / "im_main_continuous_1m.parquet",
    )
    activity_group = parser.add_mutually_exclusive_group()
    activity_group.add_argument("--activity-input", type=pathlib.Path)
    activity_group.add_argument("--activity-duckdb", type=pathlib.Path)
    parser.add_argument(
        "--seasonality-60-input",
        type=pathlib.Path,
        default=script_dir / "im_bpv_seasonality_1m_60td.parquet",
    )
    parser.add_argument(
        "--seasonality-120-input",
        type=pathlib.Path,
        default=script_dir / "im_bpv_seasonality_1m_120td.parquet",
    )
    parser.add_argument("--output-dir", type=pathlib.Path, required=True)
    parser.add_argument("--start-date", type=str)
    parser.add_argument("--end-date", type=str)
    parser.add_argument(
        "--endpoint-at",
        type=str,
        help="Compute exactly one Asia/Shanghai minute close instead of a date range",
    )
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--require-all-features", action="store_true")
    parser.add_argument("--progress-every", type=int, default=20)
    parser.add_argument(
        "--event-price-column",
        choices=["bidirectional_adjusted_close", "close"],
        default="bidirectional_adjusted_close",
    )
    parser.add_argument(
        "--allow-retrospective-adjustment",
        action="store_true",
        help="Acknowledge historical use of an adjusted-price snapshot unavailable at as_of_at",
    )
    arguments = parser.parse_args()
    if arguments.activity_input is None and arguments.activity_duckdb is None:
        local_duckdb = (
            script_dir.parent
            / "financial_futures_data"
            / "data"
            / "warehouse"
            / "financial_futures.duckdb"
        )
        arguments.activity_duckdb = local_duckdb
    if arguments.endpoint_at:
        if arguments.start_date or arguments.end_date:
            parser.error("--endpoint-at cannot be combined with --start-date/--end-date")
        if arguments.shard_count != 1 or arguments.shard_index != 0:
            parser.error("--endpoint-at does not use date sharding")
    elif not arguments.start_date or not arguments.end_date:
        parser.error("Provide both --start-date and --end-date, or provide --endpoint-at")
    if arguments.shard_count < 1 or not 0 <= arguments.shard_index < arguments.shard_count:
        parser.error("Require 0 <= shard-index < shard-count")
    if arguments.progress_every < 1:
        parser.error("--progress-every must be positive")
    return arguments


def main() -> None:
    arguments = _parse_arguments()
    load_started = time.perf_counter()
    inputs = load_feature_inputs(
        minute_input=arguments.minute_input.resolve(),
        activity_input=(
            arguments.activity_input.resolve() if arguments.activity_input else None
        ),
        activity_duckdb=(
            arguments.activity_duckdb.resolve() if arguments.activity_duckdb else None
        ),
        seasonality_60_input=arguments.seasonality_60_input.resolve(),
        seasonality_120_input=arguments.seasonality_120_input.resolve(),
        event_price_column=arguments.event_price_column,
    )
    schema = output_schema(inputs)
    output_dir = arguments.output_dir.resolve()
    print(
        f"loaded {len(inputs.source_df):,} source minutes in "
        f"{time.perf_counter() - load_started:.2f}s; snapshot={inputs.input_snapshot_id}",
        flush=True,
    )

    if arguments.endpoint_at:
        endpoint_at = pd.Timestamp(arguments.endpoint_at)
        endpoint_at = (
            endpoint_at.tz_localize(TIMEZONE)
            if endpoint_at.tzinfo is None
            else endpoint_at.tz_convert(TIMEZONE)
        )
        matches = np.flatnonzero(inputs.bar_ns == endpoint_at.value)
        if len(matches) != 1:
            raise ValueError(f"--endpoint-at must match exactly one source bar: {endpoint_at}")
        endpoint_indices_by_date = {inputs.trading_date[matches[0]]: matches}
        point_mode = True
    else:
        start_date = pd.Timestamp(arguments.start_date).date()
        end_date = pd.Timestamp(arguments.end_date).date()
        if end_date < start_date:
            raise ValueError("--end-date precedes --start-date")
        available_dates = sorted(
            date
            for date in np.unique(inputs.trading_date)
            if start_date <= date <= end_date
        )
        # Calendar ordinals keep a date on the same shard when the requested
        # start/end range changes.  Only changing shard_count remaps ownership.
        selected_dates = [
            date
            for date in available_dates
            if date.toordinal() % arguments.shard_count == arguments.shard_index
        ]
        if not selected_dates:
            raise ValueError("This shard has no source trading dates in the requested range")
        endpoint_indices_by_date = {
            date: np.flatnonzero(inputs.trading_date == date) for date in selected_dates
        }
        point_mode = False

    selected_endpoint_indices = np.concatenate(list(endpoint_indices_by_date.values()))
    if arguments.event_price_column == "bidirectional_adjusted_close":
        availability_ns = pd.DatetimeIndex(
            inputs.source_df["price_adjustment_available_at"]
        ).as_unit("ns").asi8
        retrospective = availability_ns[selected_endpoint_indices] > inputs.bar_ns[
            selected_endpoint_indices
        ]
        if np.any(retrospective) and not arguments.allow_retrospective_adjustment:
            raise RuntimeError(
                "The selected bidirectional adjusted-price snapshot was unavailable at some "
                "historical endpoints. Re-run with --allow-retrospective-adjustment to reproduce "
                "the experiment, or use --event-price-column close for an as-of-safe variant."
            )

    total_started = time.perf_counter()
    completed_endpoints = 0
    for trading_date, endpoint_indices in endpoint_indices_by_date.items():
        date_text = str(trading_date)
        if point_mode:
            endpoint_at = pd.Timestamp(inputs.source_df["bar_at"].iloc[endpoint_indices[0]])
            filename = f"point-{endpoint_at.strftime('%H%M%S')}.parquet"
        else:
            # A complete trading day has one canonical file regardless of which
            # PAI shard owns it.  Changing shard_count therefore cannot create
            # duplicate parts for an already published date.
            filename = "features.parquet"
        # Partition field uses a distinct name so a dataset reader does not try
        # to merge its string partition value with the date32 trading_date column.
        output_path = output_dir / f"trading_day={date_text}" / filename
        if output_path.exists():
            if arguments.resume and _existing_output_is_complete(
                output_path, len(endpoint_indices), schema
            ):
                print(f"skip complete output: {output_path}", flush=True)
                completed_endpoints += len(endpoint_indices)
                continue
            raise FileExistsError(f"Output already exists: {output_path}")

        records = []
        day_started = time.perf_counter()
        for within_day_position, endpoint_index in enumerate(endpoint_indices, start=1):
            record = _record_for_endpoint(
                inputs,
                int(endpoint_index),
                arguments.require_all_features,
            )
            records.append(record)
            completed_endpoints += 1
            if (
                within_day_position % arguments.progress_every == 0
                or within_day_position == len(endpoint_indices)
            ):
                print(
                    f"{date_text} {within_day_position}/{len(endpoint_indices)}; "
                    f"latest={record['valid_feature_count']}/40; "
                    f"endpoint={record['compute_seconds']:.2f}s",
                    flush=True,
                )
        _write_output_table(output_path, records, schema)
        print(
            f"committed {output_path}; rows={len(records)}; "
            f"day_seconds={time.perf_counter() - day_started:.2f}",
            flush=True,
        )

    print(
        f"complete; endpoints={completed_endpoints}; "
        f"total_seconds={time.perf_counter() - total_started:.2f}",
        flush=True,
    )


if __name__ == "__main__":
    main()
