#!/usr/bin/env python3
"""Minimal, dependency-free reproductions of seasonal Huber failure modes.

This script mirrors the relevant statsmodels estimate_location iteration and
c01_IntradaySeasonality wrapper decisions. Inputs are finite observed samples;
no missing observations are filled or silently dropped here.
"""
from __future__ import annotations

import math
from statistics import median

MAD_NORMALIZER = 1.482602218505602
HUBER_T = 1.345
HUBER_TOL = 1e-6
HUBER_MAXITER = 30


def require_finite(values: list[float]) -> list[float]:
    if not values:
        raise ValueError("sample must contain at least one observed value")
    if not all(math.isfinite(x) for x in values):
        raise ValueError("sample must contain finite observed values; no imputation is applied")
    return [float(x) for x in values]


def standardized_mad(values: list[float]) -> float:
    x = require_finite(values)
    center = median(x)
    return MAD_NORMALIZER * median([abs(v - center) for v in x])


def huber_location(values: list[float], scale: float, *, maxiter: int = HUBER_MAXITER) -> tuple[float, int]:
    """Statsmodels-style Huber IRLS, including its default median start."""
    x = require_finite(values)
    if not math.isfinite(scale) or scale <= 0:
        raise ValueError("scale must be positive and finite")
    mu = median(x)
    for iteration in range(1, maxiter + 1):
        weights = []
        for value in x:
            z = (value - mu) / scale
            az = abs(z)
            weights.append(1.0 if az <= HUBER_T else HUBER_T / az)
        denominator = sum(weights)
        if denominator <= 0:
            return mu, iteration
        new_mu = sum(w * value for w, value in zip(weights, x)) / denominator
        if abs(mu - new_mu) < scale * HUBER_TOL:
            return new_mu, iteration
        mu = new_mu
    raise ValueError(f"location estimator failed to converge in {maxiter} iterations")


def current_wrapper(values: list[float]) -> tuple[float | None, str]:
    """Relevant current wrapper behavior from c01_IntradaySeasonality."""
    x = require_finite(values)
    if all(value == x[0] for value in x):
        return x[0], "ok"
    scale = standardized_mad(x)
    if not math.isfinite(scale) or scale <= 0:
        return None, "zero_scale"
    try:
        estimate, _ = huber_location(x, scale)
    except ValueError:
        return None, "not_converged"
    return estimate, "ok"


def trimmed_mean(values: list[float], alpha: float = 0.20) -> tuple[float, int]:
    """Symmetric alpha-trimmed arithmetic mean; k=floor(alpha*n) each tail."""
    x = sorted(require_finite(values))
    if not 0 <= alpha < 0.5:
        raise ValueError("alpha must be in [0, 0.5)")
    k = math.floor(alpha * len(x))
    kept = x[k : len(x) - k]
    return sum(kept) / len(kept), k


def winsorized_mean(values: list[float], alpha: float = 0.20) -> tuple[float, int]:
    """Symmetric alpha-winsorized mean using the same finite-sample k rule."""
    x = sorted(require_finite(values))
    if not 0 <= alpha < 0.5:
        raise ValueError("alpha must be in [0, 0.5)")
    k = math.floor(alpha * len(x))
    if not k:
        return sum(x) / len(x), k
    low, high = x[k], x[len(x) - k - 1]
    capped = [min(high, max(low, value)) for value in x]
    return sum(capped) / len(capped), k


def huber_with_mean_deviation_scale(values: list[float]) -> tuple[float, int, float]:
    """Huber M location with mean absolute deviation about the arithmetic mean.

    This avoids a zero scale for nonconstant data, but the scale is non-robust;
    it is included as a tradeoff comparator, not as the recommended default.
    """
    x = require_finite(values)
    arithmetic_mean = sum(x) / len(x)
    scale = sum(abs(value - arithmetic_mean) for value in x) / len(x)
    if scale <= 0:
        return x[0], 0, scale
    mu = arithmetic_mean  # initialization only; final value solves the Huber equation
    for iteration in range(1, 10001):
        weights = []
        for value in x:
            z = (value - mu) / scale
            az = abs(z)
            weights.append(1.0 if az <= HUBER_T else HUBER_T / az)
        new_mu = sum(w * value for w, value in zip(weights, x)) / sum(weights)
        if abs(mu - new_mu) < scale * HUBER_TOL:
            return new_mu, iteration, scale
        mu = new_mu
    raise AssertionError("mean-deviation-scale Huber iteration unexpectedly failed")


def close(actual: float, expected: float, tol: float = 1e-10) -> None:
    assert abs(actual - expected) <= tol * max(1.0, abs(expected)), (actual, expected)


def main() -> None:
    zero_mad = [0, 0, 0, 0, 1]
    slow_huber = [0] * 6 + [100] * 2 + [1000] * 7

    assert current_wrapper([2, 2, 2, 2]) == (2.0, "ok")
    value, status = current_wrapper(zero_mad)
    assert value is None and status == "zero_scale"
    assert standardized_mad(zero_mad) == 0

    value, status = current_wrapper(slow_huber)
    assert value is None and status == "not_converged"
    scale = standardized_mad(slow_huber)
    assert scale > 0
    estimate_32, iterations = huber_location(slow_huber, scale, maxiter=32)
    assert iterations == 32

    try:
        current_wrapper([0, float("nan"), 1])
    except ValueError:
        pass
    else:
        raise AssertionError("non-finite values must not be silently filled or removed")

    alternatives = {
        "mean": (sum(zero_mad) / len(zero_mad), sum(slow_huber) / len(slow_huber)),
        "trimmed_20pct": (trimmed_mean(zero_mad)[0], trimmed_mean(slow_huber)[0]),
        "winsorized_20pct": (winsorized_mean(zero_mad)[0], winsorized_mean(slow_huber)[0]),
        "Huber_mean_deviation_scale": (
            huber_with_mean_deviation_scale(zero_mad)[0],
            huber_with_mean_deviation_scale(slow_huber)[0],
        ),
    }
    close(alternatives["mean"][0], 0.2)
    close(alternatives["trimmed_20pct"][0], 0.0)
    close(alternatives["winsorized_20pct"][0], 0.0)
    close(alternatives["Huber_mean_deviation_scale"][0], 0.10760001721184542)
    close(alternatives["trimmed_20pct"][1], 466.6666666666667)

    print("case | current status | arithmetic mean | 20% trimmed mean | 20% winsorized mean | Huber + mean-deviation scale")
    for name, sample in (("zero_MAD", zero_mad), ("30-iteration_limit", slow_huber)):
        current_value, current_status = current_wrapper(sample)
        mean_value = sum(sample) / len(sample)
        trimmed, _ = trimmed_mean(sample)
        winsorized, _ = winsorized_mean(sample)
        alt_huber, alt_iterations, alt_scale = huber_with_mean_deviation_scale(sample)
        print(f"{name} | {current_status} | {mean_value:.6f} | {trimmed:.6f} | {winsorized:.6f} | {alt_huber:.6f} (iter={alt_iterations}, scale={alt_scale:.6f})")
    print(f"slow-Huber: standardized_MAD={scale:.6f}; converges on iteration 32, two beyond the configured limit 30")
    print("PASS: both failure states reproduced; alternatives use observed values only and no imputation")


if __name__ == "__main__":
    main()
