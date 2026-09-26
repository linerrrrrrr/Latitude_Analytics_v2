"""Periodicity estimators shared by empirical and known-truth experiments.

The WSD core follows Boudt et al. equations 2.9-2.12. Daily scale normalization,
rolling windows, two-session Fourier smoothing and state conditioning are explicit
adaptations owned by this project. Degenerate scales are returned as NaN, not filled.
"""

import numpy as np


def estimate_variance_factor(standardized_returns, method, fourier_order=3):
    """Estimate a mean-one variance curve from complete historical signed returns.

    Input shape: historical days x within-day slots. No future day is supplied.
    Invalid or degenerate training samples produce an all-NaN curve.
    """
    standardized_returns = np.asarray(standardized_returns, dtype=np.float64)
    if standardized_returns.ndim != 2 or standardized_returns.shape[0] < 2:
        raise ValueError("Expected at least two days of signed returns")
    day_count, slot_count = standardized_returns.shape
    missing_factor = np.full(slot_count, np.nan)
    if not np.isfinite(standardized_returns).all():
        return missing_factor

    if method == "mean_rv":
        variance_factor = np.mean(standardized_returns**2, axis=0)
    elif method == "median_rv":
        variance_factor = np.median(standardized_returns**2, axis=0)
    elif method in ("boudt_wsd", "fff_wsd"):
        sorted_returns = np.sort(standardized_returns, axis=0)
        half_count = day_count // 2 + 1
        shortest_width = np.min(
            sorted_returns[half_count - 1:] - sorted_returns[:day_count - half_count + 1],
            axis=0,
        )
        shortest_half_scale = 0.741 * shortest_width
        if np.any(shortest_half_scale <= 0):
            return missing_factor
        shortest_half_factor = shortest_half_scale / np.sqrt(
            np.mean(shortest_half_scale**2)
        )
        retained_weights = (standardized_returns / shortest_half_factor)**2 <= 6.635
        retained_count = retained_weights.sum(axis=0)
        if np.any(retained_count == 0):
            return missing_factor
        variance_factor = (
            1.081 * np.sum(retained_weights * standardized_returns**2, axis=0)
            / retained_count
        )
        if method == "fff_wsd":
            if slot_count % 2:
                raise ValueError("Two-session smoothing requires an even slot count")
            # This is a project adaptation, not AB1997's original FFF regression.
            # Separate intercepts preserve relative session levels; opening dummies
            # and a linear term prevent a forced close-to-open periodic join.
            smoothed_factor = np.empty(slot_count)
            for session_slots in np.split(np.arange(slot_count), 2):
                session_time = np.linspace(0.0, 1.0, len(session_slots))
                design_columns = [np.ones(len(session_slots)), session_time]
                for harmonic in range(1, fourier_order + 1):
                    design_columns.extend([
                        np.sin(2 * np.pi * harmonic * session_time),
                        np.cos(2 * np.pi * harmonic * session_time),
                    ])
                design_columns.extend([
                    (np.arange(len(session_slots)) == index).astype(float)
                    for index in (0, 1, len(session_slots) - 1)
                ])
                fourier_design = np.column_stack(design_columns)
                coefficients = np.linalg.lstsq(
                    fourier_design, np.log(variance_factor[session_slots]), rcond=None
                )[0]
                smoothed_factor[session_slots] = np.exp(fourier_design @ coefficients)
            variance_factor = smoothed_factor
    else:
        raise ValueError(f"Unknown variance estimator: {method}")

    if not np.isfinite(variance_factor).all() or np.any(variance_factor <= 0):
        return missing_factor
    return variance_factor / np.mean(variance_factor)
