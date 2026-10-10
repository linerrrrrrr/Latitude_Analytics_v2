"""Low-rate, read-only health audit for the project's public web data sources.

This tool never writes the futures lake and deliberately performs no
automatic business retry.  It records transport evidence, latency, response
shape, date coverage, nulls, and common block-page signals so that a HTTP 200
response cannot be mistaken for valid business data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import random
import re
import statistics
import sys
import time
import urllib.parse
import urllib.robotparser
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.futures_lakehouse.external_market_entities import EXTERNAL_INDEX_ENTITIES
from config.futures_lakehouse.macro_release_entities import MACRO_RELEASE_SERIES


EASTMONEY_ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
EASTMONEY_PAGE_SIZE = 500
SUNSIRS_URL_TEMPLATE = "https://www.100ppi.com/sf/day-{observation_date}.html"

SUNSIRS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.100ppi.com/sf/",
}
EASTMONEY_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Latitude-Analytics/1.0",
}

BLOCK_PAGE_TERMS = (
    "访问过于频繁",
    "访问频繁",
    "安全验证",
    "人机验证",
    "验证码",
    "captcha",
    "access denied",
    "request blocked",
    "temporarily blocked",
    "too many requests",
    "forbidden",
)

MACRO_SERIES = tuple(
    series
    for series in MACRO_RELEASE_SERIES
    if series.dataset_name == "macro_release"
)
_macro_series_by_report: dict[str, list[Any]] = defaultdict(list)
for configured_series in MACRO_SERIES:
    _macro_series_by_report[configured_series.source_api].append(configured_series)
MACRO_FIELDS_BY_REPORT = {
    report_name: [
        "REPORT_DATE",
        *[series.source_column for series in configured_series],
    ]
    for report_name, configured_series in _macro_series_by_report.items()
}
if any(
    len({series.frequency for series in configured_series}) != 1
    for configured_series in _macro_series_by_report.values()
):
    raise ValueError("同一 Eastmoney 宏观报告的共享频率必须唯一。")
MACRO_FREQUENCY_BY_REPORT = {
    report_name: next(iter({series.frequency for series in configured_series}))
    for report_name, configured_series in _macro_series_by_report.items()
}


def parse_iso_date(value: object) -> date | None:
    """Parse an Eastmoney date value without accepting ambiguous formats."""

    if not isinstance(value, str):
        return None
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2})(?:[ T].*)?", value.strip())
    if match is None:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def percentile(values: list[float], probability: float) -> float | None:
    """Return a linearly interpolated percentile for a small audit sample."""

    if not values:
        return None
    ordered_values = sorted(values)
    position = (len(ordered_values) - 1) * probability
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    if lower_index == upper_index:
        return ordered_values[lower_index]
    lower_weight = upper_index - position
    upper_weight = position - lower_index
    return (
        ordered_values[lower_index] * lower_weight
        + ordered_values[upper_index] * upper_weight
    )


def expected_source_dates(
    range_start: date,
    range_end: date,
    frequency: str | None,
) -> list[date]:
    """Build the bounded descriptive/theoretical date sequence for one probe."""

    if frequency is None or range_start > range_end:
        return []
    if frequency == "weekday_descriptive":
        expected_dates = []
        cursor = range_start
        while cursor <= range_end:
            if cursor.weekday() < 5:
                expected_dates.append(cursor)
            cursor += timedelta(days=1)
        return expected_dates
    if frequency not in {"month_end", "quarter_end"}:
        raise ValueError(f"未知探测频率：{frequency}")

    expected_dates = []
    cursor = date(range_start.year, range_start.month, 1)
    while cursor < range_start:
        cursor = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )
    while cursor <= range_end:
        if frequency == "month_end" or cursor.month in {3, 6, 9, 12}:
            expected_dates.append(cursor)
        cursor = (
            date(cursor.year + 1, 1, 1)
            if cursor.month == 12
            else date(cursor.year, cursor.month + 1, 1)
        )
    return expected_dates


def summarize_missing_dates(
    expected_dates: list[date],
    missing_dates: set[date],
    max_ranges: int = 20,
) -> dict[str, object]:
    """Compress missing dates along an expected sequence into bounded ranges."""

    ranges = []
    range_start = None
    range_end = None
    range_count = 0
    total_range_count = 0
    for expected_date in expected_dates:
        if expected_date in missing_dates:
            if range_start is None:
                range_start = expected_date
                range_count = 0
            range_end = expected_date
            range_count += 1
            continue
        if range_start is not None:
            total_range_count += 1
            if len(ranges) < max_ranges:
                ranges.append({
                    "start": range_start.isoformat(),
                    "end": range_end.isoformat(),
                    "expected_observation_count": range_count,
                })
            range_start = None
            range_end = None
            range_count = 0
    if range_start is not None:
        total_range_count += 1
        if len(ranges) < max_ranges:
            ranges.append({
                "start": range_start.isoformat(),
                "end": range_end.isoformat(),
                "expected_observation_count": range_count,
            })
    return {
        "expected_count": len(expected_dates),
        "missing_count": sum(value in missing_dates for value in expected_dates),
        "range_count": total_range_count,
        "ranges": ranges,
        "ranges_truncated": total_range_count > len(ranges),
    }


def wait_for_host_request_slot(
    host: str,
    previous_request_finished_at_by_host: dict[str, float],
    min_interval_seconds: float,
    jitter_seconds: float,
    crawl_delay_seconds_by_host: dict[str, float],
) -> None:
    """Apply the same-host request interval, including a published crawl delay."""

    previous_request_finished_at = previous_request_finished_at_by_host.get(host)
    if previous_request_finished_at is None:
        return
    target_interval = max(
        min_interval_seconds + random.uniform(0.0, jitter_seconds),
        crawl_delay_seconds_by_host.get(host, 0.0),
    )
    elapsed_since_previous = time.monotonic() - previous_request_finished_at
    if elapsed_since_previous < target_interval:
        time.sleep(target_interval - elapsed_since_previous)


def inspect_sunsirs_response(
    response_content: bytes,
    observation_date: date,
) -> dict[str, object]:
    """Validate diagnostic HTML signals without changing the raw archive contract."""

    decoded_text = response_content.decode("utf-8", errors="replace")
    lower_text = decoded_text.lower()
    blocked_terms = [term for term in BLOCK_PAGE_TERMS if term in lower_text]
    soup = BeautifulSoup(response_content, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title is not None else None
    data_table = soup.find("table", id="fdata")

    commodity_rows = []
    header_texts = []
    empty_direct_cell_count = 0
    null_like_direct_cell_count = 0
    rows_with_null_like_cells = 0
    business_cell_count = 0
    nonempty_business_cell_count = 0
    null_like_markers = {"", "-", "--", "—", "n/a", "na", "null", "暂无"}
    if data_table is not None:
        for row in data_table.find_all("tr"):
            direct_header_cells = row.find_all("th", recursive=False)
            if direct_header_cells:
                header_texts.extend(
                    cell.get_text(" ", strip=True)
                    for cell in direct_header_cells
                )
            direct_cells = row.find_all("td", recursive=False)
            if not direct_cells:
                continue
            first_link = direct_cells[0].find("a", href=re.compile(r"/sf/\d+\.html"))
            if first_link is None:
                header_texts.extend(
                    cell.get_text(" ", strip=True)
                    for cell in direct_cells
                )
                continue
            if len(direct_cells) < 8:
                continue
            commodity_rows.append(row)
            direct_cell_texts = [cell.get_text(" ", strip=True) for cell in direct_cells]
            empty_direct_cell_count += sum(not text for text in direct_cell_texts)
            row_null_like_count = sum(
                text.strip().lower() in null_like_markers
                for text in direct_cell_texts
            )
            null_like_direct_cell_count += row_null_like_count
            rows_with_null_like_cells += row_null_like_count > 0
            business_cell_texts = direct_cell_texts[1:]
            business_cell_count += len(business_cell_texts)
            nonempty_business_cell_count += sum(
                text.strip().lower() not in null_like_markers
                for text in business_cell_texts
            )

    expected_chinese_date = (
        f"{observation_date.year:04d}年{observation_date.month:02d}月"
        f"{observation_date.day:02d}日"
    )
    visible_text = soup.get_text(" ", strip=True)
    publication_match = re.search(
        rf"{re.escape(expected_chinese_date)}\s+(\d{{2}}:\d{{2}})",
        visible_text,
    )
    table_text = (
        re.sub(r"\s+", " ", data_table.get_text(" ", strip=True))
        if data_table is not None
        else ""
    )
    normalized_header_text = re.sub(r"\s+", "", " ".join(header_texts))
    header_semantics_present = all((
        "商品" in normalized_header_text,
        "现货" in normalized_header_text and "价格" in normalized_header_text,
        "最近合约" in normalized_header_text or "近月" in normalized_header_text,
        "主力" in normalized_header_text,
        "现期差" in normalized_header_text or "基差" in normalized_header_text,
    ))
    nonempty_business_cell_ratio = (
        nonempty_business_cell_count / business_cell_count
        if business_cell_count
        else 0.0
    )
    basis_definition_present = "现期差=现货价格-期货价格" in visible_text
    structural_signals = {
        "title_matches_date_and_subject": bool(
            title
            and expected_chinese_date in title
            and "商品现货与期货价格对比表" in title
        ),
        "fdata_table_present": data_table is not None,
        "commodity_row_count": len(commodity_rows),
        "empty_direct_cell_count": empty_direct_cell_count,
        "null_like_direct_cell_count": null_like_direct_cell_count,
        "rows_with_null_like_cells": rows_with_null_like_cells,
        "business_cell_count": business_cell_count,
        "nonempty_business_cell_count": nonempty_business_cell_count,
        "nonempty_business_cell_ratio": round(nonempty_business_cell_ratio, 6),
        "header_text": " ".join(text for text in header_texts if text),
        "header_semantics_present": header_semantics_present,
        "publication_time": (
            publication_match.group(1) if publication_match is not None else None
        ),
        "basis_definition_present": basis_definition_present,
        "blocked_terms": blocked_terms,
        "replacement_character_count": decoded_text.count("\ufffd"),
    }
    failure_reasons = []
    if not structural_signals["title_matches_date_and_subject"]:
        failure_reasons.append("title_or_date_mismatch")
    if not structural_signals["fdata_table_present"]:
        failure_reasons.append("fdata_table_missing")
    if len(commodity_rows) < 10:
        failure_reasons.append("too_few_eight_column_commodity_rows")
    if not header_semantics_present:
        failure_reasons.append("expected_table_headers_missing")
    if publication_match is None:
        failure_reasons.append("publication_time_missing")
    if not basis_definition_present:
        failure_reasons.append("basis_definition_missing")
    if nonempty_business_cell_ratio < 0.60:
        failure_reasons.append("business_cells_mostly_empty")
    if blocked_terms:
        failure_reasons.append("block_page_signal")
    if decoded_text.count("\ufffd"):
        failure_reasons.append("utf8_decode_replacement_characters")
    warning_reasons = []
    if not failure_reasons and null_like_direct_cell_count:
        warning_reasons.append("some_business_cells_are_null_like")
    diagnostic_status = (
        "failed"
        if failure_reasons
        else "warning"
        if warning_reasons
        else "passed"
    )
    return {
        "diagnostic_status": diagnostic_status,
        "diagnostic_valid": (
            True
            if diagnostic_status == "passed"
            else False
            if diagnostic_status == "failed"
            else None
        ),
        "diagnostic_reasons": failure_reasons or warning_reasons,
        "response_kind": (
            "expected_html"
            if diagnostic_status == "passed"
            else "expected_html_with_warnings"
            if diagnostic_status == "warning"
            else "unexpected_html"
        ),
        "title": title,
        "semantic_sha256": hashlib.sha256(table_text.encode("utf-8")).hexdigest(),
        **structural_signals,
    }


def inspect_eastmoney_response(
    response_content: bytes,
    expected_fields: list[str],
    range_start: date,
    range_end: date,
    require_complete_result: bool = True,
    expected_indicator_id: str | None = None,
    expected_frequency: str | None = None,
) -> dict[str, object]:
    """Validate the public web endpoint's JSON envelope and bounded data slice."""

    expected_dates = expected_source_dates(
        range_start,
        range_end,
        expected_frequency,
    )
    decoded_text = response_content.decode("utf-8", errors="replace")
    lower_text = decoded_text.lower()
    blocked_terms = [term for term in BLOCK_PAGE_TERMS if term in lower_text]
    try:
        payload = json.loads(decoded_text)
    except json.JSONDecodeError as error:
        return {
            "diagnostic_status": "failed",
            "diagnostic_valid": False,
            "diagnostic_reasons": ["invalid_json"],
            "response_kind": "invalid_json",
            "json_error": str(error),
            "blocked_terms": blocked_terms,
            "body_prefix": re.sub(r"\s+", " ", decoded_text[:240]),
        }

    if not isinstance(payload, dict):
        return {
            "diagnostic_status": "failed",
            "diagnostic_valid": False,
            "diagnostic_reasons": ["unexpected_json_top_level"],
            "response_kind": "unexpected_json_top_level",
            "json_top_level_type": type(payload).__name__,
            "blocked_terms": blocked_terms,
        }

    success = payload.get("success") is True
    code = payload.get("code")
    message = str(payload.get("message") or "").strip()
    empty_confirmed = (
        not success
        and str(code) == "9201"
        and "返回数据为空" in message
    )
    if empty_confirmed:
        return {
            "diagnostic_status": "failed" if blocked_terms else "warning",
            "diagnostic_valid": False if blocked_terms else None,
            "diagnostic_reasons": (
                ["block_page_signal"]
                if blocked_terms
                else ["source_confirmed_empty"]
            ),
            "response_kind": "empty_confirmed",
            "success": False,
            "code": code,
            "message": message,
            "row_count": 0,
            "coverage_basis": expected_frequency,
            "missing_observation_ranges": summarize_missing_dates(
                [],
                set(),
            ),
            "unassessed_pending_tail": summarize_missing_dates(
                expected_dates,
                set(expected_dates),
            ),
            "blocked_terms": blocked_terms,
            "semantic_sha256": hashlib.sha256(b"[]").hexdigest(),
        }

    result = payload.get("result")
    data_rows = result.get("data") if isinstance(result, dict) else None
    if not success or not isinstance(result, dict) or not isinstance(data_rows, list):
        return {
            "diagnostic_status": "failed",
            "diagnostic_valid": False,
            "diagnostic_reasons": ["unexpected_json_envelope"],
            "response_kind": "unexpected_json_envelope",
            "success": success,
            "code": code,
            "message": message,
            "payload_keys": sorted(payload),
            "result_type": type(result).__name__,
            "blocked_terms": blocked_terms,
        }

    missing_field_counts = Counter()
    null_field_counts = Counter()
    null_dates_by_field: dict[str, set[date]] = defaultdict(set)
    invalid_numeric_counts = Counter()
    numeric_values_by_field: dict[str, list[float]] = defaultdict(list)
    unexpected_indicator_id_count = 0
    unexpected_indicator_ids = set()
    invalid_date_count = 0
    outside_range_count = 0
    parsed_dates = []
    row_key_signatures = []
    non_object_row_count = 0
    for row in data_rows:
        if not isinstance(row, dict):
            non_object_row_count += 1
            continue
        parsed_date = parse_iso_date(row.get("REPORT_DATE"))
        for field in expected_fields:
            if field not in row:
                missing_field_counts[field] += 1
            elif row[field] is None:
                null_field_counts[field] += 1
                if parsed_date is not None:
                    null_dates_by_field[field].add(parsed_date)
            elif field not in {"REPORT_DATE", "INDICATOR_ID"}:
                raw_value = row[field]
                if isinstance(raw_value, bool):
                    invalid_numeric_counts[field] += 1
                    continue
                try:
                    numeric_value = float(raw_value)
                except (TypeError, ValueError):
                    invalid_numeric_counts[field] += 1
                    continue
                if not math.isfinite(numeric_value):
                    invalid_numeric_counts[field] += 1
                    continue
                numeric_values_by_field[field].append(numeric_value)
        if expected_indicator_id is not None:
            returned_indicator_id = str(row.get("INDICATOR_ID") or "").strip()
            if returned_indicator_id != expected_indicator_id:
                unexpected_indicator_id_count += 1
                unexpected_indicator_ids.add(returned_indicator_id)
        if parsed_date is None:
            invalid_date_count += 1
        else:
            parsed_dates.append(parsed_date)
            if not range_start <= parsed_date <= range_end:
                outside_range_count += 1
        row_key_signatures.append(
            (
                str(row.get("INDICATOR_ID") or ""),
                str(row.get("REPORT_DATE") or ""),
            )
        )

    duplicate_key_count = len(row_key_signatures) - len(set(row_key_signatures))
    unique_dates = sorted(set(parsed_dates))
    weekday_gap_count = 0
    longest_calendar_gap_days = None
    if unique_dates:
        date_set = set(unique_dates)
        if expected_frequency == "weekday_descriptive":
            cursor = unique_dates[0]
            while cursor <= unique_dates[-1]:
                if cursor.weekday() < 5 and cursor not in date_set:
                    weekday_gap_count += 1
                cursor += timedelta(days=1)
        if len(unique_dates) >= 2:
            longest_calendar_gap_days = max(
                (later - earlier).days
                for earlier, later in zip(unique_dates, unique_dates[1:])
            )

    canonical_rows = json.dumps(
        data_rows,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    pages = result.get("pages")
    count = result.get("count")
    page_metadata_valid = (
        isinstance(pages, int)
        and not isinstance(pages, bool)
        and pages >= 0
        and isinstance(count, int)
        and not isinstance(count, bool)
        and count >= 0
    )
    if not page_metadata_valid:
        complete_result_valid = False
    elif require_complete_result:
        complete_result_valid = bool(
            count == len(data_rows)
            and (
                (count == 0 and pages in {0, 1})
                or (count > 0 and pages == 1)
            )
        )
    else:
        complete_result_valid = bool(
            (count == 0 and len(data_rows) == 0 and pages in {0, 1})
            or (count > 0 and len(data_rows) == 1 and pages == count)
        )
    index_value_null_count = null_field_counts.get("INDICATOR_VALUE", 0)
    constant_numeric_fields = sorted(
        field
        for field, values in numeric_values_by_field.items()
        if len(values) >= 3 and len(set(values)) == 1
    )
    all_zero_numeric_fields = sorted(
        field
        for field, values in numeric_values_by_field.items()
        if values and all(value == 0 for value in values)
    )
    numeric_field_profiles = {}
    for field, values in sorted(numeric_values_by_field.items()):
        adjacent_change_count = sum(
            previous != current
            for previous, current in zip(values, values[1:])
        )
        longest_equal_run = 0
        current_equal_run = 0
        previous_value = None
        for index, value in enumerate(values):
            if index and value == previous_value:
                current_equal_run += 1
            else:
                current_equal_run = 1
            longest_equal_run = max(longest_equal_run, current_equal_run)
            previous_value = value
        numeric_field_profiles[field] = {
            "non_null_count": len(values),
            "unique_value_count": len(set(values)),
            "adjacent_change_count": adjacent_change_count,
            "longest_equal_run": longest_equal_run,
        }
    observed_dates = set(unique_dates)
    assessed_expected_dates = (
        [
            expected_date
            for expected_date in expected_dates
            if expected_date <= unique_dates[-1]
        ]
        if unique_dates
        else []
    )
    unassessed_pending_dates = [
        expected_date
        for expected_date in expected_dates
        if not unique_dates or expected_date > unique_dates[-1]
    ]
    missing_observation_dates = set(assessed_expected_dates) - observed_dates
    missing_observation_ranges = summarize_missing_dates(
        assessed_expected_dates,
        missing_observation_dates,
    )
    unassessed_pending_tail = summarize_missing_dates(
        unassessed_pending_dates,
        set(unassessed_pending_dates),
    )
    null_value_ranges = {
        field: summarize_missing_dates(
            unique_dates,
            null_dates,
        )
        for field, null_dates in sorted(null_dates_by_field.items())
    }

    failure_reasons = []
    if not page_metadata_valid:
        failure_reasons.append("invalid_page_metadata")
    elif not complete_result_valid:
        failure_reasons.append("incomplete_or_inconsistent_page")
    if missing_field_counts:
        failure_reasons.append("missing_expected_fields")
    if invalid_date_count:
        failure_reasons.append("invalid_report_dates")
    if outside_range_count:
        failure_reasons.append("report_dates_outside_request_range")
    if duplicate_key_count:
        failure_reasons.append("duplicate_business_keys")
    if non_object_row_count:
        failure_reasons.append("non_object_data_rows")
    if invalid_numeric_counts:
        failure_reasons.append("invalid_numeric_values")
    if index_value_null_count:
        failure_reasons.append("null_index_values")
    if unexpected_indicator_id_count:
        failure_reasons.append("unexpected_indicator_id")
    if blocked_terms:
        failure_reasons.append("block_page_signal")

    warning_reasons = []
    if not failure_reasons and not data_rows:
        warning_reasons.append("empty_data_in_bounded_probe")
    if not failure_reasons and null_field_counts:
        warning_reasons.append("null_business_values")
    if not failure_reasons and constant_numeric_fields:
        warning_reasons.append("constant_numeric_fields")
    if not failure_reasons and all_zero_numeric_fields:
        warning_reasons.append("all_zero_numeric_fields")
    if (
        not failure_reasons
        and expected_frequency in {"month_end", "quarter_end"}
        and missing_observation_ranges["missing_count"]
    ):
        warning_reasons.append("missing_theoretical_macro_observations")
    diagnostic_status = (
        "failed"
        if failure_reasons
        else "warning"
        if warning_reasons
        else "passed"
    )
    return {
        "diagnostic_status": diagnostic_status,
        "diagnostic_valid": (
            True
            if diagnostic_status == "passed"
            else False
            if diagnostic_status == "failed"
            else None
        ),
        "diagnostic_reasons": failure_reasons or warning_reasons,
        "response_kind": (
            "expected_json"
            if diagnostic_status == "passed" and require_complete_result
            else "expected_json_partial"
            if diagnostic_status == "passed"
            else "expected_json_with_warnings"
            if diagnostic_status == "warning"
            else "unexpected_json_data"
        ),
        "success": success,
        "code": code,
        "message": message,
        "pages": pages,
        "reported_count": count,
        "row_count": len(data_rows),
        "expected_indicator_id": expected_indicator_id,
        "unexpected_indicator_id_count": unexpected_indicator_id_count,
        "unexpected_indicator_ids": sorted(unexpected_indicator_ids),
        "earliest_report_date": unique_dates[0].isoformat() if unique_dates else None,
        "latest_report_date": unique_dates[-1].isoformat() if unique_dates else None,
        "latest_lag_days_from_probe_end": (
            (range_end - unique_dates[-1]).days if unique_dates else None
        ),
        "missing_field_counts": dict(sorted(missing_field_counts.items())),
        "null_field_counts": dict(sorted(null_field_counts.items())),
        "null_value_ranges": null_value_ranges,
        "invalid_numeric_counts": dict(sorted(invalid_numeric_counts.items())),
        "constant_numeric_fields": constant_numeric_fields,
        "all_zero_numeric_fields": all_zero_numeric_fields,
        "numeric_field_profiles": numeric_field_profiles,
        "quality_warning": diagnostic_status == "warning",
        "coverage_basis": expected_frequency,
        "missing_observation_ranges": missing_observation_ranges,
        "unassessed_pending_tail": unassessed_pending_tail,
        "invalid_date_count": invalid_date_count,
        "outside_range_count": outside_range_count,
        "duplicate_key_count": duplicate_key_count,
        "non_object_row_count": non_object_row_count,
        "descriptive_weekday_gap_count": weekday_gap_count,
        "longest_calendar_gap_days": longest_calendar_gap_days,
        "blocked_terms": blocked_terms,
        "semantic_sha256": hashlib.sha256(canonical_rows).hexdigest(),
    }


def previous_weekdays(last_date: date, count: int) -> list[date]:
    """Return the most recent weekdays, without pretending they are exchange calendars."""

    weekdays = []
    cursor = last_date
    while len(weekdays) < count:
        if cursor.weekday() < 5:
            weekdays.append(cursor)
        cursor -= timedelta(days=1)
    return sorted(weekdays)


def build_probe_jobs(
    mode: str,
    observation_date: date,
    index_lookback_days: int,
    macro_lookback_days: int,
    only_family: str = "all",
    index_code: str | None = None,
    index_boundary: str = "recent",
) -> list[dict[str, object]]:
    """Build a bounded request manifest from the project's authoritative configs."""

    jobs: list[dict[str, object]] = []
    if only_family in {"all", "sunsirs"}:
        sunsirs_dates = (
            [observation_date]
            if mode == "smoke"
            else previous_weekdays(observation_date, 5)
        )
        for sunsirs_date in sunsirs_dates:
            jobs.append({
                "family": "sunsirs_html",
                "name": f"sunsirs_spot_futures_{sunsirs_date.isoformat()}",
                "url": SUNSIRS_URL_TEMPLATE.format(observation_date=sunsirs_date.isoformat()),
                "params": None,
                "headers": SUNSIRS_HEADERS,
                "observation_date": sunsirs_date,
            })

    index_range_start = observation_date - timedelta(days=index_lookback_days)
    index_entities = (
        EXTERNAL_INDEX_ENTITIES[:1]
        if mode == "smoke"
        else EXTERNAL_INDEX_ENTITIES
    )
    if index_code is not None:
        index_entities = tuple(
            entity for entity in EXTERNAL_INDEX_ENTITIES
            if entity.index_code == index_code
        )
        if not index_entities:
            raise ValueError(f"共享配置中不存在指数代码：{index_code}")
    if only_family not in {"all", "index"}:
        index_entities = ()
    for entity in index_entities:
        boundary_specs = (
            [("recent", "1", EASTMONEY_PAGE_SIZE, (
                f'(INDICATOR_ID="{entity.source_indicator_id}")'
                f"(REPORT_DATE>='{index_range_start.isoformat()}')"
                f"(REPORT_DATE<='{observation_date.isoformat()}')"
            ), True)]
            if index_boundary == "recent"
            else [
                ("earliest", "1", 1, f'(INDICATOR_ID="{entity.source_indicator_id}")', False),
                ("latest", "-1", 1, f'(INDICATOR_ID="{entity.source_indicator_id}")', False),
            ]
        )
        for boundary_name, sort_type, page_size, date_filter, require_complete in boundary_specs:
            jobs.append({
            "family": "eastmoney_industry_index_json",
            "name": f"eastmoney_index_{entity.index_code}_{boundary_name}",
            "url": EASTMONEY_ENDPOINT,
            "params": {
                "reportName": "RPT_INDUSTRY_INDEX",
                "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
                "filter": date_filter,
                "pageNumber": 1,
                "pageSize": page_size,
                "sortColumns": "REPORT_DATE",
                "sortTypes": sort_type,
                "source": "WEB",
                "client": "WEB",
            },
            "headers": EASTMONEY_HEADERS,
            "expected_fields": ["INDICATOR_ID", "INDICATOR_VALUE", "REPORT_DATE"],
            "expected_indicator_id": entity.source_indicator_id,
            "expected_frequency": (
                "weekday_descriptive" if require_complete else None
            ),
            "range_start": index_range_start if require_complete else date(1900, 1, 1),
            "range_end": observation_date if require_complete else date(9999, 12, 31),
            "require_complete_result": require_complete,
            "entity": {
                "source_indicator_id": entity.source_indicator_id,
                "index_code": entity.index_code,
                "index_name_zh": entity.index_name_zh,
                "index_category": entity.index_category,
            },
            })

    macro_range_start = observation_date - timedelta(days=macro_lookback_days)
    macro_reports = (
        sorted(MACRO_FIELDS_BY_REPORT.items())
        if only_family in {"all", "macro"}
        else []
    )
    for report_name, fields in macro_reports:
        jobs.append({
            "family": "eastmoney_macro_json",
            "name": f"eastmoney_macro_{report_name}",
            "url": EASTMONEY_ENDPOINT,
            "params": {
                "reportName": report_name,
                "columns": ",".join(fields),
                "filter": (
                    f"(REPORT_DATE>='{macro_range_start.isoformat()}')"
                    f"(REPORT_DATE<='{observation_date.isoformat()}')"
                ),
                "pageNumber": 1,
                "pageSize": EASTMONEY_PAGE_SIZE,
                "sortColumns": "REPORT_DATE",
                "sortTypes": "1",
                "source": "WEB",
                "client": "WEB",
            },
            "headers": EASTMONEY_HEADERS,
            "expected_fields": fields,
            "expected_indicator_id": None,
            "expected_frequency": MACRO_FREQUENCY_BY_REPORT[report_name],
            "range_start": macro_range_start,
            "range_end": observation_date,
        })
    return jobs


def inspect_robots(
    session: requests.Session,
    target_url: str,
    user_agent: str,
    timeout_seconds: float,
) -> dict[str, object]:
    """Fetch one host's robots.txt and record whether the target path is allowed."""

    parsed_target = urllib.parse.urlsplit(target_url)
    robots_url = urllib.parse.urlunsplit(
        (parsed_target.scheme, parsed_target.netloc, "/robots.txt", "", "")
    )
    started_at = time.perf_counter()
    try:
        response = session.get(
            robots_url,
            headers={"User-Agent": user_agent},
            timeout=(min(10.0, timeout_seconds), timeout_seconds),
        )
        elapsed_seconds = time.perf_counter() - started_at
    except requests.RequestException as error:
        return {
            "robots_url": robots_url,
            "status": "unavailable",
            "error": f"{type(error).__name__}: {error}",
        }

    evidence = {
        "robots_url": robots_url,
        "status": "received",
        "http_status": response.status_code,
        "elapsed_seconds": round(elapsed_seconds, 6),
        "content_type": response.headers.get("Content-Type"),
        "body_bytes": len(response.content),
        "body_sha256": hashlib.sha256(response.content).hexdigest(),
        "can_fetch": None,
        "crawl_delay_seconds": None,
        "request_rate": None,
    }
    content_type = str(response.headers.get("Content-Type") or "").lower()
    robots_text = response.text
    looks_like_robots = bool(
        "text/plain" in content_type
        or re.search(
            r"(?im)^\s*(user-agent|allow|disallow|crawl-delay|sitemap)\s*:",
            robots_text,
        )
    )
    if response.status_code == 200 and looks_like_robots:
        parser = urllib.robotparser.RobotFileParser()
        parser.set_url(robots_url)
        parser.parse(robots_text.splitlines())
        evidence["can_fetch"] = parser.can_fetch(user_agent, target_url)
        evidence["crawl_delay_seconds"] = parser.crawl_delay(user_agent)
        request_rate = parser.request_rate(user_agent)
        if request_rate is not None:
            evidence["request_rate"] = {
                "requests": request_rate.requests,
                "seconds": request_rate.seconds,
            }
    elif response.status_code == 200:
        evidence["status"] = "unexpected_content"
    elif response.status_code in {404, 410}:
        evidence["status"] = "not_published"
    else:
        evidence["status"] = "http_error"
    return evidence


def robots_policy_outcome(evidence: dict[str, object]) -> str:
    """Classify robots evidence without treating a 200 catch-all as permission."""

    if evidence.get("can_fetch") is False:
        return "disallowed"
    if evidence.get("status") in {"unavailable", "http_error"}:
        return "failed"
    if evidence.get("status") == "unexpected_content":
        return "warning"
    return "passed"


def aggregate_results(results: list[dict[str, object]]) -> dict[str, object]:
    """Summarize latency and consistency without hiding individual evidence."""

    grouped_results: dict[str, list[dict[str, object]]] = defaultdict(list)
    for result in results:
        if result.get("status") != "skipped":
            grouped_results[str(result["name"])].append(result)

    summaries = {}
    for name, group in sorted(grouped_results.items()):
        elapsed_values = [
            float(item["elapsed_seconds"])
            for item in group
            if isinstance(item.get("elapsed_seconds"), (int, float))
        ]
        status_counts = Counter(str(item.get("http_status")) for item in group)
        semantic_hashes = {
            str(item["inspection"].get("semantic_sha256"))
            for item in group
            if isinstance(item.get("inspection"), dict)
            and item["inspection"].get("semantic_sha256")
        }
        diagnostic_status_counts = Counter(
            str(item["inspection"].get("diagnostic_status"))
            for item in group
            if isinstance(item.get("inspection"), dict)
        )
        diagnostic_reason_counts = Counter(
            str(reason)
            for item in group
            if isinstance(item.get("inspection"), dict)
            for reason in item["inspection"].get("diagnostic_reasons", [])
        )
        summaries[name] = {
            "samples": len(group),
            "http_status_counts": dict(sorted(status_counts.items())),
            "diagnostic_valid_samples": sum(
                isinstance(item.get("inspection"), dict)
                and item["inspection"].get("diagnostic_valid") is True
                for item in group
            ),
            "diagnostic_status_counts": dict(sorted(diagnostic_status_counts.items())),
            "diagnostic_reason_counts": dict(sorted(diagnostic_reason_counts.items())),
            "elapsed_seconds_min": min(elapsed_values) if elapsed_values else None,
            "elapsed_seconds_median": (
                statistics.median(elapsed_values) if elapsed_values else None
            ),
            "elapsed_seconds_p95": percentile(elapsed_values, 0.95),
            "elapsed_seconds_max": max(elapsed_values) if elapsed_values else None,
            "semantic_response_variants": len(semantic_hashes),
        }
    return summaries


def write_json_exclusive_atomic(
    output_path: pathlib.Path,
    payload: dict[str, object],
) -> None:
    """Reserve a new evidence path, then atomically install a complete JSON file."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    serialized_payload = (
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    )
    temporary_path = output_path.with_name(f".{output_path.name}.tmp")
    try:
        with output_path.open("x", encoding="utf-8", newline="\n"):
            pass
    except FileExistsError as error:
        raise FileExistsError(f"证据路径已存在，拒绝覆盖：{output_path}") from error
    try:
        with temporary_path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized_payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.replace(output_path)
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        output_path.unlink(missing_ok=True)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="低频只读探测生意社与东方财富网页数据接口。",
    )
    parser.add_argument("--mode", choices=("smoke", "coverage"), default="smoke")
    parser.add_argument(
        "--only-family",
        choices=("all", "sunsirs", "index", "macro"),
        default="all",
    )
    parser.add_argument("--index-code")
    parser.add_argument(
        "--index-boundary",
        choices=("recent", "boundaries"),
        default="recent",
    )
    parser.add_argument("--observation-date", type=date.fromisoformat)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--min-interval-seconds", type=float, default=5.0)
    parser.add_argument("--jitter-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--index-lookback-days", type=int, default=120)
    parser.add_argument("--macro-lookback-days", type=int, default=800)
    parser.add_argument("--skip-robots", action="store_true")
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        help="JSON 证据路径；默认写入 checks/results 目录。",
    )
    args = parser.parse_args()
    if not 1 <= args.repeats <= 5:
        parser.error("--repeats 必须位于 1—5。")
    if args.min_interval_seconds < 3.0:
        parser.error("为避免形成压力测试，--min-interval-seconds 不得小于 3。")
    if not 0.0 <= args.jitter_seconds <= 10.0:
        parser.error("--jitter-seconds 必须位于 0—10。")
    if not 1.0 <= args.timeout_seconds <= 120.0:
        parser.error("--timeout-seconds 必须位于 1—120。")
    if not 7 <= args.index_lookback_days <= 366:
        parser.error("--index-lookback-days 必须位于 7—366。")
    if not 90 <= args.macro_lookback_days <= 3660:
        parser.error("--macro-lookback-days 必须位于 90—3660。")
    return args


def main() -> int:
    args = parse_args()
    now_shanghai = datetime.now(ZoneInfo("Asia/Shanghai"))
    observation_date = args.observation_date
    if observation_date is None:
        candidate_date = now_shanghai.date()
        if now_shanghai.hour < 18:
            candidate_date -= timedelta(days=1)
        observation_date = previous_weekdays(candidate_date, 1)[0]

    jobs = build_probe_jobs(
        mode=args.mode,
        observation_date=observation_date,
        index_lookback_days=args.index_lookback_days,
        macro_lookback_days=args.macro_lookback_days,
        only_family=args.only_family,
        index_code=args.index_code,
        index_boundary=args.index_boundary,
    )
    sessions_by_host: dict[str, requests.Session] = {}
    for job in jobs:
        host = urllib.parse.urlsplit(str(job["url"])).netloc
        sessions_by_host.setdefault(host, requests.Session())

    robots_evidence = {}
    disallowed_hosts = set()
    robots_warning_hosts: dict[str, str] = {}
    halted_hosts: dict[str, str] = {}
    previous_request_finished_at_by_host: dict[str, float] = {}
    crawl_delay_seconds_by_host: dict[str, float] = {}
    if not args.skip_robots:
        first_job_by_host = {}
        for job in jobs:
            host = urllib.parse.urlsplit(str(job["url"])).netloc
            first_job_by_host.setdefault(host, job)
        for host, job in first_job_by_host.items():
            user_agent = str(job["headers"].get("User-Agent", "*"))
            evidence = inspect_robots(
                sessions_by_host[host],
                str(job["url"]),
                user_agent,
                args.timeout_seconds,
            )
            previous_request_finished_at_by_host[host] = time.monotonic()
            robots_evidence[host] = evidence
            crawl_delay_seconds = evidence.get("crawl_delay_seconds")
            if isinstance(crawl_delay_seconds, (int, float)):
                crawl_delay_seconds_by_host[host] = float(crawl_delay_seconds)
            robots_outcome = robots_policy_outcome(evidence)
            if robots_outcome == "disallowed":
                disallowed_hosts.add(host)
            elif robots_outcome == "failed":
                halted_hosts[host] = f"robots_{evidence['status']}"
            elif robots_outcome == "warning":
                robots_warning_hosts[host] = str(evidence["status"])

    results = []
    request_sequence = 0
    for repeat_number in range(1, args.repeats + 1):
        for job in jobs:
            request_sequence += 1
            url = str(job["url"])
            host = urllib.parse.urlsplit(url).netloc
            if host in disallowed_hosts:
                results.append({
                    "sequence": request_sequence,
                    "repeat": repeat_number,
                    "family": job["family"],
                    "name": job["name"],
                    "status": "skipped",
                    "reason": "robots_disallow",
                })
                continue
            if host in halted_hosts:
                results.append({
                    "sequence": request_sequence,
                    "repeat": repeat_number,
                    "family": job["family"],
                    "name": job["name"],
                    "status": "skipped",
                    "reason": f"host_halted_after_{halted_hosts[host]}",
                })
                continue

            wait_for_host_request_slot(
                host,
                previous_request_finished_at_by_host,
                args.min_interval_seconds,
                args.jitter_seconds,
                crawl_delay_seconds_by_host,
            )

            started_at_utc = datetime.now(ZoneInfo("UTC"))
            started_at = time.perf_counter()
            try:
                response = sessions_by_host[host].get(
                    url,
                    params=job["params"],
                    headers=job["headers"],
                    timeout=(min(10.0, args.timeout_seconds), args.timeout_seconds),
                )
                elapsed_seconds = time.perf_counter() - started_at
                previous_request_finished_at_by_host[host] = time.monotonic()
            except requests.RequestException as error:
                previous_request_finished_at_by_host[host] = time.monotonic()
                halted_hosts[host] = "transport_error"
                results.append({
                    "sequence": request_sequence,
                    "repeat": repeat_number,
                    "family": job["family"],
                    "name": job["name"],
                    "status": "request_error",
                    "started_at_utc": started_at_utc.isoformat(),
                    "elapsed_seconds": round(time.perf_counter() - started_at, 6),
                    "error": f"{type(error).__name__}: {error}",
                })
                continue

            content_type = response.headers.get("Content-Type")
            result = {
                "sequence": request_sequence,
                "repeat": repeat_number,
                "family": job["family"],
                "name": job["name"],
                "status": "received",
                "started_at_utc": started_at_utc.isoformat(),
                "request_url": response.url,
                "http_status": response.status_code,
                "elapsed_seconds": round(elapsed_seconds, 6),
                "requests_elapsed_seconds": round(response.elapsed.total_seconds(), 6),
                "content_type": content_type,
                "content_encoding": response.headers.get("Content-Encoding"),
                "cache_control": response.headers.get("Cache-Control"),
                "retry_after": response.headers.get("Retry-After"),
                "response_date": response.headers.get("Date"),
                "last_modified": response.headers.get("Last-Modified"),
                "etag": response.headers.get("ETag"),
                "age": response.headers.get("Age"),
                "server": response.headers.get("Server"),
                "body_bytes": len(response.content),
                "body_sha256": hashlib.sha256(response.content).hexdigest(),
            }
            if response.status_code == 200:
                if job["family"] == "sunsirs_html":
                    result["inspection"] = inspect_sunsirs_response(
                        response.content,
                        job["observation_date"],
                    )
                else:
                    result["inspection"] = inspect_eastmoney_response(
                        response.content,
                        job["expected_fields"],
                        job["range_start"],
                        job["range_end"],
                        job.get("require_complete_result", True),
                        job.get("expected_indicator_id"),
                        job.get("expected_frequency"),
                    )
            else:
                decoded_prefix = response.content[:300].decode("utf-8", errors="replace")
                result["inspection"] = {
                    "diagnostic_status": "failed",
                    "diagnostic_valid": False,
                    "diagnostic_reasons": ["unexpected_http_status"],
                    "response_kind": "unexpected_http_status",
                    "blocked_terms": [
                        term
                        for term in BLOCK_PAGE_TERMS
                        if term in decoded_prefix.lower()
                    ],
                    "body_prefix": re.sub(r"\s+", " ", decoded_prefix),
                }
            if "entity" in job:
                result["entity"] = job["entity"]
            results.append(result)

            inspection = result["inspection"]
            if (
                isinstance(inspection, dict)
                and inspection.get("diagnostic_status") == "failed"
            ):
                halted_hosts[host] = str(
                    inspection.get("response_kind") or "diagnostic_failed"
                )

    received_diagnostic_status_counts = Counter(
        str(result["inspection"].get("diagnostic_status"))
        for result in results
        if result.get("status") == "received"
        and isinstance(result.get("inspection"), dict)
    )
    if halted_hosts or received_diagnostic_status_counts["failed"]:
        audit_status = "failed"
    elif (
        disallowed_hosts
        or robots_warning_hosts
        or received_diagnostic_status_counts["warning"]
    ):
        audit_status = "warning"
    else:
        audit_status = "passed"

    audit_document = {
        "schema_version": "1.1.0",
        "generated_at_utc": datetime.now(ZoneInfo("UTC")).isoformat(),
        "audit_status": audit_status,
        "python_executable": sys.executable,
        "mode": args.mode,
        "observation_date": observation_date.isoformat(),
        "request_policy": {
            "repeats": args.repeats,
            "min_interval_seconds": args.min_interval_seconds,
            "jitter_seconds": args.jitter_seconds,
            "timeout_seconds": args.timeout_seconds,
            "automatic_business_retry": False,
            "concurrency": 1,
            "interval_scope": "same_host_including_robots",
            "published_crawl_delay_takes_precedence": True,
            "stop_same_host_on": [
                "transport_error",
                "unsafe robots response",
                "any diagnostic_status=failed",
            ],
        },
        "scope_note": (
            "Weekday-gap counts are descriptive only; they do not subtract source-specific "
            "holidays or prove a business-data defect."
        ),
        "robots": robots_evidence,
        "robots_warning_hosts": robots_warning_hosts,
        "disallowed_hosts": sorted(disallowed_hosts),
        "halted_hosts": halted_hosts,
        "diagnostic_status_counts": dict(
            sorted(received_diagnostic_status_counts.items())
        ),
        "summaries": aggregate_results(results),
        "results": results,
    }

    output_path = args.output
    if output_path is None:
        timestamp = now_shanghai.strftime("%Y%m%dT%H%M%S%f%z")
        output_path = (
            PROJECT_ROOT
            / "R02_Market_Data"
            / "a01_Collection"
            / "checks"
            / "results"
            / f"{args.mode}_{timestamp}.json"
        )
    elif not output_path.is_absolute():
        output_path = (pathlib.Path.cwd() / output_path).resolve()
    write_json_exclusive_atomic(output_path, audit_document)
    print(output_path)

    return {"passed": 0, "failed": 1, "warning": 2}[audit_status]


if __name__ == "__main__":
    raise SystemExit(main())
