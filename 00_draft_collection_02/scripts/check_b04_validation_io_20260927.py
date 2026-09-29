"""Compare b04 before/after results and measured calls using isolated fixtures."""

import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
from collections import Counter
from datetime import date, datetime
from unittest.mock import patch

import pandas as pd
import pyarrow.dataset as ds
from click.testing import CliRunner


ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
OUT = SNAPSHOT / "reduction_checks"
OUT.mkdir(exist_ok=True)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


old = load("b04_before_reduction", SNAPSHOT / "b04_futures_bar_calendar.py")
new = load("b04_after_reduction", ROOT / "02_Futures_Lakehouse/a01_Futures_Market_Data/b04_futures_bar_calendar.py")
fixtures = load("b04_reduction_fixtures", ROOT / "00_draft_collection_02/tests/test_futures_bar_calendar.py")
fixture = fixtures.FuturesBarCalendarTests()
fixture.module = new
fixture.setUp()
checks = []
measurements = {}


def counted(module, operation):
    names = ["pandas_to_arrow", "arrow_to_pandas", "validate_structural_frame",
             "validate_bar_calendar_frame", "initial_state", "open_contract_dataset"]
    with contextlib.ExitStack() as stack:
        mocks = {name: stack.enter_context(patch.object(module, name, wraps=getattr(module, name)))
                 for name in names}
        value = operation(module)
        counts = {name: mocked.call_count for name, mocked in mocks.items()}
    return value, counts


def same_frames(left, right):
    pd.testing.assert_frame_equal(left, right, check_dtype=True, check_exact=True)


log = io.StringIO()
with contextlib.redirect_stdout(log):
    contract_df = fixture.contract_frame(session_count=2).sample(frac=1, random_state=9)
    before_fresh = old.build_fresh_partitions(contract_df, fixture.updated_at)
    after_fresh = new.build_fresh_partitions(contract_df, fixture.updated_at)
    for frequency in new.BAR_FREQUENCIES:
        same_frames(before_fresh[frequency], after_fresh[frequency])
    checks.append("generation_values_dtypes_and_order")

    for label, method, frame in [
        ("full_contract_validation", "validate_contract_input", contract_df),
        ("full_bar_validation", "validate_bar_calendar_frame", before_fresh["1m"]),
    ]:
        before, before_counts = counted(old, lambda m: getattr(m, method)(frame, "same input"))
        after, after_counts = counted(new, lambda m: getattr(m, method)(frame, "same input"))
        same_frames(before, after)
        assert after_counts["pandas_to_arrow"] == before_counts["pandas_to_arrow"] - 1
        measurements[label] = {"before": before_counts, "after": after_counts}
        checks.append(label)

    for frequency, fresh in before_fresh.items():
        existing = fresh.copy()
        existing["updated_at"] = fixture.updated_at
        existing.loc[existing["contract_code"].eq("RB2405.XSGE"), "underlying_code"] = "CHANGED"
        for label, left, right in [
            ("state_selection", fresh, existing),
            ("empty_existing", fresh, existing.iloc[:0]),
            ("empty_expected", fresh.iloc[:0], existing),
            ("both_empty", fresh.iloc[:0], existing.iloc[:0]),
        ]:
            operation = lambda m: m.assess_partition(left, right, None)
            (before_df, before_audit), before_counts = counted(old, operation)
            (after_df, after_audit), after_counts = counted(new, operation)
            same_frames(before_df, after_df)
            assert before_audit == after_audit
            assert after_counts["pandas_to_arrow"] < before_counts["pandas_to_arrow"]
            if label == "state_selection":
                measurements[f"{frequency}_state_merge_and_audit"] = {
                    "before": before_counts, "after": after_counts}
            checks.append(f"{frequency}_{label}")

    # Generated expected structures are trusted by the comparison boundary.
    # Complete dirty output still rejects both structural and state errors.
    for label, column, value in [
        ("invalid_session_count", "expected_bar_count", 999),
        ("invalid_frequency", "bar_frequency", "5m"),
        ("invalid_completion", "is_fetch_completed", True),
        ("missing_required_value", "contract_code", None),
    ]:
        invalid = before_fresh["1m"].copy()
        invalid[column] = value
        errors = []
        for m in [old, new]:
            try:
                m.validate_bar_calendar_frame(invalid, "same input")
            except (TypeError, ValueError) as error:
                errors.append((type(error).__name__, str(error)))
            else:
                raise AssertionError(f"accepted {label}")
        assert errors[0] == errors[1], errors
        checks.append(label)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixture.updated_at

    # Four month partitions, each generating 1d and 1m; all writes stay temporary.
    source_df = pd.concat([
        fixture.contract_frame(session_count=2, trading_date_value=date(2024, month, 3))
        for month in range(1, 5)
    ], ignore_index=True)
    final_tables = []
    for label, m in [("before", old), ("after", new)]:
        with tempfile.TemporaryDirectory(prefix=f"b04-reduction-{label}-") as tmp:
            lake = pathlib.Path(tmp)
            upstream = lake / "silver" / m.UPSTREAM_TABLE_NAME
            target = lake / "silver" / m.TABLE_NAME
            ds.write_dataset(m.pandas_to_arrow(source_df, m.FUTURES_CONTRACT_CALENDAR_SCHEMA),
                             upstream, format="parquet", partitioning=m.UPSTREAM_PARTITIONING)
            opened = []
            original_dataset = ds.dataset

            def record_dataset(path, *args, **kwargs):
                opened.append(pathlib.Path(path).resolve())
                return original_dataset(path, *args, **kwargs)

            with patch.object(m, "datetime", FixedDatetime), patch.object(m.ds, "dataset", side_effect=record_dataset):
                result, counts = counted(m, lambda mod: CliRunner().invoke(
                    mod.main, ["--lake-root", str(lake), "--write"]))
            assert result.exit_code == 0, result.output
            (OUT / f"{label}_multi_partition.log").write_text(result.output, encoding="utf-8")
            paths = Counter(opened)
            assert paths[upstream.resolve()] == 1
            assert paths[target.resolve()] == 0  # Empty target at initial discovery.
            assert counts["open_contract_dataset"] == 2
            assert counts["validate_bar_calendar_frame"] == 8  # Once per dirty leaf.
            assert all(paths[(target / m.partition_relative_path((freq, "XSGE", 2024, month))).resolve()] == 1
                       for freq in m.BAR_FREQUENCIES for month in range(1, 5))
            measurements[f"{label}_eight_dirty_leaves"] = counts
            table = ds.dataset(target, format="parquet", partitioning=m.HIVE_PARTITIONING).to_table()
            final_tables.append(m.arrow_to_pandas(table.select(m.FUTURES_BAR_CALENDAR_SCHEMA.names),
                m.FUTURES_BAR_CALENDAR_SCHEMA).sort_values(m.PRIMARY_KEY).reset_index(drop=True))
    same_frames(*final_tables)
    assert measurements["before_eight_dirty_leaves"]["initial_state"] == 8
    assert measurements["after_eight_dirty_leaves"]["initial_state"] == 2
    assert measurements["after_eight_dirty_leaves"]["pandas_to_arrow"] < measurements["before_eight_dirty_leaves"]["pandas_to_arrow"]
    checks.extend(["eight_leaf_output_identical", "no_per_partition_root_scan", "one_full_validation_per_dirty_leaf", "initial_state_built_once_per_frequency"])

(OUT / "function_checks.log").write_text(log.getvalue(), encoding="utf-8")
summary = {"checks": checks, "measurements": measurements, "real_api_calls": 0, "formal_lake_writes": 0}
(OUT / "results.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2))
