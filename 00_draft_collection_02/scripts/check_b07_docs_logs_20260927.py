"""用已有 fixture 和临时 Parquet 核对 b07 入口日志及只读/提交边界。"""
from __future__ import annotations

import pathlib
import sys
import tempfile
from unittest import mock

from click.testing import CliRunner
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "00_draft_collection_02/tests"))
import test_b01_ohlc_retention as fixtures

b07 = fixtures.reconciliation
runner = CliRunner()


def invoke(lake_root, *args):
    return runner.invoke(b07.main, ["--lake-root", str(lake_root), *args])


with tempfile.TemporaryDirectory(prefix="b07-log-smoke-") as temporary:
    lake_root = pathlib.Path(temporary)
    candidate = fixtures.calendar_row(
        bar_frequency="1m", quality_status="warning",
        quality_reason="0 行，等待旁证。",
        evidence_source="fact_futures_minute:formal_empty_session",
    )
    candidate.update(schedule_status="suspected_closed", actual_bar_count=0,
                     is_data_missing=True, missing_bar_count=1)
    retained = fixtures.calendar_row(
        bar_frequency="1m", quality_status="passed",
        quality_reason="其他 Session 完整。",
        evidence_source="fact_futures_minute:formal_nonempty_session",
    )
    retained.update(session_number=2, session_text="09:01-09:02",
                    session_start_at=pd.Timestamp("2026-08-03 09:01", tz="Asia/Shanghai"),
                    session_end_at=pd.Timestamp("2026-08-03 09:02", tz="Asia/Shanghai"))
    contracts = [fixtures.contract_row(), fixtures.contract_row()]
    for name in ("session_number", "session_text", "session_start_at", "session_end_at"):
        contracts[1][name] = retained[name]
    minute = fixtures.minute_row()
    minute.update(session_number=2, bar_at=retained["session_end_at"], high=101.0)
    tables = [
        ([candidate, retained], b07.FUTURES_BAR_CALENDAR_SCHEMA, b07.CALENDAR_PARTITION_COLUMNS),
        (contracts, b07.FUTURES_CONTRACT_CALENDAR_SCHEMA, b07.CONTRACT_PARTITION_COLUMNS),
        ([fixtures.daily_row(invalid_ohlc=False)], b07.FUTURES_DAILY_SCHEMA, b07.FACT_PARTITION_COLUMNS),
        ([minute], b07.FUTURES_MINUTE_SCHEMA, b07.FACT_PARTITION_COLUMNS),
    ]
    for rows, schema, partitions in tables:
        fixtures.write_partitioned(pd.DataFrame(rows), lake_root / "silver" / schema.metadata[b"table_name"].decode(), schema, partitions)
    original_files = {str(p.relative_to(lake_root)): p.read_bytes() for p in lake_root.rglob("*.parquet")}

    readonly = invoke(lake_root)
    assert readonly.exit_code == 0, readonly.output
    assert "reconciled=1; warning=1" in readonly.output, readonly.output
    assert "outcome=read_only" in readonly.output and "persisted=false" in readonly.output
    assert "committed:" not in readonly.output
    assert original_files == {str(p.relative_to(lake_root)): p.read_bytes() for p in lake_root.rglob("*.parquet")}

    with mock.patch.object(b07.ds, "write_dataset", side_effect=RuntimeError("injected staging failure")):
        failed = invoke(lake_root, "--write")
    assert isinstance(failed.exception, RuntimeError), failed.output
    assert "phase=commit; status=started" in failed.output
    assert "committed:" not in failed.output and "Reconciliation run ended" not in failed.output
    assert original_files == {str(p.relative_to(lake_root)): p.read_bytes() for p in lake_root.rglob("*.parquet")}

    written = invoke(lake_root, "--write")
    assert written.exit_code == 0, written.output
    assert "committed_partitions=1; committed_calendar_rows=2" in written.output
    assert "outcome=committed" in written.output and "persisted=true" in written.output
    calendar_dataset = b07.open_exact_dataset(
        lake_root / "silver" / b07.CALENDAR_TABLE_NAME, b07.CALENDAR_PARTITIONING,
        b07.FUTURES_BAR_CALENDAR_SCHEMA, "temporary calendar",
    )
    calendar_df = b07.arrow_to_pandas(
        calendar_dataset.to_table(columns=b07.FUTURES_BAR_CALENDAR_SCHEMA.names),
        b07.FUTURES_BAR_CALENDAR_SCHEMA,
    )
    stored = calendar_df.loc[calendar_df.session_number.eq(1)].iloc[0]
    assert stored.evidence_level == "reconciled" and stored.quality_status == "warning"
    assert stored.schedule_status == "suspected_closed" and stored.is_fetch_required
    assert stored.actual_bar_count == 0 and stored.missing_bar_count == 1

    with mock.patch.object(b07, "open_exact_dataset", wraps=b07.open_exact_dataset) as opens:
        empty = invoke(lake_root, "--write")
    assert empty.exit_code == 0 and "outcome=no_candidates" in empty.output, empty.output
    assert opens.call_count == 1
    assert "committed:" not in empty.output

    forced = invoke(lake_root, "--force", "--contract-code", fixtures.CONTRACT_CODE)
    assert forced.exit_code == 0 and "mode=force" in forced.output, forced.output
    assert "changed_candidates=1" in forced.output and "outcome=read_only" in forced.output

    for result in (readonly, written, empty, forced):
        assert result.output.splitlines().count("=" * 88) == 4
        assert "elapsed_s=" in result.output
    print("CLI smoke passed: readonly unchanged, failure without success log, 1-candidate/2-row commit, no-candidate early exit, bounded force.")
    print("Example committed output:")
    print(written.output)
