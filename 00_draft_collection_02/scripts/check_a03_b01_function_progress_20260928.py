"""函数直接调用的进度、失败与零输入日志；临时/内存数据，无 API。"""
import contextlib
import importlib.util
import io
import itertools
import json
import pathlib
import sys
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
SOURCE = ROOT/'02_Futures_Lakehouse/a03_External_Market_Data/b01_external_market_calendar.py'
spec = importlib.util.spec_from_file_location('external_calendar_progress_check', SOURCE)
calendar = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calendar)
snapshot = pathlib.Path(sys.argv[1])
empty = calendar.empty_pandas(calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA)
now = datetime.now(timezone.utc) - timedelta(seconds=5)
rows = []
for offset in range(1000):
    day = date(2024, 1, 1) + timedelta(days=offset)
    rows.append(dict(calendar_date=day, date_key=day.strftime('%Y%m%d'),
                     weekday=day.isoweekday(), is_weekend=day.isoweekday() >= 6,
                     is_trading_day=day.isoweekday() < 6, source='JQData_get_trade_days',
                     calendar_name='CN_FUTURES_MARKET', calendar_timezone='Asia/Shanghai',
                     effective_after=time(20), updated_at=now, year=day.year))
upstream = calendar.arrow_to_pandas(calendar.pa.Table.from_pylist(rows, schema=calendar.TRADE_CALENDAR_SCHEMA), calendar.TRADE_CALENDAR_SCHEMA)
log = io.StringIO()
with contextlib.redirect_stdout(log), patch.object(calendar.time, 'perf_counter', side_effect=itertools.count(0, 3)):
    generated = calendar.build_expected_calendar(upstream, empty, now)
text = log.getvalue()
assert len(generated) == 1000 * (2 + len(calendar.EXTERNAL_INDEX_ENTITIES))
assert 'processed_dates=1000;' in text
assert 'generated_rows=21000;' in text
assert 1 <= text.count('function=build_expected_calendar; phase=generate; status=running;') <= 10
assert 'function=validate_external_calendar_table; phase=validate; status=running;' in text
assert 'persisted=true' not in text
(snapshot/'direct-generation-progress.log').write_text(text, encoding='utf8')

log = io.StringIO()
with contextlib.redirect_stdout(log):
    calendar.build_expected_calendar(calendar.empty_pandas(calendar.TRADE_CALENDAR_SCHEMA), empty, now)
assert 'processed_dates=0; generated_rows=0;' in log.getvalue()

log = io.StringIO()
with contextlib.redirect_stdout(log), patch.object(calendar, 'validate_external_calendar_table', side_effect=ValueError('injected output validation failure')):
    try:
        calendar.build_expected_calendar(upstream.iloc[:1], empty, now)
    except ValueError as error:
        assert str(error) == 'injected output validation failure'
    else:
        raise AssertionError('output validation must propagate')
assert 'failed_phase=output_validation;' in log.getvalue()
assert 'phase=generate; status=completed;' not in log.getvalue()

with tempfile.TemporaryDirectory(prefix='ext-progress-') as directory:
    root = pathlib.Path(directory)
    log = io.StringIO()
    with contextlib.redirect_stdout(log):
        assert calendar.commit_partitions(empty, [], root) == 0
        try:
            calendar.open_exact_dataset(root/'absent', calendar.CALENDAR_PARTITIONING, calendar.EXTERNAL_MARKET_CALENDAR_SCHEMA, 'test absent')
        except FileNotFoundError:
            pass
        else:
            raise AssertionError('missing dataset must propagate')
    text = log.getvalue()
    assert 'calendar_state=unchanged; persisted=false; date_watermark=none' in text
    assert 'function=open_compatible_dataset; phase=dataset_open; status=failed;' in text
    assert 'function=open_exact_dataset; phase=dataset_open; status=failed;' in text
    assert 'partition_committed:' not in text and 'persisted=true' not in text
    assert not list(root.iterdir())
report = dict(direct_progress=True, generation_rows=len(generated), empty_input=True,
              generation_failure=True, missing_dataset_failure=True, skipped_commit_no_io=True,
              mocked_clock='仅推动限频分支，不是生产耗时测量', real_api_calls=0, formal_lake_writes=0)
(snapshot/'function_progress_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
