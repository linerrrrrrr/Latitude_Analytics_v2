"""a02/b01 校验/转换次数、分区物化次数和行为差分；仅使用有界临时数据。"""
import ast
import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01_exchange_report_calendar.py'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


old = load('calendar_before_validation_io', SNAPSHOT / PATH.name)
new = load('calendar_after_validation_io', PATH)
now = datetime(2026, 9, 1, tzinfo=timezone.utc)


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return now


old.datetime = new.datetime = FrozenDatetime


def quiet(function, *args, **kwargs):
    with contextlib.redirect_stdout(io.StringIO()):
        return function(*args, **kwargs)


def calendar_table(frame):
    return new.pandas_to_arrow(frame, new.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA)


def read_calendar(lake):
    return ds.dataset(lake / 'silver' / new.TABLE_NAME, format='parquet', partitioning=new.REPORT_PARTITIONING).to_table().select(new.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names).sort_by([(name, 'ascending') for name in new.PRIMARY_KEY])


def write_upstream(lake, frame):
    ds.write_dataset(new.pandas_to_arrow(frame, new.FUTURES_VARIETY_CALENDAR_SCHEMA),
                     lake / 'silver' / new.UPSTREAM_TABLE_NAME, format='parquet', partitioning=new.UPSTREAM_PARTITIONING)


checks = []
rows = [dict(exchange_code=exchange, underlying_code=code, trading_date=day,
             active_contract_count=1, source='fixture', updated_at=now, year=day.year, month=day.month)
        for exchange, code in [('XSGE', 'RB'), ('CCFX', 'IF')]
        for day in [date(2024, 1, 3), date(2024, 1, 4), date(2024, 2, 1)]]
upstream = new.arrow_to_pandas(pa.Table.from_pylist(rows, schema=new.FUTURES_VARIETY_CALENDAR_SCHEMA), new.FUTURES_VARIETY_CALENDAR_SCHEMA)
empty = new.empty_pandas(new.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA)
with patch.object(new, 'pandas_to_arrow', wraps=new.pandas_to_arrow) as conversions, patch.object(new, 'validate_report_calendar_table', wraps=new.validate_report_calendar_table) as business:
    expected = quiet(new.build_expected_calendar, upstream, empty, now)
    assert conversions.call_count == business.call_count == 1
checks.append('generation_one_conversion_one_business_validation')

# 摘要必须保留原格式；用分片、空值、重排和非连续索引比较两版。
for frame in [expected, expected.iloc[::-1], expected.iloc[[0, 3, 7]], empty]:
    table = calendar_table(frame)
    fragmented = pa.concat_tables([table.slice(0, 1), table.slice(1)])
    assert old.table_digest(frame) == new.table_digest(fragmented)
checks.append('canonical_digest_unchanged_with_nulls_and_fragments')
edited = expected.copy()
edited.loc[0, 'requirement_reason'] = '本次核对的已变更政策说明。'
edited = pd.concat([edited.iloc[2:], edited.iloc[:2]], ignore_index=False)
assert quiet(old.changed_partition_keys, edited, expected) == quiet(new.changed_partition_keys, edited, expected)
assert quiet(new.changed_partition_keys, empty, empty) == []
checks.append('partition_indices_handle_nonconsecutive_order_and_empty_frames')

metrics = {}
with tempfile.TemporaryDirectory(prefix='a02-b01-io-check-') as temporary:
    base = pathlib.Path(temporary)
    for label, module in [('before', old), ('after', new)]:
        lake = base / label
        keys = quiet(module.changed_partition_keys, expected, empty)
        reads = []
        real_open = module.open_exact_dataset
        class CountingDataset:
            def __init__(self, dataset, name):
                self.dataset, self.name = dataset, name
            def to_table(self, *args, **kwargs):
                table = self.dataset.to_table(*args, **kwargs)
                reads.append((self.name, table.num_rows))
                return table
            def __getattr__(self, name):
                return getattr(self.dataset, name)
        def counted_open(*args, **kwargs):
            return CountingDataset(real_open(*args, **kwargs), args[3])
        started = time.perf_counter()
        with patch.object(module, 'open_exact_dataset', side_effect=counted_open), patch.object(module, 'validate_report_calendar_table', wraps=module.validate_report_calendar_table) as validations, patch.object(module, 'pandas_to_arrow', wraps=module.pandas_to_arrow) as conversions:
            count = quiet(module.commit_partitions, expected, keys, lake)
        metrics[label] = dict(partitions=len(keys), business_validations=validations.call_count,
                              pandas_to_arrow_calls=conversions.call_count,
                              dataset_reads=len(reads), materialized_rows=sum(n for _, n in reads),
                              commit_rows=count, elapsed_s=round(time.perf_counter() - started, 3))
    assert read_calendar(base / 'before').equals(read_calendar(base / 'after'), check_metadata=True)
    assert metrics['before']['dataset_reads'] == len(keys) + 2
    assert metrics['after']['dataset_reads'] == 2
    assert metrics['after']['business_validations'] == 0
    assert metrics['after']['pandas_to_arrow_calls'] == 1
    checks.append('commit_same_values_with_two_dataset_reads_and_no_business_revalidation')

    # 同时覆盖增量、删除和同月范围外行保留，并核对每次入口只校验生成结果。
    runner = CliRunner()
    explicit_snapshots = []
    for label, module in [('old_explicit', old), ('new_explicit', new)]:
        lake = base / label
        # 当前上游少了 1 月 3 日，但显式 1 月 3 日范围外的 1 月 4 日及 2 月旧行要保留。
        scoped_upstream = upstream.loc[upstream.trading_date.ne(date(2024, 1, 3))]
        write_upstream(lake, scoped_upstream)
        ds.write_dataset(calendar_table(expected), lake / 'silver' / new.TABLE_NAME, format='parquet', partitioning=new.REPORT_PARTITIONING)
        with patch.object(module, 'validate_report_calendar_table', wraps=module.validate_report_calendar_table) as validations:
            result = runner.invoke(module.main, ['--lake-root', str(lake), '--start-date', '2024-01-03', '--end-date', '2024-01-03', '--write'])
        assert result.exit_code == 0, (label, result.exception, result.output)
        if module is new:
            assert validations.call_count == 1
            assert 'rows=12; persisted=true' in result.output
        explicit_snapshots.append(read_calendar(lake))
    assert explicit_snapshots[0].equals(explicit_snapshots[1], check_metadata=True)
    checks.append('explicit_deletion_preserves_same_month_and_other_month_rows')

    lake = base / 'main_noop'
    write_upstream(lake, upstream)
    ds.write_dataset(calendar_table(expected), lake / 'silver' / new.TABLE_NAME, format='parquet', partitioning=new.REPORT_PARTITIONING)
    with patch.object(new, 'validate_report_calendar_table', wraps=new.validate_report_calendar_table) as validations:
        result = runner.invoke(new.main, ['--lake-root', str(lake)])
    assert result.exit_code == 0 and validations.call_count == 1
    assert 'outcome=no_changes' in result.output
    checks.append('existing_and_upstream_skip_redundant_business_validation')

    # 受控修改复读返回值，证明内容摘要仍拦截业务规则合法但不等于期望的值。
    real_open = new.open_exact_dataset
    class AlteredReadback:
        def __init__(self, dataset):
            self.dataset = dataset
        def to_table(self, *args, **kwargs):
            table = self.dataset.to_table(*args, **kwargs)
            counts = table['actual_record_count'].to_pylist()
            counts[0] += 1
            index = table.schema.get_field_index('actual_record_count')
            return table.set_column(index, table.schema.field(index), pa.array(counts, type=pa.int32()))
    def altered_open(*args, **kwargs):
        dataset = real_open(*args, **kwargs)
        return AlteredReadback(dataset) if args[3] == '报告日历 staging' else dataset
    with patch.object(new, 'open_exact_dataset', side_effect=altered_open):
        try:
            quiet(new.commit_partitions, expected, keys, base / 'altered')
        except ValueError as error:
            assert 'staging 叶分区内容与期望不一致' in str(error)
        else:
            raise AssertionError('staging content mismatch was accepted')
    assert not (base / 'altered' / 'silver' / new.TABLE_NAME).exists()
    checks.append('staging_full_content_guard_preserved')

# 确认分区循环没有恢复全表 mask 或重新执行契约转换。
tree = ast.parse(PATH.read_text(encoding='utf-8'))
for function in [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('changed_partition_keys', 'commit_partitions')]:
    for loop in [n for n in ast.walk(function) if isinstance(n, ast.For) and ast.unparse(n.target) == 'partition_key']:
        loop_source = ast.unparse(loop)
        assert 'pd.Series(' not in loop_source and 'pandas_to_arrow(' not in loop_source
checks.append('no_full_frame_masks_or_conversions_in_partition_loops')
report = dict(checks_passed=len(checks), checks=checks, commit_metrics=metrics)
(SNAPSHOT / 'validation_io_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(report, ensure_ascii=False, indent=2))
