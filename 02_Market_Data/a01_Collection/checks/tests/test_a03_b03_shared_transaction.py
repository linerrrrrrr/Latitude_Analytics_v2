"""境外期货独立事实/日历事务；临时湖、模拟 API 和显式故障注入。"""
import contextlib
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
import pandas as pd
from click.testing import CliRunner

import test_b03_metadata_upgrade as fixtures

module = fixtures.load_notebook_module(fixtures.C03_NOTEBOOK_PATH, fixtures.C03_SKIPPED_CELL_IDS, 'overseas_transaction')
transaction_module = sys.modules[module.StagedPathTransaction.__module__]


class OverseasTransactionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='overseas-tx-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name).resolve()
        self.silver = self.root/'silver'
        self.fact_path = self.silver/module.TABLE_NAME
        self.calendar_path = self.silver/module.CALENDAR_TABLE_NAME
        self.day = date(2026, 7, 31)
        self.now = datetime.now(timezone.utc)-timedelta(seconds=1)
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)
        self.fact = fixtures.build_overseas_fact_frame(module)
        self.calendar = fixtures.build_completed_overseas_calendar(module, self.fact)
        self.july = self.fact.loc[self.fact['month'].eq(7)].copy()

    def seed(self):
        fixtures.write_partitioned_table(module, self.fact, module.OVERSEAS_FUTURES_DAILY_SCHEMA, module.PARTITION_COLUMNS, self.fact_path)
        fixtures.write_partitioned_table(module, self.calendar, module.EXTERNAL_MARKET_CALENDAR_SCHEMA, module.CALENDAR_PARTITION_COLUMNS, self.calendar_path)

    def leaf(self, *, calendar=False, month=7):
        return (self.calendar_path/f'dataset_name={module.DATASET_NAME}' if calendar else self.fact_path)/f'year=2026/month={month}'

    def fail_read(self, label, ordinal=1):
        original = module.open_exact_dataset
        calls = 0
        def opened(*args, **kwargs):
            nonlocal calls
            if args[3] == label:
                calls += 1
                if calls == ordinal:
                    raise ValueError('injected readback rejection')
            return original(*args, **kwargs)
        return patch.object(module, 'open_exact_dataset', side_effect=opened)

    def update_calendar(self):
        return module.apply_calendar_completion(self.calendar, module.grid_count_map(self.fact), {}, 'next-run', self.now)

    def assert_no_temporary(self):
        self.assertFalse(list(self.silver.glob('.*.staging-*')))
        self.assertFalse(list(self.silver.glob('.*.backup-*')))

    def test_fact_success_keeps_other_month_and_existing_marker(self):
        self.seed()
        other = fixtures.parquet_hashes(self.leaf(month=8))
        marker = (self.fact_path/'schema.parquet').read_bytes()
        self.july.loc[:, 'close'] = 100.5
        result = module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
        self.assertEqual(float(result.iloc[0]['close']), 100.5)
        self.assertEqual(other, fixtures.parquet_hashes(self.leaf(month=8)))
        self.assertEqual(marker, (self.fact_path/'schema.parquet').read_bytes())
        self.assert_no_temporary()

    def test_empty_fact_deletes_only_current_leaf_and_new_empty_lake_has_marker(self):
        self.seed()
        other = fixtures.parquet_hashes(self.leaf(month=8))
        result = module.commit_complete_fact_partition(module.empty_pandas(module.OVERSEAS_FUTURES_DAILY_SCHEMA), self.root, (2026, 7))
        self.assertTrue(result.empty)
        self.assertFalse(self.leaf().exists())
        self.assertEqual(other, fixtures.parquet_hashes(self.leaf(month=8)))
        empty_root = self.root/'new-lake'
        result = module.commit_complete_fact_partition(module.empty_pandas(module.OVERSEAS_FUTURES_DAILY_SCHEMA), empty_root, (2026, 7))
        self.assertTrue(result.empty)
        self.assertEqual(len(list((empty_root/'silver'/module.TABLE_NAME).rglob('*.parquet'))), 1)
        self.assert_no_temporary()

    def test_first_backup_failure_never_removes_old_fact_or_calendar(self):
        self.seed()
        for calendar in (False, True):
            with self.subTest(calendar=calendar):
                before = fixtures.parquet_hashes(self.silver)
                original = transaction_module.os.replace
                leaf = self.leaf(calendar=calendar)
                def replace(source, destination):
                    if pathlib.Path(source) == leaf:
                        raise OSError('injected first backup failure')
                    return original(source, destination)
                with patch.object(transaction_module.os, 'replace', side_effect=replace):
                    with self.assertRaisesRegex(OSError, 'first backup'):
                        if calendar:
                            module.commit_calendar_partitions(self.update_calendar(), {self.day}, self.root)
                        else:
                            module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
                self.assertEqual(before, fixtures.parquet_hashes(self.silver))
                self.assert_no_temporary()

    def test_install_failure_restores_old_leaf(self):
        self.seed()
        before = fixtures.parquet_hashes(self.silver)
        original = transaction_module.os.replace
        def replace(source, destination):
            if '.staging-' in str(source):
                raise OSError('injected install failure')
            return original(source, destination)
        with patch.object(transaction_module.os, 'replace', side_effect=replace):
            with self.assertRaisesRegex(OSError, 'install failure'):
                module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
        self.assertEqual(before, fixtures.parquet_hashes(self.silver))
        self.assert_no_temporary()

    def test_formal_failure_restores_old_fact_and_keeps_failed_new_leaf(self):
        self.seed()
        before = fixtures.parquet_hashes(self.fact_path)
        with self.fail_read('正式境外期货事实'), self.assertRaises(RuntimeError) as error:
            module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
        self.assertIsInstance(error.exception.__cause__, ValueError)
        self.assertEqual(before, fixtures.parquet_hashes(self.fact_path))
        self.assertTrue(list(self.silver.glob('.'+module.TABLE_NAME+'.failed-*/year=2026/month=7/*.parquet')))
        self.assert_no_temporary()

    def test_new_fact_marker_is_rolled_back_with_new_leaf(self):
        with self.fail_read('新建正式事实零行标记'), self.assertRaises(RuntimeError):
            module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
        self.assertEqual(fixtures.parquet_hashes(self.fact_path), {})
        self.assertTrue(list(self.silver.glob('.'+module.TABLE_NAME+'.failed-*/year=2026/month=7/*.parquet')))
        self.assertFalse(list(self.silver.glob('.*.failed-*/schema.parquet')))
        self.assert_no_temporary()

    def test_empty_deletion_failure_restores_old_leaf(self):
        self.seed()
        before = fixtures.parquet_hashes(self.fact_path)
        with self.fail_read('正式境外期货事实'), self.assertRaises(ValueError):
            module.commit_complete_fact_partition(module.empty_pandas(module.OVERSEAS_FUTURES_DAILY_SCHEMA), self.root, (2026, 7))
        self.assertEqual(before, fixtures.parquet_hashes(self.fact_path))
        self.assert_no_temporary()

    def test_staging_and_enter_failures_do_not_touch_formal_files(self):
        self.seed()
        before = fixtures.parquet_hashes(self.silver)
        with self.fail_read('境外期货 staging'), patch.object(module, 'StagedPathTransaction') as transaction:
            with self.assertRaises(ValueError):
                module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
            transaction.assert_not_called()
        with patch.object(module.StagedPathTransaction, '__enter__', side_effect=OSError('injected enter failure')):
            with self.assertRaises(OSError):
                module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
        self.assertEqual(before, fixtures.parquet_hashes(self.silver))
        self.assert_no_temporary()

    def test_equal_keys_and_rows_but_different_value_is_rejected(self):
        self.seed()
        for label in ('境外期货 staging', '正式境外期货事实'):
            with self.subTest(label=label):
                before = fixtures.parquet_hashes(self.fact_path)
                original = module.open_exact_dataset
                def changed_read(*args, **kwargs):
                    dataset = original(*args, **kwargs)
                    if args[3] != label:
                        return dataset
                    def changed_table(**read_kwargs):
                        table = dataset.to_table(**read_kwargs)
                        index = table.schema.get_field_index('close')
                        return table.set_column(index, table.schema.field(index), module.pa.array([100.25]*table.num_rows, type=module.pa.float64()))
                    return SimpleNamespace(to_table=changed_table)
                with patch.object(module, 'open_exact_dataset', side_effect=changed_read):
                    with self.assertRaises((ValueError, RuntimeError)):
                        module.commit_complete_fact_partition(self.july, self.root, (2026, 7))
                self.assertEqual(before, fixtures.parquet_hashes(self.fact_path))
                self.assert_no_temporary()

    def test_second_calendar_leaf_failure_keeps_first_success_and_marker(self):
        self.seed()
        second = fixtures.parquet_hashes(self.leaf(calendar=True, month=8))
        marker = (self.calendar_path/'schema.parquet').read_bytes()
        with self.fail_read('正式外部市场日历', 2), self.assertRaises(RuntimeError):
            module.commit_calendar_partitions(self.update_calendar(), set(self.fact['snapshot_date']), self.root)
        self.assertEqual(second, fixtures.parquet_hashes(self.leaf(calendar=True, month=8)))
        self.assertEqual(marker, (self.calendar_path/'schema.parquet').read_bytes())
        read = module.ds.dataset(self.calendar_path, format='parquet', partitioning=module.CALENDAR_PARTITIONING).to_table().to_pandas().set_index('month')
        self.assertEqual(read.loc[7, 'fetch_run_id'], 'next-run')
        self.assertEqual(read.loc[8, 'fetch_run_id'], 'already-complete')
        self.assert_no_temporary()

    def test_dirty_calendar_duplicate_and_invalid_status_fail_before_staging(self):
        self.seed()
        before = fixtures.parquet_hashes(self.silver)
        duplicate = pd.concat([self.calendar, self.calendar.iloc[[0]]], ignore_index=True)
        invalid = self.calendar.copy()
        invalid.loc[invalid['month'].eq(7), 'fetch_result_status'] = 'invalid'
        for frame in (duplicate, invalid):
            with patch.object(module, 'StagedPathTransaction') as transaction:
                with self.assertRaises(ValueError):
                    module.commit_calendar_partitions(frame, {self.day}, self.root)
                transaction.assert_not_called()
        self.assertEqual(before, fixtures.parquet_hashes(self.silver))

    def test_calendar_failure_keeps_fact_and_next_run_repairs_without_api(self):
        pending = self.calendar.iloc[[0]].copy()
        pending['is_fetch_completed'] = False
        pending['fetch_result_status'] = 'pending'
        pending['quality_status'] = 'pending'
        pending['actual_record_count'] = 0
        pending['fetch_run_id'] = None
        pending['fetch_completed_at'] = None
        pending['quality_checked_at'] = None
        fixtures.write_partitioned_table(module, pending, module.EXTERNAL_MARKET_CALENDAR_SCHEMA, module.CALENDAR_PARTITION_COLUMNS, self.calendar_path)
        before = fixtures.parquet_hashes(self.calendar_path)
        raw = pd.DataFrame([dict(id='july', code='JULY', name='测试', day=self.day, open=100.0, high=102.0, low=99.0,
                                close=101.0, volume=10.0, change_pct=1.0, amplitude=3.0, pre_close=None)])
        with patch.object(module, 'authenticate_jqdata', return_value=object()), patch.object(module, 'query_overseas_futures_grid', return_value=raw) as query, self.fail_read('正式外部市场日历', 2):
            result = CliRunner().invoke(module.main, ['--lake-root', str(self.root), '--write'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(query.call_count, 1)
        self.assertEqual(before, fixtures.parquet_hashes(self.calendar_path))
        fact_before = fixtures.parquet_hashes(self.fact_path)
        self.assertTrue(fact_before)
        with patch.object(module, 'authenticate_jqdata', side_effect=AssertionError('no API repair')):
            result = CliRunner().invoke(module.main, ['--lake-root', str(self.root), '--write'])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn('outcome=state_repaired', result.output)
        self.assertEqual(fact_before, fixtures.parquet_hashes(self.fact_path))

    def test_entry_notebook_script_and_import(self):
        notebook = nbformat.read(fixtures.C03_NOTEBOOK_PATH, as_version=4)
        entry = next(c.source for c in notebook.cells if c.id=='b03-c03-21')
        for kernel, has_file, name, expected in (
            (True, False, '__main__', 'notebook'), (True, True, 'imported', 'none'),
            (False, True, 'imported', 'none'), (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, name=name):
                command = Mock()
                namespace = {'sys': SimpleNamespace(modules={'ipykernel': object()} if kernel else {}), 'main': command, '__name__': name}
                if has_file: namespace['__file__'] = 'c03_overseas_futures.py'
                exec(entry, namespace)
                if expected=='notebook':
                    command.main.assert_called_once_with(args=[], prog_name='c03_overseas_futures', standalone_mode=False)
                    command.assert_not_called()
                elif expected=='script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    command.assert_not_called()
                    command.main.assert_not_called()

    def test_same_month_state_repair_then_fetch_retains_both_days(self):
        self.seed()
        extra_day = date(2026, 7, 30)
        extra = self.calendar.iloc[[0]].copy()
        extra['observation_date'] = extra_day
        extra['is_fetch_completed'] = False
        extra['fetch_result_status'] = 'pending'
        extra['quality_status'] = 'pending'
        extra['actual_record_count'] = 0
        extra['fetch_run_id'] = None
        extra['fetch_completed_at'] = None
        extra['quality_checked_at'] = None
        calendar = pd.concat([self.calendar, extra], ignore_index=True)
        calendar.loc[calendar['observation_date'].eq(self.day), 'quality_reason'] = 'stale reason'
        # 直接更新临时日历叶，构造同月一个修复日期加一个 API 待办日期。
        module.ds.write_dataset(module.pandas_to_arrow(calendar, module.EXTERNAL_MARKET_CALENDAR_SCHEMA), self.calendar_path,
                                format='parquet', partitioning=module.CALENDAR_PARTITIONING,
                                existing_data_behavior='delete_matching', basename_template='part-{i}.parquet')
        raw = pd.DataFrame([dict(id='july-extra', code='JULY', name='测试', day=extra_day, open=100.0, high=102.0,
                                low=99.0, close=101.0, volume=10.0, change_pct=1.0, amplitude=3.0, pre_close=None)])
        with patch.object(module, 'authenticate_jqdata', return_value=object()), patch.object(module, 'query_overseas_futures_grid', return_value=raw) as query:
            result = CliRunner().invoke(module.main, ['--lake-root', str(self.root), '--write'])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(query.call_args.args[1], extra_day)
        self.assertEqual(query.call_count, 1)
        read = module.ds.dataset(self.calendar_path, format='parquet', partitioning=module.CALENDAR_PARTITIONING).to_table().to_pandas().set_index('observation_date')
        self.assertTrue(read.loc[self.day, 'fetch_run_id'].startswith('state-repair-'))
        self.assertFalse(read.loc[extra_day, 'fetch_run_id'].startswith('state-repair-'))
        self.assertTrue(read['is_fetch_completed'].all())
        self.assertEqual(len(module.read_optional_fact(self.fact_path)), 3)


if __name__=='__main__':
    unittest.main()
