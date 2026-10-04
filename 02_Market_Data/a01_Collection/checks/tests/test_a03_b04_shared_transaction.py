"""外部指数路径事务：临时湖、模拟请求和显式故障；不访问正式湖。"""
import contextlib
import io
import pathlib
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
from click.testing import CliRunner

import test_b03_metadata_upgrade as fixtures

NOTEBOOK = fixtures.B03_ROOT/'c04_external_index.ipynb'
module = fixtures.load_notebook_module(NOTEBOOK, {'b03-c04-05', 'b03-c04-21'}, 'index_transaction')
transaction_module = sys.modules[module.StagedPathTransaction.__module__]


def make_frames(owner=module):
    now = datetime(2026, 8, 4, tzinfo=timezone.utc)
    requests = [(owner.EXTERNAL_INDEX_ENTITIES[0], date(2026, 7, 2)),
                (owner.EXTERNAL_INDEX_ENTITIES[0], date(2026, 8, 3)),
                (owner.EXTERNAL_INDEX_ENTITIES[6], date(2026, 7, 2))]
    facts, calendar_rows = [], []
    for entity, day in requests:
        facts.append(owner.normalize_external_index_response(
            [{'INDICATOR_ID':entity.source_indicator_id, 'REPORT_DATE':str(day), 'INDICATOR_VALUE':123.5}],
            entity, day, day, {day}, now))
        calendar_rows.append(dict(dataset_name=owner.DATASET_NAME, entity_code=entity.source_indicator_id,
            observation_date=day, is_fetch_required=True, requirement_reason='fixture required',
            is_fetch_completed=False, fetch_result_status='pending', is_data_missing=False,
            actual_record_count=0, quality_status='pending', quality_reason='fixture pending',
            fetch_run_id=None, fetch_completed_at=None, quality_checked_at=None,
            updated_at=now, year=day.year, month=day.month))
    calendar_df = pd.DataFrame(calendar_rows)
    if hasattr(owner, 'validate_calendar_table'):
        calendar_table = owner.validate_calendar_table(owner.pandas_to_arrow(calendar_df, owner.EXTERNAL_MARKET_CALENDAR_SCHEMA), 'fixture')
        calendar_df = owner.arrow_to_pandas(calendar_table, owner.EXTERNAL_MARKET_CALENDAR_SCHEMA)
    else:
        calendar_df = owner.validate_calendar_frame(calendar_df, 'fixture')
    return pd.concat(facts, ignore_index=True), calendar_df


class ExternalIndexTransactionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='external-index-tx-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name).resolve()
        self.silver = self.root/'silver'
        self.fact_path = self.silver/module.TABLE_NAME
        self.calendar_path = self.silver/module.CALENDAR_TABLE_NAME
        self.day = date(2026, 7, 2)
        self.now = datetime.now(timezone.utc)-timedelta(seconds=1)
        self.entity = module.EXTERNAL_INDEX_ENTITIES[0]
        self.key = ('shipping', 2026, 7)
        self.grid = (self.entity.source_indicator_id, self.day)
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)
        self.fact, self.pending = make_frames()
        self.calendar = module.apply_calendar_completion(self.pending, module.grid_count_map(self.fact), 'prior', self.now)
        self.current = self.fact.loc[self.fact['index_category'].eq('shipping') & self.fact['month'].eq(7)].copy()
        self.current.loc[:, 'index_value'] = 456.25

    def seed(self):
        fixtures.write_partitioned_table(module, self.fact, module.EXTERNAL_INDEX_DAILY_SCHEMA, module.PARTITION_COLUMNS, self.fact_path)
        fixtures.write_partitioned_table(module, self.calendar, module.EXTERNAL_MARKET_CALENDAR_SCHEMA, module.CALENDAR_PARTITION_COLUMNS, self.calendar_path)

    def leaf(self, *, calendar=False, month=7):
        return (self.calendar_path/f'dataset_name={module.DATASET_NAME}' if calendar else self.fact_path/'index_category=shipping')/f'year=2026/month={month}'

    def updated_calendar(self):
        return module.apply_calendar_completion(self.calendar, module.grid_count_map(self.fact), 'next', self.now)

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

    def assert_clean(self):
        self.assertFalse(list(self.silver.glob('.*.staging-*')))
        self.assertFalse(list(self.silver.glob('.*.backup-*')))

    def test_fact_success_preserves_other_leaves_and_marker(self):
        self.seed()
        other = fixtures.parquet_hashes(self.leaf(month=8))
        category = fixtures.parquet_hashes(self.fact_path/'index_category=energy')
        marker = (self.fact_path/'schema.parquet').read_bytes()
        result = module.commit_complete_fact_partition(self.current, self.root, self.key)
        self.assertEqual(float(result.iloc[0]['index_value']), 456.25)
        self.assertEqual(other, fixtures.parquet_hashes(self.leaf(month=8)))
        self.assertEqual(category, fixtures.parquet_hashes(self.fact_path/'index_category=energy'))
        self.assertEqual(marker, (self.fact_path/'schema.parquet').read_bytes())
        self.assert_clean()

    def test_empty_result_deletes_only_target_and_empty_new_lake_is_readable(self):
        self.seed()
        other = fixtures.parquet_hashes(self.leaf(month=8))
        empty = module.empty_pandas(module.EXTERNAL_INDEX_DAILY_SCHEMA)
        self.assertTrue(module.commit_complete_fact_partition(empty, self.root, self.key).empty)
        self.assertFalse(self.leaf().exists())
        self.assertEqual(other, fixtures.parquet_hashes(self.leaf(month=8)))
        new_root = self.root/'new'
        self.assertTrue(module.commit_complete_fact_partition(empty, new_root, self.key).empty)
        self.assertTrue(module.read_optional_fact(new_root/'silver'/module.TABLE_NAME).empty)
        self.assert_clean()

    def test_first_backup_failure_preserves_old_fact_and_calendar(self):
        self.seed()
        for calendar in (False, True):
            with self.subTest(calendar=calendar):
                before = fixtures.parquet_hashes(self.silver)
                original = transaction_module.os.replace
                def replace(source, destination):
                    if pathlib.Path(source) == self.leaf(calendar=calendar):
                        raise OSError('injected first backup failure')
                    return original(source, destination)
                with patch.object(transaction_module.os, 'replace', side_effect=replace), self.assertRaisesRegex(OSError, 'first backup'):
                    if calendar:
                        module.commit_calendar_partitions(self.updated_calendar(), {self.grid}, self.root)
                    else:
                        module.commit_complete_fact_partition(self.current, self.root, self.key)
                self.assertEqual(before, fixtures.parquet_hashes(self.silver))
                self.assert_clean()

    def test_install_failure_restores_old_fact_and_calendar(self):
        self.seed()
        for calendar in (False, True):
            with self.subTest(calendar=calendar):
                before = fixtures.parquet_hashes(self.silver)
                original = transaction_module.os.replace
                def replace(source, destination):
                    if '.staging-' in str(source):
                        raise OSError('injected install failure')
                    return original(source, destination)
                with patch.object(transaction_module.os, 'replace', side_effect=replace), self.assertRaisesRegex(OSError, 'install failure'):
                    if calendar:
                        module.commit_calendar_partitions(self.updated_calendar(), {self.grid}, self.root)
                    else:
                        module.commit_complete_fact_partition(self.current, self.root, self.key)
                self.assertEqual(before, fixtures.parquet_hashes(self.silver))
                self.assert_clean()

    def test_formal_rejection_restores_old_fact_and_quarantines_new(self):
        self.seed()
        before = fixtures.parquet_hashes(self.fact_path)
        with self.fail_read('正式外部指数事实'), self.assertRaises(RuntimeError) as error:
            module.commit_complete_fact_partition(self.current, self.root, self.key)
        self.assertIsInstance(error.exception.__cause__, ValueError)
        self.assertEqual(before, fixtures.parquet_hashes(self.fact_path))
        self.assertTrue(list(self.silver.glob('.'+module.TABLE_NAME+'.failed-*/index_category=shipping/year=2026/month=7/*.parquet')))
        self.assert_clean()

    def test_new_marker_and_new_leaf_roll_back_together(self):
        with self.fail_read('正式外部指数事实'), self.assertRaises(RuntimeError):
            module.commit_complete_fact_partition(self.current, self.root, self.key)
        self.assertEqual(fixtures.parquet_hashes(self.fact_path), {})
        self.assertTrue(list(self.silver.glob('.*.failed-*/index_category=shipping/year=2026/month=7/*.parquet')))
        self.assertFalse(list(self.silver.glob('.*.failed-*/schema.parquet')))
        self.assert_clean()

    def test_empty_deletion_failure_restores_old_leaf(self):
        self.seed()
        before = fixtures.parquet_hashes(self.fact_path)
        with self.fail_read('正式外部指数事实'), self.assertRaises(ValueError):
            module.commit_complete_fact_partition(module.empty_pandas(module.EXTERNAL_INDEX_DAILY_SCHEMA), self.root, self.key)
        self.assertEqual(before, fixtures.parquet_hashes(self.fact_path))
        self.assert_clean()

    def test_staging_or_enter_failure_leaves_formal_files_unchanged(self):
        self.seed()
        before = fixtures.parquet_hashes(self.silver)
        with self.fail_read('外部指数 staging'), patch.object(module, 'StagedPathTransaction') as transaction:
            with self.assertRaises(ValueError):
                module.commit_complete_fact_partition(self.current, self.root, self.key)
            transaction.assert_not_called()
        with patch.object(module.StagedPathTransaction, '__enter__', side_effect=OSError('enter failure')), self.assertRaises(OSError):
            module.commit_complete_fact_partition(self.current, self.root, self.key)
        self.assertEqual(before, fixtures.parquet_hashes(self.silver))
        self.assert_clean()

    def test_equal_keys_and_counts_do_not_hide_value_difference(self):
        self.seed()
        for label in ('外部指数 staging', '正式外部指数事实'):
            with self.subTest(label=label):
                before = fixtures.parquet_hashes(self.fact_path)
                original = module.open_exact_dataset
                def changed_read(*args, **kwargs):
                    dataset = original(*args, **kwargs)
                    if args[3] != label:
                        return dataset
                    def changed_table(**read_kwargs):
                        table = dataset.to_table(**read_kwargs)
                        column = table.schema.get_field_index('index_value')
                        return table.set_column(column, table.schema.field(column), module.pa.array([999.]*table.num_rows, type=module.pa.float64()))
                    return SimpleNamespace(to_table=changed_table)
                with patch.object(module, 'open_exact_dataset', side_effect=changed_read), self.assertRaises((ValueError, RuntimeError)):
                    module.commit_complete_fact_partition(self.current, self.root, self.key)
                self.assertEqual(before, fixtures.parquet_hashes(self.fact_path))
                self.assert_clean()

    def test_second_calendar_leaf_failure_keeps_first_leaf_and_other_entity(self):
        self.seed()
        second = fixtures.parquet_hashes(self.leaf(calendar=True, month=8))
        marker = (self.calendar_path/'schema.parquet').read_bytes()
        grids = {(self.entity.source_indicator_id, date(2026, month, day)) for month, day in [(7, 2), (8, 3)]}
        counts = {grid:1 for grid in grids}
        updated = module.apply_calendar_completion(self.calendar, counts, 'next', self.now)
        with self.fail_read('正式外部市场日历', 2), self.assertRaises(RuntimeError):
            module.commit_calendar_partitions(updated, grids, self.root)
        self.assertEqual(second, fixtures.parquet_hashes(self.leaf(calendar=True, month=8)))
        self.assertEqual(marker, (self.calendar_path/'schema.parquet').read_bytes())
        read = module.ds.dataset(self.calendar_path, format='parquet', partitioning=module.CALENDAR_PARTITIONING).to_table().to_pandas()
        self.assertEqual(read.loc[read['entity_code'].eq(self.entity.source_indicator_id) & read['month'].eq(7), 'fetch_run_id'].iloc[0], 'next')
        self.assertEqual(read.loc[read['entity_code'].ne(self.entity.source_indicator_id), 'fetch_run_id'].iloc[0], 'prior')
        self.assert_clean()

    def test_incomplete_rollback_keeps_backup_and_failed_new_leaf(self):
        self.seed()
        old_leaf = fixtures.parquet_hashes(self.leaf())
        original = transaction_module.os.replace
        def replace(source, destination):
            if '.backup-' in str(source):
                raise OSError('injected restore failure')
            return original(source, destination)
        with patch.object(transaction_module.os, 'replace', side_effect=replace), self.fail_read('正式外部指数事实'), self.assertRaisesRegex(RuntimeError, '回滚不完整'):
            module.commit_complete_fact_partition(self.current, self.root, self.key)
        backups = list(self.silver.glob('.*.backup-*/index_category=shipping/year=2026/month=7'))
        self.assertEqual(len(backups), 1)
        self.assertEqual(old_leaf, fixtures.parquet_hashes(backups[0]))
        self.assertTrue(list(self.silver.glob('.*.failed-*/index_category=shipping/year=2026/month=7/*.parquet')))
        self.assertFalse(list(self.silver.glob('.*.staging-*')))

    def test_calendar_failure_keeps_fact_and_next_run_still_fetches(self):
        pending = self.pending.loc[self.pending['entity_code'].eq(self.entity.source_indicator_id) & self.pending['month'].eq(7)]
        fixtures.write_partitioned_table(module, pending, module.EXTERNAL_MARKET_CALENDAR_SCHEMA, module.CALENDAR_PARTITION_COLUMNS, self.calendar_path)
        before = fixtures.parquet_hashes(self.calendar_path)
        response = [{'INDICATOR_ID':self.entity.source_indicator_id, 'REPORT_DATE':str(self.day), 'INDICATOR_VALUE':456.25}]
        session = Mock()
        with patch.object(module, 'create_eastmoney_session', return_value=session), patch.object(module, 'query_eastmoney_indicator_range', return_value=response), self.fail_read('正式外部市场日历', 2):
            result = CliRunner().invoke(module.main, ['--lake-root', str(self.root), '--write'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertNotIn('function=commit_calendar_partitions; phase=commit_leaf; status=completed;', result.output)
        self.assertNotIn('phase=partition_batch; status=completed;', result.output)
        self.assertEqual(before, fixtures.parquet_hashes(self.calendar_path))
        self.assertEqual(len(module.read_optional_fact(self.fact_path)), 1)
        session.close.assert_called_once()
        session = Mock()
        with patch.object(module, 'create_eastmoney_session', return_value=session), patch.object(module, 'query_eastmoney_indicator_range', return_value=response) as query:
            result = CliRunner().invoke(module.main, ['--lake-root', str(self.root), '--write'])
        self.assertEqual(result.exit_code, 0, result.output)
        query.assert_called_once()
        session.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
