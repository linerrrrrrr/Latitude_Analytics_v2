"""SHIBOR 第 7—9 项：校验次数、叶范围、同月状态继承和 metadata 边界。"""
import contextlib
import io
import pathlib
import tempfile
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import test_b04_c02_interest_rate as fixtures

M = fixtures.C02


class RangeClient:
    def __init__(self, dates):
        self.dates = dates
        self.calls = []

    def shibor(self, **kwargs):
        self.calls.append(kwargs)
        return pd.concat([fixtures.shibor_response(d) for d in self.dates
                          if kwargs['start_date'] <= d.strftime('%Y%m%d') <= kwargs['end_date']], ignore_index=True)


class InterestRateValidationIOTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='shibor-io-')
        self.addCleanup(temporary.cleanup)
        self.lake = pathlib.Path(temporary.name)
        self.capture = io.StringIO()
        redirect = contextlib.redirect_stdout(self.capture)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        self.first, self.second = date(2026, 7, 31), date(2026, 8, 3)
        fixtures.build_calendar(self.lake, self.first, self.second)
        self.fact_path = self.lake / 'silver' / M.TABLE_NAME
        self.calendar_path = self.lake / 'silver' / M.CALENDAR_TABLE_NAME

    def populate(self):
        result = fixtures.run_c02(self.lake, None, None, RangeClient([self.first, self.second]))
        self.assertEqual(result.exit_code, 0, result.output)

    def test_two_months_only_read_and_plan_once_and_validate_current_leaves(self):
        calls = {}
        names = ('read_interest_calendar', 'read_optional_fact', 'plan_interest_rate_grids',
                 'validate_interest_calendar_table', 'validate_interest_rate_table', 'full_fact_partition',
                 'apply_calendar_completion', 'open_exact_dataset')
        with contextlib.ExitStack() as stack:
            for name in names:
                calls[name] = stack.enter_context(patch.object(M, name, wraps=getattr(M, name)))
            result = fixtures.run_c02(self.lake, None, None, RangeClient([self.first, self.second]))
        self.assertEqual(result.exit_code, 0, result.output)
        for name in ('read_interest_calendar', 'read_optional_fact', 'plan_interest_rate_grids'):
            self.assertEqual(calls[name].call_count, 1, name)
        self.assertEqual(calls['validate_interest_calendar_table'].call_count, 2)
        self.assertEqual(calls['validate_interest_rate_table'].call_count, 4)
        self.assertEqual([len(c.args[0]) for c in calls['full_fact_partition'].call_args_list], [0, 0])
        self.assertEqual([len(c.args[0]) for c in calls['apply_calendar_completion'].call_args_list], [8, 8])
        self.assertTrue(all(len(c.args[0]) == 8 for c in calls['validate_interest_rate_table'].call_args_list))
        for call in calls['open_exact_dataset'].call_args_list:
            if call.args[4] in ('正式 SHIBOR 事实', '回写后的正式宏观发布日历'):
                self.assertEqual(call.args[0].name[:6], 'month=')
                self.assertIn('partition_base_dir', call.kwargs)

    def test_repair_and_fetch_in_same_month_preserve_both_states(self):
        first, second = date(2026, 7, 17), date(2026, 7, 20)
        with tempfile.TemporaryDirectory(prefix='shibor-mixed-') as directory:
            lake = pathlib.Path(directory)
            fixtures.build_calendar(lake, first, second)
            calendar = M.read_interest_calendar(lake / 'silver' / M.CALENDAR_TABLE_NAME)
            fact, _ = M.normalize_shibor_response(fixtures.shibor_response(first),
                calendar.loc[calendar['report_date'].eq(first)], first, first, datetime.now(timezone.utc))
            M.commit_complete_fact_partition(fact, lake, (2026, 7))
            client = RangeClient([second])
            with patch.object(M, 'plan_interest_rate_grids', wraps=M.plan_interest_rate_grids) as plan:
                result = fixtures.run_c02(lake, None, None, client)
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertEqual(plan.call_count, 1)
            self.assertEqual(len(client.calls), 1)
            actual = M.read_interest_calendar(lake / 'silver' / M.CALENDAR_TABLE_NAME)
            self.assertTrue(actual['is_fetch_completed'].all())
            self.assertTrue(actual.loc[actual['report_date'].eq(first), 'quality_reason'].eq(M.STATE_REPAIR_REASON).all())
            self.assertTrue(actual.loc[actual['report_date'].eq(second), 'quality_reason'].eq(M.API_SUCCESS_REASON).all())
            self.assertIn('formal_reconciled=16', result.output)
            facts, _ = M.read_optional_fact(lake / 'silver' / M.TABLE_NAME)
            self.assertEqual(len(facts), 16)

    def test_descriptive_metadata_changes_do_not_trigger_migration_or_rewrite(self):
        self.populate()
        path = next((self.fact_path / 'year=2026/month=7').glob('*.parquet'))
        table = pq.ParquetFile(path).read()
        fields = list(table.schema)
        fields[0] = fields[0].with_metadata({**(fields[0].metadata or {}), b'description_zh': b'old wording'})
        metadata = {**table.schema.metadata, b'description_zh': b'old table wording'}
        table = pa.Table.from_arrays(list(table.columns), schema=pa.schema(fields, metadata=metadata))
        pq.write_table(table, path)
        files = {p: p.read_bytes() for p in self.lake.rglob('*.parquet')}
        with patch.object(M, 'upgrade_fact_metadata', side_effect=AssertionError('description must not migrate')):
            result = fixtures.run_c02(self.lake, None, None, None)
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(files, {p: p.read_bytes() for p in self.lake.rglob('*.parquet')})
        _, is_current = M.read_optional_fact(self.fact_path)
        self.assertTrue(is_current)

    def test_each_open_checks_fragments_once_and_still_rejects_wrong_identity(self):
        self.populate()
        real_dataset = M.ds.dataset
        counts = []

        class CountingDataset:
            def __init__(self, dataset):
                self.dataset = dataset
                self.fragment_calls = 0
                counts.append(self)

            def __getattr__(self, name):
                return getattr(self.dataset, name)

            def get_fragments(self, *args, **kwargs):
                self.fragment_calls += 1
                return self.dataset.get_fragments(*args, **kwargs)

        with patch.object(M.ds, 'dataset', side_effect=lambda *a, **k: CountingDataset(real_dataset(*a, **k))):
            M.read_optional_fact(self.fact_path)
        self.assertEqual([d.fragment_calls for d in counts], [1])
        path = next((self.fact_path / 'year=2026/month=8').glob('*.parquet'))
        table = pq.ParquetFile(path).read()
        pq.write_table(table.replace_schema_metadata({**table.schema.metadata, b'table_name': b'wrong_table'}), path)
        with self.assertRaisesRegex(TypeError, '身份'):
            M.read_optional_fact(self.fact_path)

    def test_dirty_leaf_business_errors_are_rejected_before_staging(self):
        self.populate()
        facts, _ = M.read_optional_fact(self.fact_path)
        changed = facts.loc[facts['month'].eq(7)].copy()
        changed.loc[:, 'rate'] = 101.0
        with patch.object(M, 'write_fact_staging', side_effect=AssertionError('invalid values must not stage')) as staging:
            with self.assertRaisesRegex(ValueError, '硬边界'):
                M.commit_complete_fact_partition(changed, self.lake, (2026, 7))
        staging.assert_not_called()
        calendar = M.read_interest_calendar(self.calendar_path)
        calendar.loc[calendar['month'].eq(7), 'actual_record_count'] = 0
        with patch.object(M.ds, 'write_dataset', side_effect=AssertionError('invalid state must not stage')) as staging:
            with self.assertRaisesRegex(ValueError, 'success'):
                M.commit_calendar_partition(calendar, self.lake, (M.DATASET_NAME, 2026, 7))
        staging.assert_not_called()


if __name__ == '__main__':
    unittest.main()
