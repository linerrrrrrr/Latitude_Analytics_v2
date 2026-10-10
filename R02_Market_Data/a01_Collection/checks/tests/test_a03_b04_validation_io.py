"""验证读取范围、共享月份状态、dirty 业务门禁及描述性 metadata 兼容。"""
import contextlib
import io
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pandas as pd
from click.testing import CliRunner
import test_a03_b04_shared_transaction as fixtures

module=fixtures.module


class ExternalIndexValidationTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='index-validation-')
        self.addCleanup(temporary.cleanup)
        self.root=pathlib.Path(temporary.name)
        self.fact_path=self.root/'silver'/module.TABLE_NAME
        self.calendar_path=self.root/'silver'/module.CALENDAR_TABLE_NAME
        self.output=io.StringIO()
        capture=contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__,None,None,None)
        self.fact,self.calendar=fixtures.make_frames()

    def write_calendar(self,calendar=None,**kwargs):
        fixtures.fixtures.write_partitioned_table(module,self.calendar if calendar is None else calendar,module.EXTERNAL_MARKET_CALENDAR_SCHEMA,module.CALENDAR_PARTITION_COLUMNS,self.calendar_path,**kwargs)

    def write_fact(self,**kwargs):
        fixtures.fixtures.write_partitioned_table(module,self.fact,module.EXTERNAL_INDEX_DAILY_SCHEMA,module.PARTITION_COLUMNS,self.fact_path,**kwargs)

    @staticmethod
    def query(session,indicator,start,end):
        return [{'INDICATOR_ID':indicator,'REPORT_DATE':str(day.date()),'INDICATOR_VALUE':321.25} for day in pd.date_range(start,end)]

    def test_main_only_opens_table_roots_once_and_passes_current_leaves(self):
        self.write_calendar()
        self.write_fact()
        paths=[]
        original=module.open_exact_dataset
        def opened(*args,**kwargs):
            paths.append(pathlib.Path(args[0]))
            return original(*args,**kwargs)
        original_merge=module.full_fact_partition
        def merge(existing,incoming,touched,key):
            self.assertEqual(set(existing[module.PARTITION_COLUMNS].itertuples(index=False,name=None)),{key})
            return original_merge(existing,incoming,touched,key)
        original_state=module.apply_calendar_completion
        def state(calendar,*args):
            self.assertEqual(len(set(calendar[module.CALENDAR_PARTITION_COLUMNS].itertuples(index=False,name=None))),1)
            return original_state(calendar,*args)
        with patch.object(module,'open_exact_dataset',side_effect=opened),patch.object(module,'full_fact_partition',side_effect=merge),patch.object(module,'apply_calendar_completion',side_effect=state),patch.object(module,'external_index_reconciliation',wraps=module.external_index_reconciliation) as plan,patch.object(module,'read_optional_fact',wraps=module.read_optional_fact) as read,patch.object(module,'create_eastmoney_session',return_value=Mock()),patch.object(module,'query_eastmoney_indicator_range',side_effect=self.query):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        self.assertEqual(paths.count(self.calendar_path),1)
        self.assertEqual(paths.count(self.fact_path),1)
        plan.assert_called_once()
        read.assert_called_once()
        for path in paths:
            if path==self.calendar_path or path==self.fact_path or '.staging-' in str(path):continue
            self.assertTrue(path.name.startswith('month=') or path.name=='schema.parquet',str(path))

    def test_second_category_failure_preserves_first_category_state(self):
        self.calendar=self.calendar.loc[self.calendar['month'].eq(7)]
        self.write_calendar()
        def query(session,indicator,start,end):
            if indicator==module.EXTERNAL_INDEX_ENTITIES[0].source_indicator_id:
                raise ValueError('retryable_error: injected later category failure')
            return self.query(session,indicator,start,end)
        with patch.object(module,'create_eastmoney_session',return_value=Mock()),patch.object(module,'query_eastmoney_indicator_range',side_effect=query):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertNotEqual(result.exit_code,0)
        read=module.ds.dataset(self.calendar_path,format='parquet',partitioning=module.CALENDAR_PARTITIONING).to_table().to_pandas().set_index('entity_code')
        first=module.EXTERNAL_INDEX_ENTITIES[6].source_indicator_id
        second=module.EXTERNAL_INDEX_ENTITIES[0].source_indicator_id
        self.assertTrue(read.loc[first,'is_fetch_completed'])
        self.assertEqual(read.loc[first,'fetch_result_status'],'success')
        self.assertFalse(read.loc[second,'is_fetch_completed'])
        self.assertEqual(read.loc[second,'fetch_result_status'],'retryable_error')
        self.assertEqual(set(module.read_optional_fact(self.fact_path)['index_code']),{'WTI_CONC'})

    def test_description_metadata_does_not_trigger_rewrite_or_api(self):
        calendar=module.apply_calendar_completion(self.calendar,module.grid_count_map(self.fact),'prior',datetime(2026,8,4,tzinfo=timezone.utc))
        self.write_calendar(calendar,write_schema=fixtures.fixtures.schema_with_stale_metadata(module.EXTERNAL_MARKET_CALENDAR_SCHEMA))
        self.write_fact(write_schema=fixtures.fixtures.schema_with_stale_metadata(module.EXTERNAL_INDEX_DAILY_SCHEMA))
        before=fixtures.fixtures.parquet_hashes(self.root)
        with patch.object(module,'create_eastmoney_session') as session:
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        session.assert_not_called()
        self.assertEqual(before,fixtures.fixtures.parquet_hashes(self.root))

    def test_dirty_duplicate_and_invalid_calendar_state_fail_before_install(self):
        current=self.fact.loc[self.fact['index_category'].eq('shipping')&self.fact['month'].eq(7)]
        duplicated=pd.concat([current,current],ignore_index=True)
        invalid=self.calendar.copy()
        invalid.loc[invalid['month'].eq(7),'fetch_result_status']='invalid'
        with patch.object(module,'StagedPathTransaction') as transaction:
            with self.assertRaisesRegex(ValueError,'主键不唯一'):
                module.commit_complete_fact_partition(duplicated,self.root,('shipping',2026,7))
            with self.assertRaisesRegex(ValueError,'状态不在允许枚举'):
                module.commit_calendar_partitions(invalid,{(invalid.iloc[0]['entity_code'],invalid.iloc[0]['observation_date'])},self.root)
            transaction.assert_not_called()
        self.assertFalse((self.root/'silver').exists())

    def test_obsolete_month_absent_from_calendar_is_still_removed_without_api(self):
        self.write_calendar(self.calendar.loc[self.calendar['month'].eq(8)].assign(is_fetch_required=False,fetch_result_status='not_required',quality_status='not_applicable'))
        self.write_fact()
        with patch.object(module,'create_eastmoney_session') as session:
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        session.assert_not_called()
        self.assertTrue(module.read_optional_fact(self.fact_path).empty)


if __name__=='__main__':
    unittest.main()
