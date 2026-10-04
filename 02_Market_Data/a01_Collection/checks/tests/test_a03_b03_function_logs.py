"""函数直接调用的进度、内存/落盘区分及真实查询函数的模拟集成。"""
import contextlib
import io
import itertools
import pathlib
import tempfile
import sys
import unittest
from datetime import date,datetime,timedelta,timezone
from types import SimpleNamespace
from unittest.mock import Mock,patch

import pandas as pd
from click.testing import CliRunner
import test_b03_metadata_upgrade as fixtures

module=fixtures.load_notebook_module(fixtures.C03_NOTEBOOK_PATH,fixtures.C03_SKIPPED_CELL_IDS,'overseas_logs')

class OverseasFunctionLogsTests(unittest.TestCase):
    def setUp(self):
        self.output=io.StringIO()
        capture=contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__,None,None,None)
        temporary=tempfile.TemporaryDirectory(prefix='overseas-function-log-')
        self.addCleanup(temporary.cleanup)
        self.root=pathlib.Path(temporary.name)
        self.day=date(2026,7,31)
        self.now=datetime.now(timezone.utc)-timedelta(seconds=1)
        self.raw=pd.DataFrame([dict(id='source',code='TEST',name='测试',day=self.day,open=100.0,
            high=102.0,low=99.0,close=101.0,volume=10.0,change_pct=1.0,amplitude=3.0,pre_close=None)])
        self.calendar=pd.DataFrame([dict(dataset_name=module.DATASET_NAME,entity_code=module.ENTITY_CODE,
            observation_date=self.day,is_fetch_required=True,requirement_reason='required',is_fetch_completed=False,
            fetch_result_status='pending',is_data_missing=False,actual_record_count=0,quality_status='pending',
            quality_reason='待采集',fetch_run_id=None,fetch_completed_at=None,quality_checked_at=None,
            updated_at=self.now,year=self.day.year,month=self.day.month)])

    def fake_jq(self,response):
        finance=SimpleNamespace(FUT_GLOBAL_DAILY=SimpleNamespace(**{name:Mock() for name in module.JQDATA_FIELDS}),
                                run_query=Mock(return_value=response))
        return SimpleNamespace(finance=finance,query=Mock(return_value=SimpleNamespace(filter=Mock(return_value='query'))))

    def test_query_reports_receipt_before_normalization(self):
        jq=self.fake_jq(self.raw)
        self.assertIs(module.query_overseas_futures_grid(jq,self.day),self.raw)
        jq.finance.run_query.assert_called_once_with('query')
        text=self.output.getvalue()
        self.assertIn('function=query_overseas_futures_grid; phase=fetch; status=started;',text)
        self.assertIn('normalized=false; persisted=false;',text)
        self.assertNotIn('api_result:',text)

    def test_query_failure_preserves_cause_and_has_no_success(self):
        jq=self.fake_jq(None)
        original=TimeoutError('injected query timeout')
        jq.finance.run_query.side_effect=original
        with self.assertRaises(RuntimeError) as error:module.query_overseas_futures_grid(jq,self.day)
        self.assertIs(error.exception.__cause__,original)
        self.assertIn('status=failed; failed_phase=request;',self.output.getvalue())
        self.assertNotIn('api_success:',self.output.getvalue())

    def test_plan_progress_reuses_existing_fact_evidence_once(self):
        row=self.calendar.iloc[0].to_dict()
        rows=[]
        for offset in range(101):
            day=date(2026,1,1)+timedelta(days=offset)
            rows.append({**row,'observation_date':day,'year':day.year,'month':day.month})
        frame=pd.DataFrame(rows)
        with patch.object(module,'grid_count_map',wraps=module.grid_count_map) as count,patch.object(module,'ohlc_relation_warning_map',wraps=module.ohlc_relation_warning_map) as quality,patch.object(module.time,'perf_counter',side_effect=itertools.count(0,3)):
            pending,repair,completed,counts,warnings=module.plan_overseas_futures_grids(frame,module.empty_pandas(module.OVERSEAS_FUTURES_DAILY_SCHEMA),None,None)
        self.assertEqual(count.call_count,1)
        self.assertEqual(quality.call_count,1)
        self.assertEqual((len(pending),len(repair),completed),(101,0,0))
        self.assertIn('scanned_dates=100/101',self.output.getvalue())
        self.assertIn('api_pending_grid_count=101; persisted=false',self.output.getvalue())

    def test_empty_read_and_state_generation_do_not_claim_persistence(self):
        with patch.object(module,'open_exact_dataset') as opened:
            fact=module.read_optional_fact(self.root/'missing')
        opened.assert_not_called()
        self.assertTrue(fact.empty)
        result=module.apply_calendar_completion(self.calendar,{self.day:1},{},'batch',self.now)
        self.assertTrue(result.iloc[0]['is_fetch_completed'])
        text=self.output.getvalue()
        self.assertIn('outcome=no_fact_files; materialized=false',text)
        self.assertIn('function=apply_calendar_completion; phase=generate_state; status=completed;',text)
        self.assertNotIn('persisted=true',text)

    def test_no_touched_calendar_dates_have_no_commit_or_watermark_success(self):
        self.assertEqual(module.commit_calendar_partitions(self.calendar,set(),self.root),0)
        text=self.output.getvalue()
        self.assertIn('reason=no_touched_dates',text)
        self.assertNotIn('partition_committed:',text)
        self.assertNotIn('phase=calendar_state; status=completed;',text)
        self.assertFalse((self.root/'silver').exists())

    def test_actual_query_function_integrates_with_commit_logs_in_order(self):
        fixtures.write_partitioned_table(module,self.calendar,module.EXTERNAL_MARKET_CALENDAR_SCHEMA,
            module.CALENDAR_PARTITION_COLUMNS,self.root/'silver'/module.CALENDAR_TABLE_NAME)
        jq=self.fake_jq(self.raw)
        with patch.object(module,'authenticate_jqdata',return_value=jq):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        self.assertEqual(jq.finance.run_query.call_count,1)
        text=result.output
        positions=[text.index(fragment) for fragment in (
            'function=query_overseas_futures_grid; phase=fetch; status=completed;',
            'function=normalize_overseas_futures_response; phase=normalize; status=completed;',
            'function=commit_complete_fact_partition; phase=commit; status=completed;',
            'function=apply_calendar_completion; phase=generate_state; status=completed;',
            'function=commit_calendar_partitions; phase=commit_leaf; status=completed;',
            'function=commit_calendar_partitions; phase=calendar_state; status=completed;',
            'function=main; dataset=overseas_futures; phase=run; status=completed;',
        )]
        self.assertEqual(positions,sorted(positions))
        self.assertIn('completed_grids=1; persisted=true; date_watermark=none',text)
        self.assertEqual(text.count('='*88),2)

    def test_rollback_log_sink_failure_occurs_after_old_leaf_restored(self):
        fact=module.normalize_overseas_futures_response(self.raw,self.day,self.now)
        target=self.root/'silver'/module.TABLE_NAME
        fixtures.write_partitioned_table(module,fact,module.OVERSEAS_FUTURES_DAILY_SCHEMA,module.PARTITION_COLUMNS,target)
        before=fixtures.parquet_hashes(target)
        original_open=module.open_exact_dataset
        original_echo=module.click.echo
        def opened(*args,**kwargs):
            if args[-1]=='正式境外期货事实':raise ValueError('injected formal rejection')
            return original_open(*args,**kwargs)
        def echo(message,*args,**kwargs):
            if 'phase=rollback;' in str(message):raise OSError('injected log sink failure')
            return original_echo(message,*args,**kwargs)
        with patch.object(module,'open_exact_dataset',side_effect=opened),patch.object(module.click,'echo',side_effect=echo):
            with self.assertRaisesRegex(OSError,'log sink'):module.commit_complete_fact_partition(fact,self.root,(2026,7))
        self.assertEqual(fixtures.parquet_hashes(target),before)
        self.assertNotIn('function=commit_complete_fact_partition; phase=commit; status=completed;',self.output.getvalue())

    def test_restore_failure_reports_recovery_paths_without_commit_success(self):
        fact=module.normalize_overseas_futures_response(self.raw,self.day,self.now)
        target=self.root/'silver'/module.TABLE_NAME
        fixtures.write_partitioned_table(module,fact,module.OVERSEAS_FUTURES_DAILY_SCHEMA,module.PARTITION_COLUMNS,target)
        original_open=module.open_exact_dataset
        transaction_module=sys.modules[module.StagedPathTransaction.__module__]
        original_move=transaction_module.os.replace
        def opened(*args,**kwargs):
            if args[-1]=='正式境外期货事实':raise ValueError('injected formal rejection')
            return original_open(*args,**kwargs)
        def move(source,destination):
            if '.backup-' in str(source):raise OSError('injected restoration failure')
            return original_move(source,destination)
        with patch.object(module,'open_exact_dataset',side_effect=opened),patch.object(transaction_module.os,'replace',side_effect=move):
            with self.assertRaisesRegex(RuntimeError,'回滚不完整'):module.commit_complete_fact_partition(fact,self.root,(2026,7))
        text=self.output.getvalue()
        self.assertRegex(text,r'function=commit_complete_fact_partition; .*phase=rollback; status=failed;')
        self.assertIn('failed_partitions=1; backup_dir=',text)
        self.assertNotIn('phase=rollback; status=completed;',text)
        self.assertNotIn('function=commit_complete_fact_partition; phase=commit; status=completed;',text)
        self.assertTrue(list((self.root/'silver').glob('.'+module.TABLE_NAME+'.backup-*')))
        self.assertTrue(list((self.root/'silver').glob('.'+module.TABLE_NAME+'.failed-*')))

if __name__=='__main__':unittest.main()
