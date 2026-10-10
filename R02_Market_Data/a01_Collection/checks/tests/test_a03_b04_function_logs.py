"""直接调用真实函数，模拟 HTTP，检查日志阶段和完成边界。"""
import contextlib
import io
import itertools
import pathlib
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pandas as pd
from click.testing import CliRunner
import test_a03_b04_shared_transaction as fixtures

module=fixtures.module


class ExternalIndexFunctionLogsTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='index-function-logs-')
        self.addCleanup(temporary.cleanup)
        self.root=pathlib.Path(temporary.name)
        self.output=io.StringIO()
        capture=contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__,None,None,None)
        self.fact,self.calendar=fixtures.make_frames()
        self.entity=module.EXTERNAL_INDEX_ENTITIES[0]
        self.day=date(2026,7,2)
        self.now=datetime.now(timezone.utc)-timedelta(seconds=1)
        self.calendar=self.calendar.loc[self.calendar['entity_code'].eq(self.entity.source_indicator_id)&self.calendar['month'].eq(7)].copy()
        self.rows=[{'INDICATOR_ID':self.entity.source_indicator_id,'REPORT_DATE':str(self.day),'INDICATOR_VALUE':123.5}]
        self.output.seek(0)
        self.output.truncate()

    def response(self,payload):
        response=Mock()
        response.json.return_value=payload
        response.raise_for_status.return_value=None
        return response

    def payload(self,rows,pages=1,count=None):
        return {'success':True,'result':{'pages':pages,'count':len(rows) if count is None else count,'data':rows}}

    def seed(self):
        fixtures.fixtures.write_partitioned_table(module,self.calendar,module.EXTERNAL_MARKET_CALENDAR_SCHEMA,module.CALENDAR_PARTITION_COLUMNS,self.root/'silver'/module.CALENDAR_TABLE_NAME)

    def test_two_page_query_reports_each_page_and_one_receipt(self):
        second={**self.rows[0],'REPORT_DATE':'2026-07-03'}
        session=Mock()
        session.get.side_effect=[self.response(self.payload(self.rows,pages=2,count=2)),self.response(self.payload([second],pages=2,count=2))]
        with patch.object(module,'EASTMONEY_PAGE_SIZE',1):
            result=module.query_eastmoney_indicator_range(session,self.entity.source_indicator_id,self.day,date(2026,7,3))
        self.assertEqual(result,[*self.rows,second])
        self.assertEqual([call.kwargs['params']['pageNumber'] for call in session.get.call_args_list],[1,2])
        output=self.output.getvalue()
        self.assertEqual(output.count('phase=page; status=started;'),2)
        self.assertEqual(output.count('phase=page; status=completed;'),2)
        self.assertEqual(output.count('api_success:'),1)
        self.assertIn('received_rows=2/2; normalized=false; persisted=false',output)
        self.assertNotIn('api_result:',output)

    def test_both_empty_forms_report_success_without_persistence(self):
        for payload in ({'success':False,'code':9201,'message':'返回数据为空'},self.payload([],pages=0)):
            with self.subTest(payload=payload):
                self.output.seek(0)
                self.output.truncate()
                session=Mock()
                session.get.return_value=self.response(payload)
                self.assertEqual(module.query_eastmoney_indicator_range(session,self.entity.source_indicator_id,self.day,self.day),[])
                output=self.output.getvalue()
                self.assertEqual(output.count('api_success:'),1)
                self.assertIn('rows=0; outcome=empty_response; normalized=false; persisted=false',output)

    def test_http_failure_keeps_cause_and_does_not_report_receipt(self):
        session=Mock()
        timeout=module.requests.Timeout('injected timeout')
        session.get.side_effect=timeout
        with self.assertRaises(RuntimeError) as error:
            module.query_eastmoney_indicator_range(session,self.entity.source_indicator_id,self.day,self.day)
        self.assertIs(error.exception.__cause__,timeout)
        self.assertIn('failed_phase=request;',self.output.getvalue())
        self.assertIn('page=1;',self.output.getvalue())
        self.assertNotIn('api_success:',self.output.getvalue())

    def test_page_drift_reports_second_page_failure_without_receipt(self):
        session=Mock()
        session.get.side_effect=[self.response(self.payload(self.rows,pages=2,count=2)),self.response(self.payload(self.rows,pages=3,count=3))]
        with patch.object(module,'EASTMONEY_PAGE_SIZE',1),self.assertRaisesRegex(RuntimeError,'漂移'):
            module.query_eastmoney_indicator_range(session,self.entity.source_indicator_id,self.day,date(2026,7,3))
        output=self.output.getvalue()
        self.assertIn('failed_phase=response_metadata;',output)
        self.assertIn('page=2;',output)
        self.assertNotIn('api_success:',output)

    def test_invalid_value_has_no_normalization_success(self):
        rows=[{**self.rows[0],'INDICATOR_VALUE':True}]
        with self.assertRaises(ValueError):
            module.normalize_external_index_response(rows,self.entity,self.day,self.day,{self.day},self.now)
        self.assertIn('function=normalize_external_index_response; phase=normalize; status=failed;',self.output.getvalue())
        self.assertNotIn('api_result:',self.output.getvalue())

    def test_plan_progress_reuses_existing_count_work(self):
        row=self.calendar.iloc[0].to_dict()
        rows=[]
        for offset in range(1001):
            day=date(2020,1,1)+timedelta(days=offset)
            rows.append({**row,'observation_date':day,'year':day.year,'month':day.month})
        calendar=pd.DataFrame(rows)
        with patch.object(module,'grid_count_map',wraps=module.grid_count_map) as count,patch.object(module.time,'perf_counter',side_effect=itertools.count(0,3)):
            pending,obsolete,complete=module.external_index_reconciliation(calendar,module.empty_pandas(module.EXTERNAL_INDEX_DAILY_SCHEMA),None,None)
        self.assertEqual((len(pending),len(obsolete),complete),(1001,0,0))
        count.assert_called_once()
        self.assertIn('scanned_grids=1000/1001',self.output.getvalue())

    def test_empty_read_and_generated_state_are_not_persisted(self):
        with patch.object(module,'open_exact_dataset') as opened:
            self.assertTrue(module.read_optional_fact(self.root/'missing').empty)
        opened.assert_not_called()
        result=module.apply_calendar_completion(self.calendar,{(self.entity.source_indicator_id,self.day):1},'batch',self.now)
        self.assertTrue(result.iloc[0]['is_fetch_completed'])
        self.assertIn('outcome=no_fact_files; materialized=false',self.output.getvalue())
        self.assertIn('updated_grids=1; rows=1; persisted=false',self.output.getvalue())
        self.assertNotIn('persisted=true',self.output.getvalue())

    def test_no_touched_grids_do_not_claim_committed_watermark(self):
        self.assertEqual(module.commit_calendar_partitions(self.calendar,set(),self.root),0)
        output=self.output.getvalue()
        self.assertIn('outcome=no_work; persisted=false',output)
        self.assertNotIn('partition_committed:',output)
        self.assertNotIn('phase=calendar_state;',output)
        self.assertFalse((self.root/'silver').exists())

    def test_real_query_integrates_with_state_and_commit_log_order(self):
        self.seed()
        session=Mock()
        session.get.return_value=self.response(self.payload(self.rows))
        with patch.object(module,'create_eastmoney_session',return_value=session):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        session.get.assert_called_once()
        session.close.assert_called_once()
        output=result.output
        positions=[output.index(fragment) for fragment in (
            'function=query_eastmoney_indicator_range; phase=fetch; status=completed;',
            'function=normalize_external_index_response; phase=normalize; status=completed;',
            'function=commit_complete_fact_partition; phase=commit_leaf; status=completed;',
            'function=apply_calendar_completion; phase=generate_state; status=completed;',
            'function=commit_calendar_partitions; phase=commit_leaf; status=completed;',
            'function=commit_calendar_partitions; phase=calendar_state; status=completed;',
            'function=main; phase=partition_batch; status=completed;',
        )]
        self.assertEqual(positions,sorted(positions))
        self.assertIn('calendar_state=not_updated; scope=fact_leaf; persisted=true',output)
        self.assertIn('completed_grids=1; persisted=true; date_watermark=none',output)
        self.assertEqual(output.count('='*88),2)

    def test_error_state_persistence_is_not_collection_completion(self):
        self.seed()
        session=Mock()
        session.get.side_effect=module.requests.Timeout('injected timeout')
        with patch.object(module,'create_eastmoney_session',return_value=session):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertNotEqual(result.exit_code,0)
        output=result.output
        self.assertIn('function=query_eastmoney_indicator_range; phase=fetch; status=failed;',output)
        self.assertIn('completed_grids=0; persisted=true; date_watermark=none',output)
        self.assertNotIn('function=commit_complete_fact_partition; phase=commit_leaf; status=completed;',output)
        self.assertNotIn('phase=run; status=completed;',output)
        session.close.assert_called_once()


if __name__=='__main__':
    unittest.main()
