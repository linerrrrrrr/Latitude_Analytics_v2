"""函数日志必须区分内存、raw 落盘、日历落盘与失败，不增加业务 I/O。"""
import contextlib
import io
import itertools
import pathlib
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd
from click.testing import CliRunner

import test_b03_c02_raw_staging_lifecycle as fixtures

module = fixtures.MODULE


class RawFunctionLogsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='raw-log-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name)
        self.day = date(2026, 7, 1)
        self.frame = fixtures.pending_calendar_frame(self.day)
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)

    def test_plan_progress_reuses_each_existing_inspection_once(self):
        first = self.frame.iloc[0].to_dict()
        rows = []
        for offset in range(101):
            day = date(2026, 1, 1) + timedelta(days=offset)
            rows.append({**first, 'observation_date':day, 'year':day.year, 'month':day.month})
        frame = module.arrow_to_pandas(module.pandas_to_arrow(pd.DataFrame(rows), module.EXTERNAL_MARKET_CALENDAR_SCHEMA), module.EXTERNAL_MARKET_CALENDAR_SCHEMA)
        with patch.object(module, 'inspect_raw_leaf', return_value=None) as inspected, patch.object(module.time, 'perf_counter', side_effect=itertools.count(0,3)):
            pending, repair, completed = module.plan_raw_grids(frame, self.root/'raw', None, None)
        self.assertEqual(inspected.call_count, 101)
        self.assertEqual(len(pending), 101)
        self.assertTrue(repair.empty)
        self.assertEqual(completed, 0)
        output = self.output.getvalue()
        self.assertIn('checked_dates=100/101', output)
        self.assertIn('api_pending_grid_count=101; persisted=false', output)
        self.assertNotIn('persisted=true', output)

    def test_direct_fetch_reports_receipt_and_preserves_request_failure(self):
        session = fixtures.FakeSession(fixtures.FakeResponse(200,b''))
        content, _ = module.fetch_raw_response(session,self.day)
        self.assertEqual(content,b'')
        self.assertEqual(session.get_count,1)
        output = self.output.getvalue()
        self.assertIn('http_success: dataset=domestic_spot_basis; function=fetch_raw_response;',output)
        self.assertIn('bytes=0;',output)
        self.assertIn('persisted=false',output)
        self.output.seek(0)
        self.output.truncate()
        session = fixtures.FakeSession(fixtures.FakeResponse(503,b'error-body'))
        with self.assertRaises(module.RawRequestError) as error:
            module.fetch_raw_response(session,self.day)
        self.assertEqual(error.exception.response_content,b'error-body')
        self.assertIn('status=failed; failed_phase=http_status;',self.output.getvalue())
        self.assertNotIn('http_success:',self.output.getvalue())

    def test_state_generation_reports_only_in_memory_result(self):
        updated = module.apply_calendar_completion(self.frame,{self.day:{'byte_count':0,'sha256':'a'*64}},'batch',datetime.now(timezone.utc))
        self.assertTrue(updated.iloc[0]['is_fetch_completed'])
        output = self.output.getvalue()
        self.assertIn('function=apply_calendar_completion; phase=generate_state; status=completed;',output)
        self.assertIn('updated_grids=1; fetch_status=success;',output)
        self.assertNotIn('persisted=true',output)
        self.assertNotIn('partition_committed:',output)

    def test_raw_commit_success_follows_transaction_exit(self):
        raw_root = self.root/'raw'
        module.commit_raw_response(raw_root,self.day,b'bytes')
        output = self.output.getvalue()
        self.assertLess(output.index('phase=formal_readback; status=completed;'),output.index('partition_committed:'))
        self.assertIn('persisted=true; calendar_state=not_updated',output)
        self.assertFalse(list(raw_root.glob('.backup-*')))
        # Formal validation failure must never issue a raw commit success line.
        self.output.seek(0)
        self.output.truncate()
        original = module.inspect_raw_leaf
        target = module.raw_leaf_path(raw_root,self.day)
        def inspect(path):
            if path == target:
                raise ValueError('injected formal rejection')
            return original(path)
        with patch.object(module,'inspect_raw_leaf',side_effect=inspect),self.assertRaises(RuntimeError):
            module.commit_raw_response(raw_root,self.day,b'new')
        output = self.output.getvalue()
        self.assertIn('status=failed; failed_phase=formal_readback;',output)
        self.assertNotIn('partition_committed:',output)
        self.assertNotIn('persisted=true',output)
        self.assertEqual((target/'response.html').read_bytes(),b'bytes')

    def test_calendar_failure_reports_no_calendar_state_success(self):
        fixtures.write_exact_calendar(self.root,self.frame)
        updated = module.apply_calendar_completion(self.frame,{self.day:{'byte_count':1,'sha256':'a'*64}},'batch',datetime.now(timezone.utc))
        self.output.seek(0)
        self.output.truncate()
        original = module.open_exact_dataset
        def opened(*args,**kwargs):
            if args[3]=='正式外部市场日历':
                raise ValueError('injected calendar rejection')
            return original(*args,**kwargs)
        with patch.object(module,'open_exact_dataset',side_effect=opened),self.assertRaises(RuntimeError):
            module.commit_calendar_partitions(updated,{self.day},self.root)
        output = self.output.getvalue()
        self.assertIn('committed_partitions=0',output)
        self.assertNotIn('partition_committed:',output)
        self.assertNotIn('phase=calendar_state; status=completed;',output)

    def test_http_failure_can_persist_error_state_but_run_still_fails_fetch(self):
        fixtures.write_exact_calendar(self.root,self.frame)
        session = fixtures.FakeSession(fixtures.FakeResponse(404,b'not-found'))
        with patch.object(module,'create_http_session',return_value=session):
            result = CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertNotEqual(result.exit_code,0)
        output = result.output
        self.assertIn('function=preserve_failed_response; phase=failure_evidence; status=completed;',output)
        self.assertIn('formal_completion=false',output)
        self.assertIn('completed_grids=0; persisted=true; date_watermark=none',output)
        self.assertIn('function=main; phase=run; status=failed; failed_phase=fetch;',output)
        self.assertNotIn('function=commit_raw_response; phase=commit; status=completed;',output)
        self.assertNotIn('function=main; phase=run; status=completed;',output)
        self.assertEqual(output.count('='*88),2)
        self.assertTrue(session.closed)

    def test_success_reports_raw_before_calendar_before_run_result(self):
        fixtures.write_exact_calendar(self.root,self.frame)
        session = fixtures.FakeSession(fixtures.FakeResponse(200,b'\x00\xffbytes'))
        with patch.object(module,'create_http_session',return_value=session):
            result = CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        output = result.output
        phases = [output.index(text) for text in (
            'function=fetch_raw_response; phase=fetch; status=completed;',
            'function=commit_raw_response; phase=commit; status=completed;',
            'function=apply_calendar_completion; phase=generate_state; status=completed;',
            'function=commit_calendar_partitions; phase=commit_leaf; status=completed;',
            'function=commit_calendar_partitions; phase=calendar_state; status=completed;',
            'function=main; phase=run; status=completed;',
        )]
        self.assertEqual(phases,sorted(phases))
        self.assertIn('completed_grids=1; persisted=true; date_watermark=none',output)
        self.assertEqual(output.count('='*88),2)


if __name__=='__main__':
    unittest.main()
