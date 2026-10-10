"""dirty 叶验收、物理门禁、同叶状态继承；只写临时湖。"""
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
from click.testing import CliRunner

import test_b03_c02_raw_staging_lifecycle as fixtures

module=fixtures.MODULE


class RawValidationIOTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='raw-io-')
        self.addCleanup(temporary.cleanup)
        self.root=pathlib.Path(temporary.name)
        self.day=date(2026,7,1)
        self.frame=fixtures.pending_calendar_frame(self.day)
        self.target=self.root/'silver'/module.CALENDAR_TABLE_NAME
        self.output=io.StringIO()
        capture=contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__,None,None,None)

    def stored(self):
        return {p.relative_to(self.target).as_posix():p.read_bytes() for p in self.target.rglob('*.parquet')}

    def read(self):
        return module.arrow_to_pandas(module.ds.dataset(self.target,format='parquet',partitioning=module.CALENDAR_PARTITIONING).to_table(columns=module.EXTERNAL_MARKET_CALENDAR_SCHEMA.names),module.EXTERNAL_MARKET_CALENDAR_SCHEMA).sort_values('observation_date')

    def test_same_leaf_dates_keep_earlier_success_and_outside_scope_record(self):
        days=[date(2026,7,1),date(2026,7,2),date(2026,7,3),date(2026,8,1)]
        rows=pd.concat([fixtures.pending_calendar_frame(day) for day in days],ignore_index=True)
        fixtures.write_exact_calendar(self.root,rows)
        untouched=self.target/f'dataset_name={module.DATASET_NAME}/year=2026/month=8/part-0.parquet'
        old_bytes=untouched.read_bytes()
        session=fixtures.FakeSession(fixtures.FakeResponse(200,b'new-source'))
        with patch.object(module,'create_http_session',return_value=session):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--start-date','2026-07-01','--end-date','2026-07-02','--write'])
        self.assertEqual(result.exit_code,0,result.output)
        self.assertEqual(session.get_count,2)
        read=self.read()
        self.assertEqual(read['is_fetch_completed'].tolist(),[True,True,False,False])
        self.assertEqual(read['actual_record_count'].tolist(),[1,1,0,0])
        self.assertEqual(old_bytes,untouched.read_bytes())
        self.assertEqual(read.iloc[2]['quality_reason'],rows.iloc[2]['quality_reason'])

    def test_repair_and_fetch_in_same_leaf_preserve_repaired_date(self):
        second=date(2026,7,2)
        rows=pd.concat([self.frame,fixtures.pending_calendar_frame(second)],ignore_index=True)
        fixtures.write_exact_calendar(self.root,rows)
        module.commit_raw_response(self.root/module.RAW_RELATIVE_ROOT,self.day,b'existing')
        session=fixtures.FakeSession(fixtures.FakeResponse(200,b'new'))
        with patch.object(module,'create_http_session',return_value=session):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        self.assertEqual(session.get_count,1)
        read=self.read()
        self.assertEqual(read['is_fetch_completed'].tolist(),[True,True])
        self.assertTrue(read.iloc[0]['fetch_run_id'].startswith('raw-state-repair-'))
        self.assertFalse(read.iloc[1]['fetch_run_id'].startswith('raw-state-repair-'))

    def test_later_http_failure_keeps_earlier_date_completed_in_same_leaf(self):
        rows=pd.concat([self.frame,fixtures.pending_calendar_frame(date(2026,7,2))],ignore_index=True)
        fixtures.write_exact_calendar(self.root,rows)
        session=fixtures.FakeSession(fixtures.FakeResponse(200,b'first'))
        responses=iter([fixtures.FakeResponse(200,b'first'),fixtures.FakeResponse(503,b'second-error')])
        session.get=lambda *args,**kwargs:next(responses)
        with patch.object(module,'create_http_session',return_value=session):
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertNotEqual(result.exit_code,0)
        read=self.read()
        self.assertEqual(read['fetch_result_status'].tolist(),['success','retryable_error'])
        self.assertEqual(read['is_fetch_completed'].tolist(),[True,False])
        self.assertTrue(session.closed)

    def test_dirty_leaf_still_rejects_duplicate_key_or_invalid_success(self):
        fixtures.write_exact_calendar(self.root,self.frame)
        before=self.stored()
        duplicate=pd.concat([self.frame,self.frame],ignore_index=True)
        bad_count=module.apply_calendar_completion(self.frame,{self.day:{'byte_count':1,'sha256':'a'*64}},'batch',datetime.now(timezone.utc))
        bad_count.loc[:,'actual_record_count']=0
        for candidate in (duplicate,bad_count):
            with self.subTest(kind=len(candidate)),self.assertRaises(ValueError):
                module.commit_calendar_partitions(candidate,{self.day},self.root)
            self.assertEqual(before,self.stored())
            self.assertFalse(list((self.root/'silver').glob('.*')))

    def test_descriptive_metadata_drift_is_accepted_without_rewriting(self):
        fixtures.write_exact_calendar(self.root,self.frame)
        for path in self.target.rglob('*.parquet'):
            table=pq.ParquetFile(path).read()
            metadata=dict(table.schema.metadata)
            metadata[b'description_zh']='历史描述'.encode('utf8')
            fields=[field.with_metadata({**(field.metadata or {}),b'description_zh':b'prior-description'}) for field in table.schema]
            table=table.cast(pa.schema(fields,metadata=metadata))
            pq.write_table(table,path)
        before=self.stored()
        dataset=module.open_exact_dataset(self.target,module.CALENDAR_PARTITIONING,module.EXTERNAL_MARKET_CALENDAR_SCHEMA,'metadata test')
        read=module.arrow_to_pandas(dataset.to_table(columns=module.EXTERNAL_MARKET_CALENDAR_SCHEMA.names),module.EXTERNAL_MARKET_CALENDAR_SCHEMA)
        self.assertEqual(len(read),1)
        self.assertEqual(before,self.stored())

    def test_fragment_identity_type_and_nullable_mismatches_are_rejected(self):
        for change in ('identity','type','nullable'):
            with self.subTest(change=change):
                fixtures.write_exact_calendar(self.root,self.frame)
                path=next(self.target.glob('dataset_name=*/year=*/month=*/*.parquet'))
                table=pq.ParquetFile(path).read()
                if change=='identity':
                    table=table.replace_schema_metadata({**table.schema.metadata,b'primary_key':b'wrong'})
                else:
                    index=table.schema.get_field_index('actual_record_count')
                    field=table.schema.field(index)
                    replacement=field.with_type(pa.int64() if field.type!=pa.int64() else pa.int32()) if change=='type' else field.with_nullable(not field.nullable)
                    table=table.cast(table.schema.set(index,replacement))
                pq.write_table(table,path)
                with self.assertRaises(TypeError):
                    module.open_exact_dataset(self.target,module.CALENDAR_PARTITIONING,module.EXTERNAL_MARKET_CALENDAR_SCHEMA,'physical test')

    def test_staging_same_keys_and_count_but_changed_value_is_rejected(self):
        fixtures.write_exact_calendar(self.root,self.frame)
        before=self.stored()
        original=module.ds.write_dataset
        def write(table,*args,**kwargs):
            index=table.schema.get_field_index('quality_reason')
            changed=table.set_column(index,table.schema.field(index),pa.array(['injected different value']*table.num_rows,type=pa.string()))
            return original(changed,*args,**kwargs)
        with patch.object(module.ds,'write_dataset',side_effect=write),self.assertRaisesRegex(ValueError,'staging 内容'):
            module.commit_calendar_partitions(self.frame,{self.day},self.root)
        self.assertEqual(before,self.stored())

    def test_formal_same_keys_and_count_but_changed_value_rolls_back(self):
        fixtures.write_exact_calendar(self.root,self.frame)
        before=self.stored()
        original=module.open_exact_dataset
        class AlteredRead:
            def __init__(self,dataset):self.dataset=dataset
            def to_table(self,*args,**kwargs):
                table=self.dataset.to_table(*args,**kwargs)
                index=table.schema.get_field_index('quality_reason')
                return table.set_column(index,table.schema.field(index),pa.array(['injected different value']*table.num_rows,type=pa.string()))
        def opened(*args,**kwargs):
            dataset=original(*args,**kwargs)
            return AlteredRead(dataset) if args[3]=='正式外部市场日历' else dataset
        with patch.object(module,'open_exact_dataset',side_effect=opened),self.assertRaises(RuntimeError) as error:
            module.commit_calendar_partitions(self.frame,{self.day},self.root)
        self.assertIsInstance(error.exception.__cause__,ValueError)
        self.assertEqual(before,self.stored())
        self.assertTrue(list((self.root/'silver').glob('.*.failed-*')))

    def test_empty_calendar_needs_no_http_or_write(self):
        fixtures.write_exact_calendar(self.root,self.frame.iloc[:0])
        before=self.stored()
        with patch.object(module,'create_http_session') as session:
            result=CliRunner().invoke(module.main,['--lake-root',str(self.root),'--write'])
        self.assertEqual(result.exit_code,0,result.output)
        session.assert_not_called()
        self.assertEqual(before,self.stored())


if __name__=='__main__':
    unittest.main()
