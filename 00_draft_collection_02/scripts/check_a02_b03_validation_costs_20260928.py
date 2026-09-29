"""对比本轮转换次数、根标记 I/O、分区读取范围及保留的质量门禁。"""
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import shutil
import sys
import tempfile
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pyarrow as pa
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b03_warehouse_receipt.py')
sys.path.insert(0,str(ROOT/'00_draft_collection_02/tests'))
import test_b02_warehouse_receipt as fixtures

after = fixtures.MODULE
spec = importlib.util.spec_from_file_location('warehouse_validation_before',SNAPSHOT/RELATIVE)
before = importlib.util.module_from_spec(spec)
spec.loader.exec_module(before)
DAY = date(2024,3,1)
NEXT_DAY = date(2024,3,4)
NOW = datetime.now(timezone.utc)
report = {}

class FrozenDatetime(datetime):
    @classmethod
    def now(cls,tz=None): return NOW

def hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*.parquet')}

with tempfile.TemporaryDirectory(prefix='warehouse-validation-costs-') as directory, contextlib.redirect_stdout(io.StringIO()):
    temporary = pathlib.Path(directory)
    # Both table schemas retain the same marker decisions while only one footer is read.
    for schema,columns,partitioning in (
        (after.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,after.PARTITION_COLUMNS,after.FACT_PARTITIONING),
        (after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,after.CALENDAR_PARTITION_COLUMNS,after.CALENDAR_PARTITIONING),
    ):
        name = schema.metadata[b'table_name'].decode()
        target = temporary/name
        fixtures.write_exact_dataset(target,after.empty_pandas(schema),schema,columns,partitioning)
        counts = []
        for module in (before,after):
            with mock.patch.object(module.pq,'read_metadata',wraps=module.pq.read_metadata) as metadata, mock.patch.object(module.pq,'read_schema',wraps=module.pq.read_schema) as read_schema:
                assert module.validate_table_marker(target,schema,columns,'test',required=True)
            counts.append({'read_metadata':metadata.call_count,'read_schema':read_schema.call_count})
        assert counts == [{'read_metadata':1,'read_schema':1},{'read_metadata':1,'read_schema':0}]
        report[name+'_marker_footer_reads'] = counts

    schema = after.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA
    months = [DAY,date(2024,4,1)]
    calendar = pd.concat([fixtures.calendar_frame(day,underlying_code=code) for day in months for code in ('EB','EG')],ignore_index=True)
    seed = temporary/'calendar-seed'
    fixtures.write_exact_dataset(seed/'silver'/after.CALENDAR_TABLE_NAME,calendar,schema,after.CALENDAR_PARTITION_COLUMNS,after.CALENDAR_PARTITIONING)
    completed = pd.concat([
        fixtures.calendar_frame(day,underlying_code=code,is_fetch_completed=(code=='EB'),fetch_result_status='success' if code=='EB' else 'pending',actual_record_count=1 if code=='EB' else 0,quality_status='passed' if code=='EB' else 'pending')
        for day in months for code in ('EB','EG')
    ],ignore_index=True)
    touched = {('XDCE','EB',day) for day in months}
    markers, outputs = [],[]
    for module,label in ((before,'before'),(after,'after')):
        lake=temporary/('calendar-'+label)
        shutil.copytree(seed,lake)
        target=lake/'silver'/after.CALENDAR_TABLE_NAME
        with mock.patch.object(module,'validate_table_marker',wraps=module.validate_table_marker) as marker, mock.patch.object(module.ds,'dataset',wraps=module.ds.dataset) as dataset:
            assert module.commit_calendar_partitions(completed,touched,lake)==2
        formal_marker_calls = sum(pathlib.Path(call.args[0])==target for call in marker.call_args_list)
        assert all(pathlib.Path(call.args[0])!=target for call in dataset.call_args_list)
        markers.append(formal_marker_calls)
        outputs.append(hashes(lake))
    assert markers==[4,1], markers
    assert outputs[0]==outputs[1]
    report['two_calendar_leaves']={'formal_marker_checks_before_after':markers,'same_parquet_bytes':True,'table_root_dataset_opens':0,'other_underlying_rows_preserved':True}

    # A mixed empty/nonempty month exercises the removed DataFrame round trip.
    seed=temporary/'main-seed'
    calendar=pd.concat([fixtures.calendar_frame(day) for day in (DAY,NEXT_DAY)],ignore_index=True)
    fixtures.write_exact_dataset(seed/'silver'/after.CALENDAR_TABLE_NAME,calendar,schema,after.CALENDAR_PARTITION_COLUMNS,after.CALENDAR_PARTITIONING)
    conversions, monthly_frames, outputs, opened_counts = [],[],[],[]
    for module,label in ((before,'before'),(after,'after')):
        lake=temporary/('main-'+label)
        shutil.copytree(seed,lake)
        jqdata=mock.MagicMock()
        jqdata.finance.run_query.side_effect=[pd.DataFrame(),fixtures.valid_raw_response(NEXT_DAY)]
        original_merge=module.full_fact_partition
        def merge(existing_df,incoming_df,touched_dates):
            monthly_frames.append(incoming_df.copy())
            return original_merge(existing_df,incoming_df,touched_dates)
        with mock.patch.object(module,'datetime',FrozenDatetime), mock.patch.object(module.uuid,'uuid4',return_value=SimpleNamespace(hex='fixed_run')), mock.patch.object(module,'authenticate_jqdata',return_value=jqdata), mock.patch.object(module,'pandas_to_arrow',wraps=module.pandas_to_arrow) as to_arrow, mock.patch.object(module,'arrow_to_pandas',wraps=module.arrow_to_pandas) as to_pandas, mock.patch.object(module,'full_fact_partition',side_effect=merge), mock.patch.object(module.ds,'dataset',wraps=module.ds.dataset) as dataset:
            result=CliRunner().invoke(module.main,['--lake-root',str(lake),'--write'])
        assert result.exit_code==0,result.output
        assert jqdata.finance.run_query.call_count==2
        conversions.append([to_arrow.call_count,to_pandas.call_count])
        opened=[pathlib.Path(call.args[0]) for call in dataset.call_args_list]
        opened_counts.append({'fact_root':opened.count(lake/'silver'/after.TABLE_NAME),'calendar_root':opened.count(lake/'silver'/after.CALENDAR_TABLE_NAME)})
        assert all(p in (lake/'silver'/after.CALENDAR_TABLE_NAME,) or p.name=='month=3' for p in opened)
        outputs.append(hashes(lake))
        (SNAPSHOT/('mixed-month-'+label+'.log')).write_text(result.output,encoding='utf8')
    assert conversions[0][0]-conversions[1][0]==1,conversions
    assert conversions[0][1]-conversions[1][1]==1,conversions
    pd.testing.assert_frame_equal(monthly_frames[0],monthly_frames[1],check_dtype=True)
    assert outputs[0]==outputs[1]
    assert opened_counts==[{'fact_root':0,'calendar_root':1}]*2
    report['mixed_empty_nonempty_month']={'pandas_to_arrow_and_arrow_to_pandas_before_after':conversions,'same_pandas_dtypes_and_values':True,'same_parquet_bytes':True,'root_dataset_opens':opened_counts,'api_calls_per_run':2}

    first=fixtures.warehouse_frame(DAY,warehouse_name='仓库甲')
    second=fixtures.warehouse_frame(DAY,warehouse_name='仓库乙')
    valid=pd.concat([first,second],ignore_index=True)
    duplicate=pd.concat([first,first],ignore_index=True)
    schema=after.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA
    valid_table=after.pandas_to_arrow(valid,schema)
    duplicate_table=after.pandas_to_arrow(duplicate,schema)
    assert len(valid_table)==len(duplicate_table)
    assert after.primary_key_summary(valid_table,after.PRIMARY_KEY)!=after.primary_key_summary(duplicate_table,after.PRIMARY_KEY)
    assert after.primary_key_summary(valid_table,after.PRIMARY_KEY)==after.primary_key_summary(valid_table.take(pa.array([1,0])),after.PRIMARY_KEY)
    for module in (before,after):
        try:
            module.validate_warehouse_frame(duplicate,'duplicate fixture')
        except ValueError as error:
            assert '主键不唯一' in str(error)
        else:
            raise AssertionError('dirty validator accepted duplicates')
    report['primary_key_contract']={'dirty_duplicate_rejected_before_and_after':True,'same_count_duplicate_changes_digest':True,'row_order_does_not_change_digest':True}

    # Inject a faulty write in a temporary staging leaf, retaining row count.
    lake=temporary/'staging-duplicate'
    target=lake/'silver'/after.TABLE_NAME
    fixtures.write_exact_dataset(target,first,schema,after.PARTITION_COLUMNS,after.FACT_PARTITIONING)
    initial=hashes(lake)
    original_write=after.ds.write_dataset
    def write(data,*args,**kwargs):
        return original_write(pa.concat_tables([data.slice(0,1)]*2),*args,**kwargs)
    with mock.patch.object(after.ds,'write_dataset',side_effect=write):
        try:
            after.commit_complete_partition(valid,lake,('XDCE','EB',2024,3))
        except ValueError as error:
            assert 'staging 行数或主键摘要检查失败' in str(error)
        else:
            raise AssertionError('staging duplicate was accepted')
    assert hashes(lake)==initial
    assert not list((lake/'silver').glob('.*'))
    report['staging_duplicate']={'same_count_bad_keys_rejected':True,'formal_unchanged':True,'staging_cleaned':True}

(SNAPSHOT/'cost_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
