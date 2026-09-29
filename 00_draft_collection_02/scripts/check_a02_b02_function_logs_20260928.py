"""函数日志差分验证：模拟 API、临时湖、受控时间与异常，不操作正式湖。"""
import contextlib
import importlib.util
import io
import itertools
import json
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b02_futures_holding_reports.py')
sys.path.insert(0, str(ROOT/'00_draft_collection_02/tests'))
import test_b02_futures_holding_reports as fixtures
after = fixtures.holding_reports
spec = importlib.util.spec_from_file_location('holding_before_function_logs', SNAPSHOT/RELATIVE)
before = importlib.util.module_from_spec(spec)
spec.loader.exec_module(before)
checks, logs = [], {}


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 9, 1, tzinfo=timezone.utc)


def capture(name, fn, *args):
    with contextlib.redirect_stdout(io.StringIO()) as output:
        result = fn(*args)
    logs[name] = output.getvalue()
    return result, output.getvalue()


def failure(name, fn, *args):
    with contextlib.redirect_stdout(io.StringIO()) as output:
        try:
            fn(*args)
        except Exception as error:
            logs[name] = output.getvalue()
            return error, output.getvalue()
    raise AssertionError('expected controlled failure: ' + name)


response = pd.DataFrame([fixtures.source_row(), fixtures.source_row(member_name='期货公司', rank=1)])
for label, frame in (
    ('regular', response),
    ('empty', pd.DataFrame(columns=after.JQDATA_FIELDS)),
    ('duplicate', pd.DataFrame([fixtures.source_row(), fixtures.source_row(id=2, rank=12)])),
):
    old, _ = capture('before_'+label, before.normalize_rank_response, frame, fixtures.GRID, fixtures.UPDATED_AT)
    new, text = capture('after_'+label, after.normalize_rank_response, frame, fixtures.GRID, fixtures.UPDATED_AT)
    for a, b in zip(old[:2], new[:2], strict=True): pd.testing.assert_frame_equal(a, b)
    assert old[2] == new[2]
    assert 'function=normalize_rank_response; phase=generate_facts; status=completed;' in text
    assert 'persisted=false' in text
    if label == 'duplicate': assert 'source_quality_warning:' in text
checks.append('常规、空响应和可合并重复来源：两张 DataFrame 及 warning 与修改前逐值一致，生成日志由函数发出')

for label, frame in (
    ('bad_rank', pd.DataFrame([fixtures.source_row(rank=21)])),
    ('bad_date', pd.DataFrame([fixtures.source_row(day='2014-04-27')])),
    ('conflict', pd.DataFrame([fixtures.source_row(), fixtures.source_row(indicator=999)])),
):
    old, _ = failure('before_'+label, before.normalize_rank_response, frame, fixtures.GRID, fixtures.UPDATED_AT)
    new, text = failure('after_'+label, after.normalize_rank_response, frame, fixtures.GRID, fixtures.UPDATED_AT)
    assert type(old) is type(new) and str(old) == str(new)
    assert 'phase=generate_facts; status=failed;' in text and 'api_success:' not in text
checks.append('越界名次、错误日期、冲突重复来源：异常类型和文本保持，不输出生成成功')

with mock.patch.object(after, 'perf_counter', side_effect=itertools.count(0, 3)):
    _, text = capture('row_progress', after.normalize_rank_response, pd.DataFrame([fixtures.source_row()] * 2001), fixtures.GRID, fixtures.UPDATED_AT)
assert 'visited_rows=1000/2001' in text and 'visited_rows=2000/2001' in text
checks.append('强制日志时钟推进：原 2001 行循环在 1000/2000 行报告实际进度，无额外数据遍历')

dates = [fixtures.GRID['trading_date'], fixtures.GRID['trading_date'] + timedelta(days=1)]
large = pd.DataFrame([fixtures.source_row()] * 5000)
queried = []
for module, label in ((before,'before'),(after,'after')):
    jqdata = fixtures.query_jqdata(large, response, response.assign(day=dates[1]))
    result, text = capture(label+'_split', module.query_rank_date_batch, jqdata, 'XDCE', 'BB', dates)
    assert result[1:] == (3, 1) and jqdata.finance.run_query.call_count == 3
    queried.append(result)
for day in dates: pd.testing.assert_frame_equal(queried[0][0][day], queried[1][0][day])
assert 'completed_dates=2/2' in logs['after_split']
assert 'function=query_rank_date_batch; phase=query; status=completed;' in logs['after_split']
checks.append('5000 行日期二分：前后均 3 次请求、1 次拆分，按日响应完全一致，最终进度 2/2')

for module,label in ((before,'before'),(after,'after')):
    jqdata=fixtures.query_jqdata(large)
    error,text=failure(label+'_single_cap',module.query_rank_date_batch,jqdata,'XDCE','BB',dates[:1])
    assert isinstance(error,ValueError) and jqdata.finance.run_query.call_count==1
    assert 'query_batch_success:' not in text
checks.append('单日触顶仍失败且只请求一次，函数不误报成功')

grid_table = pa.table({'exchange_code':['XDCE']*70000,'underlying_code':['BB']*70000,'trading_date':[dates[0]]*70000})
dataset=ds.dataset(grid_table)
for module,label in ((before,'before'),(after,'after')):
    scanner=mock.Mock(wraps=dataset.scanner)
    proxy=SimpleNamespace(schema=dataset.schema,scanner=scanner)
    with mock.patch.object(module,'perf_counter',side_effect=itertools.count(0,3)):
        result,text=capture(label+'_scan',module.dataset_grid_count_map,proxy)
    assert result=={('XDCE','BB',dates[0]):70000} and scanner.call_count==1
    if module is after: assert 'batches=2; scanned_rows=70000; grids=1' in text
checks.append('70000 行窄列计数：前后都只建立一次 scanner，2 个原有批次，计数一致且报告扫描进度')

case_fixture=fixtures.PositionRankSpecialCaseTests()
case=case_fixture.special_case()
with tempfile.TemporaryDirectory(prefix='a02-b02-raw-') as directory:
    lake=pathlib.Path(directory);case_fixture.write_artifacts(lake,case)
    original_read_bytes=pathlib.Path.read_bytes
    original_read_text=pathlib.Path.read_text
    reads=[]
    def read_bytes(path):
        reads.append(path.name);return original_read_bytes(path)
    def read_text(path,*args,**kwargs):
        reads.append(path.name);return original_read_text(path,*args,**kwargs)
    with mock.patch.object(pathlib.Path,'read_bytes',read_bytes),mock.patch.object(pathlib.Path,'read_text',read_text):
        _,text=capture('raw_progress',after.verify_special_case_artifacts,lake,case)
    assert reads==['response.dat','response.sha256','calibration.json']
    assert all(f'verified_files={i}/3' in text for i in range(4))
checks.append('raw 验收仍只读原文、sidecar、清单各一次，逐项报告 0/3—3/3')

fixture=fixtures.LeafIsolationTests()
def seed(lake):
    with contextlib.redirect_stdout(io.StringIO()):
        for dataset_name in after.DATASET_NAMES:
            fixture.commit_calendar_leaf(lake,dataset_name,[fixtures.calendar_row(dataset_name,'BB',dates[0],completed=False)],2014,4)

trace_names=('open_exact_dataset','open_optional_exact_dataset','dataset_grid_count_map','read_complete_partition',
             'validate_calendar_frame','validate_position_frame','validate_member_frame','commit_complete_partition',
             'apply_calendar_completion','commit_calendar_partitions')
traces=[]; hashes=[]
with tempfile.TemporaryDirectory(prefix='a02-b02-diff-') as directory:
    for module,label in ((before,'before'),(after,'after')):
        lake=pathlib.Path(directory)/label;seed(lake)
        calls=[]
        def traced(name,fn):
            def invoke(*args,**kwargs):
                calls.append(name);return fn(*args,**kwargs)
            return invoke
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(module,'datetime',FrozenDatetime))
            stack.enter_context(mock.patch.object(module.uuid,'uuid4',return_value=SimpleNamespace(hex='fixed_run')))
            jqdata=fixtures.query_jqdata(response)
            stack.enter_context(mock.patch.object(module,'authenticate_jqdata',return_value=jqdata))
            for name in trace_names:stack.enter_context(mock.patch.object(module,name,side_effect=traced(name,getattr(module,name))))
            result=CliRunner().invoke(module.main,['--lake-root',str(lake),'--write'])
        logs[label+'_main_write']=result.output
        assert result.exit_code==0,result.output
        assert jqdata.finance.run_query.call_count==1
        traces.append(calls);hashes.append(fixtures.parquet_hashes(lake))
    assert traces[0]==traces[1] and hashes[0]==hashes[1]
    text=logs['after_main_write']
    assert text.index('function=query_rank_date_batch; phase=query; status=completed;') < text.index('function=normalize_rank_response; phase=generate_facts; status=started;')
    assert text.index('function=apply_calendar_completion; phase=generate_calendar_state; status=completed;') < text.index('function=commit_calendar_partitions; phase=calendar_state; status=completed;') < text.index('partition_timing:')
    assert text.count('partition_committed:')==4
    assert 'partition_committed: function=main' not in text and 'api_success: function=main' not in text
    before_hash=fixtures.parquet_hashes(lake)
    with mock.patch.object(after,'authenticate_jqdata',side_effect=AssertionError('no pending, no authentication')):
        result=CliRunner().invoke(after.main,['--lake-root',str(lake)])
    assert result.exit_code==0 and 'outcome=up_to_date' in result.output
    assert fixtures.parquet_hashes(lake)==before_hash
checks.append('固定批次和业务时间的临时湖端到端差分：请求均 1 次，读取/校验/提交调用顺序一致，所有 Parquet 字节一致；4 个单叶成功后才报告日历状态和月份完成')
checks.append('已完成临时湖再次检查：不认证、不请求、不改文件')

with tempfile.TemporaryDirectory(prefix='a02-b02-readonly-') as directory:
    lake=pathlib.Path(directory);seed(lake);old_hashes=fixtures.parquet_hashes(lake)
    with mock.patch.object(after,'authenticate_jqdata',return_value=fixtures.query_jqdata(response)):
        result=CliRunner().invoke(after.main,['--lake-root',str(lake)])
    logs['readonly']=result.output
    assert result.exit_code==0 and fixtures.parquet_hashes(lake)==old_hashes
    assert 'persisted=true' not in result.output and 'phase=calendar_state; status=completed;' not in result.output
checks.append('只读待办仍请求和生成，但不出现任何 persisted=true，文件保持原样')

for fail_call in (2,4):
    with tempfile.TemporaryDirectory(prefix='a02-b02-partial-') as directory:
        lake=pathlib.Path(directory);seed(lake)
        commit_count=[0];original_commit=after.commit_complete_partition
        def commit(*args,**kwargs):
            commit_count[0]+=1
            if commit_count[0]==fail_call: raise RuntimeError('injected leaf failure')
            return original_commit(*args,**kwargs)
        with mock.patch.object(after,'authenticate_jqdata',return_value=fixtures.query_jqdata(response)),mock.patch.object(after,'commit_complete_partition',side_effect=commit):
            result=CliRunner().invoke(after.main,['--lake-root',str(lake),'--write'])
        logs['partial_'+str(fail_call)]=result.output
        assert isinstance(result.exception,RuntimeError) and str(result.exception)=='injected leaf failure'
        assert commit_count[0]==fail_call
        assert result.output.count('partition_committed:')==fail_call-1
        assert 'phase=calendar_state; status=completed;' not in result.output
        assert 'partition_timing:' not in result.output and 'phase=run; status=completed;' not in result.output
        if fail_call==4: assert 'function=commit_calendar_partitions; phase=commit_calendar; status=failed;' in result.output
checks.append('第二事实叶/第二日历叶受控失败：此前成功叶日志保留，不报告整组日历、月份或运行完成')

with tempfile.TemporaryDirectory(prefix='a02-b02-rollback-log-') as directory:
    lake=pathlib.Path(directory);key=('XDCE','BB',2014,4)
    frame,_=fixtures.normalized_fact_frames('BB',dates[0],6,20)
    arguments=(frame,lake,after.POSITION_TABLE_NAME,after.FUTURES_POSITION_RANK_DAILY_SCHEMA,after.POSITION_PARTITION_COLUMNS,after.POSITION_PARTITIONING,key,after.validate_position_frame)
    capture('seed_fact',after.commit_complete_partition,*arguments)
    old_hash=fixtures.parquet_hashes(lake);original_read=after.read_complete_partition
    def reject_formal(*args,**kwargs):
        result=original_read(*args,**kwargs)
        if args[-1]=='正式 '+after.POSITION_TABLE_NAME:raise ValueError('injected formal rejection')
        return result
    with mock.patch.object(after,'read_complete_partition',side_effect=reject_formal):
        error,text=failure('rollback',after.commit_complete_partition,*arguments)
    assert isinstance(error,ValueError) and str(error)=='injected formal rejection'
    assert fixtures.parquet_hashes(lake)==old_hash
    assert 'phase=rollback; status=completed;' in text and 'partition_committed:' not in text
    assert 'failed_phase=formal_verify' in text
checks.append('正式复读受控拒绝：沿原分支恢复旧叶，字节不变，仅报告恢复与提交失败，不误报单叶成功')

for name,text in logs.items():(SNAPSHOT/(name+'.log')).write_text(text,encoding='utf8')
report={'checks':checks,'check_count':len(checks),'main_call_trace':traces[1],
        'real_api_calls':0,'formal_lake_writes':0,
        'assumptions':'来源响应用明确字段构造；时间/批次固定以便字节差分；异常仅人工注入临时湖，不声称正式文件损坏。'}
(SNAPSHOT/'runtime_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
