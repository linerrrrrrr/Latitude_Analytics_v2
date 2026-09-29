"""a03/b01 校验与分区读取收缩的差分核对；仅写临时湖，不调用来源 API。"""
import ast
import hashlib
import importlib.util
import io
import json
import pathlib
import shutil
import sys
import tempfile
import tokenize
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

import nbformat
import pandas as pd
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT=pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT=pathlib.Path(sys.argv[1])
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b01_external_market_calendar.ipynb')
old=json.loads((SNAPSHOT/RELATIVE).read_text(encoding='utf8'))
new=json.loads((ROOT/RELATIVE).read_text(encoding='utf8'))
code=lambda n:'\n\n'.join(''.join(c['source']) for c in n['cells'] if c['cell_type']=='code')
comments=lambda s:[t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type==tokenize.COMMENT]

old_cells={c['id']:c for c in old['cells']}
new_cells={c['id']:c for c in new['cells']}
assert [c['id'] for c in new['cells'] if c['id'] in old_cells]==list(old_cells)
assert {k:v for k,v in old.items() if k!='cells'}=={k:v for k,v in new.items() if k!='cells'}
for cell_id,old_cell in old_cells.items():
    assert {k:v for k,v in old_cell.items() if k!='source'}=={k:v for k,v in new_cells[cell_id].items() if k!='source'},cell_id
assert old_cells['5dc7bd54']==new_cells['5dc7bd54'], 'execution entry changed'
for i,c in enumerate(new['cells']):
    if c['cell_type']=='code':assert '```mermaid' in ''.join(new['cells'][i-1]['source']),c['id']
assert sum('```mermaid' in ''.join(c['source']) for c in new['cells'])==14
n=nbformat.read(ROOT/RELATIVE,as_version=4)
nbformat.validate(n)
source,_=PythonExporter().from_notebook_node(n)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==source.encode('utf8')
compile(source,str(RELATIVE),'exec')

def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

before=load('a03_b01_before_docs',SNAPSHOT/RELATIVE.with_suffix('.py'))
after=load('a03_b01_after_docs',ROOT/RELATIVE.with_suffix('.py'))
sys.path.insert(0,str(ROOT/'00_draft_collection_02/tests'))
import test_b03_metadata_upgrade as fixtures

NOW=datetime.now(timezone.utc)
class FrozenDatetime(datetime):
    @classmethod
    def now(cls,tz=None):return NOW

def hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*.parquet')}

def write_table(lake,frame,*,upstream=False,stale=False):
    schema=after.TRADE_CALENDAR_SCHEMA if upstream else after.EXTERNAL_MARKET_CALENDAR_SCHEMA
    fixtures.write_partitioned_table(after,frame,schema,after.UPSTREAM_PARTITION_COLUMNS if upstream else after.PARTITION_COLUMNS,lake/'silver'/(after.UPSTREAM_TABLE_NAME if upstream else after.TABLE_NAME),write_schema=fixtures.schema_with_stale_metadata(schema) if stale else None)

trace_names=('build_expected_calendar','changed_partition_keys','commit_partitions')
def invoke(module,lake,arguments,scenario):
    calls=[]
    original_open=module.open_exact_dataset
    def open_dataset(*args,**kwargs):
        if scenario=='formal_failure' and args[-1]=='正式外部市场日历':
            raise ValueError('injected formal rejection')
        return original_open(*args,**kwargs)
    def traced(name,function):
        def call(*args,**kwargs):
            calls.append(name)
            return function(*args,**kwargs)
        return call
    from contextlib import ExitStack
    with ExitStack() as stack:
        stack.enter_context(mock.patch.object(module,'datetime',FrozenDatetime))
        stack.enter_context(mock.patch.object(module.uuid,'uuid4',return_value=SimpleNamespace(hex='fixed_run')))
        stack.enter_context(mock.patch.object(module,'open_exact_dataset',side_effect=open_dataset))
        for name in trace_names:
            stack.enter_context(mock.patch.object(module,name,side_effect=traced(name,getattr(module,name))))
        result=CliRunner().invoke(module.main,['--lake-root',str(lake),*arguments],standalone_mode=False)
    return result,calls

scenarios=[]
with tempfile.TemporaryDirectory(prefix='a03-b01-docs-log-check-') as directory:
    temporary=pathlib.Path(directory)
    upstream=fixtures.build_trade_calendar_frame(after)
    expected=after.build_expected_calendar(upstream,after.empty_pandas(after.EXTERNAL_MARKET_CALENDAR_SCHEMA),NOW)
    states=expected.copy()
    for dataset,status,count,quality in (
        ('domestic_spot_basis','success',1,'passed'),
        ('overseas_futures','success',2,'warning'),
        ('external_index','retryable_error',0,'failed'),
    ):
        mask=states['dataset_name'].eq(dataset)&states['is_fetch_required']
        states.loc[mask,'is_fetch_completed']=status=='success'
        states.loc[mask,'fetch_result_status']=status
        states.loc[mask,'actual_record_count']=count
        states.loc[mask,'quality_status']=quality
        states.loc[mask,'quality_reason']='保留既有来源证据。'
        states.loc[mask,'fetch_run_id']='prior_run'
        states.loc[mask,'fetch_completed_at']=NOW if status=='success' else None
        states.loc[mask,'quality_checked_at']=NOW
    after.validate_external_calendar_table(after.pandas_to_arrow(states,after.EXTERNAL_MARKET_CALENDAR_SCHEMA),'fixture')
    for scenario in ('readonly','initial_write','up_to_date','gap','explicit','metadata_upgrade','legacy_domestic','empty_upstream','formal_failure'):
        seed=temporary/(scenario+'-seed')
        existing=None
        selected_upstream=upstream
        arguments=[] if scenario=='readonly' else ['--write']
        if scenario=='up_to_date':existing=states
        if scenario in ('gap','explicit','formal_failure'):existing=states.iloc[1:].copy()
        if scenario=='explicit':arguments+=['--start-date','2026-07-31','--end-date','2026-07-31']
        if scenario=='metadata_upgrade':existing=states
        if scenario=='legacy_domestic':selected_upstream,existing=fixtures.build_legacy_domestic_migration_frames(after)
        if scenario=='empty_upstream':
            selected_upstream=after.empty_pandas(after.TRADE_CALENDAR_SCHEMA)
            existing=states
        write_table(seed,selected_upstream,upstream=True)
        if existing is not None:write_table(seed,existing,stale=scenario in ('metadata_upgrade','legacy_domestic'))
        initial=hashes(seed)
        outcomes=[]
        for module,label in ((before,'before'),(after,'after')):
            lake=temporary/(scenario+'-'+label)
            shutil.copytree(seed,lake)
            result,calls=invoke(module,lake,arguments,scenario)
            (SNAPSHOT/(scenario+'-'+label+'.log')).write_text(result.output,encoding='utf8')
            outcomes.append((result,calls,hashes(lake)))
        old_result,old_trace,old_hashes=outcomes[0]
        result,trace,current_hashes=outcomes[1]
        assert old_result.exit_code==result.exit_code,scenario
        assert old_trace==trace,scenario
        assert old_hashes==current_hashes,scenario
        assert type(old_result.exception) is type(result.exception),scenario
        assert str(old_result.exception).replace(str(temporary/(scenario+'-before')), '<LAKE>')==str(result.exception).replace(str(temporary/(scenario+'-after')), '<LAKE>'),scenario
        assert type(old_result.exception.__cause__) is type(result.exception.__cause__) if result.exception else True
        if scenario=='formal_failure':
            formal_hashes={key:value for key,value in current_hashes.items() if not any(part.startswith('.a03-b01-') for part in pathlib.PurePosixPath(key).parts)}
            assert result.exit_code!=0 and formal_hashes==initial
            assert 'failed_phase=commit;' in result.output
            assert 'partition_committed:' not in result.output and 'phase=run; status=completed;' not in result.output
        else:
            assert result.exit_code==0,result.output
            assert 'phase=run; status=completed;' in result.output
            assert result.output.count('='*88)==2
            if scenario in ('readonly','up_to_date'):
                assert current_hashes==initial
                assert 'partition_committed:' not in result.output
            else:
                assert result.output.count('partition_committed:')==1
                assert 'scope=batch; persisted=true;' in result.output
        if scenario=='empty_upstream':assert 'full_root_swap=true' in result.output
        if scenario=='up_to_date':assert 'outcome=up_to_date' in result.output
        if scenario!='formal_failure':
            assert 'function=build_expected_calendar; phase=generate; status=completed;' in result.output
            assert 'function=changed_partition_keys; phase=compare; status=completed;' in result.output
            assert 'function=open_compatible_dataset; phase=dataset_open; status=completed;' in result.output
        success_lines=[line for line in result.output.splitlines() if line.startswith('partition_committed:')]
        assert all('function=commit_partitions;' in line for line in success_lines)
        if success_lines:
            assert result.output.index('function=commit_partitions; phase=formal_readback; status=completed') < result.output.index('partition_committed:')
            assert result.output.index('partition_committed:') < result.output.index('function=commit_partitions; phase=calendar_state; status=completed')
        else:
            assert 'phase=calendar_state; status=completed;' not in result.output

        scenarios.append({'scenario':scenario,'same_parquet_bytes':True,'same_business_call_trace':True,'same_exception_type_and_message':True})

    # Parameter failure must occur before any dataset is opened.
    for scenario,args in (
        ('unpaired_dates',['--start-date','2026-07-31']),
        ('reversed_dates',['--start-date','2026-08-01','--end-date','2026-07-31']),
        ('formal_date_write',['--start-date','2026-07-31','--end-date','2026-08-01','--write']),
    ):
        results=[]
        for module in (before,after):
            with mock.patch.object(module,'open_exact_dataset',side_effect=AssertionError('argument failure must not read lake')):
                results.append(CliRunner().invoke(module.main,args,standalone_mode=False))
        assert results[0].exit_code==results[1].exit_code!=0
        assert type(results[0].exception) is type(results[1].exception)
        assert str(results[0].exception)==str(results[1].exception)
        assert 'failed_phase=arguments;' in results[1].output
        scenarios.append({'scenario':scenario,'same_exception_type_and_message':True,'no_lake_io':True})

hashes_before=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,digest in hashes_before.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
assert set(changed)=={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix(),'02_Futures_Lakehouse/AGENTS.md','02_Futures_Lakehouse/README.md','00_draft_collection_02/tests/test_b03_metadata_upgrade.py','00_draft_collection_02/tests/test_a03_b01_shared_transaction.py'},changed
report={'generation_compare_commit_call_order_unchanged':True,'original_cell_state_preserved':True,'execution_entry_unchanged':True,'default_export_exact':True,'flowcharts':14,'code_cells':13,'scenarios':scenarios,'scenario_count':len(scenarios),'changed_production_files':changed,'real_api_calls':0,'formal_lake_writes':0,'assumptions':'使用契约化临时上游与日历，固定时间和批次比较字节；包含原有旧 metadata 和旧生意社状态迁移。只对临时湖主动注入正式验收失败，不假设正式文件损坏；本轮信任契约化正式输入，收缩重复校验和转换；正常输出、安装恢复、入口和完整内容摘要应保持一致。'}
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
