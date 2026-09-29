"""核对 b03 文档与日志改动不影响业务；只使用临时湖和模拟来源。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import shutil
import sys
import tempfile
import tokenize
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import nbformat
import pandas as pd
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb')
old = nbformat.read(SNAPSHOT/RELATIVE, as_version=4)
new = nbformat.read(ROOT/RELATIVE, as_version=4)
code = lambda notebook: '\n\n'.join(c.source for c in notebook.cells if c.cell_type=='code')

class BusinessTree(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func)=='click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_Import(self, node):
        if len(node.names)==1 and node.names[0].name=='time': return None
        return node

    def visit_Try(self, node):
        if len(node.handlers)==1 and node.handlers[0].name=='log_error':
            assert not node.orelse and not node.finalbody
            assert isinstance(node.handlers[0].body[-1], ast.Raise)
            assert node.handlers[0].body[-1].exc is None
            return self.generic_visit(node).body
        return self.generic_visit(node)

assert ast.dump(BusinessTree().visit(ast.parse(code(old))))==ast.dump(BusinessTree().visit(ast.parse(code(new))))
comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type==tokenize.COMMENT]
assert comments(code(old))==comments(code(new))
old_cells={c.id:c for c in old.cells}
new_cells={c.id:c for c in new.cells}
assert old.metadata==new.metadata
assert [c.id for c in new.cells if c.id in old_cells]==list(old_cells)
for cell_id, cell in old_cells.items():
    assert {k:v for k,v in cell.items() if k!='source'}=={k:v for k,v in new_cells[cell_id].items() if k!='source'},cell_id
assert old_cells['b03-c03-21']==new_cells['b03-c03-21']
for index, cell in enumerate(new.cells):
    if cell.cell_type=='code': assert '```mermaid' in new.cells[index-1].source,cell.id
assert sum('```mermaid' in c.source for c in new.cells)==19
nbformat.validate(new)
exported,_=PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==exported.encode('utf8')
assert b'\r\n' not in (ROOT/RELATIVE).read_bytes()
compile(exported,str(RELATIVE),'exec')

def load(name, path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

before=load('overseas_before_docs',SNAPSHOT/RELATIVE.with_suffix('.py'))
after=load('overseas_after_docs',ROOT/RELATIVE.with_suffix('.py'))
sys.path.insert(0,str(ROOT/'00_draft_collection_02/tests'))
import test_b03_metadata_upgrade as fixtures

NOW=datetime.now(timezone.utc)-timedelta(seconds=1)
DAY=date(2026,7,31)
SECOND=date(2026,8,3)
class FrozenDatetime(datetime):
    @classmethod
    def now(cls,tz=None): return NOW

def raw(day, warning=False):
    return pd.DataFrame([dict(id='source-'+day.isoformat(),code='OVERSEAS',name='境外期货',day=day,
                             open=105.0 if warning else 100.0,high=102.0,low=99.0,close=101.0,
                             volume=10.0,change_pct=float('nan'),amplitude=3.0,pre_close=None)])

def pending_calendar(days):
    return pd.DataFrame([dict(dataset_name=after.DATASET_NAME,entity_code=after.ENTITY_CODE,
        observation_date=day,is_fetch_required=True,requirement_reason='测试 required 日期',
        is_fetch_completed=False,fetch_result_status='pending',is_data_missing=False,
        actual_record_count=0,quality_status='pending',quality_reason='待采集',fetch_run_id=None,
        fetch_completed_at=None,quality_checked_at=None,updated_at=NOW-timedelta(seconds=1),
        year=day.year,month=day.month) for day in days])

def write_table(lake,frame,calendar,stale=False):
    schema=after.EXTERNAL_MARKET_CALENDAR_SCHEMA if calendar else after.OVERSEAS_FUTURES_DAILY_SCHEMA
    fixtures.write_partitioned_table(after,frame,schema,
        after.CALENDAR_PARTITION_COLUMNS if calendar else after.PARTITION_COLUMNS,
        lake/'silver'/(after.CALENDAR_TABLE_NAME if calendar else after.TABLE_NAME),
        write_schema=fixtures.schema_with_stale_metadata(schema) if stale else None)

def hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}

trace_names=tuple(n.name for n in ast.parse(code(old)).body if isinstance(n,ast.FunctionDef) and n.name!='main')
def invoke(module,lake,args,scenario):
    trace=[]
    query_dates=[]
    calendar_open_count=0
    original_open=module.open_exact_dataset
    def open_dataset(*args,**kwargs):
        nonlocal calendar_open_count
        if args[-1]=='正式外部市场日历':
            calendar_open_count+=1
            if scenario=='calendar_formal_failure' and calendar_open_count==2:
                raise ValueError('injected calendar formal readback failure')
        if scenario=='fact_formal_failure' and args[-1]=='正式境外期货事实':
            raise ValueError('injected fact formal readback failure')
        return original_open(*args,**kwargs)
    def query(jqdata,day):
        query_dates.append(day.isoformat())
        if scenario=='query_failure': raise RuntimeError('retryable_error: injected query failure')
        if scenario=='empty_response': return pd.DataFrame()
        result=raw(day,warning=scenario=='warning')
        if scenario=='invalid_response': result.loc[0,'open']=float('inf')
        return result
    def traced(name,function):
        def call(*args,**kwargs):
            trace.append(name)
            return function(*args,**kwargs)
        return call
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(module,'datetime',FrozenDatetime))
        stack.enter_context(patch.object(module.uuid,'uuid4',return_value=SimpleNamespace(hex='fixed_batch')))
        auth=stack.enter_context(patch.object(module,'authenticate_jqdata',return_value=SimpleNamespace()))
        stack.enter_context(patch.object(module,'query_overseas_futures_grid',side_effect=query))
        stack.enter_context(patch.object(module,'open_exact_dataset',side_effect=open_dataset))
        for name in trace_names:
            stack.enter_context(patch.object(module,name,side_effect=traced(name,getattr(module,name))))
        result=CliRunner().invoke(module.main,['--lake-root',str(lake),*args],standalone_mode=False)
    return result,trace,hashes(lake),query_dates,auth.call_count

reports=[]
with tempfile.TemporaryDirectory(prefix='a03-b03-docs-diff-') as directory:
    temp=pathlib.Path(directory)
    scenarios=('readonly','initial_write','empty_response','warning','complete','repair_readonly','repair_write',
               'mixed_repair_fetch','two_months','metadata_upgrade','query_failure','invalid_response',
               'fact_formal_failure','calendar_formal_failure')
    for scenario in scenarios:
        seed=temp/(scenario+'-seed')
        calendar=pending_calendar([DAY,SECOND] if scenario in ('mixed_repair_fetch','two_months') else [DAY])
        facts=after.normalize_overseas_futures_response(raw(DAY),DAY,NOW)
        if scenario in ('complete','repair_readonly','repair_write','mixed_repair_fetch','metadata_upgrade'):
            write_table(seed,facts,False,stale=scenario=='metadata_upgrade')
            if scenario in ('complete','metadata_upgrade'):
                calendar=after.apply_calendar_completion(calendar,{DAY:1},{},'prior',NOW)
        write_table(seed,calendar,True)
        args=[] if scenario in ('readonly','repair_readonly') else ['--write']
        outcomes=[]
        for module,label in ((before,'before'),(after,'after')):
            lake=temp/(scenario+'-'+label)
            shutil.copytree(seed,lake)
            result=invoke(module,lake,args,scenario)
            (SNAPSHOT/(scenario+'-'+label+'.log')).write_text(result[0].output,encoding='utf8')
            outcomes.append(result)
        old_result,old_trace,old_hashes,old_dates,old_auth=outcomes[0]
        result,trace,actual_hashes,dates,auth=outcomes[1]
        assert old_result.exit_code==result.exit_code,(scenario,old_result.exception,result.exception,result.output)
        assert type(old_result.exception) is type(result.exception),scenario
        assert str(old_result.exception)==str(result.exception),scenario
        if result.exception:
            assert type(old_result.exception.__cause__) is type(result.exception.__cause__),scenario
        assert old_trace==trace,scenario
        assert old_hashes==actual_hashes,scenario
        assert (old_dates,old_auth)==(dates,auth),scenario
        assert result.output.count('='*88)==2,(scenario,result.output)
        if scenario in ('query_failure','invalid_response','fact_formal_failure','calendar_formal_failure'):
            assert result.exit_code!=0 and 'phase=run; status=failed;' in result.output,scenario
            expected={'query_failure':'fetch','invalid_response':'normalize','fact_formal_failure':'fact_commit','calendar_formal_failure':'calendar_commit'}[scenario]
            assert f'failed_phase={expected};' in result.output,(scenario,result.output)
            assert 'phase=run; status=completed;' not in result.output and 'partition_committed:' not in result.output
        else:
            assert result.exit_code==0,(scenario,result.exception,result.output)
            assert 'phase=run; status=completed;' in result.output
        if scenario in ('complete','repair_readonly','repair_write','metadata_upgrade'): assert auth==0 and not dates,scenario
        if scenario in ('readonly','repair_readonly','complete'):
            assert actual_hashes==hashes(seed),scenario
            assert 'partition_committed:' not in result.output
        if scenario=='warning': assert 'api_quality_warning:' in result.output
        for line in result.output.splitlines():
            if 'function=main;' in line:
                assert all(key in line for key in ('phase=','status=','elapsed_s=')),line
                assert line.startswith(('planning_progress:','reconciliation_plan:','state_repair_plan:','partition_start:','partition_committed:','api_success:','finished:')),line
        reports.append({'scenario':scenario,'same_file_bytes':True,'same_business_call_trace':True,
                        'same_outcome':True,'api_dates':dates,'authentication_calls':auth})
    for scenario,args in (
        ('unpaired_dates',['--start-date','2026-07-31']),
        ('reversed_dates',['--start-date','2026-08-01','--end-date','2026-07-31']),
        ('formal_date_write',['--start-date','2026-07-31','--end-date','2026-07-31','--write']),
    ):
        outcomes=[]
        for module in (before,after):
            with patch.object(module,'open_exact_dataset') as opened,patch.object(module,'authenticate_jqdata') as auth:
                outcomes.append(CliRunner().invoke(module.main,args,standalone_mode=False))
                opened.assert_not_called()
                auth.assert_not_called()
        assert outcomes[0].exit_code==outcomes[1].exit_code!=0
        assert type(outcomes[0].exception) is type(outcomes[1].exception)
        assert str(outcomes[0].exception)==str(outcomes[1].exception)
        assert 'failed_phase=arguments;' in outcomes[1].output
        reports.append({'scenario':scenario,'same_outcome':True,'no_lake_or_network_io':True})

before_hashes=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,digest in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
assert set(changed)=={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix()},changed
report={'business_ast_unchanged_excluding_logs':True,'all_existing_code_comments_preserved':True,
        'original_cell_metadata_outputs_and_execution_entry_preserved':True,'default_export_exact':True,
        'flowcharts':19,'code_cells':18,'scenario_count':len(reports),'scenarios':reports,
        'changed_production_files':changed,'real_api_calls':0,'formal_lake_writes':0,
        'assumptions':'临时契约化日历/事实，模拟 JQData，固定时间和批次比较逐文件字节及全部业务函数调用顺序。旧 metadata 分支保持现状；主动注入请求、归一化、正式验收失败，仅验证日志与原行为一致，不假设正式湖损坏，不验证网络或生产耗时。'}
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
