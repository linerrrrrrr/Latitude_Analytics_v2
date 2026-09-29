"""结构、入口及旧新业务结果对照；临时湖与模拟 API，不读取正式数据。"""
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
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
import pandas as pd
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b04_external_index.ipynb')
sys.path.insert(0, str(ROOT/'00_draft_collection_02/tests'))
import test_a03_b04_shared_transaction as fixtures

old = nbformat.read(SNAPSHOT/RELATIVE, 4)
new = nbformat.read(ROOT/RELATIVE, 4)
code = lambda notebook: '\n\n'.join(c.source for c in notebook.cells if c.cell_type=='code')
old_cells = {c.id:c for c in old.cells}
new_cells = {c.id:c for c in new.cells}
assert old.metadata == new.metadata
assert [c.id for c in new.cells if c.id in old_cells] == list(old_cells)
for cell_id, cell in old_cells.items():
    assert {k:v for k,v in cell.items() if k!='source'} == {k:v for k,v in new_cells[cell_id].items() if k!='source'}
for index, cell in enumerate(new.cells):
    if cell.cell_type=='code': assert '```mermaid' in new.cells[index-1].source, cell.id
comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type==tokenize.COMMENT]
assert comments(code(old)) == comments(code(new))[:-4]
functions = lambda source: {n.name:ast.dump(n) for n in ast.parse(source).body if isinstance(n, ast.FunctionDef)}
old_functions, new_functions = functions(code(old)), functions(code(new))
changed_functions = [name for name in old_functions if old_functions[name]!=new_functions[name]]
assert set(changed_functions)=={'commit_complete_fact_partition','commit_calendar_partitions'}, changed_functions
nbformat.validate(new)
exported, _ = PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==exported.encode('utf8')
for path in (ROOT/RELATIVE, ROOT/RELATIVE.with_suffix('.py')):
    assert b'\r\n' not in path.read_bytes()
ast.parse(exported)

# 入口行为以哨兵代替 main，不执行采集。
entry = new_cells['b03-c04-21'].source
for kernel, file_present, main_name, expected in (
    (True,False,True,'notebook'), (True,True,False,'none'),
    (False,True,False,'none'), (False,True,True,'script'), (True,True,True,'script')):
    main = Mock()
    namespace = {'sys':SimpleNamespace(modules={'ipykernel':object()} if kernel else {}),
                 '__name__':'__main__' if main_name else 'imported_module', 'main':main}
    if file_present: namespace['__file__']='fixture.py'
    exec(compile(entry, '<entry>', 'exec'), namespace)
    if expected=='notebook':
        main.main.assert_called_once_with(args=[], prog_name='b04_external_index', standalone_mode=False)
        main.assert_not_called()
    elif expected=='script':
        main.assert_called_once_with()
        main.main.assert_not_called()
    else:
        main.assert_not_called()
        main.main.assert_not_called()
assert not ast.parse(new_cells['a03-b04-terminal'].source).body

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

old_module = load(SNAPSHOT/RELATIVE.with_suffix('.py'), 'index_before')
new_module = load(ROOT/RELATIVE.with_suffix('.py'), 'index_after')
NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)
class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz else NOW.replace(tzinfo=None)

def hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob('*')) if p.is_file() and not any(part.startswith('.') for part in p.relative_to(root).parts)}

reports = []
for scenario in ('readonly', 'first_write', 'empty', 'complete', 'obsolete', 'obsolete_readonly', 'mixed', 'two_categories_two_months', 'sparse', 'api_failure'):
    with tempfile.TemporaryDirectory(prefix='index-diff-') as temporary:
        temp = pathlib.Path(temporary)
        seed = temp/'seed'
        fact, calendar = fixtures.make_frames(new_module)
        if scenario!='two_categories_two_months':
            fact = fact.loc[fact['index_code'].eq('BDI') & fact['month'].eq(7)].copy()
            calendar = calendar.loc[calendar['entity_code'].eq(fact.iloc[0]['source_indicator_id']) & calendar['month'].eq(7)].copy()
        if scenario in ('obsolete','obsolete_readonly','mixed'):
            for name, value in {'is_fetch_required':False, 'fetch_result_status':'not_required', 'quality_status':'not_applicable'}.items():
                calendar.loc[:,name] = value
        if scenario in ('mixed','sparse'):
            more = calendar.iloc[[0]].copy()
            more.loc[:,'observation_date'] = date(2026,7,3)
            more.loc[:,'is_fetch_required'] = True
            more.loc[:,'fetch_result_status'] = 'pending'
            more.loc[:,'quality_status'] = 'pending'
            calendar = pd.concat([calendar,more],ignore_index=True)
        if scenario=='sparse':
            earlier = calendar.iloc[[0]].copy()
            earlier.loc[:,'observation_date']=date(2026,7,1)
            calendar = pd.concat([earlier,calendar],ignore_index=True)
        if scenario in ('complete','sparse'):
            calendar = new_module.apply_calendar_completion(calendar,new_module.grid_count_map(fact),'prior',NOW)
        fixtures.fixtures.write_partitioned_table(new_module,calendar,new_module.EXTERNAL_MARKET_CALENDAR_SCHEMA,new_module.CALENDAR_PARTITION_COLUMNS,seed/'silver'/new_module.CALENDAR_TABLE_NAME)
        if scenario in ('complete','obsolete','obsolete_readonly','mixed','sparse'):
            fixtures.fixtures.write_partitioned_table(new_module,fact,new_module.EXTERNAL_INDEX_DAILY_SCHEMA,new_module.PARTITION_COLUMNS,seed/'silver'/new_module.TABLE_NAME)
        observations = []
        for label, owner in [('before',old_module),('after',new_module)]:
            root = temp/label
            shutil.copytree(seed,root)
            api_calls, trace = [], []
            session = Mock()
            def query(session_arg, indicator, start, end):
                api_calls.append((indicator,str(start),str(end)))
                if scenario=='api_failure': raise ValueError('retryable_error: injected request failure')
                if scenario=='empty': return []
                return [{'INDICATOR_ID':indicator,'REPORT_DATE':str(day.date()),'INDICATOR_VALUE':321.25}
                        for day in pd.date_range(start,end)]
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch.object(owner,'datetime',FrozenDatetime))
                stack.enter_context(patch.object(owner.uuid,'uuid4',return_value=SimpleNamespace(hex='fixed')))
                created=stack.enter_context(patch.object(owner,'create_eastmoney_session',return_value=session))
                stack.enter_context(patch.object(owner,'query_eastmoney_indicator_range',side_effect=query))
                for name in old_functions:
                    if name in ('main','create_eastmoney_session','query_eastmoney_indicator_range'): continue
                    original=getattr(owner,name)
                    def traced(*args,_name=name,_original=original,**kwargs):
                        trace.append(_name)
                        return _original(*args,**kwargs)
                    stack.enter_context(patch.object(owner,name,side_effect=traced))
                args=['--lake-root',str(root)]
                if scenario not in ('readonly','obsolete_readonly'): args.append('--write')
                result=CliRunner().invoke(owner.main,args)
                expected_created=0 if scenario in ('complete','obsolete','obsolete_readonly') else 1
                assert created.call_count==expected_created, (scenario, result.output)
                assert session.close.call_count==expected_created, scenario
            assert result.exit_code==(1 if scenario=='api_failure' else 0), (scenario,result.output,result.exception)
            if scenario in ('readonly','obsolete_readonly'): assert hashes(root)==hashes(seed),scenario
            if scenario=='sparse': assert [(x[1],x[2]) for x in api_calls]==[('2026-07-01','2026-07-01'),('2026-07-03','2026-07-03')]
            if scenario=='two_categories_two_months':
                persisted=owner.ds.dataset(root/'silver'/owner.CALENDAR_TABLE_NAME,format='parquet',partitioning=owner.CALENDAR_PARTITIONING).to_table().to_pandas()
                assert persisted['is_fetch_completed'].all()
            observations.append((hashes(root),api_calls,trace,result.exit_code,type(result.exception),str(result.exception)))
        assert observations[0]==observations[1],scenario
        reports.append({'scenario':scenario,'same_formal_bytes_requests_call_order':True})

with tempfile.TemporaryDirectory(prefix='index-gates-') as temporary:
    root=pathlib.Path(temporary)
    for args in (['--start-date','2026-07-01'],
                 ['--start-date','2026-07-03','--end-date','2026-07-01'],
                 ['--start-date','2026-07-01','--end-date','2026-07-03','--write']):
        with patch.object(new_module,'settings',SimpleNamespace(futures_lake_root=root)), patch.object(new_module,'open_exact_dataset') as opened, patch.object(new_module,'create_eastmoney_session') as session:
            result=CliRunner().invoke(new_module.main,['--lake-root',str(root),*args])
        assert result.exit_code==2, result.output
        opened.assert_not_called()
        session.assert_not_called()

before_hashes=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,digest in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
allowed={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix(),'AGENTS.md','02_Futures_Lakehouse/AGENTS.md','02_Futures_Lakehouse/README.md'}
assert set(changed)==allowed, changed
report={'default_export_exact':True,'original_comments_and_cell_state_preserved':True,
        'unchanged_business_functions':len(old_functions)-2,'changed_functions':changed_functions,
        'entry_branches':5,'date_gates':3,'flowcharts':sum('```mermaid' in c.source for c in new.cells),
        'differential_scenarios':reports,'changed_production_files':changed,
        'assumptions':'单写入者、同文件系统、临时契约数据、固定时间和批次、模拟 HTTP 返回；故障显式注入，不假定正式文件损坏，不模拟强杀和并发。',
        'real_api_calls':0,'formal_lake_reads_or_writes':0}
(SNAPSHOT/'transaction_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
