"""本轮检查：业务等价、注释/单元格状态、图表和日志所声明的落盘边界。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import tokenize
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import nbformat
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
old = nbformat.read(SNAPSHOT/REL,4)
new = nbformat.read(ROOT/REL,4)
code = lambda nb: '\n\n'.join(c.source for c in nb.cells if c.cell_type=='code')

class WithoutLogs(ast.NodeTransformer):
    def visit_Import(self,node):
        node.names=[n for n in node.names if n.name!='time']
        return node if node.names else None

    def visit_Assign(self,node):
        if all(isinstance(n,ast.Name) and n.id.startswith('log_') for n in node.targets):
            return None
        return self.generic_visit(node)

    def visit_Expr(self,node):
        if isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='click.echo':
            return None
        return self.generic_visit(node)

    def visit_Try(self,node):
        if len(node.handlers)==1 and node.handlers[0].name=='log_error':
            body=[]
            for child in node.body:
                result=self.visit(child)
                if isinstance(result,list): body.extend(result)
                elif result is not None: body.append(result)
            return body
        return self.generic_visit(node)

assert ast.dump(WithoutLogs().visit(ast.parse(code(old))))==ast.dump(WithoutLogs().visit(ast.parse(code(new))))
defs=lambda src:{n.name:ast.dump(n) for n in ast.parse(src).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
old_defs,new_defs=defs(code(old)),defs(code(new))
unchanged=[name for name in old_defs if old_defs[name]==new_defs[name]]
assert set(old_defs)-set(unchanged)=={'main'}
comments=lambda src:[token.string for token in tokenize.generate_tokens(io.StringIO(src).readline) if token.type==tokenize.COMMENT]
assert comments(code(old))==comments(code(new))
assert old.metadata==new.metadata
new_by_id={c.id:c for c in new.cells}
for c in old.cells:
    assert {k:v for k,v in c.items() if k!='source'}=={k:v for k,v in new_by_id[c.id].items() if k!='source'}
assert old.cells[-1].source==new.cells[-1].source
for index,c in enumerate(new.cells):
    if c.cell_type=='code': assert '```mermaid' in new.cells[index-1].source
nbformat.validate(new)
generated,_=PythonExporter().from_notebook_node(new)
script=(ROOT/REL).with_suffix('.py')
assert script.read_bytes()==generated.encode('utf8')
assert b'\r\n' not in script.read_bytes() and b'\r\n' not in (ROOT/REL).read_bytes()
ast.parse(generated)
rendered=json.loads((SNAPSHOT/'flowcharts/render_results.json').read_text(encoding='utf8'))
assert len(rendered)==31 and not any(c['clipped'] for c in rendered)

spec=importlib.util.spec_from_file_location('a04_b03_log_fixtures',ROOT/'00_draft_collection_02/tests/test_b04_c03_macro_release.py')
fixtures=importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
module=fixtures.C03
runner=CliRunner()
checks=[]
with tempfile.TemporaryDirectory(prefix='macro-log-check-') as directory:
    lake=pathlib.Path(directory)
    report_date,source_date=date(2026,7,31),date(2026,7,1)
    with contextlib.redirect_stdout(io.StringIO()):
        fixtures.build_calendar(lake,report_date)
    before_files={p.relative_to(lake):p.read_bytes() for p in lake.rglob('*') if p.is_file()}
    settings=SimpleNamespace(futures_lake_root=lake)
    fake=fixtures.FakeEastmoneySession(fixtures.all_report_rows(source_date))
    with patch.object(module,'settings',settings),patch.object(module,'create_eastmoney_session',return_value=fake):
        readonly=runner.invoke(module.main,[])
        assert readonly.exit_code==0,readonly.exception
        assert 'phase=request; status=started' in readonly.output
        assert 'phase=normalize_merge; status=completed' in readonly.output
        assert 'persisted=true' not in readonly.output and 'committed:' not in readonly.output
        assert readonly.output.splitlines()[0]==readonly.output.splitlines()[-1]=='='*80
        assert len(fake.calls)==3 and fake.closed
        checks.append('readonly_calls_three_report_windows_without_writing_or_claiming_commit')
    failing_session=fixtures.FakeEastmoneySession(fixtures.all_report_rows(source_date))
    injected=OSError('injected fact commit failure')
    with patch.object(module,'settings',settings),patch.object(module,'create_eastmoney_session',return_value=failing_session),patch.object(module,'commit_complete_fact_partition',side_effect=injected):
        failed=runner.invoke(module.main,['--write'])
    assert failed.exception is injected
    assert 'phase=fact_commit; status=failed; error=OSError' in failed.output
    assert 'committed:' not in failed.output and 'phase=run; status=completed' not in failed.output
    assert failed.output.splitlines()[-1]=='='*80 and failing_session.closed
    assert len(failing_session.calls)==1
    assert before_files=={p.relative_to(lake):p.read_bytes() for p in lake.rglob('*') if p.is_file()}
    checks.append('commit_failure_propagates_and_closes_session_without_success_claim')
    with patch.object(module,'settings',settings),patch.object(module,'read_macro_calendar',side_effect=AssertionError('must not read')):
        invalid=runner.invoke(module.main,['--start-date','2026-07-31','--end-date','2026-07-31','--write'])
    assert invalid.exit_code==2
    assert 'phase=parameters; status=failed; error=UsageError' in invalid.output
    checks.append('explicit_date_formal_write_gate_precedes_lake_io')
    bad_rows=fixtures.all_report_rows(source_date)
    del bad_rows['RPT_ECONOMY_CPI'][0]['NATIONAL_SAME']
    bad=fixtures.FakeEastmoneySession(bad_rows)
    with patch.object(module,'settings',settings),patch.object(module,'create_eastmoney_session',return_value=bad):
        source_failed=runner.invoke(module.main,['--write'])
    assert source_failed.exit_code==1
    assert 'calendar_state=failed; is_fetch_completed=false; persisted=true' in source_failed.output
    assert 'phase=run; status=completed' not in source_failed.output
    assert len(bad.calls)==3 and bad.closed
    with contextlib.redirect_stdout(io.StringIO()):
        final_calendar=module.read_macro_calendar(lake/'silver'/module.CALENDAR_TABLE_NAME)
    cpi_codes={s.series_code for s in module.SERIES_BY_REPORT['RPT_ECONOMY_CPI']}
    cpi=final_calendar.loc[final_calendar['series_code'].isin(cpi_codes)]
    assert not cpi['is_fetch_completed'].any()
    assert cpi['fetch_result_status'].eq('permanent_error').all()
    checks.append('failed_source_state_is_saved_as_incomplete_while_other_reports_continue')
    for name,outcome in [('readonly',readonly),('commit_failed',failed),('parameter_gate',invalid),('source_failed',source_failed)]:
        (SNAPSHOT/(name+'.log')).write_text(outcome.output,encoding='utf8')

report={
    'business_ast_unchanged_after_removing_logs':True,
    'unchanged_non_main_definitions':unchanged,
    'original_comments_and_cell_state_preserved':True,
    'entry_unchanged':True,'flowcharts':31,'clipped_labels':0,
    'default_export_exact':True,'log_checks':checks,
    'real_api_calls':0,'formal_lake_io':False,
    'sha256':hashlib.sha256(script.read_bytes()).hexdigest(),
}
(SNAPSHOT/'docs_logs_verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
