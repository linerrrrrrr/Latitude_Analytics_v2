"""a04/b02 文档与日志检查：业务 AST、原单元格状态及隔离日志行为。"""
import ast
import contextlib
import copy
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

ROOT=pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT=pathlib.Path(sys.argv[1])
REL=pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb')
old=nbformat.read(SNAPSHOT/REL,4)
new=nbformat.read(ROOT/REL,4)
code=lambda nb:'\n\n'.join(c.source for c in nb.cells if c.cell_type=='code')
old_code,new_code=code(old),code(new)


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


assert ast.dump(WithoutLogs().visit(ast.parse(old_code)))==ast.dump(WithoutLogs().visit(ast.parse(new_code)))
defs=lambda src:{n.name:ast.dump(n) for n in ast.parse(src).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
old_defs,new_defs=defs(old_code),defs(new_code)
unchanged=[name for name in old_defs if old_defs[name]==new_defs[name]]
assert set(old_defs)-set(unchanged)=={'main'}
comments=lambda src:[token.string for token in tokenize.generate_tokens(io.StringIO(src).readline) if token.type==tokenize.COMMENT]
assert comments(old_code)==comments(new_code)
assert old.metadata==new.metadata
new_by_id={c.id:c for c in new.cells}
for c in old.cells:
    assert {k:v for k,v in c.items() if k!='source'}=={k:v for k,v in new_by_id[c.id].items() if k!='source'}
assert next(c.source for c in old.cells if c.id=='de9e1f3f')==new_by_id['de9e1f3f'].source
for index,c in enumerate(new.cells):
    if c.cell_type=='code':
        assert '```mermaid' in new.cells[index-1].source
nbformat.validate(new)
generated,_=PythonExporter().from_notebook_node(new)
script=(ROOT/REL).with_suffix('.py')
assert script.read_bytes()==generated.encode('utf8')
assert b'\r\n' not in script.read_bytes() and b'\r\n' not in (ROOT/REL).read_bytes()
ast.parse(generated)
rendered=json.loads((SNAPSHOT/'flowcharts/render_results.json').read_text(encoding='utf8'))
assert len(rendered)==31 and not any(c['clipped'] for c in rendered)


def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


fixtures=load('a04_b02_log_fixtures',ROOT/'00_draft_collection_02/tests/test_b04_c02_interest_rate.py')
module=fixtures.C02
runner=CliRunner()
checks=[]
with tempfile.TemporaryDirectory(prefix='shibor-log-check-') as directory:
    lake=pathlib.Path(directory)
    with contextlib.redirect_stdout(io.StringIO()):
        fixtures.build_calendar(lake,date(2026,8,17),date(2026,8,17))
    before_files={p.relative_to(lake):p.read_bytes() for p in lake.rglob('*') if p.is_file()}
    settings=SimpleNamespace(futures_lake_root=lake)
    fake=fixtures.FakeTushareClient(fixtures.shibor_response(date(2026,8,17)))
    with patch.object(module,'settings',settings),patch.object(module,'create_tushare_client',return_value=fake):
        readonly=runner.invoke(module.main,[])
        assert readonly.exit_code==0,readonly.exception
        assert 'phase=request; status=started' in readonly.output
        assert 'phase=normalize_merge; status=completed' in readonly.output
        assert 'persisted=true' not in readonly.output and 'committed:' not in readonly.output
        assert readonly.output.splitlines()[0]==readonly.output.splitlines()[-1]=='='*80
        assert len(fake.calls)==1
        checks.append('readonly_calls_fake_api_without_writing_or_claiming_commit')
        injected=OSError('injected fact commit failure')
        with patch.object(module,'commit_complete_fact_partition',side_effect=injected):
            failed=runner.invoke(module.main,['--write'])
        assert failed.exception is injected
        assert 'phase=fact_commit; status=failed; error=OSError' in failed.output
        assert 'committed:' not in failed.output and 'phase=run; status=completed' not in failed.output
        assert failed.output.splitlines()[-1]=='='*80
        checks.append('commit_failure_propagates_without_success_claim')
    assert before_files=={p.relative_to(lake):p.read_bytes() for p in lake.rglob('*') if p.is_file()}
    with patch.object(module,'settings',settings),patch.object(module,'read_interest_calendar',side_effect=AssertionError('must not read')):
        invalid=runner.invoke(module.main,['--start-date','2026-08-17','--end-date','2026-08-17','--write'])
    assert invalid.exit_code==2
    assert 'phase=parameters; status=failed; error=UsageError' in invalid.output
    checks.append('date_write_gate_precedes_lake_io')
    bad=fixtures.FakeTushareClient(fixtures.shibor_response(date(2026,8,17)).drop(columns=['on']))
    with patch.object(module,'settings',settings),patch.object(module,'create_tushare_client',return_value=bad):
        source_failed=runner.invoke(module.main,['--write'])
    assert source_failed.exit_code==1
    assert 'calendar_state=failed; is_fetch_completed=false; persisted=true' in source_failed.output
    assert 'phase=run; status=completed' not in source_failed.output
    assert len(bad.calls)==1
    checks.append('source_failure_records_incomplete_state_without_retry')
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
