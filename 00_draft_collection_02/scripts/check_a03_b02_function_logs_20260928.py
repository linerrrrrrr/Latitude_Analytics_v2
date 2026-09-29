"""日志改动的业务 AST、原单元格状态与 14 个情景差分核对。"""
import ast
import contextlib
import copy
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
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.ipynb')
old = nbformat.read(SNAPSHOT/RELATIVE, as_version=4)
new = nbformat.read(ROOT/RELATIVE, as_version=4)
assert [c.id for c in old.cells] == [c.id for c in new.cells]
assert old.metadata == new.metadata
for old_cell,new_cell in zip(old.cells,new.cells,strict=True):
    assert {k:v for k,v in old_cell.items() if k!='source'} == {k:v for k,v in new_cell.items() if k!='source'}
    if old_cell.cell_type=='code':
        comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type==tokenize.COMMENT]
        assert comments(old_cell.source)==comments(new_cell.source),old_cell.id

class WithoutLogs(ast.NodeTransformer):
    def visit_Import(self,node):
        if all(alias.name=='time' for alias in node.names):
            return None
        return node

    def visit_FunctionDef(self,node):
        if node.name=='main':
            for item in ast.walk(node):
                if isinstance(item,ast.Assign) and ast.unparse(item).startswith('digest = hashlib.sha256(response_content)'):
                    item.targets[0].id='log_digest'
        return self.generic_visit(node)

    def visit_Expr(self,node):
        if isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self,node):
        if all(isinstance(t,ast.Name) and t.id.startswith('log_') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_AugAssign(self,node):
        if isinstance(node.target,ast.Name) and node.target.id.startswith('log_'):
            return None
        return self.generic_visit(node)

    def visit_If(self,node):
        node=self.generic_visit(node)
        return node if node.body else None

    def visit_Try(self,node):
        if len(node.handlers)==1 and node.handlers[0].name=='log_error':
            assert isinstance(node.handlers[0].body[-1],ast.Raise) and node.handlers[0].body[-1].exc is None
            return self.generic_visit(node).body
        return self.generic_visit(node)

def canonical(source):
    tree=WithoutLogs().visit(ast.parse(source))
    for node in ast.walk(tree):
        if not hasattr(node,'body') or not isinstance(node.body,list):
            continue
        for index in range(len(node.body)-2,-1,-1):
            assignment,following=node.body[index:index+2]
            if (isinstance(assignment,ast.Assign) and len(assignment.targets)==1
                and isinstance(assignment.targets[0],ast.Name)
                and assignment.targets[0].id in ('validated_calendar_df','planned_raw_grids')
                and isinstance(following,ast.Return) and isinstance(following.value,ast.Name)
                and following.value.id==assignment.targets[0].id):
                following.value=assignment.value
                del node.body[index]
    return ast.dump(tree)

for old_cell,new_cell in zip(old.cells,new.cells,strict=True):
    if old_cell.cell_type=='code':
        assert canonical(old_cell.source)==canonical(new_cell.source),old_cell.id
nbformat.validate(new)
source,_=PythonExporter().from_notebook_node(new)
assert source.encode('utf8')==(ROOT/RELATIVE.with_suffix('.py')).read_bytes()
assert b'\r\n' not in (ROOT/RELATIVE).read_bytes()
compile(source,str(RELATIVE),'exec')

# The existing draft comparison uses a frozen clock/id, temporary Parquet, and fake HTTP.
# Reuse that exact scenario runner; its transaction-specific structure checks do not run here.
comparison=(ROOT/'00_draft_collection_02/scripts/check_a03_b02_transaction_entry_20260928.py').read_text(encoding='utf8')
scenario_source=comparison[comparison.index('def load('):comparison.index('\nbefore_hashes =')]
exec(compile(scenario_source,'<raw scenario comparison>','exec'))

before_hashes=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,d in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=d]
assert set(changed)=={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix()},changed
for item in reports:
    if 'same_call_order' not in item:
        continue
    output=(SNAPSHOT/(item['scenario']+'-after.log')).read_text(encoding='utf8')
    assert output.count('='*88)==2,item['scenario']
    if item['scenario'].startswith('http_'):
        assert 'function=main; phase=run; status=failed; failed_phase=fetch;' in output
        assert 'function=main; phase=run; status=completed;' not in output
        assert 'completed_grids=0; persisted=true; date_watermark=none' in output
    else:
        assert 'function=main; phase=run; status=completed;' in output
    if item['scenario'] in ('readonly','repair_readonly','complete'):
        assert 'partition_committed:' not in output
    for line in output.splitlines():
        if line.startswith('http_success:'):
            assert 'function=fetch_raw_response;' in line and 'persisted=false' in line
        if line.startswith('partition_committed:'):
            assert 'function=commit_raw_response;' in line or 'function=commit_calendar_partitions;' in line
report={'business_AST_unchanged_ignoring_logs_and_moved_HTTP_log_digest':True,
        'original_comments_and_notebook_state_preserved':True,'entry_unchanged':True,
        'default_export_exact':True,'changed_production_files':changed,'scenario_count':len(reports),
        'scenarios':reports,'real_API_calls':0,'formal_lake_writes':0,
        'assumptions':'临时契约化日历、假 HTTP 200/404/503 响应，固定时间和批次比较前后字节、业务调用与结果。故障由测试主动注入；不假定正式文件损坏；不测试真实网络、适配器重试耗时或正式批次性能。'}
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
