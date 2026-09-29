"""b03 函数日志的结构、日志所有权及既有情景差分；不调用真实来源。"""
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

ROOT=pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT=pathlib.Path(sys.argv[1])
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb')
old=nbformat.read(SNAPSHOT/RELATIVE,as_version=4)
new=nbformat.read(ROOT/RELATIVE,as_version=4)
code=lambda notebook:'\n\n'.join(c.source for c in notebook.cells if c.cell_type=='code')

class WithoutLogs(ast.NodeTransformer):
    def visit_Expr(self,node):
        if isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='click.echo': return None
        return self.generic_visit(node)
    def visit_Assign(self,node):
        if all(isinstance(t,ast.Name) and t.id.startswith('log_') for t in node.targets): return None
        return self.generic_visit(node)
    def visit_AugAssign(self,node):
        if isinstance(node.target,ast.Name) and node.target.id.startswith('log_'): return None
        return self.generic_visit(node)
    def visit_If(self,node):
        node=self.generic_visit(node)
        return node if node.body else None
    def visit_For(self,node):
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
        if not hasattr(node,'body') or not isinstance(node.body,list):continue
        for index in range(len(node.body)-2,-1,-1):
            assignment,following=node.body[index:index+2]
            if (isinstance(assignment,ast.Assign) and len(assignment.targets)==1 and isinstance(assignment.targets[0],ast.Name)
                and assignment.targets[0].id in ('opened_dataset','validated_calendar_df','validated_fact_df','quality_warning_by_date',
                    'empty_fact_df','normalized_fact_df','merged_fact_df','updated_calendar_df')
                and isinstance(following,ast.Return) and isinstance(following.value,ast.Name)
                and following.value.id==assignment.targets[0].id):
                following.value=assignment.value
                del node.body[index]
    return ast.dump(tree)

assert [c.id for c in old.cells]==[c.id for c in new.cells]
assert old.metadata==new.metadata
for old_cell,new_cell in zip(old.cells,new.cells,strict=True):
    assert {k:v for k,v in old_cell.items() if k!='source'}=={k:v for k,v in new_cell.items() if k!='source'}
    if old_cell.cell_type=='code':
        comments=lambda source:[t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type==tokenize.COMMENT]
        assert comments(old_cell.source)==comments(new_cell.source),old_cell.id
        assert canonical(old_cell.source)==canonical(new_cell.source),old_cell.id
assert next(c for c in old.cells if c.id=='b03-c03-21')==next(c for c in new.cells if c.id=='b03-c03-21')
nbformat.validate(new)
exported,_=PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==exported.encode('utf8')
assert b'\r\n' not in (ROOT/RELATIVE).read_bytes()
compile(exported,str(RELATIVE),'exec')

# 同一组 17 情景；只有日志所有权断言改变，函数调用顺序和文件字节仍逐项相等。
comparison=(ROOT/'00_draft_collection_02/scripts/check_a03_b03_docs_logs_20260928.py').read_text(encoding='utf8')
scenario_source=comparison[comparison.index('def load('):comparison.index('\nbefore_hashes=')]
scenario_source=scenario_source.replace("assert 'phase=run; status=completed;' not in result.output and 'partition_committed:' not in result.output", "assert 'phase=run; status=completed;' not in result.output")
# 原来 schema_fixture 生成没有日志；本轮仅隐藏测试准备日志，CLI 捕获仍完整保留。
with contextlib.redirect_stdout(io.StringIO()):
    exec(compile(scenario_source,'<overseas differential scenarios>','exec'))

for item in reports:
    if 'same_file_bytes' not in item:continue
    output=(SNAPSHOT/(item['scenario']+'-after.log')).read_text(encoding='utf8')
    for line in output.splitlines():
        if line.startswith('partition_committed:'):
            assert any(f'function={name};' in line for name in ('upgrade_fact_metadata','commit_complete_fact_partition','commit_calendar_partitions')),line
            assert 'persisted=true' in line
        if line.startswith('api_success:'):
            assert 'function=query_overseas_futures_grid;' in line and 'normalized=false; persisted=false' in line,line
        if line.startswith('api_result:'):
            assert 'function=normalize_overseas_futures_response;' in line and 'persisted=false' in line,line
        if 'phase=generate_state; status=completed;' in line:
            assert 'persisted=false' in line,line
        if 'phase=calendar_state; status=completed;' in line:
            assert 'function=commit_calendar_partitions;' in line and 'persisted=true; date_watermark=none' in line,line
    if item['scenario'] in ('query_failure','invalid_response'):
        assert 'completed_grids=0; persisted=true; date_watermark=none' in output
        assert 'function=commit_complete_fact_partition; phase=commit; status=completed;' not in output
    if item['scenario']=='fact_formal_failure':
        assert 'function=commit_complete_fact_partition; phase=commit; status=completed;' not in output
        assert 'phase=calendar_state; status=completed;' not in output
    if item['scenario']=='calendar_formal_failure':
        assert 'function=commit_complete_fact_partition; phase=commit; status=completed;' in output
        assert 'phase=calendar_state; status=completed;' not in output
    if item['scenario']=='initial_write':
        order=[output.index(text) for text in (
            'function=normalize_overseas_futures_response; phase=normalize; status=completed;',
            'function=full_fact_partition; phase=generate; status=completed;',
            'function=commit_complete_fact_partition; phase=commit; status=completed;',
            'function=apply_calendar_completion; phase=generate_state; status=completed;',
            'function=commit_calendar_partitions; phase=commit_leaf; status=completed;',
            'function=commit_calendar_partitions; phase=calendar_state; status=completed;',
            'function=main; dataset=overseas_futures; phase=run; status=completed;',
        )]
        assert order==sorted(order)
before_hashes=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,digest in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
assert set(changed)=={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix()},changed
report={'business_AST_unchanged_ignoring_logs_and_named_returns':True,'comments_and_cell_state_preserved':True,
        'entry_unchanged':True,'default_export_exact':True,'function_owned_logs_checked':True,'scenario_count':len(reports),
        'scenarios':reports,'changed_production_files':changed,'real_api_calls':0,'formal_lake_writes':0,
        'assumptions':'契约化临时湖、模拟 JQData，固定时间与批次对比全部业务函数调用和落盘字节。主动注入请求、数值、正式复读失败；不假定正式文件损坏，不测真实网络、并发或正式批次耗时。'}
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
