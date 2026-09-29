"""日志改动的业务 AST、原注释、对照场景和导出证明。"""
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

ROOT=pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT=pathlib.Path(sys.argv[1])
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b04_external_index.ipynb')
old=nbformat.read(SNAPSHOT/RELATIVE,4)
new=nbformat.read(ROOT/RELATIVE,4)
code=lambda notebook:'\n\n'.join(c.source for c in notebook.cells if c.cell_type=='code')
source=(ROOT/'00_draft_collection_02/scripts/check_a03_b03_function_logs_20260928.py').read_text(encoding='utf8')
exec(source[source.index('class WithoutLogs'):source.index('\nassert [c.id')].replace("'opened_dataset','validated_calendar_df','validated_fact_df','quality_warning_by_date',\n                    'empty_fact_df','normalized_fact_df','merged_fact_df','updated_calendar_df'", "'validated_calendar_df','validated_index_df','empty_fact_df','normalized_index_df','merged_index_df','updated_calendar_df'"))
assert [c.id for c in old.cells]==[c.id for c in new.cells]
assert old.metadata==new.metadata
for previous,current in zip(old.cells,new.cells,strict=True):
    assert {k:v for k,v in previous.items() if k!='source'}=={k:v for k,v in current.items() if k!='source'}
    if previous.cell_type=='code':
        comments=lambda source:[t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type==tokenize.COMMENT]
        assert comments(previous.source)==comments(current.source),previous.id
        assert canonical(previous.source)==canonical(current.source),previous.id
for cell_id in ('b03-c04-21','a03-b04-terminal'):
    assert next(c for c in old.cells if c.id==cell_id)==next(c for c in new.cells if c.id==cell_id)
nbformat.validate(new)
exported,_=PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==exported.encode('utf8')
assert b'\r\n' not in (ROOT/RELATIVE).read_bytes()
compile(exported,str(RELATIVE),'exec')
for previous,current in zip(old.cells,new.cells):
    if '```mermaid' in previous.source:
        assert previous.source.split('```mermaid')[1].split('```')[0]==current.source.split('```mermaid')[1].split('```')[0]

sys.path.insert(0,str(ROOT/'00_draft_collection_02/tests'))
import test_a03_b04_shared_transaction as fixtures
old_functions={n.name:ast.dump(n) for n in ast.parse(code(old)).body if isinstance(n,ast.FunctionDef)}
comparison=(ROOT/'00_draft_collection_02/scripts/check_a03_b04_transaction_entry_20260928.py').read_text(encoding='utf8')
scenarios=comparison[comparison.index('def load('):comparison.index('\nbefore_hashes=')]
# 保存 CLI 日志，数据与调用顺序对照保持原断言。
scenarios=scenarios.replace('            observations.append(', "            if label=='after': (SNAPSHOT/(scenario+'-after.log')).write_text(result.output,encoding='utf8')\n            observations.append(")
with contextlib.redirect_stdout(io.StringIO()):
    exec(compile(scenarios,'<external index differential>','exec'))
for item in reports:
    output=(SNAPSHOT/(item['scenario']+'-after.log')).read_text(encoding='utf8')
    for line in output.splitlines():
        if line.startswith('partition_committed:'):
            assert any(f'function={name};' in line for name in ('commit_complete_fact_partition','commit_calendar_partitions')),line
            assert 'persisted=true' in line,line
        if line.startswith('api_result:'):
            assert 'function=normalize_external_index_response;' in line and 'normalized=true; persisted=false' in line,line
        if line.startswith('reconciliation_plan:'):
            assert 'function=external_index_reconciliation;' in line,line
        if 'phase=generate_state; status=completed;' in line:
            assert 'persisted=false' in line,line
        if 'phase=calendar_state; status=completed;' in line:
            assert 'function=commit_calendar_partitions;' in line and 'persisted=true; date_watermark=none' in line,line
    if item['scenario']=='api_failure':
        assert 'completed_grids=0; persisted=true; date_watermark=none' in output
        assert 'function=commit_complete_fact_partition; phase=commit_leaf; status=completed;' not in output
    if item['scenario']=='first_write':
        positions=[output.index(fragment) for fragment in (
            'function=normalize_external_index_response; phase=normalize; status=completed;',
            'function=full_fact_partition; phase=generate; status=completed;',
            'function=commit_complete_fact_partition; phase=commit_leaf; status=completed;',
            'function=apply_calendar_completion; phase=generate_state; status=completed;',
            'function=commit_calendar_partitions; phase=commit_leaf; status=completed;',
            'function=commit_calendar_partitions; phase=calendar_state; status=completed;',
            'function=main; phase=partition_batch; status=completed;',
        )]
        assert positions==sorted(positions)
before_hashes=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,digest in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
assert set(changed)=={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix()},changed
report={'business_AST_unchanged_ignoring_logs_and_named_returns':True,
        'comments_cell_state_entry_and_mermaid_graphs_preserved':True,'default_export_exact':True,
        'function_owned_logs_checked':True,'scenario_count':len(reports),'scenarios':reports,
        'real_api_calls':0,'formal_lake_reads_or_writes':0,'changed_production_files':changed}
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
