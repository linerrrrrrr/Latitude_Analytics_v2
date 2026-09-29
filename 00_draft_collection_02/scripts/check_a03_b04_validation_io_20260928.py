"""复用 10 个真实调用情景，对照落盘字节、请求与业务顺序并统计工作量。"""
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
from collections import Counter
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
code=lambda n:'\n\n'.join(c.source for c in n.cells if c.cell_type=='code')
assert old.metadata==new.metadata
assert [c.id for c in old.cells]==[c.id for c in new.cells]
for a,b in zip(old.cells,new.cells):
    assert {k:v for k,v in a.items() if k!='source'}=={k:v for k,v in b.items() if k!='source'}
for cell_id in ('b03-c04-21','a03-b04-terminal'):
    assert next(c for c in old.cells if c.id==cell_id)==next(c for c in new.cells if c.id==cell_id)
nbformat.validate(new)
exported,_=PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==exported.encode('utf8')
assert b'\r\n' not in (ROOT/RELATIVE).read_bytes()
ast.parse(exported)
sys.path.insert(0,str(ROOT/'00_draft_collection_02/tests'))
import test_a03_b04_shared_transaction as fixtures
old_functions={n.name:None for source in (code(old),code(new)) for n in ast.parse(source).body if isinstance(n,ast.FunctionDef)}
old_functions.update({name:None for name in ('pandas_to_arrow','arrow_to_pandas')})
comparison=(ROOT/'00_draft_collection_02/scripts/check_a03_b04_transaction_entry_20260928.py').read_text(encoding='utf8')
scenarios=comparison[comparison.index('def load('):comparison.index('\nbefore_hashes=')]
scenarios=scenarios.replace('                    original=getattr(owner,name)','                    if not hasattr(owner,name): continue\n                    original=getattr(owner,name)')
scenarios=scenarios.replace('        assert observations[0]==observations[1],scenario','''        previous,current=observations
        assert previous[:2]==current[:2],scenario
        assert previous[3:]==current[3:],scenario
        stable={'normalize_external_index_response','full_fact_partition','commit_complete_fact_partition',
                'apply_calendar_completion','apply_calendar_failure','commit_calendar_partitions','pending_request_ranges'}
        assert [n for n in previous[2] if n in stable]==[n for n in current[2] if n in stable],scenario
        assert Counter(current[2])['external_index_reconciliation']==1,scenario
        assert Counter(current[2])['read_optional_fact']==1,scenario''')
scenarios=scenarios.replace("reports.append({'scenario':scenario,'same_formal_bytes_requests_call_order':True})", "reports.append({'scenario':scenario,'same_formal_bytes_requests_core_order':True,'before_calls':dict(Counter(previous[2])),'after_calls':dict(Counter(current[2]))})")
with contextlib.redirect_stdout(io.StringIO()):
    exec(compile(scenarios,'<external index reduced validation>','exec'))
before_hashes=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,digest in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
allowed={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix(),'02_Futures_Lakehouse/AGENTS.md','02_Futures_Lakehouse/README.md','00_draft_collection_02/tests/test_a03_b04_shared_transaction.py'}
assert set(changed)<=allowed,changed
report={'cell_state_entry_and_default_export_verified':True,'scenario_count':len(reports),'scenarios':reports,
        'changed_snapshot_files':changed,'real_api_calls':0,'formal_lake_reads_or_writes':0,
        'assumptions':'契约化临时数据、固定时间及批次、模拟响应、单写入者；不假定正式文件损坏，不测真实网络或并发。'}
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps({'scenario_count':len(reports),'two_categories_two_months':next(r for r in reports if r['scenario']=='two_categories_two_months'),'report':str(SNAPSHOT/'verification.json')},ensure_ascii=False,indent=2))
