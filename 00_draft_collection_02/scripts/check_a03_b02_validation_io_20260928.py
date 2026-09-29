"""a03/b02 校验/读取收缩的差分结果与范围检查。"""
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
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.ipynb')
old=nbformat.read(SNAPSHOT/RELATIVE,as_version=4)
new=nbformat.read(ROOT/RELATIVE,as_version=4)
assert old.metadata==new.metadata
assert [c.id for c in old.cells]==[c.id for c in new.cells]
for old_cell,new_cell in zip(old.cells,new.cells,strict=True):
    assert {k:v for k,v in old_cell.items() if k!='source'}=={k:v for k,v in new_cell.items() if k!='source'}
    if old_cell.id in ('e3f69e92','a03-b02-manual'):
        assert old_cell.source==new_cell.source
    if old_cell.cell_type=='code':
        comments=lambda s:[t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type==tokenize.COMMENT]
        assert [c for c in comments(old_cell.source) if not c.startswith('# Hive 分区列')] == [c for c in comments(new_cell.source) if not c.startswith('# Hive 分区列')]
nbformat.validate(new)
source,_=PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==source.encode('utf8')
assert b'\r\n' not in (ROOT/RELATIVE).read_bytes()
compile(source,str(RELATIVE),'exec')
comparison=(ROOT/'00_draft_collection_02/scripts/check_a03_b02_transaction_entry_20260928.py').read_text(encoding='utf8')
scenario_source=comparison[comparison.index('def load('):comparison.index('\nbefore_hashes =')]
trace_start=scenario_source.index('trace_names =')
trace_end=scenario_source.index('\ndef run(',trace_start)
scenario_source=scenario_source[:trace_start]+'''trace_names = ('fetch_raw_response', 'commit_raw_response', 'preserve_failed_response',
               'apply_calendar_completion', 'apply_calendar_failure', 'commit_calendar_partitions')
'''+scenario_source[trace_end:]
with contextlib.redirect_stdout(io.StringIO()):
    exec(compile(scenario_source,'<raw output comparison>','exec'))
before_hashes=json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed=[p for p,d in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=d]
assert set(changed)=={RELATIVE.as_posix(),RELATIVE.with_suffix('.py').as_posix(),
    '02_Futures_Lakehouse/AGENTS.md','02_Futures_Lakehouse/README.md',
    '00_draft_collection_02/tests/test_b03_c02_raw_staging_lifecycle.py'},changed
report={'original_cell_state_and_execution_entry_preserved':True,'default_export_exact':True,
        'raw_reconciliation_HTTP_state_generation_and_commit_order_unchanged':True,
        'scenario_count':len(reports),'scenarios':reports,'changed_existing_files':changed,
        'real_api_calls':0,'formal_lake_writes':0,
        'assumptions':'上游业务由正式提交保证；临时数据符合权威契约。固定时间/批次，用假 HTTP 比较落盘字节及采集、状态更新、提交顺序；对比中不要求被删除的重复校验/读取次数相等。物理门禁、dirty 业务校验和复读失败另行注入测试；不假设正式文件损坏或并发写入。'}
(SNAPSHOT/'verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf8')
print(json.dumps(report,ensure_ascii=False,indent=2))
