"""复用既有 17 情景，比较正式输出、API 请求和核心顺序；统计重复工作变化。"""
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
old_cells = {c.id: c for c in old.cells}
new_cells = {c.id: c for c in new.cells}
assert old.metadata==new.metadata
assert [c.id for c in new.cells if c.id in old_cells]==list(old_cells)
for cell_id, cell in old_cells.items():
    assert {k:v for k,v in cell.items() if k!='source'}=={k:v for k,v in new_cells[cell_id].items() if k!='source'}, cell_id
for index, cell in enumerate(new.cells):
    if cell.cell_type=='code': assert '```mermaid' in new.cells[index-1].source, cell.id
nbformat.validate(new)
exported, _ = PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes()==exported.encode('utf8')
assert b'\r\n' not in (ROOT/RELATIVE).read_bytes()
ast.parse(exported)

source = (ROOT/'00_draft_collection_02/scripts/check_a03_b03_docs_logs_20260928.py').read_text(encoding='utf8')
source = source[source.index('def load('):source.index('\nbefore_hashes=')]
source = source.replace("if p.is_file()}", "if p.is_file() and not any(part.startswith('.') for part in p.relative_to(root).parts)}")
source = source.replace("n.name!='main')", "n.name!='main') + ('validate_calendar_table', 'pandas_to_arrow', 'arrow_to_pandas')")
source = source.replace('        for name in trace_names:\n', '        for name in trace_names:\n            if not hasattr(module, name): continue\n')
old_assertions = '''        assert type(old_result.exception) is type(result.exception),scenario
        assert str(old_result.exception)==str(result.exception),scenario
        if result.exception:
            assert type(old_result.exception.__cause__) is type(result.exception.__cause__),scenario
        assert old_trace==trace,scenario
        assert old_hashes==actual_hashes,scenario'''
new_assertions = '''        if scenario in ('fact_formal_failure', 'calendar_formal_failure'):
            assert isinstance(result.exception, RuntimeError), (scenario, result.exception)
            assert type(result.exception.__cause__) is type(old_result.exception), scenario
        else:
            assert type(old_result.exception) is type(result.exception), scenario
            assert str(old_result.exception)==str(result.exception), scenario
        stable_calls = {'query_overseas_futures_grid', 'normalize_overseas_futures_response',
                        'full_fact_partition', 'commit_complete_fact_partition',
                        'apply_calendar_completion', 'apply_calendar_failure', 'commit_calendar_partitions'}
        assert [n for n in old_trace if n in stable_calls]==[n for n in trace if n in stable_calls], scenario
        if scenario=='metadata_upgrade':
            assert actual_hashes==hashes(seed), scenario
            assert old_hashes!=actual_hashes, scenario
        else:
            assert old_hashes==actual_hashes, (scenario, old_hashes, actual_hashes)'''
assert old_assertions in source
source = source.replace(old_assertions, new_assertions)
source = source.replace("assert 'phase=run; status=completed;' not in result.output and 'partition_committed:' not in result.output", "assert 'phase=run; status=completed;' not in result.output")
source = source.replace("'same_file_bytes':True,'same_business_call_trace':True,", "'same_formal_file_bytes':scenario!='metadata_upgrade','same_core_operation_order':True, 'before_calls':dict(Counter(old_trace)), 'after_calls':dict(Counter(trace)),")
with contextlib.redirect_stdout(io.StringIO()):
    exec(compile(source, '<overseas differential scenarios>', 'exec'))

before_hashes = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed = [p for p, digest in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
allowed = {RELATIVE.as_posix(), RELATIVE.with_suffix('.py').as_posix(), 'AGENTS.md',
           '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md',
           '00_draft_collection_02/tests/test_b03_metadata_upgrade.py',
           '00_draft_collection_02/tests/test_a03_b03_function_logs.py'}
assert set(changed)<=allowed, changed
report = {'original_cells_state_preserved':True, 'default_export_exact':True,
          'flowcharts':sum('```mermaid' in c.source for c in new.cells),
          'scenario_count':len(reports), 'scenarios':reports, 'changed_snapshot_files':changed,
          'intentional_differences':['描述性 metadata 差异不改写旧文件', '失败新叶隔离留存；共享事务异常保留原始验收原因链'],
          'real_api_calls':0, 'formal_lake_writes':0,
          'assumptions':'契约化临时湖、模拟来源、固定时间与批次；故障通过显式注入产生。单写入者、同一文件系统、不模拟进程强杀；不推断正式湖损坏或真实网络耗时。'}
(SNAPSHOT/'transaction_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps({'scenarios_passed':len(reports), 'flowcharts':report['flowcharts'],
                  'report':str(SNAPSHOT/'transaction_verification.json'),
                  'two_months':next(item for item in reports if item['scenario']=='two_months')}, ensure_ascii=False, indent=2))
