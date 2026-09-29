"""共享事务接入的范围、Notebook 状态和正常输出差分检查；不访问真实来源或正式湖。"""
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
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.ipynb')
old = nbformat.read(SNAPSHOT/RELATIVE, as_version=4)
new = nbformat.read(ROOT/RELATIVE, as_version=4)
old_cells = {c.id:c for c in old.cells}
new_cells = {c.id:c for c in new.cells}
assert new.metadata == old.metadata
assert [c.id for c in new.cells if c.id in old_cells] == list(old_cells)
for cell_id, cell in old_cells.items():
    assert {k:v for k,v in cell.items() if k!='source'} == {k:v for k,v in new_cells[cell_id].items() if k!='source'}
code = lambda notebook: '\n\n'.join(c.source for c in notebook.cells if c.cell_type == 'code')
old_functions = {n.name: ast.dump(n) for n in ast.parse(code(old)).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
new_functions = {n.name: ast.dump(n) for n in ast.parse(code(new)).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
assert old_functions.keys() == new_functions.keys()
changed_functions = [name for name in old_functions if old_functions[name] != new_functions[name]]
assert changed_functions == ['commit_raw_response', 'commit_calendar_partitions'], changed_functions
changed_code = [i for i,c in old_cells.items() if c.cell_type=='code' and c.source!=new_cells[i].source]
assert changed_code == ['0402fa68', 'e2d51f8b', 'e77863a1', 'e3f69e92'], changed_code
comments = lambda source: [token.string for token in tokenize.generate_tokens(io.StringIO(source).readline) if token.type==tokenize.COMMENT]
old_comments = comments(code(old))
new_comments = comments(code(new))
changed_comment = '# Notebook 默认只读计划；写入必须由操作者显式修改参数。'
assert all(comment in new_comments for comment in old_comments if comment != changed_comment)
for cell_id, start, end in (
    ('e2d51f8b', 'def commit_raw_response(', '    target_had_existing ='),
    ('e77863a1', 'def commit_calendar_partitions(', '        saved_path ='),
):
    old_source = old_cells[cell_id].source
    prelude = old_source[old_source.index(start):old_source.index(end)]
    assert prelude in new_cells[cell_id].source, cell_id
for cell_id, start, end in (
    ('e2d51f8b', '        committed_evidence = inspect_raw_leaf(target_path)', '    except Exception as commit_error:'),
    ('e77863a1', '            committed_dataset = open_exact_dataset(', '        except Exception as commit_error:'),
):
    old_source = old_cells[cell_id].source
    block = [line.strip() for line in old_source[old_source.index(start):old_source.index(end)].splitlines() if line.strip()]
    new_lines = [line.strip() for line in new_cells[cell_id].source.splitlines() if line.strip()]
    block_start = new_lines.index(block[0])
    assert new_lines[block_start:block_start+len(block)] == block
nbformat.validate(new)
exported, _ = PythonExporter().from_notebook_node(new)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes() == exported.encode('utf8')
compile(exported, str(RELATIVE), 'exec')

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

before = load('raw_before_tx', SNAPSHOT/RELATIVE.with_suffix('.py'))
after = load('raw_after_tx', ROOT/RELATIVE.with_suffix('.py'))
sys.path.insert(0, str(ROOT/'00_draft_collection_02/tests'))
import test_b03_c02_raw_staging_lifecycle as fixtures

NOW = datetime.now(timezone.utc) - timedelta(seconds=1)
class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW

def hashes(root):
    return {p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}

trace_names = ('open_exact_dataset', 'validate_calendar_frame', 'plan_raw_grids', 'fetch_raw_response',
               'commit_raw_response', 'preserve_failed_response', 'apply_calendar_completion',
               'apply_calendar_failure', 'commit_calendar_partitions')

def run(module, lake, arguments, response):
    session = fixtures.FakeSession(response)
    trace = []
    def traced(name, fn):
        def call(*args, **kwargs):
            trace.append(name)
            return fn(*args, **kwargs)
        return call
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(module, 'datetime', FrozenDatetime))
        stack.enter_context(patch.object(module.uuid, 'uuid4', return_value=SimpleNamespace(hex='fixed')))
        made_session = stack.enter_context(patch.object(module, 'create_http_session', return_value=session))
        for name in trace_names:
            stack.enter_context(patch.object(module, name, side_effect=traced(name, getattr(module, name))))
        result = CliRunner().invoke(module.main, ['--lake-root', str(lake), *arguments], standalone_mode=False)
    return result, trace, hashes(lake), session.get_count, made_session.call_count, session.closed

reports = []
with tempfile.TemporaryDirectory(prefix='raw-diff-') as directory:
    temp = pathlib.Path(directory)
    day = date(2026, 7, 1)
    second = date(2026, 8, 1)
    scenarios = ('readonly', 'initial_write', 'empty_200', 'complete', 'repair_readonly', 'repair_write',
                 'mixed_repair_fetch', 'two_months', 'http_404', 'http_503', 'explicit_test_lake')
    for scenario in scenarios:
        seed = temp/(scenario+'-seed')
        rows = fixtures.pending_calendar_frame(day)
        if scenario in ('mixed_repair_fetch', 'two_months'):
            rows = pd.concat([rows, fixtures.pending_calendar_frame(second)], ignore_index=True)
        rows['updated_at'] = NOW - timedelta(seconds=1)
        if scenario in ('complete', 'repair_readonly', 'repair_write', 'mixed_repair_fetch'):
            evidence = after.commit_raw_response(seed/after.RAW_RELATIVE_ROOT, day, b'already-archived')
            if scenario == 'complete':
                rows = after.apply_calendar_completion(rows, {day:evidence}, 'prior', NOW)
        fixtures.write_exact_calendar(seed, rows)
        arguments = [] if scenario in ('readonly', 'repair_readonly') else ['--write']
        if scenario == 'explicit_test_lake':
            arguments += ['--start-date', '2026-07-01', '--end-date', '2026-07-01']
        status = 404 if scenario=='http_404' else 503 if scenario=='http_503' else 200
        response = fixtures.FakeResponse(status, b'' if scenario=='empty_200' else b'\x00\xffraw-source')
        results = []
        for module, label in ((before, 'before'), (after, 'after')):
            lake = temp/(scenario+'-'+label)
            shutil.copytree(seed, lake)
            result = run(module, lake, arguments, response)
            (SNAPSHOT/(scenario+'-'+label+'.log')).write_text(result[0].output, encoding='utf8')
            results.append(result)
        old_result, old_trace, old_hashes, old_count, old_sessions, old_closed = results[0]
        result, trace, actual_hashes, count, sessions, closed = results[1]
        assert result.exit_code == old_result.exit_code, scenario
        assert type(result.exception) is type(old_result.exception), scenario
        assert str(result.exception) == str(old_result.exception), scenario
        assert trace == old_trace, scenario
        assert actual_hashes == old_hashes, scenario
        assert (count, sessions, closed) == (old_count, old_sessions, old_closed), scenario
        if scenario in ('complete', 'repair_readonly', 'repair_write'):
            assert count == sessions == 0, scenario
        if scenario in ('readonly', 'repair_readonly', 'complete'):
            assert actual_hashes == hashes(seed), scenario
        assert result.exit_code != 0 if status != 200 else result.exit_code == 0, (scenario, result.exception, result.output)
        reports.append({'scenario':scenario, 'same_file_bytes':True, 'same_call_order':True,
                        'same_outcome':True, 'http_calls':count, 'created_sessions':sessions})

    for scenario, args in (
        ('unpaired_dates', ['--start-date', '2026-07-01']),
        ('reversed_dates', ['--start-date', '2026-07-02', '--end-date', '2026-07-01']),
        ('formal_date_write', ['--start-date', '2026-07-01', '--end-date', '2026-07-01', '--write']),
    ):
        results = []
        for module in (before, after):
            with patch.object(module, 'open_exact_dataset') as opened, patch.object(module, 'create_http_session') as session:
                results.append(CliRunner().invoke(module.main, args, standalone_mode=False))
                opened.assert_not_called()
                session.assert_not_called()
        assert results[0].exit_code == results[1].exit_code != 0
        assert type(results[0].exception) is type(results[1].exception)
        assert str(results[0].exception) == str(results[1].exception)
        reports.append({'scenario':scenario, 'same_outcome':True, 'no_lake_or_network_io':True})

before_hashes = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed = [p for p, digest in before_hashes.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest()!=digest]
assert set(changed) == {RELATIVE.as_posix(), RELATIVE.with_suffix('.py').as_posix(), 'AGENTS.md',
                       '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md',
                       '00_draft_collection_02/tests/test_b03_c02_raw_staging_lifecycle.py'}, changed
report = {'only_two_commit_functions_changed':True, 'unchanged_business_and_retry_functions':True,
          'staging_and_formal_validation_unchanged':True, 'notebook_cell_state_preserved':True,
          'default_export_exact':True, 'original_code_comments_preserved_except_misleading_entry_comment':True,
          'scenario_count':len(reports), 'scenarios':reports, 'changed_existing_files':changed,
          'real_api_calls':0, 'formal_lake_writes':0,
          'assumptions':'使用契约化临时日历和假 HTTP 响应，固定时间及批次比较完整落盘字节。异常恢复另由显式注入的安装/验收故障测试；不假设正式文件损坏，不测网络重试耗时、进程强杀或并发写入。'}
(SNAPSHOT/'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
