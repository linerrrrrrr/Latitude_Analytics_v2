"""本轮差分、完成边界与 Notebook 保留性验证；模拟 API，仅写临时湖。"""
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
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

import nbformat
import pandas as pd
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b02_futures_holding_reports.ipynb')
sys.path.insert(0, str(ROOT/'00_draft_collection_02/tests'))
import test_b02_futures_holding_reports as fixtures

after = fixtures.holding_reports
spec = importlib.util.spec_from_file_location('a02_b02_before_transaction', SNAPSHOT/RELATIVE.with_suffix('.py'))
before = importlib.util.module_from_spec(spec)
spec.loader.exec_module(before)
checks = []

old = json.loads((SNAPSHOT/RELATIVE).read_text(encoding='utf8'))
new = json.loads((ROOT/RELATIVE).read_text(encoding='utf8'))
assert {k:v for k,v in old.items() if k != 'cells'} == {k:v for k,v in new.items() if k != 'cells'}
assert [c['id'] for c in old['cells']] == [c['id'] for c in new['cells'][:len(old['cells'])]]
changed_cells = []
for old_cell, new_cell in zip(old['cells'], new['cells'], strict=False):
    assert {k:v for k,v in old_cell.items() if k != 'source'} == {k:v for k,v in new_cell.items() if k != 'source'}
    if old_cell['cell_type'] == 'code':
        def comments(cell):
            return [t.string for t in tokenize.generate_tokens(io.StringIO(''.join(cell['source'])).readline) if t.type == tokenize.COMMENT]
        assert comments(old_cell) == comments(new_cell), old_cell['id']
        if old_cell['source'] != new_cell['source']:
            changed_cells.append(old_cell['id'])
assert changed_cells == ['12bacfb9', 'aa14209b', 'abaf342b'], changed_cells
old_tree = ast.parse((SNAPSHOT/RELATIVE.with_suffix('.py')).read_text(encoding='utf8'))
new_tree = ast.parse((ROOT/RELATIVE.with_suffix('.py')).read_text(encoding='utf8'))
old_functions = {n.name:ast.dump(n) for n in old_tree.body if isinstance(n, ast.FunctionDef)}
new_functions = {n.name:ast.dump(n) for n in new_tree.body if isinstance(n, ast.FunctionDef)}
assert old_functions.keys() == new_functions.keys()
assert [name for name in old_functions if old_functions[name] != new_functions[name]] == ['commit_complete_partition']
notebook = nbformat.read(ROOT/RELATIVE, as_version=4)
nbformat.validate(notebook)
exported, _ = PythonExporter().from_notebook_node(notebook)
assert (ROOT/RELATIVE.with_suffix('.py')).read_bytes() == exported.encode('utf8')
compile(exported, str(RELATIVE.with_suffix('.py')), 'exec')
checks.append('仅初始化导入、单叶提交、执行入口三个旧代码格改变；原注释、ID、元数据、输出与执行计数全部保留；其余函数 AST 不变')
checks.append('Notebook 结构、脚本语法和默认 PythonExporter 逐字节一致')


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 9, 1, tzinfo=timezone.utc)


dates = [fixtures.GRID['trading_date'], fixtures.GRID['trading_date']+timedelta(days=1)]
response = pd.DataFrame([fixtures.source_row(), fixtures.source_row(member_name='期货公司', rank=1)])
fixture = fixtures.LeafIsolationTests()
trace_names = ('open_exact_dataset', 'open_optional_exact_dataset', 'dataset_grid_count_map',
               'read_complete_partition', 'validate_calendar_frame', 'validate_position_frame',
               'validate_member_frame', 'commit_complete_partition', 'apply_calendar_completion',
               'commit_calendar_partitions')
traces, hashes = [], []
with tempfile.TemporaryDirectory(prefix='a02-b02-transaction-diff-') as directory:
    temporary = pathlib.Path(directory)
    seed = temporary/'seed'
    with contextlib.redirect_stdout(io.StringIO()):
        for dataset_name in after.DATASET_NAMES:
            fixture.commit_calendar_leaf(seed, dataset_name, [fixtures.calendar_row(dataset_name, 'BB', day, completed=False) for day in dates], 2014, 4)
    for module, label in ((before, 'before'), (after, 'after')):
        lake = temporary/label
        shutil.copytree(seed, lake)
        calls = []
        def traced(name, function):
            def invoke(*args, **kwargs):
                calls.append(name)
                return function(*args, **kwargs)
            return invoke
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(module, 'datetime', FrozenDatetime))
            stack.enter_context(mock.patch.object(module.uuid, 'uuid4', return_value=SimpleNamespace(hex='fixed_run')))
            jqdata = fixtures.query_jqdata(response)
            stack.enter_context(mock.patch.object(module, 'authenticate_jqdata', return_value=jqdata))
            for name in trace_names:
                stack.enter_context(mock.patch.object(module, name, side_effect=traced(name, getattr(module, name))))
            result = CliRunner().invoke(module.main, ['--lake-root', str(lake), '--write'])
        (SNAPSHOT/f'{label}_main_write.log').write_text(result.output, encoding='utf8')
        assert result.exit_code == 0, result.output
        assert jqdata.finance.run_query.call_count == 1
        assert result.output.count('partition_committed:') == 4
        traces.append(calls)
        hashes.append(fixtures.parquet_hashes(lake))
    assert traces[0] == traces[1]
    assert hashes[0] == hashes[1]
    checks.append('两天待办：一天两类非空、一天明确为空；前后均请求一次、提交四叶，业务调用顺序和全部 Parquet 字节一致')
    completed_lake = temporary/'after'
    with mock.patch.object(after, 'authenticate_jqdata', side_effect=AssertionError('no authentication expected')):
        result = CliRunner().invoke(after.main, ['--lake-root', str(completed_lake), '--write'])
    assert result.exit_code == 0 and 'outcome=up_to_date' in result.output, result.output
    assert fixtures.parquet_hashes(completed_lake) == hashes[1]
    checks.append('成功后再次启动无待办：不认证、不请求、不改文件')

    lake = temporary/'readonly'
    shutil.copytree(seed, lake)
    initial = fixtures.parquet_hashes(lake)
    with mock.patch.object(after, 'authenticate_jqdata', return_value=fixtures.query_jqdata(response)):
        result = CliRunner().invoke(after.main, ['--lake-root', str(lake)])
    assert result.exit_code == 0 and fixtures.parquet_hashes(lake) == initial, result.output
    assert 'partition_committed:' not in result.output
    checks.append('默认只读仍采集和验收，有待办时不提交事实或日历')

    for failure_kind in ('second_fact', 'second_calendar'):
        lake = temporary/failure_kind
        shutil.copytree(seed, lake)
        original = after.read_complete_partition
        def read(*args, **kwargs):
            frame = original(*args, **kwargs)
            fail = (args[-1] == '正式 '+after.MEMBER_TABLE_NAME) if failure_kind == 'second_fact' else (
                args[-1] == '正式 '+after.CALENDAR_TABLE_NAME and args[3][0] == 'position_rank'
            )
            if fail:
                raise ValueError('injected later formal rejection')
            return frame
        with mock.patch.object(after, 'datetime', FrozenDatetime), mock.patch.object(after.uuid, 'uuid4', return_value=SimpleNamespace(hex='fixed_run')), mock.patch.object(after, 'authenticate_jqdata', return_value=fixtures.query_jqdata(response)), mock.patch.object(after, 'read_complete_partition', side_effect=read):
            result = CliRunner().invoke(after.main, ['--lake-root', str(lake), '--write'])
        (SNAPSHOT/f'{failure_kind}.log').write_text(result.output, encoding='utf8')
        assert result.exit_code != 0
        assert isinstance(result.exception, RuntimeError) and isinstance(result.exception.__cause__, ValueError)
        assert result.output.count('partition_committed:') == (1 if failure_kind == 'second_fact' else 3)
        assert 'phase=calendar_state; status=completed;' not in result.output
        assert 'partition_timing:' not in result.output and 'phase=run; status=completed;' not in result.output
        with contextlib.redirect_stdout(io.StringIO()):
            calendar = fixture.read_calendar_month(lake, 2014, 4)
            position_dataset = after.open_optional_exact_dataset(lake/'silver'/after.POSITION_TABLE_NAME, after.POSITION_PARTITIONING, after.FUTURES_POSITION_RANK_DAILY_SCHEMA, 'position')
            member_dataset = after.open_optional_exact_dataset(lake/'silver'/after.MEMBER_TABLE_NAME, after.MEMBER_PARTITIONING, after.FUTURES_MEMBER_POSITION_DAILY_SCHEMA, 'member')
            pending, complete = after.pending_report_grids(calendar, after.dataset_grid_count_map(position_dataset), after.dataset_grid_count_map(member_dataset), None, None)
        assert len(pending) == 2 and complete == 0
        position_hashes = fixtures.parquet_hashes(lake/'silver'/after.POSITION_TABLE_NAME)
        expected = {name.removeprefix('silver/'+after.POSITION_TABLE_NAME+'/'):digest for name,digest in hashes[1].items() if name.startswith('silver/'+after.POSITION_TABLE_NAME+'/')}
        assert position_hashes == expected
        assert not list((lake/'silver').glob('*.staging-*'))
        assert list((lake/'silver').glob('.*.failed-*'))
        checks.append(f'{failure_kind} 正式复读拒绝：此前成功叶保留，当前叶恢复且保留失败证据，两天继续待办，无整组成功日志')

hashes_before = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed = [p for p, digest in hashes_before.items() if hashlib.sha256((ROOT/p).read_bytes()).hexdigest() != digest]
assert set(changed) == {RELATIVE.as_posix(), RELATIVE.with_suffix('.py').as_posix(), 'AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'}, changed
report = {
    'checks': checks, 'check_count': len(checks), 'changed_code_cells': changed_cells,
    'unchanged_function_count': len(old_functions)-1,
    'changed_production_and_specification_files': changed,
    'shared_module_and_data_contracts_unchanged': True,
    'successful_main_call_trace': traces[1],
    'real_api_calls': 0, 'formal_lake_writes': 0,
    'assumptions': '模拟完整来源响应、固定业务时间与批次以便字节比较；临时湖内主动注入文件移动或验收异常，验证失败处理，不假设正式文件已经损坏。单个写入者、同一文件系统；不验证进程被杀后的恢复或并发写入。',
}
(SNAPSHOT/'final_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
