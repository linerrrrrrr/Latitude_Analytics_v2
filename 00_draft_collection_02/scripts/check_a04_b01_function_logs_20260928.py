"""a04/b01 函数进度的差分核对；临时目录、固定时间、不调用 API。"""
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
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import nbformat
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')
before_nb = nbformat.read(SNAPSHOT / RELATIVE, 4)
after_nb = nbformat.read(ROOT / RELATIVE, 4)
code = lambda notebook: '\n\n'.join(cell.source for cell in notebook.cells if cell.cell_type == 'code')
update = ast.parse((ROOT / '00_draft_collection_02/scripts/update_a04_b01_function_logs_20260928.py').read_text(encoding='utf8'))
definitions = [node for node in update.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in ('RemoveLogging', 'canonical')]
exec(compile(ast.Module(body=definitions, type_ignores=[]), '<log-only AST comparison>', 'exec'))
assert canonical(code(before_nb)) == canonical(code(after_nb))
comments = lambda source: [token.string for token in tokenize.generate_tokens(io.StringIO(source).readline) if token.type == tokenize.COMMENT]
assert comments(code(before_nb)) == comments(code(after_nb))
assert before_nb.metadata == after_nb.metadata
for old, new in zip(before_nb.cells, after_nb.cells, strict=True):
    assert {key: value for key, value in old.items() if key != 'source'} == {key: value for key, value in new.items() if key != 'source'}
    if '```mermaid' in old.source or old.id == '4a5b544e':
        assert old.source == new.source
nbformat.validate(after_nb)
source, _ = PythonExporter().from_notebook_node(after_nb)
assert (ROOT / RELATIVE.with_suffix('.py')).read_bytes() == source.encode('utf8')
ast.parse(source)

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

before = load('macro_calendar_before_function_logs', SNAPSHOT / RELATIVE.with_suffix('.py'))
after = load('macro_calendar_after_function_logs', ROOT / RELATIVE.with_suffix('.py'))
fixtures = load('macro_calendar_test_fixtures', ROOT / '00_draft_collection_02/tests/test_b04_c01_macro_release_calendar.py')
NOW = datetime(2026, 8, 17, 12, tzinfo=timezone.utc)

class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW.astimezone(tz) if tz is not None else NOW.replace(tzinfo=None)

with contextlib.redirect_stdout(io.StringIO()):
    base_frame = after.build_expected_calendar(date(2026, 7, 17), date(2026, 8, 17), after.empty_pandas(after.MACRO_RELEASE_CALENDAR_SCHEMA), date(2026, 8, 17), NOW)

def prepare(lake_root, scenario):
    if scenario in ('readonly_empty', 'first_write', 'formal_rejection'):
        return
    frame = base_frame.copy()
    if scenario in ('internal_gap', 'leaf_install_failure', 'rollback_failure'):
        frame = frame.drop(frame.index[0]).reset_index(drop=True)
    schema = fixtures.stale_metadata_schema(after.MACRO_RELEASE_CALENDAR_SCHEMA) if scenario in ('legacy_metadata', 'first_backup_failure') else None
    fixtures.write_partitioned_calendar(after, frame, lake_root / 'silver' / after.TABLE_NAME, write_schema=schema)

TRACE_NAMES = ('open_compatible_dataset', 'open_exact_dataset', 'validate_macro_calendar_table', 'build_expected_calendar', 'table_digest', 'changed_partition_keys', 'commit_partitions')

def invoke(module, lake_root, scenario):
    calls = []
    real_move = module.shutil.move
    move_failed = False

    def move(source, destination):
        nonlocal move_failed
        if scenario == 'first_backup_failure' and '.backup-' in str(destination) and not move_failed:
            move_failed = True
            raise OSError('injected first backup failure')
        if scenario in ('leaf_install_failure', 'rollback_failure') and '.staging-' in str(source) and not move_failed:
            move_failed = True
            raise OSError('injected leaf installation failure')
        if scenario == 'rollback_failure' and '.backup-' in str(source) and move_failed:
            raise OSError('injected restore failure')
        return real_move(source, destination)

    def traced(name, original):
        def call(*args, **kwargs):
            calls.append(name)
            if scenario == 'formal_rejection' and name == 'open_exact_dataset' and args[1] == '正式宏观发布日历':
                raise ValueError('injected formal readback rejection')
            return original(*args, **kwargs)
        return call

    arguments = ['--lake-root', str(lake_root)]
    if scenario != 'readonly_empty':
        arguments.append('--write')
    if scenario == 'explicit_scope':
        arguments.extend(['--start-date', '2026-08-01', '--end-date', '2026-08-10'])
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(module, 'settings', SimpleNamespace(futures_lake_root=lake_root / 'formal_not_used', futures_data_start_date=date(2026, 7, 17))))
        stack.enter_context(patch.object(module, 'datetime', FrozenDatetime))
        stack.enter_context(patch.object(module.uuid, 'uuid4', return_value=SimpleNamespace(hex='fixed')))
        stack.enter_context(patch.object(module.shutil, 'move', side_effect=move))
        for name in TRACE_NAMES:
            stack.enter_context(patch.object(module, name, side_effect=traced(name, getattr(module, name))))
        result = CliRunner().invoke(module.main, arguments)
    files = {path.relative_to(lake_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in lake_root.rglob('*') if path.is_file()}
    return result, calls, files

scenarios = ['readonly_empty', 'first_write', 'unchanged', 'internal_gap', 'explicit_scope', 'legacy_metadata', 'first_backup_failure', 'leaf_install_failure', 'formal_rejection', 'rollback_failure']
results = []
with tempfile.TemporaryDirectory(prefix='a04-b01-function-diff-') as directory:
    temporary = pathlib.Path(directory)
    for scenario in scenarios:
        runs = []
        for name, module in [('before', before), ('after', after)]:
            lake_root = temporary / scenario / name
            lake_root.mkdir(parents=True)
            with contextlib.redirect_stdout(io.StringIO()):
                prepare(lake_root, scenario)
            runs.append(invoke(module, lake_root, scenario))
        old, new = runs
        assert old[0].exit_code == new[0].exit_code, scenario
        assert type(old[0].exception) == type(new[0].exception), scenario
        assert old[1] == new[1], (scenario, old[1], new[1])
        assert old[2] == new[2], scenario
        output = new[0].output
        assert output.splitlines()[0] == '=' * 80 and output.splitlines()[-1] == '=' * 80
        assert 'function=build_expected_calendar; phase=generate; status=completed' in output
        assert 'function=main; phase=generate_expected; status=completed' not in output
        commit_lines = [line for line in output.splitlines() if line.startswith('committed:') and 'status=completed' in line]
        if scenario in ('first_write', 'internal_gap', 'legacy_metadata'):
            assert len(commit_lines) == 1
            assert 'function=commit_partitions;' in commit_lines[0]
            assert 'calendar_state=committed; date_watermark=none; persisted=true' in commit_lines[0]
            assert output.index('phase=formal_readback; status=completed') < output.index(commit_lines[0])
        else:
            assert not commit_lines, scenario
        if scenario.endswith('failure') or scenario == 'formal_rejection':
            rollback_status = 'failed' if scenario == 'rollback_failure' else 'completed'
            assert f'function=commit_partitions; phase=rollback; status={rollback_status}' in output
            assert 'phase=run; status=completed' not in output
        if scenario == 'readonly_empty':
            assert not new[2]
            assert 'persisted=true' not in output
        (SNAPSHOT / f'{scenario}_after.log').write_text(output, encoding='utf8')
        results.append({'scenario': scenario, 'exit_code': new[0].exit_code, 'same_files': True, 'same_call_order': True, 'commit_events': len(commit_lines)})

# 函数独立调用也拥有完整边界；空输入及无工作不冒充已提交。
with contextlib.redirect_stdout(io.StringIO()) as capture:
    empty = after.empty_pandas(after.MACRO_RELEASE_CALENDAR_SCHEMA)
    after.build_expected_calendar(date(2026, 8, 2), date(2026, 8, 1), empty, date(2026, 8, 17), NOW)
assert 'generated_rows=0; inherited_rows=0; initialized_rows=0; persisted=false' in capture.getvalue()
with contextlib.redirect_stdout(io.StringIO()) as capture:
    assert after.commit_partitions(empty, [], pathlib.Path('unused_log_only')) == 0
assert 'phase=commit; status=skipped' in capture.getvalue() and 'persisted=true' not in capture.getvalue()
with tempfile.TemporaryDirectory(prefix='a04-b01-read-failure-') as directory:
    with contextlib.redirect_stdout(io.StringIO()) as capture:
        try:
            after.open_compatible_dataset(pathlib.Path(directory), 'isolated missing dataset')
        except FileNotFoundError:
            pass
        else:
            raise AssertionError('empty dataset must still fail')
    assert 'phase=discovery; status=failed; error=FileNotFoundError' in capture.getvalue()

report = {
    'business_ast_unchanged': True,
    'comments_cell_state_entry_and_diagrams_preserved': True,
    'notebook_export_exact': True,
    'scenarios': results,
    'standalone_log_checks': ['empty_generation', 'no_work_commit', 'missing_dataset_failure'],
    'api_calls': 0,
    'formal_lake_io': False,
    'sha256': hashlib.sha256(source.encode('utf8')).hexdigest(),
}
(SNAPSHOT / 'function_logs_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
