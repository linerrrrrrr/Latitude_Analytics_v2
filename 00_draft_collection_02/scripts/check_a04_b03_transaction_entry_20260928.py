"""宏观发布本地固定响应前后对照；不联网、不访问正式湖。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import tokenize
import types
from collections import Counter
from datetime import date, datetime, timezone
from unittest.mock import patch

import nbformat
import pyarrow.parquet as pq
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def hashes(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}


class FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = datetime(2026, 9, 28, 10, tzinfo=timezone.utc)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)


before = load('macro_before', SNAPSHOT / REL.with_suffix('.py'))
after = load('macro_after', ROOT / REL.with_suffix('.py'))
fixtures = load('macro_fixtures', ROOT / '00_draft_collection_02/tests/test_b04_c03_macro_release.py')
first, second = date(2026, 6, 30), date(2026, 7, 31)


class Session:
    def __init__(self, scenario):
        self.scenario = scenario
        self.calls = []
        self.closed = False

    def get(self, url, *, params, timeout):
        self.calls.append({'params': dict(params), 'timeout': timeout})
        source_date = date.fromisoformat(re.search(r"REPORT_DATE>='([0-9-]+)'", params['filter']).group(1))
        report = params['reportName']
        row = fixtures.source_row(report, source_date)
        if report == 'RPT_ECONOMY_CPI' and source_date.month == 6:
            field = before.SERIES_BY_REPORT[report][0].source_column
            if self.scenario == 'source_failure_continues':
                del row[field]
            elif self.scenario == 'confirmed_empty':
                row[field] = None
        return fixtures.FakeResponse({'success': True, 'result': {'pages': 1, 'count': 1, 'data': [row]}})

    def close(self):
        self.closed = True


def run(module, lake, session, *, write=True, capture_costs=False):
    calls = Counter()
    inputs = {'full_fact_partition': [], 'apply_calendar_completion': []}
    root_opens = []
    def recording(name, real):
        def invoke(*args, **kwargs):
            calls[name] += 1
            if name in inputs:
                inputs[name].append(len(args[0]))
            if name == 'open_exact_dataset' and args[4] in ('正式宏观发布事实', '回写后的正式宏观发布日历', '待回写的正式宏观发布日历'):
                if args[0] in (lake / 'silver' / module.TABLE_NAME, lake / 'silver' / module.CALENDAR_TABLE_NAME):
                    root_opens.append(str(args[0]))
            return real(*args, **kwargs)
        return invoke
    with contextlib.ExitStack() as stack:
        stack.enter_context(patch.object(module, 'datetime', FrozenDatetime))
        stack.enter_context(patch.object(module.uuid, 'uuid4', return_value=types.SimpleNamespace(hex='fixed')))
        creating = stack.enter_context(patch.object(module, 'create_eastmoney_session', return_value=session))
        if capture_costs:
            for name in ('read_macro_calendar', 'read_optional_fact', 'plan_macro_release_grids',
                         'validate_macro_calendar_table', 'validate_macro_release_frame',
                         'full_fact_partition', 'apply_calendar_completion', 'open_exact_dataset'):
                stack.enter_context(patch.object(module, name, side_effect=recording(name, getattr(module, name))))
        result = CliRunner().invoke(module.main, ['--lake-root', str(lake)] + (['--write'] if write else []))
    return result, creating.call_count, {'calls': dict(calls), 'leaf_input_rows': inputs, 'commit_table_root_opens': len(root_opens)}


results, costs = [], {}
with tempfile.TemporaryDirectory(prefix='macro-compare-') as directory:
    base = pathlib.Path(directory)
    seed = base / 'seed'
    with patch.object(fixtures.C01, 'datetime', FrozenDatetime):
        built = CliRunner().invoke(fixtures.C01.main, ['--lake-root', str(seed), '--start-date', first.isoformat(), '--end-date', second.isoformat(), '--write'])
    assert built.exit_code == 0, built.output
    completed = base / 'completed'
    shutil.copytree(seed, completed)
    prepared, _, _ = run(before, completed, Session('success'))
    assert prepared.exit_code == 0, prepared.output
    for scenario in ('readonly', 'first_write', 'confirmed_empty', 'source_failure_continues',
                     'unchanged', 'state_repair', 'metadata_upgrade', 'empty_leaf_delete', 'repair_and_fetch_same_month'):
        runs = []
        for label, module in (('before', before), ('after', after)):
            lake = base / scenario / label
            initialized = scenario in ('unchanged', 'state_repair', 'metadata_upgrade', 'empty_leaf_delete', 'repair_and_fetch_same_month')
            shutil.copytree(completed if initialized else seed, lake)
            if scenario in ('state_repair', 'repair_and_fetch_same_month'):
                for source in (seed / 'silver' / module.CALENDAR_TABLE_NAME).rglob('*.parquet'):
                    shutil.copyfile(source, lake / source.relative_to(seed))
                if scenario == 'repair_and_fetch_same_month':
                    for path in (lake / 'silver' / module.TABLE_NAME).rglob('part-*.parquet'):
                        table = pq.ParquetFile(path).read()
                        codes = {s.series_code for s in module.SERIES_BY_REPORT['RPT_ECONOMY_CPI']}
                        table = table.filter(module.pa.array([s in codes for s in table['series_code'].to_pylist()]))
                        pq.write_table(table, path)
            elif scenario == 'metadata_upgrade':
                for path in (lake / 'silver' / module.TABLE_NAME).rglob('*.parquet'):
                    table = pq.ParquetFile(path).read()
                    metadata = dict(table.schema.metadata)
                    metadata[b'schema_version'] = b'1.0.0'
                    pq.write_table(table.replace_schema_metadata(metadata), path)
            initial = hashes(lake)
            session = Session(scenario)
            if scenario == 'empty_leaf_delete':
                with contextlib.redirect_stdout(io.StringIO()) as log, patch.object(module.uuid, 'uuid4', return_value=types.SimpleNamespace(hex='fixed')):
                    committed = module.commit_complete_fact_partition(module.empty_pandas(module.MACRO_RELEASE_SCHEMA), lake, (2026, 6))
                result = types.SimpleNamespace(exit_code=0, exception=None, output=log.getvalue())
                creating = 0
                assert committed.empty
            else:
                result, creating, measured = run(module, lake, session, write=scenario != 'readonly', capture_costs=scenario == 'first_write')
                if scenario == 'first_write':
                    costs[label] = measured
            expected_exit = 1 if scenario == 'source_failure_continues' else 0
            assert result.exit_code == expected_exit, result.output
            if scenario == 'source_failure_continues':
                assert len(session.calls) == 7 and session.closed
            if scenario in ('unchanged', 'state_repair', 'metadata_upgrade'):
                assert creating == 0 and not session.calls
            if scenario == 'repair_and_fetch_same_month':
                assert len(session.calls) == 5
            if scenario in ('readonly', 'unchanged'):
                assert hashes(lake) == initial
            runs.append((result.exit_code, type(result.exception).__name__ if result.exception else None, creating, session.calls, hashes(lake)))
            (SNAPSHOT / f'{scenario}_{label}.log').write_text(result.output, encoding='utf8')
        reencoded = []
        if runs[0] != runs[1] and '--allow-parquet-reencoding' in sys.argv:
            assert runs[0][:4] == runs[1][:4], f'请求或运行结果不同：{scenario}'
            assert runs[0][4].keys() == runs[1][4].keys(), f'文件集合不同：{scenario}'
            for name in runs[0][4]:
                if runs[0][4][name] == runs[1][4][name]:
                    continue
                a, b = base / scenario / 'before' / name, base / scenario / 'after' / name
                assert a.suffix == '.parquet', name
                pa, pb = pq.ParquetFile(a), pq.ParquetFile(b)
                ta, tb = pa.read(), pb.read()
                assert ta.schema.equals(tb.schema, check_metadata=True), (scenario, name, 'schema')
                keys = [(key, 'ascending') for key in ('series_code', 'report_date') if key in ta.column_names]
                assert ta.sort_by(keys).equals(tb.sort_by(keys)), (scenario, name, 'business row values')
                reencoded.append({'path': name, 'values_and_full_schema_equal': True, 'physical_row_order_equal': ta.equals(tb),
                                  'row_groups_before': [pa.metadata.row_group(i).num_rows for i in range(pa.num_row_groups)],
                                  'row_groups_after': [pb.metadata.row_group(i).num_rows for i in range(pb.num_row_groups)]})
                pa.close()
                pb.close()
        else:
            assert runs[0] == runs[1], f'前后结果不一致：{scenario}'
        results.append({'scenario': scenario, 'exit_code': runs[1][0], 'api_calls': len(runs[1][3]),
                        'all_file_bytes_identical': runs[0][4] == runs[1][4], 'reencoded_parquet': reencoded})

assert costs['after']['commit_table_root_opens'] == 0
for name in ('read_macro_calendar', 'read_optional_fact', 'plan_macro_release_grids'):
    assert costs['after']['calls'][name] == 1
assert costs['after']['calls']['validate_macro_calendar_table'] == 7
assert costs['after']['calls']['validate_macro_release_frame'] == 14
new_nb = nbformat.read(ROOT / REL, 4)
nbformat.validate(new_nb)
generated, _ = PythonExporter().from_notebook_node(new_nb)
assert (ROOT / REL.with_suffix('.py')).read_bytes() == generated.encode('utf8')
assert '\r' not in generated
compile(generated, str(REL), 'exec')
structure = {}
if '--transaction-structure' in sys.argv:
    old_nb = nbformat.read(SNAPSHOT / REL, 4)
    assert old_nb.metadata == new_nb.metadata
    assert [c.id for c in old_nb.cells] == [c.id for c in new_nb.cells]
    for old, new in zip(old_nb.cells, new_nb.cells, strict=True):
        assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in new.items() if k != 'source'}
    code = lambda nb: '\n\n'.join(c.source for c in nb.cells if c.cell_type == 'code')
    defs = lambda s: {n.name: ast.dump(n) for n in ast.parse(s).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
    old, new = defs(code(old_nb)), defs(code(new_nb))
    assert old.keys() == new.keys()
    changed = {n for n in old if old[n] != new[n]}
    assert changed == {'upgrade_fact_metadata', 'commit_complete_fact_partition', 'commit_calendar_partition'}
    comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
    assert all(c in comments(code(new_nb)) for c in comments(code(old_nb)))
    assert 'shutil.move' not in code(new_nb)
    for node in ast.parse(code(new_nb)).body:
        if isinstance(node, ast.FunctionDef) and node.name in changed:
            transaction = next(n for n in ast.walk(node) if isinstance(n, ast.With))
            assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'open_exact_dataset' for n in ast.walk(transaction))
    structure = {'changed_functions': sorted(changed), 'all_original_comments_preserved': True, 'cell_state_preserved': True}
with patch.dict(sys.modules, {'ipykernel': object()}), patch.object(after.click.Command, 'main', side_effect=AssertionError('import must not run CLI')):
    load('macro_kernel_import', ROOT / REL.with_suffix('.py'))
help_result = subprocess.run([sys.executable, '-B', '-X', 'utf8', str(ROOT / REL.with_suffix('.py')), '--help'], cwd=ROOT / REL.parent, capture_output=True, text=True, encoding='utf8')
assert help_result.returncode == 0 and '--write' in help_result.stdout, help_result.stderr
report = {'comparisons': results, 'costs': costs, **structure, 'kernel_import_safe': True, 'cli_help_from_subdirectory': True, 'export_exact': True, 'real_api_calls': 0, 'formal_lake_io': False, 'sha256': hashlib.sha256(generated.encode('utf8')).hexdigest()}
(SNAPSHOT / 'macro_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
