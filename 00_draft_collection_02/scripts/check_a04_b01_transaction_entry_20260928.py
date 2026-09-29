"""第 10—12 项：成功结果前后对照、改动范围和入口检查。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import subprocess
import sys
import tempfile
from datetime import date, datetime, timezone
from unittest.mock import patch

import nbformat
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def hashes(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}


before = load('macro_transaction_before', SNAPSHOT / REL.with_suffix('.py'))
after = load('macro_transaction_after', ROOT / REL.with_suffix('.py'))
fixtures = load('macro_transaction_fixtures', ROOT / '00_draft_collection_02/tests/test_b04_c01_macro_release_calendar.py')
with contextlib.redirect_stdout(io.StringIO()):
    frame = after.build_expected_calendar(
        date(2026, 7, 17), date(2026, 8, 31), after.empty_pandas(after.MACRO_RELEASE_CALENDAR_SCHEMA),
        date(2026, 9, 17), datetime(2026, 9, 17, tzinfo=timezone.utc),
    )
results = []
with tempfile.TemporaryDirectory(prefix='a04-diff-') as directory:
    base = pathlib.Path(directory)
    for scenario in ('first_write', 'partial_update', 'update_and_delete', 'full_swap', 'zero_row_root', 'unchanged'):
        expected = frame.copy()
        existing = frame
        if scenario == 'first_write':
            existing = after.empty_pandas(after.MACRO_RELEASE_CALENDAR_SCHEMA)
        elif scenario == 'zero_row_root':
            expected = after.empty_pandas(after.MACRO_RELEASE_CALENDAR_SCHEMA)
        elif scenario != 'unchanged':
            expected.loc[expected['dataset_name'].eq('interest_rate'), 'quality_reason'] = '本批内容更新'
            if scenario == 'update_and_delete':
                expected = expected.loc[~(expected['dataset_name'].eq('macro_release') & expected['month'].eq(7))].copy()
        runs = []
        for name, module in (('before', before), ('after', after)):
            lake = base / scenario / name
            with contextlib.redirect_stdout(io.StringIO()) as output:
                if scenario != 'first_write':
                    fixtures.write_partitioned_calendar(module, existing, lake / 'silver' / module.TABLE_NAME)
                keys = module.changed_partition_keys(expected, existing)
                count = module.commit_partitions(expected, keys, lake, force_full_swap=scenario == 'full_swap')
            runs.append((count, hashes(lake)))
            (SNAPSHOT / f'{scenario}_{name}.log').write_text(output.getvalue(), encoding='utf8')
        assert runs[0] == runs[1], scenario
        results.append({'scenario': scenario, 'written_rows': runs[1][0], 'all_file_bytes_identical': True})

old_nb, new_nb = nbformat.read(SNAPSHOT / REL, 4), nbformat.read(ROOT / REL, 4)
nbformat.validate(new_nb)
assert old_nb.metadata == new_nb.metadata
assert [c.id for c in old_nb.cells] == [c.id for c in new_nb.cells]
changed_cells = []
for old, new in zip(old_nb.cells, new_nb.cells, strict=True):
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in new.items() if k != 'source'}
    if old.source != new.source:
        changed_cells.append(new.id)
assert set(changed_cells) == {'b5ed1a92', 'f4c3582d', '4a5b544e', 'a04-b01-doc-imports',
    'a04-b01-flow-overview', '62335bcd', 'a04-b01-flow-f4c3582d', 'dfa42b3a', 'a04-b01-flow-4a5b544e'}
functions = lambda nb: {n.name: ast.dump(n) for n in ast.parse('\n\n'.join(c.source for c in nb.cells if c.cell_type == 'code')).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
old_functions, new_functions = functions(old_nb), functions(new_nb)
assert old_functions.keys() == new_functions.keys()
assert [name for name in old_functions if old_functions[name] != new_functions[name]] == ['commit_partitions']
generated, _ = PythonExporter().from_notebook_node(new_nb)
assert (ROOT / REL.with_suffix('.py')).read_bytes() == generated.encode('utf8')
assert '\r' not in generated
tree = ast.parse(generated)
commit = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'commit_partitions')
for node in ast.walk(commit):
    if isinstance(node, (ast.For, ast.While)):
        assert not any(isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == 'to_table' for call in ast.walk(node))

# 用真实模块导入覆盖“内核内 import”路径，禁止 Click 或展示执行。
with patch.dict(sys.modules, {'ipykernel': object()}), patch.object(after.click.Command, 'main', side_effect=AssertionError('import ran CLI')):
    load('macro_transaction_import_in_kernel', ROOT / REL.with_suffix('.py'))
help_result = subprocess.run([sys.executable, '-B', '-X', 'utf8', str(ROOT / REL.with_suffix('.py')), '--help'],
    cwd=ROOT / REL.parent, capture_output=True, text=True, encoding='utf8')
assert help_result.returncode == 0, help_result.stderr
assert '--write' in help_result.stdout and '--lake-root' in help_result.stdout
(SNAPSHOT / 'cli_help.log').write_text(help_result.stdout, encoding='utf8')
report = {
    'comparisons': results, 'changed_cell_ids': changed_cells, 'changed_function': 'commit_partitions',
    'notebook_state_preserved': True, 'default_python_exporter_exact': True,
    'kernel_import_has_no_entry_side_effects': True, 'cli_help_from_business_directory': True,
    'published_sha256': hashlib.sha256(generated.encode('utf8')).hexdigest(),
    'production_lake_io': False, 'api_calls': 0,
}
(SNAPSHOT / 'transaction_entry_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
