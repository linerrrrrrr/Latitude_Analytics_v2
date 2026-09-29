"""局部事务迁移：真实临时 Parquet 的前后对照与 Notebook/导出边界核查。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import tokenize
import types
from datetime import date, datetime, timezone
from unittest.mock import patch

import nbformat
import pandas as pd
import pyarrow.parquet as pq
from click.testing import CliRunner
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb')


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


before = load('shibor_transaction_before', SNAPSHOT / REL.with_suffix('.py'))
after = load('shibor_transaction_after', ROOT / REL.with_suffix('.py'))
fixtures = load('shibor_fixtures', ROOT / '00_draft_collection_02/tests/test_b04_c02_interest_rate.py')
results = []
first, second = date(2026, 7, 31), date(2026, 8, 3)


class Client:
    def __init__(self, scenario):
        self.scenario = scenario
        self.calls = []

    def shibor(self, **kwargs):
        self.calls.append(kwargs)
        if self.scenario == 'source_failure_continues' and kwargs['start_date'] == '20260731':
            raise TimeoutError('injected source timeout')
        dates = [value for value in (first, second)
                 if kwargs['start_date'] <= value.strftime('%Y%m%d') <= kwargs['end_date']]
        return pd.concat([fixtures.shibor_response(value, missing_column='on' if self.scenario == 'confirmed_empty' else None)
                          for value in dates], ignore_index=True)


def run(module, lake, client, *, write=True):
    args = ['--lake-root', str(lake)] + (['--write'] if write else [])
    with patch.object(module, 'datetime', FrozenDatetime), patch.object(
        module.uuid, 'uuid4', return_value=types.SimpleNamespace(hex='fixed'),
    ), patch.object(module, 'create_tushare_client', return_value=client) as creating:
        outcome = CliRunner().invoke(module.main, args)
    return outcome, creating.call_count


with tempfile.TemporaryDirectory(prefix='shibor-diff-') as directory:
    base = pathlib.Path(directory)
    seed = base / 'seed'
    with patch.object(fixtures.C01, 'datetime', FrozenDatetime):
        fixtures.build_calendar(seed, first, second)
    completed = base / 'completed'
    shutil.copytree(seed, completed)
    prepared, _ = run(before, completed, Client('success'))
    assert prepared.exit_code == 0, prepared.output
    for scenario in ('readonly', 'first_write', 'confirmed_empty', 'source_failure_continues',
                     'unchanged', 'state_repair', 'metadata_upgrade', 'empty_leaf_delete'):
        runs = []
        for label, module in (('before', before), ('after', after)):
            lake = base / scenario / label
            initialized = scenario in ('unchanged', 'state_repair', 'metadata_upgrade', 'empty_leaf_delete')
            shutil.copytree(completed if initialized else seed, lake)
            if scenario == 'state_repair':
                # 事实已成功、日历尚未安装时，旧日历原字节仍是上游正式版本。
                for source in (seed / 'silver' / module.CALENDAR_TABLE_NAME).rglob('*.parquet'):
                    target = lake / source.relative_to(seed)
                    shutil.copyfile(source, target)
            elif scenario == 'metadata_upgrade':
                for path in (lake / 'silver' / module.TABLE_NAME).rglob('*.parquet'):
                    table = pq.ParquetFile(path).read()
                    metadata = dict(table.schema.metadata)
                    metadata[b'schema_version'] = b'1.0.0'
                    pq.write_table(table.replace_schema_metadata(metadata), path)
            initial_hashes = hashes(lake)
            client = Client(scenario)
            if scenario == 'empty_leaf_delete':
                with contextlib.redirect_stdout(io.StringIO()) as log, patch.object(
                    module.uuid, 'uuid4', return_value=types.SimpleNamespace(hex='fixed'),
                ):
                    committed = module.commit_complete_fact_partition(
                        module.empty_pandas(module.INTEREST_RATE_DAILY_SCHEMA), lake, (2026, 7),
                    )
                result = types.SimpleNamespace(exit_code=0, exception=None, output=log.getvalue())
                creating_count = 0
                assert committed.empty
            else:
                result, creating_count = run(module, lake, client, write=scenario != 'readonly')
            if scenario == 'source_failure_continues':
                assert result.exit_code == 1 and len(client.calls) == 2, result.output
            else:
                assert result.exit_code == 0, result.output
            if scenario in ('unchanged', 'state_repair', 'metadata_upgrade'):
                assert creating_count == 0 and not client.calls
            if scenario in ('readonly', 'unchanged'):
                assert hashes(lake) == initial_hashes
            runs.append((result.exit_code, type(result.exception).__name__ if result.exception else None,
                         creating_count, client.calls, hashes(lake)))
            (SNAPSHOT / f'{scenario}_{label}.log').write_text(result.output, encoding='utf8')
        assert runs[0] == runs[1], f'前后结果不一致：{scenario}'
        results.append({'scenario': scenario, 'exit_code': runs[1][0], 'api_calls': len(runs[1][3]),
                        'all_file_bytes_identical': True})

old_nb, new_nb = nbformat.read(SNAPSHOT / REL, 4), nbformat.read(ROOT / REL, 4)
nbformat.validate(new_nb)
assert old_nb.metadata == new_nb.metadata
assert [c.id for c in old_nb.cells] == [c.id for c in new_nb.cells]
for old, new in zip(old_nb.cells, new_nb.cells, strict=True):
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in new.items() if k != 'source'}
code = lambda nb: '\n\n'.join(c.source for c in nb.cells if c.cell_type == 'code')
definitions = lambda source: {n.name: ast.dump(n) for n in ast.parse(source).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
old_functions, new_functions = definitions(code(old_nb)), definitions(code(new_nb))
assert old_functions.keys() == new_functions.keys()
changed = {name for name in old_functions if old_functions[name] != new_functions[name]}
assert changed == {'upgrade_fact_metadata', 'commit_complete_fact_partition', 'commit_calendar_partition'}
comments = lambda text: [t.string for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type == tokenize.COMMENT]
assert all(c in comments(code(new_nb)) for c in comments(code(old_nb)))
generated, _ = PythonExporter().from_notebook_node(new_nb)
assert (ROOT / REL.with_suffix('.py')).read_bytes() == generated.encode('utf8')
assert '\r' not in generated and b'\r' not in (ROOT / REL).read_bytes()
compile(generated, str(REL), 'exec')
assert 'shutil.move' not in code(new_nb)
for node in ast.parse(code(new_nb)).body:
    if isinstance(node, ast.FunctionDef) and node.name in changed:
        transaction = next(n for n in ast.walk(node) if isinstance(n, ast.With))
        assert any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'open_exact_dataset'
                   for n in ast.walk(transaction)), node.name

with patch.dict(sys.modules, {'ipykernel': object()}), patch.object(
    after.click.Command, 'main', side_effect=AssertionError('import must not run CLI'),
):
    load('shibor_import_in_kernel', ROOT / REL.with_suffix('.py'))
help_result = subprocess.run([sys.executable, '-B', '-X', 'utf8', str(ROOT / REL.with_suffix('.py')), '--help'],
                             cwd=ROOT / REL.parent, capture_output=True, text=True, encoding='utf8')
assert help_result.returncode == 0 and '--write' in help_result.stdout, help_result.stderr
(SNAPSHOT / 'cli_help.log').write_text(help_result.stdout, encoding='utf8')
report = {'comparisons': results, 'changed_functions': sorted(changed), 'original_comments_preserved': True,
          'notebook_state_preserved': True, 'default_export_exact': True, 'kernel_import_safe': True,
          'cli_help_from_business_directory': True, 'real_api_calls': 0, 'formal_lake_io': False,
          'sha256': hashlib.sha256(generated.encode('utf8')).hexdigest()}
(SNAPSHOT / 'transaction_entry_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
