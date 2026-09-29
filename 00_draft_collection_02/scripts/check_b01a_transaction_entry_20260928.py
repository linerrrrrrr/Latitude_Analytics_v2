"""当前轮次的差异、注释、Notebook 元数据与默认导出验证。"""
import ast
import hashlib
import io
import json
import pathlib
import sys
import tokenize

import nbformat
from nbconvert.exporters import PythonExporter

root = pathlib.Path(__file__).resolve().parents[2]
snapshot = pathlib.Path(sys.argv[1])
relative = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01a_position_rank_special_case_calibration.ipynb')
before = json.loads((snapshot / relative).read_text(encoding='utf8'))
after = json.loads((root / relative).read_text(encoding='utf8'))
before_cells = {c['id']: c for c in before['cells']}
after_cells = {c['id']: c for c in after['cells']}
assert {k: v for k, v in before.items() if k != 'cells'} == {k: v for k, v in after.items() if k != 'cells'}
assert [c['id'] for c in after['cells']][:len(before['cells'])] == list(before_cells)
code_changes = []
for cell_id, old in before_cells.items():
    new = after_cells[cell_id]
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in new.items() if k != 'source'}, cell_id
    if old['cell_type'] == 'code':
        def comments(cell):
            return [token.string for token in tokenize.generate_tokens(io.StringIO(''.join(cell['source'])).readline)
                    if token.type == tokenize.COMMENT]
        assert comments(old) == comments(new), cell_id
        if old['source'] != new['source']:
            code_changes.append(cell_id)
assert code_changes == ['3d5940bc', 'b01a-commit-artifacts', '30c24e87'], code_changes
old_tree = ast.parse((snapshot / relative.with_suffix('.py')).read_text(encoding='utf8'))
new_tree = ast.parse((root / relative.with_suffix('.py')).read_text(encoding='utf8'))
old_functions = {n.name: ast.dump(n) for n in old_tree.body if isinstance(n, ast.FunctionDef)}
new_functions = {n.name: ast.dump(n) for n in new_tree.body if isinstance(n, ast.FunctionDef)}
assert old_functions.keys() == new_functions.keys()
unchanged_functions = [name for name in old_functions if name != 'commit_artifacts']
assert all(old_functions[name] == new_functions[name] for name in unchanged_functions)
notebook = nbformat.read(root / relative, as_version=4)
nbformat.validate(notebook)
exported, _ = PythonExporter().from_notebook_node(notebook)
assert (root / relative.with_suffix('.py')).read_bytes() == exported.encode('utf8')
compile(exported, str(relative.with_suffix('.py')), 'exec')
terminal = after_cells['b01a-terminal-command']
assert ast.parse(''.join(terminal['source'])).body == []
assert terminal['execution_count'] is None and terminal['outputs'] == []
hashes = json.loads((snapshot / 'hashes.json').read_text(encoding='utf8'))
changed = [p for p, digest in hashes.items() if hashlib.sha256((root / p).read_bytes()).hexdigest() != digest]
expected = {relative.as_posix(), relative.with_suffix('.py').as_posix(), 'AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'}
assert set(changed) == expected, changed
report = {
    'changed_code_cells': code_changes,
    'original_comments_metadata_outputs_execution_counts_preserved': True,
    'unchanged_function_asts': unchanged_functions,
    'new_helpers_or_business_parameters': 0,
    'notebook_and_default_export_byte_exact': True,
    'terminal_cell_comments_only': True,
    'changed_production_and_specification_files': changed,
    'shared_module_and_frozen_configuration_unchanged': True,
}
(snapshot / 'final_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
