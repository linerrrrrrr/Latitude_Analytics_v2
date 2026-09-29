"""本轮单元格状态、注释、源/导出和改动范围检查。"""
import ast
import hashlib
import io
import json
import pathlib
import sys
import tokenize

import nbformat
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb')
old, new = nbformat.read(SNAPSHOT / REL, 4), nbformat.read(ROOT / REL, 4)
code = lambda nb: '\n\n'.join(c.source for c in nb.cells if c.cell_type == 'code')
comments = lambda nb: [t.string for t in tokenize.generate_tokens(io.StringIO(code(nb)).readline) if t.type == tokenize.COMMENT]
assert old.metadata == new.metadata
old_cells = {c.id: c for c in old.cells}
removed = set(old_cells) - {c.id for c in new.cells}
assert removed == {'a04-b02-' + kind + '-' + suffix for kind in ('code', 'doc', 'flow')
                   for suffix in ('dataset-has-exact-schema-metadata', 'partition-expression', 'table-digest')}
for cell in new.cells:
    original = old_cells[cell.id]
    assert {k: v for k, v in original.items() if k != 'source'} == {k: v for k, v in cell.items() if k != 'source'}
removed_comments = [c for c in comments(old) if c not in comments(new)]
assert removed_comments == [
    '# 日历生产者不因描述文字变化重写历史；消费端按同一物理、身份和版本边界读取。',
    '# 最终必须从两个正式路径重读并再次求差，防止日历领先于事实。',
]
assert next(c.source for c in new.cells if c.id == 'de9e1f3f') == old_cells['de9e1f3f'].source
nbformat.validate(new)
generated, _ = PythonExporter().from_notebook_node(new)
assert generated.encode('utf8') == (ROOT / REL.with_suffix('.py')).read_bytes()
assert '\r' not in generated and b'\r' not in (ROOT / REL).read_bytes()
compile(generated, str(REL), 'exec')


class RemoveLogs(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if any(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_AugAssign(self, node):
        if isinstance(node.target, ast.Name) and node.target.id.startswith('log_'):
            return None
        return self.generic_visit(node)

    def visit_If(self, node):
        node = self.generic_visit(node)
        return node if node.body else None


# 完整测试通过之后只微调说明/日志；确认随后没有改变业务 AST。
tested = (SNAPSHOT / 'export2/isolated_lakehouse' / REL.relative_to('02_Futures_Lakehouse').with_suffix('.py')).read_text(encoding='utf8')
assert ast.dump(RemoveLogs().visit(ast.parse(tested))) == ast.dump(RemoveLogs().visit(ast.parse(generated)))

before_hashes = json.loads((SNAPSHOT / 'hashes.json').read_text())
changed_files = [p for p, digest in before_hashes.items() if hashlib.sha256((ROOT / p).read_bytes()).hexdigest() != digest]
allowed = {REL.as_posix(), REL.with_suffix('.py').as_posix(), 'config/data_contracts.py',
           '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md', '03_Futures_Database/AGENTS.md',
           '00_draft_collection_02/tests/test_b04_c02_interest_rate.py',
           '00_draft_collection_02/tests/test_a04_b02_shared_transaction.py',
           '00_draft_collection_02/a04_b02_docs_logs_verification_20260928.md'}
assert set(changed_files) <= allowed, changed_files
report = {'notebook_state_preserved': True, 'removed_auxiliary_blocks': 3,
          'retained_entry_unchanged': True, 'removed_or_updated_comments': removed_comments,
          'export_and_syntax_verified': True, 'final_business_ast_matches_tested_revision': True,
          'old_code_lines': len(code(old).splitlines()), 'new_code_lines': len(code(new).splitlines()),
          'changed_existing_files': changed_files, 'sha256': hashlib.sha256(generated.encode()).hexdigest()}
(SNAPSHOT / 'validation_structure.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
