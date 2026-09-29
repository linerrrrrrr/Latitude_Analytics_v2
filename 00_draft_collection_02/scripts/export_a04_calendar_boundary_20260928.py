"""本轮三个 Notebook 的标准隔离同步；只发布本轮三个导出。"""
import ast
import hashlib
import json
import pathlib
import subprocess
import sys

import nbformat
from nbconvert.exporters import PythonExporter

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
FINAL = SNAPSHOT / 'final_export'
FINAL.mkdir(exist_ok=False)
directory = pathlib.Path('a04_Macro_And_Interest_Rates')
names = ['b01_macro_release_calendar', 'b02_interest_rate', 'b03_macro_release']
previous = {name: (ROOT / '02_Futures_Lakehouse' / directory / (name + '.py')).read_bytes() for name in names}
result = subprocess.run([sys.executable, '-B', '-X', 'utf8', str(ROOT / '00_draft_collection_02/scripts/export_a04_b01_isolated_20260928.py'), str(FINAL)], cwd=ROOT, capture_output=True, text=True, encoding='utf8')
(FINAL / 'sync_output.log').write_text(result.stdout + result.stderr, encoding='utf8')
if result.returncode:
    print(result.stdout + result.stderr)
    raise SystemExit(result.returncode)
hashes = {}
for name in names:
    relative = directory / (name + '.ipynb')
    notebook_path = ROOT / '02_Futures_Lakehouse' / relative
    script_path = notebook_path.with_suffix('.py')
    isolated = FINAL / 'isolated_lakehouse' / relative
    assert notebook_path.read_bytes() == isolated.read_bytes()
    generated = isolated.with_suffix('.py').read_bytes()
    assert ast.dump(ast.parse(generated)) == ast.dump(ast.parse(previous[name])), '本次只允许同步最终 Markdown'
    expected, _ = PythonExporter().from_notebook_node(nbformat.read(notebook_path, 4))
    assert generated == expected.encode('utf8')
    if name != names[0]:
        assert script_path.read_bytes() == previous[name]
        script_path.write_bytes(generated)
    assert script_path.read_bytes() == generated
    hashes[name] = hashlib.sha256(generated).hexdigest()
report_path = SNAPSHOT / 'validation_io_verification.json'
report = json.loads(report_path.read_text(encoding='utf8'))
report['exports'] = hashes
report['final_markdown_only_sync'] = True
report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps({'standard_write_check': True, 'workflow_count': 19, 'published_targets': hashes, 'final_business_ast_unchanged': True}, ensure_ascii=False, indent=2))
