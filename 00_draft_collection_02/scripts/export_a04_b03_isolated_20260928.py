"""在临时副本运行原标准同步入口，仅发布 a04_b03，避免覆盖其他正在编辑的工作流。"""
import ast
import hashlib
import json
import pathlib
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
LAKEHOUSE = ROOT/'02_Futures_Lakehouse'
TARGET_RELATIVE = pathlib.Path('a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
notebook_path = LAKEHOUSE/TARGET_RELATIVE
script_path = notebook_path.with_suffix('.py')
before_notebook = notebook_path.read_bytes()
before_script = script_path.read_bytes()
isolated = SNAPSHOT/'isolated_lakehouse'
isolated.mkdir(exist_ok=False)
shutil.copyfile(LAKEHOUSE/'a00_02_sync_notebook_exports.py', isolated/'a00_02_sync_notebook_exports.py')
for directory in LAKEHOUSE.glob('a[0-9][0-9]_*'):
    if directory.is_dir():
        (isolated/directory.name).mkdir()
        for source in directory.glob('b*.*'):
            if source.suffix in ('.ipynb', '.py'):
                shutil.copyfile(source, isolated/directory.name/source.name)
for mode in ('--write', '--check'):
    result = subprocess.run(
        [sys.executable, '-B', '-X', 'utf8', str(isolated/'a00_02_sync_notebook_exports.py'), mode],
        cwd=ROOT, text=True, encoding='utf8', capture_output=True,
    )
    (SNAPSHOT/('isolated_export_'+mode[2:]+'.log')).write_text(result.stdout+result.stderr, encoding='utf8')
    if result.returncode:
        print(result.stdout+result.stderr)
        raise SystemExit(result.returncode)
    print(result.stdout.strip().splitlines()[-1])
generated = (isolated/TARGET_RELATIVE.with_suffix('.py')).read_bytes()
ast.parse(generated.decode('utf8'), filename=str(script_path))
assert notebook_path.read_bytes()==before_notebook, 'a04_b03 Notebook 在导出期间发生变化，未发布'
assert script_path.read_bytes()==before_script, 'a04_b03 Python 在导出期间发生变化，未发布'
script_path.write_bytes(generated)
assert script_path.read_bytes()==generated
report = {
    'standard_entry':'a00_02_sync_notebook_exports.py',
    'isolated_write_check':True,
    'workflow_count':19,
    'published_target':str(script_path),
    'published_sha256':hashlib.sha256(generated).hexdigest(),
    'other_production_files_written':0,
    'note':'19 组检查在临时副本完成；不表示原工作区其他正在编辑的双轨已同步。',
}
(SNAPSHOT/'export_verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
