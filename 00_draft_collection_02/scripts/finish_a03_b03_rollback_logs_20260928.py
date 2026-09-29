"""把恢复日志放到实际恢复之后；日志异常不能阻断恢复或触发证据清理。"""
import ast
import pathlib
import textwrap
import nbformat

ROOT=pathlib.Path(__file__).resolve().parents[2]
path=ROOT/'02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb'
notebook=nbformat.read(path,as_version=4)
cells={c.id:c for c in notebook.cells}
editing=(ROOT/'00_draft_collection_02/scripts/update_a03_b01_function_logs_20260928.py').read_text(encoding='utf8')
exec(editing[editing.index('class FunctionEdit:'):editing.index('\ndef event(')])
for cell_id,name in (
    ('b03-c03-15','upgrade_fact_metadata'),
    ('a03-b03-fact-commit','commit_complete_fact_partition'),
    ('a03-b03-calendar-commit','commit_calendar_partitions'),
):
    edit=FunctionEdit(cell_id,name)
    for node in ast.walk(edit.function):
        if isinstance(node,ast.Expr) and isinstance(node.value,ast.Call) and ast.unparse(node.value.func)=='click.echo' and 'phase=rollback; status=started;' in ast.unparse(node):
            edit.replace(ast.unparse(node),'')
    edit.insert('cleanup_recovery_paths = False',f'''click.echo(
        f"planning_progress: dataset={{DATASET_NAME}}; function={name}; phase=rollback; status=failed; "
        f"original_failed_phase={{log_phase}}; failed_paths={{len(rollback_errors)}}; backup_dir={{backup_path}}; "
        f"quarantine_dir={{quarantine_path}}; elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
    )''',after=True)
    edit.apply()
nbformat.validate(notebook)
path.write_text(nbformat.writes(notebook)+'\n',encoding='utf8',newline='\n')
