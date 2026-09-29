"""One-time edits for a00 dashboard telemetry; no collection or export execution."""
from pathlib import Path

root = Path(__file__).resolve().parents[2]
control_path = root / "02_Futures_Lakehouse/operations/runtime/verify_operations_runtime.py"
source = control_path.read_text(encoding="utf-8")
for phase, title, before, after in (
    ("atomic_path", "长路径原子替换", "        probe_root = pathlib.Path(temporary_directory)", '        print(f"python_atomic_replace_long_path_ok: {len(str(status_path))}")'),
    ("shared_read", "共享读取与再次替换", "        powershell_environment = os.environ.copy()", '        print("powershell_fileshare_readwrite_delete_ok: True")'),
    ("arrow_roundtrip", "Arrow 长路径往返", '        parquet_path = unicode_space_directory / "round trip 中文.parquet"', '        print(f"pyarrow_long_unicode_space_round_trip_ok: {len(str(parquet_path))}")'),
):
    for anchor, state, completed in ((before, "running", 0), (after, "completed", 1)):
        event_source = f'''        print("progress_event: " + json.dumps({{
            "event_version": 1, "event": "phase", "scope_id": "{phase}", "phase": "{phase}", "state": "{state}",
            "object": {{"check": "{title}"}},
            "counters": {{"checks": {{"completed": {completed}, "total": 1, "unit": "项检查"}}}},
        }}, ensure_ascii=False), flush=True)'''
        assert source.count(anchor) == 1, anchor
        source = source.replace(anchor, event_source + "\n" + anchor if state == "running" else anchor + "\n" + event_source)
control_path.write_text(source, encoding="utf-8", newline="\n")

export_path = root / "02_Futures_Lakehouse/a00_02_sync_notebook_exports.py"
source = export_path.read_text(encoding="utf-8")
source = source.replace('        try:\n            notebook =', '        export_error = ""\n        try:\n            notebook =', 1)
source = source.replace('            export_error_count += 1', '            export_error_count += 1\n            export_error = f"{type(error).__name__}: {error}"', 1)
anchor = '\n    print("progress_event: " + json.dumps({\n        "event_version": 1, "event": "phase", "scope_id": "export"'
insert = '''
        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "phase", "scope_id": "export:" + str(notebook_path.relative_to(PROJECT_DIR)),
            "parent_scope_id": "export", "phase": "export", "state": "failed" if export_error else "completed",
            "object": {"notebook": str(notebook_path.relative_to(PROJECT_DIR))}, "message": export_error[:2048],
        }, ensure_ascii=False), flush=True)
'''
assert source.count(anchor) == 1
source = source.replace(anchor, insert + anchor)
anchor = '''            artifact.script_path.write_text(
                artifact.script_source,
                encoding="utf-8",
                newline="\\n",
            )'''
assert source.count(anchor) == 1
source = source.replace(anchor, '''            write_error = ""
            try:
                artifact.script_path.write_text(
                    artifact.script_source,
                    encoding="utf-8",
                    newline="\\n",
                )
            except Exception as error:
                write_error = f"{type(error).__name__}: {error}"
                raise
            finally:
                print("progress_event: " + json.dumps({
                    "event_version": 1, "event": "phase", "scope_id": "write:" + str(artifact.notebook_path.relative_to(PROJECT_DIR)),
                    "parent_scope_id": "write", "phase": "write", "state": "failed" if write_error else "completed",
                    "object": {"notebook": str(artifact.notebook_path.relative_to(PROJECT_DIR))}, "message": write_error[:2048],
                }, ensure_ascii=False), flush=True)''')
source = source.replace('        try:\n            if not artifact.script_path.is_file():', '        check_error = ""\n        check_warning = False\n        try:\n            if not artifact.script_path.is_file():', 1)
source = source.replace('                    warning_count += 1', '                    warning_count += 1\n                    check_warning = True', 1)
source = source.replace('            failed_count += 1', '            failed_count += 1\n            check_error = f"{type(error).__name__}: {error}"', 1)
anchor = '\n    print("progress_event: " + json.dumps({\n        "event_version": 1, "event": "phase", "scope_id": "check"'
assert source.count(anchor) == 1
source = source.replace(anchor, '''
        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "phase", "scope_id": "check:" + str(artifact.notebook_path.relative_to(PROJECT_DIR)),
            "parent_scope_id": "check", "phase": "check", "state": "failed" if check_error else "completed",
            "outcome": "warning" if check_warning else "failed" if check_error else "passed",
            "object": {"notebook": str(artifact.notebook_path.relative_to(PROJECT_DIR))}, "message": check_error[:2048],
        }, ensure_ascii=False), flush=True)
''' + anchor)
export_path.write_text(source, encoding="utf-8", newline="\n")
