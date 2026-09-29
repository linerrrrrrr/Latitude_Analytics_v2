"""Freeze the complete pre-refactor operations tree as a verified local archive."""
import hashlib
import json
import os
import pathlib
import stat
import sys
import zipfile
from datetime import datetime, timezone

import psutil

project_root = pathlib.Path.cwd().resolve()
operations_root = project_root / "02_Futures_Lakehouse" / "operations"
assert operations_root.is_relative_to(project_root)
if (operations_root / "run_history" / ".active_formal_run").exists():
    raise RuntimeError("Active lock exists; preserve the running checkout.")
for process in psutil.process_iter(["cmdline"]):
    if process.pid == os.getpid():
        continue
    command = process.info["cmdline"] or []
    if any(pathlib.Path(arg).name in {"run_daily_update.py", "background_worker.py", "watch_batch.ps1", "run_contract_calendar_full_update.py"} for arg in command):
        raise RuntimeError(f"Operations process still running: {process.pid}")

reference_root = operations_root / "referance"
if reference_root.exists():
    raise FileExistsError(reference_root)
original_paths = sorted(operations_root.rglob("*"))
if any(path.is_symlink() for path in original_paths):
    raise RuntimeError("Unexpected symbolic link in archive source.")
created_at = datetime.now(timezone.utc)
archive_name = f"operations-before-console-{created_at:%Y%m%dT%H%M%SZ}.zip"
reference_root.mkdir()
archive_path = reference_root / archive_name
entries = []
with zipfile.ZipFile(archive_path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
    for path in original_paths:
        relative_path = path.relative_to(operations_root).as_posix()
        if path.is_dir():
            archive.write(path, "operations/" + relative_path + "/")
            continue
        payload = path.read_bytes()
        entries.append({"path": relative_path, "byte_size": len(payload), "sha256": hashlib.sha256(payload).hexdigest(), "mtime_ns": path.stat().st_mtime_ns})
        archive.writestr("operations/" + relative_path, payload)
with zipfile.ZipFile(archive_path) as archive:
    assert archive.testzip() is None
    for entry in entries:
        payload = archive.read("operations/" + entry["path"])
        assert hashlib.sha256(payload).hexdigest() == entry["sha256"]
        assert (operations_root / entry["path"]).read_bytes() == payload
manifest = {
    "created_at": created_at.isoformat(), "source": str(operations_root),
    "archive": archive_name, "archive_sha256": hashlib.sha256(archive_path.read_bytes()).hexdigest(),
    "file_count": len(entries), "total_bytes": sum(entry["byte_size"] for entry in entries),
    "scope": "All pre-existing files and directories, including ignored history, caches and local modifications. No exclusions.",
    "read_only": True, "files": entries,
}
manifest_path = reference_root / "snapshot_manifest.json"
manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
for path in (archive_path, manifest_path):
    path.chmod(stat.S_IREAD)
print(json.dumps({key: manifest[key] for key in ("archive", "archive_sha256", "file_count", "total_bytes")}, ensure_ascii=False))
