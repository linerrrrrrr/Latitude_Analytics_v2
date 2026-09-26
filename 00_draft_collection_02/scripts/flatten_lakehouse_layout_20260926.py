"""Apply the approved source/layout reference migration; directory moves are separate."""
from pathlib import Path
import hashlib
import json
import os
import re
import subprocess
import sys
import zipfile

import nbformat
import pyarrow as pa

root = Path(__file__).resolve().parents[2]
lake = root / "02_Futures_Lakehouse"
collection = lake / "a01_Data_Collection"
operations = lake / "a02_Data_Collection_Operations"
record_dir = root / "00_draft_collection_02" / "lakehouse_layout_20260926"
sys.path.insert(0, str(root))
from config import data_contracts

groups = {
    "b01_Futures_Market_Data": "a01_Futures_Market_Data",
    "b02_Futures_Exchange_Reports": "a02_Futures_Exchange_Reports",
    "b03_External_Market_Data": "a03_External_Market_Data",
    "b04_Macro_And_Interest_Rates": "a04_Macro_And_Interest_Rates",
}
stems = {p.stem: "b" + p.stem[1:] for group in groups for p in (collection / group).glob("c*.ipynb")}
assert len(stems) == 19, len(stems)
assert not (operations / "run_history/.active_formal_run").exists()
assert not record_dir.exists(), "A migration record already exists; do not overwrite it."

def relocated(path):
    path = path.resolve()
    assert path.is_relative_to(root), path
    if path.is_relative_to(collection):
        parts = list(path.relative_to(collection).parts)
        if not parts:
            return lake
        parts[0] = groups.get(parts[0], parts[0])
        if parts[-1] == "b00_sync_notebook_exports.py":
            parts[-1] = "sync_notebook_exports.py"
        elif Path(parts[-1]).stem in stems:
            parts[-1] = stems[Path(parts[-1]).stem] + Path(parts[-1]).suffix
        return lake.joinpath(*parts)
    if path.is_relative_to(operations):
        parts = list(path.relative_to(operations).parts)
        if parts:
            parts[0] = {"b02_Manual_Maintenance": "manual_maintenance", "b03_Archived_One_Off_Batches": "archived_batches"}.get(parts[0], parts[0])
            if parts[-1] == "run_c03_contract_calendar_full_update.py":
                parts[-1] = "run_contract_calendar_full_update.py"
        return (lake / "operations").joinpath(*parts)
    return path

archive_manifest = json.loads((operations / "b03_Archived_One_Off_Batches/snapshot_manifest.json").read_text(encoding="utf-8"))
historical_sources = {root / item["source_path"] for item in archive_manifest["snapshots"]}
historical_sources.add(root / "00_draft_collection_02/scripts/migrate_lakehouse_config_layout_20260926.py")

def immutable(path):
    return (
        "run_history" in path.parts or "run_status" in path.parts
        or "migration_reports" in path.parts or "b03_Archived_One_Off_Batches" in path.parts
        or "05_Old_Projects" in path.parts or "futures_lake" in path.parts
        or path in historical_sources or path == Path(__file__)
        or path == root / "config/data_contracts.py"
        or path.name == "notebook_schema_browser_before_visual_20260926.py"
    )

def rewrite(text, path):
    new_path = relocated(path)
    # Resolve Markdown links against their original owner before changing paths.
    if path.suffix == ".md" or path.suffix == ".ipynb":
        def link(match):
            reference = match.group(1)
            if "://" in reference or reference.startswith(("#", "mailto:")):
                return match.group(0)
            target, marker, anchor = reference.partition("#")
            resolved = (path.parent / target).resolve()
            if not resolved.is_relative_to(root):
                return match.group(0)
            destination = relocated(resolved)
            return "](" + Path(os.path.relpath(destination, new_path.parent)).as_posix() + (marker + anchor if marker else "") + ")"
        text = re.sub(r"\]\(([^\n)]+)\)", link, text)
    short_ids = path.is_relative_to(lake) or path in {
        root / "AGENTS.md", root / "03_Futures_Database/AGENTS.md", root / ".env.template",
        root / "config/futures_lakehouse/macro_release_entities.py",
    }
    if short_ids:
        text = re.sub(r"(?<![A-Za-z0-9_])b(0[1-4])(?![A-Za-z0-9_])", r"a\1", text)
        text = re.sub(r"(?<![A-Za-z0-9_])c(0[1-8]a?)(?![A-Za-z0-9_])", r"b\1", text)
    for separator in ("/", "\\", "."):
        text = text.replace("02_Futures_Lakehouse" + separator + "a01_Data_Collection", "02_Futures_Lakehouse")
    # pathlib joins may span physical lines.
    if path.suffix in {".py", ".ipynb"} and "02_Futures_Lakehouse" in text:
        text = re.sub(r'\s*/\s*[\"\']a01_Data_Collection[\"\']', "", text)
    for old, new in groups.items():
        text = text.replace(old, new)
    for old, new in stems.items():
        text = text.replace(old, new)
    for old, new in {
        "a02_Data_Collection_Operations": "operations",
        "b02_Manual_Maintenance": "manual_maintenance",
        "b03_Archived_One_Off_Batches": "archived_batches",
        "run_c03_contract_calendar_full_update.py": "run_contract_calendar_full_update.py",
    }.items():
        text = text.replace(old, new)
    if path.is_relative_to(lake) or path in {root / "AGENTS.md", root / "03_Futures_Database/AGENTS.md"} or path.is_relative_to(root / "00_draft_collection_02"):
        text = text.replace("b00_sync_notebook_exports.py", "sync_notebook_exports.py")
    if path.is_relative_to(operations):
        for number in range(1, 5):
            text = text.replace(f"B0{number}_ROOT", f"A0{number}_ROOT")
    return text

file_names = subprocess.check_output([
    "rg", "--files", "--hidden", "-g", "!.git/**", "-g", "!05_Old_Projects/**",
    "-g", "!03_Futures_Database/futures_lake/**", "-g", "*.py", "-g", "*.md", "-g", "*.ipynb", "-g", "*.ps1",
], cwd=root, text=True, encoding="utf-8").splitlines()
files = {root / name for name in file_names} | {root / ".env.template", root / ".gitignore"}
edits = {}
for path in sorted(files):
    if immutable(path):
        continue
    # Exported scripts are regenerated from Notebook sources after relocation.
    if path.suffix == ".py" and path.with_suffix(".ipynb").exists():
        continue
    original = path.read_bytes()
    text = original.decode("utf-8")
    if path.suffix == ".ipynb":
        notebook = json.loads(text)
        changed = False
        for cell in notebook["cells"]:
            source = "".join(cell["source"])
            revised = rewrite(source, path)
            if revised != source:
                cell["source"] = revised.splitlines(keepends=True)
                changed = True
        if not changed:
            continue
        revised_bytes = (json.dumps(notebook, ensure_ascii=False, indent=1) + "\n").encode("utf-8")
        nbformat.validate(nbformat.reads(revised_bytes.decode("utf-8"), as_version=4))
    else:
        revised_bytes = rewrite(text, path).encode("utf-8")
    if revised_bytes != original:
        edits[path] = revised_bytes

record_dir.mkdir()
backups = set(edits) | {p for p in lake.rglob("*") if p.is_file() and "__pycache__" not in p.parts}
with zipfile.ZipFile(record_dir / "before.zip", "x", compression=zipfile.ZIP_DEFLATED) as backup:
    for path in sorted(backups):
        backup.write(path, path.relative_to(root).as_posix())
evidence = {}
for folder in (operations / "run_history", operations / "b03_Archived_One_Off_Batches"):
    for path in folder.rglob("*"):
        if path.is_file():
            evidence[relocated(path).relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
schema_hashes = {name: hashlib.sha256(value.serialize().to_pybytes()).hexdigest() for name, value in vars(data_contracts).items() if isinstance(value, pa.Schema)}
record = {
    "changed_sources": [relocated(path).relative_to(root).as_posix() for path in edits],
    "workflow_stems": stems,
    "evidence_sha256": evidence,
    "schema_sha256": schema_hashes,
}
(record_dir / "migration_record.json").write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
for path, content in edits.items():
    path.write_bytes(content)
print(json.dumps({"source_edits":len(edits), "workflows":len(stems), "evidence_files":len(evidence), "backup":str(record_dir / "before.zip")}, ensure_ascii=False))
