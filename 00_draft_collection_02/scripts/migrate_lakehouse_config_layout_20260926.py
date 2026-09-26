"""One-off, source-preserving migration of the approved lakehouse module layout."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import zipfile
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument("--write", action="store_true")
args = parser.parse_args()
root = Path(__file__).resolve().parents[2]
policy_names = (
    "futures_fact_collection_policy",
    "external_market_entities",
    "macro_release_entities",
    "futures_position_rank_special_cases",
)
moves = {
    root / "config" / f"{name}.py": root / "config/futures_lakehouse" / f"{name}.py"
    for name in policy_names
}
moves[root / "config/notebook_schema_browser.py"] = (
    root / "02_Futures_Lakehouse/notebook_schema_browser.py"
)
for source, destination in moves.items():
    assert source.resolve().is_relative_to(root)
    assert destination.resolve().is_relative_to(root)
    assert source.is_file(), source
    assert not destination.exists(), destination


def replace_references(source: str) -> str:
    for name in policy_names:
        for separator in (".", "/", "\\"):
            source = source.replace(
                f"config{separator}{name}",
                f"config{separator}futures_lakehouse{separator}{name}",
            )
    source = source.replace("config.notebook_schema_browser", "notebook_schema_browser")
    for separator in ("/", "\\"):
        source = source.replace(
            f"config{separator}notebook_schema_browser.py",
            f"02_Futures_Lakehouse{separator}notebook_schema_browser.py",
        )
    return source


def notebook_source_spans(text: str):
    """Find cell source JSON spans without reserializing outputs or metadata."""
    decoder = json.JSONDecoder()
    position = text.index('"cells"') + len('"cells"')
    position = text.index("[", position) + 1
    while True:
        while text[position].isspace() or text[position] == ",":
            position += 1
        if text[position] == "]":
            return
        assert text[position] == "{"
        position += 1
        while True:
            while text[position].isspace() or text[position] == ",":
                position += 1
            if text[position] == "}":
                position += 1
                break
            key, position = decoder.raw_decode(text, position)
            position = text.index(":", position) + 1
            while text[position].isspace():
                position += 1
            value_start = position
            value, position = decoder.raw_decode(text, position)
            if key == "source":
                yield value_start, position, value


excluded = (
    ".git", ".idea", "05_Old_Projects", "__pycache__", ".ipynb_checkpoints",
    "control_history", "experiment_records", "supporting_materials", "references",
    "node_modules", ".venv", "b03_Archived_One_Off_Batches",
)
command = ["rg", "--files", "--hidden", "--no-ignore"]
for pattern in ("*.py", "*.md", "*.ipynb", ".env.template"):
    command.extend(["-g", pattern])
for directory in excluded:
    command.extend(["-g", f"!**/{directory}/**"])
paths = subprocess.check_output(command, cwd=root, text=True, encoding="utf-8").splitlines()
changes = {}
notebook_count = 0
browser_count = 0
old_references = re.compile(
    r"config[./\\](?:notebook_schema_browser|" + "|".join(policy_names) + ")"
)
for relative in sorted(paths):
    path = root / relative
    if path == Path(__file__).resolve() or path in moves or path == root / "config/data_contracts.py":
        continue
    before = path.read_bytes()
    if not old_references.search(before.decode("utf-8-sig")):
        continue
    # Paired Python files are regenerated only by PythonExporter.
    if path.suffix == ".py" and path.with_suffix(".ipynb").exists():
        continue
    text = before.decode("utf-8")
    if path.suffix != ".ipynb":
        after = replace_references(text).encode("utf-8")
    else:
        notebook = json.loads(text)
        sources = ["".join(cell.get("source", [])) for cell in notebook["cells"]]
        has_browser = any("config.notebook_schema_browser" in source for source in sources)
        edits = []
        added_paths = 0
        for start, end, value in notebook_source_spans(text):
            source = "".join(value)
            updated = replace_references(source)
            if has_browser:
                updated, inserted = re.subn(
                    r"(?m)^([ \t]*)sys\.path\.insert\(0, str\(candidate_root\)\)(\r?\n)",
                    lambda match: match[0] + match[1]
                    + 'sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))'
                    + match[2],
                    updated,
                )
                added_paths += inserted
            if updated == source:
                continue
            raw = text[start:end]
            if isinstance(value, str):
                replacement = json.dumps(updated, ensure_ascii=False)
            elif "\n" in raw:
                child_indent = re.match(r"\s*", raw.splitlines()[1])[0]
                closing_indent = re.match(r"\s*", raw.splitlines()[-1])[0]
                newline = "\r\n" if "\r\n" in raw else "\n"
                replacement = "[" + newline + ("," + newline).join(
                    child_indent + json.dumps(line, ensure_ascii=False)
                    for line in updated.splitlines(keepends=True)
                ) + newline + closing_indent + "]"
            else:
                replacement = json.dumps(updated.splitlines(keepends=True), ensure_ascii=False)
            edits.append((start, end, replacement))
        assert added_paths == int(has_browser), (path, added_paths)
        for start, end, replacement in reversed(edits):
            text = text[:start] + replacement + text[end:]
        migrated = json.loads(text)
        for old_cell, new_cell in zip(notebook["cells"], migrated["cells"], strict=True):
            assert {key: value for key, value in old_cell.items() if key != "source"} == {
                key: value for key, value in new_cell.items() if key != "source"
            }, path
        assert {key: value for key, value in notebook.items() if key != "cells"} == {
            key: value for key, value in migrated.items() if key != "cells"
        }, path
        after = text.encode("utf-8")
        notebook_count += bool(edits)
        browser_count += has_browser
    if before != after:
        changes[path] = (before, after)

print(json.dumps({
    "moves": {str(a.relative_to(root)): str(b.relative_to(root)) for a, b in moves.items()},
    "changed_files": [str(path.relative_to(root)) for path in changes],
    "notebooks": notebook_count,
    "browser_callers": browser_count,
}, ensure_ascii=False, indent=2))
if not args.write:
    raise SystemExit(0)

backup_path = root / "00_draft_collection_02/lakehouse_config_layout_20260926_before.zip"
assert not backup_path.exists(), backup_path
baseline = {path: before for path, (before, _) in changes.items()}
for path in [*moves, root / "config/settings.py", root / "config/jqdata_connection.py", root / "config/data_contracts.py"]:
    baseline[path] = path.read_bytes()
for path in (root / "02_Futures_Lakehouse/a01_Data_Collection").glob("b*/c*.py"):
    baseline[path] = path.read_bytes()
with zipfile.ZipFile(backup_path, "x", compression=zipfile.ZIP_DEFLATED) as backup:
    for path, before in baseline.items():
        backup.writestr(path.relative_to(root).as_posix(), before)
for path, (before, _) in changes.items():
    assert path.read_bytes() == before, path
for path, (_, after) in changes.items():
    path.write_bytes(after)
(root / "config/futures_lakehouse").mkdir(exist_ok=False)
(root / "config/futures_lakehouse/__init__.py").write_text(
    '"""正式期货湖仓范围、映射与校准规则的共享配置。"""\n', encoding="utf-8"
)
for source, destination in moves.items():
    source.rename(destination)
print("migration_written: True")
