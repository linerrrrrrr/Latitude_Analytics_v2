"""Read-only verification of the approved lakehouse directory migration."""
import ast
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.parse import unquote
import zipfile

import pyarrow as pa

root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root))
from config import data_contracts

record_dir = root / "00_draft_collection_02/lakehouse_layout_20260926"
record = json.loads((record_dir / "migration_record.json").read_text(encoding="utf-8"))
lake = root / "02_Futures_Lakehouse"
assert not (lake / "a01_Data_Collection").exists()
assert not (lake / "a02_Data_Collection_Operations").exists()

for relative, expected in record["evidence_sha256"].items():
    assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected, relative
schema_hashes = {
    name: hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
    for name, schema in vars(data_contracts).items() if isinstance(schema, pa.Schema)
}
assert schema_hashes == record["schema_sha256"]

notebook_results = []
missing_links = []
checked_links = 0
with zipfile.ZipFile(record_dir / "before.zip") as backup:
    for notebook_path in sorted(lake.glob("a*/b*.ipynb")):
        old_group = "b" + notebook_path.parent.name[1:]
        old_stem = "c" + notebook_path.stem[1:]
        old_prefix = f"02_Futures_Lakehouse/a01_Data_Collection/{old_group}/{old_stem}"
        before = json.loads(backup.read(old_prefix + ".ipynb"))
        after = json.loads(notebook_path.read_text(encoding="utf-8"))
        assert len(before["cells"]) == len(after["cells"]), notebook_path
        for old_cell, new_cell in zip(before["cells"], after["cells"], strict=True):
            assert {k: v for k, v in old_cell.items() if k != "source"} == {
                k: v for k, v in new_cell.items() if k != "source"
            }, notebook_path
        assert before["metadata"] == after["metadata"], notebook_path
        old_tree = ast.parse(backup.read(old_prefix + ".py").decode("utf-8"))
        new_tree = ast.parse(notebook_path.with_suffix(".py").read_text(encoding="utf-8"))
        old_constants = [n for n in ast.walk(old_tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        new_constants = [n for n in ast.walk(new_tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        assert len(old_constants) == len(new_constants), notebook_path
        changes = [{"before": a.value, "after": b.value} for a, b in zip(old_constants, new_constants, strict=True) if a.value != b.value]
        for node in old_constants + new_constants:
            node.value = "<text>"
        assert ast.dump(old_tree) == ast.dump(new_tree), f"Non-text business logic changed: {notebook_path}"
        notebook_results.append({"path": str(notebook_path.relative_to(root)), "changed_text_constants": changes})

    for relative in record["changed_sources"]:
        path = root / relative
        if path.suffix == ".md":
            source = path.read_text(encoding="utf-8")
        elif path.suffix == ".ipynb":
            notebook = json.loads(path.read_text(encoding="utf-8"))
            source = "\n".join("".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "markdown")
        else:
            continue
        for reference in re.findall(r"\]\(([^\n)]+)\)", source):
            if "://" in reference or reference.startswith(("#", "mailto:")):
                continue
            target = unquote(reference.partition("#")[0].strip("<>"))
            checked_links += 1
            if not (path.parent / target).resolve().exists():
                missing_links.append({"source": relative, "target": target})

assert len(notebook_results) == 19
report = {
    "historical_files_unchanged": len(record["evidence_sha256"]),
    "schemas_unchanged": len(schema_hashes),
    "business_logic_unchanged": len(notebook_results),
    "notebook_metadata_and_outputs_unchanged": len(notebook_results),
    "checked_local_links": checked_links,
    "missing_local_links": missing_links,
    "notebook_text_changes": notebook_results,
}
(record_dir / "audit_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps({k: v for k, v in report.items() if k != "notebook_text_changes"}, ensure_ascii=False, indent=2))
