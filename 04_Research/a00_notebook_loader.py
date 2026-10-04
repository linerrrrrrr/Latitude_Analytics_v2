"""Load trusted Notebook ``export`` cells into a same-name Python proxy.

The proxy calls ``load_notebook_exports(__file__, globals())``. Its normal
module identity, package, and file path are retained. Only tagged code cells
are compiled and executed; demos and saved outputs are ignored. This is code
loading, not a sandbox: tagged cells must contain import-safe definitions.
"""

from __future__ import annotations

import __future__
import json
import linecache
from pathlib import Path


def load_notebook_exports(proxy_path: str | Path, module_namespace: dict) -> None:
    """Execute ``export`` cells from the proxy's sibling ``.ipynb``.

    All selected cells are validated and compiled before any is executed.
    Future imports affect later exported cells, as in a Notebook kernel;
    unexported cells and the loader's own compiler settings have no effect.
    Diagnostics use the Notebook path and zero-based cell index. Runtime
    failures retain their exception type and may leave prior definitions in
    the namespace, just as a failing ordinary Python module import can.
    """
    notebook_path = Path(proxy_path).resolve().with_suffix(".ipynb")
    try:
        notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"Invalid Notebook JSON in {notebook_path}: {error}") from error
    if not isinstance(notebook, dict) or not isinstance(notebook.get("cells"), list):
        raise ValueError(f"Notebook must contain a cells list: {notebook_path}")

    future_flags_mask = 0
    for feature_name in __future__.all_feature_names:
        future_flags_mask |= getattr(__future__, feature_name).compiler_flag
    future_flags = 0
    compiled_cells = []
    for cell_index, cell in enumerate(notebook["cells"]):
        cell_location = f"{notebook_path}:cell[{cell_index}]"
        if not isinstance(cell, dict):
            raise ValueError(f"Cell must be an object: {cell_location}")
        if cell.get("cell_type") != "code":
            continue
        metadata = cell.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError(f"Cell metadata must be an object: {cell_location}")
        tags = metadata.get("tags", [])
        if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
            raise ValueError(f"Cell tags must be a list of strings: {cell_location}")
        if "export" not in tags:
            continue
        source = cell.get("source")
        if isinstance(source, list) and all(isinstance(line, str) for line in source):
            source = "".join(source)
        if not isinstance(source, str):
            raise ValueError(f"Export source must be text or a list of strings: {cell_location}")
        compiled_code = compile(
            source, cell_location, "exec", flags=future_flags, dont_inherit=True
        )
        future_flags |= compiled_code.co_flags & future_flags_mask
        compiled_cells.append((cell_location, source, compiled_code))

    if not compiled_cells:
        raise ValueError(f"No code cells tagged 'export': {notebook_path}")
    for cell_location, source, compiled_code in compiled_cells:
        # Keep source available to tracebacks and inspect after import finishes.
        linecache.cache[cell_location] = (
            len(source), None, source.splitlines(keepends=True), cell_location
        )
        try:
            exec(compiled_code, module_namespace)
        except Exception as error:
            if callable(getattr(error, "add_note", None)):
                error.add_note(f"While loading Notebook export: {cell_location}")
            raise
