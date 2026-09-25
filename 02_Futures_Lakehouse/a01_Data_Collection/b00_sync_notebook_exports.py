"""生成并检查四个业务目录中 Notebook 的同名 PythonExporter 脚本。"""

# py文件生成：激活 latitude 环境，在本脚本所在目录运行 python b00_sync_notebook_exports.py --write。
# 仅做检查：运行 python b00_sync_notebook_exports.py --check；不传参数也默认只检查，不修改文件。

from __future__ import annotations

import argparse
import ast
import sys
from dataclasses import dataclass
from pathlib import Path

import nbformat
from nbconvert.exporters import PythonExporter


EXPECTED_ENVIRONMENT = "latitude"
PROJECT_DIR = Path(__file__).resolve().parent
WORKFLOW_DIRS = (
    PROJECT_DIR / "b01_Futures_Market_Data",
    PROJECT_DIR / "b02_Futures_Exchange_Reports",
    PROJECT_DIR / "b03_External_Market_Data",
    PROJECT_DIR / "b04_Macro_And_Interest_Rates",
)


@dataclass(frozen=True)
class ExportArtifact:
    notebook_path: Path
    script_path: Path
    script_source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步或检查 a01 的 19 个业务 Notebook。")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true", help="只检查（默认）。")
    modes.add_argument("--write", action="store_true", help="重新生成同名 .py 后检查。")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if Path(sys.prefix).name.casefold() != EXPECTED_ENVIRONMENT.casefold():
        print(
            f"ERROR: 必须使用 {EXPECTED_ENVIRONMENT!r} Conda 环境；当前为 {sys.executable!r}。",
            file=sys.stderr,
        )
        return 1

    missing_directories = [path for path in WORKFLOW_DIRS if not path.is_dir()]
    if missing_directories:
        for path in missing_directories:
            print(f"ERROR: 缺少正式业务目录 {path}。", file=sys.stderr)
        return 1

    notebooks = sorted(
        path
        for directory in WORKFLOW_DIRS
        for pattern in (
            "c[0-9][0-9]_*.ipynb",
            "c[0-9][0-9][a-z]_*.ipynb",
        )
        for path in directory.glob(pattern)
    )
    scripts = sorted(
        path
        for directory in WORKFLOW_DIRS
        for pattern in (
            "c[0-9][0-9]_*.py",
            "c[0-9][0-9][a-z]_*.py",
        )
        for path in directory.glob(pattern)
    )
    if len(notebooks) != 19:
        print(f"ERROR: 应发现 19 个 Notebook，实际 {len(notebooks)}。", file=sys.stderr)
        return 1

    notebook_keys = {(path.parent, path.stem) for path in notebooks}
    script_keys = {(path.parent, path.stem) for path in scripts}
    for directory, stem in sorted(script_keys - notebook_keys):
        print(f"ERROR: {directory / (stem + '.py')} 缺少同名 Notebook。", file=sys.stderr)
    if script_keys - notebook_keys:
        return 1

    artifacts: list[ExportArtifact] = []
    for notebook_path in notebooks:
        try:
            notebook = nbformat.read(notebook_path, as_version=4)
            nbformat.validate(notebook)
            script_source, _ = PythonExporter().from_notebook_node(notebook)
            ast.parse(script_source, filename=str(notebook_path.with_suffix(".py")))
            artifacts.append(
                ExportArtifact(notebook_path, notebook_path.with_suffix(".py"), script_source)
            )
        except Exception as error:
            print(
                f"ERROR: {notebook_path}: {type(error).__name__}: {error}",
                file=sys.stderr,
            )
            return 1

    if args.write:
        for artifact in artifacts:
            artifact.script_path.write_text(
                artifact.script_source,
                encoding="utf-8",
                newline="\n",
            )

    failed = False
    for artifact in artifacts:
        if not artifact.script_path.exists():
            failed = True
            print(f"ERROR: 缺少 {artifact.script_path}。", file=sys.stderr)
        elif artifact.script_path.read_bytes() != artifact.script_source.encode("utf-8"):
            failed = True
            print(f"ERROR: {artifact.script_path} 与默认导出不一致。", file=sys.stderr)
        else:
            print(f"OK: {artifact.notebook_path.relative_to(PROJECT_DIR)}")

    if failed:
        return 1
    mode = "write" if args.write else "check"
    print(f"{mode}_ok: True; workflow_count={len(artifacts)}; python={sys.executable}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
