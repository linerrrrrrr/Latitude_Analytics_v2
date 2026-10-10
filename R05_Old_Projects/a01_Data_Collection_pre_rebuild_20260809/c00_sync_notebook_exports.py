"""生成并检查本目录业务 Notebook 的同名 PythonExporter 脚本。"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import nbformat
from nbconvert.exporters import PythonExporter


EXPECTED_ENVIRONMENT = "latitude"
WORKFLOW_DIR = Path(__file__).resolve().parent
WORKFLOW_PATTERN = "c[0-9][0-9]_*.ipynb"


@dataclass(frozen=True)
class ExportArtifact:
    """一个已校验 Notebook 及其默认 PythonExporter 输出。"""

    notebook_path: Path
    script_path: Path
    script_source: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "同步并检查 a01_Data_Collection 中 c01 起始的业务 Notebook 与同名脚本。"
        )
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--check",
        action="store_true",
        help="只检查，不写文件（默认）。",
    )
    mode_group.add_argument(
        "--write",
        action="store_true",
        help="使用默认 PythonExporter 重新生成脚本，然后执行全部检查。",
    )
    return parser.parse_args()


def ensure_standard_runtime() -> None:
    environment_name = Path(sys.prefix).name
    if environment_name.casefold() != EXPECTED_ENVIRONMENT.casefold():
        raise RuntimeError(
            f"必须使用 {EXPECTED_ENVIRONMENT!r} Conda 环境，"
            f"当前解释器为 {sys.executable!r}。"
        )


def discover_workflows() -> tuple[list[Path], list[Path]]:
    """只发现当前目录的 c01+ 工作流；不递归进入任何其他目录。"""
    notebook_paths = sorted(
        path
        for path in WORKFLOW_DIR.glob(WORKFLOW_PATTERN)
        if not path.stem.startswith("c00_")
    )
    script_paths = sorted(
        path
        for path in WORKFLOW_DIR.glob("c[0-9][0-9]_*.py")
        if not path.stem.startswith("c00_")
    )
    return notebook_paths, script_paths


def build_artifact(notebook_path: Path) -> ExportArtifact:
    notebook = nbformat.read(str(notebook_path), as_version=4)
    nbformat.validate(notebook)
    script_source, _ = PythonExporter().from_notebook_node(notebook)
    script_path = notebook_path.with_suffix(".py")
    compile(script_source, str(script_path), "exec")
    return ExportArtifact(notebook_path, script_path, script_source)


def main() -> int:
    args = parse_args()
    mode = "write" if args.write else "check"

    try:
        ensure_standard_runtime()
    except RuntimeError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    notebook_paths, script_paths = discover_workflows()
    if not notebook_paths:
        print(f"ERROR: {WORKFLOW_DIR} 中没有发现业务 Notebook。", file=sys.stderr)
        return 1

    notebook_stems = {path.stem for path in notebook_paths}
    script_stems = {path.stem for path in script_paths}
    script_only_stems = sorted(script_stems - notebook_stems)
    if script_only_stems:
        for stem in script_only_stems:
            print(f"ERROR: {stem}.py 缺少同名 Notebook。", file=sys.stderr)
        return 1

    artifacts: list[ExportArtifact] = []
    has_error = False
    for notebook_path in notebook_paths:
        try:
            artifacts.append(build_artifact(notebook_path))
        except Exception as error:
            has_error = True
            print(
                f"ERROR: {notebook_path.name}: {type(error).__name__}: {error}",
                file=sys.stderr,
            )
    if has_error:
        return 1

    if mode == "write":
        for artifact in artifacts:
            artifact.script_path.write_text(
                artifact.script_source,
                encoding="utf-8",
                newline="\n",
            )

    for artifact in artifacts:
        if not artifact.script_path.exists():
            has_error = True
            print(
                f"ERROR: {artifact.notebook_path.name} 缺少同名脚本。",
                file=sys.stderr,
            )
            continue
        if artifact.script_path.read_bytes() != artifact.script_source.encode("utf-8"):
            has_error = True
            print(
                f"ERROR: {artifact.script_path.name} 与默认 PythonExporter 输出不一致。",
                file=sys.stderr,
            )
            continue
        print(
            f"OK: {artifact.notebook_path.name} -> {artifact.script_path.name} "
            "(notebook/export/syntax/bytes)"
        )

    if has_error:
        return 1

    print(
        f"{mode}_ok: True; workflow_count={len(artifacts)}; "
        f"python={sys.executable}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
