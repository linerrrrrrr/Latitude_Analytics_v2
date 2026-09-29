"""生成并检查四个业务目录中 Notebook 的同名 PythonExporter 脚本。"""

# py文件生成：激活 latitude 环境，在本脚本所在目录运行 python a00_02_sync_notebook_exports.py --write。
# 仅做检查：运行 python a00_02_sync_notebook_exports.py --check；不传参数也默认只检查，不修改文件。
# 运行前正文检查：--check --check-level code；注释差异只警告，--write 始终完整导出和逐字节复核。

from __future__ import annotations

import argparse
import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import nbformat
from nbconvert.exporters import PythonExporter


EXPECTED_ENVIRONMENT = "latitude"
PROJECT_DIR = Path(__file__).resolve().parent
WORKFLOW_DIRS = (
    PROJECT_DIR / "a01_Futures_Market_Data",
    PROJECT_DIR / "a02_Futures_Exchange_Reports",
    PROJECT_DIR / "a03_External_Market_Data",
    PROJECT_DIR / "a04_Macro_And_Interest_Rates",
)


@dataclass(frozen=True)
class ExportArtifact:
    notebook_path: Path
    script_path: Path
    script_source: str
    script_ast: ast.Module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步或检查 a01— a04 的 19 个业务 Notebook。")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true", help="只检查（默认）。")
    modes.add_argument("--write", action="store_true", help="重新生成同名 .py 后检查。")
    parser.add_argument(
        "--check-level",
        choices=("full", "code"),
        default="full",
        help="full 逐字节检查（默认）；code 检查 Python 正文 AST，注释/格式差异只警告。--write 始终使用 full。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    check_level = "full" if args.write else args.check_level
    if args.write and args.check_level != "full":
        print("INFO: --write 始终完整导出并按 full 逐字节复核；本次不使用 code 检查级别。", flush=True)
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
            "b[0-9][0-9]_*.ipynb",
            "b[0-9][0-9][a-z]_*.ipynb",
        )
        for path in directory.glob(pattern)
    )
    scripts = sorted(
        path
        for directory in WORKFLOW_DIRS
        for pattern in (
            "b[0-9][0-9]_*.py",
            "b[0-9][0-9][a-z]_*.py",
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

    mode = "write" if args.write else "check"
    print(
        f"export_check_start: mode={mode}; check_level={check_level}; "
        f"workflow_count={len(notebooks)}; python={sys.executable}",
        flush=True,
    )
    artifacts: list[ExportArtifact] = []
    export_error_count = 0
    for notebook_index, notebook_path in enumerate(notebooks, start=1):
        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "progress", "scope_id": "export", "phase": "export", "state": "running",
            "object": {"notebook": str(notebook_path.relative_to(PROJECT_DIR))},
            "counters": {"notebooks": {"completed": notebook_index - 1, "total": len(notebooks), "unit": "份 Notebook"}},
        }, ensure_ascii=False), flush=True)
        print(
            f"[{notebook_index}/{len(notebooks)}] 导出并验证 Notebook："
            f"{notebook_path.relative_to(PROJECT_DIR)}",
            flush=True,
        )
        export_error = ""
        try:
            notebook = nbformat.read(notebook_path, as_version=4)
            nbformat.validate(notebook)
            script_source, _ = PythonExporter().from_notebook_node(notebook)
            script_path = notebook_path.with_suffix(".py")
            script_ast = ast.parse(script_source, filename=str(script_path))
            compile(script_ast, str(script_path), "exec")
            artifacts.append(
                ExportArtifact(notebook_path, script_path, script_source, script_ast)
            )
        except Exception as error:
            export_error_count += 1
            export_error = f"{type(error).__name__}: {error}"
            print(
                f"ERROR: {notebook_path}: {type(error).__name__}: {error}",
                file=sys.stderr,
                flush=True,
            )

        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "phase", "scope_id": "export:" + str(notebook_path.relative_to(PROJECT_DIR)),
            "parent_scope_id": "export", "phase": "export", "state": "failed" if export_error else "completed",
            "object": {"notebook": str(notebook_path.relative_to(PROJECT_DIR))}, "message": export_error[:2048],
        }, ensure_ascii=False), flush=True)

    print("progress_event: " + json.dumps({
        "event_version": 1, "event": "phase", "scope_id": "export", "phase": "export",
        "state": "failed" if export_error_count else "completed",
        "counters": {"notebooks": {"completed": len(notebooks), "total": len(notebooks), "unit": "份已处理 Notebook"}},
    }, ensure_ascii=False), flush=True)
    # 任何 Notebook 无法生成合法脚本时，整批不写入，避免只同步一部分入口。
    if export_error_count:
        print(
            f"{mode}_ok: False; check_level={check_level}; workflow_count={len(notebooks)}; "
            f"exported={len(artifacts)}; checked=0; passed=0; warnings=0; failed={export_error_count}; "
            f"python={sys.executable}",
            flush=True,
        )
        return 1

    if args.write:
        for artifact_index, artifact in enumerate(artifacts, start=1):
            print("progress_event: " + json.dumps({
                "event_version": 1, "event": "progress", "scope_id": "write", "phase": "write", "state": "running",
                "object": {"script": str(artifact.script_path.relative_to(PROJECT_DIR))},
                "counters": {"scripts": {"completed": artifact_index - 1, "total": len(artifacts), "unit": "份导出"}},
            }, ensure_ascii=False), flush=True)
            print(
                f"[{artifact_index}/{len(artifacts)}] 写入完整导出："
                f"{artifact.script_path.relative_to(PROJECT_DIR)}",
                flush=True,
            )
            write_error = ""
            try:
                artifact.script_path.write_text(
                    artifact.script_source,
                    encoding="utf-8",
                    newline="\n",
                )
            except Exception as error:
                write_error = f"{type(error).__name__}: {error}"
                raise
            finally:
                print("progress_event: " + json.dumps({
                    "event_version": 1, "event": "phase", "scope_id": "write:" + str(artifact.notebook_path.relative_to(PROJECT_DIR)),
                    "parent_scope_id": "write", "phase": "write", "state": "failed" if write_error else "completed",
                    "object": {"notebook": str(artifact.notebook_path.relative_to(PROJECT_DIR))}, "message": write_error[:2048],
                }, ensure_ascii=False), flush=True)
        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "phase", "scope_id": "write", "phase": "write", "state": "completed",
            "counters": {"scripts": {"completed": len(artifacts), "total": len(artifacts), "unit": "份导出"}},
        }, ensure_ascii=False), flush=True)

    passed_count = 0
    warning_count = 0
    failed_count = 0
    for artifact_index, artifact in enumerate(artifacts, start=1):
        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "progress", "scope_id": "check", "phase": "check", "state": "running",
            "object": {"script": str(artifact.script_path.relative_to(PROJECT_DIR)), "check_level": check_level},
            "counters": {"scripts": {"completed": artifact_index - 1, "total": len(artifacts), "unit": "份已检查脚本"}},
        }, ensure_ascii=False), flush=True)
        print(
            f"[{artifact_index}/{len(artifacts)}] 检查 {check_level}："
            f"{artifact.script_path.relative_to(PROJECT_DIR)}",
            flush=True,
        )
        check_error = ""
        check_warning = False
        try:
            if not artifact.script_path.is_file():
                raise FileNotFoundError(f"缺少 {artifact.script_path}。")
            existing_script_bytes = artifact.script_path.read_bytes()
            full_matches = existing_script_bytes == artifact.script_source.encode("utf-8")
            if check_level == "code":
                # ast 不含注释；忽略行列位置，但保留 docstring、字面值及全部语句结构。
                existing_script_ast = ast.parse(
                    existing_script_bytes, filename=str(artifact.script_path)
                )
                compile(existing_script_ast, str(artifact.script_path), "exec")
                if ast.dump(existing_script_ast, include_attributes=False) != ast.dump(
                    artifact.script_ast, include_attributes=False
                ):
                    raise ValueError("Python 正文 AST 与默认导出不一致。")
                if not full_matches:
                    warning_count += 1
                    check_warning = True
                    print(
                        f"WARN: {artifact.notebook_path.relative_to(PROJECT_DIR)} "
                        "正文 AST 一致，完整导出存在注释/格式等非正文差异；code 检查通过，尚未完整同步。",
                        flush=True,
                    )
            elif not full_matches:
                raise ValueError("与默认 PythonExporter 完整导出不一致。")
            passed_count += 1
            print(f"OK: {artifact.notebook_path.relative_to(PROJECT_DIR)}", flush=True)
        except Exception as error:
            failed_count += 1
            check_error = f"{type(error).__name__}: {error}"
            print(
                f"ERROR: {artifact.script_path}: {type(error).__name__}: {error}",
                file=sys.stderr,
                flush=True,
            )

        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "phase", "scope_id": "check:" + str(artifact.notebook_path.relative_to(PROJECT_DIR)),
            "parent_scope_id": "check", "phase": "check", "state": "failed" if check_error else "completed",
            "outcome": "warning" if check_warning else "failed" if check_error else "passed",
            "object": {"notebook": str(artifact.notebook_path.relative_to(PROJECT_DIR))}, "message": check_error[:2048],
        }, ensure_ascii=False), flush=True)

    print("progress_event: " + json.dumps({
        "event_version": 1, "event": "phase", "scope_id": "check", "phase": "check",
        "state": "failed" if failed_count else "completed",
        "object": {"check_level": check_level, "passed": passed_count, "warnings": warning_count, "failed": failed_count},
        "counters": {"scripts": {"completed": len(artifacts), "total": len(artifacts), "unit": "份已检查脚本"}},
    }, ensure_ascii=False), flush=True)
    print(
        f"{mode}_ok: {failed_count == 0}; check_level={check_level}; "
        f"workflow_count={len(artifacts)}; checked={len(artifacts)}; "
        f"passed={passed_count}; warnings={warning_count}; failed={failed_count}; python={sys.executable}",
        flush=True,
    )
    return int(failed_count != 0)


if __name__ == "__main__":
    raise SystemExit(main())
