"""Invoke one exported collection Click entrypoint with the remaining CLI args."""

from __future__ import annotations

import pathlib
import runpy
import sys
import ast
import hashlib
from dataclasses import dataclass

import click


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


COLLECTION_ROOT = (
    PROJECT_ROOT / "02_Market_Data/a01_Collection"
).resolve()
COLLECTION_DIRS = tuple(
    COLLECTION_ROOT / name
    for name in (
        "b01_Futures_Market_Data", "b02_Futures_Exchange_Reports",
        "b03_External_Market_Data", "b04_Macro_And_Interest_Rates",
    )
)
ENTRYPOINT_OPTION = "--entrypoint-path"


@dataclass(frozen=True)
class EntryPointContract:
    """The entrypoint's own CLI and introductory notes, read without importing it."""

    path: pathlib.Path
    name: str
    sha256: str
    command: click.Command
    notes: str


def read_cli_contract(entrypoint_path: pathlib.Path) -> EntryPointContract:
    """Read only literal Click declarations; never execute business module code."""
    entrypoint_path = entrypoint_path.resolve()
    if entrypoint_path.parent not in COLLECTION_DIRS or not entrypoint_path.with_suffix(".ipynb").is_file():
        raise ValueError("只能读取 b01—b04 中的正式双轨业务入口。")
    source_bytes = entrypoint_path.read_bytes()
    source = source_bytes.decode("utf-8-sig")
    module = ast.parse(source, filename=str(entrypoint_path))
    declarations = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "main"]
    if len(declarations) != 1:
        raise ValueError(f"{entrypoint_path.name}: 必须只有一个 main。")
    constants = {}
    for node in module.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    constants[target.id] = value

    def declaration_value(node):
        # This deliberately supports a small, data-only grammar. New callbacks
        # or dynamic defaults must never start running while browsing the UI.
        if isinstance(node, ast.Name):
            if node.id in constants:
                return constants[node.id]
            if node.id in {"str", "int", "float", "bool"}:
                return {"str": str, "int": int, "float": float, "bool": bool}[node.id]
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id == "pathlib" and node.attr == "Path":
                return pathlib.Path
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.value.id == "click" and node.func.attr in {"Path", "DateTime", "IntRange", "FloatRange", "Choice"}:
                if any(keyword.arg is None for keyword in node.keywords):
                    raise ValueError("CLI 声明不支持动态关键字展开。")
                return getattr(click, node.func.attr)(
                    *(declaration_value(argument) for argument in node.args),
                    **{keyword.arg: declaration_value(keyword.value) for keyword in node.keywords},
                )
        try:
            return ast.literal_eval(node)
        except (ValueError, TypeError) as error:
            raise ValueError(f"CLI 含动态声明，无法只读解析：{ast.unparse(node)}") from error

    def never_execute(**parameters):
        raise RuntimeError("只读 CLI 元数据不得执行业务。")

    command = never_execute
    for decorator in reversed(declarations[0].decorator_list):
        if not (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute)
                and isinstance(decorator.func.value, ast.Name) and decorator.func.value.id == "click"
                and decorator.func.attr in {"option", "command"}):
            raise ValueError("main 含不支持的动态装饰器。")
        if any(keyword.arg in {None, "callback", "cls", "context_settings", "envvar", "prompt"} for keyword in decorator.keywords):
            raise ValueError("CLI 元数据禁止执行回调或读取动态输入。")
        command = getattr(click, decorator.func.attr)(
            *(declaration_value(argument) for argument in decorator.args),
            **{keyword.arg: declaration_value(keyword.value) for keyword in decorator.keywords},
        )(command)
    if not isinstance(command, click.Command):
        raise ValueError("main 没有 Click command 声明。")
    if any(not isinstance(option, click.Option) or option.nargs != 1 or (option.is_flag and not option.is_bool_flag) for option in command.params):
        raise ValueError("入口含尚未支持的 CLI 参数形式。")

    notes = []
    in_diagram = False
    for line in source.splitlines():
        if line.startswith("from ") or line.startswith("import "):
            break
        if not line.startswith("#") or line.startswith(("#!", "# coding:", "# In[")):
            continue
        text = line[1:].lstrip()
        if text.startswith("```"):
            in_diagram = not in_diagram
            continue
        if not in_diagram and not text.startswith("%%"):
            notes.append(text)
    return EntryPointContract(
        path=entrypoint_path,
        name=entrypoint_path.parent.name[:3] + "/" + entrypoint_path.stem,
        sha256=hashlib.sha256(source_bytes).hexdigest(),
        command=command,
        notes="\n".join(notes).strip(),
    )


def selected_entrypoint(argv: list[str]) -> pathlib.Path:
    try:
        option_index = argv.index(ENTRYPOINT_OPTION)
        entrypoint_path = pathlib.Path(argv[option_index + 1]).resolve()
    except (ValueError, IndexError) as error:
        raise ValueError(f"必须提供 {ENTRYPOINT_OPTION} PATH。") from error

    if entrypoint_path.suffix.lower() != ".py" or not entrypoint_path.is_file():
        raise FileNotFoundError(f"采集入口不存在：{entrypoint_path}")
    if entrypoint_path.parent not in COLLECTION_DIRS or not entrypoint_path.with_suffix(".ipynb").is_file():
        raise ValueError("入口必须是 b01— b04 正式业务目录内的 Notebook 导出脚本。")
    del argv[option_index : option_index + 2]
    return entrypoint_path


def main() -> None:
    entrypoint_path = selected_entrypoint(sys.argv)
    if "--source-sha256" in sys.argv:
        index = sys.argv.index("--source-sha256")
        expected_sha256 = sys.argv[index + 1]
        del sys.argv[index:index + 2]
        if hashlib.sha256(entrypoint_path.read_bytes()).hexdigest() != expected_sha256:
            raise RuntimeError("业务文件在批次确认后发生变化，请重新检查并启动新批次。")
    exported_main = runpy.run_path(str(entrypoint_path)).get("main")
    if exported_main is None or not callable(exported_main):
        raise RuntimeError(f"导出入口没有可调用 main：{entrypoint_path}")
    exported_main()


if __name__ == "__main__":
    main()
