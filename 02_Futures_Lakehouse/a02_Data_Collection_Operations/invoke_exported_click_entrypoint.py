"""Invoke one exported collection Click entrypoint with the remaining CLI args."""

from __future__ import annotations

import pathlib
import runpy
import sys


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


COLLECTION_ROOT = (
    PROJECT_ROOT / "02_Futures_Lakehouse" / "a01_Data_Collection"
).resolve()
ENTRYPOINT_OPTION = "--entrypoint-path"


def selected_entrypoint(argv: list[str]) -> pathlib.Path:
    try:
        option_index = argv.index(ENTRYPOINT_OPTION)
        entrypoint_path = pathlib.Path(argv[option_index + 1]).resolve()
    except (ValueError, IndexError) as error:
        raise ValueError(f"必须提供 {ENTRYPOINT_OPTION} PATH。") from error

    if entrypoint_path.suffix.lower() != ".py" or not entrypoint_path.is_file():
        raise FileNotFoundError(f"采集入口不存在：{entrypoint_path}")
    if not entrypoint_path.is_relative_to(COLLECTION_ROOT):
        raise ValueError("入口必须位于当前正式 a01_Data_Collection。")
    if "c08" in entrypoint_path.stem.lower():
        raise ValueError("c08 不得由 operations invoker 执行。")

    del argv[option_index : option_index + 2]
    return entrypoint_path


def main() -> None:
    entrypoint_path = selected_entrypoint(sys.argv)
    exported_main = runpy.run_path(str(entrypoint_path)).get("main")
    if exported_main is None or not callable(exported_main):
        raise RuntimeError(f"导出入口没有可调用 main：{entrypoint_path}")
    exported_main()


if __name__ == "__main__":
    main()

