"""显式调用导出脚本中的 Click ``main``，并保留其余 CLI 参数。"""

from __future__ import annotations

import pathlib
import runpy
import sys


ENTRYPOINT_OPTION = "--entrypoint-path"


if __name__ == "__main__":
    try:
        option_index = sys.argv.index(ENTRYPOINT_OPTION)
        entrypoint_path = pathlib.Path(sys.argv[option_index + 1]).resolve()
    except (ValueError, IndexError) as error:
        raise SystemExit(f"必须提供 {ENTRYPOINT_OPTION} PATH。") from error

    del sys.argv[option_index : option_index + 2]
    runpy.run_path(str(entrypoint_path))["main"]()
