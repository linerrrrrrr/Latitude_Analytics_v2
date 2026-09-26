"""执行一次 a01/b03 正式湖全量刷新，并复用安全 worker/monitor 控制面。"""

from __future__ import annotations

import argparse
import pathlib

import run_b01_b04_full_update as worker


worker.STAGES = (
    (
        "a01/b03_futures_contract_calendar_full",
        pathlib.Path(__file__).with_name(
            "invoke_exported_click_entrypoint.py"
        ),
        False,
        (
            "--entrypoint-path",
            str(worker.B01_ROOT / "b03_futures_contract_calendar.py"),
            "--full",
        ),
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-root", type=pathlib.Path, required=True)
    args = parser.parse_args()
    return worker.run_job("formal", args.run_root, None, None)


if __name__ == "__main__":
    raise SystemExit(main())
