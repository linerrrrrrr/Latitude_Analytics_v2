"""Run the sole formal manual maintenance operation: b01/c03 ``--full``."""

from __future__ import annotations

import argparse
import pathlib
import sys


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")


OPERATIONS_ROOT = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a02_Data_Collection_Operations"
)
sys.path.insert(0, str(OPERATIONS_ROOT))

from background_worker import StageSpec, run_batch  # noqa: E402


COLLECTION_ENTRYPOINT = (
    PROJECT_ROOT
    / "02_Futures_Lakehouse"
    / "a01_Data_Collection"
    / "b01_Futures_Market_Data"
    / "c03_futures_contract_calendar.py"
)
MANUAL_STAGES = (
    StageSpec(
        name="b01/c03_futures_contract_calendar_full",
        entrypoint_path=(
            OPERATIONS_ROOT / "invoke_exported_click_entrypoint.py"
        ),
        arguments=(
            "--entrypoint-path",
            str(COLLECTION_ENTRYPOINT),
            "--full",
            "--write",
        ),
    ),
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="正式执行一次 c03 合约日历全量维护。"
    )
    parser.add_argument("--run-root", type=pathlib.Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_args(argv)
    return run_batch(
        operation_name="c03_contract_calendar_full_maintenance",
        run_root=arguments.run_root,
        stages=MANUAL_STAGES,
    )


if __name__ == "__main__":
    raise SystemExit(main())

