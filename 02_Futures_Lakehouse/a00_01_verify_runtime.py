"""Verify that commands are using the project's canonical Python runtime."""

from __future__ import annotations

import json
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


EXPECTED_ENVIRONMENT = "latitude"
REQUIRED_PACKAGES = ("numpy", "pandas", "polars", "pyarrow", "psutil")


def main() -> None:
    environment_name = Path(sys.prefix).name

    print(f"python: {sys.executable}")
    print(f"environment: {environment_name}")

    environment_matches = environment_name.casefold() == EXPECTED_ENVIRONMENT.casefold()
    print("progress_event: " + json.dumps({
        "event_version": 1, "event": "phase", "scope_id": "environment", "phase": "environment",
        "state": "completed" if environment_matches else "failed",
        "object": {"python": sys.executable, "environment": environment_name},
        "counters": {"checks": {"completed": 1, "total": 1, "unit": "项环境检查"}},
    }, ensure_ascii=False), flush=True)

    missing_packages = []
    for package_index, package_name in enumerate(REQUIRED_PACKAGES):
        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "progress", "scope_id": "packages", "phase": "packages", "state": "running",
            "object": {"package": package_name},
            "counters": {"checks": {"completed": package_index, "total": len(REQUIRED_PACKAGES), "unit": "项依赖检查"}},
        }, ensure_ascii=False), flush=True)
        try:
            package_version = version(package_name)
            print(f"{package_name}: {package_version}")
        except PackageNotFoundError:
            missing_packages.append(package_name)
            package_version = "MISSING"
            print(f"{package_name}: MISSING")
        print("progress_event: " + json.dumps({
            "event_version": 1, "event": "phase", "scope_id": f"packages:{package_name}", "parent_scope_id": "packages",
            "phase": "packages", "state": "failed" if package_name in missing_packages else "completed",
            "object": {"package": package_name, "version": package_version},
        }, ensure_ascii=False), flush=True)

    print("progress_event: " + json.dumps({
        "event_version": 1, "event": "phase", "scope_id": "packages", "phase": "packages",
        "state": "failed" if missing_packages else "completed",
        "counters": {"checks": {"completed": len(REQUIRED_PACKAGES), "total": len(REQUIRED_PACKAGES), "unit": "项依赖检查"}},
    }, ensure_ascii=False), flush=True)

    if environment_name.casefold() != EXPECTED_ENVIRONMENT.casefold():
        raise SystemExit(
            f"Wrong environment: expected {EXPECTED_ENVIRONMENT!r}, "
            f"got {environment_name!r}."
        )
    if missing_packages:
        raise SystemExit(f"Missing packages: {', '.join(missing_packages)}")

    print("runtime_ok: True")


if __name__ == "__main__":
    main()
