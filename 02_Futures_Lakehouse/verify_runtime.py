"""Verify that commands are using the project's canonical Python runtime."""

from __future__ import annotations

import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path


EXPECTED_ENVIRONMENT = "latitude"
REQUIRED_PACKAGES = ("numpy", "pandas", "polars", "pyarrow", "psutil")


def main() -> None:
    environment_name = Path(sys.prefix).name

    print(f"python: {sys.executable}")
    print(f"environment: {environment_name}")

    missing_packages = []
    for package_name in REQUIRED_PACKAGES:
        try:
            package_version = version(package_name)
            print(f"{package_name}: {package_version}")
        except PackageNotFoundError:
            missing_packages.append(package_name)
            print(f"{package_name}: MISSING")

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
