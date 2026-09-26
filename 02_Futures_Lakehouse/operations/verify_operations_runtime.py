"""Probe Windows control-plane I/O without touching any business lake data."""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


OPERATIONS_ROOT = pathlib.Path(__file__).resolve().parent
POWERSHELL_SHARE_READ_SCRIPT = r"""
$StatusPath = $env:LATITUDE_OPERATIONS_STATUS_PATH
$share = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
$stream = [System.IO.File]::Open(
    $StatusPath,
    [System.IO.FileMode]::Open,
    [System.IO.FileAccess]::Read,
    $share
)
$reader = [System.IO.StreamReader]::new(
    $stream,
    [System.Text.UTF8Encoding]::new($false),
    $true,
    4096,
    $false
)
try {
    [Console]::Out.WriteLine(('SHARE ' + [int]$share))
    [Console]::Out.Flush()
    [Console]::Out.Write($reader.ReadToEnd())
    [Console]::Out.Flush()
}
finally {
    $reader.Dispose()
}
"""


def main() -> None:
    print(f"python: {sys.executable}")
    if pathlib.Path(sys.prefix).name.casefold() != "latitude":
        raise RuntimeError("verify_operations_runtime 必须使用 latitude 环境。")

    with tempfile.TemporaryDirectory(
        prefix=".operations_runtime_",
        dir=OPERATIONS_ROOT,
    ) as temporary_directory:
        probe_root = pathlib.Path(temporary_directory)
        long_directory = probe_root
        segment_index = 0
        while len(str(long_directory / "status.json")) <= 300:
            segment_index += 1
            long_directory /= (
                f"segment_{segment_index:02d}_" + ("x" * 32)
            )
        long_directory.mkdir(parents=True)
        unicode_space_directory = long_directory / "中文 空格"
        unicode_space_directory.mkdir()

        status_path = unicode_space_directory / "status.json"
        temporary_status_path = status_path.with_name(
            f".{status_path.name}.{uuid.uuid4().hex}.tmp"
        )
        expected_status = {
            "protocol_version": 1,
            "operation_name": "runtime_probe",
            "phase": "probe",
        }
        expected_status_text = json.dumps(
            expected_status,
            ensure_ascii=False,
        )
        with temporary_status_path.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            handle.write(expected_status_text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_status_path, status_path)
        if len(str(status_path)) <= 260:
            raise AssertionError("长路径 probe 未超过 260 字符。")
        if status_path.read_text(encoding="utf-8") != expected_status_text:
            raise AssertionError("Python 长路径原子替换复读不一致。")
        print(f"python_atomic_replace_long_path_ok: {len(str(status_path))}")

        powershell_environment = os.environ.copy()
        powershell_environment["LATITUDE_OPERATIONS_STATUS_PATH"] = str(
            status_path
        )
        powershell_result = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                POWERSHELL_SHARE_READ_SCRIPT,
            ],
            cwd=OPERATIONS_ROOT,
            env=powershell_environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if powershell_result.returncode != 0:
            raise RuntimeError(
                "PowerShell FileShare probe 失败："
                + powershell_result.stderr.strip()
            )
        share_line, separator, powershell_status_text = (
            powershell_result.stdout.partition("\n")
        )
        if share_line.strip() != "SHARE 7" or not separator:
            raise AssertionError("PowerShell 未使用 FileShare 数值 7。")
        if powershell_status_text != expected_status_text:
            raise AssertionError("PowerShell 共享读取未复读完整状态文件。")

        replacement_status = {
            **expected_status,
            "phase": "replaced_after_shared_read",
        }
        replacement_status_text = json.dumps(
            replacement_status,
            ensure_ascii=False,
        )
        second_temporary_status_path = status_path.with_name(
            f".{status_path.name}.{uuid.uuid4().hex}.tmp"
        )
        with second_temporary_status_path.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            handle.write(replacement_status_text)
            handle.flush()
            os.fsync(handle.fileno())
        for replace_attempt in range(1, 51):
            try:
                os.replace(second_temporary_status_path, status_path)
                break
            except PermissionError as error:
                if (
                    getattr(error, "winerror", None) not in {5, 32}
                    or replace_attempt == 50
                ):
                    raise
                time.sleep(0.1)

        if status_path.read_text(encoding="utf-8") != replacement_status_text:
            raise AssertionError(
                "PowerShell 释放共享句柄后 Python os.replace 未成功。"
            )
        print("powershell_fileshare_readwrite_delete_ok: True")

        parquet_path = unicode_space_directory / "round trip 中文.parquet"
        expected_table = pa.table(
            {
                "probe_id": pa.array([1, 2], type=pa.int64()),
                "probe_text": pa.array(["alpha", "中文 beta"], type=pa.string()),
            }
        )
        pq.write_table(expected_table, parquet_path)
        actual_table = pq.read_table(parquet_path)
        if len(str(parquet_path)) <= 260:
            raise AssertionError("PyArrow probe 路径未超过 260 字符。")
        if not actual_table.equals(expected_table):
            raise AssertionError("PyArrow Parquet round-trip 不一致。")
        print(f"pyarrow_long_unicode_space_round_trip_ok: {len(str(parquet_path))}")

    print("operations_runtime_ok: True")


if __name__ == "__main__":
    main()
