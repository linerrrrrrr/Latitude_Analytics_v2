from __future__ import annotations

import hashlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone


OPERATIONS_ROOT = pathlib.Path(__file__).resolve().parents[1]
PROJECT_ROOT = OPERATIONS_ROOT.parents[1]
MONITOR_PATH = OPERATIONS_ROOT / "watch_batch.ps1"
RUNTIME_PROBE_PATH = OPERATIONS_ROOT / "verify_operations_runtime.py"
ARCHIVE_ROOT = OPERATIONS_ROOT / "b03_Archived_One_Off_Batches"
MANIFEST_PATH = ARCHIVE_ROOT / "snapshot_manifest.json"


class MonitorRuntimeAndSnapshotTests(unittest.TestCase):
    def run_monitor_once(self, run_root: pathlib.Path) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "pwsh.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(MONITOR_PATH),
                "-RunRoot",
                str(run_root),
                "-Once",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
            check=False,
        )

    def directory_fingerprint(
        self,
        directory: pathlib.Path,
    ) -> dict[str, tuple[bytes, int]]:
        return {
            str(path.relative_to(directory)): (
                path.read_bytes(),
                path.stat().st_mtime_ns,
            )
            for path in directory.rglob("*")
            if path.is_file()
        }

    def base_status(self) -> dict[str, object]:
        now = datetime.now(timezone.utc).isoformat()
        return {
            "mode": "formal",
            "state": "succeeded",
            "started_at": now,
            "finished_at": now,
            "heartbeat_at": now,
            "worker_pid": os.getpid(),
            "child_pid": None,
            "monitor_pid": None,
            "stage_index": 18,
            "stage_total": 18,
            "stage_name": "全部阶段完成",
            "stage_started_at": now,
            "stage_exit_code": 0,
            "progress": "stage=18/18",
            "last_line": "done",
            "recent_lines": ["done"],
            "error": None,
        }

    def test_monitor_once_does_not_create_missing_run_root(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            missing_run_root = pathlib.Path(temporary_directory) / "missing"
            result = self.run_monitor_once(missing_run_root)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(missing_run_root.exists())

    def test_monitor_once_renders_new_and_legacy_protocol_without_writes(self) -> None:
        status_cases = []
        new_status = {
            **self.base_status(),
            "protocol_version": 1,
            "operation_name": "daily_collection_update",
            "phase": "terminal",
        }
        status_cases.append(
            (new_status, ("daily_collection_update", "terminal", "1"))
        )
        status_cases.append(
            (
                self.base_status(),
                ("legacy_formal_collection_batch", "legacy", "succeeded"),
            )
        )

        for status, expected_texts in status_cases:
            with self.subTest(expected_texts=expected_texts):
                with tempfile.TemporaryDirectory() as temporary_directory:
                    run_root = pathlib.Path(temporary_directory) / "run"
                    run_root.mkdir()
                    (run_root / "status.json").write_text(
                        json.dumps(status, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    (run_root / "monitor.pid").write_text(
                        "24680\n",
                        encoding="utf-8",
                    )
                    before = self.directory_fingerprint(run_root)
                    result = self.run_monitor_once(run_root)
                    after = self.directory_fingerprint(run_root)

                    self.assertEqual(result.returncode, 0, result.stderr)
                    for expected_text in expected_texts:
                        self.assertIn(expected_text, result.stdout)
                    self.assertEqual(after, before)
                    self.assertEqual(
                        (run_root / "monitor.pid").read_text(encoding="utf-8"),
                        "24680\n",
                    )

    def test_operations_runtime_probe_passes_all_three_io_boundaries(self) -> None:
        result = subprocess.run(
            [sys.executable, str(RUNTIME_PROBE_PATH)],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("python_atomic_replace_long_path_ok:", result.stdout)
        self.assertIn(
            "powershell_fileshare_readwrite_delete_ok: True",
            result.stdout,
        )
        self.assertIn(
            "pyarrow_long_unicode_space_round_trip_ok:",
            result.stdout,
        )
        self.assertIn("operations_runtime_ok: True", result.stdout)

    def test_archive_manifest_covers_six_non_executable_byte_snapshots(self) -> None:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        self.assertFalse(manifest["default_executable"])
        self.assertEqual(manifest["hash_algorithm"], "SHA-256")
        snapshots = manifest["snapshots"]
        self.assertEqual(len(snapshots), 6)
        expected_sources = {
            "00_draft_collection_02/scripts/run_b01_b04_historical_full_batch.py",
            "00_draft_collection_02/scripts/run_metadata_sync_and_b02_b04.py",
            "00_draft_collection_02/scripts/sync_current_silver_parquet_metadata.py",
            "00_draft_collection_02/scripts/invoke_c03_contract_calendar.py",
            "00_draft_collection_02/tests/test_b01_b04_historical_full_batch_manifest.py",
            "00_draft_collection_02/tests/test_sync_current_silver_parquet_metadata.py",
        }
        self.assertEqual(
            {entry["source_path"] for entry in snapshots},
            expected_sources,
        )

        for entry in snapshots:
            snapshot_path = ARCHIVE_ROOT / entry["snapshot_path"]
            snapshot_bytes = snapshot_path.read_bytes()
            self.assertEqual(snapshot_path.suffix, ".snapshot")
            self.assertEqual(len(snapshot_bytes), entry["byte_size"])
            self.assertEqual(
                hashlib.sha256(snapshot_bytes).hexdigest(),
                entry["sha256"],
            )

            source_path = PROJECT_ROOT / entry["source_path"]
            source_bytes = source_path.read_bytes()
            if source_path.name == "invoke_c03_contract_calendar.py":
                reconstructed_historical_bytes = source_bytes.replace(
                    b"02_Futures_Lakehouse",
                    b"02_Quant_Trading",
                )
                self.assertEqual(snapshot_bytes, reconstructed_historical_bytes)
                self.assertIn(b"02_Quant_Trading", snapshot_bytes)
                self.assertNotIn(b"02_Futures_Lakehouse", snapshot_bytes)
            else:
                self.assertEqual(snapshot_bytes, source_bytes)


if __name__ == "__main__":
    unittest.main()
