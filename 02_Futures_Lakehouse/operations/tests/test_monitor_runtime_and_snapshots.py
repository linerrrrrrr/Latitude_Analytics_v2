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
sys.path.insert(0, str(OPERATIONS_ROOT / "runtime"))
import background_worker as worker
RUNTIME_PROBE_PATH = OPERATIONS_ROOT / "runtime" / "verify_operations_runtime.py"
ARCHIVE_ROOT = OPERATIONS_ROOT / "archived_batches"
MANIFEST_PATH = ARCHIVE_ROOT / "snapshot_manifest.json"


class MonitorRuntimeAndSnapshotTests(unittest.TestCase):
    def test_shared_reader_allows_repeated_atomic_replace(self) -> None:
        import threading
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "status.json"
            worker.atomic_write_json(path, {"counter": 0})
            failures = []
            finished = threading.Event()

            def read_loop():
                try:
                    while not finished.is_set():
                        self.assertIsInstance(worker.read_json_shared(path)["counter"], int)
                except BaseException as error:
                    failures.append(error)

            thread = threading.Thread(target=read_loop)
            thread.start()
            try:
                for counter in range(1, 101):
                    worker.atomic_write_json(path, {"counter": counter})
            finally:
                finished.set()
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(worker.read_json_shared(path), {"counter": 100})

    def test_complete_reference_archive_and_preserved_history(self) -> None:
        import stat
        import zipfile
        reference = OPERATIONS_ROOT / "referance"
        manifest = json.loads((reference / "snapshot_manifest.json").read_text(encoding="utf-8"))
        archive_path = reference / manifest["archive"]
        if not archive_path.exists():
            self.skipTest("Local reference ZIP is intentionally not tracked by Git.")
        self.assertEqual(hashlib.sha256(archive_path.read_bytes()).hexdigest(), manifest["archive_sha256"])
        if os.name == "nt":
            for path in (archive_path, reference / "snapshot_manifest.json"):
                self.assertTrue(path.stat().st_file_attributes & stat.FILE_ATTRIBUTE_READONLY)
        self.assertEqual(len(manifest["files"]), 154)
        with zipfile.ZipFile(archive_path) as archive:
            self.assertIsNone(archive.testzip())
            for entry in manifest["files"]:
                payload = archive.read("operations/" + entry["path"])
                self.assertEqual(len(payload), entry["byte_size"])
                self.assertEqual(hashlib.sha256(payload).hexdigest(), entry["sha256"])
                if entry["path"].startswith(("run_history/", "archived_batches/")):
                    self.assertEqual((OPERATIONS_ROOT / entry["path"]).read_bytes(), payload)

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
