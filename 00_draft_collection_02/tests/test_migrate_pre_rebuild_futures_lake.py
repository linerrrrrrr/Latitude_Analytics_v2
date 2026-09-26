from __future__ import annotations

import importlib.util
import json
import pathlib
import shutil
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
SCRIPT_PATH = (
    PROJECT_ROOT
    / "00_draft_collection_02"
    / "scripts"
    / "migrate_pre_rebuild_futures_lake.py"
)
SPEC = importlib.util.spec_from_file_location("legacy_lake_migration", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class LegacyLakeMigrationTests(unittest.TestCase):
    def test_footer_only_patch_preserves_values_and_can_restore(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        tmp_path = pathlib.Path(temporary.name)
        source = next(
            (
                MODULE.settings.futures_lake_root.resolve()
                / "silver"
                / "fact_futures_minute"
            ).rglob("*.parquet")
        )
        copied = tmp_path / "minute.parquet"
        shutil.copy2(source, copied)
        before = pq.read_table(copied)
        before_bytes = copied.read_bytes()

        journal = tmp_path / "footer.bin"
        with journal.open("ab") as handle:
            MODULE.patch_parquet_footer(
                copied,
                MODULE.FUTURES_MINUTE_SCHEMA,
                handle,
                "exchange_code=X/underlying_code=Y/year=2020/month=1/minute.parquet",
            )
        after = pq.read_table(copied)
        self.assertTrue(after.equals(before))
        self.assertTrue(pq.read_schema(copied).equals(
            MODULE.file_schema(MODULE.FUTURES_MINUTE_SCHEMA),
            check_metadata=True,
        ))

        relative, footer_start, old_footer = MODULE.read_footer_journal(journal)[0]
        self.assertTrue(relative.endswith("minute.parquet"))
        with copied.open("r+b") as handle:
            handle.truncate(footer_start)
            handle.seek(footer_start)
            handle.write(old_footer)
        self.assertEqual(copied.read_bytes(), before_bytes)


    def test_legacy_daily_six_new_fields_may_all_be_null(self):
        old_root = (
            MODULE.settings.futures_lake_root.resolve()
            / "silver"
            / "fact_futures_daily"
        )
        leaf = next(
            path.parent
            for path in old_root.rglob("*.parquet")
            if path.parent != old_root
        )
        table = pa.concat_tables(
            [pq.ParquetFile(path).read() for path in leaf.glob("*.parquet")]
        )
        frame = table.to_pandas()
        daily_partition_columns = MODULE.partition_columns(
            MODULE.FUTURES_DAILY_SCHEMA
        )
        key = MODULE.parse_leaf_key(
            old_root,
            leaf,
            daily_partition_columns,
        )
        for name, value in zip(daily_partition_columns, key, strict=True):
            frame[name] = value
        for name in MODULE.FUTURES_DAILY_SCHEMA.names:
            if name not in frame.columns:
                frame[name] = None
        for name in [
            "previous_close",
            "previous_settlement",
            "settlement",
            "close_change_from_previous_settlement",
            "settlement_change_from_previous_settlement",
            "open_interest_change",
        ]:
            frame[name] = None
        frame["source"] = "JQData_get_price_1d_skip_paused"
        checked = MODULE.validate_small_table("fact_futures_daily", frame)
        self.assertEqual(len(checked), len(frame))


    def test_plan_is_read_only_and_finds_expected_minute_split(self):
        with tempfile.TemporaryDirectory(prefix="legacy-plan-") as directory:
            root = pathlib.Path(directory)
            old_silver = root / "old" / "silver"
            formal_lake = root / "formal_lake"
            formal_silver = formal_lake / "silver"
            for table_name in [
                "dim_trade_calendar",
                "dim_futures_variety_calendar",
                "dim_futures_contract_calendar",
                "dim_futures_session_schedule_signal",
                "fact_futures_daily",
                "fact_futures_fetch_status",
                "fact_futures_minute",
                "fact_futures_missing_bar",
            ]:
                (old_silver / table_name).mkdir(parents=True)
            (formal_silver / "fact_futures_minute").mkdir(parents=True)

            for base, month in [(old_silver, 1), (old_silver, 2), (formal_silver, 2)]:
                leaf = (
                    base
                    / "fact_futures_minute"
                    / "exchange_code=XSGE"
                    / "underlying_code=CU"
                    / "year=2020"
                    / f"month={month}"
                )
                leaf.mkdir(parents=True)
                pq.write_table(
                    pa.Table.from_batches(
                        [],
                        schema=MODULE.file_schema(MODULE.FUTURES_MINUTE_SCHEMA),
                    ),
                    leaf / "part-0.parquet",
                )

            before = {
                path: path.stat().st_mtime_ns for path in old_silver.iterdir()
            }
            result = MODULE.plan(old_silver, formal_lake)
            after = {
                path: path.stat().st_mtime_ns for path in old_silver.iterdir()
            }
            self.assertEqual(before, after)
            self.assertEqual(result["minute_non_overlap_leaves"], 1)
            self.assertEqual(result["minute_overlap_leaves"], 1)

    def test_resume_preflight_failure_does_not_touch_formal_root(self):
        with tempfile.TemporaryDirectory(prefix="legacy-resume-fail-") as directory:
            root = pathlib.Path(directory)
            formal_lake = root / "formal_lake"
            formal_silver = formal_lake / "silver"
            formal_silver.mkdir(parents=True)
            marker = formal_silver / "formal-marker.bin"
            marker.write_bytes(b"formal-unchanged")
            transaction = root / ".legacy-migration-test-run"
            transaction.mkdir()
            event_path = transaction / "events.jsonl"
            event_path.write_text("", encoding="utf-8")
            old_silver = root / "old" / "silver"
            old_silver.mkdir(parents=True)

            with mock.patch.object(
                MODULE,
                "prove_ready_for_c07",
                side_effect=RuntimeError("checkpoint incomplete"),
            ):
                with self.assertRaisesRegex(RuntimeError, "checkpoint incomplete"):
                    MODULE.resume_transaction(transaction, old_silver, formal_lake)

            self.assertEqual(marker.read_bytes(), b"formal-unchanged")
            self.assertEqual(event_path.read_bytes(), b"")
            self.assertFalse((transaction / "resume.pid").exists())

    def test_phase_aware_resume_starts_at_c07_and_never_rebuilds_c04(self):
        with tempfile.TemporaryDirectory(prefix="legacy-resume-c07-") as directory:
            root = pathlib.Path(directory)
            formal_lake = root / "formal_lake"
            formal_silver = formal_lake / "silver"
            formal_silver.mkdir(parents=True)
            transaction = root / ".legacy-migration-test-run"
            candidate_silver = transaction / "candidate_lake" / "silver"
            candidate_silver.mkdir(parents=True)
            old_silver = root / "old" / "silver"
            old_silver.mkdir(parents=True)
            (transaction / "events.jsonl").write_text("", encoding="utf-8")
            (transaction / "minute_footer_journal.bin").write_bytes(b"")

            for table_name in MODULE.AFFECTED_TABLES:
                formal_table = formal_silver / table_name
                formal_table.mkdir()
                (formal_table / "origin.txt").write_text("formal", encoding="utf-8")
                if table_name != "fact_futures_missing_bar":
                    candidate_table = candidate_silver / table_name
                    candidate_table.mkdir()
                    (candidate_table / "origin.txt").write_text(
                        "candidate",
                        encoding="utf-8",
                    )

            entry_calls = []

            def fake_entry(filename, lake_root, arguments, log_path):
                entry_calls.append(filename)
                if filename == "b08_full_minute_quality.py":
                    missing = candidate_silver / "fact_futures_missing_bar"
                    missing.mkdir()
                    (missing / "origin.txt").write_text("candidate", encoding="utf-8")

            temporary_project = root / "project"
            with (
                mock.patch.object(MODULE, "PROJECT_ROOT", temporary_project),
                mock.patch.object(
                    MODULE,
                    "prove_ready_for_c07",
                    return_value={"resume_from": "c07"},
                ),
                mock.patch.object(MODULE, "run_current_entry", side_effect=fake_entry),
                mock.patch.object(MODULE, "validate_exact_dataset", return_value=1),
                mock.patch.object(MODULE, "file_identity_manifest", return_value={}),
                mock.patch.object(MODULE, "copy_tree") as copy_tree,
                mock.patch.object(MODULE, "build_bar_calendar_vectorized") as build_c04,
                mock.patch.object(MODULE, "reconcile_calendar_from_facts") as reconcile,
            ):
                result = MODULE.resume_transaction(transaction, old_silver, formal_lake)

            self.assertEqual(
                entry_calls,
                [
                    "b07_suspected_session_reconciliation.py",
                    "b08_full_minute_quality.py",
                ],
            )
            copy_tree.assert_not_called()
            build_c04.assert_not_called()
            reconcile.assert_not_called()
            self.assertEqual(result["resume_checkpoint"], {"resume_from": "c07"})
            for table_name in MODULE.AFFECTED_TABLES:
                self.assertEqual(
                    (formal_silver / table_name / "origin.txt").read_text(
                        encoding="utf-8"
                    ),
                    "candidate",
                )
            self.assertFalse(transaction.exists())
            report_path = pathlib.Path(result["report_path"])
            self.assertEqual(json.loads(report_path.read_text(encoding="utf-8"))["run_id"], "test-run")

    def test_c08_retry_reapplies_minute_and_skips_completed_c07(self):
        with tempfile.TemporaryDirectory(prefix="legacy-resume-c08-") as directory:
            root = pathlib.Path(directory)
            formal_lake = root / "formal_lake"
            formal_silver = formal_lake / "silver"
            formal_silver.mkdir(parents=True)
            transaction = root / ".legacy-migration-test-run"
            candidate_silver = transaction / "candidate_lake" / "silver"
            candidate_silver.mkdir(parents=True)
            old_silver = root / "old" / "silver"
            old_silver.mkdir(parents=True)
            (transaction / "events.jsonl").write_text(
                json.dumps({"action": "c07_completed"}) + "\n",
                encoding="utf-8",
            )
            (transaction / "minute_footer_journal.bin").write_bytes(b"")

            for table_name in MODULE.AFFECTED_TABLES:
                formal_table = formal_silver / table_name
                formal_table.mkdir()
                (formal_table / "origin.txt").write_text("formal", encoding="utf-8")
                if table_name != "fact_futures_missing_bar":
                    candidate_table = candidate_silver / table_name
                    candidate_table.mkdir()
                    (candidate_table / "origin.txt").write_text(
                        "candidate",
                        encoding="utf-8",
                    )

            entry_calls = []

            def fake_entry(filename, lake_root, arguments, log_path):
                entry_calls.append(filename)
                missing = candidate_silver / "fact_futures_missing_bar"
                missing.mkdir()
                (missing / "origin.txt").write_text("candidate", encoding="utf-8")

            temporary_project = root / "project"
            with (
                mock.patch.object(MODULE, "PROJECT_ROOT", temporary_project),
                mock.patch.object(
                    MODULE,
                    "prove_c07_retained_after_rollback",
                    return_value={"resume_from": "minute_reapply_then_c08"},
                ),
                mock.patch.object(
                    MODULE,
                    "reapply_minute_after_c08_rollback",
                    return_value={"leaves": 6496, "rows": 517390417},
                ) as reapply,
                mock.patch.object(MODULE, "run_current_entry", side_effect=fake_entry),
                mock.patch.object(MODULE, "validate_exact_dataset", return_value=1),
                mock.patch.object(MODULE, "file_identity_manifest", return_value={}),
            ):
                result = MODULE.resume_transaction(transaction, old_silver, formal_lake)

            self.assertEqual(entry_calls, ["b08_full_minute_quality.py"])
            reapply.assert_called_once()
            self.assertEqual(result["resume_from"], "minute_reapply_then_c08")
            self.assertFalse(transaction.exists())

    def test_c08_short_transaction_names_stay_below_windows_max_path(self):
        script_path = (
            PROJECT_ROOT
            / "02_Futures_Lakehouse"
            / "a01_Futures_Market_Data"
            / "b08_full_minute_quality.py"
        )
        source = script_path.read_text(encoding="utf-8")
        for name in [
            ".c08-m-s-",
            ".c08-c-s-",
            ".c08-m-b-",
            ".c08-m-f-",
            ".c08-c-b-",
            ".c08-c-f-",
        ]:
            self.assertIn(name, source)

        silver_root = pathlib.Path(
            r"E:\Latitude_Analytics_v2\03_Futures_Database"
            r"\.legacy-migration-20260817T232818Z-13588d1e"
            r"\candidate_lake\silver"
        )
        longest_leaf = (
            silver_root
            / ".c08-m-s-715f43ca7b13"
            / "bar_frequency=1m"
            / "exchange_code=GFEX"
            / "underlying_code=SI"
            / "year=2023"
            / "month=1"
            / "part-0.parquet"
        )
        self.assertLess(len(str(longest_leaf)), 260)

    def test_reapply_leaf_replaces_only_an_exact_empty_target(self):
        with tempfile.TemporaryDirectory(prefix="legacy-empty-leaf-") as directory:
            root = pathlib.Path(directory)
            old_leaf = root / "old" / "month=1"
            old_leaf.mkdir(parents=True)
            (old_leaf / "part-0.parquet").write_bytes(b"journal-proven")
            candidate_leaf = root / "candidate" / "month=1"
            candidate_leaf.mkdir(parents=True)

            MODULE.move_reapplied_minute_leaf(old_leaf, candidate_leaf)

            self.assertFalse(old_leaf.exists())
            self.assertEqual(
                (candidate_leaf / "part-0.parquet").read_bytes(),
                b"journal-proven",
            )

    def test_reapply_leaf_rejects_marker_or_extra_file_in_target(self):
        for filename in ["schema.parquet", "part-extra.parquet"]:
            with self.subTest(filename=filename):
                with tempfile.TemporaryDirectory(
                    prefix="legacy-nonempty-leaf-"
                ) as directory:
                    root = pathlib.Path(directory)
                    old_leaf = root / "old" / "month=1"
                    old_leaf.mkdir(parents=True)
                    (old_leaf / "part-0.parquet").write_bytes(b"journal-proven")
                    candidate_leaf = root / "candidate" / "month=1"
                    candidate_leaf.mkdir(parents=True)
                    (candidate_leaf / filename).write_bytes(b"must-not-overwrite")

                    with self.assertRaisesRegex(RuntimeError, "不是精确空目录"):
                        MODULE.move_reapplied_minute_leaf(old_leaf, candidate_leaf)

                    self.assertTrue((old_leaf / "part-0.parquet").is_file())
                    self.assertEqual(
                        (candidate_leaf / filename).read_bytes(),
                        b"must-not-overwrite",
                    )

    def test_windows_rename_retries_only_transient_sharing_denial(self):
        with tempfile.TemporaryDirectory(prefix="legacy-rename-retry-") as directory:
            root = pathlib.Path(directory)
            source = root / "source"
            target = root / "target"
            source.mkdir()
            (source / "part.parquet").write_bytes(b"candidate")
            event_path = root / "events.jsonl"
            event_path.write_text("", encoding="utf-8")
            original_rename = pathlib.Path.rename
            calls = []

            def flaky_rename(path, destination):
                calls.append((path, destination))
                if len(calls) == 1:
                    error = PermissionError(5, "injected sharing denial")
                    error.winerror = 5
                    raise error
                return original_rename(path, destination)

            guard = mock.Mock()
            with (
                mock.patch.object(pathlib.Path, "rename", flaky_rename),
                mock.patch.object(MODULE.time, "sleep") as sleep,
            ):
                MODULE.rename_directory_with_retry(
                    source,
                    target,
                    event_path,
                    "candidate_to_formal",
                    "fact_futures_minute",
                    guard,
                )

            self.assertFalse(source.exists())
            self.assertEqual((target / "part.parquet").read_bytes(), b"candidate")
            self.assertEqual(len(calls), 2)
            self.assertEqual(guard.call_count, 2)
            sleep.assert_called_once_with(0.25)
            event = json.loads(event_path.read_text(encoding="utf-8"))
            self.assertEqual(event["action"], "windows_rename_retry")
            self.assertEqual(event["winerror"], 5)

    def test_windows_rename_does_not_retry_unrelated_permission_error(self):
        with tempfile.TemporaryDirectory(prefix="legacy-rename-stop-") as directory:
            root = pathlib.Path(directory)
            source = root / "source"
            target = root / "target"
            source.mkdir()
            event_path = root / "events.jsonl"
            event_path.write_text("", encoding="utf-8")
            error = PermissionError(13, "not a Windows sharing denial")
            error.winerror = 13
            with (
                mock.patch.object(pathlib.Path, "rename", side_effect=error),
                mock.patch.object(MODULE.time, "sleep") as sleep,
            ):
                with self.assertRaises(PermissionError):
                    MODULE.rename_directory_with_retry(
                        source,
                        target,
                        event_path,
                        "candidate_to_formal",
                        "fact_futures_minute",
                        lambda: None,
                    )

            self.assertTrue(source.is_dir())
            self.assertFalse(target.exists())
            sleep.assert_not_called()
            self.assertEqual(event_path.read_bytes(), b"")

    def test_post_c08_checkpoint_rejects_payload_tampering(self):
        with tempfile.TemporaryDirectory(prefix="legacy-checkpoint-") as directory:
            path = pathlib.Path(directory) / "checkpoint.json"
            MODULE.write_fsync_json(path, {"stage": "ready", "rows": 7})
            envelope = json.loads(path.read_text(encoding="utf-8"))
            envelope["payload"]["rows"] = 8
            path.write_text(json.dumps(envelope), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "摘要不匹配"):
                MODULE.read_fsync_json(path)

    def test_post_c08_lock_removes_only_a_proven_stale_pid(self):
        with tempfile.TemporaryDirectory(prefix="legacy-stale-lock-") as directory:
            root = pathlib.Path(directory)
            event_path = root / "events.jsonl"
            event_path.write_text("", encoding="utf-8")
            lock_path = root / "resume.pid"
            lock_path.write_text(
                json.dumps({"pid": 999991, "child_pid": 999992, "run_id": "run"}),
                encoding="utf-8",
            )
            with mock.patch.object(MODULE, "process_is_running", return_value=False):
                MODULE.acquire_post_c08_lock(lock_path, event_path, "run")

            lock = json.loads(lock_path.read_text(encoding="utf-8"))
            self.assertEqual(lock["pid"], MODULE.os.getpid())
            self.assertEqual(lock["run_id"], "run")
            event = json.loads(event_path.read_text(encoding="utf-8"))
            self.assertEqual(event["action"], "post_c08_stale_pid_lock_removed")

    def test_post_c08_prepare_snapshots_legacy_formal_without_new_schema_gate(self):
        with tempfile.TemporaryDirectory(prefix="legacy-formal-baseline-") as directory:
            root = pathlib.Path(directory)
            transaction = root / ".legacy-migration-test-run"
            candidate_silver = transaction / "candidate_lake" / "silver"
            formal_lake = root / "formal_lake"
            formal_silver = formal_lake / "silver"
            old_silver = root / "old" / "silver"
            candidate_silver.mkdir(parents=True)
            formal_silver.mkdir(parents=True)
            old_silver.mkdir(parents=True)
            (transaction / "events.jsonl").write_text("", encoding="utf-8")
            preserved = transaction / MODULE.POST_C08_MISSING_PRESERVED
            preserved.mkdir()
            (preserved / "part.parquet").write_bytes(b"preserved")

            for table_name in MODULE.AFFECTED_TABLES:
                candidate_table = candidate_silver / table_name
                candidate_table.mkdir()
                (candidate_table / "part.parquet").write_bytes(
                    b"preserved"
                    if table_name == "fact_futures_missing_bar"
                    else b"candidate"
                )
                formal_table = formal_silver / table_name
                formal_table.mkdir()
                pq.write_table(
                    pa.table({"legacy_only": [1]}),
                    formal_table / "part.parquet",
                )

            expected_rows = MODULE.POST_C08_EXACT_ROWS.copy()

            def candidate_rows(path, schema):
                return expected_rows[path.name]

            with (
                mock.patch.object(
                    MODULE,
                    "prove_post_c08_outputs",
                    return_value={"proof": True},
                ),
                mock.patch.object(
                    MODULE,
                    "reapply_minute_after_c08_rollback",
                    return_value={"leaves": 6496, "rows": 517390417},
                ),
                mock.patch.object(
                    MODULE,
                    "validate_exact_dataset",
                    side_effect=candidate_rows,
                ),
            ):
                checkpoint = MODULE.prepare_post_c08_swap(
                    transaction,
                    old_silver,
                    formal_lake,
                )

            self.assertEqual(checkpoint["stage"], "ready")
            self.assertEqual(
                checkpoint["formal_rows_before"],
                {table_name: 1 for table_name in MODULE.AFFECTED_TABLES},
            )

    def test_post_c08_swap_resumes_from_a_proven_mid_swap_state(self):
        with tempfile.TemporaryDirectory(prefix="legacy-post-c08-swap-") as directory:
            root = pathlib.Path(directory)
            formal_lake = root / "formal_lake"
            formal_silver = formal_lake / "silver"
            formal_silver.mkdir(parents=True)
            transaction = root / ".legacy-migration-test-run"
            candidate_silver = transaction / "candidate_lake" / "silver"
            candidate_silver.mkdir(parents=True)
            old_silver = root / "old" / "silver"
            old_silver.mkdir(parents=True)
            event_path = transaction / "events.jsonl"
            event_path.write_text(
                json.dumps({"action": "c08_completed"}) + "\n",
                encoding="utf-8",
            )

            for table_name in MODULE.AFFECTED_TABLES:
                formal_table = formal_silver / table_name
                formal_table.mkdir()
                (formal_table / "part.parquet").write_bytes(
                    f"formal-{table_name}".encode("utf-8")
                )
                candidate_table = candidate_silver / table_name
                candidate_table.mkdir()
                (candidate_table / "part.parquet").write_bytes(
                    f"candidate-{table_name}".encode("utf-8")
                )

            candidate_manifests = {
                table_name: MODULE.json_file_identity_manifest(
                    candidate_silver / table_name
                )
                for table_name in MODULE.AFFECTED_TABLES
            }
            formal_manifests = {
                table_name: MODULE.json_file_identity_manifest(
                    formal_silver / table_name
                )
                for table_name in MODULE.AFFECTED_TABLES
            }
            checkpoint = {
                "version": 1,
                "stage": "ready",
                "run_id": "test-run",
                "candidate_exact_rows": {
                    table_name: 1 for table_name in MODULE.AFFECTED_TABLES
                },
                "candidate_manifests": candidate_manifests,
                "formal_manifests_before": formal_manifests,
            }
            MODULE.write_fsync_json(
                transaction / MODULE.POST_C08_CHECKPOINT,
                checkpoint,
            )

            first = MODULE.AFFECTED_TABLES[0]
            first_formal = formal_silver / first
            first_candidate = candidate_silver / first
            first_backup = MODULE.post_c08_backup_path(
                formal_silver,
                first,
                "test-run",
            )
            first_formal.rename(first_backup)
            first_candidate.rename(first_formal)

            temporary_project = root / "project"
            report_path = (
                temporary_project
                / "00_draft_collection_02"
                / "migration_reports"
                / "legacy-futures-lake-test-run.json"
            )
            report_path.parent.mkdir(parents=True)
            report_path.write_text("{}", encoding="utf-8")
            with (
                mock.patch.object(MODULE, "PROJECT_ROOT", temporary_project),
                mock.patch.object(MODULE, "run_post_c08_prepare_child") as prepare,
                mock.patch.object(MODULE, "run_post_c08_formal_verify_child") as verify,
            ):
                result = MODULE.resume_post_c08_transaction(
                    transaction,
                    old_silver,
                    formal_lake,
                )

            prepare.assert_not_called()
            verify.assert_called_once()
            self.assertEqual(result["resume_from"], "post_c08_swap_only")
            for table_name in MODULE.AFFECTED_TABLES:
                self.assertEqual(
                    (formal_silver / table_name / "part.parquet").read_bytes(),
                    f"candidate-{table_name}".encode("utf-8"),
                )
            self.assertFalse(transaction.exists())
            self.assertFalse(old_silver.exists())

    def test_post_c08_swap_failure_restores_every_formal_root(self):
        with tempfile.TemporaryDirectory(prefix="legacy-post-c08-rollback-") as directory:
            root = pathlib.Path(directory)
            formal_lake = root / "formal_lake"
            formal_silver = formal_lake / "silver"
            formal_silver.mkdir(parents=True)
            transaction = root / ".legacy-migration-test-run"
            candidate_silver = transaction / "candidate_lake" / "silver"
            candidate_silver.mkdir(parents=True)
            old_silver = root / "old" / "silver"
            old_silver.mkdir(parents=True)
            (transaction / "events.jsonl").write_text(
                json.dumps({"action": "c08_completed"}) + "\n",
                encoding="utf-8",
            )

            for table_name in MODULE.AFFECTED_TABLES:
                formal_table = formal_silver / table_name
                formal_table.mkdir()
                (formal_table / "part.parquet").write_bytes(
                    f"formal-{table_name}".encode("utf-8")
                )
                candidate_table = candidate_silver / table_name
                candidate_table.mkdir()
                (candidate_table / "part.parquet").write_bytes(
                    f"candidate-{table_name}".encode("utf-8")
                )

            checkpoint = {
                "version": 1,
                "stage": "ready",
                "run_id": "test-run",
                "candidate_exact_rows": {
                    table_name: 1 for table_name in MODULE.AFFECTED_TABLES
                },
                "candidate_manifests": {
                    table_name: MODULE.json_file_identity_manifest(
                        candidate_silver / table_name
                    )
                    for table_name in MODULE.AFFECTED_TABLES
                },
                "formal_manifests_before": {
                    table_name: MODULE.json_file_identity_manifest(
                        formal_silver / table_name
                    )
                    for table_name in MODULE.AFFECTED_TABLES
                },
            }
            MODULE.write_fsync_json(
                transaction / MODULE.POST_C08_CHECKPOINT,
                checkpoint,
            )
            temporary_project = root / "project"
            report_path = (
                temporary_project
                / "00_draft_collection_02"
                / "migration_reports"
                / "legacy-futures-lake-test-run.json"
            )
            report_path.parent.mkdir(parents=True)
            report_path.write_text("{}", encoding="utf-8")
            original_rename = MODULE.rename_directory_with_retry

            def injected_rename(
                source,
                target,
                event_path,
                action,
                table_name,
                identity_guard,
            ):
                if action == "candidate_to_formal" and table_name == "fact_futures_minute":
                    raise PermissionError("injected minute root lock")
                return original_rename(
                    source,
                    target,
                    event_path,
                    action,
                    table_name,
                    identity_guard,
                )

            with (
                mock.patch.object(MODULE, "PROJECT_ROOT", temporary_project),
                mock.patch.object(MODULE, "run_post_c08_prepare_child") as prepare,
                mock.patch.object(
                    MODULE,
                    "rename_directory_with_retry",
                    side_effect=injected_rename,
                ),
                mock.patch.object(MODULE, "rollback_old_minute_moves") as minute_rollback,
            ):
                with self.assertRaisesRegex(PermissionError, "minute root lock"):
                    MODULE.resume_post_c08_transaction(
                        transaction,
                        old_silver,
                        formal_lake,
                    )

            prepare.assert_not_called()
            minute_rollback.assert_called_once()
            for table_name in MODULE.AFFECTED_TABLES:
                self.assertEqual(
                    (formal_silver / table_name / "part.parquet").read_bytes(),
                    f"formal-{table_name}".encode("utf-8"),
                )
                self.assertEqual(
                    (candidate_silver / table_name / "part.parquet").read_bytes(),
                    f"candidate-{table_name}".encode("utf-8"),
                )
                self.assertFalse(
                    MODULE.post_c08_backup_path(
                        formal_silver,
                        table_name,
                        "test-run",
                    ).exists()
                )
            rolled_back = MODULE.read_fsync_json(
                transaction / MODULE.POST_C08_CHECKPOINT
            )
            self.assertEqual(rolled_back["stage"], "rolled_back")
            self.assertTrue(old_silver.is_dir())


if __name__ == "__main__":
    unittest.main()
