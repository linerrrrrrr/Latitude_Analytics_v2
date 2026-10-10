"""共享文件事务的职责测试；不访问正式湖，不调用 API。"""

import io
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        PROJECT_ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
from R02_Market_Data.a01_Collection import b00_04_staged_path_transaction as transaction_module
from R02_Market_Data.a01_Collection.b00_04_staged_path_transaction import StagedPathTransaction


class StagedPathTransactionTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = pathlib.Path(self.temporary.name)
        self.root = self.base / 'silver'
        self.stage = self.root / '.stage'
        self.backup = self.root / '.backup'
        self.quarantine = self.root / '.quarantine'
        self.output = io.StringIO()
        self.capture = redirect_stdout(self.output)
        self.capture.__enter__()
        self.addCleanup(self.capture.__exit__, None, None, None)

    def write(self, path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding='utf-8')
        return path

    def transaction(self, quarantine=False):
        return StagedPathTransaction(
            root_path=self.root, staging_dir=self.stage, backup_dir=self.backup,
            quarantine_dir=self.quarantine if quarantine else None,
            log_context='table=test; run_id=test',
        )

    def test_group_commits_table_root_leaf_and_marker_without_touching_other_data(self):
        self.write(self.root / 'missing/old', 'old missing')
        self.write(self.root / 'calendar/month=1/data', 'old calendar')
        untouched = self.write(self.root / 'calendar/month=2/data', 'untouched')
        self.write(self.stage / 'missing/new', 'new missing')
        self.write(self.stage / 'calendar/data', 'new calendar')
        self.write(self.stage / 'marker', 'schema')
        # 对应 b08 整根+日历叶和 b05 跨表同组恢复的现有目标形态。
        with self.transaction() as transaction:
            transaction.replace(target_path=self.root / 'missing', staged_path=self.stage / 'missing')
            transaction.replace(target_path=self.root / 'calendar/month=1', staged_path=self.stage / 'calendar')
            transaction.replace(target_path=self.root / 'calendar/schema.parquet', staged_path=self.stage / 'marker')
            self.assertEqual((self.root / 'missing/new').read_text(), 'new missing')
        self.assertEqual(untouched.read_text(), 'untouched')
        self.assertFalse(self.backup.exists())
        self.assertFalse(self.stage.exists())

    def test_explicit_delete_commits_but_missing_source_does_not_mean_delete(self):
        target = self.write(self.root / 'calendar/data', 'old')
        with self.assertRaises(FileNotFoundError), self.transaction() as transaction:
            transaction.replace(target_path=target, staged_path=self.stage / 'missing')
        self.assertEqual(target.read_text(), 'old')
        with self.transaction() as transaction:
            transaction.replace(target_path=target, staged_path=None)
        self.assertFalse(target.exists())

    def test_formal_validation_failure_restores_replacements_deletes_and_new_marker(self):
        old = self.write(self.root / 'calendar/month=1/data', 'old')
        deleted = self.write(self.root / 'calendar/month=2/data', 'deleted old')
        self.write(self.stage / 'month1/data', 'new')
        self.write(self.stage / 'marker', 'schema')
        with self.assertRaisesRegex(ValueError, 'formal readback'), self.transaction() as transaction:
            transaction.replace(target_path=old.parent, staged_path=self.stage / 'month1')
            transaction.replace(target_path=deleted.parent, staged_path=None)
            transaction.replace(target_path=self.root / 'calendar/schema.parquet', staged_path=self.stage / 'marker')
            raise ValueError('formal readback')
        self.assertEqual(old.read_text(), 'old')
        self.assertEqual(deleted.read_text(), 'deleted old')
        self.assertFalse((self.root / 'calendar/schema.parquet').exists())
        self.assertFalse(self.backup.exists())

    def test_first_backup_failure_never_deletes_original(self):
        target = self.write(self.root / 'calendar/data', 'old')
        source = self.write(self.stage / 'data', 'new')
        with patch.object(transaction_module.os, 'replace', side_effect=PermissionError('backup locked')):
            with self.assertRaisesRegex(PermissionError, 'backup locked'), self.transaction() as transaction:
                transaction.replace(target_path=target, staged_path=source)
        self.assertEqual(target.read_text(), 'old')

    def test_second_install_failure_rolls_back_all_preceding_targets(self):
        targets = [self.write(self.root / f'table{index}/data', f'old{index}') for index in range(2)]
        sources = [self.write(self.stage / f'data{index}', f'new{index}') for index in range(2)]
        original_replace = transaction_module.os.replace

        def fail_second(source, destination):
            if source == sources[1]:
                raise OSError('second install')
            return original_replace(source, destination)

        with patch.object(transaction_module.os, 'replace', side_effect=fail_second):
            with self.assertRaisesRegex(OSError, 'second install'), self.transaction() as transaction:
                for target, source in zip(targets, sources):
                    transaction.replace(target_path=target, staged_path=source)
        self.assertEqual([path.read_text() for path in targets], ['old0', 'old1'])

    def test_successful_rollback_can_retain_failed_leaf_but_remove_marker(self):
        old = self.write(self.root / 'table/leaf/data', 'old')
        self.write(self.stage / 'leaf/data', 'failed new')
        self.write(self.stage / 'marker', 'failed marker')
        with self.assertRaisesRegex(RuntimeError, '旧目标已恢复') as caught:
            with self.transaction(quarantine=True) as transaction:
                transaction.replace(target_path=old.parent, staged_path=self.stage / 'leaf')
                transaction.replace(target_path=self.root / 'table/schema.parquet', staged_path=self.stage / 'marker', quarantine_new=False)
                raise ValueError('validation failed')
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(old.read_text(), 'old')
        self.assertEqual((self.quarantine / 'table/leaf/data').read_text(), 'failed new')
        self.assertFalse((self.root / 'table/schema.parquet').exists())
        self.assertFalse(self.backup.exists())

    def test_restore_failure_keeps_backup_and_still_restores_other_targets(self):
        targets = [self.write(self.root / f'table{index}/data', f'old{index}') for index in range(2)]
        sources = [self.write(self.stage / f'data{index}', f'new{index}') for index in range(2)]
        original_replace = transaction_module.os.replace

        def fail_one_restore(source, destination):
            if source == self.backup / 'table1/data':
                raise PermissionError('restore locked')
            return original_replace(source, destination)

        with patch.object(transaction_module.os, 'replace', side_effect=fail_one_restore):
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                with self.transaction() as transaction:
                    for target, source in zip(targets, sources):
                        transaction.replace(target_path=target, staged_path=source)
                    raise ValueError('formal validation')
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual((self.backup / 'table1/data').read_text(), 'old1')
        self.assertEqual(targets[0].read_text(), 'old0')
        self.assertFalse(self.stage.exists())

    def test_overlapping_targets_abort_and_restore_previous_install(self):
        old = self.write(self.root / 'table/leaf/data', 'old')
        self.write(self.stage / 'leaf/data', 'new')
        with self.assertRaisesRegex(ValueError, '重复或重叠'), self.transaction() as transaction:
            transaction.replace(target_path=old.parent, staged_path=self.stage / 'leaf')
            transaction.replace(target_path=self.root / 'table', staged_path=None)
        self.assertEqual(old.read_text(), 'old')

    def test_interrupted_restore_does_not_clean_the_only_old_copy(self):
        target = self.write(self.root / 'table/data', 'old')
        source = self.write(self.stage / 'data', 'new')
        original_replace = transaction_module.os.replace

        def interrupt_restore(source, destination):
            if source == self.backup / 'table/data':
                raise KeyboardInterrupt()
            return original_replace(source, destination)

        with patch.object(transaction_module.os, 'replace', side_effect=interrupt_restore):
            with self.assertRaises(KeyboardInterrupt), self.transaction() as transaction:
                transaction.replace(target_path=target, staged_path=source)
                raise ValueError('formal validation')
        self.assertEqual((self.backup / 'table/data').read_text(), 'old')

    def test_targets_outside_scope_and_overlapping_recovery_paths_are_rejected(self):
        outside = self.write(self.base / 'outside/data', 'untouched')
        with self.assertRaisesRegex(ValueError, '超出'), self.transaction() as transaction:
            transaction.replace(target_path=outside, staged_path=None)
        self.assertEqual(outside.read_text(), 'untouched')
        with self.assertRaisesRegex(ValueError, '重叠'), self.transaction() as transaction:
            transaction.replace(target_path=self.stage, staged_path=None)
        transaction = StagedPathTransaction(root_path=self.root, staging_dir=self.stage,
                                            backup_dir=self.stage / 'backup', log_context='test')
        with self.assertRaisesRegex(ValueError, '覆盖'), transaction:
            self.fail('must not enter')

    def test_existing_backup_is_never_reused_or_cleaned(self):
        evidence = self.write(self.backup / 'evidence', 'keep')
        with self.assertRaises(FileExistsError), self.transaction():
            self.fail('must not enter')
        self.assertEqual(evidence.read_text(), 'keep')


if __name__ == '__main__':
    unittest.main()
