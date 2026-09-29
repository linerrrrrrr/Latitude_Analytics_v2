"""b06 共享事务与执行入口：临时 Parquet、明确 I/O 故障注入，不访问 API/正式湖。"""

import contextlib
import hashlib
import io
import pathlib
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import nbformat
import pandas as pd

import test_b01_c06_incremental_daily as fixture

b06 = fixture.minute
transaction_module = sys.modules[b06.StagedPathTransaction.__module__]
NOTEBOOK = pathlib.Path(b06.__file__).with_suffix('.ipynb')


class B06SharedTransactionTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='b06-transaction-')
        self.addCleanup(temporary.cleanup)
        self.root = pathlib.Path(temporary.name)
        self.silver = self.root / 'silver'
        self.fact_key = ('XSGE', 'RB', 2026, 8)
        self.calendar_key = ('1m', 'XSGE', 2026, 8)
        self.output = io.StringIO()
        capture = contextlib.redirect_stdout(self.output)
        capture.__enter__()
        self.addCleanup(capture.__exit__, None, None, None)

    def fact_frame(self, high=101.0, month=8):
        row = fixture.minute_row()
        row.update(high=high, month=month,
                   trading_date=pd.Timestamp(f'2026-{month:02d}-03').date(),
                   bar_at=pd.Timestamp(f'2026-{month:02d}-03 09:01',tz='Asia/Shanghai'))
        return pd.DataFrame([row]).loc[:, b06.FUTURES_MINUTE_SCHEMA.names]

    def commit_fact(self, high=101.0, month=8, empty=False):
        frame = b06.empty_pandas(b06.FUTURES_MINUTE_SCHEMA) if empty else self.fact_frame(high,month)
        return b06.commit_complete_partition(
            frame,self.root,b06.TABLE_NAME,b06.FUTURES_MINUTE_SCHEMA,
            b06.PARTITION_COLUMNS,b06.HIVE_PARTITIONING,('XSGE','RB',2026,month),
            b06.validate_minute_frame,
        )

    def clear_output(self):
        self.output.seek(0)
        self.output.truncate()

    def hashes(self):
        return {str(p.relative_to(self.silver)):hashlib.sha256(p.read_bytes()).hexdigest()
                for table in (b06.TABLE_NAME,b06.CALENDAR_TABLE_NAME)
                for p in (self.silver/table).rglob('*.parquet')}

    def assert_recovered(self, expected, quarantine=True):
        self.assertEqual(self.hashes(),expected)
        self.assertFalse(list(self.silver.glob('.s-*')))
        self.assertFalse(list(self.silver.glob('.b-*')))
        self.assertEqual(bool(list(self.silver.glob('.q-*'))),quarantine)
        output = self.output.getvalue()
        self.assertIn('phase=rollback; status=completed',output)
        self.assertNotIn('persisted=true',output)
        self.assertNotIn('partition_committed:',output)

    def test_success_preserves_compatible_marker_and_reports_after_readback(self):
        self.commit_fact()
        marker = self.silver/b06.TABLE_NAME/'schema.parquet'
        marker_bytes = marker.read_bytes()
        original = transaction_module.os.replace
        self.clear_output()
        with patch.object(transaction_module.os,'replace',wraps=original) as moves:
            result = self.commit_fact(high=102.0)
        self.assertEqual(result.iloc[0]['high'],102.0)
        self.assertEqual(marker.read_bytes(),marker_bytes)
        self.assertTrue(all(marker.resolve() not in call.args for call in moves.call_args_list))
        output = self.output.getvalue()
        self.assertLess(output.index('phase=formal_readback; status=completed'),output.index('persisted=true'))
        self.assertFalse(list(self.silver.glob('.[sbq]-*')))

    def test_empty_fact_remains_zero_row_leaf(self):
        self.commit_fact()
        self.clear_output()
        result = self.commit_fact(empty=True)
        leaf = b06.exact_partition_path(self.silver/b06.TABLE_NAME,b06.PARTITION_COLUMNS,self.fact_key)
        self.assertTrue(result.empty)
        self.assertTrue(leaf.is_dir())
        self.assertEqual([b06.pq.read_metadata(p).num_rows for p in leaf.glob('*.parquet')],[0])
        self.assertEqual(b06.pq.read_metadata(self.silver/b06.TABLE_NAME/'schema.parquet').num_rows,0)
        self.assertIn('persisted=true',self.output.getvalue())

    def test_first_backup_failure_does_not_move_or_delete_old_leaf(self):
        self.commit_fact()
        before = self.hashes()
        self.clear_output()
        original = transaction_module.os.replace

        def fail(source,target):
            if any(part.startswith('.b-') for part in target.parts):
                raise PermissionError('injected first backup failure')
            return original(source,target)

        with patch.object(transaction_module.os,'replace',side_effect=fail):
            with self.assertRaisesRegex(PermissionError,'first backup failure'):
                self.commit_fact(high=102.0)
        self.assert_recovered(before,quarantine=False)

    def test_install_failure_restores_old_leaf(self):
        self.commit_fact()
        before = self.hashes()
        self.clear_output()
        original = transaction_module.os.replace

        def fail(source,target):
            if any(part.startswith('.s-') for part in source.parts):
                raise OSError('injected leaf install failure')
            return original(source,target)

        with patch.object(transaction_module.os,'replace',side_effect=fail):
            with self.assertRaisesRegex(OSError,'leaf install failure'):
                self.commit_fact(high=102.0)
        self.assert_recovered(before,quarantine=False)

    def test_staging_readback_failure_leaves_formal_data_untouched(self):
        self.commit_fact()
        before = self.hashes()
        self.clear_output()
        original = b06.pq.read_schema

        def fail(path,*args,**kwargs):
            if any(part.startswith('.s-') for part in pathlib.Path(path).parts):
                raise OSError('injected staging readback failure')
            return original(path,*args,**kwargs)

        with patch.object(b06.pq,'read_schema',side_effect=fail):
            with self.assertRaisesRegex(OSError,'staging readback failure'):
                self.commit_fact(high=102.0)
        self.assertEqual(self.hashes(),before)
        self.assertFalse(list(self.silver.glob('.[sbq]-*')))
        self.assertNotIn('persisted=true',self.output.getvalue())

    def test_new_marker_write_install_readback_failures_restore_leaf_and_remove_marker(self):
        for phase in ('write','install','readback'):
            for existing_leaf in (False,True):
                with self.subTest(phase=phase,existing_leaf=existing_leaf), tempfile.TemporaryDirectory(prefix='b06-marker-') as temporary:
                    self.root = pathlib.Path(temporary)
                    self.silver = self.root/'silver'
                    if existing_leaf:
                        fixture.write_partitioned(self.fact_frame(),self.silver/b06.TABLE_NAME,
                                                  b06.FUTURES_MINUTE_SCHEMA,b06.PARTITION_COLUMNS)
                    before = self.hashes()
                    self.clear_output()
                    module,method = {'write':(b06.pq,'write_table'),
                                     'install':(transaction_module.os,'replace'),
                                     'readback':(b06.pq,'ParquetFile')}[phase]
                    original = getattr(module,method)

                    def fail(*args,**kwargs):
                        path = pathlib.Path(args[1] if phase in ('write','install') else args[0])
                        if path.name == 'schema.parquet':
                            raise OSError(f'injected marker {phase} failure')
                        return original(*args,**kwargs)

                    with patch.object(module,method,side_effect=fail):
                        with self.assertRaisesRegex(RuntimeError,'旧目标已恢复') as error:
                            self.commit_fact(high=102.0)
                    self.assertIsInstance(error.exception.__cause__,OSError)
                    self.assert_recovered(before)
                    self.assertFalse((self.silver/b06.TABLE_NAME/'schema.parquet').exists())
                    isolated = list(self.silver.glob('.q-*/**/*.parquet'))
                    self.assertEqual(len(isolated),1)
                    self.assertNotEqual(isolated[0].name,'schema.parquet')

    def test_formal_failure_restores_current_leaf_and_keeps_prior_successful_leaf(self):
        self.commit_fact()
        self.commit_fact(month=7)
        before = self.hashes()
        self.clear_output()
        leaf = b06.exact_partition_path(self.silver/b06.TABLE_NAME,b06.PARTITION_COLUMNS,self.fact_key)
        original = b06.pq.read_schema

        def fail(path,*args,**kwargs):
            if pathlib.Path(path).is_relative_to(leaf):
                raise OSError('injected formal failure')
            return original(path,*args,**kwargs)

        with patch.object(b06.pq,'read_schema',side_effect=fail):
            with self.assertRaisesRegex(RuntimeError,'旧目标已恢复'):
                self.commit_fact(high=102.0)
        self.assert_recovered(before)

    def test_calendar_failure_keeps_committed_fact_and_old_completion_state(self):
        calendar = pd.DataFrame([fixture.calendar_row(completed=False)]).loc[:,b06.FUTURES_BAR_CALENDAR_SCHEMA.names]
        b06.commit_complete_partition(calendar,self.root,b06.CALENDAR_TABLE_NAME,b06.FUTURES_BAR_CALENDAR_SCHEMA,
                                      b06.CALENDAR_PARTITION_COLUMNS,b06.CALENDAR_PARTITIONING,self.calendar_key,
                                      b06.validate_calendar_state_frame)
        facts = self.commit_fact(high=102.0)
        updates = b06.build_calendar_completion_updates(calendar,facts,{})
        before = self.hashes()
        self.clear_output()
        leaf = b06.exact_partition_path(self.silver/b06.CALENDAR_TABLE_NAME,b06.CALENDAR_PARTITION_COLUMNS,self.calendar_key)
        original_move,original_read = transaction_module.os.replace,b06.pq.read_schema
        installed = False

        def track_install(source,target):
            nonlocal installed
            result = original_move(source,target)
            if target == leaf.resolve() and any(part.startswith('.s-') for part in source.parts):
                installed = True
            return result

        def fail(path,*args,**kwargs):
            if installed and pathlib.Path(path).is_relative_to(leaf):
                raise OSError('injected calendar formal failure')
            return original_read(path,*args,**kwargs)

        with patch.object(transaction_module.os,'replace',side_effect=track_install), patch.object(b06.pq,'read_schema',side_effect=fail):
            with self.assertRaisesRegex(RuntimeError,'旧目标已恢复'):
                b06.commit_calendar_updates({},updates,self.root,'new-run',fixture.CHECKED_AT)
        self.assert_recovered(before)
        self.assertNotIn('phase=completion_state;',self.output.getvalue())
        restored = b06.read_complete_partition(self.silver/b06.CALENDAR_TABLE_NAME,b06.FUTURES_BAR_CALENDAR_SCHEMA,
                                               b06.CALENDAR_PARTITION_COLUMNS,self.calendar_key)
        self.assertFalse(restored.iloc[0]['is_fetch_completed'])

    def test_incomplete_restore_keeps_backup_and_failed_new_leaf(self):
        self.commit_fact()
        leaf = b06.exact_partition_path(self.silver/b06.TABLE_NAME,b06.PARTITION_COLUMNS,self.fact_key)
        old_bytes = next(leaf.glob('*.parquet')).read_bytes()
        self.clear_output()
        original_move,original_read = transaction_module.os.replace,b06.pq.read_schema

        def fail_restore(source,target):
            if any(part.startswith('.b-') for part in source.parts):
                raise PermissionError('injected restore failure')
            return original_move(source,target)

        def fail_read(path,*args,**kwargs):
            if pathlib.Path(path).is_relative_to(leaf):
                raise OSError('injected formal failure')
            return original_read(path,*args,**kwargs)

        with patch.object(transaction_module.os,'replace',side_effect=fail_restore), patch.object(b06.pq,'read_schema',side_effect=fail_read):
            with self.assertRaisesRegex(RuntimeError,'回滚不完整') as error:
                self.commit_fact(high=102.0)
        self.assertIsInstance(error.exception.__cause__,OSError)
        backups = list(self.silver.glob('.b-*/**/*.parquet'))
        self.assertEqual(len(backups),1)
        self.assertEqual(backups[0].read_bytes(),old_bytes)
        self.assertEqual(len(list(self.silver.glob('.q-*/**/*.parquet'))),1)
        self.assertFalse(list(self.silver.glob('.s-*')))
        self.assertIn('phase=rollback; status=failed',self.output.getvalue())
        self.assertNotIn('persisted=true',self.output.getvalue())


class B06EntryCellTest(unittest.TestCase):
    def test_notebook_script_and_import_routes(self):
        notebook = nbformat.read(NOTEBOOK,as_version=4)
        source = next(cell.source for cell in notebook.cells if cell.id=='b06-entry')
        cases = [
            ({'ipykernel':object()},{},'__main__','notebook'),
            ({},{'__file__':str(NOTEBOOK.with_suffix('.py'))},'__main__','script'),
            ({},{'__file__':str(NOTEBOOK.with_suffix('.py'))},'imported_b06','import'),
            ({'ipykernel':object()},{'__file__':str(NOTEBOOK.with_suffix('.py'))},'imported_b06','import'),
        ]
        for modules,extras,name,expected in cases:
            with self.subTest(expected=expected,kernel=bool(modules)):
                command = Mock()
                namespace = {'__name__':name,'main':command,
                             'sys':SimpleNamespace(modules=modules,argv=['ipykernel_launcher.py','-f','connection.json']),**extras}
                exec(compile(source,str(NOTEBOOK),'exec'),namespace)
                if expected=='notebook':
                    command.assert_not_called()
                    command.main.assert_called_once_with(args=['--start-date','2026-08-01','--end-date','2026-08-15'],
                                                         prog_name='b06_futures_minute',standalone_mode=False)
                elif expected=='script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    self.assertEqual(command.mock_calls,[])


if __name__=='__main__':
    unittest.main()
