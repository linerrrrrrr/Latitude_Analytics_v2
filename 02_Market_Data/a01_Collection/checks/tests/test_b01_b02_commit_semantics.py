"""b01/b02 已复现故障的回归检查；仅临时湖、模拟 API。"""

import ast
import hashlib
import io
import json
import pathlib
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, patch

import click
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from click.testing import CliRunner

from test_b01_c01_c02_daily_tail_modes import (
    FixedDateTime,
    PROJECT_ROOT,
    c01,
    c02,
    trade_calendar_df,
    variety_calendar_df,
    write_trade_calendar,
    write_variety_calendar,
)
from test_futures_variety_calendar_complete_catalog import FakeJQData
import b00_04_staged_path_transaction as transaction_module
from config import data_contracts


def parquet_snapshot(table_path):
    return {
        str(path.relative_to(table_path)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in table_path.rglob('*.parquet')
    }


class CommitSemanticsTest(unittest.TestCase):
    def test_b01_arrow_validation_is_not_repeated_inside_business_validation(self):
        for mode, expected_calls in (('collect', 1), ('empty_commit', 2), ('merge_commit', 4)):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                lake_root = pathlib.Path(directory)
                if mode == 'merge_commit':
                    write_trade_calendar(lake_root, trade_calendar_df(date(2024, 1, 1), date(2024, 1, 2)))
                client = Mock()
                client.get_trade_days.return_value = [date(2024, 1, 3)]
                with (
                    redirect_stdout(io.StringIO()),
                    patch.object(data_contracts, 'validate_arrow_table', wraps=data_contracts.validate_arrow_table) as validate,
                    patch.object(c01, 'validate_arrow_table', validate),
                    patch('config.jqdata_connection.authenticate_jqdata', return_value=client),
                ):
                    if mode == 'collect':
                        frame = c01.collect(date(2024, 1, 3), date(2024, 1, 3))
                        self.assertEqual(frame.calendar_date.tolist(), [date(2024, 1, 3)])
                    else:
                        committed_rows = c01.commit_partitions(
                            trade_calendar_df(date(2024, 1, 3), date(2024, 1, 3)), lake_root,
                        )
                        self.assertEqual(committed_rows, 1)
                self.assertEqual(validate.call_count, expected_calls)

    def test_b01_schema_and_business_failures_still_stop_before_writing(self):
        for field, bad_value in (('source', None), ('weekday', 7)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                frame = trade_calendar_df(date(2024, 1, 3), date(2024, 1, 3))
                frame.loc[0, field] = bad_value
                with redirect_stdout(io.StringIO()), patch.object(c01.ds, 'write_dataset') as write:
                    with self.assertRaises(ValueError):
                        c01.commit_partitions(frame, pathlib.Path(directory))
                write.assert_not_called()

    def test_formal_file_schema_is_checked_once_for_single_and_multiple_files(self):
        for module in (c01, c02):
            for rows_per_file in (1, 10):
                with self.subTest(table=module.TABLE_NAME, rows_per_file=rows_per_file), tempfile.TemporaryDirectory() as directory:
                    start, end = date(2024, 1, 2), date(2024, 1, 3)
                    frame = trade_calendar_df(start, end) if module is c01 else pd.concat([
                        variety_calendar_df(start), variety_calendar_df(end),
                    ], ignore_index=True)
                    original_write = ds.write_dataset

                    def write_files(table, base_dir, **kwargs):
                        return original_write(table, base_dir, **kwargs,
                                              max_rows_per_file=rows_per_file, max_rows_per_group=rows_per_file)

                    lake_root = pathlib.Path(directory)
                    with (
                        redirect_stdout(io.StringIO()),
                        patch.object(module.ds, 'write_dataset', side_effect=write_files),
                        patch.object(module, 'validate_compatible_dataset_schema', wraps=module.validate_compatible_dataset_schema) as validate,
                    ):
                        if module is c01:
                            module.commit_partitions(frame, lake_root)
                        else:
                            module.commit_partitions(frame, lake_root, start, end)
                    files = list((lake_root / 'silver' / module.TABLE_NAME).rglob('*.parquet'))
                    checks = [call.args[2] for call in validate.call_args_list if call.args[2].startswith('正式分区')]
                    self.assertEqual(len(checks), len(files))
                    self.assertEqual(len(set(checks)), len(files))

    def test_b02_reads_upstream_once_and_preserves_modes_and_api_counts(self):
        for mode in ('tail', 'no_new', 'full', 'explicit'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                lake_root = pathlib.Path(directory)
                start = date(2024, 1, 2)
                end = start if mode == 'no_new' else date(2024, 1, 3)
                write_trade_calendar(lake_root, trade_calendar_df(start, end))
                write_variety_calendar(lake_root, variety_calendar_df(start))
                client = FakeJQData()
                original_dataset = ds.dataset
                upstream_opens = []
                upstream_path = lake_root / 'silver' / c02.TRADE_TABLE_NAME

                def open_dataset(source, *args, **kwargs):
                    if pathlib.Path(source) == upstream_path:
                        upstream_opens.append(source)
                    return original_dataset(source, *args, **kwargs)

                arguments = ['--full'] if mode == 'full' else (
                    ['--start-date', str(start), '--end-date', str(end)] if mode == 'explicit' else []
                )
                with (
                    patch.object(c02, 'settings', SimpleNamespace(futures_lake_root=lake_root, jqdata_id='test', jqdata_secret='test')),
                    patch.object(c02.ds, 'dataset', side_effect=open_dataset),
                    patch.object(c02, 'validate_dataset_fragment_schemas', wraps=c02.validate_dataset_fragment_schemas) as validate,
                    patch('config.jqdata_connection.authenticate_jqdata', return_value=client) as authenticate,
                    patch.object(c02, 'collect', wraps=c02.collect) as collect,
                ):
                    result = CliRunner().invoke(c02.main, arguments)
                self.assertEqual(result.exit_code, 0, result.output)
                self.assertEqual(len(upstream_opens), 1)
                self.assertEqual(len([call for call in validate.call_args_list if call.args[3] == '上游交易日历 ']), 1)
                self.assertEqual(len(client.calls), 0 if mode == 'no_new' else 1)
                if mode == 'no_new':
                    authenticate.assert_not_called()
                    collect.assert_not_called()
                elif mode == 'tail':
                    self.assertEqual(collect.call_args.kwargs['selected_trading_dates'], [end])
                    self.assertIn('phase=upstream_reuse; status=completed; trading_dates=1', result.output)
                    self.assertIn('new_trading_date_count=1; rows=4; api_calls=1', result.output)
                else:
                    self.assertNotIn('selected_trading_dates', collect.call_args.kwargs)

    def test_b01_read_or_install_failure_preserves_old_partition(self):
        for failure_phase in ('staging_leaf_read', 'install'):
            with self.subTest(phase=failure_phase), tempfile.TemporaryDirectory() as directory:
                lake_root = pathlib.Path(directory)
                table_path = lake_root / 'silver' / c01.TABLE_NAME
                write_trade_calendar(lake_root, trade_calendar_df(date(2024, 1, 2), date(2024, 1, 3)))
                before = parquet_snapshot(table_path)
                original_dataset, original_move = ds.dataset, transaction_module.os.replace
                fault_calls = []

                def fail_read(source, *args, **kwargs):
                    source_path = pathlib.Path(source)
                    if failure_phase == 'staging_leaf_read' and source_path.name == 'year=2024' and source_path.parent.name.startswith('.c01s-'):
                        fault_calls.append(source_path)
                        raise OSError('injected staging leaf read failure')
                    return original_dataset(source, *args, **kwargs)

                def fail_install(source, destination, *args, **kwargs):
                    if failure_phase == 'install' and pathlib.Path(source).parent.name.startswith('.c01s-'):
                        fault_calls.append(source)
                        raise OSError('injected install failure')
                    return original_move(source, destination, *args, **kwargs)

                output = io.StringIO()
                with (
                    redirect_stdout(output),
                    patch.object(c01.ds, 'dataset', side_effect=fail_read),
                    patch.object(transaction_module.os, 'replace', side_effect=fail_install),
                ):
                    with self.assertRaisesRegex(OSError, 'injected'):
                        c01.commit_partitions(trade_calendar_df(date(2024, 1, 3), date(2024, 1, 3)), lake_root)
                self.assertEqual(len(fault_calls), 1)
                self.assertEqual(parquet_snapshot(table_path), before)
                self.assertIn('phase=rollback; status=completed', output.getvalue())
                self.assertFalse(any(line.startswith('committed:') for line in output.getvalue().splitlines()))

    def test_each_readback_gate_rejects_later_file_drift_and_restores_old_data(self):
        for module, schema, writer, field_name, drift_type in (
            (c01, c01.TRADE_CALENDAR_SCHEMA, write_trade_calendar, 'weekday', pa.int16()),
            (c02, c02.FUTURES_VARIETY_CALENDAR_SCHEMA, write_variety_calendar, 'active_contract_count', pa.int32()),
        ):
            for gate in ('staging', 'formal'):
                with self.subTest(table=module.TABLE_NAME, gate=gate), tempfile.TemporaryDirectory() as directory:
                    lake_root = pathlib.Path(directory)
                    table_path = lake_root / 'silver' / module.TABLE_NAME
                    start, end = date(2024, 1, 2), date(2024, 1, 3)
                    frame = trade_calendar_df(start, end) if module is c01 else pd.concat([
                        variety_calendar_df(start), variety_calendar_df(end),
                    ], ignore_index=True)
                    old_frame = frame.copy()
                    old_frame['is_trading_day' if module is c01 else 'active_contract_count'] = False if module is c01 else 2
                    writer(lake_root, old_frame)
                    before = parquet_snapshot(table_path)
                    original_write, original_move = ds.write_dataset, transaction_module.os.replace
                    changed_files = []

                    def corrupt_second_file(directory_path):
                        fragments = sorted(pathlib.Path(directory_path).rglob('*.parquet'))
                        self.assertEqual(len(fragments), 2)
                        fragment_path = fragments[1]
                        with pq.ParquetFile(fragment_path) as parquet_file:
                            table = parquet_file.read()
                        index = table.schema.get_field_index(field_name)
                        field = table.schema.field(index)
                        drift_schema = table.schema.set(index, pa.field(
                            field.name, drift_type, nullable=field.nullable, metadata=field.metadata,
                        ))
                        pq.write_table(table.cast(drift_schema), fragment_path)
                        changed_files.append(fragment_path)

                    def split_write(table, base_dir, **kwargs):
                        original_write(table, base_dir, **kwargs, max_rows_per_file=1, max_rows_per_group=1)
                        if gate == 'staging':
                            corrupt_second_file(base_dir)

                    def corrupt_after_install(source, destination, *args, **kwargs):
                        moved = original_move(source, destination, *args, **kwargs)
                        if gate == 'formal' and table_path in pathlib.Path(destination).parents and any(
                            part.startswith(('.c01s-', '.c02s-')) for part in pathlib.Path(source).parts
                        ):
                            corrupt_second_file(destination)
                        return moved

                    output = io.StringIO()
                    with (
                        redirect_stdout(output),
                        patch.object(module.ds, 'write_dataset', side_effect=split_write),
                        patch.object(transaction_module.os, 'replace', side_effect=corrupt_after_install),
                    ):
                        with self.assertRaises((TypeError, RuntimeError)) as caught:
                            if module is c01:
                                module.commit_partitions(frame, lake_root)
                            else:
                                module.commit_partitions(frame, lake_root, start, end)
                    original_error = caught.exception.__cause__ or caught.exception
                    self.assertIsInstance(original_error, TypeError)
                    self.assertIn(field_name, str(original_error))
                    self.assertEqual(len(changed_files), 1)
                    self.assertEqual(parquet_snapshot(table_path), before)
                    self.assertFalse(any(line.startswith('committed:') for line in output.getvalue().splitlines()))

    def test_b02_empty_marker_failure_restores_previous_table(self):
        for old_rows in (False, True):
            for fault in ('type', 'row_count', 'read'):
                with self.subTest(old_rows=old_rows, fault=fault), tempfile.TemporaryDirectory() as directory:
                    lake_root = pathlib.Path(directory)
                    table_path = lake_root / 'silver' / c02.TABLE_NAME
                    trading_date = date(2024, 1, 2)
                    frame = variety_calendar_df(trading_date)
                    if old_rows:
                        write_variety_calendar(lake_root, frame)
                    before = parquet_snapshot(table_path)
                    original_write, original_parquet_file = pq.write_table, pq.ParquetFile
                    faults = []

                    def write_marker(table, where, **kwargs):
                        self.assertEqual(pathlib.Path(where).name, 'schema.parquet')
                        if fault == 'type':
                            index = table.schema.get_field_index('active_contract_count')
                            field = table.schema.field(index)
                            table = table.cast(table.schema.set(index, pa.field(
                                field.name, pa.int32(), nullable=field.nullable, metadata=field.metadata,
                            )))
                            faults.append(fault)
                        elif fault == 'row_count':
                            full_table = c02.pandas_to_arrow(frame, c02.FUTURES_VARIETY_CALENDAR_SCHEMA)
                            table = full_table.select(table.schema.names)
                            faults.append(fault)
                        original_write(table, where, **kwargs)

                    def read_marker(source, *args, **kwargs):
                        if fault == 'read' and pathlib.Path(source).name == 'schema.parquet':
                            faults.append(fault)
                            raise OSError('injected marker read failure')
                        return original_parquet_file(source, *args, **kwargs)

                    output = io.StringIO()
                    expected_error = {'type': TypeError, 'row_count': ValueError, 'read': OSError}[fault]
                    with (
                        redirect_stdout(output),
                        patch.object(c02.pq, 'write_table', side_effect=write_marker),
                        patch.object(c02.pq, 'ParquetFile', side_effect=read_marker),
                    ):
                        with self.assertRaises(expected_error):
                            c02.commit_partitions(frame.iloc[0:0], lake_root, trading_date, trading_date)
                    self.assertEqual(faults, [fault])
                    self.assertEqual(parquet_snapshot(table_path), before)
                    self.assertFalse((table_path / 'schema.parquet').exists())
                    self.assertFalse(any(line.startswith('committed:') for line in output.getvalue().splitlines()))

    def test_b01_entry_distinguishes_notebook_script_and_import(self):
        notebook = json.loads(pathlib.Path(c01.__file__).with_suffix('.ipynb').read_text(encoding='utf-8'))
        entry = next(''.join(cell['source']) for cell in notebook['cells'] if cell['cell_type'] == 'code'
                     and 'notebook_args =' in ''.join(cell['source']))
        for mode in ('notebook', 'script', 'import'):
            with self.subTest(mode=mode):
                callback = Mock()
                command = click.Command('b01', params=c01.main.params, callback=callback)
                namespace = {'sys': sys, 'main': command, '__name__': '__main__'}
                arguments = ['ipykernel_launcher.py', '-f', 'connection.json']
                if mode != 'notebook':
                    namespace['__file__'] = c01.__file__
                if mode == 'import':
                    namespace['__name__'] = c01.__name__
                elif mode == 'script':
                    arguments = [c01.__file__, '--full']
                with patch.dict(sys.modules, {'ipykernel': Mock()}), patch.object(sys, 'argv', arguments):
                    if mode == 'script':
                        with self.assertRaises(SystemExit) as caught:
                            exec(compile(entry, '<b01 entry>', 'exec'), namespace)
                        self.assertEqual(caught.exception.code, 0)
                    else:
                        exec(compile(entry, '<b01 entry>', 'exec'), namespace)
                if mode == 'import':
                    callback.assert_not_called()
                else:
                    callback.assert_called_once()
                    self.assertFalse(callback.call_args.kwargs['write'])
                    self.assertEqual(callback.call_args.kwargs['full_refresh'], mode == 'script')
                    if mode == 'notebook':
                        self.assertEqual(callback.call_args.kwargs['start_date'].date(), date(2026, 8, 1))
                        self.assertEqual(callback.call_args.kwargs['end_date'].date(), date(2026, 8, 15))

    def test_b01_main_summaries_refresh_worker_progress(self):
        worker_source = (PROJECT_ROOT / '02_Market_Data/a01_Collection/operations/runtime/background_worker.py').read_text(encoding='utf-8')
        prefixes = next(ast.literal_eval(node.value) for node in ast.parse(worker_source).body
                        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name)
                        and target.id == 'DEFAULT_PROGRESS_PREFIXES' for target in node.targets))
        for mode in ('full_uptodate', 'full_changes', 'explicit'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                lake_root = pathlib.Path(directory)
                start, end = date(2024, 1, 1), date(2024, 1, 3)
                existing = trade_calendar_df(start, end)
                if mode == 'full_changes':
                    existing = existing.iloc[[0, 2]]
                write_trade_calendar(lake_root, existing)
                settings = SimpleNamespace(futures_lake_root=lake_root.resolve(), futures_data_start_date=start,
                                           jqdata_id='test', jqdata_secret='test')
                client = Mock()
                client.get_trade_days.return_value = list(pd.date_range(start, end).date)
                arguments = ['--full'] if mode != 'explicit' else ['--start-date', str(start), '--end-date', str(end)]
                with patch.object(c01, 'settings', settings), patch.object(c01, 'datetime', FixedDateTime), patch(
                    'config.jqdata_connection.authenticate_jqdata', return_value=client,
                ):
                    result = CliRunner().invoke(c01.main, arguments)
                self.assertEqual(result.exit_code, 0, result.output)
                progress = [line for line in result.output.splitlines() if line.startswith(prefixes)]
                self.assertTrue(progress[0].startswith('planning_progress:'))
                self.assertIn('phase=run; status=started', progress[0])
                self.assertIn('=' * 88, result.output)
                if mode == 'full_uptodate':
                    self.assertTrue(progress[-1].startswith('up_to_date:'))
                else:
                    self.assertIn('phase=run; status=completed', progress[-1])
                    self.assertIn('write=false', progress[-1])
                if mode == 'full_changes':
                    self.assertTrue(any(line.startswith('reconciliation_plan:') for line in progress))


if __name__ == '__main__':
    unittest.main()
