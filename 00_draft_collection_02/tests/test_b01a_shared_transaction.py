"""b01a raw 单案例事务：临时文件系统、模拟请求和显式故障注入。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

from click.testing import CliRunner
import test_position_rank_special_case_calibration as fixtures
import a00_04_staged_path_transaction as shared

calibration = fixtures.calibration


class B01aTransactionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='b01a-test-')
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        fixture = fixtures.PositionRankSpecialCaseCalibrationTests()
        self.case = fixture.special_case()
        self.content = fixture.response_content()
        self.log = io.StringIO()
        redirect = contextlib.redirect_stdout(self.log)
        redirect.__enter__()
        self.addCleanup(redirect.__exit__, None, None, None)
        self.manifest = calibration.validate_response(self.content, self.case)
        self.target = self.root / 'raw' / self.case['raw_relative_path']
        self.staging = self.target.parent / '.b01a-s-test'
        self.backup = self.target.parent / '.b01a-b-test'
        self.quarantine = self.target.parent / '.b01a-f-test'
        uuid_patch = mock.patch.object(calibration.uuid, 'uuid4', return_value=types.SimpleNamespace(hex='test'))
        uuid_patch.start()
        self.addCleanup(uuid_patch.stop)
        network_patch = mock.patch.object(calibration.requests, 'get', side_effect=AssertionError('禁止真实请求'))
        network_patch.start()
        self.addCleanup(network_patch.stop)

    def commit(self):
        calibration.commit_artifacts(self.target, self.content, self.manifest, self.case)

    def expected_files(self):
        return {
            'response.dat': self.content,
            'response.sha256': (self.manifest['official_response_sha256'] + '\n').encode('ascii'),
            'calibration.json': (json.dumps(self.manifest, ensure_ascii=False, indent=2) + '\n').encode('utf8'),
        }

    def assert_files(self, path):
        self.assertEqual({p.name: p.read_bytes() for p in path.iterdir()}, self.expected_files())

    def fail_formal(self, path, case):
        self.original_verify(path, case)
        if path == self.target:
            raise ValueError('injected formal readback failure')

    def test_success_exact_bytes_readback_order_and_downstream_compatibility(self):
        self.original_verify = calibration.verify_artifacts
        with mock.patch.object(calibration, 'verify_artifacts', wraps=self.original_verify) as verify:
            self.commit()
        self.assertEqual(verify.call_args_list, [mock.call(self.staging, self.case), mock.call(self.target, self.case)])
        self.assert_files(self.target)
        for path in (self.staging, self.backup, self.quarantine):
            self.assertFalse(path.exists())
        output = self.log.getvalue()
        self.assertGreater(output.index('special_case_committed:'), output.index('phase=formal_verify; status=started'))
        self.assertIn('phase=artifact_state; status=completed;', output)
        # 直接执行下游现有证据验收函数，避免导入其无关行情处理依赖。
        downstream = fixtures.MODULE_PATH.with_name('b02_futures_holding_reports.py')
        node = next(node for node in ast.parse(downstream.read_text(encoding='utf8')).body
                    if isinstance(node, ast.FunctionDef) and node.name == 'verify_special_case_artifacts')
        namespace = {'pathlib': pathlib, 'hashlib': hashlib, 'json': json}
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(downstream), 'exec'), namespace)
        namespace['verify_special_case_artifacts'](self.root, self.case)

    def test_existing_valid_is_read_only_and_ready(self):
        self.commit()
        before = {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.target.iterdir()}
        with mock.patch.object(calibration, 'POSITION_RANK_SPECIAL_CASES', (self.case,)), mock.patch.object(
            calibration, 'commit_artifacts', side_effect=AssertionError('已有目录不能再次提交')
        ):
            result = CliRunner().invoke(calibration.main, ['--lake-root', str(self.root), '--write'])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn('position_rank_special_cases_ready: true', result.output)
        self.assertEqual(before, {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.target.iterdir()})

    def test_read_only_missing_requests_once_without_creating_raw(self):
        with mock.patch.object(calibration, 'POSITION_RANK_SPECIAL_CASES', (self.case,)), mock.patch.object(
            calibration.requests, 'get', return_value=fixtures.FakeResponse(self.content)
        ) as request:
            result = CliRunner().invoke(calibration.main, ['--lake-root', str(self.root)])
        self.assertEqual(result.exit_code, 0, result.output)
        request.assert_called_once_with(self.case['official_url'], timeout=60)
        self.assertIn('position_rank_special_cases_ready: false', result.output)
        self.assertFalse((self.root / 'raw').exists())

    def test_existing_invalid_fails_without_network_or_overwrite(self):
        self.target.mkdir(parents=True)
        (self.target / 'existing').write_bytes(b'keep')
        with mock.patch.object(calibration, 'POSITION_RANK_SPECIAL_CASES', (self.case,)):
            result = CliRunner().invoke(calibration.main, ['--lake-root', str(self.root), '--write'])
        self.assertIsInstance(result.exception, ValueError)
        self.assertEqual((self.target / 'existing').read_bytes(), b'keep')
        self.assertNotIn('position_rank_special_cases_ready:', result.output)

    def test_staging_rejection_precedes_transaction(self):
        with mock.patch.object(calibration, 'verify_artifacts', side_effect=ValueError('staging rejected')), mock.patch.object(
            calibration, 'StagedPathTransaction', side_effect=AssertionError('staging 未通过不能安装')
        ):
            with self.assertRaisesRegex(ValueError, 'staging rejected'):
                self.commit()
        self.assertFalse(self.target.exists())
        self.assertFalse(self.staging.exists())
        self.assertFalse(self.backup.exists())

    def test_transaction_entry_failure_cleans_only_staging(self):
        with mock.patch.object(shared.StagedPathTransaction, '__enter__', side_effect=PermissionError('entry denied')):
            with self.assertRaisesRegex(PermissionError, 'entry denied'):
                self.commit()
        self.assertFalse(self.target.exists())
        self.assertFalse(self.staging.exists())

    def test_install_failure_leaves_target_absent(self):
        with mock.patch.object(shared.os, 'replace', side_effect=PermissionError('install denied')) as replace:
            with self.assertRaisesRegex(PermissionError, 'install denied'):
                self.commit()
        self.assertEqual(replace.call_count, 1)
        for path in (self.target, self.staging, self.backup, self.quarantine):
            self.assertFalse(path.exists())
        self.assertNotIn('special_case_committed:', self.log.getvalue())

    def test_formal_failure_quarantines_exact_bytes_and_restores_absence(self):
        self.original_verify = calibration.verify_artifacts
        with mock.patch.object(calibration, 'verify_artifacts', side_effect=self.fail_formal):
            with self.assertRaisesRegex(RuntimeError, '新目标隔离') as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        for path in (self.target, self.staging, self.backup):
            self.assertFalse(path.exists())
        self.assert_files(self.quarantine / self.case['case_id'])
        self.assertNotIn('special_case_committed:', self.log.getvalue())
        self.assertNotIn('phase=artifact_state;', self.log.getvalue())

    def test_quarantine_failure_keeps_evidence_and_reports_incomplete_recovery(self):
        self.original_verify = calibration.verify_artifacts
        original_replace = shared.os.replace

        def replace(source, target):
            if pathlib.Path(source) == self.target:
                raise PermissionError('quarantine denied')
            return original_replace(source, target)

        with mock.patch.object(calibration, 'verify_artifacts', side_effect=self.fail_formal), mock.patch.object(
            shared.os, 'replace', side_effect=replace
        ) as move:
            with self.assertRaisesRegex(RuntimeError, '回滚不完整') as caught:
                self.commit()
        self.assertIsInstance(caught.exception.__cause__, ValueError)
        self.assertEqual(move.call_count, 2)
        self.assert_files(self.target)
        self.assertTrue(self.backup.exists())
        self.assertFalse(self.staging.exists())
        self.assertNotIn('special_case_committed:', self.log.getvalue())

    def test_existing_directory_guard_never_overwrites(self):
        self.target.mkdir(parents=True)
        (self.target / 'existing').write_bytes(b'keep')
        with mock.patch.object(shared.os, 'replace', side_effect=AssertionError('禁止覆盖')):
            with self.assertRaisesRegex(RuntimeError, '并发出现'):
                self.commit()
        self.assertEqual((self.target / 'existing').read_bytes(), b'keep')
        self.assertFalse(self.staging.exists())
        self.assertFalse(self.backup.exists())

    def test_second_case_failure_preserves_first_and_stops_third(self):
        cases = tuple({**self.case, 'case_id': name, 'raw_relative_path': 'shfe/position_rank_special_cases/' + name}
                      for name in ('first', 'second', 'third'))
        original_verify = calibration.verify_artifacts

        def verify(path, case):
            original_verify(path, case)
            if path.name == 'second':
                raise ValueError('second formal rejection')

        with mock.patch.object(calibration, 'POSITION_RANK_SPECIAL_CASES', cases), mock.patch.object(
            calibration.requests, 'get', return_value=fixtures.FakeResponse(self.content)
        ) as request, mock.patch.object(calibration, 'verify_artifacts', side_effect=verify):
            result = CliRunner().invoke(calibration.main, ['--lake-root', str(self.root), '--write'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(request.call_count, 2)
        first = self.target.parent / 'first'
        original_verify(first, cases[0])
        self.assertEqual((first / 'response.dat').read_bytes(), self.content)
        self.assertFalse((self.target.parent / 'second').exists())
        self.assertFalse((self.target.parent / 'third').exists())
        original_verify(self.quarantine / 'second', cases[1])
        self.assertEqual(result.output.count('special_case_committed:'), 1)
        self.assertNotIn('position_rank_special_cases_ready:', result.output)
        self.assertNotIn('phase=run; status=completed;', result.output)

    def test_entry_selects_notebook_script_and_import_without_kernel_args(self):
        notebook = json.loads(fixtures.MODULE_PATH.with_suffix('.ipynb').read_text(encoding='utf8'))
        source = ''.join(next(c for c in notebook['cells'] if c['id'] == '30c24e87')['source'])
        for kernel, has_file, module_name, expected in (
            (True, False, '__main__', 'notebook'), (False, True, '__main__', 'script'),
            (True, True, '__main__', 'script'), (True, True, 'imported', 'none'),
            (False, True, 'imported', 'none'),
        ):
            with self.subTest(kernel=kernel, has_file=has_file, module_name=module_name):
                command = mock.Mock()
                namespace = {'sys': types.SimpleNamespace(modules={'ipykernel': object()} if kernel else {}),
                             '__name__': module_name, 'main': command}
                if has_file:
                    namespace['__file__'] = str(fixtures.MODULE_PATH)
                exec(compile(source, '<entry>', 'exec'), namespace)
                if expected == 'notebook':
                    command.assert_not_called()
                    command.main.assert_called_once_with(args=[], prog_name='b01a_position_rank_special_case_calibration', standalone_mode=False)
                elif expected == 'script':
                    command.assert_called_once_with()
                    command.main.assert_not_called()
                else:
                    self.assertEqual(command.mock_calls, [])

    def test_real_module_import_inside_kernel_never_starts_business(self):
        spec = importlib.util.spec_from_file_location('b01a_kernel_import_test', fixtures.MODULE_PATH)
        module = importlib.util.module_from_spec(spec)
        with mock.patch.dict(sys.modules, {'ipykernel': types.ModuleType('ipykernel')}), mock.patch.object(
            calibration.click.Command, 'main', side_effect=AssertionError('导入不能启动业务')
        ):
            spec.loader.exec_module(module)
        self.assertTrue(callable(module.commit_artifacts))


if __name__ == '__main__':
    unittest.main()
