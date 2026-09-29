"""第 2/5 轮日志行为差分；所有网络请求均模拟，文件仅写系统临时目录。"""
import hashlib
import importlib.util
import json
import pathlib
import sys
import tempfile
from unittest.mock import patch

from click.testing import CliRunner

ROOT = pathlib.Path(__file__).resolve().parents[2]
SNAPSHOT = pathlib.Path(sys.argv[1])
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01a_position_rank_special_case_calibration.py'
sys.path.insert(0, str(ROOT / '00_draft_collection_02/tests'))
from test_position_rank_special_case_calibration import FakeResponse, PositionRankSpecialCaseCalibrationTests


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


old = load('b01a_before_docs_logs', SNAPSHOT / PATH.name)
new = load('b01a_after_docs_logs', PATH)
fixture = PositionRankSpecialCaseCalibrationTests()
content = fixture.response_content()
case = fixture.special_case()
checks = []


def file_bytes(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob('*') if p.is_file()}


def seed(module, target, special_case):
    target.mkdir(parents=True)
    manifest = module.validate_response(content, special_case)
    (target/'response.dat').write_bytes(content)
    (target/'response.sha256').write_text(manifest['official_response_sha256']+'\n', encoding='ascii', newline='\n')
    (target/'calibration.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+'\n', encoding='utf8', newline='\n')


scenarios = (
    'missing_read_only', 'missing_write', 'existing_read_only', 'existing_write',
    'existing_invalid', 'http_failure', 'hash_mismatch', 'top_twenty_mismatch',
    'staging_failure', 'formal_failure', 'second_case_failure',
)
with tempfile.TemporaryDirectory(prefix='b01a-style-check-') as temporary:
    temporary = pathlib.Path(temporary)
    for scenario in scenarios:
        evidence = []
        for label, module in (('before', old), ('after', new)):
            lake = temporary/scenario/label
            lake.mkdir(parents=True)
            special_case = dict(case)
            if scenario == 'top_twenty_mismatch':
                special_case['official_rows'] = ((1, '另一会员', 999, -9), *case['official_rows'][1:])
            cases = [special_case]
            if scenario == 'second_case_failure':
                cases.append(dict(case, case_id='second_case', raw_relative_path='shfe/position_rank_special_cases/second_case'))
            target = lake/'raw'/str(special_case['raw_relative_path'])
            if scenario.startswith('existing'):
                seed(module, target, special_case)
                if scenario == 'existing_invalid':
                    (target/'response.sha256').write_text('wrong\n', encoding='ascii')
            original_verify = module.verify_artifacts
            def verify(path, supplied_case):
                if scenario == 'staging_failure' and path.name.startswith('.'):
                    raise ValueError('injected staging failure')
                if scenario == 'formal_failure' and path == target:
                    raise ValueError('injected formal failure')
                return original_verify(path, supplied_case)
            responses = [FakeResponse(content)]
            if scenario == 'http_failure':
                responses = [FakeResponse(b'blocked', 503)]
            elif scenario == 'hash_mismatch':
                responses = [FakeResponse(b'different bytes')]
            elif scenario == 'second_case_failure':
                responses.append(FakeResponse(b'blocked', 503))
            if scenario.startswith('existing'):
                responses = []
            args = ['--lake-root', str(lake)]
            if not scenario.endswith('read_only'):
                args.append('--write')
            with patch.object(module, 'POSITION_RANK_SPECIAL_CASES', tuple(cases)), patch.object(module.requests, 'get', side_effect=responses) as request, patch.object(module, 'verify_artifacts', side_effect=verify):
                result = CliRunner().invoke(module.main, args)
            expected_calls = 0 if scenario.startswith('existing') else 2 if scenario == 'second_case_failure' else 1
            assert request.call_count == expected_calls, (scenario, label, request.call_count)
            for call in request.call_args_list:
                assert call.kwargs == {'timeout': 60}
            expected_success = scenario in ('missing_read_only', 'missing_write', 'existing_read_only', 'existing_write')
            assert (result.exit_code == 0) == expected_success, (scenario, label, result.exception)
            if not expected_success:
                assert 'position_rank_special_cases_ready: true' not in result.output
            assert not list(lake.rglob('*.staging-*'))
            if scenario in ('formal_failure', 'second_case_failure'):
                assert target.is_dir(), '本轮应保留原来的已安装目录行为'
            if scenario == 'missing_read_only':
                assert not (lake/'raw').exists()
            if label == 'after':
                assert 'planning_progress: artifact=position_rank_special_cases; function=main; phase=run; status=started' in result.output
                assert '=' * 88 in result.output
                assert 'case_index=1/' in result.output and 'elapsed_s=' in result.output
                if expected_success:
                    assert 'phase=run; status=completed' in result.output
                    assert 'phase=run; status=failed' not in result.output
                else:
                    assert 'phase=run; status=failed' in result.output
                    assert 'phase=run; status=completed' not in result.output
                if scenario == 'missing_write':
                    assert result.output.index('phase=formal_verify; status=started') < result.output.index('special_case_committed:')
                if scenario == 'missing_read_only':
                    assert 'phase=commit; status=skipped' in result.output
                    assert 'special_case_committed:' not in result.output
                if scenario == 'formal_failure':
                    assert 'failed_phase=formal_verify' in result.output
                    assert 'special_case_committed:' not in result.output
                (SNAPSHOT/f'{scenario}.log').write_text(result.output, encoding='utf8')
            evidence.append((result.exit_code, type(result.exception), str(result.exception), request.call_count, file_bytes(lake)))
        assert evidence[0] == evidence[1], scenario
        checks.append(scenario + ': same exit/exception/request count/artifact bytes')

hashes = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed = [name for name, digest in hashes.items() if hashlib.sha256((ROOT/name).read_bytes()).hexdigest() != digest]
assert set(changed) == {str(PATH.relative_to(ROOT)), str(PATH.with_suffix('.ipynb').relative_to(ROOT))}, changed
checks.append('only target production notebook and export changed')
report = {'checks_passed': len(checks), 'checks': checks, 'changed_files': changed, 'real_api_calls': 0, 'formal_lake_writes': 0}
(SNAPSHOT/'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
