"""b01a 函数日志、计数和状态归属；差分基线＋模拟请求＋临时文件。"""
import ast
import contextlib
import hashlib
import importlib.util
import io
import itertools
import json
import pathlib
import sys
import tempfile
from unittest.mock import patch

import nbformat
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


old = load('b01a_before_function_logs', SNAPSHOT/PATH.name)
new = load('b01a_after_function_logs', PATH)
fixture = PositionRankSpecialCaseCalibrationTests()
content, case = fixture.response_content(), fixture.special_case()
checks = []
logs = {}


def capture(label, function, *args):
    with contextlib.redirect_stdout(io.StringIO()) as output:
        result = function(*args)
    logs[label] = output.getvalue()
    return result, output.getvalue()


def failure(label, function, *args):
    try:
        with contextlib.redirect_stdout(io.StringIO()) as output:
            function(*args)
    except Exception as error:
        logs[label] = output.getvalue()
        return error, output.getvalue()
    raise AssertionError(label + ': expected controlled failure')


manifest, log = capture('response_generation', new.validate_response, content, case)
assert manifest == old.validate_response(content, case)
assert 'function=validate_response; phase=generate_manifest; status=completed' in log
assert 'persisted=false' in log and 'persisted=true' not in log
assert 'scanned_source_rows=21/21; matched_rows=20' in log
checks.append('direct response generation and parser report their own completion; identical manifest')

for label, payload, supplied_case, expected_phase in (
    ('hash_rejection', b'different', case, 'response_digest'),
    ('top_twenty_rejection', content, dict(case, official_rows=()), 'parse_official_rows'),
):
    old_error, _ = failure('old_'+label, old.validate_response, payload, supplied_case)
    new_error, log = failure(label, new.validate_response, payload, supplied_case)
    assert type(old_error) is type(new_error) and str(old_error) == str(new_error)
    assert f'failed_phase={expected_phase}' in log
    assert 'function=validate_response; phase=generate_manifest; status=completed' not in log
checks.append('digest and frozen Top 20 rejection preserve original exceptions and no generation success')

payload = json.loads(content)
payload['o_cursor'].extend([payload['o_cursor'][-1]]*980)
long_content = json.dumps(payload, ensure_ascii=False).encode('utf8')
with patch.object(new.time, 'perf_counter', side_effect=itertools.count(0, 3)):
    rows, log = capture('parser_progress', new.parse_official_volume_rows, long_content, case)
assert rows == case['official_rows']
assert 'scanned_source_rows=1000/1001; matched_rows=20' in log
checks.append('existing parse loop reports bounded 1000/1001 progress without an extra traversal')

with patch.object(new.requests, 'get', return_value=FakeResponse(content)) as request:
    (response, fetched_manifest), log = capture('fetch', new.fetch_official_response, case)
assert response.content == content and fetched_manifest == manifest
request.assert_called_once_with(case['official_url'], timeout=60)
assert 'special_case_source_valid: artifact=position_rank_special_cases; function=fetch_official_response' in log
checks.append('direct fetch reports request/response/source completion with one request')
with patch.object(new.requests, 'get', return_value=FakeResponse(b'blocked', 503)) as request:
    error, log = failure('fetch_http_failure', new.fetch_official_response, case)
assert isinstance(error, RuntimeError) and request.call_count == 1
assert 'failed_phase=request' in log and 'special_case_source_valid:' not in log
checks.append('HTTP rejection has a function-owned failure and no retry')


def stored_bytes(root):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob('*') if p.is_file()}


def seed(target, supplied_case):
    target.mkdir(parents=True)
    source_manifest = old.validate_response(content, supplied_case)
    (target/'response.dat').write_bytes(content)
    (target/'response.sha256').write_text(source_manifest['official_response_sha256']+'\n', encoding='ascii', newline='\n')
    (target/'calibration.json').write_text(json.dumps(source_manifest, ensure_ascii=False, indent=2)+'\n', encoding='utf8', newline='\n')


with tempfile.TemporaryDirectory(prefix='b01a-function-check-') as temporary:
    temporary = pathlib.Path(temporary)
    target = temporary/'direct/raw'/str(case['raw_relative_path'])
    _, log = capture('direct_commit', new.commit_artifacts, target, content, manifest, case)
    for n in range(1, 4):
        assert f'written_files={n}/3' in log
    assert log.rindex('function=verify_artifacts; phase=verify; status=completed') < log.index('special_case_committed:') < log.index('phase=artifact_state; status=completed')
    assert 'date_watermark=none' in log
    checks.append('direct commit reports file counts and publishes persisted state only after formal verification')
    read_calls = []
    original_bytes, original_text = pathlib.Path.read_bytes, pathlib.Path.read_text
    def read_bytes(path, *args, **kwargs):
        read_calls.append(path.name)
        return original_bytes(path, *args, **kwargs)
    def read_text(path, *args, **kwargs):
        read_calls.append(path.name)
        return original_text(path, *args, **kwargs)
    with patch.object(pathlib.Path, 'read_bytes', read_bytes), patch.object(pathlib.Path, 'read_text', read_text):
        verified, log = capture('direct_verify', new.verify_artifacts, target, case)
    assert verified == manifest
    assert read_calls == ['response.dat', 'response.sha256', 'calibration.json']
    for n in range(4):
        assert f'verified_files={n}/3' in log
    checks.append('direct evidence verification reads each of three files once; log counts follow validation')
    (target/'response.sha256').write_text('wrong', encoding='ascii')
    error, log = failure('sidecar_failure', new.verify_artifacts, target, case)
    assert isinstance(error, ValueError) and 'failed_phase=sidecar; verified_files=1/3' in log
    assert 'function=verify_artifacts; phase=verify; status=completed' not in log
    checks.append('failed sidecar does not increment verified files or report verification completion')

    scenarios = ('missing_read_only', 'missing_write', 'existing_read_only', 'existing_write', 'mixed_read_only',
                 'existing_invalid', 'http_failure', 'hash_mismatch', 'staging_failure', 'formal_failure', 'second_case_failure')
    for scenario in scenarios:
        results = []
        for label, module in (('before', old), ('after', new)):
            lake = temporary/scenario/label
            lake.mkdir(parents=True)
            cases = [case]
            if scenario in ('mixed_read_only', 'second_case_failure'):
                cases.append(dict(case, case_id='second_case', raw_relative_path='shfe/position_rank_special_cases/second_case'))
            artifact_path = lake/'raw'/str(case['raw_relative_path'])
            if scenario.startswith('existing') or scenario == 'mixed_read_only':
                seed(artifact_path, case)
                if scenario == 'existing_invalid':
                    (artifact_path/'response.sha256').write_text('wrong', encoding='ascii')
            original_verify = module.verify_artifacts
            def verify(path, supplied_case):
                if scenario == 'staging_failure' and path.name.startswith('.'):
                    raise ValueError('injected staging failure')
                if scenario == 'formal_failure' and path == artifact_path:
                    raise ValueError('injected formal failure')
                return original_verify(path, supplied_case)
            responses = [FakeResponse(content)]
            if scenario.startswith('existing'):
                responses = []
            elif scenario == 'http_failure':
                responses = [FakeResponse(b'blocked', 503)]
            elif scenario == 'hash_mismatch':
                responses = [FakeResponse(b'different')]
            elif scenario == 'second_case_failure':
                responses.append(FakeResponse(b'blocked', 503))
            args = ['--lake-root', str(lake)]
            if not scenario.endswith('read_only'):
                args.append('--write')
            with patch.object(module, 'POSITION_RANK_SPECIAL_CASES', tuple(cases)), patch.object(module.requests, 'get', side_effect=responses) as request, patch.object(module, 'verify_artifacts', side_effect=verify):
                result = CliRunner().invoke(module.main, args)
            success = scenario in ('missing_read_only', 'missing_write', 'existing_read_only', 'existing_write', 'mixed_read_only')
            assert (result.exit_code == 0) == success, (scenario, label, result.exception)
            expected_requests = 0 if scenario.startswith('existing') else 2 if scenario == 'second_case_failure' else 1
            assert request.call_count == expected_requests
            assert not list(lake.rglob('*.staging-*'))
            if scenario in ('formal_failure', 'second_case_failure'):
                assert artifact_path.is_dir(), '本轮不改变已安装目录保留语义'
            if scenario == 'missing_read_only':
                assert not (lake/'raw').exists()
            if label == 'after':
                logs[scenario] = result.output
                if success:
                    ready = scenario not in ('missing_read_only', 'mixed_read_only')
                    assert f'position_rank_special_cases_ready: {str(ready).lower()}' in result.output
                    assert 'phase=run; status=completed' in result.output
                    if scenario == 'mixed_read_only':
                        assert 'existing_verified_cases=1; committed_cases=0; source_only_cases=1' in result.output
                else:
                    assert 'position_rank_special_cases_ready:' not in result.output
                    assert 'phase=run; status=completed' not in result.output
                    assert 'phase=run; status=failed' in result.output
                assert 'function=main; phase=source_request;' not in result.output
                assert 'function=main; phase=generate_manifest;' not in result.output
                assert 'function=main; phase=commit; status=started' not in result.output
                assert 'function=main; phase=commit; status=completed' not in result.output
                if scenario in ('staging_failure', 'formal_failure'):
                    assert 'function=commit_artifacts; phase=commit; status=failed' in result.output
                    assert 'special_case_committed:' not in result.output
                    assert 'persisted=true' not in result.output
                if scenario == 'formal_failure':
                    assert 'failed_phase=formal_verify' in result.output
            results.append((result.exit_code, type(result.exception), str(result.exception), request.call_count, stored_bytes(lake)))
        assert results[0] == results[1], scenario
        checks.append(scenario + ': exception/request/artifact equivalence and correct readiness/log ownership')


class RemoveLogging(ast.NodeTransformer):
    def visit_Expr(self, node):
        return None if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo' else self.generic_visit(node)
    def visit_Assign(self, node):
        return None if all(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets) else self.generic_visit(node)
    def visit_AugAssign(self, node):
        return None if isinstance(node.target, ast.Name) and node.target.id.startswith('log_') else self.generic_visit(node)
    def visit_If(self, node):
        node = self.generic_visit(node)
        return None if not node.body and not node.orelse else node
    def visit_Try(self, node):
        logged = len(node.handlers) == 1 and node.handlers[0].name == 'log_error'
        node = self.generic_visit(node)
        return node.body if logged else node


old_tree = ast.parse((SNAPSHOT/PATH.name).read_text(encoding='utf8'))
new_tree = ast.parse(PATH.read_text(encoding='utf8'))
old_functions = {n.name:n for n in old_tree.body if isinstance(n, ast.FunctionDef)}
new_functions = {n.name:n for n in new_tree.body if isinstance(n, ast.FunctionDef)}
for name in ('parse_official_volume_rows', 'calibration_manifest', 'verify_artifacts', 'validate_response'):
    original = RemoveLogging().visit(old_functions[name])
    changed = RemoveLogging().visit(new_functions[name])
    if name == 'validate_response':
        assert isinstance(changed.body[-2], ast.Assign) and ast.unparse(changed.body[-2].value).startswith('calibration_manifest(')
        changed.body[-1].value = changed.body[-2].value
        changed.body.pop(-2)
    assert ast.dump(original) == ast.dump(changed), name
checks.append('all four original validation/manifest functions retain business AST after removing logs')
before = nbformat.read(SNAPSHOT/PATH.with_suffix('.ipynb').name, as_version=4)
after = nbformat.read(PATH.with_suffix('.ipynb'), as_version=4)
assert before.metadata == after.metadata
cells = {c.id:c for c in after.cells}
for c in before.cells:
    assert {k:v for k,v in c.items() if k!='source'} == {k:v for k,v in cells[c.id].items() if k!='source'}
assert next(c for c in before.cells if c.id=='30c24e87') == cells['30c24e87']
checks.append('existing cell state and execution entry unchanged')
hashes = json.loads((SNAPSHOT/'hashes.json').read_text(encoding='utf8'))
changed_files = [name for name,digest in hashes.items() if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=digest]
assert set(changed_files) == {str(PATH.relative_to(ROOT)), str(PATH.with_suffix('.ipynb').relative_to(ROOT))}
checks.append('only target production notebook and export changed')
for label, log in logs.items():
    (SNAPSHOT/f'{label}.log').write_text(log, encoding='utf8')
report = dict(checks_passed=len(checks), checks=checks, changed_files=changed_files, real_api_calls=0, formal_lake_writes=0)
(SNAPSHOT/'verification.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf8')
print(json.dumps(report, ensure_ascii=False, indent=2))
