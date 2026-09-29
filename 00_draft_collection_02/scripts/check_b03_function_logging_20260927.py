"""本轮离线日志归位检查：复用 b03 既有临时湖与模拟来源。"""

import contextlib
import hashlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
from datetime import date
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "b03_existing_tests", ROOT / "00_draft_collection_02/tests/test_futures_contract_calendar_builder.py"
)
tests = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tests)
module = tests.contract_calendar
fixture = tests.ContractCalendarBuilderTest()
fixture.setUp()
evidence_dir = pathlib.Path(sys.argv[1])
evidence_dir.mkdir(parents=True, exist_ok=True)
checks = []


def save(name, output):
    (evidence_dir / f"{name}.log").write_text(output, encoding="utf-8")
    checks.append(name)


def leaf_hashes(lake_root):
    table_root = lake_root / "silver" / module.TABLE_NAME
    return {str(p.relative_to(table_root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in table_root.rglob('*.parquet')}


output = io.StringIO()
with contextlib.redirect_stdout(output), patch(
    "config.jqdata_connection.authenticate_jqdata", side_effect=AssertionError("empty request must not authenticate")
):
    _, dates, summary = module.collect_source_data(fixture.variety_frame([date(2024, 1, 2)]).iloc[:0], [])
assert dates == {} and summary['api_call_count'] == 0
assert output.getvalue().count('phase=collect; status=started') == 1
assert output.getvalue().count('phase=collect; status=completed') == 1
save('empty_collection', output.getvalue())

# 人工把批次容量降为 1，仅用两个既有固定月份合约验证多批进度；正式值仍为 200。
assert module.INFO_BATCH_SIZE == 200
source = tests.FakeJQData(fixture.two_valid_securities_df)
output = io.StringIO()
with contextlib.redirect_stdout(output), patch('config.jqdata_connection.authenticate_jqdata', return_value=source), patch.object(module, 'INFO_BATCH_SIZE', 1):
    _, _, summary = module.collect_source_data(fixture.variety_frame([date(2024, 1, 2)]), [date(2024, 1, 2)])
assert summary == {'api_call_count': 4, 'contract_count': 2, 'info_batch_count': 2, 'boundary_trade_day_call_count': 1}
assert len(source.get_futures_info_calls) == 2
assert 'completed=1; total=2' in output.getvalue() and 'completed=2; total=2' in output.getvalue()
assert output.getvalue().count('outcome=source_ready') == 1
save('two_info_batches', output.getvalue())

source = tests.FakeJQData(fixture.valid_securities_df)
source_error = RuntimeError('injected API failure')
output = io.StringIO()
with contextlib.redirect_stdout(output), patch('config.jqdata_connection.authenticate_jqdata', return_value=source), patch.object(source, 'get_futures_info', side_effect=source_error) as api:
    try:
        module.collect_source_data(fixture.variety_frame([date(2024, 1, 2)]), [date(2024, 1, 2)])
    except RuntimeError as error:
        assert error is source_error
    else:
        raise AssertionError('API exception was swallowed')
api.assert_called_once()
assert 'phase=collect; status=failed; failed_phase=contract_info_request' in output.getvalue()
assert 'phase=collect; status=completed' not in output.getvalue()
save('collection_failure', output.getvalue())

output = io.StringIO()
with contextlib.redirect_stdout(output):
    built, audit = fixture.build_frame_with_securities([date(2024, 1, 2), date(2024, 1, 3)], fixture.valid_securities_df)
assert len(built) == 4 and audit['candidate_contract_day_count'] == 2
assert output.getvalue().count('phase=build; status=started') == 1
assert output.getvalue().count('phase=build; status=completed') == 1
assert 'completed=2; total=2' in output.getvalue()
save('direct_build', output.getvalue())

output = io.StringIO()
build_error = ValueError('injected output validation failure')
with contextlib.redirect_stdout(output), patch.object(module, 'validate_contract_calendar_frame', side_effect=build_error):
    try:
        fixture.build_frame_with_securities([date(2024, 1, 2)], fixture.valid_securities_df)
    except ValueError as error:
        assert error is build_error
    else:
        raise AssertionError('build exception was swallowed')
assert 'phase=build; status=failed' in output.getvalue() and 'failed_phase=output_validation' in output.getvalue()
assert 'phase=build; status=completed' not in output.getvalue()
save('build_failure', output.getvalue())

with tempfile.TemporaryDirectory(prefix='b03-function-logs-check-') as temporary:
    lake_root = pathlib.Path(temporary)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        committed_count = module.commit_partition(built, lake_root, ('XSGE', 2024, 1))
    assert committed_count == 4
    assert output.getvalue().count('partition_committed:') == 1
    assert 'phase=commit; status=failed' not in output.getvalue()
    save('direct_commit', output.getvalue())
    old_hashes = leaf_hashes(lake_root)
    replacement = built.copy()
    replacement['tick_size'] = 2.0
    original_validate = module.validate_compatible_dataset_schema
    original_echo = module.click.echo

    def reject_formal(actual, expected, context):
        if context.startswith('正式分区'):
            raise TypeError('injected formal readback failure')
        return original_validate(actual, expected, context)

    def echo_after_recovery(message, *args, **kwargs):
        if 'phase=commit; status=failed' in str(message) and 'failed_phase=' in str(message):
            assert leaf_hashes(lake_root) == old_hashes
            assert not list((lake_root / 'silver').glob('.c03s-*'))
        return original_echo(message, *args, **kwargs)

    output = io.StringIO()
    with contextlib.redirect_stdout(output), patch.object(module, 'validate_compatible_dataset_schema', side_effect=reject_formal), patch.object(module.click, 'echo', side_effect=echo_after_recovery):
        try:
            module.commit_partition(replacement, lake_root, ('XSGE', 2024, 1))
        except RuntimeError as error:
            assert isinstance(error.__cause__, TypeError)
        else:
            raise AssertionError('commit failure was swallowed')
    assert leaf_hashes(lake_root) == old_hashes
    assert 'failed_phase=formal_readback' in output.getvalue() and 'phase=rollback; status=completed' in output.getvalue()
    assert 'partition_committed:' not in output.getvalue()
    save('commit_failure_after_recovery', output.getvalue())

    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        module.write_automatic_tail_processed_through(lake_root, date(2024, 1, 3))
    assert module.read_automatic_tail_processed_through(lake_root / 'silver' / module.TABLE_NAME) == date(2024, 1, 3)
    assert output.getvalue().count('outcome=watermark_committed') == 1
    save('direct_watermark', output.getvalue())
    marker = lake_root / 'silver' / module.TABLE_NAME / 'schema.parquet'
    marker_bytes = marker.read_bytes()
    watermark_error = PermissionError('injected watermark install failure')
    output = io.StringIO()
    with contextlib.redirect_stdout(output), patch.object(module.os, 'replace', side_effect=watermark_error):
        try:
            module.write_automatic_tail_processed_through(lake_root, date(2024, 1, 4))
        except PermissionError as error:
            assert error is watermark_error
        else:
            raise AssertionError('watermark failure was swallowed')
    assert marker.read_bytes() == marker_bytes and not list(marker.parent.glob('.c03m-*'))
    assert 'phase=watermark; status=failed; failed_phase=install' in output.getvalue()
    assert 'outcome=watermark_committed' not in output.getvalue()
    save('watermark_failure', output.getvalue())

with tempfile.TemporaryDirectory(prefix='b03-function-logs-cli-') as temporary:
    lake_root = pathlib.Path(temporary)
    fixture.write_variety_calendar(lake_root, [date(2024, 1, 2)])
    result, authenticate = fixture.invoke_with_jqdata(lake_root, ['--write'], tests.FakeJQData(fixture.valid_securities_df))
    assert result.exit_code == 0, result.output
    assert result.output.count('outcome=source_ready') == 1
    assert result.output.count('outcome=watermark_committed') == 1
    assert result.output.count('partition_committed:') == 1
    assert result.output.count('phase=commit_batch; status=completed') == 1
    assert 'phase=commit_batch; status=running; completed=1; total=1' in result.output
    assert 'phase=run; status=completed' in result.output
    save('cli_no_duplicate_operation_summaries', result.output)

regression_output, regression_result = io.StringIO(), io.StringIO()
with contextlib.redirect_stdout(regression_output):
    suite = unittest.defaultTestLoader.loadTestsFromModule(tests)
    result = unittest.TextTestRunner(stream=regression_result, verbosity=2).run(suite)
(evidence_dir / 'existing_regression.log').write_text(regression_result.getvalue(), encoding='utf-8')
assert result.wasSuccessful(), regression_result.getvalue()
summary = {'logging_checks': checks, 'existing_tests_passed': result.testsRun, 'real_api_calls': 0, 'formal_lake_writes': 0}
(evidence_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
print(json.dumps(summary, ensure_ascii=False))
