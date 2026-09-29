"""a03/b02 第 5—6 项：函数内真实进度与提交状态日志，不改变业务操作。"""
import ast
import copy
import hashlib
import io
import json
import pathlib
import shutil
import tempfile
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.ipynb')
PATH = ROOT/RELATIVE
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id:c for c in notebook.cells}
assert 'import time' not in cells['0402fa68'].source
cells['0402fa68'].source = cells['0402fa68'].source.replace('import sys\n', 'import sys\nimport time\n')
# Reuse only the local draft editing class, without executing another workflow's migration.
editing_source = (ROOT/'00_draft_collection_02/scripts/update_a03_b01_function_logs_20260928.py').read_text(encoding='utf8')
exec(editing_source[editing_source.index('class FunctionEdit:'):editing_source.index('\ndef event(')])

def event(function, phase, status, fields='', prefix='planning_progress', err=False):
    return f'''click.echo(
    f"{prefix}: dataset={{DATASET_NAME}}; function={function}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields else ''}elapsed_s={{time.perf_counter() - log_started_at:.3f}}"{', err=True' if err else ''}
)'''

def boundary(edit, phase, fields='', setup='', failure_fields=''):
    edit.wrap(
        f'log_started_at = time.perf_counter()\nlog_phase = "{phase}"\n{setup}\n'
        + event(edit.function.name, phase, 'started', fields),
        'except Exception as log_error:\n' + textwrap.indent(event(
            edit.function.name, phase, 'failed',
            f'failed_phase={{log_phase}}; error={{type(log_error).__name__}}{failure_fields}'
        )+'\nraise', '    '))

edit = FunctionEdit('2764b9ee', 'open_exact_dataset')
edit.insert('dataset = ds.dataset', 'log_phase = "dataset_open"')
edit.insert('if not reconstructed_schema', 'log_phase = "schema"')
edit.insert('for fragment in dataset.get_fragments():', 'log_phase = "fragment_schema"')
edit.insert('if not fragment.physical_schema.equals', 'log_checked_fragments += 1\n'
    'if log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('open_exact_dataset', 'fragment_schema', 'running', 'label={label}; checked_fragments={log_checked_fragments}; files={len(parquet_files)}; materialized=false')+'\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('return dataset', event('open_exact_dataset', 'dataset_open', 'completed', 'label={label}; checked_fragments={log_checked_fragments}; files={len(parquet_files)}; materialized=false'))
boundary(edit, 'dataset_open', 'label={label}; path={table_path}; materialized=false',
         'log_checked_fragments = 0\nlog_last_progress_at = log_started_at', '; label={label}; checked_fragments={log_checked_fragments}')

edit = FunctionEdit('2764b9ee', 'validate_calendar_frame')
edit.insert('checked =', 'log_phase = "conversion"')
edit.insert('if normalized.duplicated', 'log_phase = "primary_key"')
edit.insert('for row in checked.to_pylist():', 'log_phase = "business_validation"')
edit.insert("if row['updated_at'] > now_utc:", 'log_checked_rows += 1\n'
    'if log_checked_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('validate_calendar_frame', 'validate', 'running', 'context={context}; checked_rows={log_checked_rows}/{len(frame)}')+'\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
returned = edit.find('return normalized.sort_values').value
edit.replace('return normalized.sort_values', 'log_phase = "sort"\nvalidated_calendar_df = '+ast.unparse(returned)+'\n'
    + event('validate_calendar_frame', 'validate', 'completed', 'context={context}; checked_rows={log_checked_rows}; rows={len(validated_calendar_df)}')+'\nreturn validated_calendar_df')
boundary(edit, 'validate', 'context={context}; rows={len(frame)}', 'log_checked_rows = 0\nlog_last_progress_at = log_started_at', '; context={context}; checked_rows={log_checked_rows}')

edit = FunctionEdit('e2d51f8b', 'plan_raw_grids')
edit.insert('selected_df = calendar_df.loc', 'log_phase = "select_required"')
edit.insert('for row in selected_df.to_dict', 'log_phase = "raw_inspection"\n'+event('plan_raw_grids', 'raw_inspection', 'started', 'selected_dates={len(selected_df)}; path={raw_root}'))
edit.insert('observation_date =', 'if log_checked_dates and log_checked_dates % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('plan_raw_grids', 'raw_inspection', 'running', 'checked_dates={log_checked_dates}/{len(selected_df)}; complete_grid_count={complete_count}; state_repair_count={len(repair_rows)}; api_pending_grid_count={len(pending_rows)}')+'\nlog_last_progress_at = time.perf_counter()', '    '))
edit.insert('evidence = inspect_raw_leaf', 'log_date = observation_date')
edit.insert('evidence = inspect_raw_leaf', 'log_checked_dates += 1', after=True)
edit.insert('result_columns =', 'log_phase = "plan_frames"')
returned = edit.find('return (pending_df.sort_values').value
edit.replace('return (pending_df.sort_values', 'planned_raw_grids = '+ast.unparse(returned)+'\n'
    + event('plan_raw_grids', 'plan', 'completed', 'checked_dates={log_checked_dates}; complete_grid_count={complete_count}; state_repair_count={len(repair_df)}; api_pending_grid_count={len(pending_df)}; persisted=false', prefix='reconciliation_plan')+'\nreturn planned_raw_grids')
boundary(edit, 'plan', 'calendar_rows={len(calendar_df)}; start_date={start_date}; end_date={end_date}; persisted=false',
         'log_checked_dates = 0\nlog_date = None\nlog_last_progress_at = log_started_at', '; date={log_date}; checked_dates={log_checked_dates}')

edit = FunctionEdit('e2d51f8b', 'fetch_raw_response')
edit.insert('response = session.get', 'log_phase = "http_request"')
edit.insert('response_content =', 'log_phase = "http_status"')
edit.insert('return (response_content, url)', 'log_digest = hashlib.sha256(response_content).hexdigest()\n'
    + event('fetch_raw_response', 'fetch', 'completed', 'date={observation_date}; bytes={len(response_content)}; sha256={log_digest}; url={url}; http_status=200; fetch_calls=1; persisted=false', prefix='http_success'))
boundary(edit, 'fetch', 'date={observation_date}; timeout_s={REQUEST_TIMEOUT_SECONDS}; persisted=false', failure_fields='; date={observation_date}; persisted=false')

edit = FunctionEdit('e2d51f8b', 'commit_raw_response')
edit.insert('digest =', 'log_phase = "prepare"')
edit.insert('staging_path.mkdir', 'log_phase = "staging_write"\n'+event('commit_raw_response', 'staging_write', 'started', 'date={observation_date}; bytes={len(response_content)}; persisted=false'))
edit.insert('staged_evidence =', 'log_phase = "staging_readback"')
edit.insert('if staged_evidence is None:', event('commit_raw_response', 'staging_readback', 'completed', 'date={observation_date}; bytes={staged_evidence[\'byte_count\']}; persisted=false'), after=True)
edit.insert('with StagedPathTransaction', 'log_phase = "install"\n'+event('commit_raw_response', 'install', 'started', 'date={observation_date}; target={target_path}; transaction_state=pending'))
edit.insert('committed_evidence =', 'log_phase = "formal_readback"\n'+event('commit_raw_response', 'formal_readback', 'started', 'date={observation_date}; transaction_state=pending'))
edit.insert('if committed_evidence != staged_evidence:', event('commit_raw_response', 'formal_readback', 'completed', 'date={observation_date}; bytes={committed_evidence[\'byte_count\']}; transaction_state=pending')+'\nlog_phase = "transaction_exit"', after=True)
edit.insert('return committed_evidence', event('commit_raw_response', 'commit', 'completed', 'date={observation_date}; run_id={run_id}; scope=raw_date; bytes={committed_evidence[\'byte_count\']}; sha256={committed_evidence[\'sha256\']}; persisted=true; calendar_state=not_updated', prefix='partition_committed'))
boundary(edit, 'commit', 'date={observation_date}; bytes={len(response_content)}; scope=raw_date', failure_fields='; date={observation_date}')

edit = FunctionEdit('e2d51f8b', 'preserve_failed_response')
edit.insert('failed_leaf.mkdir', 'log_phase = "write_evidence"')
edit.insert('return failed_leaf', event('preserve_failed_response', 'failure_evidence', 'completed', 'date={observation_date}; bytes={len(response_content)}; path={failed_leaf}; persisted=true; formal_completion=false', err=True))
boundary(edit, 'failure_evidence', 'date={observation_date}; bytes={len(response_content)}; formal_completion=false', failure_fields='; date={observation_date}; formal_completion=false')

for name in ('apply_calendar_completion', 'apply_calendar_failure'):
    edit = FunctionEdit('e77863a1', name)
    if name == 'apply_calendar_completion':
        edit.insert('for observation_date, evidence', 'log_phase = "update_state"')
        edit.insert("updated_df.loc[mask, 'updated_at'] =", 'log_updated_grids += 1\n'
            'if log_updated_grids % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
            + textwrap.indent(event(name, 'generate_state', 'running', 'updated_grids={log_updated_grids}/{len(evidence_by_date)}; persisted=false')+'\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
        fields = 'planned_grids={len(evidence_by_date)}; fetch_status=success'
        completed_fields = 'updated_grids={log_updated_grids}; fetch_status=success'
        setup = 'log_updated_grids = 0\nlog_last_progress_at = log_started_at'
    else:
        edit.insert('updated_df =', 'log_phase = "update_state"')
        fields = completed_fields = 'date={observation_date}; fetch_status={fetch_status}; fetch_completed=false'
        setup = ''
    returned = edit.find('return validate_calendar_frame').value
    edit.replace('return validate_calendar_frame', 'log_phase = "output_validation"\nvalidated_calendar_df = '+ast.unparse(returned)+'\n'
        + event(name, 'generate_state', 'completed', completed_fields+'; rows={len(validated_calendar_df)}; persisted=false')+'\nreturn validated_calendar_df')
    boundary(edit, 'generate_state', fields+'; persisted=false', setup, '; persisted=false')

edit = FunctionEdit('e77863a1', 'commit_calendar_partitions')
edit.insert('return 0', event('commit_calendar_partitions', 'commit', 'skipped', 'reason=no_touched_dates; calendar_state=unchanged; persisted=false; date_watermark=none'))
edit.insert('touched_mask =', 'log_phase = "select_partitions"')
edit.insert('partition_mask =', 'log_partition = partition_key\nlog_phase = "leaf_validation"\n'
    + event('commit_calendar_partitions', 'leaf_validation', 'started', 'partition={partition_key}; committed_partitions={log_committed_partitions}/{len(partition_keys)}'))
edit.insert('staging_path.mkdir', 'log_phase = "staging_write"\n'
    + event('commit_calendar_partitions', 'staging_write', 'started', 'partition={partition_key}; rows={complete_table.num_rows}; persisted=false'))
edit.insert('staged_dataset =', 'log_phase = "staging_readback"')
edit.insert('if not pandas_to_arrow(staged_df', event('commit_calendar_partitions', 'staging_readback', 'completed', 'partition={partition_key}; rows={len(staged_df)}; persisted=false'), after=True)
edit.insert('with StagedPathTransaction', 'log_phase = "install"\n'+event('commit_calendar_partitions', 'install', 'started', 'partition={partition_key}; transaction_state=pending'))
edit.insert('committed_dataset =', 'log_phase = "formal_readback"\n'+event('commit_calendar_partitions', 'formal_readback', 'started', 'partition={partition_key}; transaction_state=pending'))
edit.insert('if not pandas_to_arrow(committed_df', event('commit_calendar_partitions', 'formal_readback', 'completed', 'partition={partition_key}; rows={len(committed_df)}; transaction_state=pending')+'\nlog_phase = "transaction_exit"', after=True)
edit.insert('try:\n    with StagedPathTransaction', 'log_committed_partitions += 1\n'
    + event('commit_calendar_partitions', 'commit_leaf', 'completed', 'partition={partition_key}; rows={len(committed_df)}; committed_partitions={log_committed_partitions}/{len(partition_keys)}; scope=calendar_leaf; persisted=true', prefix='partition_committed'), after=True)
edit.insert('return len(touched_df)', event('commit_calendar_partitions', 'commit', 'completed', 'committed_partitions={log_committed_partitions}; touched_grids={len(touched_df)}')+'\n'
    'if log_committed_partitions:\n'+textwrap.indent(event('commit_calendar_partitions', 'calendar_state', 'completed', 'touched_grids={len(touched_df)}; completed_grids={int(touched_df[\'is_fetch_completed\'].sum())}; persisted=true; date_watermark=none'), '    '))
boundary(edit, 'commit', 'touched_dates={len(touched_dates)}; scope=calendar_leaves', 'log_partition = None\nlog_committed_partitions = 0', '; partition={log_partition}; committed_partitions={log_committed_partitions}')

edit = FunctionEdit('b88b5aef', 'main')
for node in ast.walk(edit.function):
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and ast.unparse(node.value.func)=='click.echo':
        edit.replace(ast.unparse(node), '')
edit.replace('digest = hashlib.sha256(response_content)', '')
edit.insert('calendar_dataset = open_exact_dataset(calendar_path, CALENDAR_PARTITIONING, EXTERNAL_MARKET_CALENDAR_SCHEMA, \'正式外部市场日历\')', 'log_phase = "calendar_read"')
edit.insert('calendar_dataset = open_exact_dataset(calendar_path, CALENDAR_PARTITIONING, EXTERNAL_MARKET_CALENDAR_SCHEMA, \'raw 状态修复后的正式外部市场日历\')', 'log_phase = "repair_readback"')
edit.insert('mode =', event('main', 'run_context', 'completed', 'mode={mode}; lake_root={resolved_lake_root}; write={str(write).lower()}'), after=True)
edit.insert('raise click.ClickException', 'log_phase = "fetch"')
for context, phase in (('正式路径读取的', 'calendar_read'), ('raw 状态修复后的正式', 'repair_readback')):
    node = next(n for n in ast.walk(edit.function) if isinstance(n, ast.Assign) and any(isinstance(t,ast.Name) and t.id=='calendar_df' for t in n.targets) and isinstance(n.value,ast.Call) and getattr(n.value.func,'id',None)=='validate_calendar_frame' and isinstance(n.value.args[-1],ast.Constant) and n.value.args[-1].value==context)
    edit.insert(ast.unparse(node), f'log_phase = "{phase}"\n'+event('main', phase, 'started', 'path={calendar_path}; materialized=false'))
    edit.insert(ast.unparse(node), event('main', phase, 'completed', 'rows={len(calendar_df)}; materialized=true'), after=True)
for node in ast.walk(edit.function):
    if isinstance(node, ast.Assign) and isinstance(node.value,ast.Call) and getattr(node.value.func,'id',None)=='plan_raw_grids':
        edit.insertions.setdefault(node.lineno-1, []).append(' '*node.col_offset+'log_phase = "plan"\n')

def finish(fields):
    return event('main', 'run', 'completed', fields+'; write={str(write).lower()}', prefix='finished')+'\nclick.echo(log_boundary)'

empty_if = edit.find('if pending_df.empty and repair_df.empty:')
# Returns share their text; insert by the existing return nodes' line positions.
first_return = next(n for n in empty_if.body if isinstance(n, ast.Return))
edit.insertions.setdefault(first_return.lineno-1, []).append(textwrap.indent(finish('outcome=up_to_date; fetch_calls=0')+'\n', ' '*first_return.col_offset))
repair_only = edit.find('if pending_df.empty:')
last_return = next(n for n in repair_only.body if isinstance(n,ast.Return))
edit.insertions.setdefault(last_return.lineno-1, []).append(textwrap.indent(finish("outcome={'state_repaired' if write else 'repair_planned'}; complete_grids={complete_count}; fetch_calls=0")+'\n', ' '*last_return.col_offset))
edit.insert('repair_evidence =', 'log_phase = "repair_plan"')
edit.insert('repair_evidence =', event('main', 'repair_plan', 'completed', 'grids={len(repair_evidence)}; write={str(write).lower()}; fetch_calls=0; persisted=false', prefix='state_repair_plan'), after=True)
edit.insert('session = create_http_session()', 'log_phase = "http_session"')
edit.insert('try:\n    response_content, url =', 'log_phase = "fetch"\nlog_date = observation_date\n'
    + event('main', 'fetch_batch', 'running', 'date={observation_date}; processed_grids={processed_grid_count}/{len(pending_df)}; bytes={total_bytes}'))
for node in ast.walk(edit.function):
    if isinstance(node,ast.Assign) and isinstance(node.value,ast.Call):
        name = getattr(node.value.func,'id',None)
        if name in ('apply_calendar_completion','apply_calendar_failure','preserve_failed_response','commit_raw_response'):
            edit.insert(ast.unparse(node), 'log_phase = "'+name+'"')
    if isinstance(node,ast.Expr) and isinstance(node.value,ast.Call) and getattr(node.value.func,'id',None)=='commit_calendar_partitions':
        edit.insertions.setdefault(node.lineno-1, []).append(' '*node.col_offset+'log_phase = "calendar_commit"\n')
edit.insert('final_calendar_dataset =', 'log_phase = "final_readback"')
edit.insert('final_calendar_df =', event('main', 'final_readback', 'started', 'path={calendar_path}; materialized=false'))
edit.insert('final_calendar_df =', event('main', 'final_readback', 'completed', 'rows={len(final_calendar_df)}; materialized=true'), after=True)
# Add final result after all original validation, before the end of the function.
edit.insertions.setdefault(edit.function.end_lineno, []).append('    '+finish("outcome={'written' if write else 'readonly'}; processed_grids={processed_grid_count}; bytes={total_bytes}").replace('\n', '\n    ')+'\n')
edit.wrap('log_started_at = time.perf_counter()\nlog_phase = "arguments"\nlog_date = None\nlog_boundary = "=" * 88\nclick.echo(log_boundary)\n'
          + event('main', 'run', 'started', 'write={str(write).lower()}; start_date={start_date}; end_date={end_date}'),
          'except Exception as log_error:\n'+textwrap.indent(event('main', 'run', 'failed', 'failed_phase={log_phase}; date={log_date}; error={type(log_error).__name__}; write={str(write).lower()}')+'\nclick.echo(log_boundary)\nraise','    '))

cells['9adb1e47'].source += '\n\n打开函数报告文件发现、fragment 检查和耗时，`materialized=false` 表示尚未读取表内容；实际 `to_table()` 的读取起止与行数由所在调用方报告。校验函数报告输入行数、已校验行数和失败阶段。fragment 每 100 个、日历每 10000 行检查一次 2 秒日志间隔，不增加扫描或校验。'
cells['74881b81'].source = cells['74881b81'].source.replace('本次只替换路径安装与恢复实现。', '')
cells['74881b81'].source += '\n\n待办函数自行报告选中日期数、已复读日期数以及已完成、需修复、需请求的数量，每 100 个日期检查一次 2 秒输出间隔。`inspect_raw_leaf()` 保持安静，由其调用方按逻辑块报告进度。采集函数自行报告起止、响应字节、摘要和失败；`http_success` 仅表示 HTTP 200 已接收，`persisted=false`。单日期 raw 提交在退出共享事务后才报告 `partition_committed`，此时日历尚未回写。隐藏失败正文由保存函数报告，并明确 `formal_completion=false`。'
cells['ac34e98a'].source += '\n\n两个状态生成函数自行报告生成和校验结果，均标明 `persisted=false`。提交函数报告各叶 staging、安装、正式复读及累计成功叶数；事务内仅报告 `transaction_state=pending`，退出当前事务后才报告该叶 `partition_committed`。全部触达叶成功后再报告 `calendar_state` 的触达数、完成数和 `date_watermark=none`；错误状态已落盘并不等于该格点已采集完成。'
cells['2951329c'].source += '\n\n`main()` 只报告本身的参数、表内容物化、无 API 修复计划、日期批次进度和整次运行结果；HTTP 成功、失败正文留存和正式提交日志由对应函数负责。运行边界使用 88 个等号，日志统一包含 `function/phase/status/elapsed_s`；耗时为当前函数调用累计值。函数异常原样向上传播，不因日志重试。'
for cell_id, replacements in {
    'a03-b02-flow-raw-transaction': [('日历 required 日期；复读 raw 原文与摘要', '报告待办生成；计数复读 raw 原文与摘要'), ('请求该日原始字节；保留现有适配器重试', '函数报告采集起止；保留现有适配器重试'), ('退出事务成功；返回 raw 证据', '退出事务后报告 raw 落盘；返回证据')],
    'a03-b02-flow-calendar-transaction': [('生成相应日历状态', '函数报告状态生成；尚未落盘'), ('成功退出当前事务；处理下一叶', '退出事务后报告当前叶成功；处理下一叶'), ('返回触达行数；不写独立水位', '报告日历落盘与完成数；无独立水位')],
}.items():
    for old_text, new_text in replacements:
        assert old_text in cells[cell_id].source
        cells[cell_id].source = cells[cell_id].source.replace(old_text,new_text)

snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b02-logs-before-'))
hashes = {}
for path in [ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md', ROOT/'02_Futures_Lakehouse/README.md',
             ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py',
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')),
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')),
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py'))]:
    hashes[path.relative_to(ROOT).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
for path in (PATH,PATH.with_suffix('.py')):
    destination = snapshot/path.relative_to(ROOT)
    destination.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(path,destination)
(snapshot/'hashes.json').write_text(json.dumps(hashes,ensure_ascii=False,indent=2),encoding='utf8')
assert before.metadata == notebook.metadata
assert [c.id for c in before.cells] == [c.id for c in notebook.cells]
for old_cell in before.cells:
    new_cell = cells[old_cell.id]
    assert {k:v for k,v in old_cell.items() if k!='source'} == {k:v for k,v in new_cell.items() if k!='source'}
    if old_cell.cell_type=='code':
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type==tokenize.COMMENT]
        assert comments(old_cell.source) == comments(new_cell.source)
        ast.parse(new_cell.source)
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook)+'\n',encoding='utf8',newline='\n')
print(snapshot)
