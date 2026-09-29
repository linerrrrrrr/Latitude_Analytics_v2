"""a02/b02 第 5—6 项：函数拥有进度，保持业务语句及 I/O 顺序。"""
import ast
import json
import pathlib
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b02_futures_holding_reports.ipynb'
notebook = json.loads(PATH.read_text(encoding='utf8'))
cells = {c['id']: c for c in notebook['cells']}


class FunctionEdit:
    def __init__(self, name):
        self.name = name
        matches = [(c, n) for c in notebook['cells'] if c['cell_type'] == 'code'
                   for n in ast.parse(''.join(c['source'])).body if isinstance(n, ast.FunctionDef) and n.name == name]
        assert len(matches) == 1, name
        self.cell, self.function = matches[0]
        self.lines = ''.join(self.cell['source']).splitlines(keepends=True)
        self.insertions, self.replacements = {}, {}

    def find(self, prefix):
        nodes = [n for n in ast.walk(self.function) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)]
        assert len(nodes) == 1, (self.name, prefix, len(nodes))
        return nodes[0]

    def insert_node(self, node, source, after=False):
        index = node.end_lineno if after else node.lineno - 1
        self.insertions.setdefault(index, []).append(textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * node.col_offset))

    def insert(self, prefix, source, after=False):
        self.insert_node(self.find(prefix), source, after)

    def replace_node(self, node, source):
        source = textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * node.col_offset) if source else ''
        self.replacements[node.lineno - 1] = (node.end_lineno, source)

    def replace(self, prefix, source):
        self.replace_node(self.find(prefix), source)

    def capture_return(self, name, log):
        node = max((n for n in ast.walk(self.function) if isinstance(n, ast.Return)), key=lambda n: n.lineno)
        source = textwrap.dedent(''.join(self.lines[node.lineno - 1:node.end_lineno]))
        self.replace_node(node, source.replace('return ', name + ' = ', 1).rstrip() + '\n' + log + '\nreturn ' + name)

    def render(self, start, end):
        parts = []
        index = start
        while index < end:
            parts.extend(self.insertions.get(index, []))
            if index in self.replacements:
                index, source = self.replacements[index]
                parts.append(source)
            else:
                parts.append(self.lines[index]); index += 1
        parts.extend(self.insertions.get(end, []))
        return ''.join(parts)

    def apply(self, phase=None, table='log_table_name', fields='', setup=''):
        if phase is None:
            source = self.render(0, len(self.lines))
        else:
            first = self.function.body[0]
            start = first.lineno - 1
            while start > self.function.lineno and (not self.lines[start - 1].strip() or self.lines[start - 1].lstrip().startswith('#')):
                start -= 1
            end = self.function.end_lineno
            prefix = 'log_started_at = perf_counter()\n'
            if 'log_last_progress_at' in self.render(start, end):
                prefix += 'log_last_progress_at = log_started_at\n'
            prefix += 'log_phase = ' + repr(phase) + '\n'
            if setup:
                prefix += setup.strip() + '\n'
            prefix += event(self.name, phase, 'started', table, fields) + '\n'
            failure = 'except Exception as log_error:\n' + textwrap.indent(
                event(self.name, phase, 'failed', table, 'failed_phase={log_phase}; error={type(log_error).__name__}') + '\nraise\n', '    ')
            source = (''.join(self.lines[:start]) + textwrap.indent(prefix, '    ') + '    try:\n'
                      + textwrap.indent(self.render(start, end).rstrip() + '\n', '    ')
                      + textwrap.indent(failure, '    ') + ''.join(self.lines[end:]))
        ast.parse(source)
        self.cell['source'] = source.splitlines(keepends=True)


def event(function, phase, status, table='log_table_name', fields='', prefix='planning_progress'):
    return f'''click.echo(
    f"{prefix}: table={{{table}}}; function={function}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields else ''}elapsed_s={{perf_counter() - log_started_at:.3f}}"
)'''


def progress(function, phase, counter, fields, table='log_table_name', every=1000):
    frequency = f'{counter} % {every} == 0 and ' if every > 1 else ''
    return f'''if {frequency}perf_counter() - log_last_progress_at >= 2.0:
    log_last_progress_at = perf_counter()
{textwrap.indent(event(function, phase, 'running', table, fields), '    ')}'''


schema_name = 'log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")'
facts_name = 'log_table_name = POSITION_TABLE_NAME + "+" + MEMBER_TABLE_NAME'

edit = FunctionEdit('open_exact_dataset')
edit.insert('dataset = ds.dataset(', 'log_phase = "open_dataset"\n' + event(edit.name, 'read_dataset', 'running', fields='path={table_path}; discovered_files={len(parquet_files)}'))
edit.insert('return dataset', event(edit.name, 'read_dataset', 'completed', fields='path={table_path}; discovered_files={len(parquet_files)}; outcome=opened'))
edit.apply('read_dataset', fields='path={table_path}; label={label}', setup=schema_name)

edit = FunctionEdit('open_optional_exact_dataset')
edit.insert('return None', 'click.echo(\n    f"planning_progress: function=open_optional_exact_dataset; phase=read_dataset; status=skipped; "\n    f"path={table_path}; label={label}; reason=no_parquet; elapsed_s=0.000"\n)')
edit.apply()

edit = FunctionEdit('dataset_grid_count_map')
edit.insert('return {}', event(edit.name, 'count_grids', 'completed', fields='batches=0; scanned_rows=0; grids=0; outcome=no_dataset'))
edit.insert('grid_df = record_batch.to_pandas()', 'log_phase = "count_batch"')
loop = edit.find('for grid_key, count in batch_counts.items():')
edit.insert_node(loop, 'log_scanned_rows += record_batch.num_rows\nlog_batch_count += 1\n' + progress(edit.name, 'count_grids', 'log_batch_count', 'batches={log_batch_count}; scanned_rows={log_scanned_rows}; grids={len(counts_by_grid)}', every=1), after=True)
edit.insert('return counts_by_grid', event(edit.name, 'count_grids', 'completed', fields='batches={log_batch_count}; scanned_rows={log_scanned_rows}; grids={len(counts_by_grid)}'))
edit.apply('count_grids', setup='log_table_name = (dataset.schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8") if dataset is not None else "absent_facts"\nlog_scanned_rows = 0\nlog_batch_count = 0')

edit = FunctionEdit('read_complete_partition')
for node in ast.walk(edit.function):
    if isinstance(node, ast.Return) and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == 'empty_pandas':
        edit.insert_node(node, event(edit.name, 'read_leaf', 'completed', fields='key={partition_key}; path={table_path}; rows=0; outcome=empty'))
edit.insert('marker_file = pq.ParquetFile(', 'log_phase = "read_marker"')
edit.insert('physical_tables = []', 'log_phase = "read_files"\n' + event(edit.name, 'read_leaf', 'running', fields='key={partition_key}; path={leaf_path}; files=0/{len(leaf_files)}'))
edit.insert('physical_tables.append(', 'log_read_files += 1\n' + progress(edit.name, 'read_leaf', 'log_read_files', 'key={partition_key}; files={log_read_files}/{len(leaf_files)}', every=1), after=True)
edit.insert('return validator(', 'log_phase = "validate_leaf"')
edit.capture_return('validated_partition_df', event(edit.name, 'read_leaf', 'completed', fields='key={partition_key}; path={table_path}; files={log_read_files}/{len(leaf_files)}; rows={len(validated_partition_df)}'))
edit.apply('read_leaf', fields='key={partition_key}; path={table_path}; label={label}', setup=schema_name + '\nlog_read_files = 0')

edit = FunctionEdit('verify_special_case_artifacts')
edit.insert('response_sha256 = hashlib.sha256(', 'log_phase = "response_hash"')
edit.insert('sidecar_sha256 = ', 'log_phase = "sidecar"\n' + event(edit.name, 'verify_raw', 'running', 'POSITION_TABLE_NAME', "case_id={special_case['case_id']}; verified_files=1/3"))
edit.insert('expected_manifest = ', 'log_phase = "manifest"\n' + event(edit.name, 'verify_raw', 'running', 'POSITION_TABLE_NAME', "case_id={special_case['case_id']}; verified_files=2/3"))
edit.insert_node(edit.function.body[-1], event(edit.name, 'verify_raw', 'completed', 'POSITION_TABLE_NAME', "case_id={special_case['case_id']}; verified_files=3/3"), after=True)
edit.apply('verify_raw', 'POSITION_TABLE_NAME', "case_id={special_case['case_id']}; verified_files=0/3")

edit = FunctionEdit('apply_position_rank_special_cases')
edit.insert('normalized_codes = ', 'log_phase = "match_payload"\nlog_matched_cases += 1\n' + event(edit.name, 'special_case', 'running', 'POSITION_TABLE_NAME', "case_id={special_case['case_id']}; grid={grid}"))
node = edit.find("if actual_rows == special_case['official_rows']:")
edit.insert_node(node.body[0], event(edit.name, 'special_case', 'running', 'POSITION_TABLE_NAME', "case_id={special_case['case_id']}; outcome=already_official; persisted=false"))
edit.insert('verify_special_case_artifacts(', 'log_phase = "verify_raw"')
edit.insert('official_rows_by_rank = ', 'log_phase = "calibrate"')
edit.insert('quality_warnings.append(', 'log_calibrated_cases += 1\n' + event(edit.name, 'special_case', 'running', 'POSITION_TABLE_NAME', "case_id={special_case['case_id']}; outcome=calibrated; persisted=false"), after=True)
edit.insert('return (calibrated_df, quality_warnings)', '''for log_warning in quality_warnings:
''' + textwrap.indent(event(edit.name, 'special_case', 'warning', 'POSITION_TABLE_NAME', 'grid={grid}; details={log_warning}; persisted=false', 'source_quality_warning'), '    ') + '\n' + event(edit.name, 'special_case', 'completed', 'POSITION_TABLE_NAME', 'grid={grid}; matched_cases={log_matched_cases}; calibrated_cases={log_calibrated_cases}; persisted=false'))
edit.apply('special_case', 'POSITION_TABLE_NAME', 'grid={grid}; persisted=false', setup='log_matched_cases = 0\nlog_calibrated_cases = 0')

edit = FunctionEdit('normalize_rank_response')
empty_return = next(n for n in ast.walk(edit.function) if isinstance(n, ast.Return) and isinstance(n.value, ast.Tuple) and isinstance(n.value.elts[0], ast.Call) and ast.unparse(n.value.elts[0]).startswith('empty_pandas'))
edit.insert_node(empty_return, event(edit.name, 'generate_facts', 'completed', fields='grid={grid}; source_rows=0; position_rows=0; member_rows=0; persisted=false', prefix='api_success'))
edit.insert('position_rows:', 'log_phase = "normalize_rows"\nlog_source_rows = len(response_df)')
edit.insert('metric_name = metric_name_from_rank_type(', 'log_visited_rows += 1\n' + progress(edit.name, 'generate_facts', 'log_visited_rows', 'grid={grid}; visited_rows={log_visited_rows}/{log_source_rows}'))
last_return = max((n for n in ast.walk(edit.function) if isinstance(n, ast.Return)), key=lambda n: n.lineno)
edit.replace_node(last_return, '''log_phase = "validate_facts"
validated_position_df = validate_position_frame(position_df, "JQData 转换后的")
validated_member_df = validate_member_frame(member_df, "JQData 转换后的")
for log_dataset_name, log_warnings in quality_warnings_by_dataset.items():
    if log_warnings:
''' + textwrap.indent(event(edit.name, 'generate_facts', 'warning', fields="grid={grid}; dataset={log_dataset_name}; details={' | '.join(log_warnings)}; persisted=false", prefix='source_quality_warning'), '        ') + '\n'
    + event(edit.name, 'generate_facts', 'completed', fields='grid={grid}; source_rows={len(response_df)}; position_rows={len(validated_position_df)}; member_rows={len(validated_member_df)}; persisted=false', prefix='api_success')
    + '\nreturn validated_position_df, validated_member_df, quality_warnings_by_dataset')
edit.apply('generate_facts', fields='grid={grid}; persisted=false', setup=facts_name + '\nlog_visited_rows = 0')

edit = FunctionEdit('pending_report_grids')
edit.insert('for grid_key in grid_keys:', 'log_phase = "compare_completion"\nlog_grid_count = len(grid_keys)')
edit.insert('if is_complete:', 'log_checked_grids += 1\n' + progress(edit.name, 'plan', 'log_checked_grids', 'checked_grids={log_checked_grids}/{log_grid_count}; pending_grids={len(pending_rows) + int(not is_complete)}', 'CALENDAR_TABLE_NAME'))
edit.insert('return (pending_df, complete_count)', event(edit.name, 'plan', 'completed', 'CALENDAR_TABLE_NAME', 'checked_grids={log_checked_grids}; complete_grid_count={complete_count}; pending_grid_count={len(pending_df)}', 'reconciliation_plan'))
edit.apply('plan', 'CALENDAR_TABLE_NAME', setup='log_checked_grids = 0')

edit = FunctionEdit('full_fact_partition')
edit.insert('return validator(', 'log_phase = "validate_merged_leaf"')
edit.capture_return('validated_complete_df', event(edit.name, 'merge', 'completed', fields='key={partition_key}; retained_rows={len(retained_df)}; incoming_rows={len(incoming_df)}; complete_rows={len(validated_complete_df)}; persisted=false'))
edit.apply('merge', fields='key={partition_key}; persisted=false', setup=schema_name)

edit = FunctionEdit('commit_complete_partition')
edit.insert('silver_root = ', 'log_phase = "prepare_paths"')
edit.insert('staging_path.mkdir(', 'log_phase = "staging_write"\n' + event(edit.name, 'staging_write', 'started', 'table_name', 'key={partition_key}; rows={len(complete_table)}; path={staging_path}; batch_state=pending'))
edit.insert('staged_df = ', 'log_phase = "staging_verify"\n' + event(edit.name, 'staging_verify', 'started', 'table_name', 'key={partition_key}; batch_state=pending'))
edit.insert('target_path.mkdir(', 'log_phase = "install"\n' + event(edit.name, 'install', 'started', 'table_name', 'key={partition_key}; path={destination_path}; batch_state=pending'))
edit.insert('committed_df = ', 'log_phase = "formal_verify"\n' + event(edit.name, 'formal_verify', 'started', 'table_name', 'key={partition_key}; batch_state=pending'))
edit.insert('rollback_errors = []', event(edit.name, 'rollback', 'started', 'table_name', 'key={partition_key}; failed_phase={log_phase}; backup_path={backup_path}'))
edit.insert('cleanup_recovery_paths = False', event(edit.name, 'rollback', 'failed', 'table_name', 'key={partition_key}; backup_path={backup_path}; quarantine_path={quarantine_path}; errors={rollback_errors}'), after=True)
handler = next(n for n in ast.walk(edit.function) if isinstance(n, ast.ExceptHandler) and n.name == 'commit_error')
edit.insert_node(handler.body[-1], event(edit.name, 'rollback', 'completed', 'table_name', 'key={partition_key}; outcome=recovery_branch_completed'))
edit.insert('return committed_df', event(edit.name, 'commit', 'completed', 'table_name', 'key={partition_key}; rows={len(committed_df)}; persisted=true; scope=leaf', 'partition_committed'))
edit.apply('commit', 'table_name', 'key={partition_key}; batch_state=pending')

edit = FunctionEdit('apply_calendar_completion')
edit.insert('dataset_name = row', 'log_visited_rows += 1\n' + progress(edit.name, 'generate_calendar_state', 'log_visited_rows', 'visited_rows={log_visited_rows}/{len(updated_df)}; updated_rows={log_updated_rows}; persisted=false', 'CALENDAR_TABLE_NAME'))
edit.insert("updated_df.at[index, 'updated_at'] = completed_at", 'log_updated_rows += 1', after=True)
edit.insert('return validate_calendar_frame(', 'log_phase = "validate_calendar"')
edit.capture_return('validated_calendar_df', event(edit.name, 'generate_calendar_state', 'completed', 'CALENDAR_TABLE_NAME', 'updated_rows={log_updated_rows}; grid_count={len(grid_counts)}; persisted=false; date_watermark=none'))
edit.apply('generate_calendar_state', 'CALENDAR_TABLE_NAME', 'persisted=false; date_watermark=none', setup='log_visited_rows = 0\nlog_updated_rows = 0')

edit = FunctionEdit('commit_calendar_partitions')
edit.insert('return 0', event(edit.name, 'commit_calendar', 'skipped', 'CALENDAR_TABLE_NAME', 'reason=no_grids; persisted=false'))
edit.insert('for partition_key in sorted(partition_keys):', 'log_phase = "commit_leaves"\n' + event(edit.name, 'commit_calendar', 'running', 'CALENDAR_TABLE_NAME', 'partitions=0/{len(partition_keys)}; touched_rows={len(touched_df)}; batch_state=pending'))
edit.insert('commit_complete_partition(', 'log_committed_partitions += 1\n' + event(edit.name, 'commit_calendar', 'running', 'CALENDAR_TABLE_NAME', 'partitions={log_committed_partitions}/{len(partition_keys)}; key={partition_key}; batch_state=pending'), after=True)
edit.insert('return len(touched_df)', event(edit.name, 'calendar_state', 'completed', 'CALENDAR_TABLE_NAME', 'partitions={log_committed_partitions}; touched_rows={len(touched_df)}; grid_count={len(grid_counts)}; persisted=true; date_watermark=none'))
edit.apply('commit_calendar', 'CALENDAR_TABLE_NAME', 'date_watermark=none', setup='log_committed_partitions = 0')

edit = FunctionEdit('query_rank_date_batch')
edit.insert('source_call_count += 1', 'log_phase = "request"\n' + event(edit.name, 'query', 'running', fields='exchange={exchange_code}; underlying={underlying_code}; request={source_call_count}; dates={date_group[0]}..{date_group[-1]}; request_dates={len(date_group)}; completed_dates={log_completed_dates}/{len(requested_dates)}', prefix='request_batch'), after=True)
edit.insert('if raw_df is None:', 'log_phase = "validate_response"')
edit.insert('if len(raw_df) >= RUN_QUERY_ROW_LIMIT:', 'log_phase = "split_or_accept"')
for node in ast.walk(edit.function):
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
        edit.replace_node(node, event(edit.name, 'query', 'split', fields='exchange={exchange_code}; underlying={underlying_code}; dates={len(date_group)}; left={len(left_dates)}; right={len(right_dates)}; rows={len(raw_df)}; source_calls={source_call_count}; splits={split_count}', prefix='query_batch_split'))
edit.insert('for trading_date in date_group:', 'log_completed_dates += len(date_group)\nlog_accepted_rows += len(response_df)\n' + event(edit.name, 'query', 'running', fields='exchange={exchange_code}; underlying={underlying_code}; completed_dates={log_completed_dates}/{len(requested_dates)}; source_calls={source_call_count}; splits={split_count}; accepted_rows={log_accepted_rows}', prefix='fetch_progress'), after=True)
edit.insert('return (raw_frames_by_date, source_call_count, split_count)', event(edit.name, 'query', 'completed', fields='exchange={exchange_code}; underlying={underlying_code}; dates={len(requested_dates)}; source_calls={source_call_count}; splits={split_count}; raw_rows={log_accepted_rows}; query_seconds={perf_counter() - log_started_at:.3f}', prefix='query_batch_success'))
edit.apply('query', fields='exchange={exchange_code}; underlying={underlying_code}', setup=facts_name + '\nlog_completed_dates = 0\nlog_accepted_rows = 0')

# main 只保留实际仍在入口完成的读取、汇总和月份调度日志。
edit = FunctionEdit('main')
for node in ast.walk(edit.function):
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
        text = ast.unparse(node)
        if any(prefix in text for prefix in ('reconciliation_plan:', 'query_batch_success:', 'api_success:', 'source_quality_warning:')):
            edit.replace_node(node, '')
        elif 'partition_committed:' in text:
            source = textwrap.dedent(''.join(edit.lines[node.lineno - 1:node.end_lineno]))
            source = source.replace('partition_committed: function=main; phase=commit;', 'partition_timing: function=main; phase=partition;')
            source = '\n'.join(line for line in source.splitlines() if 'commit_seconds=' not in line)
            edit.replace_node(node, source)
for prefix in ('planning_started_at =', 'query_started_at =', 'raw_row_count =', 'commit_started_at ='):
    edit.replace(prefix, '')
edit.insert('calendar_planning_df = ', 'log_phase = "read_calendar_plan"\n' + event(edit.name, 'read_calendar_plan', 'started', 'CALENDAR_TABLE_NAME', 'columns={len(CALENDAR_COMPLETION_COLUMNS)}; required_only=true'))
edit.insert('calendar_planning_df = ', event(edit.name, 'read_calendar_plan', 'completed', 'CALENDAR_TABLE_NAME', 'rows={len(calendar_planning_df)}; required_only=true'), after=True)
edit.insert('incoming_position_df = ', event(edit.name, 'assemble_month', 'started', 'POSITION_TABLE_NAME', 'key={partition_key}; persisted=false'))
edit.insert('incoming_member_df = ', event(edit.name, 'assemble_month', 'completed', 'POSITION_TABLE_NAME', 'key={partition_key}; rows={len(incoming_position_df)}; persisted=false'))
edit.insert('incoming_member_df = ', event(edit.name, 'assemble_month', 'completed', 'MEMBER_TABLE_NAME', 'key={partition_key}; rows={len(incoming_member_df)}; persisted=false'), after=True)
edit.apply()

append = {
'8d0a5cce': '\n\n`open_exact_dataset()` 自行报告发现文件、打开与物理契约核对的起止；缺失的可选事实表报告跳过。这里不为日志读取总行数。',
'a02-b02-leaf-read-heading': '\n\n窄列计数按现有 RecordBatch 累计批次与扫描行数，至多每 2 秒报告一次。完整叶读取按现有文件循环累计已读文件，并报告空表、读取、验收及失败阶段；不新增目录扫描或复读。',
'70556751': '\n\n特殊案例函数自行报告匹配与校准结果、持久化前的 warning；正式 raw 验收函数沿三次既有读取报告 `verified_files=1/3` 至 `3/3`。校准完成表示内存响应已调整，仍为 `persisted=false`。',
'5d0661f6': '\n\n生成函数自行报告输入处理、两张事实验收和输出条数；沿原行循环每 1000 行检查 2 秒进度间隔。`api_success:` 在本函数生成及验收成功后发出，标记 `persisted=false`；异常仍保留原类型及内容。',
'a1b983e9': '\n\n`pending_report_grids()` 自行报告已比较格点、完整格点及待办数，并拥有 `reconciliation_plan:` 完成日志；不为日志重新扫描事实或日历。',
'a02-b02-fact-merge-heading': '\n\n函数自行报告合并开始、保留行数、新行数、完整叶行数与失败阶段；输出仍在内存，标记 `persisted=false`。',
'5d672c31': '\n\n提交函数自行报告完整叶验收、staging 写入与复读、安装、正式复读、恢复分支和失败阶段。只有原有正式复读及清理完成后才输出 `partition_committed:`，其中 `scope=leaf` 仅代表当前叶。恢复日志如实报告原分支是否执行完成，不把它扩写成跨叶共同恢复承诺。',
'a02-b02-completion-heading': '\n\n状态生成函数报告已访问与已更新行数，返回前记 `persisted=false; date_watermark=none`；该日志不能替代日历提交成功。',
'a02-b02-calendar-commit-heading': '\n\n日历提交函数自行累计成功叶数；全部触达叶通过后才报告 `phase=calendar_state; persisted=true; date_watermark=none`。中途失败不输出整组完成状态，已经成功叶的落盘日志仍有效。',
'6f0ee3df': '\n\n查询函数自行报告每次请求的日期组、请求次数、日期二分、已完成日期与已接纳行数。`query_batch_success:` 计时现在只覆盖本函数的查询、验收和按日拆分，不包含后续校准及两张事实生成。网络等待期间不虚构百分比或新增心跳线程。',
}
for cell_id, text in append.items():
    cells[cell_id]['source'] = (''.join(cells[cell_id]['source']).rstrip() + text).splitlines(keepends=True)
source = ''.join(cells['badd061e']['source'])
source = source.replace('全部成功后输出当前月份的 `partition_committed:`。', '全部成功后输出当前月份的 `partition_timing:` 汇总。')
source = source.replace('当前 `query_batch_success` 的计时覆盖请求及逐日校准/归一化，不能解释成纯网络耗时。', '`query_batch_success` 由查询函数发出，只覆盖查询、验收和按日拆分；生成、合并、单叶提交及日历状态各由负责函数报告。')
source = source.replace('本轮保持现有日志归属；读取、转换与提交函数的独立进度及日志归位在后续第 5—6 项处理。', '`main()` 保留运行边界、实际仍在入口进行的日历窄列读取和月份拼接、只读结果与累计进度，不代报被调用函数的完成。')
cells['badd061e']['source'] = source.splitlines(keepends=True)
flow_changes = {
'a02-b02-flow-read': [('分批扫描格点窄列；累计每个格点行数', '函数沿现有批次报告扫描行数与格点计数'), ('完整叶业务验收；返回 DataFrame', '验收；函数报告文件数、行数及完成')],
'a02-b02-flow-special': [('返回响应与 warning；不修改其他榜单', '函数报告校准与 warning；返回内存响应')],
'a02-b02-flow-normalize': [('逐行识别榜单类别；核对名次、指标与身份', '逐行核对类别、名次、指标、身份并报告进度'), ('两张事实分别验收；返回事实及 warning', '两表验收；函数报告生成完成、persisted=false')],
'a02-b02-flow-plan': [('返回待办与完整数量', '函数报告完整与待办数量；返回计划')],
'a02-b02-flow-merge': [('返回待提交完整叶；尚未落盘', '函数报告合并行数；返回内存完整叶')],
'a02-b02-flow-commit': [('清理临时路径；返回正式叶', '清理临时路径；函数报告当前叶已提交')],
'a02-b02-flow-completion': [('返回待提交日历；此时尚未落盘', '函数报告状态生成完成；persisted=false')],
'a02-b02-flow-calendar-commit': [('返回触达日历行数；当前月份可报告完成', '函数报告日历状态已落盘；返回触达行数')],
'a02-b02-flow-query': [('取一个日期组；单次 run_query', '报告日期组与请求次数；单次 run_query'), ('返回按日响应、请求次数与拆分次数', '函数报告查询完成；返回按日响应和计数')],
'a02-b02-flow-cli': [('报告月份提交完成；persisted=true', '报告月份累计进度；函数已报告各自提交')],
}
for cell_id, pairs in flow_changes.items():
    source = ''.join(cells[cell_id]['source'])
    for old, new in pairs:
        assert source.count(old) == 1, (cell_id, old)
        source = source.replace(old, new)
    cells[cell_id]['source'] = source.splitlines(keepends=True)
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        ast.parse(''.join(cell['source']))
PATH.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + '\n', encoding='utf8', newline='\n')
print('Updated function-owned progress and explanations; business equivalence/export validation pending.')
