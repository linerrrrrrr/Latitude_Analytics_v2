"""b06 第 5—6 项：各函数直接报告自身进度；保留原业务与恢复顺序。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b06_futures_minute.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


class FunctionEdit:
    def __init__(self, cell_id, name):
        self.cell = cells[cell_id]
        self.function = next(n for n in ast.parse(self.cell.source).body if isinstance(n, ast.FunctionDef) and n.name == name)
        self.lines = self.cell.source.splitlines(keepends=True)
        self.insertions = {}
        self.replacements = {}

    def find(self, prefix):
        matches = [n for n in ast.walk(self.function) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)]
        assert len(matches) == 1, (self.function.name, prefix, len(matches))
        return matches[0]

    def insert(self, prefix, source, *, after=False, indent=None):
        node = self.find(prefix)
        index = node.end_lineno if after else node.lineno - 1
        indentation = node.col_offset if indent is None else indent
        self.insertions.setdefault(index, []).append(textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * indentation))

    def replace(self, prefix, source):
        node = self.find(prefix)
        self.replacements[node.lineno - 1] = (node.end_lineno, textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * node.col_offset))

    def wrap(self, setup, failure):
        first = self.function.body[0]
        start = first.end_lineno if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str) else first.lineno - 1
        if start == first.lineno - 1:
            while start > self.function.lineno and (not self.lines[start - 1].strip() or self.lines[start - 1].lstrip().startswith('#')):
                start -= 1
        end = self.function.end_lineno
        body = []
        index = start
        while index < end:
            body.extend(self.insertions.get(index, []))
            if index in self.replacements:
                index, replacement = self.replacements[index]
                body.append(replacement)
            else:
                body.append(self.lines[index])
                index += 1
        body.extend(self.insertions.get(end, []))
        self.cell.source = (
            ''.join(self.lines[:start])
            + textwrap.indent(textwrap.dedent(setup).strip() + '\n', '    ')
            + '    try:\n'
            + textwrap.indent(''.join(body).rstrip() + '\n', '    ')
            + textwrap.indent(textwrap.dedent(failure).strip() + '\n', '    ')
            + ''.join(self.lines[end:])
        )
        ast.parse(self.cell.source)


def event(name, phase, status, *, table='TABLE_NAME', fields='', prefix='planning_progress', elapsed=True):
    # 本次编辑使用的模板；生产代码保留直接 click.echo，不新增日志包装层。
    return f'''click.echo(
    f"{prefix}: table={{{table}}}; function={name}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields and elapsed else ''}{'elapsed_s={time.perf_counter() - log_started_at:.3f}' if elapsed else ''}"
)'''


def boundary(edit, phase, *, table='TABLE_NAME', fields='', setup='', initial_phase=None):
    edit.wrap(
        f'log_started_at = time.perf_counter()\nlog_phase = "{initial_phase or phase}"\n{setup}\n'
        + event(edit.function.name, phase, 'started', table=table, fields=fields, elapsed=False),
        'except Exception as log_error:\n' + textwrap.indent(event(
            edit.function.name, phase, 'failed', table=table,
            fields='failed_phase={log_phase}; error={type(log_error).__name__}',
        ) + '\nraise', '    '),
    )


edit = FunctionEdit('cf2450be', 'open_contract_dataset')
edit.insert('return None', event('open_contract_dataset', 'dataset_open', 'completed', table='table_path.name', fields='outcome=absent_optional; checked_fragments=0'))
edit.insert('dataset = ds.dataset', 'log_phase = "dataset_open"')
edit.insert('actual_schema = reconstructed_schema', 'log_phase = "logical_schema"')
edit.insert('for fragment in dataset.get_fragments():', 'log_phase = "fragment_schema"\nlog_fragment_count = 0\n' + event('open_contract_dataset', 'fragment_schema', 'started', table='table_path.name'))
edit.insert('if not schema_identity_compatible(fragment_schema', '''
    log_fragment_count += 1
    if log_fragment_count == 1 or log_fragment_count % 250 == 0:
''' + textwrap.indent(event('open_contract_dataset', 'fragment_schema', 'running', table='table_path.name', fields='checked_fragments={log_fragment_count}'), '        '), after=True)
edit.insert('return dataset', event('open_contract_dataset', 'dataset_open', 'completed', table='table_path.name', fields='outcome=ready; checked_fragments={log_fragment_count}'))
boundary(edit, 'dataset_open', table='table_path.name', fields='path={table_path}; required={str(required).lower()}', initial_phase='discovery')

edit = FunctionEdit('126fe7bc', 'partition_keys_from_dataset')
edit.insert('keys.add(tuple(values))', 'log_file_count += 1\nif log_file_count == 1 or log_file_count % 250 == 0:\n' + textwrap.indent(event('partition_keys_from_dataset', 'partition_discovery', 'running', table='table_path.name', fields='files={log_file_count}; partitions={len(keys)}'), '    '), after=True)
edit.insert('return keys', event('partition_keys_from_dataset', 'partition_discovery', 'completed', table='table_path.name', fields='files={log_file_count}; partitions={len(keys)}'))
boundary(edit, 'partition_discovery', table='table_path.name', fields='path={table_path}', setup='log_file_count = 0')

edit = FunctionEdit('126fe7bc', 'read_complete_partition')
edit.replace('return empty_pandas(schema)', 'read_partition_df = empty_pandas(schema)\n' + event('read_complete_partition', 'leaf_read', 'completed', table='table_path.name', fields='partition={partition_key}; outcome=absent_leaf; rows=0') + '\nreturn read_partition_df')
edit.insert('expected_file_schema = parquet_file_schema', 'log_phase = "file_schema"\n' + event('read_complete_partition', 'file_schema', 'started', table='table_path.name', fields='partition={partition_key}; files={len(parquet_files)}'))
edit.insert('file_table = ds.dataset', 'log_phase = "leaf_scan"\n' + event('read_complete_partition', 'leaf_scan', 'started', table='table_path.name', fields='partition={partition_key}'))
edit.insert('logical_table = pa.Table.from_arrays', 'log_phase = "conversion"')
edit.replace('return arrow_to_pandas(logical_table, schema)', 'read_partition_df = arrow_to_pandas(logical_table, schema)\n' + event('read_complete_partition', 'leaf_read', 'completed', table='table_path.name', fields='partition={partition_key}; outcome=read; rows={len(read_partition_df)}') + '\nreturn read_partition_df')
boundary(edit, 'leaf_read', table='table_path.name', fields='partition={partition_key}; path={table_path}', initial_phase='locate_leaf')

edit = FunctionEdit('c06-arrow-policy-plan', 'minute_policy_plan')
edit.insert('selected_underlyings = sorted', 'log_phase = "whitelist"')
edit.insert('completed_mask = pc.and_', 'log_phase = "completion_evidence"')
edit.insert('return (desired_required, policy_changed_mask, pending_mask, completed_mask)', event('minute_policy_plan', 'policy_plan', 'completed', table='CALENDAR_TABLE_NAME', fields='exchange={exchange_code}; rows={len(planned_table)}; completion_source=is_fetch_completed; persisted=false'))
boundary(edit, 'policy_plan', table='CALENDAR_TABLE_NAME', fields='exchange={exchange_code}; rows={len(planning_table)}', initial_phase='planning_schema')

edit = FunctionEdit('b358d80e', 'quota_spare')
edit.insert('if get_query_count is None:', 'log_phase = "quota_api"', after=False)
edit.insert('quota = get_query_count()', event('quota_spare', 'quota_api', 'started'))
edit.replace("return int(quota['spare'])", 'spare = int(quota["spare"])\n' + event('quota_spare', 'quota_read', 'completed', fields='outcome=available; spare={spare}') + '\nreturn spare')
# 两个 None 返回分别对应接口不可用和没有 spare；保持原分支。
none_returns = [n for n in ast.walk(edit.function) if isinstance(n, ast.Return) and isinstance(n.value, ast.Constant) and n.value.value is None]
assert len(none_returns) == 2
for node in none_returns:
    edit.insertions.setdefault(node.lineno - 1, []).append(textwrap.indent(event('quota_spare', 'quota_read', 'completed', fields='outcome=unavailable; spare=unknown') + '\n', ' ' * node.col_offset))
boundary(edit, 'quota_read', fields='source=get_query_count')

edit = FunctionEdit('b358d80e', 'request_batches')
edit.insert('batches.append(', 'log_group_count += 1\nif log_group_count == 1 or log_group_count % 100 == 0:\n' + textwrap.indent(event('request_batches', 'request_plan', 'running', fields='batches={log_group_count}; sessions={len(sessions_df)}'), '    '), after=True)
edit.insert('return batches', event('request_batches', 'request_plan', 'completed', fields='sessions={len(sessions_df)}; batches={len(batches)}; persisted=false'))
boundary(edit, 'request_plan', fields='sessions={len(sessions_df)}', setup='log_group_count = 0')

edit = FunctionEdit('b31dc6c2', 'collect_partition')
edit.insert('for batch_number, batch_df in enumerate', 'log_phase = "request_batches"\n' + event('collect_partition', 'request_plan', 'completed', fields='sessions={len(sessions_df)}; batches={len(batches)}'))
edit.insert('raw_df = jqdata.get_price', 'log_phase = "get_price"\nlog_api_started_at = time.perf_counter()')
edit.insert('normalized_df = normalize_minute_response', event('collect_partition', 'api_request', 'completed', fields='batch={batch_number}/{len(batches)}; api=get_price; request_elapsed_s={time.perf_counter() - log_api_started_at:.3f}') + '\nlog_phase = "normalize_response"')
edit.insert('if normalized_df.empty:', 'log_completed_batches += 1\n' + event('collect_partition', 'normalize_response', 'completed', fields='batch={batch_number}/{len(batches)}; rows={len(normalized_df)}'))
edit.insert('session_intervals = pd.IntervalIndex.from_arrays', 'log_phase = "session_alignment"\n' + event('collect_partition', 'session_alignment', 'started', fields='batch={batch_number}/{len(batches)}; rows={len(normalized_df)}; sessions={len(batch_df)}'))
edit.insert('invalid_ohlc = invalid_ohlc_mask', 'log_phase = "source_quality"')
edit.insert('collected_df = pd.concat', 'log_phase = "validate_output"\n' + event('collect_partition', 'validate_output', 'started', fields='completed_batches={log_completed_batches}; returned_rows={returned_rows}'))
edit.replace('return (validate_minute_frame(', '''validated_minute_df = validate_minute_frame(
    collected_df, "JQData 分钟转换结果", sessions_df,
)
''' + event('collect_partition', 'collect', 'completed', prefix='api_success', fields='sessions={len(sessions_df)}; batches={len(batches)}; fact_rows={len(validated_minute_df)}; returned_rows={returned_rows}; invalid_ohlc_rows={sum(invalid_session_counts.values())}; persisted=false') + '\nreturn validated_minute_df, returned_rows, invalid_session_counts')
boundary(edit, 'collect', fields='sessions={len(sessions_df)}; persisted=false', setup='log_completed_batches = 0', initial_phase='request_plan')
cells['b31dc6c2'].source = cells['b31dc6c2'].source.replace('function=collect_partition; phase=request_batch; status=started', 'function=collect_partition; phase=api_request; status=started')

edit = FunctionEdit('41986f77', 'commit_complete_partition')
edit.insert('complete_table = pandas_to_arrow', 'log_phase = "arrow_conversion"')
edit.insert('silver_root = lake_root.resolve()', 'log_phase = "prepare_paths"')
edit.insert('staging_path.mkdir', 'log_phase = "staging_write"\n' + event('commit_complete_partition', 'staging_write', 'started', table='table_name', fields='partition={partition_key}; rows={len(complete_table)}; run_id={run_id}'))
edit.insert('staged_leaf_path = staging_path', 'log_phase = "staging_readback"\n' + event('commit_complete_partition', 'staging_readback', 'started', table='table_name', fields='partition={partition_key}; run_id={run_id}'))
edit.insert('source_path = staging_path', event('commit_complete_partition', 'staging_readback', 'completed', table='table_name', fields='partition={partition_key}; files={len(staged_files)}; rows={staged_row_count}; run_id={run_id}') + '\nlog_phase = "prepare_install"')
edit.insert('if destination_path.exists():\n    shutil.move', 'log_phase = "install"\n' + event('commit_complete_partition', 'install', 'started', table='table_name', fields='partition={partition_key}; run_id={run_id}'))
edit.insert('if marker_path.exists():', 'log_phase = "schema_marker"\n' + event('commit_complete_partition', 'schema_marker', 'started', table='table_name', fields='partition={partition_key}; run_id={run_id}'))
edit.insert('formal_files = list', 'log_phase = "formal_readback"\n' + event('commit_complete_partition', 'formal_readback', 'started', table='table_name', fields='partition={partition_key}; run_id={run_id}'))
edit.insert('commit_succeeded = True', event('commit_complete_partition', 'formal_readback', 'completed', table='table_name', fields='partition={partition_key}; files={len(formal_files)}; rows={formal_row_count}; run_id={run_id}'))
# 先按旧逻辑恢复，再报告；不让新增日志打断恢复操作。
edit.insert('if rollback_errors:', 'log_rollback_status = "failed" if rollback_errors else "completed"\n' + event('commit_complete_partition', 'rollback', '{log_rollback_status}', table='table_name', fields='partition={partition_key}; failed_phase={log_phase}; errors={len(rollback_errors)}; backup={backup_path}; quarantine={quarantine_path}'))
edit.insert('return complete_df', event('commit_complete_partition', 'commit_leaf', 'completed', table='table_name', prefix='partition_committed', fields='partition={partition_key}; rows={len(complete_df)}; run_id={run_id}; persisted=true'))
boundary(edit, 'commit_leaf', table='table_name', fields='partition={partition_key}; input_rows={len(frame)}', initial_phase='validate_dirty_leaf')

edit = FunctionEdit('3ee121e7', 'commit_fact_partition')
edit.insert('desired_df = pd.concat', 'log_phase = "merge_fact_leaf"')
edit.insert('committed_df = commit_complete_partition', event('commit_fact_partition', 'merge_fact_leaf', 'completed', fields='partition={partition_key}; retained_rows={len(retained_df)}; new_rows={len(collected_df)}; complete_rows={len(desired_df)}; persisted=false') + '\nlog_phase = "commit_fact_leaf"')
edit.insert('return collected_df.sort_values', event('commit_fact_partition', 'fact_commit', 'completed', fields='partition={partition_key}; fact_rows={len(collected_df)}; complete_rows={len(committed_df)}; calendar_state=pending'))
boundary(edit, 'fact_commit', fields='partition={partition_key}; new_rows={len(collected_df)}; sessions={len(pending_sessions_df)}', initial_phase='select_retained_rows')

edit = FunctionEdit('c06-unified-calendar-commit', 'build_calendar_completion_updates')
edit.insert('return {}', event('build_calendar_completion_updates', 'completion_summary', 'completed', table='CALENDAR_TABLE_NAME', fields='sessions=0; partitions=0; persisted=false'))
edit.insert('reported_invalid_counts =', 'log_phase = "verify_quality_counts"')
edit.insert('session_summary_df =', 'log_phase = "build_session_summary"')
edit.insert('updates_by_partition = {}', 'log_phase = "group_calendar_leaves"')
edit.insert('return updates_by_partition', event('build_calendar_completion_updates', 'completion_summary', 'completed', table='CALENDAR_TABLE_NAME', fields='sessions={len(session_summary_df)}; partitions={len(updates_by_partition)}; persisted=false'))
boundary(edit, 'completion_summary', table='CALENDAR_TABLE_NAME', fields='sessions={len(sessions_df)}; fact_rows={len(committed_requested_df)}; persisted=false', initial_phase='count_facts')

edit = FunctionEdit('c06-unified-calendar-commit', 'commit_calendar_updates')
edit.insert('calendar_df = read_complete_partition', 'log_phase = "read_calendar_leaf"\n' + event('commit_calendar_updates', 'build_calendar_leaf', 'started', table='CALENDAR_TABLE_NAME', fields='partition={partition_key}; completed={committed_partition_count}; total={len(partition_keys)}; persisted=false'))
edit.insert('policy_updates_df =', 'log_phase = "apply_policy"')
edit.insert('completion_updates_df =', 'log_phase = "apply_completion"')
edit.insert('commit_complete_partition(', event('commit_calendar_updates', 'build_calendar_leaf', 'completed', table='CALENDAR_TABLE_NAME', fields='partition={partition_key}; rows={len(complete_df)}; persisted=false') + '\nlog_phase = "commit_calendar_leaf"')
edit.insert('committed_partition_count += 1', '''
    if completion_updates_df is not None and not completion_updates_df.empty:
''' + textwrap.indent(event('commit_calendar_updates', 'completion_state', 'completed', table='CALENDAR_TABLE_NAME', fields='partition={partition_key}; sessions={len(completion_updates_df)}; source=is_fetch_completed; fetch_run_id={fetch_run_id}; persisted=true'), '    ') + '\n' + event('commit_calendar_updates', 'calendar_commit', 'running', table='CALENDAR_TABLE_NAME', fields='partition={partition_key}; committed_partitions={committed_partition_count}; total={len(partition_keys)}; policy_rows={policy_row_count}; completed_sessions={completion_row_count}'), after=True)
edit.insert('return (policy_row_count, completion_row_count, committed_partition_count)', event('commit_calendar_updates', 'calendar_commit', 'completed', table='CALENDAR_TABLE_NAME', fields='policy_rows={policy_row_count}; state_rows={completion_row_count}; partitions={committed_partition_count}'))
boundary(edit, 'calendar_commit', table='CALENDAR_TABLE_NAME', fields='policy_leaves={len(policy_updates_by_partition)}; completion_leaves={len(completion_updates_by_partition)}; fetch_run_id={fetch_run_id}', initial_phase='plan_dirty_leaves')

# 入口只保留调度、配额与整批累计，不再代替被调函数报告单次完成。
source = cells['86251818'].source
tree = ast.parse(source)
lines = source.splitlines(keepends=True)
replacements = {}
for node in ast.walk(tree):
    if isinstance(node, ast.Assign):
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if names in (['calendar_commit_started_at'], ['log_partition_started_at'], ['log_fact_commit_started_at']):
            replacements[node.lineno - 1] = (node.end_lineno, '')
    if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo'):
        continue
    text = ast.unparse(node)
    new = None
    if 'function=main; phase=calendar_commit;' in text:
        new = ''
    elif 'function=main; phase=collect_partition; status=started' in text:
        new = event('main', 'collect_batch', 'running', fields='event=dispatch; partition={partition_number}/{len(plans)}; key={partition_key}; sessions={len(plan[\'pending_sessions_df\'])}; expected_rows={expected_rows}')
    elif 'function=main; phase=collect_partition; status=completed' in text:
        new = event('main', 'collect_batch', 'running', fields='processed_partitions={processed_partitions}; total_partitions={len(plans)}; processed_sessions={log_processed_sessions}; planned_sessions={pending_count}; returned_rows={returned_rows}; invalid_ohlc_rows={invalid_ohlc_rows}')
    elif 'function=main; phase=fact_commit;' in text:
        new = event('main', 'commit_batch', 'running', fields='completed_fact_partitions={completed_partitions}; total_fact_partitions={len(plans)}; fact_rows={committed_rows}; pending_calendar_sessions={completed_session_count}')
    if new is not None:
        replacements[node.lineno - 1] = (node.end_lineno, textwrap.indent(new + '\n' if new else '', ' ' * node.col_offset))
new_lines = []
index = 0
while index < len(lines):
    if index in replacements:
        index, replacement = replacements[index]
        new_lines.append(replacement)
    else:
        new_lines.append(lines[index])
        index += 1
cells['86251818'].source = ''.join(new_lines)

notes = {
    'f21ba82b': '`open_contract_dataset()`、`partition_keys_from_dataset()` 与 `read_complete_partition()` 分别报告根打开、分区发现与当前叶读取的起止和失败阶段；文件进度沿原遍历在首个及每 250 个报告，不额外列目录或扫描数据。缺叶、读到零行和异常分别表达。',
    '667a1d22': '`minute_policy_plan()` 自行报告当前交易所和规划行数，明确完成依据为 `is_fetch_completed`。入口保留各分区的 dirty、pending、completed 累计，不为日志重复计算这些掩码。',
    '8dcc5023': '`quota_spare()` 报告额度读取及不可用结果；`request_batches()` 报告组批数量和进度。入口仍负责“当前分区是否可以开始”的配额决定，函数日志不增加 API 请求或重试。',
    '702de1a0': '`collect_partition()` 自行报告组批、逐请求、归一化、Session 对齐、来源质量与输出校验。接口返回非 None 仅记录请求完成；`api_success` 在整个分区的转换结果通过校验后才输出。空响应也有明确的请求进度；失败记录当前阶段并原样抛出。',
    '47bc969c': '`commit_complete_partition()` 自行报告 dirty 叶校验、staging 写入及复读、安装、标记检查和正式复读。失败时先按既有路径尝试恢复，再报告恢复结果；成功日志在正式复读及原有清理结束后输出。`persisted=true` 属于日志中指定的表与叶，事实表提交不表示日历完成凭证已经回写。',
    '481fb127': '`commit_fact_partition()` 报告保留、替换与合并的行数，合并结果带 `persisted=false`。单叶提交成功后报告事实提交完成，并明确 `calendar_state=pending`；入口只累计成功事实叶数量。',
    'c6ef32c3': '完成摘要和日历叶生成分别由对应函数报告，内存结果均标记 `persisted=false`。`commit_calendar_updates()` 在对应日历叶的正式提交成功后，才为本次完成摘要报告 `phase=completion_state; persisted=true`。单纯政策回写不新增采集完成凭证，也不把零行契约标记当成日期水位。',
}
for cell_id, note in notes.items():
    cells[cell_id].source += '\n\n' + note
old = '当前仍由入口记录分区与日历提交汇总，函数日志归位留到后续步骤。'
assert old in cells['42b891dc'].source
cells['42b891dc'].source = cells['42b891dc'].source.replace(old, '读取、规划、采集、事实合并、完成摘要与提交由对应函数报告；入口只保留整批规划、分区调度、配额停止、累计进度和运行结果，不重复输出函数级起止。')

diagram_replacements = {
    'b06-flow-dataset': [('打开 Dataset；恢复逻辑 Schema', '记录开始；打开 Dataset 并恢复 Schema'), ('核对逻辑与各文件物理契约、表身份', '核对契约；沿原遍历报告文件进度'), ('返回 Dataset', '报告完成与文件数；返回 Dataset')],
    'b06-flow-partition-read': [('返回分区键集合', '报告文件与分区数；返回键集合'), ('构造精确叶路径', '记录读取开始；构造精确叶路径'), ('返回权威 Schema 空表', '记录缺叶；返回契约空表'), ('补回分区字段；转换 Pandas', '补分区字段并转换；报告行数与耗时')],
    'b06-flow-policy': [('规划窄表；当前交易所白名单', '记录规划开始；窄表与交易所白名单'), ('已有完成凭证：completed', '已有凭证：completed；报告规划完成')],
    'b06-flow-quota-batches': [('返回 spare 或 None；由入口检查预留量', '报告 spare 或不可用；入口检查预留量'), ('组内按 Session 编号排序；返回请求批次', '按 Session 排序；报告组批进度与完成')],
    'b06-flow-collect': [('待办 Session 按合约日组批', '记录采集开始；待办按合约日组批'), ('None 抛错；其余响应归一化', 'None 抛错；报告 API 返回与归一化结果'), ('返回分钟、来源条数与异常计数', '通过校验后记录 api_success；返回结果')],
    'b06-flow-commit': [('完整叶业务校验；检查分区并转换 Arrow', '记录提交开始；完整叶校验与 Arrow 转换'), ('清理临时目录；返回已校验完整叶', '清理后报告当前表叶已提交；返回结果'), ('保留隔离或未恢复备份；抛错停止', '报告恢复结果；保留证据并抛错')],
    'b06-flow-fact-merge': [('拼接本次采集结果', '拼接本次结果；报告 persisted=false'), ('返回本次分钟；日历仍待回写', '报告事实已提交；日历仍待回写')],
    'b06-flow-calendar-updates': [('按日历叶生成完成摘要；入口汇集', '生成摘要；记录 persisted=false；入口汇集'), ('完整叶校验与提交', '正式提交成功后报告完成凭证 persisted=true')],
    'b06-flow-main': [('逐分区检查额度、采集；可选事实提交', '按分区调度函数；累计采集与事实提交量'), ('集中回写日历；核对完成摘要数', '调用日历回写函数；核对完成摘要数')],
}
for cell_id, pairs in diagram_replacements.items():
    for old, new in pairs:
        assert old in cells[cell_id].source, (cell_id, old)
        cells[cell_id].source = cells[cell_id].source.replace(old, new)


class RemoveLogging(ast.NodeTransformer):
    """去除日志后对比原控制流；只展开此次加入的捕获后原样抛错边界。"""
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and (t.id.startswith('log_') or t.id == 'calendar_commit_started_at') for t in node.targets):
            return None
        return self.generic_visit(node)

    def visit_AugAssign(self, node):
        if isinstance(node.target, ast.Name) and node.target.id.startswith('log_'):
            return None
        return self.generic_visit(node)

    def visit_If(self, node):
        node = self.generic_visit(node)
        return node if node.body else None

    def visit_Try(self, node):
        if len(node.handlers) == 1 and node.handlers[0].name == 'log_error':
            assert isinstance(node.handlers[0].body[-1], ast.Raise) and node.handlers[0].body[-1].exc is None
            return [out for stmt in node.body if (out := self.visit(stmt)) is not None]
        return self.generic_visit(node)


def canonical(source):
    tree = RemoveLogging().visit(ast.parse(source))
    # 显式保存返回值只是让日志位于转换成功之后；还原为原返回表达式比较。
    for node in ast.walk(tree):
        if not hasattr(node, 'body') or not isinstance(node.body, list):
            continue
        body = node.body
        for i in range(len(body) - 2, -1, -1):
            assignment, following = body[i:i + 2]
            if not (isinstance(assignment, ast.Assign) and len(assignment.targets) == 1 and isinstance(assignment.targets[0], ast.Name) and isinstance(following, ast.Return)):
                continue
            name = assignment.targets[0].id
            if name not in ('read_partition_df', 'spare', 'validated_minute_df'):
                continue
            class Substitute(ast.NodeTransformer):
                def visit_Name(self, item):
                    return copy.deepcopy(assignment.value) if item.id == name else item
            body[i + 1] = Substitute().visit(following)
            del body[i]
    return ast.dump(tree)


assert notebook.metadata == before.metadata
assert [c.id for c in notebook.cells] == [c.id for c in before.cells]
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        assert comments(old.source) == comments(new.source), old.id
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        compile(new.source, str(PATH), 'exec')
        assert canonical(old.source) == canonical(new.source), old.id
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print('b06 function logs updated; business AST, comments and execution state preserved.')
