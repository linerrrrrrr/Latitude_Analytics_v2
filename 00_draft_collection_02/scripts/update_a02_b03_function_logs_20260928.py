"""a02/b03 第 5—6 项：函数进度与日志归属，保持业务步骤和计时边界。"""
import ast
import hashlib
import json
import pathlib
import shutil
import tempfile
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b03_warehouse_receipt.ipynb')
PATH = ROOT/RELATIVE
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a02-b03-function-logs-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py']
paths.extend(p for p in (ROOT/'02_Futures_Lakehouse').rglob('*') if p.is_file() and p.suffix in ('.py', '.ipynb', '.md'))
hashes = {p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
for relative in (RELATIVE, RELATIVE.with_suffix('.py')):
    target = snapshot/relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT/relative, target)
(snapshot/'hashes.json').write_text(json.dumps(hashes, ensure_ascii=False, indent=2), encoding='utf8')
notebook = json.loads(PATH.read_text(encoding='utf8'))
cells = {c['id']:c for c in notebook['cells']}


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
            prefix = 'log_started_at = time.perf_counter()\n'
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
    f"{fields}{'; ' if fields else ''}elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
)'''


def progress(function, phase, counter, fields, table='log_table_name', every=1000):
    frequency = f'{counter} % {every} == 0 and ' if every > 1 else ''
    return f'''if {frequency}time.perf_counter() - log_last_progress_at >= 2.0:
    log_last_progress_at = time.perf_counter()
{textwrap.indent(event(function, phase, 'running', table, fields), '    ')}'''


schema_name = 'log_table_name = (schema.metadata or {}).get(b"table_name", b"unknown").decode("utf-8")'

edit = FunctionEdit('validate_table_marker')
edit.insert('return False', event(edit.name, 'read_marker', 'completed', fields='path={table_path}; outcome=absent_allowed; marker_present=false'))
edit.insert('marker_metadata = ', 'log_phase = "read_marker_metadata"')
edit.insert('return True', event(edit.name, 'read_marker', 'completed', fields='path={marker_path}; rows=0; marker_present=true'))
edit.apply('read_marker', fields='path={table_path}; label={label}; required={str(required).lower()}', setup=schema_name)

edit = FunctionEdit('open_planning_calendar_dataset')
edit.insert('calendar_dataset = ', 'log_phase = "open_dataset"')
loop = edit.find('for fragment in calendar_dataset.get_fragments():')
edit.insert_node(loop, 'log_phase = "check_fragments"\n'+event(edit.name, 'read_dataset', 'running', 'CALENDAR_TABLE_NAME', 'checked_fragments=0; path={calendar_path}'))
edit.insert_node(loop.body[-1], 'log_checked_fragments += 1\n'+progress(edit.name, 'read_dataset', 'log_checked_fragments', 'checked_fragments={log_checked_fragments}; path={calendar_path}', 'CALENDAR_TABLE_NAME', every=1), after=True)
edit.insert('return calendar_dataset', event(edit.name, 'read_dataset', 'completed', 'CALENDAR_TABLE_NAME', 'checked_fragments={log_checked_fragments}; path={calendar_path}; outcome=opened'))
edit.apply('read_dataset', 'CALENDAR_TABLE_NAME', 'path={calendar_path}', setup='log_checked_fragments = 0')

for name in ('read_partition_leaf', 'read_leaf_primary_key_summary'):
    edit = FunctionEdit(name)
    phase = 'read_leaf' if name == 'read_partition_leaf' else 'read_leaf_summary'
    if name == 'read_partition_leaf':
        edit.insert('return empty_pandas(schema)', event(name, phase, 'completed', fields='key={partition_key}; path={leaf_path}; rows=0; outcome=absent_leaf'))
    else:
        node = edit.find('return primary_key_summary(empty_table, primary_key, label)')
        edit.replace_node(node, 'empty_summary = primary_key_summary(empty_table, primary_key, label)\n'+event(name, phase, 'completed', fields='key={partition_key}; path={leaf_path}; rows=0; outcome=absent_leaf')+'\nreturn empty_summary')
    loop = edit.find('for parquet_path in parquet_files:')
    edit.insert_node(loop, 'log_phase = "check_files"\n'+event(name, phase, 'running', fields='key={partition_key}; checked_files=0/{len(parquet_files)}; path={leaf_path}'))
    edit.insert_node(loop.body[-1], 'log_checked_files += 1\n'+progress(name, phase, 'log_checked_files', 'key={partition_key}; checked_files={log_checked_files}/{len(parquet_files)}', every=1), after=True)
    edit.insert('leaf_dataset = ', 'log_phase = "open_leaf"')
    if name == 'read_partition_leaf':
        edit.insert('leaf_table = ', 'log_phase = "read_columns"\n'+event(name, phase, 'running', fields='key={partition_key}; columns={len(schema.names)}; path={leaf_path}'))
        edit.capture_return('partition_df', event(name, phase, 'completed', fields='key={partition_key}; rows={len(partition_df)}; checked_files={log_checked_files}/{len(parquet_files)}'))
    else:
        edit.insert('summary_table = ', 'log_phase = "read_key_columns"\n'+event(name, phase, 'running', fields='key={partition_key}; columns={len(summary_columns)}; path={leaf_path}'))
        edit.insert('return primary_key_summary(summary_table.select(primary_key)', 'log_phase = "key_summary"')
        edit.capture_return('leaf_summary', event(name, phase, 'completed', fields='key={partition_key}; rows={leaf_summary[0]}; checked_files={log_checked_files}/{len(parquet_files)}'))
    edit.apply(phase, fields='key={partition_key}; path={table_path}; label={label}', setup=schema_name+'\nlog_checked_files = 0')

edit = FunctionEdit('normalize_warehouse_response')
edit.insert('return empty_pandas(', event(edit.name, 'generate_fact', 'completed', 'TABLE_NAME', 'grid={grid}; rows=0; outcome=empty_response; persisted=false', 'api_success'))
edit.insert('missing_columns = ', 'log_phase = "validate_source"')
edit.insert('rows = []', 'log_phase = "normalize_rows"\n'+event(edit.name, 'generate_fact', 'running', 'TABLE_NAME', 'grid={grid}; source_rows={len(response_df)}; visited_rows=0; persisted=false'))
edit.insert('rows.append(', 'log_visited_rows += 1\n'+progress(edit.name, 'generate_fact', 'log_visited_rows', 'grid={grid}; visited_rows={log_visited_rows}/{len(response_df)}; persisted=false', 'TABLE_NAME'), after=True)
edit.insert('warehouse_table = ', 'log_phase = "convert_contract"')
edit.capture_return('warehouse_df', event(edit.name, 'generate_fact', 'completed', 'TABLE_NAME', 'grid={grid}; rows={len(warehouse_df)}; persisted=false', 'api_success'))
edit.apply('generate_fact', 'TABLE_NAME', 'grid={grid}; persisted=false', setup='log_visited_rows = 0')

edit = FunctionEdit('pending_report_grids')
edit.insert('return (pending_df, complete_count)', event(edit.name, 'plan', 'completed', 'CALENDAR_TABLE_NAME', 'planning_rows={len(calendar_planning_df)}; complete_grid_count={complete_count}; pending_grid_count={len(pending_df)}; basis=calendar_completion_snapshot', 'reconciliation_plan'))
edit.apply('plan', 'CALENDAR_TABLE_NAME')

edit = FunctionEdit('full_fact_partition')
edit.insert('return validate_warehouse_frame(', 'log_phase = "validate_dirty_fact"')
edit.capture_return('validated_fact_partition_df', event(edit.name, 'generate_complete_leaf', 'completed', 'TABLE_NAME', 'retained_rows={len(retained_df)}; new_rows={len(incoming_df)}; rows={len(validated_fact_partition_df)}; persisted=false'))
edit.apply('generate_complete_leaf', 'TABLE_NAME', 'persisted=false')

edit = FunctionEdit('ensure_pending_calendar_grids')
loop = edit.find('for grid in grid_records:')
edit.insert_node(loop.body[-1], 'log_checked_grids += 1\n'+progress(edit.name, 'check_pending', 'log_checked_grids', 'checked_grids={log_checked_grids}/{len(grid_records)}', 'CALENDAR_TABLE_NAME', every=1), after=True)
edit.insert_node(loop, event(edit.name, 'check_pending', 'completed', 'CALENDAR_TABLE_NAME', 'checked_grids={log_checked_grids}/{len(grid_records)}'), after=True)
edit.apply('check_pending', 'CALENDAR_TABLE_NAME', setup='log_checked_grids = 0')

for name in ('apply_calendar_completion', 'apply_calendar_failure'):
    edit = FunctionEdit(name)
    phase = 'generate_calendar_completion' if name == 'apply_calendar_completion' else 'generate_calendar_failure'
    loop = edit.find('for index, row in updated_df.iterrows():')
    edit.insert_node(loop.body[0], 'log_visited_rows += 1\n'+progress(name, phase, 'log_visited_rows', 'visited_rows={log_visited_rows}/{len(updated_df)}; persisted=false', 'CALENDAR_TABLE_NAME'))
    edit.insert('return validate_calendar_frame(', 'log_phase = "validate_dirty_calendar"')
    fields = ('updated_grids={len(updated_grid_keys)}; fetch_completed=true' if name == 'apply_calendar_completion' else 'grid={grid_key}; fetch_status={fetch_status}; fetch_completed=false')
    edit.capture_return('validated_calendar_partition_df', event(name, phase, 'completed', 'CALENDAR_TABLE_NAME', fields+'; rows={len(validated_calendar_partition_df)}; visited_rows={log_visited_rows}; persisted=false; date_watermark=none'))
    edit.apply(phase, 'CALENDAR_TABLE_NAME', 'persisted=false; date_watermark=none', setup='log_visited_rows = 0')

edit = FunctionEdit('query_warehouse_grid')
edit.insert('raw_df = jqdata.finance.run_query(', 'log_phase = "source_request"\n'+event(edit.name, 'query', 'running', 'TABLE_NAME', 'grid={grid}; source_calls=1; response_pending=true', 'request_batch'))
edit.insert('return raw_df', event(edit.name, 'query', 'completed', 'TABLE_NAME', 'grid={grid}; source_calls=1; outcome=response_received; business_validated=false; persisted=false', 'api_result'))
edit.apply('query', 'TABLE_NAME', 'grid={grid}; source_calls=0')

for name in ('commit_complete_partition', 'commit_calendar_partitions'):
    edit = FunctionEdit(name)
    fact = name == 'commit_complete_partition'
    table = 'TABLE_NAME' if fact else 'CALENDAR_TABLE_NAME'
    if not fact:
        edit.insert('return 0', event(name, 'commit_calendar', 'skipped', table, 'reason=no_grids; persisted=false; date_watermark=none'))
        edit.insert('for partition_key in sorted(partition_keys):', event(name, 'commit_calendar', 'running', table, 'partitions=0/{len(partition_keys)}; touched_rows={len(touched_df)}; batch_state=pending'))
        edit.insert('partition_mask = ', 'log_phase = "prepare_leaf"')
    edit.insert('expected_summary = ', 'log_phase = "expected_summary"')
    edit.insert('staging_path.mkdir(', 'log_phase = "staging_write"\n'+event(name, 'staging_write', 'started', table, 'key={partition_key}; rows={len(complete_table)}; path={staging_path}; batch_state=pending'))
    edit.insert('staged_summary = ', 'log_phase = "staging_verify"\n'+event(name, 'staging_verify', 'started', table, 'key={partition_key}; batch_state=pending'))
    install_try = next(n for n in ast.walk(edit.function) if isinstance(n, ast.Try) and any(h.name == 'commit_error' for h in n.handlers))
    edit.insert_node(install_try.body[0], 'log_phase = "install"\n'+event(name, 'install', 'started', table, 'key={partition_key}; path={destination_path}; batch_state=pending'))
    formal_marker = next(n for n in install_try.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call) and ast.unparse(n.value.func) == 'validate_table_marker')
    edit.insert_node(formal_marker, 'log_phase = "formal_verify"\n'+event(name, 'formal_verify', 'started', table, 'key={partition_key}; batch_state=pending'))
    handler = next(h for h in install_try.handlers if h.name == 'commit_error')
    edit.insert_node(handler.body[0], event(name, 'rollback', 'started', table, 'key={partition_key}; failed_phase={log_phase}; backup_path={backup_path}'))
    failure_if = next(n for n in handler.body if isinstance(n, ast.If) and ast.unparse(n.test) == 'rollback_errors')
    edit.insert_node(failure_if.body[0], event(name, 'rollback', 'failed', table, 'key={partition_key}; backup_path={backup_path}; quarantine_path={quarantine_path}; errors={rollback_errors}'))
    edit.insert_node(handler.body[-1], event(name, 'rollback', 'completed', table, 'key={partition_key}; outcome=recovery_branch_completed'))
    success = event(name, 'commit', 'completed', table, 'key={partition_key}; rows={len(complete_table)}; persisted=true; scope=leaf', 'partition_committed')
    if fact:
        edit.insert('return len(complete_table)', success)
        edit.apply('commit', table, 'key={partition_key}; batch_state=pending')
    else:
        edit.insert_node(install_try, 'log_committed_partitions += 1\n'+success+'\n'+event(name, 'commit_calendar', 'running', table, 'partitions={log_committed_partitions}/{len(partition_keys)}; batch_state=pending'), after=True)
        edit.insert('return len(touched_df)', event(name, 'calendar_state', 'completed', table, 'partitions={log_committed_partitions}; rows={len(touched_df)}; persisted=true; date_watermark=none; outcome=state_committed'))
        edit.apply('commit_calendar', table, 'batch_state=pending; date_watermark=none', setup='log_committed_partitions = 0')

edit = FunctionEdit('main')
for node in ast.walk(edit.function):
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
        text = ast.unparse(node)
        if 'reconciliation_plan:' in text or 'partition_committed:' in text:
            edit.replace_node(node, '')
        elif 'api_success:' in text:
            edit.replace_node(node, event('main', 'assemble_month', 'completed', 'TABLE_NAME', 'key={partition_key}; rows={len(incoming_df)}; persisted=false'))
        elif 'phase=planning; status=completed;' in text:
            source = textwrap.dedent(''.join(edit.lines[node.lineno-1:node.end_lineno]))
            edit.replace_node(node, source.replace('mode={mode}; ', 'mode={mode}; planning_seconds={time.perf_counter() - planning_started_at:.3f}; '))
        elif 'partition_timing:' in text and 'calendar_seconds={calendar_seconds' in text:
            source = textwrap.dedent(''.join(edit.lines[node.lineno-1:node.end_lineno]))
            edit.replace_node(node, source.replace('partition_seconds={partition_seconds:.3f}"', 'partition_seconds={partition_seconds:.3f}; calendar_rows={calendar_rows}"'))
edit.insert('calendar_planning_df = ', 'log_phase = "read_calendar_plan"\n'+event('main', 'read_calendar_plan', 'started', 'CALENDAR_TABLE_NAME', 'columns={len(CALENDAR_PLANNING_COLUMNS)}; required_only=true'))
edit.insert('calendar_planning_df = ', event('main', 'read_calendar_plan', 'completed', 'CALENDAR_TABLE_NAME', 'rows={len(calendar_planning_df)}; required_only=true'), after=True)
edit.insert('if any((not frame.empty for frame in frames)):', event('main', 'assemble_month', 'started', 'TABLE_NAME', 'key={partition_key}; persisted=false'))
edit.apply()

append = {
'1daeab12': '\n\n标记读取和日历打开函数自行报告开始、空表分支、物理契约核对及完成。fragment 进度沿既有循环累计，至多每 2 秒报告一次，不另扫目录统计总数。日历规划列的实际读取仍由 `main()` 报告。',
'a02-b03-leaf-read-heading': '\n\n两个读取函数自行报告叶缺失、待检查文件数、已检查文件数、列读取与结果行数；进度计数附着在原文件循环中，至多每 2 秒报告一次。摘要读取仍只投影主键和分区，不为日志追加业务列或第二次读取。',
'1552c334': '\n\n生成函数自行报告来源验收、行归一化和契约转换。沿原行循环每 1000 行检查 2 秒进度间隔；最终 `api_success:` 表示该日响应已生成事实，仍为 `persisted=false`。空响应同样由本函数报告，查询收到响应不提前代表业务验收通过。',
'1e13ae1c': '\n\n待办函数拥有 `reconciliation_plan:`，报告规划行数、可信完成数与待办数；不为日志重新计算事实完成证据。',
'a02-b03-fact-merge-heading': '\n\n待办复核函数沿原格点循环报告检查数量；完整叶生成函数自行报告保留行、新行、验证后完整叶行数和失败阶段，生成结果明确标为 `persisted=false`。',
'406f1843': '\n\n事实提交函数自行报告预期摘要、staging、安装、正式验收及恢复分支。只有原有正式验收和清理结束后才输出 `partition_committed: ... scope=leaf`。恢复日志只陈述现有恢复分支执行结果，不承诺尚未接入的共享事务行为。',
'49b7a75f': '\n\n两条状态生成函数沿原日历行循环报告已访问行数，返回前分别标明 `fetch_completed=true` 或 `false`、`persisted=false` 和 `date_watermark=none`；内存生成日志不代表日历状态已经落盘。',
'a02-b03-calendar-commit-heading': '\n\n日历提交函数逐叶报告准备、安装、验收和恢复，当前叶通过原有清理后输出 `partition_committed: ... scope=leaf`；全部触达叶成功后才输出 `phase=calendar_state; persisted=true`。这里的提交完成同时适用于成功状态和失败状态，不能推断 `is_fetch_completed=true`；本入口没有独立日期水位文件。',
'a407d548': '\n\n查询函数拥有请求起止和单次调用日志，返回前的 `api_result:` 只表示收到响应，标明 `business_validated=false`。来源验收成功由归一化函数随后报告，任何异常仍不自动重试。',
}
for cell_id, text in append.items():
    cells[cell_id]['source'] = (''.join(cells[cell_id]['source']).rstrip()+text).splitlines(keepends=True)
cell = cells['b4752348']
text = ''.join(cell['source'])
text = text.replace('`partition_committed:` 仍在事实和日历均成功后输出，`persisted=false` 标明只读结果。', '`partition_committed:` 由事实和日历提交函数分别在对应叶成功后发出，`persisted=false` 标明内存或只读结果。')
text = text.replace('函数内部进度与日志归属留待第 5—6 项；本轮只规范现有主流程日志，异常仍按原类型和异常链抛出。', '`main()` 只保留运行边界、自己实际进行的日历窄列读取和月份汇总、原有分区计时及性能门槛，不重复函数内部的生成或提交完成日志。原有计时起止位置和性能窗口条件保持不变；日历状态生成与落盘日志分别由所属函数发出。异常仍按原类型和异常链抛出。')
cell['source'] = text.splitlines(keepends=True)
flow_replacements = {
'a02-b03-flow-contract': [('逐 fragment 检查物理契约', '沿原 fragment 循环核对契约并报告进度'), ('返回 Dataset；main 读取仓单规划列', '函数报告打开完成；main 读取并报告规划行数')],
'a02-b03-flow-read': [('只枚举当前叶文件；核对物理契约', '只枚举当前叶；沿文件循环核对并报告进度'), ('排序主键；返回行数与 SHA-256', '排序主键；函数报告摘要完成和行数')],
'a02-b03-flow-normalize': [('逐行验收仓库、数量、单位与变化', '逐行验收仓库、数量、单位与变化；报告进度'), ('转换为权威 Arrow/Pandas 结果', '契约转换；函数报告 api_success；persisted=false')],
'a02-b03-flow-plan': [('返回待办与完成计数；不扫描事实历史', '函数报告 reconciliation_plan；不扫描事实历史')],
'a02-b03-flow-merge': [('返回内存完整叶；尚未提交', '函数报告完整叶生成完成；persisted=false')],
'a02-b03-flow-fact-commit': [('清理临时路径；返回事实行数', '清理临时路径；函数报告当前事实叶已提交')],
'a02-b03-flow-state': [('完整验收 dirty 日历叶；返回内存结果', '完整验收；函数报告生成结果；persisted=false')],
'a02-b03-flow-calendar-commit': [('备份并安装当前叶；正式摘要复读', '安装和正式摘要复读；函数报告当前叶提交'), ('返回触达行数', '函数报告日历状态已落盘；date_watermark=none')],
'a02-b03-flow-query': [('run_query 一次', '函数报告请求开始；run_query 一次'), ('返回响应给归一化函数', '函数报告收到响应；交给归一化函数验收')],
'a02-b03-flow-cli': [('逐日查询与来源归一化', '逐日查询与归一化；函数自行报告'), ('生成并提交日历；报告当前分区成功及耗时', '生成并提交日历；main 汇总原有分区耗时')],
}
for cell_id, replacements in flow_replacements.items():
    text = ''.join(cells[cell_id]['source'])
    for old, new in replacements:
        assert old in text, (cell_id, old)
        text = text.replace(old, new)
    cells[cell_id]['source'] = text.splitlines(keepends=True)
PATH.write_text(json.dumps(notebook, ensure_ascii=False, indent=1)+'\n', encoding='utf8', newline='\n')
print(snapshot)
