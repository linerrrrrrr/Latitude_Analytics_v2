"""b08 第 5—6 项：日志归属函数，保持计算、读取、提交与回滚语义。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b08_full_minute_quality.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id: c for c in notebook.cells}


class FunctionEdit:
    def __init__(self, cell_id, name):
        self.cell = cells[cell_id]
        self.function = next(n for n in ast.parse(self.cell.source).body if isinstance(n, ast.FunctionDef) and n.name == name)
        self.lines = self.cell.source.splitlines(keepends=True)
        self.insertions = {}
        self.replacements = {}

    def find(self, prefix, contains=None):
        matches = [n for n in ast.walk(self.function) if isinstance(n, ast.stmt)
                   and ast.unparse(n).startswith(prefix) and (contains is None or contains in ast.unparse(n))]
        assert len(matches) == 1, (self.function.name, prefix, contains, len(matches))
        return matches[0]

    def insert_node(self, node, source, after=False):
        index = node.end_lineno if after else node.lineno - 1
        self.insertions.setdefault(index, []).append(textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * node.col_offset))

    def insert(self, prefix, source, *, after=False, contains=None):
        self.insert_node(self.find(prefix, contains), source, after)

    def remove(self, prefix, contains=None):
        node = self.find(prefix, contains)
        self.replacements[node.lineno - 1] = node.end_lineno

    def render(self, start, end):
        result = []
        index = start
        while index < end:
            result.extend(self.insertions.get(index, []))
            if index in self.replacements:
                index = self.replacements[index]
            else:
                result.append(self.lines[index])
                index += 1
        result.extend(self.insertions.get(end, []))
        return ''.join(result)

    def apply(self):
        self.cell.source = self.render(0, len(self.lines))
        ast.parse(self.cell.source)

    def wrap(self, setup, failure):
        first = self.function.body[0]
        start = first.lineno - 1
        while start > self.function.lineno and (not self.lines[start - 1].strip() or self.lines[start - 1].lstrip().startswith('#')):
            start -= 1
        end = self.function.end_lineno
        indent = ' ' * first.col_offset
        self.cell.source = (
            ''.join(self.lines[:start])
            + textwrap.indent(textwrap.dedent(setup).strip() + '\n', indent)
            + indent + 'try:\n'
            + textwrap.indent(self.render(start, end).rstrip() + '\n', '    ')
            + textwrap.indent(textwrap.dedent(failure).strip() + '\n', indent)
            + ''.join(self.lines[end:])
        )
        ast.parse(self.cell.source)


def event(function, phase, status, fields='', *, table='MISSING_TABLE_NAME', prefix='planning_progress'):
    return f'''click.echo(
    f"{prefix}: table={{{table}}}; function={function}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields else ''}elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
)'''


def boundary(edit, phase, *, table='MISSING_TABLE_NAME', fields='', setup='', initial_phase=None, failure_fields='', failed_phase='log_phase'):
    edit.wrap(
        f'log_started_at = time.perf_counter()\nlog_phase = "{initial_phase or phase}"\n{setup}\n'
        + event(edit.function.name, phase, 'started', fields, table=table),
        'except Exception as log_error:\n' + textwrap.indent(event(
            edit.function.name, phase, 'failed',
            f'failed_phase={{{failed_phase}}}; error={{type(log_error).__name__}}{failure_fields}', table=table,
        ) + '\nraise', '    '),
    )


schema_log_name = '''log_table_name = (
    CALENDAR_TABLE_NAME if schema is FUTURES_BAR_CALENDAR_SCHEMA
    else MINUTE_TABLE_NAME if schema is FUTURES_MINUTE_SCHEMA
    else MISSING_TABLE_NAME if schema is FUTURES_MISSING_BAR_SCHEMA
    else "unknown"
)'''

edit = FunctionEdit('c08-dataset-helpers', 'open_exact_dataset')
edit.insert('dataset = ds.dataset', event('open_exact_dataset', 'discovery', 'completed', 'label={label}; files={len(parquet_files)}; materialized=false', table='log_table_name') + '\nlog_phase = "dataset_open"')
edit.insert('if not physically_and_identity_compatible(reconstructed_schema', 'log_phase = "logical_schema"')
edit.insert('for fragment in dataset.get_fragments():', 'log_phase = "fragment_schema"')
edit.insert('if not physically_and_identity_compatible(fragment.physical_schema', '''log_checked_fragments += 1
if log_checked_fragments == 1 or log_checked_fragments % 250 == 0:
''' + textwrap.indent(event('open_exact_dataset', 'fragment_schema', 'running', 'label={label}; checked_fragments={log_checked_fragments}; materialized=false', table='log_table_name'), '    '), after=True)
edit.insert('return dataset', event('open_exact_dataset', 'dataset_open', 'completed', 'label={label}; path={table_path}; checked_fragments={log_checked_fragments}; materialized=false', table='log_table_name'))
boundary(edit, 'dataset_open', table='log_table_name', fields='label={label}; path={table_path}', setup=schema_log_name+'\nlog_checked_fragments = 0', initial_phase='discovery', failure_fields='; label={label}; checked_fragments={log_checked_fragments}')

edit = FunctionEdit('c08-dataset-helpers', 'discover_partition_keys')
edit.insert("if parquet_path.name == 'schema.parquet':", '''log_visited_files += 1
if log_visited_files == 1 or log_visited_files % 250 == 0:
''' + textwrap.indent(event('discover_partition_keys', 'partition_discovery', 'running', 'path={table_path}; visited_files={log_visited_files}; discovered_partitions={len(partition_keys)}', table='table_path.name'), '    '))
edit.insert('return partition_keys', event('discover_partition_keys', 'partition_discovery', 'completed', 'path={table_path}; visited_files={log_visited_files}; partitions={len(partition_keys)}', table='table_path.name'))
boundary(edit, 'partition_discovery', table='table_path.name', fields='path={table_path}', setup='log_visited_files = 0', failure_fields='; path={table_path}; visited_files={log_visited_files}')

edit = FunctionEdit('c08-dataset-helpers', 'table_digest')
edit.insert('sort_indices = pc.sort_indices', 'log_phase = "sort"\n' + event('table_digest', 'digest_sort', 'started', 'rows={table.num_rows}', table='log_table_name'))
edit.insert('sorted_table = typed_table.take', event('table_digest', 'digest_sort', 'completed', 'rows={table.num_rows}', table='log_table_name')+'\nlog_phase = "canonicalize"', after=True)
edit.insert('canonical_columns.append(array)', '''log_canonical_columns += 1
if log_canonical_columns == 1 or log_canonical_columns % 16 == 0 or log_canonical_columns == len(schema):
''' + textwrap.indent(event('table_digest', 'canonicalize', 'running', 'columns={log_canonical_columns}/{len(schema)}; rows={table.num_rows}', table='log_table_name'), '    '), after=True)
edit.insert('sink = pa.BufferOutputStream()', 'log_phase = "serialize_and_hash"')
edit.insert('return digest.hexdigest()', event('table_digest', 'digest', 'completed', 'rows={table.num_rows}; columns={log_canonical_columns}', table='log_table_name'))
boundary(edit, 'digest', table='log_table_name', fields='rows={table.num_rows}; columns={table.num_columns}', setup=schema_log_name+'\nlog_canonical_columns = 0', initial_phase='contract', failure_fields='; columns={log_canonical_columns}')

for name, table, phase, source_rows in (
    ('validate_missing_output', 'MISSING_TABLE_NAME', 'validate_missing', 'table.num_rows'),
    ('validate_calendar_audit_output', 'CALENDAR_TABLE_NAME', 'validate_calendar', 'len(frame)'),
):
    edit = FunctionEdit('c08-output-validation', name)
    edit.insert('actual_partition_keys =', 'log_phase = "partition_membership"')
    if name == 'validate_missing_output':
        edit.insert('if checked_frame.select', 'log_phase = "primary_key"')
    else:
        edit.insert('selected = checked_frame.loc', 'log_phase = "audit_fields"')
        edit.insert('detected_timestamp =', 'log_phase = "audit_timestamps"')
    for node in ast.walk(edit.function):
        if isinstance(node, ast.Return):
            edit.insert_node(node, event(name, phase, 'completed', 'context={context}; partition={partition_key}; rows={checked_table.num_rows}', table=table))
    boundary(edit, phase, table=table, fields=f'context={{context}}; partition={{partition_key}}; rows={{{source_rows}}}', initial_phase='conversion', failure_fields='; context={context}; partition={partition_key}')

edit = FunctionEdit('c08-build', 'build_full_audit_staging')
owner = 'build_full_audit_staging'
edit.insert('calendar_dataset =', 'log_phase = "open_upstream"')
edit.insert('calendar_partition_keys =', 'log_phase = "partition_discovery"')
edit.insert('readiness_table =', 'log_phase = "readiness"\n' + event(owner, 'readiness', 'started', 'calendar_partitions={len(calendar_partition_keys)}', table='CALENDAR_TABLE_NAME'))
edit.insert('missing_staging_path.mkdir', event(owner, 'readiness', 'completed', 'required_sessions={len(readiness_df)}; calendar_partitions={len(calendar_partition_keys)}', table='CALENDAR_TABLE_NAME')+'\nlog_phase = "prepare_staging"')
edit.insert('calendar_table = calendar_dataset.to_table', '''log_calendar_index += 1
log_partition = calendar_partition_key
log_minute_partition = None
log_phase = "calendar_read"
''' + event(owner, 'calendar_read', 'started', 'partition={calendar_partition_key}; calendar_index={log_calendar_index}/{len(calendar_partition_keys)}', table='CALENDAR_TABLE_NAME'))
edit.insert('selected_df = calendar_df.loc', event(owner, 'calendar_read', 'completed', 'partition={calendar_partition_key}; rows={len(calendar_df)}', table='CALENDAR_TABLE_NAME')+'\nlog_phase = "select_sessions"')
edit.insert('continue', event(owner, 'calendar_partition', 'skipped', 'partition={calendar_partition_key}; reason=no_required_sessions; calendar_index={log_calendar_index}/{len(calendar_partition_keys)}', table='CALENDAR_TABLE_NAME'))
edit.insert('minute_table = minute_dataset.to_table', '''log_minute_partition = minute_partition_key
log_phase = "minute_read"
''' + event(owner, 'minute_read', 'started', 'partition={minute_partition_key}; sessions={len(selected_underlying_df)}; columns=contract_code,bar_at', table='MINUTE_TABLE_NAME'))
edit.insert('actual_keys =', event(owner, 'minute_read', 'completed', 'partition={minute_partition_key}; rows={minute_table.num_rows}; columns=contract_code,bar_at', table='MINUTE_TABLE_NAME')+'\nlog_phase = "expand_expected"')
edit.insert('expected_points =', event(owner, 'expand_expected', 'started', 'partition={minute_partition_key}; sessions={len(selected_underlying_df)}'))
edit.insert('missing_points = expected_points.join', event(owner, 'expand_expected', 'completed', 'partition={minute_partition_key}; expected_keys={expected_count}')+'\nlog_phase = "key_difference"\n'+event(owner, 'key_difference', 'started', 'partition={minute_partition_key}'))
edit.insert('missing_counts =', event(owner, 'key_difference', 'completed', 'partition={minute_partition_key}; expected_keys={expected_count}; missing_keys={missing_points.height}; persisted=false')+'\nlog_phase = "calendar_update"')
edit.insert('missing_output =', 'log_phase = "missing_staging_write"\n'+event(owner, 'missing_staging', 'started', 'partition={missing_partition_key}; rows={missing_points.height}; persisted=false'))
edit.insert('staged_missing_dataset = ds.dataset', 'log_phase = "missing_staging_readback"')
edit.insert('missing_digests[missing_partition_key] =', event(owner, 'missing_staging', 'completed', 'partition={missing_partition_key}; rows={missing_arrow.num_rows}; persisted=false'), after=True)
edit.insert('total_missing +=', 'log_underlying_groups += 1\n'+event(owner, 'underlying_partition', 'completed', 'partition={minute_partition_key}; processed_groups={log_underlying_groups}; sessions={len(selected_underlying_df)}; expected_keys={expected_count}; actual_keys={partition_actual}; missing_keys={partition_missing}; total_sessions={total_sessions}; total_missing={total_missing}; persisted=false'), after=True)
edit.insert('calendar_arrow = validate_calendar_audit_output', 'log_phase = "calendar_staging_write"\n'+event(owner, 'calendar_staging', 'started', 'partition={calendar_partition_key}; rows={len(calendar_df)}; persisted=false', table='CALENDAR_TABLE_NAME'))
edit.insert('staged_calendar_dataset = ds.dataset', 'log_phase = "calendar_staging_readback"')
edit.insert('calendar_digests[calendar_partition_key] =', event(owner, 'calendar_staging', 'completed', 'partition={calendar_partition_key}; calendar_index={log_calendar_index}/{len(calendar_partition_keys)}; staged_calendar_partitions={len(calendar_digests)}; persisted=false', table='CALENDAR_TABLE_NAME'), after=True)
edit.insert('staged_missing_dataset = open_exact_dataset', 'log_phase = "staging_final_readback"\n'+event(owner, 'staging_final_readback', 'started', 'calendar_partitions={len(calendar_digests)}; missing_partitions={len(missing_digests)}; expected_missing_rows={total_missing}'))
edit.insert('return {', event(owner, 'build_audit', 'completed', 'sessions={total_sessions}; expected_keys={total_expected}; actual_keys={total_actual}; missing_keys={total_missing}; calendar_partitions={len(calendar_digests)}; missing_partitions={len(missing_digests)}; persisted=false', prefix='missing_audit_total'))
boundary(edit, 'build_audit', fields='lake_root={lake_root}; detected_at={detected_at}; missing_staging={missing_staging_path}; calendar_staging={calendar_staging_path}; persisted=false', initial_phase='prepare_paths', setup='''log_calendar_index = 0
log_underlying_groups = 0
log_partition = None
log_minute_partition = None''', failure_fields='; calendar_partition={log_partition}; minute_partition={log_minute_partition}; calendar_index={log_calendar_index}; processed_groups={log_underlying_groups}; persisted=false')

edit = FunctionEdit('c08-commit', 'commit_full_audit')
owner = 'commit_full_audit'
edit.insert('if missing_had_existing:', 'log_phase = "missing_install"\n'+event(owner, 'missing_install', 'started', 'run_id={run_id}; target={missing_target_path}; rows={total_missing}; batch_state=pending'))
edit.insert('shutil.move(', event(owner, 'missing_install', 'completed', 'run_id={run_id}; rows={total_missing}; batch_state=pending'), after=True, contains='str(missing_staging_path), str(missing_target_path)')
edit.insert('calendar_backup_path.mkdir', 'log_phase = "prepare_calendar_install"')
edit.insert('relative_path = pathlib.Path', 'log_phase = "calendar_install"\nlog_partition = partition_key\n'+event(owner, 'calendar_install', 'started', 'run_id={run_id}; partition={partition_key}; installed_calendar_partitions={log_installed_calendar}/{len(calendar_digests)}; batch_state=pending', table='CALENDAR_TABLE_NAME'))
edit.insert('shutil.move(str(source_path)', 'log_installed_calendar += 1\n'+event(owner, 'calendar_install', 'completed', 'run_id={run_id}; partition={partition_key}; installed_calendar_partitions={log_installed_calendar}/{len(calendar_digests)}; batch_state=pending', table='CALENDAR_TABLE_NAME'), after=True)
edit.insert('committed_missing_dataset =', 'log_phase = "missing_formal_readback"\nlog_partition = None\n'+event(owner, 'missing_formal_readback', 'started', 'run_id={run_id}; partitions={len(missing_digests)}; rows={total_missing}; batch_state=pending'))
edit.insert('table = committed_missing_dataset.to_table', 'log_phase = "missing_formal_leaf"\nlog_partition = partition_key\n'+event(owner, 'missing_formal_leaf', 'started', 'run_id={run_id}; partition={partition_key}; batch_state=pending'))
edit.insert('if table_digest(table, FUTURES_MISSING_BAR_SCHEMA', 'log_verified_missing += 1\n'+event(owner, 'missing_formal_leaf', 'completed', 'run_id={run_id}; partition={partition_key}; checked_partitions={log_verified_missing}/{len(missing_digests)}; rows={table.num_rows}; batch_state=pending'), after=True)
edit.insert('committed_calendar_dataset =', event(owner, 'missing_formal_readback', 'completed', 'run_id={run_id}; partitions={log_verified_missing}; rows={total_missing}; batch_state=pending')+'\nlog_phase = "calendar_formal_readback"\nlog_partition = None\n'+event(owner, 'calendar_formal_readback', 'started', 'run_id={run_id}; partitions={len(calendar_digests)}; batch_state=pending', table='CALENDAR_TABLE_NAME'))
edit.insert('table = committed_calendar_dataset.to_table', 'log_phase = "calendar_formal_leaf"\nlog_partition = partition_key\n'+event(owner, 'calendar_formal_leaf', 'started', 'run_id={run_id}; partition={partition_key}; batch_state=pending', table='CALENDAR_TABLE_NAME'))
edit.insert('if table_digest(table, FUTURES_BAR_CALENDAR_SCHEMA', 'log_verified_calendar += 1\n'+event(owner, 'calendar_formal_leaf', 'completed', 'run_id={run_id}; partition={partition_key}; checked_partitions={log_verified_calendar}/{len(calendar_digests)}; rows={table.num_rows}; batch_state=pending', table='CALENDAR_TABLE_NAME'), after=True)
edit.insert('for partition_key, expected_digest in calendar_digests.items():', event(owner, 'calendar_formal_readback', 'completed', 'run_id={run_id}; partitions={log_verified_calendar}; batch_state=pending', table='CALENDAR_TABLE_NAME'), after=True)
edit.insert('for destination_path, saved_path, relative_path, had_existing in reversed', '''log_failure_phase = log_phase
log_phase = "rollback"
''' + event(owner, 'rollback', 'started', 'run_id={run_id}; failed_phase={log_failure_phase}; calendar_targets={len(moved_calendar_partitions)}; missing_backup={missing_backup_path}; calendar_backup={calendar_backup_path}'))
edit.insert('if had_existing and saved_path.exists():', 'log_restored_calendar += 1\n'+event(owner, 'rollback', 'running', 'run_id={run_id}; partition={relative_path}; restored_calendar_partitions={log_restored_calendar}/{len(moved_calendar_partitions)}', table='CALENDAR_TABLE_NAME'), after=True)
edit.insert('if missing_had_existing and missing_backup_path.exists():', event(owner, 'rollback', 'completed', 'run_id={run_id}; restored_calendar_partitions={log_restored_calendar}; audit_state=restored'), after=True)
edit.insert('cleanup_recovery_paths = False', event(owner, 'rollback', 'failed', 'run_id={run_id}; restored_calendar_partitions={log_restored_calendar}; error={type(rollback_error).__name__}; missing_backup={missing_backup_path}; calendar_backup={calendar_backup_path}; missing_quarantine={missing_quarantine_path}; calendar_quarantine={calendar_quarantine_path}; audit_state=unresolved'), after=True)
edit.insert('shutil.rmtree(missing_staging_path', 'log_phase = "cleanup"')
edit.insert('return (total_missing, len(calendar_digests))', event(owner, 'commit', 'completed', 'run_id={run_id}; write=true; committed_missing_rows={total_missing}; committed_calendar_partitions={len(calendar_digests)}; persisted=true', prefix='committed')+'\n'+event(owner, 'audit_state', 'completed', 'run_id={run_id}; calendar_table={CALENDAR_TABLE_NAME}; scope=missing_table_and_touched_calendar_leaves; persisted=true; date_watermark=none; completion_and_quality_preserved=true'))
boundary(edit, 'commit', fields='run_id={run_id}; lake_root={lake_root}', initial_phase='prepare_paths', setup='''log_partition = None
log_installed_calendar = 0
log_verified_missing = 0
log_verified_calendar = 0
log_restored_calendar = 0
log_failure_phase = None''', failed_phase='log_failure_phase or log_phase', failure_fields='; recovery_phase={log_phase}; partition={log_partition}; installed_calendar_partitions={log_installed_calendar}; verified_missing_partitions={log_verified_missing}; verified_calendar_partitions={log_verified_calendar}; restored_calendar_partitions={log_restored_calendar}')

edit = FunctionEdit('c08-cli', 'main')
edit.remove('log_build_started_at =')
edit.remove('log_commit_started_at =')
for prefix in ('planning_progress: table=', 'missing_audit_total: table=', 'committed: table='):
    for node in ast.walk(edit.function):
        if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call) or ast.unparse(node.value.func) != 'click.echo':
            continue
        text = ast.unparse(node)
        if prefix in text and ('phase=build_audit;' in text or 'phase=commit; status=started;' in text or 'committed: table=' in text):
            edit.replacements[node.lineno - 1] = node.end_lineno
edit.apply()

cells['c08-dataset-heading'].source += '''

读取函数自行报告发现、打开、契约检查和失败阶段；`materialized=false` 表示仅打开和检查文件，尚未读取业务行。fragment 检查沿现有遍历在首个及每 250 个文件报告数量；分区发现也沿原目录遍历报告已访问文件和已发现叶数。`table_digest()` 报告契约转换、排序、规范化与摘要完成，列规范化在首列、每 16 列和末列报告进度。实际业务行的读取日志仍紧贴生成/提交函数中的 `to_table()`，不新增读取或日志包装函数。'''
cells['c08-output-heading'].source += '''

两个 validator 自行报告输入行数、目标叶、完成与失败阶段；空缺失表和无 required 行的日历叶也有完成日志。类型转换、主键与分区归属、计数关系和审计时间的原有检查均保留。'''
cells['c08-build-heading'].source += '''

生成函数自行报告全局就绪、当前日历叶与品种月读取、理论展开、主键求差、两类 staging 写入/复读和整批验收。沿现有循环累计日历进度、已处理品种组、Session 数与缺失键数，不为日志另扫数据；无 required 行的叶报告跳过。`missing_audit_total:` 由本函数在所有 staging 验收完成后输出，始终记 `persisted=false`。失败报告当前阶段、日历叶、品种月及处理进度，并原样抛错。'''
cells['c08-commit-heading'].source += '''

提交函数自行报告缺失表根和日历叶安装、两表正式复读及恢复进度。安装或单叶验收通过仍记 `batch_state=pending`；只有两表全部验收并结束既有清理，才输出 `committed:` 和 `phase=audit_state; persisted=true; date_watermark=none`。这是缺失快照与日历审计字段共同落盘，不改变完成凭证或质量结论。恢复完整记 `audit_state=restored`，恢复未完成记 `audit_state=unresolved` 并给出备份、隔离路径；失败不报告成功落盘。'''
cells['c08-cli-heading'].source = cells['c08-cli-heading'].source.replace(
    '再调用全量生成、输出汇总，并决定是否协调提交。', '再调用全量生成函数取得摘要与统计，并决定是否协调提交。')
cells['c08-cli-heading'].source = cells['c08-cli-heading'].source.replace(
    '`committed:` 仅在协调提交返回后报告持久结果。', '`committed:` 由提交函数在两表协调提交成功后报告持久结果。')
cells['c08-cli-heading'].source = cells['c08-cli-heading'].source.replace(
    '本轮日志起止与汇总仍由入口报告，读取、生成及提交函数的细分进度尚未迁入函数。',
    '读取、分区发现、摘要、输出校验、生成和提交函数分别报告自身起止、进度及失败；入口只报告运行范围、跳过提交和最终结果，不重复输出生成汇总或提交起止。')

diagram_replacements = {
    'b08-flow-read': [
        ('open_exact_dataset：发现 Parquet 并打开', '读取函数报告开始；发现 Parquet 并打开'),
        ('重建逻辑 Schema；检查稳定身份和所有 fragment', '检查逻辑契约和 fragment；沿原遍历报告进度'),
        ('返回 Dataset；调用方按分区条件物化', '报告完成；materialized=false；调用方物化'),
        ('跳过根标记；检查目录结构并解析叶键', '跳过根标记；解析叶键；报告发现数量'),
        ('摘要覆盖完整内容与有效位', '报告列进度与摘要完成；异常报告阶段并抛错'),
    ],
    'b08-flow-validate': [
        ('缺失明细 Arrow 输入', '报告缺失明细校验开始与输入行数'),
        ('返回已校验缺失表', '报告完成；返回已校验缺失表'),
        ('完整日历叶输入', '报告日历叶校验开始与输入行数'),
        ('返回已校验日历表；质量与旁证保持原值', '报告完成；返回日历表；保留质量与旁证'),
        ('X["抛错"]', 'X["报告失败阶段；原样抛错"]'),
    ],
    'b08-flow-build': [
        ('打开上游；发现 1m 日历叶；检查全局完成状态', '报告生成开始；打开上游、发现叶并检查就绪'),
        ('读取当前完整日历叶；选择 required 行', '报告当前日历叶读取进度；选择 required 行'),
        ('按品种读取分钟两列键；展开理论分钟并核对数量', '报告品种月读取、理论展开与求差进度'),
        ('累计本组 Session 和主键数量', '累计并报告本组及整批 Session、主键数量'),
        ('验收 staging 总量和契约；返回摘要与统计', 'staging 总验收后报告生成完成；persisted=false'),
    ],
    'b08-flow-commit': [
        ('确认目标和临时路径位于 silver 内', '报告提交开始；确认路径位于 silver 内'),
        ('依次备份并安装全部触达日历叶', '逐叶安装并报告进度；batch_state=pending'),
        ('全部验收通过；清理临时目录后返回提交数量', '全部通过并清理后；报告提交与 audit_state 已落盘'),
        ('清理 staging、backup、隔离目录；抛出原异常', '报告恢复完整；清理临时目录；抛出原异常'),
        ('保留恢复现场；抛出回滚异常', '报告恢复未完成及现场路径；抛出回滚异常'),
    ],
    'b08-flow-main': [
        ('报告生成开始；调用 build_full_audit_staging', '调用生成函数；函数自行报告进度与汇总'),
        ('汇总理论、实际、缺失键及 Session 数', '取得已验证的 staging 摘要与统计'),
        ('报告提交开始；调用 commit_full_audit', '调用提交函数；函数自行报告安装、验收和恢复'),
        ('提交返回后报告 persisted=true', '提交函数报告 audit_state 已落盘后返回'),
    ],
}
for cell_id, pairs in diagram_replacements.items():
    for old, new in pairs:
        assert cells[cell_id].source.count(old) == 1, (cell_id, old)
        cells[cell_id].source = cells[cell_id].source.replace(old, new)


class RemoveLogging(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(t, ast.Name) and t.id.startswith('log_') for t in node.targets):
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
            return self.generic_visit(node).body
        return self.generic_visit(node)


assert before.metadata == notebook.metadata
assert [c.id for c in before.cells] == [c.id for c in notebook.cells]
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        assert comments(old.source) == comments(new.source), old.id
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        compile(new.source, str(PATH), 'exec')
        assert ast.dump(RemoveLogging().visit(ast.parse(old.source))) == ast.dump(RemoveLogging().visit(ast.parse(new.source))), old.id
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print('b08 function logs updated; business AST, comments, cell metadata and execution state preserved.')
