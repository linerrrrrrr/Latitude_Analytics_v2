"""b07 第 5—6 项：函数报告自身进度，保留计算、I/O 与恢复语义。"""
import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b07_suspected_session_reconciliation.ipynb'
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

    def find(self, prefix):
        matches = [n for n in ast.walk(self.function) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)]
        assert len(matches) == 1, (self.function.name, prefix, len(matches))
        return matches[0]

    def insert(self, prefix, source, *, after=False):
        node = self.find(prefix)
        index = node.end_lineno if after else node.lineno - 1
        self.insertions.setdefault(index, []).append(textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * node.col_offset))

    def replace(self, prefix, source):
        node = self.find(prefix)
        replacement = textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * node.col_offset) if source else ''
        self.replacements[node.lineno - 1] = (node.end_lineno, replacement)

    def render(self, start, end):
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
        return ''.join(body)

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


def event(function, phase, status, *, table='CALENDAR_TABLE_NAME', fields='', prefix='planning_progress', elapsed=True):
    return f'''click.echo(
    f"{prefix}: table={{{table}}}; function={function}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields and elapsed else ''}{'elapsed_s={time.perf_counter() - log_started_at:.3f}' if elapsed else ''}"
)'''


def boundary(edit, phase, *, table='CALENDAR_TABLE_NAME', fields='', setup='', initial_phase=None, failure_fields=''):
    edit.wrap(
        f'log_started_at = time.perf_counter()\nlog_phase = "{initial_phase or phase}"\n{setup}\n'
        + event(edit.function.name, phase, 'started', table=table, fields=fields, elapsed=False),
        'except Exception as log_error:\n' + textwrap.indent(event(
            edit.function.name, phase, 'failed', table=table,
            fields=f'failed_phase={{log_phase}}; error={{type(log_error).__name__}}{failure_fields}',
        ) + '\nraise', '    '),
    )


edit = FunctionEdit('595a964d', 'open_exact_dataset')
edit.insert('dataset = ds.dataset', 'log_phase = "dataset_open"')
edit.insert('actual_schema = reconstructed_schema', 'log_phase = "logical_schema"')
edit.insert('marker_path = table_path', 'log_phase = "schema_marker"')
edit.insert('return dataset', event('open_exact_dataset', 'dataset_open', 'completed', table='table_path.name', fields='label={label}; path={table_path}; outcome=ready; materialized=false'))
boundary(edit, 'dataset_open', table='table_path.name', fields='label={label}; path={table_path}', initial_phase='discovery')

edit = FunctionEdit('595a964d', 'validate_read_fragments')
edit.insert('for fragment in dataset.get_fragments', 'log_phase = "fragment_schema"')
edit.insert('if not physically_and_identity_compatible(', '''
    log_fragment_count += 1
    if log_fragment_count == 1 or log_fragment_count % 250 == 0:
''' + textwrap.indent(event('validate_read_fragments', 'fragment_schema', 'running', table='log_table_name', fields='label={label}; checked_fragments={log_fragment_count}'), '        '), after=True)
edit.insert('for fragment in dataset.get_fragments', event('validate_read_fragments', 'fragment_schema', 'completed', table='log_table_name', fields='label={label}; checked_fragments={log_fragment_count}; materialized=false'), after=True)
boundary(edit, 'fragment_schema', table='log_table_name', fields='label={label}; filter={filter_expression}', initial_phase='file_schema', setup='''
log_fragment_count = 0
log_table_name = (
    CALENDAR_TABLE_NAME if schema is FUTURES_BAR_CALENDAR_SCHEMA
    else CONTRACT_TABLE_NAME if schema is FUTURES_CONTRACT_CALENDAR_SCHEMA
    else DAILY_TABLE_NAME if schema is FUTURES_DAILY_SCHEMA
    else MINUTE_TABLE_NAME if schema is FUTURES_MINUTE_SCHEMA
    else "unknown"
)
''', failure_fields='; label={label}; checked_fragments={log_fragment_count}')

edit = FunctionEdit('7d33cf49', 'validate_calendar_frame')
edit.insert('if checked_df.duplicated', 'log_phase = "primary_key"')
edit.insert('for row in table.to_pylist', 'log_phase = "business_rules"')
edit.insert("if row['evidence_level'] == 'reconciled':", '''
    log_checked_rows += 1
    if log_checked_rows == 1 or log_checked_rows % 10000 == 0:
''' + textwrap.indent(event('validate_calendar_frame', 'business_rules', 'running', fields='context={context}; checked_rows={log_checked_rows}; rows={len(frame)}'), '        '), after=True)
edit.replace('return checked_df.sort_values(', '''
log_phase = "sort_output"
validated_calendar_df = checked_df.sort_values(
    CALENDAR_PRIMARY_KEY
).reset_index(drop=True)
''' + event('validate_calendar_frame', 'validate_calendar', 'completed', fields='context={context}; rows={len(validated_calendar_df)}; persisted=false') + '\nreturn validated_calendar_df')
boundary(edit, 'validate_calendar', fields='context={context}; rows={len(frame)}', initial_phase='conversion', setup='log_checked_rows = 0', failure_fields='; context={context}; checked_rows={log_checked_rows}')

edit = FunctionEdit('ea42f696', 'reconcile_partition')
edit.insert('for index in candidate_indices:', 'log_phase = "candidate_evidence"\n' + event('reconcile_partition', 'candidate_plan', 'completed', fields='requested_candidates={len(candidate_keys)}; selected_candidates={len(candidate_indices)}; persisted=false'))
edit.insert("session_number = row['session_number']", 'log_candidate_key = (contract_code, trading_date, session_number)\nlog_phase = "candidate_evidence"', after=True)
edit.insert('aggregated_values =', 'log_phase = "aggregate_evidence"')
edit.insert('if issues:', 'log_phase = "compare_evidence"')
edit.insert('changed_keys.add(', '''
log_processed_candidates += 1
log_reconciled_candidates += int(updated_df.at[index, "evidence_level"] == "reconciled")
if log_processed_candidates == 1 or log_processed_candidates % 100 == 0 or log_processed_candidates == len(candidate_indices):
''' + textwrap.indent(event('reconcile_partition', 'reconcile_candidate', 'running', fields='candidate={log_candidate_key}; processed_candidates={log_processed_candidates}/{len(candidate_indices)}; reconciled={log_reconciled_candidates}; warning={log_processed_candidates}; persisted=false'), '    '), after=True)
edit.insert('reconciled_table = pandas_to_arrow', 'log_phase = "output_conversion"')
edit.insert('return (reconciled_df, changed_df, 0)', event('reconcile_partition', 'reconcile', 'completed', prefix='reconciliation_plan', fields='rows={len(reconciled_df)}; changed_candidates={len(changed_df)}; reconciled={log_reconciled_candidates}; warning={log_processed_candidates}; evidence_rule={EVIDENCE_RULE_VERSION}; persisted=false'))
boundary(edit, 'reconcile', fields='calendar_rows={len(calendar_df)}; requested_candidates={len(candidate_keys)}; contract_rows={len(contract_df)}; daily_rows={len(daily_df)}; minute_rows={len(minute_df)}; persisted=false', initial_phase='candidate_selection', setup='''
log_candidate_key = None
log_processed_candidates = 0
log_reconciled_candidates = 0
''', failure_fields='; candidate={log_candidate_key}; processed_candidates={log_processed_candidates}; persisted=false')

edit = FunctionEdit('96342530', 'commit_calendar_partitions')
edit.insert('return 0', event('commit_calendar_partitions', 'commit', 'completed', fields='outcome=no_partitions; committed_partitions=0; committed_calendar_rows=0; evidence_state=unchanged'))
edit.insert('checked_df = validate_calendar_frame', event('commit_calendar_partitions', 'prepare_leaf', 'started', fields='partition={partition_key}; rows={len(frame)}'))
edit.insert('expected_tables[partition_key] = pandas_to_arrow', 'log_prepared_partitions += 1\n' + event('commit_calendar_partitions', 'prepare_leaf', 'completed', fields='partition={partition_key}; prepared_partitions={log_prepared_partitions}/{len(partition_frames)}; rows={len(checked_df)}; persisted=false'), after=True)
edit.insert('combined_df = pd.concat', 'log_phase = "combine_output"')
edit.insert('silver_root = lake_root.resolve()', 'log_phase = "prepare_paths"')
edit.insert('staging_path.mkdir', 'log_phase = "staging_write"\n' + event('commit_calendar_partitions', 'staging_write', 'started', fields='run_id={run_id}; partitions={len(expected_tables)}; rows={len(combined_table)}'))
edit.insert('expected_file_schema = pa.schema', 'log_phase = "staging_schema"\n' + event('commit_calendar_partitions', 'staging_write', 'completed', fields='run_id={run_id}; rows={len(combined_table)}; persisted=false'))
edit.insert('for partition_key, expected_table in expected_tables.items():', 'log_phase = "staging_readback"\n' + event('commit_calendar_partitions', 'staging_readback', 'started', fields='run_id={run_id}; partitions={len(expected_tables)}'))
edit.insert('if len(staged_primary_key_df) != len(expected_table):', 'log_staged_partitions += 1\n' + event('commit_calendar_partitions', 'staging_readback', 'running', fields='run_id={run_id}; partition={partition_key}; checked_partitions={log_staged_partitions}/{len(expected_tables)}; rows={len(staged_primary_key_df)}; persisted=false'), after=True)
edit.insert('moved_partitions:', event('commit_calendar_partitions', 'staging_readback', 'completed', fields='run_id={run_id}; checked_partitions={log_staged_partitions}; persisted=false') + '\nlog_phase = "prepare_install"')
edit.insert('relative_path = pathlib.Path', 'log_phase = "install"\n' + event('commit_calendar_partitions', 'install', 'started', fields='run_id={run_id}; partition={partition_key}; verified_partitions={log_verified_partitions}/{len(expected_tables)}; batch_state=pending'))
edit.insert('committed_dataset = ds.dataset', 'log_phase = "formal_readback"\n' + event('commit_calendar_partitions', 'formal_readback', 'started', fields='run_id={run_id}; partition={partition_key}; batch_state=pending'))
edit.insert('if len(committed_primary_key_df) != len(expected_tables[partition_key]):', 'log_verified_partitions += 1\n' + event('commit_calendar_partitions', 'formal_readback', 'completed', fields='run_id={run_id}; partition={partition_key}; verified_partitions={log_verified_partitions}/{len(expected_tables)}; rows={len(committed_primary_key_df)}; batch_state=pending'), after=True)
edit.insert('for destination_path, saved_path, relative_path in reversed(', '''
log_failure_phase = log_phase
log_phase = "rollback"
''' + event('commit_calendar_partitions', 'rollback', 'started', fields='run_id={run_id}; failed_phase={log_failure_phase}; moved_partitions={len(moved_partitions)}; backup={backup_path}; quarantine={quarantine_path}'))
edit.insert('if saved_path.exists():', 'log_restored_partitions += 1\n' + event('commit_calendar_partitions', 'rollback', 'running', fields='run_id={run_id}; partition={relative_path}; restored_partitions={log_restored_partitions}/{len(moved_partitions)}'), after=True)
edit.insert('for destination_path, saved_path, relative_path in reversed(', event('commit_calendar_partitions', 'rollback', 'completed', fields='run_id={run_id}; restored_partitions={log_restored_partitions}; evidence_state=restored'), after=True)
edit.insert('cleanup_recovery_paths = False', event('commit_calendar_partitions', 'rollback', 'failed', fields='run_id={run_id}; restored_partitions={log_restored_partitions}/{len(moved_partitions)}; error={type(rollback_error).__name__}; backup={backup_path}; quarantine={quarantine_path}; evidence_state=unresolved'), after=True)
edit.insert('return len(combined_table)', event('commit_calendar_partitions', 'commit', 'completed', prefix='committed', fields='write=true; committed_partitions={len(expected_tables)}; committed_calendar_rows={len(combined_table)}; run_id={run_id}; persisted=true') + '\n' + event('commit_calendar_partitions', 'evidence_state', 'completed', fields='scope=committed_calendar_leaves; partitions={len(expected_tables)}; run_id={run_id}; persisted=true; date_watermark=none; default_candidate_source=fact_futures_minute:formal_empty_session'))
boundary(edit, 'commit', fields='partitions={len(partition_frames)}; lake_root={lake_root}', initial_phase='validate_output', setup='''
log_prepared_partitions = 0
log_staged_partitions = 0
log_verified_partitions = 0
log_restored_partitions = 0
log_failure_phase = None
''', failure_fields='; original_failed_phase={log_failure_phase}; prepared_partitions={log_prepared_partitions}; verified_partitions={log_verified_partitions}; restored_partitions={log_restored_partitions}')

# 入口保留它实际拥有的读取/调度；不为直接 PyArrow 读取增加转发包装。
edit = FunctionEdit('57c9061e', 'main')
for prefix in [
    'log_reconcile_started_at =',
    "click.echo(f'planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=reconcile; status=started')",
    'log_commit_started_at =',
    "click.echo(f'planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=commit; status=started; ",
    "click.echo(f'committed: table={CALENDAR_TABLE_NAME}; function=main; phase=commit; status=completed; ",
]:
    edit.replace(prefix, '')
for assignment, table, partition in [
    ('calendar_partition_table = calendar_dataset.to_table', 'CALENDAR_TABLE_NAME', 'calendar_partition_key'),
    ('contract_table = contract_dataset.to_table', 'CONTRACT_TABLE_NAME', 'contract_partition_key'),
]:
    edit.insert(assignment, event('main', 'leaf_read', 'started', table=table, fields=f'partition={{{partition}}}'))
for assignment, table, partition, frame in [
    ('calendar_partition_df = arrow_to_pandas', 'CALENDAR_TABLE_NAME', 'calendar_partition_key', 'calendar_partition_df'),
    ('contract_df = arrow_to_pandas', 'CONTRACT_TABLE_NAME', 'contract_partition_key', 'contract_df'),
]:
    edit.insert(assignment, event('main', 'leaf_read', 'completed', table=table, fields=f'partition={{{partition}}}; rows={{len({frame})}}'), after=True)
for stem, table in [('daily', 'DAILY_TABLE_NAME'), ('minute', 'MINUTE_TABLE_NAME')]:
    edit.insert(f'{stem}_tables.append(', event('main', 'leaf_read', 'started', table=table, fields='partition={fact_partition_key}'))
    edit.insert(f'{stem}_tables.append(', event('main', 'leaf_read', 'completed', table=table, fields=f'partition={{fact_partition_key}}; rows={{len({stem}_tables[-1])}}'), after=True)
edit.insert('for calendar_partition_key in calendar_partition_keys:', 'log_processed_partitions = 0\n' + event('main', 'reconcile_batch', 'started', fields='planned_partitions={len(calendar_partition_keys)}; selected_candidates={len(candidate_df)}'))
# 同一条件还出现在预览处；按完整语句定位分区调度进度。
edit.insert('if not changed_df.empty:\n    partition_frames[', 'log_processed_partitions += 1\n' + event('main', 'reconcile_batch', 'running', fields='partition={calendar_partition_key}; processed_partitions={log_processed_partitions}/{len(calendar_partition_keys)}; changed_candidates_in_partition={len(changed_df)}; persisted=false'), after=True)
edit.apply()
cells['57c9061e'].source = cells['57c9061e'].source.replace('phase=reconcile; status=completed', 'phase=reconcile_batch; status=completed').replace('time.perf_counter() - log_reconcile_started_at', 'time.perf_counter() - log_started_at')

notes = {
    '2a85dca5': '`validate_calendar_frame()` 自行报告转换、主键检查、逐行业务校验与排序的起止和失败阶段；沿原行遍历在首行及每 10000 行报告进度，不为日志另做全表扫描。',
    '782170ec': '`open_exact_dataset()` 自行报告路径发现、Dataset 打开与契约检查；`materialized=false` 表示尚未读取业务行。`validate_read_fragments()` 沿原文件遍历在首个及每 250 个文件报告进度，结束时报告实际检查数。实际 `to_table()` 仍在入口，读取起止与返回行数就在对应调用位置报告；不增加读取包装函数或额外 I/O。',
    'd9a63ecb': '`reconcile_partition()` 自行报告候选选择、证据准备、聚合、比较和输出转换；在首个、每 100 个及最后一个候选报告累计进度。生成结果始终标记 `persisted=false`；失败记录候选、当前阶段与已处理数量并原样抛错。',
    'e492cfe6': '提交函数自行报告准备、暂存写入与复读、安装、正式复读及回滚。单叶验收通过仍记 `batch_state=pending`，因为本批后续失败会共同回滚；全部正式叶验收和原有清理结束后，才报告 `committed` 与 `phase=evidence_state; persisted=true`。这里的状态推进是完整日历叶中的旁证落盘，明确 `date_watermark=none`；不新增日期水位文件，也不把候选条数与完整叶行数混用。空提交报告状态未变，回滚完整与恢复未完成分别报告。',
}
for cell_id, note in notes.items():
    cells[cell_id].source += '\n\n' + note
cells['a006bfaa'].source = cells['a006bfaa'].source.replace('`committed:` 只在提交返回后输出', '`committed:` 由提交函数在本批验收与清理成功后输出')
old = '本轮统一入口现有日志及运行边界，函数级进度归位留待后续步骤。普通异常继续向上抛出，不输出成功结束日志。'
assert old in cells['a006bfaa'].source
cells['a006bfaa'].source = cells['a006bfaa'].source.replace(old, '读取函数、业务校验、旁证生成和提交函数报告各自起止、进度及失败阶段。入口保留参数门禁、实际数据物化、整批调度、累计计数、预览与运行结果；不重复报告函数级生成或提交起止。普通异常继续向上抛出，不输出成功结束日志。')

diagram_replacements = {
    'b07-flow-validate': [('按权威 Schema 转换完整日历叶', '记录开始；按权威 Schema 转换完整叶'), ('逐行检查枚举、原因、分区派生值和 Session 结构', '检查枚举、派生值和 Session；沿原行遍历报告进度'), ('按主键排序返回', '排序；报告完成、行数与耗时'), ('抛错停止提交', '报告失败阶段；原样抛错')],
    'b07-flow-read': [('open_exact_dataset：确认存在并打开 Dataset', 'open_exact_dataset：记录开始；确认存在并打开'), ('返回 Dataset；暂不物化业务行', '报告打开完成；materialized=false'), ('validate_read_fragments：检查可能读取的文件契约', 'validate_read_fragments：检查文件契约并报告数量进度'), ('调用方按过滤条件执行 to_table', '调用方记录读取起止；to_table 返回后报告行数')],
    'b07-flow-reconcile': [('复制完整日历叶；选出候选主键', '记录生成开始；复制完整叶并选出候选'), ('更新证据来源与审计时间；保留调度和完成状态', '更新内存证据；报告候选进度，persisted=false'), ('契约转换与排序；返回完整叶、候选子集、0', '转换排序后报告完成；返回完整叶、候选子集、0'), ('抛错停止', '报告候选与失败阶段；抛错')],
    'b07-flow-commit': [('返回 0', '报告无分区、状态未变；返回 0'), ('每个完整叶业务校验一次；确认分区归属', '记录提交开始；逐叶校验并报告准备进度'), ('仅复读当前正式叶的契约、主键和行数', '复读当前正式叶；通过仍记 batch_state=pending'), ('清理临时目录；返回完整叶总行数', '清理后报告提交及旁证已落盘；返回行数'), ('清理临时目录；抛出原异常', '报告恢复完整；清理并抛出原异常'), ('保留备份与隔离目录；报告路径并抛错', '报告恢复未完成；保留备份与隔离目录并抛错')],
    'b07-flow-main': [('调用 reconcile_partition；汇集完整叶和候选结果', '调用生成函数；汇集结果并报告整批进度'), ('提交开始日志；调用 commit_calendar_partitions', '调用 commit_calendar_partitions；函数自行报告'), ('提交成功日志；运行结束日志', '提交返回后只报告运行结束')],
}
for cell_id, pairs in diagram_replacements.items():
    for old, new in pairs:
        assert old in cells[cell_id].source, (cell_id, old)
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
            node = self.generic_visit(node)
            return node.body
        return self.generic_visit(node)


def canonical(source):
    tree = RemoveLogging().visit(ast.parse(source))
    for node in ast.walk(tree):
        if not hasattr(node, 'body') or not isinstance(node.body, list):
            continue
        for index in range(len(node.body) - 2, -1, -1):
            assignment, following = node.body[index:index + 2]
            if (isinstance(assignment, ast.Assign) and len(assignment.targets) == 1
                    and isinstance(assignment.targets[0], ast.Name) and assignment.targets[0].id == 'validated_calendar_df'
                    and isinstance(following, ast.Return) and isinstance(following.value, ast.Name)
                    and following.value.id == 'validated_calendar_df'):
                following.value = assignment.value
                del node.body[index]
    return ast.dump(tree)


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
        assert canonical(old.source) == canonical(new.source), old.id
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print('b07 function logs updated; business AST, comments, cell metadata and execution state preserved.')
