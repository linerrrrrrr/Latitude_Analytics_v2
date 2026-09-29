"""a02/b01 第 5—6 项：日志归属函数；保留计算、I/O、提交及恢复语义。"""
import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01_exchange_report_calendar.ipynb'
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


def event(function, phase, status, *, table='TABLE_NAME', fields='', prefix='planning_progress'):
    return f'''click.echo(
    f"{prefix}: table={{{table}}}; function={function}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields else ''}elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
)'''


def boundary(edit, phase, *, table='TABLE_NAME', fields='', setup='', failure_fields='', failure_phase='log_phase'):
    edit.wrap(
        f'log_started_at = time.perf_counter()\nlog_phase = "{phase}"\n{setup}\n'
        + event(edit.function.name, phase, 'started', table=table, fields=fields),
        'except Exception as log_error:\n' + textwrap.indent(event(
            edit.function.name, phase, 'failed', table=table,
            fields=f'failed_phase={{{failure_phase}}}; error={{type(log_error).__name__}}{failure_fields}',
        ) + '\nraise', '    '),
    )


edit = FunctionEdit('5d05b12f', 'open_exact_dataset')
edit.insert('parquet_files =', 'log_phase = "discovery"')
edit.insert('dataset = ds.dataset', event('open_exact_dataset', 'discovery', 'completed', table='log_table_name', fields='label={label}; files={len(parquet_files)}; materialized=false') + '\nlog_phase = "dataset_open"')
edit.insert('if not physically_and_identity_compatible', 'log_phase = "schema"')
edit.insert('return dataset', event('open_exact_dataset', 'dataset_open', 'completed', table='log_table_name', fields='label={label}; path={table_path}; files={len(parquet_files)}; materialized=false'))
boundary(edit, 'dataset_open', table='log_table_name', fields='label={label}; path={table_path}; materialized=false', setup='log_table_name = UPSTREAM_TABLE_NAME if schema is FUTURES_VARIETY_CALENDAR_SCHEMA else TABLE_NAME', failure_fields='; label={label}; path={table_path}; materialized=false')

for name, table_name, return_name, finish_prefix in [
    ('validate_upstream_table', 'UPSTREAM_TABLE_NAME', 'validated_upstream_df', "if row['trading_date'].year"),
    ('validate_report_calendar_table', 'TABLE_NAME', 'validated_report_calendar_df', "if row['updated_at'] > now_utc:"),
]:
    edit = FunctionEdit('5d05b12f', name)
    edit.insert('checked = validate_arrow_table', 'log_phase = "convert"')
    edit.insert('if frame.duplicated', 'log_phase = "primary_key"')
    edit.insert('for row in checked.to_pylist():', 'log_phase = "business_validation"')
    edit.insert(finish_prefix, 'log_checked_rows += 1\n'
        'if log_checked_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
        + textwrap.indent(event(name, 'validate', 'running', table=table_name, fields='context={context}; checked_rows={log_checked_rows}/{table.num_rows}') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
    return_node = edit.find('return frame.sort_values')
    edit.replace('return frame.sort_values', 'log_phase = "sort"\n'
        + f'{return_name} = {ast.unparse(return_node.value)}\n'
        + event(name, 'validate', 'completed', table=table_name, fields=f'context={{context}}; checked_rows={{log_checked_rows}}; rows={{len({return_name})}}')
        + f'\nreturn {return_name}')
    boundary(edit, 'validate', table=table_name, fields='context={context}; rows={table.num_rows}', setup='log_checked_rows = 0\nlog_last_progress_at = log_started_at', failure_fields='; context={context}; checked_rows={log_checked_rows}')

edit = FunctionEdit('38ff53e0', 'build_expected_calendar')
edit.insert('existing_rows_by_key =', 'log_phase = "existing_index"')
edit.insert('upstream_rows =', 'log_phase = "upstream_conversion"')
edit.insert('for upstream_row in upstream_rows:', 'log_phase = "expand"')
edit.insert('for dataset_name in DATASET_NAMES:', 'log_processed_upstream += 1\n'
    'if log_processed_upstream % 1000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('build_expected_calendar', 'generate', 'running', fields='processed_upstream={log_processed_upstream}/{len(upstream_rows)}; generated_rows={len(expected_rows)}; persisted=false') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('if expected_rows:', 'log_phase = "output_frame"')
return_node = edit.find('return validate_report_calendar_table')
edit.replace('return validate_report_calendar_table', 'log_phase = "output_validation"\n'
    + f'expected_calendar_df = {ast.unparse(return_node.value)}\n'
    + event('build_expected_calendar', 'generate', 'completed', fields='processed_upstream={log_processed_upstream}; generated_rows={len(expected_calendar_df)}; persisted=false')
    + '\nreturn expected_calendar_df')
boundary(edit, 'generate', fields='upstream_rows={len(upstream_df)}; existing_rows={len(existing_df)}; report_types={len(DATASET_NAMES)}; persisted=false', setup='log_processed_upstream = 0\nlog_last_progress_at = log_started_at', failure_fields='; processed_upstream={log_processed_upstream}; persisted=false')

edit = FunctionEdit('b14ee43f', 'changed_partition_keys')
edit.insert('expected_keys =', 'log_phase = "partition_keys"')
edit.insert('for partition_key in sorted(', 'log_total_partitions = len(expected_keys | existing_keys)\nlog_phase = "compare"')
edit.insert('expected_mask =', 'log_partition = partition_key\n'
    'if log_compared_partitions == 0 or time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('changed_partition_keys', 'compare', 'running', fields='partition={partition_key}; compared_partitions={log_compared_partitions}/{log_total_partitions}; changed_partitions={len(changed_keys)}') + '\nlog_last_progress_at = time.perf_counter()', '    '))
edit.insert('if expected_digest != existing_digest:', 'log_compared_partitions += 1', after=True)
edit.insert('return changed_keys', event('changed_partition_keys', 'compare', 'completed', fields='compared_partitions={log_compared_partitions}/{log_total_partitions}; changed_partitions={len(changed_keys)}'))
boundary(edit, 'compare', fields='expected_rows={len(expected_df)}; existing_rows={len(existing_df)}', setup='log_compared_partitions = 0\nlog_partition = None\nlog_last_progress_at = log_started_at', failure_fields='; partition={log_partition}; compared_partitions={log_compared_partitions}')

edit = FunctionEdit('6f1d04d3', 'commit_partitions')
edit.insert('return 0', event('commit_partitions', 'commit', 'skipped', fields='reason=no_partitions; committed_rows=0; committed_partitions=0; calendar_state=unchanged; date_watermark=none'))
edit.insert('expected_df = validate_report_calendar_table', 'log_phase = "input_validation"')
edit.insert('expected_digest =', 'log_phase = "expected_digest"')
edit.insert('silver_root =', 'log_phase = "paths"')
edit.insert('staging_path.mkdir', 'log_phase = "staging_prepare"\n' + event('commit_partitions', 'staging_prepare', 'started', fields='run_id={run_id}; partitions={len(partition_keys)}; staging_path={staging_path}'))
edit.insert('changed_row_mask =', 'log_phase = "select_changed_rows"')
edit.insert('changed_row_mask |=', 'log_selected_partitions += 1\n'
    'if time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('commit_partitions', 'select_changed_rows', 'running', fields='partition={partition_key}; selected_partitions={log_selected_partitions}/{len(partition_keys)}') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('if not changed_rows_df.empty:', 'log_phase = "staging_write"\n' + event('commit_partitions', 'staging_write', 'started', fields='run_id={run_id}; rows={len(changed_rows_df)}; partitions={len(partition_keys)}'))
edit.insert('if not changed_rows_df.empty:', event('commit_partitions', 'staging_write', 'completed', fields='run_id={run_id}; rows={len(changed_rows_df)}; persisted=false'), after=True)
edit.insert('staged_dataset =', 'log_phase = "staging_readback"\n' + event('commit_partitions', 'staging_readback', 'started', fields='run_id={run_id}; expected_rows={len(changed_rows_df)}'))
edit.insert('if len(staged_df) !=', event('commit_partitions', 'staging_readback', 'completed', fields='rows={len(staged_df)}; persisted=false'), after=True)
edit.insert('staged_partition_table =', 'log_partition = partition_key\nlog_phase = "staging_leaf"\n' + event('commit_partitions', 'staging_leaf', 'started', fields='partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; expected_rows={len(expected_partition_df)}'))
edit.insert('if not expected_partition_df.empty', 'log_staged_partitions += 1\n' + event('commit_partitions', 'staging_leaf', 'completed', fields='partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; rows={len(staged_partition_df)}; persisted=false'), after=True)
edit.insert('target_had_existing =', 'log_phase = "install"\nlog_partition = None\n' + event('commit_partitions', 'install', 'started', fields='run_id={run_id}; partitions={len(partition_keys)}; full_swap={str(expected_df.empty).lower()}; batch_state=pending'))
edit.insert('shutil.move(str(staging_path), str(target_path))', 'log_installed_targets += 1\n' + event('commit_partitions', 'install', 'running', fields='target={target_path}; full_swap=true; installed_targets={log_installed_targets}; batch_state=pending'), after=True)
edit.insert('relative_path =', 'log_partition = partition_key\n' + event('commit_partitions', 'install', 'running', fields='partition={partition_key}; installed_targets={log_installed_targets}; batch_state=pending'))
edit.insert('if should_exist:\n    shutil.move', 'log_installed_targets += 1\n' + event('commit_partitions', 'install_leaf', 'completed', fields="partition={partition_key}; action={'replace' if should_exist else 'delete'}; installed_targets={log_installed_targets}; batch_state=pending"), after=True)
edit.insert('committed_dataset =', 'log_phase = "formal_readback"\nlog_partition = None\n' + event('commit_partitions', 'formal_readback', 'started', fields='expected_rows={len(expected_df)}; batch_state=pending'))
edit.insert('if table_digest(committed_df) !=', event('commit_partitions', 'formal_readback', 'completed', fields='rows={len(committed_df)}; batch_state=pending'), after=True)
edit.insert('rollback_errors =', 'log_failed_phase = log_phase\n' + event('commit_partitions', 'rollback', 'started', fields='failed_phase={log_phase}; error={type(commit_error).__name__}; partition={log_partition}; installed_targets={log_installed_targets}') + '\nlog_phase = "rollback"')
edit.insert('if target_had_existing and backup_path.exists():', 'log_restored_targets += 1\n' + event('commit_partitions', 'rollback', 'running', fields='target={target_path}; restored_targets={log_restored_targets}; full_swap=true'), after=True)
edit.insert('if had_existing and saved_path.exists():', 'log_restored_targets += 1\n' + event('commit_partitions', 'rollback', 'running', fields='target={destination_path}; restored_targets={log_restored_targets}'), after=True)
edit.insert('cleanup_recovery_paths = False', event('commit_partitions', 'rollback', 'failed', fields='restored_targets={log_restored_targets}; backup_path={backup_path}; quarantine_path={quarantine_path}; recovery_complete=false'), after=True)
edit.insert('if rollback_errors:', event('commit_partitions', 'rollback', 'completed', fields='restored_targets={log_restored_targets}; recovery_complete=true'), after=True)
edit.insert('return len(changed_rows_df)', event('commit_partitions', 'commit', 'completed', prefix='committed', fields='run_id={run_id}; committed_rows={len(changed_rows_df)}; committed_partitions={len(partition_keys)}; persisted=true') + '\n'
    + event('commit_partitions', 'calendar_state', 'completed', fields='run_id={run_id}; rows={len(committed_df)}; persisted=true; date_watermark=none; scope=expected_report_calendar'))
boundary(edit, 'commit', fields='expected_rows={len(expected_df)}; planned_partitions={len(partition_keys)}', setup='log_partition = None\nlog_failed_phase = None\nlog_selected_partitions = 0\nlog_staged_partitions = 0\nlog_installed_targets = 0\nlog_restored_targets = 0\nlog_last_progress_at = log_started_at', failure_fields='; partition={log_partition}; installed_targets={log_installed_targets}; restored_targets={log_restored_targets}', failure_phase='log_failed_phase or log_phase')

edit = FunctionEdit('40229d27', 'main')
for node in ast.walk(edit.function):
    if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo'):
        continue
    text = ast.unparse(node)
    if any(fragment in text for fragment in ('phase=generate;', 'phase=plan; status=started', 'phase=commit; status=started', 'phase=commit; status=completed', 'phase=read_inputs;')):
        edit.replace(text, '')
edit.insert('upstream_df = validate_upstream_table', 'log_phase = "upstream_read"\n' + event('main', 'upstream_read', 'started', table='UPSTREAM_TABLE_NAME', fields='path={upstream_path}; explicit_dates={str(has_explicit_dates).lower()}'))
edit.insert('upstream_df = validate_upstream_table', event('main', 'upstream_read', 'completed', table='UPSTREAM_TABLE_NAME', fields='rows={len(upstream_df)}; materialized=true'), after=True)
edit.insert('existing_df = validate_report_calendar_table', 'log_phase = "existing_read"\n' + event('main', 'existing_read', 'started', fields='path={target_path}'))
edit.insert('existing_df = validate_report_calendar_table', event('main', 'existing_read', 'completed', fields='rows={len(existing_df)}; materialized=true'), after=True)
edit.insert('existing_df = empty_pandas', event('main', 'existing_read', 'skipped', fields='reason=no_existing_parquet; rows=0'), after=True)
edit.insert('expected_scope_df =', 'log_phase = "generate"')
edit.insert('expected_full_df = pd.concat', 'log_phase = "merge_scope"\n' + event('main', 'merge_scope', 'started', fields='outside_scope_rows={len(outside_scope_df)}; expected_scope_rows={len(expected_scope_df)}'))
edit.insert('expected_full_df = validate_report_calendar_table', event('main', 'merge_scope', 'completed', fields='expected_full_rows={len(expected_full_df)}; persisted=false'), after=True)
edit.insert('partition_keys =', 'log_phase = "compare"')
edit.insert('existing_rows_by_key =', 'log_phase = "plan_summary"')
edit.insert('committed_rows = commit_partitions', 'log_phase = "commit"')
edit.apply()
# 主流程仅包围原有运行日志之后的业务段，参数错误仍由原 Click 门禁报告。
edit = FunctionEdit('40229d27', 'main')
run_start = next(n for n in edit.function.body if isinstance(n, ast.Expr) and 'phase=run; status=started' in ast.unparse(n))
start = run_start.end_lineno
tail = ''.join(edit.lines[start:edit.function.end_lineno])
failure = 'except Exception as log_error:\n' + textwrap.indent(event('main', 'run', 'failed', fields='failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}') + '\nraise', '    ')
edit.cell.source = ''.join(edit.lines[:start]) + '    log_phase = "inputs"\n    try:\n' + textwrap.indent(tail.rstrip() + '\n', '    ') + textwrap.indent(failure + '\n', '    ') + ''.join(edit.lines[edit.function.end_lineno:])

notes = {
    'dbf59f85': '`open_exact_dataset()` 自行报告发现的文件数、打开和契约检查及失败阶段；`materialized=false` 表示只打开 Dataset。实际 `to_table()` 的起止与行数由执行读取的调用方报告。两个 validator 自行报告转换、主键检查、逐行校验、排序和失败位置；沿原行循环每 10000 行检查一次是否距上次进度已达 2 秒，不增加额外数据扫描。',
    '8a21e2d0': '`build_expected_calendar()` 自行报告索引准备、上游转换、格点展开和输出校验。沿原品种日循环每 1000 行检查一次 2 秒进度间隔，报告已处理品种日和已生成报告行；零行输入也报告开始与完成。生成结果标记 `persisted=false`，输出校验成功后才报告生成完成。',
    '0bbe59a0': '`changed_partition_keys()` 自行报告候选分区准备、当前比较键、已比较分区数、差异分区数和耗时；首个分区报告当前进度，其余比较每隔至少 2 秒报告一次，结束时报告完整计数。底层摘要函数不逐次输出日志。',
    '20aadb37': '`commit_partitions()` 自行报告输入校验、staging 写入与复读、逐叶安装、正式整表复读和恢复结果。安装与正式复读阶段仍记 `batch_state=pending`；原有验收及清理全部成功后，才输出 `committed:` 和 `phase=calendar_state; persisted=true`。这里表示完整期望报告日历已经落盘，明确 `date_watermark=none`，不新增日期水位文件。空提交报告状态未变；发生异常时记录当前阶段和已处理数量，并保留原异常传播与恢复行为。',
}
for cell_id, note in notes.items():
    cells[cell_id].source += '\n\n' + note
cells['637e05c1'].source = cells['637e05c1'].source.replace(
    '并以 `elapsed_s` 表示本次主流程累计秒数。读取、生成、规划和提交的阶段日志当前由 `main()` 输出；',
    '并以 `elapsed_s` 表示当前函数调用累计秒数。各函数承担自己的起止、进度和失败日志；入口保留实际数据物化、显式范围合并、计划汇总、跳过提交与整次运行结果，不重复报告函数生成或提交起止；',
).replace('正式复读通过后才输出提交成功；', '提交函数在正式复读与清理成功后才输出提交及日历状态落盘；')
diagram_replacements = {
    'a02-b01-flow-validation': [('open_exact_dataset：发现 Parquet', '记录打开开始；发现 Parquet'), ('检查物理字段和表身份', '检查物理契约；报告打开完成，尚未物化'), ('调用方按列及过滤条件物化', '调用方记录物化起止与行数'), ('应采、完成、缺失、质量与时间关系', '沿行校验并报告数量；异常记录失败阶段'), ('按对应主键排序后返回', '排序完成后报告校验结果并返回')],
    'a02-b01-flow-build': [('现有行按主键索引', '报告生成开始；现有行按主键索引'), ('汇集期望行；零行保留契约', '汇集期望行并报告进度；零行保留契约'), ('校验报告日历后返回', '校验后报告生成完成；persisted=false')],
    'a02-b01-flow-diff': [('期望与现有分区键取并集', '报告比较开始；分区键取并集'), ('逐键筛选两边完整叶', '逐键筛选完整叶；报告已比较分区数'), ('遍历结束后返回变更键', '报告完成与差异分区数；返回变更键')],
    'a02-b01-flow-commit': [('返回 0', '报告无分区、状态未变；返回 0'), ('完整期望表校验与摘要；准备路径', '记录提交开始；期望校验、摘要和路径'), ('staging 总行数与逐叶内容验收', 'staging 复读；报告逐叶验收数量'), ('逐叶备份；安装新叶或删除旧叶', '逐叶备份和安装；报告进度，批次仍待验收'), ('清理临时路径；返回变更行数', '清理后报告提交与日历落盘；返回行数'), ('清理临时路径；重抛提交错误', '报告恢复完成；清理后重抛原错误'), ('保留 staging、backup、failed；抛出恢复错误', '报告恢复未完成；保留现场并抛错')],
    'a02-b01-flow-cli': [('调用 commit_partitions；成功后报告提交', '调用 commit_partitions；函数自行报告落盘'), ('输出正常运行完成边界', '入口只报告正常运行完成边界')],
}
for cell_id, replacements in diagram_replacements.items():
    for old, new in replacements:
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
            return self.generic_visit(node).body
        return self.generic_visit(node)


def canonical(source):
    tree = RemoveLogging().visit(ast.parse(source))
    for node in ast.walk(tree):
        if not hasattr(node, 'body') or not isinstance(node.body, list):
            continue
        for index in range(len(node.body) - 2, -1, -1):
            assignment, following = node.body[index:index + 2]
            if (isinstance(assignment, ast.Assign) and len(assignment.targets) == 1
                    and isinstance(assignment.targets[0], ast.Name)
                    and assignment.targets[0].id in ('validated_upstream_df', 'validated_report_calendar_df', 'expected_calendar_df')
                    and isinstance(following, ast.Return) and isinstance(following.value, ast.Name)
                    and following.value.id == assignment.targets[0].id):
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
print('a02/b01 function logs updated; business AST, comments, cell metadata and execution state preserved.')
