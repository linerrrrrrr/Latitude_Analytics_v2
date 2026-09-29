"""a03/b01 第 5—6 项：增加函数内日志；保留业务、I/O 与共享事务。"""
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
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b01_external_market_calendar.ipynb')
PATH = ROOT / RELATIVE
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
                    and assignment.targets[0].id in ('validated_upstream_df', 'validated_external_calendar_df', 'expected_calendar_df')
                    and isinstance(following, ast.Return) and isinstance(following.value, ast.Name)
                    and following.value.id == assignment.targets[0].id):
                following.value = assignment.value
                del node.body[index]
    return ast.dump(tree)


edit = FunctionEdit('49bfe04b', 'open_compatible_dataset')
edit.insert('parquet_files =', 'log_phase = "discovery"')
edit.insert('dataset = ds.dataset', event('open_compatible_dataset', 'discovery', 'completed', table='log_table_name', fields='label={label}; files={len(parquet_files)}; materialized=false') + '\nlog_phase = "dataset_open"')
edit.insert('expected_names =', 'log_phase = "schema"')
edit.insert('for fragment in dataset.get_fragments():', 'log_phase = "fragment_schema"')
edit.insert('if not physical_schema_matches', 'log_checked_fragments += 1\n'
    'if log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('open_compatible_dataset', 'fragment_schema', 'running', table='log_table_name', fields='label={label}; checked_fragments={log_checked_fragments}; discovered_files={len(parquet_files)}; materialized=false') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('is_exact =', 'log_phase = "metadata_exactness"')
edit.insert('return (dataset, is_exact)', event('open_compatible_dataset', 'dataset_open', 'completed', table='log_table_name', fields='label={label}; checked_fragments={log_checked_fragments}; metadata_exact={str(is_exact).lower()}; materialized=false'))
boundary(edit, 'dataset_open', table='log_table_name', fields='label={label}; path={table_path}; materialized=false', setup='log_table_name = TABLE_NAME if schema is EXTERNAL_MARKET_CALENDAR_SCHEMA else UPSTREAM_TABLE_NAME\nlog_checked_fragments = 0\nlog_last_progress_at = log_started_at', failure_fields='; label={label}; checked_fragments={log_checked_fragments}; materialized=false')

edit = FunctionEdit('49bfe04b', 'open_exact_dataset')
edit.insert('if not is_exact:', 'log_phase = "metadata_exactness"')
edit.insert('return dataset', event('open_exact_dataset', 'dataset_open', 'completed', table='log_table_name', fields='label={label}; metadata_exact=true; materialized=false'))
boundary(edit, 'dataset_open', table='log_table_name', fields='label={label}; path={table_path}; materialized=false', setup='log_table_name = TABLE_NAME if schema is EXTERNAL_MARKET_CALENDAR_SCHEMA else UPSTREAM_TABLE_NAME', failure_fields='; label={label}; materialized=false')

for cell_id, name, table_name, return_name, last_check in [
    ('a03-b01-upstream-validation', 'validate_upstream_table', 'UPSTREAM_TABLE_NAME', 'validated_upstream_df', "if row['is_weekend'] !="),
    ('a03-b01-output-validation', 'validate_external_calendar_table', 'TABLE_NAME', 'validated_external_calendar_df', "if row['updated_at'] > now_utc:"),
]:
    edit = FunctionEdit(cell_id, name)
    edit.insert('checked = validate_arrow_table', 'log_phase = "convert"')
    edit.insert('if frame.duplicated', 'log_phase = "primary_key"')
    edit.insert('for row in checked.to_pylist():', 'log_phase = "business_validation"')
    edit.insert(last_check, 'log_checked_rows += 1\n'
        'if log_checked_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
        + textwrap.indent(event(name, 'validate', 'running', table=table_name, fields='context={context}; checked_rows={log_checked_rows}/{table.num_rows}') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
    return_node = edit.find('return frame.sort_values')
    edit.replace('return frame.sort_values', 'log_phase = "sort"\n'
        + f'{return_name} = {ast.unparse(return_node.value)}\n'
        + event(name, 'validate', 'completed', table=table_name, fields=f'context={{context}}; checked_rows={{log_checked_rows}}; rows={{len({return_name})}}')
        + f'\nreturn {return_name}')
    boundary(edit, 'validate', table=table_name, fields='context={context}; rows={table.num_rows}', setup='log_checked_rows = 0\nlog_last_progress_at = log_started_at', failure_fields='; context={context}; checked_rows={log_checked_rows}')

edit = FunctionEdit('74b6ae55', 'build_expected_calendar')
edit.insert('existing_rows_by_key =', 'log_phase = "existing_index"')
edit.insert('upstream_rows =', 'log_phase = "upstream_conversion"')
edit.insert('for upstream_row in upstream_rows:', 'log_phase = "expand"')
daily_loop = edit.find('for upstream_row in upstream_rows:')
inner_loop = next(node for node in daily_loop.body if isinstance(node, ast.For) and isinstance(node.iter, ast.Name) and node.iter.id == 'request_entities')
edit.insert(ast.unparse(inner_loop), 'log_processed_dates += 1\n'
    'if log_processed_dates % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('build_expected_calendar', 'generate', 'running', fields='processed_dates={log_processed_dates}/{len(upstream_rows)}; generated_rows={len(expected_rows)}; inherited_rows={log_inherited_rows}; persisted=false') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('for name in STATE_COLUMNS:', 'log_inherited_rows += 1', after=True)
edit.insert('if expected_rows:', 'log_phase = "output_frame"')
return_node = edit.find('return validate_external_calendar_table')
edit.replace('return validate_external_calendar_table', 'log_phase = "output_validation"\n'
    + f'expected_calendar_df = {ast.unparse(return_node.value)}\n'
    + event('build_expected_calendar', 'generate', 'completed', fields='processed_dates={log_processed_dates}; generated_rows={len(expected_calendar_df)}; inherited_rows={log_inherited_rows}; persisted=false')
    + '\nreturn expected_calendar_df')
boundary(edit, 'generate', fields='upstream_dates={len(upstream_df)}; existing_rows={len(existing_df)}; persisted=false', setup='log_processed_dates = 0\nlog_inherited_rows = 0\nlog_last_progress_at = log_started_at', failure_fields='; processed_dates={log_processed_dates}; persisted=false')

edit = FunctionEdit('a03-b01-difference-plan', 'changed_partition_keys')
edit.insert('expected_keys =', 'log_phase = "partition_keys"')
edit.insert('for partition_key in sorted(', 'log_total_partitions = len(expected_keys | existing_keys)\nlog_phase = "compare"')
edit.insert('expected_mask =', 'log_partition = partition_key\n'
    'if log_compared_partitions == 0 or time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('changed_partition_keys', 'compare', 'running', fields='partition={partition_key}; compared_partitions={log_compared_partitions}/{log_total_partitions}; changed_partitions={len(changed_keys)}') + '\nlog_last_progress_at = time.perf_counter()', '    '))
edit.insert('if expected_digest != existing_digest:', 'log_compared_partitions += 1', after=True)
edit.insert('return changed_keys', event('changed_partition_keys', 'compare', 'completed', fields='compared_partitions={log_compared_partitions}/{log_total_partitions}; changed_partitions={len(changed_keys)}'))
boundary(edit, 'compare', fields='expected_rows={len(expected_df)}; existing_rows={len(existing_df)}', setup='log_compared_partitions = 0\nlog_partition = None\nlog_last_progress_at = log_started_at', failure_fields='; partition={log_partition}; compared_partitions={log_compared_partitions}')

edit = FunctionEdit('018ccc71', 'commit_partitions')
edit.insert('return 0', event('commit_partitions', 'commit', 'skipped', fields='reason=no_partitions; committed_rows=0; calendar_state=unchanged; persisted=false; date_watermark=none'))
edit.insert('expected_df = validate_external_calendar_table', 'log_phase = "input_validation"')
edit.insert('expected_digest =', 'log_phase = "expected_digest"')
edit.insert('silver_root =', 'log_phase = "paths"')
edit.insert('staging_path.mkdir', 'log_phase = "staging_prepare"\n' + event('commit_partitions', 'staging_prepare', 'started', fields='run_id={run_id}; partitions={len(partition_keys)}; staging_path={staging_path}'))
edit.insert('changed_row_mask =', 'log_phase = "select_changed_rows"')
edit.insert('changed_row_mask |=', 'log_selected_partitions += 1\n'
    'if time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('commit_partitions', 'select_changed_rows', 'running', fields='partition={partition_key}; selected_partitions={log_selected_partitions}/{len(partition_keys)}') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('if not changed_rows_df.empty:', 'log_phase = "staging_write"\n' + event('commit_partitions', 'staging_write', 'started', fields='rows={len(changed_rows_df)}; partitions={len(partition_keys)}; persisted=false'))
edit.insert('if not changed_rows_df.empty:', event('commit_partitions', 'staging_write', 'completed', fields='rows={len(changed_rows_df)}; persisted=false'), after=True)
edit.insert('staged_dataset =', 'log_phase = "staging_readback"\n' + event('commit_partitions', 'staging_readback', 'started', fields='expected_rows={len(changed_rows_df)}; persisted=false'))
edit.insert('if force_full_swap and', event('commit_partitions', 'staging_readback', 'completed', fields='rows={len(staged_df)}; persisted=false'), after=True)
edit.insert('staged_partition_table =', 'log_partition = partition_key\nlog_phase = "staging_leaf"\n' + event('commit_partitions', 'staging_leaf', 'started', fields='partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; expected_rows={len(expected_partition_df)}'))
edit.insert('if not expected_partition_df.empty', 'log_staged_partitions += 1\n' + event('commit_partitions', 'staging_leaf', 'completed', fields='partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; rows={len(staged_partition_df)}; persisted=false'), after=True)
edit.insert('target_had_existing =', 'log_phase = "install"\nlog_partition = None\n' + event('commit_partitions', 'install', 'started', fields='run_id={run_id}; partitions={len(partition_keys)}; full_root_swap={str(force_full_swap or expected_df.empty).lower()}; batch_state=pending'))
edit.insert('transaction.replace(target_path=target_path', 'log_installed_targets += 1\n' + event('commit_partitions', 'install', 'running', fields='target={target_path}; full_root_swap=true; installed_targets={log_installed_targets}; batch_state=pending'), after=True)
edit.insert('relative_path =', 'log_partition = partition_key\n' + event('commit_partitions', 'install', 'running', fields='partition={partition_key}; installed_targets={log_installed_targets}; batch_state=pending'))
edit.insert('transaction.replace(target_path=destination_path', 'log_installed_targets += 1\n' + event('commit_partitions', 'install_leaf', 'completed', fields="partition={partition_key}; action={'replace' if should_exist else 'delete'}; installed_targets={log_installed_targets}; batch_state=pending"), after=True)
edit.insert('committed_dataset =', 'log_phase = "formal_readback"\nlog_partition = None\n' + event('commit_partitions', 'formal_readback', 'started', fields='expected_rows={len(expected_df)}; batch_state=pending'))
edit.insert('if table_digest(committed_df) !=', event('commit_partitions', 'formal_readback', 'completed', fields='rows={len(committed_df)}; batch_state=pending'), after=True)
edit.insert('return len(changed_rows_df)', event('commit_partitions', 'commit', 'completed', prefix='partition_committed', fields='scope=batch; persisted=true; committed_rows={len(changed_rows_df)}; committed_partitions={len(partition_keys)}; full_root_swap={str(full_swap).lower()}; run_id={run_id}') + '\n'
    + event('commit_partitions', 'calendar_state', 'completed', fields='run_id={run_id}; rows={len(committed_df)}; persisted=true; date_watermark=none; scope=expected_external_calendar'))
boundary(edit, 'commit', fields='expected_rows={len(expected_df)}; planned_partitions={len(partition_keys)}; force_full_swap={str(force_full_swap).lower()}', setup='log_partition = None\nlog_selected_partitions = 0\nlog_staged_partitions = 0\nlog_installed_targets = 0\nlog_last_progress_at = log_started_at', failure_fields='; partition={log_partition}; installed_targets={log_installed_targets}')

edit = FunctionEdit('ebf489c1', 'main')
commit_log = next(node for node in ast.walk(edit.function) if isinstance(node,ast.Expr) and 'partition_committed:' in ast.unparse(node))
edit.replace(ast.unparse(commit_log), '')
# Keep the existing commit return assignment for exact business-AST comparison in this logging-only step.
edit.insert('upstream_df = validate_upstream_table', event('main', 'upstream_read', 'started', table='UPSTREAM_TABLE_NAME', fields='path={upstream_path}; explicit_dates={str(has_explicit_dates).lower()}'))
edit.insert('upstream_df = validate_upstream_table', event('main', 'upstream_read', 'completed', table='UPSTREAM_TABLE_NAME', fields='rows={len(upstream_df)}; materialized=true'), after=True)
edit.insert('existing_df = validate_external_calendar_table', event('main', 'existing_read', 'started', fields='path={target_path}'))
edit.insert('existing_df = validate_external_calendar_table', event('main', 'existing_read', 'completed', fields='rows={len(existing_df)}; materialized=true'), after=True)
edit.insert('existing_df = empty_pandas', event('main', 'existing_read', 'skipped', fields='reason=no_existing_parquet; rows=0'), after=True)
edit.insert('expected_full_df = pd.concat', 'log_phase = "merge_scope"\n' + event('main', 'merge_scope', 'started', fields='outside_scope_rows={len(outside_scope_df)}; expected_scope_rows={len(expected_scope_df)}'))
edit.insert('expected_full_df = validate_external_calendar_table', event('main', 'merge_scope', 'completed', fields='expected_full_rows={len(expected_full_df)}; persisted=false'), after=True)
edit.apply()
cells['ebf489c1'].source = cells['ebf489c1'].source.replace('; date_watermark=none; elapsed_s=', '; elapsed_s=')

notes = {
    '2d5c68c2': '两个打开函数自行报告文件发现、fragment 契约检查、metadata 结论和失败阶段；`materialized=false` 只表示 Dataset 已打开。实际 `to_table()` 的读取起止和行数由调用处报告。fragment 检查沿原循环计数，每 100 个检查一次 2 秒输出间隔，不增加扫描。',
    'a03-b01-upstream-heading': '校验函数自行报告转换、主键、逐行检查、排序与失败阶段；沿现有循环每 10000 行检查一次 2 秒进度间隔。日志不增加数据检查，也不改变当前校验条件。',
    'a03-b01-output-heading': '校验函数自行报告输入行数、已检查行数和失败阶段；沿现有循环限频报告进度，完整校验与排序成功后才报告完成。',
    '3f56c68e': '`build_expected_calendar()` 自行报告索引、上游转换、格点展开与输出校验。每处理 100 个自然日检查一次 2 秒进度间隔，报告已处理日期、生成行和继承状态行数；空输入也有开始与完成。生成完成明确标记 `persisted=false`。',
    'a03-b01-difference-heading': '比较函数自行报告候选分区数、当前分区、已比较数与变化数；首个分区及随后间隔至少 2 秒时报告进度。摘要函数不逐次输出日志。',
    '068d0136': '提交函数自行报告输入校验、staging 写入/复读、逐叶安装及正式验收的进度。安装和正式复读完成仍为 `batch_state=pending`；成功退出共享事务后才输出 `partition_committed:` 与 `phase=calendar_state; persisted=true; date_watermark=none`。空提交报告状态未变；失败保留阶段与已安装数，恢复日志由共享模块负责。',
}
for cell_id, note in notes.items():
    cells[cell_id].source += '\n\n' + note
cells['7150a5fe'].source = cells['7150a5fe'].source.replace('提交完成日志必须在 `commit_partitions()` 成功返回后发出', '提交完成日志由 `commit_partitions()` 在正式验收并成功退出共享事务后发出').replace('函数内部的细粒度进度及日志归位留待第 5—6 项处理。', '各函数报告自身起止、数量、耗时和失败阶段；main 只报告自身实际物化、范围合并、计划汇总、跳过提交及整次运行结果。`elapsed_s` 是当前函数调用累计耗时。')
diagram_replacements = {
    'a03-b01-flow-read': [('枚举 Parquet；打开 Dataset', '报告打开开始；枚举 Parquet 和检查契约'), ('返回 Dataset 和 metadata 是否精确', '报告打开完成、metadata 结论；尚未物化'), ('返回 Dataset', '报告精确打开完成；尚未物化')],
    'a03-b01-flow-generate': [('按主键索引现有行', '报告生成开始；按主键索引现有行'), ('收集完整理论格点；完整业务验收', '报告已生成/继承数量；完整业务验收'), ('返回内存期望日历；尚未提交', '报告生成完成；persisted=false')],
    'a03-b01-flow-difference': [('期望与现有分区键并集', '报告比较开始；候选分区键并集'), ('逐键选取两侧完整分区', '逐键比较并报告已比较/变化分区数'), ('返回排序后的变化分区', '报告比较完成；返回变化分区')],
    'a03-b01-flow-commit': [('完整期望验收与摘要；准备本批路径', '报告提交开始；完整期望验收、摘要和路径'), ('staging 完整及逐叶复读验收', 'staging 完整及逐叶复读；报告数量'), ('共享模块替换或显式删除全部变化叶', '逐叶安装并报告进度；批次仍为 pending'), ('成功退出事务；返回本批写入行数', '成功退出后报告提交与日历落盘；返回行数')],
    'a03-b01-flow-cli': [('调用提交；成功返回才报告整批落盘', '调用提交；函数自行报告整批落盘'), ('运行完成；date_watermark=none', '入口报告运行完成')],
}
for cell_id, pairs in diagram_replacements.items():
    for old_text,new_text in pairs:
        assert old_text in cells[cell_id].source, (cell_id,old_text)
        cells[cell_id].source = cells[cell_id].source.replace(old_text,new_text)

assert before.metadata == notebook.metadata
assert [c.id for c in before.cells] == [c.id for c in notebook.cells]
for old in before.cells:
    new = cells[old.id]
    assert {k:v for k,v in old.items() if k!='source'} == {k:v for k,v in new.items() if k!='source'},old.id
    if old.cell_type == 'code':
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        assert comments(old.source) == comments(new.source),old.id
        assert canonical(old.source) == canonical(new.source),old.id
        compile(new.source,str(PATH),'exec')
nbformat.validate(notebook)
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b01-function-logs-before-'))
hashes = {}
for path in [ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md', ROOT/'02_Futures_Lakehouse/README.md', ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py', *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')), *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')), *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py'))]:
    relative=path.relative_to(ROOT)
    hashes[relative.as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
for path in (PATH,PATH.with_suffix('.py')):
    destination=snapshot/path.relative_to(ROOT)
    destination.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(path,destination)
(snapshot/'hashes.json').write_text(json.dumps(hashes,ensure_ascii=False,indent=2),encoding='utf8')
with PATH.open('w',encoding='utf8',newline='\n') as handle:
    nbformat.write(notebook,handle)
print(snapshot)
print('Business AST, original comments and cell state preserved.')

