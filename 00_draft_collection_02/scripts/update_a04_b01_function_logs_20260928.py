"""a04/b01 第 5—6 项：函数内进度和提交状态归位，保留业务及 I/O。"""
import ast
import copy
import io
import json
import pathlib
import shutil
import tempfile
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')
path = ROOT / RELATIVE
original_bytes = path.read_bytes()
notebook = nbformat.read(path, 4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}

# 复用草稿中仅用于保留原格式的 AST 定位编辑器，不导入或执行旧迁移脚本。
editor_source = (ROOT / '00_draft_collection_02/scripts/update_a03_b01_function_logs_20260928.py').read_text(encoding='utf8')
editor = next(node for node in ast.parse(editor_source).body if isinstance(node, ast.ClassDef) and node.name == 'FunctionEdit')
exec(compile(ast.Module(body=[editor], type_ignores=[]), '<draft FunctionEdit>', 'exec'))

def event(function, phase, status, fields='', prefix='planning_progress'):
    return f'''click.echo(
    f"{prefix}: table={{TABLE_NAME}}; function={function}; phase={phase}; status={status}; "
    f"{fields}{'; ' if fields else ''}elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
)'''

def boundary(edit, phase, fields='', setup='', failure_fields=''):
    edit.wrap(
        f'log_started_at = time.perf_counter()\nlog_phase = "{phase}"\n{setup}\n'
        + event(edit.function.name, phase, 'started', fields),
        'except Exception as log_error:\n' + textwrap.indent(event(
            edit.function.name, '{log_phase}', 'failed',
            f'error={{type(log_error).__name__}}{failure_fields}',
        ) + '\nraise', '    '),
    )

edit = FunctionEdit('a04-b01-code-compatible', 'open_compatible_dataset')
edit.insert('parquet_files =', 'log_phase = "discovery"')
edit.insert('dataset = ds.dataset', event('open_compatible_dataset', 'discovery', 'completed', 'label={label}; files={len(parquet_files)}; materialized=false') + '\nlog_phase = "dataset_open"')
edit.insert('if not physical_schema_matches(reconstructed_schema', 'log_phase = "schema"')
edit.insert('for fragment in dataset.get_fragments():', 'log_phase = "fragment_schema"')
edit.insert('if not physical_schema_matches(pa.schema', 'log_checked_fragments += 1\n'
    'if log_checked_fragments % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('open_compatible_dataset', 'fragment_schema', 'running', 'label={label}; checked_fragments={log_checked_fragments}/{len(parquet_files)}; materialized=false') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('is_exact =', 'log_phase = "metadata_exactness"\n' + event('open_compatible_dataset', 'metadata_exactness', 'started', 'label={label}; checked_fragments={log_checked_fragments}; materialized=false'))
edit.insert('return (dataset, is_exact)', event('open_compatible_dataset', 'dataset_open', 'completed', 'label={label}; checked_fragments={log_checked_fragments}; metadata_exact={str(is_exact).lower()}; materialized=false'))
boundary(edit, 'dataset_open', 'label={label}; path={table_path}; materialized=false', 'log_checked_fragments = 0\nlog_last_progress_at = log_started_at', '; label={label}; checked_fragments={log_checked_fragments}; materialized=false')

edit = FunctionEdit('a04-b01-code-exact', 'open_exact_dataset')
edit.insert('if not is_exact:', 'log_phase = "metadata_exactness"')
edit.insert('return dataset', event('open_exact_dataset', 'dataset_open', 'completed', 'label={label}; metadata_exact=true; materialized=false'))
boundary(edit, 'dataset_open', 'label={label}; path={table_path}; materialized=false', failure_fields='; label={label}; materialized=false')

edit = FunctionEdit('ebbfdc80', 'validate_macro_calendar_table')
edit.insert('checked = validate_arrow_table', 'log_phase = "convert"')
edit.insert('if frame.duplicated', 'log_phase = "primary_key"')
edit.insert('for row in checked.to_pylist():', 'log_phase = "business_validation"')
edit.insert('if not allow_legacy_policy:', 'log_checked_rows += 1\n'
    'if log_checked_rows % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('validate_macro_calendar_table', 'validate', 'running', 'context={context}; checked_rows={log_checked_rows}/{table.num_rows}; persisted=false') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
return_node = edit.find('return frame.sort_values')
edit.replace('return frame.sort_values', 'log_phase = "sort"\n'
    + 'validated_macro_calendar_df = ' + ast.unparse(return_node.value) + '\n'
    + event('validate_macro_calendar_table', 'validate', 'completed', 'context={context}; checked_rows={log_checked_rows}; rows={len(validated_macro_calendar_df)}; persisted=false')
    + '\nreturn validated_macro_calendar_df')
boundary(edit, 'validate', 'context={context}; rows={table.num_rows}; allow_legacy_policy={str(allow_legacy_policy).lower()}; persisted=false', 'log_checked_rows = 0\nlog_last_progress_at = log_started_at', '; context={context}; checked_rows={log_checked_rows}')

edit = FunctionEdit('d9a85e0d', 'build_expected_calendar')
edit.insert('existing_rows_by_key =', 'log_phase = "existing_index"')
edit.insert('expected_rows =', event('build_expected_calendar', 'existing_index', 'completed', 'existing_rows={len(existing_rows_by_key)}; persisted=false'))
edit.insert("if series.frequency == 'business_day':", 'log_phase = "series_dates"\n'
    + event('build_expected_calendar', 'series_dates', 'started', 'series={series.series_code}; series_index={log_processed_series + 1}/{len(MACRO_RELEASE_SERIES)}; frequency={series.frequency}; persisted=false'))
edit.insert('for report_date in report_dates:', 'log_phase = "expand"')
edit.insert('for name in STATE_COLUMNS:', 'log_inherited_rows += 1', after=True)
edit.insert('expected_rows.append(row)',
    'if len(expected_rows) % 10000 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('build_expected_calendar', 'expand', 'running', 'series={series.series_code}; generated_rows={len(expected_rows)}; inherited_rows={log_inherited_rows}; persisted=false') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('for report_date in report_dates:', 'log_processed_series += 1\n'
    + event('build_expected_calendar', 'series_dates', 'completed', 'series={series.series_code}; processed_series={log_processed_series}/{len(MACRO_RELEASE_SERIES)}; series_rows={len(report_dates)}; generated_rows={len(expected_rows)}; inherited_rows={log_inherited_rows}; persisted=false'), after=True)
edit.insert('if expected_rows:', 'log_phase = "output_frame"')
return_node = edit.find('return validate_macro_calendar_table')
edit.replace('return validate_macro_calendar_table', 'log_phase = "output_validation"\n'
    + 'expected_calendar_df = ' + ast.unparse(return_node.value) + '\n'
    + event('build_expected_calendar', 'generate', 'completed', 'processed_series={log_processed_series}; generated_rows={len(expected_calendar_df)}; inherited_rows={log_inherited_rows}; initialized_rows={len(expected_calendar_df) - log_inherited_rows}; persisted=false')
    + '\nreturn expected_calendar_df')
boundary(edit, 'generate', 'start_date={start_date}; end_date={end_date}; visible_date={visible_date}; series={len(MACRO_RELEASE_SERIES)}; existing_rows={len(existing_df)}; persisted=false', 'log_processed_series = 0\nlog_inherited_rows = 0\nlog_last_progress_at = log_started_at', '; processed_series={log_processed_series}; persisted=false')

edit = FunctionEdit('a04-b01-code-changed', 'changed_partition_keys')
edit.insert('for partition_key in sorted(', 'log_phase = "compare"')
edit.insert('expected_mask =', 'log_partition = partition_key\nlog_seen_partitions += 1\n'
    'if log_seen_partitions == 1 or time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('changed_partition_keys', 'compare', 'running', 'partition={partition_key}; partition_index={log_seen_partitions}; changed_partitions={len(changed_keys)}; persisted=false') + '\nlog_last_progress_at = time.perf_counter()', '    '))
edit.insert('return changed_keys', event('changed_partition_keys', 'compare', 'completed', 'compared_partitions={log_seen_partitions}; changed_partitions={len(changed_keys)}; persisted=false'))
boundary(edit, 'compare', 'expected_rows={len(expected_df)}; existing_rows={len(existing_df)}; persisted=false', 'log_partition = None\nlog_seen_partitions = 0\nlog_last_progress_at = log_started_at', '; partition={log_partition}; partition_index={log_seen_partitions}')

edit = FunctionEdit('f4c3582d', 'commit_partitions')
edit.insert('return 0', event('commit_partitions', 'commit', 'skipped', 'reason=no_changed_partitions; rows=0; partitions=0; persisted=false', 'committed'))
edit.insert('expected_df = validate_macro_calendar_table', 'log_phase = "validate_expected"')
edit.insert('silver_root =', 'log_phase = "prepare_paths"')
edit.insert('staging_path.mkdir', 'log_phase = "staging_write"\n' + event('commit_partitions', 'staging_write', 'started', 'run_id={run_id}; partitions={len(partition_keys)}; persisted=false'))
edit.insert('changed_row_mask =', 'log_phase = "select_changed_rows"')
edit.insert('changed_row_mask |=', 'log_selected_partitions += 1\n'
    'if log_selected_partitions % 100 == 0 and time.perf_counter() - log_last_progress_at >= 2.0:\n'
    + textwrap.indent(event('commit_partitions', 'select_changed_rows', 'running', 'run_id={run_id}; processed_partitions={log_selected_partitions}/{len(partition_keys)}; persisted=false') + '\nlog_last_progress_at = time.perf_counter()', '    '), after=True)
edit.insert('if not changed_rows_df.empty:', 'log_phase = "staging_write"')
edit.insert('staged_dataset =', event('commit_partitions', 'staging_write', 'completed', 'run_id={run_id}; rows={len(changed_rows_df)}; persisted=false')
    + '\nlog_phase = "staging_readback"\n'
    + event('commit_partitions', 'staging_readback', 'started', 'run_id={run_id}; persisted=false'))
edit.insert('staged_partition_df =', 'log_partition = partition_key')
edit.insert('if not expected_partition_df.empty and', 'log_staged_partitions += 1\n'
    + event('commit_partitions', 'staging_readback', 'running', 'run_id={run_id}; partition={partition_key}; checked_partitions={log_staged_partitions}/{len(partition_keys)}; rows={len(staged_partition_df)}; persisted=false'), after=True)
edit.insert('target_had_existing =', event('commit_partitions', 'staging_readback', 'completed', 'run_id={run_id}; checked_partitions={log_staged_partitions}; rows={len(staged_df)}; persisted=false') + '\nlog_phase = "install"')
edit.insert('cleanup_recovery_paths = True', event('commit_partitions', 'install', 'started', 'run_id={run_id}; full_root_swap={str(full_swap).lower()}; partitions={len(partition_keys)}; pending_acceptance=true'), after=True)
edit.insert('new_target_installed = True', 'log_installed_partitions = len(partition_keys)\n'
    + event('commit_partitions', 'install', 'running', 'run_id={run_id}; full_root_swap=true; pending_acceptance=true'), after=True)
edit.insert('relative_path =', 'log_partition = partition_key\n'
    + event('commit_partitions', 'install', 'started', 'run_id={run_id}; partition={partition_key}; partition_index={log_installed_partitions + 1}/{len(partition_keys)}; pending_acceptance=true', 'partition_start'))
# 在当前旧代码的安装分支全部完成后报告进度，暂不报告提交成功。
edit.insert('if should_exist:\n', 'log_installed_partitions += 1\n'
    + event('commit_partitions', 'install', 'running', 'run_id={run_id}; partition={partition_key}; installed_partitions={log_installed_partitions}/{len(partition_keys)}; action={\'replace\' if should_exist else \'delete\'}; pending_acceptance=true'), after=True)
edit.insert('committed_dataset =', 'log_phase = "formal_readback"\n'
    + event('commit_partitions', 'formal_readback', 'started', 'run_id={run_id}; expected_rows={len(expected_df)}; pending_acceptance=true'))
edit.insert('if table_digest(committed_df) != expected_digest:',
    event('commit_partitions', 'formal_readback', 'completed', 'run_id={run_id}; rows={len(committed_df)}; pending_cleanup=true'), after=True)
edit.insert('cleanup_recovery_paths = False', event('commit_partitions', 'rollback', 'failed', 'run_id={run_id}; failed_phase={log_phase}; error={type(commit_error).__name__}; rollback_errors={len(rollback_errors)}; backup_dir={backup_path}; quarantine_dir={quarantine_path}'), after=True)
edit.insert('if rollback_errors:', event('commit_partitions', 'rollback', 'completed', 'run_id={run_id}; failed_phase={log_phase}; error={type(commit_error).__name__}; rollback_errors=0; backup_dir={backup_path}; quarantine_dir={quarantine_path}'), after=True)
edit.insert('return len(changed_rows_df)', event('commit_partitions', 'commit', 'completed', 'run_id={run_id}; rows={len(changed_rows_df)}; partitions={len(partition_keys)}; full_root_swap={str(full_swap).lower()}; calendar_rows={len(committed_df)}; calendar_state=committed; date_watermark=none; persisted=true', 'committed'))
boundary(edit, 'commit', 'expected_rows={len(expected_df)}; partitions={len(partition_keys)}; force_full_swap={str(force_full_swap).lower()}; persisted=false', 'log_partition = None\nlog_selected_partitions = 0\nlog_staged_partitions = 0\nlog_installed_partitions = 0\nlog_last_progress_at = log_started_at', '; partition={log_partition}; installed_partitions={log_installed_partitions}')

edit = FunctionEdit('cd6a9f44', 'main')
for prefix in (
    "click.echo(f'planning_progress: table={TABLE_NAME}; function=main; phase=generate_expected; status=started; ",
    "click.echo(f'planning_progress: table={TABLE_NAME}; function=main; phase=generate_expected; status=completed; ",
    "click.echo(f'planning_progress: table={TABLE_NAME}; function=main; phase=commit; status=started; ",
    "click.echo(f'committed: table={TABLE_NAME}; function=main; phase=commit; status=completed; ",
):
    edit.replace(prefix, '')
edit.replace('full_root_swap_used =', '')
commit_call = edit.find('committed_rows = commit_partitions')
edit.replace('committed_rows = commit_partitions', ast.unparse(commit_call.value))
edit.insert('existing_df = validate_macro_calendar_table', event('main', 'existing_materialize', 'completed', 'rows={existing_table.num_rows}; materialized=true; persisted=false'))
edit.insert('existing_table = existing_dataset.to_table', event('main', 'existing_materialize', 'started', 'path={target_path}; persisted=false'))
edit.insert('expected_full_df = pd.concat', 'log_phase = "merge_scope"\n' + event('main', 'merge_scope', 'started', 'outside_scope_rows={len(outside_scope_df)}; generated_rows={len(expected_scope_df)}; persisted=false'))
edit.insert('expected_full_df = validate_macro_calendar_table', event('main', 'merge_scope', 'completed', 'rows={len(expected_full_df)}; persisted=false'), after=True)
edit.apply()

# main 只报告批次完成；持久化凭据只由实际提交函数报告。
cells['cd6a9f44'].source = cells['cd6a9f44'].source.replace('; persisted={str(write).lower()}; ', '; ')
cells['05826f25'].source += '\n函数自行报告日期范围、系列总数、逐系列日期展开、累计生成/继承行数及输出验收；`initialized_rows` 表示未继承旧状态的生成行，不是本批已提交条数。所有生成日志均为 `persisted=false`。\n'
cells['a04-b01-doc-compatible'].source += '\n函数自行报告文件发现、Dataset 打开、fragment 物理检查及 metadata 检查；记录 `materialized=false`。实际记录物化仍留在调用方，不能把打开成功当作数据已读完。fragment 检查利用已有循环计数，达到 100 个片段且间隔 2 秒时报告一次进度。\n'
cells['2dc99ad7'].source += '\n校验函数自行报告上下文、输入/已验收行数和失败阶段；每 10000 行检查一次 2 秒进度间隔，不为日志增加读取或重验。验收日志仅表示当前内存表通过；即使其来源是正式复读，也由提交函数最终报告持久化完成。\n'
cells['a04-b01-doc-changed'].source += '\n比较函数利用现有分区循环报告当前键、已开始比较的分区序号及已发现变化数；完成时报告总比较数。进度约按 2 秒间隔输出，不增加摘要计算或全表扫描。\n'
cells['62335bcd'].source += '\n提交函数现在自行报告 staging 写入/复读、逐叶安装、正式整表复读和失败恢复。安装进度标明 `pending_acceptance=true`，不提前使用 committed；只有正式内容复读成功、现有清理执行结束，才报告 `calendar_state=committed; date_watermark=none; persisted=true`。这表示本批日历状态持久化，没有新增独立日期水位文件，也不代表 b02/b03 事实已经完成。\n'
old = '本轮日志统一为事件前缀与 `table / function / phase / status / elapsed_s` 字段，批次首尾使用 `=` 分隔。阶段日志暂由 `main()` 报告范围、读取、生成、比较和提交；后续第 5—6 项再将函数内进度与完成日志归位。计划/生成完成用 `persisted=false`，提交成功才用 `persisted=true`；失败保留原异常。'
new = '日志沿用事件前缀与 `table / function / phase / status / elapsed_s` 字段，批次首尾使用 `=` 分隔。打开、校验、生成、比较和提交函数报告自己的进度与失败；`main()` 保留参数门禁、现有表物化和策略识别、范围合并、计划汇总与批次结果，不再重复报告生成起止或提交成功。函数耗时以自身调用为起点，main 耗时以批次为起点；失败继续传播原异常。'
assert old in cells['2dcd351e'].source
cells['2dcd351e'].source = cells['2dcd351e'].source.replace(old, new)

class RemoveLogging(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        if all(isinstance(target, ast.Name) and (target.id.startswith('log_') or target.id == 'full_root_swap_used') for target in node.targets):
            return None
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == 'committed_rows':
            return ast.Expr(value=node.value)
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
                    and assignment.targets[0].id in ('validated_macro_calendar_df', 'expected_calendar_df')
                    and isinstance(following, ast.Return) and isinstance(following.value, ast.Name)
                    and following.value.id == assignment.targets[0].id):
                following.value = assignment.value
                del node.body[index]
    return ast.dump(tree)

code = lambda n: '\n\n'.join(c.source for c in n.cells if c.cell_type == 'code')
comments = lambda source: [token.string for token in tokenize.generate_tokens(io.StringIO(source).readline) if token.type == tokenize.COMMENT]
assert canonical(code(before)) == canonical(code(notebook)), '剔除日志后的业务 AST 有变化'
assert comments(code(before)) == comments(code(notebook)), '原代码注释变化'
assert before.metadata == notebook.metadata
for old_cell, new_cell in zip(before.cells, notebook.cells, strict=True):
    assert {k: v for k, v in old_cell.items() if k != 'source'} == {k: v for k, v in new_cell.items() if k != 'source'}
nbformat.validate(notebook)
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a04-b01-function-logs-before-'))
for relative in (RELATIVE, RELATIVE.with_suffix('.py')):
    target = snapshot / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, target)
assert path.read_bytes() == original_bytes
path.write_text(nbformat.writes(notebook) + '\n', encoding='utf8', newline='\n')
print(json.dumps({'snapshot': str(snapshot), 'business_ast_unchanged': True}, ensure_ascii=False))
