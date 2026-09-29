"""b04 第 5—6 项：在现有操作内记录日志；Notebook 为唯一修改源。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b04_futures_bar_calendar.ipynb'
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
        start = self.function.body[0].lineno - 1
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


edit = FunctionEdit('c04-dataset-helpers', 'open_contract_dataset')
edit.insert('return None', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
        f"status=completed; path={table_path}; outcome=absent_optional; checked_fragments=0; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.insert('dataset = ds.dataset', 'log_phase = "dataset_open"')
edit.insert('actual_schema = reconstructed_schema', 'log_phase = "logical_schema"')
edit.insert('for fragment in dataset.get_fragments()', '''
    log_phase = "fragment_schema"
    log_fragment_count = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; "
        f"phase=fragment_schema; status=started; path={table_path}; checked_fragments=0"
    )
''')
edit.insert('if not physically_and_identity_compatible(fragment.physical_schema', '''
    log_fragment_count += 1
    if log_fragment_count == 1 or log_fragment_count % 250 == 0:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; "
            f"phase=fragment_schema; status=running; path={table_path}; "
            f"checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
''', after=True)
edit.insert('return dataset', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
        f"status=completed; path={table_path}; outcome=ready; checked_fragments={log_fragment_count}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.wrap('''
    log_started_at = time.perf_counter()
    log_phase = "file_discovery"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
        f"status=started; path={table_path}; label={label}; required={str(required).lower()}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=open_contract_dataset; phase=dataset_open; "
            f"status=failed; path={table_path}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

edit = FunctionEdit('c04-dataset-helpers', 'partition_keys_from_files')
# 两个 return keys 各自插入；不为日志额外遍历目录。
for node in [n for n in ast.walk(edit.function) if isinstance(n, ast.Return)]:
    edit.insertions.setdefault(node.lineno - 1, []).append(textwrap.indent('''click.echo(
    f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; phase=partition_discovery; "
    f"status=completed; path={table_path}; files_seen={log_file_count}; partitions={len(keys)}; "
    f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
)
''', ' ' * node.col_offset))
edit.insert("if parquet_path.name == 'schema.parquet'", 'log_file_count += 1')
edit.insert('keys.add(tuple(values))', '''
    if log_file_count == 1 or log_file_count % 250 == 0:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; phase=partition_discovery; "
            f"status=running; path={table_path}; files_seen={log_file_count}; partitions={len(keys)}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
''', after=True)
edit.wrap('''
    log_started_at = time.perf_counter()
    log_file_count = 0
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; "
        f"phase=partition_discovery; status=started; path={table_path}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=partition_keys_from_files; phase=partition_discovery; "
            f"status=failed; path={table_path}; files_seen={log_file_count}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

edit = FunctionEdit('c04-dataset-helpers', 'read_partition')
edit.insert('table = dataset.to_table', 'log_phase = "scan"')
edit.replace('return arrow_to_pandas', '''
    log_phase = "schema_conversion"
    partition_df = arrow_to_pandas(table, schema)
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_partition; phase=read; status=completed; "
        f"partition={partition_key}; rows={len(partition_df)}; columns={len(schema.names)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    return partition_df
''')
edit.wrap('''
    log_started_at = time.perf_counter()
    log_phase = "filter"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_partition; phase=read; status=started; "
        f"partition={partition_key}; columns={len(schema.names)}; start_date={start_date}; end_date={end_date}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_partition; phase=read; status=failed; "
            f"partition={partition_key}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

edit = FunctionEdit('c04-build', 'build_structural_partitions')
edit.insert('minute_bar_structure_df = upstream_contract_structure_df.rename', '''
    log_phase = "minute_structure"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=minute_structure; "
        "status=started; bar_frequency=1m"
    )
''')
edit.insert('daily_bar_structure_df = upstream_contract_structure_df.drop_duplicates', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
        f"status=running; completed=1; total=2; bar_frequency=1m; rows={len(minute_bar_structure_df)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    log_phase = "daily_structure"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=daily_structure; "
        "status=started; bar_frequency=1d"
    )
''')
edit.insert('return {', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
        f"status=completed; completed=2; total=2; daily_rows={len(daily_bar_structure_df)}; "
        f"minute_rows={len(minute_bar_structure_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.wrap('''
    log_started_at = time.perf_counter()
    log_phase = "input_conversion"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
        f"status=started; completed=0; total=2; upstream_rows={len(upstream_contract_structure_df)}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_structural_partitions; phase=build_structure; "
            f"status=failed; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

edit = FunctionEdit('c04-build', 'build_fresh_partitions')
edit.insert('structural_by_frequency = build_structural_partitions', 'log_phase = "structure_build"')
edit.insert('fresh_df = structural_df.copy()', 'log_phase = "initial_state"')
edit.insert('fresh_by_frequency[frequency] = validate_bar_calendar_frame', 'log_phase = "output_validation"')
edit.insert('fresh_by_frequency[frequency] = validate_bar_calendar_frame', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
        f"status=running; bar_frequency={frequency}; completed={len(fresh_by_frequency)}; "
        f"total={len(structural_by_frequency)}; rows={len(fresh_by_frequency[frequency])}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''', after=True)
edit.insert('return fresh_by_frequency', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
        f"status=completed; completed={len(fresh_by_frequency)}; total={len(structural_by_frequency)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.wrap('''
    log_started_at = time.perf_counter()
    log_phase = "input_validation"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
        f"status=started; upstream_rows={len(contract_frame)}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=build_fresh_partitions; phase=build_fresh; "
            f"status=failed; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

edit = FunctionEdit('c04-commit', 'read_existing_partition_leaf')
edit.insert('return empty_pandas', '''
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
        f"status=completed; partition={partition_key}; outcome=absent_leaf; rows=0; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.insert('leaf_dataset = ds.dataset', 'log_phase = "leaf_schema"')
edit.replace('return read_partition', '''
    log_phase = "leaf_read"
    existing_partition_df = read_partition(
        leaf_dataset,
        FUTURES_BAR_CALENDAR_SCHEMA,
        PARTITION_COLUMNS,
        partition_key,
    )
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
        f"status=completed; partition={partition_key}; files={len(parquet_files)}; rows={len(existing_partition_df)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    return existing_partition_df
''')
edit.wrap('''
    log_started_at = time.perf_counter()
    log_phase = "leaf_discovery"
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
        f"status=started; partition={partition_key}; path={target_path}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=read_existing_partition_leaf; phase=read_leaf; "
            f"status=failed; partition={partition_key}; failed_phase={log_phase}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

edit = FunctionEdit('c04-commit', 'commit_partition')
for prefix, phase in (
    ('incoming_table = pandas_to_arrow', 'input_conversion'),
    ('silver_root = lake_root.resolve()', 'paths'),
    ('if replace_start_date is None:', 'merge'),
    ('complete_partition_df = validate_bar_calendar_frame', 'dirty_validation'),
    ('relative_path = partition_relative_path', 'staging_write'),
    ('validate_output_parquet_files(source_path)', 'staging_readback'),
    ('backup_path.mkdir', 'prepare_install'),
    ('if destination_path.exists():', 'install'),
    ('expected_file_schema = parquet_file_schema()', 'schema_marker'),
    ('if destination_path.is_dir():', 'formal_readback'),
):
    edit.insert(prefix, f'''log_phase = "{phase}"
click.echo(
    f"planning_progress: table={{TABLE_NAME}}; function=commit_partition; phase={phase}; "
    f"status=started; partition={{partition_key}}"
)''')
# 恢复代码和异常链原样保留，完成/失败状态只在实际恢复结果已知后设置。
edit.insert('if marker_created and marker_path.exists()', 'log_rollback_status = "failed"')
edit.insert('if rollback_errors:', 'log_rollback_status = "failed" if rollback_errors else "completed"')
edit.insert('return len(incoming_df)', '''
    click.echo(
        f"partition_committed: table={TABLE_NAME}; function=commit_partition; phase=commit; status=completed; "
        f"partition={partition_key}; completed=1; total=1; rows={len(incoming_df)}; "
        f"replacement_rows={len(complete_partition_table)}; run_id={run_id}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
''')
edit.wrap('''
    log_started_at = time.perf_counter()
    log_phase = "replacement_scope"
    log_rollback_status = "not_required"
    click.echo(
        f"partition_start: table={TABLE_NAME}; function=commit_partition; phase=commit; status=started; "
        f"partition={partition_key}; completed=0; total=1; rows={len(frame)}; lake_root={lake_root}"
    )
''', '''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=commit; status=failed; "
            f"partition={partition_key}; failed_phase={log_phase}; rollback={log_rollback_status}; "
            f"error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise
''')

source = cells['c04-cli'].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new)


replace_once('''            click.echo(
                f"partition_start: table={TABLE_NAME}; phase=commit; status=started; "
                f"partition={partition_key}; completed={log_processed_partitions}; "
                f"total={len(dirty_partition_plans)}; rows={len(partition_commit_df)}"
            )
''', '')
replace_once('''                f"partition_committed: table={TABLE_NAME}; phase=commit; status=completed; "''',
             '''                f"planning_progress: table={TABLE_NAME}; phase=commit_batch; status=running; "''')
replace_once('''                f"elapsed_s={commit_elapsed:.3f}"
            )''', '''                f"elapsed_s={time.perf_counter() - run_started_at:.3f}"
            )''')

start = source.index('    automatic_tail_watermark_by_frequency_exchange:')
end = source.index('    if has_explicit_dates:\n', start)
watermark = source[start:end]
watermark = watermark.rstrip() + '\n'
# 单个频率—交易所水位取值后报告，不重复读取尾叶或计算最大日期。
watermark += '''            log_watermark_count += 1
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=watermark; status=running; "
                f"frequency_exchange={frequency_exchange}; partition={tail_partition_key}; "
                f"watermark={automatic_tail_watermark_by_frequency_exchange.get(frequency_exchange)}; "
                f"completed={log_watermark_count}; total={len(target_partition_keys_by_frequency_exchange)}; "
                f"rows={len(target_tail_structure_df)}; source=target_max_trading_date; "
                f"elapsed_s={time.perf_counter() - log_watermark_started_at:.3f}"
            )
'''
source = source[:start] + '''    log_watermark_started_at = time.perf_counter()
    log_watermark_count = 0
    if automatic_tail_mode:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=started; "
            "source=target_max_trading_date"
        )
    else:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=skipped; "
            f"mode={log_mode}; reason=explicit_scope"
        )
    try:
''' + textwrap.indent(watermark, '    ') + '''    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=failed; "
            f"completed={log_watermark_count}; error={type(log_error).__name__}; "
            f"elapsed_s={time.perf_counter() - log_watermark_started_at:.3f}"
        )
        raise
    if automatic_tail_mode:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; phase=watermark; status=completed; "
            f"completed={log_watermark_count}; total={log_watermark_count}; "
            f"watermark_groups={len(automatic_tail_watermark_by_frequency_exchange)}; "
            f"rows={target_tail_structure_row_count}; source=target_max_trading_date; "
            f"elapsed_s={time.perf_counter() - log_watermark_started_at:.3f}"
        )

''' + source[end:]
cells['c04-cli'].source = source

# 说明和图同步函数职责；不把日志改动写成新的数据契约。
cells['c04-dataset-notes'].source += '''

读取进度由函数自己记录：Dataset 打开报告逐 fragment 检查进度，分区发现报告已见文件和分区数，两者复用原遍历，不为日志额外扫描目录；投影读取报告分区、列数、日期范围、返回行数和耗时。可选下游不存在、空结果和读取异常分别表达；异常记录阶段后原样抛出。'''
cells['c04-build-notes'].source += '''

`build_structural_partitions()` 自行报告输入行数、分钟结构完成、日线结构完成及总耗时，进度单位为两种频率。独立 `build_fresh_partitions()` 另报告完整状态结果的逐频率进度；它调用结构生成函数产生的日志保留独立的 `function/phase`。空输入也完成校验并报告零行结果；异常只报告失败，不报告生成成功。'''
cells['c04-commit-notes'].source = cells['c04-commit-notes'].source.replace(
    'staging、安装和恢复实现保持原样，本轮仅整理说明和入口日志。',
    'staging、安装和恢复实现保持原样。当前叶读取函数自行报告是否存在、文件数和行数；`commit_partition()` 自行报告范围、合并校验、暂存、安装、契约标记与正式复读阶段。完成日志只在原有清理结束后输出，失败日志在原有恢复及清理路径结束后报告失败阶段与恢复状态。')
cells['c04-cli-notes'].source = cells['c04-cli-notes'].source.replace(
    '计数与日志仍由当前 `main()` 负责，函数内部进度归位留待后续。',
    '读取、结构生成、完整结果生成和单叶提交日志由对应函数负责；`main()` 保留模式、计划、批次计数与累计耗时，不重复报告单叶提交起止。水位选择仍属于 `main()`，日志紧贴尾叶读取和最大交易日计算，分别报告各频率—交易所水位及总进度；full 与显式日期记录跳过自动水位选择。')
cells['c04-cli-notes'].source += '\n\nb04 没有独立水位写入。水位日志描述正式目标最大交易日的读取结果；提交函数中的 `schema_marker` 描述零行契约标记，两者不混称水位提交。'
cells['b04-flow-dataset'].source = cells['b04-flow-dataset'].source.replace('根 Dataset 在入口各打开一次；后续读取按分区键和日期过滤，投影所需列。',
    '根 Dataset 在入口各打开一次；发现与读取函数自行报告进度和结果，异常记录后继续抛出。').replace(
    '打开 Dataset；检查逻辑及 fragment 物理契约', '打开 Dataset；逐 fragment 检查并报告进度').replace(
    '按指定 Schema 转成 DataFrame', '按 Schema 转成 DataFrame；报告行数与完成耗时')
cells['b04-flow-build'].source = cells['b04-flow-build'].source.replace('取得 b03 本月结构投影；排序', '记录生成开始；取得结构投影并排序').replace(
    '分别校验两种频率结构；返回结果', '分别校验并报告频率进度；记录完成耗时').replace(
    '主流程进入状态继承；独立 helper 校验完整输出', '主流程进入状态继承；独立 helper 校验并报告完整结果')
cells['b04-flow-commit'].source = cells['b04-flow-commit'].source.replace('本图对应现有本地事务实现；尚未接入共享安装模块。完整业务校验与物理复读是不同边界。',
    '仍使用本地事务；提交函数内部报告各阶段。失败日志位于原有恢复及清理之后，完成日志只在成功后输出。').replace(
    '检查分区与替换范围；保留日期范围外行', '记录开始；检查范围并保留区间外行').replace(
    '保留隔离证据；恢复不完整则保留备份并抛错', '保留隔离及必要备份；清理后记录失败并抛错').replace(
    '清理临时目录；返回提交输入行数', '清理临时目录；记录完成并返回输入行数')
cells['b04-flow-main'].source = cells['b04-flow-main'].source.replace('读取各频率—交易所尾叶；选择尾月及后续月份',
    '逐频率—交易所读取尾叶并记录水位；选择尾部月份').replace('报告分区进度；处理剩余计划', '报告批次进度；处理剩余计划')

assert notebook.metadata == before.metadata
for original in before.cells:
    current = cells[original.id]
    assert original.metadata == current.metadata
    if original.cell_type == 'code':
        assert original.outputs == current.outputs and original.execution_count == current.execution_count
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        assert comments(original.source) == comments(current.source), original.id
        ast.parse(current.source)
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook), encoding='utf-8', newline='\n')
print('Updated b04 function-owned read/build/commit logs and inline watermark logs; comments and execution state retained.')
