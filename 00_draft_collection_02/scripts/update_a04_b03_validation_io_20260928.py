"""宏观发布 第 7—9 项：业务验收归位、叶映射复用及局部物理复读。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
SNAPSHOT = pathlib.Path(tempfile.mkdtemp(prefix='a04-b03-validation-before-'))
selected = [REL, REL.with_suffix('.py'), pathlib.Path('AGENTS.md'), pathlib.Path('03_Futures_Database/AGENTS.md'),
            pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md'),
            pathlib.Path('config/data_contracts.py'), pathlib.Path('00_draft_collection_02/tests/test_b04_c03_macro_release.py'),
            pathlib.Path('00_draft_collection_02/a04_b03_docs_logs_verification_20260928.md')]
for rel in selected:
    saved = SNAPSHOT / rel
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / rel, saved)
paths = [ROOT / rel for rel in selected] + list((ROOT / '02_Futures_Lakehouse').glob('a*/*.ipynb')) + list((ROOT / '02_Futures_Lakehouse').glob('a*/*.py'))
(SNAPSHOT / 'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}), encoding='utf8')
notebook = nbformat.read(ROOT / REL, 4)
before = copy.deepcopy(notebook)
cells = {c.id: c for c in notebook.cells}
helper_source = (ROOT / '00_draft_collection_02/scripts/update_a03_b03_validation_io_20260928.py').read_text(encoding='utf8')
exec(helper_source[helper_source.index('def replace('):helper_source.index('# 重用上一环节')])


def cell_for(function):
    return next(c.id for c in notebook.cells if c.cell_type == 'code' and any(
        isinstance(n, ast.FunctionDef) and n.name == function for n in ast.parse(c.source).body))


def edit(function, prefix, replacement):
    statement(cell_for(function), function, prefix, replacement)


def swap(function, old, new, count=1):
    replace(cell_for(function), old, new, count)


# 兼容检查合并成一次 fragment 遍历；版本用于旧契约迁移，描述性差异不影响兼容。
cell = cell_for('open_compatible_dataset')
swap('open_compatible_dataset', '    label: str,\n) -> tuple[ds.Dataset, bool]:',
     '    label: str,\n    *,\n    partition_base_dir: pathlib.Path | None = None,\n) -> tuple[ds.Dataset, bool]:')
swap('open_compatible_dataset', '            else []\n', '            else [table_path] if table_path.is_file() else []\n')
swap('open_compatible_dataset', '            partitioning=partitioning,\n', '            partitioning=partitioning,\n            partition_base_dir=partition_base_dir.as_posix() if partition_base_dir is not None else None,\n')
edit('open_compatible_dataset', 'if not physical_schema_matches(reconstructed_schema(', '''actual_schema = reconstructed_schema(dataset, schema)
if not physical_schema_matches(actual_schema, schema):
    raise TypeError(f"{label}物理字段、顺序、类型或 nullable 与权威契约不兼容。")
identity_keys = (b"table_name", b"primary_key", b"partition_columns")
if any((actual_schema.metadata or {}).get(key) != schema.metadata[key] for key in identity_keys):
    raise TypeError(f"{label}表名、主键或分区身份与权威契约不一致。")
is_exact = (actual_schema.metadata or {}).get(b"schema_version") == schema.metadata[b"schema_version"]''')
swap('open_compatible_dataset', '            log_checked_fragments += 1', '''            fragment_metadata = fragment.physical_schema.metadata or {}
            if any(fragment_metadata.get(key) != schema.metadata[key] for key in identity_keys):
                raise TypeError(f"{label}存在表身份不兼容的 fragment：{fragment.path}")
            is_exact = is_exact and fragment_metadata.get(b"schema_version") == schema.metadata[b"schema_version"]
            log_checked_fragments += 1''')
edit('open_compatible_dataset', 'is_exact = dataset_has_exact_schema_metadata(', '')
swap('open_exact_dataset', '    label: str,\n) -> ds.Dataset:',
     '    label: str,\n    *,\n    partition_base_dir: pathlib.Path | None = None,\n) -> ds.Dataset:')
swap('open_exact_dataset', '            label,\n', '            label,\n            partition_base_dir=partition_base_dir,\n')

# 校验函数只消费已经转换的 Arrow，对来源输出或 dirty 完整叶做一次业务验收。
edit('validate_macro_calendar_table', 'checked =', '')
edit('validate_macro_calendar_table', 'frame =', 'calendar_keys_df = table.select(CALENDAR_PRIMARY_KEY).to_pandas()')
swap('validate_macro_calendar_table', 'frame.duplicated(CALENDAR_PRIMARY_KEY)', 'calendar_keys_df.duplicated()')
swap('validate_macro_calendar_table', 'not frame.empty and not frame["dataset_name"].eq(DATASET_NAME).all()',
     'table.num_rows and not all(value == DATASET_NAME for value in table["dataset_name"].to_pylist())')
swap('validate_macro_calendar_table', 'for row in checked.to_pylist():', 'for row in table.to_pylist():')
swap('validate_macro_calendar_table', 'checked.num_rows', 'table.num_rows')
edit('validate_macro_calendar_table', 'validated_calendar_df =', 'validated_calendar_table = table.sort_by(CALENDAR_SORT_KEYS)')
cells[cell_for('validate_macro_calendar_table')].source = cells[cell_for('validate_macro_calendar_table')].source.replace('validated_calendar_df', 'validated_calendar_table').replace(') -> pd.DataFrame:', ') -> pa.Table:')

# 宏观 value 物理可空，且 Arrow 会接受布尔转浮点；保留转换前的值门禁。
edit('validate_macro_release_frame', 'checked =', '')
edit('validate_macro_release_frame', 'checked_df =', 'fact_keys_df = table.select(PRIMARY_KEY).to_pandas()')
swap('validate_macro_release_frame', 'checked_df.duplicated(PRIMARY_KEY)', 'fact_keys_df.duplicated()')
swap('validate_macro_release_frame', 'for row in checked.to_pylist():', 'for row in table.to_pylist():')
swap('validate_macro_release_frame', 'checked.num_rows', 'table.num_rows')
edit('validate_macro_release_frame', 'if not np.isfinite(numeric_values.to_numpy()).all():', '')
edit('validate_macro_release_frame', 'validated_fact_df =', 'validated_fact_table = table.sort_by(FACT_SORT_KEYS)')
cell = cell_for('validate_macro_release_frame')
cells[cell].source = cells[cell].source.replace(') -> pd.DataFrame:', ') -> pa.Table:').replace('validated_fact_df', 'validated_fact_table')

# 当前正式版本信任生产者业务证明；只读转换承担物理字段和 null 契约。
edit('read_macro_calendar', 'validated_calendar_df =', 'calendar_df = arrow_to_pandas(table, MACRO_RELEASE_CALENDAR_SCHEMA)')
cells[cell_for('read_macro_calendar')].source = cells[cell_for('read_macro_calendar')].source.replace('validated_calendar_df', 'calendar_df').replace('log_phase = "validate"', 'log_phase = "convert"')
edit('read_optional_fact', 'current_table =', '')
edit('read_optional_fact', 'current_df =', 'current_df = arrow_to_pandas(source_table, MACRO_RELEASE_SCHEMA)')
cells[cell_for('read_optional_fact')].source = cells[cell_for('read_optional_fact')].source.replace('log_phase = "validate"', 'log_phase = "convert"')

# 来源值、日期及分页门禁保留；只删除按同一待办循环必然成立的覆盖重验。
edit('normalize_macro_release_response', 'fact_df = validate_macro_release_frame(', '''fact_table = validate_macro_release_frame(fact_df, "Eastmoney 转换")
fact_df = arrow_to_pandas(fact_table, MACRO_RELEASE_SCHEMA)''')
edit('normalize_macro_release_response', 'expected_keys =', '')
edit('normalize_macro_release_response', 'if set(outcome_counts) != expected_keys:', '')

# main 传入一次分组得到的当前事实叶；合并不再重复完整业务校验。
edit('full_fact_partition', 'partition_df = existing_fact_df.loc', 'partition_df = existing_fact_df')
edit('full_fact_partition', 'year, month =', '')
edit('full_fact_partition', 'merged_fact_df =', '')
cells[cell_for('full_fact_partition')].source = cells[cell_for('full_fact_partition')].source.replace('merged_fact_df', 'merged_df').replace('log_phase = "output_validation"', 'log_phase = "merge_ready"')

# staging 写入复用已通过质量验收的 Arrow 对象和模块级文件 Schema。
cell = cell_for('write_fact_staging')
edit('write_fact_staging', 'file_schema =', '')
edit('write_fact_staging', 'table = pandas_to_arrow(', '')
cells[cell].source = cells[cell].source.replace('    frame: pd.DataFrame,', '    table: pa.Table,').replace('len(frame)', 'table.num_rows').replace('schema=file_schema', 'schema=FACT_FILE_SCHEMA').replace('        log_phase = "convert"\n', '')
cells['166bdeb3'].source += '''
# 同一运行期间不变的契约表示，供各月份复用。
FACT_COLUMNS = MACRO_RELEASE_SCHEMA.names
CALENDAR_COLUMNS = MACRO_RELEASE_CALENDAR_SCHEMA.names
FACT_SORT_KEYS = [(name, "ascending") for name in PRIMARY_KEY]
CALENDAR_SORT_KEYS = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
FACT_FILE_SCHEMA = pa.schema(
    [field for field in MACRO_RELEASE_SCHEMA if field.name not in PARTITION_COLUMNS],
    metadata=MACRO_RELEASE_SCHEMA.metadata,
)
'''


for function in ('upgrade_fact_metadata', 'commit_complete_fact_partition'):
    input_name = 'existing_fact_df' if function == 'upgrade_fact_metadata' else 'frame'
    context = '待升级完整 宏观发布 事实' if function == 'upgrade_fact_metadata' else '待提交完整 宏观发布 事实分区'
    edit(function, 'complete_df =', f'''complete_table = validate_macro_release_frame(
    {input_name}.loc[:, FACT_COLUMNS], "{context}",
)''')
    swap(function, 'write_fact_staging(complete_df, staging_path)', 'write_fact_staging(complete_table, staging_path)')
    if function == 'upgrade_fact_metadata':
        edit(function, 'staged_df =', '''staged_table = validate_arrow_table(
    staged_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
        edit(function, 'committed_df =', '''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
    else:
        edit(function, 'if not complete_df.empty:\n    actual_keys =', '''if complete_table.num_rows:
    actual_keys = set(zip(*(complete_table[name].to_pylist() for name in PARTITION_COLUMNS), strict=True))
    if actual_keys != {partition_key}:
        raise ValueError("待提交 宏观发布 内容越出指定事实叶分区。")''')
        edit(function, 'staged_table =', '''staged_table = validate_arrow_table(
    staged_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
        edit(function, 'staged_df =', '')
        edit(function, 'committed_dataset =', '''if complete_table.num_rows == 0 and destination_path.exists():
    raise ValueError("确认空的正式 宏观发布 事实叶仍然存在。")
committed_dataset = open_exact_dataset(
    destination_path if complete_table.num_rows else target_marker_path,
    FACT_PARTITIONING, MACRO_RELEASE_SCHEMA, PARTITION_COLUMNS,
    "正式宏观发布事实", partition_base_dir=target_path,
)
if marker_created and complete_table.num_rows:
    marker_dataset = open_exact_dataset(
        target_marker_path, FACT_PARTITIONING, MACRO_RELEASE_SCHEMA,
        PARTITION_COLUMNS, "新建正式宏观发布零行标记", partition_base_dir=target_path,
    )
    if marker_dataset.count_rows() != 0:
        raise ValueError("新建正式 宏观发布 标记必须为零行。")''')
        edit(function, 'committed_table =', '''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=FACT_COLUMNS), MACRO_RELEASE_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
        edit(function, 'committed_df =', '')
    edit(function, 'if table_digest(staged_df', '''if not staged_table.equals(complete_table):
    raise ValueError("宏观发布 staging 完整内容逐值复读失败。")''')
    edit(function, 'if table_digest(committed_df', '''if not committed_table.equals(complete_table):
    raise ValueError("正式 宏观发布 完整内容逐值复读失败。")
committed_df = arrow_to_pandas(committed_table, MACRO_RELEASE_SCHEMA)''')
    cell = cell_for(function)
    cells[cell].source = cells[cell].source.replace('len(staged_df)', 'staged_table.num_rows').replace('not complete_df.empty', 'complete_table.num_rows > 0')

for function in ('apply_calendar_completion', 'apply_calendar_failure'):
    edit(function, 'updated_calendar_df =', '')
    cell = cell_for(function)
    cells[cell].source = cells[cell].source.replace('updated_calendar_df', 'updated_df').replace('log_phase = "output_validation"', 'log_phase = "state_ready"')

# 日历只校验 dirty 叶，staging 和正式复读不重新执行整表业务验收。
function = 'commit_calendar_partition'
edit(function, 'complete_calendar_df =', '')
cell = cell_for(function)
cells[cell].source = cells[cell].source.replace('complete_calendar_df', 'calendar_df')
edit(function, 'partition_table =', '''partition_table = validate_macro_calendar_table(
    pandas_to_arrow(partition_df.loc[:, CALENDAR_COLUMNS], MACRO_RELEASE_CALENDAR_SCHEMA),
    "待提交完整日历分区",
)''')
edit(function, 'open_exact_dataset(', '')
edit(function, 'file_schema =', '')
edit(function, 'pq.write_table(', '')
edit(function, 'staged_dataset =', '''staged_dataset = open_exact_dataset(
    staging_path / relative_path, CALENDAR_PARTITIONING, MACRO_RELEASE_CALENDAR_SCHEMA,
    CALENDAR_PARTITION_COLUMNS, "宏观发布日历 staging", partition_base_dir=staging_path,
)''')
edit(function, 'staged_table =', '''staged_table = validate_arrow_table(
    staged_dataset.to_table(columns=CALENDAR_COLUMNS), MACRO_RELEASE_CALENDAR_SCHEMA,
).sort_by(CALENDAR_SORT_KEYS)''')
edit(function, 'staged_df =', '')
edit(function, 'if table_digest(staged_df', '''if not staged_table.equals(partition_table):
    raise ValueError("宏观发布日历 staging 完整叶逐值复读失败。")''')
edit(function, 'committed_dataset =', '''committed_dataset = open_exact_dataset(
    destination_path, CALENDAR_PARTITIONING, MACRO_RELEASE_CALENDAR_SCHEMA,
    CALENDAR_PARTITION_COLUMNS, "回写后的正式宏观发布日历", partition_base_dir=target_path,
)''')
edit(function, 'committed_table =', '''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=CALENDAR_COLUMNS), MACRO_RELEASE_CALENDAR_SCHEMA,
).sort_by(CALENDAR_SORT_KEYS)''')
edit(function, 'committed_df =', '')
edit(function, 'if table_digest(committed_df', '''if not committed_table.equals(partition_table):
    raise ValueError("正式宏观发布日历完整叶逐值复读失败。")
committed_df = arrow_to_pandas(committed_table, MACRO_RELEASE_CALENDAR_SCHEMA)''')
cells[cell].source = cells[cell].source.replace('len(staged_df)', 'staged_table.num_rows')
# 分区相对路径在 staging、正式读写各阶段共同复用。
fn = next(n for n in ast.parse(cells[cell].source).body if isinstance(n, ast.FunctionDef))
relative = next(n for n in ast.walk(fn) if isinstance(n, ast.Assign) and ast.unparse(n).startswith('relative_path ='))
relative_text = ast.get_source_segment(cells[cell].source, relative)
edit(function, 'relative_path =', '')
edit(function, 'silver_root =', relative_text + '\nsilver_root = lake_root.resolve() / "silver"')


# 日历上游只需确认当前叶存在；不再为每次回写打开整表根。
edit('commit_calendar_partition', 'destination_path =', '')
edit('commit_calendar_partition', 'if not destination_path.is_dir():', '')
swap('commit_calendar_partition', '        target_path = silver_root / CALENDAR_TABLE_NAME', '''        target_path = silver_root / CALENDAR_TABLE_NAME
        destination_path = target_path / relative_path
        if not destination_path.is_dir():
            raise FileNotFoundError(f"正式宏观发布日历缺少待回写完整叶：{relative_path}")''')
cells[cell_for('commit_calendar_partition')].source = cells[cell_for('commit_calendar_partition')].source.replace('        log_phase = "upstream_contract"\n', '')

# 当前请求窗口的日期过滤、列串，以及当前报告的频率都不随页/行变化。
function = 'query_eastmoney_report_range'
source = cells[cell_for(function)].source
fn = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef))
date_filter = next(n for n in ast.walk(fn) if isinstance(n, ast.Assign) and ast.unparse(n).startswith('date_filter ='))
date_text = ast.unparse(date_filter)
edit(function, 'date_filter =', '')
edit(function, 'page_number = 1', date_text + '\nrequest_columns = ",".join(fields)\npage_number = 1')
swap(function, '",".join(fields),', 'request_columns,')

function = 'normalize_macro_release_response'
source = cells[cell_for(function)].source
fn = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef))
frequency_prefixes = ('report_frequencies =', 'if len(report_frequencies) != 1:', 'report_frequency =')
frequency_nodes = [next(n for n in ast.walk(fn) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)) for prefix in frequency_prefixes]
frequency_text = '\n'.join(ast.unparse(n) for n in frequency_nodes)
for prefix in frequency_prefixes:
    edit(function, prefix, '')
edit(function, 'values_by_date:', frequency_text + '\nvalues_by_date: dict[date, dict[str, float | None]] = {}')

# 每次启动只读表和规划一次；各报告共用当月已提交的叶映射。
function = 'main'
swap(function, '''        pending_df, repair_df, complete_count = plan_macro_release_grids(
            calendar_df,
            existing_fact_df,
            requested_start_date,
            requested_end_date,
        )''', '''        pending_df, repair_df, complete_count = plan_macro_release_grids(
            calendar_df, existing_fact_df, requested_start_date, requested_end_date,
        )
        fact_leaves = {tuple(key): frame for key, frame in existing_fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True)}
        calendar_leaves = {tuple(key): frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True)}
        empty_fact_df = empty_pandas(MACRO_RELEASE_SCHEMA)''')
edit(function, 'if not repair_df.empty:', '''if not repair_df.empty:
    repair_count = len(repair_df)
    if write:
        repair_run_id = uuid.uuid4().hex
        repair_time = datetime.now(timezone.utc)
        for key, repair_leaf_df in repair_df.groupby(["year", "month"], sort=True, observed=True):
            calendar_partition_key = (DATASET_NAME, int(key[0]), int(key[1]))
            repair_counts = {(row.series_code, row.report_date): 1 for row in repair_leaf_df.itertuples(index=False)}
            repair_reasons = {key: STATE_REPAIR_REASON for key in repair_counts}
            log_phase = "completion_state"
            calendar_leaf_df = apply_calendar_completion(
                calendar_leaves[calendar_partition_key], repair_counts, repair_reasons, repair_run_id, repair_time,
            )
            log_phase = "repair_calendar_commit"
            calendar_leaves[calendar_partition_key] = commit_calendar_partition(
                calendar_leaf_df, resolved_lake_root, calendar_partition_key,
            )
        complete_count += repair_count
        repair_df = repair_df.iloc[0:0]
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=state_repair; status=completed; "
            f"rows={repair_count}; remaining_api_pending={len(pending_df)}; message=state_repaired: rows={repair_count}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
    else:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=state_repair; status=skipped; "
            f"rows={repair_count}; reason=dry_run; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )''')
cell = cell_for(function)
source = cells[cell].source
start = source.index('            for (')
end = source.index('        finally:\n            session.close()', start)
loop = source[start:end]
loop = loop.replace('                partition_key = (int(year), int(month))', '''                partition_key = (int(year), int(month))
                calendar_partition_key = (DATASET_NAME, *partition_key)
                calendar_leaf_df = calendar_leaves[calendar_partition_key]
                existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)''')
loop = loop.replace('calendar_df', 'calendar_leaf_df')
loop = loop.replace('                    existing_fact_df,\n', '                    existing_fact_leaf_df,\n')
loop = loop.replace('                        commit_calendar_partition(', '                        calendar_leaves[calendar_partition_key] = commit_calendar_partition(')
loop = loop.replace('                commit_calendar_partition(', '                calendar_leaves[calendar_partition_key] = commit_calendar_partition(')
loop = loop.replace('(DATASET_NAME, int(year), int(month)),', 'calendar_partition_key,')
tail = loop.index('                outside_partition_df =')
loop = loop[:tail] + '''                fact_leaves[partition_key] = committed_partition_df
                complete_count += len(touched_grids)
'''
source = source[:start] + loop + source[end:]
start = source.index('        # 最终必须从两个正式路径重读')
end = source.index('        click.echo(', source.index('        if not final_pending_df.empty', start))
source = source[:start] + '        # 各 dirty 叶已经正式验收；复用本批提交结果，不再扫描 clean 历史。\n' + source[end:]
source = source.replace('formal_reconciled={final_complete_count}; fact_rows={len(final_fact_df)}',
                        'formal_reconciled={complete_count}; fact_rows={sum(len(frame) for frame in fact_leaves.values())}')
cells[cell].source = source

# 删除已无调用的 metadata 重扫、分区过滤及往返序列化摘要块。
removed_ids = {'a04-b03-'+kind+'-'+name for kind in ('code','doc','flow')
               for name in ('dataset_has_exact_schema_metadata','partition_expression','table_digest')}
notebook.cells = [c for c in notebook.cells if c.id not in removed_ids]
cells['83cfef17'].source = cells['83cfef17'].source.replace('import hashlib\n', '')
for c in notebook.cells:
    if c.cell_type == 'code':
        ast.parse(c.source)
assert before.metadata == notebook.metadata
for c in notebook.cells:
    old = next(x for x in before.cells if x.id == c.id)
    assert {k:v for k,v in old.items() if k!='source'} == {k:v for k,v in c.items() if k!='source'}
nbformat.validate(notebook)
(ROOT/REL).write_text(nbformat.writes(notebook),encoding='utf8',newline='\n')
print(SNAPSHOT)
