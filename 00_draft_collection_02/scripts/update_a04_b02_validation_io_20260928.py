"""SHIBOR 第 7—9 项：业务验收归位、叶映射复用及局部物理复读。"""
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
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb')
SNAPSHOT = pathlib.Path(tempfile.mkdtemp(prefix='a04-b02-validation-before-'))
selected = [REL, REL.with_suffix('.py'), pathlib.Path('AGENTS.md'), pathlib.Path('03_Futures_Database/AGENTS.md'),
            pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md'),
            pathlib.Path('config/data_contracts.py'), pathlib.Path('00_draft_collection_02/tests/test_b04_c02_interest_rate.py'),
            pathlib.Path('00_draft_collection_02/tests/test_a04_b02_shared_transaction.py'),
            pathlib.Path('00_draft_collection_02/a04_b02_docs_logs_verification_20260928.md')]
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
swap('open_compatible_dataset', '            partitioning=partitioning,\n', '            partitioning=partitioning,\n            partition_base_dir=partition_base_dir,\n')
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
edit('validate_interest_calendar_table', 'checked =', '')
edit('validate_interest_calendar_table', 'frame =', 'calendar_keys_df = table.select(CALENDAR_PRIMARY_KEY).to_pandas()')
swap('validate_interest_calendar_table', 'frame.duplicated(CALENDAR_PRIMARY_KEY)', 'calendar_keys_df.duplicated()')
swap('validate_interest_calendar_table', 'not frame.empty and not frame["dataset_name"].eq(DATASET_NAME).all()',
     'table.num_rows and not all(value == DATASET_NAME for value in table["dataset_name"].to_pylist())')
swap('validate_interest_calendar_table', 'for row in checked.to_pylist():', 'for row in table.to_pylist():')
swap('validate_interest_calendar_table', 'checked.num_rows', 'table.num_rows')
edit('validate_interest_calendar_table', 'validated_calendar_df =', 'validated_calendar_table = table.sort_by(CALENDAR_SORT_KEYS)')
cells[cell_for('validate_interest_calendar_table')].source = cells[cell_for('validate_interest_calendar_table')].source.replace('validated_calendar_df', 'validated_calendar_table').replace(') -> pd.DataFrame:', ') -> pa.Table:')

edit('validate_interest_rate_frame', 'ordered_input_df =', '')
edit('validate_interest_rate_frame', 'table =', '')
edit('validate_interest_rate_frame', 'checked =', '')
edit('validate_interest_rate_frame', 'checked_df =', 'fact_keys_df = table.select(PRIMARY_KEY).to_pandas()')
swap('validate_interest_rate_frame', 'checked_df.duplicated(PRIMARY_KEY)', 'fact_keys_df.duplicated()')
swap('validate_interest_rate_frame', 'for row in checked.to_pylist():', 'for row in table.to_pylist():')
swap('validate_interest_rate_frame', 'checked.num_rows', 'table.num_rows')
edit('validate_interest_rate_frame', 'validated_fact_df =', 'validated_fact_table = table.sort_by(FACT_SORT_KEYS)')
cell = cell_for('validate_interest_rate_frame')
cells[cell].source = cells[cell].source.replace('validate_interest_rate_frame', 'validate_interest_rate_table').replace('    frame: pd.DataFrame,', '    table: pa.Table,').replace(') -> pd.DataFrame:', ') -> pa.Table:').replace('len(frame)', 'table.num_rows').replace('validated_fact_df', 'validated_fact_table')

# 当前正式版本信任生产者业务证明；只读转换承担物理字段和 null 契约。
edit('read_interest_calendar', 'validated_calendar_df =', 'calendar_df = arrow_to_pandas(table, MACRO_RELEASE_CALENDAR_SCHEMA)')
cells[cell_for('read_interest_calendar')].source = cells[cell_for('read_interest_calendar')].source.replace('validated_calendar_df', 'calendar_df').replace('log_phase = "validate"', 'log_phase = "convert"')
edit('read_optional_fact', 'current_table =', '')
edit('read_optional_fact', 'current_df =', 'current_df = arrow_to_pandas(source_table, INTEREST_RATE_DAILY_SCHEMA)')
cells[cell_for('read_optional_fact')].source = cells[cell_for('read_optional_fact')].source.replace('log_phase = "validate"', 'log_phase = "convert"')

# required_rows 已由 required_df 唯一产生，无需建立集合后再次判断自己是否属于集合。
edit('plan_interest_rate_grids', 'required_keys =', '')
edit('plan_interest_rate_grids', 'if key not in required_keys:', '')
edit('normalize_shibor_response', 'fact_df = validate_interest_rate_frame(', '''fact_table = validate_interest_rate_table(
    pandas_to_arrow(fact_df, INTEREST_RATE_DAILY_SCHEMA), "Tushare 转换",
)
fact_df = arrow_to_pandas(fact_table, INTEREST_RATE_DAILY_SCHEMA)''')
edit('normalize_shibor_response', 'expected_keys =', '')
edit('normalize_shibor_response', 'if set(outcome_counts) != expected_keys:', '')

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
cells['edfb8977'].source += '''
# 同一运行期间不变的契约表示，供各月份复用。
FACT_COLUMNS = INTEREST_RATE_DAILY_SCHEMA.names
CALENDAR_COLUMNS = MACRO_RELEASE_CALENDAR_SCHEMA.names
FACT_SORT_KEYS = [(name, "ascending") for name in PRIMARY_KEY]
CALENDAR_SORT_KEYS = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
FACT_FILE_SCHEMA = pa.schema(
    [field for field in INTEREST_RATE_DAILY_SCHEMA if field.name not in PARTITION_COLUMNS],
    metadata=INTEREST_RATE_DAILY_SCHEMA.metadata,
)
SHIBOR_REQUEST_FIELDS = ",".join(SHIBOR_FIELDS)
'''
swap('query_shibor_window', '",".join(SHIBOR_FIELDS)', 'SHIBOR_REQUEST_FIELDS')

for function in ('upgrade_fact_metadata', 'commit_complete_fact_partition'):
    input_name = 'existing_fact_df' if function == 'upgrade_fact_metadata' else 'frame'
    context = '待升级完整 SHIBOR 事实' if function == 'upgrade_fact_metadata' else '待提交完整 SHIBOR 事实分区'
    edit(function, 'complete_df =', f'''complete_table = validate_interest_rate_table(
    pandas_to_arrow({input_name}.loc[:, FACT_COLUMNS], INTEREST_RATE_DAILY_SCHEMA), "{context}",
)''')
    swap(function, 'write_fact_staging(complete_df, staging_path)', 'write_fact_staging(complete_table, staging_path)')
    if function == 'upgrade_fact_metadata':
        edit(function, 'staged_df =', '''staged_table = validate_arrow_table(
    staged_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
        edit(function, 'committed_df =', '''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
    else:
        edit(function, 'if not complete_df.empty:', '''if complete_table.num_rows:
    actual_keys = set(zip(*(complete_table[name].to_pylist() for name in PARTITION_COLUMNS), strict=True))
    if actual_keys != {partition_key}:
        raise ValueError("待提交 SHIBOR 内容越出指定事实叶分区。")''')
        edit(function, 'staged_table =', '''staged_table = validate_arrow_table(
    staged_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
        edit(function, 'staged_df =', '')
        edit(function, 'committed_dataset =', '''if complete_table.num_rows == 0 and destination_path.exists():
    raise ValueError("确认空的正式 SHIBOR 事实叶仍然存在。")
committed_dataset = open_exact_dataset(
    destination_path if complete_table.num_rows else target_marker_path,
    FACT_PARTITIONING, INTEREST_RATE_DAILY_SCHEMA, PARTITION_COLUMNS,
    "正式 SHIBOR 事实", partition_base_dir=target_path,
)
if marker_created and complete_table.num_rows:
    marker_dataset = open_exact_dataset(
        target_marker_path, FACT_PARTITIONING, INTEREST_RATE_DAILY_SCHEMA,
        PARTITION_COLUMNS, "新建正式 SHIBOR 零行标记", partition_base_dir=target_path,
    )
    if marker_dataset.count_rows() != 0:
        raise ValueError("新建正式 SHIBOR 标记必须为零行。")''')
        edit(function, 'committed_table =', '''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=FACT_COLUMNS), INTEREST_RATE_DAILY_SCHEMA,
).sort_by(FACT_SORT_KEYS)''')
        edit(function, 'committed_df =', '')
        swap(function, '        staging_marker_path = staging_path / "schema.parquet"', '        staging_marker_path = staging_path / "schema.parquet"\n        marker_created = not target_marker_path.exists()')
        swap(function, 'if not target_marker_path.exists():', 'if marker_created:')
    edit(function, 'if table_digest(staged_df', '''if not staged_table.equals(complete_table):
    raise ValueError("SHIBOR staging 完整内容逐值复读失败。")''')
    edit(function, 'if table_digest(committed_df', '''if not committed_table.equals(complete_table):
    raise ValueError("正式 SHIBOR 完整内容逐值复读失败。")
committed_df = arrow_to_pandas(committed_table, INTEREST_RATE_DAILY_SCHEMA)''')
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
edit(function, 'partition_table =', '''partition_table = validate_interest_calendar_table(
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

# 主流程只规划一次。每个月份只取得既有叶映射，不更新/重验累计整表。
function = 'main'
swap(function, '''        pending_df, repair_df, complete_count = plan_interest_rate_grids(
            calendar_df,
            existing_fact_df,
            requested_start_date,
            requested_end_date,
        )''', '''        pending_df, repair_df, complete_count = plan_interest_rate_grids(
            calendar_df, existing_fact_df, requested_start_date, requested_end_date,
        )
        fact_leaves = {tuple(key): frame for key, frame in existing_fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True)}
        calendar_leaves = {tuple(key): frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True)}
        empty_fact_df = empty_pandas(INTEREST_RATE_DAILY_SCHEMA)''')
edit(function, 'if not repair_df.empty:', '''if not repair_df.empty:
    repair_count = len(repair_df)
    if write:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=state_repair; status=started; "
            f"rows={repair_count}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        repair_run_id = uuid.uuid4().hex
        repair_time = datetime.now(timezone.utc)
        for key, repair_leaf_df in repair_df.groupby(["year", "month"], sort=True, observed=True):
            calendar_partition_key = (DATASET_NAME, int(key[0]), int(key[1]))
            repair_counts = {(row.series_code, row.report_date): 1 for row in repair_leaf_df.itertuples(index=False)}
            repair_reasons = {key: STATE_REPAIR_REASON for key in repair_counts}
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
start = source.index('        for (year, month), partition_pending_df')
end = source.index('        log_phase = "batch_result"', start)
loop = source[start:end]
loop = loop.replace('            partition_key = (int(year), int(month))', '''            partition_key = (int(year), int(month))
            calendar_partition_key = (DATASET_NAME, *partition_key)
            calendar_leaf_df = calendar_leaves[calendar_partition_key]
            existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)''')
loop = loop.replace('calendar_df = apply_calendar_', 'calendar_leaf_df = apply_calendar_').replace('                    calendar_df,', '                    calendar_leaf_df,').replace('                calendar_df,', '                calendar_leaf_df,')
loop = loop.replace('                existing_fact_df,\n', '                existing_fact_leaf_df,\n')
loop = loop.replace('                    commit_calendar_partition(', '                    calendar_leaves[calendar_partition_key] = commit_calendar_partition(')
loop = loop.replace('            commit_calendar_partition(', '            calendar_leaves[calendar_partition_key] = commit_calendar_partition(')
loop = loop.replace('(DATASET_NAME, int(year), int(month)),', 'calendar_partition_key,')
tail = loop.index('            log_phase = "accumulated_fact_validation"')
loop = loop[:tail] + '''            fact_leaves[partition_key] = committed_partition_df
            complete_count += len(touched_grids)

'''
source = source[:start] + loop + source[end:]
start = source.index('        # 最终必须从两个正式路径重读')
end = source.index('        click.echo(', source.index('        if not final_pending_df.empty', start))
source = source[:start] + '''        # 各 dirty 叶已在事务内正式验收；复用本批提交结果，不再扫描 clean 历史。
''' + source[end:]
source = source.replace('formal_reconciled={final_complete_count}; fact_rows={len(final_fact_df)}',
                        'formal_reconciled={complete_count}; fact_rows={sum(len(frame) for frame in fact_leaves.values())}')
cells[cell].source = source

# 删除不再有业务调用的重复 metadata 遍历、叶过滤和往返序列化摘要辅助块。
removed_ids = set()
for suffix in ('dataset-has-exact-schema-metadata', 'partition-expression', 'table-digest'):
    removed_ids.update('a04-b02-' + kind + '-' + suffix for kind in ('code', 'doc', 'flow'))
notebook.cells = [c for c in notebook.cells if c.id not in removed_ids]
cells['0e3603da'].source = cells['0e3603da'].source.replace('import hashlib\n', '')
for c in notebook.cells:
    if c.cell_type == 'code':
        ast.parse(c.source)
assert before.metadata == notebook.metadata
for c in notebook.cells:
    old = next(x for x in before.cells if x.id == c.id)
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in c.items() if k != 'source'}
nbformat.validate(notebook)
(ROOT / REL).write_text(nbformat.writes(notebook), encoding='utf8', newline='\n')
print(SNAPSHOT)
