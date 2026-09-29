"""a04/b01 第 7—9 项：单次业务验收、分组复用、契约版本与描述文字分离。"""
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
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')
path = ROOT / RELATIVE
original_bytes = path.read_bytes()
notebook = nbformat.read(path, 4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a04-b01-validation-before-'))
related = [RELATIVE, RELATIVE.with_suffix('.py'), pathlib.Path('AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md'), pathlib.Path('03_Futures_Database/AGENTS.md'), pathlib.Path('config/data_contracts.py'), pathlib.Path('00_draft_collection_02/tests/test_b04_c01_macro_release_calendar.py')]
for relative in related:
    destination = snapshot / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, destination)
print(snapshot, flush=True)

def replace(cell_id, old, new, count=1):
    assert cells[cell_id].source.count(old) == count, (cell_id, old, cells[cell_id].source.count(old))
    cells[cell_id].source = cells[cell_id].source.replace(old, new)

def block(indent, source):
    return textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * indent)

def statement(cell_id, function_name, prefix, replacement):
    source = cells[cell_id].source
    function = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == function_name)
    matches = [node for node in ast.walk(function) if isinstance(node, ast.stmt) and ast.unparse(node).startswith(prefix)]
    assert len(matches) == 1, (function_name, prefix, len(matches))
    node = matches[0]
    lines = source.splitlines(keepends=True)
    lines[node.lineno - 1:node.end_lineno] = [block(node.col_offset, replacement)] if replacement else []
    cells[cell_id].source = ''.join(lines)

replace('b5ed1a92', '    validate_arrow_table,\n', '')

# 同一次 fragment 遍历检查物理结构、表身份和契约版本，不逐字段比较描述文字。
statement('a04-b01-code-compatible', 'open_compatible_dataset', 'if not physical_schema_matches(reconstructed_schema', '''
actual_schema = reconstructed_schema(dataset, MACRO_RELEASE_CALENDAR_SCHEMA)
if not physical_schema_matches(actual_schema, MACRO_RELEASE_CALENDAR_SCHEMA):
    raise TypeError(f"{label}物理字段、顺序、类型或 nullable 与权威契约不兼容。")
identity_metadata_keys = (b"table_name", b"primary_key", b"partition_columns")
if any(
    (actual_schema.metadata or {}).get(key) != MACRO_RELEASE_CALENDAR_SCHEMA.metadata[key]
    for key in identity_metadata_keys
):
    raise TypeError(f"{label}表名、主键或分区 metadata 与权威契约不兼容。")
if not (actual_schema.metadata or {}).get(b"schema_version"):
    raise TypeError(f"{label}缺少契约版本 metadata。")
schema_is_current = actual_schema.metadata[b"schema_version"] == MACRO_RELEASE_CALENDAR_SCHEMA.metadata[b"schema_version"]
''')
replace('a04-b01-code-compatible', '        for fragment in dataset.get_fragments():\n', '        for fragment in dataset.get_fragments():\n            fragment_schema = fragment.physical_schema\n')
replace('a04-b01-code-compatible', 'pa.schema(list(fragment.physical_schema))', 'fragment_schema')
replace('a04-b01-code-compatible', '            log_checked_fragments += 1\n', block(12, '''
if any(
    (fragment_schema.metadata or {}).get(key) != MACRO_RELEASE_CALENDAR_SCHEMA.metadata[key]
    for key in identity_metadata_keys
):
    raise TypeError(f"{label}存在表身份 metadata 不兼容的 fragment：{fragment.path}")
if not (fragment_schema.metadata or {}).get(b"schema_version"):
    raise TypeError(f"{label} fragment 缺少契约版本：{fragment.path}")
schema_is_current = schema_is_current and fragment_schema.metadata[b"schema_version"] == MACRO_RELEASE_CALENDAR_SCHEMA.metadata[b"schema_version"]
log_checked_fragments += 1
'''))
statement('a04-b01-code-compatible', 'open_compatible_dataset', 'is_exact = dataset_has_exact_schema_metadata', '')
statement('a04-b01-code-compatible', 'open_compatible_dataset', 'log_phase = \'metadata_exactness\'', '')
statement('a04-b01-code-compatible', 'open_compatible_dataset', "click.echo(f'planning_progress: table={TABLE_NAME}; function=open_compatible_dataset; phase=metadata_exactness", '')
replace('a04-b01-code-compatible', 'metadata_exact={str(is_exact).lower()}', 'contract_current={str(schema_is_current).lower()}')
replace('a04-b01-code-compatible', 'return dataset, is_exact', 'return dataset, schema_is_current')
replace('a04-b01-code-exact', 'dataset, is_exact =', 'dataset, schema_is_current =')
replace('a04-b01-code-exact', 'if not is_exact:', 'if not schema_is_current:')
replace('a04-b01-code-exact', 'Schema/metadata 与权威契约不一致。', '契约版本与当前权威契约不一致。')
replace('a04-b01-code-exact', 'metadata_exact=true', 'contract_current=true')
replace('a04-b01-code-exact', 'log_phase = "metadata_exactness"', 'log_phase = "contract_version"')

# 生成前的 Pandas→Arrow 已完成权威转换；业务验收不重复走转换契约。
replace('ebbfdc80', '    *,\n    allow_legacy_policy: bool = False,\n', '')
replace('ebbfdc80', '; allow_legacy_policy={str(allow_legacy_policy).lower()}', '')
statement('ebbfdc80', 'validate_macro_calendar_table', 'checked = validate_arrow_table', '')
statement('ebbfdc80', 'validate_macro_calendar_table', 'frame = arrow_to_pandas', 'frame = table.to_pandas(types_mapper=pd.ArrowDtype)')
source = cells['ebbfdc80'].source
node = next(node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.If) and ast.unparse(node.test) == 'not allow_legacy_policy')
lines = source.splitlines(keepends=True)
strict_body = ''.join(lines[node.body[0].lineno - 1:node.body[-1].end_lineno])
statement('ebbfdc80', 'validate_macro_calendar_table', 'if not allow_legacy_policy:', strict_body)
replace('ebbfdc80', '        for row in checked.to_pylist():\n', block(8, '''
timestamp_fields = ("fetch_completed_at", "quality_checked_at")
audit_fields = ("fetch_run_id", "fetch_completed_at", "quality_checked_at")
completed_statuses = {"success", "empty_confirmed"}
for row in table.to_pylist():
'''))
replace('ebbfdc80', '''for timestamp_name in [
                "fetch_completed_at",
                "quality_checked_at",
            ]:''', 'for timestamp_name in timestamp_fields:')
replace('ebbfdc80', '''row["fetch_result_status"] in {
                "success",
                "empty_confirmed",
            }''', 'row["fetch_result_status"] in completed_statuses')
# 严格段外层 if 已移除，缩进以实际 AST 为准。
node = next(node for node in ast.walk(ast.parse(cells['ebbfdc80'].source)) if isinstance(node, ast.List) and [getattr(item, 'value', None) for item in node.elts] == ['fetch_run_id', 'fetch_completed_at', 'quality_checked_at'])
old = ast.get_source_segment(cells['ebbfdc80'].source, node)
replace('ebbfdc80', old, 'audit_fields')

statement('d9a85e0d', 'build_expected_calendar', 'existing_rows_by_key =', '''
existing_rows_by_key = {
    tuple(row[name] for name in PRIMARY_KEY): row
    for row in existing_df.to_dict(orient="records")
}
''')
replace('d9a85e0d', '        if start_date <= end_date:\n', block(8, '''
if start_date <= end_date:
    report_dates_by_frequency = {
        "business_day": pd.bdate_range(start_date, end_date).date,
        "month_end": pd.date_range(start_date, end_date, freq="ME").date,
        "quarter_end": pd.date_range(start_date, end_date, freq="QE-DEC").date,
    }
'''))
statement('d9a85e0d', 'build_expected_calendar', "if series.frequency == 'business_day':", 'report_dates = report_dates_by_frequency[series.frequency]')
statement('d9a85e0d', 'build_expected_calendar', 'expected_calendar_df = validate_macro_calendar_table', '''
expected_calendar_table = pandas_to_arrow(expected_df, MACRO_RELEASE_CALENDAR_SCHEMA)
expected_calendar_df = validate_macro_calendar_table(expected_calendar_table, "期望")
''')

replace('db0b5fa0', 'def table_digest(frame: pd.DataFrame)', 'def table_digest(calendar_table: pa.Table)')
statement('db0b5fa0', 'table_digest', 'ordered_df =', 'ordered_table = calendar_table.sort_by([(name, "ascending") for name in PRIMARY_KEY])')
statement('db0b5fa0', 'table_digest', 'source_table =', '')
replace('db0b5fa0', 'source_table.to_pylist()', 'ordered_table.to_pylist()')

statement('a04-b01-code-changed', 'changed_partition_keys', 'expected_keys =', '''
expected_indices_by_partition = expected_df.groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
existing_indices_by_partition = existing_df.groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
expected_calendar_table = pa.Table.from_pandas(
    expected_df, schema=MACRO_RELEASE_CALENDAR_SCHEMA, preserve_index=False,
)
existing_calendar_table = pa.Table.from_pandas(
    existing_df, schema=MACRO_RELEASE_CALENDAR_SCHEMA, preserve_index=False,
)
partition_keys = sorted(expected_indices_by_partition.keys() | existing_indices_by_partition.keys())
''')
statement('a04-b01-code-changed', 'changed_partition_keys', 'existing_keys =', '')
replace('a04-b01-code-changed', 'for partition_key in sorted(expected_keys | existing_keys):', 'for partition_key in partition_keys:')
source = cells['a04-b01-code-changed'].source
start = source.index('            expected_mask =')
end = source.index('        click.echo(\n            f"planning_progress: table={TABLE_NAME}; function=changed_partition_keys; phase=compare; status=completed;', start)
source = source[:start] + block(12, '''
expected_indices = expected_indices_by_partition.get(partition_key)
existing_indices = existing_indices_by_partition.get(partition_key)
if expected_indices is None or existing_indices is None:
    changed_keys.append(partition_key)
    continue
if len(expected_indices) != len(existing_indices):
    changed_keys.append(partition_key)
    continue
if table_digest(expected_calendar_table.take(expected_indices)) != table_digest(existing_calendar_table.take(existing_indices)):
    changed_keys.append(partition_key)
''') + '\n' + source[end:]
cells['a04-b01-code-changed'].source = source

# 完整期望已通过生成验收；只转换一次供摘要、分组、写入、验收和安装复用。
statement('f4c3582d', 'commit_partitions', 'expected_df = validate_macro_calendar_table', '''
expected_df = expected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
calendar_columns = MACRO_RELEASE_CALENDAR_SCHEMA.names
expected_calendar_table = pandas_to_arrow(expected_df, MACRO_RELEASE_CALENDAR_SCHEMA)
empty_calendar_table = expected_calendar_table.slice(0, 0)
expected_indices_by_partition = expected_df.groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
partition_relative_paths = {
    partition_key: pathlib.Path(*[
        f"{name}={value}"
        for name, value in zip(PARTITION_COLUMNS, partition_key, strict=True)
    ])
    for partition_key in partition_keys
}
''')
replace('f4c3582d', 'log_phase = "validate_expected"', 'log_phase = "prepare_expected"')
replace('f4c3582d', 'expected_digest = table_digest(expected_df)', 'expected_digest = table_digest(expected_calendar_table)')
source = cells['f4c3582d'].source
start = source.index('            changed_row_mask =')
end = source.index('            log_phase = "staging_write"', start)
source = source[:start] + block(12, '''
changed_indices = sorted(
    index
    for partition_key in partition_keys
    for index in expected_indices_by_partition.get(partition_key, ())
)
changed_calendar_table = (
    expected_calendar_table if force_full_swap
    else expected_calendar_table.take(pa.array(changed_indices, type=pa.int64()))
)
''') + source[end:]
cells['f4c3582d'].source = source
replace('f4c3582d', '    log_selected_partitions = 0\n', '')
replace('f4c3582d', '    log_last_progress_at = log_started_at\n', '')
replace('f4c3582d', 'if not changed_rows_df.empty:', 'if changed_calendar_table.num_rows:')
statement('f4c3582d', 'commit_partitions', 'ds.write_dataset(', '''
ds.write_dataset(
    changed_calendar_table,
    staging_path,
    format="parquet",
    partitioning=CALENDAR_PARTITIONING,
    existing_data_behavior="delete_matching",
    basename_template="part-{i}.parquet",
)
''')
replace('f4c3582d', 'len(changed_rows_df)', 'changed_calendar_table.num_rows', count=4)
statement('f4c3582d', 'commit_partitions', 'staged_df = validate_macro_calendar_table', '''
staged_calendar_table = staged_dataset.to_table(columns=calendar_columns)
staged_indices_by_partition = staged_calendar_table.select(PARTITION_COLUMNS).to_pandas().groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
''')
replace('f4c3582d', 'len(staged_df)', 'staged_calendar_table.num_rows', count=2)
replace('f4c3582d', 'table_digest(staged_df)', 'table_digest(staged_calendar_table)')
source = cells['f4c3582d'].source
start = source.index('                expected_mask =')
end = source.index('                if len(staged_partition_df)', start)
source = source[:start] + block(16, '''
log_partition = partition_key
expected_indices = expected_indices_by_partition.get(partition_key)
staged_indices = staged_indices_by_partition.get(partition_key)
expected_partition_table = (
    expected_calendar_table.take(expected_indices)
    if expected_indices is not None else empty_calendar_table
)
staged_partition_table = (
    staged_calendar_table.take(staged_indices)
    if staged_indices is not None else empty_calendar_table
)
''') + source[end:]
cells['f4c3582d'].source = source
replace('f4c3582d', 'len(staged_partition_df)', 'staged_partition_table.num_rows', count=2)
replace('f4c3582d', 'len(expected_partition_df)', 'expected_partition_table.num_rows')
replace('f4c3582d', 'not expected_partition_df.empty', 'expected_partition_table.num_rows > 0')
replace('f4c3582d', 'table_digest(staged_partition_df)', 'table_digest(staged_partition_table)')
replace('f4c3582d', 'table_digest(expected_partition_df)', 'table_digest(expected_partition_table)')
statement('f4c3582d', 'commit_partitions', 'relative_path =', 'relative_path = partition_relative_paths[partition_key]')
source = cells['f4c3582d'].source
start = source.index('                    expected_mask =')
end = source.index('                    if should_exist !=', start)
source = source[:start] + block(20, 'should_exist = partition_key in expected_indices_by_partition') + source[end:]
cells['f4c3582d'].source = source
statement('f4c3582d', 'commit_partitions', 'committed_df = validate_macro_calendar_table', 'committed_calendar_table = committed_dataset.to_table(columns=calendar_columns)')
replace('f4c3582d', 'len(committed_df)', 'committed_calendar_table.num_rows', count=3)
replace('f4c3582d', 'table_digest(committed_df)', 'table_digest(committed_calendar_table)')

replace('cd6a9f44', 'existing_dataset, schema_is_exact', 'existing_dataset, schema_is_current')
replace('cd6a9f44', 'not schema_is_exact', 'not schema_is_current')
statement('cd6a9f44', 'main', 'existing_df = validate_macro_calendar_table', 'existing_df = arrow_to_pandas(existing_table, MACRO_RELEASE_CALENDAR_SCHEMA)')
source = cells['cd6a9f44'].source
node = next(node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Try) and any(isinstance(statement_, ast.Expr) and '现有正式当前策略' in ast.get_source_segment(source, statement_) for statement_ in node.body))
legacy_check = ''.join(source.splitlines(keepends=True)[node.lineno - 1:node.end_lineno])
replace('cd6a9f44', legacy_check, '            if metadata_upgrade_required:\n' + textwrap.indent(legacy_check, '    '))
statement('cd6a9f44', 'main', 'if not existing_policy_is_current and', '')
# 版本迁移的正式写入不能借测试日期局部触发；只读当前策略仍可按原范围查看。
replace('cd6a9f44', 'if has_explicit_dates and not existing_policy_is_current:', 'if has_explicit_dates and (not existing_policy_is_current or (write and metadata_upgrade_required)):')
replace('cd6a9f44', '现有宏观发布日历包含旧系列、可用日规则或旧状态语义；', '现有宏观发布日历需要契约版本或旧策略迁移；')
replace('cd6a9f44', '# 只有当前策略严格有效的行才允许继承事实状态；旧 metadata/旧策略整表重置。', '# 当前版本信任正式提交状态；旧契约不能通过当前策略时才整表重置。')
statement('cd6a9f44', 'main', 'expected_full_df = validate_macro_calendar_table', '')
statement('cd6a9f44', 'main', 'existing_rows_by_key =', '''
existing_rows_by_key = {
    tuple(row[name] for name in PRIMARY_KEY): row
    for row in existing_df.to_dict(orient="records")
}
''')
statement('cd6a9f44', 'main', 'expected_rows_by_key =', '''
expected_rows_by_key = {
    tuple(row[name] for name in PRIMARY_KEY): row
    for row in expected_full_df.to_dict(orient="records")
}
''')

# 两个独立展示单元格由本轮之前创建，当前已无调用；一并删除其说明和图。
removed_ids = {
    'a04-b01-doc-metadata', 'a04-b01-code-metadata', 'a04-b01-flow-a04-b01-code-metadata',
    'a04-b01-doc-partition', 'a04-b01-code-partition', 'a04-b01-flow-a04-b01-code-partition',
}
notebook.cells = [cell for cell in notebook.cells if cell.id not in removed_ids]
ast.parse('\n\n'.join(cell.source for cell in notebook.cells if cell.cell_type == 'code'))
nbformat.validate(notebook)
assert before.metadata == notebook.metadata
for old in before.cells:
    if old.id not in removed_ids:
        assert {key: value for key, value in old.items() if key != 'source'} == {key: value for key, value in cells[old.id].items() if key != 'source'}
assert path.read_bytes() == original_bytes
path.write_text(nbformat.writes(notebook) + '\n', encoding='utf8', newline='\n')
(snapshot / 'change_scope.json').write_text(json.dumps({'removed_cell_ids': sorted(removed_ids)}, indent=2), encoding='utf8')
print(json.dumps({'snapshot': str(snapshot), 'flowcharts': 15}, ensure_ascii=False))
