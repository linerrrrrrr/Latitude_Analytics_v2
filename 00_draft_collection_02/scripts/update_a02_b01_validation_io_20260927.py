"""a02/b01 第 7—9 项：可信输入、单次业务校验、一次分组与转换复用。"""
import ast
import copy
import pathlib
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01_exchange_report_calendar.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id: c for c in notebook.cells}


def replace(cell_id, old, new, count=1):
    source = cells[cell_id].source
    assert source.count(old) == count, (cell_id, old, source.count(old))
    cells[cell_id].source = source.replace(old, new)


def block(indent, source):
    return textwrap.indent(textwrap.dedent(source).strip() + '\n', ' ' * indent)


def replace_statement(cell_id, function_name, prefix, replacement):
    source = cells[cell_id].source
    function = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == function_name)
    nodes = [n for n in ast.walk(function) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)]
    assert len(nodes) == 1, (function_name, prefix, len(nodes))
    node = nodes[0]
    lines = source.splitlines(keepends=True)
    lines[node.lineno - 1:node.end_lineno] = [block(node.col_offset, replacement)] if replacement else []
    cells[cell_id].source = ''.join(lines)


# 转换操作直接留在 main 的物化位置；已由 a01/b02 证明的业务键和年月不再复核。
replace_statement('5d05b12f', 'validate_upstream_table', 'def validate_upstream_table', '')
replace_statement('5d05b12f', 'partition_expression', 'def partition_expression', '')
replace('5d05b12f', block(8, '''
# Arrow 转换先固定列顺序、类型、nullable 以及全部中文 metadata。
log_phase = "convert"
checked = validate_arrow_table(
    table.cast(FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, safe=True),
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
)
frame = arrow_to_pandas(
    checked,
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
)
'''), block(8, '''
# 输入已由调用方完成权威 Arrow 转换，此处只承担报告日历业务校验。
log_phase = "convert"
frame = table.to_pandas(types_mapper=pd.ArrowDtype)
'''))
replace('5d05b12f', '        for row in checked.to_pylist():\n', block(8, '''
audit_fields = ("fetch_run_id", "fetch_completed_at", "quality_checked_at")
completed_statuses = {"success", "empty_confirmed"}
checked_quality_statuses = {"passed", "warning", "failed"}
for row in table.to_pylist():
'''))
replace('5d05b12f', '''for name in [
                            "fetch_run_id",
                            "fetch_completed_at",
                            "quality_checked_at",
                        ]''', 'for name in audit_fields')
replace('5d05b12f', '''row["fetch_result_status"] in {
                "success",
                "empty_confirmed",
            }''', 'row["fetch_result_status"] in completed_statuses')
replace('5d05b12f', 'row["quality_status"] in {"passed", "warning", "failed"}', 'row["quality_status"] in checked_quality_statuses')

replace('38ff53e0', '# 现有行已经通过本表完整校验，可以按权威主键安全索引。', '# 现有正式行的业务语义由提交保证，可按权威主键索引并继承状态。')
replace('38ff53e0', '''for row in pandas_to_arrow(
                existing_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ).to_pylist()''', 'for row in existing_df.to_dict(orient="records")')
replace('38ff53e0', block(8, '''
log_phase = "upstream_conversion"
upstream_rows = pandas_to_arrow(
    upstream_df.loc[:, FUTURES_VARIETY_CALENDAR_SCHEMA.names],
    FUTURES_VARIETY_CALENDAR_SCHEMA,
).to_pylist()
'''), block(8, '''
log_phase = "upstream_records"
upstream_rows = upstream_df.to_dict(orient="records")
required_reason_by_dataset = {
    dataset_name: (
        "品种位于期货事实采集白名单；"
        f"{dataset_name} 当前未配置额外 API 覆盖或交易所支持排除。"
    )
    for dataset_name in DATASET_NAMES
}
excluded_reason_by_dataset = {
    dataset_name: (
        "品种不在期货事实采集白名单；"
        f"保留 {dataset_name} 报告日历格点但不采集事实。"
    )
    for dataset_name in DATASET_NAMES
}
'''))
replace('38ff53e0', block(20, '''
requirement_reason = (
    "品种位于期货事实采集白名单；"
    f"{dataset_name} 当前未配置额外 API 覆盖或交易所支持排除。"
)
'''), block(20, 'requirement_reason = required_reason_by_dataset[dataset_name]'))
replace('38ff53e0', block(20, '''
requirement_reason = (
    "品种不在期货事实采集白名单；"
    f"保留 {dataset_name} 报告日历格点但不采集事实。"
)
'''), block(20, 'requirement_reason = excluded_reason_by_dataset[dataset_name]'))
replace('38ff53e0', '        expected_calendar_df = validate_report_calendar_table(pandas_to_arrow(candidate_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names], FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA), \'期望\')\n', block(8, '''
expected_calendar_table = pandas_to_arrow(
    candidate_df, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
)
expected_calendar_df = validate_report_calendar_table(
    expected_calendar_table, "期望",
)
'''))

# 摘要是已转换 Arrow 数据的独立内容操作；保留标量重建以消除物理缓冲区布局差异。
replace('b14ee43f', 'def table_digest(frame: pd.DataFrame) -> str:', 'def table_digest(calendar_table: pa.Table) -> str:')
replace('b14ee43f', block(4, '''
ordered_df = frame.sort_values(PRIMARY_KEY).reset_index(drop=True)
source_table = pandas_to_arrow(
    ordered_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
)
'''), block(4, 'ordered_table = calendar_table.sort_by([(name, "ascending") for name in PRIMARY_KEY])'))
replace('b14ee43f', 'source_table.to_pylist()', 'ordered_table.to_pylist()')
replace('b14ee43f', block(8, '''
expected_keys = set(
    expected_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
)
existing_keys = set(
    existing_df[PARTITION_COLUMNS].itertuples(index=False, name=None)
)
changed_keys = []

log_total_partitions = len(expected_keys | existing_keys)
log_phase = "compare"
for partition_key in sorted(expected_keys | existing_keys):
'''), block(8, '''
expected_indices_by_partition = expected_df.groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
existing_indices_by_partition = existing_df.groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
partition_keys = sorted(expected_indices_by_partition.keys() | existing_indices_by_partition.keys())
expected_calendar_table = pa.Table.from_pandas(
    expected_df, schema=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, preserve_index=False,
)
existing_calendar_table = pa.Table.from_pandas(
    existing_df, schema=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, preserve_index=False,
)
changed_keys = []

log_total_partitions = len(partition_keys)
log_phase = "compare"
for partition_key in partition_keys:
'''))
replace('b14ee43f', block(12, '''
expected_mask = pd.Series(True, index=expected_df.index)
existing_mask = pd.Series(True, index=existing_df.index)

for column, value in zip(
    PARTITION_COLUMNS,
    partition_key,
    strict=True,
):
    expected_mask &= expected_df[column].eq(value)
    existing_mask &= existing_df[column].eq(value)

expected_partition_df = expected_df.loc[expected_mask]
existing_partition_df = existing_df.loc[existing_mask]
expected_digest = (
    table_digest(expected_partition_df)
    if not expected_partition_df.empty
    else None
)
existing_digest = (
    table_digest(existing_partition_df)
    if not existing_partition_df.empty
    else None
)
'''), block(12, '''
expected_indices = expected_indices_by_partition.get(partition_key)
existing_indices = existing_indices_by_partition.get(partition_key)
expected_digest = (
    table_digest(expected_calendar_table.take(expected_indices))
    if expected_indices is not None else None
)
existing_digest = (
    table_digest(existing_calendar_table.take(existing_indices))
    if existing_indices is not None else None
)
'''))

replace('6f1d04d3', block(8, '''
# 提交前先验证完整期望表，保证删除旧行也来自合法的上游当前真值。
log_phase = "input_validation"
expected_df = validate_report_calendar_table(
    pandas_to_arrow(
        expected_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
    ),
    "待提交完整",
)
log_phase = "expected_digest"
expected_digest = table_digest(expected_df)
'''), block(8, '''
# 期望新行已通过生成校验，范围外旧行继承正式提交证明；本函数验收安装内容。
log_phase = "input_conversion"
calendar_columns = FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
expected_calendar_table = pandas_to_arrow(
    expected_df.loc[:, calendar_columns], FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
)
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
log_phase = "expected_digest"
expected_digest = table_digest(expected_calendar_table)
'''))
replace('6f1d04d3', block(12, '''
log_phase = "select_changed_rows"
changed_row_mask = pd.Series(False, index=expected_df.index)
for partition_key in partition_keys:
    partition_mask = pd.Series(True, index=expected_df.index)
    for column, value in zip(
        PARTITION_COLUMNS,
        partition_key,
        strict=True,
    ):
        partition_mask &= expected_df[column].eq(value)
    changed_row_mask |= partition_mask
    log_selected_partitions += 1
    if time.perf_counter() - log_last_progress_at >= 2.0:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partitions; phase=select_changed_rows; status=running; "
            f"partition={partition_key}; selected_partitions={log_selected_partitions}/{len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_last_progress_at = time.perf_counter()

changed_rows_df = expected_df.loc[changed_row_mask]
'''), block(12, '''
log_phase = "select_changed_rows"
changed_indices = sorted(
    index
    for partition_key in partition_keys
    for index in expected_indices_by_partition.get(partition_key, ())
)
changed_calendar_table = expected_calendar_table.take(pa.array(changed_indices, type=pa.int64()))
'''))
replace('6f1d04d3', '    log_selected_partitions = 0\n', '')
replace('6f1d04d3', '    log_last_progress_at = log_started_at\n', '')
replace('6f1d04d3', '''if not changed_rows_df.empty:
                ds.write_dataset(
                    pandas_to_arrow(
                        changed_rows_df.loc[:, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names],
                        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                    ),''', '''if changed_calendar_table.num_rows:
                ds.write_dataset(
                    changed_calendar_table,''')
cells['6f1d04d3'].source = cells['6f1d04d3'].source.replace('len(changed_rows_df)', 'changed_calendar_table.num_rows')
replace('6f1d04d3', block(12, '''
staged_df = validate_report_calendar_table(
    staged_dataset.to_table(
        columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
    ),
    "staging ",
)
if len(staged_df) != changed_calendar_table.num_rows:
'''), block(12, '''
staged_calendar_table = validate_arrow_table(
    staged_dataset.to_table(columns=calendar_columns),
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
)
staged_indices_by_partition = staged_calendar_table.select(PARTITION_COLUMNS).to_pandas(
    types_mapper=pd.ArrowDtype,
).groupby(PARTITION_COLUMNS, sort=False, observed=True, dropna=False).indices
if staged_calendar_table.num_rows != changed_calendar_table.num_rows:
'''))
replace('6f1d04d3', 'len(staged_df)', 'staged_calendar_table.num_rows')
replace('6f1d04d3', block(16, '''
expected_mask = pd.Series(True, index=expected_df.index)
for column, value in zip(
    PARTITION_COLUMNS,
    partition_key,
    strict=True,
):
    expected_mask &= expected_df[column].eq(value)
expected_partition_df = expected_df.loc[expected_mask]
'''), block(16, '''
expected_indices = expected_indices_by_partition.get(partition_key, ())
staged_indices = staged_indices_by_partition.get(partition_key, ())
'''))
replace('6f1d04d3', 'len(expected_partition_df)', 'len(expected_indices)', count=2)
replace('6f1d04d3', block(16, '''
staged_partition_table = staged_dataset.to_table(
    columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
    filter=partition_expression(partition_key),
)
staged_partition_df = validate_report_calendar_table(
    staged_partition_table,
    "staging 叶分区",
)

if len(staged_partition_df) != len(expected_indices):
'''), block(16, '''
if len(staged_indices) != len(expected_indices):
'''))
replace('6f1d04d3', '''not expected_partition_df.empty
                    and table_digest(staged_partition_df)
                    != table_digest(expected_partition_df)''', '''len(expected_indices)
                    and table_digest(staged_calendar_table.take(staged_indices))
                    != table_digest(expected_calendar_table.take(expected_indices))''')
replace('6f1d04d3', 'len(staged_partition_df)', 'len(staged_indices)')
replace_statement('6f1d04d3', 'commit_partitions', 'relative_path = pathlib.Path', 'relative_path = partition_relative_paths[partition_key]')
replace('6f1d04d3', block(20, '''
expected_mask = pd.Series(True, index=expected_df.index)
for column, value in zip(
    PARTITION_COLUMNS,
    partition_key,
    strict=True,
):
    expected_mask &= expected_df[column].eq(value)
should_exist = bool(expected_mask.any())
'''), block(20, 'should_exist = partition_key in expected_indices_by_partition'))
replace('6f1d04d3', block(12, '''
committed_df = validate_report_calendar_table(
    committed_dataset.to_table(
        columns=FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
    ),
    "正式路径复读的",
)
if len(committed_df) != len(expected_df):
'''), block(12, '''
committed_calendar_table = validate_arrow_table(
    committed_dataset.to_table(columns=calendar_columns),
    FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
)
if committed_calendar_table.num_rows != expected_calendar_table.num_rows:
'''))
replace('6f1d04d3', 'table_digest(committed_df)', 'table_digest(committed_calendar_table)')
replace('6f1d04d3', 'len(committed_df)', 'committed_calendar_table.num_rows', count=2)

replace('40229d27', '''upstream_df = validate_upstream_table(
            upstream_dataset.to_table(''', '''upstream_df = arrow_to_pandas(
            upstream_dataset.to_table(''')
replace('40229d27', '            "正式路径读取的",\n', '            FUTURES_VARIETY_CALENDAR_SCHEMA,\n')
replace('40229d27', '''existing_df = validate_report_calendar_table(
                existing_dataset.to_table(''', '''existing_df = arrow_to_pandas(
                existing_dataset.to_table(''')
replace('40229d27', '                "现有正式",\n', '                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,\n')
replace_statement('40229d27', 'main', 'expected_full_df = validate_report_calendar_table', '')
for frame in ('scoped_existing_df', 'expected_scope_df'):
    replace('40229d27', f'''for row in pandas_to_arrow(
                {frame}.loc[
                    :, FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names
                ],
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ).to_pylist()''', f'for row in {frame}.to_dict(orient="records")')

cells['dbf59f85'].source = '''## 契约化读取与本环节业务校验

`open_exact_dataset()` 确认 Parquet 存在，打开带 Hive 分区的 Dataset，并检查重建的逻辑物理字段、类型、nullable 与表身份。描述性 metadata 以当前 Schema 为准。函数报告文件数、打开结果与失败阶段，`materialized=false` 表示业务行尚未读取。

实际 `to_table()` 和 `arrow_to_pandas()` 留在入口：每份上游品种日历、既有报告日历各做一次契约转换。它们的主键、年月和已提交业务状态由对应生产者保证，本入口继承这些证明。

`validate_report_calendar_table()` 只接收已完成权威 Arrow 转换的本环节生成结果，执行一次主键、枚举、原因、年月、条数以及采集义务、完成、缺失和质量时间关系校验。其内部直接转为 Pandas 并遍历 Arrow 行，不再重复 cast 或调用契约 validator；固定审计字段和状态集合在行循环前准备。

校验沿原行循环每 10000 行检查一次 2 秒进度间隔，排序完成后报告结果。staging 和正式复读只核对物理契约、行数与完整内容摘要，不再次执行报告日历业务校验。'''
cells['0bbe59a0'].source = '''## 一次分组与完整叶内容差异

`table_digest()` 接收已经按权威类型转换的 Arrow Table，按主键排序，从 Python 标量重建规范缓冲区后对 IPC 字节计算 SHA-256。保留标量重建，以消除 fragment 切片和 null 槽位物理布局差异；摘要内部不再往返 Pandas 或重复转换校验。

`changed_partition_keys()` 对期望与现有 DataFrame 各执行一次分组，得到分区到行位置的索引，并各恢复一次 Arrow 表示。循环仅提取当前叶的行位置并比较摘要，新增、修改和待删除旧叶仍进入变更键列表。

政策未变的行沿用旧 `updated_at`，同一输入再次运行可得到无差异。函数报告分区数、当前键、已比较数和差异数；这些是日历差异，不是事实 API 待办数。'''
cells['20aadb37'].source = '''## 完整叶暂存、内容复读与失败恢复

`commit_partitions()` 接收已经生成并完成业务校验的期望新行，以及显式范围外可信正式旧行组成的完整期望表。调用方负责确定变更键；提交函数固定 Arrow 契约并验收写入内容，不重复执行业务 validator。

1. 在分区循环前转换完整期望表一次、计算完整内容摘要、建立分区行位置与相对路径映射。按变更键汇集行位置，从同一 Arrow 表提取待写数据。
2. staging 写入零行契约标记及变更完整叶。整批数据只读取一次，做物理契约和总行数检查后建立分区索引；逐叶在内存中核对行数和完整内容摘要，不再从 Dataset 逐叶读取。
3. 完整期望表为空时整根替换为零行数据集；其余情况按预计算路径备份并安装叶，期望中已无该叶时只移走旧目录。判断叶是否存在直接查分区索引。
4. 正式安装后保留一次整表物理契约、总行数和完整内容摘要复读。这项检查保证新增、修改、删除和未变历史共同等于完整期望结果；它不重新执行业务校验。

安装与复读阶段仍记 `batch_state=pending`，验收及原有清理成功后才报告提交和日历状态已落盘，`date_watermark=none`。暂存失败清理 staging；安装或验收失败沿原逻辑恢复；恢复失败保留现场。此轮保持本地安装与恢复实现、返回的变更行数以及共同回滚范围。'''
cells['8a21e2d0'].source = cells['8a21e2d0'].source.replace('索引准备、上游转换、格点展开和输出校验', '索引准备、上游行展开、格点生成和输出校验')
cells['8a21e2d0'].source += '\n\n已契约化的输入直接使用 Pandas 记录建立索引和遍历，不再转换为 Arrow 再取 Python 行。各报告类型的固定政策原因在品种日循环前准备。新生成结果只在输出边界做一次 `pandas_to_arrow()`，随后执行一次业务 validator。'
cells['637e05c1'].source += '\n\n显式范围内新行已经通过生成校验，范围外旧行继承正式提交证明，两组日期互斥；合并不再整表复验。计划统计直接读取已有 Pandas 行记录，避免为了构造字典再转换 Arrow。'
diagram_replacements = {
    'a02-b01-flow-validation': [('契约转换；主键与年月检查', '一次契约转换；信任上游已提交语义'), ('契约转换；主键与枚举检查', '自身生成结果：一次业务校验'), ('沿行校验并报告数量；异常记录失败阶段', '逐行校验；既有正式行只契约读取')],
    'a02-b01-flow-build': [('报告生成开始；现有行按主键索引', '直接索引可信现有行；预备固定政策原因'), ('校验后报告生成完成；persisted=false', '输出转换与业务校验各一次；报告完成')],
    'a02-b01-flow-diff': [('报告比较开始；分区键取并集', '两边各一次分组和 Arrow 表示恢复'), ('逐键筛选完整叶；报告已比较分区数', '按分区索引提取当前叶；报告比较进度'), ('非空叶按主键排序并固定 Arrow 表示', '当前 Arrow 叶按主键排序')],
    'a02-b01-flow-commit': [('记录提交开始；期望校验、摘要和路径', '期望转换一次；预备分区索引、摘要和路径'), ('staging 复读；报告逐叶验收数量', 'staging 读取一次；内存分组核对各叶'), ('正式整表业务、行数和摘要验收', '正式整表契约、行数和内容摘要复读一次')],
    'a02-b01-flow-overview': [('正式整表业务校验与摘要核对', '正式整表物理契约与内容摘要核对')],
}
for cell_id, pairs in diagram_replacements.items():
    for old, new in pairs:
        assert old in cells[cell_id].source, (cell_id, old)
        cells[cell_id].source = cells[cell_id].source.replace(old, new)

cells['a02-b01-flow-validation'].source = cells['a02-b01-flow-validation'].source.split('flowchart TD')[0] + '''flowchart TD
    subgraph INPUT["可信正式输入"]
        direction TB
        A["打开 Dataset；检查物理字段与表身份"] --> B["报告打开完成；尚未物化"]
        B --> C["调用方读取数据；执行一次契约转换"]
        C --> D["品种日历与既有报告行：继承已提交业务证明"]
    end
    subgraph OUTPUT["本环节生成结果"]
        direction TB
        E["生成结果经过一次 pandas_to_arrow"] --> F["一次主键、枚举、状态与时间业务校验"]
        F --> G["沿行循环报告进度；排序后返回"]
    end
```'''

assert notebook.metadata == before.metadata
assert [c.id for c in notebook.cells] == [c.id for c in before.cells]
for old in before.cells:
    new = cells[old.id]
    assert new.metadata == old.metadata
    if old.cell_type == 'code':
        assert new.outputs == old.outputs and new.execution_count == old.execution_count
        compile(new.source, str(PATH), 'exec')
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print('a02/b01: repeated validation/conversions removed; partition indices reused; existing recovery preserved.')
