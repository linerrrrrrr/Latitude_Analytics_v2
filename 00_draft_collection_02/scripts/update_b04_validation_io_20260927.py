"""Apply the reviewed b04 validation/conversion and loop-invariant reduction."""

import copy
import io
import pathlib
import tokenize

import nbformat


ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / "02_Futures_Lakehouse/a01_Futures_Market_Data/b04_futures_bar_calendar.ipynb"
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new, count=1):
    source = cells[cell_id].source
    assert source.count(old) == count, (cell_id, old, source.count(old))
    cells[cell_id].source = source.replace(old, new)


replace("c04-imports", 'BAR_FREQUENCIES = {"1d", "1m"}', '''BAR_FREQUENCIES = ("1d", "1m")
PRIMARY_KEY_SCHEMA = pa.schema(
    [FUTURES_BAR_CALENDAR_SCHEMA.field(name) for name in PRIMARY_KEY],
    metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
)
PARQUET_FILE_SCHEMA = pa.schema(
    [
        FUTURES_BAR_CALENDAR_SCHEMA.field(name)
        for name in FUTURES_BAR_CALENDAR_SCHEMA.names
        if name not in PARTITION_COLUMNS
    ],
    metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
)''')

# A full frame and its structural projection share the same business rules;
# choose the conversion schema once instead of converting the projection again.
replace("c04-validation", '''def validate_contract_structure(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(
        frame.loc[:, UPSTREAM_STRUCTURE_COLUMNS],
        UPSTREAM_STRUCTURE_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, UPSTREAM_STRUCTURE_SCHEMA)''', '''def validate_contract_structure(
    frame: pd.DataFrame,
    context: str,
    *,
    schema: pa.Schema = UPSTREAM_STRUCTURE_SCHEMA,
) -> pd.DataFrame:
    """按所需投影或完整契约转换一次，再检查共用的 Session 结构规则。"""
    table = pandas_to_arrow(frame.loc[:, schema.names], schema)
    checked_df = arrow_to_pandas(table, schema)''')
replace("c04-validation", '''    table = pandas_to_arrow(
        frame.loc[:, FUTURES_CONTRACT_CALENDAR_SCHEMA.names],
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, FUTURES_CONTRACT_CALENDAR_SCHEMA)
    validate_contract_structure(checked_df, context)
    return checked_df.sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)''', '''    return validate_contract_structure(
        frame, context, schema=FUTURES_CONTRACT_CALENDAR_SCHEMA
    )''')
replace("c04-validation", '''def validate_structural_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    table = pandas_to_arrow(frame.loc[:, STRUCTURAL_COLUMNS], STRUCTURAL_SCHEMA)
    checked_df = arrow_to_pandas(table, STRUCTURAL_SCHEMA)''', '''def validate_structural_frame(
    frame: pd.DataFrame,
    context: str,
    *,
    schema: pa.Schema = STRUCTURAL_SCHEMA,
) -> pd.DataFrame:
    """按结构投影或完整契约转换一次；两种调用均执行全部结构规则。"""
    table = pandas_to_arrow(frame.loc[:, schema.names], schema)
    checked_df = arrow_to_pandas(table, schema)''')
replace("c04-validation", '''    table = pandas_to_arrow(
        frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    checked_df = arrow_to_pandas(table, FUTURES_BAR_CALENDAR_SCHEMA)
    checked_df = checked_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
    validate_structural_frame(checked_df, context)''', '''    checked_df = validate_structural_frame(
        frame, context, schema=FUTURES_BAR_CALENDAR_SCHEMA
    )''')
replace("c04-validation", '''    expected_df = validate_structural_frame(expected_frame, "当前上游结构")''', '''    """期望结构由生成函数校验；此处检查现有结构并计算双向差异。"""
    expected_df = expected_frame.loc[:, STRUCTURAL_COLUMNS]''')
replace("c04-validation", '''    expected_table = pandas_to_arrow(expected_df, STRUCTURAL_SCHEMA)
    existing_table = pandas_to_arrow(existing_df, STRUCTURAL_SCHEMA)

    if expected_table.equals(existing_table) and quality_error is None:''', '''    if expected_df.equals(existing_df) and quality_error is None:''')

replace("c04-merge", '''    merged_table = pandas_to_arrow(
        merged_df.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    return arrow_to_pandas(
        merged_table, FUTURES_BAR_CALENDAR_SCHEMA
    ).sort_values(PRIMARY_KEY).reset_index(drop=True)''', '''    return merged_df.loc[
        :, FUTURES_BAR_CALENDAR_SCHEMA.names
    ].sort_values(PRIMARY_KEY).reset_index(drop=True)''')
replace("c04-merge", '''    checked_existing_df = arrow_to_pandas(
        pandas_to_arrow(
            existing_frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        ),
        FUTURES_BAR_CALENDAR_SCHEMA,
    )
    desired_df = merge_preserving_state(
        fresh_frame,
        checked_existing_df,
    )''', '''    desired_df = merge_preserving_state(fresh_frame, existing_frame)''')
replace("c04-merge", 'checked_existing_df.loc[:, STRUCTURAL_COLUMNS]', 'existing_frame.loc[:, STRUCTURAL_COLUMNS]')

replace("c04-commit", '''def parquet_file_schema() -> pa.Schema:
    return pa.schema(
        [
            FUTURES_BAR_CALENDAR_SCHEMA.field(name)
            for name in FUTURES_BAR_CALENDAR_SCHEMA.names
            if name not in PARTITION_COLUMNS
        ],
        metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
    )


''', '')
replace("c04-commit", '    expected_schema = parquet_file_schema()\n', '')
replace("c04-commit", 'actual_schema.equals(expected_schema, check_metadata=True)', 'actual_schema.equals(PARQUET_FILE_SCHEMA, check_metadata=True)')
replace("c04-commit", '''        primary_key_schema = pa.schema(
            [
                FUTURES_BAR_CALENDAR_SCHEMA.field(name)
                for name in PRIMARY_KEY
            ],
            metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
        )
''', '')
replace("c04-commit", 'primary_key_schema', 'PRIMARY_KEY_SCHEMA', count=3)
replace("c04-commit", '            expected_file_schema = parquet_file_schema()\n', '')
replace("c04-commit", 'expected_file_schema', 'PARQUET_FILE_SCHEMA', count=3)

replace("c04-cli", 'sorted(BAR_FREQUENCIES)', 'BAR_FREQUENCIES', count=4)
replace("c04-cli", '    exchange_codes = set(upstream_months_by_exchange)', '    exchange_codes = sorted(upstream_months_by_exchange)')
replace("c04-cli", '        for exchange_code in sorted(exchange_codes):', '        for exchange_code in exchange_codes:')
replace("c04-cli", '''    # 同一基础分区重新投影正式 b03 结构，逐分区构造完整 b04 目标并原子提交。
    for base_key in planned_base_keys:''', '''    initial_state_by_frequency = {
        frequency: initial_state(frequency, run_updated_at)
        for frequency in BAR_FREQUENCIES
    }
    # 同一基础分区重新投影正式 b03 结构，逐分区构造完整 b04 目标并原子提交。
    for base_key in planned_base_keys:''')
replace("c04-cli", '''            for column, value in initial_state(
                frequency, run_updated_at
            ).items():''', '''            for column, value in initial_state_by_frequency[frequency].items():''')

replace("c04-validation-notes", '本节描述当前校验边界，本轮不删减校验或转换。', '完整输入校验直接指定完整 Schema，复用结构校验中的一次类型转换和排序，不再将同一份结构投影重复转换。比较函数要求期望结构已经由生成函数校验，复用其 Arrow 扩展类型；空值安全的相等比较直接使用 DataFrame，不再为比较转成两份 Arrow Table。现有结构的审计和 dirty 完整叶的最终校验继续保留。')
replace("c04-merge-notes", '`assess_partition()` 合并状态并返回结构审计结果。', '`assess_partition()` 将输入直接交给 `merge_preserving_state()`，由后者各规范化一次新旧完整表，再合并状态并返回结构审计结果。状态继承仅从已经固定类型的新旧列逐值选择，结果保留这些类型，不再整表往返转换。')
replace("c04-imports-notes", cells['c04-imports-notes'].source, cells['c04-imports-notes'].source + '\n\n频率按 `1d`、`1m` 固定顺序复用；主键投影 Schema 与去除 Hive 字段的物理文件 Schema 在初始化时由权威 Schema 派生一次，分区提交直接复用。')
replace("c04-cli-notes", '写入时按基础分区重新生成结构、补初始状态，再精确读取当前 dirty 叶并继承状态。', '写入前按两种频率各生成一次本批初始状态（共用 `run_updated_at`）；写入时按基础分区重新生成结构、填入相应初始状态，再精确读取当前 dirty 叶并继承状态。')
replace("b04-flow-validation", '比较期望结构与现有结构', '复用已校验期望结构；检查现有结构并比较')
replace("b04-flow-merge", '形成期望完整结果；固定 Schema 并排序', '形成期望完整结果；保留列类型并排序')
replace("b04-flow-main", '重建 dirty 范围；复读当前完整叶并继承状态', '复用本批初始状态；重建 dirty 范围并继承当前叶状态')

assert notebook.metadata == before.metadata
for original, current in zip(before.cells, notebook.cells, strict=True):
    assert original.id == current.id and original.metadata == current.metadata
    if original.cell_type == 'code':
        assert original.outputs == current.outputs
        assert original.execution_count == current.execution_count
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        assert comments(original.source) == comments(current.source), original.id
        compile(current.source, f'{PATH}:{current.id}', 'exec')
nbformat.validate(notebook)
nbformat.write(notebook, PATH)
print(PATH)
