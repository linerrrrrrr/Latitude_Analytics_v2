"""a03/b01 第 7—9 项：可信输入、单次业务校验、一次分组与转换复用。"""
import ast
import copy
import pathlib
import hashlib
import json
import shutil
import tempfile
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a03_External_Market_Data/b01_external_market_calendar.ipynb'
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


RELATIVE = PATH.relative_to(ROOT)
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b01-validation-before-'))
hashes = {}
for path in [ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md', ROOT/'02_Futures_Lakehouse/README.md', ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py', *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')), *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')), *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py')), ROOT/'00_draft_collection_02/tests/test_b03_metadata_upgrade.py',ROOT/'00_draft_collection_02/tests/test_a03_b01_shared_transaction.py']:
    relative=path.relative_to(ROOT)
    hashes[relative.as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
    if path in (PATH,PATH.with_suffix('.py'),ROOT/'02_Futures_Lakehouse/AGENTS.md',ROOT/'02_Futures_Lakehouse/README.md') or path.parent.name=='tests':
        destination=snapshot/relative
        destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(path,destination)
(snapshot/'hashes.json').write_text(json.dumps(hashes,ensure_ascii=False,indent=2),encoding='utf8')
print(snapshot,flush=True)

replace('44837881', 'from datetime import date, datetime, timedelta, timezone', 'from datetime import date, datetime, timezone')
replace_statement('49bfe04b','dataset_has_exact_schema_metadata','def dataset_has_exact_schema_metadata','')
replace_statement('49bfe04b','partition_expression','def partition_expression','')
replace('49bfe04b', '        expected_names = set(schema.names)\n', block(8, '''
expected_names = set(schema.names)
actual_schema = reconstructed_schema(dataset, schema)
identity_metadata_keys = (b"table_name", b"primary_key", b"partition_columns")
'''))
replace('49bfe04b', '                reconstructed_schema(dataset, schema),', '                actual_schema,')
replace('49bfe04b', block(8, '''
expected_file_schema = pa.schema([
    field for field in schema if field.name not in partition_columns
])
'''), block(8, '''
if any(
    (dataset.schema.metadata or {}).get(key) != schema.metadata[key]
    for key in identity_metadata_keys
):
    raise TypeError(f"{label}表名、主键或分区 metadata 与权威契约不兼容。")
expected_file_schema = pa.schema(
    [field for field in schema if field.name not in partition_columns],
    metadata=schema.metadata,
)
is_exact = actual_schema.equals(schema, check_metadata=True)
'''))
replace('49bfe04b','            if not physical_schema_matches(\n                pa.schema(list(fragment.physical_schema)),','            fragment_schema = fragment.physical_schema\n            if not physical_schema_matches(\n                fragment_schema,')
replace('49bfe04b', '            log_checked_fragments += 1\n', block(12, '''
if any(
    (fragment_schema.metadata or {}).get(key) != schema.metadata[key]
    for key in identity_metadata_keys
):
    raise TypeError(f"{label}存在表身份 metadata 不兼容的 fragment：{fragment.path}")
is_exact = is_exact and fragment_schema.equals(expected_file_schema, check_metadata=True)
log_checked_fragments += 1
'''))
replace_statement('49bfe04b','open_compatible_dataset','is_exact = dataset_has_exact_schema_metadata','')
replace('49bfe04b','        # staging、上游与提交后输出必须逐 fragment 精确匹配当前 metadata。','        # staging 与提交后输出必须逐 fragment 精确匹配当前 metadata。')

cells['a03-b01-upstream-validation'].source = '# 上游物化与权威转换直接在 main() 执行，信任 a01/b01 的正式业务证明。'
replace('a03-b01-output-validation','    *,\n    allow_legacy_domestic_state: bool = False,\n','')
replace('a03-b01-output-validation', block(8, '''
# Arrow 转换固定字段顺序、类型、nullable 和全部中文 metadata。
log_phase = "convert"
checked = validate_arrow_table(table, EXTERNAL_MARKET_CALENDAR_SCHEMA)
frame = arrow_to_pandas(checked, EXTERNAL_MARKET_CALENDAR_SCHEMA)
'''), block(8, '''
# 调用方已完成权威 Arrow 转换；此处只验证外部日历业务语义。
log_phase = "convert"
frame = table.to_pandas(types_mapper=pd.ArrowDtype)
'''))
replace('a03-b01-output-validation','        for row in checked.to_pylist():', block(8, '''
audit_fields = ("fetch_run_id", "fetch_completed_at", "quality_checked_at")
completed_statuses = {"success", "empty_confirmed"}
checked_quality_statuses = {"passed", "warning", "failed"}
for row in table.to_pylist():
''').rstrip())
replace('a03-b01-output-validation', '''for name in [
                        "fetch_run_id",
                        "fetch_completed_at",
                        "quality_checked_at",
                    ]''', 'for name in audit_fields')
replace('a03-b01-output-validation', '''row["fetch_result_status"] in {
                "success",
                "empty_confirmed",
            }''', 'row["fetch_result_status"] in completed_statuses')
replace('a03-b01-output-validation','row["quality_status"] in {"passed", "warning", "failed"}','row["quality_status"] in checked_quality_statuses')
replace('a03-b01-output-validation','                    not allow_legacy_domestic_state\n                    and row["fetch_result_status"]', '                    row["fetch_result_status"]')
replace('a03-b01-output-validation','                not allow_legacy_domestic_state\n                and row["dataset_name"]','                row["dataset_name"]')

replace('74b6ae55','# 现有行已经通过本表完整校验，可以按权威主键安全索引。','# 现有正式行的业务语义由提交保证，可按权威主键索引并继承状态。')
replace_statement('74b6ae55','build_expected_calendar','existing_rows_by_key =', '''
existing_rows_by_key = {
    tuple(row[name] for name in PRIMARY_KEY): row
    for row in existing_df.to_dict(orient="records")
}
''')
replace_statement('74b6ae55','build_expected_calendar','upstream_rows =', '''
upstream_rows = upstream_df.to_dict(orient="records")
config_reason_prefix = f"外部市场请求实体配置 v{EXTERNAL_MARKET_ENTITY_CONFIG_VERSION}；"
index_required_reasons = {
    entity.source_indicator_id: (
        f"普通工作日，按 INDICATOR_ID={entity.source_indicator_id} 请求 Eastmoney 指数。"
    )
    for entity in EXTERNAL_INDEX_ENTITIES
}
''')
replace('74b6ae55','log_phase = "upstream_conversion"','log_phase = "upstream_records"')
replace_statement('74b6ae55','build_expected_calendar',"reason = f'普通工作日", 'reason = index_required_reasons[entity.source_indicator_id]')
replace_statement('74b6ae55','build_expected_calendar','requirement_reason =', 'requirement_reason = config_reason_prefix + selection_reason')
replace_statement('74b6ae55','build_expected_calendar','expected_calendar_df =', '''
expected_calendar_table = pandas_to_arrow(candidate_df, EXTERNAL_MARKET_CALENDAR_SCHEMA)
expected_calendar_df = validate_external_calendar_table(expected_calendar_table, "期望")
''')

replace('a177c572','def table_digest(frame: pd.DataFrame) -> str:','def table_digest(calendar_table: pa.Table) -> str:')
replace_statement('a177c572','table_digest','ordered_df =','ordered_table = calendar_table.sort_by([(name, "ascending") for name in PRIMARY_KEY])')
replace_statement('a177c572','table_digest','source_table =','')
replace('a177c572','source_table.to_pylist()','ordered_table.to_pylist()')

replace_statement('a03-b01-difference-plan','changed_partition_keys','expected_keys =','''
expected_indices_by_partition = expected_df.groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
existing_indices_by_partition = existing_df.groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
partition_keys = sorted(expected_indices_by_partition.keys() | existing_indices_by_partition.keys())
expected_calendar_table = pa.Table.from_pandas(
    expected_df, schema=EXTERNAL_MARKET_CALENDAR_SCHEMA, preserve_index=False,
)
existing_calendar_table = pa.Table.from_pandas(
    existing_df, schema=EXTERNAL_MARKET_CALENDAR_SCHEMA, preserve_index=False,
)
''')
replace_statement('a03-b01-difference-plan','changed_partition_keys','existing_keys =','')
replace('a03-b01-difference-plan','len(expected_keys | existing_keys)','len(partition_keys)')
replace('a03-b01-difference-plan','for partition_key in sorted(expected_keys | existing_keys):','for partition_key in partition_keys:')
s=cells['a03-b01-difference-plan'].source
start=s.index('            expected_mask =')
end=s.index('            if expected_digest !=',start)
s=s[:start]+block(12, '''
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

''')+'\n'+s[end:]
cells['a03-b01-difference-plan'].source=s

replace('018ccc71','    log_selected_partitions = 0\n','')
replace('018ccc71','    log_last_progress_at = log_started_at\n','')
replace('018ccc71','# 提交前先验证完整期望表，保证删除旧行也来自合法的上游当前真值。','# 新期望已通过生成校验；范围外旧行继承正式提交证明，此处验收写入内容。')
replace('018ccc71','log_phase = "input_validation"','log_phase = "input_conversion"')
replace_statement('018ccc71','commit_partitions','expected_df = validate_external_calendar_table','''
expected_df = expected_df.sort_values(PRIMARY_KEY).reset_index(drop=True)
calendar_columns = EXTERNAL_MARKET_CALENDAR_SCHEMA.names
expected_calendar_table = pandas_to_arrow(expected_df, EXTERNAL_MARKET_CALENDAR_SCHEMA)
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
replace('018ccc71','expected_digest = table_digest(expected_df)','expected_digest = table_digest(expected_calendar_table)')
s=cells['018ccc71'].source
start=s.index('            changed_row_mask =')
end=s.index('            # metadata 过期时 staging',start)
s=s[:start]+block(12, '''
changed_indices = sorted(
    index
    for partition_key in partition_keys
    for index in expected_indices_by_partition.get(partition_key, ())
)
''')+'\n'+s[end:]
cells['018ccc71'].source=s
replace_statement('018ccc71','commit_partitions','changed_rows_df =','''
changed_calendar_table = (
    expected_calendar_table if force_full_swap
    else expected_calendar_table.take(pa.array(changed_indices, type=pa.int64()))
)
''')
replace('018ccc71','if not changed_rows_df.empty:', 'if changed_calendar_table.num_rows:')
replace('018ccc71',block(20, '''
pandas_to_arrow(
    changed_rows_df.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
),
'''),block(20,'changed_calendar_table,'))
cells['018ccc71'].source=cells['018ccc71'].source.replace('len(changed_rows_df)','changed_calendar_table.num_rows')
replace_statement('018ccc71','commit_partitions','staged_df =','''
staged_calendar_table = validate_arrow_table(
    staged_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
)
staged_indices_by_partition = staged_calendar_table.select(PARTITION_COLUMNS).to_pandas().groupby(
    PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
''')
cells['018ccc71'].source=cells['018ccc71'].source.replace('len(staged_df)','staged_calendar_table.num_rows').replace('table_digest(staged_df)','table_digest(staged_calendar_table)')
s=cells['018ccc71'].source
start=s.index('                expected_mask =')
end=s.index('                log_partition =',start)
s=s[:start]+block(16, '''
expected_indices = expected_indices_by_partition.get(partition_key)
expected_partition_table = (
    expected_calendar_table.take(expected_indices)
    if expected_indices is not None else empty_calendar_table
)
''')+s[end:]
cells['018ccc71'].source=s
replace_statement('018ccc71','commit_partitions','staged_partition_table =','''
staged_indices = staged_indices_by_partition.get(partition_key)
staged_partition_table = (
    staged_calendar_table.take(staged_indices)
    if staged_indices is not None else empty_calendar_table
)
''')
replace_statement('018ccc71','commit_partitions','staged_partition_df =','')
cells['018ccc71'].source=cells['018ccc71'].source.replace('len(expected_partition_df)','expected_partition_table.num_rows').replace('len(staged_partition_df)','staged_partition_table.num_rows').replace('not expected_partition_df.empty','expected_partition_table.num_rows').replace('table_digest(staged_partition_df)','table_digest(staged_partition_table)').replace('table_digest(expected_partition_df)','table_digest(expected_partition_table)')
replace_statement('018ccc71','commit_partitions','relative_path =','relative_path = partition_relative_paths[partition_key]')
replace_statement('018ccc71','commit_partitions','expected_mask =','')
replace_statement('018ccc71','commit_partitions','for column, value in zip(PARTITION_COLUMNS, partition_key','')
replace_statement('018ccc71','commit_partitions','should_exist =','should_exist = partition_key in expected_indices_by_partition')
replace_statement('018ccc71','commit_partitions','committed_df =','''
committed_calendar_table = validate_arrow_table(
    committed_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
)
''')
cells['018ccc71'].source=cells['018ccc71'].source.replace('len(committed_df)','committed_calendar_table.num_rows').replace('table_digest(committed_df)','table_digest(committed_calendar_table)')

replace_statement('ebf489c1','main','upstream_dataset = open_exact_dataset','''
upstream_dataset, _ = open_compatible_dataset(
    upstream_path, UPSTREAM_PARTITIONING, TRADE_CALENDAR_SCHEMA,
    UPSTREAM_PARTITION_COLUMNS, "正式中国自然日历",
)
''')
replace_statement('ebf489c1','main','upstream_df = validate_upstream_table','''
upstream_df = arrow_to_pandas(
    upstream_dataset.to_table(columns=TRADE_CALENDAR_SCHEMA.names, filter=upstream_filter),
    TRADE_CALENDAR_SCHEMA,
).sort_values(UPSTREAM_PRIMARY_KEY).reset_index(drop=True)
''')
replace_statement('ebf489c1','main','existing_df = validate_external_calendar_table','''
existing_df = arrow_to_pandas(
    existing_dataset.to_table(columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names),
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
)
''')
replace_statement('ebf489c1','main','expected_full_df = validate_external_calendar_table','')
for frame_name in ('scoped_existing_df','expected_scope_df'):
    old=f'''for row in pandas_to_arrow(
                {frame_name}.loc[:, EXTERNAL_MARKET_CALENDAR_SCHEMA.names],
                EXTERNAL_MARKET_CALENDAR_SCHEMA,
            ).to_pylist()'''
    replace('ebf489c1',old,f'for row in {frame_name}.to_dict(orient="records")')
replace('ebf489c1','            committed_rows = commit_partitions(','            commit_partitions(')

# Notebook 说明随实现更新，不保留已完成任务的待办描述。
cells['2d5c68c2'].source = '''## Dataset 物理兼容与精确 metadata 读取

`open_compatible_dataset()` 枚举文件后，只遍历一次 fragments，同时检查字段、类型、nullable 和表名/主键/分区身份，并累计 metadata 是否精确匹配。Schema 汇总重建、身份键和文件 Schema 都在 fragment 循环前准备。

上游 `dim_trade_calendar` 接受物理与身份兼容的描述性 metadata 差异；其已提交业务语义由 a01/b01 保证。已有外部日历仍允许物理兼容但 metadata 过期，以维持既有整根迁移规则。`open_exact_dataset()` 用于 staging 和正式安装后的精确验收。

两个打开函数自行报告文件数、fragment 检查及失败阶段；`materialized=false` 表示尚未读取记录。实际物化日志仍由调用处报告。'''
cells['a03-b01-upstream-heading'].source = '''## 信任上游正式业务证明

自然日历主键、日期连续性、year、weekday 和周末关系由 a01/b01 提交时保证，本环节不再逐项复算。`main()` 直接物化选择范围、调用一次 `arrow_to_pandas()` 固定契约表示并排序，报告实际读取行数；不另设只包装转换的校验函数。'''
cells['a03-b01-flow-upstream'].source = '''### 局部流程：可信上游读取

```mermaid
flowchart TD
    A["物理与身份兼容的正式自然日历"] --> B["main 按范围物化；一次权威转换"]
    B --> C["排序并报告读取行数；信任上游业务证明"]
```'''
cells['a03-b01-output-heading'].source = '''## 生成结果的一次完整业务验收

`build_expected_calendar()` 先执行一次 `pandas_to_arrow()`，再由 `validate_external_calendar_table()` 检查主键、请求实体、选择/完成状态、计数和时间。校验函数只消费已经契约化的 Arrow 表，不再重复转换。审计字段和状态集合在逐行循环前建立，进度仍沿原循环报告。

生意社新完成结果必须为 `success + passed`、数量 1 且不缺失；其他来源保留确认空和 warning 语义。历史旧生意社行只作为可信已有行读取，规则原因变化后由生成逻辑重置；不再给新输出验收设置历史宽容开关。范围外旧行沿用正式证明，显式范围合并不再重跑全表业务校验。'''
cells['a03-b01-flow-output'].source = '''### 局部流程：生成结果业务校验

```mermaid
flowchart TD
    A["生成函数完成一次权威 Arrow 转换"] --> B["一次主键、枚举、实体与状态自洽检查"]
    B --> C["一次计数与审计时间检查；沿行报告进度"]
    C --> D["严格生意社 raw 完成规则；排序返回"]
```'''
cells['3f56c68e'].source = cells['3f56c68e'].source.replace('索引、上游转换、格点展开','索引、上游记录准备、格点展开') + '\n\n已有行和上游行直接使用当前 DataFrame 的记录表示，不为建立 Python 字典往返 Arrow。配置版本前缀与各指数固定请求原因在日期循环前准备。'
cells['6a91b06b'].source = '''## 完整内容摘要

`table_digest()` 直接接收已按权威 Schema 转换的 Arrow 表，按主键排序后仍从 Python 标量重建缓冲区，消除 fragment 切片布局差异，再计算完整 IPC SHA-256。摘要包括状态和更新时间；保留原摘要格式，不改成只比较行数或主键。调用方在分区循环前转换整表，逐叶只取行。'''
cells['a03-b01-difference-heading'].source = '''## 完整叶差异计划

`changed_partition_keys()` 对期望和已有 DataFrame 各分组一次，保存每个分区的行位置，并各建立一次 Arrow 表。分区循环只按位置取行和比较完整内容摘要，不再为每个键创建全表布尔掩码。缺失一侧仍记为 None，保留新增、修订和删除语义；比较进度与返回排序不变。'''
cells['068d0136'].source = cells['068d0136'].source.replace('先验收完整期望表并计算摘要，再暂存变化叶','接收已验证的新期望和可信范围外旧行，排序并转换完整期望一次、计算摘要，再暂存变化叶').replace('现有 staging 完整及逐叶业务校验、行数与内容摘要核对继续保留。','staging 只物化一次，检查物理契约与总行数，再在内存按分区索引核对每叶行数和完整内容摘要；metadata 整根迁移另保留完整表摘要核对。').replace('核对完整业务约束、总行数和完整内容摘要','核对物理契约、总行数和完整内容摘要').replace('输入校验、staging 写入/复读','输入转换、staging 写入/复读') + '\n\n期望分组索引、权威列清单、空 Arrow 表和各分区相对路径在分区循环前准备并复用；安装时通过索引判断替换或显式删除。正式整表仍在全部安装后复读一次，并非逐叶重复读取。生成后、提交前、staging 和正式安装后不再重复执行同一业务行校验。'
cells['7150a5fe'].source = cells['7150a5fe'].source.replace('旧生意社状态仅允许在历史读取分支宽容接纳，新期望必须满足当前 raw 语义','已有正式状态直接按契约读取，旧生意社语义通过规则变化重置，新期望仍必须满足当前 raw 语义')
for cell_id,pairs in {
    'a03-b01-flow-read':[('核对汇总与每个 fragment 物理结构','一次检查汇总和 fragments 的物理结构与表身份'),('比较当前 metadata','同一遍历累计 metadata 精确匹配结果')],
    'a03-b01-flow-digest':[('按主键排序完整权威行','已转换 Arrow 表按主键排序'),('转换 Arrow；按标量重建统一缓冲区','保留标量重建；统一缓冲区')],
    'a03-b01-flow-difference':[('报告比较开始；候选分区键并集','各分组和转换一次；候选键并集'),('逐键比较并报告已比较/变化分区数','逐键按索引取行；比较摘要并报告进度')],
    'a03-b01-flow-commit':[('报告提交开始；完整期望验收、摘要和路径','报告提交开始；一次转换、分组、摘要和路径'),('staging 完整及逐叶复读；报告数量','一次物化 staging；内存逐叶核对并报告数量'),('本函数正式整表验收；行数与完整内容摘要一致','一次正式整表物理验收；行数与完整内容摘要一致')],
}.items():
    for old_text,new_text in pairs:
        replace(cell_id,old_text,new_text)

assert notebook.metadata==before.metadata
assert [c.id for c in notebook.cells]==[c.id for c in before.cells]
for old in before.cells:
    new=cells[old.id]
    assert {k:v for k,v in old.items() if k!='source'}=={k:v for k,v in new.items() if k!='source'}
    if new.cell_type=='code':
        compile(new.source,str(PATH),'exec')
nbformat.validate(notebook)
with PATH.open('w',encoding='utf8',newline='\n') as handle:
    nbformat.write(notebook,handle)

old_text='staging 的完整及逐叶业务验收继续保留，正式整表业务校验、总行数和完整内容摘要在事务内复读；'
new_text='生成结果执行一次完整业务校验；期望按分区分组和转换后复用，staging 只物化一次并在内存逐叶核对，正式整表物理契约、总行数和完整内容摘要在事务内复读一次；'
for relative in ('02_Futures_Lakehouse/AGENTS.md','02_Futures_Lakehouse/README.md'):
    path=ROOT/relative
    source=path.read_text(encoding='utf8')
    assert source.count(old_text)==1
    path.write_text(source.replace(old_text,new_text),encoding='utf8',newline='\n')
print('a03/b01 validation, conversion and partition I/O updated.')
