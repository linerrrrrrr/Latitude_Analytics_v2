"""a03/b03 第 7—9 项：可信历史、叶级复读、证据复用和循环不变量。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile
import textwrap

import nbformat

ROOT=pathlib.Path(__file__).resolve().parents[2]
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb')
PATH=ROOT/RELATIVE
notebook=nbformat.read(PATH,as_version=4)
before=copy.deepcopy(notebook)
cells={c.id:c for c in notebook.cells}
snapshot=pathlib.Path(tempfile.mkdtemp(prefix='a03-b03-validation-before-'))
selected=[RELATIVE,RELATIVE.with_suffix('.py'),pathlib.Path('02_Futures_Lakehouse/AGENTS.md'),pathlib.Path('02_Futures_Lakehouse/README.md'),
          pathlib.Path('00_draft_collection_02/tests/test_b03_metadata_upgrade.py'),pathlib.Path('00_draft_collection_02/tests/test_a03_b03_function_logs.py')]
paths=[ROOT/'AGENTS.md',ROOT/'03_Futures_Database/AGENTS.md',ROOT/'config/data_contracts.py',*(ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb'),*(ROOT/'02_Futures_Lakehouse').glob('a*/*.py'),*(ROOT/relative for relative in selected)]
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},ensure_ascii=False,indent=2),encoding='utf8')
for relative in selected:
    destination=snapshot/relative
    destination.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(ROOT/relative,destination)

def replace(cell_id,old,new,count=1):
    assert cells[cell_id].source.count(old)==count,(cell_id,old,cells[cell_id].source.count(old))
    cells[cell_id].source=cells[cell_id].source.replace(old,new)

def statement(cell_id,function,prefix,replacement):
    cell=cells[cell_id]
    fn=next(n for n in ast.parse(cell.source).body if isinstance(n,ast.FunctionDef) and n.name==function)
    found=[n for n in ast.walk(fn) if isinstance(n,ast.stmt) and ast.unparse(n).startswith(prefix)]
    assert len(found)==1,(function,prefix,len(found))
    node=found[0]
    lines=cell.source.splitlines(keepends=True)
    lines[node.lineno-1:node.end_lineno]=[textwrap.indent(textwrap.dedent(replacement).strip()+'\n',' '*node.col_offset)] if replacement else []
    cell.source=''.join(lines)

# 重用上一环节已验证的物理/身份读取实现；不定义共享薄包装。
raw_notebook=nbformat.read(ROOT/'02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.ipynb',as_version=4)
raw_code='\n\n'.join(c.source for c in raw_notebook.cells if c.cell_type=='code')
raw_tree=ast.parse(raw_code)
reader='\n\n'.join(ast.get_source_segment(raw_code,n) for n in raw_tree.body if isinstance(n,ast.FunctionDef) and n.name in ('reconstructed_schema','open_exact_dataset'))
reader=reader.replace('if table_path.is_dir()\n            else []', 'if table_path.is_dir()\n            else [table_path] if table_path.is_file() else []')
assert '[table_path] if table_path.is_file()' in reader
cells['b03-c03-09'].source=reader+'\n'

# pandas_to_arrow 已经执行完整 Arrow 契约检查；移除紧接着重复的外层调用。
for cell_id,name in (('a03-b03-calendar-validation','validate_calendar_frame'),('a03-b03-fact-validation','validate_overseas_futures_frame')):
    fn=next(n for n in ast.parse(cells[cell_id].source).body if isinstance(n,ast.FunctionDef))
    checked=next(n for n in ast.walk(fn) if isinstance(n,ast.Assign) and ast.unparse(n).startswith('checked ='))
    assert checked.value.func.id=='validate_arrow_table' and checked.value.args[0].func.id=='pandas_to_arrow'
    statement(cell_id,name,'checked =','checked = '+ast.unparse(checked.value.args[0]))

# 日历 dirty 叶直接消费已转换的 Arrow；不再往返完整 Pandas 表。
cell='a03-b03-calendar-validation'
statement(cell,'validate_calendar_frame','checked =','')
statement(cell,'validate_calendar_frame','normalized =','calendar_keys_df = calendar_table.select(CALENDAR_PRIMARY_KEY).to_pandas()')
replace(cell,'normalized.duplicated(CALENDAR_PRIMARY_KEY)','calendar_keys_df.duplicated()')
replace(cell,'for row in checked.to_pylist():','for row in calendar_table.to_pylist():')
statement(cell,'validate_calendar_frame','now_utc =','''now_utc = datetime.now(timezone.utc)
completed_fetch_statuses = {"success", "empty_confirmed"}
checked_quality_statuses = {"passed", "warning", "failed"}
calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]''')
statement(cell,'validate_calendar_frame','completed_status =','completed_status = row["fetch_result_status"] in completed_fetch_statuses')
replace(cell,'row["quality_status"] in {"passed", "warning", "failed"}','row["quality_status"] in checked_quality_statuses')
statement(cell,'validate_calendar_frame','validated_calendar_df =','validated_calendar_table = calendar_table.sort_by(calendar_sort_keys)')
replace(cell,'    frame: pd.DataFrame,\n    context: str,\n) -> pd.DataFrame:','    calendar_table: pa.Table,\n    context: str,\n) -> pa.Table:')
cells[cell].source=cells[cell].source.replace('validate_calendar_frame','validate_calendar_table').replace('len(frame)','calendar_table.num_rows').replace('validated_calendar_df','validated_calendar_table').replace('log_phase = "conversion"','log_phase = "primary_key"')

# 事实转换前非有限数门禁必须保留，防止 NaN 被 Pandas→Arrow 转成 null。
# 转换后的业务检查也保留；仅将各行重复建立的固定列清单提前。
cell='a03-b03-fact-validation'
statement(cell,'validate_overseas_futures_frame','now_utc =','now_utc = datetime.now(timezone.utc)\nprice_columns = ("open", "high", "low", "close", "previous_close")')
replace(cell,'for field_name in ["open", "high", "low", "close", "previous_close"]:', 'for field_name in price_columns:')

# 可信、带 Arrow dtype 的事实已有列契约，旁证直接读取标量记录，避免再转 Arrow。
statement('a03-b03-quality','ohlc_relation_warning_map','checked_rows =','checked_rows = frame.loc[:, OVERSEAS_FUTURES_DAILY_SCHEMA.names].to_dict("records")')
statement('a03-b03-quality','ohlc_relation_warning_map','details_by_date:','details_by_date: dict[date, list[str]] = {}\nrelation_price_columns = ("open", "close")')
replace('a03-b03-quality','for field_name in ["open", "close"]:', 'for field_name in relation_price_columns:')

statement('a03-b03-read-fact','read_optional_fact','validated_fact_df =','validated_fact_df = arrow_to_pandas(table, OVERSEAS_FUTURES_DAILY_SCHEMA).sort_values(PRIMARY_KEY).reset_index(drop=True)')
replace('a03-b03-read-fact','# 空事实目录是合法的全量起点；已有事实则必须精确符合新契约。','# 空事实目录是合法的全量起点；已有正式事实只检查物理契约并信任业务证明。')

# 单次规划同时返回已计算的事实证据，供状态修复与日期失败回写复用。
replace('b03-c03-13',') -> tuple[pd.DataFrame, pd.DataFrame, int]:',') -> tuple[pd.DataFrame, pd.DataFrame, int, dict[date, int], dict[date, str]]:')
statement('b03-c03-13','plan_overseas_futures_grids','calendar_rows =','calendar_rows = relevant_df.to_dict("records")')
statement('b03-c03-13','plan_overseas_futures_grids','return (pending_df, state_repair_df, complete_count)','return pending_df, state_repair_df, complete_count, fact_counts, fact_warning_by_date')

statement('a03-b03-merge','full_fact_partition','merged_fact_df =','')
cells['a03-b03-merge'].source=cells['a03-b03-merge'].source.replace('len(merged_fact_df)','len(complete_df)').replace('return merged_fact_df','return complete_df').replace('log_phase = "output_validation"','log_phase = "merge_ready"')

# 描述性 metadata 不再触发全历史改写；保留实际使用的分区表达式。
src=cells['b03-c03-15'].source
cells['b03-c03-15'].source=src[:src.index('# 物理兼容但 metadata 过期的事实必须')].rstrip()+'\n'

# 事实 staging 只含本次一个叶及空标记，保留现有写法；去掉重复业务验收。
cell='a03-b03-fact-commit'
statement(cell,'commit_complete_fact_partition','complete_table = pandas_to_arrow','''complete_table = pandas_to_arrow(complete_df, OVERSEAS_FUTURES_DAILY_SCHEMA)
fact_columns = OVERSEAS_FUTURES_DAILY_SCHEMA.names
fact_sort_keys = [(name, "ascending") for name in PRIMARY_KEY]''')
statement(cell,'commit_complete_fact_partition','staged_df =','staged_table = validate_arrow_table(staged_table, OVERSEAS_FUTURES_DAILY_SCHEMA).sort_by(fact_sort_keys)')
statement(cell,'commit_complete_fact_partition','if not pandas_to_arrow(staged_df','''if not staged_table.equals(complete_table):
    raise ValueError("境外期货 staging 完整分区内容检查失败。")''')
statement(cell,'commit_complete_fact_partition','committed_dataset =','''if complete_table.num_rows == 0 and destination_path.exists():
    raise ValueError("确认空的正式事实叶仍然存在。")
committed_dataset = open_exact_dataset(
    destination_path if complete_table.num_rows else target_marker_path,
    FACT_PARTITIONING, OVERSEAS_FUTURES_DAILY_SCHEMA, "正式境外期货事实",
    partition_base_dir=target_path,
)
if marker_created and complete_table.num_rows:
    committed_marker_dataset = open_exact_dataset(
        target_marker_path, FACT_PARTITIONING, OVERSEAS_FUTURES_DAILY_SCHEMA,
        "新建正式事实零行标记", partition_base_dir=target_path,
    )
    if committed_marker_dataset.count_rows() != 0:
        raise ValueError("新建正式事实标记必须为零行。")''')
statement(cell,'commit_complete_fact_partition','committed_table =','''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=fact_columns), OVERSEAS_FUTURES_DAILY_SCHEMA,
).sort_by(fact_sort_keys)''')
statement(cell,'commit_complete_fact_partition','committed_df =','')
statement(cell,'commit_complete_fact_partition','if not pandas_to_arrow(committed_df','''if not committed_table.equals(complete_table):
    raise ValueError("正式境外期货完整分区内容检查失败。")
committed_df = arrow_to_pandas(committed_table, OVERSEAS_FUTURES_DAILY_SCHEMA)''')
replace(cell,'len(staged_df)','staged_table.num_rows')

# 状态生成只改内存；验收由 dirty 完整日历叶提交承担。
for name in ('apply_calendar_completion','apply_calendar_failure'):
    statement('b03-c03-17',name,'updated_calendar_df =','')
cells['b03-c03-17'].source=cells['b03-c03-17'].source.replace('len(updated_calendar_df)','len(updated_df)').replace('return updated_calendar_df','return updated_df').replace('log_phase = "output_validation"','log_phase = "state_ready"')

cell='a03-b03-calendar-commit'
statement(cell,'commit_calendar_partitions','partition_keys =','''partition_keys = set(touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None))
calendar_indices_by_partition = calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False).indices
calendar_columns = EXTERNAL_MARKET_CALENDAR_SCHEMA.names
calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
silver_root = lake_root.resolve() / "silver"
target_path = silver_root / CALENDAR_TABLE_NAME''')
statement(cell,'commit_calendar_partitions','partition_mask =','')
statement(cell,'commit_calendar_partitions','for column, value in zip(','')
statement(cell,'commit_calendar_partitions','complete_df =','''complete_table = validate_calendar_table(
    pandas_to_arrow(calendar_df.iloc[calendar_indices_by_partition[partition_key]].loc[:, calendar_columns], EXTERNAL_MARKET_CALENDAR_SCHEMA),
    "待提交的完整外部市场日历分区",
)''')
statement(cell,'commit_calendar_partitions','complete_table = pandas_to_arrow','')
replace(cell,'            silver_root = lake_root.resolve() / "silver"\n            target_path = silver_root / CALENDAR_TABLE_NAME\n','')
statement(cell,'commit_calendar_partitions','file_schema =','')
statement(cell,'commit_calendar_partitions','pq.write_table(','')
statement(cell,'commit_calendar_partitions','staged_dataset =','''staged_dataset = open_exact_dataset(
    staging_path / relative_path, CALENDAR_PARTITIONING, EXTERNAL_MARKET_CALENDAR_SCHEMA,
    "外部市场日历 staging", partition_base_dir=staging_path,
)''')
statement(cell,'commit_calendar_partitions','staged_table =','''staged_table = validate_arrow_table(
    staged_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
).sort_by(calendar_sort_keys)''')
statement(cell,'commit_calendar_partitions','staged_df =','')
statement(cell,'commit_calendar_partitions','if not pandas_to_arrow(staged_df','''if not staged_table.equals(complete_table):
    raise ValueError("外部市场日历 staging 内容检查失败。")''')
statement(cell,'commit_calendar_partitions','committed_dataset =','''committed_dataset = open_exact_dataset(
    destination_path, CALENDAR_PARTITIONING, EXTERNAL_MARKET_CALENDAR_SCHEMA,
    "正式外部市场日历", partition_base_dir=target_path,
)''')
statement(cell,'commit_calendar_partitions','committed_table =','''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
).sort_by(calendar_sort_keys)''')
statement(cell,'commit_calendar_partitions','committed_df =','')
statement(cell,'commit_calendar_partitions','if not pandas_to_arrow(committed_df','''if not committed_table.equals(complete_table):
    raise ValueError("正式外部市场日历分区内容检查失败。")''')
cells[cell].source=cells[cell].source.replace('len(staged_df)','staged_table.num_rows').replace('len(committed_df)','committed_table.num_rows')

cell='b03-c03-19'
# 首次读取只消费本数据集日历，信任正式上游业务证明。
src=cells[cell].source
start=src.index('        calendar_df = validate_calendar_frame(')
end=src.index('        click.echo(',start)
src=src[:start]+'''        calendar_df = arrow_to_pandas(
            calendar_dataset.to_table(columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names, filter=ds.field("dataset_name") == DATASET_NAME),
            EXTERNAL_MARKET_CALENDAR_SCHEMA,
        ).sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)
'''+src[end:]
start=src.index('        # 事实物理兼容但 metadata 过期时')
end=src.index('        mode =',start)
src=src[:start]+'''        # 正式历史业务已由提交证明；描述性 metadata 以当前契约为准。
        log_phase = "read_fact"
        fact_df = read_optional_fact(fact_path)

'''+src[end:]
start=src.index('        if fact_metadata_upgrade_required:')
end=src.index('        # 正式事实和日历状态共同参与差集',start)
src=src[:start]+src[end:]
src=src.replace('pending_df, state_repair_df, complete_count = plan_overseas_futures_grids(', 'pending_df, state_repair_df, complete_count, fact_counts, fact_warning_by_date = plan_overseas_futures_grids(')
src=src.replace('                fact_counts = grid_count_map(fact_df)\n                fact_warning_by_date = ohlc_relation_warning_map(fact_df)\n','')
start=src.index('                # 从正式日历重新规划')
end=src.index('                click.echo(\n                    f"planning_progress: dataset={DATASET_NAME}; function=main; phase=repair_verification;',start)
src=src[:start]+'''                # 成功提交已经完成各 dirty 叶的正式复读，无需重新全表求差。
                complete_count += len(repair_dates)
'''+src[end:]
src=src.replace('phase=repair_verification;', 'phase=repair_batch;')
src=src.replace('pending_df.groupby(["year", "month"], sort=True)', 'pending_df.groupby(PARTITION_COLUMNS, sort=True)')
anchor='        total_rows = 0\n'
assert src.count(anchor)==1
src=src.replace(anchor,'''        if write:
            fact_leaves = {key: frame for key, frame in fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
            calendar_leaves = {key: frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
            empty_fact_df = empty_pandas(OVERSEAS_FUTURES_DAILY_SCHEMA)

'''+anchor)
src=src.replace('            updated_at = datetime.now(timezone.utc)', '''            if write:
                partition_values = dict(zip(PARTITION_COLUMNS, partition_key, strict=True))
                partition_values["dataset_name"] = DATASET_NAME
                calendar_partition_key = tuple(partition_values[column] for column in CALENDAR_PARTITION_COLUMNS)
                calendar_leaf_df = calendar_leaves[calendar_partition_key]
                existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)
            updated_at = datetime.now(timezone.utc)''')
src=src.replace('current_fact_count = grid_count_map(fact_df).get(', 'current_fact_count = fact_counts.get(')
start=src.index('        for group_number,')
end=src.index('        if write:\n            # 全部分区结束后',start)
loop=src[start:end]
loop=loop.replace('calendar_df = apply_calendar_', 'calendar_leaf_df = apply_calendar_').replace('                            calendar_df,','                            calendar_leaf_df,').replace('                calendar_df,','                calendar_leaf_df,').replace('                fact_df,\n                incoming_df,','                existing_fact_leaf_df,\n                incoming_df,')
loop=loop.replace('''validate_overseas_futures_frame(
                    pd.concat(frames, ignore_index=True),
                    "本分区 JQData 响应汇总后的",
                )''','pd.concat(frames, ignore_index=True)')
a=loop.index('            committed_warning_map =')
b=loop.index('            completed_at =',a)
loop=loop[:a]+'''            # 正式完整内容已逐值复读一致，可复用本批来源 OHLC 结论。
            committed_quality_warning_by_date = api_quality_warning_by_date
'''+loop[b:]
a=loop.index('            # 后续月份继续使用本次已经提交的正式事实内容。')
b=loop.index('            total_rows +=',a)
loop=loop[:a]+'''            # 保留各叶本批已提交的状态，不重建整张事实表。
            fact_leaves[partition_key] = committed_partition_df
            calendar_leaves[calendar_partition_key] = calendar_leaf_df

'''+loop[b:]
src=src[:start]+loop+src[end:]
start=src.index('        if write:\n            # 全部分区结束后')
end=src.index('        click.echo(\n            f"finished:',start)
src=src[:start]+src[end:]
cells[cell].source=src

assert before.metadata==notebook.metadata
assert [c.id for c in before.cells]==[c.id for c in notebook.cells]
for old_cell,new_cell in zip(before.cells,notebook.cells,strict=True):
    assert {k:v for k,v in old_cell.items() if k!='source'}=={k:v for k,v in new_cell.items() if k!='source'}
    if new_cell.cell_type=='code':ast.parse(new_cell.source)
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook)+'\n',encoding='utf8',newline='\n')
print(snapshot)
