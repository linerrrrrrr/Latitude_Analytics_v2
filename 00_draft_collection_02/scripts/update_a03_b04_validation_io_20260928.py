"""外部指数第 7—9 项：单次业务校验、当前叶计算/复读和循环不变量。"""
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
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b04_external_index.ipynb')
PATH=ROOT/RELATIVE
notebook=nbformat.read(PATH,4)
before=copy.deepcopy(notebook)
cells={c.id:c for c in notebook.cells}
snapshot=pathlib.Path(tempfile.mkdtemp(prefix='a03-b04-validation-before-'))
selected=[RELATIVE,RELATIVE.with_suffix('.py'),pathlib.Path('02_Futures_Lakehouse/AGENTS.md'),pathlib.Path('02_Futures_Lakehouse/README.md'),pathlib.Path('00_draft_collection_02/tests/test_a03_b04_shared_transaction.py')]
paths=[ROOT/'AGENTS.md',ROOT/'03_Futures_Database/AGENTS.md',ROOT/'config/data_contracts.py',*(ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb'),*(ROOT/'02_Futures_Lakehouse').glob('a*/*.py'),*(ROOT/relative for relative in selected)]
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},indent=2),encoding='utf8')
for relative in selected:
    destination=snapshot/relative
    destination.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(ROOT/relative,destination)
source=(ROOT/'00_draft_collection_02/scripts/update_a03_b03_validation_io_20260928.py').read_text(encoding='utf8')
exec(source[source.index('def replace('):source.index('# 重用上一环节')])

# 物理字段及表身份仍严格检查；描述性 metadata 使用当前契约。
peer=nbformat.read(ROOT/'02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb',4)
peer_code='\n\n'.join(c.source for c in peer.cells if c.cell_type=='code')
cells['b03-c04-09'].source='\n\n'.join(ast.get_source_segment(peer_code,n) for n in ast.parse(peer_code).body if isinstance(n,ast.FunctionDef) and n.name in ('reconstructed_schema','open_exact_dataset'))+'\n'

# 两类业务验收接收调用方已转换的 Arrow 表，返回排序后的同一表示。
for cell,name,table_name,schema_name,keys_name,validated_name in (
    ('a03-b04-calendar-validation','validate_calendar_frame','calendar_table','EXTERNAL_MARKET_CALENDAR_SCHEMA','CALENDAR_PRIMARY_KEY','validated_calendar'),
    ('a03-b04-fact-validation','validate_external_index_frame','index_table','EXTERNAL_INDEX_DAILY_SCHEMA','PRIMARY_KEY','validated_index'),
):
    statement(cell,name,'checked =','')
    statement(cell,name,'normalized =',f'keys_df = {table_name}.select({keys_name}).to_pandas()')
    replace(cell,f'normalized.duplicated({keys_name})','keys_df.duplicated()')
    replace(cell,'for row in checked.to_pylist():',f'for row in {table_name}.to_pylist():')
    statement(cell,name,'now_utc =','now_utc = datetime.now(timezone.utc)\ncurrent_date = date.today()' if table_name=='index_table' else 'now_utc = datetime.now(timezone.utc)')
    if table_name=='index_table': replace(cell,'row["observation_date"] > date.today()','row["observation_date"] > current_date')
    statement(cell,name,validated_name+'_df =',f'{validated_name}_table = {table_name}.sort_by([(name, "ascending") for name in {keys_name}])')
    replace(cell,'    frame: pd.DataFrame,\n    context: str,\n) -> pd.DataFrame:',f'    {table_name}: pa.Table,\n    context: str,\n) -> pa.Table:')
    cells[cell].source=cells[cell].source.replace(name,name.replace('_frame','_table')).replace('len(frame)',table_name+'.num_rows').replace(validated_name+'_df',validated_name+'_table').replace('log_phase = "conversion"','log_phase = "primary_key"')
replace('a03-b04-calendar-validation','# 日历消费者只检查当前调度和状态回写直接依赖的关系。','# 日历状态生产者对待提交的 dirty 完整叶执行业务验收。')

statement('a03-b04-read-fact','read_optional_fact','validated_index_df =','validated_index_df = arrow_to_pandas(table, EXTERNAL_INDEX_DAILY_SCHEMA).sort_values(PRIMARY_KEY).reset_index(drop=True)')
replace('a03-b04-read-fact','# 空事实目录是合法的全量起点；已有事实则必须精确符合新契约。','# 空事实目录是合法的全量起点；已有正式事实只检查物理契约并信任业务证明。')
replace('a03-b04-read-fact','log_phase = "validate"','log_phase = "convert"')

cell='a03-b04-query'
fn=next(n for n in ast.parse(cells[cell].source).body if isinstance(n,ast.FunctionDef))
date_filter=next(n for n in ast.walk(fn) if isinstance(n,ast.Assign) and ast.unparse(n).startswith('date_filter ='))
filter_source=ast.unparse(date_filter)
statement(cell,'query_eastmoney_indicator_range','date_filter =','')
statement(cell,'query_eastmoney_indicator_range','response_rows =','response_rows = []\n'+filter_source+'\nrequest_columns = ",".join(EASTMONEY_FIELDS)')
replace(cell,'"columns": ",".join(EASTMONEY_FIELDS)','"columns": request_columns')
cell='a03-b04-normalize'
statement(cell,'normalize_external_index_response','seen_grids =','seen_grids = set()\nrequired_fields = set(EASTMONEY_FIELDS)\ncurrent_date = date.today()')
replace(cell,'set(EASTMONEY_FIELDS) - set(item)','required_fields - item.keys()')
replace(cell,'observation_date > date.today()','observation_date > current_date')
statement(cell,'normalize_external_index_response','normalized_index_df =','''normalized_index_table = validate_external_index_table(
    pandas_to_arrow(frame, EXTERNAL_INDEX_DAILY_SCHEMA), "Eastmoney 完整分页响应转换后的",
)
normalized_index_df = arrow_to_pandas(normalized_index_table, EXTERNAL_INDEX_DAILY_SCHEMA)''')
statement('b03-c04-13','external_index_reconciliation','calendar_rows =','calendar_rows = relevant_df.to_dict("records")')
replace('b03-c04-13','log_phase = "calendar_conversion"','log_phase = "calendar_records"')

# 合并只处理调用方提供的当前叶；所有完整业务验收归提交函数。
cell='a03-b04-merge'
statement(cell,'full_fact_partition','partition_mask =','')
statement(cell,'full_fact_partition','for column, value in zip(','')
statement(cell,'full_fact_partition','existing_partition_df =','existing_partition_df = existing_df')
statement(cell,'full_fact_partition','merged_index_df =','')
cells[cell].source=cells[cell].source.replace('len(merged_index_df)','len(complete_df)').replace('return merged_index_df','return complete_df').replace('log_phase = "output_validation"','log_phase = "merge_ready"')

cell='a03-b04-fact-commit'
statement(cell,'commit_complete_fact_partition','complete_df =','''complete_table = validate_external_index_table(
    pandas_to_arrow(frame.loc[:, EXTERNAL_INDEX_DAILY_SCHEMA.names], EXTERNAL_INDEX_DAILY_SCHEMA),
    "待提交完整外部指数分区",
)
fact_columns = EXTERNAL_INDEX_DAILY_SCHEMA.names
fact_sort_keys = [(name, "ascending") for name in PRIMARY_KEY]''')
statement(cell,'commit_complete_fact_partition','if not complete_df.empty:','''if complete_table.num_rows:
    actual_keys = set(zip(*(complete_table[column].to_pylist() for column in PARTITION_COLUMNS), strict=True))
    if actual_keys != {partition_key}:
        raise ValueError("待提交外部指数内容越出指定 Hive 叶分区。")''')
statement(cell,'commit_complete_fact_partition','complete_table = pandas_to_arrow','')
statement(cell,'commit_complete_fact_partition','staged_df =','staged_table = validate_arrow_table(staged_table, EXTERNAL_INDEX_DAILY_SCHEMA).sort_by(fact_sort_keys)')
statement(cell,'commit_complete_fact_partition','if not pandas_to_arrow(staged_df','''if not staged_table.equals(complete_table):
    raise ValueError("外部指数 staging 完整分区内容检查失败。")''')
replace(cell,'        staging_marker_path = staging_path / "schema.parquet"','        staging_marker_path = staging_path / "schema.parquet"\n        marker_created = not target_marker_path.exists()')
replace(cell,'if not target_marker_path.exists():','if marker_created:')
statement(cell,'commit_complete_fact_partition','committed_dataset =','''if complete_table.num_rows == 0 and destination_path.exists():
    raise ValueError("确认空的正式事实叶仍然存在。")
committed_dataset = open_exact_dataset(
    destination_path if complete_table.num_rows else target_marker_path,
    FACT_PARTITIONING, EXTERNAL_INDEX_DAILY_SCHEMA, "正式外部指数事实",
    partition_base_dir=target_path,
)
if marker_created and complete_table.num_rows:
    committed_marker_dataset = open_exact_dataset(
        target_marker_path, FACT_PARTITIONING, EXTERNAL_INDEX_DAILY_SCHEMA,
        "新建正式事实零行标记", partition_base_dir=target_path,
    )
    if committed_marker_dataset.count_rows() != 0:
        raise ValueError("新建正式事实标记必须为零行。")''')
statement(cell,'commit_complete_fact_partition','committed_table =','''committed_table = validate_arrow_table(
    committed_dataset.to_table(columns=fact_columns), EXTERNAL_INDEX_DAILY_SCHEMA,
).sort_by(fact_sort_keys)''')
statement(cell,'commit_complete_fact_partition','committed_df =','')
statement(cell,'commit_complete_fact_partition','if not pandas_to_arrow(committed_df','''if not committed_table.equals(complete_table):
    raise ValueError("正式外部指数完整分区内容检查失败。")
committed_df = arrow_to_pandas(committed_table, EXTERNAL_INDEX_DAILY_SCHEMA)''')
cells[cell].source=cells[cell].source.replace('len(complete_df)','complete_table.num_rows')

for name in ('apply_calendar_completion','apply_calendar_failure'):
    statement('b03-c04-17',name,'updated_calendar_df =','')
cells['b03-c04-17'].source=cells['b03-c04-17'].source.replace('len(updated_calendar_df)','len(updated_df)').replace('return updated_calendar_df','return updated_df').replace('log_phase = "output_validation"','log_phase = "state_ready"')

cell='a03-b04-calendar-commit'
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
cells[cell].source=cells[cell].source.replace('len(committed_df)','committed_table.num_rows').replace('len(complete_df)','complete_table.num_rows')

cell='b03-c04-19'
statement(cell,'main','calendar_df = validate_calendar_frame','''calendar_df = arrow_to_pandas(
    calendar_dataset.to_table(columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names, filter=ds.field("dataset_name") == DATASET_NAME),
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
)''')
statement(cell,'main','total_rows =','''fact_leaves = {tuple(key): frame for key, frame in fact_df.groupby(PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
calendar_leaves = {tuple(key): frame for key, frame in calendar_df.groupby(CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False)}
empty_fact_df = empty_pandas(EXTERNAL_INDEX_DAILY_SCHEMA)
empty_calendar_df = empty_pandas(EXTERNAL_MARKET_CALENDAR_SCHEMA)
total_rows = 0''')
src=cells[cell].source
src=src.replace('                updated_at = datetime.now(timezone.utc)','''                calendar_partition_key = (DATASET_NAME, partition_key[1], partition_key[2])
                calendar_leaf_df = calendar_leaves.get(calendar_partition_key, empty_calendar_df)
                existing_fact_leaf_df = fact_leaves.get(partition_key, empty_fact_df)
                updated_at = datetime.now(timezone.utc)''')
start=src.index('            for group_number,')
end=src.index('        finally:\n            if session is not None:',start)
loop=src[start:end]
loop=loop.replace('calendar_df = apply_calendar_', 'calendar_leaf_df = apply_calendar_').replace('                        calendar_df,','                        calendar_leaf_df,').replace('                            calendar_df,','                            calendar_leaf_df,').replace('                                    calendar_df,','                                    calendar_leaf_df,')
loop=loop.replace('grid_count_map(fact_df)','grid_count_map(existing_fact_leaf_df)').replace('                    fact_df,\n                    incoming_df,','                    existing_fact_leaf_df,\n                    incoming_df,')
loop=loop.replace('''validate_external_index_frame(
                        pd.concat(frames, ignore_index=True),
                        "本分区 Eastmoney 响应汇总后的",
                    )''','pd.concat(frames, ignore_index=True)')
a=loop.index('                # 后续分区继续使用本次已经提交的正式事实内容。')
b=loop.index('                total_rows +=',a)
loop=loop[:a]+'''                # 保留本批已提交叶；同月后续分类继承已更新日历。
                fact_leaves[partition_key] = committed_partition_df
                calendar_leaves[calendar_partition_key] = calendar_leaf_df

'''+loop[b:]
src=src[:start]+loop+src[end:]
start=src.index('        if write:\n            # 全部分区结束后')
end=src.index('        click.echo(f"finished:',start)
src=src[:start]+src[end:]
cells[cell].source=src

assert before.metadata==notebook.metadata
for old_cell,new_cell in zip(before.cells,notebook.cells,strict=True):
    assert {k:v for k,v in old_cell.items() if k!='source'}=={k:v for k,v in new_cell.items() if k!='source'}
    if new_cell.cell_type=='code': ast.parse(new_cell.source)
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook),encoding='utf8',newline='\n')
print(snapshot)
