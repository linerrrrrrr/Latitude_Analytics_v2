"""a03/b02 第 7—9 项：可信输入、dirty 叶业务验收一次、叶级复读与循环不变量。"""
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
RELATIVE=pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.ipynb')
PATH=ROOT/RELATIVE
notebook=nbformat.read(PATH,as_version=4)
before=copy.deepcopy(notebook)
cells={c.id:c for c in notebook.cells}
assert 'def validate_calendar_frame(' in cells['2764b9ee'].source
snapshot=pathlib.Path(tempfile.mkdtemp(prefix='a03-b02-io-before-'))
hashes={}
for path in [ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md', ROOT/'02_Futures_Lakehouse/README.md',
             ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py',
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')),
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')),
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py')),
             ROOT/'00_draft_collection_02/tests/test_b03_c02_raw_staging_lifecycle.py']:
    relative=path.relative_to(ROOT)
    hashes[relative.as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
    if path in (PATH,PATH.with_suffix('.py'),ROOT/'02_Futures_Lakehouse/AGENTS.md',ROOT/'02_Futures_Lakehouse/README.md',ROOT/'00_draft_collection_02/tests/test_b03_c02_raw_staging_lifecycle.py'):
        (snapshot/relative).parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(path,snapshot/relative)
(snapshot/'hashes.json').write_text(json.dumps(hashes,ensure_ascii=False,indent=2),encoding='utf8')

def replace(cell_id,old,new,count=1):
    assert cells[cell_id].source.count(old)==count,(cell_id,old,cells[cell_id].source.count(old))
    cells[cell_id].source=cells[cell_id].source.replace(old,new)

def statement(cell_id,function_name,prefix,replacement):
    cell=cells[cell_id]
    fn=next(n for n in ast.parse(cell.source).body if isinstance(n,ast.FunctionDef) and n.name==function_name)
    nodes=[n for n in ast.walk(fn) if isinstance(n,ast.stmt) and ast.unparse(n).startswith(prefix)]
    assert len(nodes)==1,(function_name,prefix,len(nodes))
    node=nodes[0]
    lines=cell.source.splitlines(keepends=True)
    lines[node.lineno-1:node.end_lineno]=[textwrap.indent(textwrap.dedent(replacement).strip()+'\n',' '*node.col_offset)] if replacement else []
    cell.source=''.join(lines)

# Physical compatibility is exact for fields/types/nullable and identity metadata.
# Descriptive metadata is interpreted using the current authoritative schema.
replace('2764b9ee','    label: str,\n) -> ds.Dataset:', '    label: str,\n    *,\n    partition_base_dir: pathlib.Path | None = None,\n) -> ds.Dataset:')
replace('2764b9ee','            partitioning=partitioning,\n', '            partitioning=partitioning,\n            partition_base_dir=str(partition_base_dir) if partition_base_dir is not None else None,\n')
statement('2764b9ee','open_exact_dataset','if not reconstructed_schema', '''
identity_metadata_keys = (b"table_name", b"primary_key", b"partition_columns")
if (
    len(dataset.schema.names) != len(schema.names)
    or set(dataset.schema.names) != set(schema.names)
    or not reconstructed_schema(dataset, schema).equals(schema, check_metadata=False)
):
    raise TypeError(f"{label}物理字段、类型或 nullable 与权威契约不一致。")
if any((dataset.schema.metadata or {}).get(key) != schema.metadata[key] for key in identity_metadata_keys):
    raise TypeError(f"{label}表名、主键或分区 metadata 与权威契约不一致。")
''')
statement('2764b9ee','open_exact_dataset','if not fragment.physical_schema.equals', '''
fragment_schema = fragment.physical_schema
if not fragment_schema.equals(expected_file_schema, check_metadata=False):
    raise TypeError(f"{label}存在物理字段、类型或 nullable 不一致的 fragment：{fragment.path}")
if any((fragment_schema.metadata or {}).get(key) != schema.metadata[key] for key in identity_metadata_keys):
    raise TypeError(f"{label}存在表身份 metadata 不一致的 fragment：{fragment.path}")
''')
replace('2764b9ee','# Hive 分区列由目录补回；按权威字段顺序重建后比较整表 metadata。', '# Hive 分区列由目录补回；按权威字段顺序重建后比较物理结构。')

statement('2764b9ee','validate_calendar_frame','checked =','')
statement('2764b9ee','validate_calendar_frame','normalized =', 'calendar_keys_df = calendar_table.select(CALENDAR_PRIMARY_KEY).to_pandas()')
replace('2764b9ee','if normalized.duplicated(CALENDAR_PRIMARY_KEY).any():','if calendar_keys_df.duplicated().any():')
statement('2764b9ee','validate_calendar_frame','now_utc =', '''
now_utc = datetime.now(timezone.utc)
completed_fetch_statuses = {"success", "empty_confirmed"}
checked_quality_statuses = {"passed", "warning", "failed"}
calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
''')
replace('2764b9ee','for row in checked.to_pylist():','for row in calendar_table.to_pylist():')
statement('2764b9ee','validate_calendar_frame','completed_status =','completed_status = row["fetch_result_status"] in completed_fetch_statuses')
replace('2764b9ee','row["quality_status"] in {"passed", "warning", "failed"}','row["quality_status"] in checked_quality_statuses')
statement('2764b9ee','validate_calendar_frame','validated_calendar_df =','validated_calendar_table = calendar_table.sort_by(calendar_sort_keys)')
replace('2764b9ee','    frame: pd.DataFrame,\n    context: str,\n) -> pd.DataFrame:', '    calendar_table: pa.Table,\n    context: str,\n) -> pa.Table:')
cells['2764b9ee'].source=cells['2764b9ee'].source.replace('validate_calendar_frame','validate_calendar_table').replace('len(frame)','calendar_table.num_rows').replace('len(validated_calendar_df)','validated_calendar_table.num_rows').replace('return validated_calendar_df','return validated_calendar_table').replace('log_phase = "conversion"','log_phase = "primary_key"')

# One lookup of eligible row positions for an entire repair batch, instead of a full mask for each date.
statement('e77863a1','apply_calendar_completion','updated_df =', '''
updated_df = calendar_df.copy()
required_rows = updated_df.loc[
    updated_df["dataset_name"].eq(DATASET_NAME)
    & updated_df["entity_code"].eq(ENTITY_CODE)
    & updated_df["is_fetch_required"],
    ["observation_date"],
]
required_row_index_by_date = dict(zip(required_rows["observation_date"], required_rows.index, strict=True))
''')
statement('e77863a1','apply_calendar_completion','mask =', '''
if observation_date not in required_row_index_by_date:
    raise ValueError(f"待完成 raw 格点不存在或无需请求：{observation_date}")
row_index = required_row_index_by_date[observation_date]
''')
statement('e77863a1','apply_calendar_completion','if int(mask.sum()) != 1:', '')
source=cells['e77863a1'].source
cut=source.index('def apply_calendar_failure(')
source=source[:cut].replace('updated_df.loc[mask,','updated_df.loc[row_index,')+source[cut:]
cells['e77863a1'].source=source
for name in ('apply_calendar_completion','apply_calendar_failure'):
    statement('e77863a1',name,'validated_calendar_df =','')
cells['e77863a1'].source=cells['e77863a1'].source.replace('len(validated_calendar_df)','len(updated_df)').replace('return validated_calendar_df','return updated_df').replace('log_phase = "output_validation"','log_phase = "state_ready"')

statement('e77863a1','commit_calendar_partitions','partition_keys =', '''
partition_keys = set(touched_df[CALENDAR_PARTITION_COLUMNS].itertuples(index=False, name=None))
calendar_indices_by_partition = calendar_df.groupby(
    CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
).indices
calendar_columns = EXTERNAL_MARKET_CALENDAR_SCHEMA.names
calendar_sort_keys = [(name, "ascending") for name in CALENDAR_PRIMARY_KEY]
silver_root = lake_root.resolve() / "silver"
target_path = silver_root / CALENDAR_TABLE_NAME
''')
statement('e77863a1','commit_calendar_partitions','partition_mask =','')
statement('e77863a1','commit_calendar_partitions','for column, value in zip(', '')
statement('e77863a1','commit_calendar_partitions','complete_df =', '''
complete_table = pandas_to_arrow(
    calendar_df.iloc[calendar_indices_by_partition[partition_key]].loc[:, calendar_columns],
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
)
''')
statement('e77863a1','commit_calendar_partitions','complete_table = pandas_to_arrow(complete_df', '''
complete_table = validate_calendar_table(complete_table, "待提交的完整外部市场日历分区")
''')
# Remove only the old per-leaf copies of these invariant paths.
source=cells['e77863a1'].source
assert source.count('            silver_root = lake_root.resolve() / "silver"')==1
source=source.replace('            silver_root = lake_root.resolve() / "silver"\n            target_path = silver_root / CALENDAR_TABLE_NAME\n','')
cells['e77863a1'].source=source
statement('e77863a1','commit_calendar_partitions','file_schema =','')
statement('e77863a1','commit_calendar_partitions','pq.write_table(', '')
statement('e77863a1','commit_calendar_partitions','staged_dataset =', '''
staged_dataset = open_exact_dataset(
    staging_path / relative_path,
    CALENDAR_PARTITIONING,
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    "外部市场日历 staging",
    partition_base_dir=staging_path,
)
''')
statement('e77863a1','commit_calendar_partitions','expression =','')
statement('e77863a1','commit_calendar_partitions','staged_df =', '''
staged_calendar_table = validate_arrow_table(
    staged_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
).sort_by(calendar_sort_keys)
''')
statement('e77863a1','commit_calendar_partitions','if not pandas_to_arrow(staged_df', '''
if not staged_calendar_table.equals(complete_table):
    raise ValueError("外部市场日历 staging 内容检查失败。")
''')
statement('e77863a1','commit_calendar_partitions','committed_dataset =', '''
committed_dataset = open_exact_dataset(
    destination_path,
    CALENDAR_PARTITIONING,
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
    "正式外部市场日历",
    partition_base_dir=target_path,
)
''')
statement('e77863a1','commit_calendar_partitions','committed_df =', '''
committed_calendar_table = validate_arrow_table(
    committed_dataset.to_table(columns=calendar_columns), EXTERNAL_MARKET_CALENDAR_SCHEMA,
).sort_by(calendar_sort_keys)
''')
statement('e77863a1','commit_calendar_partitions','if not pandas_to_arrow(committed_df', '''
if not committed_calendar_table.equals(complete_table):
    raise ValueError("正式外部市场日历分区内容检查失败。")
''')
cells['e77863a1'].source=cells['e77863a1'].source.replace('len(staged_df)','staged_calendar_table.num_rows').replace('len(committed_df)','committed_calendar_table.num_rows')
# The temporary zero-row marker was only used to open the staging root; leaf reads no longer need it.
replace('0402fa68','import pyarrow.parquet as pq\n','')

# Upstream business proof is trusted; formal writes are already checked by the owning commit function.
source=cells['b88b5aef'].source
fn=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='main')
initial=next(n for n in ast.walk(fn) if isinstance(n,ast.Assign) and ast.unparse(n).startswith('calendar_df = validate_calendar_frame') and n.col_offset==8)
lines=source.splitlines(keepends=True)
lines[initial.lineno-1:initial.end_lineno]=[textwrap.indent('''calendar_df = arrow_to_pandas(
    calendar_dataset.to_table(
        columns=EXTERNAL_MARKET_CALENDAR_SCHEMA.names,
        filter=ds.field("dataset_name") == DATASET_NAME,
    ),
    EXTERNAL_MARKET_CALENDAR_SCHEMA,
).sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)
''',' '*8)]
source=''.join(lines)
start=source.index('                log_phase = "repair_readback"')
end=source.index('\n        if pending_df.empty:',start)
source=source[:start]+'                complete_count += len(repair_evidence)\n'+source[end:]
start=source.index('        if write:\n            log_phase = "final_readback"')
end=source.index('        click.echo(\n            f"finished:',start)
source=source[:start]+source[end:]
cells['b88b5aef'].source=source

# Group once outside the HTTP date loop. Every date updates only its owning full calendar leaf.
statement('b88b5aef','main','session = create_http_session()', '''
if write:
    calendar_leaves = {
        key: frame for key, frame in calendar_df.groupby(
            CALENDAR_PARTITION_COLUMNS, sort=False, observed=True, dropna=False,
        )
    }
session = create_http_session()
''')
replace('b88b5aef','            for observation_date in pending_df["observation_date"].tolist():', '''            for pending_row in pending_df.itertuples(index=False):
                observation_date = pending_row.observation_date
                if write:
                    partition_key = tuple(getattr(pending_row, name) for name in CALENDAR_PARTITION_COLUMNS)
                    calendar_leaf_df = calendar_leaves[partition_key]''')
source=cells['b88b5aef'].source
start=source.index('            for pending_row in pending_df.itertuples(')
end=source.index('        finally:\n            session.close()',start)
loop=source[start:end]
loop=loop.replace('calendar_df = apply_calendar_', 'calendar_leaf_df = apply_calendar_').replace('                            calendar_df,','                            calendar_leaf_df,').replace('                        calendar_df,','                        calendar_leaf_df,')
# Keep each successful date visible to the next date in the same leaf.
anchor='                processed_grid_count += 1'
assert loop.count(anchor)==1
loop=loop.replace(anchor,'                if write:\n                    calendar_leaves[partition_key] = calendar_leaf_df\n\n'+anchor)
cells['b88b5aef'].source=source[:start]+loop+source[end:]

# Resolve the common raw scope once before the existing path-validation loop.
statement('e2d51f8b','commit_raw_response','target_path =','resolved_raw_root = raw_root.resolve()\ntarget_path = raw_leaf_path(raw_root, observation_date)')
replace('e2d51f8b','is_relative_to(raw_root.resolve()):\n                raise ValueError(f"raw 管理路径越界', 'is_relative_to(resolved_raw_root):\n                raise ValueError(f"raw 管理路径越界')

cells['9adb1e47'].source='''## 可信上游读取与 dirty 日历叶验收

正式日历的表级业务语义由生产者提交保证。`main()` 只物化 `domestic_spot_basis` 记录并完成一次权威 Arrow→Pandas 转换，不再重验全表业务。`open_exact_dataset()` 精确核对物理字段、类型、nullable 和表名/主键/分区身份；描述性 metadata 以当前契约为准。指定 `partition_base_dir` 时只打开当前叶，仍从 Hive 路径补回分区列。

`validate_calendar_table()` 仅接收已经通过 `pandas_to_arrow()` 的当前 dirty 完整叶，在提交前执行一次主键、状态、计数、日期与审计时间的完整业务验收并排序。主键检测只转换主键投影，业务循环直接消费 Arrow 记录；没有全表 Pandas/Arrow 往返。状态集合和排序键在逐行循环前准备。

打开函数与业务校验函数保留起止、数量和失败日志；实际表物化由调用方报告。fragment 每 100 个、日历每 10000 行检查一次 2 秒日志间隔，不增加扫描。'''
cells['ac34e98a'].source='''## 状态生成、dirty 叶一次业务验收与逐叶共享事务

raw 正式提交并复读成功后，才生成日历完成状态；HTTP 错误只生成未完成状态。两个生成函数只更新内存，不重验全部历史；多日期修复先建立 required 日期→行位置映射，再逐日期更新。完整业务验收统一归 `commit_calendar_partitions()` 的当前 dirty 完整叶负责。

提交函数在分区循环前准备一次分组索引、列清单、排序键与正式根路径。每叶只转换并执行一次业务校验；staging 和正式复读直接打开该叶，核对物理契约及排序后的完整 Arrow 内容，不再扫描表根或重复业务验收。staging 不再写从未安装的零行标记，已有正式根级标记保持原样。

每个日历叶仍独立进入共享事务，在事务内完成正式复读，成功退出后报告当前叶落盘。失败只恢复当前叶，此前 raw 与成功日历叶保留；失败新叶隔离，恢复不完整保留旧备份，staging 清理。全部触达叶成功后报告日历触达数、完成数及 `date_watermark=none`；错误状态已落盘不等于采集完成。

raw 日期和日历叶互相独立；再次运行可根据已提交 raw 无 API 修复日历。这里不提供跨目录原子可见性、进程终止后的自动恢复或并发写入协调。'''
cells['2951329c'].source='''## CLI：一次差集、局部状态更新与是否写入

入口在读取或联网前执行正式湖显式日期写入门禁。启动时只物化生意社日历记录，一次复读全部所选 raw 日期并生成已完成、无 API 修复和 HTTP 待办。修复提交成功后信任逐叶正式复读，不再打开全表或重扫全部 raw；完成数只增加实际已提交的修复数。

存在 HTTP 待办且启用写入时，在日期循环前把当前日历按叶分组。每个日期只生成和提交其所属完整叶，并将成功状态保留给同叶后续日期；提交顺序仍是当前日期 raw 在先、日历在后。HTTP 错误保留失败正文、写入错误状态后停止；此前成功日期保留。没有写入时仍采集，但不生成持久状态。

每次提交函数已完成必要的正式验收，因此批末不再全表业务校验、全历史 raw 扫描或重新求差。正式历史是可信快照；本轮没有并发写入协调或进程中断后自动恢复。运行起止及日期进度由 main 报告，采集、状态生成和落盘由对应函数报告；没有独立日期水位。'''
cells['74881b81'].source += '\n\n原文两文件的完整性检查、内容 SHA-256，以及 raw staging 和正式路径复读继续保留。启动计划只扫描一次所选日期；本次成功提交是后续状态修复与批末结束的依据，不为复核重复扫描 clean raw 历史。'
for cell_id,pairs in {
    'a03-b02-flow-calendar-transaction':[('函数报告状态生成；尚未落盘','仅更新内存状态；尚未落盘'),('选取触达的完整日历叶；保留同叶其他记录','按预建分组取 dirty 完整叶；一次业务验收'),('逐叶 staging 写入、校验与逐值比较','写 staging；只复读当前叶物理契约和完整内容'),('事务内正式复读、校验与逐值比较','事务内只复读当前叶；物理契约和完整内容一致')],
}.items():
    for old,new in pairs:
        replace(cell_id,old,new)
assert before.metadata==notebook.metadata
assert [c.id for c in before.cells]==[c.id for c in notebook.cells]
for old in before.cells:
    new=cells[old.id]
    assert {k:v for k,v in old.items() if k!='source'}=={k:v for k,v in new.items() if k!='source'}
    if new.cell_type=='code':
        ast.parse(new.source)
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook)+'\n',encoding='utf8',newline='\n')

old_text='raw staging 与正式路径仍复读字节及 SHA-256，日历仍执行原有 staging 和正式完整校验与逐值比较，正式验收均在事务内。'
new_text=('raw staging 与正式路径仍复读字节及 SHA-256。日历信任上游正式业务证明，每个 dirty 完整叶只在提交前执行一次业务校验；'
          'staging 与正式安装仅复读当前叶物理契约及完整内容，正式验收在事务内。启动只求差一次，日期循环只更新所属日历叶，'
          '成功修复后与批末不重扫全历史 raw 或日历；描述性 metadata 以当前契约为准。')
for relative in ('02_Futures_Lakehouse/AGENTS.md','02_Futures_Lakehouse/README.md'):
    path=ROOT/relative
    source=path.read_text(encoding='utf8')
    assert source.count(old_text)==1
    path.write_text(source.replace(old_text,new_text),encoding='utf8',newline='\n')
print(snapshot)
