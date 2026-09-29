"""a02/b03 第 7—9 项：收缩重复转换/摘要校验及循环不变量。"""
import hashlib
import json
import pathlib
import shutil
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b03_warehouse_receipt.ipynb')
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a02-b03-validation-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'.env.template']
for directory in ('02_Futures_Lakehouse', '03_Futures_Database', 'config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py', '.ipynb', '.md'))
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},ensure_ascii=False,indent=2),encoding='utf8')
for relative in (RELATIVE, RELATIVE.with_suffix('.py')):
    target = snapshot/relative
    target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(ROOT/relative,target)
path = ROOT/RELATIVE
notebook = json.loads(path.read_text(encoding='utf8'))
cells = {c['id']:c for c in notebook['cells']}

def source(cell_id):
    return ''.join(cells[cell_id]['source'])

def put(cell_id,text):
    cells[cell_id]['source'] = text.splitlines(keepends=True)

def replace(cell_id, old, new):
    code=source(cell_id)
    assert code.count(old)==1, (cell_id,old)
    put(cell_id,code.replace(old,new))

replace('8e8b39eb','pq.read_schema(marker_path),','marker_metadata.schema.to_arrow_schema(),')
replace('a02-b03-leaf-read','''    primary_key: list[str],
    label: str,
) -> tuple[int, str]:
    primary_key_table = table.select(primary_key)
    primary_key_df = primary_key_table.to_pandas()
    if primary_key_df.duplicated(primary_key).any():
        raise ValueError(f"{label}主键不唯一。")
''','''    primary_key: list[str],
) -> tuple[int, str]:
    primary_key_table = table.select(primary_key)
''')
replace('a02-b03-leaf-read','primary_key_summary(empty_table, primary_key, label)','primary_key_summary(empty_table, primary_key)')
replace('a02-b03-leaf-read','''            summary_table.select(primary_key),
            primary_key,
            label,
''','''            summary_table,
            primary_key,
''')
replace('df69af57','''            complete_df.loc[:, FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA.names],
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
''','''            complete_df,
            FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
''')
replace('df69af57','''            PRIMARY_KEY,
            "待提交完整仓单分区",
''','''            PRIMARY_KEY,
''')
replace('a02-b03-calendar-commit','''                CALENDAR_PRIMARY_KEY,
                "待提交完整报告日历分区",
''','''                CALENDAR_PRIMARY_KEY,
''')

code=source('a02-b03-calendar-commit')
root_start=code.index('            silver_root = lake_root.resolve() / "silver"')
root_end=code.index('            run_id = uuid.uuid4().hex',root_start)
root_block=code[root_start:root_end]
code=code[:root_start]+code[root_end:]
schema_start=code.index('                file_schema = pa.schema(')
schema_end=code.index('                pq.write_table(',schema_start)
schema_block=code[schema_start:schema_end]
code=code[:schema_start]+code[schema_end:]
loop_start=code.index('        for partition_key in sorted(partition_keys):')
invariants='''        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME
        calendar_partition_groups = calendar_df.groupby(
            CALENDAR_PARTITION_COLUMNS,
            sort=False,
        )
'''
invariants+=''.join(line[8:] for line in schema_block.splitlines(keepends=True))
marker_block=root_block[root_block.index('            validate_table_marker('):]
invariants+='        if partition_keys:\n'+marker_block+'\n'
code=code[:loop_start]+invariants+code[loop_start:]
old='''            partition_mask = pd.Series(True, index=calendar_df.index)
            for column, value in zip(CALENDAR_PARTITION_COLUMNS, partition_key, strict=True):
                partition_mask &= calendar_df[column].eq(value)
            complete_df = calendar_df.loc[
                partition_mask,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
            ]
'''
new='''            complete_df = calendar_partition_groups.get_group(partition_key).loc[
                :,
                FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA.names,
            ]
'''
assert code.count(old)==1
put('a02-b03-calendar-commit',code.replace(old,new))
replace('a02-b03-calendar-commit','''                    validate_table_marker(
                        target_path,
                        FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
                        CALENDAR_PARTITION_COLUMNS,
                        "正式报告日历",
                        required=True,
                    )
''','')

code=source('f4cfe66c')
start=code.index('            if any(not frame.empty for frame in frames):')
end=code.index('            click.echo(',start)
code=code[:start]+'''            incoming_df = pd.concat(frames, ignore_index=True)
'''+code[end:]
start=code.index('            grid_results = {')
end=code.index('            calendar_started_at = time.perf_counter()',start)
code=code[:start]+'''            grid_results = {}
            for grid in group_records:
                grid_key = tuple(grid[column] for column in GRID_COLUMNS)
                grid_results[grid_key] = (
                    partition_counts.get(grid_key, 0),
                    missing_unit_counts.get(grid_key, 0),
                )
'''+code[end:]
put('f4cfe66c',code)

replace('a02-b03-leaf-read-heading','检查范围、主键唯一性，并对排序后的主键计算 SHA-256 与行数。','检查分区范围，并对排序后的主键计算 SHA-256 与行数。主键唯一性由 dirty 叶完整业务校验保证；预期、staging 与正式摘要保留全部主键及其重复次数，行数和摘要一致即可证明安装没有增删或重复主键，不再额外转成 Pandas 重查唯一性。')
replace('a02-b03-flow-read','投影主键和分区；核对范围及唯一性','投影主键和分区；核对范围')
replace('1daeab12','根级 `schema.parquet` 必须是零行且满足物理字段、类型、nullable 和表名/主键/分区身份 metadata。','根级 `schema.parquet` 必须是零行且满足物理字段、类型、nullable 和表名/主键/分区身份 metadata。一次读取文件 metadata，同时取得行数和 Arrow Schema，不再为同一标记重复打开 footer。')
replace('a02-b03-calendar-commit-heading','每个叶写 staging，','先按分区键对输入日历分组一次，目录、物理 Schema 和不变根标记检查放在分区循环前；逐叶直接取得完整组，不再对整份输入重复构造分区筛选。每个叶写 staging，')
replace('a02-b03-flow-calendar-commit','A["已验证日历；定位触达的 warehouse_receipt 叶"] --> B["逐叶取完整内容；保留其他品种和日期"]','A["一次分组并准备目录、物理 Schema；检查根标记"] --> B["逐叶取得完整组；保留其他品种和日期"]')
put('b4752348',source('b4752348')+'\n各日响应已经由归一化函数按权威 Schema 转换，并固定为请求格点。月度汇总直接拼接这些同类型结果，不再往返转换或重复检查格点子集；日历结果计数复用已有 `group_records`。事实合并后的 dirty 叶业务校验、staging/正式摘要验收及事务边界均保留。启动只读取一次日历规划投影，逐分区仍仅读取当前事实叶与对应日历叶，不扫描 clean 事实历史。\n')
replace('a02-b03-flow-cli','X["汇总当月来源结果"]','X["直接拼接已契约化日响应"]')
for cell in notebook['cells']:
    if cell['cell_type']=='code':
        compile(''.join(cell['source']),cell['id'],'exec')
path.write_text(json.dumps(notebook,ensure_ascii=False,indent=1)+'\n',encoding='utf8',newline='\n')
print(snapshot)
