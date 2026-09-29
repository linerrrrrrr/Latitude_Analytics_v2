"""b03 第 7—9 项局部修改；Notebook 为唯一编辑源。"""

import ast
import copy
import hashlib
import io
import json
import pathlib
import tempfile
import textwrap
import tokenize

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b03_futures_contract_calendar.ipynb'
before_bytes = PATH.read_bytes()
notebook = json.loads(before_bytes)
before = copy.deepcopy(notebook)
cells = {c['id']: c for c in notebook['cells']}


def replace_once(cell_id, old, new):
    source = ''.join(cells[cell_id]['source'])
    assert source.count(old) == 1, (cell_id, old[:100], source.count(old))
    cells[cell_id]['source'] = source.replace(old, new, 1).splitlines(keepends=True)


def replace_statement(cell_id, prefix, replacement):
    source = ''.join(cells[cell_id]['source'])
    nodes = [n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.stmt) and ast.unparse(n).startswith(prefix)]
    assert len(nodes) == 1, (prefix, len(nodes))
    n = nodes[0]
    lines = source.splitlines(keepends=True)
    text = textwrap.indent(textwrap.dedent(replacement).strip() + '\n', ' ' * n.col_offset) if replacement.strip() else ''
    lines[n.lineno - 1:n.end_lineno] = [text]
    cells[cell_id]['source'] = ''.join(lines).splitlines(keepends=True)


replace_once('d36703b192451764', 'PHYSICAL_METADATA_KEYS =', '''CONTRACT_CALENDAR_PHYSICAL_SCHEMA = pa.schema(
    [FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name) for name in FUTURES_CONTRACT_CALENDAR_SCHEMA.names if name not in PARTITION_COLUMNS],
    metadata=FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata,
)
PHYSICAL_PRIMARY_KEY = [name for name in PRIMARY_KEY if name not in PARTITION_COLUMNS]
PHYSICAL_METADATA_KEYS =''')
replace_once('d36703b192451764', 'INFO_FIELDS = [', '''BUSINESS_PRIMARY_KEY_POSITIONS = tuple(BUSINESS_COLUMNS.index(name) for name in PRIMARY_KEY)
INFO_FIELDS = [''')
replace_statement('d36703b192451764', 'marker_schema = pa.schema', '''
    marker_schema = CONTRACT_CALENDAR_PHYSICAL_SCHEMA.with_metadata(marker_metadata)
''')

replace_statement('c03-parser-validation', 'validated_contract_calendar_df = arrow_to_pandas', '''
    validated_contract_calendar_df = contract_calendar_table.to_pandas(types_mapper=pd.ArrowDtype)
''')
replace_statement('c03-parser-validation', 'def business_rows_by_key(', '''
    def business_rows_by_key(
        validated_contract_calendar_df: pd.DataFrame,
    ) -> dict[tuple[object, ...], tuple[object, ...]]:
        """消费已完成本表类型校验的行；只构造比较映射，不再次转换或校验。"""
        rows_by_key = {}
        for row in validated_contract_calendar_df.loc[:, BUSINESS_COLUMNS].itertuples(index=False, name=None):
            business_values = tuple(None if value is pd.NA else value for value in row)
            key = tuple(business_values[position] for position in BUSINESS_PRIMARY_KEY_POSITIONS)
            rows_by_key[key] = business_values
        return rows_by_key
''')

replace_once('c03-source', '        contract_info_by_code = {}', '        required_info_fields = set(INFO_FIELDS)\n        contract_info_by_code = {}')
replace_once('c03-source', 'set(INFO_FIELDS) - set(contract_record)', 'required_info_fields - set(contract_record)')

# 只把合约日内不随 Session 变化的标量归一化移出 Session 循环，不提前解析无有效规则的合约。
source = ''.join(cells['c03-build']['source'])
tree = ast.parse(source)
nodes = [n for n in ast.walk(tree) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in {'contract_multiplier', 'tick_size'} for t in n.targets)]
assert len(nodes) == 2
session_loop = next(n for n in ast.walk(tree) if isinstance(n, ast.For) and ast.unparse(n.target) == '(session_number, session_value)')
lines = source.splitlines(keepends=True)
moved = ''.join(''.join(lines[n.lineno - 1:n.end_lineno]) for n in sorted(nodes, key=lambda n: n.lineno))
moved = ''.join(line[4:] if line.startswith('    ') else line for line in moved.splitlines(keepends=True))
for n in sorted(nodes, key=lambda n: n.lineno, reverse=True):
    del lines[n.lineno - 1:n.end_lineno]
lines.insert(session_loop.lineno - 1, moved + '\n')
cells['c03-build']['source'] = ''.join(lines).splitlines(keepends=True)

replace_statement('c03-commit', 'replacement_contract_calendar_df = validate_contract_calendar_frame', '')
replace_once('c03-commit', 'commit_phase = "input_validation"', 'commit_phase = "replacement_scope"')
replace_once('c03-commit', '        target_path = silver_root / TABLE_NAME\n', '''        target_path = silver_root / TABLE_NAME
        relative_path = pathlib.Path(
            f"exchange_code={exchange_code}", f"year={year}", f"month={month}"
        )
        destination_path = target_path / relative_path
''')
replace_once('c03-commit', '            target_path.is_dir()\n            and any(target_path.rglob("*.parquet"))', '            destination_path.is_dir()\n            and any(destination_path.rglob("*.parquet"))')
replace_once('c03-commit', '''                target_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
''', '''                destination_path,
                format="parquet",
                partitioning=HIVE_PARTITIONING,
                partition_base_dir=target_path.as_posix(),
''')
source = ''.join(cells['c03-commit']['source'])
assert source.count('"现有正式数据集 "') == 2
source = source.replace('"现有正式数据集 "', '"现有正式分区 "')
source = source.replace('"existing_dataset"', '"existing_partition"').replace('phase=existing_dataset;', 'phase=existing_partition;')
cells['c03-commit']['source'] = source.splitlines(keepends=True)
replace_statement('c03-commit', 'existing_contract_calendar_table = existing_contract_calendar_dataset.to_table', '''
    existing_contract_calendar_table = existing_contract_calendar_dataset.to_table(
        columns=FUTURES_CONTRACT_CALENDAR_SCHEMA.names,
    )
''')
replace_statement('c03-commit', 'complete_partition_table = pandas_to_arrow', '''
    complete_partition_table = pa.Table.from_pandas(
        complete_partition_df,
        schema=FUTURES_CONTRACT_CALENDAR_SCHEMA,
        preserve_index=False,
        safe=True,
    ).replace_schema_metadata(FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata)
''')
# 删除原来位于合并后的路径构造，提前的同一构造供精确旧叶读取使用。
source = ''.join(cells['c03-commit']['source'])
tree = ast.parse(source)
for name in ('relative_path', 'destination_path'):
    nodes = [n for n in ast.walk(tree) if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)]
    assert len(nodes) == 2, name
    node = max(nodes, key=lambda n: n.lineno)
    lines = source.splitlines(keepends=True)
    del lines[node.lineno - 1:node.end_lineno]
    source = ''.join(lines)
    tree = ast.parse(source)
cells['c03-commit']['source'] = source.splitlines(keepends=True)
replace_statement('c03-commit', 'data_files = []', '')
replace_statement('c03-commit', 'if not len(complete_partition_table):', '')
replace_statement('c03-commit', 'if not len(complete_partition_table) and (not data_files)', '''
    if (
        not len(complete_partition_table)
        and not marker_path.exists()
        and not any(path.name != "schema.parquet" for path in target_path.rglob("*.parquet"))
    ):
        pq.write_table(
            pa.Table.from_batches([], schema=CONTRACT_CALENDAR_PHYSICAL_SCHEMA),
            marker_path,
        )
        marker_created = True
''')
replace_statement('c03-commit', 'expected_partition_schema = pa.schema', '')
replace_once('c03-commit', 'expected_partition_schema,', 'CONTRACT_CALENDAR_PHYSICAL_SCHEMA,')
replace_statement('c03-commit', 'physical_primary_key =', '')
source = ''.join(cells['c03-commit']['source'])
source = source.replace('physical_primary_key', 'PHYSICAL_PRIMARY_KEY')
cells['c03-commit']['source'] = source.splitlines(keepends=True)

markdown_replacements = {
    'b03-config-heading': [
        ('配置物理检查与 Session 解析所需常量。', '配置物理检查与 Session 解析所需常量。去掉 Hive 分区列后的物理 Schema、物理主键列和业务比较键的位置在这里构造一次，供各分区复用。'),
    ],
    'c03-parser-validation-heading': [
        ('先按权威 Schema 转换，再检查', '先通过 `pandas_to_arrow()` 完成一次权威类型与非空校验，直接转成对应的 Pandas Arrow 扩展类型后检查'),
        ('当前调用位置包括生成结果、计划中的本地比较范围、提交输入及合并后完整叶。', '完整业务校验保留在生成结果、计划中的本地比较范围和合并后完整叶；提交入口只检查替换范围，不再对输入单独运行一遍完整业务校验。'),
        ('`business_rows_by_key()` 生成排除 `updated_at` 的主键到业务值映射，用于双向比较。', '`business_rows_by_key()` 只消费已完成类型校验的 DataFrame，直接按业务列生成排除 `updated_at` 的比较映射，不再转回 Arrow。可空标量的 `pd.NA` 归一成 `None`，保持原来的空值比较语义。'),
    ],
    'b03-flow-validation': [
        ('M["按 Schema 转换；排除 updated_at"]', 'M["直接读取已校验业务列；排除 updated_at；统一空值"]'),
    ],
    'c03-build-heading': [
        ('再逐段生成北京时间的 Session。', '再逐段生成北京时间的 Session。合约乘数与 tick 的空值和浮点归一化在该合约日的 Session 循环前完成一次，各段复用。'),
    ],
    'c03-commit-heading': [
        ('`commit_partition()` 依次执行替换范围检查、输入业务校验、读取旧叶、按模式合并、完整叶业务校验，再写 staging 并复读。', '`commit_partition()` 依次执行替换范围检查、精确读取旧叶、按模式合并、完整叶业务校验，再写 staging 并复读。合并后的完整叶负责一次完整业务校验，提交输入不再单独重复检查；序列化时按同一权威 Schema 构造 Arrow 表，不再调用已完成的校验。'),
        ('staging 通过后备份旧叶、安装新叶或删除空叶，最后在正式路径检查物理 Schema、表身份 metadata、主键与行数。', 'staging 通过后备份旧叶、安装新叶或删除空叶，最后在正式路径检查物理 Schema、表身份 metadata、主键与行数。入口的全表检查保留一次；每次提交仅打开当前叶，并用 `partition_base_dir` 还原 Hive 分区列。独立调用时也检查该叶物理契约，不检查其他叶。'),
    ],
    'b03-flow-commit': [
        ('A["检查替换参数、输入业务规则与范围"]', 'A["检查替换参数与输入范围"]'),
        ('B["检查现有表物理契约；读取当前旧叶"]', 'B["仅打开当前旧叶；检查该叶物理契约并读取"]'),
        ('D["校验完整叶；构造 Arrow 表"]', 'D["完整叶业务校验一次；按同一 Schema 序列化"]'),
    ],
    'c03-cli-heading': [
        ('`main()` 负责参数门禁、正式数据集检查、水位与上游范围选择、差异计划及顺序提交。', '`main()` 负责参数门禁、一次正式数据集检查、水位与上游范围选择、差异计划及顺序提交。提交循环不再重复打开全表或检查全部 fragment，只读取各自触达叶。'),
    ],
}
for cell_id, replacements in markdown_replacements.items():
    for old, new in replacements:
        replace_once(cell_id, old, new)

code = lambda nb: '\n\n'.join(''.join(c['source']) for c in nb['cells'] if c['cell_type'] == 'code')
ast.parse(code(notebook))
comments = lambda text: [t.string for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type == tokenize.COMMENT]
assert comments(code(before)) == comments(code(notebook))
for c in before['cells']:
    assert {k:v for k,v in c.items() if k != 'source'} == {k:v for k,v in cells[c['id']].items() if k != 'source'}
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='b03-validation-io-before-'))
(snapshot / PATH.name).write_bytes(before_bytes)
(snapshot / PATH.with_suffix('.py').name).write_bytes(PATH.with_suffix('.py').read_bytes())
hashes = {
    str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
    for d in (ROOT / '02_Futures_Lakehouse').glob('a0[1-4]_*')
    for p in d.glob('*.py') if p != PATH.with_suffix('.py')
}
(snapshot / 'other_script_hashes.json').write_text(json.dumps(hashes, indent=2), encoding='utf-8')
PATH.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + '\n', encoding='utf-8', newline='\n')
print(json.dumps({'snapshot':str(snapshot), 'code_comments_unchanged':True}))
