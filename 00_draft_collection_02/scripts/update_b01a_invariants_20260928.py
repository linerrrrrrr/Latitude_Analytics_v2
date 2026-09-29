"""b01a 第 7—9 项：保留 raw 验收；只外移已有循环中的固定计算。"""
import ast
import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01a_position_rank_special_case_calibration.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id:c for c in notebook.cells}


def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)


replace('ad3d3d92', '''        log_phase = "row_scan"
        official_rows = []''', '''        log_phase = "row_scan"
        official_instrument_id = special_case["official_instrument_id"]
        log_source_row_count = len(source_rows)
        official_rows = []''')
replace('ad3d3d92', 'if instrument_id != special_case["official_instrument_id"]:', 'if instrument_id != official_instrument_id:')
cells['ad3d3d92'].source = cells['ad3d3d92'].source.replace('{log_scanned_rows}/{len(source_rows)}', '{log_scanned_rows}/{log_source_row_count}')
replace('e38b03b7', '    log_case_index = 0\n', '    log_case_index = 0\n    log_case_count = len(POSITION_RANK_SPECIAL_CASES)\n')
cells['e38b03b7'].source = cells['e38b03b7'].source.replace('{len(POSITION_RANK_SPECIAL_CASES)}', '{log_case_count}')
replace('e38b03b7', '''    try:
        for special_case in POSITION_RANK_SPECIAL_CASES:''', '''    try:
        resolved_raw_root = raw_root.resolve()
        for special_case in POSITION_RANK_SPECIAL_CASES:''')
replace('e38b03b7', 'artifact_path.resolve().is_relative_to(raw_root.resolve())', 'artifact_path.resolve().is_relative_to(resolved_raw_root)')
cells['db16e4ab'].source += '''

固定目标合约与响应记录总数在原文行循环前读取一次。每行的合约清理、类型检查、筛选，以及最终名次覆盖、非递增和冻结值比较继续保留。响应摘要不能替代这些显式内容约束；JSON 解码、官方行排序和清单行转为 JSON 列表各有不同用途，本轮没有确认可删除的重复转换。'''
cells['b01a-verify-heading'].source += '''

这里没有“逐分区重复全表检查”：本环节不使用 DataFrame、Arrow Dataset 或 Hive 分区。每个案例只读取自己的三个证据文件；已有证据执行一次完整复读，新归档则分别验收网络字节、staging 和正式目录。三个验收位置承担不同责任，继续保留；不从 raw 契约套用 silver 的上游信任规则。'''
cells['ca173bcd'].source += '''

配置案例总数和用于边界判断的规范 raw 根路径在案例循环前计算一次；每个案例目录仍分别解析并检查边界。固定值的外移不改变请求次数、现有证据分支、状态汇总或文件提交顺序。'''
replace('b01a-flow-response', '沿原循环报告扫描进度；筛选目标合约并核对类型', '循环前固定目标合约及记录数；逐行筛选、核对并报告进度')
replace('b01a-flow-cli', '运行开始；逐案例检查路径', '循环前固定 raw 根及案例数；逐案例检查路径')

assert notebook.metadata == before.metadata
for old in before.cells:
    new = cells[old.id]
    assert {k:v for k,v in old.items() if k!='source'} == {k:v for k,v in new.items() if k!='source'}
    if old.cell_type == 'code':
        comments = lambda text: [t.string for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type==tokenize.COMMENT]
        assert comments(old.source) == comments(new.source)
        ast.parse(new.source)
assert cells['30c24e87'] == next(c for c in before.cells if c.id=='30c24e87')
nbformat.validate(notebook)
nbformat.write(notebook, PATH)
print('b01a: 4 loop invariants hoisted; all raw validation/conversion boundaries preserved; descriptions updated.')
