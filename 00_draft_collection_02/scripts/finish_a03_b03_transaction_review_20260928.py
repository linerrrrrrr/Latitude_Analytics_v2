"""收尾：读取函数使用其传入的 Hive 分区定义；说明准确展示循环与日志。"""
from pathlib import Path
import ast
import nbformat

path = Path(__file__).resolve().parents[2]/'02_Futures_Lakehouse/a03_External_Market_Data/b03_overseas_futures.ipynb'
notebook = nbformat.read(path, as_version=4)
cells = {c.id:c for c in notebook.cells}
cell = cells['b03-c03-09']
cell.source = cell.source.replace('        expected_file_schema = pa.schema(', '        partition_columns = partitioning.schema.names\n        expected_file_schema = pa.schema(')
cell.source = cell.source.replace('if field.name not in CALENDAR_PARTITION_COLUMNS', 'if field.name not in partition_columns')
cell = cells['a03-b03-calendar-validation']
cell.source = cell.source.replace('# 日历消费者只检查当前调度和状态回写直接依赖的关系。', '# 日历状态生产者对待提交的 dirty 完整叶执行业务验收。')
cell.source = cell.source.replace('calendar_keys_df = calendar_table.select(CALENDAR_PRIMARY_KEY).to_pandas()\n        log_phase = "primary_key"', 'calendar_keys_df = calendar_table.select(CALENDAR_PRIMARY_KEY).to_pandas()')
cell = cells['a03-b03-fact-commit']
cell.source = cell.source.replace('f"path={target_path}; transaction_state=pending;', 'f"path={destination_path if complete_table.num_rows else target_marker_path}; transaction_state=pending;')
cell = cells['a03-b03-calendar-commit']
cell.source = cell.source.replace('f"path={target_path}; transaction_state=pending;', 'f"path={destination_path}; transaction_state=pending;')
cell = cells['a03-b03-flow-overview']
cell.source = cell.source.replace('D -->|是| E["认证；按月逐日查询与归一化"]', 'D -->|是| N["认证一次；准备月份循环"]\n    N --> E["当前月逐日查询与归一化"]')
cell.source = cell.source.replace('    G --> Z', '    G --> M{"还有月份？"}\n    M -->|是| E\n    M -->|否| Z')
cell = cells['a03-b03-flow-calendar-commit']
cell.source = cell.source.replace('    E --> F["全部触达叶成功；报告完成数；无独立水位"]', '    E --> M{"还有日历叶？"}\n    M -->|是| B\n    M -->|否| F["全部触达叶成功；报告完成数；无独立水位"]')
cells['a03-b03-flow-13'].source = cells['a03-b03-flow-13'].source.replace('返回三个集合的结果', '返回三类结果与可复用计数和 warning 映射')
for cell in notebook.cells:
    if cell.cell_type=='code': ast.parse(cell.source)
nbformat.validate(notebook)
path.write_text(nbformat.writes(notebook)+'\n', encoding='utf8', newline='\n')
