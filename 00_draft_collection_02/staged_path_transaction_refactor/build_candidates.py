"""从本轮快照构造可审阅候选；不覆盖正式 Notebook。"""

import json
import pathlib

import nbformat
from nbconvert.exporters import PythonExporter


DRAFT = pathlib.Path(__file__).resolve().parent
BASELINE = pathlib.Path(json.loads((DRAFT / 'baseline.json').read_text(encoding='utf-8'))['root'])
WORKFLOW = pathlib.Path('02_Futures_Lakehouse/a01_Futures_Market_Data')


def replace_once(source, before, after):
    assert source.count(before) == 1, (source.count(before), before)
    return source.replace(before, after, 1)


for stem in ('b01_trade_calendar', 'b02_futures_variety_calendar'):
    notebook = nbformat.read(BASELINE / WORKFLOW / f'{stem}.ipynb', as_version=4)
    import_cell = next(cell for cell in notebook.cells if cell.cell_type == 'code' and 'import uuid' in cell.source)
    import_cell.source += '\nfrom a00_04_staged_path_transaction import StagedPathTransaction\n'
    commit_cell = next(cell for cell in notebook.cells if cell.cell_type == 'code' and 'def commit_partitions(' in cell.source)
    source = commit_cell.source
    if stem.startswith('b01'):
        import_cell.source = replace_once(import_cell.source, 'import shutil\n', '')
        source = replace_once(source, '    # moved_partitions 记录已经开始替换的叶分区，用于提交失败时倒序回滚。\n',
                              '    # 共享事务记录实际移动；安装或正式验收失败时倒序回滚。\n')
        start = source.index('    moved_partitions = []')
        end = source.index('        # 第一阶段', start)
        source = source[:start] + '''    with StagedPathTransaction(
        root_path=calendar_path, staging_dir=staging_dir, backup_dir=backup_dir,
        log_context=f'table={TABLE_NAME}; run_id={run_id}',
    ) as transaction:

''' + source[end:]
        source = replace_once(source, '        backup_dir.mkdir(parents=True, exist_ok=False) # 建立本批独立备份目录\n', '')
        source = replace_once(source, '            backup_partition_path = backup_dir / partition_relative_path\n', '')
        start = source.index('            # 确保正式分区和备份分区的父目录存在')
        end = source.index('            staged_partition_row_count = ', start)
        source = source[:start] + source[end:]
        start = source.index('            # 如果正式分区已经存在，先把旧分区整体移动到 backup')
        end = source.index('            # 正式安装后只复读', start)
        source = source[:start] + '''            # 如果正式分区已经存在，先把旧分区整体移动到 backup。
            # 共享事务登记实际移动，再将已验证的 staging 分区安装到正式位置。
            transaction.replace(target_path=formal_partition_path, staged_path=staged_partition_path)
''' + source[end:]
        start = source.index('    # try 内任何步骤失败都会进入回滚')
        end = source.index("    click.echo(\n        f'committed:", start)
        source = source[:start] + source[end:]
    else:
        source = replace_once(source, '    moved_partition_records = []\n    variety_calendar_backup_dir.mkdir(parents=True, exist_ok=False)\n', '')
        source = replace_once(source, '    commit_succeeded = False\n', '')
        source = replace_once(source, '    schema_marker_created = False\n', '')
        source = replace_once(source, '''    try:
        for partition_index, (exchange_code, year, month) in enumerate(touched_partition_keys, start=1):''', '''    with StagedPathTransaction(
        root_path=variety_calendar_path, staging_dir=variety_calendar_staging_dir,
        backup_dir=variety_calendar_backup_dir, quarantine_dir=variety_calendar_quarantine_dir,
        log_context=f"table={TABLE_NAME}; run_id={run_id}",
    ) as transaction:
        for partition_index, (exchange_code, year, month) in enumerate(touched_partition_keys, start=1):''')
        source = replace_once(source, '            backup_partition_path = variety_calendar_backup_dir / partition_relative_path\n', '')
        start = source.index('            formal_partition_path.parent.mkdir(parents=True, exist_ok=True)')
        end = source.index('            if formal_partition_path.is_dir():', start)
        source = source[:start] + '''            expected_partition_row_count = int(
                expected_partition_row_count_by_key.get((exchange_code, year, month), 0)
            )
            # 合并结果为零行才明确删除；应有数据但 staging 缺失时由事务报错。
            transaction.replace(
                target_path=formal_partition_path,
                staged_path=staged_partition_path if expected_partition_row_count else None,
            )
''' + source[end:]
        source = replace_once(source, '''            schema_marker_created = True
            pq.write_table(pa.Table.from_batches([], schema=expected_partition_schema), schema_marker_path)''', '''            staged_schema_marker_path = variety_calendar_staging_dir / "schema.parquet"
            pq.write_table(pa.Table.from_batches([], schema=expected_partition_schema), staged_schema_marker_path)
            transaction.replace(
                target_path=schema_marker_path, staged_path=staged_schema_marker_path,
                quarantine_new=False,
            )''')
        start = source.index('        commit_succeeded = True')
        end = source.index('    click.echo(\n        f"committed:', start)
        source = source[:start] + source[end:]
    commit_cell.source = source
    for cell in notebook.cells:
        if cell.cell_type != 'markdown':
            continue
        if cell.id.endswith('commit-heading'):
            cell.source += (
                '\n\n安装与失败恢复共用 `StagedPathTransaction`：环节仍按顺序合并、写入并验收 staging，'
                '在事务 `with` 中逐分区安装并正式复读；整组退出成功才完成提交。'
                '正式复读抛出异常时，共享事务恢复本次实际移动的目标。'
            )
            if stem.startswith('b02'):
                cell.source += '删除由合并后的预期行数为零明确决定；应有数据但 staging 缺失会报错。空表 marker 先写入 staging，再由同一事务安装、正式复读和恢复。'
        if cell.id.startswith(('b01-flow-', 'b02-flow-')) and 'commit' in cell.id:
            cell.source = cell.source.replace('旧年份移入备份；登记回滚记录；安装新年份', '共享事务：备份旧年份；登记实际移动；安装新年份')
            cell.source = cell.source.replace('按已登记记录倒序删除新分区、恢复旧分区', '共享事务：按实际移动倒序删除新分区、恢复旧分区')
            cell.source = cell.source.replace('逐叶：备份旧叶、登记回滚、安装新叶或清空', '共享事务：备份旧叶；按预期行数安装或显式删除')
            cell.source = cell.source.replace('写零行 schema marker 并复读', 'staging 写零行 marker；共享事务安装；正式复读')
            cell.source = cell.source.replace('移除本批 marker；倒序隔离新叶、恢复旧叶', '共享事务倒序恢复：移除新 marker；隔离新叶、恢复旧叶')
            cell.source += '\n\n共享事务负责安装状态记录、失败恢复与清理；逐分区正式验收和进度仍由本环节负责。'
        if cell.id in ('b01-flow-overview', 'b02-flow-overview'):
            cell.source = cell.source.replace('逐年安装与正式复读', '共享事务逐年安装与正式复读')
            cell.source = cell.source.replace('逐叶安装与正式复读', '共享事务逐叶安装与正式复读')
    nbformat.validate(notebook)
    notebook_path = DRAFT / f'{stem}.ipynb'
    with notebook_path.open('w', encoding='utf-8', newline='\n') as notebook_file:
        nbformat.write(notebook, notebook_file)
    script, _ = PythonExporter().from_notebook_node(notebook)
    notebook_path.with_suffix('.py').write_text(script, encoding='utf-8', newline='\n')
    compile(script, str(notebook_path.with_suffix('.py')), 'exec')
    print(f'candidate: {notebook_path.name}')
