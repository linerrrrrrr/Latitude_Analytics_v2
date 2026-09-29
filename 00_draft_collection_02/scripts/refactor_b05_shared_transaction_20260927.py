"""将 b05 的同组安装/恢复交给既有共享模块，并拆出执行入口。"""

import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / "02_Futures_Lakehouse/a01_Futures_Market_Data/b05_futures_daily.ipynb"
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)


replace("a68d585d", "from config.settings import settings",
        "from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction")
replace("b43616a1", '    log_rollback_status = "not_required"\n', '')
source = cells["b43616a1"].source
start = source.index('        backup_root.mkdir(parents=True, exist_ok=False)')
end = source.index('        for log_spec in prepared_specs:', start)
install_start = source.index('            # 同一日历叶涉及', start)
install_end = source.index('            commit_succeeded = True', install_start)
install = source[install_start:install_end]
old_move_start = install.index('                saved_path = (')
old_move_end = install.index('                marker_path = ', old_move_start)
install = install[:old_move_start] + '''                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path,
                )

''' + install[old_move_end:]
install = install.replace('''                if not marker_path.exists():
                    pq.write_table(
                        pa.Table.from_batches([], schema=spec["file_schema"]),
                        marker_path,
                    )
                    created_markers.append(marker_path)
''', '''                if not marker_path.exists():
                    log_phase = "schema_marker_install"
                    click.echo(
                        f"planning_progress: table={table_name}; function=commit_validated_leaf_group; phase=schema_marker_install; status=started; "
                        f"run_id={run_id}"
                    )
                    staged_marker_path = staging_root / table_name / "schema.parquet"
                    pq.write_table(
                        pa.Table.from_batches([], schema=spec["file_schema"]),
                        staged_marker_path,
                    )
                    transaction.replace(
                        target_path=marker_path,
                        staged_path=staged_marker_path,
                        quarantine_new=False,
                    )
                    with pq.ParquetFile(marker_path) as marker_file:
                        if marker_file.metadata.num_rows != 0 or not physically_and_identity_compatible(
                            marker_file.schema_arrow, spec["file_schema"]
                        ):
                            raise ValueError("正式 schema.parquet 必须匹配文件契约且为 0 行。")
''')
install = install.replace('len(moved_specs)', 'len(prepared_specs)')
install = install.replace('''            for spec, destination_path, _ in moved_specs:
                target_table_path = silver_root / spec["table_name"]
''', '''            for spec in prepared_specs:
                target_table_path = silver_root / spec["table_name"]
                destination_path = target_table_path / spec["relative_path"]
''')
source = source[:start] + '''        with StagedPathTransaction(
            root_path=silver_root,
            staging_dir=staging_root,
            backup_dir=backup_root,
            quarantine_dir=quarantine_root,
            log_context=f"table={TABLE_NAME}; function=commit_validated_leaf_group; run_id={run_id}",
        ) as transaction:
''' + install + '\n' + source[end:]
source = source.replace('failed_phase={log_phase}; rollback={log_rollback_status}; ', 'failed_phase={log_phase}; ')
cells["b43616a1"].source = source

replace('71d11aec', '\n\nif __name__ == "__main__":\n    main()', '')
cells['720c2940'].source = '''## 事实与日历叶的协调提交

`commit_validated_leaf_group()` 接收已经过完整业务校验的叶规格，检查非空、分区范围和目标唯一性，再转换为 Arrow 并核对表根零行标记。写入 staging 后，仅复读文件物理契约、主键唯一性和行数；全部通过才进入共享 `StagedPathTransaction` 安装。

一组包含一个 `1d—交易所—年月` 日历叶及其本次触达的全部事实叶，整个组共用一次事务。缺失的 `schema.parquet` 先写入 staging，再纳入同组安装，并正式复读确认物理契约及零行要求；已有兼容标记保持原样，描述性 metadata 差异不触发改写。

正式安装后仍只精确复读本组叶的物理契约、主键和行数。安装或正式复读失败时，共享模块按实际移动记录逆序移除本次新建标记、隔离失败新叶并恢复旧叶；恢复不完整时保留备份。此前已经成功提交的其他组不回滚。这是进程内协调恢复，不提供跨目录原子可见性、进程终止后的自动恢复或并发写入协调。

规格、标记门禁、staging 写入及复读、安装和正式复读日志仍由本函数记录。staging 准备失败由本函数清理；事务内失败由共享模块恢复并报告结果，本函数随后报告失败阶段及耗时。只有整组正式验收通过并退出事务后才发布 `persisted=true` 和组完成日志；入口仅保留整批累计提交量。

规格准备阶段为每个叶构造一次文件 Schema，后续零行标记检查、staging 复读、标记创建和正式复读均复用它；不改变这些不同 I/O 阶段各自的验收。准备阶段的 Pandas→Arrow 转换仍是实际写 Parquet 所需，不移除。'''
replace('b05-flow-commit', '### 流程：单组暂存、安装与失败恢复', '### 流程：单组暂存、共享安装与失败恢复')
replace('b05-flow-commit', '备份旧叶；安装本组新叶；补标记', '共享事务安装整组叶；暂存、安装并验收新标记')
replace('b05-flow-commit', '逆序移除新标记、隔离新叶、恢复旧叶', '共享模块逆序移除新标记、隔离新叶、恢复旧叶')
replace('43d2aa31', '完成本组所有正式复读和原有清理后', '完成本组所有正式复读并成功退出共享事务后')
replace('c2d11e3b', '当前单元格仍包含 `if __name__ == "__main__": main()`：普通模块导入不启动业务，直接运行 `.py` 使用命令行参数；在 Notebook 中执行本格也可能启动 Click 并读取内核参数。执行单元格尚未改为 b01 的独立参数形式，可通过同名导出脚本运行。', '本单元格只定义 Click 命令，不启动业务。最后的独立执行单元格区分 Notebook 与脚本环境：Notebook 显式传入参数；直接运行 `.py` 读取命令行参数；普通模块导入不触发入口。')

reference = nbformat.read(PATH.with_name('b04_futures_bar_calendar.ipynb'), as_version=4)
for reference_cell in reference.cells[-5:]:
    cell = copy.deepcopy(reference_cell)
    cell.id = cell.id.replace('b04-', 'b05-')
    cell.source = cell.source.replace('b04_futures_bar_calendar', 'b05_futures_daily')
    if cell.cell_type == 'code':
        cell.outputs = []
        cell.execution_count = None
    notebook.cells.append(cell)
cells = {cell.id: cell for cell in notebook.cells}
cells['b05-entry-heading'].source = '''## 执行入口

Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数。当前单元格使用与 b01—b04 一致的显式日期只读示例（2026-08-01 至 2026-08-15），不带 `--write`；存在未完成待办时仍会调用 JQData 采集并校验，但不提交事实或回写日历。脚本运行时使用命令行参数，模式与写入限制见开篇表格。在 Notebook 中导入同名 Python 模块不会触发入口。'''
replace('b05-flow-entry', '当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会进入本地生成与规划流程。流程图本身不执行代码。', '当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会规划待办，并在存在请求批次时采集和校验。流程图本身不执行代码。')
replace('b05-flow-manual', '默认尾部更新并提交', '按白名单与完成凭证规划；采集并逐组提交')

# 原有代码注释逐项保留，输出、计数和元数据均不改动。
for old_cell in before.cells:
    new_cell = cells[old_cell.id]
    assert old_cell.metadata == new_cell.metadata
    if old_cell.cell_type == 'code':
        assert old_cell.outputs == new_cell.outputs
        assert old_cell.execution_count == new_cell.execution_count
        comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type == tokenize.COMMENT]
        assert comments(old_cell.source) == comments(new_cell.source), old_cell.id
nbformat.validate(notebook)
nbformat.write(notebook, PATH)
print(f'updated {PATH}; cells={len(notebook.cells)}')

for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    content = path.read_text(encoding='utf-8')
    content = content.replace('b01、b02、b03、b04 共用', 'b01、b02、b03、b04、b05 共用')
    content = content.replace('b01、b02、b03、b04 已共用', 'b01、b02、b03、b04、b05 已共用')
    content = content.replace('b02、b03、b04 保留隔离', 'b02、b03、b04、b05 保留隔离')
    detail = 'b05 将一个 `1d—交易所—年月` 日历叶、本次触达的全部对应事实叶及必要的新建零行标记纳入同一事务；新标记先暂存再安装并验收，已有兼容标记保持原样。整组成功后才报告完成凭证已落盘，此前成功组保留，b05 没有独立日期水位文件。'
    if relative == '02_Futures_Lakehouse/AGENTS.md':
        content = content.replace('其他入口继续使用现有实现，按用户选定的环节逐项接入。', detail + '其他入口继续使用现有实现，按用户选定的环节逐项接入。')
    elif relative == '02_Futures_Lakehouse/README.md':
        content = content.replace('该模块使用同一文件系统内的路径替换', detail + '该模块使用同一文件系统内的路径替换')
    path.write_text(content, encoding='utf-8', newline='\n')
