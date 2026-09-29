"""接入 b04 单叶共享事务，并对齐 Notebook/脚本独立执行单元格。"""

import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / "02_Futures_Lakehouse/a01_Futures_Market_Data/b04_futures_bar_calendar.ipynb"
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new):
    source = cells[cell_id].source
    assert source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = source.replace(old, new)


replace("c04-imports", "from config.settings import settings",
        "from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction")
replace("c04-commit", '    log_rollback_status = "not_required"\n', '')
source = cells['c04-commit'].source
start = source.index('        source_path = staging_path / relative_path\n        destination_path')
end = source.index('        click.echo(\n            f"partition_committed:', start)
readback_start = source.index('            log_phase = "formal_readback"', start)
readback_end = source.index('            commit_succeeded = True', readback_start)
readback = source[readback_start:readback_end]
source = source[:start] + '''        source_path = staging_path / relative_path
        destination_path = target_path / relative_path
        marker_path = target_path / "schema.parquet"

        # 正式替换使用备份和隔离目录，异常时恢复原分区。
        log_phase = "prepare_install"
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=prepare_install; "
            f"status=started; partition={partition_key}"
        )
        with StagedPathTransaction(
            root_path=target_path,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=f"table={TABLE_NAME}; partition={partition_key}; run_id={run_id}",
        ) as transaction:
            log_phase = "install"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=install; "
                f"status=started; partition={partition_key}"
            )
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path if len(complete_partition_table) else None,
            )

            log_phase = "schema_marker"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; function=commit_partition; phase=schema_marker; "
                f"status=started; partition={partition_key}"
            )
            marker_needs_install = not marker_path.exists()
            if marker_path.exists():
                marker_schema = pq.read_schema(marker_path)
                if not marker_schema.equals(
                    PARQUET_FILE_SCHEMA,
                    check_metadata=True,
                ):
                    marker_metadata = pq.read_metadata(marker_path)
                    if marker_metadata.num_rows:
                        raise ValueError("schema.parquet 必须是 0 行契约标记。")
                    marker_needs_install = True

            if marker_needs_install:
                staged_marker_path = staging_path / "schema.parquet"
                pq.write_table(
                    pa.Table.from_batches([], schema=PARQUET_FILE_SCHEMA),
                    staged_marker_path,
                )
                transaction.replace(
                    target_path=marker_path,
                    staged_path=staged_marker_path,
                    quarantine_new=False,
                )
                with pq.ParquetFile(marker_path) as marker_file:
                    if not marker_file.schema_arrow.equals(
                        PARQUET_FILE_SCHEMA, check_metadata=True
                    ) or marker_file.metadata.num_rows != 0:
                        raise ValueError("正式 schema.parquet 必须匹配文件契约且为 0 行。")

''' + readback + source[end:]
source = source.replace('failed_phase={log_phase}; rollback={log_rollback_status}; ', 'failed_phase={log_phase}; ')
cells['c04-commit'].source = source

replace('c04-cli', '\n\nif __name__ == "__main__":\n    main()', '')
replace('c04-introduction', cells['c04-introduction'].source,
        cells['c04-introduction'].source + '\n\n函数定义与执行入口分开：依次运行定义单元格不会启动业务；最后的执行入口使用显式 Notebook 参数，或由同名脚本读取命令行参数。')
replace('b04-flow-overview', '停止后续；按原路径恢复当前叶；保留此前成功叶',
        '停止后续；共享事务恢复当前叶及变更标记；保留此前成功叶')
cells['c04-commit-notes'].source = '''## 单叶暂存、共享安装与失败恢复

`commit_partition()` 检查输入分区和替换范围，合并显式日期以外的保留行，再对 dirty 完整叶执行一次业务校验。非空结果写入 staging，复读文件 Schema/metadata、主键与行数后，再进入 `StagedPathTransaction` 安装；空结果以 `staged_path=None` 显式删除该叶，应有数据但来源路径缺失则报错。自动和 full 模式由调用方传入完整目标叶，显式日期写入只允许用于非正式湖。

一次共享事务只负责当前叶及本次变更的 `schema.parquet`。标记不存在时新建；已存在但与当前文件 Schema/metadata 不同的零行标记会替换。新标记先写 staging，再由同一事务备份、安装并正式复读，确认文件契约和零行要求。已经匹配的标记保留原文件。这个标记不保存 b03 那样的已处理日期水位。

正式叶仍只检查当前叶的物理契约、主键和行数。安装或验收失败时，共享模块按实际移动记录倒序恢复旧标记和旧叶；失败的新叶隔离，新标记直接移除；恢复不完整时保留备份。此前成功叶不参与回滚。staging 写入或复读失败由本函数清理；事务开始后的清理与恢复由共享模块负责。

读取、合并校验、暂存、安装、标记与正式叶复读阶段仍由对应函数报告。共享模块报告恢复结果；本函数在事务退出后报告失败阶段及耗时，成功退出后才报告 `partition_committed`。业务合并、状态继承、水位选择和返回行数语义保持不变。'''
cells['b04-flow-commit'].source = '''### 局部流程：单叶提交与共享恢复

共享模块负责路径安装与恢复；业务校验、文件写入和正式验收留在 b04。当前叶与本次变更的契约标记共同回滚。

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["记录开始；检查范围并保留区间外行"] --> B["完整叶业务校验；转换 Arrow"]
    B --> C{"结果有行？"}
    C -->|是| D["写 staging；复读文件契约、主键和行数"]
    C -->|否| E["明确删除当前叶"]
    D -. 暂存失败 .-> X["本函数清理 staging；抛错"]
    D --> F["进入共享事务；备份并安装或删除当前叶"]
    E --> F
    F --> G["必要时暂存、安装并验收零行 schema.parquet"]
    G --> H["仅复读正式当前叶；检查物理契约、主键和行数"]
    F -. 安装失败 .-> R["共享事务倒序恢复实际移动的标记与当前叶"]
    G -. 标记失败 .-> R
    H -. 验收失败 .-> R
    R --> S["隔离失败新叶；移除新标记；恢复不完整保留备份"]
    S --> U["清理并记录恢复结果；本函数报告失败阶段并抛错"]
    H --> T["成功退出并清理；记录完成，返回输入行数"]
```'''
replace('c04-cli-notes', '当前代码单元格仍包含 `if __name__ == "__main__": main()`。直接运行 `.py` 使用命令行参数，普通导入不启动业务；Notebook 中若 `__name__` 为 `__main__`，执行本格也会调用 Click 并读取内核参数。本轮不改写执行单元格，实际运行可使用同名导出脚本。',
        '本格只定义 Click 入口；实际启动由后面的独立执行单元格负责。Notebook 显式传入参数，脚本使用命令行参数，普通导入不启动业务。')
replace('b04-flow-main', '入口调用仍保留在本代码单元格末尾。', '实际调用位于独立执行单元格。')

b03 = nbformat.read(ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b03_futures_contract_calendar.ipynb', as_version=4)
for original in b03.cells[-5:]:
    cell = copy.deepcopy(original)
    cell.id = cell.id.replace('b03', 'b04').replace('c03', 'c04')
    cell.source = cell.source.replace('b03_futures_contract_calendar', 'b04_futures_bar_calendar')
    if cell.cell_type == 'code':
        cell.outputs = []
        cell.execution_count = None
    else:
        cell.source = cell.source.replace('b01、b02 一致', 'b01—b03 一致').replace('执行会发起来源请求', '执行只读取本地 b03/b04 并输出计划，不调用外部 API').replace('进入采集流程', '进入本地生成与规划流程').replace('不触发采集', '不触发业务')
    notebook.cells.append(cell)

assert notebook.metadata == before.metadata
for original in before.cells:
    current = cells[original.id]
    assert original.metadata == current.metadata
    if original.cell_type == 'code':
        assert original.outputs == current.outputs and original.execution_count == current.execution_count
        comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
        assert comments(original.source) == comments(current.source), original.id
for cell in notebook.cells:
    if cell.cell_type == 'code':
        compile(cell.source, f'{PATH}:{cell.id}', 'exec')
nbformat.validate(notebook)
nbformat.write(notebook, PATH)
print(PATH)

# Synchronize only the existing descriptions affected by this new caller.
for name in ['AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md']:
    path = ROOT / name
    content = path.read_text(encoding='utf-8')
    content = content.replace('a01/b01、b02、b03 共用', 'a01/b01、b02、b03、b04 共用')
    content = content.replace('a01/b01、b02、b03 已共用', 'a01/b01、b02、b03、b04 已共用')
    if name == '02_Futures_Lakehouse/AGENTS.md':
        content = content.replace('其他入口继续使用现有实现，按用户选定的环节逐项接入。',
            'b04 同样保持逐叶事务，当前叶与本次新建或替换的零行契约标记共同恢复；它没有独立日期水位文件。其他入口继续使用现有实现，按用户选定的环节逐项接入。')
    if name == '02_Futures_Lakehouse/README.md':
        content = content.replace('b02、b03 保留隔离的新分区', 'b02、b03、b04 保留隔离的新分区')
        content = content.replace('该模块使用同一文件系统内的路径替换',
            'b04 每次将当前叶与本次新建或替换的零行契约标记纳入同一事务；新标记先暂存再安装并验收，已匹配标记保持原样，b04 不另写日期水位文件。该模块使用同一文件系统内的路径替换')
    path.write_text(content, encoding='utf-8', newline='\n')
    print(path)
