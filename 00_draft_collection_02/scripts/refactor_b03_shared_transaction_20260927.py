"""将 b03 的当前叶安装接入正式共享事务，并对齐独立执行单元格。"""

import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
notebook_path = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b03_futures_contract_calendar.ipynb'
notebook = nbformat.read(notebook_path, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace_once(cell_id, old, new):
    source = cells[cell_id].source
    assert source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = source.replace(old, new)


replace_once('c03-initialization', 'from config.settings import settings',
             'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')
replace_once('c03-commit', '    rollback_status = "not_required"\n', '')
source = cells['c03-commit'].source
start = source.index('        commit_phase = "prepare_install"')
end = source.index('        click.echo(\n            f"partition_committed:', start)
readback_start = source.index('            if destination_path.is_dir():', start)
readback_end = source.index('            commit_succeeded = True', readback_start)
readback = source[readback_start:readback_end]
source = source[:start] + '''        commit_phase = "prepare_install"
        source_path = staging_path / relative_path
        marker_path = target_path / "schema.parquet"
        with StagedPathTransaction(
            root_path=target_path,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=f"table={TABLE_NAME}; partition={partition_key}; run_id={run_id}",
        ) as transaction:
            commit_phase = "install"
            click.echo(
                f"partition_start: table={TABLE_NAME}; phase=install; status=started; "
                f"partition={partition_key}; completed=0; total=1; run_id={run_id}"
            )
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path if len(complete_partition_table) else None,
            )

            commit_phase = "formal_readback"
            click.echo(
                f"planning_progress: table={TABLE_NAME}; phase=formal_readback; status=started; "
                f"partition={partition_key}; run_id={run_id}"
            )
            if (
                not len(complete_partition_table)
                and not marker_path.exists()
                and not any(path.name != "schema.parquet" for path in target_path.rglob("*.parquet"))
            ):
                commit_phase = "empty_marker"
                staged_marker_path = staging_path / "schema.parquet"
                pq.write_table(
                    pa.Table.from_batches([], schema=CONTRACT_CALENDAR_PHYSICAL_SCHEMA),
                    staged_marker_path,
                )
                transaction.replace(
                    target_path=marker_path, staged_path=staged_marker_path,
                    quarantine_new=False,
                )
                with pq.ParquetFile(marker_path) as marker_file:
                    validate_compatible_dataset_schema(
                        marker_file.schema_arrow, CONTRACT_CALENDAR_PHYSICAL_SCHEMA, "正式空表 marker ",
                    )
                    if marker_file.metadata.num_rows != 0:
                        raise ValueError("正式空表 marker 必须为零行。")
                commit_phase = "formal_readback"

''' + readback + '\n' + source[end:]
source = source.replace('failed_phase={commit_phase}; rollback={rollback_status}; ', 'failed_phase={commit_phase}; ')
cells['c03-commit'].source = source

replace_once('c03-cli', '\n\nif "ipykernel" not in sys.modules and __name__ == "__main__":\n    main()', '')
replace_once('c03-intro', '单分区提交 → 三模式入口。', '单分区提交 → 三模式入口 → 执行单元格。')
replace_once('b03-flow-overview', '按失败阶段清理或恢复当前叶', '按失败阶段清理或由共享事务恢复当前叶')
replace_once('c03-table-layout', '失败时只尝试恢复当前分区；此前已成功分区保留。',
             '每个分区各自使用 `StagedPathTransaction`；失败时只恢复该次实际移动的叶和新建空表标记，此前已成功分区保留。')
cells['c03-commit-heading'].source = '''## 单分区暂存、提交与失败回滚

`commit_partition()` 依次执行替换范围检查、精确读取旧叶、按模式合并、完整叶业务校验，再写 staging 并复读。合并后的完整叶负责一次完整业务校验，提交输入不再单独重复检查；序列化时按同一权威 Schema 构造 Arrow 表，不再调用已完成的校验。入口的全表检查保留一次；每次提交仅打开当前叶，并用 `partition_base_dir` 还原 Hive 分区列。独立调用时也检查该叶物理契约，不检查其他叶。

staging 通过后，在 `StagedPathTransaction` 的 `with` 中安装当前叶，再由本函数检查正式路径的物理 Schema、表身份 metadata、主键与行数。合并结果为零行时显式传入 `staged_path=None` 删除该叶；应有数据但 staging 缺失会报错，不推断为删除。全表清空且没有标记时，先在 staging 写零行 `schema.parquet`，由同一事务安装并正式复读。

staging 失败由本函数清理后抛出；安装或正式验收失败由共享模块按实际移动记录倒序恢复。失败的新叶保留在隔离目录，本次新建空表标记直接移除；恢复不完整时保留旧备份。一次调用只有一个叶及其必要的新建标记，此前成功叶不参与回滚。自动水位仍在本批分区成功后另行原子写入。

本函数报告输入、旧叶读取、合并校验、暂存复读、正式安装与完成耗时；共享模块负责恢复结果日志。本函数在事务退出及清理结束后用 `planning_progress` 报告 `failed_phase` 和异常，保留 monitor 可识别的前缀；`partition_committed` 只在事务成功退出后报告完成。'''
cells['b03-flow-commit'].source = '''### 局部流程：单分区提交与失败恢复

合并、Parquet 写入和物理验收留在 b03；共享模块仅负责路径安装与恢复。空表标记与当前叶共同回滚，自动水位由 main 在本批成功后另行推进。

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["检查替换范围；精确读取当前旧叶"] --> B["按键、日期区间或整叶合并"]
    B --> C["完整叶业务校验一次；按 Schema 序列化"]
    C --> D{"完整叶有行？"}
    D -->|是| E["写 staging；复读 Schema、主键及行数"]
    D -->|否| F["明确删除当前叶"]
    E -. 暂存失败 .-> X["清理 staging；记录失败并抛错"]
    E --> G["进入单叶共享事务；备份并安装或删除"]
    F --> G
    G --> H["必要时暂存并安装空表标记；正式复读验收"]
    G -. 安装失败 .-> R["共享事务倒序恢复实际移动的目标"]
    H -. 验收失败 .-> R
    R --> S["隔离失败新叶；移除新标记；恢复失败则保留备份"]
    S --> U["清理 staging；记录失败并抛错"]
    H --> T["成功退出事务；清理临时目录；记录完成并返回"]
```'''
replace_once('c03-cli-heading',
             '当前单元格在 Notebook 中只定义入口，不自动运行；直接运行同名 `.py` 时读取命令行参数并调用 `main()`。本轮不改写执行入口。',
             '当前单元格只定义 `main()`。后面的独立执行单元格与 b01、b02 一致：Notebook 使用显式只读参数，Python 脚本读取命令行参数，模块导入不触发采集。')

b01 = nbformat.read(notebook_path.with_name('b01_trade_calendar.ipynb'), as_version=4)
for reference, new_id in zip(b01.cells[-5:], (
    'b03-entry-heading', 'b03-flow-entry', 'b03-entry', 'b03-flow-manual', 'b03-manual',
)):
    new_source = reference.source.replace('b01_trade_calendar', 'b03_futures_contract_calendar')
    cell = (nbformat.v4.new_code_cell(new_source) if reference.cell_type == 'code'
            else nbformat.v4.new_markdown_cell(new_source))
    cell.id = new_id
    notebook.cells.append(cell)


def comments(cell):
    return [item.string for item in tokenize.generate_tokens(io.StringIO(cell.source).readline)
            if item.type == tokenize.COMMENT] if cell.cell_type == 'code' else []


assert notebook.metadata == before.metadata
for old, new in zip(before.cells, notebook.cells):
    assert old.id == new.id and old.metadata == new.metadata
    assert comments(old) == comments(new), old.id
    if old.cell_type == 'code':
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
nbformat.validate(notebook)
notebook_path.write_text(nbformat.writes(notebook), encoding='utf-8', newline='\n')
print('Updated b03 source, retained all existing comments and notebook execution state.')
