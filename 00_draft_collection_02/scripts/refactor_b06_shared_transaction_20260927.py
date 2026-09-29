"""将 b06 单叶安装/恢复接入共享模块，并对齐 Notebook 独立执行入口。"""

import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b06_futures_minute.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)


replace('5158a600', 'from config.settings import settings',
        'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')
source = cells['41986f77'].source
start = source.index('        saved_path = backup_path / relative_path')
end = source.index('        click.echo(\n            f"partition_committed:', start)
install_start = source.index('            log_phase = "install"', start)
install_end = source.index('            commit_succeeded = True', install_start)
install = source[install_start:install_end]
install = install.replace('''            if destination_path.exists():
                shutil.move(str(destination_path), str(saved_path))
            shutil.move(str(source_path), str(destination_path))''', '''            transaction.replace(
                target_path=destination_path,
                staged_path=source_path,
            )''')
install = install.replace('''            else:
                pq.write_table(
                    pa.Table.from_batches([], schema=expected_file_schema),
                    marker_path,
                )
                marker_created = True''', '''            else:
                staged_marker_path = staging_path / "schema.parquet"
                pq.write_table(
                    pa.Table.from_batches([], schema=expected_file_schema),
                    staged_marker_path,
                )
                transaction.replace(
                    target_path=marker_path,
                    staged_path=staged_marker_path,
                    quarantine_new=False,
                )
                with pq.ParquetFile(marker_path) as marker_file:
                    if (
                        marker_file.metadata.num_rows != 0
                        or not physically_compatible(marker_file.schema_arrow, expected_file_schema)
                        or not schema_identity_compatible(marker_file.schema_arrow, expected_file_schema)
                    ):
                        raise ValueError("正式 schema.parquet 必须匹配文件契约且为 0 行。")''')
assert 'shutil.move' not in install and 'marker_created' not in install
cells['41986f77'].source = source[:start] + '''        marker_path = target_path / "schema.parquet"
        with StagedPathTransaction(
            root_path=target_path,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=(
                f"table={table_name}; function=commit_complete_partition; "
                f"partition={partition_key}; run_id={run_id}"
            ),
        ) as transaction:
''' + install + '\n' + source[end:]

replace('86251818', '\n\nif __name__ == "__main__":\n    main()', '')
cells['47bc969c'].source = '''## 单叶暂存、共享安装与失败恢复

`commit_complete_partition()` 接收事实或日历的完整目标叶，执行一次完整业务校验并转换为 Arrow。非空结果写入 staging；空结果仍写零行叶文件，不把空结果解释为删除整个分区。staging 只复读物理 Schema、身份 metadata、主键唯一性与行数，通过后进入共享 `StagedPathTransaction`。

一次事务包含当前叶及必要的新建 `schema.parquet`。已有标记仍检查零行、物理契约与表身份，兼容时原样保留；描述性 metadata 差异不触发改写。缺失标记先写入 staging，再由同一事务安装并正式复读，确认零行与文件契约。随后只精确复读当前正式叶的物理契约、主键和行数。

安装或正式验收失败时，共享模块按实际移动记录倒序恢复：移除本次新建标记、隔离失败新叶并恢复旧叶；恢复不完整时保留备份。第一次备份尚未成功的旧目标不会被当作新叶移走。staging 写入或复读失败由本函数清理，事务开始后的恢复与清理由共享模块负责。

事实叶逐个提交，采集循环结束或额度停止后再逐叶提交日历；当前事务失败不撤销此前成功的事实叶或日历叶。共享模块使用同一文件系统内的路径替换，不提供跨表共同回滚、跨目录原子可见性、进程终止后的自动恢复或并发写入协调。b06 没有独立日期水位文件。

本函数报告校验、暂存、安装、标记和正式复读进度，共享模块报告恢复结果；本函数在失败时记录阶段和耗时并继续抛错。只有正式验收通过、成功退出共享事务后才报告当前叶 `persisted=true`；事实叶成功尚不表示日历完成凭证已回写。'''
cells['b06-flow-commit'].source = '''### 流程：单叶暂存、共享安装与恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
  A["记录开始；完整叶业务校验与 Arrow 转换"] --> B["写 staging；空结果写零行叶"]
  B --> C["复读文件契约、主键和行数"]
  C -- 失败 --> X["本函数清理 staging 并抛错"]
  C -- 通过 --> D["进入共享事务；备份并安装当前叶"]
  D --> E["检查已有标记；必要时暂存、安装并验收新标记"]
  E --> F["精确复读当前正式叶"]
  D -. 失败 .-> R["共享模块倒序恢复实际移动的目标"]
  E -. 失败 .-> R
  F -- 失败 --> R
  R --> S["移除新标记；隔离新叶并恢复旧叶；恢复不完整保留备份"]
  S --> T["报告恢复结果与失败阶段；抛错"]
  F -- 通过 --> G["成功退出并清理；报告当前叶已提交"]
```'''
replace('b06-flow-overview', '普通异常从发生点直接抛出，跳过后续处理；已经成功提交的叶保留。',
        '当前叶的安装或验收异常由共享事务恢复当前叶及本次新建标记，再抛出并停止后续处理；此前成功提交的事实叶或日历叶保留。')
replace('42b891dc',
        '当前单元格同时定义命令和保留 `if __name__ == "__main__": main()`。直接运行 `.py` 读取命令行参数，普通模块导入不启动业务；Notebook 执行此格会尝试读取内核参数，独立执行单元格尚未对齐。',
        '本单元格只定义 Click 命令。后面的独立执行单元格区分 Notebook 与脚本环境：Notebook 显式传入参数，避免读取内核的 `-f` 参数；直接运行 `.py` 使用命令行参数；普通模块导入不触发业务。')

reference = nbformat.read(PATH.with_name('b05_futures_daily.ipynb'), as_version=4)
for original in reference.cells[-5:]:
    cell = copy.deepcopy(original)
    cell.id = cell.id.replace('b05-', 'b06-')
    cell.source = cell.source.replace('b05_futures_daily', 'b06_futures_minute')
    if cell.cell_type == 'code':
        cell.outputs = []
        cell.execution_count = None
    notebook.cells.append(cell)
cells = {cell.id: cell for cell in notebook.cells}
cells['b06-entry-heading'].source = '''## 执行入口

Notebook 通过 `notebook_args` 显式传入 Click 参数，与 b01—b05 保持一致。当前参数是 2026-08-01 至 2026-08-15 的成对日期只读示例，不带 `--write`；存在待办时仍会调用 JQData 并完成采集与校验，但不提交事实或回写日历。直接运行脚本使用命令行参数；在 Notebook 中导入同名 Python 模块不会触发业务。具体模式与正式湖写入限制见开篇表格。'''
replace('b06-flow-manual', '按白名单与完成凭证规划；采集并逐组提交',
        '按白名单与完成凭证采集；事实逐叶提交后回写日历')

assert notebook.metadata == before.metadata
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        comments = lambda source: [t.string for t in tokenize.generate_tokens(io.StringIO(source).readline) if t.type == tokenize.COMMENT]
        assert comments(old.source) == comments(new.source), old.id
for cell in notebook.cells:
    if cell.cell_type == 'code':
        compile(cell.source, f'{PATH}:{cell.id}', 'exec')
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)

b06_boundary = ('b06 保持事实叶逐个提交、之后集中回写日历的顺序，每次事务仅包含当前事实或日历叶及必要的新建零行标记；'
                '新标记先暂存再安装并验收，已有兼容标记保持原样，空事实结果仍写零行叶。'
                '当前事务失败不撤销此前成功叶，日历完成凭证只在对应日历叶成功后生效，b06 没有独立日期水位文件。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    text = path.read_text(encoding='utf-8')
    text = text.replace('b01、b02、b03、b04、b05 共用', 'b01、b02、b03、b04、b05、b06 共用')
    text = text.replace('b01、b02、b03、b04、b05 已共用', 'b01、b02、b03、b04、b05、b06 已共用')
    text = text.replace('b02、b03、b04、b05 保留隔离', 'b02、b03、b04、b05、b06 保留隔离')
    if relative != 'AGENTS.md':
        anchor = 'b05 没有独立日期水位文件。'
        assert text.count(anchor) == 1
        text = text.replace(anchor, anchor + b06_boundary)
    if relative.endswith('README.md'):
        old = ('  共享白名单，把选择结果写回 `is_fetch_required` 和 `selection_reason`，再调用 JQData `get_price(1m)`；')
        assert old in text
        text = text.replace(old, '  共享白名单形成待办，再调用 JQData `get_price(1m)`；选择结果在下述日历提交阶段回写 `is_fetch_required` 和 `selection_reason`。')
    path.write_text(text, encoding='utf-8', newline='\n')
print(f'b06 shared transaction integrated; notebook cells={len(notebook.cells)}; descriptions synchronized.')
