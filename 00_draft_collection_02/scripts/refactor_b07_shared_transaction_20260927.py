"""b07 整批日历叶接入共享事务，并对齐独立执行单元格。"""

import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b07_suspected_session_reconciliation.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)


replace('b8d9e4f3', 'from config.settings import settings',
        'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')
source = cells['96342530'].source
start = source.index('        log_phase = "prepare_install"')
end = source.index('        click.echo(\n            f"committed:', start)
loop_start = source.index('            for partition_key in sorted(expected_tables):', start)
loop_end = source.index('        except Exception:', loop_start)
install = source[loop_start:loop_end]
install = install.replace('                saved_path = backup_path / relative_path\n', '')
move_start = install.index('                destination_path.parent.mkdir(')
move_end = install.index('                log_phase = "formal_readback"', move_start)
install = (install[:move_start] + '''                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path,
                )
''' + install[move_end:])
install = ''.join('    ' + line if line.strip() else line for line in install.splitlines(keepends=True))
cells['96342530'].source = source[:start] + '''        log_phase = "prepare_install"
        # 第二阶段：保存旧叶分区，移动已验证分区，再精确复核刚触达的正式叶。
        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; "
                    f"run_id={run_id}"
                ),
            ) as transaction:
''' + install + '''        except Exception:
            # 事务进入前的异常也需清理 staging；进入后的恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

''' + source[end:]
replace('96342530', '    log_restored_partitions = 0\n    log_failure_phase = None\n', '')
replace('96342530', 'original_failed_phase={log_failure_phase}; ', '')
replace('96342530', 'restored_partitions={log_restored_partitions}; ', '')
replace('57c9061e', '\n\nif __name__ == "__main__":\n    main()', '')

cells['e492cfe6'].source = '''## 完整叶暂存、共享安装与本批共同回滚

`commit_calendar_partitions()` 接受触达叶的全部行。空映射直接返回 0；非空时，各叶先做一次完整业务校验并确认分区归属，再将已校验 Arrow 叶直接拼接写入 staging。staging 检查当前完整 Schema/metadata，以及各叶主键唯一性和行数。

所有暂存叶通过后，在一个共享 `StagedPathTransaction` 内按分区顺序备份旧叶、安装新叶，并仅打开刚安装的正式叶复读物理/身份契约、主键和行数。一次事务覆盖本次调用的全部日历叶；后一个叶失败时，前面已经安装并复读通过的叶也共同恢复。返回的 `committed_calendar_rows` 是提交完整叶的总行数，包含同叶保留行，不能当作候选数量。

共享模块根据实际移动记录倒序恢复：隔离已安装的新叶，再恢复旧叶；原来不存在的叶恢复为不存在。第一次备份失败时尚未移动的旧叶保持原位。恢复成功清理 staging 和 backup，保留失败新叶的隔离目录；恢复不完整保留 backup 和隔离目录，并继续尝试恢复其余叶。异常报告现场路径，以原提交异常作为原因向上抛出。正式替换前的 staging 写入、验收或事务进入失败，只清理本次 staging 并抛错。

本函数报告准备、暂存、安装与正式复读进度，共享模块报告失败恢复结果。单叶验收通过仍记 `batch_state=pending`；全部正式叶验收并成功退出共享事务后，才报告 `committed` 与 `phase=evidence_state; persisted=true`。空提交报告状态未变。旁证与质量判断仍由本环节负责，不交给共享模块。

b07 不写独立日期水位，也不修改根级 `schema.parquet`。共享模块使用同一文件系统内的路径替换；整批共同回滚不表示所有叶对其他读者原子可见，也不提供进程终止后的自动恢复或并发写入协调。'''
replace('b07-flow-overview', '逐叶备份、安装、正式叶复读', '一个共享事务内逐叶安装、正式叶复读')
replace('b07-flow-overview', '倒序恢复本批已移动叶；抛错', '共享模块恢复本批已移动叶；保留失败现场并抛错')
replace('b07-flow-environment', '转换函数和 settings', '转换函数、settings 和共享事务')
cells['b07-flow-commit'].source = '''### 局部流程：暂存、共享安装与共同恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["报告提交开始"] --> B{"有待提交分区？"}
    B -->|否| C["报告状态未变；返回 0"]
    B -->|是| D["逐叶业务校验；拼接 Arrow 叶写 staging"]
    D --> E["复读 staging 契约、主键和行数"]
    D -. 失败 .-> X["清理 staging；抛错"]
    E -. 失败 .-> X
    E --> F["进入一个共享事务；范围为本批全部日历叶"]
    F -. 进入失败 .-> X
    F --> G["共享模块备份、安装当前叶"]
    G --> H["本函数复读当前正式叶；通过仍记 pending"]
    H --> I{"全部叶通过？"}
    I -->|否| G
    I -->|是| J["成功退出事务并清理；报告旁证落盘"]
    G -. 失败 .-> R["倒序隔离新叶并恢复本批旧叶；逐项尝试恢复"]
    H -. 失败 .-> R
    R --> S{"恢复完整？"}
    S -->|是| T["清理 backup；保留失败新叶；抛错"]
    S -->|否| U["保留 backup 和失败新叶；报告现场并抛错"]
```'''
replace('a006bfaa', '本批验收与清理成功后', '本批验收并成功退出共享事务后')
replace('a006bfaa',
        '此单元格仍保留 Click 定义和原有 `main()` 执行方式；最后执行单元格的对齐尚未在本轮实施。',
        '此单元格只定义 Click 命令；后面的独立执行单元格区分 Notebook、直接运行脚本和模块导入。')

reference = nbformat.read(PATH.with_name('b06_futures_minute.ipynb'), as_version=4)
for original in reference.cells[-5:]:
    cell = copy.deepcopy(original)
    cell.id = cell.id.replace('b06-', 'b07-')
    cell.source = cell.source.replace('b06_futures_minute', 'b07_suspected_session_reconciliation')
    if cell.cell_type == 'code':
        cell.outputs = []
        cell.execution_count = None
    notebook.cells.append(cell)
cells = {cell.id: cell for cell in notebook.cells}
cells['b07-entry-heading'].source = '''## 执行入口

Notebook 通过 `notebook_args` 显式传入 Click 参数，与 b01—b06 保持一致。当前参数是 2026-08-01 至 2026-08-15 的成对日期只读示例，不带 `--write` 或 `--force`，只在该范围内计算默认新候选并预览；全部读取来自本地 silver。直接运行脚本使用命令行参数；在 Notebook 中导入同名 Python 模块不触发业务。具体候选范围与正式写入限制见开篇表格。'''
replace('b07-flow-entry',
        '当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会规划待办，并在存在请求批次时采集和校验。流程图本身不执行代码。',
        '当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会扫描范围内的新候选，并在有候选时读取本地事实、计算旁证和预览。流程图本身不执行代码。')
replace('b07-flow-manual', '按白名单与完成凭证采集；事实逐叶提交后回写日历',
        '读取本地新候选及事实；整批日历叶共同提交旁证')

assert notebook.metadata == before.metadata
for old in before.cells:
    new = cells[old.id]
    assert old.metadata == new.metadata
    if old.cell_type == 'code':
        assert old.outputs == new.outputs and old.execution_count == new.execution_count
        if old.id != '96342530':
            comments = lambda text: [t.string for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type == tokenize.COMMENT]
            assert comments(old.source) == comments(new.source), old.id
for cell in notebook.cells:
    if cell.cell_type == 'code':
        compile(cell.source, f'{PATH}:{cell.id}', 'exec')
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)

boundary = ('b07 在一个事务内安装并验收本次调用的全部日历叶，后一个叶失败时共同恢复前面已经安装的叶；'
            '恢复完整仍保留失败新叶的隔离目录，恢复不完整保留旧备份并继续尝试恢复其余叶。'
            '全部叶成功后才报告旁证已落盘；b07 不修改根级契约标记，也没有独立日期水位文件。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    text = path.read_text(encoding='utf-8')
    text = text.replace('b01、b02、b03、b04、b05、b06 共用', 'b01、b02、b03、b04、b05、b06、b07 共用')
    text = text.replace('b01、b02、b03、b04、b05、b06 已共用', 'b01、b02、b03、b04、b05、b06、b07 已共用')
    text = text.replace('b02、b03、b04、b05、b06 保留隔离', 'b02、b03、b04、b05、b06、b07 保留隔离')
    if relative != 'AGENTS.md':
        anchor = 'b06 没有独立日期水位文件。'
        assert text.count(anchor) == 1
        text = text.replace(anchor, anchor + boundary)
    path.write_text(text, encoding='utf-8', newline='\n')
print(f'b07 shared transaction integrated; notebook cells={len(notebook.cells)}; descriptions synchronized.')
