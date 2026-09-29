"""b08 两表共同事务、独立执行单元格与相关说明同步。"""
import ast
import copy
import io
import pathlib
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b08_full_minute_quality.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)


replace('c08-imports', 'from config.settings import settings',
        'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')
source = cells['c08-commit'].source
source = source.replace('    missing_staging_path: pathlib.Path,\n    calendar_staging_path: pathlib.Path,',
                        '    staging_path: pathlib.Path,')
source = source.replace('    log_restored_calendar = 0\n    log_failure_phase = None\n', '')
start = source.index('        missing_backup_path = ')
end = source.index('        calendar_columns = ', start)
source = source[:start] + '''        staging_path = staging_path.resolve()
        if staging_path == silver_root or not staging_path.is_relative_to(silver_root):
            raise ValueError(f"staging 路径必须位于 silver 根目录内：{staging_path}")
        missing_staging_path = staging_path / MISSING_TABLE_NAME
        calendar_staging_path = staging_path / CALENDAR_TABLE_NAME
        backup_path = silver_root / f".b08-b-{short_run_id}"
        quarantine_path = silver_root / f".b08-f-{short_run_id}"

''' + source[end:]
start = source.index('        missing_had_existing = ')
end = source.index('        try:\n', start)
source = source[:start] + '\n' + source[end:]
start = source.index('            if missing_had_existing:')
end = source.index('            click.echo(', start)
source = source[:start] + '''            transaction.replace(
                target_path=missing_target_path,
                staged_path=missing_staging_path,
            )
''' + source[end:]
source = source.replace('            calendar_backup_path.mkdir(parents=True, exist_ok=False)\n', '')
source = source.replace('            calendar_target_path.mkdir(parents=True, exist_ok=True)\n', '')
start = source.index('                saved_path = ')
end = source.index('                log_installed_calendar += ', start)
source = source[:start] + '''                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path,
                )
''' + source[end:]

start = source.index('        try:\n')
end = source.index('        except Exception:\n', start)
body = source[start + len('        try:\n'):end]
body = ''.join('    ' + line if line.strip() else line for line in body.splitlines(keepends=True))
finish = source.index('        click.echo(\n            f"committed:', end)
source = source[:start] + '''        try:
            with StagedPathTransaction(
                root_path=silver_root,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={MISSING_TABLE_NAME}; function=commit_full_audit; run_id={run_id}"
                ),
            ) as transaction:
''' + body + '''        except Exception:
            # 事务进入前的异常也需清理 staging；进入后的恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

''' + source[finish:]
source = source.replace('failed_phase={log_failure_phase or log_phase}', 'failed_phase={log_phase}')
source = source.replace('recovery_phase={log_phase}; ', '')
source = source.replace('restored_calendar_partitions={log_restored_calendar}; ', '')
cells['c08-commit'].source = source

# 两张表置于同一个批次 staging 下，供共享事务统一管理。
replace('c08-cli', '''        missing_staging_path = silver_root / f".b08-m-s-{short_run_id}"
        calendar_staging_path = silver_root / f".b08-c-s-{short_run_id}"''',
        '''        staging_path = silver_root / f".b08-s-{short_run_id}"''')
replace('c08-cli', '''        temporary_root = pathlib.Path(temporary_directory.name)
        missing_staging_path = temporary_root / MISSING_TABLE_NAME
        calendar_staging_path = temporary_root / CALENDAR_TABLE_NAME''',
        '''        staging_path = pathlib.Path(temporary_directory.name)

    missing_staging_path = staging_path / MISSING_TABLE_NAME
    calendar_staging_path = staging_path / CALENDAR_TABLE_NAME''')
replace('c08-cli', '''                missing_staging_path,
                calendar_staging_path,
                audit_result,''', '''                staging_path,
                audit_result,''')
replace('c08-cli', '''            shutil.rmtree(missing_staging_path, ignore_errors=True)
            shutil.rmtree(calendar_staging_path, ignore_errors=True)''',
        '''            shutil.rmtree(staging_path, ignore_errors=True)''')
replace('c08-cli', '\n\nif __name__ == "__main__":\n    main()', '')

cells['c08-commit-heading'].source = '''## 缺失表根与日历叶的共享事务

`commit_full_audit()` 接收已验证的本批 staging 根、叶摘要与汇总。在 `silver/.b08-s-<run_id前12位>/` 下分别暂存缺失表和日历表，进入一个 `StagedPathTransaction`：先安装整个缺失表根，再按顺序安装全部触达日历叶。两者属于同一次共同回滚范围，缺失表零行契约标记随整根安装；日历根级标记保持原样。

正式复读仍直接留在本函数：缺失表检查物理/身份契约、总行数、分区集合和逐叶完整内容摘要；日历检查数据集契约及触达叶完整内容摘要。各输出叶在生成时已完成业务校验，复读不重复这些业务检查。当前正式日历表根只打开一次检查全部 fragment，再逐个读取触达叶。

共享模块按实际移动记录倒序恢复，先隔离已安装的新日历叶并恢复旧叶，最后恢复缺失表根；原本不存在的目标恢复为不存在。第一次备份失败时，未移动的旧目标保持原位。一处恢复失败仍继续尝试其余目标。恢复完整清理 staging 和 backup、保留失败新数据的隔离目录；恢复不完整保留 backup 和隔离目录，staging 仍清理。备份和隔离分别位于 `silver/.b08-b-<run_id前12位>/`、`silver/.b08-f-<run_id前12位>/`，内部按正式表名与叶路径保留对应关系。异常报告现场路径，并以原安装或验收异常作为原因。

本函数报告安装和正式复读进度，共享模块报告失败恢复结果。单项安装或验收成功仍记 `batch_state=pending`；只有两表正式验收并成功退出事务后，才报告 `committed:` 和 `phase=audit_state; persisted=true; date_watermark=none`，随后返回缺失行数和日历分区数。b08 不写独立日期水位，也不改变完成凭证、质量结论或 b07 旁证。

生成失败及事务进入失败只清理本次 staging，不动正式目标。共享模块使用同一文件系统内的路径替换，不提供外部读者的跨目录原子可见性、进程终止后的自动恢复或并发写入协调。'''
replace('b08-flow-overview', '备份并安装缺失表根及全部触达日历叶', '一个共享事务内安装缺失表根及全部触达日历叶')
replace('b08-flow-overview', '倒序恢复日历叶和缺失表；抛错', '共享模块倒序恢复日历叶和缺失表；逐项尝试')
replace('b08-flow-overview', '恢复不完整保留现场；无成功日志', '保留失败新数据；恢复不完整保留备份；抛错')
replace('b08-flow-environment', '转换函数及 settings', '转换函数、settings 及共享事务')
cells['b08-flow-commit'].source = '''### 局部流程：两表共享安装与共同恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["报告提交开始；确认 staging 位于 silver 内"] --> B["进入一个共享事务：缺失表根＋全部触达日历叶"]
    B -. 进入失败 .-> X["清理 staging；抛错；正式目标不变"]
    B --> C["共享模块备份并安装缺失表根、逐个日历叶"]
    C --> D["本函数复读缺失表契约、总量、分区集合及内容摘要"]
    D --> E["本函数复读日历契约与触达叶内容摘要；仍记 pending"]
    E --> F["成功退出事务并清理；报告 audit_state 已落盘"]
    C -. 失败 .-> R["共享模块倒序隔离新目标、恢复旧目标；逐项尝试"]
    D -. 失败 .-> R
    E -. 失败 .-> R
    R --> S{"恢复完整？"}
    S -->|是| T["清理 staging 和 backup；保留隔离的新数据；抛错"]
    S -->|否| U["清理 staging；保留 backup 和隔离数据；报告现场并抛错"]
```'''
replace('c08-cli-heading', '写入模式在尚未尝试提交时发生异常，由入口清理两个 staging 目录。进入提交阶段后的恢复和清理由提交函数按现有逻辑处理。', '写入模式在尚未尝试提交时发生异常，由入口清理统一的本批 staging 根。事务进入失败由提交函数清理 staging，进入后的安装恢复由共享模块处理。')
replace('c08-cli-heading', '此代码格仍保留 Click 命令定义和原有 `if __name__ == "__main__": main()`；直接运行 `.py` 使用命令行参数，模块导入不启动业务，Notebook 的独立执行单元格尚未对齐。', '此代码格只定义 Click 命令；后面的独立执行单元格区分 Notebook、直接运行脚本和模块导入。')
replace('b08-flow-main', '调用提交函数；函数自行报告安装、验收和恢复', '调用提交函数；自行报告安装验收，共享模块负责恢复')

reference = nbformat.read(PATH.with_name('b07_suspected_session_reconciliation.ipynb'), as_version=4)
for original in reference.cells[-5:]:
    cell = copy.deepcopy(original)
    cell.id = cell.id.replace('b07-', 'b08-')
    cell.source = cell.source.replace('b07_suspected_session_reconciliation', 'b08_full_minute_quality')
    if cell.cell_type == 'code':
        cell.outputs = []
        cell.execution_count = None
    notebook.cells.append(cell)
cells = {cell.id: cell for cell in notebook.cells}
cells['b08-entry-heading'].source = '''## 执行入口

Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，与 b01—b07 一致。当前只传 `--confirm-full-quality`，不带 `--write`；执行此格会全量读取本地 required 1m Session 和分钟事实两列主键，在系统临时目录生成、复读缺失明细与日历 staging，退出时清理，不改动正式湖。

b08 不提供日期、月份或合约范围参数，不能复制其他环节的成对日期示例。只读模式同样执行全量审计，运行时间取决于全部待审计数据量；查看说明或流程图不会启动。直接运行脚本使用命令行参数，在 Notebook 中导入同名 Python 模块不触发业务；正式提交仍需人工显式 `--confirm-full-quality --write`。'''
cells['b08-flow-entry'].source = cells['b08-flow-entry'].source.replace('当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会扫描范围内的新候选，并在有候选时读取本地事实、计算旁证和预览。流程图本身不执行代码。', '当前 Notebook 参数是人工全量只读确认；执行此代码格会全量读取本地数据并生成临时 staging。流程图本身不执行代码。').replace('进入运行模式分支', '进入人工全量审计；是否提交由 write 决定')
replace('b08-entry', '["--start-date", "2026-08-01", "--end-date", "2026-08-15",]', '["--confirm-full-quality"]')
replace('b08-flow-manual', '手动运行对应 .py --write', '手动运行对应 .py --confirm-full-quality --write')
replace('b08-flow-manual', '读取本地新候选及事实；整批日历叶共同提交旁证', '全量分钟主键求差；缺失表根和日历叶共同提交')
replace('b08-manual', 'b08_full_minute_quality.py --write', 'b08_full_minute_quality.py --confirm-full-quality --write')

assert notebook.metadata == before.metadata
for old in before.cells:
    new = cells[old.id]
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in new.items() if k != 'source'}
    if old.cell_type == 'code' and old.id != 'c08-commit':
        comments = lambda text: [t.string for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type == tokenize.COMMENT]
        assert comments(old.source) == comments(new.source), old.id
for cell in notebook.cells:
    if cell.cell_type == 'code':
        ast.parse(cell.source)
nbformat.validate(notebook)
nbformat.write(notebook, PATH)

boundary = ('b08 在一个事务内安装整个缺失明细表根和全部触达日历叶，正式复读仍由 b08 执行；'
            '任一安装或验收失败共同恢复，一处恢复失败仍尝试其余目标。恢复完整保留失败新数据的隔离目录，'
            '恢复不完整另保留旧备份，staging 均清理。两表全部验收并成功退出事务后才报告审计状态落盘；'
            '日历根级标记保持原样，b08 不写独立日期水位。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    text = path.read_text(encoding='utf-8')
    text = text.replace('b01、b02、b03、b04、b05、b06、b07 共用', 'b01、b02、b03、b04、b05、b06、b07、b08 共用')
    text = text.replace('b01、b02、b03、b04、b05、b06、b07 已共用', 'b01、b02、b03、b04、b05、b06、b07、b08 已共用')
    text = text.replace('b02、b03、b04、b05、b06、b07 保留隔离的新分区', 'b02、b03、b04、b05、b06、b07、b08 保留隔离的新目标')
    if relative != 'AGENTS.md':
        anchor = 'b07 不修改根级契约标记，也没有独立日期水位文件。'
        assert text.count(anchor) == 1
        text = text.replace(anchor, anchor + boundary)
    path.write_text(text, encoding='utf-8', newline='\n')
print(f'b08 shared transaction integrated; notebook cells={len(notebook.cells)}; descriptions synchronized.')
