"""a02/b01 接入共享事务；保留整表内容复读和整批恢复边界。"""
import ast
import copy
import pathlib

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01_exchange_report_calendar.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}


def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)


replace('bdd11053', 'from config.settings import settings',
        'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')
source = cells['6f1d04d3'].source
source = source.replace('    log_failed_phase = None\n', '').replace('    log_restored_targets = 0\n', '')
source = source.replace('f".{TABLE_NAME}.staging-{run_id}"', 'f".a02-b01-s-{run_id[:12]}"')
source = source.replace('f".{TABLE_NAME}.backup-{run_id}"', 'f".a02-b01-b-{run_id[:12]}"')
source = source.replace('f".{TABLE_NAME}.failed-{run_id}"', 'f".a02-b01-f-{run_id[:12]}"')
source = source.replace('        moved_partitions = []\n', '').replace('        cleanup_recovery_paths = True\n', '')
source = source.replace('''                if target_had_existing:
                    shutil.move(str(target_path), str(backup_path))
                shutil.move(str(staging_path), str(target_path))''', '''                transaction.replace(target_path=target_path, staged_path=staging_path)''')
source = source.replace('''                backup_path.mkdir(parents=True, exist_ok=False)
                target_path.mkdir(parents=True, exist_ok=True)

''', '')
source = source.replace('                    saved_path = backup_path / relative_path\n', '')
start = source.index('                    destination_path.parent.mkdir(')
end = source.index('                    log_installed_targets += 1', start)
source = source[:start] + '''                    transaction.replace(
                        target_path=destination_path,
                        staged_path=source_path if should_exist else None,
                    )
''' + source[end:]
start = source.index('        try:\n            if full_swap:')
end = source.index('        except Exception as commit_error:', start)
body = source[start + len('        try:\n'):end]
body = ''.join('    ' + line if line.strip() else line for line in body.splitlines(keepends=True))
finish = source.index('        click.echo(\n            f"committed:', end)
source = source[:start] + '''        try:
            with StagedPathTransaction(
                root_path=silver_root,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={TABLE_NAME}; function=commit_partitions; run_id={run_id}",
            ) as transaction:
''' + body + '''        except Exception:
            # 事务进入前的异常也需清理 staging；进入后的恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            if (
                not target_had_existing
                and target_path.is_dir()
                and next(target_path.rglob("*.parquet"), None) is None
            ):
                shutil.rmtree(target_path)
            raise

''' + source[finish:]
source = source.replace('failed_phase={log_failed_phase or log_phase}', 'failed_phase={log_phase}')
source = source.replace('restored_targets={log_restored_targets}; ', '')
cells['6f1d04d3'].source = source

replace('20aadb37', '## 完整叶暂存、内容复读与失败恢复', '## 完整叶暂存、共享安装与内容复读')
replace('20aadb37', '3. 完整期望表为空时整根替换为零行数据集；其余情况按预计算路径备份并安装叶，期望中已无该叶时只移走旧目录。判断叶是否存在直接查分区索引。',
        '3. 进入一个 `StagedPathTransaction`。完整期望表为空时整根替换为零行数据集；其余情况按预计算路径安装或显式删除全部变更叶，未触达叶与已有根级契约标记保持原样。删除通过 `staged_path=None` 表达；应有的 staging 叶缺失仍报错。')
replace('20aadb37', '4. 正式安装后保留一次整表物理契约、总行数和完整内容摘要复读。',
        '4. 在同一事务内，正式安装后保留一次整表物理契约、总行数和完整内容摘要复读。')
replace('20aadb37', '安装与复读阶段仍记 `batch_state=pending`，验收及原有清理成功后才报告提交和日历状态已落盘，`date_watermark=none`。暂存失败清理 staging；安装或验收失败沿原逻辑恢复；恢复失败保留现场。此轮保持本地安装与恢复实现、返回的变更行数以及共同回滚范围。',
'''本函数报告暂存、安装与复读进度，共享模块负责失败恢复日志。安装与复读阶段仍记 `batch_state=pending`；验收并成功退出事务后才报告提交和日历状态已落盘，`date_watermark=none`。返回值仍为变更叶中实际写入的行数，删除行不计入。

本批路径为 `silver/.a02-b01-s-<run_id前12位>/`（暂存）、`.a02-b01-b-.../`（备份）、`.a02-b01-f-.../`（隔离）。共享模块按实际移动记录倒序恢复整个批次；首次备份未成功时旧目标保持原位，一处恢复失败仍继续尝试其余目标。恢复完整时清理 staging 和 backup，保留失败新数据的隔离目录；恢复不完整时另保留旧备份，staging 仍清理。异常报告现场，并保留原安装或验收异常的原因链。

暂存或事务进入失败只清理本次 staging；空湖失败恢复后清理无 Parquet 的新建表目录。共享模块只负责同一文件系统内的路径替换，不提供外部读者的跨目录原子可见性、进程终止后的自动恢复或并发写入协调。''')
replace('96f73c42', '事实白名单和项目设置。', '事实白名单、项目设置和共享路径事务。')
replace('a02-b01-flow-init', '两张 Schema、白名单与设置', '两张 Schema、白名单、设置与共享事务')
replace('a02-b01-flow-overview', '整表置空或逐叶替换、删除', '同一共享事务：整表置空或逐叶替换、删除')
replace('a02-b01-flow-overview', '恢复已移动目标；抛出异常', '共享模块逐项恢复；保留失败新数据与必要备份；抛错')
cells['a02-b01-flow-commit'].source = '''### 局部流程：暂存、共享安装与整批恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A{"变更键为空？"} -->|是| B["报告无分区、状态未变；返回 0"]
    A -->|否| C["期望转换一次；预备分区索引、摘要和路径"]
    C --> D["写入 staging；读取一次并在内存核对各叶"]
    D --> E["进入一个共享事务"]
    D -. 失败 .-> X["清理 staging；抛错；正式目标不变"]
    E -. 进入失败 .-> X
    E --> F{"完整期望表为空？"}
    F -->|是| G["共享模块备份旧表根；安装零行表根"]
    F -->|否| H["共享模块逐叶替换或显式删除；批次 pending"]
    G --> I["本函数复读正式整表契约、行数与内容摘要"]
    H --> I
    I -->|通过| J["成功退出事务；报告提交与日历落盘"]
    G -. 失败 .-> K["共享模块倒序隔离新目标、恢复旧目标；逐项尝试"]
    H -. 失败 .-> K
    I -. 失败 .-> K
    K --> L{"恢复完整？"}
    L -->|是| M["清理暂存及备份；保留隔离新数据；抛错"]
    L -->|否| N["清理暂存；保留备份及隔离数据；报告现场并抛错"]
```'''
cells['abe75249'].source = '''## Notebook 与脚本执行入口

与 b01、b02 一样，Notebook 使用 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，并用 `standalone_mode=False` 返回单元格。当前参数为 `[]`，默认读取正式湖的本地数据并生成完整自动计划，不带 `--write`，不调用 API。

只有交互内核且没有 `__file__` 时才使用 Notebook 分支；在 Notebook 中导入同名 Python 模块不执行入口。直接运行 `.py` 时读取命令行参数。最后一格仅列出终端命令注释；正式提交仍需人工运行 `--write`，不能用日期参数截断正式写入范围。'''
replace('03fe6466', 'if "ipykernel" in sys.modules:', 'if "ipykernel" in sys.modules and "__file__" not in globals():')
replace('03fe6466', '    main.main(', '    notebook_args = []\n    main.main(')
replace('03fe6466', 'args=[]', 'args=notebook_args')
cells['a02-b01-flow-entry'].source = '''### 局部流程：Notebook 与脚本执行入口

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；不读取内核参数"]
    B --> C["main.main；standalone_mode=False"]
    C --> D["当前空参数：本地全量只读计划"]
    A -->|否| E{"直接运行 Python 脚本？"}
    E -->|是| F["main 读取命令行参数"]
    E -->|否| G["模块导入：不执行入口"]
```'''
notebook.cells.append(nbformat.v4.new_markdown_cell('''### 局部流程：终端手动运行

下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart LR
    A["在终端激活 latitude"] --> B["切换到项目根目录"]
    B --> C["手动运行对应 .py --write"]
    C --> D["本地全量比较；整批提交变更叶"]
```''', id='a02-b01-flow-manual'))
notebook.cells.append(nbformat.v4.new_code_cell('''# conda env list
# conda activate latitude
# cd E:\\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\\a02_Futures_Exchange_Reports\\b01_exchange_report_calendar.py --write''', id='a02-b01-manual'))

assert notebook.metadata == before.metadata
for old in before.cells:
    new = cells[old.id]
    assert {k: v for k, v in old.items() if k != 'source'} == {k: v for k, v in new.items() if k != 'source'}
for cell in notebook.cells:
    if cell.cell_type == 'code':
        ast.parse(cell.source)
nbformat.validate(notebook)
nbformat.write(notebook, PATH)

boundary = ('a02/b01 将全部变更报告日历叶纳入同一事务；完整期望为空时改为整根安装可读零行表，'
            '非空时只替换或显式删除变更叶，已有根级标记保持原样。正式整表物理契约、行数和完整内容摘要复读仍由环节在事务内执行。'
            '共享模块倒序逐项恢复，恢复完整保留失败新数据，恢复不完整另保留旧备份，staging 均清理。'
            '全部验收并成功退出事务后才报告日历状态落盘；没有独立日期水位，也不改变全量比较与显式日期写入边界。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    text = path.read_text(encoding='utf-8')
    old = 'a01/b01、b02、b03、b04、b05、b06、b07、b08'
    assert old in text
    text = text.replace(old, old + ' 与 a02/b01')
    if relative != 'AGENTS.md':
        anchor = '日历根级标记保持原样，b08 不写独立日期水位。'
        assert text.count(anchor) == 1
        text = text.replace(anchor, anchor + boundary)
    path.write_text(text, encoding='utf-8', newline='\n')
print('a02/b01 shared transaction, entry cells and documentation updated.')
