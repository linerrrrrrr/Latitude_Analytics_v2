"""SHIBOR 第 10—12 项：只改 Notebook、受影响说明和既有故障注入点。"""
import ast
import io
import json
import pathlib
import shutil
import tempfile
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb')
SNAPSHOT = pathlib.Path(tempfile.mkdtemp(prefix='a04-b02-transaction-before-'))
FILES = [REL, REL.with_suffix('.py'), pathlib.Path('AGENTS.md'),
         pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md'),
         pathlib.Path('00_draft_collection_02/tests/test_b04_c02_interest_rate.py'),
         pathlib.Path('00_draft_collection_02/a04_b02_docs_logs_verification_20260928.md')]
for relative in FILES:
    saved = SNAPSHOT / relative
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, saved)

notebook = nbformat.read(ROOT / REL, 4)
cells = {cell.id: cell for cell in notebook.cells}
old_code = '\n\n'.join(c.source for c in notebook.cells if c.cell_type == 'code')
cells['0e3603da'].source = cells['0e3603da'].source.replace(
    'from config.settings import settings',
    'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction',
)

for function, table in [('upgrade_fact_metadata', 'TABLE_NAME'),
                        ('commit_complete_fact_partition', 'TABLE_NAME'),
                        ('commit_calendar_partition', 'CALENDAR_TABLE_NAME')]:
    cell = cells['a04-b02-code-' + function.replace('_', '-')]
    source = cell.source
    for line in ('        old_target_moved = False\n', '        new_target_installed = False\n',
                 '        cleanup_recovery_paths = True\n', '        saved_path = backup_path / relative_path\n',
                 '        target_had_partition = destination_path.exists()\n', '        marker_created = False\n',
                 '        old_partition_moved = False\n', '        new_partition_installed = False\n'):
        source = source.replace(line, '')
    start = source.index('        try:\n', source.index('        log_phase = "install"'))
    end = source.index('\n        click.echo(\n            f"committed:', start)
    install = source[start:source.index('        except Exception as commit_error:', start)]
    if function == 'upgrade_fact_metadata':
        first = install.index('            if target_path.exists():')
        last = install.index('\n            click.echo(', first)
        install = install[:first] + '            transaction.replace(target_path=target_path, staged_path=staging_path)\n' + install[last:]
    elif function == 'commit_complete_fact_partition':
        first = install.index('            target_path.mkdir(')
        last = install.index('\n            click.echo(', first)
        install = install[:first] + '''            if not target_marker_path.exists():
                transaction.replace(
                    target_path=target_marker_path,
                    staged_path=staging_marker_path,
                    quarantine_new=False,
                )
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path if not complete_df.empty else None,
            )
''' + install[last:]
    else:
        first = install.index('            saved_path.parent.mkdir(')
        last = install.index('\n            click.echo(', first)
        install = install[:first] + '''            transaction.replace(
                target_path=destination_path,
                staged_path=source_path,
            )
''' + install[last:]
    body = install[len('        try:\n'):]
    partition_context = '; partition={partition_key}' if function != 'upgrade_fact_metadata' else ''
    install = f'''        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={{{table}}}; function={function}; run_id={{run_id}}{partition_context}",
            ) as transaction:
''' + ''.join('    ' + line if line.strip() else line for line in body.splitlines(keepends=True))
    source = source[:start] + install + '''        except Exception:
            # 事务进入失败也清理 staging；安装、验收失败恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise
''' + source[end:]
    ast.parse(source)
    assert 'shutil.move' not in source and 'rollback_errors' not in source
    cell.source = source

cells['de9e1f3f'].source = '''if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b02_interest_rate",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()
'''
cells['a04-b02-doc-imports'].source = cells['a04-b02-doc-imports'].source.replace(
    '导入权威契约、配置和库', '导入权威契约、配置、共享安装/恢复模块和库')
cells['a04-b02-flow-0e3603da'].source = cells['a04-b02-flow-0e3603da'].source.replace(
    '导入 Schema、宏观配置与设置', '导入 Schema、配置、设置与共享事务')
cells['a04-b02-flow-overview'].source = cells['a04-b02-flow-overview'].source.replace(
    '事实 staging、安装与正式复读', '事实 staging；共享事务内安装与正式复读').replace(
    '恢复当前目标；抛错停止；此前成功叶保留', '共享模块恢复当前目标；保留失败新数据；抛错停止')

cells['a04-b02-doc-upgrade-fact-metadata'].source = '''## 现有事实整根迁移与共享恢复

只由无日期写入模式触发。保留本环节现有完整事实校验、staging 复读与内容摘要核对；随后在一个 `StagedPathTransaction` 内替换整个事实表根，并执行正式整表复读与逐值比较。成功退出事务后才报告 `metadata_upgraded; persisted=true` 并返回正式事实。

整根备份和失败新根分别位于本批 `.backup-<run_id>/_root` 与 `.failed-<run_id>/_root`。首次备份失败不删除旧根；安装或验收失败恢复旧根，已经安装的失败新数据隔离保留。恢复不完整时另保留旧备份并抛出带原始原因的异常；staging 始终清理。暂存或事务进入失败也清理 staging。

本项只接入路径事务，任意事实 metadata 差异触发迁移的旧行为尚未收缩；纯描述性 metadata 不应触发重写的待处理边界仍按开篇说明标记，不能把当前实现当成新的规范。
'''
cells['a04-b02-flow-upgrade-fact-metadata'].source = '''### 流程：事实整根迁移与共享恢复

```mermaid
flowchart TD
A["完整事实校验；整根 staging"] --> B["staging 复读与完整摘要核对"]
B --> C["进入共享事务；备份旧根并安装新根"]
C --> D["事务内正式整表复读；逐值比较"]
D --> E["成功退出事务；报告迁移提交；返回正式事实"]
C -. 失败 .-> R["共享模块隔离失败新根并恢复旧根；抛错"]
D -. 失败 .-> R
R --> S["保留失败新根；恢复不完整另保留备份；清理 staging"]
A -. 暂存失败 .-> X["清理 staging；抛错"]
B -. 失败 .-> X
```
'''
cells['a04-b02-doc-commit-complete-fact-partition'].source = '''## 当前事实完整叶与新标记共同提交

保留完整月叶的现有校验、暂存和逐值复读。一个共享事务覆盖当前事实叶及本次必要的新建 `schema.parquet`：既有标记保持原样，新标记回滚时直接移除。非空叶安装经复读的 staging 叶；空叶通过 `staged_path=None` 显式删除旧叶，不能从暂存叶缺失推断删除。

正式目标安装后在事务内部按现有契约打开、过滤目标叶并复读比较；成功退出事务后才报告事实已提交，明确日历尚未由本函数提交。未触达叶不在恢复范围内；日历随后独立提交，失败不能撤销已成功事实。

共享模块按实际移动记录倒序恢复。首次备份失败保留旧目标；安装或验收失败隔离已安装的新叶并恢复旧叶，一处恢复失败仍继续其他目标的恢复。完整恢复清理 staging 和备份，保留失败新叶；恢复不完整另保留旧备份。路径位于同一 silver 根下，逐叶证据保留分区相对路径；不递归清理正式表根，空父目录可保留。本环节没有独立日期水位。
'''
cells['a04-b02-flow-commit-complete-fact-partition'].source = '''### 流程：事实叶与新标记共享事务

```mermaid
flowchart TD
A["完整事实叶校验；staging 写入和逐值复读"] --> B["进入共享事务；必要时安装新根标记"]
B --> C{"完整叶非空？"}
C -->|是| D["共享模块备份旧叶并安装新叶"]
C -->|否| E["显式删除旧叶；staged_path=None"]
D --> F["事务内正式叶复读与摘要一致"]
E --> F
F --> G["成功退出事务；事实已提交；日历随后独立回写"]
B -. 失败 .-> R["倒序恢复当前叶和新标记；保留失败新叶；抛错"]
D -. 失败 .-> R
E -. 失败 .-> R
F -. 失败 .-> R
R --> S["恢复不完整保留旧备份；清理 staging；不递归删除表根"]
A -. 失败 .-> X["清理 staging；正式目标未改动；抛错"]
```
'''
cells['a04-b02-doc-commit-calendar-partition'].source = '''## 当前日历完整叶独立回写

保留现有日历校验、完整 `interest_rate/year/month` 叶选取、staging 写入与逐值复读。上游日历根和目标叶必须已经存在，b02 不创建或迁移日历根；已有根标记、其他月份及 a04/b03 使用的 `macro_release` 叶保持原样。

每次回写使用一个独立共享事务，只替换当前日历叶，正式复读与摘要验收在事务内完成。成功退出后才报告 `calendar_state=committed; collection_completion=per_grid; persisted=true`：这表示状态已落盘，是否采集完成仍由逐格点字段决定。失败状态成功保存不等于事实采集完成，也不写独立日期水位。

安装或验收失败只恢复当前日历叶，已成功的事实和此前月份保留。共享模块保留已安装的失败新叶；恢复不完整另保留旧备份；staging 清理。下一次人工运行仍可以从已提交的事实无 API 修复日历，没有自动业务重试。
'''
cells['a04-b02-flow-commit-calendar-partition'].source = '''### 流程：当前日历叶独立共享事务

```mermaid
flowchart TD
A["完整日历校验；选取当前 interest_rate 月叶"] --> B["确认上游日历；staging 写入与逐值复读"]
B --> C["进入独立共享事务；确认目标叶已存在"]
C --> D["备份旧叶；安装新叶；既有根标记不变"]
D --> E["事务内正式复读与完整摘要比较"]
E --> F["成功退出；报告状态已落盘；完成与否按格点字段"]
C -. 失败 .-> R["恢复当前日历叶；此前成功事实与日历保留；抛错"]
D -. 失败 .-> R
E -. 失败 .-> R
R --> S["保留失败新叶；恢复不完整保留备份；清理 staging"]
B -. 失败 .-> X["清理 staging；抛错"]
```
'''
cells['2dfb140a'].source = '''## Notebook 与脚本执行入口

与 a01/b01 使用相同的入口结构：仅在交互内核且没有 `__file__` 时，以显式 `notebook_args` 和 `standalone_mode=False` 调用 Click，不读取 PyCharm/Jupyter 内核的 `-f` 参数。默认空列表不带 `--write`，只读湖并按自动范围求差；有 API 待办时仍会请求 Tushare 和转换响应，但不提交事实或日历。

终端直接运行同名 `.py` 时正常读取命令行参数，只有显式 `--write` 才提交；成对日期与正式湖写入门禁仍由 main 执行。普通 Python 或 Notebook 内导入同名模块均不启动业务入口或 Schema 浏览。
'''
cells['a04-b02-flow-de9e1f3f'].source = '''### 流程：Notebook 与脚本执行入口

```mermaid
flowchart TD
A{"交互内核且没有脚本文件变量？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
B --> C["默认空参数；自动求差；有待办请求 API；不写湖"]
A -->|否| D{"以脚本直接运行？"}
D -->|是| E["main 读取终端参数；write 与日期门禁保持"]
D -->|否| F["模块导入；不启动入口"]
```
'''
new_code = '\n\n'.join(c.source for c in notebook.cells if c.cell_type == 'code')
definitions = lambda code: {n.name: ast.dump(n) for n in ast.parse(code).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
old_definitions, new_definitions = definitions(old_code), definitions(new_code)
assert old_definitions.keys() == new_definitions.keys()
assert {name for name in old_definitions if old_definitions[name] != new_definitions[name]} == {
    'upgrade_fact_metadata', 'commit_complete_fact_partition', 'commit_calendar_partition'}
comments = lambda code: [t.string for t in tokenize.generate_tokens(io.StringIO(code).readline) if t.type == tokenize.COMMENT]
assert all(comment in comments(new_code) for comment in comments(old_code))
nbformat.validate(notebook)
(ROOT / REL).write_text(nbformat.writes(notebook), encoding='utf8', newline='\n')

boundary = ('a04/b02 保持三个独立事务边界：兼容旧 metadata 迁移替换整个事实表根，'
            '每个事实完整月叶及本次必要的新建零行标记共同提交，每个 interest_rate 日历完整月叶随后独立提交。'
            '已有根标记和未触达叶保持原样，空事实以 staged_path=None 显式删除旧叶。'
            '保留本环节现有 staging 和正式复读验收；正式验收在共享事务内，成功退出才报告已提交。'
            '首次备份失败保留原目标，安装或验收失败按实际移动倒序恢复；失败新数据隔离留存，'
            '恢复不完整另保留旧备份，staging 清理且不递归删除正式表根。'
            '此前成功事实和日历叶保留，日历失败不撤销事实，下次人工运行可无 API 修复状态；'
            '失败状态落盘不等于采集完成，没有独立日期水位。'
            '共享模块只负责路径安装和恢复，不改变本环节的采集范围、来源门禁或 metadata 迁移判定。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    text = path.read_text(encoding='utf8')
    text = text.replace('与 a04/b01 共用', '与 a04/b01、b02 共用')
    if relative.endswith('/AGENTS.md'):
        anchor = '其他入口继续使用现有实现，按用户选定的环节逐项接入。'
        assert anchor in text
        text = text.replace(anchor, boundary + anchor, 1)
    elif relative.endswith('/README.md'):
        anchor = '\n## 半自动运行约定'
        # 按现有一级说明边界插入，不创建第二份契约。
        position = text.index('\n## ', text.index('a04/b01 将本次全部变化宏观日历叶'))
        text = text[:position] + '\n' + boundary + '\n' + text[position:]
    path.write_text(text, encoding='utf8', newline='\n')

test_path = ROOT / '00_draft_collection_02/tests/test_b04_c02_interest_rate.py'
text = test_path.read_text(encoding='utf8')
text = text.replace('import pathlib\n', 'import pathlib\nimport sys\n', 1)
text = text.replace('C02 = load_module("test_b04_c02", C02_PATH)',
                    'C02 = load_module("test_b04_c02", C02_PATH)\nTRANSACTION = sys.modules[C02.StagedPathTransaction.__module__]')
text = text.replace('original_move = shutil.move', 'original_move = TRANSACTION.os.replace')
text = text.replace('mock.patch.object(C02.shutil, "move",', 'mock.patch.object(TRANSACTION.os, "replace",')
test_path.write_text(text, encoding='utf8', newline='\n')
print(json.dumps({'snapshot': str(SNAPSHOT), 'changed_business_functions': sorted(
    name for name in old_definitions if old_definitions[name] != new_definitions[name]),
    'all_original_comments_preserved': True}, ensure_ascii=False, indent=2))
