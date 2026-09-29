"""本次局部迁移：仅编辑 Notebook 源及对应文字/故障注入边界。"""
import ast
import json
import pathlib
import shutil
import tempfile

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b01_macro_release_calendar.ipynb')
SNAPSHOT = pathlib.Path(tempfile.mkdtemp(prefix='a04-b01-transaction-before-'))
files = [REL, REL.with_suffix('.py'), pathlib.Path('AGENTS.md'),
         pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md'),
         pathlib.Path('00_draft_collection_02/tests/test_b04_c01_macro_release_calendar.py'),
         pathlib.Path('00_draft_collection_02/a04_b01_docs_logs_verification_20260928.md')]
for relative in files:
    saved = SNAPSHOT / relative
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, saved)

notebook = nbformat.read(ROOT / REL, as_version=4)
cells = {cell.id: cell for cell in notebook.cells}
cells['b5ed1a92'].source = cells['b5ed1a92'].source.replace(
    'from config.settings import settings',
    'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction',
)
source = cells['f4c3582d'].source
source = source.replace('        moved_partitions = []\n        old_target_moved = False\n        new_target_installed = False\n        cleanup_recovery_paths = True\n', '')
start = source.index('        try:\n            if full_swap:')
end = source.index('\n        click.echo(\n            f"committed:', start)
install = source[start:end]
install = install[:install.index('        except Exception as commit_error:')]
install = install.replace('        try:\n', '''        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={TABLE_NAME}; function=commit_partitions; run_id={run_id}",
            ) as transaction:
''', 1)
body_start = install.index('            if full_swap:')
install = install[:body_start] + ''.join('    ' + line if line.strip() else line for line in install[body_start:].splitlines(keepends=True))
old = '''                    if target_had_existing:
                        shutil.move(str(target_path), str(backup_path))
                        old_target_moved = True
                    shutil.move(str(staging_path), str(target_path))
                    new_target_installed = True'''
assert old in install
install = install.replace(old, '                    transaction.replace(target_path=target_path, staged_path=staging_path)')
install = install.replace('                    backup_path.mkdir(parents=True, exist_ok=False)\n', '')
install = install.replace('                        saved_path = backup_path / relative_path\n', '')
move_start = install.index('                        had_existing = destination_path.exists()')
move_end = install.index('                        log_installed_partitions += 1', move_start)
install = install[:move_start] + '''                        # 共享模块在安装新叶前登记旧叶的实际备份，安装失败仍可恢复。
                        transaction.replace(
                            target_path=destination_path,
                            staged_path=source_path if should_exist else None,
                        )
''' + install[move_end:]
source = source[:start] + install + '''        except Exception:
            # 事务进入失败也清理 staging；进入后的安装与恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise
''' + source[end:]
ast.parse(source)
assert 'shutil.move' not in source and 'rollback_errors' not in source
cells['f4c3582d'].source = source
cells['4a5b544e'].source = '''if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b01_macro_release_calendar",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()
'''
cells['a04-b01-doc-imports'].source = cells['a04-b01-doc-imports'].source.replace('权威 Schema、宏观配置和项目设置', '权威 Schema、宏观配置、项目设置和共享安装/恢复模块')
cells['a04-b01-flow-overview'].source = cells['a04-b01-flow-overview'].source.replace('现有手写恢复本批；抛错', '共享模块恢复本批；保留失败新数据；抛错').replace('正式整表验收；成功才报告落盘', '事务内正式整表验收；成功退出才报告落盘')
cells['62335bcd'].source = '''## 分组复用、单次 staging 物化与共享恢复

完整期望已经由生成结果与不相交范围外可信旧行组成。`commit_partitions()` 排序并转换一次 Arrow，循环前准备分区位置映射、叶路径、字段列表和空表；这些对象供选取变化行、摘要、staging 检查与安装共同复用，不重新验收全表业务。

staging 写入后只物化一次完整表，按分区窄列建立索引，在内存逐叶核对行数和完整内容摘要；不逐叶调用 `to_table()`，也不逐叶扫描完整期望表。强制旧契约迁移仍核对完整 staging 摘要。

本次全部变化叶放在同一个 `StagedPathTransaction` 内依次替换或显式删除，删除使用 `staged_path=None`；应存在的暂存叶缺失仍报错。未触达叶和既有根标记保持原样。新表、完整期望为空或旧契约迁移改为在同一事务中安装整个表根。安装后的正式整表只复读一次，检查物理契约、总行数和完整内容摘要；验收必须在事务内完成。

共享模块按实际移动记录倒序恢复；首次备份失败保留原位旧目标，一处恢复失败仍继续恢复其余目标。恢复完整时清理 staging 和备份，已安装的失败新数据保留在隔离目录；恢复不完整时另保留旧备份，staging 仍清理。暂存或事务进入失败也会清理本批 staging。临时目录沿用同一 silver 根下的 `.<表名>.staging-<run_id>`、`.backup-<run_id>` 和 `.failed-<run_id>`；整根备份/隔离位于其中的 `_root` 子目录，逐叶证据保留分区相对路径。异常报告现场路径并保留原始原因链。

安装与正式复读阶段仍属待完成；成功退出共享事务后才报告 `calendar_state=committed; date_watermark=none; persisted=true`，不代表下游事实已完成。返回值仍为本次实际写入的变化叶行数，不计删除行。生成、数据验收、更新范围和日志仍归环节所有，共享模块只负责同一文件系统内路径安装与恢复；不提供跨目标原子可见性、进程强杀后的自动恢复或并发写入协调。
'''
cells['a04-b01-flow-f4c3582d'].source = '''### 流程：共享提交和整批恢复

```mermaid
flowchart TD
A["完整期望排序转 Arrow；预建分区及路径映射"] --> B["选取变化行；写 staging"]
B --> C["staging 一次物化；内存逐叶核对完整摘要"]
C --> T["进入一个共享事务"]
T --> D{"新表、空表或旧契约迁移？"}
D -->|是| E["共享模块备份旧根并安装整个表根"]
D -->|否| F["共享模块依次替换或显式删除变化叶"]
E --> G["事务内正式整表复读一次：物理契约、行数、完整摘要"]
F --> G
G --> H["成功退出后报告日历已提交；无独立水位文件"]
E -. 失败 .-> R["倒序隔离失败新目标并恢复旧目标；逐项尝试"]
F -. 失败 .-> R
G -. 失败 .-> R
R --> S{"恢复完整？"}
S -->|是| U["清理 staging 和备份；保留失败新数据；抛错"]
S -->|否| V["清理 staging；保留旧备份与失败新数据；抛错"]
B -. 失败 .-> X["清理 staging；正式目标未改动；抛错"]
C -. 失败 .-> X
T -. 进入失败 .-> X
```
'''
cells['dfa42b3a'].source = '''## Notebook 与脚本执行入口

与 a01/b01 使用相同的入口结构：仅在交互内核且不存在 `__file__` 时，以显式 `notebook_args` 和 `standalone_mode=False` 调用 Click，不读取内核的 `-f` 参数。这里保留本环节原有空参数默认值，生成正式湖只读计划，不调用 API。

终端直接运行同名 `.py` 时由 Click 读取命令行参数；只有显式 `--write` 才提交，成对日期与正式湖写入门禁仍由 main 执行。普通 Python 或 Notebook 内导入同名模块均不触发执行入口或 Schema 浏览。
'''
cells['a04-b01-flow-4a5b544e'].source = '''### 流程：Notebook 与脚本执行入口

```mermaid
flowchart TD
A{"内核已加载且没有脚本文件变量？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
B --> C["默认空参数：只读计划；不调用 API"]
A -->|否| D{"直接运行脚本？"}
D -->|是| E["Click 读取终端参数；遵守写入门禁"]
D -->|否| F["模块导入：不执行入口"]
```
'''
nbformat.validate(notebook)
(ROOT / REL).write_text(nbformat.writes(notebook), encoding='utf8', newline='\n')

scope = ('a04/b01 将本次全部变化宏观日历叶纳入一个共享事务；普通路径替换或显式删除变化叶，'
         '未触达叶与已有根标记保持原样；首次建表、完整期望为空或旧契约版本迁移使用整根替换。'
         'staging 一次物化及内存逐叶核对保持不变，正式整表的物理契约、行数和完整内容摘要在事务内复读一次；'
         '成功退出后才报告日历状态落盘。首次备份失败保留原目标，倒序恢复时一处失败仍尝试其余目标；'
         '失败新数据隔离留存，恢复不完整另保留旧备份，staging 均清理。没有独立日期水位，'
         '不改变完整理论比较、状态继承或显式日期写入边界。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    text = path.read_text(encoding='utf8')
    old_callers = '、b03 与 a03/b01、b02、b03、b04'
    assert old_callers in text
    text = text.replace(old_callers, '、b03、a03/b01、b02、b03、b04 与 a04/b01')
    if relative == '02_Futures_Lakehouse/AGENTS.md':
        anchor = '其他入口继续使用现有实现，按用户选定的环节逐项接入。'
        assert anchor in text
        text = text.replace(anchor, scope + anchor)
    elif relative.endswith('README.md'):
        # 独立段落避免继续加长已有共享模块总览。
        line = next(line for line in text.splitlines() if '已共用 [StagedPathTransaction]' in line)
        text = text.replace(line, line + '\n\n' + scope)
        text = text.replace('b01 清理失败的新分区，b02、b03、b04、b05、b06、b07、b08 保留隔离的新目标',
            'a01/b01 清理失败的新分区，a01/b02、b03、b04、b05、b06、b07、b08 保留隔离的新目标')
        text = text.replace('规则未变时继承事实状态；变更叶分区经 staging 和正式路径逐值复读后提交，旧策略必须无日期整表迁移。',
            '规则未变时继承事实状态，规则变化逐格点重置；变更叶在同一共享事务内安装并经正式整表复读后提交。旧契约版本写入迁移必须使用无日期自动模式；纯描述性 metadata 不触发历史重写。')
    path.write_text(text, encoding='utf8', newline='\n')

# 故障仍注入同一真实移动边界，只将底层操作从 shutil.move 对齐为 os.replace。
test_path = ROOT / '00_draft_collection_02/tests/test_b04_c01_macro_release_calendar.py'
test = test_path.read_text(encoding='utf8').replace('import pathlib\n', 'import pathlib\nimport sys\n', 1)
test = test.replace('real_move = self.module.shutil.move', 'transaction_module = sys.modules[self.module.StagedPathTransaction.__module__]\n            real_move = transaction_module.os.replace')
test = test.replace('                self.module.shutil,\n                "move",', '                transaction_module.os,\n                "replace",')
test_path.write_text(test, encoding='utf8', newline='\n')
(SNAPSHOT / 'scope.json').write_text(json.dumps({'snapshot':str(SNAPSHOT), 'files':[str(p) for p in files]}, indent=2), encoding='utf8')
print(SNAPSHOT)
