"""宏观发布第 10—12 项：保留三种业务提交边界，接入既有路径事务。"""
import ast
import hashlib
import io
import json
import pathlib
import shutil
import tempfile
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
SNAPSHOT = pathlib.Path(tempfile.mkdtemp(prefix='a04-b03-transaction-before-'))
files = [REL, REL.with_suffix('.py'), pathlib.Path('AGENTS.md'),
         pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md'),
         pathlib.Path('00_draft_collection_02/tests/test_b04_c03_macro_release.py'),
         pathlib.Path('00_draft_collection_02/a04_b03_docs_logs_verification_20260928.md')]
for relative in files:
    saved = SNAPSHOT / relative
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, saved)
hashes = {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
          for p in (ROOT / '02_Futures_Lakehouse').rglob('*')
          if p.suffix in ('.py', '.ipynb', '.md') and p.is_file()}
(SNAPSHOT / 'production_hashes.json').write_text(json.dumps(hashes), encoding='utf8')
nb = nbformat.read(ROOT / REL, 4)
cells = {c.id: c for c in nb.cells}
old_code = '\n\n'.join(c.source for c in nb.cells if c.cell_type == 'code')
cells['83cfef17'].source += '\nfrom a00_04_staged_path_transaction import StagedPathTransaction\n'

for function, table in [('upgrade_fact_metadata', 'TABLE_NAME'),
                        ('commit_complete_fact_partition', 'TABLE_NAME'),
                        ('commit_calendar_partition', 'CALENDAR_TABLE_NAME')]:
    cell = cells['a04-b03-code-' + function]
    source = cell.source
    for line in ('        old_target_moved = False\n', '        new_target_installed = False\n',
                 '        cleanup_recovery_paths = True\n', '        saved_path = backup_path / relative_path\n',
                 '        target_had_partition = destination_path.exists()\n',
                 '        old_partition_moved = False\n', '        new_partition_installed = False\n'):
        source = source.replace(line, '')
    start = source.index('        try:\n', source.index('        log_phase = "install"'))
    end = source.index('\n        click.echo(\n            f"committed:', start)
    install = source[start:source.index('        except Exception as commit_error:', start)]
    if function == 'upgrade_fact_metadata':
        first = install.index('            if target_path.exists():')
        body = '            transaction.replace(target_path=target_path, staged_path=staging_path)\n'
    elif function == 'commit_complete_fact_partition':
        first = install.index('            target_path.mkdir(')
        body = '''            if not target_marker_path.exists():
                transaction.replace(
                    target_path=target_marker_path,
                    staged_path=staging_marker_path,
                    quarantine_new=False,
                )
                marker_created = True
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path if complete_table.num_rows else None,
            )
'''
    else:
        first = install.index('            saved_path.parent.mkdir(')
        body = '''            transaction.replace(
                target_path=destination_path,
                staged_path=source_path,
            )
'''
    last = install.index('\n            click.echo(', first)
    install = install[:first] + body + install[last:]
    body = install[len('        try:\n'):]
    context = '; partition={partition_key}' if function != 'upgrade_fact_metadata' else ''
    install = f'''        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"table={{{table}}}; function={function}; run_id={{run_id}}{context}",
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

cells['14766fcd'].source = '''if "ipykernel" in sys.modules and "__file__" not in globals():
    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = []
    main.main(
        args=notebook_args,
        prog_name="b03_macro_release",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()
'''

def doc(cell_id, text):
    cells[cell_id].source = text.strip() + '\n'

def flow(cell_id, title, graph):
    doc(cell_id, '### 流程：' + title + '\n\n```mermaid\nflowchart TD\n' + graph.strip() + '\n```')

doc('0370ee69', '''## dirty 完整事实叶与共享恢复

每个报告窗口只合并当前事实月叶，保留其他报告。来源输出和 dirty 完整叶各验收一次，复用 Arrow 写 staging 并作物理及逐值复读。普通正式复读直达当前叶或零行标记，不扫描其他月份。

旧版本迁移使用整根事务；普通事实提交的事务覆盖当前完整叶和必要的新标记；日历随后独立提交。共享模块负责安装、显式删除和失败恢复，业务验收仍在本环节，且正式复读必须在事务内部完成。成功退出事务才报告已提交。''')
doc('a04-b03-doc-upgrade_fact_metadata', '''### 旧版本事实整根迁移

无日期写入时，兼容旧版本事实先进行一次当前业务验收，暂存并作物理及逐值复读。一个 `StagedPathTransaction` 替换整个事实表根，在事务内正式整表复读并逐值比较，成功退出后报告迁移已提交。纯描述性 metadata 差异不触发此路径。

旧根与失败新根分别保存在本批 `.backup-<run_id>/_root` 和 `.failed-<run_id>/_root`。首次备份失败保留原根；安装或验收失败恢复原根，保留已安装的失败新根；恢复不完整另保留备份并抛出异常。暂存或事务进入失败也清理 staging。''')
doc('a04-b03-doc-commit_complete_fact_partition', '''### 当前完整事实叶与新增标记共同提交

dirty 完整月叶业务验收一次，复用 Arrow 暂存并逐值比较。一个共享事务覆盖当前月叶及本次必要的新建 `schema.parquet`；已有标记保持原样，新标记失败时直接移除。非空叶安装 staging 叶，空叶以 `staged_path=None` 显式删除旧叶，不能从来源路径缺失推断删除。

正式验收在事务内直读当前叶或零行标记，新建标记另确认零行。成功退出才报告事实已提交；日历随后独立提交。本函数不写日期水位。失败时按实际移动倒序恢复当前叶和新标记，失败新叶隔离留存，恢复不完整另保留旧备份；staging 清理，不递归删除正式表根，未触达叶与其他文件保留。''')
doc('a04-b03-doc-commit_calendar_partition', '''### 当前日历完整叶独立提交

上游当前 `macro_release/year/month` 叶必须已经存在；b03 不创建或迁移日历根。dirty 日历叶业务验收一次，暂存后作物理及逐值复读。一个独立共享事务安装当前日历叶，并在事务内正式复读与逐值比较。

成功退出才报告 `calendar_state=committed; collection_completion=per_grid; persisted=true`，表示状态已经保存；失败状态保存不等于采集完成。既有日历根标记、其他月份和 `interest_rate` 叶保持原样。日历失败仅恢复当前日历叶，已成功事实保留，下次人工运行可无 API 修复。失败新叶隔离留存，恢复不完整另保留备份，staging 清理；没有独立日期水位或新增业务重试。''')
flow('a04-b03-flow-upgrade_fact_metadata', '旧版本整根共享事务', '''A["旧事实一次业务验收；整根暂存与逐值复读"] --> B["共享事务备份旧根并安装新根"]
B --> C["事务内正式整表物理及逐值复读"]
C --> D["成功退出；清理备份与 staging；报告迁移提交"]
B -. 失败 .-> R["恢复旧根；保留失败新根；抛错"]
C -. 失败 .-> R
R --> S["恢复不完整另保留旧备份；staging 清理"]''')
flow('a04-b03-flow-commit_complete_fact_partition', '事实叶与新增标记共享事务', '''A["dirty 完整叶验收一次；暂存及逐值复读"] --> B["共享事务内安装必要的新标记"]
B --> C{"事实非空？"}
C -->|是| D["备份旧叶；安装新叶"]
C -->|否| E["显式删除旧叶"]
D --> F["事务内直读当前叶或标记；物理及逐值验收"]
E --> F
F --> G["成功退出；报告事实提交；日历后续独立提交"]
B -. 失败 .-> R["倒序恢复叶与新标记；保留失败新叶；抛错"]
D -. 失败 .-> R
E -. 失败 .-> R
F -. 失败 .-> R''')
flow('a04-b03-flow-commit_calendar_partition', '当前日历叶独立共享事务', '''A["确认上游叶存在；dirty 叶一次业务验收"] --> B["暂存当前叶；物理及逐值比较"]
B --> C["独立共享事务备份旧叶、安装新叶"]
C --> D["事务内直读正式叶；物理及逐值验收"]
D --> E["成功退出；报告状态已保存"]
C -. 失败 .-> R["恢复当前日历叶；此前成功事实保留；抛错"]
D -. 失败 .-> R
R --> S["失败新叶隔离；恢复不完整保留备份；staging 清理"]''')
for cell in nb.cells:
    if cell.cell_type == 'markdown':
        cell.source = cell.source.replace('原手写恢复；停止；此前成功叶保留', '共享模块恢复当前目标；停止；此前成功叶保留').replace('原恢复；立即停止；关闭会话', '共享模块恢复当前目标；立即停止；关闭会话')
cells['a04-b03-doc-imports'].source += '\n路径安装与失败恢复使用湖仓级 `a00_04_staged_path_transaction.py`；不新增共享层。\n'
cells['a04-b03-flow-83cfef17'].source = cells['a04-b03-flow-83cfef17'].source.replace('Schema', 'Schema 与共享事务', 1)
doc('f55c4a11', '''## Notebook 与脚本执行入口

与 a01/b01 同样：交互内核且没有 `__file__` 时，以显式 `notebook_args` 和 `standalone_mode=False` 调用 Click，不读取 PyCharm/Jupyter 的 `-f` 参数。默认空参数不带 `--write`，自动读取和求差；有待办仍请求 Eastmoney 并转换，但不提交事实或日历。

终端直接执行 `.py` 正常读取命令行参数，显式 `--write` 才提交；日期与正式湖门禁仍由 main 执行。普通导入或内核中导入同名模块均不启动业务或 Schema 浏览。''')
flow('a04-b03-flow-14766fcd', 'Notebook 与脚本入口', '''A{"交互内核且无脚本文件变量？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
B --> C["默认空参数；自动求差；有待办请求 API；不写湖"]
A -->|否| D{"脚本直接运行？"}
D -->|是| E["main 读取命令行参数"]
D -->|否| F["模块导入；不启动入口"]''')
new_code = '\n\n'.join(c.source for c in nb.cells if c.cell_type == 'code')
definitions = lambda s: {n.name: ast.dump(n) for n in ast.parse(s).body if isinstance(n, (ast.FunctionDef, ast.ClassDef))}
old, new = definitions(old_code), definitions(new_code)
assert old.keys() == new.keys()
changed = {n for n in old if old[n] != new[n]}
assert changed == {'upgrade_fact_metadata', 'commit_complete_fact_partition', 'commit_calendar_partition'}
comments = lambda s: [t.string for t in tokenize.generate_tokens(io.StringIO(s).readline) if t.type == tokenize.COMMENT]
assert all(c in comments(new_code) for c in comments(old_code))
nbformat.validate(nb)
(ROOT / REL).write_text(nbformat.writes(nb), encoding='utf8', newline='\n')

boundary = ('a04/b03 使用三个独立共享事务边界：兼容旧版本迁移替换整个事实表根，当前事实完整月叶与必要的新建零行标记共同提交，当前 macro_release 日历完整月叶随后独立提交。'
            'dirty 叶业务验收一次，staging 与正式路径物理及逐值复读；正式验收在事务内部，成功退出才报告提交。'
            '已有标记和未触达叶保持原样，空事实以 staged_path=None 显式删除旧叶。首次备份失败保留原目标，安装或验收失败按实际移动倒序恢复；'
            '失败新数据隔离留存，新标记回退时移除，恢复不完整另保留旧备份，staging 清理且不递归删除正式表根。'
            '日历失败不撤销已提交事实，下次人工运行可无 API 修复；失败状态保存不等于采集完成，没有独立日期水位。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    p = ROOT / relative
    s = p.read_text(encoding='utf8')
    s = s.replace('与 a04/b01、b02 共用', '与 a04/b01、b02、b03 共用')
    if relative != 'AGENTS.md':
        s = s.replace('当前仍使用原手写恢复，未接入共享安装模块。', boundary)
    p.write_text(s, encoding='utf8', newline='\n')

p = ROOT / '00_draft_collection_02/tests/test_b04_c03_macro_release.py'
s = p.read_text(encoding='utf8').replace('import pathlib\n', 'import pathlib\nimport sys\n', 1)
s = s.replace('C03 = load_module("test_b04_c03", C03_PATH)', 'C03 = load_module("test_b04_c03", C03_PATH)\nTRANSACTION = sys.modules[C03.StagedPathTransaction.__module__]')
s = s.replace('original_move = shutil.move', 'original_move = TRANSACTION.os.replace').replace('mock.patch.object(C03.shutil, "move",', 'mock.patch.object(TRANSACTION.os, "replace",')
p.write_text(s, encoding='utf8', newline='\n')
print(json.dumps({'snapshot': str(SNAPSHOT), 'changed_functions': sorted(changed), 'original_comments_preserved': True}, ensure_ascii=False, indent=2))
