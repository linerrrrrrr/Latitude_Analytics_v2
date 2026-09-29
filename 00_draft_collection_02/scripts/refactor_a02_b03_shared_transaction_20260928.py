"""a02/b03 的共享事务、入口和说明迁移；仅编辑 Notebook 源。"""
import hashlib
import json
import pathlib
import shutil
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b03_warehouse_receipt.ipynb')
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a02-b03-transaction-before-'))
paths = [ROOT/p for p in ('AGENTS.md', '.env.template', '.gitattributes', '.gitignore')]
for directory in ('02_Futures_Lakehouse', '03_Futures_Database', 'config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py', '.ipynb', '.md'))
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}, ensure_ascii=False, indent=2), encoding='utf8')
for relative in (RELATIVE, RELATIVE.with_suffix('.py'), pathlib.Path('AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md'), pathlib.Path('00_draft_collection_02/tests/test_b02_warehouse_receipt.py')):
    target = snapshot/relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT/relative, target)

notebook_path = ROOT/RELATIVE
notebook = json.loads(notebook_path.read_text(encoding='utf8'))
cells = {c['id']:c for c in notebook['cells']}

def source(cell_id):
    return ''.join(cells[cell_id]['source'])

def put(cell_id, text):
    cells[cell_id]['source'] = text.splitlines(keepends=True)

put('123d7ac2', source('123d7ac2').replace('from config.settings import settings', 'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction'))
for cell_id, function, table, indent in (
    ('df69af57', 'commit_complete_partition', 'TABLE_NAME', 8),
    ('a02-b03-calendar-commit', 'commit_calendar_partitions', 'CALENDAR_TABLE_NAME', 12),
):
    code = source(cell_id)
    pad = ' ' * indent
    start = code.index(pad + 'saved_path = backup_path / relative_path')
    end_marker = (pad + 'click.echo(\n' + pad + '    f"partition_committed:') if indent == 8 else pad + 'log_committed_partitions += 1'
    end = code.index(end_marker, start)
    formal_start = code.index(pad + '    log_phase = "formal_verify"', start)
    formal_end = code.index(pad + 'except Exception as commit_error:', formal_start)
    formal_code = ''.join('    '+line if line.strip() else line for line in code[formal_start:formal_end].splitlines(keepends=True))
    marker_setup = '''target_marker_path = target_path / "schema.parquet"
staging_marker_path = staging_path / "schema.parquet"

''' if indent == 8 else ''
    installation = '''if not target_marker_path.exists():
    transaction.replace(
        target_path=target_marker_path,
        staged_path=staging_marker_path,
        quarantine_new=False,
    )
transaction.replace(
    target_path=destination_path,
    staged_path=source_path if len(complete_table) else None,
)
''' if indent == 8 else '''transaction.replace(
    target_path=destination_path,
    staged_path=source_path,
)
'''
    replacement = ''.join(pad+line if line.strip() else line for line in marker_setup.splitlines(keepends=True))
    prefix = f'''try:
    log_phase = "install"
    click.echo(
        f"planning_progress: table={{{table}}}; function={function}; phase=install; status=started; "
        f"key={{partition_key}}; path={{destination_path}}; batch_state=pending; elapsed_s={{time.perf_counter() - log_started_at:.3f}}"
    )
    with StagedPathTransaction(
        root_path=target_path,
        staging_dir=staging_path,
        backup_dir=backup_path,
        quarantine_dir=quarantine_path,
        log_context=(
            f"table={{{table}}}; function={function}; "
            f"key={{partition_key}}; run_id={{run_id}}; scope=leaf"
        ),
    ) as transaction:
'''
    replacement += ''.join(pad+line if line.strip() else line for line in prefix.splitlines(keepends=True))
    replacement += ''.join(pad+'        '+line if line.strip() else line for line in installation.splitlines(keepends=True))+'\n'
    replacement += formal_code
    suffix = '''except Exception:
    cleanup_path = destination_path.parent
    while cleanup_path.is_relative_to(target_path):
        try:
            cleanup_path.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            break
        if cleanup_path == target_path:
            break
        cleanup_path = cleanup_path.parent
    raise
finally:
    shutil.rmtree(staging_path, ignore_errors=True)

'''
    replacement += ''.join(pad+line if line.strip() else line for line in suffix.splitlines(keepends=True))
    put(cell_id, code[:start]+replacement+code[end:])
    assert 'shutil.move' not in source(cell_id)
    assert '.rglob(' not in source(cell_id)
    compile(source(cell_id), cell_id, 'exec')

put('406f1843', '''## 当前事实叶 staging、共享安装与恢复

`commit_complete_partition()` 接收已完整验证的 dirty 事实叶，检查分区范围并生成预期行数/主键摘要。staging 写入零行标记和非空叶，通过物理契约与摘要复读后进入 `StagedPathTransaction`：本次新建的根标记与当前事实叶属于同一事务，已有标记保持原样；空结果通过 `staged_path=None` 显式删除旧叶。

正式标记、叶物理契约与主键摘要仍在本函数内检查，不重复完整业务 validator 或逐值读取全部业务列。验收在事务内完成，成功退出事务后才报告 `partition_committed: ... scope=leaf`。

共享模块按实际成功的移动记录恢复：首次备份失败保留原目标，安装或正式验收失败时隔离已安装的新叶并恢复旧叶；本次新建标记回退时移除。恢复完整才清理旧备份，恢复不完整保留备份并抛错，staging 均清理。本函数只尝试移除当前目标的空父目录，不再递归扫描或删除表根。

共享模块报告恢复结果，本函数报告失败阶段。事务仅覆盖当前叶及必要的新标记；此前成功叶保留，不提供跨表共同回滚、进程终止后的自动恢复或并发写入协调。
''')
put('a02-b03-flow-fact-commit', '''### 局部流程：当前事实叶安装与共享恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["已验证事实叶；检查范围并计算主键摘要"] --> B["写 staging 标记与非空叶；物理契约和摘要复读"]
    B --> C["进入共享事务：当前叶及必要的新标记"]
    C --> D["安装新标记；备份旧叶；安装或显式删除叶"]
    D --> E["本函数复读正式标记、叶物理契约与摘要"]
    E --> F["成功退出事务；报告当前事实叶已提交"]
    D -. 失败 .-> R["按实际移动倒序恢复；已安装新叶隔离留存"]
    E -. 失败 .-> R
    R --> S{"恢复完整？"}
    S -->|是| T["清理备份；移除空父目录；抛错"]
    S -->|否| U["保留备份和失败数据；抛错"]
    B -. 失败 .-> V["清理 staging；正式数据未改动"]
    C -. 进入失败 .-> V
    T --> W["staging 清理；此前成功叶保留"]
    U --> W
```
''')
put('a02-b03-calendar-commit-heading', '''## 仓单日历逐叶提交

只选择本次触达的仓单日历叶，保留叶内其他品种和日期；输入来自已完整验证的完成或失败状态生成函数。每个叶写 staging，复读物理契约与行数/主键摘要，再各自进入 `StagedPathTransaction` 安装；正式物理契约与摘要验收仍在事务内由本函数执行，不重复完整业务 validator。

每个日历叶独立恢复，正式根级日历标记只验证、不替换。安装或验收失败按实际移动恢复当前叶，已安装的新叶隔离留存；恢复不完整保留旧备份，staging 清理。日历失败不撤销此前已提交的事实或日历叶。

成功退出当前叶事务后才输出 `partition_committed: ... scope=leaf`，全部触达叶成功后才输出 `phase=calendar_state; persisted=true`。失败状态写入也走同一提交函数，因此“状态已落盘”不能推断 `is_fetch_completed=true`；本入口没有独立日期水位文件。
''')
put('a02-b03-flow-calendar-commit', '''### 局部流程：日历逐叶安装与共享恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["已验证日历；定位触达的 warehouse_receipt 叶"] --> B["逐叶取完整内容；保留其他品种和日期"]
    B --> C["写 staging；物理契约与主键摘要复读"]
    C --> D["进入当前叶共享事务；安装并正式摘要验收"]
    D --> E["成功退出事务；报告当前叶提交"]
    E --> F{"还有日历叶？"}
    F -->|是| B
    F -->|否| G["报告日历状态已落盘；date_watermark=none"]
    D -. 失败 .-> H["按实际移动恢复当前叶；新叶隔离留存"]
    H --> I["恢复不完整保留旧备份；抛错停止"]
    I --> J["staging 清理；此前成功事实和日历叶保留"]
    C -. 失败 .-> K["清理 staging；当前正式叶未改动"]
```
''')
put('4bf5b012', source('4bf5b012').replace('和项目设置。', '、项目设置和湖仓共享路径事务。'))
put('12f98087', source('12f98087').replace('它们当前不共同回滚。', '每个当前叶通过共享模块独立安装和恢复，事实与日历不共同回滚。'))
put('a02-b03-flow-overview', source('a02-b03-flow-overview').replace('事实单叶提交；正式物理契约与摘要复读', '事实单叶共享事务；正式物理契约与摘要复读').replace('日历逐叶提交；正式摘要复读', '日历逐叶共享事务；正式摘要复读'))

reference = json.loads((ROOT/'02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b02_futures_holding_reports.ipynb').read_text(encoding='utf8'))
refcells = {c['id']:c for c in reference['cells']}
put('25a0004c', ''.join(refcells['3adba1ea']['source']))
put('a02-b03-flow-entry', ''.join(refcells['a02-b02-flow-entry']['source']))
entry = source('d4e12ca7').replace('if "ipykernel" in sys.modules:', 'if "ipykernel" in sys.modules and "__file__" not in globals():')
entry = entry.replace('    main.main(\n        args=[],', '    notebook_args = []\n    main.main(\n        args=notebook_args,')
put('d4e12ca7', entry)
for ref_id in ('a02-b02-flow-terminal', 'a02-b02-terminal-command'):
    cell = json.loads(json.dumps(refcells[ref_id]))
    cell['id'] = ref_id.replace('a02-b02', 'a02-b03')
    cell['source'] = ''.join(cell['source']).replace('b02_futures_holding_reports.py', 'b03_warehouse_receipt.py').splitlines(keepends=True)
    notebook['cells'].append(cell)
notebook_path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1)+'\n', encoding='utf8', newline='\n')

description = ('a02/b03 保持事实叶先提交、日历叶随后逐个提交的顺序，每个当前叶使用独立事务；事实侧必要的新建零行标记与当前叶共同恢复，已有事实标记及日历根标记保持原样。'
    '空事实以显式删除旧叶表达。dirty 叶业务校验仍只执行一次，staging 与正式安装仍只复读物理契约、行数和主键摘要，正式验收在事务内完成，成功退出后才报告当前叶提交。'
    '安装或验收失败按实际移动恢复，已安装的新叶隔离留存；恢复不完整保留旧备份，staging 清理，不递归清理表根。'
    '此前成功事实和日历叶保留，日历失败不撤销事实；失败状态写入成功不等于采集完成，没有独立日期水位。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT/relative
    old_bytes = path.read_bytes()
    old = path.read_text(encoding='utf8')
    text = old.replace('与 a02/b01、b01a、b02 ', '与 a02/b01、b01a、b02、b03 ')
    if relative == '02_Futures_Lakehouse/AGENTS.md':
        text = text.replace('其他入口继续使用现有实现，按用户选定的环节逐项接入。', description+'其他入口继续使用现有实现，按用户选定的环节逐项接入。')
    if relative == '02_Futures_Lakehouse/README.md':
        text = text.replace('该模块使用同一文件系统内的路径替换', description+'该模块使用同一文件系统内的路径替换')
    assert old != text
    path.write_bytes(text.replace('\n', '\r\n').encode('utf8') if b'\r\n' in old_bytes else text.encode('utf8'))
print(snapshot)
