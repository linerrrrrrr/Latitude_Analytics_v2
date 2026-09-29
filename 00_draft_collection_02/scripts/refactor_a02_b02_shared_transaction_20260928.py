"""a02/b02：只修改 Notebook 事务/入口及受影响说明；Python 由标准同步入口导出。"""
import hashlib
import json
import pathlib
import shutil
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b02_futures_holding_reports.ipynb')
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a02-b02-transaction-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'.env.template', ROOT/'.gitattributes', ROOT/'.gitignore']
for directory in ('02_Futures_Lakehouse', '03_Futures_Database', 'config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py', '.ipynb', '.md'))
hashes = {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
for relative in (RELATIVE, RELATIVE.with_suffix('.py'), pathlib.Path('AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/AGENTS.md'), pathlib.Path('02_Futures_Lakehouse/README.md')):
    target = snapshot/relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT/relative, target)
(snapshot/'hashes.json').write_text(json.dumps(hashes, ensure_ascii=False, indent=2), encoding='utf8')

notebook_path = ROOT/RELATIVE
notebook = json.loads(notebook_path.read_text(encoding='utf8'))
cells = {cell['id']: cell for cell in notebook['cells']}

def source(cell_id):
    return ''.join(cells[cell_id]['source'])

def put(cell_id, text):
    cells[cell_id]['source'] = text.splitlines(keepends=True)

put('12bacfb9', source('12bacfb9').replace('from config.settings import settings', 'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction'))
code = source('aa14209b')
start = code.index('        saved_path = backup_path / relative_path')
end = code.index('\n        click.echo(\n            f"partition_committed:', start)
code = code[:start] + '''        target_marker_path = target_path / "schema.parquet"
        staging_marker_path = staging_path / "schema.parquet"

        try:
            log_phase = "install"
            click.echo(
                f"planning_progress: table={table_name}; function=commit_complete_partition; phase=install; status=started; "
                f"key={partition_key}; path={destination_path}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
            )
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={table_name}; function=commit_complete_partition; "
                    f"key={partition_key}; run_id={run_id}; scope=leaf"
                ),
            ) as transaction:
                if not target_marker_path.exists():
                    transaction.replace(
                        target_path=target_marker_path,
                        staged_path=staging_marker_path,
                        quarantine_new=False,
                    )
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path if len(complete_table) else None,
                )

                log_phase = "formal_verify"
                click.echo(
                    f"planning_progress: table={table_name}; function=commit_complete_partition; phase=formal_verify; status=started; "
                    f"key={partition_key}; batch_state=pending; elapsed_s={perf_counter() - log_started_at:.3f}"
                )
                committed_df = read_complete_partition(
                    target_path,
                    schema,
                    partition_columns,
                    partition_key,
                    validator,
                    f"正式 {table_name}",
                )
                if not pandas_to_arrow(committed_df.loc[:, schema.names], schema).equals(complete_table):
                    raise ValueError("正式完整分区内容检查失败。")
        except Exception:
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
''' + code[end:]
put('aa14209b', code)

put('5d672c31', '''## 单叶 staging、共享安装与失败恢复

`commit_complete_partition()` 完整验证待提交叶并检查分区范围，转换为 Arrow，写入 staging 的零行根标记和非空叶。staging 完整复读并逐值一致后，进入 `StagedPathTransaction`，安装缺失的正式根标记并替换当前叶；空结果使用 `staged_path=None` 显式删除旧叶，保留可读零行根标记。已有标记保持原样。

正式目标叶的完整业务校验和逐值比对仍在本函数内完成。只有验收通过且成功退出事务才返回并报告 `partition_committed: ... scope=leaf`。共享模块按实际成功的移动记录恢复旧叶，首次备份失败不会移走原目标；本次新建标记与当前叶共同恢复，标记无需隔离。

安装或正式验收失败时，已安装的新叶隔离留存；恢复完整才清理旧备份，恢复不完整则保留备份并抛错。staging 清理，本函数仅尝试移除目标下的空父目录，不清理备份或失败数据。共享模块负责恢复日志，本函数报告失败阶段；该机制不提供进程终止后的自动恢复或并发写入协调。

两张事实和各日历叶仍分别提交，后一个叶失败时此前成功叶保留；格点完成仍由两张正式事实计数和两类报告日历状态共同证明。本轮不改变查询、业务校验、更新范围及日历回写顺序，也没有独立日期水位文件。
''')
put('a02-b02-flow-commit', '''### 局部流程：单叶安装与共享恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["验收完整叶与分区范围；转换 Arrow"] --> B["写 staging 标记和非空叶；完整复读比对"]
    B --> C["进入共享事务：当前叶及必要的新标记"]
    C --> D["安装新标记；备份旧叶；安装或显式删除叶"]
    D --> E["本函数完整复读正式叶；逐值比对"]
    E --> F["成功退出事务；报告当前叶已提交"]
    D -. 失败 .-> R["共享模块按实际移动倒序恢复；新叶隔离留存"]
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
put('3adba1ea', '''## Notebook 与脚本运行入口

Notebook 使用显式 `notebook_args = []` 和 `standalone_mode=False`，避免读取内核的 `-f` 参数。默认不带 `--write`；有待办仍会认证和请求，只读不代表不联网。只有交互内核且没有 `__file__` 时才走 Notebook 分支；在 Notebook 中导入同名 Python 模块不执行入口。直接运行 `.py` 时使用命令行参数。

最后一个代码单元格仅保存终端命令注释，实际提交由操作者在终端显式运行 `--write`。自动模式按上游待办推进；成对日期只允许只读检查或非正式测试湖写入，本入口没有 `--full`。
''')
put('a02-b02-flow-entry', '''### 局部流程：Notebook 与脚本执行入口

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A{"交互内核且无 __file__？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
    B --> C["默认自动只读；有待办仍会认证和请求"]
    A -->|否| D{"直接运行脚本？"}
    D -->|是| E["main 读取命令行参数"]
    D -->|否| F["模块导入：不执行入口"]
```
''')
entry = source('abaf342b').replace('if "ipykernel" in sys.modules:', 'if "ipykernel" in sys.modules and "__file__" not in globals():')
entry = entry.replace('    main.main(\n        args=[],', '    notebook_args = []\n    main.main(\n        args=notebook_args,')
put('abaf342b', entry)
notebook['cells'].extend([
    {'cell_type':'markdown', 'id':'a02-b02-flow-terminal', 'metadata':{}, 'source':'''### 局部流程：终端手动运行

下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart LR
    A["在终端激活 latitude"] --> B["切换到项目根目录"]
    B --> C["手动运行对应 .py --write"]
    C --> D["按上游待办采集并逐叶提交"]
```
'''.splitlines(keepends=True)},
    {'cell_type':'code', 'id':'a02-b02-terminal-command', 'metadata':{}, 'execution_count':None, 'outputs':[], 'source':r'''# conda env list
# conda activate latitude
# cd E:\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\a02_Futures_Exchange_Reports\b02_futures_holding_reports.py --write
'''.splitlines(keepends=True)},
])
assert 'from a00_04_staged_path_transaction import StagedPathTransaction' in source('12bacfb9')
assert 'shutil.move' not in source('aa14209b')
notebook_path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1)+'\n', encoding='utf8', newline='\n')

description = ('a02/b02 保持单叶事务：当前事实或日历完整叶与本次新建的零行标记共同恢复，已有标记保持原样；空事实以显式删除旧叶表达。'
    'staging 和正式叶的完整校验及逐值复读仍由环节承担，正式复读在事务内执行，成功退出后才报告当前叶提交。'
    '安装或验收失败按实际移动恢复，已安装的新叶隔离留存，新标记回退时移除；恢复不完整保留旧备份，staging 清理。'
    '两张事实及各日历叶依次提交，此前成功叶保留；完成凭证仍由两张事实计数和两类日历状态共同证明，不写独立日期水位。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT/relative
    old = path.read_text(encoding='utf8')
    text = old.replace('与 a02/b01、b01a ', '与 a02/b01、b01a、b02 ')
    if relative == '02_Futures_Lakehouse/AGENTS.md':
        text = text.replace('其他入口继续使用现有实现，按用户选定的环节逐项接入。', description+'其他入口继续使用现有实现，按用户选定的环节逐项接入。')
    if relative == '02_Futures_Lakehouse/README.md':
        text = text.replace('该模块使用同一文件系统内的路径替换', description+'该模块使用同一文件系统内的路径替换')
    assert text != old, relative
    original_bytes = path.read_bytes()
    path.write_bytes(text.replace('\n', '\r\n').encode('utf8') if b'\r\n' in original_bytes else text.encode('utf8'))
print(snapshot)
