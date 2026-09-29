"""b01a 第 10—12 项：只编辑 Notebook 源与已有规范中的受影响说明。"""
import ast
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
TARGET = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b01a_position_rank_special_case_calibration.ipynb'
notebook = json.loads(TARGET.read_text(encoding='utf8'))
cells = {cell['id']: cell for cell in notebook['cells']}


def replace(cell_id, old, new):
    source = ''.join(cells[cell_id]['source'])
    assert source.count(old) == 1, (cell_id, old)
    cells[cell_id]['source'] = source.replace(old, new).splitlines(keepends=True)


def markdown(cell_id, source):
    cells[cell_id]['source'] = source.splitlines(keepends=True)


replace('3d5940bc', 'import os\n', '')
replace('3d5940bc', 'from config.settings import settings', 'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')
replace('b01a-commit-artifacts', '''        staging_path = artifact_path.parent / (
            f".{special_case['case_id']}.staging-{uuid.uuid4().hex}"
        )''', '''        run_id = uuid.uuid4().hex[:12]
        staging_path = artifact_path.parent / f".b01a-s-{run_id}"
        backup_path = artifact_path.parent / f".b01a-b-{run_id}"
        quarantine_path = artifact_path.parent / f".b01a-f-{run_id}"''')
commit_source = ''.join(cells['b01a-commit-artifacts']['source'])
start = commit_source.index('            if artifact_path.exists():')
end = commit_source.index('        except Exception:', start)
install_block = commit_source[start:end].replace(
    'os.replace(staging_path, artifact_path)',
    'transaction.replace(target_path=artifact_path, staged_path=staging_path)',
)
transaction_block = '''            with StagedPathTransaction(
                root_path=artifact_path.parent,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    "artifact=position_rank_special_cases; function=commit_artifacts; "
                    f"case_id={special_case['case_id']}; run_id={run_id}"
                ),
            ) as transaction:
''' + ''.join('    ' + line if line.strip() else line for line in install_block.splitlines(keepends=True))
cells['b01a-commit-artifacts']['source'] = (commit_source[:start] + transaction_block + commit_source[end:]).splitlines(keepends=True)
replace('30c24e87', 'if "ipykernel" in sys.modules:\n', 'if "ipykernel" in sys.modules and "__file__" not in globals():\n    notebook_args = []\n')
replace('30c24e87', 'args=[],', 'args=notebook_args,')
replace('b01a-boundary', '当前安装使用同目录 `os.replace()`。正式安装后复读失败时，现有异常分支仅清理 staging，已安装的案例目录仍在原位；这里如实描述现状，共享事务与失败恢复由后续改动处理。', '每个案例的完整三文件目录使用 `StagedPathTransaction` 单独安装，并在事务内正式复读。安装或验收失败时恢复提交前的目录缺失状态；已经安装的新目录移入隔离目录留存，staging 清理。恢复不完整时保留现场并停止，不自动重试。该机制使用同一文件系统内的目录替换，不提供进程终止后的自动恢复或并发写入协调。')
replace('b01a-flow-overview', '整目录安装；正式复读', '单案例事务：整目录安装并正式复读')
replace('b01a-flow-overview', '仅清理 staging；抛错；已安装目录不回滚', '按实际安装记录恢复；隔离已安装新目录；清理 staging')
replace('b01a-init-heading', '导入 Click、Requests、项目设置及唯一案例配置。', '导入 Click、Requests、项目设置、唯一案例配置及共享安装与恢复模块。')
replace('b01a-commit-heading', '只有正式复读通过，才由本函数输出', '只有正式复读通过并成功退出事务，才由本函数输出')
replace('b01a-commit-heading', '提交机制保持原样：在目标旁暂存，确认正式目录仍不存在后用 `os.replace()` 安装。失败只清理 staging 并报告具体阶段；安装后的正式目录目前不回滚。共享事务属于后续第 10 项，此前成功案例也不在当前案例的恢复范围内。', '在目标旁使用短运行标识创建 staging、备份和隔离路径；三个文件的格式与正式目录不变。staging 验收后进入 `StagedPathTransaction`，保留“正式目录仍不存在”的原有检查，再安装整个案例目录并完成正式复读。失败时共享模块恢复当前案例提交前的缺失状态，隔离已安装的新目录；恢复不完整则保留现场并抛错。staging 清理，后续案例停止，此前成功案例保留。已有证据始终只复读，不进入覆盖事务。')
diagram_init = '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%'
markdown('b01a-flow-commit', '''### 局部流程：单案例事务与落盘状态

```mermaid
''' + diagram_init + '''
flowchart TD
    A["准备 staging；逐个写三个文件；报告进度"] --> B["完整复读 staging"]
    B --> C["进入共享事务；确认正式目录仍不存在"]
    C --> D["整目录安装；仍 pending"]
    D --> E["事务内完整复读正式目录"]
    E --> F["成功退出事务；报告 committed 和已落盘状态"]
    A -. 失败 .-> X["清理 staging；抛错并停止后续案例"]
    B -. 失败 .-> X
    C -. 失败 .-> R["按实际移动记录恢复当前案例"]
    D -. 失败 .-> R
    E -. 失败 .-> R
    R --> Q{"恢复完整？"}
    Q -->|是| S["隔离已安装新目录；未移动的目标保持原样"]
    Q -->|否| T["保留备份与现场；报告恢复失败"]
    S --> X
    T --> X
    X --> U["此前成功案例保留；不自动重试"]
```
''')
markdown('7566e6fc', '''## Notebook 与脚本执行入口

与 b01、b02 一样，Notebook 使用 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，并用 `standalone_mode=False` 返回单元格。当前参数为 `[]`，不带 `--write`：已有证据只复读，缺失证据仍请求一次官方来源并验收，不落盘。

只有交互内核且没有 `__file__` 时才使用 Notebook 分支；Notebook 中导入同名 Python 模块不执行入口。直接运行 `.py` 时读取命令行参数。最后一格仅保存终端命令注释；正式归档需人工执行 `--write`。本入口没有日期、`--full` 或案例筛选参数。
''')
markdown('b01a-flow-entry', '''### 局部流程：Notebook 与脚本执行入口

```mermaid
''' + diagram_init + '''
flowchart TD
    A{"交互内核且没有文件路径变量？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
    B --> C["当前空参数：只读验收；证据缺失仍请求一次"]
    A -->|否| D{"直接运行脚本？"}
    D -->|是| E["main 读取命令行参数"]
    D -->|否| F["模块导入：不执行入口"]
```
''')
notebook['cells'].extend([
    {'cell_type': 'markdown', 'id': 'b01a-flow-terminal', 'metadata': {}, 'source': ('''### 局部流程：终端手动运行

下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。

```mermaid
''' + diagram_init + '''
flowchart LR
    A["在终端激活 latitude"] --> B["切换到项目根目录"]
    B --> C["手动运行对应 .py --write"]
    C --> D["已有证据只读；缺失案例逐个请求并归档"]
```
''').splitlines(keepends=True)},
    {'cell_type': 'code', 'id': 'b01a-terminal-command', 'metadata': {}, 'execution_count': None, 'outputs': [], 'source': (
        '# conda env list\n# conda activate latitude\n# cd E:\\Latitude_Analytics_v2\n'
        '# python 02_Futures_Lakehouse\\a02_Futures_Exchange_Reports\\b01a_position_rank_special_case_calibration.py --write\n'
    ).splitlines(keepends=True)},
])
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        ast.parse(''.join(cell['source']))
TARGET.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + '\n', encoding='utf8', newline='\n')

boundary = ('a02/b01a 以一个案例的完整三文件目录为事务范围，已有证据只复读，不进入覆盖事务。'
            '新案例在事务内安装并完整正式复读，成功退出后才报告已落盘；安装或验收失败恢复此前的目录缺失状态，'
            '已安装的新目录隔离留存，恢复不完整保留现场并停止，staging 清理。此前成功案例保留，'
            '不自动重试，不写独立日期水位；整批 ready 只统计已有正式证据与本次成功提交。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / relative
    text = path.read_text(encoding='utf8')
    assert '与 a02/b01 ' in text
    text = text.replace('与 a02/b01 ', '与 a02/b01、b01a ')
    if relative == '02_Futures_Lakehouse/AGENTS.md':
        anchor = '其他入口继续使用现有实现，按用户选定的环节逐项接入。'
        assert text.count(anchor) == 1
        text = text.replace(anchor, boundary + anchor)
    elif relative == '02_Futures_Lakehouse/README.md':
        anchor = '该模块使用同一文件系统内的路径替换'
        assert text.count(anchor) == 1
        text = text.replace(anchor, boundary + anchor)
    path.write_text(text, encoding='utf8', newline='\n')
print('Updated b01a Notebook and three existing specification documents; export pending.')
