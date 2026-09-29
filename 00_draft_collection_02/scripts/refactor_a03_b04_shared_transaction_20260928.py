"""外部指数当前叶接入共享事务，保持业务验收及独立提交顺序。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT/'02_Futures_Lakehouse/a03_External_Market_Data/b04_external_index.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {c.id: c for c in notebook.cells}
assert 'StagedPathTransaction' not in cells['b03-c04-03'].source
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b04-tx-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py',
         *sorted((ROOT/'02_Futures_Lakehouse').rglob('*.md')),
         *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')),
         *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')),
         *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py'))]
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}, indent=2), encoding='utf8')
for path in (PATH, PATH.with_suffix('.py'), ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md', ROOT/'02_Futures_Lakehouse/README.md'):
    destination = snapshot/path.relative_to(ROOT)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, destination)

cells['b03-c04-03'].source += '\nfrom a00_04_staged_path_transaction import StagedPathTransaction\n'
cell = cells['a03-b04-fact-commit']
source = cell.source
start = source.index('    saved_path = backup_path / relative_path')
formal_start = source.index('        # 正式路径复读成功是日历可以推进完成水位的前提。', start)
formal_end = source.index('    except Exception as commit_error:', formal_start)
formal = ''.join('    '+line if line.strip() else line for line in source[formal_start:formal_end].splitlines(keepends=True))
end = source.index('    return committed_df', formal_end)
cell.source = source[:start]+'''    target_marker_path = target_path / "schema.parquet"
    staging_marker_path = staging_path / "schema.parquet"

    try:
        with StagedPathTransaction(
            root_path=target_path,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=f"dataset={DATASET_NAME}; function=commit_complete_fact_partition; partition={partition_key}",
        ) as transaction:
            if not target_marker_path.exists():
                transaction.replace(
                    target_path=target_marker_path,
                    staged_path=staging_marker_path,
                    quarantine_new=False,
                )
            transaction.replace(
                target_path=destination_path,
                staged_path=source_path if complete_table.num_rows else None,
            )

'''+formal+'''    finally:
        shutil.rmtree(staging_path, ignore_errors=True)

'''+source[end:]

cell = cells['a03-b04-calendar-commit']
source = cell.source
start = source.index('        saved_path = backup_path / relative_path')
formal_start = source.index('            committed_dataset = open_exact_dataset(', start)
formal_end = source.index('        except Exception as commit_error:', formal_start)
formal = ''.join('    '+line if line.strip() else line for line in source[formal_start:formal_end].splitlines(keepends=True))
end = source.index('    return len(touched_df)', formal_end)
cell.source = source[:start]+'''
        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=f"dataset={DATASET_NAME}; function=commit_calendar_partitions; partition={partition_key}",
            ) as transaction:
                transaction.replace(target_path=destination_path, staged_path=source_path)

'''+formal+'''        finally:
            shutil.rmtree(staging_path, ignore_errors=True)

'''+source[end:]

# 原注释移到它解释的事实校验单元格，文字不变。
comment = '# 事实生产者承担本表全部字段、配置映射与格点关系质检。'
assert cells['a03-b04-calendar-validation'].source.rstrip().endswith(comment)
cells['a03-b04-calendar-validation'].source = cells['a03-b04-calendar-validation'].source.rstrip()[:-len(comment)].rstrip()+'\n'
cells['a03-b04-fact-validation'].source = comment+'\n'+cells['a03-b04-fact-validation'].source

cells['b03-c04-21'].source = cells['b03-c04-21'].source.replace(
    'if "ipykernel" in sys.modules:', 'if "ipykernel" in sys.modules and "__file__" not in globals():').replace(
    '    main.main(', '    notebook_args = []\n    main.main(').replace('args=[]', 'args=notebook_args')

cells['b03-c04-02'].source += '\n\n湖仓级 `a00_04_staged_path_transaction.py` 负责路径安装与失败恢复；业务合并、数据校验及事务分组仍由本环节决定。'
cells['a03-b04-fact-commit-text'].source = '''## 当前事实叶与新增标记共同提交

完整叶验收并确认分区范围后写 staging 与零行标记，完整复读并逐值比较。每个事实叶使用一个 `StagedPathTransaction`：已有根标记保持原样；缺失的零行标记与当前叶共同恢复。非空结果安装完整新叶，空结果以显式删除旧叶表达。

正式路径完整复读和逐值比较在事务内执行，成功退出后才返回事实供日历回写。首次备份失败保持原目标；安装或验收失败按实际移动倒序恢复，只移除本次新增标记。失败新叶保留在 `.failed-<run_id>`，恢复不完整另保留 `.backup-<run_id>`；staging 清理，不递归删除正式表根。此前成功事实叶保留。'''
cells['a03-b04-calendar-commit-text'].source = '''## 当前日历完整叶独立提交

精确触达格点须与日历主键一一对应，按 dataset—年月选择完整叶，保留同月其他指标及此前分类的已提交状态。逐叶写 staging、完整验收并逐值比较；每个当前叶分别使用 `StagedPathTransaction` 安装，正式复读和逐值比较仍在事务内完成。

失败只恢复当前叶，此前成功事实及日历叶保留；已有日历根标记不替换。失败新叶隔离留存，恢复不完整另保留旧备份，staging 清理。全部触达叶成功后返回行数，不写独立日期水位。

日历失败不撤销已成功事实，再次运行仍按事实计数和日历状态共同判定待办，可能重新请求 API。本实现不提供跨表原子可见性、进程终止后的自动恢复或并发写入协调。'''
cells['b03-c04-18'].source = cells['b03-c04-18'].source.replace(
    '本轮统一入口日志。函数自行报告页级和行级进度、生成与提交日志归位留到第 5—6 项；重复校验、共享事务与执行入口仍保留现有实现。',
    '入口日志使用 `function/phase/status/elapsed_s` 和批次 `=` 分隔线；API 归一化完成不等于正式提交。事实与日历分别使用逐叶共享事务，只有正式复读成功并退出事务才返回；当前仍保留既有完整校验与批末全表复验。')
cells['b03-c04-20'].source = '''## Notebook 与脚本执行入口

与 a01/b01、b02 一样，Notebook 用显式 `notebook_args` 和 `standalone_mode=False` 执行；默认 `[]` 不写湖，有待办时仍请求 API。交互分支同时要求没有 `__file__`，在内核中导入同名 Python 模块不会触发业务。直接执行 `.py` 时正常读取终端参数。

最后一格仅保存手动终端命令注释，运行全部单元格不会额外启动正式写入。正式自动更新使用 `--write`，不能用显式日期截断正式湖生产范围。'''

def flow(title, body):
    return '### 局部流程：'+title+'\n\n```mermaid\n%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\nflowchart TD\n'+body+'\n```'

cells['a03-b04-flow-fact-commit'].source = flow('事实叶与新增标记共享事务', '''A["完整叶验收；写 staging 并逐值复读"] --> B["进入共享事务；必要时安装新标记"]
B --> C["替换当前叶；空结果显式删除"]
C --> D["事务内正式复读；完整逐值比较"]
D --> E["成功退出；返回事实；日历尚未回写"]
B -. 失败 .-> R["按实际移动倒序恢复；移除新增标记"]
C -. 失败 .-> R
D -. 失败 .-> R
R --> S["失败新叶隔离；恢复不全保留备份；抛错"]
A -. 失败 .-> T["清理 staging；抛错"]''')
cells['a03-b04-flow-calendar-commit'].source = flow('日历独立叶事务', '''A["触达格点一一对应；选取完整日历叶"] --> B["完整验收；staging 写入和逐值复读"]
B --> C["当前叶共享事务；安装并正式逐值验收"]
C --> D["成功退出；继续下一日历叶"]
D --> E["全部成功；返回触达行数；无独立水位"]
C -. 失败 .-> R["仅恢复当前旧叶；此前成功事实和日历叶保留"]
R --> S["失败新叶隔离；恢复不全保留备份；抛错"]''')
cells['a03-b04-flow-21'].source = flow('Notebook 与脚本入口', '''A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；默认不写入"]
A -->|否| C{"直接执行脚本？"}
C -->|是| D["Click 读取终端参数并运行"]
C -->|否| E["模块导入不运行"]''')
cells['a03-b04-flow-overview'].source = cells['a03-b04-flow-overview'].source.replace(
    '提交并复读事实；核对 0/1 与清退计数', '事实叶共享事务及正式复读；核对计数').replace(
    '有采集格点才提交对应日历状态', '有采集格点才独立提交对应日历叶')
cells['a03-b04-flow-19'].source = cells['a03-b04-flow-19'].source.replace('提交事实完整叶', '共享事务提交事实完整叶')
notebook.cells.append(nbformat.v4.new_markdown_cell(flow('终端手动运行', '''A["在终端激活 latitude；切换项目根目录"] --> B["人工执行对应 Python 脚本 --write"]
B --> C["自动计算待办与清退；独立提交事实和日历叶"]'''), id='a03-b04-flow-terminal'))
notebook.cells.append(nbformat.v4.new_code_cell('''# conda env list
# conda activate latitude
# cd E:\\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\\a03_External_Market_Data\\b04_external_index.py --write
''', id='a03-b04-terminal'))

ast.parse('\n\n'.join(c.source for c in notebook.cells if c.cell_type=='code'))
nbformat.validate(notebook)
assert [c.id for c in notebook.cells[:len(before.cells)]] == [c.id for c in before.cells]
for old, new in zip(before.cells, notebook.cells):
    assert {k:v for k,v in old.items() if k!='source'} == {k:v for k,v in new.items() if k!='source'}
PATH.write_text(nbformat.writes(notebook), encoding='utf8', newline='\n')

description = ('a03/b04 对每个完整指数分类—年月事实叶与本次必要的新建零行标记使用一个共享事务；'
               '每个日历完整叶随后独立提交，已有根标记保持原样，空事实以显式删除旧叶表达。'
               'staging 和正式完整校验及逐值比较仍由本环节执行，正式验收在事务内完成。'
               '首次备份失败保留原目标；安装或验收失败按实际移动恢复，失败新叶隔离留存，'
               '新增标记回退时移除，恢复不完整另保留旧备份，staging 清理且不递归删除正式表根。'
               '此前成功事实与日历叶保留；日历失败不撤销事实，下次仍按事实计数与日历状态共同判定待办，'
               '可能重新请求 API。没有独立日期水位，不改变分页、精确待办、无 API 清退及显式日期写入边界。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT/relative
    text = path.read_text(encoding='utf8')
    assert '与 a03/b01、b02、b03' in text
    text = text.replace('与 a03/b01、b02、b03', '与 a03/b01、b02、b03、b04')
    if relative == '02_Futures_Lakehouse/AGENTS.md':
        text = text.replace('其他入口继续使用现有实现，按用户选定的环节逐项接入。', description+'其他入口继续使用现有实现，按用户选定的环节逐项接入。')
    elif relative == '02_Futures_Lakehouse/README.md':
        text = text.replace('该模块使用同一文件系统内的路径替换', description+'该模块使用同一文件系统内的路径替换')
    path.write_text(text, encoding='utf8', newline='\n')
print(snapshot)
