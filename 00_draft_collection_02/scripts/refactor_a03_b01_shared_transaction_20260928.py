"""a03/b01 共享事务与执行入口局部接入；不改变生成、比较与复读契约。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b01_external_market_calendar.ipynb')
PATH = ROOT / RELATIVE
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}
assert 'StagedPathTransaction' not in cells['44837881'].source
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b01-transaction-before-'))
hashes = {}
for path in [ROOT/'AGENTS.md', ROOT/'.env.template', *sorted((ROOT/'02_Futures_Lakehouse').rglob('*.md')),
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')), *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')),
             *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py')), ROOT/'03_Futures_Database/AGENTS.md',
             ROOT/'config/data_contracts.py', ROOT/'00_draft_collection_02/tests/test_b03_metadata_upgrade.py']:
    relative = path.relative_to(ROOT)
    hashes[relative.as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    if path in [PATH, PATH.with_suffix('.py'), ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md', ROOT/'02_Futures_Lakehouse/README.md', ROOT/'00_draft_collection_02/tests/test_b03_metadata_upgrade.py']:
        (snapshot/relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, snapshot/relative)
(snapshot/'hashes.json').write_text(json.dumps(hashes, ensure_ascii=False, indent=2), encoding='utf8')

def replace(cell_id, old, new):
    assert cells[cell_id].source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = cells[cell_id].source.replace(old, new)

replace('44837881', 'from config.settings import settings',
        'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')
source = cells['018ccc71'].source
source = source.replace('f".{TABLE_NAME}.staging-{run_id}"', 'f".a03-b01-s-{run_id[:12]}"')
source = source.replace('f".{TABLE_NAME}.backup-{run_id}"', 'f".a03-b01-b-{run_id[:12]}"')
source = source.replace('f".{TABLE_NAME}.failed-{run_id}"', 'f".a03-b01-f-{run_id[:12]}"')
start = source.index('    target_had_existing = target_path.exists()')
formal_start = source.index('        # 正式路径必须与完整期望表逐行一致', start)
formal_end = source.index('    except Exception as commit_error:', formal_start)
formal = ''.join('    ' + line if line.strip() else line for line in source[formal_start:formal_end].splitlines(keepends=True))
source = source[:start] + '''    target_had_existing = target_path.exists()
    full_swap = force_full_swap or expected_df.empty
    try:
        with StagedPathTransaction(
            root_path=silver_root,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=f"table={TABLE_NAME}; function=commit_partitions; run_id={run_id}",
        ) as transaction:
            if full_swap:
                # 空上游或 metadata 升级都以整根 swap 提交，避免部分叶混合契约。
                transaction.replace(target_path=target_path, staged_path=staging_path)
            else:
                for partition_key in partition_keys:
                    relative_path = pathlib.Path(*[
                        f"{name}={value}"
                        for name, value in zip(
                            PARTITION_COLUMNS,
                            partition_key,
                            strict=True,
                        )
                    ])
                    source_path = staging_path / relative_path
                    destination_path = target_path / relative_path

                    expected_mask = pd.Series(True, index=expected_df.index)
                    for column, value in zip(
                        PARTITION_COLUMNS,
                        partition_key,
                        strict=True,
                    ):
                        expected_mask &= expected_df[column].eq(value)
                    should_exist = bool(expected_mask.any())

                    if should_exist != source_path.is_dir():
                        raise FileNotFoundError(
                            f"staging 叶分区存在性与期望不一致：{relative_path}"
                        )

                    transaction.replace(
                        target_path=destination_path,
                        staged_path=source_path if should_exist else None,
                    )

''' + formal + '''    except Exception:
        # 事务进入前的异常也需清理 staging；进入后的恢复由共享模块负责。
        shutil.rmtree(staging_path, ignore_errors=True)
        if (
            not target_had_existing
            and target_path.is_dir()
            and next(target_path.rglob("*.parquet"), None) is None
        ):
            shutil.rmtree(target_path)
        raise

    return len(changed_rows_df)
'''
cells['018ccc71'].source = source

replace('54c00aed', '共享实体配置和项目设置。', '共享实体配置、项目设置和共享路径事务。')
replace('a03-b01-flow-init', '加载契约和配置', '加载契约、配置和共享事务')
replace('a03-b01-flow-overview', '整批变化叶安装；迁移或空期望整根替换', '同一共享事务：全部变化叶或整根替换')
replace('a03-b01-flow-overview', '尝试恢复本次旧目标；抛错停止', '逐项恢复本批旧目标；保留失败新数据；抛错')
cells['068d0136'].source = '''## staging、共享安装与整批失败恢复

`commit_partitions()` 先验收完整期望表并计算摘要，再暂存变化叶；metadata 迁移时暂存完整期望表。staging 根级零行标记使全部待删除或空期望也能复读。现有 staging 完整及逐叶业务校验、行数与内容摘要核对继续保留。

全部变化叶使用同一个 `StagedPathTransaction`。普通提交替换或显式删除变化叶，未触达叶和已有根级标记保持原样；metadata 迁移或完整期望为空时整根替换。删除使用 `staged_path=None`，应存在的暂存叶缺失仍报错。在同一事务内，本函数执行一次正式整表验收，核对完整业务约束、总行数和完整内容摘要；成功退出事务后才允许报告本批已落盘。返回值仍是实际写入的变化叶行数，删除行不计入。

本批临时目录位于同一 silver 根，分别为 `.a03-b01-s-<run_id前12位>`（staging）、`.a03-b01-b-...`（备份）和 `.a03-b01-f-...`（失败新数据隔离）。共享模块按实际移动记录倒序恢复；首次备份失败不会触碰仍在原位的旧目标，一处恢复失败仍继续其余目标。恢复完整时清理 staging 和备份，保留已安装新数据的隔离副本；恢复不完整时另保留旧备份，staging 仍清理。异常包含现场路径，并保留原安装或验收异常的原因链。

暂存或事务进入失败清理本批 staging；空湖失败恢复后清理无 Parquet 的新建表目录。本环节不写独立日期水位。共享模块仅处理同一文件系统内路径替换，不负责生成或数据验收，也不提供跨目录原子可见性、进程中断后自动恢复或并发写入协调。'''
cells['a03-b01-flow-commit'].source = '''### 局部流程：staging、共享安装与整批恢复

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A["完整期望验收与摘要；准备本批路径"] --> B["写零行标记及变化叶；迁移时写完整表"]
    B --> C["staging 完整及逐叶复读验收"]
    C --> D["进入一个共享事务"]
    D --> E{"metadata 迁移或完整期望为空？"}
    E -->|是| F["共享模块备份旧根；安装 staging 根"]
    E -->|否| G["共享模块替换或显式删除全部变化叶"]
    F --> H["本函数正式整表验收；行数与完整内容摘要一致"]
    G --> H
    H --> I["成功退出事务；返回本批写入行数"]
    F -. 失败 .-> R["共享模块倒序隔离新目标、恢复旧目标；逐项尝试"]
    G -. 失败 .-> R
    H -. 失败 .-> R
    R --> S{"恢复完整？"}
    S -->|是| T["清理 staging、备份；保留失败新数据；抛错"]
    S -->|否| U["清理 staging；保留备份与失败新数据；抛错"]
    B -. 失败 .-> V["清理 staging；正式目标未改动"]
    C -. 失败 .-> V
    D -. 进入失败 .-> V
```'''
cells['a3acee6c'].source = '''## Notebook 与脚本执行入口

与 a01/b01、b02 一样，Notebook 通过 `notebook_args` 显式传入 Click 参数，避免读取内核的 `-f` 参数，并使用 `standalone_mode=False` 返回单元格。当前参数为 `[]`，默认读取本地正式湖并生成完整只读计划，不调用外部 API、不提交。

只有交互内核且没有 `__file__` 时才进入 Notebook 分支；在 Notebook 中导入同名 Python 模块不执行入口。直接运行 `.py` 时读取终端参数。最后一格仅列出人工终端执行命令；正式提交需运行 `--write`，不能用显式日期截断正式写入范围。'''
replace('5dc7bd54', 'if "ipykernel" in sys.modules:', 'if "ipykernel" in sys.modules and "__file__" not in globals():')
replace('5dc7bd54', '    main.main(', '    notebook_args = []\n    main.main(')
replace('5dc7bd54', 'args=[]', 'args=notebook_args')
cells['a03-b01-flow-entry'].source = '''### 局部流程：Notebook 与脚本执行入口

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart TD
    A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；不读取内核参数"]
    B --> C["main.main；standalone_mode=False"]
    C --> D["当前空参数：本地全量只读计划"]
    A -->|否| E{"直接运行 Python 脚本？"}
    E -->|是| F["main 读取终端参数"]
    E -->|否| G["模块导入：不执行入口"]
```'''
notebook.cells.append(nbformat.v4.new_markdown_cell('''### 局部流程：终端手动运行

下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。

```mermaid
%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
flowchart LR
    A["在终端激活 latitude"] --> B["切换到项目根目录"]
    B --> C["手动运行对应 .py --write"]
    C --> D["本地完整比较；共同提交变化叶或整根迁移"]
```''', id='a03-b01-flow-manual'))
notebook.cells.append(nbformat.v4.new_code_cell('''# conda env list
# conda activate latitude
# cd E:\\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\\a03_External_Market_Data\\b01_external_market_calendar.py --write''', id='a03-b01-manual'))

assert notebook.metadata == before.metadata
for old in before.cells:
    assert {k:v for k,v in old.items() if k!='source'} == {k:v for k,v in cells[old.id].items() if k!='source'}
for cell in notebook.cells:
    if cell.cell_type == 'code':
        ast.parse(cell.source)
nbformat.validate(notebook)
nbformat.write(notebook, PATH)

boundary = ('a03/b01 将本次全部变化外部市场日历叶纳入同一事务；普通路径替换或显式删除变化叶，未触达叶与已有根级标记保持原样；'
            'metadata 迁移或完整期望为空时整根替换。staging 的完整及逐叶业务验收继续保留，正式整表业务校验、总行数和完整内容摘要在事务内复读；'
            '成功退出后才报告本批落盘。共享模块倒序逐项恢复，完整恢复仍保留失败新数据，恢复不完整另保留旧备份，staging 均清理。'
            '没有独立日期水位，不改变理论格点、状态继承及显式日期写入边界。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT/relative
    text = path.read_text(encoding='utf8')
    old = 'a01/b01、b02、b03、b04、b05、b06、b07、b08 与 a02/b01、b01a、b02、b03'
    assert old in text, relative
    text = text.replace(old, 'a01/b01、b02、b03、b04、b05、b06、b07、b08、a02/b01、b01a、b02、b03 与 a03/b01')
    if relative != 'AGENTS.md':
        anchor = '此前成功事实和日历叶保留，日历失败不撤销事实；失败状态写入成功不等于采集完成，没有独立日期水位。'
        assert text.count(anchor) == 1, relative
        text = text.replace(anchor, anchor + boundary)
    path.write_text(text, encoding='utf8', newline='\n')
print(snapshot)
