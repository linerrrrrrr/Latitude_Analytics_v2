"""生意社 raw 日期与日历叶分别接入共享事务；只编辑 Notebook 源。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b02_domestic_spot_basis.ipynb')
PATH = ROOT / RELATIVE
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}
assert 'StagedPathTransaction' not in cells['0402fa68'].source
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b02-tx-before-'))
tracked_paths = [ROOT/'AGENTS.md', ROOT/'.env.template', ROOT/'03_Futures_Database/AGENTS.md',
                 ROOT/'config/data_contracts.py', *sorted((ROOT/'02_Futures_Lakehouse').rglob('*.md')),
                 *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.ipynb')),
                 *sorted((ROOT/'02_Futures_Lakehouse').glob('a*/*.py')),
                 *sorted((ROOT/'02_Futures_Lakehouse').glob('a00_*.py')),
                 ROOT/'00_draft_collection_02/tests/test_b03_c02_raw_staging_lifecycle.py']
hashes = {}
for path in tracked_paths:
    relative = path.relative_to(ROOT)
    hashes[relative.as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    if path in (PATH, PATH.with_suffix('.py'), ROOT/'AGENTS.md', ROOT/'02_Futures_Lakehouse/AGENTS.md',
                ROOT/'02_Futures_Lakehouse/README.md', ROOT/'00_draft_collection_02/tests/test_b03_c02_raw_staging_lifecycle.py'):
        (snapshot/relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, snapshot/relative)
(snapshot/'hashes.json').write_text(json.dumps(hashes, ensure_ascii=False, indent=2), encoding='utf8')

cells['0402fa68'].source = cells['0402fa68'].source.replace(
    'from config.settings import settings',
    'from config.settings import settings\nfrom a00_04_staged_path_transaction import StagedPathTransaction')

source = cells['e2d51f8b'].source
begin = source.index('    target_had_existing = target_path.exists()', source.index('def commit_raw_response('))
end = source.index('    return committed_evidence', begin)
source = source[:begin] + '''    try:
        with StagedPathTransaction(
            root_path=raw_root,
            staging_dir=staging_path,
            backup_dir=backup_path,
            quarantine_dir=quarantine_path,
            log_context=(
                f"dataset={DATASET_NAME}; function=commit_raw_response; "
                f"date={observation_date}; run_id={run_id}"
            ),
        ) as transaction:
            transaction.replace(target_path=target_path, staged_path=staging_path)

            committed_evidence = inspect_raw_leaf(target_path)
            if committed_evidence != staged_evidence:
                raise ValueError("生意社 raw 正式路径逐字节复读失败。")
    finally:
        # 事务进入前的异常也需清理 staging；安装后的恢复由共享模块负责。
        shutil.rmtree(staging_path, ignore_errors=True)

''' + source[end:]
cells['e2d51f8b'].source = source

source = cells['e77863a1'].source
begin = source.index('        saved_path = backup_path / relative_path')
formal_begin = source.index('            committed_dataset = open_exact_dataset(', begin)
formal_end = source.index('        except Exception as commit_error:', formal_begin)
formal = ''.join('    '+line if line.strip() else line for line in source[formal_begin:formal_end].splitlines(keepends=True))
end = source.index('    return len(touched_df)', formal_end)
source = source[:begin] + '''
        try:
            with StagedPathTransaction(
                root_path=silver_root,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; "
                    f"partition={relative_path.as_posix()}; run_id={run_id}"
                ),
            ) as transaction:
                transaction.replace(
                    target_path=destination_path,
                    staged_path=source_path,
                )

''' + formal + '''        finally:
            # 当前叶单独恢复；此前已成功的 raw 和日历叶不回退。
            shutil.rmtree(staging_path, ignore_errors=True)

''' + source[end:]
cells['e77863a1'].source = source

cells['c2a1fc43'].source += '\n\n安装与失败恢复由湖仓级 `a00_04_staged_path_transaction.py` 负责；raw 摘要与日历验收仍由本环节执行。'
cells['74881b81'].source = '''## raw 完整性、自动待办与逐日期共享事务

`plan_raw_grids()` 区分已完成、仅需日历修复和需要 HTTP 采集的日期。原文与摘要完整但日历陈旧时，无需创建 HTTP Session；缺少原文或摘要不匹配才进入采集。HTTP 200 的任意原始字节均可归档，包括空正文；没有页面解析或内容质量判断。

`commit_raw_response()` 暂存一个日期的 `response.html` 和 `response.sha256` 并复读摘要，然后用一个 `StagedPathTransaction` 安装完整日期目录。正式路径的字节与摘要仍在事务内复读；只有成功退出后才返回证据，供后续日历回写使用。

安装或正式验收失败时，共享模块按实际移动记录恢复当前日期的旧目录；首次备份失败不删除原目录。已安装的新目录保留在本批 `.quarantine-<run_id>` 内，恢复不完整时另保留 `.backup-<run_id>`；staging 清理，异常保留原因链与现场路径。隐藏的恢复目录不算正式完成。HTTP 非 200 的响应正文仍由 `preserve_failed_response()` 写入 `.failed-<batch_id>`，与事务隔离目录分开。

本次只替换路径安装与恢复实现。`create_http_session()` 现有 `Retry(total=4)` 配置保持不变；一次日期采集调用可能包含适配器重试，不能把日期数等同于实际 HTTP 尝试次数。'''
cells['ac34e98a'].source = '''## 日历状态回写与逐叶共享事务

raw 正式提交并复读成功后，才生成日历完成状态。HTTP 200 的归档记录统一写为 `success + passed`、条数 1、无缺失；HTTP 失败只回写未完成的错误状态。日历按完整 `dataset_name/year/month` 叶提交，保留同叶其他日期和实体，已有根级零行契约标记保持原样。

`commit_calendar_partitions()` 保留现有 staging 与正式路径的完整校验、逐值比较及返回行数。每个日历叶分别使用一个共享事务，正式复读必须在事务内完成；安装或验收失败只恢复当前叶，此前成功叶保留。失败新叶留在本批 `.dim_external_market_calendar.failed-<run_id>`，恢复不完整时保留旧备份，staging 清理。

raw 日期事务和日历叶事务相互独立。日历提交失败不撤销已经成功归档的 raw；再次运行时可复读 raw，无 API 修复日历。本环节没有独立日期水位文件，也不提供跨目录原子可见性、进程终止后的自动恢复或并发写入协调。'''
cells['5c215ad9'].source = '''## Notebook 与脚本执行入口

与 a01/b01、b02 一样，Notebook 用显式 `notebook_args` 调用 Click，并以 `standalone_mode=False` 返回单元格，避免读入内核参数。默认 `[]` 不写湖，但存在 API 待办时仍会请求生意社；只有纯状态修复或已经完整时才不请求网络。

Notebook 分支要求交互内核且不存在 `__file__`，因此在内核中导入同名 Python 模块不会执行入口。直接运行 `.py` 时读取终端参数。最后一格仅保存手动终端命令注释，不在运行全部单元格时额外启动写入；正式写入使用 `--write`，不得用显式日期截断正式范围。'''
cells['e3f69e92'].source = cells['e3f69e92'].source.replace(
    'if "ipykernel" in sys.modules:', 'if "ipykernel" in sys.modules and "__file__" not in globals():').replace(
    '# Notebook 默认只读计划；写入必须由操作者显式修改参数。',
    '# Notebook 默认不写入；有待采日期时仍会请求 HTTP，写入须显式修改参数。').replace(
    '    main.main(', '    notebook_args = []\n    main.main(').replace('args=[]', 'args=notebook_args')

def flow(title, body, cell_id):
    return nbformat.v4.new_markdown_cell(
        f'### 局部流程：{title}\n\n```mermaid\n'
        '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        'flowchart TD\n'+body+'\n```', id=cell_id)

flows = {
    'e2d51f8b': flow('raw 待办与逐日期安装', '''    A["日历 required 日期；复读 raw 原文与摘要"] --> B{"raw 与日历都完整？"}
    B -->|是| C["跳过已完成日期"]
    B -->|仅 raw 完整| D["生成无 HTTP 状态修复计划"]
    B -->|raw 不完整| E["请求该日原始字节；保留现有适配器重试"]
    E --> F{"HTTP 200？"}
    F -->|否且写入| G["隐藏目录保存失败正文；回写未完成状态；抛错"]
    F -->|是且写入| H["暂存两文件；复读字节与 SHA-256"]
    H --> I["当前日期共享事务：安装目录并正式复读"]
    I --> J["退出事务成功；返回 raw 证据"]
    I -. 失败 .-> K["恢复当前旧目录；保留失败新数据；抛错"]''', 'a03-b02-flow-raw-transaction'),
    'e77863a1': flow('日历状态与独立叶事务', '''    A["已提交 raw 证据，或请求错误"] --> B["生成相应日历状态"]
    B --> C["选取触达的完整日历叶；保留同叶其他记录"]
    C --> D["逐叶 staging 写入、校验与逐值比较"]
    D --> E["当前叶共享事务：备份旧叶；安装新叶"]
    E --> F["事务内正式复读、校验与逐值比较"]
    F --> G["成功退出当前事务；处理下一叶"]
    G -->|还有叶| C
    G -->|全部成功| H["返回触达行数；不写独立水位"]
    E -. 失败 .-> R["只恢复当前叶；保留失败现场；抛错"]
    F -. 失败 .-> R
    R --> S["此前 raw 与成功日历叶保留；再次运行可无 HTTP 修复"]''', 'a03-b02-flow-calendar-transaction'),
    'e3f69e92': flow('Notebook 与脚本入口', '''    A{"交互内核且没有 __file__？"} -->|是| B["显式 notebook_args；standalone_mode=False"]
    B --> C["默认不写入；有待采日期仍请求 HTTP"]
    A -->|否| D{"直接运行脚本？"}
    D -->|是| E["读取终端参数并执行 main"]
    D -->|否| F["模块导入不执行入口"]''', 'a03-b02-flow-entry'),
}
updated_cells = []
for cell in notebook.cells:
    if cell.id in flows:
        updated_cells.append(flows[cell.id])
    updated_cells.append(cell)
notebook.cells = updated_cells
notebook.cells.append(flow('终端手动运行', '''    A["在终端激活 latitude；切换到项目根目录"] --> B["人工运行对应 .py --write"]
    B --> C["自动补 raw 或修复日历；逐日期和逐叶提交"]''', 'a03-b02-flow-manual'))
notebook.cells.append(nbformat.v4.new_code_cell('''# conda env list
# conda activate latitude
# cd E:\\Latitude_Analytics_v2
# python 02_Futures_Lakehouse\\a03_External_Market_Data\\b02_domestic_spot_basis.py --write''', id='a03-b02-manual'))
assert notebook.metadata == before.metadata
for cell in before.cells:
    assert {k:v for k,v in cell.items() if k != 'source'} == {k:v for k,v in cells[cell.id].items() if k != 'source'}
for cell in notebook.cells:
    if cell.cell_type == 'code':
        ast.parse(cell.source)
nbformat.validate(notebook)
PATH.write_text(nbformat.writes(notebook) + '\n', encoding='utf8', newline='\n')

boundary = ('a03/b02 将每个 raw 日期目录和每个日历完整叶分别放入独立共享事务，保持 raw 先提交、日历随后回写的顺序；'
            'raw staging 与正式路径仍复读字节及 SHA-256，日历仍执行原有 staging 和正式完整校验与逐值比较，正式验收均在事务内。'
            '安装或验收失败只恢复当前目标，此前成功 raw 与日历叶保留；日历失败后再次运行可从已提交 raw 无 API 修复。'
            '已安装的失败新目标隔离留存，恢复不完整另保留旧备份，staging 清理；已有日历根标记保持原样，没有独立日期水位。')
for relative in ('AGENTS.md', '02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT/relative
    text = path.read_text(encoding='utf8')
    old_members = 'a01/b01、b02、b03、b04、b05、b06、b07、b08、a02/b01、b01a、b02、b03 与 a03/b01'
    assert old_members in text
    text = text.replace(old_members, old_members+'、b02')
    if relative != 'AGENTS.md':
        anchor = '没有独立日期水位，不改变理论格点、状态继承及显式日期写入边界。'
        assert text.count(anchor) == 1
        text = text.replace(anchor, anchor+boundary)
    if relative.endswith('README.md'):
        old = '- 生意社国内现货基差原文：b02 每个 API 待办日期只请求一次 `day-{YYYY-MM-DD}.html`，把 HTTP'
        new = ('- 生意社国内现货基差原文：b02 每个 API 待办日期调用一次采集函数获取 `day-{YYYY-MM-DD}.html`；'
               'HTTP 适配器沿用 `Retry(total=4)`，因此日期数不等于实际请求尝试数。业务循环不重跑失败格点或事务。将 HTTP')
        assert old in text
        text = text.replace(old, new)
    path.write_text(text, encoding='utf8', newline='\n')
print(snapshot)
