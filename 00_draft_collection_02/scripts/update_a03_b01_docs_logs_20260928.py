"""a03/b01 第 1—4 项：说明、流程图与 CLI 日志；保留业务与执行入口。"""
import ast
import hashlib
import json
import pathlib
import shutil
import tempfile
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a03_External_Market_Data/b01_external_market_calendar.ipynb')
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a03-b01-docs-logs-before-'))
paths = [ROOT/'AGENTS.md',ROOT/'.env.template']
for directory in ('02_Futures_Lakehouse','03_Futures_Database','config'):
    paths.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and p.suffix in ('.py','.ipynb','.md'))
(snapshot/'hashes.json').write_text(json.dumps({p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},ensure_ascii=False,indent=2),encoding='utf8')
for relative in (RELATIVE,RELATIVE.with_suffix('.py')):
    target=snapshot/relative
    target.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(ROOT/relative,target)
path=ROOT/RELATIVE
notebook=json.loads(path.read_text(encoding='utf8'))
cells={c['id']:c for c in notebook['cells']}

def source(cell_id): return ''.join(cells[cell_id]['source'])
def put(cell_id,text): cells[cell_id]['source']=text.strip('\n').splitlines(keepends=True)
def markdown(cell_id,text): return {'cell_type':'markdown','id':cell_id,'metadata':{},'source':text.strip('\n').splitlines(keepends=True)}
def code_cell(cell_id,text): return {'cell_type':'code','id':cell_id,'metadata':{},'execution_count':None,'outputs':[],'source':text.strip('\n').splitlines(keepends=True)}
def flow(cell_id,title,body):
    return markdown(cell_id,'### 局部流程：'+title+'\n\n```mermaid\n%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'+textwrap.dedent(body).strip()+'\n```')

put('d571fa05', '''# b01 外部市场数据采集日历

从上游中国自然日历和共享请求实体配置生成 `dim_external_market_calendar`，为三个下游入口提供日期—请求实体格点。本入口只读取和计算本地数据，不调用生意社、JQData 或 Eastmoney。

| 上下游 | 与本环节的关系 |
| --- | --- |
| a01/b01 `dim_trade_calendar` | 唯一上游维度；提供当前有效自然日、周几和中国期货交易日标记。 |
| `config/futures_lakehouse/external_market_entities.py` | 请求实体、配置版本、指数有效期和下游指数映射的唯一来源。 |
| a03/b02 生意社原文归档 | 消费 `domestic_spot_basis/ALL`；成功原样归档后回写 `success + passed`、产物数 1，不生产结构化基差事实。 |
| a03/b03 境外期货 | 消费 `overseas_futures/ALL`，按日请求整表；负责事实计数、确认空和质量状态回写。 |
| a03/b04 外部指数 | 消费 `external_index/INDICATOR_ID`；负责指数事实和状态回写，以及退出 required 范围的旧事实清退。 |
| operations | 人工批次中先运行本日历，再运行 a03/b02—b04；识别计划和运行进度日志。 |

本环节不读取下游 raw 或事实来重新证明完成状态。字段、主键和分区来自 `config/data_contracts.py`；运行边界见湖仓 `README.md`、`AGENTS.md` 与数据库 `AGENTS.md`。
''')
put('ecd5e46c', '''## 自动更新、状态继承与写入边界

自动模式从配置起点到上游当前有效自然日展开完整理论格点，再比较现有日历；新增、历史缺口、规则变化和上游撤销都进入差异计划。周末、非中国交易日或指数有效期外的格点仍保留，以 `is_fetch_required=false` 表达无需请求。

| 运行方式 | 当前行为 |
| --- | --- |
| 默认不带参数 | 完整生成与比较，只输出计划；不调用 API，不提交。 |
| `--write` | 提交变化的 `dataset_name/year/month` 完整叶；全部验收通过才报告本次提交完成。 |
| 成对日期 | 只重建范围内格点，范围外旧行带入完整期望；只读可用，写入必须选择非正式测试湖。 |
| 物理兼容而 metadata 过期 | 以完整期望表 staging 并整根替换，避免新旧 metadata 混用。 |
| 没有变化 | 直接报告无需更新；日历状态和文件保持原样。 |

只有 `is_fetch_required` 与带配置版本的 `requirement_reason` 均不变时，才逐字段继承下游状态及原 `updated_at`；选择语义变化时使用新规则初始化状态。日历行存在、当前应采集和下游已完成是三个不同概念。

`--start-date`、`--end-date` 必须成对；没有 `--full`。正式路径来自 `settings.futures_lake_root`，不得用显式日期写正式湖。目标为空时由同一入口建表，上游为空时完整期望也可为空。本入口没有独立日期水位文件。
''')
put('54c00aed','''## 初始化与共享依赖

按项目规定的三个标记定位根目录，导入上游与输出权威 Schema、转换函数、共享实体配置和项目设置。此单元格只加载依赖，不读取业务表、不请求网络、不写入数据。
''')
put('06d8cf40','''## Schema 契约与有界本地样例

仅在交互内核且没有 `__file__` 时展示上游自然日历与当前外部市场日历。字段解释来自权威 Schema；传入 `lake_root` 后，用户显式选择样例时会进行有界本地读取。

本格不调用来源 API、不创建数据或回写状态；普通脚本执行跳过展示。
''')
put('02175666','''## 表身份、分区与请求实体

表名、主键和分区从权威 Schema metadata 各读取一次；输出主键是数据集—请求实体—观测日期，叶分区为 `dataset_name/year/month`。同一个指数月叶包含全部配置指数，提交必须保留叶内其他实体。

`POLICY_COLUMNS` 决定是否继承旧状态，`STATE_COLUMNS` 列出必须共同继承的完成、质量和审计字段。生意社与境外期货请求实体为 `ALL`，指数为来源 `INDICATOR_ID`；实体和有效期只从共享配置读取。
''')
put('2d5c68c2','''## Dataset 物理兼容与精确 metadata 读取

`open_compatible_dataset()` 枚举 Parquet，检查 Dataset 汇总结构与每个 fragment 的字段、类型和 nullable，再判定 metadata 是否精确匹配。已有输出表允许物理兼容但 metadata 过期，以便进入原有整表迁移。

`open_exact_dataset()` 在此基础上要求当前 metadata；目前用于上游、staging 和正式验收。Hive 字段按权威顺序重建，`partition_expression()` 将有序分区键转为 Arrow 过滤条件。本轮保留现有读取与检查次数，后续第 7—9 项再收缩已确认的重复。
''')
put('3f56c68e','''## 生成完整理论格点并继承状态

`build_expected_calendar()` 为每个自然日生成生意社 `ALL`、境外期货 `ALL` 和全部配置指数格点。生意社按中国期货交易日请求，境外期货按普通工作日请求，指数同时满足普通工作日与配置有效期；无需请求日仍保留为 `not_required`。

现有行按主键索引。两项选择语义都未变化时继承完整 `STATE_COLUMNS`，包括成功、确认空、warning、错误与原更新时间；规则变化则初始化为当前的 pending 或 not_required。返回前完整验证生成结果；生成日历不表示下游已经采集完成。
''')
put('6a91b06b','''## 完整内容摘要

`table_digest()` 按主键排序并固定权威列、类型与 metadata，再从 Python 标量重建 Arrow 缓冲区，消除 fragment 切片布局差异，最后对 IPC 内容计算 SHA-256。

摘要包含状态和 `updated_at`，不只是行数或主键。状态继承保证未变化行的更新时间不变；本轮保留缓冲区规整步骤。
''')
put('068d0136','''## staging、整批安装与现有失败恢复

`commit_partitions()` 先验收完整期望表并计算摘要，再暂存变化叶；metadata 迁移时暂存完整期望表。staging 根级零行标记使全部待删除或空期望也能复读。当前会先完整复读 staging，再逐叶核对行数和内容摘要。

普通提交依次备份、替换或删除全部变化叶，之后一次正式整表复读并与完整期望比较。metadata 迁移或完整期望为空时执行整根 swap。任一安装或正式验收失败，现有恢复分支尝试恢复本次已移动的全部旧目标；并非逐叶独立成功。

目前仍使用手写恢复：完整恢复后会清理 staging、备份及隔离目录；恢复自身失败则保留现场并抛错。共享安装与恢复模块留待第 10 项接入。本轮不改变提交范围、验收内容或恢复策略。
''')
put('7150a5fe','''## CLI：完整比较与运行日志

`main()` 检查日期和正式写入边界，读取上游及现有日历，生成范围内期望并保留范围外旧行，然后比较完整叶差异。旧生意社状态仅允许在历史读取分支宽容接纳，新期望必须满足当前 raw 语义；metadata 过期触发整表迁移计划。

`complete_calendar_grids` 表示现有行与期望行完整相同的格点数，不是 `is_fetch_completed=true` 的数量。只读只报告计划；提交完成日志必须在 `commit_partitions()` 成功返回后发出，并区分日历落盘与下游采集完成。

运行边界统一使用 `=` 分隔线和 `function/phase/status/elapsed_s`。保留 `reconciliation_plan:` 前缀供 operations 识别，新增统一的完成、无需更新和失败日志；异常按原类型与异常链继续抛出。函数内部的细粒度进度及日志归位留待第 5—6 项处理。
''')
put('a3acee6c','''## 当前 Notebook 与脚本入口

Notebook 当前使用 `args=[]`、`standalone_mode=False`，避免读取内核参数；默认只读生成和比较，不调用外部 API。直接运行脚本时由 Click 读取终端参数。

现有入口只检查 `ipykernel` 是否加载，因此在 Notebook 内导入同名 Python 模块仍可能触发只读计划。`notebook_args`、模块导入保护及末尾终端命令格留待第 11 项对齐，本轮保留入口代码。
''')

# Split only at existing top-level function boundaries; retain old cell metadata and outputs.
reading=source('49bfe04b')
upstream_start=reading.index('def validate_upstream_table(')
output_start=reading.index('def validate_external_calendar_table(')
put('49bfe04b',reading[:upstream_start])
upstream_code=code_cell('a03-b01-upstream-validation',reading[upstream_start:output_start])
output_code=code_cell('a03-b01-output-validation',reading[output_start:])
digest=source('a177c572')
change_start=digest.index('def changed_partition_keys(')
put('a177c572',digest[:change_start])
change_code=code_cell('a03-b01-difference-plan',digest[change_start:])

put('44837881',source('44837881').replace('import sys\n','import sys\nimport time\n'))
main_source=source('ebf489c1')
main_node=next(node for node in ast.parse(main_source).body if isinstance(node,ast.FunctionDef))
lines=main_source.splitlines(keepends=True)
body=''.join(lines[main_node.body[0].lineno-1:])
for marker,phase in (
    ('    upstream_dataset = open_exact_dataset(', 'read_upstream'),
    ('    metadata_upgrade_required = False', 'read_existing'),
    ('    expected_scope_df = build_expected_calendar(', 'generate_expected'),
    ('    partition_keys = changed_partition_keys(', 'compare_partitions'),
    ('    existing_rows_by_key = {', 'plan_counts'),
    ('        committed_rows = commit_partitions(', 'commit'),
):
    assert body.count(marker)==1,marker
    indent=marker[:len(marker)-len(marker.lstrip())]
    body=body.replace(marker,indent+f'log_phase = "{phase}"\n'+marker)
body=body.replace('f"table={TABLE_NAME}; mode={run_mode}; "','f"planning_progress: table={TABLE_NAME}; function=main; phase=planning; status=completed; mode={run_mode}; "')
body=body.replace('f"lake_root={resolved_lake_root}; write={str(write).lower()}"','f"lake_root={resolved_lake_root}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
body=body.replace('"reconciliation_plan: "','f"reconciliation_plan: table={TABLE_NAME}; function=main; phase=plan; status=completed; "')
body=body.replace('f"changed_partitions={len(partition_keys)}"','f"changed_partitions={len(partition_keys)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
body=body.replace('        click.echo("外部市场日历已经与上游自然日历和当前请求实体配置一致。")','''        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; outcome=up_to_date; "
            f"pending_partitions=0; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )''')
body=body.replace('f"committed_rows={committed_rows}; "','f"partition_committed: table={TABLE_NAME}; function=main; phase=commit; status=completed; scope=batch; persisted=true; committed_rows={committed_rows}; "')
body=body.replace('f"dry_run_rows={len(expected_scope_df)}; "','f"planning_progress: table={TABLE_NAME}; function=main; phase=plan; status=completed; outcome=read_only; persisted=false; dry_run_rows={len(expected_scope_df)}; "')
body=body.replace('f"full_root_swap={str(metadata_upgrade_required).lower()}"','f"full_root_swap={str(metadata_upgrade_required or expected_full_df.empty).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
body+='''
    click.echo(
        f"finished: table={TABLE_NAME}; function=main; phase=run; status=completed; outcome={'committed' if write else 'read_only'}; "
        f"write={str(write).lower()}; date_watermark=none; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
    )
'''
prefix='''    log_started_at = time.perf_counter()
    log_phase = "arguments"
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\\n外部市场采集日历 / External market calendar\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; api_calls=0; elapsed_s=0.000"
    )
    try:
'''
suffix='''    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )
        raise
'''
put('ebf489c1',''.join(lines[:main_node.body[0].lineno-1])+prefix+textwrap.indent(body,'    ')+suffix)

diagrams={
'44837881':flow('a03-b01-flow-init','初始化','''flowchart TD
    A["当前目录及父目录"] --> B{"三个项目标记齐全？"}
    B -->|是| C["加入项目与湖仓模块路径；加载契约和配置"]
    B -->|否| D["抛错"]
    C --> E["仅加载依赖；不读取业务表或写入"]'''),
'30256ca1':flow('a03-b01-flow-schema','Schema 与本地样例','''flowchart TD
    A{"交互内核且无 __file__？"} -->|否| B["跳过展示"]
    A -->|是| C["展示上游与输出权威 Schema"]
    C --> D["用户显式选择时读取有界本地样例"]
    D --> E["不请求来源 API；不回写状态"]'''),
'a15bd7bc':flow('a03-b01-flow-constants','契约和实体常量','''flowchart TD
    A["两张权威 Schema"] --> B["读取表名、主键和分区；建立 Hive partitioning"]
    B --> C["共享配置提供 ALL 和指数请求实体"]
    C --> D["区分选择字段与完整状态继承字段"]'''),
'49bfe04b':flow('a03-b01-flow-read','兼容与精确契约读取','''flowchart TD
    A["枚举 Parquet；打开 Dataset"] --> B["核对汇总与每个 fragment 物理结构"]
    B --> C{"物理兼容？"}
    C -->|否| D["拒绝读取"]
    C -->|是| E["比较当前 metadata"]
    E --> F{"调用方要求精确？"}
    F -->|否| G["返回 Dataset 和 metadata 是否精确"]
    F -->|是| H{"metadata 精确？"}
    H -->|是| I["返回 Dataset"]
    H -->|否| D'''),
'a03-b01-upstream-validation':flow('a03-b01-flow-upstream','现有上游校验','''flowchart TD
    A["上游指定范围的 Arrow 表"] --> B["权威转换；检查主键"]
    B --> C["当前实现复核年月、周几和周末标记"]
    C --> D["检查自然日连续性；按主键排序返回"]'''),
'a03-b01-output-validation':flow('a03-b01-flow-output','当前日历业务校验','''flowchart TD
    A["待验证 Arrow 表"] --> B["固定契约；检查主键、枚举、年月和原因"]
    B --> C["核对请求实体与 required 状态自洽"]
    C --> D["核对完成、计数、缺失和审计时间"]
    D --> E{"允许读取旧生意社状态？"}
    E -->|是| F["仅放宽旧生意社完成规则；返回历史行"]
    E -->|否| G["生意社完成必须 success、passed、产物数 1"]
    G --> H["按主键排序返回"]'''),
'74b6ae55':flow('a03-b01-flow-generate','理论格点与状态继承','''flowchart TD
    A["按主键索引现有行"] --> B["逐自然日读取交易日与周几"]
    B --> C["生成生意社、境外期货及全部指数请求实体"]
    C --> D["依据交易日、工作日和有效期设置 required 与原因"]
    D --> E{"现有行两项选择语义不变？"}
    E -->|是| F["逐字段继承下游状态及原 updated_at"]
    E -->|否| G["初始化 pending 或 not_required"]
    F --> H["收集完整理论格点；完整业务验收"]
    G --> H
    H --> I["返回内存期望日历；尚未提交"]'''),
'a177c572':flow('a03-b01-flow-digest','完整内容摘要','''flowchart TD
    A["按主键排序完整权威行"] --> B["转换 Arrow；按标量重建统一缓冲区"]
    B --> C["固定 Schema 的 IPC 内容"]
    C --> D["SHA-256；包含业务状态与更新时间"]'''),
'a03-b01-difference-plan':flow('a03-b01-flow-difference','完整叶差异计划','''flowchart TD
    A["期望与现有分区键并集"] --> B["逐键选取两侧完整分区"]
    B --> C["非空分区计算完整内容摘要；空侧记为 None"]
    C --> D{"摘要相同？"}
    D -->|是| E["不加入变更集合"]
    D -->|否| F["加入变更集合；包括旧叶撤销"]
    E --> G["返回排序后的变化分区"]
    F --> G'''),
'018ccc71':flow('a03-b01-flow-commit','staging、安装与当前恢复','''flowchart TD
    A["完整期望验收与摘要；准备本批路径"] --> B["写零行标记及变化叶；迁移时写完整表"]
    B --> C["staging 全量及逐叶复读验收"]
    C --> D{"metadata 迁移或完整期望为空？"}
    D -->|是| E["备份旧根；安装 staging 根"]
    D -->|否| F["依次备份并替换或删除本批全部变化叶"]
    E --> G["正式整表复读；行数和完整内容摘要一致"]
    F --> G
    G --> H["清理临时路径；返回本批提交行数"]
    E -. 失败 .-> R["现有恢复：整根恢复或倒序恢复已记录叶"]
    F -. 失败 .-> R
    G -. 失败 .-> R
    R --> S{"恢复完整？"}
    S -->|是| T["清理 staging、备份、隔离；重抛异常"]
    S -->|否| U["保留现场；抛出恢复异常"]
    B -. 失败 .-> V["清理 staging；正式数据未改动"]
    C -. 失败 .-> V'''),
'ebf489c1':flow('a03-b01-flow-cli','运行计划与日志','''flowchart TD
    A["运行开始；参数和正式写入边界"] --> B["读取上游与现有日历"]
    B --> C["生成范围内期望；保留范围外旧行"]
    C --> D["差异计划；metadata 迁移时覆盖全部叶"]
    D --> E["报告计划：一致、新增修订、撤销、变化叶"]
    E --> F{"有变化或迁移？"}
    F -->|否| G["报告 up_to_date；结束"]
    F -->|是| H{"启用 --write？"}
    H -->|否| I["只读计划；运行完成"]
    H -->|是| J["调用提交；成功返回才报告整批落盘"]
    J --> K["运行完成；date_watermark=none"]
    B -. 失败 .-> R["报告失败阶段；原异常继续抛出"]
    C -. 失败 .-> R
    D -. 失败 .-> R
    J -. 失败 .-> R'''),
'5dc7bd54':flow('a03-b01-flow-entry','当前执行入口','''flowchart TD
    A{"ipykernel 已加载？"} -->|是| B["main.main；args=[]；standalone_mode=False"]
    B --> C["默认只读日历计划；不调用 API"]
    A -->|否| D{"直接运行脚本？"}
    D -->|是| E["main 读取终端参数"]
    D -->|否| F["普通 Python 模块导入不启动"]'''),
}
overview=flow('a03-b01-flow-overview','外部市场日历总流程','''flowchart TD
    A["参数、正式写入边界；全程不调用 API"] --> B["读取上游有效自然日与已有外部日历"]
    B --> C["自然日乘请求实体；生成完整理论格点"]
    C --> D["选择规则不变时继承下游状态"]
    D --> E["显式范围保留范围外旧行；比较完整叶"]
    E --> F{"无变化且无需 metadata 迁移？"}
    F -->|是| G["无需更新"]
    F -->|否| H{"启用 --write？"}
    H -->|否| I["只读计划结束"]
    H -->|是| J["staging；普通变化叶或完整迁移表"]
    J --> K["整批变化叶安装；迁移或空期望整根替换"]
    K --> L["正式整表验收；成功后报告日历落盘"]
    K -. 失败 .-> M["尝试恢复本次旧目标；抛错停止"]
    L -. 失败 .-> M
    L --> N["下游 b02—b04 再消费 required 格点并回写状态"]''')
overview['source']=''.join(overview['source']).replace('### 局部流程：','## 总流程：',1).splitlines(keepends=True)
insert_after={
    'ecd5e46c':[overview],
    '49bfe04b':[
        markdown('a03-b01-upstream-heading','''## 当前上游校验边界

上游 `dim_trade_calendar` 的主键、连续自然日及派生字段已由 a01/b01 保证。当前 `validate_upstream_table()` 仍重复检查这些关系，并按主键排序后返回；这是后续第 7 项要收缩的已有实现，本轮保持代码原样，不将重复检查提升为新的消费契约。'''),
        upstream_code,
        markdown('a03-b01-output-heading','''## 当前日历的完整业务验收

`validate_external_calendar_table()` 校验本表主键、请求实体、选择状态、完成状态、计数和审计字段。生意社当前完成状态必须为 `success + passed`、数量 1 且不缺失，禁止确认空；其他来源保留各自确认空和 warning 语义。

`allow_legacy_domestic_state=True` 只用于 main 读取已有旧生意社状态，新期望、staging 和正式输出仍严格验收。本轮保留现有多次调用，后续按调用关系判断冗余。'''),
        output_code,
    ],
    'a177c572':[
        markdown('a03-b01-difference-heading','''## 完整叶差异计划

`changed_partition_keys()` 遍历期望与现有分区键的并集，逐键比较两侧完整内容摘要。只有旧内容而无期望内容的叶仍进入结果，供提交阶段显式删除；范围外旧行已经由 main 合并回完整期望。

此处当前仍按每个分区反复筛选整份内存表；正式整表复读则发生在整批安装后一次，不能混称为每分区扫描正式全表。'''),
        change_code,
    ],
}
result=[]
for cell in notebook['cells']:
    for item in [cell,*insert_after.get(cell['id'],[])]:
        if item['cell_type']=='code': result.append(diagrams[item['id']])
        result.append(item)
notebook['cells']=result
for cell in notebook['cells']:
    if cell['cell_type']=='code': compile(''.join(cell['source']),cell['id'],'exec')
path.write_text(json.dumps(notebook,ensure_ascii=False,indent=1)+'\n',encoding='utf8',newline='\n')
print(snapshot)
