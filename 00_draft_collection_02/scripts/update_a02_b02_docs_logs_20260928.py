"""a02/b02 第 1—4 项；仅修改 Notebook 说明、逻辑块分格与日志。"""
import ast
import copy
import json
import pathlib
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b02_futures_holding_reports.ipynb'
notebook = json.loads(PATH.read_text(encoding='utf8'))
before = copy.deepcopy(notebook)
cells = {c['id']: c for c in notebook['cells']}


def set_source(cell, source):
    cell['source'] = source.splitlines(keepends=True)


def replace(cell_id, old, new):
    source = ''.join(cells[cell_id]['source'])
    assert source.count(old) == 1, (cell_id, old)
    set_source(cells[cell_id], source.replace(old, new))


def md(cell_id, source):
    return {'cell_type': 'markdown', 'id': cell_id, 'metadata': {}, 'source': source.splitlines(keepends=True)}


def flow(cell_id, title, source):
    return md(cell_id, title + '\n\n```mermaid\n'
              '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
              + textwrap.dedent(source).strip() + '\n```')


descriptions = {
'29d7c713': '''# b02 JQData 期货会员成交持仓排名

从报告日历选择待办交易所—品种—交易日，串行合并同一品种月份的请求日期，读取 JQData `finance.FUT_MEMBER_POSITION_RANK`。一份长表响应同时生成逐会员排名与明确参与者类型两张事实表，再回写各自报告日历状态。

| 上下游 | 本环节的关系 |
| --- | --- |
| a02/b01 报告日历 | 提供 `position_rank`、`member_position` 成对的 required 格点；本环节回写完成状态、条数和质量证据。 |
| a02/b01a 与特殊案例配置 | 仅当完整坏载荷指纹命中冻结案例时，读取其正式 raw 三文件证据并应用指定校准。 |
| JQData 共享连接 | 只在存在待办时认证；认证和 Windows TUN 出口交给 `config/jqdata_connection.py`。 |
| `fact_futures_position_rank_daily` | 逐来源合约—会员宽行，保存三类榜单的名次、指标及变化；未上榜指标允许为空。 |
| `fact_futures_member_position_daily` | 只接纳明确参与者类型汇总标签；不能从普通会员名称猜测类型。 |
| a02/b03 仓单 | 与本环节共享报告日历，但消费 `warehouse_receipt`；本环节保留其行和状态。 |

三张 silver 表的字段、主键与分区来自 `config/data_contracts.py`；特殊案例唯一来源为 `config/futures_lakehouse/futures_position_rank_special_cases.py`。本环节没有独立日期水位文件。业务文本权威见湖仓根目录 `README.md`、`AGENTS.md` 和 `03_Futures_Database/AGENTS.md`。
''',
'd1b4d507': '''## 自动更新与写入边界

待办定义为：`当前 required 成对报告格点 − 两张事实计数与两类日历状态共同证明完整的格点`。仅有事实行或仅有日历完成标记均不够；已完成的 `warning` 可以构成完成证据，不因质量不是 `passed` 而重拉。

| 运行方式 | 行为 |
| --- | --- |
| 默认不带参数 | 计算全部自动待办；有待办才认证、请求和验收，不写数据湖。 |
| `--write` | 每个品种月份先提交两张事实，再回写两类报告日历完整叶。 |
| 成对日期 | 仅限制检查范围；只读可用，带写入时必须显式选择不同于正式湖的临时湖。 |
| 没有待办 | 报告已完整并退出，不认证 JQData。 |

`--start-date` 与 `--end-date` 必须同时提供；没有 `--full`。`--lake-root` 默认来自 `settings.futures_lake_root`。空事实表由同一差集入口自然得到待办。

当前按叶依次提交，不是整个月份共同回滚：第二张事实或后一个日历叶失败时，此前成功叶保留；因共同完成证据仍不齐，该格点会留在下次人工启动的待办中。来源请求不自动重试。
''',
'b0fc1a13': '''## 初始化与共享依赖

按 `.env.template` 的标记文件约定定位项目根，导入三张权威 Schema、转换函数、共享 JQData 连接、项目设置和冻结特殊案例配置。本格只加载定义，不认证、不请求或写入。
''',
'207ba035': '''## 三张表的 Schema 与有界样例

仅在交互内核且没有 `__file__` 时调用共享 Schema 浏览器。依次展示报告日历、排名事实和参与者类型事实；字段说明来自权威 Schema。传入 `lake_root` 后可以显式浏览有界本地样例，因此不能把本格描述成完全不读取数据湖。

展示不认证 JQData、不调用来源 API、不写入文件；普通脚本执行跳过展示。
''',
'27c07013': '''## 表身份、分区与来源约定

表名、主键及分区列在初始化时从三张 Schema metadata 各读取一次；Hive partitioning 同样在此建立。事实叶为交易所—品种—年月，日历叶为报告类型—交易所—年月，因此日历叶中还包含其他品种，提交时必须保留。

`JQDATA_FIELDS` 固定来源列；交易所别名只用于来源匹配，参与者标签只接受明确映射。`RUN_QUERY_ROW_LIMIT=5000` 是响应截顶边界，`MAX_SOURCE_RANK=20` 是每个具体合约、每类榜单的名次边界，不能相互替代。
''',
'8d0a5cce': '''## 数据集物理契约与表身份

`open_exact_dataset()` 发现表根 Parquet 文件、打开 Hive Dataset，并重建分区字段后核对物理类型、nullable 及表名/主键/分区身份。描述性 metadata 允许漂移，以当前配置说明为准。

该入口用于启动时的完成计划；月份循环中的完整叶读取使用后面的 `read_complete_partition()`，不在每个月重新发现整张表。
''',
'70556751': '''## 已冻结特殊案例的显式校准

只处理配置中的指定交易所、品种、交易日、来源合约和榜单类别。目标榜必须恰好有 20 行，再核对完整名次、会员、指标与变化。

| 目标榜载荷 | 处理 |
| --- | --- |
| 已等于冻结官方值 | 原样通过，不要求 raw 证据，也不新加校准 warning。 |
| 精确等于冻结坏指纹 | 验收 b01a 正式 raw 的三文件集合、完整原文摘要、sidecar 和清单，然后整榜替换为冻结官方值。 |
| 任何第三种载荷 | 硬失败，不推断、不局部修补。 |

校准不修改其他榜单或未配置格点；返回包含案例 ID 和官方原文摘要的 warning，随后永久写入报告日历。这里不请求上期所来源、不创建 raw 证据。
''',
'5d0661f6': '''## 来源长表归一化与两张事实生成

`normalize_rank_response()` 核对来源类型、列、行数、日期、品种及交易所。按 `rank_type` 文字识别成交量、持买仓和持卖仓，`rank_type_ID` 只用于检查类别映射一致性；来源名次必须是 1—20 的整数。

每行按来源合约及会员身份归入逐会员事实或明确参与者类型事实，保留来源合约代码；标准合约只在来源代码具有月份数字时生成。相应指标与变化透视为宽行，缺少的榜单指标保持空值。

同一事实键的重复来源行，只有原始标签、类别 ID/文字、指标及变化全部一致才合并；逐会员名次取最小值，并返回重复来源 warning。任何业务值冲突都失败。最终两张输出各自通过业务 validator；空响应返回权威空表。
''',
'a1b983e9': '''## 完成证明与自动待办

启动时只投影两张事实的格点列计数，再与两类 required 日历行比较。`pending_report_grids()` 要求每个交易所—品种—日同时具备成对日历行。

非空事实需要 `success`、实际计数匹配、无缺失标记、完整审计字段以及 `passed` 或 `warning`；零行需要 `empty_confirmed + warning` 及匹配计数和缺失标记。只有两类都完整才扣除；其他格点进入按交易所—品种—年月排序的待办。

这一启动扫描是当前 b02 的完成判定契约。本轮不将其改为仅信任 `is_fetch_completed`，也不删除事实计数。
''',
'5d672c31': '''## 当前单叶 staging、安装与失败恢复

`commit_complete_partition()` 完整验证待提交叶并检查分区范围，转换为 Arrow，写入 staging 的零行根标记和非空叶。staging 完整复读并逐值一致后，再安装缺失的正式根标记、备份旧叶并安装新叶；空结果通过移走旧叶表达，保留可读零行根标记。

正式目标叶完整复读且逐值一致后才返回。当前手写异常分支尝试隔离目标、恢复旧叶、移除本次新标记；恢复成功会清理 staging、备份及隔离目录，恢复异常则保留现场并报错。

当前恢复实现按目录存在情况判断目标，尚未按实际移动记录接入共享事务；首次备份失败的边界已列入第 10 项。本轮只呈现当前机制，不提前改变回滚范围。两张事实和各日历叶仍分别提交，此前成功叶不会随后续叶共同撤销。
''',
'6f0ee3df': '''## JQData 月份待办查询与确定性二分

只将当前交易所—品种—月份的待办日期放入 `day.in_(...)`，串行调用 `finance.run_query()`。每次响应核对类型、来源列、日期范围、品种和交易所；空响应也必须具有来源列。

少于 5000 行时按请求日期拆回响应，未返回的日期对应空表。达到上限则丢弃本次截顶结果，把有序日期从中点分成两组继续查询；单日仍触顶则失败。该分段用于取得完整响应，不是对业务异常的自动重试。网络或权限异常直接抛出，不继续请求后续日期组。
''',
'badd061e': '''## CLI：计划、月份调度与完成汇总

`main()` 检查日期和正式湖写入边界，读取窄列完成证据、形成待办；无待办直接退出。存在待办才认证，随后依次处理品种月份：查询、逐日特殊案例校准与归一化、汇总两张事实，只读时到此结束当前月份。

写入时先读两张旧事实叶与两类日历完整叶，确认当前待办格点仍 required，再合并和分别提交两张事实。只有两张事实正式复读成功，才按复读计数生成日历完成状态并逐叶提交；全部成功后输出当前月份的 `partition_committed:`。

日志使用 88 个 `=` 的运行边界与 `table/function/phase/status` 字段；保留原监控前缀。只读月份使用 `persisted=false`，正式完成使用 `persisted=true`。`elapsed_s` 表达所属阶段或运行的累计耗时，原计数字段继续保留。当前 `query_batch_success` 的计时覆盖请求及逐日校准/归一化，不能解释成纯网络耗时。

本轮保持现有日志归属；读取、转换与提交函数的独立进度及日志归位在后续第 5—6 项处理。异常报告失败阶段后继续抛出，不输出成功收尾；没有独立日期水位。
''',
'3adba1ea': '''## 当前 Notebook 与脚本运行入口

Notebook 当前显式传入 `args=[]`、`standalone_mode=False`，避免读取内核参数。默认不带 `--write`；有待办仍会认证和请求，不能将只读理解为不联网。

当前判断仅检查 `ipykernel` 是否加载，所以在 Notebook 中导入同名模块也可能触发入口。直接运行 `.py` 时读取 CLI 参数；显式 `notebook_args`、导入保护及终端命令单元格留待第 11 项对齐，本轮不改入口。
''',
}
for cell_id, source in descriptions.items():
    set_source(cells[cell_id], source.rstrip())

flows = {
'12bacfb9': ('init', '初始化', '''
flowchart TD
    A["当前目录与父目录"] --> B{"找到三个项目标记？"}
    B -->|是| C["加入项目与湖仓模块路径；导入依赖和配置"]
    B -->|否| D["抛出根目录定位错误"]
    C --> E["仅加载定义；不请求、不写入"]
'''),
'72594ab3': ('schema', 'Schema 与样例展示', '''
flowchart TD
    A{"交互内核且没有文件路径变量？"} -->|是| B["共享浏览器展示三张权威 Schema"]
    B --> C["可显式查看有界本地样例；不调用 API 或写入"]
    A -->|否| D["跳过展示"]
'''),
'bf75ec37': ('constants', '契约与来源常量', '''
flowchart TD
    A["三张权威 Schema metadata"] --> B["读取表名、主键、分区列；建立 Hive partitioning"]
    B --> C["固定来源字段、交易所别名与参与者标签"]
    C --> D["区分响应 5000 行上限与榜单名次 20 上限"]
'''),
'6c44d64e': ('contract', '数据集打开与兼容性', '''
flowchart TD
    A["启动时发现表根 Parquet"] --> B{"存在可读文件？"}
    B -->|否| X["抛出缺失异常"]
    B -->|是| C["打开 Hive Dataset；补回分区字段"]
    C --> D{"物理字段与表身份兼容？"}
    D -->|否| Y["抛出契约异常"]
    D -->|是| E["返回 Dataset；允许描述性 metadata 漂移"]
'''),
'a02-b02-validators': ('validators', '三张表业务验收', '''
flowchart TD
    A["输入 DataFrame"] --> B["按权威 Schema 转换；检查主键唯一"]
    B --> C{"所属表？"}
    C -->|日历| D["状态枚举、完成证据、计数、时间与分区"]
    C -->|排名事实| E["来源、会员、名次与数量配对、合约及分区"]
    C -->|类型事实| F["明确参与者类型、来源、数量及分区"]
    D --> G["按主键排序返回；任一检查失败即抛错"]
    E --> G
    F --> G
'''),
'a02-b02-leaf-read': ('read', '窄列计数与完整叶读取', '''
flowchart TD
    A{"读取目的？"} -->|启动计划| B["可选事实表：缺失返回空；存在则核对契约"]
    B --> C["分批扫描格点窄列；累计每个格点行数"]
    A -->|当前叶| D["检查根级零行标记；仅枚举目标叶文件"]
    D --> E["核对每个物理文件；读取并补回分区列"]
    E --> F["完整叶业务验收；返回 DataFrame"]
'''),
'22904b8b': ('special', '冻结特殊案例', '''
flowchart TD
    A{"命中配置格点？"} -->|否| B["保持来源响应"]
    A -->|是| C["定位目标榜；核对完整 20 行与类别"]
    C --> D{"完整载荷等于哪一份冻结值？"}
    D -->|官方值| B
    D -->|坏指纹| E["验收 b01a 三文件正式 raw 证据"]
    D -->|均不匹配| X["抛错；不猜测修复"]
    E --> F["整榜替换为官方值；附案例与摘要 warning"]
    F --> G["返回响应与 warning；不修改其他榜单"]
    B --> G
'''),
'91b575c7': ('normalize', '来源归一化', '''
flowchart TD
    A["核对来源结构、请求范围及行数"] --> B["逐行识别榜单类别；核对名次、指标与身份"]
    B --> C{"明确参与者类型标签？"}
    C -->|否| D["合约与会员排名宽行"]
    C -->|是| E["合约与参与者类型宽行"]
    D --> F["重复键业务值一致才合并；冲突抛错"]
    E --> F
    F --> G["排名取最小值；记录来源重复 warning"]
    G --> H["两张事实分别验收；返回事实及 warning"]
'''),
'6a26bc2a': ('plan', '完成证明与待办', '''
flowchart TD
    A["当前 required 日历；两张事实格点计数"] --> B["应用可选日期；要求两类日历成对"]
    B --> C{"两类计数、状态和审计证据共同完整？"}
    C -->|是| D["计入完整格点；不请求"]
    C -->|否| E["加入待办；补年月并按分区、日期排序"]
    D --> F["返回待办与完整数量"]
    E --> F
'''),
'a02-b02-fact-merge': ('merge', '完整事实叶合并', '''
flowchart TD
    A["当前旧事实叶与本批新事实"] --> B["定位叶分区；移除本次触达日期的旧行"]
    B --> C["保留其他日期；拼接本批行"]
    C --> D["空结果使用权威空表；完整叶业务验收"]
    D --> E["返回待提交完整叶；尚未落盘"]
'''),
'aa14209b': ('commit', '当前单叶安装与恢复', '''
flowchart TD
    A["验收完整叶与分区范围；转换 Arrow"] --> B["写 staging 标记和非空叶；完整复读比对"]
    B --> C["安装缺失的根标记；备份旧叶"]
    C --> D["非空安装新叶；空结果保持目标叶缺失"]
    D --> E["完整复读正式叶；逐值比对"]
    E --> F["清理临时路径；返回正式叶"]
    C -. 失败 .-> R["当前手写恢复：隔离目标、恢复备份、移除新标记"]
    D -. 失败 .-> R
    E -. 失败 .-> R
    R --> S{"恢复分支是否报错？"}
    S -->|否| T["清理 staging、备份、隔离；重抛提交异常"]
    S -->|是| U["保留现场；抛出恢复异常"]
    B -. 失败 .-> V["清理 staging；抛错"]
'''),
'a02-b02-completion': ('completion', '内存日历状态生成', '''
flowchart TD
    A["两张事实正式复读计数；当前日历叶"] --> B["只更新本次格点及两类报告"]
    B --> C["保留已有重复来源与特殊校准 warning"]
    C --> D{"对应事实有行？"}
    D -->|是| E["success；有证据则 warning，否则 passed"]
    D -->|否| F["empty_confirmed；缺失标记与 warning"]
    E --> G["写入计数、批次、时间；验收内存日历"]
    F --> G
    G --> H["返回待提交日历；此时尚未落盘"]
'''),
'a02-b02-calendar-commit': ('calendar-commit', '日历逐叶提交', '''
flowchart TD
    A["按本次格点定位两类日历叶"] --> B["逐叶取完整内容；保留其他品种及日期"]
    B --> C["业务验收；调用单叶提交及正式复读"]
    C --> D{"还有叶？"}
    D -->|是| B
    D -->|否| E["返回触达日历行数；当前月份可报告完成"]
    C -. 失败 .-> F["停止；此前成功事实和日历叶保留"]
'''),
'0a7103a8': ('query', '月份查询及日期二分', '''
flowchart TD
    A["日期排序；拒绝空日期或重复日期"] --> B["取一个日期组；单次 run_query"]
    B --> C["验收来源列、类型与请求范围"]
    C --> D{"响应达到 5000 行？"}
    D -->|否| E["按日期拆分；无返回的日期为空表"]
    D -->|是| F{"日期组只有一天？"}
    F -->|是| X["硬失败；不能证明完整性"]
    F -->|否| G["丢弃截顶响应；有序日期二分入队"]
    E --> H{"还有日期组？"}
    G --> H
    H -->|是| B
    H -->|否| I["返回按日响应、请求次数与拆分次数"]
    B -. 请求异常 .-> Y["抛错；不重试"]
    C -. 验收失败 .-> Y
'''),
'8179acb7': ('cli', '月份调度', '''
flowchart TD
    A["参数与写入边界；读取完成证据；形成待办"] --> B{"有待办？"}
    B -->|否| C["报告 up_to_date；不认证"]
    B -->|是| D["认证；逐品种月份请求、校准与归一化"]
    D --> E{"启用写入？"}
    E -->|否| F["报告只读验收；persisted=false"]
    E -->|是| G["读当前完整叶；核对 required；合并事实"]
    G --> H["排名事实 → 类型事实 → 两类日历叶"]
    H --> I["报告月份提交完成；persisted=true"]
    F --> J["继续下一月份；最后汇总运行"]
    I --> J
    D -. 异常 .-> X["报告当前阶段失败；抛错；不输出成功收尾"]
    G -. 异常 .-> X
    H -. 异常 .-> X
'''),
'abaf342b': ('entry', '当前执行入口', '''
flowchart TD
    A{"ipykernel 已加载？"} -->|是| B["main.main；空参数；standalone_mode=False"]
    B --> C["默认只读；有待办仍会认证和请求"]
    A -->|否| D{"直接运行脚本？"}
    D -->|是| E["main 读取命令行参数"]
    D -->|否| F["普通模块导入不启动"]
'''),
}

# 仅在独立函数边界分格，原始语句和注释留在原顺序。
splits = {
'6c44d64e': [
    ('def validate_calendar_frame(', 'a02-b02-validators', '## 三张表的业务验收\n\n三个 validator 分别验证日历状态、逐会员排名及参与者类型事实，并按权威主键排序。当前实现包含 Schema 往返、主键、分区及逐行检查；本轮全部保留，重复验收的收缩留待第 7 项。'),
    ('def open_optional_exact_dataset(', 'a02-b02-leaf-read', '## 启动窄列计数与当前完整叶读取\n\n启动阶段允许事实表不存在，并分批累计格点行数；写入阶段只枚举指定叶文件，检查根级零行标记、每个文件的物理契约，补回分区字段后完整验收。不存在的表或叶返回权威空表；缺失或非法标记按现有分支处理。')],
'6a26bc2a': [
    ('def full_fact_partition(', 'a02-b02-fact-merge', '## 当前事实叶的保留与替换\n\n从已读取的旧完整叶中保留未触达日期，替换本次待办日期，再拼接新事实并完整验收。两张事实分别执行同一规则；空结果也保留权威结构，此函数只生成内存结果。')],
'aa14209b': [
    ('def apply_calendar_completion(', 'a02-b02-completion', '## 事实复读后的日历状态生成\n\n两张事实正式复读成功后，按格点计数生成 `success` 或 `empty_confirmed`，记录本批 ID、完成时间及质检时间。已有来源重复和特殊案例校准 warning 永久保留；未触达格点、其他品种和仓单状态保持原样。这里仅修改内存日历，不能视为已提交。'),
    ('def commit_calendar_partitions(', 'a02-b02-calendar-commit', '## 两类报告日历逐叶提交\n\n从本批完成格点找到两类日历叶，依次提交每个完整叶。叶中其他品种及日期一起保留；后一个日历叶失败不撤销前一个成功叶。所有触达叶通过正式复读后，调用方才报告该品种月份提交完成。')],
}

main_id = '8179acb7'
replace('0a7103a8', '"query_batch_split: "', '"query_batch_split: function=query_rank_date_batch; phase=query; status=split; "')
replace(main_id, 'f"table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; mode={mode}; "', 'f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=plan; status=completed; mode={mode}; "')
replace(main_id, '"reconciliation_plan: "', '"reconciliation_plan: function=main; phase=plan; status=completed; "')
replace(main_id, 'f"planning_seconds={perf_counter() - planning_started_at:.3f}"', 'f"planning_seconds={perf_counter() - planning_started_at:.3f}; elapsed_s={perf_counter() - planning_started_at:.3f}"')
replace(main_id, 'click.echo("两张持仓事实及报告日历状态已经完整一致。")', '''click.echo(
            "up_to_date: function=main; phase=plan; status=completed; pending_grids=0; "
            "两张持仓事实及报告日历状态已经完整一致。"
        )
        click.echo(
            f"{log_boundary}\\n成交持仓报告运行完成 / Holding reports run completed\\n"
            f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome=up_to_date; write={str(write).lower()}; elapsed_s={perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )''')
replace(main_id, 'click.echo(f"pending_partitions={partition_count}")', 'click.echo(f"partition_plan: function=main; phase=plan; status=completed; pending_partitions={partition_count}")')
replace(main_id, 'f"partition_start: {group_number}/{partition_count}; "', 'f"partition_start: function=main; phase=partition; status=started; partitions={group_number}/{partition_count}; "')
replace(main_id, '"source_quality_warning: "', '"source_quality_warning: function=main; phase=normalize; status=warning; "')
replace(main_id, 'f"query_batch_success: key={partition_key}; dates={len(pending_dates)}; "', 'f"query_batch_success: function=main; phase=source_prepare; status=completed; key={partition_key}; dates={len(pending_dates)}; "')
replace(main_id, 'f"query_seconds={perf_counter() - query_started_at:.3f}"', 'f"query_seconds={perf_counter() - query_started_at:.3f}; elapsed_s={perf_counter() - query_started_at:.3f}"')
replace(main_id, 'f"api_success: key={partition_key}; "', 'f"api_success: function=main; phase=generate; status=completed; key={partition_key}; persisted=false; "')
replace(main_id, 'f"partition_checked: key={partition_key}; grids={len(group_df)}; "', 'f"partition_checked: function=main; phase=partition; status=completed; outcome=read_only; persisted=false; key={partition_key}; grids={len(group_df)}; "')
replace(main_id, 'f"partition_committed: key={partition_key}; "', 'f"partition_committed: function=main; phase=commit; status=completed; persisted=true; key={partition_key}; "')
source = ''.join(cells[main_id]['source'])
source = source.replace('f"elapsed_seconds={perf_counter() - partition_started_at:.3f}; "', 'f"elapsed_seconds={perf_counter() - partition_started_at:.3f}; elapsed_s={perf_counter() - partition_started_at:.3f}; "')
set_source(cells[main_id], source)
replace(main_id, 'f"finished: grids={completed_grid_count}; "', 'f"finished: function=main; phase=run; status=completed; outcome={\'committed\' if write else \'read_only\'}; grids={completed_grid_count}; "')
replace(main_id, '    planning_started_at = perf_counter()', '    log_phase = "plan"\n    planning_started_at = perf_counter()')
replace(main_id, '    jqdata = authenticate_jqdata(', '    log_phase = "authenticate"\n    jqdata = authenticate_jqdata(')
replace(main_id, '        query_started_at = perf_counter()', '        log_phase = "query"\n        query_started_at = perf_counter()')
replace(main_id, '        for grid in group_records:\n            raw_df =', '        log_phase = "normalize"\n        for grid in group_records:\n            raw_df =')
replace(main_id, '        incoming_position_df = (', '        log_phase = "generate"\n        incoming_position_df = (')
replace(main_id, '        commit_started_at = perf_counter()', '        log_phase = "read_complete_leaves"\n        commit_started_at = perf_counter()')
replace(main_id, '        complete_position_df = full_fact_partition(', '        log_phase = "merge"\n        complete_position_df = full_fact_partition(')
replace(main_id, '        committed_position_partition_df = commit_complete_partition(', '        log_phase = "commit_position"\n        committed_position_partition_df = commit_complete_partition(')
replace(main_id, '        committed_member_partition_df = commit_complete_partition(', '        log_phase = "commit_member"\n        committed_member_partition_df = commit_complete_partition(')
replace(main_id, '        completed_calendar_partition_df = apply_calendar_completion(', '        log_phase = "calendar_completion"\n        completed_calendar_partition_df = apply_calendar_completion(')
replace(main_id, '        calendar_rows = commit_calendar_partitions(', '        log_phase = "commit_calendar"\n        calendar_rows = commit_calendar_partitions(')
source = ''.join(cells[main_id]['source'])
body_line = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef)).body[0].lineno - 1
lines = source.splitlines(keepends=True)
body = ''.join(lines[body_line:]).rstrip() + '\n'
body += '''    click.echo(
        f"{log_boundary}\\n成交持仓报告运行完成 / Holding reports run completed\\n"
        f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=completed; "
        f"outcome={'committed' if write else 'read_only'}; write={str(write).lower()}; "
        f"elapsed_s={perf_counter() - log_started_at:.3f}\\n{log_boundary}"
    )
'''
prefix = '''    log_started_at = perf_counter()
    log_boundary = "=" * 88
    log_phase = "arguments"
    click.echo(
        f"{log_boundary}\\n成交持仓报告同步 / Holding reports run\\n"
        f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=started; "
        f"write={str(write).lower()}; elapsed_s=0.000"
    )
    try:
'''
suffix = '''    except Exception as log_error:
        click.echo(
            f"planning_progress: table={POSITION_TABLE_NAME}+{MEMBER_TABLE_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}; "
            f"elapsed_s={perf_counter() - log_started_at:.3f}"
        )
        raise
'''
set_source(cells[main_id], ''.join(lines[:body_line]) + prefix + textwrap.indent(body, '    ') + suffix)

overview = flow('a02-b02-flow-overview', '## 总流程：两张事实与成对报告日历', '''
flowchart TD
    A["检查参数；读取 required 日历与两张事实格点计数"] --> B["比较成对完成证据；形成自动待办"]
    B --> C{"存在待办？"}
    C -->|否| D["已完整；不认证、不请求"]
    C -->|是| E["认证；按交易所、品种、年月串行处理"]
    E --> F["查询待办日；触顶按日期二分；单日触顶失败"]
    F --> G["逐日冻结案例校准；归一化为两张事实与 warning"]
    G --> H{"启用 --write？"}
    H -->|否| I["仅验收；不落盘"]
    H -->|是| J["读取当前完整叶；保留未触达数据并合并"]
    J --> K["排名事实叶提交与复读 → 类型事实叶提交与复读"]
    K --> L["按正式计数生成状态；两类日历叶依次提交"]
    L --> M["该月份提交完成；累计进度"]
    I --> N{"还有月份？"}
    M --> N
    N -->|是| F
    N -->|否| O["汇总运行；无独立日期水位"]
    F -. 异常 .-> X["停止后续处理；此前成功叶保留；不自动重试"]
    G -. 异常 .-> X
    J -. 异常 .-> X
    K -. 异常 .-> X
    L -. 异常 .-> X
''')
new_cells = []
for cell in notebook['cells']:
    if cell['cell_type'] != 'code':
        new_cells.append(cell)
        if cell['id'] == 'd1b4d507':
            new_cells.append(overview)
        continue
    parts = [(cell, None)]
    if cell['id'] in splits:
        source = ''.join(cell['source'])
        positions = [(source.index(marker), cell_id, heading) for marker, cell_id, heading in splits[cell['id']]]
        set_source(cell, source[:positions[0][0]])
        for i, (position, new_id, heading) in enumerate(positions):
            end = positions[i + 1][0] if i + 1 < len(positions) else len(source)
            new_cell = {'cell_type': 'code', 'id': new_id, 'metadata': {}, 'execution_count': None, 'outputs': [], 'source': source[position:end].splitlines(keepends=True)}
            parts.append((new_cell, md(new_id + '-heading', heading)))
    for part, heading in parts:
        if heading:
            new_cells.append(heading)
        flow_id, title, source = flows[part['id']]
        new_cells.append(flow('a02-b02-flow-' + flow_id, '### 局部流程：' + title, source))
        new_cells.append(part)
notebook['cells'] = new_cells
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        ast.parse(''.join(cell['source']))
PATH.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + '\n', encoding='utf8', newline='\n')
print('Updated Notebook: explanations, 17 flowcharts, and log presentation; export pending.')
