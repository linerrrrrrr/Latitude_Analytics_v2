"""a02/b03 第 1—4 项：说明、流程图、分格和 main 日志；业务逻辑不变。"""
import ast
import copy
import hashlib
import json
import pathlib
import shutil
import tempfile
import textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]
RELATIVE = pathlib.Path('02_Futures_Lakehouse/a02_Futures_Exchange_Reports/b03_warehouse_receipt.ipynb')
path = ROOT/RELATIVE
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a02-b03-docs-logs-before-'))
paths = [ROOT/'AGENTS.md', ROOT/'.env.template', ROOT/'.gitattributes', ROOT/'.gitignore', ROOT/'03_Futures_Database/AGENTS.md', ROOT/'config/data_contracts.py']
paths.extend(p for p in (ROOT/'02_Futures_Lakehouse').rglob('*') if p.is_file() and p.suffix in ('.py', '.ipynb', '.md'))
hashes = {p.relative_to(ROOT).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
for relative in (RELATIVE, RELATIVE.with_suffix('.py')):
    target = snapshot/relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT/relative, target)
(snapshot/'hashes.json').write_text(json.dumps(hashes, ensure_ascii=False, indent=2), encoding='utf8')
notebook = json.loads(path.read_text(encoding='utf8'))
before = copy.deepcopy(notebook)
cells = {c['id']:c for c in notebook['cells']}


def set_source(cell, source):
    cell['source'] = source.splitlines(keepends=True)


def md(cell_id, source):
    return {'cell_type':'markdown', 'id':cell_id, 'metadata':{}, 'source':source.splitlines(keepends=True)}


def flow(name, title, source):
    return md('a02-b03-flow-'+name, title+'\n\n```mermaid\n'
        '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        +textwrap.dedent(source).strip()+'\n```')


descriptions = {
'ec1ca63b': '''# b03 JQData 期货仓单日报

从交易所报告日历选择未完成的仓单格点，每个交易所—品种—交易日串行查询一次 JQData `finance.FUT_WAREHOUSE_RECEIPT`。保存逐仓库数量、来源计量单位及较昨日变化，按完整品种月事实叶提交，再回写报告日历。

| 上下游 | 与本环节的关系 |
| --- | --- |
| a02/b01 报告日历 | 提供 `warehouse_receipt` 候选格点、当前采集义务和持久完成快照；本环节只回写仓单状态。 |
| 共享事实采集政策 | 白名单由上游日历生产者应用；这里消费其 required 标记，不再定义另一份名单。 |
| a02/b01a、a02/b02 | 运维顺序上位于本环节之前；仓单不读取其特殊案例 raw 或成交持仓事实，也不修改两类排名日历行。 |
| JQData 共享连接 | 只有存在待办才认证；认证和 Windows TUN 出口由 `config/jqdata_connection.py` 承担。 |
| `fact_futures_warehouse_receipt_daily` | 仓单事实输出；粒度为日期—交易所—品种—仓库，标准读取 Demo 和后续研究按权威 Schema 消费。 |
| operations | 按人工选定批次启动本入口，识别进度与分区耗时日志；本入口不启动未来批次。 |

字段、主键和分区来自 `config/data_contracts.py`；运行语义见湖仓根目录 `README.md`、`AGENTS.md` 与 `03_Futures_Database/AGENTS.md`。本入口没有独立日期水位文件。
''',
'12f98087': '''## 自动更新与写入边界

待办直接定义为：`dataset_name=warehouse_receipt AND is_fetch_required=true AND is_fetch_completed=false`。正式完成快照可信，启动时不从 clean 事实历史重新证明或修复完成状态；事实为空也不会自动推翻已完成日历。

| 运行方式 | 行为 |
| --- | --- |
| 默认不带参数 | 读取日历快照形成全部待办；有待办则认证、请求和转换，不提交事实或状态。 |
| `--write` | 按品种月份合并完整事实叶并提交，再生成和提交仓单日历状态。 |
| 成对日期 | 只过滤当前未完成待办，不重新审计已完成历史；只读可用，写入必须选择非正式测试湖。 |
| 没有待办 | 依据日历快照报告无需采集并退出，不认证 JQData。 |
| 可选性能门槛 | 两参数必须成对，仅允许无显式日期的自动正式 `--write`；不默认启用。 |

`--start-date` 与 `--end-date` 必须同时提供；没有 `--full`。正式湖根目录来自 `settings.futures_lake_root`。空湖必须先由上游 b01 提供报告日历，仓单事实由同一待办路径自然建表。

事实叶先提交，日历叶随后逐个提交；它们当前不共同回滚。日历失败时已提交事实保留，未完成状态仍会形成下一次人工启动的待办。查询或转换失败时，带 `--write` 会尝试回写该格点的失败状态，然后停止；不会自动重试。
''',
'4bf5b012': '''## 初始化与共享依赖

按 `.env.template` 的三个项目标记定位根目录，导入两张权威 Schema、统一转换入口、JQData 共享认证和项目设置。本格只加载依赖，不认证、不请求或写入。
''',
'dffc6cfc': '''## Schema 契约与有界本地样例

仅在交互内核且没有 `__file__` 时调用共享浏览器，依次展示报告日历和仓单事实。字段解释来自权威 Schema；传入 `lake_root` 后支持显式查看有界本地样例，不能把本格描述成完全不读取数据湖。

展示不认证 JQData、不调用来源 API、不写文件；普通脚本执行跳过展示。
''',
'f68b275b': '''## 表身份、Hive 分区与来源约定

表名、主键和分区列各从 Schema metadata 读取一次，Hive partitioning 在此建立。事实叶为 `exchange_code/underlying_code/year/month`；日历叶为 `dataset_name/exchange_code/year/month`，同一叶可包含其他品种，回写时必须保留。

`CALENDAR_PLANNING_COLUMNS` 限定启动规划投影，`JQDATA_FIELDS` 固定请求字段。交易所别名仅用于复核来源，输出身份仍继承日历；数量、单位和变化均保留来源语义。
''',
'1daeab12': '''## 零行标记与启动日历物理契约

根级 `schema.parquet` 必须是零行且满足物理字段、类型、nullable 和表名/主键/分区身份 metadata。日历根标记必须存在；事实表可以尚未创建，但已经有内容的根目录不能缺少标记。

`open_planning_calendar_dataset()` 在启动时核对日历 Dataset 和各 fragment 的物理契约；随后 `main()` 只读取 required 仓单的规划列。描述性 metadata 以当前配置为准，不要求历史文件重写；这里不重新验证上游完整业务语义。
''',
'1552c334': '''## JQData 仓单响应归一化

`normalize_warehouse_response()` 要求 DataFrame；空响应直接返回权威空表，包括当前允许的零行零列响应。非空响应必须包含约定列、少于 5000 行，并精确属于待办日期、品种和允许的交易所别名；单日触顶直接失败，不分页或拆日。

逐行保留仓库名称、有限非负数量、可空来源单位和可空有限数量变化。单位空白或来源缺失归一为 null，后续在日历记录 warning；变化保留正负号。补入日历身份、年月和本批更新时间后转换为权威 Arrow/Pandas 表示。

这里只做来源验收和契约转换。完整事实主键及业务校验放在合并后的 dirty 叶；只读路径不读取旧事实或提交状态。
''',
'1e13ae1c': '''## 可信完成快照与待办规划

`pending_report_grids()` 接收已在读取时过滤为 required 仓单的规划列，以 `is_fetch_completed` 区分完成和待办，再按交易所—品种—年月—日期排序。它不扫描事实历史、不按历史事实计数修复日历。

`grid_count_map()` 只用于当前完整叶的格点计数：成功回写取本次已验收事实的计数，采集失败回写取当前正式叶的计数；不参与启动时的全历史完成证明。
''',
'406f1843': '''## 当前事实叶 staging、安装与恢复

`commit_complete_partition()` 接收已完整验证的 dirty 事实叶，检查分区范围，生成预期主键/行数摘要。staging 写入零行标记和非空叶，经物理契约及摘要复读后，安装缺失的根标记、备份旧叶并安装新叶；空结果通过移走旧叶表达。

正式标记与叶摘要通过后才返回。这里不重复完整业务 validator，也不逐值复读全部业务列。当前仍为手写恢复：尝试隔离目标、恢复旧叶并移除新标记；恢复成功清理临时路径，恢复失败保留现场并抛错。

这套恢复尚未按实际移动记录接入共享事务，首次备份失败的边界及异常路径表根清理留待第 10 项处理。本轮保留实现，不把图示写成已经具备跨表共同回滚。
''',
'49b7a75f': '''## 仓单日历的完成与失败状态生成

`apply_calendar_completion()` 只更新当前格点：有事实行写 `success`，无行写 `empty_confirmed + warning`，有缺失单位的非空事实写 `success + warning`。同时记录条数、批次与时间；其他品种和日期保持原样。计数和单位缺失来自已验收的内存完整叶，其正式路径已通过物理契约及主键摘要检查。

`apply_calendar_failure()` 在查询或转换失败时保留未完成状态，记录 `retryable_error` 或 `permanent_error`、失败原因、当前正式事实计数和质量失败时间，完成时间保持空。两条生成路径各在返回前完整验证自己的 dirty 日历叶；返回值仍只是内存结果。
''',
'a407d548': '''## JQData 单格点查询边界

`query_warehouse_grid()` 显式选择来源列，按一个品种和交易日调用一次 `finance.run_query()`；来源交易所由后续响应归一化复核。权限或缺表异常标记为 `permanent_error`，其他查询异常及返回 `None` 标记为 `retryable_error`，原异常链保留。

错误分类不代表程序会自动重试。查询或转换失败均交由 `main()` 的既有失败状态回写分支处理，然后停止批次。
''',
'b4752348': '''## CLI：月份调度、失败停止与耗时汇总

`main()` 检查日期、正式写入及性能参数，读取日历窄列完成快照；无待办直接结束。存在待办才认证，依次处理每个品种月份：逐日请求和转换、拼接本次来源事实；只读到此结束当前月份。

写入时读取当前事实叶和仓单日历叶，复核待办身份，保留未触达日期并生成 dirty 完整事实叶。事实安装及摘要复读成功后，才生成并提交日历完成状态。查询或转换异常时，只在启用 `--write` 后尝试提交该格点失败状态，再抛错；若回写本身失败，同样停止。

当前 `main()` 保留原有 API、事实、日历、分区总耗时计时位置及性能门槛。运行日志统一使用 88 个 `=`、`function/phase/status/elapsed_s`，并保留 monitor 使用的前缀；`partition_committed:` 仍在事实和日历均成功后输出，`persisted=false` 标明只读结果。无待办日志只表述可信快照无待办，不声称重新核验了全部事实。

函数内部进度与日志归属留待第 5—6 项；本轮只规范现有主流程日志，异常仍按原类型和异常链抛出。
''',
'25a0004c': '''## 当前 Notebook 与脚本运行入口

Notebook 当前显式使用 `args=[]`、`standalone_mode=False`，避免读取内核参数；默认不带 `--write`，有待办仍会认证和请求。

当前入口只检查 `ipykernel` 是否加载，因此 Notebook 中导入同名 Python 模块仍可能触发采集。显式 `notebook_args`、模块导入保护和末尾终端命令格留待第 11 项对齐，本轮保留原入口。
''',
}
for cell_id, text in descriptions.items():
    set_source(cells[cell_id], text.rstrip())

inserted = {}
def split(cell_id, parts):
    source = ''.join(cells[cell_id]['source'])
    offsets = [0]+[source.index('def '+name+'(') for name, *_ in parts]+[len(source)]
    set_source(cells[cell_id], source[:offsets[1]])
    added = []
    for index, (name, new_id, heading) in enumerate(parts, start=1):
        added.append(md(new_id+'-heading', heading))
        added.append({'cell_type':'code', 'id':new_id, 'metadata':{}, 'execution_count':None, 'outputs':[], 'source':source[offsets[index]:offsets[index+1]].splitlines(keepends=True)})
    inserted[cell_id] = added

split('8e8b39eb', [
    ('validate_calendar_frame', 'a02-b03-validators', '''## dirty 输出叶的业务校验

日历 validator 检查仓单身份、主键、状态、计数、审计字段和年月；事实 validator 检查主键、仓库名称、来源、数量、变化和更新时间。输入先按权威 Schema 转换，再按主键排序返回。

它们分别由完整事实合并、日历完成或失败状态生成调用；clean 读取以及 staging/正式安装不重新调用这些完整业务规则。'''),
    ('partition_relative_path', 'a02-b03-leaf-read', '''## 当前叶读取与主键摘要

`read_partition_leaf()` 只枚举指定叶的 Parquet 文件，核对物理契约、补回 Hive 列并检查分区范围，返回当前完整叶。不存在的叶返回权威空表，存在但无 Parquet 的叶报错。

`read_leaf_primary_key_summary()` 在 staging 和正式路径只投影主键与分区列；检查范围、主键唯一性，并对排序后的主键计算 SHA-256 与行数。摘要证明行数和主键集合一致，不表示逐值比较所有业务列。'''),
])
split('48782ab9', [
    ('full_fact_partition', 'a02-b03-fact-merge', '''## 完整事实叶合并与提交前待办复核

`full_fact_partition()` 保留旧叶中未触达日期，拼接本次事实，执行一次完整业务校验；空结果保留权威结构。`ensure_pending_calendar_grids()` 从刚读取的当前日历叶确认本次格点唯一、required 且仍未完成。

这些操作只针对当前品种月份及其对应日历叶，不重新打开或物化整张历史事实表。'''),
    ('performance_gate_result', 'a02-b03-performance', '''## 可选性能门槛

两个性能参数必须同时提供，且只用于无显式日期的自动正式 `--write`。窗口不足时不判断；样本是成功完成事实与日历提交的分区总耗时，仅在首个完整窗口形成时检查一次。

已记录的恢复配置为 `--performance-window-size 50 --performance-max-median-seconds 15.4`：首 50 个成功分区中位数不超过 15.4 秒为通过；超限则在该分区完成后停止，保留已成功提交，不处理第 51 个分区。默认日常运行不开启此门槛。'''),
])
split('51ddd24b', [
    ('commit_calendar_partitions', 'a02-b03-calendar-commit', '''## 仓单日历逐叶提交

只选择本次触达的仓单日历叶，保留叶内其他品种和日期；输入来自已完整验证的完成或失败状态生成函数。每个叶写 staging，复读物理契约与主键/行数摘要，再备份、安装并正式摘要验收；不重复完整业务 validator。

当前每个日历叶独立恢复，根级日历标记只验证、不替换。日历失败不撤销此前已提交事实或日历叶；失败状态写入也走同一提交函数，不得把“失败记录已落盘”描述为“采集完成”。共享事务留待第 10 项接入。'''),
])

flows = {
'123d7ac2': flow('init', '### 局部流程：初始化', '''
flowchart TD
    A["当前目录及父目录"] --> B{"三个项目标记齐全？"}
    B -->|是| C["加入项目和湖仓模块路径；加载契约与配置"]
    B -->|否| D["抛错"]
    C --> E["仅加载定义；不请求或写入"]
'''),
'b604aed0': flow('schema', '### 局部流程：Schema 与样例展示', '''
flowchart TD
    A{"交互内核且无 __file__？"} -->|否| B["跳过展示"]
    A -->|是| C["展示两张表的权威 Schema"]
    C --> D["用户显式选择时读取有界本地样例"]
    D --> E["不请求来源或写入文件"]
'''),
'a9476637': flow('constants', '### 局部流程：契约常量', '''
flowchart TD
    A["两张权威 Schema"] --> B["一次读取表名、主键、分区列"]
    B --> C["构建 Hive partitioning"]
    C --> D["固定规划列、来源列、来源标签与交易所别名"]
'''),
'8e8b39eb': flow('contract', '### 局部流程：启动物理契约', '''
flowchart TD
    A["检查根级零行标记"] --> B{"标记存在且契约兼容？"}
    B -->|是| C["打开日历 Dataset；重建 Hive 字段核对身份"]
    B -->|否| D["日历或已有内容报错；允许未创建的事实表"]
    C --> E["逐 fragment 检查物理契约"]
    E --> F["返回 Dataset；main 读取仓单规划列"]
'''),
'a02-b03-validators': flow('validators', '### 局部流程：dirty 叶业务校验', '''
flowchart TD
    A["生成的完整 dirty 叶"] --> B["按 Schema 转换；检查主键唯一性"]
    B --> C{"输出类型？"}
    C -->|事实| D["仓库、来源、年月、数量、变化、审计时间"]
    C -->|日历| E["仓单身份、状态一致性、计数、审计时间"]
    D --> F["按主键排序；返回验证结果"]
    E --> F
'''),
'a02-b03-leaf-read': flow('read', '### 局部流程：完整叶和摘要读取', '''
flowchart TD
    A["按分区键定位当前叶"] --> B{"叶存在？"}
    B -->|否| C["返回权威空表或空主键摘要"]
    B -->|是| D["只枚举当前叶文件；核对物理契约"]
    D --> E{"读取目的？"}
    E -->|合并输入| F["读取完整叶；核对范围并转换"]
    E -->|安装验收| G["投影主键和分区；核对范围及唯一性"]
    G --> H["排序主键；返回行数与 SHA-256"]
'''),
'c7c6a8f1': flow('normalize', '### 局部流程：来源响应归一化', '''
flowchart TD
    A["来源必须为 DataFrame"] --> B{"空响应？"}
    B -->|是| C["返回权威空表"]
    B -->|否| D["列齐全；少于 5000 行；精确格点匹配"]
    D --> E["逐行验收仓库、数量、单位与变化"]
    E --> F["单位缺失转 null；补日历身份和年月"]
    F --> G["转换为权威 Arrow/Pandas 结果"]
'''),
'48782ab9': flow('plan', '### 局部流程：快照待办', '''
flowchart TD
    A["已过滤的 required 仓单规划行"] --> B{"is_fetch_completed？"}
    B -->|是| C["信任完成凭证；计入完成数"]
    B -->|否| D["加入待办；按交易所、品种、年月、日期排序"]
    C --> E["返回待办与完成计数；不扫描事实历史"]
    D --> E
'''),
'a02-b03-fact-merge': flow('merge', '### 局部流程：待办复核与完整叶生成', '''
flowchart TD
    A["读取当前日历叶与旧事实叶"] --> B["确认待办唯一、required、未完成"]
    B --> C["旧事实保留未触达日期；拼接新事实"]
    C --> D["完整 dirty 叶执行一次业务校验"]
    D --> E["返回内存完整叶；尚未提交"]
'''),
'a02-b03-performance': flow('performance', '### 局部流程：首个成功窗口的性能判断', '''
flowchart TD
    A["当前事实和日历均已成功提交"] --> B{"开启门槛且恰好达到首个窗口？"}
    B -->|否| C["继续后续分区"]
    B -->|是| D["计算首窗口分区总耗时中位数"]
    D --> E{"不超过上限？"}
    E -->|是| C
    E -->|否| F["报告门槛失败并停止；已提交分区保留"]
'''),
'df69af57': flow('fact-commit', '### 局部流程：当前事实叶安装与恢复', '''
flowchart TD
    A["已验证事实叶；检查范围并计算主键摘要"] --> B["写 staging 零行标记和非空叶"]
    B --> C["物理契约、行数及主键摘要复读"]
    C --> D["安装新标记；备份旧叶；非空安装新叶"]
    D --> E["正式标记与叶摘要复读"]
    E --> F["清理临时路径；返回事实行数"]
    D -. 失败 .-> R["现有手写恢复：尝试隔离目标、恢复旧叶、移除新标记"]
    E -. 失败 .-> R
    R --> S{"恢复分支报错？"}
    S -->|否| T["清理 staging、备份与隔离；重抛提交异常"]
    S -->|是| U["保留现场；抛出恢复异常"]
    B -. 失败 .-> V["清理 staging；抛错"]
    C -. 失败 .-> V
'''),
'51ddd24b': flow('state', '### 局部流程：完成与失败状态生成', '''
flowchart TD
    A{"当前结果？"} -->|事实已提交| B{"当前格点有事实行？"}
    B -->|否| C["empty_confirmed；completed=true；warning"]
    B -->|是| D["success；completed=true；单位缺失则 warning"]
    A -->|查询或转换失败| E["retryable/permanent_error；completed=false；quality=failed"]
    C --> F["更新本次格点计数、批次与时间；保留其他行"]
    D --> F
    E --> F
    F --> G["完整验收 dirty 日历叶；返回内存结果"]
'''),
'a02-b03-calendar-commit': flow('calendar-commit', '### 局部流程：日历逐叶安装', '''
flowchart TD
    A["已验证日历；定位触达的 warehouse_receipt 叶"] --> B["逐叶取完整内容；保留其他品种和日期"]
    B --> C["写 staging；物理契约与主键摘要复读"]
    C --> D["备份并安装当前叶；正式摘要复读"]
    D --> E{"还有日历叶？"}
    E -->|是| B
    E -->|否| F["返回触达行数"]
    D -. 失败 .-> G["现有手写恢复当前叶；抛错停止"]
    G --> H["此前成功事实和日历叶保留"]
'''),
'f5cf142d': flow('query', '### 局部流程：单日查询', '''
flowchart TD
    A["显式来源列；品种和交易日过滤"] --> B["run_query 一次"]
    B --> C{"异常或 None？"}
    C -->|否| D["返回响应给归一化函数"]
    C -->|是| E["权限/缺表分类为 permanent；其他为 retryable"]
    E --> F["保留异常链并抛出；不自动重试"]
'''),
'f4cfe66c': flow('cli', '### 局部流程：月份调度与运行日志', '''
flowchart TD
    A["运行开始；参数检查与窄列规划"] --> B{"有待办？"}
    B -->|否| C["依据完成快照报告无需采集"]
    B -->|是| D["整批认证一次"]
    D --> P["当前品种月开始日志"]
    P --> E["逐日查询与来源归一化"]
    E --> X["汇总当月来源结果"]
    X --> F{"启用 --write？"}
    F -->|否| G["只读检查与分区耗时；不提交"]
    F -->|是| H["读取当前叶；复核待办；合并并提交事实"]
    H --> I["生成并提交日历；报告当前分区成功及耗时"]
    I --> J["必要时检查首个完整性能窗口"]
    J -->|超限| K["失败停止；已提交保留"]
    J -->|继续| L{"还有分区？"}
    G --> L
    L -->|是| P
    L -->|否| M["运行完成汇总"]
    E -. 查询或转换失败 .-> N["带 write 时尝试回写失败状态；随后抛错"]
    N --> K
    H -. 失败 .-> K
    I -. 失败 .-> K
'''),
'd4e12ca7': flow('entry', '### 局部流程：当前执行入口', '''
flowchart TD
    A{"ipykernel 已加载？"} -->|是| B["main.main；args=[]；standalone_mode=False"]
    B --> C["默认只读；有待办仍认证并请求"]
    A -->|否| D{"直接运行脚本？"}
    D -->|是| E["main 读取命令行参数"]
    D -->|否| F["普通模块导入不启动"]
'''),
}
overview = flow('overview', '## 总流程：仓单采集与状态回写', '''
flowchart TD
    A["参数、正式写入与性能门槛边界"] --> B["读取 required 仓单日历快照"]
    B --> C{"存在未完成格点？"}
    C -->|否| D["无需采集；不认证"]
    C -->|是| E["认证；按交易所、品种、年月分组"]
    E --> F["逐待办日请求一次；归一化来源"]
    F --> G{"启用 --write？"}
    G -->|否| H["只读汇总；进入下一分区"]
    G -->|是| I["读当前叶；复核待办；生成完整事实叶"]
    I --> J["事实单叶提交；正式物理契约与摘要复读"]
    J --> K["生成 success 或 empty_confirmed；保留单位缺失 warning"]
    K --> L["日历逐叶提交；正式摘要复读"]
    L --> M["记录成功分区总耗时；必要时检查性能门槛"]
    M --> N{"门槛允许继续？"}
    N -->|是| O["后续分区或运行结束"]
    H --> O
    N -->|否| P["停止；已提交分区保留"]
    F -. 失败 .-> Q{"启用 --write？"}
    Q -->|是| R["读取当前叶并回写该格点失败状态；仍未完成"]
    Q -->|否| P
    R --> P
    J -. 提交失败 .-> P
    L -. 提交失败 .-> P
''')

code = ''.join(cells['f4cfe66c']['source'])
def replace(old, new):
    global code
    assert code.count(old) == 1, old
    code = code.replace(old, new)

replace('    planning_started_at = time.perf_counter()', '    log_phase = "planning"\n    planning_started_at = time.perf_counter()')
replace('        f"table={TABLE_NAME}; mode={mode}; "', '        f"{log_boundary}\\nplanning_progress: table={TABLE_NAME}; function=main; phase=planning; status=completed; mode={mode}; "')
replace('        f"lake_root={resolved_lake_root}; write={str(write).lower()}"', '        f"lake_root={resolved_lake_root}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace('        "reconciliation_plan: "', '        "reconciliation_plan: function=main; phase=plan; status=completed; "')
replace('        click.echo("仓单事实与报告日历状态已经完整一致。")', '''        click.echo(
            f"up_to_date: table={TABLE_NAME}; function=main; phase=run; status=completed; outcome=up_to_date; "
            f"basis=calendar_completion_snapshot; pending_grids=0; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )''')
replace('    jqdata = authenticate_jqdata(', '    log_phase = "authenticate"\n    jqdata = authenticate_jqdata(')
replace('    click.echo(f"pending_partitions={partition_count}")', '    click.echo(f"partition_plan: function=main; phase=plan; status=completed; pending_partitions={partition_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}")')
replace('        partition_key = tuple(raw_partition_key)', '        log_phase = "partition"\n        partition_key = tuple(raw_partition_key)')
replace('            f"key={partition_key}; grids={len(group_df)}"', '            f"function=main; phase=partition; status=started; key={partition_key}; grids={len(group_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace('                raw_df = query_warehouse_grid(jqdata, grid)', '                log_phase = "query"\n                raw_df = query_warehouse_grid(jqdata, grid)')
replace('                grid_df = normalize_warehouse_response(raw_df, grid, updated_at)', '                log_phase = "normalize"\n                grid_df = normalize_warehouse_response(raw_df, grid, updated_at)')
replace('            except Exception as error:\n                message = str(error)', '            except Exception as error:\n                log_source_failure_phase = log_phase\n                message = str(error)')
replace('                if write:\n                    fetch_status = (', '                if write:\n                    log_phase = "record_failure"\n                    fetch_status = (')
replace('                raise click.ClickException(message) from error', '                log_phase = log_source_failure_phase\n                raise click.ClickException(message) from error')
replace('        if any(not frame.empty for frame in frames):', '        log_phase = "assemble_facts"\n        if any(not frame.empty for frame in frames):')
replace('            f"api_success: key={partition_key}; rows={len(incoming_df)}"', '            f"api_success: function=main; phase=generate_facts; status=completed; key={partition_key}; rows={len(incoming_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace('                f"partition_checked: key={partition_key}; grids={len(group_df)}"', '                f"partition_checked: function=main; phase=partition; status=completed; outcome=read_only; key={partition_key}; grids={len(group_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
# 两条耗时日志保留原字段和计时起止位置。
code = code.replace('f"partition_timing: key={partition_key}; grids={len(group_df)}; "', 'f"partition_timing: function=main; phase=partition; status=completed; key={partition_key}; grids={len(group_df)}; "')
replace('        fact_started_at = time.perf_counter()', '        log_phase = "read_current_leaves"\n        fact_started_at = time.perf_counter()')
replace('        complete_df = full_fact_partition(', '        log_phase = "merge_fact"\n        complete_df = full_fact_partition(')
replace('        commit_complete_partition(\n', '        log_phase = "commit_fact"\n        commit_complete_partition(\n')
replace('        completed_calendar_partition_df = apply_calendar_completion(', '        log_phase = "generate_calendar_state"\n        completed_calendar_partition_df = apply_calendar_completion(')
replace('        calendar_rows = commit_calendar_partitions(', '        log_phase = "commit_calendar"\n        calendar_rows = commit_calendar_partitions(')
replace('            f"partition_committed: key={partition_key}; "', '            f"partition_committed: function=main; phase=commit; status=completed; persisted=true; key={partition_key}; "')
replace('            f"calendar_rows={calendar_rows}; grids={len(group_df)}"', '            f"calendar_rows={calendar_rows}; grids={len(group_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"')
replace('            gate_result = performance_gate_result(', '            log_phase = "performance_gate"\n            gate_result = performance_gate_result(')
replace('                click.echo(message)', '                click.echo(f"{message}; function=main; phase=performance_gate; status=failed; elapsed_s={time.perf_counter() - log_started_at:.3f}")')
replace('                f"max_median_seconds={performance_max_median_seconds:.3f}"\n            )', '                f"max_median_seconds={performance_max_median_seconds:.3f}; function=main; phase=performance_gate; status=completed; elapsed_s={time.perf_counter() - log_started_at:.3f}"\n            )')
replace('        f"finished: grids={processed_grid_count}; rows={total_rows}; "', '        f"finished: function=main; phase=run; status=completed; outcome={\'committed\' if write else \'read_only\'}; grids={processed_grid_count}; rows={total_rows}; "')
replace('        f"performance_samples={len(successful_partition_seconds)}"', '        f"performance_samples={len(successful_partition_seconds)}; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"')
body_start = code.index('    formal_lake_root = settings.futures_lake_root.resolve()')
code = code[:body_start]+'''    log_started_at = time.perf_counter()
    log_phase = "arguments"
    log_boundary = "=" * 88
    click.echo(
        f"{log_boundary}\\n仓单日报 / Warehouse receipts\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; write={str(write).lower()}; elapsed_s=0.000"
    )
    try:
'''+textwrap.indent(code[body_start:].rstrip(), '    ')+'''
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )
        raise
'''
ast.parse(code)
set_source(cells['f4cfe66c'], code)
expanded = []
for cell in notebook['cells']:
    expanded.append(cell)
    if cell['id'] == '12f98087':
        expanded.append(overview)
    expanded.extend(inserted.get(cell['id'], []))
final_cells = []
for cell in expanded:
    if cell['cell_type'] == 'code':
        final_cells.append(flows[cell['id']])
    final_cells.append(cell)
notebook['cells'] = final_cells
path.write_text(json.dumps(notebook, ensure_ascii=False, indent=1)+'\n', encoding='utf8', newline='\n')
print(snapshot)
