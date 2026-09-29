"""同步 SHIBOR 校验责任、流程图、契约文字及测试调用边界。"""
import ast
import pathlib

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb'
nb = nbformat.read(PATH, 4)
cells = {c.id: c for c in nb.cells}


def doc(cell, text):
    cells[cell].source = text.strip() + '\n'


def flow(cell, title, graph):
    doc(cell, f'### 流程：{title}\n\n```mermaid\nflowchart TD\n{graph.strip()}\n```')


doc('d8b916dd', '''## 自动范围、来源结果与写入边界

默认范围仍是上游 `interest_rate` required 格点减去正式事实与日历状态共同完整的格点。只在启动时对账一次，事实存在但状态陈旧的格点无 API 修复；没有事实也没有完整确认空凭证的格点按年月请求。事实越出当前 required 水位仍报错，不静默删除。

成对日期只筛选本次格点，显式日期禁止写正式湖；不带 `--write` 仍可能请求 API 并转换响应，但不提交事实或日历。每月请求一次，以待办观测日的最小、最大值为边界，来源日期唯一、范围、必需列、2000 行上限、非空数值类型、有限性及 `[-100, 100]` 百分比门禁全部保留，利率不除以 100。

来源转换结果做一次业务验收；完整响应中缺日或缺值形成 0 行预期，当前事实完整叶正式复读为 0 后才写 `empty_confirmed + warning`。非法响应记录未完成的失败状态，继续其他月份，批末抛错，不自动重试。安装或验收失败立即停止，此前成功提交保留。

当前契约的正式历史信任生产者业务证明，读取只确认物理结构、表名、主键、分区及契约版本；纯描述性 metadata 差异使用当前说明，不重写历史。物理/身份不兼容直接拒绝；兼容旧事实版本仍须在无日期写入模式经一次当前规则验收后整根迁移，日历旧版本由上游 b01 先迁移。

相关材料：[湖仓规则](../AGENTS.md)、[湖仓说明](../README.md)、[数据库规则](../../03_Futures_Database/AGENTS.md)。''')
flow('a04-b02-flow-overview', 'SHIBOR 事实与日历完成凭证', '''A["读取可信日历和事实；物理、身份、版本检查"] --> B{"事实为兼容旧版本？"}
B -->|是| C["无日期写入才整根迁移；描述变化不迁移"]
B -->|否| D["启动一次：事实计数和日历完成状态共同对账"]
C --> D
D --> E["预建事实、日历叶映射；已有事实无 API 修复日历"]
E --> F["按月请求；只转换精确待办；验收来源输出"]
F --> G{"启用 write？"}
G -->|否| H["只读结果；不提交"]
G -->|是| I["合并当前事实叶；dirty 业务验收一次"]
I --> J["共享事务内安装与正式叶逐值复读"]
J --> K["按正式计数生成当前日历叶；独立事务提交"]
K --> L["更新已提交叶映射和计数；批末不重扫历史"]
F -. 来源失败 .-> R["只回写当前日历失败状态；继续其他月；批末抛错"]
J -. 失败 .-> X["恢复当前目标；保留证据；停止"]
K -. 失败 .-> X
E --> N["无 API 待办则结束；不创建客户端"]''')
doc('a04-b02-doc-open-compatible-dataset', '''## 一次 fragment 遍历的物理与身份读取

支持表根、完整叶及零行标记文件；读取叶时显式指定 `partition_base_dir`，从其相对 Hive 路径补回分区列。检查 Dataset 物理字段、类型、nullable 与表身份，随后在同一次 fragment 遍历中检查物理结构、表名、主键、分区和版本。纯描述性表级或字段 metadata 差异不触发重写；旧版本用返回标志表达，物理或身份错误直接拒绝。

只打开 Dataset，不物化记录。函数自主报告发现文件数、已检查片段数及兼容结果；不再额外遍历所有片段来重复检查 metadata。''')
flow('a04-b02-flow-open-compatible-dataset', '一次物理与身份检查', '''A["指定根、叶或标记文件；叶指定分区根"] --> B["打开 Dataset；检查物理结构与表身份"]
B --> C["一次遍历 fragment：物理、身份、版本"]
C --> D["返回 Dataset 和当前版本标志；尚未物化"]
B -. 不兼容 .-> X["拒绝读取"]
C -. 不兼容 .-> X''')
doc('a04-b02-doc-open-exact-dataset', '''## 按当前物理、身份与版本契约读取

复用兼容读取，要求版本标志为真；日历和事实都以当前 Schema 的说明性 metadata 为准。表根用于启动读取或旧版本整根迁移，月度提交直接打开当前叶，不遍历其他月份。''')
doc('ab4baaf6', '''## 日历 dirty 完整叶的业务验收

输入是已经由 `pandas_to_arrow()` 完成字段和类型契约转换的 Arrow 表，直接检查主键、interest_rate 类型、系列、年月、0/1 计数、updated_at 与完成状态组合，返回排序后的同一 Arrow 表示。不再在函数里转换整张 Pandas 表。

只在日历提交前对当前 dirty 完整叶调用一次。上游日历读取、状态生成、staging 和正式复读不重复调用；上游自身的可用日规则继续由 b01 负责。''')
flow('a04-b02-flow-af5c2739', '日历 dirty 叶业务验收', '''A["已经转换的当前日历叶 Arrow 表"] --> B["主键、系列、年月、计数和完成状态检查"]
B --> C["按主键排序；返回 Arrow 表；尚未提交"]''')
doc('a04-b02-doc-validate-interest-rate-frame', '''## 来源输出与事实 dirty 完整叶验收

`validate_interest_rate_table()` 直接消费已经转换的 Arrow 表，检查主键、8 期限、来源标签、有限值、百分比范围、年月和 updated_at，返回按主键排序的 Arrow 表。

来源转换结果验收一次；事实提交前合并得到的 dirty 完整叶验收一次。当前版本 clean 历史、合并函数、staging 与正式复读不重复做这套业务检查；旧版本事实整根迁移是单独的完整业务验收边界。''')
flow('a04-b02-flow-validate-interest-rate-frame', '事实业务验收', '''A["已经转换的来源输出或 dirty 事实叶"] --> B["主键、系列、来源、利率、年月和时间检查"]
B --> C["排序返回 Arrow 表；复用到 staging 和比较"]''')
doc('a04-b02-doc-read-interest-calendar', '''## 读取可信上游日历

启动时打开正式日历，确认物理、身份和当前版本，物化 `interest_rate` 行并按权威 Schema 转为 Pandas。信任 b01 正式提交的主键、理论格点和状态业务证明，不重新执行整表业务校验。上游缺失或旧版本仍报错，b02 不创建日历根。''')
flow('a04-b02-flow-read-interest-calendar', '读取上游日历', '''A["正式日历物理、身份与版本检查"] --> B["物化 interest_rate 行"]
B --> C["权威 Arrow 到 Pandas 转换；信任业务证明"]''')
doc('a04-b02-doc-read-optional-fact', '''## 读取可信事实与旧版本标志

空目录返回契约化空表；非空目录检查物理字段、表身份和版本，物化一次并直接按权威 Schema 转为 Pandas，不先重建一份 Arrow 表再验收历史业务。版本标志只由 schema_version 决定，描述性变化不触发历史改写；旧版本写入迁移仍由 main 的无日期门禁控制。''')
flow('a04-b02-flow-read-optional-fact', '读取事实', '''A{"存在 Parquet？"} -->|否| B["返回契约空表和当前版本标志"]
A -->|是| C["物理、身份、版本检查；物化一次"]
C --> D["直接按权威 Schema 转 Pandas；返回版本标志"]''')
doc('a04-b02-doc-plan-interest-rate-grids', '''## 一次求差与无 API 修复计划

启动时以正式事实格点计数和日历状态共同判断完整。保留事实越出 required 水位拒绝、格点最多 1 行、确认空凭证及陈旧状态修复规则。required_df 本身已经定义本次遍历范围，删除“由它生成集合后再判断同一行属于集合”的重复判断。

得到 API 待办、修复待办和完整格点数后复用，不在修复后和批末全表重读重算。后续只在当前叶正式验收成功后累计完成数和更新叶映射。''')
doc('dd29e6da', '''## 合并当前事实完整月叶

调用方从循环前建立的 `fact_leaves` 取当前叶；函数不再从完整历史按年月筛选。仅删除本次触达主键对应的旧行，再与来源已验收的精确待办事实合并，同月未触达数据保留。

合并只生成候选完整叶，不再重复调用业务校验；正式写入前由提交函数对这份 dirty 完整叶验收一次。只读模式仍已验收来源输出。''')
flow('a04-b02-flow-45aafcf7', '合并当前事实叶', '''A["当前旧叶、精确待办事实、触达主键"] --> B["确认触达月份；剔除旧叶中触达主键"]
B --> C["合并来源事实；保留同月其他行"]
C --> D["返回候选完整叶；提交前业务验收"]''')
doc('a04-b02-doc-write-fact-staging', '''## 复用 Arrow 的事实暂存

接收提交函数已经转换并完成质量验收的 Arrow 表，直接写零行标记和非空事实叶；不再次执行 Pandas 转 Arrow。文件 Schema、列序、排序键、请求字段字符串等稳定对象在模块初始化时准备，各月份复用。''')
flow('a04-b02-flow-write-fact-staging', '事实暂存', '''A["已通过业务验收的 Arrow 表"] --> B["写零行标记；复用文件 Schema"]
B --> C{"有数据？"}
C -->|是| D["直接写当前事实叶"]
C -->|否| E["只保留空标记"]
D --> F["报告暂存完成；未落盘"]
E --> F''')
doc('a04-b02-doc-upgrade-fact-metadata', '''## 兼容旧版本事实整根迁移

仅 schema_version 过期且物理结构、表身份兼容时进入该路径，纯描述性 metadata 差异不迁移。main 仍只允许无日期写入模式提交迁移。

完整旧事实按当前业务规则验收一次，复用同一 Arrow 表写 staging。staging 和正式整表仅检查物理契约、行数及逐值内容，不再次执行业务校验或通过 Pandas/IPC 往返生成摘要。正式验收在共享事务内，成功退出后才报告 metadata_upgraded。

整根备份/隔离位于本批恢复目录的 `_root`；共享模块负责安装与恢复，保留失败新根，恢复不完整另保留旧备份，staging 清理。''')
flow('a04-b02-flow-upgrade-fact-metadata', '旧版本事实迁移', '''A["兼容旧版本；完整业务验收一次"] --> B["复用 Arrow 写 staging；物理与逐值复读"]
B --> C["共享事务安装整根"]
C --> D["事务内正式整表物理及逐值复读"]
D --> E["成功退出；报告迁移已提交"]
C -. 失败 .-> R["共享恢复；保留失败新根；抛错"]
D -. 失败 .-> R''')
doc('a04-b02-doc-commit-complete-fact-partition', '''## 事实 dirty 完整叶与新标记共同提交

完整候选叶只转换一次 Arrow、验收一次业务，随后复用排序后的表写 staging 并逐值比较。staging 只包含当前叶和空标记；正式复读直接打开当前叶，使用 `partition_base_dir` 补回 Hive 列，不扫描正式表根的其他月份。空结果显式删除旧叶并复读零行标记；新建标记单独确认零行，既有标记保持原样。

当前叶与必要的新标记共用一个事务；物理契约和完整内容比较在事务内，成功退出才报告事实提交。正式复读所得 Arrow 表转为 Pandas 返回，供事实计数和叶映射复用。日历随后独立提交，失败不撤销已成功事实。失败新叶隔离保留，恢复不完整保留旧备份，staging 清理且不递归删除表根。''')
flow('a04-b02-flow-commit-complete-fact-partition', '事实叶一次业务验收及共享提交', '''A["dirty 叶转 Arrow；业务验收一次"] --> B["复用 Arrow 写 staging；物理与逐值比较"]
B --> C["共享事务：必要时新建标记；替换或显式删除叶"]
C --> D["事务内直读当前叶或空标记；物理与逐值比较"]
D --> E["成功退出；返回正式叶与提交日志；日历随后回写"]
C -. 失败 .-> R["恢复当前叶与新标记；保留失败证据；停止"]
D -. 失败 .-> R''')
for key in ('49ae48fd', 'a04-b02-doc-apply-calendar-failure'):
    # 去掉与旧完整业务复验不再相符的段落，保留状态语义。
    title = '生成完成状态' if key == '49ae48fd' else '生成失败状态'
    meaning = '正式复读计数为 1 时记 success/passed，为 0 时记 empty_confirmed/warning；原因逐格点覆盖。' if key == '49ae48fd' else '只允许 retryable_error/permanent_error，清空完成凭证，计数归零、quality_status=failed；不伪装确认空。'
    doc(key, f'''## {title}

只复制并更新调用方提供的当前日历完整叶。{meaning} 未触达行保持原样。

生成函数报告扫描/更新数量和 persisted=false，不在返回前再转 Arrow 或完整业务校验；这些责任集中到随后日历叶提交前。''')
flow('a04-b02-flow-40c35598', '生成当前叶完成状态', '''A["当前日历叶和正式复读格点计数"] --> B["原因覆盖及 0/1 计数门禁"]
B --> C["更新触达行成功或确认空；保留其他行"]
C --> D["返回当前叶；尚未提交"]''')
flow('a04-b02-flow-apply-calendar-failure', '生成当前叶失败状态', '''A["当前日历叶和失败格点"] --> B["检查失败类别；清除触达行完成凭证"]
B --> C["返回当前叶；生成完成不代表落盘"]''')
doc('a04-b02-doc-commit-calendar-partition', '''## 日历 dirty 完整叶独立提交

先选取当前完整叶，再执行一次 Arrow 转换和业务验收，不对传入的全历史日历验收。main 已从循环前建立的日历叶映射传入当前叶；同月先前修复的状态由映射保存并继承。

staging 只写当前叶，不另写未使用的日历根标记。staging 与正式复读直接打开该叶，检查物理、表身份、版本及完整内容，不重复业务验收或扫描其他分区。目标叶必须已经由 b01 创建，既有根标记与其他数据集保持原样。

正式复读在当前叶的共享事务内，成功退出才返回已提交叶并报告 calendar_state=committed；完成与否仍以每个格点字段为准。失败状态保存不是采集完成，也无独立日期水位。事务失败只恢复当前日历叶，已成功事实保留，下一次可无 API 修复。''')
flow('a04-b02-flow-commit-calendar-partition', '当前日历叶独立提交', '''A["选当前叶；转 Arrow 并业务验收一次"] --> B["写 staging 当前叶；物理与逐值复读"]
B --> C["共享事务；确认目标叶存在并替换"]
C --> D["直读当前正式叶；物理与逐值比较"]
D --> E["成功退出；返回已提交叶；完成按格点字段"]
C -. 失败 .-> R["恢复当前叶；此前成功事实保留；抛错"]
D -. 失败 .-> R''')
doc('48b4a6ce', '''## CLI：一次规划、分区映射与逐叶推进

参数门禁、来源错误分类、按月串行请求及独立提交顺序保持不变。启动读取两表并对账一次，随后按分区构造事实/日历叶映射及契约空事实表；当前月份只消费所属叶，不再筛选、拼接或完整校验累计历史。

先逐叶执行无 API 修复，把提交函数返回的正式日历叶写回映射，再消费原先互不相交的 API 待办；同月采集会继承已经修复的状态。成功或失败状态均只更新当前叶，成功提交后更新映射。批末复用已验收叶和完成格点计数，不再从两个表根重新物化并求差。只读模式不累计正式提交凭证。

main 保留批次、参数、月份、跳过及最终结果日志；来源读取、生成和提交进度由对应函数负责。来源失败仍继续后续月份，批末失败；提交或恢复异常立即传播，既有成功叶保留。''')
flow('a04-b02-flow-f90d35b3', '一次规划与分区推进', '''A["参数门禁；可信两表读取；必要的旧版本迁移"] --> B["启动一次对账；预建分区映射和空表"]
B --> C["逐叶无 API 修复；保存正式叶返回值"]
C --> D{"有 API 待办？"}
D -->|否| E["不创建客户端；结束"]
D -->|是| F["遍历待办月份；从映射取当前事实和日历叶"]
F --> G["请求、转换和完整叶合并"]
G --> H{"write？"}
H -->|否| J["下一月；只读结束"]
H -->|是| K["先事实提交并核对计数，再日历提交"]
K --> L["更新已提交叶映射及完整格点数"]
L --> M["批末复用证据；不重读全历史"]
G -. 来源失败 .-> R["仅当前日历叶回写失败；继续；批末抛错"]
K -. 提交失败 .-> X["共享恢复；立即停止"]''')
# 日历上游保证改为局部存在门禁：不保留没有动作的阶段赋值。
c = cells['a04-b02-code-commit-calendar-partition']
c.source = c.source.replace('        log_phase = "upstream_contract"\n', '')
nbformat.validate(nb)
PATH.write_text(nbformat.writes(nb), encoding='utf8', newline='\n')

boundary = ('a04/b02 启动时只做一次事实格点计数与日历完成状态对账；当前版本正式历史信任生产者业务证明，'
            '只确认物理结构、表名、主键、分区及契约版本。纯描述性 metadata 差异不触发重写；'
            '兼容旧事实版本仍只在无日期写入模式执行一次完整业务验收并整根迁移。'
            '来源转换结果与待提交 dirty 完整叶各做一次业务验收，staging 和正式路径仅物理及逐值复读，'
            '正式验收仍在共享事务内。主循环复用预先分组的事实/日历叶、列序、排序键、文件 Schema 和请求字段；'
            '每月只合并和更新当前叶，日历修复与本月采集继承同一叶映射，不重新校验累计整表。'
            '提交直接复读当前叶，不逐分区扫描正式表根；修复后和批末复用已验收证据，不再整表重读重算。'
            '来源门禁、精确待办、事实先提交与日历后回写、确认空和失败状态语义不变。')
for rel in ('02_Futures_Lakehouse/AGENTS.md', '02_Futures_Lakehouse/README.md'):
    path = ROOT / rel
    text = path.read_text(encoding='utf8')
    anchor = '共享模块只负责路径安装和恢复，不改变本环节的采集范围、来源门禁或 metadata 迁移判定。'
    assert anchor in text
    text = text.replace(anchor, '共享模块只负责路径安装和恢复。' + boundary)
    path.write_text(text, encoding='utf8', newline='\n')

path = ROOT / 'config/data_contracts.py'
text = path.read_text(encoding='utf8')
old = '完整 year/month 事实叶与 interest_rate/year/month 日历叶在 staging 和正式路径均须通过精确 Schema/metadata 与逐值复读，事实正式复读后才能推进日历。'
new = ('来源转换结果与待提交的 dirty 完整叶分别执行业务验收一次；当前契约正式历史信任生产者业务证明。'
       '完整 year/month 事实叶与 interest_rate/year/month 日历叶在 staging 和正式路径均须通过物理字段、类型、nullable、表名、主键、分区、契约版本及逐值复读；'
       '描述性 metadata 差异使用当前契约，不触发历史重写。正式验收在事务内执行，事实正式复读后才能推进日历；'
       '启动一次事实计数与日历状态对账，后续只处理当前完整叶并复用已提交证据。')
assert text.count(old) == 1
path.write_text(text.replace(old, new), encoding='utf8', newline='\n')

# 原先测试用的摘要算法保留在测试中，与生产新的 Arrow 逐值比较相互独立。
path = ROOT / '00_draft_collection_02/tests/test_b04_c02_interest_rate.py'
text = path.read_text(encoding='utf8')
text = text.replace('import importlib.util', 'import hashlib\nimport importlib.util', 1)
text = text.replace('C02.table_digest(', 'frame_digest(')
anchor = '\n\nclass FakeTushareClient:'
digest = '''

def frame_digest(frame, schema, primary_key):
    ordered_df = frame.sort_values(primary_key).reset_index(drop=True)
    table = C02.pandas_to_arrow(ordered_df.loc[:, schema.names], schema)
    stable_table = pa.Table.from_pylist(table.to_pylist(), schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        writer.write_table(stable_table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()
'''
assert anchor in text
text = text.replace(anchor, digest + anchor, 1)
path.write_text(text, encoding='utf8', newline='\n')

path = ROOT / '00_draft_collection_02/tests/test_a04_b02_shared_transaction.py'
text = path.read_text(encoding='utf8')
text = text.replace('def opening(path, *args):', 'def opening(path, *args, **kwargs):')
text = text.replace('self.real_open(path, *args)', 'self.real_open(path, *args, **kwargs)')
text = text.replace('if path == target else dataset', 'if path.is_relative_to(target) else dataset')
text = text.replace('if path == self.fact_path:', 'if path.is_relative_to(self.fact_path):')
path.write_text(text, encoding='utf8', newline='\n')
