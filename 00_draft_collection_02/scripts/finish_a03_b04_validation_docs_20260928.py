"""将第 7—9 项的实际职责与读取范围同步到已有说明。"""
import pathlib
import nbformat

ROOT=pathlib.Path(__file__).resolve().parents[2]
PATH=ROOT/'02_Futures_Lakehouse/a03_External_Market_Data/b04_external_index.ipynb'
n=nbformat.read(PATH,4)
cells={c.id:c for c in n.cells}
descriptions={
'b03-c04-08':'''## 物理契约与表身份读取

`open_exact_dataset()` 检查 Dataset 和各 Parquet fragment 的字段、类型、nullable，以及表名、主键、分区身份 metadata。描述性 metadata 以当前 `config/data_contracts.py` 为准，不因说明差异重写历史。支持表根、当前叶和零行标记，叶读取通过 `partition_base_dir` 恢复 Hive 字段。

启动只读取一次正式日历与事实；提交时直读当前叶，空事实复读根级零行标记，不再逐叶枚举正式表根。函数报告结构、fragment 检查进度和失败阶段，`materialized=false` 表示尚未读取数据行。''',
'a03-b04-calendar-validation-text':'''## dirty 日历叶业务验收

`validate_calendar_table()` 消费调用方已按权威 Schema 转换的 Arrow 表，检查主键、状态、年月、原因、0/1 计数、required 关系和审计时间，排序后返回 Arrow 表。只在每个待提交完整日历叶执行一次；上游正式日历和内存状态生成不再重复业务验收。

原有循环仍报告扫描进度，每 10000 行检查一次 2 秒日志间隔。''',
'a03-b04-fact-validation-text':'''## 来源结果与 dirty 事实叶业务验收

`validate_external_index_table()` 接收已转换的 Arrow 表，检查主键、来源身份、共享代码/名称/分类映射、日期、年月、有限指数值和审计时间，排序后返回同一表示。用于来源转换结果和提交前完整 dirty 叶，分别保证来源输出和旧新合并结果的业务质量。

不在响应汇总、合并返回、staging 或正式复读时重复调用，不做 Pandas—Arrow 往返转换。当前日期在逐行循环前取得；扫描日志沿用原有迭代与节流。''',
'a03-b04-read-fact-text':'''## 可选正式事实读取

目录不存在或没有 Parquet 时返回权威空表；否则检查物理契约并一次物化、按主键排序，信任正式提交已完成的业务验证。启动所需的全历史事实计数和清退集合仍保留，批末不再重读全表。

函数区分空目录、实际物化、成功行数和异常。''',
'a03-b04-merge-text':'''## 当前完整事实叶合并

调用方传入当前分类—年月旧事实叶。函数保留未触达格点，删除本次采集和清退触达的旧行，再拼接已归一化来源结果；确认空与清退不造新行。结果为空时返回权威空表。

合并只报告 `persisted=false` 的内存结果，完整业务验收交给提交函数一次执行。主循环前建立事实叶映射，当前叶内不再筛选整张历史事实。''',
'b03-c04-14':'''## staging 事实叶过滤表达式

`fact_partition_expression()` 按权威分区列构造当前分类—年月过滤条件，只选择 staging 当前叶。正式复读直接打开目标叶或零行标记，不再通过表根过滤当前叶。''',
'a03-b04-fact-commit-text':'''## 当前事实叶与新增标记共同提交

完整 dirty 叶转换为 Arrow 并执行业务验收一次，确认内容没有越出指定分区。写 staging 和零行标记后只复读物理契约、排序并与期望 Arrow 表逐值比较；不再转换回 Pandas 做第二次业务校验。

每个事实叶使用一个 `StagedPathTransaction`。已有根标记保持原样；缺失标记与当前叶共同恢复。非空结果安装新叶，空结果显式删除旧叶。事务内直读正式叶，空结果确认叶不存在并复读零行标记；新增标记单独验收零行契约。完整逐值比较成功后才转换一次 Pandas 返回，供后续计数和日历回写。

首次备份失败保留原目标，失败按实际移动恢复；失败新叶隔离留存，新增标记回退移除，恢复不完整另保留旧备份，staging 清理。成功退出事务及当前清理后才报告 `partition_committed; calendar_state=not_updated`；此前成功叶保留。''',
'b03-c04-16':'''## 当前日历叶的完成与失败状态生成

`apply_calendar_completion()` 根据正式事实 0/1 计数生成成功或确认空状态；`apply_calendar_failure()` 仅更新失败请求段的精确待办格点，保留已有事实计数，记录未完成及失败原因。两者只修改传入的当前日历叶，返回 `persisted=false` 内存结果，不做完整业务复验；验收统一由 dirty 叶提交承担。

请求段失败不部分提交当前事实叶；此前成功叶保留。主循环维护已更新日历叶映射，因此多个分类在同月回写时保留彼此前序状态。进度计数复用原迭代，不增加扫描。''',
'a03-b04-calendar-commit-text':'''## 日历完整叶独立提交

精确触达格点必须与日历一一对应。循环前准备分区索引、列名、排序键和正式根路径，再取每个完整叶转换并业务验收一次。staging 直接写当前叶，不生成不会安装的临时零行标记；staging 和正式路径均直读当前叶，检查物理契约并逐值比较 Arrow 表。

每个日历叶独立使用共享事务，正式验收在事务内。此前成功事实和日历叶保留；已有日历根标记不替换。失败新叶隔离留存，恢复不完整另保留备份，staging 清理。日历失败不会撤销事实，下一次仍可能重新请求 API。

叶成功退出事务及当前清理后报告提交；全部触达叶成功后报告完成格点数和 `date_watermark=none`。错误状态落盘不表示采集完成。没有跨表原子可见性、强杀后的自动恢复或并发写入协调。''',
'b03-c04-18':'''## CLI：一次对账与当前叶推进

参数门禁后，只物化本数据集的正式日历并读取一次事实。启动仍按事实计数和日历状态共同判断完整格点，并生成采集与清退集合；原有完整性标准不变。循环前按分区建立事实和日历叶映射，有采集待办才创建 HTTP 会话。

每个事实叶只使用当前旧事实叶及对应同月日历叶，按 required 连续待办段请求并归一化。响应汇总与合并不重复业务验收，统一在来源输出和 dirty 叶提交承担；事实正式逐值复读后仍核对 API 计数和清退计数，随后生成并独立提交日历。各类别共享更新后的同月日历；不逐分区重建全表，也不在批末再次全表对账。

读取、查询、归一化、生成、提交与日历状态各自报告，main 负责批次及分区汇总；内存结果、事实提交、日历状态明确区分。函数日志记录调用耗时，main 记录整批耗时，保留 `=` 起止分隔线和异常原因链。''',
}
for key,value in descriptions.items(): cells[key].source=value
cells['a03-b04-query-text'].source+='\n\n日期过滤和请求列字符串在分页循环前准备；页号与每页响应元数据仍在循环内处理。'
cells['a03-b04-normalize-text'].source+='\n\n必需来源字段集合与当前可见日期在逐条循环前准备；转换后业务验收返回 Arrow，再转换一次 Pandas 供内存合并。'
cells['b03-c04-12'].source+='\n\n规划直接消费已契约化的日历记录，不为分类再次转成 Arrow；整批只执行一次，提交后的完成证据由各叶正式逐值复读及计数核对提供。'

def flow(title,body,overall=False):
    return ('## 总流程：' if overall else '### 局部流程：')+title+'\n\n```mermaid\n%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\nflowchart TD\n'+body+'\n```'
graphs={
'09':('物理契约与表身份','''A["表根、当前叶或零行标记"] --> B["打开 Dataset；恢复 Hive 字段"]
B --> C["字段、类型、nullable 及身份 metadata"]
C --> D["逐 fragment 物理检查；报告进度"]
D --> E["返回 Dataset；描述性差异采用当前契约"]'''),
'calendar-validation':('dirty 日历叶验收','''A["已转换的 Arrow 完整叶"] --> B["主键、枚举、日期与状态关系"]
B --> C["计数、完成凭证及审计时间"]
C --> D["排序并返回 Arrow；每次提交只验收一次"]'''),
'fact-validation':('来源及 dirty 事实叶验收','''A["已转换的 Arrow 表"] --> B["主键与共享身份映射"]
B --> C["来源、日期、年月、有限值及审计时间"]
C --> D["排序返回 Arrow；用于来源输出或 dirty 叶"]'''),
'read-fact':('可选事实读取','''A{"有正式 Parquet？"} -->|否| B["权威空表"]
A -->|是| C["物理契约检查；一次物化并排序"]
C --> D["信任已提交业务证明；返回事实供启动对账"]
B --> D'''),
'merge':('当前事实叶合并','''A["当前旧叶和已归一化响应"] --> B["保留未触达；移除采集与清退格点旧行"]
B --> C["拼接新行；全空则权威空表"]
C --> D["返回内存完整叶；提交函数业务验收"]'''),
'15':('staging 事实叶过滤','''A["当前分类年月和权威分区列"] --> B["构造 Arrow 相等条件"]
B --> C["用于 staging 当前叶；正式路径直接读叶"]'''),
'fact-commit':('当前事实叶事务','''A["dirty 完整叶转换；业务验收一次"] --> B["写 staging；物理契约和逐值复读"]
B --> C["共享事务：新增标记及当前叶替换或删除"]
C --> D["直读正式叶或零行标记；逐值比较"]
D --> E["成功退出及清理；事实已提交；日历未更新"]
C -. 失败 .-> R["按实际移动恢复；新增标记移除"]
D -. 失败 .-> R
R --> S["失败新叶隔离；恢复不全保留备份；抛错"]'''),
'17':('当前日历叶状态生成','''A{"正式计数或请求失败？"} -->|计数| B["按 0 或 1 生成确认空或成功"]
A -->|失败| C["保留当前叶事实计数；精确格点记未完成"]
B --> D["更新当前日历叶；保留同月其他指标"]
C --> D
D --> E["返回内存结果；dirty 叶提交统一验收"]'''),
'calendar-commit':('日历独立叶事务','''A["触达格点一一对应；循环前准备分组索引"] --> B["当前完整叶转换；业务验收一次"]
B --> C["staging 直读当前叶；物理和逐值比较"]
C --> D["共享事务内安装并直读正式叶；逐值比较"]
D --> E{"成功退出；还有叶？"}
E -->|是| B
E -->|否| F["报告已完成格点；无独立日期水位"]
D -. 失败 .-> R["恢复当前叶；此前成功叶保留；抛错"]'''),
'19':('一次规划与叶内推进','''A["参数门禁；启动读取并对账一次"] --> B["循环前建立事实和日历叶映射"]
B --> C["当前叶：按 required 连续段分页及归一化"]
C --> D{"启用 write？"}
D -->|否| E["累计内存结果"]
D -->|是| F["合并当前事实叶；业务校验及事务提交"]
F --> G["正式 API 计数和清退计数核对"]
G --> H["有采集才生成并独立提交当前日历叶"]
H --> I["更新叶映射；同月分类继承日历状态"]
I --> J{"还有事实叶？"}
E --> J
J -->|是| C
J -->|否| K["关闭会话；报告结果；不重扫全历史"]
C -. 失败 .-> R["按 write 回写精确失败格点；停止"]
F -. 失败 .-> S["恢复当前事务；停止"]
H -. 失败 .-> S'''),
}
for suffix,(title,body) in graphs.items(): cells['a03-b04-flow-'+suffix].source=flow(title,body)
overview=cells['a03-b04-flow-overview'].source
overview=overview.replace('合并分类年月计划；有采集才建 HTTP 会话','准备分类年月计划和叶映射；有采集才建会话').replace('关闭会话；write 时正式全表复验','关闭会话；完成证据来自当前叶验收')
cells['a03-b04-flow-overview'].source=overview
nbformat.validate(n)
PATH.write_text(nbformat.writes(n),encoding='utf8',newline='\n')
for relative in ('02_Futures_Lakehouse/AGENTS.md','02_Futures_Lakehouse/README.md'):
    path=ROOT/relative
    text=path.read_text(encoding='utf8')
    old='staging 和正式完整校验及逐值比较仍由本环节执行，正式验收在事务内完成。'
    assert text.count(old)==1
    text=text.replace(old,'来源输出及 dirty 完整叶分别承担一次业务验收，staging 与正式路径只检查物理契约并逐值比较，正式验收在事务内直读当前叶。启动保留一次事实计数与日历状态共同对账，主循环复用事实及日历叶映射，多个分类继承同月已提交状态；不逐分区重建整表或批末全表复验。描述性 metadata 以当前契约为准，不触发历史重写。')
    path.write_text(text,encoding='utf8',newline='\n')
