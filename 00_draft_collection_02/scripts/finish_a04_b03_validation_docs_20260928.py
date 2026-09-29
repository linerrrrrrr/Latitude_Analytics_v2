"""宏观发布第 7—9 项：同步当前校验责任、流程图和相关契约文字。"""
import ast
import pathlib
import nbformat

ROOT=pathlib.Path(__file__).resolve().parents[2]
path=ROOT/'02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb'
nb=nbformat.read(path,4); cells={c.id:c for c in nb.cells}

def doc(cell,text):cells[cell].source=text.strip()+'\n'
def flow(cell,title,graph):doc(cell,'### 流程：'+title+'\n\n```mermaid\nflowchart TD\n'+graph.strip()+'\n```')

doc('18615d3c','''## 可信历史的物理结构、表身份与版本

日历及事实均在一次 fragment 遍历中确认物理字段、类型、nullable、表名、主键、分区和契约版本；描述性 metadata 使用当前 Schema，不触发历史重写。当前版本历史信任生产者已完成的业务验收。日历旧版本先由 b01 迁移；兼容旧事实版本仅在无日期写入模式经过一次当前业务验收后整根迁移。''')
doc('48d2f1c3','''## 生产者质量责任与读取边界

来源转换结果和待提交 dirty 完整叶各承担一次业务验收。事实 value 的空值与布尔值门禁保留在 Arrow 转换前，避免布尔值被转换为浮点数；来源列、日期、频率、范围、唯一性、有限数及偏移规则均保留。

可信历史读取不重做业务校验；staging 和正式复读只验物理契约并逐值比较。日历 dirty 叶的验证直接消费已经转换的 Arrow，不把全表来回转换。''')
doc('1c280f89','''## 一次求差与无 API 修复

启动保留一次事实计数、日历完成凭证及 available_date 对账，并拒绝事实越出全部 required 水位。之后把事实和日历按完整叶分组，修复和采集都只使用当前叶。修复提交后直接更新叶映射，不再全表重读重算；相同月份的后续报告继承此前已提交的事实及日历状态。''')
doc('0370ee69','''## dirty 完整事实叶与现有恢复

每个报告窗口从叶映射取得当前月事实，替换触达键并保留其他报告。提交前仅对合并后的 dirty 完整叶验收一次，复用 Arrow 写 staging，再物理及逐值复读。正式读取直接定位当前叶，不遍历其他月份。空叶仍删除旧内容并复读零行标记。

兼容旧版本迁移保持整根操作；普通提交仍保持本次叶和必要的新标记共同恢复，日历随后独立提交。共享事务尚未接入，原手写移动、恢复及清理策略保留。''')
doc('ceea72e1','''## 当前日历叶的状态与提交

状态生成只接收当前完整日历叶。正式事实 1 行对应 success/passed，完整来源且正式复读 0 行对应 empty_confirmed/warning，来源失败保持未完成。只在提交前对 dirty 日历叶执行业务验收一次，staging 和正式路径直读当前叶并逐值比较；根级标记与其他叶保持原样。''')
doc('755be747','''## CLI：一次对账、逐报告叶处理和提交证据

先检查日期及正式湖写入边界，读取两表、按需迁移兼容旧事实版本，并对账一次。分区映射和空事实表在循环前准备。无 API 修复先更新对应已提交日历叶；没有 API 待办则不创建会话。

每个年月—报告窗口使用当前事实和日历叶，来源失败可回写本叶失败状态并继续，批末汇总失败；提交异常立即停止。事实先提交并核对触达计数，日历随后独立提交，两者成功后更新叶映射与完成计数。同月不同报告不会覆盖此前结果。只读模式仍请求和转换但不提交。

成功批末复用各叶正式验收结果，不重读全历史或重验累计事实。读取、分页、生成、校验及提交日志继续由函数负责，main 保留参数、模式、失败分类和批次汇总；没有独立日期水位文件。''')
c=cells['2512abcb']
old='事实越出全部 required 水位或可用日与上游不一致时停止，不静默删改。事实目前仍把任意 metadata 差异视为迁移条件；无日期写入时整根升级。这是后续待收缩的旧实现，不是新增规范。'
new='启动保留一次事实计数、日历凭证和可用日对账，事实越出 required 或 available_date 不等于上游时停止。当前版本历史信任生产者证明，描述性 metadata 不重写历史；只有物理与身份兼容的旧事实版本在无日期写入时整根迁移。来源输出与每次待提交 dirty 完整叶各验收一次，staging/正式复读仅物理及逐值检查。'
assert old in c.source;c.source=c.source.replace(old,new)

notes={
'open_compatible_dataset':'支持表根、当前叶及零行标记文件。一次 fragment 遍历同时确认物理结构、表身份和版本；分区根以字符串传给 PyArrow 补回 Hive 列。描述性差异不影响兼容，不另扫 metadata。',
'open_exact_dataset':'要求前一函数返回当前版本；表身份或物理结构错误仍拒绝。普通叶提交传入当前叶和 partition_base_dir，不打开整个正式表根。',
'validate_macro_calendar_table':'输入为已转换的 Arrow 日历叶，只做本次输出的业务验收；窄主键用于唯一性检查，返回排序后的 Arrow 表。不重转完整 Pandas 日历。',
'validate_macro_release_frame':'保留转换前的空值、布尔值和数值转换门禁，只转一次 Arrow；以窄主键检查唯一性，逐行确认系列、来源、有限性、日期和时间，返回排序后的 Arrow。来源输出及 dirty 叶分别调用一次，物理复读不调用。',
'read_macro_calendar':'确认物理、身份与当前版本后，只物化 macro_release 行并按权威契约转 Pandas；信任上游已完成的业务验证。',
'read_optional_fact':'无 Parquet 返回权威空表；有文件则物理兼容读取并按权威 Schema 转 Pandas。返回版本匹配标志，纯描述差异不触发迁移；本函数不写湖、不重验完整业务。',
'normalize_macro_release_response':'保留完整来源质量门禁；报告频率在来源行循环前确认。只逐个生成精确待办结果，不再从同一待办重建集合后重复核对覆盖；非空来源输出一次业务验收并返回事实及 0/1 预期。',
'full_fact_partition':'调用方传入已分组的当前月事实叶，保留未触达键并追加本次非空事实；不筛选或拼接历史全表，也不在此重复完整业务验收。写入时由提交函数验收 dirty 完整叶。',
'write_fact_staging':'复用已经验收的 Arrow 表及模块级文件 Schema，写零行标记与非空叶；没有重复 Pandas 转换。staging 完成仍不等于正式提交。',
'upgrade_fact_metadata':'兼容旧事实版本一次业务验收，复用 Arrow 写 staging，整根 staging 和正式复读仅做物理契约及排序后逐值比较。原整根恢复和清理不变；成功后报告迁移已提交。',
'commit_complete_fact_partition':'dirty 完整叶一次业务验收，复用 Arrow 暂存；正式路径只读当前叶或零行标记，物理契约和全部值必须一致。新建标记另确认零行。仍用原手写恢复，验收及清理后才报告事实提交；日历后续独立提交。',
'apply_calendar_completion':'只复制并修改当前日历叶，保留其他报告状态；生成后不重复业务验收，提交函数承担 dirty 叶验收。内存完成状态仍 persisted=false。',
'apply_calendar_failure':'只修改当前叶的触达格点；不重验全部日历。仍保持未完成及失败原因，后续提交负责验收，保存失败状态不代表采集完成。',
'commit_calendar_partition':'先确认当前上游叶存在，只对 dirty 日历叶业务验收一次；staging 与正式路径直读该叶，物理与逐值一致后完成。复用相对路径，不写新日历根标记，不扫描正式表根。原恢复与清理保持；成功只证明叶状态已保存。',
}
for name,text in notes.items():
    cid='a04-b03-doc-'+name
    doc(cid,cells[cid].source.splitlines()[0]+'\n\n`'+name+'`：'+text)
flow('a04-b03-flow-overview','宏观事实与日历完成凭证','''A["读可信日历和事实；物理、身份与版本检查"] --> B["启动一次：计数、状态和可用日共同对账"]
B --> C["准备叶映射；已有事实无 API 修复日历"]
C --> D{"还有 API 待办？"}
D -->|否| Z["提前结束；不创建会话"]
D -->|是| E["按报告与年月严格分页；来源输出验收一次"]
E --> F["合并当前完整月叶；保留其他报告"]
F --> G{"启用 write？"}
G -->|否| H["只读转换结果"]
G -->|是| I["dirty 事实叶验收一次；安装并逐值复读"]
I --> J["依据正式计数生成当前日历叶；验收并独立提交"]
J --> K["更新叶映射和完成数；同月后续报告继承"]
K --> L["批末复用正式提交证据；不重扫历史"]
E -. 来源失败 .-> R["当前叶保存未完成状态；继续；批末失败"]
I -. 提交失败 .-> X["原手写恢复；停止；此前成功叶保留"]
J -. 提交失败 .-> X''')
graphs={
'open_compatible_dataset':('一次物理与身份读取','A["发现当前路径文件；打开 Hive Dataset"] --> B["检查整体结构、表身份和版本"]\nB --> C["一次遍历 fragment：物理、身份、版本"]\nC --> D["返回 Dataset 与当前版本标志"]'),
'read_macro_calendar':('可信上游日历','A["物理、身份和版本确认"] --> B["只物化 macro_release 权威列"]\nB --> C["契约转 Pandas；返回可信日历"]'),
'read_optional_fact':('可信现有事实','A{"存在 Parquet？"} -->|否| B["权威空表"]\nA -->|是| C["兼容读取并物化事实"]\nC --> D["契约转 Pandas；返回事实及版本标志"]'),
'validate_macro_release_frame':('事实一次业务验收','A["空值、布尔与数值转换门禁"] --> B["一次 Arrow 转换；检查主键"]\nB --> C["逐行检查系列、来源、有限值、日期与时间"]\nC --> D["返回排序后的已验收 Arrow 表"]'),
'normalize_macro_release_response':('来源与精确待办转换','A["确认报告字段和频率"] --> B["来源日期、唯一性和数值门禁；应用偏移"]\nB --> C["逐个精确待办：有效值写事实；缺值记 0"]\nC --> D["非空输出一次业务验收；返回事实与计数"]'),
'write_fact_staging':('复用 Arrow 暂存','A["创建 staging；复用文件 Schema 写零行标记"] --> B{"Arrow 表非空？"}\nB -->|是| C["直接写当前年月叶"]\nB -->|否| D["保留可读零行 staging"]'),
'upgrade_fact_metadata':('旧版本事实整根迁移','A["旧事实一次业务验收；复用 Arrow 暂存"] --> B["物理及逐值复读"]\nB --> C["原手写备份与安装整个根"]\nC --> D["正式整表物理及逐值复读"]\nD --> E["清理后报告提交"]\nC -. 失败 .-> R["原手写恢复；保留既有失败边界"]\nD -. 失败 .-> R'),
'commit_complete_fact_partition':('当前事实叶验收与提交','A["dirty 完整叶业务验收一次"] --> B["复用 Arrow 暂存；物理及逐值比较"]\nB --> C["原安装：当前叶及必要新标记；空叶删除"]\nC --> D["只复读当前叶或零行标记"]\nD --> E["物理及逐值一致；清理后报告提交"]\nC -. 失败 .-> R["原手写恢复当前叶与新标记"]\nD -. 失败 .-> R'),
'apply_calendar_failure':('当前叶生成失败状态','A["复制当前完整日历叶"] --> B["只修改触达格点：未完成、失败状态与审计字段"]\nB --> C["返回内存叶；由提交函数验收"]'),
'commit_calendar_partition':('当前日历叶验收与提交','A["确认上游叶存在；dirty 叶一次业务验收"] --> B["暂存当前叶；物理及逐值比较"]\nB --> C["原手写备份和安装"]\nC --> D["只复读当前正式叶；物理及逐值比较"]\nD --> E["清理后报告状态已保存"]\nC -. 失败 .-> R["原手写恢复当前日历叶"]\nD -. 失败 .-> R'),
}
for name,(title,graph) in graphs.items():flow('a04-b03-flow-'+name,title,graph)
flow('a04-b03-flow-dbe3f417','日历 dirty 叶业务验收','A["已转换的 Arrow 日历叶"] --> B["主键、系列与逐行状态业务检查"]\nB --> C["返回排序后的 Arrow 表"]')
flow('a04-b03-flow-359e5a66','当前完整事实叶合并','A["已分组的当前月事实叶"] --> B["检查触达范围；删除触达旧键"]\nB --> C["追加新事实；保留其他报告"]\nC --> D["返回完整叶；提交时一次业务验收"]')
flow('a04-b03-flow-6d6af383','当前叶生成完成状态','A["当前日历叶与正式 0/1 计数"] --> B["只更新触达格点和审计字段"]\nB --> C["返回内存状态；尚未提交"]')
flow('a04-b03-flow-4bc93daf','一次规划与当前叶处理','A["参数门禁；读取、按需迁移及一次规划"] --> B["建立叶映射；无 API 修复更新当前日历叶"]\nB --> C{"有 API 待办？"}\nC -->|否| Z["结束；不创建会话"]\nC -->|是| D["逐报告窗口分页、转换与当前叶合并"]\nD --> E{"write？"}\nE -->|否| F["只读；继续窗口"]\nE -->|是| G["事实提交及计数核对；日历独立提交"]\nG --> H["更新叶映射与完成数；后续报告继承"]\nH --> I["批末复用证据；关闭会话"]\nD -. 来源失败 .-> R["保存当前叶失败状态；继续；批末抛错"]\nG -. 提交失败 .-> X["原恢复；立即停止；关闭会话"]')

# 去掉已无对应工作的日志阶段。
for c in nb.cells:
    if c.cell_type!='code':continue
    if 'def full_fact_partition(' in c.source:c.source=c.source.replace('log_phase = "validate_output"','log_phase = "merge_ready"')
    if 'def apply_calendar_' in c.source:c.source=c.source.replace('log_phase = "validate_output"','log_phase = "state_ready"')
    if 'def normalize_macro_release_response(' in c.source:c.source=c.source.replace('        log_phase = "coverage"\n','')
    if 'def read_optional_fact(' in c.source:c.source=c.source.replace('        log_phase = "current_contract"\n','')
    if 'def open_compatible_dataset(' in c.source:
        lines=c.source.splitlines(keepends=True)
        for node in sorted([x for x in ast.walk(ast.parse(c.source)) if isinstance(x,ast.Expr) and isinstance(x.value,ast.Call) and ast.unparse(x.value.func)=='click.echo' and 'phase=metadata;' in ast.unparse(x)],key=lambda x:x.lineno,reverse=True):lines[node.lineno-1:node.end_lineno]=[]
        c.source=''.join(lines).replace('        log_phase = "metadata"\n','')
nbformat.validate(nb);path.write_text(nbformat.writes(nb),encoding='utf8',newline='\n')

boundary='''a04/b03 启动只进行一次事实计数、日历完成状态及可用日对账。当前版本正式历史信任生产者业务证明，读取只检查物理结构、表身份及契约版本；描述性 metadata 差异不重写历史，兼容旧事实版本仍限无日期写入时经一次业务验收后整根迁移。来源输出和每次待提交 dirty 完整叶各验收一次，staging 与正式路径仅物理及逐值复读。循环前分组事实与日历，逐报告只合并、更新和复读当前完整叶，同月后续报告及无 API 修复共用已提交叶映射；修复后和批末不再全表复验。严格分页、原列和数值偏移、月末/季末归一、上游可用日、确认空、失败分类和事实先于日历的独立提交保持不变。当前仍使用原手写恢复，未接入共享安装模块。'''
p=ROOT/'02_Futures_Lakehouse/AGENTS.md';s=p.read_text(encoding='utf8')
old='a04/b02 事实验收按本文件对应环节的单次业务校验与叶级复读规则执行，a04/b03 保留现有事实验收规则。'
assert old in s;s=s.replace(old,'a04/b02、b03 事实验收按各自单次业务校验与叶级复读规则执行。')
anchor='- `a01/b08` 的全量审计边界'
assert anchor in s;s=s.replace(anchor,'- '+boundary+'\n'+anchor,1)
p.write_text(s,encoding='utf8',newline='\n')
p=ROOT/'02_Futures_Lakehouse/README.md';s=p.read_text(encoding='utf8')
anchor='至此 19 个正式采集入口均已迁移完成。';assert anchor in s
p.write_text(s.replace(anchor,anchor+'\n\n'+boundary,1),encoding='utf8',newline='\n')
p=ROOT/'03_Futures_Database/AGENTS.md';s=p.read_text(encoding='utf8')
old='在 staging 与正式路径通过精确 Schema/metadata 和逐值复读，事实正式复读成功后才能推进日历完成状态。'
new='在 staging 与正式路径通过物理字段、类型、nullable、表名、主键、分区、契约版本及逐值复读，事实正式复读成功后才能推进日历完成状态。\n当前版本历史信任生产者业务证明；来源输出与每次 dirty 完整叶分别执行业务验收一次。描述性 metadata 差异不重写历史，\n兼容旧事实版本仍限无日期写入时验收并迁移。启动一次共同对账后，只处理当前叶并继承同月已提交结果，修复后和批末不重读全历史。'
assert s.count(old)==1;p.write_text(s.replace(old,new),encoding='utf8',newline='\n')
p=ROOT/'config/data_contracts.py';s=p.read_text(encoding='utf8')
old='完整 year/month 事实叶与 macro_release/year/month 日历叶在 staging 和正式路径均须通过精确 Schema/metadata 与逐值复读，事实正式复读后才能推进日历。'
new='来源输出和每次待提交 dirty 完整叶分别执行业务验收一次；当前版本正式历史信任生产者业务证明。完整 year/month 事实叶与 macro_release/year/month 日历叶在 staging 和正式路径须通过物理字段、类型、nullable、表名、主键、分区、契约版本及逐值复读；描述性 metadata 差异使用当前契约，不触发历史重写。事实正式复读后才能推进日历；启动一次计数、状态和可用日对账，后续只处理当前完整叶并复用已提交证据。'
assert s.count(old)==1;p.write_text(s.replace(old,new),encoding='utf8',newline='\n')
print('Notebook descriptions, flowcharts and affected contract text synchronized')
