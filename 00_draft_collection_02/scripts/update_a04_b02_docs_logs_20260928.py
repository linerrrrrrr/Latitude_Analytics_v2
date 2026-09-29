"""a04/b02 第 1—4 项：重排解释、绘制现有流程、统一 main 日志。"""
import ast
import copy
import json
import pathlib
import re
import shutil
import tempfile
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b02_interest_rate.ipynb')
path = ROOT / REL
nb = nbformat.read(path, 4)
before = copy.deepcopy(nb)
assert not any(c.id.startswith('a04-b02-') for c in nb.cells)
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a04-b02-docs-logs-before-'))
for relative in (REL, REL.with_suffix('.py')):
    saved = snapshot / relative
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, saved)
cells = {c.id: c for c in nb.cells}

cells['1193f280'].source = '''# b02 SHIBOR 期限利率

本入口生产 `fact_interest_rate_daily`，读取 a04/b01 已提交的 `dim_macro_release_calendar`，只消费 `dataset_name=interest_rate` 的 required 系列—观测日格点。事实完整性与日历状态共同决定是否需要采集；它不自行生成交易日历或扩大上游水位。

| 上下游或依赖 | 与本环节的关系 |
| --- | --- |
| a04/b01 宏观发布日历 | 提供 8 个 SHIBOR 期限的理论格点、项目可用日和调度状态。 |
| 共享宏观配置 | `config/futures_lakehouse/macro_release_entities.py` 唯一定义期限与 Tushare 原列映射。 |
| 权威数据契约 | `config/data_contracts.py` 定义日历与事实的字段、主键、分区和质量边界。 |
| Tushare Pro | 有实际 API 待办时才创建客户端，按年月窗口调用 `pro.shibor`，只接纳精确待办格点。 |
| 现有正式事实 | 为完整格点和无 API 日历修复提供证据；同月未触达事实原样保留。 |
| 日历状态回写 | 事实正式复读后，回写完整 `interest_rate/year/month` 叶，供下次运行求差及上游继承。 |
| a04/b03 宏观事实 | 使用同一日历的 `macro_release` 叶；本入口不处理它的格点或事实。 |
| operations / 数据库读取 Demo | 默认阶段顺序为 a04/b01 → b02 → b03；Demo 只读消费正式表。 |

事实与日历依次提交，不是一个跨表事务。日历回写失败不会撤销已经成功的事实；后续人工再次运行时，已有事实可用于无 API 状态修复。
'''
cells['d8b916dd'].source = '''## 自动范围、来源结果与写入边界

| 模式或结果 | 当前行为 |
| --- | --- |
| 默认自动范围 | 从上游全部 required 格点减去事实与日历共同完整的格点；空事实表和内部缺口走同一流程。 |
| 成对显式日期 | 只筛选指定范围内的 required 格点；正式湖禁止带日期写入，非正式湖可用于范围检查。 |
| 不带 `--write` | 仍读取本地表；有 API 待办仍会请求并转换响应，但不提交事实、状态修复或失败状态。 |
| 已有事实、日历陈旧 | 写入模式先无 API 修复日历；只读模式只报告修复计划。没有 API 待办时不创建客户端。 |
| 有效值 | 保留来源百分比年利率，不除以 100；只接纳精确待办期限—日期。 |
| 精确格点没有值 | 完整响应中的缺日或缺值形成 0 行预期；正式事实复读为 0 后才写 `empty_confirmed + warning`。 |
| 来源结构或数值不合法 | 拒绝本月转换；写入模式记录未完成的失败状态，不伪装为确认空。 |

每个待办月份调用一次范围请求，边界取该月待办观测日的最小和最大值。来源日期唯一、范围、必需列、现有 2000 行上限，以及非空值不能为布尔值/无穷值/非数值、必须在 `[-100, 100]` 内的门禁继续保留。

当前请求或转换失败会记录本月失败，继续后续月份，批末汇总抛错；不会自动重试失败月份。安装或正式验收异常直接传播并停止本次执行，此前成功叶保留。`retryable_error` 是状态分类，不代表本次会重试。

正式事实越出当前上游 required 范围时直接停止，不静默删除。事实仍将任意 metadata 差异视为旧契约并在无日期写入时整根升级；这尚未收缩为“纯描述性差异不重写历史”，后续步骤需处理，不能把现状当作新的规范。

相关材料：[湖仓规则](../AGENTS.md)、[湖仓说明](../README.md)、[数据库规则](../../03_Futures_Database/AGENTS.md)。
'''
cells['d737eb22'].source = '''## Schema 契约与有界本地样例

只在交互内核且未定义 `__file__` 时，按依赖顺序展示宏观发布日历和 SHIBOR 事实的权威 Schema。已经传入 `lake_root`，显式选择样例会有界读取本地湖；不调用来源 API、不写湖，也不维护第二份契约。
'''
cells['106d39c5'].source = '''## 表身份、来源映射与质量状态

日历和事实各自的表名、主键、分区从具名 Schema metadata 读取一次。事实按 `series_code/observation_date` 唯一、按 `year/month` 分区；日历按 `dataset_name/series_code/report_date` 唯一，回写完整 `interest_rate/year/month` 叶。

从共享配置选择且确认恰好 8 个 SHIBOR 系列，建立来源列双向映射及显式请求字段。成功、确认空和无 API 修复的原因文本也是当前完成判定的一部分；状态日志不能替代这些持久字段。
'''

# 描述及流程均按当前函数行为编写，不预先描述后续重构结果。
specs = {
'reconstructed_schema': ('Hive 字段与完整 Schema 重建', '按权威顺序从 Dataset 取字段，组合文件与 Hive 分区字段并保留 Dataset metadata；缺字段报错，不读取记录。', 'A["Dataset 与权威列序"] --> B["依次取字段；补回 Hive 列"]\nB --> C["保留 Dataset metadata；返回 Schema"]\nB -. 缺字段 .-> X["报错"]'),
'physical_schema_matches': ('物理字段兼容性', '只比较字段名及顺序、类型和 nullable，返回布尔值，不判断 metadata 或业务状态。', 'A["实际和期望 Schema"] --> B{"名称、顺序、类型及 nullable 一致？"}\nB -->|是| T["True"]\nB -->|否| F["False"]'),
'dataset_has_exact_schema_metadata': ('日历与事实的 metadata 边界', '宏观日历逐 fragment 检查物理字段、表名、主键、分区和当前契约版本，容忍描述性差异；旧版本必须先由 b01 迁移。SHIBOR 事实目前仍要求表级及字段级 metadata 全部精确一致。', 'A["Dataset 与每个 fragment"] --> B{"宏观日历 Schema？"}\nB -->|是| C["检查物理结构、身份及当前版本；容忍描述差异"]\nB -->|否| D["事实：检查全部 Schema 与 metadata"]\nC --> E["返回是否全部匹配"]\nD --> E'),
'open_compatible_dataset': ('打开物理兼容的 Dataset', '发现 Parquet 后打开 Hive Dataset，检查 Dataset 和全部 fragment 的物理结构，再判断 metadata 是否匹配。返回 Dataset 和匹配标志，此时尚未物化记录；当前仍有两轮 fragment 检查。', 'A["发现 Parquet 文件"] --> B["打开 Hive Dataset"]\nB --> C["检查 Dataset 与全部 fragment 物理结构"]\nC --> D["检查 metadata；返回 Dataset 和标志"]\nA -. 无文件 .-> X["报错"]\nC -. 不兼容 .-> X'),
'open_exact_dataset': ('按当前契约打开 Dataset', '复用兼容读取并要求 metadata 匹配标志为真。日历和事实的“匹配”分别服从前一函数的边界，不表示两者采用相同的描述性 metadata 策略。', 'A["兼容读取 Dataset 和匹配标志"] --> B{"匹配当前契约？"}\nB -->|是| C["返回 Dataset；尚未读记录"]\nB -->|否| X["拒绝读取"]'),
'partition_expression': ('完整叶过滤表达式', '按权威分区列与分区键构造相等条件的交集；长度不匹配或空键报错。它只决定 Dataset 读取过滤条件，不改变业务待办。', 'A["分区列与分区键"] --> B["逐列相等条件取交集"]\nB --> C{"非空表达式？"}\nC -->|是| D["返回 Arrow Expression"]\nC -->|否| X["报错"]'),
'validate_interest_calendar_table': ('当前日历校验', '当前先执行 Arrow 契约转换，再检查主键、interest_rate 类型、系列、年月、0/1 计数、updated_at 与完成状态组合。它不复算 b01 的可用日规则，但仍重复检查部分上游已保证的业务条件；本轮不删除这些检查。返回排序后的 Pandas 表。', 'A["Arrow 契约与 Pandas 转换"] --> B["主键、数据集与系列检查"]\nB --> C["逐行检查年月、计数、时间和完成关系"]\nC --> D["主键排序；返回日历"]'),
'validate_interest_rate_frame': ('当前事实质量校验', '当前按权威列序转换 Arrow、重复验契约并转回 Pandas，检查主键、8 期限、来源标签、有限值、百分比范围、年月和 updated_at。来源转换、完整叶合并及多次复读仍复用此函数，重复调用在后续步骤收缩。', 'A["按权威列序转换并验契约"] --> B["主键与系列、来源检查"]\nB --> C["有限数、百分比范围、年月与时间检查"]\nC --> D["排序返回事实表"]'),
'read_interest_calendar': ('读取正式上游日历', '按当前日历契约打开 Dataset，只物化 interest_rate 行，再执行现有日历校验。缺少正式上游直接报错，b02 不创建日历根。', 'A["打开正式宏观日历"] --> B["过滤 interest_rate；物化权威列"]\nB --> C["当前日历校验；返回结果"]'),
'read_optional_fact': ('读取现有事实与迁移标志', '没有 Parquet 时返回权威空表；否则确认物理兼容、读取全部事实，在内存附着当前 Schema 后校验。返回事实及 metadata 匹配标志；附着当前 metadata 本身没有改写磁盘。', 'A{"已有事实 Parquet？"} -->|否| B["返回权威空表及匹配标志"]\nA -->|是| C["兼容读取；物化全部事实"]\nC --> D["内存附着当前 Schema；业务校验"]\nD --> E["返回事实及迁移判定标志"]'),
'table_digest': ('完整内容摘要', '权威字段按主键排序后转 Arrow，再从标量重建稳定缓冲区、写 IPC 流并求 SHA-256。摘要包含全部业务和审计字段，用于完整叶及迁移整表的逐值比较。', 'A["权威字段按主键排序"] --> B["转 Arrow；标量重建稳定表示"]\nB --> C["IPC 流求 SHA-256"]'),
'fact_grid_count_map': ('正式事实格点计数', '按事实主键统计每个期限—观测日的行数，供求差及提交后检查使用；空事实返回空映射。', 'A{"事实为空？"} -->|是| B["空计数映射"]\nA -->|否| C["按主键分组计数"]\nC --> D["返回格点到行数的映射"]'),
'calendar_grid_is_complete': ('单格点完成凭证', 'required、实际计数、批次与审计时间必须齐全。1 行事实匹配 success/passed 及认可原因，0 行匹配 empty_confirmed/warning 及确认空原因；其余计数报错。此函数不请求 API、不写状态。', 'A["required、实际计数与审计证据"] --> B{"事实计数？"}\nB -->|1| C["核对 success、passed、非缺失及认可原因"]\nB -->|0| D["核对 empty_confirmed、warning、缺失及确认空原因"]\nB -->|其他| X["报错"]\nC --> E["返回是否完整"]\nD --> E'),
'plan_interest_rate_grids': ('自动求差与无 API 修复计划', '从全部 required 格点选择当前范围，并拒绝事实越出全部上游 required 水位。逐格点结合事实计数与日历凭证分为已完整、已有 1 行事实但需修复日历、需要 API 三类。返回计划，不写入；显式日期不豁免范围外事实越界检查。', 'A["选择 required 及当前日期范围"] --> B["事实键不得越出全部 required 水位"]\nB --> C["建立事实计数；逐格点判断"]\nC --> D{"日历与事实共同完整？"}\nD -->|是| E["统计完整格点"]\nD -->|否| F{"已有 1 行事实？"}\nF -->|是| G["无 API 日历修复"]\nF -->|否| H["API 待办"]\nE --> Z["返回三类计划"]\nG --> Z\nH --> Z'),
'ShiborRequestError': ('请求失败分类', '异常保存结果状态和原因，供 main 回写失败日历。retryable_error 只是可由后续人工重跑处理的分类，不实现自动重试。', 'A["请求失败状态与原因"] --> B["构造 ShiborRequestError"]\nB --> C["交由 main 记录；不自动重试"]'),
'create_tushare_client': ('按需创建 Tushare 客户端', '仅在仍有 API 待办时调用。读取项目设置中的 token 创建客户端；缺少 token 抛出 permanent_error。本单元格只定义函数，不在定义时认证。', 'A["main 确认有 API 待办"] --> B{"已配置 token？"}\nB -->|否| X["抛出 permanent_error"]\nB -->|是| C["创建 Tushare Pro 客户端"]'),
'query_shibor_window': ('单个月度窗口请求', '显式传入月内日期边界和 8 期限字段，调用一次 shibor；异常按现有提示分类并保留原因链。None 或非 DataFrame 不属于成功响应。完整列、日期和数值质量由下一步转换检查。', 'A["日期范围与显式字段"] --> B["一次 pro.shibor 请求"]\nB --> C{"返回 DataFrame？"}\nC -->|是| D["返回原始宽表"]\nC -->|否| X["按现有规则抛出请求异常"]\nB -. 请求异常 .-> X'),
'normalize_shibor_response': ('来源宽表到精确待办事实', '先检查必需列、行数上限、日期唯一及范围，再校验所有来源期限的非空数值。之后只逐个接纳精确待办：有效值形成事实，缺日或 None/pd.NA/NaN 形成 0 行预期。转换结果检查与待办覆盖检查仍保留；0 行预期此时尚不是已落盘的确认空。', 'A["检查列、行数、日期与来源数值"] --> B["来源按日期建索引"]\nB --> C["逐个精确待办映射期限列"]\nC --> D{"有有效值？"}\nD -->|是| E["保留百分比值；生成 1 行事实"]\nD -->|否| F["记录 0 行预期；尚未确认空"]\nE --> G["事实校验与待办覆盖检查"]\nF --> G\nG --> H["返回事实及格点预期计数"]'),
'full_fact_partition': ('合并完整事实月分区', '取出旧年月完整叶，检查触达日期所属月份；删除旧叶内本次触达的精确主键后，追加本次非空事实。同月未触达行保留；合并后仍执行现有完整业务校验。', 'A["选择旧年月完整叶"] --> B["检查触达格点所属月份"]\nB --> C["移除旧触达键；追加本次非空事实"]\nC --> D["空表或完整叶校验；返回合并结果"]'),
'write_fact_staging': ('暂存事实与零行契约标记', '创建本批 staging，先写去除 Hive 列的 schema.parquet，再转换并写入非空事实。即使完整叶为零行，staging 也可按权威契约复读；本函数不安装正式路径。', 'A["创建 staging"] --> B["写零行 schema.parquet"]\nB --> C["事实转换为 Arrow"]\nC --> D{"非空？"}\nD -->|是| E["按年月写 Parquet"]\nD -->|否| F["保留可读零行 staging"]'),
'upgrade_fact_metadata': ('现有事实整根迁移与手写恢复', '只由无日期写入模式触发。完整事实经 staging 校验与内容摘要核对后，备份旧根、安装新根、正式整表复读并逐值比较。当前仍是手写恢复，成功恢复会清理失败隔离目录；恢复不完整保留现场。纯描述 metadata 触发迁移的现存边界留待后续处理。', 'A["完整事实校验；整根 staging"] --> B["staging 校验及完整摘要"]\nB --> C["记录实际移动；备份旧根并安装新根"]\nC --> D["正式整表校验与摘要一致"]\nD --> E["清理；返回正式事实"]\nC -. 异常 .-> R["手写恢复旧根；完整则清理，不完整保留现场并抛错"]\nD -. 异常 .-> R\nA -. 暂存失败 .-> X["清理 staging；抛错"]\nB -. 失败 .-> X'),
'commit_complete_fact_partition': ('当前事实完整叶提交', '输入完整月叶再次校验后暂存并逐值复读；必要时安装新根标记，备份旧叶，非空结果安装新叶，空结果不安装叶即表达删除。正式表根按当前契约打开后只物化目标叶，校验摘要再返回。当前手写恢复只覆盖本次事实叶和新标记，日历尚未回写；恢复完整清理隔离目录，恢复不完整保留现场。', 'A["完整事实叶校验；staging 写入和逐值复读"] --> B["必要时安装根标记；备份旧叶"]\nB --> C{"完整叶非空？"}\nC -->|是| D["安装新叶"]\nC -->|否| E["不安装新叶；删除旧内容"]\nD --> F["正式契约检查；目标叶复读与摘要一致"]\nE --> F\nF --> G["清理；返回正式叶；尚未回写日历"]\nB -. 异常 .-> R["手写恢复本叶与新标记；失败抛出"]\nD -. 异常 .-> R\nF -. 异常 .-> R\nA -. 失败 .-> X["清理 staging；抛错"]'),
'apply_calendar_completion': ('生成完成状态', '输入正式复读的格点计数和逐格点原因。当前复制并遍历全部 interest_rate 日历，只修改触达格点：1 行写 success/passed，0 行写 empty_confirmed/warning，并记录批次、审计时间和 updated_at；最后校验完整日历。这里只生成内存状态，不证明日历已落盘。', 'A["核对计数与原因键集合"] --> B["复制并遍历日历；只修改触达格点"]\nB --> C{"正式复读计数？"}\nC -->|1| D["success、passed、非缺失"]\nC -->|0| E["empty_confirmed、warning、缺失"]\nC -->|其他| X["报错"]\nD --> F["写批次及时间；校验并返回内存日历"]\nE --> F'),
'apply_calendar_failure': ('生成失败状态', '只接收 retryable_error 或 permanent_error。当前复制并遍历完整日历，对失败格点写未完成、0 计数、非确认缺失、failed 质量与原因、批次和检查时间；随后校验。失败状态是否落盘仍取决于 write 及后续日历提交。', 'A["检查失败状态枚举"] --> B["复制日历；修改失败格点"]\nB --> C["未完成、计数 0、质量 failed；记录原因和批次"]\nC --> D["校验后返回内存日历；未落盘"]'),
'commit_calendar_partition': ('当前日历完整叶回写', '当前先转换和校验整份 interest_rate 日历，再选择非空目标年月叶。上游日历必须已由 b01 提交，本函数不创建或迁移日历根。目标叶经 staging 校验、手写备份/安装、正式复读及完整摘要比较后返回；本次失败恢复当前日历叶，不撤销此前成功的事实或其他日历叶。', 'A["校验完整日历；选非空目标叶"] --> B["确认上游正式日历存在且契约匹配"]\nB --> C["写 staging；目标叶校验及摘要"]\nC --> D["备份旧日历叶；安装新叶"]\nD --> E["正式目标叶复读；校验与摘要一致"]\nE --> F["清理；返回正式日历叶"]\nD -. 失败 .-> R["手写恢复当前日历叶；不撤销事实"]\nE -. 失败 .-> R\nC -. 失败 .-> X["清理 staging；抛错"]'),
}
original_docs = {
    '73a5f858':'a62a089c', 'af5c2739':'ab4baaf6', 'a6323e94':'bf61bfd6',
    'cb730c9e':'61aa2ae8', '45aafcf7':'dd29e6da', '40c35598':'49ae48fd',
}
extra_cells = {}
graphs = {}
for cell_id, doc_id in original_docs.items():
    cell = cells[cell_id]
    nodes = ast.parse(cell.source).body
    assert all(isinstance(n,(ast.FunctionDef,ast.ClassDef)) for n in nodes)
    lines = cell.source.splitlines(keepends=True)
    starts = [0] + [n.lineno - 1 for n in nodes[1:]] + [len(lines)]
    parts = [''.join(lines[a:b]).rstrip()+'\n' for a,b in zip(starts,starts[1:])]
    additions = []
    for index,(node,part) in enumerate(zip(nodes,parts,strict=True)):
        title, description, graph = specs[node.name]
        if index == 0:
            cells[doc_id].source = '## '+title+'\n\n'+description+'\n'
            cell.source = part
            graphs[cell_id] = (title,graph)
        else:
            short = node.name.replace('_','-')
            code = nbformat.v4.new_code_cell(part,id='a04-b02-code-'+short)
            doc = nbformat.v4.new_markdown_cell('## '+title+'\n\n'+description+'\n',id='a04-b02-doc-'+short)
            additions.extend([doc,code]); cells[code.id]=code
            graphs[code.id]=(title,graph)
    extra_cells[cell_id]=additions

cells['0e3603da'].source = cells['0e3603da'].source.replace('import sys\n','import sys\nimport time\n',1)
main = cells['f90d35b3'].source

def log(phase,status,fields='',event='planning_progress',indent=4,table='TABLE_NAME'):
    return ' '*indent+'click.echo(\n'+' '*(indent+4)+f'f"{event}: table={{{table}}}; function=main; phase={phase}; status={status}; "\n'+(' '*(indent+4)+f'f"{fields}; "\n' if fields else '')+' '*(indent+4)+'f"elapsed_s={time.perf_counter() - log_started_at:.3f}"\n'+' '*indent+')\n'

def replace(old,new):
    global main
    pattern=re.compile('^'+re.escape(old),re.MULTILINE)
    assert len(pattern.findall(main))==1,(old,len(pattern.findall(main)))
    main=pattern.sub(lambda match:new,main,count=1)

# 先替换既有日志，只保留事件格式变化；需要兼容的中文/旧提示放在 message 字段。
node = ast.parse(main).body[0]
echoes=[n for n in ast.walk(node) if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and ast.unparse(n.value.func)=='click.echo']
logs = {
'metadata_upgrade_required': ('metadata_plan','completed','rows={len(existing_fact_df)}; write={str(write).lower()}; persisted=false; message=metadata_upgrade_required', 'planning_progress'),
'metadata_upgraded': ('metadata_upgrade','completed','rows={len(existing_fact_df)}; persisted=true; message=metadata_upgraded', 'committed'),
'plan:': ('plan','completed','complete={complete_count}; state_repair={len(repair_df)}; api_pending={len(pending_df)}; write={str(write).lower()}; lake_root={resolved_lake_root}; persisted=false', 'reconciliation_plan'),
'state_repaired:': ('state_repair','completed','rows={len(repair_counts)}; remaining_api_pending={len(pending_df)}; persisted=true; message=state_repaired: rows={len(repair_counts)}', 'planning_progress'),
'dry_run_state_repair:': ('state_repair','skipped','rows={len(repair_counts)}; reason=dry_run; persisted=false', 'planning_progress'),
'complete: no_api_pending': ('run','completed','reason=no_api_pending; write={str(write).lower()}; state_repair_pending={len(repair_df)}; message=Tushare client not created', 'planning_progress'),
'api_failure:': ('api_window','failed','partition={partition_key}; result_status={failure_status}; grids={len(touched_grids)}; reason={failure_reason}; persisted=false; automatic_retry=false', 'planning_progress'),
'api_success:': ('normalize_merge','completed','partition={partition_key}; requested_grids={len(touched_grids)}; nonempty_grids={sum(expected_counts.values())}; complete_partition_rows={len(complete_partition_df)}; persisted=false', 'api_success'),
'dry_run_complete:': ('run','completed','write=false; persisted=false; message=API 响应已转换和质检，未写事实或日历', 'dry_run'),
'complete: formal_reconciled': ('run','completed','formal_reconciled={final_complete_count}; fact_rows={len(final_fact_df)}; write=true; date_watermark=none', 'planning_progress'),
}
lines=main.splitlines(keepends=True)
for n in sorted(echoes,key=lambda n:n.lineno,reverse=True):
    original=''.join(lines[n.lineno-1:n.end_lineno])
    matches=[key for key in logs if key in original]
    assert len(matches)==1,original
    phase,status,fields,event=logs[matches[0]]
    table='CALENDAR_TABLE_NAME' if phase=='state_repair' else 'TABLE_NAME'
    lines[n.lineno-1:n.end_lineno]=[log(phase,status,fields,event,n.col_offset,table)]
main=''.join(lines)

replace('    calendar_df = read_interest_calendar(calendar_path)',
    log('parameters','completed','mode={\'explicit\' if has_explicit_dates else \'automatic\'}; start_date={requested_start_date}; end_date={requested_end_date}; lake_root={resolved_lake_root}')+
    '    log_phase = "read_calendar"\n'+log('read_calendar','started',table='CALENDAR_TABLE_NAME')+
    '    calendar_df = read_interest_calendar(calendar_path)\n'+log('read_calendar','completed','rows={len(calendar_df)}',table='CALENDAR_TABLE_NAME'))
replace('    existing_fact_df, fact_metadata_is_exact = read_optional_fact(fact_path)',
    '    log_phase = "read_fact"\n'+log('read_fact','started')+'    existing_fact_df, fact_metadata_is_exact = read_optional_fact(fact_path)\n'+log('read_fact','completed','rows={len(existing_fact_df)}; metadata_is_exact={str(fact_metadata_is_exact).lower()}'))
replace('            existing_fact_df = upgrade_fact_metadata(',
    '            log_phase = "metadata_upgrade"\n'+log('metadata_upgrade','started','rows={len(existing_fact_df)}; persisted=false',indent=12)+'            existing_fact_df = upgrade_fact_metadata(')
replace('    pending_df, repair_df, complete_count = plan_interest_rate_grids(',
    '    log_phase = "plan"\n'+log('plan','started')+'    pending_df, repair_df, complete_count = plan_interest_rate_grids(')
replace('    if not repair_df.empty:', '    log_phase = "state_repair"\n    if not repair_df.empty:')
replace('            repair_run_id = uuid.uuid4().hex',log('state_repair','started','rows={len(repair_counts)}; persisted=false',indent=12,table='CALENDAR_TABLE_NAME')+'            repair_run_id = uuid.uuid4().hex')
replace('                commit_calendar_partition(\n                    calendar_df,\n                    resolved_lake_root,\n                    (DATASET_NAME, int(year), int(month)),\n                )\n            calendar_df = read_interest_calendar(calendar_path)',
    '                log_phase = "repair_calendar_commit"\n'+log('calendar_commit','started','partition={(DATASET_NAME, int(year), int(month))}; mode=state_repair; persisted=false',indent=16,table='CALENDAR_TABLE_NAME')+
    '                commit_calendar_partition(\n                    calendar_df,\n                    resolved_lake_root,\n                    (DATASET_NAME, int(year), int(month)),\n                )\n'+log('calendar_commit','completed','partition={(DATASET_NAME, int(year), int(month))}; mode=state_repair; persisted=true; date_watermark=none','committed',16,'CALENDAR_TABLE_NAME')+'            calendar_df = read_interest_calendar(calendar_path)')
replace('            calendar_df = read_interest_calendar(calendar_path)',
    '            log_phase = "repair_readback_plan"\n'+log('repair_readback_plan','started',indent=12)+'            calendar_df = read_interest_calendar(calendar_path)')
replace('    client = create_tushare_client()',
    '    log_phase = "create_client"\n'+log('create_client','started')+'    client = create_tushare_client()\n'+log('create_client','completed'))
replace('        partition_key = (int(year), int(month))',
    '        log_phase = "prepare_window"\n        partition_key = (int(year), int(month))\n'+log('api_window','started','partition={partition_key}; pending_grids={len(partition_pending_df)}; persisted=false','partition_start',8))
replace('            raw_df = query_shibor_window(',
    '            log_phase = "request"\n'+log('request','started','partition={partition_key}; start_date={request_start_date}; end_date={request_end_date}; requested_grids={len(touched_grids)}','request_batch',12)+'            raw_df = query_shibor_window(')
replace('            incoming_fact_df, expected_counts = normalize_shibor_response(',
    log('request','completed','partition={partition_key}; response_rows={len(raw_df)}; persisted=false','api_result',12)+
    '            log_phase = "normalize"\n'+log('normalize','started','partition={partition_key}; response_rows={len(raw_df)}; persisted=false',indent=12)+'            incoming_fact_df, expected_counts = normalize_shibor_response(')
replace('        if failure_status is not None:',
    '        if failure_status is not None:')
replace('                calendar_df = apply_calendar_failure(',
    '                log_phase = "failure_state"\n'+log('failure_state','started','partition={partition_key}; grids={len(touched_grids)}; persisted=false',indent=16,table='CALENDAR_TABLE_NAME')+'                calendar_df = apply_calendar_failure(')
# 第二个相同调用此时只剩失败回写路径。
replace('                commit_calendar_partition(\n                    calendar_df,\n                    resolved_lake_root,\n                    (DATASET_NAME, int(year), int(month)),\n                )\n            continue',
    '                log_phase = "failure_calendar_commit"\n'+log('calendar_commit','started','partition={(DATASET_NAME, int(year), int(month))}; mode=failure_state; persisted=false',indent=16,table='CALENDAR_TABLE_NAME')+
    '                commit_calendar_partition(\n                    calendar_df,\n                    resolved_lake_root,\n                    (DATASET_NAME, int(year), int(month)),\n                )\n'+log('calendar_commit','completed','partition={(DATASET_NAME, int(year), int(month))}; calendar_state=failed; is_fetch_completed=false; persisted=true; date_watermark=none','committed',16,'CALENDAR_TABLE_NAME')+'            continue')
replace('        complete_partition_df = full_fact_partition(',
    '        log_phase = "merge_fact"\n'+log('merge_fact','started','partition={partition_key}; incoming_rows={len(incoming_fact_df)}; persisted=false',indent=8)+'        complete_partition_df = full_fact_partition(')
replace('        committed_partition_df = commit_complete_fact_partition(',
    '        log_phase = "fact_commit"\n'+log('fact_commit','started','partition={partition_key}; rows={len(complete_partition_df)}; persisted=false',indent=8)+'        committed_partition_df = commit_complete_fact_partition(')
replace('        committed_counts = fact_grid_count_map(committed_partition_df)',
    log('fact_commit','completed','partition={partition_key}; rows={len(committed_partition_df)}; persisted=true; calendar_state=pending','committed',8)+
    '        log_phase = "fact_count_check"\n        committed_counts = fact_grid_count_map(committed_partition_df)')
replace('        completion_reasons = {','        log_phase = "completion_state"\n        completion_reasons = {')
replace('        commit_calendar_partition(\n            calendar_df,\n            resolved_lake_root,\n            (DATASET_NAME, int(year), int(month)),\n        )',
    '        log_phase = "completion_calendar_commit"\n'+log('calendar_commit','started','partition={(DATASET_NAME, int(year), int(month))}; mode=completion; persisted=false',indent=8,table='CALENDAR_TABLE_NAME')+
    '        commit_calendar_partition(\n            calendar_df,\n            resolved_lake_root,\n            (DATASET_NAME, int(year), int(month)),\n        )\n'+log('calendar_commit','completed','partition={(DATASET_NAME, int(year), int(month))}; calendar_state=committed; persisted=true; date_watermark=none','committed',8,'CALENDAR_TABLE_NAME'))
replace('        outside_partition_df = existing_fact_df.loc[',
    '        log_phase = "accumulated_fact_validation"\n'+log('accumulated_fact_validation','started','partition={partition_key}',indent=8)+'        outside_partition_df = existing_fact_df.loc[')
replace('    if failed_window_count:',
    '    log_phase = "batch_result"\n    if failed_window_count:')
replace('    final_calendar_df = read_interest_calendar(calendar_path)',
    '    log_phase = "final_readback"\n'+log('final_readback','started')+'    final_calendar_df = read_interest_calendar(calendar_path)')
replace('    final_pending_df, final_repair_df, final_complete_count = (',
    log('final_readback','completed','calendar_rows={len(final_calendar_df)}; fact_rows={len(final_fact_df)}')+'    log_phase = "final_reconcile"\n'+log('final_reconcile','started')+'    final_pending_df, final_repair_df, final_complete_count = (')
node=ast.parse(main).body[0]; lines=main.splitlines(keepends=True); start=node.body[0].lineno-1
cells['f90d35b3'].source=''.join(lines[:start])+'    log_started_at = time.perf_counter()\n    log_phase = "parameters"\n    click.echo("=" * 80)\n'+log('run','started','write={str(write).lower()}')+'    try:\n'+textwrap.indent(''.join(lines[start:]),'    ')+'    except Exception as log_error:\n'+log('{log_phase}','failed','error={type(log_error).__name__}',indent=8)+'        raise\n    finally:\n        click.echo("=" * 80)\n'
ast.parse(cells['f90d35b3'].source)

cells['48b4a6ce'].source='''## CLI：求差、状态修复、月度采集与最终对账

main 在任何湖读取或 API 初始化前检查日期配对、顺序及正式湖写入限制。读取两表后识别事实 metadata，必要时在无日期写入模式整根迁移；求差之后先处理无 API 日历修复，修复后当前仍全量重读日历并重新规划。

有 API 待办才创建客户端，按年月请求、转换并合并完整事实叶。只读模式执行到转换/合并便进入下个月；写入模式依次提交事实、核对触达格点的正式计数、生成完成状态并提交日历。事实成功和日历成功分别报告，不能把请求成功当成落盘。

当前每个成功月后仍重建、校验累计事实全表，批末仍从两个正式路径重读再求差；这些现存重复工作留待第 7—9 项收缩。请求/转换失败按现有规则记录失败状态并继续下个月，最后汇总抛错；提交异常直接传播。

本轮统一 main 日志为事件前缀及 table/function/phase/status/elapsed_s，包含月份、格点与行数，首尾使用 80 个 `=`。阶段日志暂留在 main；第 5—6 项再将函数内细分进度与提交完成日志归位。生成和计划不报告持久化；正式提交函数成功返回后才报告对应表落盘，失败状态落盘明确标为未完成。日志不新增读表、摘要或业务校验。
'''
cells['2dfb140a'].source='''## 当前执行入口

当前执行格仍为 `if __name__ == "__main__": main()`。直接运行 `.py` 时 Click 读取终端参数；普通模块导入不执行入口。Notebook 内通常也使用 `__main__`，因此直接运行本格可能让 Click 读取内核的 `-f` 参数，并不等同于空参数只读模式。

显式 notebook_args 与交互内核保护将在第 11 项对齐，本轮保留执行代码，流程图按当前行为展示。
'''
graphs.update({
'0e3603da':('初始化与项目依赖','A["当前目录向上搜索项目标记"] --> B{"找到根目录？"}\nB -->|是| C["导入 Schema、宏观配置与设置"]\nB -->|否| X["报错"]\nC --> D["定义依赖；不调用 API、不写湖"]'),
'54f7f8e9':('Schema 浏览与样例','A{"交互内核且无脚本文件变量？"} -->|是| B["展示上游日历与本事实契约"]\nA -->|否| Z["跳过展示"]\nB --> C["显式选择样例时有界读取本地湖"]'),
'edfb8977':('表身份和来源映射','A["两张权威 Schema 与共享系列配置"] --> B["读取表名、主键、分区；构造 Hive 规则"]\nA --> C["选择 8 个 SHIBOR 系列；建立来源列映射"]\nC --> D["显式请求字段；质量范围与持久原因文本"]'),
'f90d35b3':('主流程与阶段日志','A["批次开始；参数与写入门禁"] --> B["读两表；识别并按模式迁移事实 metadata"]\nB --> C["共同证据求差；先处理无 API 修复"]\nC --> D{"存在 API 待办？"}\nD -->|否| Z["结束；不创建客户端"]\nD -->|是| E["创建客户端；逐月请求、转换与合并"]\nE --> F{"本月响应成功？"}\nF -->|否| G["记录失败；可写则提交失败日历；继续月份"]\nF -->|是| H{"启用 write？"}\nH -->|否| I["只读转换结果；继续月份"]\nH -->|是| J["提交事实并核对计数；随后提交日历"]\nJ --> K["更新并校验累计事实；继续月份"]\nG --> L["批末：有失败则抛错；只读则结束；写入则正式全量对账"]\nI --> L\nK --> L\nJ -. 提交异常 .-> X["记录阶段；立即抛错；此前成功叶保留"]\nL --> Z'),
'de9e1f3f':('当前执行入口','A{"模块名是主程序？"} -->|否| B["导入：不执行入口"]\nA -->|是| C["调用 main；Click 读取进程参数"]\nC --> D["终端按 CLI 执行；Notebook 可能读到内核参数"]'),
})
overview=nbformat.v4.new_markdown_cell('''## 总流程：SHIBOR 事实与日历完成凭证

```mermaid
flowchart TD
A["a04/b01 已提交 interest_rate 日历"] --> B["读取既有 SHIBOR 事实；确认契约"]
B --> C["required 格点与正式事实、日历状态共同求差"]
C --> D["完整格点跳过；已有事实但状态陈旧则无 API 修复"]
C --> E["事实缺口按月请求 Tushare；只转换精确待办"]
E --> F["请求和转换门禁；完整事实叶合并"]
F --> G{"启用 write？"}
G -->|否| H["只读结果；不写两表"]
G -->|是| I["事实 staging、安装与正式复读"]
I --> J["1 行成功或 0 行确认空；随后提交日历完整叶"]
D --> K["按 write 决定提交修复；无 API"]
J --> L["现有批末正式两表重读和对账"]
K --> L
E -. 请求或转换失败 .-> R["写入模式回写失败日历；继续其他月；批末抛错"]
I -. 安装或验收失败 .-> X["恢复当前目标；抛错停止；此前成功叶保留"]
J -. 日历提交失败 .-> X
J -. 下一次运行 .-> C
```
''',id='a04-b02-flow-overview')
# 修复分支不保证经过批末对账：无 API 待办时当前实现提前返回。
overview.source=overview.source.replace('K --> L','K --> M{"修复后还有 API 待办？"}\nM -->|否| N["提前结束；不创建客户端"]\nM -->|是| E')
expanded=[]
for cell in nb.cells:
    expanded.append(cell)
    if cell.id=='d8b916dd':
        expanded.append(overview)
        expanded.append(nbformat.v4.new_markdown_cell('## 初始化与依赖\n\n沿用项目根标记搜索，导入权威契约、配置和库。token 只由客户端函数按需读取，不在说明或日志中输出。\n',id='a04-b02-doc-imports'))
    expanded.extend(extra_cells.get(cell.id,[]))
nb.cells=[]
for cell in expanded:
    if cell.cell_type=='code':
        title,graph=graphs[cell.id]
        nb.cells.append(nbformat.v4.new_markdown_cell('### 流程：'+title+'\n\n```mermaid\nflowchart TD\n'+graph+'\n```\n',id='a04-b02-flow-'+cell.id.replace('a04-b02-code-','')))
    nb.cells.append(cell)
nbformat.validate(nb)
path.write_text(nbformat.writes(nb),encoding='utf8',newline='\n')
(snapshot/'scope.json').write_text(json.dumps({'target':str(REL),'flowcharts':len(graphs)+1,'original_cell_ids':[c.id for c in before.cells]},indent=2),encoding='utf8')
print(snapshot)
print('flowcharts:',len(graphs)+1)
