"""本轮一次性迁移：a04/b03 解释、现有流程图与 main 日志，不改变业务分支。"""
import ast
import copy
import json
import pathlib
import shutil
import tempfile
import textwrap

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
REL = pathlib.Path('02_Futures_Lakehouse/a04_Macro_And_Interest_Rates/b03_macro_release.ipynb')
path = ROOT / REL
nb = nbformat.read(path, 4)
before = copy.deepcopy(nb)
assert not any(c.id.startswith('a04-b03-') for c in nb.cells)
snapshot = pathlib.Path(tempfile.mkdtemp(prefix='a04-b03-docs-logs-before-'))
for relative in (REL, REL.with_suffix('.py')):
    saved = snapshot / relative
    saved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / relative, saved)
cells = {c.id: c for c in nb.cells}

cells['edbd3b24'].source = '''# b03 中国宏观发布事实

本入口生产 `fact_macro_release`，只消费 a04/b01 已正式提交的宏观发布日历中 `dataset_name=macro_release` 的 required 系列—报告期格点。它以事实内容与日历状态共同证明完成，不自行生成理论格点或扩大上游水位。

| 上下游或依赖 | 与本环节的关系 |
| --- | --- |
| a04/b01 宏观发布日历 | 提供理论报告期、项目可用日及采集状态；b03 的 `available_date` 只取 `expected_available_date`。 |
| 共享宏观配置 | `config/futures_lakehouse/macro_release_entities.py` 唯一定义 17 个系列、报告名、原列、频率及数值偏移。 |
| 权威数据契约 | `config/data_contracts.py` 定义事实及日历的字段、主键、分区和质量边界。 |
| Eastmoney 数据中心 | 按 CPI、PPI、PMI、GDP 报告名与年月窗口严格分页；只接纳精确待办系列。 |
| 现有正式事实 | 共同证明格点完成，并为陈旧日历提供无 API 修复依据；同月未触达事实保留。 |
| 日历状态回写 | 事实正式复读后独立提交完整 `macro_release/year/month` 叶，供下次求差及 b01 状态继承。 |
| a04/b02 SHIBOR | 使用同一日历的 `interest_rate` 叶；b03 不改写其格点或事实。 |
| operations 与读取 Demo | 默认顺序为 a04/b01 → b02 → b03；数据库 Demo 只读消费正式表。 |

事实与日历依次独立提交。日历失败不会撤销此前成功的事实；下次人工运行可从正式事实无 API 修复状态。本环节没有独立日期水位文件，完成证据保存在事实与日历中。
'''
cells['2512abcb'].source = '''## 自动范围、来源含义与写入边界

| 模式或结果 | 当前行为 |
| --- | --- |
| 默认自动范围 | 上游 required 格点减去事实与日历共同完整的格点；空湖和内部缺口走同一流程。 |
| 成对显式日期 | 筛选 required 报告期；正式湖禁止带日期写入，非正式湖可做范围检查。 |
| 不带 `--write` | 读取本地表；有 API 待办仍会请求、转换及合并，但不提交事实、修复状态或失败状态。 |
| 正式事实完整、日历陈旧 | 写入模式先修复日历并复读；只读模式只报告修复计划；没有 API 待办就不创建会话。 |
| 来源有有效值 | 依据共享原列和偏移生成精确待办事实；同月其他报告或已完成系列保留。 |
| 完整响应缺日或缺值 | 形成该格点 0 行预期；正式事实复读为 0 后才回写 `empty_confirmed + warning`。 |
| 分页、结构或数值异常 | 不把部分结果当成有效值或确认空；写入模式回写未完成的失败日历。 |

Eastmoney 的 `REPORT_DATE` 用报告月 1 日编码，不是发布日期。CPI/PPI/PMI 归一到月末；GDP 只接纳 3、6、9、12 月并归一到季末。PPI 同比读取 `BASE_SAME`；CPI 全国/城市/农村累计及 PPI 累计按共享配置减 100，转成累计同比百分比。项目可用日始终取上游日历。

每个报告窗口冻结首页 `pages/count`，检查后续页元数据、页长、累计行数、日期和跨页唯一性。首次明确空响应也要符合当前空响应契约。非空值不能为布尔值、非数值或非有限数；原列缺失不是确认空。

当前 HTTP 适配器保留 `Retry(total=3)` 的有界重试。请求函数最终抛出后，main 记录失败窗口并继续其他报告窗口，批末汇总抛错，不重新执行该业务窗口；`retryable_error` 是状态分类。安装或正式验收异常直接停止，之前成功叶保留。

事实越出全部 required 水位或可用日与上游不一致时停止，不静默删改。事实目前仍把任意 metadata 差异视为迁移条件；无日期写入时整根升级。这是后续待收缩的旧实现，不是新增规范。相关材料：[湖仓规则](../AGENTS.md)、[湖仓说明](../README.md)、[数据库规则](../../03_Futures_Database/AGENTS.md)。Notebook 为权威源，同名脚本由标准 PythonExporter 生成。
'''
cells['444f6cdb'].source = '''## Schema 契约与有界本地样例

仅在交互内核且没有 `__file__` 时，依次展示宏观日历与宏观事实的权威 Schema。这里已经传入 `lake_root`，显式选择数据样例会有界读取本地湖；不请求来源 API、不写湖，也不定义第二份契约。
'''
cells['0a1df430'].source = '''## 表身份、来源映射与状态原因

表名、主键与 Hive 分区从两张具名 Schema 读取一次。事实按 `series_code/report_date` 唯一、按 `year/month` 分区；日历回写完整 `macro_release/year/month` 叶。

配置先选择 17 个宏观系列，再按 4 个报告构造映射和请求字段，并检查原列及累计值偏移。成功、确认空与无 API 修复的原因文本参与完成凭证判定，日志不替代这些持久字段。
'''
cells['18615d3c'].source = '''## Dataset 的物理结构与 metadata

日历按上游已经采用的物理结构、表身份及当前契约版本读取，容忍描述性 metadata 差异。事实目前仍要求全部 metadata 精确一致；兼容旧事实只在无日期模式读入，写入时先整根迁移。下面展示当前读取步骤，重复 fragment 检查尚未收缩。
'''
cells['48d2f1c3'].source = '''## 当前业务校验与正式读取

事实生产者负责自己的来源、值、主键、分区与时间边界。当前日历读取仍重复检查部分上游已保证的主键与状态条件；来源转换、叶合并、staging、正式复读和累计事实也仍多次复用完整校验。本轮只整理说明与日志，这些重复工作留待后续逐项收缩。
'''
cells['1c280f89'].source = '''## 共同完成证据与无 API 修复

先检查全部事实没有越出当前 required 水位，且可用日逐值等于上游；显式日期只限制本次待办，不豁免这些已有检查。再把范围内格点分为已经完整、已有事实待修复日历、需要 API 三类。缺少事实但已有完整确认空证据的格点不再请求。
'''
cells['74ccf0c0'].source = '''## 报告窗口严格分页与精确格点转换

先完整取得一个报告窗口，再归一来源报告期、验证值并映射精确待办。任何分页不完整都不能进入事实提交；转换得到的 0/1 只是预期计数，仍需正式事实复读后才能推进日历。现有 HTTP 重试策略与失败分类如下，业务窗口不在 main 中重试。
'''
cells['0370ee69'].source = '''## 完整事实叶、旧 metadata 迁移与当前恢复路径

同月多个报告共用一个事实叶。每次仅替换本报告触达的精确键，保留其他报告和未触达系列；成功后 main 更新累计事实，后续报告继承先前提交结果。当前整根迁移和逐叶提交仍使用手写移动标记与恢复，共享事务接入留待后续。
'''
cells['ceea72e1'].source = '''## 日历状态生成与完整叶提交

正式事实复读 1 行写 `success + passed`；完整来源响应且正式复读 0 行才写 `empty_confirmed + warning`。失败状态保持 `is_fetch_completed=false`，保存失败日历不等于完成采集。同月后续报告继承当前日历状态，完整叶内未触达格点保留。
'''
cells['755be747'].source = '''## CLI：参数、求差、报告窗口与最终对账

main 先检查日期配对、顺序及正式湖写入边界，再读两表、判断旧事实 metadata 并规划。无 API 修复优先；写入修复后当前仍重读日历并重新规划。没有 API 待办时提前结束，不创建 Eastmoney 会话。

有待办时按年月和报告名顺序请求、转换及合并。只读模式止于内存结果；写入模式依次提交事实、核对触达计数、生成并提交日历，再重建校验累计事实。来源窗口失败可回写失败状态后继续，批末汇总失败；提交异常直接停止。会话在 finally 中关闭。正常写入批末目前仍重读两张正式表并再次对账。

本轮 main 日志统一为事件前缀及 `table/function/phase/status/elapsed_s`，报告年月、报告名、格点和行数，批次首尾为 80 个 `=`。请求完成、内存生成和正式落盘分别表达；日历失败状态落盘明确标为未完成。日志复用现有结果，不增加读取或校验。函数内逐页进度与日志归位属于后续第 5—6 项。
'''
cells['f55c4a11'].source = '''## 当前执行入口

当前仍是 `if __name__ == "__main__": main()`。脚本直接运行读取终端参数，普通导入不调用入口。Notebook 通常也处于 `__main__`，直接运行末格可能把内核 `-f` 参数交给 Click；不能称为显式空参数模式。

显式 `notebook_args` 与内核保护留待第 11 项，本轮保留原执行代码。
'''

# 复用已审阅的同语义描述，再逐项覆写宏观报告独有的处理边界。
prior = ast.parse((ROOT/'00_draft_collection_02/scripts/update_a04_b02_docs_logs_20260928.py').read_text(encoding='utf8'))
specs_node = next(n.value for n in prior.body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'specs' for t in n.targets))
specs = ast.literal_eval(specs_node)
rename = {'validate_interest_calendar_table':'validate_macro_calendar_table', 'validate_interest_rate_frame':'validate_macro_release_frame', 'read_interest_calendar':'read_macro_calendar', 'plan_interest_rate_grids':'plan_macro_release_grids'}
specs = {rename.get(k,k):tuple(v.replace('SHIBOR','宏观发布').replace('interest_rate','macro_release').replace('期限—观测日','系列—报告期').replace('b02 不创建','b03 不创建') for v in values) for k,values in specs.items() if k not in {'ShiborRequestError','create_tushare_client','query_shibor_window','normalize_shibor_response'}}
specs.update({
'validate_macro_release_frame': ('当前事实完整质量检查', '先检查 value 不为空、不为布尔值并转为有限浮点值，再按权威契约在 Pandas 与 Arrow 间转换。检查主键、共享系列、来源、可用日、年月和 UTC 审计时间；结果排序返回。该函数目前在多个边界重复调用，未在本轮收缩。', 'A["检查并转换 value"] --> B["按权威 Schema 转换与验契约"]\nB --> C["主键、系列、来源、日期和时间检查"]\nC --> D["排序返回事实"]'),
'plan_macro_release_grids': ('格点求差与无 API 修复计划', '全部事实键必须属于上游 required，available_date 必须等于 expected_available_date。再对当前范围结合计数、质量及审计凭证分类：完整跳过，已有 1 行但状态陈旧则修复，其余进入 API 待办。这里只返回计划。', 'A["选择 required 与当前日期范围"] --> B["全部事实不得越界；可用日须等于上游"]\nB --> C["逐格点核对正式计数与日历凭证"]\nC --> D{"共同完整？"}\nD -->|是| E["统计完整"]\nD -->|否| F{"已有 1 行事实？"}\nF -->|是| G["无 API 修复"]\nF -->|否| H["API 待办"]'),
'MacroReleaseRequestError': ('请求错误与持久状态分类', '保存 retryable_error 或 permanent_error 及原因。main 用于失败回写和批末汇总；该异常类本身不执行重试，也不写日历。', 'A["错误状态与原因"] --> B["构造请求异常"]\nB --> C["main 按原分支处理；不在类内重试"]'),
'create_eastmoney_session': ('按需创建 Eastmoney 会话', '只有 API 待办非空才调用。保留现有 HTTPAdapter 和 Retry(total=3)：连接、读取及指定 HTTP 错误受有界重试策略控制，并尊重 Retry-After。创建会话本身不发业务请求；main 负责关闭。', 'A["存在 API 待办"] --> B["创建 Requests Session"]\nB --> C["挂载现有 HTTP 有界重试策略与请求头"]\nC --> D["返回会话；main 最终关闭"]'),
'query_eastmoney_report_range': ('单报告窗口的严格分页', '从第 1 页串行请求。首次明确空响应按已有规则返回空列表；非空响应冻结 pages/count，逐页核对类型、总页数、页长和元数据，全部结束再核对累计行数。任一步失败抛分类异常，不返回部分分页。', 'A["报告字段与日期窗口；从第 1 页请求"] --> B["HTTP、JSON、success 和结构检查"]\nB --> C{"首个响应符合明确空契约？"}\nC -->|是| Z["返回空列表"]\nC -->|否| D["冻结或核对 pages/count；检查页长"]\nD --> E["累计当前页"]\nE --> F{"还有页？"}\nF -->|是| A\nF -->|否| G["累计行数等于 count；返回完整响应"]\nB -. 异常 .-> X["抛分类异常；不返回部分结果"]\nD -. 不一致 .-> X'),
'normalize_macro_release_response': ('来源报告期与精确待办事实', '校验来源对象、必需列、月初日期和请求范围；按频率归一月末或季末，拒绝跨页重复。按共享原列转换值并施加配置偏移，之后只处理精确待办，available_date 取上游。缺日/缺值形成 0 行预期，有效值生成事实；最后校验事实和覆盖。', 'A["来源列、月初日期、范围与频率检查"] --> B["归一月末或季末；拒绝重复"]\nB --> C["原列值检查；施加共享数值偏移"]\nC --> D["逐个精确待办取值"]\nD --> E{"有值？"}\nE -->|是| F["生成事实；可用日取上游"]\nE -->|否| G["0 行预期；尚未确认空"]\nF --> H["事实与待办覆盖检查；返回结果"]\nG --> H'),
})
# 日历类型与系列数量是本入口自己的描述，不能沿用利率边界。
title,_,graph = specs['validate_macro_calendar_table']
specs['validate_macro_calendar_table'] = (title, 'Arrow 契约转成 Pandas 后检查主键、macro_release 类型、17 个系列、年月、0/1 计数、updated_at 和状态组合。不重算 b01 的可用日规则，但仍重复部分上游业务检查。本轮保持现状，排序后返回。', graph)

def log(phase, status, fields='', event='planning_progress', indent=4, table='TABLE_NAME'):
    return (' '*indent+'click.echo(\n'+' '*(indent+4)+f'f"{event}: table={{{table}}}; function=main; phase={phase}; status={status}; "\n'
            +(' '*(indent+4)+f'f"{fields}; "\n' if fields else '')+' '*(indent+4)+'f"elapsed_s={time.perf_counter() - log_started_at:.3f}"\n'+' '*indent+')\n')

cells['83cfef17'].source = cells['83cfef17'].source.replace('import sys\n','import sys\nimport time\n',1)
main = cells['4bc93daf'].source
lines = main.splitlines(keepends=True)
node = ast.parse(main).body[0]
logs = {
'metadata_upgrade_required': ('metadata_plan','completed','rows={len(existing_fact_df)}; persisted=false; message=metadata_upgrade_required','planning_progress'),
'metadata_upgraded': ('metadata_upgrade','completed','rows={len(existing_fact_df)}; persisted=true; message=metadata_upgraded','committed'),
'plan:': ('plan','completed','complete={complete_count}; state_repair={len(repair_df)}; api_pending={len(pending_df)}; write={str(write).lower()}; lake_root={resolved_lake_root}','reconciliation_plan'),
'state_repaired:': ('state_repair','completed','rows={len(repair_counts)}; remaining_api_pending={len(pending_df)}; persisted=true; message=state_repaired: rows={len(repair_counts)}','planning_progress'),
'dry_run_state_repair:': ('state_repair','skipped','rows={len(repair_counts)}; persisted=false; reason=dry_run','planning_progress'),
'complete: no_api_pending': ('run','completed','reason=no_api_pending; write={str(write).lower()}; state_repair_pending={len(repair_df)}; message=Eastmoney session not created','planning_progress'),
'api_failure:': ('api_window','failed','partition={partition_key}; report={report_name}; result_status={failure_status}; grids={len(touched_grids)}; reason={failure_reason}; persisted=false; business_window_retry=false','planning_progress'),
'api_success:': ('normalize_merge','completed','partition={partition_key}; report={report_name}; requested_grids={len(touched_grids)}; nonempty_grids={sum(expected_counts.values())}; complete_partition_rows={len(complete_partition_df)}; persisted=false','api_success'),
'dry_run_complete:': ('run','completed','write=false; persisted=false; message=API 响应已转换和质检，未写事实或日历','dry_run'),
'complete: formal_reconciled': ('run','completed','formal_reconciled={final_complete_count}; fact_rows={len(final_fact_df)}; write=true; date_watermark=none','planning_progress'),
}
edits = []
for statement in ast.walk(node):
    if not isinstance(statement, (ast.Assign, ast.Expr)) or not isinstance(statement.value, ast.Call):
        continue
    call = statement.value
    name = ast.unparse(call.func)
    indent = statement.col_offset
    original = ''.join(lines[statement.lineno-1:statement.end_lineno])
    prefix = suffix = ''
    phase = None
    fields = ''
    table = 'TABLE_NAME'
    event = 'planning_progress'
    if name == 'click.echo':
        matches = [k for k in logs if k in original]
        assert len(matches) == 1, original
        phase,status,fields,event = logs[matches[0]]
        table = 'CALENDAR_TABLE_NAME' if phase == 'state_repair' else 'TABLE_NAME'
        edits.append((statement.lineno-1, statement.end_lineno, log(phase,status,fields,event,indent,table)))
        continue
    target = ast.unparse(statement.targets[0]) if isinstance(statement, ast.Assign) else ''
    if name == 'read_macro_calendar':
        phase,table = 'read_calendar','CALENDAR_TABLE_NAME'
        suffix = log(phase,'completed',f'rows={{len({target})}}',indent=indent,table=table)
    elif name == 'read_optional_fact':
        phase = 'read_fact'
        dfname, flagname = [ast.unparse(t) for t in statement.targets[0].elts]
        suffix = log(phase,'completed',f'rows={{len({dfname})}}; metadata_is_exact={{str({flagname}).lower()}}',indent=indent)
    elif name == 'upgrade_fact_metadata':
        phase,fields = 'metadata_upgrade','rows={len(existing_fact_df)}; persisted=false'
    elif name == 'plan_macro_release_grids':
        phase = 'plan'
    elif name == 'create_eastmoney_session':
        phase = 'create_session'
        suffix = log(phase,'completed','http_retry_total=3; business_window_retry=false',indent=indent)
    elif name == 'query_eastmoney_report_range':
        phase,event = 'request','request_batch'
        fields = 'partition={partition_key}; report={report_name}; start_date={request_start_date}; end_date={request_end_date}; requested_grids={len(touched_grids)}'
        suffix = log(phase,'completed','report={report_name}; response_rows={len(response_rows)}; persisted=false','api_result',indent)
    elif name == 'normalize_macro_release_response':
        phase,fields = 'normalize','report={report_name}; response_rows={len(response_rows)}; pending_grids={len(partition_pending_df)}; persisted=false'
        suffix = log(phase,'completed','report={report_name}; fact_rows={len(incoming_fact_df)}; persisted=false',indent=indent)
    elif name == 'full_fact_partition':
        phase,fields = 'merge_fact','partition={partition_key}; report={report_name}; incoming_rows={len(incoming_fact_df)}; persisted=false'
    elif name == 'commit_complete_fact_partition':
        phase,fields = 'fact_commit','partition={partition_key}; report={report_name}; rows={len(complete_partition_df)}; persisted=false'
        suffix = log(phase,'completed','partition={partition_key}; report={report_name}; rows={len(committed_partition_df)}; persisted=true; calendar_state=pending','committed',indent)
    elif name in {'apply_calendar_completion','apply_calendar_failure'}:
        phase = 'failure_state' if name.endswith('failure') else 'completion_state'
        table = 'CALENDAR_TABLE_NAME'
        fields = 'persisted=false'
        suffix = log(phase,'completed','persisted=false',indent=indent,table=table)
    elif name == 'commit_calendar_partition':
        phase,table = 'calendar_commit','CALENDAR_TABLE_NAME'
        mode = {16:'state_repair',20:'failure_state',12:'completion'}[indent]
        fields = f'partition={{(DATASET_NAME, int(year), int(month))}}; mode={mode}; persisted=false'
        outcome = 'calendar_state=failed; is_fetch_completed=false' if mode=='failure_state' else f'mode={mode}; calendar_state=committed'
        suffix = log(phase,'completed',f'partition={{(DATASET_NAME, int(year), int(month))}}; {outcome}; persisted=true; date_watermark=none','committed',indent,table)
    elif name == 'validate_macro_release_frame':
        phase = 'accumulated_fact_validation'
        fields = 'partition={partition_key}; report={report_name}'
        suffix = log(phase,'completed','rows={len(existing_fact_df)}',indent=indent)
    elif name == 'fact_grid_count_map':
        phase = 'fact_count_check'
    if phase is not None:
        prefix = ' '*indent+f'log_phase = "{phase}"\n'+log(phase,'started',fields,event,indent,table)
        edits.append((statement.lineno-1,statement.end_lineno,prefix+original.rstrip('\n')+'\n'+suffix))
for start,end,replacement in sorted(edits,reverse=True):
    lines[start:end] = [replacement]
main = ''.join(lines)
main = main.replace('    if failed_window_count:', '    log_phase = "batch_result"\n    if failed_window_count:', 1)
main = main.replace('    if not final_pending_df.empty or not final_repair_df.empty:', '    log_phase = "final_reconcile"\n    if not final_pending_df.empty or not final_repair_df.empty:', 1)
node = ast.parse(main).body[0]
lines = main.splitlines(keepends=True)
start = node.body[0].lineno-1
cells['4bc93daf'].source = (''.join(lines[:start])+'    log_started_at = time.perf_counter()\n    log_phase = "parameters"\n    click.echo("=" * 80)\n'
    +log('run','started','write={str(write).lower()}')+'    try:\n'+textwrap.indent(''.join(lines[start:]),'    ')
    +'    except Exception as log_error:\n'+log('{log_phase}','failed','error={type(log_error).__name__}',indent=8)+'        raise\n    finally:\n        click.echo("=" * 80)\n')
ast.parse(cells['4bc93daf'].source)

graphs = {
'83cfef17': ('初始化与项目依赖','A["当前目录向上搜索项目标记"] --> B{"找到项目根？"}\nB -->|是| C["导入契约、共享配置与设置"]\nB -->|否| X["报错"]\nC --> D["仅初始化；不请求来源、不写湖"]'),
'08ed5352': ('Schema 浏览与本地样例','A{"交互内核且无脚本文件变量？"} -->|是| B["展示日历与事实契约"]\nA -->|否| Z["跳过"]\nB --> C["显式选择样例后有界读本地湖"]'),
'166bdeb3': ('契约身份与共享映射','A["两张权威 Schema"] --> B["读取表名、主键和分区；构造 Hive 规则"]\nC["共享宏观系列"] --> D["选 17 系列、4 报告；建立字段映射"]\nD --> E["检查 PPI 原列和累计偏移；定义状态原因"]'),
'4bc93daf': ('main 与阶段日志','A["参数门禁；读两表；按模式迁移旧事实"] --> B["共同求差；优先处理无 API 修复"]\nB --> C{"还有 API 待办？"}\nC -->|否| Z["提前结束；不创建会话"]\nC -->|是| D["按年月与报告请求、转换、合并"]\nD --> E{"窗口成功？"}\nE -->|否| F["可写则提交失败日历；继续其他窗口"]\nE -->|是| G{"启用 write？"}\nG -->|否| H["保留内存结果；继续窗口"]\nG -->|是| I["事实提交和计数核对；日历随后提交"]\nI --> J["更新并校验累计事实；继续窗口"]\nF --> K["关闭会话；批末汇总失败或正常结束"]\nH --> K\nJ --> K\nK --> L["正常写入批末：正式两表重读并求差"]\nI -. 提交异常 .-> X["停止；此前成功叶保留；关闭会话"]'),
'14766fcd': ('当前执行格','A{"模块名为主程序？"} -->|否| B["普通导入：不执行入口"]\nA -->|是| C["main 读取进程参数"]\nC --> D["终端正常执行；Notebook 可能读到内核参数"]'),
}
overview = nbformat.v4.new_markdown_cell('''## 总流程：宏观事实与日历完成凭证

```mermaid
flowchart TD
A["b01 正式 macro_release 日历及现有事实"] --> B["required 与正式事实、日历状态共同求差"]
B --> C["完整跳过；已有事实但状态陈旧则无 API 修复"]
C --> D{"还有 API 待办？"}
D -->|否| Z["提前结束；不创建会话"]
D -->|是| E["逐个报告与年月严格分页"]
E --> F["报告期归一；共享偏移；精确待办转换"]
F --> G["合并完整事实月叶；保留同月其他报告"]
G --> H{"启用 write？"}
H -->|否| I["只读结果；不写两表"]
H -->|是| J["事实 staging、安装与正式复读"]
J --> K["核对 0/1；随后提交完整日历叶"]
K --> L["后续窗口继承状态；正常批末正式两表对账"]
E -. 请求失败 .-> R["可写则保存未完成状态；继续窗口；批末失败"]
F -. 转换失败 .-> R
J -. 提交失败 .-> X["恢复当前目标；停止；此前成功叶保留"]
K -. 日历失败 .-> X
```

无 API 修复仅在写入模式提交；只读模式报告修复计划。事实与日历是两次独立提交，图中的成功箭头不能解释成跨表共同回滚。
''',id='a04-b03-flow-overview')

expanded = []
for cell in nb.cells:
    if cell.cell_type != 'code' or cell.id in graphs:
        expanded.append(cell)
        if cell.id == '2512abcb':
            expanded.append(overview)
            expanded.append(nbformat.v4.new_markdown_cell('## 初始化与依赖\n\n沿用项目根标记搜索及标准库导入，随后定义函数。配置映射只有一份，来源请求由 main 的待办分支触发。',id='a04-b03-doc-imports'))
        continue
    source_lines = cell.source.splitlines(keepends=True)
    definitions = [n for n in ast.parse(cell.source).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))]
    assert len(definitions) >= 1
    for index,definition in enumerate(definitions):
        start = 0 if index == 0 else definition.lineno-1
        end = definitions[index+1].lineno-1 if index+1 < len(definitions) else len(source_lines)
        source = ''.join(source_lines[start:end]).rstrip()+'\n'
        title,description,graph = specs[definition.name]
        expanded.append(nbformat.v4.new_markdown_cell(f'### {title}\n\n`{definition.name}`：{description}\n', id='a04-b03-doc-'+definition.name))
        part = cell if index == 0 else nbformat.v4.new_code_cell(id='a04-b03-code-'+definition.name)
        part.source = source
        graphs[part.id] = (title,graph)
        expanded.append(part)
nb.cells = []
for cell in expanded:
    if cell.cell_type == 'code':
        title,graph = graphs[cell.id]
        nb.cells.append(nbformat.v4.new_markdown_cell('### 流程：'+title+'\n\n```mermaid\nflowchart TD\n'+graph+'\n```\n',id='a04-b03-flow-'+cell.id.replace('a04-b03-code-','')))
    nb.cells.append(cell)
nbformat.validate(nb)
path.write_text(nbformat.writes(nb),encoding='utf8',newline='\n')
(snapshot/'scope.json').write_text(json.dumps({'target':str(REL),'flowcharts':len(graphs)+1,'original_cell_ids':[c.id for c in before.cells]},indent=2),encoding='utf8')
print(snapshot)
print('flowcharts:',len(graphs)+1)
