"""b05 第 1—4 项：整理说明、增加流程图和统一入口日志，不执行业务。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b05_futures_daily.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}
assert not any(cell.id == 'b05-flow-overview' for cell in notebook.cells)

descriptions = {
    '32e0f0c3': '''
        # b05_futures_daily

        读取 b04 的 `dim_futures_bar_calendar`，从 JQData 采集日线并生成 `fact_futures_daily`；写入时将事实叶和对应日历叶共同提交，回写采集完成、缺失和质量状态。

        默认按当前事实白名单评估全部历史日线格点，API 待办为 **当前需要采集且尚未完成** 的格点。正式完成状态是可信快照：已完成的空结果、缺失占位和 warning 不重新拉取，也不扫描全历史事实重新证明完成。

        阅读顺序：初始化与契约 → 输出校验 → Dataset 边界 → 白名单和请求计划 → 来源响应与事实生成 → 完整叶准备及协调提交 → 运行入口。函数按定义顺序展开，总流程图按实际调用顺序阅读。Notebook 是唯一编辑源，同名 `.py` 由默认 PythonExporter 生成。
    ''',
    '15c0a640': '''
        ## 项目定位与依赖

        从当前目录向上搜索 `.git`、`.env` 和 `config/settings.py`，先定位项目根，再导入配置、具名权威 Schema 和共享事实采集白名单。正式湖根目录由 `settings.futures_lake_root` 读取，唯一配置来源是 `.env` 的 `FUTURES_LAKE_ROOT`。

        初始化只建立依赖与定义，不认证 JQData、不采集、不写湖。
    ''',
    'c05-schema-browser-heading': '''
        ## Schema 契约浏览

        交互式 Notebook 通过共享展示模块查看本环节的权威 Schema 与有界只读样例；导出的命令行脚本跳过展示。字段含义、主键和分区以 `config/data_contracts.py` 为准，浏览不代替生产校验。
    ''',
    'a79ddbd5': '''
        ## 表身份、规划投影与请求常量

        表名、主键和 Hive 分区在初始化时从具名 Schema metadata 各读取一次。事实主键用于响应对齐和完整叶合并；API 待办依赖日历完成状态，不通过事实主键全表求差。

        `CALENDAR_PLANNING_COLUMNS` 仅包含日线主键、白名单身份、休市状态、required、completed 和年月分区。JQData 字段、来源标识、质量枚举及请求上限在此声明，不发起 I/O。
    ''',
    'd5230ef4': '''
        ## 日线事实校验与 OHLC 质量判定

        `close_mask()` 用于浮点派生关系比较；`invalid_ohlc_mask()` 识别有限价格之间的 high/low 关系异常。OHLC 关系异常保留来源原值，随后在日历中记 warning；非空 NaN/Inf、负成交量/成交额/持仓量仍拒绝通过。

        `validate_daily_frame()` 检查 Schema、主键、来源、合约与日期分区、缺失占位及派生值。当前采集批与本次采集汇总均调用它，并关闭前序行关系检查；写入前在合并后的 dirty 完整事实叶上执行完整业务校验。staging 与正式安装后只核对物理契约、主键唯一性和行数，不重复业务 validator。
    ''',
    '4835c283': '''
        ## 待回写日历完整叶校验

        `validate_calendar_state_frame()` 对本环节准备提交的 b04 完整叶负责：检查结构、枚举、采集完成凭证、实际与缺失条数，以及质量说明和时间。它支持日历 Schema 中的两种频率，但 b05 主流程仅回写 `1d` 叶。

        这里校验的是合并了政策与本次完成状态的 dirty 输出叶；日常规划信任 b04 已提交的上游业务语义，不重复扫描全历史日历做业务质检。
    ''',
    'b86cefb4': '''
        ## Dataset 的物理契约与稳定表身份

        `reconstructed_schema()` 按权威字段顺序重建 Dataset 逻辑 Schema；`physically_and_identity_compatible()` 比较字段、类型、nullable，以及表名、主键和分区 metadata。描述性 metadata 以当前契约为准，其差异不触发历史数据重写。

        `open_contract_dataset()` 打开上游根 Dataset，检查逻辑 Schema 和各 fragment 的物理字段；主流程随后用 Arrow filter 裁剪频率及可选日期，只读取规划窄列。写入前的事实和日历读取则使用后面的精确叶路径入口。
    ''',
    'c05-daily-policy-markdown': '''
        ## 窄列白名单规划与状态继承

        `daily_policy_plan()` 在 Arrow 窄表上检查 `1d` 和 `session_number=0`，按共享白名单与 `confirmed_closed` 形成期望 required。政策 dirty 只取 required 布尔值发生变化的行；API pending 只取期望 required 且 `is_fetch_completed=false` 的行，中文说明变化本身不产生 dirty。

        白名单缩小时，`apply_policy_to_calendar_leaf()` 保留既有事实、完成批次、条数、质量及 b07 证据，只关闭 required 并清零当前缺失。重新纳入时，已有完成证据仍阻止 API；已完成且有缺失检查时间的行按现有条数恢复当前缺失计数。未触达行保留原值。

        `plan_daily_policy()` 为 DataFrame 调用提供转换和可选日期裁剪；主流程直接调用 Arrow 版本，不经过这一入口。
    ''',
    '57f0d8d8': '''
        ## 请求分批与逐批配额检查

        `request_batches()` 先按交易所和年份分组，再按合约顺序组批，通常每批至多 100 个合约、估计返回量至多 90,000；单合约即使超过估计量上限仍形成一批。请求起点向前回看 45 个自然日，为上一有效结算价和持仓量提供上下文，回看日期不增加输出键。

        `quota_spare()` 只读取可用额度 spare；接口不存在或返回格式不支持时返回 `None`，不把未知额度当成零。入口在每批请求前检查预计消耗后是否仍满足 `--quota-reserve`（默认 5,000,000）。额度不足则停止后续请求；已采集结果仍进入汇总，并由 `--write` 决定是否与政策变化一起提交。
    ''',
    '46ad3856': '''
        ## 来源响应归一化

        `normalize_price_response()` 将日线响应统一为合约—日期长表；`normalize_extra_response()` 将结算价或持仓量的日期×合约宽表转成长表。非空响应检查请求列、合约集合、重复键及数值边界；请求日期范围在 `collect_batch()` 中核对。

        空 DataFrame 是可接受的空结果；`None` 或不支持的响应类型抛出异常。主流程没有自动重试，异常停止后续工作。
    ''',
    'fbc92c70': '''
        ## 一批来源采集、派生值计算与待办对齐

        `collect_batch()` 顺序调用一次 `get_price(frequency="daily")` 和两次 `get_extras()`，分别取得价格与量额、结算价、持仓量。先在含回看期的扩展字段序列上计算上一有效值与变化，再将结果左连接到本批待办键。

        没有有效收盘价的待办保留一行 `has_market_data=false` 占位，行情指标全空；有效行情保留来源数值，有限 OHLC 关系异常不改值。函数完成本批业务校验后返回事实与返回量计数；进度日志目前由入口在调用前后报告。
    ''',
    '2a22008f': '''
        ## 文件 Schema 与精确完整叶读取

        `parquet_file_schema()` 从文件 Schema 排除由 Hive 目录承载的分区列；`partition_relative_path()` 按分区字段顺序构造目标路径。`read_partition_leaf()` 直接打开这个叶目录，结合表根恢复 Hive 分区字段，检查物理契约后读取完整叶。

        不存在或没有 Parquet 的叶返回契约化空表，事实准备可据此建叶；日历准备要求 b04 叶已经存在，空表会报错。此处没有重新打开事实表根，也没有逐叶扫描全表。
    ''',
    '720c2940': '''
        ## 事实与日历叶的协调提交

        `commit_validated_leaf_group()` 接收已经过完整业务校验的叶规格，检查非空、分区范围和目标唯一性，再转换为 Arrow 并核对表根零行标记。写入 staging 后，仅复读文件物理契约、主键唯一性和行数；全部通过才备份旧叶、安装新叶并补充缺失的零行标记。

        正式安装后仍只精确复读本组叶的物理契约、主键和行数。安装或正式复读失败时，移除本次新建标记，逆序隔离失败新叶并恢复旧叶；恢复不完整时保留备份。此前已经成功提交的其他组不回滚。

        一组包含一个 `1d—交易所—年月` 日历叶及其本次触达的事实叶。这是现有进程内协调事务，目前仍由本文件实现；共享安装与回滚模块的接入属于后续步骤。
    ''',
    'd7734812': '''
        ## 合并并校验 dirty 完整事实叶

        `prepare_fact_leaf_specs()` 按事实 Hive 分区处理本批结果，精确读取旧叶，保留不在本批主键集合中的历史行，再合并新行。每个 dirty 完整叶执行一次完整 `validate_daily_frame()`，包括前序行关系检查，返回供协调提交使用的规格。

        没有新事实时返回空规格列表；纯白名单政策变化可以只提交日历叶。白名单缩小不会删除历史事实。
    ''',
    '43d2aa31': '''
        ## 完成状态生成与 dirty 日历叶准备

        `apply_completion_to_calendar_leaf()` 按本次事实生成完成凭证：正常行情为实际 1 条、passed；有限 OHLC 关系异常仍为实际 1 条，但记 warning；无有效收盘价为实际 0 条、缺失 1 条、warning。三者都记为已完成，日常不会因此重拉。

        `prepare_calendar_leaf_spec()` 精确读取对应 b04 完整叶，先应用政策变化，再应用本次完成状态，最后执行一次完整状态校验。未触达行和无关证据保留原值。此时只形成内存结果，事实与日历随后在同组安装并共同验收；提交失败遵循上一节的恢复边界。

        b05 没有独立日期水位文件；下次待办继续由日历中正式提交的 required 与 completed 决定。
    ''',
    'c2d11e3b': '''
        ## 运行入口：规划、采集汇总与逐组提交

        `main()` 校验参数，读取日历窄列并形成政策 dirty、API pending 和批次。只有存在请求批次时才认证 JQData；采集后汇总校验，再根据 `--write` 输出只读结果或逐组准备完整叶并协调提交。没有政策变化和新事实时直接结束；每组失败都停止后续组。

        日志沿用 b01、b02 的 88 个 `=` 运行边界，使用 `table/function/phase/status` 字段，报告规划量、请求批次、提交组数和耗时。保留 `request_batch:`、`api_result:`、`committed:`、`quota_stop:` 等事件前缀，并使用监控可识别的 `planning_progress:` 表达运行状态。配额停止与全部完成分开表达；异常继续沿原调用链抛出，不输出成功结束日志。本轮日志仍在入口，函数内部进度归位留待后续。

        当前单元格仍包含 `if __name__ == "__main__": main()`：普通模块导入不启动业务，直接运行 `.py` 使用命令行参数；在 Notebook 中执行本格也可能启动 Click 并读取内核参数。执行单元格尚未改为 b01 的独立参数形式，可通过同名导出脚本运行。
    ''',
}
for cell_id, source in descriptions.items():
    cells[cell_id].source = textwrap.dedent(source).strip()

FLOW_INIT = '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%'


def markdown(cell_id, source):
    cell = nbformat.v4.new_markdown_cell(textwrap.dedent(source).strip())
    cell.id = cell_id
    return cell


def flow(cell_id, title, body):
    return markdown(cell_id, f'{title}\n\n```mermaid\n{FLOW_INIT}\n{textwrap.dedent(body).strip()}\n```')


overview = flow('b05-flow-overview', '## 总流程：日线待办到事实与完成状态', '''
    flowchart TD
      A["参数与正式湖写入边界"] --> B["读取 b04 日线规划窄列"]
      B --> C["评估白名单：政策 dirty 与未完成 pending"]
      C --> D{"存在 API 批次？"}
      D -- 否 --> H["汇总已采集事实并校验"]
      D -- 是 --> E["认证 JQData"] --> P["请求前检查本批配额"]
      P -- 额度不足 --> Q["记录 quota_stop；停止后续请求"]
      Q --> H
      P -- 可请求 --> F["三次 API；归一化、派生与待办对齐"]
      F --> G{"还有批次？"}
      G -- 是 --> P
      G -- 否 --> H
      H --> I{"启用 write 且有变化？"}
      I -- 否 --> J["只读结束或无需更新；保留配额停止结论"]
      I -- 是 --> K["逐组准备事实与日历完整叶；业务校验"]
      K --> L["staging → 共同安装 → 正式复读"]
      L -- 成功 --> M["记录本组完成；继续下一组"]
      M -- 全部组结束 --> N["输出已提交统计；有 quota_stop 则报告停止"]
      M -- 还有组 --> K
      L -- 失败 --> R["恢复当前组并抛错；此前成功组保留"]
''')
overview.source += '\n\n认证每次运行至多一次。图中未展开的读取、采集、生成和校验异常沿原调用链抛出，停止后续工作；只有进入安装阶段后的失败才涉及当前组恢复。'
scope = markdown('b05-run-scope', '''
    ## 更新范围与写入边界

    | 方式 | 处理范围 | 写入条件 |
    | --- | --- | --- |
    | 默认自动 | 全历史日线窄列评估；只采集当前 required 且未完成格点 | `--write` 可提交正式湖 |
    | 成对 `--start-date/--end-date` | 只评估指定日期的日线格点 | 正式湖仅可只读；写入必须指定非正式湖 |
    | 不带 `--write` | 仍采集待办并完成内存校验 | 不提交事实或日历状态 |

    b05 不提供 `--full`。白名单来自共享配置，已完成证据不因零条或 warning 失效。配额不足停止后续请求；若启用写入，已完成批次和政策变化仍可提交，然后报告 `quota_stop`。业务异常不自动重试。
''')

# 每个非空代码单元格前保留一个对应的局部流程图。
flow_specs = {
    'a68d585d': ('init', '### 流程：初始化', '''
        flowchart LR
          A["当前目录向上搜索项目标记"] --> B["导入配置与权威 Schema"] --> C["导入白名单与展示依赖"]
    '''),
    'c05-schema-browser': ('schema', '### 流程：只读契约浏览', '''
        flowchart LR
          A{"交互式 Notebook？"} -- 是 --> B["展示权威 Schema 与有界样例"]
          A -- 否 --> C["跳过展示"]
    '''),
    'b99c10ba': ('constants', '### 流程：建立本环节常量', '''
        flowchart LR
          A["具名 Schema metadata"] --> B["表名、主键与分区"] --> C["规划投影、请求字段与限额"] --> D["构造 Hive partitioning"]
    '''),
    '26d91005': ('quality-masks', '### 流程：数值关系判断', '''
        flowchart LR
          A["待比较数值"] --> B["close_mask：容差比较"]
          C["行情 OHLC"] --> D["invalid_ohlc_mask：high/low 关系"] --> E["返回异常掩码；不修改行情"]
    '''),
    '79ac3c03': ('fact-validation', '### 流程：事实业务校验', '''
        flowchart TD
          A["转换至权威事实 Schema"] --> B["检查主键、身份、分区和来源"] --> C["检查占位、有限数、数量与派生值"]
          C --> D{"启用前序行关系检查？"}
          D -- 是 --> E["按合约日期检查前序关系"] --> F["返回已校验事实"]
          D -- 否 --> F
    '''),
    'a0b4e7d0': ('calendar-validation', '### 流程：日历输出叶校验', '''
        flowchart LR
          A["转换至日历 Schema"] --> B["结构与枚举约束"] --> C["完成、缺失和质量约束"] --> D["返回完整日历叶"]
    '''),
    '0b80489f': ('schema-reconstruction', '### 流程：恢复逻辑字段顺序', '''
        flowchart LR
          A["Dataset 逻辑 Schema"] --> B["按权威 Schema 顺序取字段"] --> C["保留 Dataset 表 metadata"]
    '''),
    'a5e1c885': ('compatibility', '### 流程：物理契约与表身份比较', '''
        flowchart LR
          A["实际与权威 Schema"] --> B["字段名、类型、nullable"] --> C["表名、主键、分区 metadata"] --> D["返回兼容性；忽略描述差异"]
    '''),
    'f1d01294': ('dataset', '### 流程：根 Dataset 打开', '''
        flowchart TD
          A{"表路径有 Parquet？"} -- 否 --> B{"必需上游？"}
          B -- 是 --> C["抛错"]
          B -- 否 --> D["返回 None"]
          A -- 是 --> E["打开 Hive Dataset"] --> F["检查逻辑与 fragment 物理契约"] --> G["返回 Dataset"]
    '''),
    'c05-daily-policy': ('policy', '### 流程：政策计划与叶内应用', '''
        flowchart TD
          A["日线规划窄表"] --> B["白名单且未确认休市 → 期望 required"]
          B --> C["required 改变 → dirty"]
          B --> D["required 且未完成 → pending"]
          B --> E["required 且已完成 → completed"]
          C --> F["写入准备时按主键匹配当前日历叶"]
          F --> G["更新 required/原因与当前缺失；保留完成和质量证据"]
    '''),
    'bc59b121': ('batches', '### 流程：构造请求批次', '''
        flowchart TD
          A["pending 按交易所、年分组"] --> B["依次尝试加入合约；估算含回看期的返回量"]
          B --> C{"已有合约且加入后超限？"}
          C -- 是 --> D["保存上一批；当前合约开始新批"]
          C -- 否 --> E["加入当前批"]
          D --> F["继续合约；组尾保存剩余批次"]
          E --> F
          F --> G["返回批次列表"]
    '''),
    '78ada88d': ('quota', '### 流程：读取可用配额', '''
        flowchart TD
          A{"SDK 有配额查询？"} -- 否 --> B["返回 None"]
          A -- 是 --> C["查询额度"] --> D{"返回 dict 且 spare 非空？"}
          D -- 是 --> E["返回整数 spare"]
          D -- 否 --> B
    '''),
    '107d2b6d': ('price-response', '### 流程：日线响应归一化', '''
        flowchart TD
          A["get_price 响应"] --> B{"合法 DataFrame？"}
          B -- 否 --> X["抛错"]
          B -- 是 --> C{"空响应？"}
          C -- 是 --> D["返回空长表"]
          C -- 否 --> E["整理日期与合约；检查列和请求合约"] --> F["检查数值与重复键；返回长表"]
    '''),
    '8a50c9fb': ('extra-response', '### 流程：扩展字段响应归一化', '''
        flowchart TD
          A["get_extras 响应"] --> B{"合法 DataFrame？"}
          B -- 否 --> X["抛错"]
          B -- 是 --> C{"空响应？"}
          C -- 是 --> D["返回空长表"]
          C -- 否 --> E["检查请求合约列；宽表转长表"] --> F["检查数值与重复键；返回长表"]
    '''),
    'd180c167': ('collect', '### 流程：采集并生成一批事实', '''
        flowchart TD
          A["一次日线、两次扩展字段 API"] --> B["响应归一化并检查请求日期范围"]
          B --> C["在回看序列上计算上一结算与持仓及变化"]
          C --> D["左连接本批 pending 键"] --> E{"有有效收盘价？"}
          E -- 是 --> F["保留来源行情与派生值"]
          E -- 否 --> G["行情全空；标记缺失占位"]
          F --> H["本批校验；暂不检查前序行关系"]
          G --> H
          H --> I["返回事实与返回量计数"]
    '''),
    'b205c6aa': ('leaf-read', '### 流程：精确读取叶分区', '''
        flowchart TD
          A["分区列与键 → Hive 叶路径"] --> B{"叶存在且有 Parquet？"}
          B -- 否 --> C["返回契约化空表"]
          B -- 是 --> D["以表根恢复 Hive 列并打开叶 Dataset"] --> E["物理与逻辑契约检查"] --> F["读取完整叶并转换"]
    '''),
    'b43616a1': ('commit', '### 流程：单组暂存、安装与失败恢复', '''
        flowchart TD
          A["已校验完整叶规格"] --> B["分区、重复目标与零行标记门禁"]
          B --> C["写 staging；复核物理契约、主键与行数"]
          C -- 失败 --> D["清理 staging 并抛错"]
          C -- 通过 --> E["备份旧叶；安装本组新叶；补标记"]
          E --> F["精确复读本组正式叶"]
          F -- 通过 --> G["清理 staging 与备份；返回行数"]
          E -- 失败 --> H["逆序移除新标记、隔离新叶、恢复旧叶"]
          F -- 失败 --> H
          H --> I["保留隔离证据及未恢复备份；抛错"]
    '''),
    'b5abb5b2': ('fact-leaves', '### 流程：准备完整事实叶', '''
        flowchart LR
          A["按事实分区拆分本批结果"] --> B["精确读取旧叶"] --> C["保留其他键；合并新事实"] --> D["完整叶业务校验"] --> E["返回事实叶规格"]
    '''),
    'f35dba7f': ('calendar-leaf', '### 流程：准备完整日历叶', '''
        flowchart TD
          A["精确读取 b04 日历叶；不存在则报错"] --> B["应用 required 政策变化"]
          B --> C["按事实生成完成、缺失与质量状态"] --> D["主键匹配；更新命中行"]
          D --> E["完整状态校验"] --> F["返回日历叶规格；等待与事实共同提交"]
    '''),
    '71d11aec': ('main', '### 流程：入口分支与日志边界', '''
        flowchart TD
          A["参数检查通过；记录 run started"] --> B["窄列规划与请求分批；报告计划"]
          B --> C["逐批配额检查与采集；记录批次进度"] --> D["汇总校验；报告 api_result"]
          D --> E{"write 且有政策或事实变化？"}
          E -- 否 --> F["输出只读或无需更新结果"]
          E -- 是 --> G["逐组准备、提交；报告组数与耗时"]
          F --> H{"发生配额停止？"}
          G --> H
          H -- 是 --> I["quota_stop；run stopped"]
          H -- 否 --> J["run completed"]
    '''),
}

source = cells['71d11aec'].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new, 1)


replace_once('    run_updated_at = datetime.now(timezone.utc)', '''    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"{log_boundary}\\n日线运行开始 / Daily run started\\n"
        "function=main()\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}\\n{log_boundary}"
    )
    run_updated_at = datetime.now(timezone.utc)''')
replace_once('    calendar_dataset = open_contract_dataset(', '''    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=planning; status=started"
    )
    calendar_dataset = open_contract_dataset(''')
replace_once('f"planning_columns={len(CALENDAR_PLANNING_COLUMNS)}"', '''f"planning_columns={len(CALENDAR_PLANNING_COLUMNS)}; "
        f"function=main; phase=planning; status=running; "
        f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"''')
replace_once('f"planning_seconds={time.perf_counter() - planning_started_at:.3f}"', '''f"planning_seconds={time.perf_counter() - planning_started_at:.3f}; "
        f"function=main; phase=planning; status=completed; "
        f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"''')
replace_once('    collected_frames = []', '''    log_collect_started_at = time.perf_counter()
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=collect; status=started; "
        f"batches={len(batches)}; pending={len(pending_df)}"
    )
    collected_frames = []''')
replace_once('f"remaining_pending={remaining_pending_count}"', '''f"remaining_pending={remaining_pending_count}; "
                    f"table={TABLE_NAME}; function=main; phase=collect; status=stopped; "
                    f"elapsed_s={time.perf_counter() - log_collect_started_at:.3f}"''')
replace_once('            click.echo(\n                "request_batch: "', '''            log_batch_started_at = time.perf_counter()
            click.echo(
                "request_batch: "''')
replace_once('f"end={batch[\'request_end\']}"', '''f"end={batch['request_end']}; "
                f"table={TABLE_NAME}; function=main; phase=request_batch; status=started"''')
replace_once('            completed_pending_count += len(batch["pending_df"])', '''            completed_pending_count += len(batch["pending_df"])
            click.echo(
                f"api_success: table={TABLE_NAME}; function=main; phase=request_batch; status=completed; "
                f"batch={batch_number}/{len(batches)}; fact_rows={len(batch_df)}; "
                f"returned_values={batch_returned_count}; completed_pending={completed_pending_count}; "
                f"pending_total={len(pending_df)}; elapsed_s={time.perf_counter() - log_batch_started_at:.3f}"
            )''')
replace_once('f"invalid_ohlc_rows={invalid_ohlc_count}"', '''f"invalid_ohlc_rows={invalid_ohlc_count}; "
        f"table={TABLE_NAME}; function=main; phase=collect; "
        f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
        f"remaining_pending={remaining_pending_count}; "
        f"elapsed_s={time.perf_counter() - log_collect_started_at:.3f}"''')
old = 'click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")'
assert source.count(old) == 2
source = source.replace(old, '''click.echo(
                f"up_to_date: table={TABLE_NAME}; mode={mode}; function=main; phase=run; status=completed"
            )''')
replace_once('f"fact_rows={len(collected_df)}; write=False"', '''f"fact_rows={len(collected_df)}; write=false; "
                f"table={TABLE_NAME}; function=main; phase=preview; status=completed"
'''.rstrip())


def run_end(outcome, *, indent):
    return textwrap.indent('''click.echo(
    f"{log_boundary}\\n日线运行结束 / Daily run ended\\n"
    "function=main()\\n"
    f"planning_progress: table={TABLE_NAME}; function=main; phase=run; "
    f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
    f"outcome={OUTCOME}; mode={mode}; write={str(write).lower()}; "
    f"remaining_pending={remaining_pending_count}; "
    f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
)'''.replace('OUTCOME', outcome), ' ' * indent)


replace_once('        return\n    if dirty_policy_df.empty and collected_df.empty:',
    run_end("'quota_stopped' if quota_stop_message is not None else ('up_to_date' if dirty_policy_df.empty and collected_df.empty else 'read_only')", indent=8)
    + '\n        return\n    if dirty_policy_df.empty and collected_df.empty:')
replace_once('        return\n\n    dirty_calendar_keys = set(',
    run_end("'quota_stopped' if quota_stop_message is not None else 'up_to_date'", indent=8)
    + '\n        return\n\n    dirty_calendar_keys = set(')
replace_once('    committed_fact_rows = 0', '''    log_commit_started_at = time.perf_counter()
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=commit_batch; status=started; "
        f"leaf_groups={len(dirty_calendar_keys)}; fact_rows={len(collected_df)}; policy_rows={len(dirty_policy_df)}"
    )
    committed_fact_rows = 0''')
replace_once('        exchange_code, year, month = base_key', '''        log_leaf_started_at = time.perf_counter()
        click.echo(
            f"partition_start: table={TABLE_NAME}; function=main; phase=commit_group; status=started; "
            f"base_partition={base_key}; group={committed_leaf_groups + 1}/{len(dirty_calendar_keys)}"
        )
        exchange_code, year, month = base_key''')
replace_once('f"policy_rows={len(leaf_policy_df)}"', '''f"policy_rows={len(leaf_policy_df)}; "
            f"table={TABLE_NAME}; function=main; phase=commit_group; status=completed; "
            f"group={committed_leaf_groups}/{len(dirty_calendar_keys)}; "
            f"elapsed_s={time.perf_counter() - log_leaf_started_at:.3f}"''')
replace_once('    click.echo(\n        f"committed: mode={mode}; "', '''        click.echo(
            f"planning_progress: table={TABLE_NAME}; function=main; phase=commit_batch; status=running; "
            f"completed_groups={committed_leaf_groups}; total_groups={len(dirty_calendar_keys)}; "
            f"elapsed_s={time.perf_counter() - log_commit_started_at:.3f}"
        )

    click.echo(
        f"committed: mode={mode}; "''')
replace_once('f"remaining_current_plan={remaining_pending_count}"', '''f"remaining_current_plan={remaining_pending_count}; "
        f"table={TABLE_NAME}; function=main; phase=commit_batch; status=completed; "
        f"elapsed_s={time.perf_counter() - log_commit_started_at:.3f}"''')
replace_once('\n\n\nif __name__ == "__main__":', '\n' + run_end("'quota_stopped' if quota_stop_message is not None else 'committed'", indent=4) + '\n\n\nif __name__ == "__main__":')
cells['71d11aec'].source = source

updated_cells = []
for cell in notebook.cells:
    if cell.cell_type == 'code' and not cell.source.strip():
        assert not cell.outputs and cell.execution_count is None and not cell.metadata
        continue
    if cell.id in {'f688e08b', 'd7d27f46'}:
        # 将旧上游说明及重复规划标题并入政策章节，避免标题与代码错位。
        continue
    if cell.id in flow_specs:
        suffix, title, body = flow_specs[cell.id]
        updated_cells.append(flow(f'b05-flow-{suffix}', title, body))
    updated_cells.append(cell)
    if cell.id == '32e0f0c3':
        updated_cells.extend([overview, scope])
notebook.cells = updated_cells


class RemoveLogging(ast.NodeTransformer):
    def visit_Expr(self, node):
        if isinstance(node.value, ast.Call) and ast.unparse(node.value.func) == 'click.echo':
            return None
        return self.generic_visit(node)

    def visit_Assign(self, node):
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        if names and all(name.startswith('log_') for name in names):
            return None
        if names == ['quota_stop_message'] and isinstance(node.value, ast.JoinedStr):
            node.value = ast.Constant(value='quota_stop logging text')
        return self.generic_visit(node)


before_code = [cell for cell in before.cells if cell.cell_type == 'code' and cell.source.strip()]
after_code = [cell for cell in notebook.cells if cell.cell_type == 'code']
assert [cell.id for cell in before_code] == [cell.id for cell in after_code]
for old, new in zip(before_code, after_code, strict=True):
    if old.id != '71d11aec':
        assert old == new, old.id
    assert old.metadata == new.metadata and old.outputs == new.outputs and old.execution_count == new.execution_count
    old_ast = RemoveLogging().visit(ast.parse(old.source))
    new_ast = RemoveLogging().visit(ast.parse(new.source))
    assert ast.dump(old_ast) == ast.dump(new_ast), old.id
    comments = lambda value: [t.string for t in tokenize.generate_tokens(io.StringIO(value).readline) if t.type == tokenize.COMMENT]
    assert comments(old.source) == comments(new.source), old.id
    compile(new.source, str(PATH), 'exec')
assert len(after_code) == len(flow_specs)
assert notebook.metadata == before.metadata
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)
print(f'Updated {PATH.name}: {len(descriptions)} explanations, {len(flow_specs) + 1} diagrams; business AST and original comments unchanged.')
