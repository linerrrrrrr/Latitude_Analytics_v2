"""b06 第 1—4 项：整理说明与流程图，统一现有日志，不改变业务 AST。"""

import ast
import copy
import io
import pathlib
import textwrap
import tokenize

import nbformat

ROOT = pathlib.Path(__file__).resolve().parents[2]
PATH = ROOT / '02_Futures_Lakehouse/a01_Futures_Market_Data/b06_futures_minute.ipynb'
notebook = nbformat.read(PATH, as_version=4)
before = copy.deepcopy(notebook)
cells = {cell.id: cell for cell in notebook.cells}
assert not any(cell.id == 'b06-flow-overview' for cell in notebook.cells)

descriptions = {
    '5c46936e': '''
        # b06_futures_minute

        读取 b04 的 `dim_futures_bar_calendar`，从 JQData 采集一分钟行情并生成 `fact_futures_minute`；写入时先逐个提交分钟事实叶，再按日历叶集中回写采集完成、缺失和质量状态。

        日常按当前白名单评估全部历史 `1m` 格点，API 待办为 **当前需要采集且尚未完成** 的 Session。已完成的零行、部分缺失和 warning 是可信快照，日常不重拉，也不从全历史分钟事实重新证明完成。白名单缩小保留历史事实和完成证据；再次纳入的已完成格点不重复请求。

        阅读顺序：初始化与契约 → 输出校验 → Dataset 与精确叶读取 → 白名单与请求规划 → 分钟响应及 Session 对齐 → 完整叶提交 → 日历完成状态 → CLI。函数按定义顺序展开，总流程图按实际调用顺序阅读。Notebook 是唯一编辑源，同名 `.py` 由默认 PythonExporter 生成。
    ''',
    'aa8bf9c9': '''
        ## 项目定位与依赖

        从当前目录向上搜索 `.git`、`.env` 和 `config/settings.py`，先定位项目根，再导入配置、具名权威 Schema 和共享事实白名单。正式湖根目录使用 `settings.futures_lake_root`，唯一配置来源为 `.env` 的 `FUTURES_LAKE_ROOT`。

        初始化只建立依赖和定义，不认证 JQData、不采集、不写湖。
    ''',
    'c06-schema-browser-heading': '''
        ## Schema 契约浏览

        交互式 Notebook 通过共享展示模块查看分钟事实及行情日历的权威 Schema，并按需查看有界只读样例；导出的命令行脚本跳过展示。字段说明来自 `config/data_contracts.py`，浏览不代替生产校验。
    ''',
    'c6c12141': '''
        ## 表身份、Session 键与规划常量

        表名、主键和 Hive 分区从具名 Schema metadata 各读取一次。事实按交易所—品种—年月分区；日历按频率—交易所—年月分区。两者的提交键分别使用自己的契约。

        `SESSION_KEY` 用于分钟条数汇总与日历回写；`MINUTE_PLANNING_COLUMNS` 是政策和待办规划所需的窄列。JQData 字段、来源标识、质量枚举、配额预留与旧 b07 旁证列在此声明，不发起 I/O。
    ''',
    '0ce9204f': '''
        ## 待回写日历完整叶校验

        `validate_calendar_state_frame()` 对本环节合并了政策与完成状态的 dirty 日历叶负责，检查 Schema、主键、结构与枚举、完成批次及时间、实际与缺失条数，以及质量说明和 b07 比较证据。

        校验器支持日历契约的两种频率，b06 主流程只回写 `1m` 叶。完整业务校验在 dirty 输出叶上执行一次；staging 和正式安装后仅检查物理契约、主键与行数。
    ''',
    '3af2af5f': '''
        ## 分钟事实校验与 Session 边界

        `invalid_ohlc_mask()` 识别有限价格之间的 high/low 关系异常；这类来源值保留，日历随后记 warning。`validate_minute_frame()` 在 Arrow 转换前拒绝非空 NaN/Inf，再检查 Schema、主键、合约身份、日期分区、来源和非负数量；真正的 `None`、`pd.NA` 按字段 nullable 契约处理。

        提供 Session 时，继续检查归属、业务属性、时间范围 `(session_start_at, session_end_at]`、整分钟格点和理论条数上界。来源转换结果与合并后的 dirty 完整事实叶分别承担自身校验，正式历史不参与全表业务复查。
    ''',
    'f21ba82b': '''
        ## Dataset 物理契约与精确叶读取

        `reconstructed_schema()` 恢复 Dataset 的逻辑字段顺序；两个兼容性函数分别核对物理字段、类型、nullable 和表名、主键、分区身份 metadata。描述性 metadata 以当前权威契约为准，差异不触发历史重写。

        `open_contract_dataset()` 在入口分别打开上游日历与现有分钟事实，检查 Schema 和 fragment 物理契约。规划只投影日历窄列，不读取历史事实行。分区发现从文件路径解析 Hive 键；提交时由 `read_complete_partition()` 定位当前完整叶，复原目录中的分区字段并转换为 Pandas，不重开事实表根。
    ''',
    '667a1d22': '''
        ## 白名单窄列规划与选择状态

        `minute_policy_plan()` 按交易所取共享白名单，并排除 `confirmed_closed`，形成期望 required。政策 dirty 取 required 发生变化的格点；pending 取期望 required 且 `is_fetch_completed=false`，completed 取期望 required 且已有完成凭证。

        日常逐个读取日历 `1m` 分区的规划窄列，再按分钟事实分区组织待办。政策变更只定向回写对应日历完整叶；白名单缩小时保留事实、完成批次、质量和 b07 旁证，只停止未来采集并清零当前缺失。再次纳入的已完成格点不发起 API。
    ''',
    '8dcc5023': '''
        ## 配额门禁与合约日请求分组

        `quota_spare()` 只使用 JQData 返回的 `spare`，不把总额度当作当前可用量。入口在每个事实分区的第一项行情请求前，以全部待办 Session 的理论分钟数检查剩余额度和 `--quota-reserve`；不足时停止后续分区，不请求当前分区。

        `request_batches()` 按合约—交易日分组并按 Session 编号排序。`collect_partition()` 每组调用一次 `get_price(frequency="1m")`，时间覆盖该合约日待办 Session 的最早起点至最晚终点；不自动重试。
    ''',
    '702de1a0': '''
        ## 来源响应归一化与分钟生成

        `normalize_minute_response()` 将 JQData 的时间索引或时间列统一为 `bar_at`，处理单合约代码、北京时间和数值类型，拒绝未请求合约、重复分钟主键及非空非有限数。空 DataFrame 保留为空结果；API 返回 `None` 则报错。

        `collect_partition()` 将每条分钟定位到待办 Session，继承交易日、Session 编号和分区身份。时间区间右闭左开；时间单位必须无损对齐，休盘间隔、Session 起点或其他未匹配的时间戳都会硬失败，不静默过滤。转换后的完整批次再检查理论分钟格点与数量边界。

        有限 OHLC 关系异常按原值保留，记录异常分钟与各 Session 异常计数；这些计数随后用于日历 warning，不直接改变开市证据。
    ''',
    '47bc969c': '''
        ## 单个完整叶的暂存、安装与失败恢复

        `commit_complete_partition()` 接收事实或日历的完整目标叶，执行一次完整业务校验并转换为 Arrow。非空结果写入 staging；空结果仍写零行叶文件，不把空结果解释为删除整个分区。

        staging 只复读物理 Schema、身份 metadata、主键唯一性与行数。通过后备份旧叶、安装新叶，检查已有表根 `schema.parquet` 或补建缺失标记，再精确复读当前正式叶。出现安装或验收异常时，尝试恢复旧叶、隔离失败新叶，恢复不完整则保留备份；已成功的其他叶不参与回滚。

        当前安装与恢复仍由本文件实现，尚未接入共享模块。一次事务只覆盖当前叶；事实叶与日历叶分开提交，不提供跨表共同回滚或进程终止后的自动恢复。
    ''',
    '481fb127': '''
        ## 当前事实叶的 Session 替换

        入口精确读取当前 dirty 事实叶，`commit_fact_partition()` 用 `rows_for_sessions()` 找到真正待办 Session 的旧分钟，只替换这部分内容，保留同叶其他事实和白名单外历史。

        合并结果交给 `commit_complete_partition()` 完成单叶校验与安装。成功后返回本次采集的分钟，用于生成完成摘要；这里的事实提交成功尚不表示日历状态已回写。空响应也会清除该待办 Session 的旧分钟，并为后续零行完成状态提供依据。
    ''',
    'c6ef32c3': '''
        ## 完成摘要生成与日历集中回写

        `build_calendar_completion_updates()` 在事实叶成功后，按 Session 生成实际条数与 OHLC 异常数，核对采集阶段记录的异常计数，并按日历叶组织摘要。入口先汇集各成功事实叶的摘要，再调用 `commit_calendar_updates()`；生成摘要本身不写日历。

        `commit_calendar_updates()` 对政策变化与完成摘要涉及的日历叶取并集，每叶精确读取一次，先应用白名单变化，再更新本批完成状态，最后执行一次完整叶业务校验和提交。未触达行保持原值；新完成状态清空本次 Session 的旧 b07 旁证，政策缩小本身保留既有旁证。

        零行 Session 记为已完成 warning，并产生 `suspected_closed` 信号交给 b07；部分缺失或有限 OHLC 异常也形成已完成 warning。OHLC 异常本身不改写开市证据。只有本次非空事实才取消对应的旧疑似休市信号。

        b06 没有独立日期水位文件，下次待办依据日历中的 required 与 completed。配额停止仍进入后续日历回写，处理此前成功事实和政策变化；普通异常则直接停止，保留此前已提交的叶，不额外补写尚未提交的状态。
    ''',
    '42b891dc': '''
        ## CLI：规划、调度与运行结果

        `main()` 校验日期及正式湖写入边界，打开 Dataset 并形成政策变化和分钟待办。没有请求计划时不认证 JQData；若只有政策变化且启用写入，直接提交日历政策。存在待办时逐事实分区检查配额、调用采集并按 `--write` 决定是否提交，最后集中回写日历。

        日志使用与 b01、b02 一致的 88 个 `=` 运行边界及 `table/function/phase/status` 字段，区分规划、采集、事实提交、日历回写和最终结果。配额停止报告 `stopped`，不会显示为整批完成；普通异常继续抛出，不输出成功结束日志。当前仍由入口记录分区与日历提交汇总，函数日志归位留到后续步骤。

        当前单元格同时定义命令和保留 `if __name__ == "__main__": main()`。直接运行 `.py` 读取命令行参数，普通模块导入不启动业务；Notebook 执行此格会尝试读取内核参数，独立执行单元格尚未对齐。
    ''',
}
for cell_id, source in descriptions.items():
    cells[cell_id].source = textwrap.dedent(source).strip()


def flow(cell_id, title, body):
    return nbformat.v4.new_markdown_cell(
        f'### {title}\n\n```mermaid\n'
        '%%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%\n'
        + textwrap.dedent(body).strip() + '\n```', id=cell_id,
    )


overview = flow('b06-flow-overview', '总流程：分钟采集与两阶段提交', '''
    flowchart TD
      A["参数检查；打开根 Dataset 并核对物理契约"] --> B["逐个 1m 叶投影窄列；形成政策变化与待办"]
      B --> C{"有事实请求计划？"}
      C -- 否 --> D["write 时提交政策变化；无 API"]
      C -- 是 --> E["认证 JQData；遍历事实分区"]
      E --> F{"剩余额度可覆盖当前分区并保留预留量？"}
      F -- 否 --> G["停止后续请求；保留已处理结果"]
      F -- 是 --> H["按合约日请求；Session 对齐和事实校验"]
      H --> I{"write？"}
      I -- 否 --> J["累计采集量；继续下一个分区"]
      I -- 是 --> K["精确读当前事实叶；合并、校验与提交"]
      K --> L["生成完成摘要；汇集到日历叶"]
      J --> M{"还有事实分区？"}
      L --> M
      M -- 是 --> F
      M -- 否 --> N{"write？"}
      G --> N
      N -- 否 --> O["输出只读结果；不回写日历"]
      N -- 是 --> P["逐个日历叶合并政策与完成摘要并提交"]
      D --> Q["报告运行结果与耗时"]
      O --> Q
      P --> Q
      Q --> R["正常完成或 quota_stop"]
''')
overview.source = overview.source.replace('```mermaid', '普通异常从发生点直接抛出，跳过后续处理；已经成功提交的叶保留。\n\n```mermaid', 1)
scope = nbformat.v4.new_markdown_cell('''## 更新范围与写入边界

| 方式 | 处理范围 | 写入条件 |
| --- | --- | --- |
| 默认自动 | 全历史 `1m` 窄列评估，只请求当前 required 且未完成的 Session | `--write` 可提交正式湖 |
| 成对 `--start-date/--end-date` | 只评估指定交易日期内的 Session | 正式湖仅可只读；写入必须指定非正式湖 |
| 不带 `--write` | 仍采集待办并完成分钟转换与校验 | 不提交事实或日历状态 |

b06 不提供 `--full`。配额检查按事实分区进行，默认预留 5,000,000 条，可通过 `--quota-reserve` 调整；检查使用接口返回的剩余额度，接口未提供 `spare` 时现有逻辑不据此阻断请求。

当前事务边界是“事实叶逐个提交，之后集中回写日历”。后续失败不会撤销此前成功事实叶；只有日历也成功提交，才持久保存对应 Session 的完成凭证。流程图用于解释，不执行采集。''', id='b06-run-scope')

flow_specs = {
    '5158a600': ('init', '流程：定位项目与加载依赖', '''
        flowchart LR
          A["向上搜索三个项目标记"] --> B["加入项目与湖仓模块路径"] --> C["导入配置、Schema 和共享白名单"]
    '''),
    'c06-schema-browser': ('schema', '流程：交互式契约浏览', '''
        flowchart TD
          A{"Notebook 交互环境？"} -- 是 --> B["展示分钟事实、行情日历契约与可选样例"]
          A -- 否 --> C["脚本跳过展示"]
    '''),
    '3c31bb50': ('constants', '流程：建立表身份与规划常量', '''
        flowchart LR
          A["权威 Schema metadata"] --> B["读取表名、主键与分区"] --> C["声明 Session 键、API 字段与状态常量"] --> D["建立 Hive 分区与窄列 Schema"]
    '''),
    '00267e64': ('calendar-validation', '流程：待提交日历完整叶校验', '''
        flowchart TD
          A["按权威 Schema 转换"] --> B["主键、枚举与日线分钟结构"] --> C["完成凭证、缺失计数与质量时间"] --> D["b07 比较证据一致性"] --> E["排序并返回完整叶；任一失败抛错"]
    '''),
    '8bfa11f6': ('minute-validation', '流程：分钟质量与可选 Session 校验', '''
        flowchart TD
          A["来源非空值有限性；转换 Arrow"] --> B["主键、合约身份、来源、年月与非负数量"]
          B --> C{"提供 Session 且有分钟行？"}
          C -- 是 --> D["匹配 Session；检查属性与右闭左开范围"]
          D --> E["理论整分钟格点与条数上界"] --> F["返回排序后的校验结果"]
          C -- 否 --> F
          G["invalid_ohlc_mask：有限价格关系比较"] --> H["异常掩码；保留原值供 warning 使用"]
    '''),
    'ed1ba9df': ('session-rows', '流程：选取待替换 Session 的旧分钟', '''
        flowchart TD
          A{"分钟或 Session 为空？"} -- 是 --> B["返回同结构空表"]
          A -- 否 --> C["按 Session 键关联时间边界"] --> D["选取右闭左开范围内分钟"] --> E["检查选取主键并排序返回"]
    '''),
    'fb113af1': ('schema-reconstruction', '流程：恢复 Dataset 逻辑 Schema', '''
        flowchart LR
          A["Dataset Schema 与分区 Schema"] --> B["按权威字段顺序取实际字段"] --> C["组成含 Dataset metadata 的逻辑 Schema"]
    '''),
    '9974ab65': ('compatibility', '流程：物理契约与表身份比较', '''
        flowchart LR
          A["实际与期望 Schema"] --> B["字段顺序、类型、nullable"]
          A --> C["表名、主键、分区 metadata"]
          B --> D["分别返回是否兼容；不比较描述性文字"]
          C --> D
    '''),
    'cf2450be': ('dataset', '流程：打开根 Dataset 的物理门禁', '''
        flowchart TD
          A{"目录存在 Parquet？"} -- 否 --> B{"上游必需？"}
          B -- 是 --> C["缺失报错"]
          B -- 否 --> D["返回 None"]
          A -- 是 --> E["打开 Dataset；恢复逻辑 Schema"] --> F["核对逻辑与各文件物理契约、表身份"] --> G["返回 Dataset"]
    '''),
    '126fe7bc': ('partition-read', '流程：分区发现与当前完整叶读取', '''
        flowchart TD
          A["根 Dataset 文件路径"] --> B["跳过零行标记；解析 Hive 目录与类型"] --> C["返回分区键集合"]
          D["表根、分区列与当前键"] --> E["构造精确叶路径"] --> F{"存在叶文件？"}
          F -- 否 --> G["返回权威 Schema 空表"]
          F -- 是 --> H["检查各文件物理契约；读取当前叶"] --> I["补回分区字段；转换 Pandas"]
    '''),
    'c06-arrow-policy-plan': ('policy', '流程：白名单与可信完成凭证形成待办', '''
        flowchart TD
          A["规划窄表；当前交易所白名单"] --> B["命中白名单且未确认休市：desired required"]
          B --> C["与原 required 比较：政策 dirty"]
          B --> D["尚未完成：pending"]
          B --> E["已有完成凭证：completed"]
    '''),
    'b358d80e': ('quota-batches', '流程：额度读取与合约日组批', '''
        flowchart TD
          A["quota_spare：请求剩余额度"] --> B["返回 spare 或 None；由入口检查预留量"]
          C["request_batches：待办 Session"] --> D["按合约和交易日分组"] --> E["组内按 Session 编号排序；返回请求批次"]
    '''),
    '41d66b26': ('response', '流程：分钟响应归一化', '''
        flowchart TD
          A["要求 DataFrame"] --> B{"空响应？"}
          B -- 是 --> C["返回规定列的空表"]
          B -- 否 --> D["统一时间、合约与字段"] --> E["核对合约；归一北京时间"] --> F["拒绝非有限值；严格转换数值"] --> G["检查重复分钟主键；返回长表"]
    '''),
    'b31dc6c2': ('collect', '流程：单个事实分区的分钟采集', '''
        flowchart TD
          A["待办 Session 按合约日组批"] --> B["记录请求进度；调用 get_price 1m"]
          B --> C["None 抛错；其余响应归一化"] --> D{"响应有行？"}
          D -- 否 --> H{"还有批次？"}
          D -- 是 --> E["无损对齐时间单位；定位 Session，越界抛错"] --> F["继承业务属性；保留并记录 OHLC 异常"] --> G["积累分钟行与异常计数"] --> H
          H -- 是 --> B
          H -- 否 --> I["合并采集结果；验证分钟及 Session"] --> J["返回分钟、来源条数与异常计数"]
    '''),
    '7ee73481': ('file-schema', '流程：构造 Parquet 文件 Schema', '''
        flowchart LR
          A["逻辑 Schema 与分区列"] --> B["移除目录承载的分区字段"] --> C["保留字段定义及完整 metadata"]
    '''),
    '5420a9da': ('partition-filter', '流程：构造精确分区过滤条件', '''
        flowchart LR
          A["分区列与对应值"] --> B["逐列等值条件以 AND 合并"] --> C["返回过滤表达式；空键报错"]
    '''),
    '41986f77': ('commit', '流程：本地单叶暂存与安装恢复', '''
        flowchart TD
          A["完整叶业务校验；检查分区并转换 Arrow"] --> B["写 staging；空结果写零行叶"] --> C["复读物理契约、主键与行数"]
          C -- 失败 --> D["清理 staging 并抛错"]
          C -- 通过 --> E["备份旧叶；安装新叶；检查或补建表根标记"] --> F["精确复读当前正式叶"]
          F -- 通过 --> G["清理临时目录；返回已校验完整叶"]
          E -- 失败 --> H["移除新建标记；尝试隔离新叶与恢复旧叶"]
          F -- 失败 --> H
          H --> I["保留隔离或未恢复备份；抛错停止"]
    '''),
    '3ee121e7': ('fact-merge', '流程：待办 Session 替换与事实提交', '''
        flowchart LR
          A["当前事实叶与待办 Session"] --> B["保留非待办旧分钟"] --> C["拼接本次采集结果"] --> D["校验并提交完整事实叶"] --> E["返回本次分钟；日历仍待回写"]
    '''),
    'c06-unified-calendar-commit': ('calendar-updates', '流程：完成摘要与逐叶日历回写', '''
        flowchart TD
          A["成功事实分区与待办 Session"] --> B["统计实际及异常条数；核对响应审计"] --> C["按日历叶生成完成摘要；入口汇集"]
          C --> D["合并政策变化与完成摘要的叶键"] --> E["精确读取当前日历完整叶"] --> F["应用白名单变化；保留既有完成证据"]
          F --> G["写本次完成、缺失与质量状态"] --> H["零行形成疑似休市；清空本次旧 b07 旁证"] --> I["完整叶校验与提交"] --> J{"还有日历叶？"}
          J -- 是 --> E
          J -- 否 --> K["返回政策行、完成 Session 和提交叶数量"]
    '''),
    '86251818': ('main', '流程：入口分支与日志边界', '''
        flowchart TD
          A["参数检查；记录 run started"] --> B["规划窄列与请求分区；报告计划"] --> C{"存在请求计划？"}
          C -- 否 --> D["可选政策写入；报告无需采集"]
          C -- 是 --> E["逐分区检查额度、采集；可选事实提交"] --> F{"write？"}
          F -- 否 --> G["只读采集汇总"]
          F -- 是 --> H["集中回写日历；核对完成摘要数"]
          G --> I{"配额停止？"}
          H --> I
          I -- 是 --> J["quota_stop；run stopped"]
          I -- 否 --> K["run completed"]
          D --> K
    '''),
}


def replace_in(cell_id, old, new):
    source = cells[cell_id].source
    assert source.count(old) == 1, (cell_id, old)
    cells[cell_id].source = source.replace(old, new)


replace_in('b31dc6c2', 'f"start={request_start}; end={request_end}"', '''f"start={request_start}; end={request_end}; "
            f"table={TABLE_NAME}; function=collect_partition; phase=request_batch; status=started"''')
replace_in('b31dc6c2', 'f"ohlc={(invalid_row.open, invalid_row.high, invalid_row.low, invalid_row.close)}"', '''f"ohlc={(invalid_row.open, invalid_row.high, invalid_row.low, invalid_row.close)}; "
                    f"table={TABLE_NAME}; function=collect_partition; phase=source_quality; status=warning"''')

source = cells['86251818'].source


def replace_once(old, new):
    global source
    assert source.count(old) == 1, old
    source = source.replace(old, new)


replace_once('    silver_root = resolved_lake_root / "silver"', '''    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_mode = "explicit" if has_explicit_dates else "automatic"
    click.echo(
        f"{log_boundary}\\n分钟运行开始 / Minute run started\\n"
        "function=main()\\n"
        f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}\\n{log_boundary}"
    )
    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=planning; status=started"
    )
    silver_root = resolved_lake_root / "silver"''')
replace_once('''        "planning_start: "
        f"table={TABLE_NAME}; calendar_partitions''', '''        "planning_progress: "
        f"table={TABLE_NAME}; function=main; phase=planning; status=running; calendar_partitions''')
replace_once('f"calendar_files={len(calendar_dataset.files)}"', '''f"calendar_files={len(calendar_dataset.files)}; "
        f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"''')
replace_once('''                f"table={TABLE_NAME}; "
                f"partitions=''','''                f"table={TABLE_NAME}; function=main; phase=planning; status=running; "
                f"partitions=''')
source = source.replace('elapsed_seconds={time.perf_counter() - planning_started_at:.3f}', 'elapsed_s={time.perf_counter() - planning_started_at:.3f}')
replace_once('f"{plan_name}: table={TABLE_NAME}; "', 'f"{plan_name}: table={TABLE_NAME}; function=main; phase=planning; status=completed; "')
replace_once('f"planning_seconds={time.perf_counter() - planning_started_at:.3f}"', 'f"elapsed_s={time.perf_counter() - planning_started_at:.3f}"')

# 两处日历提交汇总仍保留原调用位置，只统一样式与监控可识别前缀。
assert source.count('"calendar_committed: "') == 2
source = source.replace('"calendar_committed: "', 'f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=calendar_commit; status=completed; "')
source = source.replace('f"elapsed_seconds="', 'f"elapsed_s="')
replace_once('''        click.echo(f"up_to_date: table={TABLE_NAME}; mode={mode}")
        return''', '''        click.echo(
            f"up_to_date: table={TABLE_NAME}; mode={mode}; function=main; phase=collect; status=completed; "
            f"pending_sessions=0; policy_rows={policy_changed_count}; write={str(write).lower()}"
        )
        click.echo(
            f"{log_boundary}\\n分钟运行结束 / Minute run ended\\n"
            "function=main()\\n"
            f"planning_progress: table={TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome={'policy_committed' if write and policy_updates_by_partition else ('read_only' if policy_updates_by_partition else 'up_to_date')}; "
            f"mode={mode}; write={str(write).lower()}; elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
        )
        return''')
replace_once('    processed_partitions = 0', '''    log_processed_sessions = 0
    processed_partitions = 0''')
replace_once('''                "未请求或写入当前分区。"''', '''                f"table={TABLE_NAME}; function=main; phase=quota; status=stopped; "
                f"processed_sessions={log_processed_sessions}; planned_sessions={pending_count}; "
                "未请求或写入当前分区。"''')
replace_once('''        click.echo(
            "partition_start: "''', '''        log_partition_started_at = time.perf_counter()
        click.echo(
            "partition_start: "''')
replace_once('f"expected_rows={expected_rows}"', '''f"expected_rows={expected_rows}; "
            f"table={TABLE_NAME}; function=main; phase=collect_partition; status=started"''')
replace_once('''        invalid_ohlc_rows += partition_invalid_rows
        if not write:''', '''        invalid_ohlc_rows += partition_invalid_rows
        log_processed_sessions += len(plan["pending_sessions_df"])
        click.echo(
            f"api_success: table={TABLE_NAME}; function=main; phase=collect_partition; status=completed; "
            f"partition={partition_number}/{len(plans)}; key={partition_key}; "
            f"sessions={len(plan['pending_sessions_df'])}; fact_rows={len(collected_df)}; "
            f"returned_rows={partition_returned_rows}; invalid_ohlc_rows={partition_invalid_rows}; "
            f"elapsed_s={time.perf_counter() - log_partition_started_at:.3f}"
        )
        if not write:''')
replace_once('''        existing_df = read_complete_partition(''', '''        log_fact_commit_started_at = time.perf_counter()
        existing_df = read_complete_partition(''')
replace_once('''            "fact_partition_committed: "''', '''            f"partition_committed: table={TABLE_NAME}; function=main; phase=fact_commit; status=completed; "''')
replace_once('''            f"invalid_ohlc_rows={partition_invalid_rows}"
''', '''            f"invalid_ohlc_rows={partition_invalid_rows}; calendar_state=pending; "
            f"elapsed_s={time.perf_counter() - log_fact_commit_started_at:.3f}"
''')
replace_once('''            f"api_success: partitions={processed_partitions}; "''', '''            f"api_result: table={TABLE_NAME}; function=main; phase=collect_summary; "
            f"status={'stopped' if quota_stop_message is not None else 'completed'}; partitions={processed_partitions}; "''')
replace_once('''            f"sessions={pending_count}; returned_rows={returned_rows}; "''', '''            f"planned_sessions={pending_count}; processed_sessions={log_processed_sessions}; returned_rows={returned_rows}; "''')
replace_once('''            f"invalid_ohlc_rows={invalid_ohlc_rows}; write=False"''', '''            f"invalid_ohlc_rows={invalid_ohlc_rows}; write=false"''')


def run_end(outcome, indent):
    return textwrap.indent('''click.echo(
    f"{log_boundary}\\n分钟运行结束 / Minute run ended\\n"
    "function=main()\\n"
    f"planning_progress: table={TABLE_NAME}; function=main; phase=run; "
    f"status={'stopped' if quota_stop_message is not None else 'completed'}; "
    f"outcome={OUTCOME}; mode={mode}; write={str(write).lower()}; "
    f"planned_sessions={pending_count}; processed_sessions={log_processed_sessions}; "
    f"elapsed_s={time.perf_counter() - log_started_at:.3f}\\n{log_boundary}"
)'''.replace('OUTCOME', outcome), ' ' * indent)


replace_once('''        return

    completion_updates_by_partition = {''', run_end("'quota_stopped' if quota_stop_message is not None else 'read_only'", 8) + '''
        return

    completion_updates_by_partition = {''')
replace_once('''        "committed: mode=automatic_gap_fill"
        if not has_explicit_dates
        else "committed: mode=explicit_non_formal"''', '''        f"committed: table={TABLE_NAME}; function=main; phase=commit_batch; status=completed; "
        f"mode={'explicit_non_formal' if has_explicit_dates else 'automatic_gap_fill'}; "
        f"quota_stopped={str(quota_stop_message is not None).lower()}"''')
replace_once('''    click.echo(
        f"completed_partitions={completed_partitions}; "''', '''    click.echo(
        f"planning_progress: table={TABLE_NAME}; function=main; phase=commit_summary; status=completed; "
        f"completed_partitions={completed_partitions}; "''')
replace_once('\n\n\nif __name__ == "__main__":', '\n' + run_end("'quota_stopped' if quota_stop_message is not None else 'committed'", 4) + '\n\n\nif __name__ == "__main__":')
cells['86251818'].source = source

updated_cells = []
for cell in notebook.cells:
    if cell.id in flow_specs:
        suffix, title, body = flow_specs[cell.id]
        updated_cells.append(flow(f'b06-flow-{suffix}', title, body))
    updated_cells.append(cell)
    if cell.id == '5c46936e':
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

    def visit_AugAssign(self, node):
        if isinstance(node.target, ast.Name) and node.target.id.startswith('log_'):
            return None
        return self.generic_visit(node)


old_code = [cell for cell in before.cells if cell.cell_type == 'code']
new_code = [cell for cell in notebook.cells if cell.cell_type == 'code']
assert [c.id for c in old_code] == [c.id for c in new_code]
for old, new in zip(old_code, new_code, strict=True):
    if old.id not in ('86251818', 'b31dc6c2'):
        assert old == new, old.id
    for key in ('metadata', 'outputs', 'execution_count'):
        assert old[key] == new[key], (old.id, key)
    assert ast.dump(RemoveLogging().visit(ast.parse(old.source))) == ast.dump(RemoveLogging().visit(ast.parse(new.source))), old.id
    comments = lambda value: [t.string for t in tokenize.generate_tokens(io.StringIO(value).readline) if t.type == tokenize.COMMENT]
    assert comments(old.source) == comments(new.source), old.id
    compile(new.source, str(PATH), 'exec')
assert len(new_code) == len(flow_specs)
assert notebook.metadata == before.metadata
nbformat.validate(notebook)
with PATH.open('w', encoding='utf-8', newline='\n') as handle:
    nbformat.write(notebook, handle)

readme = ROOT / '02_Futures_Lakehouse/README.md'
content = readme.read_text(encoding='utf-8')
old = '  共同一致时才属于完整下游格点；每个品种月分区成功后立即回写状态，遇配额边界正常停止。'
assert content.count(old) == 1
content = content.replace(old, '  共同一致时才属于完整下游格点。当前先逐个提交事实叶并汇集完成摘要，采集循环结束或配额停止后，\n  再按日历叶集中回写政策与成功事实的完成状态；普通异常直接停止，此前成功叶保留，尚未回写的完成状态不提前生效。')
readme.write_text(content, encoding='utf-8', newline='\n')
print(f'Updated {PATH.name}: {len(descriptions)} explanations, {len(flow_specs) + 1} diagrams; business AST and original comments unchanged.')
