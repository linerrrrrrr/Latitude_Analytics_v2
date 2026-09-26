# 国内期货数据采集链路

本目录负责从 JQData 构建国内商品期货日历、拉取状态、日线和分钟线，并写入
`03_Futures_Database/futures_lake/silver`。数据起点统一为 `2010-01-01`。

规范索引：

- [根目录 AGENTS.md](../../AGENTS.md)：项目环境、根目录定位与规范路由。
- [量化交易 AGENTS.md](../AGENTS.md)：Notebook/Python 双轨及旧项目只读规则。
- [数据库 AGENTS.md](../../03_Futures_Database/AGENTS.md)：字段、类型、Schema 与读取规范。
- [特征工程 README](../a02_Feature_Engineering/README.md)：消费本目录 silver 表的主力连续合约与 gold 派生表规范。
- [可执行数据契约](../../config/data_contracts.py)：八张 silver 采集表及 gold 派生表的唯一 Arrow Schema。
- [数据湖读取 Demo](../../03_Futures_Database/read_futures_lake_demo.ipynb)：八张 silver 表与两张 gold 表各自独立的契约化读取示例。
- [数据采集系统重建蓝图](../a01_Data_Collection_Rebuild_Blueprint/README.md)：删除本目录现有实现后使用的目标表、中文 metadata、维度依赖、空库/增量更新、质检和实现步骤；当前目录尚未按该蓝图迁移。

## 工作流与共享模块

业务入口必须同时保留同名 `.ipynb` 与 `.py`；Notebook 是唯一可直接编辑的源文件：

```text
c01_dimension_trade_calendar.ipynb              ↔ c01_dimension_trade_calendar.py
c02_dimension_futures_variety_calendar.ipynb    ↔ c02_dimension_futures_variety_calendar.py
c03_dimension_futures_contract_calendar.ipynb   ↔ c03_dimension_futures_contract_calendar.py
c04_fact_futures_daily.ipynb                     ↔ c04_fact_futures_daily.py
c05_fact_futures_minute.ipynb                    ↔ c05_fact_futures_minute.py
c06_dimension_futures_session_schedule_signal.ipynb ↔ c06_dimension_futures_session_schedule_signal.py
c07_fact_futures_fetch_status.ipynb              ↔ c07_fact_futures_fetch_status.py
c08_fact_futures_missing_bar.ipynb               ↔ c08_fact_futures_missing_bar.py
```

`c00_*.py` 是共享库或运维入口，不要求同名 Notebook：

- `c00_futures_universe.py`：日线全品种与已确认 59 个分钟品种的唯一筛选配置。
- `c00_futures_fetch_control.py`：`build_fetch_requirements()`、`mark_fetch_completed()`、`detect_missing_bars()`。
- `c00_futures_contract_calendar.py`：合约 Session 日历的分区流式构建。
- `c00_futures_daily_fetch.py` / `c00_futures_minute_fetch.py`：配额感知的事实表拉取与分区写入。
- `c00_jqdata_connection.py`：认证与 Windows 直连出口。
- `c00_lakehouse.py`：Arrow/Hive 读写、流式复读校验和可恢复替换。
- `c00_sync_notebook_exports.py`：默认 PythonExporter 双轨同步。

双轨同步命令：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c00_sync_notebook_exports.py --check
```

## 数据来源与遍历依据

```text
get_trade_days
  → dim_trade_calendar
  → get_all_securities(list_date / delist_date)
  → dim_futures_variety_calendar
  → get_futures_info(trade_time / multiplier / tick_size)
  → dim_futures_contract_calendar
  → dim_futures_session_schedule_signal（推断/权威 Session 开闭市证据）
  → c00_futures_universe 业务选择 + 仅权威确认的拉取豁免
  → fact_futures_fetch_status
  → get_price(1d / 1m)
  → fact_futures_daily / fact_futures_minute
  → fact_futures_missing_bar + 回写状态表缺失字段
```

`get_futures_info` 不是判断某日有哪些合约的唯一来源。固定合约全集、上市日和退市日来自
`get_all_securities(["futures"])`；`get_futures_info` 只补充合约乘数、最小变动价位和按有效期变化的
`trade_time`。某交易日的候选合约由“品种日历中的交易日 + 上市/退市区间”确定；只有当天存在
有效 `trade_time` 规则时才生成 Session、日线合约日和拉取要求。候选区间内没有有效规则的合约日
不视为可交易，c03 会跳过并报告数量；Session 编号、起止时间和理论分钟数由有效规则展开。

日线不再遍历“所有合约 × 全历史日期”的矩形。`c04` 只读取状态表中的合约日键，按交易所/年份
分组，再根据每个合约的预期有效交易日数构造请求批次。每批目标最多 90,000 返回行、最多 200
个合约，使用 `skip_paused=True`，单次返回达到 1,000,000 行即停止；每次请求前要求保留默认
5,000,000 行日配额。JQData 连接保持单进程串行，不并发消耗配额。

## 品种范围

第一阶段执行全部维表与 87 个固定商品期货品种的全部日线；排除 CCFX，排除 `8888`、`9999`
连续代码。实际 87 数量由 2010 年以来上游元数据确定，日线业务条件是
`all_fixed_commodity_futures`，不会在代码里另列一份容易过期的 87 代码清单。

分钟线第一阶段不拉取。后续已确认 59 个品种，配置集中在 `c00_futures_universe.py`：

```text
GFEX: LC PD PS PT SI
XDCE: BB BZ EB EG FB I J JM L LG PG PP V
XINE: BC EC LU NR SC
XSGE: AD AG AL AO AU BR BU CU FU HC NI OP PB RB RU SN SP SS WR ZN
XZCE: CY FG MA ME PF PL PR PX SA SF SH SM TA TC UR ZC
```

`RB`、`CU` 以 `required` 原因优先；其余按能源、金属、工业分类。若将来某交易所在上述规则下
没有任何可用品种，则依次优先 GFEX `SI`、XDCE `I`、XINE `SC`、XSGE `RB`、XZCE `TA`；仍
不可用时取该交易所代码排序第一的固定品种，并记录 `exchange_fallback`。

## 状态与缺失语义

`fact_futures_fetch_status` 是“是否要拉取 / 是否已经拉取 / 是否缺失”的主表：

- 日线粒度：`bar_frequency + contract_code + trading_date + session_number`，其中日线固定
  `bar_frequency='1d'`、`session_number=0`。
- 分钟粒度：同一主键，`bar_frequency='1m'`，每个合约日 Session 一行。
- `is_fetch_required`：由上游日历、合约区间、分钟品种配置及权威确认的 Session 拉取豁免确定；
  `suspected_closed / inferred` 信号仍保持为真。
- `is_fetch_completed`：只有对应 API 范围完整执行、事实数据写入并复读校验成功后才为真；它与
  是否有行情是两个独立事实。
- `is_data_missing`：只对 `is_fetch_required=True AND is_fetch_completed=True` 的行检测；未拉取
  不能被误标为缺失。
- `actual_bar_count` / `missing_bar_count`：记录该日线键或 Session 的实际与缺失数量。

`fact_futures_missing_bar` 只保存真实缺失：日线用交易日零点（Asia/Shanghai）表示缺失日；分钟线
逐条保存 `(session_start_at, session_end_at]` 内缺少的具体分钟。不区分认证、网络、超时或其他 API
异常，也不把这些异常写入缺失表。

`dim_futures_session_schedule_signal` 单独保存 Session 开闭市证据。由“相邻交易日之间存在工作日
休市”派生的记录使用 `schedule_status='suspected_closed'`、`evidence_level='inferred'`、
`is_fetch_exempt=False`，不会阻止 API 拉取，也不会隐藏实际行情。只有依据交易所公告等权威来源写入
`evidence_level='authoritative'` 且 `is_fetch_exempt=True` 后，c07 才允许把对应 Session 的
`is_fetch_required` 设为假。旧 `dim_futures_session_exception_calendar` 不再作为生产输入。

## 八张 silver 采集表的粒度、主键与 Hive 分区

| 数据集 | 粒度 / 主键 | Hive 分区顺序 |
|---|---|---|
| `dim_trade_calendar` | `calendar_date` | `year` |
| `dim_futures_variety_calendar` | `underlying_code + exchange_code + trading_date` | `exchange_code / year / month` |
| `dim_futures_contract_calendar` | `contract_code + trading_date + session_number` | `exchange_code / year / month` |
| `dim_futures_session_schedule_signal` | `exchange_code + trading_date + session_start_at + session_end_at` | `exchange_code / year / month` |
| `fact_futures_fetch_status` | `bar_frequency + contract_code + trading_date + session_number` | `bar_frequency / exchange_code / year / month` |
| `fact_futures_missing_bar` | `bar_frequency + contract_code + trading_date + session_number + expected_bar_at` | `bar_frequency / exchange_code / underlying_code / year / month` |
| `fact_futures_daily` | `contract_code + trading_date` | `exchange_code / year / month` |
| `fact_futures_minute` | `contract_code + bar_at` | `exchange_code / underlying_code / year / month` |

`bar_frequency` 必须是状态表和缺失表的第一层 Hive 分区，使日线/分钟线可以在目录裁剪阶段完全
分离。所有生产写入都通过 `config/data_contracts.py` 的 Arrow Schema；分区或全表结果先写旁路目录，
流式复读类型与行数校验通过后再替换正式目录。旧正式目录在替换过程中暂存为同盘备份，失败会恢复。

## 第一阶段正式执行顺序

```powershell
E:\anaconda3\envs\latitude\python.exe 00_draft_collection_02/scripts/verify_runtime.py
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c01_dimension_trade_calendar.py --start-date 2010-01-01 --full-refresh
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c02_dimension_futures_variety_calendar.py --full-refresh
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c03_dimension_futures_contract_calendar.py --full-refresh
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c06_dimension_futures_session_schedule_signal.py
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c07_fact_futures_fetch_status.py
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c04_fact_futures_daily.py --full-refresh --dry-run
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c04_fact_futures_daily.py --full-refresh
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c08_fact_futures_missing_bar.py
E:\anaconda3\envs\latitude\python.exe 00_draft_collection_02/scripts/verify_futures_calendar_pipeline.py
```

第一阶段不运行 `c05_fact_futures_minute.py`。分钟阶段确认后，先执行：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c05_fact_futures_minute.py --dry-run
```

本项目的生产更新是半自动流程：由操作者手动执行下面的命令，程序在本次进程内按默认“仅未完成状态”
模式连续处理；不配置 LLM 心跳、定时任务、cron 或额度重置后的自动续跑。

日常日线增量更新与数据质检使用两个相互独立的批处理入口：

```powershell
& "E:\Latitude_Analytics_v2\02_Quant_Trading\a01_Data_Collection\run_futures_daily_update.bat"
& "E:\Latitude_Analytics_v2\02_Quant_Trading\a01_Data_Collection\run_futures_data_quality_check.bat"
```

`run_futures_daily_update.bat` 只更新交易日历、品种日历、合约日历（包含 `get_all_securities` 与
`get_futures_info` 元数据）、Session 信号、拉取状态和日线事实表，不执行预演、交互确认、缺失检测或
全链路验证。`run_futures_data_quality_check.bat` 独立执行缺失检测和全链路验证。两者均须由操作者手动
启动；通常先完成更新，再按需运行质检。

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c05_fact_futures_minute.py
```

需要人工优先处理指定品种时，可重复传入 `--target EXCHANGE.UNDERLYING`；例如只续拉上期所铜和
螺纹钢：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c05_fact_futures_minute.py --target XSGE.CU --target XSGE.RB
```

目标参数只缩小本次运行范围，不修改固定 59 品种宇宙或其他品种的应拉取状态。

每次 API 请求只覆盖一个交易所/品种/年/月分区，理论返回量不得
超过 900,000 行；请求前读取 JQData 剩余额度，并默认保留 5,000,000 行。余额不足以完整覆盖下一个
分区时正常停止，不会发起半分区请求。每个分区即使返回空集，也会先原子替换对应事实分区，再标记
对应 Session 完成；因此中断后可从未完成状态继续，不依赖模糊的全局最大日期水位，也不会把旧分区
残留误认为本次返回结果。额度重置后由操作者再次手动运行同一命令。

当 `--dry-run` 显示 `session_count: 0` 后，由操作者手动执行缺失检测和最终验证：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c08_fact_futures_missing_bar.py
E:\anaconda3\envs\latitude\python.exe 00_draft_collection_02/scripts/verify_futures_calendar_pipeline.py
```
