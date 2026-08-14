# 国内期货特征工程链路

本目录消费 `.env` 的 `FUTURES_LAKE_ROOT` 下稳定的 `silver` 契约化行情，开展下游数据组织、
特征探索和复权方法探索。gold 不设全项目统一表集合或 Schema；某个实验需要落盘时，只在所属
工作流内定义和校验当前输出结构。业务代码应通过 `settings.futures_lake_root` 定位，不在本目录
保存 CSV、Parquet、模型或 Notebook 输出数据。

规范索引：

- [根目录 AGENTS.md](../../AGENTS.md)：项目环境、根目录定位与规范路由。
- [.env.template](../../.env.template)：正式湖仓根路径 `FUTURES_LAKE_ROOT` 的权威模板。
- [量化交易 AGENTS.md](../AGENTS.md)：Notebook/Python 双轨与 PythonExporter 规则。
- [数据库 AGENTS.md](../../03_Futures_Database/AGENTS.md)：字段、类型、Schema 与读取规范。
- [数据采集 README](../a01_Data_Collection/README.md)：18 张 silver 表的粒度、分区与运行边界。
- [数据采集系统重建蓝图](../a01_Data_Collection_Rebuild_Blueprint/README.md)：当前 silver 重建目标、字段与维度依赖。
- [可执行数据契约](../../config/data_contracts.py)：18 张稳定 silver 表的唯一 Arrow Schema。
- [数据湖读取 Demo](../../03_Futures_Database/read_futures_lake_demo.ipynb)：18 张稳定 silver 表的逐表契约化读取示例。

## b01 主力连续合约

业务入口：

```text
b01_main_continuous_daily.ipynb ↔ b01_main_continuous_daily.py
```

输入位于 silver 层：

| 数据集 | 用途 |
|---|---|
| `dim_futures_variety_calendar` | 确定每个交易所、品种的正式交易日序列 |
| `dim_futures_contract_calendar` | 获取真实合约存续状态与退市日，不解析合约代码猜测到期月份 |
| `fact_futures_daily` | 提供成交量、持仓量、收盘价及主力合约日线行情 |

`dim_futures_variety_calendar` 来自 `get_all_securities(["futures"], date=None)` 的完整固定月份合约目录，
不受分钟或逐品种报告事实白名单限制。当前实验实际可生成的品种范围由已存在的 `fact_futures_daily`
决定；交互候选也读取完整品种日历，不得把期货事实采集白名单当作日线或特征工程宇宙。
该策略的唯一权威来源是 [`config/futures_fact_collection_policy.py`](../../config/futures_fact_collection_policy.py)，
特征工程只识别其作用边界，不复制或维护白名单内容。

当前 b01 实验把以下结果写入 gold 层；名称、字段和分区是该实验的局部实现，不是数据库级永久契约：

| 数据集 | 粒度 / 主键 | Hive 分区顺序 | 语义 |
|---|---|---|---|
| `fact_futures_main_contract_daily` | `exchange_code + underlying_code + trading_date` | `exchange_code / underlying_code / year / month` | 日级主力映射、选择证据与换月 log-gap |
| `fact_futures_main_continuous_daily` | `exchange_code + underlying_code + trading_date` | `exchange_code / underlying_code / year / month` | 主力合约原始日线及 log 前、后复权序列 |

### 主力判定

- 只使用 `signal_trading_date` 已收盘的日线信息决定下一交易日 `trading_date` 的主力合约，
  不用当日成交量反向选择当日合约。
- 首个可用信号日以成交量最大合约初始化；成交量并列时依次按持仓量、较近退市日、合约代码
  确定唯一结果。
- 后续只允许向退市日更晚的合约滚动，防止主力回退到更早到期合约。
- 挑战合约成交量严格大于现主力的 `roll_trigger_ratio` 倍时，下一交易日发生换月；默认阈值为
  `1.10`。现主力无有效行情或不再存续时，选择成交量最大的后月有效合约。
- 若现主力在下一交易日不再存续，且信号日没有任何成交量为正的后月候选，则结束当前
  `continuity_segment`，不为缺乏流动性的日期编造主力合约。后续重新出现有效候选时启动新片段；
  log 复权因子不跨片段累计。
- 主力连续表中的成交量、成交额和持仓量均来自当日主力合约自身。全品种合约汇总量属于另一类
  市场活跃度特征，不与主力价格混存。

### log 双向复权

换月在交易日 `tau` 生效。优先使用信号日 `tau-1`，若该日任一合约缺少有效正收盘价，则向前
寻找最近一个旧、新合约都有有效正收盘价的共同交易日 `roll_anchor_date`：

```text
roll_log_gap[tau] = log(new_close[roll_anchor_date]) - log(old_close[roll_anchor_date])
```

令原始主力合约 log 价格为 `x[t]`：

```text
forward_adjusted_x[t]  = x[t] + sum(roll_log_gap[tau] for tau > t)
backward_adjusted_x[t] = x[t] - sum(roll_log_gap[tau] for tau <= t)
```

`forward_adjusted_*` 以最新端为价格锚，调整历史；`backward_adjusted_*` 以最早端为价格锚，
调整未来。两者在 log 空间只相差一个常数，因此收益率一致。原始 OHLC 和原始 log OHLC 同时保留，
便于审计；任何 `has_market_data=True` 的 OHLC 若非正数，流程直接失败，不静默制造 log 空值。

### 期限结构边界

期限结构使用同一时点各真实合约的原始价格。可以对每个合约独立取 log，或构造相对主力价差，
但不得套用主力连续序列的累计复权因子，因为跨合约价差、斜率与曲率正是期限结构信息。
`fact_futures_main_contract_daily` 可用于标注期限结构中的主力位置，不改写任何真实合约价格。

## 生成、检查与执行

Notebook 是唯一直接编辑的业务源文件。使用当前目录的同步入口生成或检查同名脚本：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/b00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/b00_sync_notebook_exports.py --check
```

Notebook 默认只显示交互控件，不自动读取行情；每次计算必须同时指定交易所和品种。导出的 `.py` 默认只计算和打印
该目标摘要，不写数据。日期筛选只允许预览；写入时重算该品种完整历史，并只替换该品种的年月分区：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/b01_main_continuous_daily.py --exchange-code XSGE --underlying-code CU --start-date 2024-01-01 --end-date 2024-12-31
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/b01_main_continuous_daily.py --exchange-code XSGE --underlying-code CU --write
```

Notebook 的最后一个单元格提供只读交互界面：交易所下拉框、可输入品种框、展示起止日期、换月阈值、
“只读计算”和“清空输出”按钮。计算后展示摘要、换月审计明细以及原始、前复权、后复权 log 收盘价图。
交互区不提供写入按钮。日期控件只裁剪展示结果；为保证区间起点的主力状态和前复权因子正确，后台只读取
所选单品种的完整历史，不读取其他品种。

当前实验的更新水位是“单品种完整历史、年月分区替换”。其输出 Schema 直接定义在
`b01_main_continuous_daily.ipynb` 中。每个目标分区先在旁路目录写入并通过该局部 Arrow Schema
校验，再以可恢复方式替换对应叶分区；流程只接受一个明确的目标品种。由于前复权依赖目标品种
后续全部换月 gap，写入时禁止日期截断。更细的增量水位需在后续任务中另行设计并验证。
