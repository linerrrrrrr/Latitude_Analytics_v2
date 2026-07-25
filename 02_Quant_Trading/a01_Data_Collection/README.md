# 国内期货日历采集链路

三张表严格串行依赖：

```text
dim_trade_calendar
→ dim_futures_variety_calendar
→ dim_futures_contract_calendar
```

下游只更新到上游的最大日期，不读取系统日期决定自己的水位。每个脚本均支持：

- 目标不存在时从头创建；
- 默认从自身最大日期增量更新到上游最大日期；
- `--full-refresh` 删除逻辑旧结果并完整重建。

## 1. 全局交易日历

固定起点为 `2024-01-01`，默认终点按北京时间 20:00 截止规则确定：

```powershell
python 02_Quant_Trading/a01_Data_Collection/c01_dimension_trade_calendar.py
```

Hive 分区：

```text
dim_trade_calendar/year=YYYY/*.parquet
```

## 2. 品种日历

只使用全局交易日历中的交易日。每行表示某个交易所品种在某交易日存在有效固定合约：

```powershell
python 02_Quant_Trading/a01_Data_Collection/c02_dimension_futures_variety_calendar.py
```

唯一键：

```text
underlying_code + exchange_code + trading_date
```

Hive 分区：

```text
dim_futures_variety_calendar/exchange_code=.../year=YYYY/month=M/*.parquet
```

## 3. 合约日历

逐行读取品种日历，为每个品种交易日展开有效固定合约，并通过
`get_futures_info` 匹配该合约在该交易日唯一有效的交易时段规则：

```powershell
python 02_Quant_Trading/a01_Data_Collection/c03_dimension_futures_contract_calendar.py
```

粒度和唯一键：

```text
contract_code + trading_date + session_number
```

夜盘从上一交易日开始；跨午夜的凌晨段仍属于当前交易日。连续合约
`8888/9999` 不进入合约日历。

当前 JQData 账户只具备国内商品期货历史行情权限，因此品种日历在源头排除
中金所 `CCFX`；大商所、上期所、郑商所、能源中心和广期所进入链路。

Hive 分区：

```text
dim_futures_contract_calendar/exchange_code=.../year=YYYY/month=M/*.parquet
```

## 4. 期货日线

严格按照合约日历中的 `contract_code + trading_date` 获取日线。即使行情接口缺失，
也保留该键并将 `has_market_data` 标记为 `False`：

```powershell
python 02_Quant_Trading/a01_Data_Collection/c04_fact_futures_daily.py
```

Hive 分区：

```text
fact_futures_daily/exchange_code=.../year=YYYY/month=M/*.parquet
```

## 5. 期货分钟线

分钟时间戳 `bar_at` 表示自然日时间，`trading_date` 来自合约日历 session，
不能由 `bar_at.date()` 推断。JQData 分钟时间戳是分钟结束时间，匹配条件为：

```text
session_start_at < bar_at <= session_end_at
```

当前默认每个交易所抽取 1 个品种，并始终包含螺纹钢 `RB` 与铜 `CU`：

```powershell
python 02_Quant_Trading/a01_Data_Collection/c05_fact_futures_minute.py
```

Hive 分区：

```text
fact_futures_minute/
  exchange_code=.../underlying_code=.../year=YYYY/month=M/*.parquet
```

更新水位按“交易所 + 品种 + 年 + 月”判断；扩大抽样范围不会被其他品种的
最新日期阻挡。同月新增交易日时完整重写该品种月分区。

## 验证

```powershell
python scripts/verify_futures_calendar_pipeline.py
python scripts/verify_futures_minute_sample.py
```
