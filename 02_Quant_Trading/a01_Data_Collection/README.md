# a01_Data_Collection

新数据采集系统。第一步建立交易日历，作为后续日频数据的时间锚点。

## 交易日历

`c01_dimension_trade_calendar.py` 只做四件事：

1. 读取已有日历；
2. 从 JQData 查询尚未保存的交易日期；
3. 为每个自然日生成 `is_trading_day` 标记；
4. 按统一 Arrow schema 写入单个 Parquet 文件。

北京时间 20:00 前默认更新至昨天，20:00 后更新至今天。输出位置：

```text
03_Futures_Database/futures_lake/silver/dim_trade_calendar.parquet
```

普通增量更新：

```powershell
python 02_Quant_Trading/a01_Data_Collection/c01_dimension_trade_calendar.py
```

指定日期或全量重建：

```powershell
python 02_Quant_Trading/a01_Data_Collection/c01_dimension_trade_calendar.py `
  --start-date 2010-01-01 `
  --end-date 2026-07-13 `
  --full-refresh
```

数据字段、Python 变量和跨引擎类型规范见
`03_Futures_Database/DATA_CONVENTIONS.md`。生产 schema 定义在
`config/data_contracts.py`，Parquet 写入必须先通过 schema 校验。
