# DuckDB + Parquet JQData Demo

这个 demo 展示一个轻量的数据湖设计：

- Parquet 是事实存储，按 Hive 风格目录分区。
- DuckDB 只保存视图和少量元数据，不重复存储大表。
- JQData 拉取层和落盘层解耦，方便以后替换数据源或加调度。

## 目录设计

```text
futures_lake/
  silver/
    dim_futures_contract/
      source=JQData/
        *.parquet
    fact_futures_daily/
      source=JQData/exchange=XSGE/variety=CU/year=2026/
        *.parquet
  futures.duckdb
```

建议：

- 维表：合同主数据体量小，可按 `source` 分区或不分区。
- 日线事实表：按 `source/exchange/variety/year` 分区，常见查询会按品种和日期裁剪。
- 分钟线事实表：建议再加 `month` 或 `trade_date` 分区，避免单目录文件过多。
- DuckDB view 用 `read_parquet(..., hive_partitioning=1)` 直接扫 Parquet。

## 安装依赖

```powershell
pip install -r demo_duckdb_parquet/requirements.txt
```

## 离线跑通

```powershell
python demo_duckdb_parquet/jqdata_duckdb_parquet_demo.py --mock
```

## 使用真实 JQData

```powershell
$env:JQDATA_USER="your_user"
$env:JQDATA_PASSWORD="your_password"
python demo_duckdb_parquet/jqdata_duckdb_parquet_demo.py --start-date 2026-01-01 --end-date 2026-01-10 --symbols CU2601.XSGE,AL2601.XSGE
```

如果只想测试 Parquet 写入、不创建 DuckDB 视图：

```powershell
python demo_duckdb_parquet/jqdata_duckdb_parquet_demo.py --mock --skip-duckdb
```
