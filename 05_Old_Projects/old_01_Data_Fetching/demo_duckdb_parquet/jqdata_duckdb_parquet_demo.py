#!/usr/bin/env python
# coding: utf-8

# # jqdata_duckdb_parquet_demo
# 
# JQData 期货 Parquet 数据湖与 DuckDB 视图示例。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


EXCHANGE_CN_MAP = {
    "XSGE": "上期所",
    "XDCE": "大商所",
    "XZCE": "郑商所",
    "CCFX": "中金所",
    "XINE": "能源中心",
    "XGFE": "广期所",
    "GFEX": "广期所",
}

CONTRACT_COLUMNS = [
    "jq_code",
    "symbol",
    "variety",
    "exchange",
    "exchange_cn",
    "display_name",
    "name",
    "start_date",
    "end_date",
    "type",
    "is_active",
    "source",
    "updated_at",
]

DAILY_COLUMNS = [
    "trade_date",
    "jq_code",
    "symbol",
    "variety",
    "exchange",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "open_interest",
    "source",
    "updated_at",
    "year",
]


# In[ ]:


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch JQData futures data, write partitioned Parquet, and register DuckDB views."
    )
    parser.add_argument(
        "--lake-root",
        default="demo_duckdb_parquet/futures_lake",
        help="Output lake root. Default: demo_duckdb_parquet/futures_lake",
    )
    parser.add_argument(
        "--start-date",
        default="2026-01-01",
        help="Start date for futures daily bars.",
    )
    parser.add_argument(
        "--end-date",
        default="2026-01-10",
        help="End date for futures daily bars.",
    )
    parser.add_argument(
        "--symbols",
        default="CU2601.XSGE,AL2601.XSGE",
        help="Comma-separated JQData futures codes.",
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Use built-in mock data instead of calling JQData.",
    )
    parser.add_argument(
        "--skip-duckdb",
        action="store_true",
        help="Only write Parquet files and skip DuckDB view registration.",
    )
    return parser.parse_args()


# In[ ]:


def split_symbols(raw_symbols: str) -> list[str]:
    return [item.strip() for item in raw_symbols.split(",") if item.strip()]


# In[ ]:


def normalize_contracts(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.copy()
    if "jq_code" not in df.columns:
        df = df.rename_axis("jq_code").reset_index()

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["jq_code"] = df["jq_code"].astype(str)
    df["symbol"] = df["jq_code"].str.split(".").str[0]
    df["exchange"] = df["jq_code"].str.split(".").str[1]
    df["variety"] = df["symbol"].str.extract(r"^([A-Za-z]+)", expand=False).str.upper()
    df["exchange_cn"] = df["exchange"].map(EXCHANGE_CN_MAP).fillna(df["exchange"])

    for column in ["display_name", "name", "start_date", "end_date", "type"]:
        if column not in df.columns:
            df[column] = None

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.date
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce").dt.date
    df["is_active"] = pd.to_datetime(df["end_date"], errors="coerce") >= pd.Timestamp.today().normalize()
    df["source"] = "JQData"
    df["updated_at"] = now
    return df[CONTRACT_COLUMNS].sort_values(["exchange", "variety", "symbol"]).reset_index(drop=True)


# In[ ]:


def normalize_daily(raw: pd.DataFrame, fallback_symbols: Iterable[str]) -> pd.DataFrame:
    df = raw.copy()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    has_trade_date = "trade_date" in df.columns

    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    else:
        df = df.reset_index()

    rename_map = {
        "time": "trade_date",
        "date": "trade_date",
        "index": "trade_date",
        "code": "jq_code",
        "security": "jq_code",
    }
    if has_trade_date:
        rename_map.pop("index", None)
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    if "index" in df.columns:
        df = df.drop(columns=["index"])

    if "jq_code" not in df.columns:
        symbols = list(fallback_symbols)
        if len(symbols) != 1:
            raise ValueError("JQData daily result has no code column; pass one symbol or add jq_code before normalizing.")
        df["jq_code"] = symbols[0]

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["jq_code"] = df["jq_code"].astype(str)
    df["symbol"] = df["jq_code"].str.split(".").str[0]
    df["exchange"] = df["jq_code"].str.split(".").str[1]
    df["variety"] = df["symbol"].str.extract(r"^([A-Za-z]+)", expand=False).str.upper()

    for column in ["open", "high", "low", "close", "volume", "money", "open_interest"]:
        if column not in df.columns:
            df[column] = pd.NA

    df["source"] = "JQData"
    df["updated_at"] = now
    df["year"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.year.astype("Int64").astype(str)
    return df[DAILY_COLUMNS].sort_values(["jq_code", "trade_date"]).reset_index(drop=True)


# In[ ]:


def fetch_contracts_from_jqdata() -> pd.DataFrame:
    import jqdatasdk

    raw = jqdatasdk.get_all_securities(types=["futures"])
    return normalize_contracts(raw)


# In[ ]:


def fetch_daily_from_jqdata(symbols: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    import jqdatasdk

    fields = ["open", "high", "low", "close", "volume", "money", "open_interest"]
    raw = jqdatasdk.get_price(
        symbols,
        start_date=start_date,
        end_date=end_date,
        frequency="daily",
        fields=fields,
        panel=False,
    )
    return normalize_daily(raw, fallback_symbols=symbols)


# In[ ]:


def auth_jqdata_from_env() -> None:
    import jqdatasdk

    user = os.getenv("JQDATA_USER")
    password = os.getenv("JQDATA_PASSWORD")
    if not user or not password:
        raise RuntimeError("Set JQDATA_USER and JQDATA_PASSWORD, or run with --mock.")
    jqdatasdk.auth(user, password)


# In[ ]:


def mock_contracts(symbols: list[str]) -> pd.DataFrame:
    today = pd.Timestamp.today().normalize()
    rows = []
    for jq_code in symbols:
        symbol = jq_code.split(".")[0]
        variety = re.match(r"^[A-Za-z]+", symbol).group(0).upper()
        rows.append(
            {
                "jq_code": jq_code,
                "display_name": f"{variety} mock contract",
                "name": symbol,
                "start_date": today - pd.Timedelta(days=180),
                "end_date": today + pd.Timedelta(days=180),
                "type": "futures",
            }
        )
    return normalize_contracts(pd.DataFrame(rows))


# In[ ]:


def mock_daily(symbols: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    dates = pd.date_range(start_date, end_date, freq="B")
    rows = []
    for code_index, jq_code in enumerate(symbols):
        base_price = 50000 + code_index * 1000
        for day_index, date_value in enumerate(dates):
            close = base_price + day_index * 100 + code_index * 10
            rows.append(
                {
                    "trade_date": date_value,
                    "jq_code": jq_code,
                    "open": close - 50,
                    "high": close + 120,
                    "low": close - 160,
                    "close": close,
                    "volume": 10000 + day_index * 100,
                    "money": (10000 + day_index * 100) * close,
                    "open_interest": 80000 + day_index * 50,
                }
            )
    return normalize_daily(pd.DataFrame(rows), fallback_symbols=symbols)


# In[ ]:


def write_parquet_dataset(df: pd.DataFrame, base_dir: Path, partition_cols: list[str]) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    partition_schema = pa.schema([(column, pa.string()) for column in partition_cols])
    ds.write_dataset(
        table,
        base_dir=str(base_dir),
        format="parquet",
        partitioning=ds.partitioning(partition_schema, flavor="hive"),
        existing_data_behavior="delete_matching",
        basename_template="part-{i}.parquet",
    )


# In[ ]:


def register_duckdb_views(lake_root: Path) -> None:
    import duckdb

    db_path = lake_root / "futures.duckdb"
    dim_path = (lake_root / "silver" / "dim_futures_contract" / "**" / "*.parquet").as_posix()
    daily_path = (lake_root / "silver" / "fact_futures_daily" / "**" / "*.parquet").as_posix()
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW dim_futures_contract AS
            SELECT *
            FROM read_parquet('{dim_path}', hive_partitioning = 1);
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE VIEW fact_futures_daily AS
            SELECT *
            FROM read_parquet('{daily_path}', hive_partitioning = 1);
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_load_log (
                loaded_at TIMESTAMP,
                table_name VARCHAR,
                row_count BIGINT
            );
            """
        )
        con.execute(
            """
            INSERT INTO etl_load_log
            SELECT now(), 'dim_futures_contract', count(*) FROM dim_futures_contract
            UNION ALL
            SELECT now(), 'fact_futures_daily', count(*) FROM fact_futures_daily;
            """
        )
        print(con.execute("SELECT * FROM etl_load_log ORDER BY loaded_at DESC LIMIT 4").df())
        print(
            con.execute(
                """
                SELECT variety, min(trade_date) AS start_date, max(trade_date) AS end_date, count(*) AS rows
                FROM fact_futures_daily
                GROUP BY variety
                ORDER BY variety;
                """
            ).df()
        )
    finally:
        con.close()


# In[ ]:


def main() -> None:
    args = parse_args()
    symbols = split_symbols(args.symbols)
    lake_root = Path(args.lake_root)
    silver_root = lake_root / "silver"

    if args.mock:
        contracts = mock_contracts(symbols)
        daily = mock_daily(symbols, args.start_date, args.end_date)
    else:
        auth_jqdata_from_env()
        contracts = fetch_contracts_from_jqdata()
        daily = fetch_daily_from_jqdata(symbols, args.start_date, args.end_date)

    write_parquet_dataset(
        contracts,
        silver_root / "dim_futures_contract",
        partition_cols=["source"],
    )
    write_parquet_dataset(
        daily,
        silver_root / "fact_futures_daily",
        partition_cols=["source", "exchange", "variety", "year"],
    )

    print(f"Wrote {len(contracts)} contract rows")
    print(f"Wrote {len(daily)} daily rows")
    print(f"Lake root: {lake_root.resolve()}")

    if not args.skip_duckdb:
        register_duckdb_views(lake_root)


# In[ ]:


if __name__ == "__main__":
    main()

