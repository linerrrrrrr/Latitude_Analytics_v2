#!/usr/bin/env python
# coding: utf-8

# # c07 疑似休市 Session 定向校对
# 
# 本环节读取本地 silver，以同一合约日的其他完整 Session 重聚合结果核对 JQData 日线，只向 `dim_futures_bar_calendar` 回写旁证、质量原因和审计时间。全部读取来自本地 Parquet，不调用行情 API。
# 
# | 上下游 | 数据职责与本环节的关系 |
# | --- | --- |
# | c03 合约日历 | `dim_futures_contract_calendar` 提供合约日的 `tick_size`；Session 结构经 c04 进入行情日历。 |
# | c04 行情日历 | 提供理论 Session、理论条数及状态载体；结构不变时保留下游状态，结构修订时按 c04 规则重建安全初态。 |
# | c05 日线事实 | `fact_futures_daily` 提供同合约日唯一日线及 `has_market_data`，供旁证比较。 |
# | c06 分钟事实与日历回写 | `fact_futures_minute` 提供其他 Session 的正式分钟值；完成的零行 Session 在日历中形成 `formal_empty_session` 候选。 |
# | c07 当前输出 | 更新完整日历叶中的候选行，保留未触达行；不修改日线或分钟事实。 |
# | c08 人工全量缺失检查 | 独立核对理论与事实分钟主键，保留 c07 旁证与质量原因，不调用本环节。 |
# 
# 四类比较全部一致才记录 `reconciled`。它表示强旁证，不能确认交易所休市：当前 Session 仍是 `suspected_closed`，`is_fetch_required` 与完成/缺失计数保持原值，质量仍为 `warning`。证据不足或比较不一致时记录 `inferred + warning`。
# 
# 运行语义见湖仓根目录 `README.md` 与 `02_Market_Data/a02_Lake/AGENTS.md`；字段、主键、分区和状态含义以 `config/data_contracts.py` 为准。下方 Schema 浏览器直接展示该可执行契约。

# ## 候选范围、运行方式与写入边界
# 
# 默认候选为 `bar_frequency=1m`、`is_fetch_required=true`、`schedule_status=suspected_closed`，且 `evidence_source=fact_futures_minute:formal_empty_session` 的行。校对并提交后证据来源变为 `c07:daily_vs_other_sessions:rule=c07-v3`，因此既有 c07 证据不会在日常重复校对；只读运行没有持久回写，下次仍可选中同一候选。
# 
# | 方式 | 候选范围 | 写入规则 |
# | --- | --- | --- |
# | 默认运行 | c06 尚未被 c07 覆盖的新空 Session 证据 | 不带 `--write` 只计算与预览；带 `--write` 回写完整日历叶。 |
# | 成对日期或合约范围 | 默认候选与指定范围的交集；日期和合约同时提供时也取交集 | 可只读检查；定向写正式湖必须同时带 `--force`。 |
# | 有界 `--force` | 指定范围内仍 required 且疑似休市的 Session，包含已有旁证 | 无论只读或写入、正式或临时湖，都必须给出成对日期或至少一个非空 `--contract-code`。 |
# 
# 日期必须成对且起始不晚于结束；合约可重复指定，入口去除空白、转大写并去重。`--write` 只控制提交。正式湖来自 `settings.futures_lake_root`，`--lake-root` 可指定独立临时湖。c07 没有独立日期水位文件，增量边界由日历证据来源表达。
# 
# operations 总控台的日常快捷选择在 c06 后包含本环节，操作者可取消勾选；也可单独选择本环节并配置原 CLI 参数。c08 不进入日常快捷选择，需人工单独勾选并显式确认。无候选时仅完成行情日历候选扫描，不打开合约、日线、分钟三张依赖表。

# ## 总流程：从空 Session 到持久旁证
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["启动；检查日期、合约及 force 写入范围"] --> B["扫描行情日历中的 required 疑似 Session"]
#     B --> C{"是否 force？"}
#     C -->|否| D["仅保留 c06 formal_empty_session 新证据"]
#     C -->|是| E["在显式范围内包含已有旁证"]
#     D --> F{"有候选？"}
#     E --> F
#     F -->|否| Z["无候选结束；不开其他三张表"]
#     F -->|是| G["按日历叶读取完整日历及相关合约、日线、分钟叶"]
#     G --> H["排除当前疑似 Session；聚合其他完整 Session"]
#     H --> I["与日线比较；生成旁证、warning 和审计时间"]
#     I --> J["汇总并预览；调度与完成状态保留"]
#     J --> K{"启用 write？"}
#     K -->|否| L["只读结束；旁证未持久化"]
#     K -->|是| M["完整 dirty 叶校验；staging 写入及复读"]
#     M --> N["一个共享事务内逐叶安装、正式叶复读"]
#     N --> O["整批成功；持久旁证供后续读取"]
#     N -. 安装或复读失败 .-> P["共享模块恢复本批已移动叶；保留失败现场并抛错"]
# ```

# ### 局部流程：环境与权威依赖
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["导入标准库"] --> B["从当前目录向上查找项目标记"]
#     B --> C{"找到项目根？"}
#     C -->|否| X["抛错停止"]
#     C -->|是| D["加入项目根与湖仓模块路径"]
#     D --> E["导入 DataFrame 库、四张 Schema、转换函数、settings 和共享事务"]
# ```

# In[1]:


from __future__ import annotations

import pathlib
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Market_Data/a01_Collection"))
        break
else:
    raise RuntimeError("未找到项目根目录")

import click
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from config.data_contracts import (
    FUTURES_BAR_CALENDAR_SCHEMA,
    FUTURES_CONTRACT_CALENDAR_SCHEMA,
    FUTURES_DAILY_SCHEMA,
    FUTURES_MINUTE_SCHEMA,
    arrow_to_pandas,
    pandas_to_arrow,
)
from config.settings import settings
from b00_04_staged_path_transaction import StagedPathTransaction


# ## Schema 契约与有界样例
# 
# 仅在交互式 Notebook 中展示四张直接依赖表的权威 Schema。浏览器从 `config/data_contracts.py` 读取定义；样例按用户选择有界读取本地湖。此单元格不执行旁证校对或数据提交，脚本运行时跳过展示。

# ### 局部流程：Schema 与样例展示
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A{"交互式 Notebook？"} -->|否| B["跳过展示"]
#     A -->|是| C["调用共享 Schema 浏览器"]
#     C --> D["展示四张权威 Schema；可选有界本地样例"]
# ```

# In[2]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from b00_03_notebook_schema_browser import display_schema_metadata

    display_schema_metadata([
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        FUTURES_BAR_CALENDAR_SCHEMA,
        FUTURES_DAILY_SCHEMA,
        FUTURES_MINUTE_SCHEMA,
    ], lake_root=settings.futures_lake_root)


# ## 表身份、分区和旁证规则
# 
# 表名、主键与分区列均从四张具名 Schema 的 metadata 初始化；日线和分钟事实的分区列必须相同。行情日历按 `bar_frequency/exchange_code/year/month`，合约日历按 `exchange_code/year/month`，事实表按 `exchange_code/underlying_code/year/month` 定向读取。
# 
# 持久标识 `c07-v3` 和 `c07:daily_vs_other_sessions:` 保持稳定。它们标识旁证规则与来源，不是输入指纹；日常不会据此重新证明全历史。
# 
# | 比较项 | 当前容差 |
# | --- | --- |
# | open / high / low / close | 每项绝对误差不超过 `tick_size / 2`，相对容差为 0。 |
# | volume、open_interest | 各自绝对容差 `1e-6`，相对容差为 0。 |
# | money | `np.isclose`：绝对容差 `0.01`，相对容差 `1e-6`。 |
# 
# 每次保存七项日线原值、七项分钟重聚合值和四类比较布尔值；证据不足时保留可取得的原值，比较布尔值留空。

# ### 局部流程：表身份与比较规则
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["从 Schema metadata 读取表名、主键、分区列"] --> B{"日线与分钟分区一致？"}
#     B -->|否| X["抛错停止"]
#     B -->|是| C["构造四张表的 Hive partitioning 与文件 Schema"]
#     C --> D["定义 c07-v3、四类比较容差和旁证列"]
#     D --> E["定义调度、证据和质量状态集合"]
# ```

# In[3]:


CALENDAR_TABLE_NAME = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
CONTRACT_TABLE_NAME = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"table_name"
].decode("utf-8")
DAILY_TABLE_NAME = FUTURES_DAILY_SCHEMA.metadata[b"table_name"].decode(
    "utf-8"
)
MINUTE_TABLE_NAME = FUTURES_MINUTE_SCHEMA.metadata[b"table_name"].decode(
    "utf-8"
)

CALENDAR_PRIMARY_KEY = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
CONTRACT_PRIMARY_KEY = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")
DAILY_PRIMARY_KEY = FUTURES_DAILY_SCHEMA.metadata[b"primary_key"].decode(
    "utf-8"
).split(",")
MINUTE_PRIMARY_KEY = FUTURES_MINUTE_SCHEMA.metadata[
    b"primary_key"
].decode("utf-8").split(",")

CALENDAR_PARTITION_COLUMNS = FUTURES_BAR_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
CONTRACT_PARTITION_COLUMNS = FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
DAILY_PARTITION_COLUMNS = FUTURES_DAILY_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
MINUTE_PARTITION_COLUMNS = FUTURES_MINUTE_SCHEMA.metadata[
    b"partition_columns"
].decode("utf-8").split(",")
if DAILY_PARTITION_COLUMNS != MINUTE_PARTITION_COLUMNS:
    raise RuntimeError("日线和分钟事实的 Hive 分区契约不一致。")
FACT_PARTITION_COLUMNS = DAILY_PARTITION_COLUMNS

CALENDAR_PARTITIONING = ds.partitioning(
    pa.schema(
        [
            FUTURES_BAR_CALENDAR_SCHEMA.field(name)
            for name in CALENDAR_PARTITION_COLUMNS
        ]
    ),
    flavor="hive",
)
CONTRACT_PARTITIONING = ds.partitioning(
    pa.schema(
        [
            FUTURES_CONTRACT_CALENDAR_SCHEMA.field(name)
            for name in CONTRACT_PARTITION_COLUMNS
        ]
    ),
    flavor="hive",
)
DAILY_PARTITIONING = ds.partitioning(
    pa.schema(
        [FUTURES_DAILY_SCHEMA.field(name) for name in FACT_PARTITION_COLUMNS]
    ),
    flavor="hive",
)
MINUTE_PARTITIONING = ds.partitioning(
    pa.schema(
        [FUTURES_MINUTE_SCHEMA.field(name) for name in FACT_PARTITION_COLUMNS]
    ),
    flavor="hive",
)

CALENDAR_FILE_SCHEMA = pa.schema(
    [field for field in FUTURES_BAR_CALENDAR_SCHEMA if field.name not in CALENDAR_PARTITION_COLUMNS],
    metadata=FUTURES_BAR_CALENDAR_SCHEMA.metadata,
)

CONTRACT_FILE_SCHEMA = pa.schema(
    [field for field in FUTURES_CONTRACT_CALENDAR_SCHEMA if field.name not in CONTRACT_PARTITION_COLUMNS],
    metadata=FUTURES_CONTRACT_CALENDAR_SCHEMA.metadata,
)

DAILY_FILE_SCHEMA = pa.schema(
    [field for field in FUTURES_DAILY_SCHEMA if field.name not in DAILY_PARTITION_COLUMNS],
    metadata=FUTURES_DAILY_SCHEMA.metadata,
)

MINUTE_FILE_SCHEMA = pa.schema(
    [field for field in FUTURES_MINUTE_SCHEMA if field.name not in MINUTE_PARTITION_COLUMNS],
    metadata=FUTURES_MINUTE_SCHEMA.metadata,
)

# 持久证据标识保持稳定；目录改名不改变规则版本。
EVIDENCE_RULE_VERSION = "c07-v3"
PRICE_TOLERANCE_FACTOR = 0.5
VOLUME_ATOL = 1e-6
MONEY_RTOL = 1e-6
MONEY_ATOL = 0.01
OPEN_INTEREST_ATOL = 1e-6

PRICE_METRICS = [  # 逐项使用 tick_size/2 容差比较的价格度量。
    "open",  # 开盘价。
    "high",  # 最高价。
    "low",  # 最低价。
    "close",  # 收盘价。
]
QUANTITY_METRICS = [  # 使用各自契约容差比较的数量/金额度量。
    "volume",  # 成交量，单位为手。
    "money",  # 成交额，单位为元。
    "open_interest",  # 收盘或末条有效持仓量，单位为手。
]
EVIDENCE_METRICS = [  # 日线原值与分钟重聚合值共同保存的七类度量。
    *PRICE_METRICS,  # 开、高、低、收四类价格。
    *QUANTITY_METRICS,  # 成交量、成交额、持仓量。
]
EVIDENCE_COLUMNS = [  # b07 写回行情日历的原值和四类比较结论。
    *[f"daily_{name}" for name in EVIDENCE_METRICS],  # JQData 日线原值旁证。
    *[f"aggregated_{name}" for name in EVIDENCE_METRICS],  # 其他完整 Session 的重聚合值。
    "ohlc_matches_daily",  # 四项价格是否全部在容差内匹配。
    "volume_matches_daily",  # 成交量是否匹配。
    "money_matches_daily",  # 成交额是否匹配。
    "open_interest_matches_daily",  # 末持仓量是否匹配。
]

SCHEDULE_STATUSES = {
    "scheduled",
    "suspected_closed",
    "confirmed_closed",
}
EVIDENCE_LEVELS = {
    "contract_rule",
    "inferred",
    "reconciled",
    "authoritative",
}
QUALITY_STATUSES = {
    "pending",
    "passed",
    "warning",
    "failed",
    "not_applicable",
}


# ## 待提交完整日历叶的业务校验
# 
# `validate_calendar_frame()` 在提交前校验每个 dirty 完整叶一次：先固定 Arrow 契约，再检查主键、状态枚举、分区派生值、Session 结构、完成凭证、缺失计数和旁证一致性，最后按主键排序返回。`reconciled` 必须仍属疑似休市，且四类比较均为真。
# 
# 这项校验由 `commit_calendar_partitions()` 调用；只读校对完成转换与旁证判断，不会调用提交前完整叶校验。staging 和正式安装后的复读检查文件契约、主键唯一性及行数，不再重复完整业务校验。
# 
# `validate_calendar_frame()` 自行报告转换、主键检查、逐行业务校验与排序的起止和失败阶段；沿原行遍历在首行及每 10000 行报告进度，不为日志另做全表扫描。
# 
# Pandas 输入先经 `pandas_to_arrow()` 完成契约校验，随后直接以 `pd.ArrowDtype` 转回 Pandas；不再立刻调用 `arrow_to_pandas()` 重做同一类型门禁。逐行固定说明字段和四项比较列在行循环前定义，全部业务校验保持原样。

# ### 局部流程：待提交叶业务校验
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录开始；按权威 Schema 转换完整叶"] --> B["检查主键唯一性"]
#     B --> C["检查枚举、派生值和 Session；沿原行遍历报告进度"]
#     C --> D["检查完成凭证、缺失计数和质量时间"]
#     D --> E["reconciled 必须疑似休市且四类比较为真"]
#     E --> F["排序；报告完成、行数与耗时"]
#     B -. 不满足约束 .-> X["报告失败阶段；原样抛错"]
#     C -. 不满足约束 .-> X
#     D -. 不满足约束 .-> X
#     E -. 不满足约束 .-> X
# ```

# In[4]:


def validate_calendar_frame(
    frame: pd.DataFrame,
    context: str,
) -> pd.DataFrame:
    log_started_at = time.perf_counter()
    log_phase = "conversion"
    log_checked_rows = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_frame; phase=validate_calendar; status=started; "
        f"context={context}; rows={len(frame)}"
    )
    try:
        # Arrow 转换先固定列顺序、类型、nullable 和全部中文 metadata。
        table = pandas_to_arrow(
            frame.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
            FUTURES_BAR_CALENDAR_SCHEMA,
        )
        checked_df = table.to_pandas(types_mapper=pd.ArrowDtype)

        log_phase = "primary_key"
        if checked_df.duplicated(CALENDAR_PRIMARY_KEY).any():
            raise ValueError(f"{context}主键不唯一。")

        log_phase = "business_rules"
        text_fields = ['schedule_signal_reason', 'evidence_source', 'selection_reason', 'quality_reason']
        comparison_columns = ['ohlc_matches_daily', 'volume_matches_daily', 'money_matches_daily', 'open_interest_matches_daily']
        for row in table.to_pylist():
            # 状态枚举与中文说明是可审计契约，不能用空字符串占位。
            if row["bar_frequency"] not in {"1d", "1m"}:
                raise ValueError(f"{context}bar_frequency 不在允许枚举中。")
            if row["schedule_status"] not in SCHEDULE_STATUSES:
                raise ValueError(f"{context}schedule_status 不在允许枚举中。")
            if row["evidence_level"] not in EVIDENCE_LEVELS:
                raise ValueError(f"{context}evidence_level 不在允许枚举中。")
            if row["quality_status"] not in QUALITY_STATUSES:
                raise ValueError(f"{context}quality_status 不在允许枚举中。")

            if any(not str(row[name]).strip() for name in text_fields):
                raise ValueError(f"{context}状态说明字段不得为空。")

            # 合约后缀和日期必须能复算 Hive 分区，避免写入错误叶目录。
            if not row["contract_code"].endswith(
                f".{row['exchange_code']}"
            ):
                raise ValueError(f"{context}交易所与合约代码后缀不一致。")
            if (
                row["year"] != row["trading_date"].year
                or row["month"] != row["trading_date"].month
            ):
                raise ValueError(
                    f"{context}year/month 与 trading_date 不一致。"
                )
            if row["expected_bar_count"] <= 0:
                raise ValueError(
                    f"{context}expected_bar_count 必须大于 0。"
                )
            if row["actual_bar_count"] < 0 or row["missing_bar_count"] < 0:
                raise ValueError(f"{context}实际与缺失条数不得为负。")

            # 日线只有保留编号 0；分钟线必须具有完整 Session 边界。
            if row["bar_frequency"] == "1d":
                session_values = [
                    row["session_text"],
                    row["session_start_at"],
                    row["session_end_at"],
                    row["is_night_session"],
                ]
                if row["session_number"] != 0:
                    raise ValueError(
                        f"{context}日线 session_number 必须为 0。"
                    )
                if any(value is not None for value in session_values):
                    raise ValueError(
                        f"{context}日线 Session 专属字段必须为空。"
                    )
                if row["expected_bar_count"] != 1:
                    raise ValueError(f"{context}日线理论条数必须为 1。")
            else:
                if row["session_number"] <= 0:
                    raise ValueError(
                        f"{context}分钟 session_number 必须大于 0。"
                    )
                if row["session_start_at"] >= row["session_end_at"]:
                    raise ValueError(
                        f"{context}分钟 Session 起点必须早于终点。"
                    )

            # 只有权威证据可以确认休市并免除拉取。
            if row["schedule_status"] == "confirmed_closed":
                if row["evidence_level"] != "authoritative":
                    raise ValueError(f"{context}确认休市必须具有权威证据。")
                if row["is_fetch_required"]:
                    raise ValueError(f"{context}确认休市不得继续要求拉取。")

            # 完成状态、运行批次和正式复读时间必须共同出现。
            if row["is_fetch_completed"]:
                if not row["fetch_run_id"] or row["fetch_completed_at"] is None:
                    raise ValueError(f"{context}完成状态缺少批次或完成时间。")
            elif row["fetch_completed_at"] is not None:
                raise ValueError(f"{context}未完成格点不得具有完成时间。")

            # 缺失状态由理论条数和正式事实实际条数直接复算。
            if row["missing_checked_at"] is None:
                if row["is_data_missing"] or row["missing_bar_count"] != 0:
                    raise ValueError(f"{context}未经检查不得记录缺失。")
            else:
                expected_missing = (
                    max(
                        row["expected_bar_count"]
                        - row["actual_bar_count"],
                        0,
                    )
                    if row["is_fetch_required"]
                    else 0
                )
                if row["missing_bar_count"] != expected_missing:
                    raise ValueError(
                        f"{context}missing_bar_count 无法复算。"
                    )
                if row["is_data_missing"] != (expected_missing > 0):
                    raise ValueError(
                        f"{context}is_data_missing 与缺失条数不一致。"
                    )

            if (
                row["quality_status"] != "pending"
                and row["quality_checked_at"] is None
            ):
                raise ValueError(f"{context}非 pending 状态缺少质检时间。")

            # reconciled 只能表达当前疑似 Session 的全部比较均一致。
            if row["evidence_level"] == "reconciled":
                if row["schedule_status"] != "suspected_closed":
                    raise ValueError(
                        f"{context}reconciled 只能用于疑似休市 Session。"
                    )
                if not all(row[name] is True for name in comparison_columns):
                    raise ValueError(
                        f"{context}reconciled 必须具有四类一致旁证。"
                    )
            log_checked_rows += 1
            if log_checked_rows == 1 or log_checked_rows % 10000 == 0:
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_frame; phase=business_rules; status=running; "
                    f"context={context}; checked_rows={log_checked_rows}; rows={len(frame)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )

        log_phase = "sort_output"
        validated_calendar_df = checked_df.sort_values(
            CALENDAR_PRIMARY_KEY
        ).reset_index(drop=True)
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_frame; phase=validate_calendar; status=completed; "
            f"context={context}; rows={len(validated_calendar_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return validated_calendar_df
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=validate_calendar_frame; phase=validate_calendar; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; context={context}; checked_rows={log_checked_rows}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## 数据集读取、物理契约与分区过滤
# 
# `open_exact_dataset()` 确认表存在，以 Hive 分区打开并检查逻辑 Schema；已有 `schema.parquet` 时检查其文件契约，不创建标记。`reconstructed_schema()` 按权威字段顺序重建逻辑 Schema，`physically_and_identity_compatible()` 比较字段、类型、nullable 以及表名/主键/分区身份，允许描述性 metadata 差异。
# 
# `partition_expression()` 把各分区列的等值条件以 AND 连接。每次物化前，`validate_read_fragments()` 只检查过滤条件可能读取的文件；随后由调用方读取数据。候选过滤减少返回行，但仍可能检查多个历史文件，不代表候选扫描完全没有历史文件 I/O。
# 
# 有候选后，入口按日历叶读取完整日历、对应合约月及候选品种月的日线/分钟事实。候选与输入表的主键唯一性由各自生产者保证，入口不再重复检查；自身 dirty 输出叶及 staging/正式叶的主键门禁仍保留。
# 
# `open_exact_dataset()` 自行报告路径发现、Dataset 打开与契约检查；`materialized=false` 表示尚未读取业务行。`validate_read_fragments()` 沿原文件遍历在首个及每 250 个文件报告进度，结束时报告实际检查数。实际 `to_table()` 仍在入口，读取起止与返回行数就在对应调用位置报告；不增加读取包装函数或额外 I/O。
# 
# 四张文件 Schema 从权威 Schema 和分区列在模块加载时派生，`validate_read_fragments()` 直接接收文件 Schema 与表名。候选先按日历分区建立位置索引，分区循环直接取本组；当前没有逐分区扫描全湖事实或重新证明全历史的检查，物化仍限定在当前日历叶、合约月及候选品种月。

# ### 局部流程：按调用边界读取数据
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["open_exact_dataset：记录开始；确认存在并打开"] --> B["重建逻辑 Schema；检查物理字段和稳定身份"]
#     B --> C["已有 schema.parquet 时检查文件契约"]
#     C --> D["报告打开完成；materialized=false"]
#     D --> E["候选一次分组；调用方构造当前分区条件"]
#     E --> F["validate_read_fragments：检查文件契约并报告数量进度"]
#     F --> G["调用方记录读取起止；to_table 返回后报告行数"]
#     B -. 不兼容 .-> X["抛错"]
#     C -. 不兼容 .-> X
#     F -. 不兼容 .-> X
# ```

# In[5]:


def reconstructed_schema(
    dataset: ds.Dataset,
    schema: pa.Schema,
) -> pa.Schema:
    try:
        fields = [dataset.schema.field(name) for name in schema.names]
    except KeyError as error:
        raise TypeError("数据集缺少契约字段。") from error

    return pa.schema(fields, metadata=dataset.schema.metadata)


SCHEMA_IDENTITY_METADATA_KEYS = [
    b"table_name",
    b"primary_key",
    b"partition_columns",
]


def physically_and_identity_compatible(
    actual_schema: pa.Schema,
    expected_schema: pa.Schema,
) -> bool:
    """忽略描述 metadata；硬检查物理字段和稳定表身份。"""
    if actual_schema.names != expected_schema.names:
        return False
    if any(
        actual_field.type != expected_field.type
        or actual_field.nullable != expected_field.nullable
        for actual_field, expected_field in zip(
            actual_schema,
            expected_schema,
            strict=True,
        )
    ):
        return False

    actual_metadata = actual_schema.metadata or {}
    expected_metadata = expected_schema.metadata or {}
    return all(
        actual_metadata.get(key) == expected_metadata.get(key)
        and expected_metadata.get(key) is not None
        for key in SCHEMA_IDENTITY_METADATA_KEYS
    )


def open_exact_dataset(
    table_path: pathlib.Path,
    partitioning: ds.Partitioning,
    schema: pa.Schema,
    label: str,
) -> ds.Dataset:
    log_started_at = time.perf_counter()
    log_phase = "discovery"

    click.echo(
        f"planning_progress: table={table_path.name}; function=open_exact_dataset; phase=dataset_open; status=started; "
        f"label={label}; path={table_path}"
    )
    try:
        first_parquet = (
            next(table_path.rglob("*.parquet"), None)
            if table_path.is_dir()
            else None
        )
        if first_parquet is None:
            raise FileNotFoundError(f"{label}不存在：{table_path}")

        log_phase = "dataset_open"
        dataset = ds.dataset(
            table_path,
            format="parquet",
            partitioning=partitioning,
        )
        log_phase = "logical_schema"
        actual_schema = reconstructed_schema(dataset, schema)
        if not physically_and_identity_compatible(actual_schema, schema):
            raise TypeError(f"{label} 物理结构或表身份与契约不一致。")

        log_phase = "schema_marker"
        marker_path = table_path / "schema.parquet"
        if marker_path.exists():
            expected_marker_schema = pa.schema(
                [
                    field
                    for field in schema
                    if field.name not in partitioning.schema.names
                ],
                metadata=schema.metadata,
            )
            if not physically_and_identity_compatible(
                pq.read_schema(marker_path),
                expected_marker_schema,
            ):
                raise TypeError(
                    f"{label} schema.parquet 物理结构或表身份与契约不一致。"
                )

        click.echo(
            f"planning_progress: table={table_path.name}; function=open_exact_dataset; phase=dataset_open; status=completed; "
            f"label={label}; path={table_path}; outcome=ready; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return dataset
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_path.name}; function=open_exact_dataset; phase=dataset_open; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def validate_read_fragments(
    dataset: ds.Dataset,
    filter_expression: ds.Expression | None,
    expected_file_schema: pa.Schema,
    table_name: str,
    label: str,
) -> None:
    log_started_at = time.perf_counter()
    log_phase = "fragment_schema"

    log_fragment_count = 0

    click.echo(
        f"planning_progress: table={table_name}; function=validate_read_fragments; phase=fragment_schema; status=started; "
        f"label={label}; filter={filter_expression}"
    )
    try:
        log_phase = "fragment_schema"
        for fragment in dataset.get_fragments(filter=filter_expression):
            if not physically_and_identity_compatible(
                fragment.physical_schema,
                expected_file_schema,
            ):
                raise TypeError(
                    f"{label}包含物理结构或表身份不兼容的 Parquet fragment："
                    f"{fragment.path}"
                )
            log_fragment_count += 1
            if log_fragment_count == 1 or log_fragment_count % 250 == 0:
                click.echo(
                    f"planning_progress: table={table_name}; function=validate_read_fragments; phase=fragment_schema; status=running; "
                    f"label={label}; checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        click.echo(
            f"planning_progress: table={table_name}; function=validate_read_fragments; phase=fragment_schema; status=completed; "
            f"label={label}; checked_fragments={log_fragment_count}; materialized=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={table_name}; function=validate_read_fragments; phase=fragment_schema; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; label={label}; checked_fragments={log_fragment_count}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


def partition_expression(
    partition_columns: list[str],
    partition_key: tuple[object, ...],
) -> ds.Expression:
    expression = None

    for column, value in zip(
        partition_columns,
        partition_key,
        strict=True,
    ):
        condition = ds.field(column) == value
        expression = (
            condition
            if expression is None
            else expression & condition
        )

    if expression is None:
        raise ValueError("分区键不得为空。")
    return expression


# ## 完整日历叶内的逐候选旁证计算
# 
# `reconcile_partition()` 复制完整日历叶，只更新传入主键集合中的候选。每个候选先清空旧旁证，再检查当前零行完成状态、当日唯一正 `tick_size`、唯一有效日线，以及其他 Session 的可用性。另有疑似 Session 时无法归因；权威确认休市的其他 Session 可以排除，其余必须都是 required、scheduled 且完整的 Session。
# 
# 保留分钟按 `bar_at` 排序：开盘取首条、收盘取末条、最高/最低取极值、成交量和成交额求和、持仓量取最后一个非空值。`invalid_ohlc_mask()` 识别有限 OHLC 跨列异常；这类原值由上游保留，但本环节不能据此升级为 `reconciled`。正式日线的有限数门禁由 c05 保证；本环节保留聚合结果的非有限检查，因为求和可能产生新的溢出。
# 
# | 判断 | 旁证与质量 |
# | --- | --- |
# | 证据不足、其他 Session 不完整或 OHLC 关系异常 | 保存可取得的原值，比较布尔值留空，记录 `inferred + warning` 及原因。 |
# | 证据完备，四类比较全部一致 | 保存比较结果，记录 `reconciled + warning`。 |
# | 证据完备，但至少一类比较不一致 | 保存真/假比较结果，记录 `inferred + warning` 及不一致项。 |
# 
# 函数返回更新后的完整叶、已处理候选子集和保留兼容性的固定第三返回值 0。候选的调度状态、拉取要求与完成/缺失计数不变；未触达行保持原值。`changed_candidates` 表示本次写入内存旁证和审计时间的候选数，尚不代表已落盘。
# 
# `reconcile_partition()` 自行报告候选选择、证据准备、聚合、比较和输出转换；在首个、每 100 个及最后一个候选报告累计进度。生成结果始终标记 `persisted=false`；失败记录候选、当前阶段与已处理数量并原样抛错。
# 
# 四份输入在候选循环前各按合约日建立位置索引，避免每个候选重复筛选当前叶全表；旁证来源字符串也在循环前构造。tick 已由读取契约固定为数值，仅保留缺值、唯一性、有限且为正的比较前提。当前疑似 Session 仍要求已完成且实际 0 行；其他保留 Session 仍要求已完成且实际条数等于理论条数，直接信任上游提交的计数，不从事实重数，也不再次复算同义的缺失标志。

# ### 局部流程：逐候选生成旁证
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["记录开始；选出候选并建立合约日位置索引"] --> B["按索引取同合约日数据；清空旧旁证"]
#     B --> C["检查零行完成状态、tick_size 和有效日线"]
#     C --> D["排除当前 Session 和其他权威休市 Session"]
#     D --> E["信任提交的完成状态与条数；检查旁证 OHLC"]
#     E --> F["聚合分钟；保存可取得的七项日线与聚合值"]
#     F -. 聚合结果非有限 .-> X["报告候选与失败阶段；抛错"]
#     F --> G{"证据与前提完备？"}
#     G -->|否| H["inferred + warning；比较值留空"]
#     G -->|是| I["按容差比较 OHLC、成交量、金额、持仓量"]
#     I --> J{"四类全部一致？"}
#     J -->|是| K["reconciled + warning"]
#     J -->|否| L["inferred + warning；记录不一致项"]
#     H --> M["更新内存证据；报告候选进度，persisted=false"]
#     K --> M
#     L --> M
#     M --> N{"还有候选？"}
#     N -->|是| B
#     N -->|否| O["转换排序后报告完成；返回完整叶、候选子集、0"]
# ```

# In[6]:


def invalid_ohlc_mask(frame: pd.DataFrame) -> pd.Series:
    # b07 不拒收上游事实，只识别不能用于通过旁证的有限 OHLC 跨列异常。
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)

    comparable_max = frame[["open", "close", "low"]].max(
        axis=1,
        skipna=True,
    )
    comparable_min = frame[["open", "close", "high"]].min(
        axis=1,
        skipna=True,
    )
    invalid_mask = (
        frame["high"].notna()
        & comparable_max.notna()
        & frame["high"].lt(comparable_max)
    ) | (
        frame["low"].notna()
        & comparable_min.notna()
        & frame["low"].gt(comparable_min)
    )
    return invalid_mask.fillna(False).astype(bool)


def reconcile_partition(
                calendar_df: pd.DataFrame,
                candidate_keys: set[tuple[object, ...]],
                contract_df: pd.DataFrame,
                daily_df: pd.DataFrame,
                minute_df: pd.DataFrame,
                checked_at: datetime,
            ) -> tuple[pd.DataFrame, pd.DataFrame, int]:
                log_started_at = time.perf_counter()
                log_phase = "candidate_selection"

                log_candidate_key = None
                log_processed_candidates = 0
                log_reconciled_candidates = 0

                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=reconcile_partition; phase=reconcile; status=started; "
                    f"calendar_rows={len(calendar_df)}; requested_candidates={len(candidate_keys)}; contract_rows={len(contract_df)}; daily_rows={len(daily_df)}; minute_rows={len(minute_df)}; persisted=false"
                )
                try:
                    updated_df = calendar_df.copy()
                    changed_keys: set[tuple[object, ...]] = set()

                    key_series = updated_df[CALENDAR_PRIMARY_KEY].apply(tuple, axis=1)
                    candidate_indices = updated_df.index[key_series.isin(candidate_keys)]

                    log_phase = "candidate_evidence"
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=reconcile_partition; phase=candidate_plan; status=completed; "
                        f"requested_candidates={len(candidate_keys)}; selected_candidates={len(candidate_indices)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    calendar_indices_by_day = updated_df.groupby(
                        ["bar_frequency", "contract_code", "trading_date"],
                        sort=False, dropna=False, observed=True,
                    ).indices
                    contract_day_columns = ["contract_code", "trading_date"]
                    contract_indices_by_day = contract_df.groupby(
                        contract_day_columns, sort=False, dropna=False, observed=True,
                    ).indices
                    daily_indices_by_day = daily_df.groupby(
                        contract_day_columns, sort=False, dropna=False, observed=True,
                    ).indices
                    minute_indices_by_day = minute_df.groupby(
                        contract_day_columns, sort=False, dropna=False, observed=True,
                    ).indices
                    # 显式规则版本只标识本次校对算法，不再承担输入指纹或自动重跑水位。
                    evidence_source = (
                        "c07:daily_vs_other_sessions:"
                        f"rule={EVIDENCE_RULE_VERSION}"
                    )
                    for index in candidate_indices:
                        row = updated_df.loc[index]
                        contract_code = row["contract_code"]
                        trading_date = row["trading_date"]
                        session_number = row["session_number"]
                        log_candidate_key = (contract_code, trading_date, session_number)
                        log_phase = "candidate_evidence"

                        # 同日 Session 状态决定哪些分钟可以作为“保留 Session”旁证。
                        contract_day_key = (contract_code, trading_date)
                        same_day_calendar_df = updated_df.iloc[
                            calendar_indices_by_day.get(("1m", *contract_day_key), [])
                        ]
                        other_calendar_df = same_day_calendar_df.loc[
                            same_day_calendar_df["session_number"].ne(session_number)
                        ].copy()

                        same_day_contract_df = contract_df.iloc[
                            contract_indices_by_day.get(contract_day_key, [])
                        ]
                        same_day_daily_df = daily_df.iloc[
                            daily_indices_by_day.get(contract_day_key, [])
                        ]
                        same_day_minute_df = minute_df.iloc[
                            minute_indices_by_day.get(contract_day_key, [])
                        ]


                        # 每轮先清空旧旁证，避免证据不足时保留上一轮的比较布尔值。
                        for evidence_column in EVIDENCE_COLUMNS:
                            updated_df.at[index, evidence_column] = None

                        issues: list[str] = []

                        # b07 只接受 b06 已完成、正式复读为 0 条的当前疑似 Session。
                        if not row["is_fetch_completed"]:
                            issues.append("当前疑似 Session 尚未完成正式事实复读")
                        if row["actual_bar_count"] != 0:
                            issues.append("当前疑似 Session 的正式实际条数不是 0")

                        # tick_size 在同一合约日的各 Session 中必须唯一且为正。
                        tick_sizes = same_day_contract_df["tick_size"].dropna().unique()
                        tick_size = None
                        if (
                            len(tick_sizes) == 1
                            and np.isfinite(tick_sizes[0])
                            and tick_sizes[0] > 0
                        ):
                            tick_size = float(tick_sizes[0])
                        else:
                            issues.append("当日 tick_size 缺失、不唯一或非正")

                        # 日线必须唯一且明确具有市场数据。
                        daily_row = None
                        if len(same_day_daily_df) != 1:
                            issues.append("JQData 日线事实缺失或不唯一")
                        elif not bool(same_day_daily_df.iloc[0]["has_market_data"]):
                            issues.append("JQData 日线没有有效市场数据")
                            daily_row = same_day_daily_df.iloc[0]
                        else:
                            daily_row = same_day_daily_df.iloc[0]

                        # 权威确认休市的其他 Session 可以排除；另一个疑似 Session 会使旁证不可归因。
                        uncertain_other_df = other_calendar_df.loc[
                            other_calendar_df["schedule_status"].eq(
                                "suspected_closed"
                            )
                        ]
                        if not uncertain_other_df.empty:
                            issues.append("同一合约日还存在其他疑似休市 Session")

                        retained_calendar_df = other_calendar_df.loc[
                            other_calendar_df["schedule_status"].eq("scheduled")
                            & other_calendar_df["is_fetch_required"].eq(True)
                        ].copy()
                        non_authoritative_other_df = other_calendar_df.loc[
                            other_calendar_df["schedule_status"].ne(
                                "confirmed_closed"
                            )
                        ]
                        if retained_calendar_df.empty:
                            issues.append("没有可用于重聚合的其他计划开市 Session")
                        if len(retained_calendar_df) != len(non_authoritative_other_df):
                            issues.append("其他非权威休市 Session 未全部处于需拉取计划状态")

                        retained_session_numbers = set(
                            retained_calendar_df["session_number"].tolist()
                        )
                        retained_minute_df = same_day_minute_df.loc[
                            same_day_minute_df["session_number"].isin(
                                retained_session_numbers
                            )
                        ].sort_values("bar_at")
                        invalid_retained_count = int(
                            invalid_ohlc_mask(retained_minute_df).sum()
                        )
                        if invalid_retained_count:
                            issues.append(
                                "其他完整 Session 包含 "
                                f"{invalid_retained_count} 条原始 OHLC 跨列关系异常"
                            )

                        # 每个保留 Session 使用上游正式提交的完成状态和条数证明完整。
                        for retained_row in retained_calendar_df.to_dict(
                            orient="records"
                        ):
                            retained_number = retained_row["session_number"]
                            if (
                                not retained_row["is_fetch_completed"]
                                or retained_row["actual_bar_count"]
                                != retained_row["expected_bar_count"]
                            ):
                                issues.append(
                                    f"其他 Session {retained_number} 未被正式状态与事实共同证明完整"
                                )

                        # 有多少证据就保存多少原值；比较结论只有在全部必要证据完备时才生成。
                        daily_values = {name: None for name in EVIDENCE_METRICS}
                        if daily_row is not None:
                            for name in EVIDENCE_METRICS:
                                raw_value = daily_row[name]
                                if pd.notna(raw_value):
                                    normalized_value = float(raw_value)
                                    daily_values[name] = normalized_value
                            if invalid_ohlc_mask(same_day_daily_df).any():
                                issues.append(
                                    "JQData 日线事实包含原始 OHLC 跨列关系异常"
                                )

                        log_phase = "aggregate_evidence"
                        aggregated_values = {
                            name: None for name in EVIDENCE_METRICS
                        }
                        if not retained_minute_df.empty:
                            open_interest_series = retained_minute_df[
                                "open_interest"
                            ].dropna()
                            aggregated_raw = {
                                "open": retained_minute_df.iloc[0]["open"],
                                "high": retained_minute_df["high"].max(),
                                "low": retained_minute_df["low"].min(),
                                "close": retained_minute_df.iloc[-1]["close"],
                                "volume": retained_minute_df["volume"].sum(
                                    min_count=1
                                ),
                                "money": retained_minute_df["money"].sum(
                                    min_count=1
                                ),
                                "open_interest": (
                                    open_interest_series.iloc[-1]
                                    if not open_interest_series.empty
                                    else None
                                ),
                            }
                            for name, raw_value in aggregated_raw.items():
                                if pd.notna(raw_value):
                                    normalized_value = float(raw_value)
                                    if not np.isfinite(normalized_value):
                                        raise ValueError(
                                            f"分钟直接依赖字段 {name} 包含非有限数。"
                                        )
                                    aggregated_values[name] = normalized_value

                        for name in EVIDENCE_METRICS:
                            updated_df.at[index, f"daily_{name}"] = daily_values[name]
                            updated_df.at[index, f"aggregated_{name}"] = (
                                aggregated_values[name]
                            )

                        missing_evidence = [
                            name
                            for name in EVIDENCE_METRICS
                            if daily_values[name] is None
                            or aggregated_values[name] is None
                        ]
                        if missing_evidence:
                            issues.append(
                                "必要日线或分钟证据缺值："
                                + ",".join(missing_evidence)
                            )

                        log_phase = "compare_evidence"
                        if issues:
                            # 证据不足或结构不完整时不猜测比较结果，也不升级证据等级。
                            updated_df.at[index, "evidence_level"] = "inferred"
                            updated_df.at[index, "quality_status"] = "warning"
                            updated_df.at[index, "quality_reason"] = (
                                "疑似休市定向校对证据不足："
                                + "；".join(dict.fromkeys(issues))
                                + "。仍保持疑似休市及拉取要求。"
                            )
                        else:
                            price_atol = tick_size * PRICE_TOLERANCE_FACTOR
                            ohlc_matches = all(
                                np.isclose(
                                    daily_values[name],
                                    aggregated_values[name],
                                    rtol=0.0,
                                    atol=price_atol,
                                )
                                for name in PRICE_METRICS
                            )
                            volume_matches = bool(
                                np.isclose(
                                    daily_values["volume"],
                                    aggregated_values["volume"],
                                    rtol=0.0,
                                    atol=VOLUME_ATOL,
                                )
                            )
                            money_matches = bool(
                                np.isclose(
                                    daily_values["money"],
                                    aggregated_values["money"],
                                    rtol=MONEY_RTOL,
                                    atol=MONEY_ATOL,
                                )
                            )
                            open_interest_matches = bool(
                                np.isclose(
                                    daily_values["open_interest"],
                                    aggregated_values["open_interest"],
                                    rtol=0.0,
                                    atol=OPEN_INTEREST_ATOL,
                                )
                            )

                            updated_df.at[index, "ohlc_matches_daily"] = bool(
                                ohlc_matches
                            )
                            updated_df.at[index, "volume_matches_daily"] = (
                                volume_matches
                            )
                            updated_df.at[index, "money_matches_daily"] = money_matches
                            updated_df.at[index, "open_interest_matches_daily"] = (
                                open_interest_matches
                            )

                            comparison_results = {
                                "OHLC": bool(ohlc_matches),
                                "volume": volume_matches,
                                "money": money_matches,
                                "open_interest": open_interest_matches,
                            }
                            mismatched_names = [
                                name
                                for name, is_matched in comparison_results.items()
                                if not is_matched
                            ]

                            if not mismatched_names:
                                updated_df.at[index, "evidence_level"] = "reconciled"
                                updated_df.at[index, "quality_status"] = "warning"
                                updated_df.at[index, "quality_reason"] = (
                                    "排除当前疑似 Session 后，其他完整 Session 重聚合与 "
                                    "JQData 日线一致；OHLC 容差为 tick_size/2，成交量和持仓量 "
                                    "绝对容差为 1e-6，成交额相对容差为 1e-6 且绝对容差为 "
                                    "0.01。该结论仅为 reconciled 旁证；当前 Session 仍缺失，"
                                    "质量状态保持 warning，不确认休市且不取消拉取。"
                                )
                            else:
                                updated_df.at[index, "evidence_level"] = "inferred"
                                updated_df.at[index, "quality_status"] = "warning"
                                updated_df.at[index, "quality_reason"] = (
                                    "排除当前疑似 Session 后，其他完整 Session 重聚合与 "
                                    "JQData 日线存在不一致："
                                    + ",".join(mismatched_names)
                                    + "。不能据此确认休市，仍保持拉取要求。"
                                )

                        # 本入口只写证据、质量和审计时间；调度状态与拉取要求保持原值。
                        updated_df.at[index, "evidence_source"] = evidence_source
                        updated_df.at[index, "quality_checked_at"] = checked_at
                        updated_df.at[index, "updated_at"] = checked_at
                        changed_keys.add(
                            tuple(row[name] for name in CALENDAR_PRIMARY_KEY)
                        )
                        log_processed_candidates += 1
                        log_reconciled_candidates += int(updated_df.at[index, "evidence_level"] == "reconciled")
                        if log_processed_candidates == 1 or log_processed_candidates % 100 == 0 or log_processed_candidates == len(candidate_indices):
                            click.echo(
                                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=reconcile_partition; phase=reconcile_candidate; status=running; "
                                f"candidate={log_candidate_key}; processed_candidates={log_processed_candidates}/{len(candidate_indices)}; reconciled={log_reconciled_candidates}; warning={log_processed_candidates}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                            )

                    log_phase = "output_conversion"
                    reconciled_table = pandas_to_arrow(
                        updated_df.loc[:, FUTURES_BAR_CALENDAR_SCHEMA.names],
                        FUTURES_BAR_CALENDAR_SCHEMA,
                    )
                    reconciled_df = reconciled_table.to_pandas(
                        types_mapper=pd.ArrowDtype,
                    ).sort_values(CALENDAR_PRIMARY_KEY).reset_index(drop=True)
                    if changed_keys:
                        checked_key_series = reconciled_df[CALENDAR_PRIMARY_KEY].apply(
                            tuple,
                            axis=1,
                        )
                        changed_df = reconciled_df.loc[
                            checked_key_series.isin(changed_keys)
                        ].copy()
                    else:
                        changed_df = reconciled_df.iloc[0:0].copy()

                    # 第三个返回值保留公开 helper 兼容性；不再执行历史指纹命中计数。
                    click.echo(
                        f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=reconcile_partition; phase=reconcile; status=completed; "
                        f"rows={len(reconciled_df)}; changed_candidates={len(changed_df)}; reconciled={log_reconciled_candidates}; warning={log_processed_candidates}; evidence_rule={EVIDENCE_RULE_VERSION}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    return reconciled_df, changed_df, 0
                except Exception as log_error:
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=reconcile_partition; phase=reconcile; status=failed; "
                        f"failed_phase={log_phase}; error={type(log_error).__name__}; candidate={log_candidate_key}; processed_candidates={log_processed_candidates}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    raise


# ## 完整叶暂存、共享安装与本批共同回滚
# 
# `commit_calendar_partitions()` 接受触达叶的全部行。空映射直接返回 0；非空时，各叶先做一次完整业务校验并确认分区归属，再将已校验 Arrow 叶直接拼接写入 staging。staging 检查当前完整 Schema/metadata，以及各叶主键唯一性和行数。
# 
# 所有暂存叶通过后，在一个共享 `StagedPathTransaction` 内按分区顺序备份旧叶、安装新叶，并仅打开刚安装的正式叶复读物理/身份契约、主键和行数。一次事务覆盖本次调用的全部日历叶；后一个叶失败时，前面已经安装并复读通过的叶也共同恢复。返回的 `committed_calendar_rows` 是提交完整叶的总行数，包含同叶保留行，不能当作候选数量。
# 
# 共享模块根据实际移动记录倒序恢复：隔离已安装的新叶，再恢复旧叶；原来不存在的叶恢复为不存在。第一次备份失败时尚未移动的旧叶保持原位。恢复成功清理 staging 和 backup，保留失败新叶的隔离目录；恢复不完整保留 backup 和隔离目录，并继续尝试恢复其余叶。异常报告现场路径，以原提交异常作为原因向上抛出。正式替换前的 staging 写入、验收或事务进入失败，只清理本次 staging 并抛错。
# 
# 本函数报告准备、暂存、安装与正式复读进度，共享模块报告失败恢复结果。单叶验收通过仍记 `batch_state=pending`；全部正式叶验收并成功退出共享事务后，才报告 `committed` 与 `phase=evidence_state; persisted=true`。空提交报告状态未变。旁证与质量判断仍由本环节负责，不交给共享模块。
# 
# c07 不写独立日期水位，也不修改根级 `schema.parquet`。共享模块使用同一文件系统内的路径替换；整批共同回滚不表示所有叶对其他读者原子可见，也不提供进程终止后的自动恢复或并发写入协调。

# ### 局部流程：暂存、共享安装与共同恢复
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["报告提交开始"] --> B{"有待提交分区？"}
#     B -->|否| C["报告状态未变；返回 0"]
#     B -->|是| D["逐叶业务校验；拼接 Arrow 叶写 staging"]
#     D --> E["复读 staging 契约、主键和行数"]
#     D -. 失败 .-> X["清理 staging；抛错"]
#     E -. 失败 .-> X
#     E --> F["进入一个共享事务；范围为本批全部日历叶"]
#     F -. 进入失败 .-> X
#     F --> G["共享模块备份、安装当前叶"]
#     G --> H["本函数复读当前正式叶；通过仍记 pending"]
#     H --> I{"全部叶通过？"}
#     I -->|否| G
#     I -->|是| J["成功退出事务并清理；报告旁证落盘"]
#     G -. 失败 .-> R["倒序隔离新叶并恢复本批旧叶；逐项尝试恢复"]
#     H -. 失败 .-> R
#     R --> S{"恢复完整？"}
#     S -->|是| T["清理 backup；保留失败新叶；抛错"]
#     S -->|否| U["保留 backup 和失败新叶；报告现场并抛错"]
# ```

# In[7]:


def commit_calendar_partitions(
    partition_frames: dict[tuple[object, ...], pd.DataFrame],
    lake_root: pathlib.Path,
) -> int:
    log_started_at = time.perf_counter()
    log_phase = "validate_output"

    log_prepared_partitions = 0
    log_staged_partitions = 0
    log_verified_partitions = 0

    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit; status=started; "
        f"partitions={len(partition_frames)}; lake_root={lake_root}"
    )
    try:
        if not partition_frames:
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit; status=completed; "
                f"outcome=no_partitions; committed_partitions=0; committed_calendar_rows=0; evidence_state=unchanged; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            return 0

        expected_tables: dict[tuple[object, ...], pa.Table] = {}

        for partition_key, frame in partition_frames.items():
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=prepare_leaf; status=started; "
                f"partition={partition_key}; rows={len(frame)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            checked_df = validate_calendar_frame(
                frame,
                "待提交完整日历分区",
            )
            actual_partition_keys = set(
                checked_df[CALENDAR_PARTITION_COLUMNS].itertuples(
                    index=False,
                    name=None,
                )
            )
            if actual_partition_keys != {partition_key}:
                raise ValueError("待提交日历数据越出指定 Hive 分区。")

            expected_tables[partition_key] = pandas_to_arrow(
                checked_df,
                FUTURES_BAR_CALENDAR_SCHEMA,
            )
            log_prepared_partitions += 1
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=prepare_leaf; status=completed; "
                f"partition={partition_key}; prepared_partitions={log_prepared_partitions}/{len(partition_frames)}; rows={len(checked_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        log_phase = "combine_output"
        combined_table = pa.concat_tables(list(expected_tables.values()))

        log_phase = "prepare_paths"
        silver_root = lake_root.resolve() / "silver"
        target_path = silver_root / CALENDAR_TABLE_NAME
        run_id = uuid.uuid4().hex
        staging_path = silver_root / (
            f".{CALENDAR_TABLE_NAME}.staging-{run_id}"
        )
        backup_path = silver_root / (
            f".{CALENDAR_TABLE_NAME}.backup-{run_id}"
        )
        quarantine_path = silver_root / (
            f".{CALENDAR_TABLE_NAME}.failed-{run_id}"
        )

        silver_root.mkdir(parents=True, exist_ok=True)
        for managed_path in (
            target_path,
            staging_path,
            backup_path,
            quarantine_path,
        ):
            if not managed_path.resolve().is_relative_to(silver_root):
                raise ValueError(
                    f"数据集路径越出 silver 根目录：{managed_path}"
                )

        # 第一阶段：写 staging，并逐叶复核当前完整契约、主键和行数摘要。
        log_phase = "staging_write"
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=staging_write; status=started; "
            f"run_id={run_id}; partitions={len(expected_tables)}; rows={len(combined_table)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        staging_path.mkdir(parents=True, exist_ok=False)
        try:
            ds.write_dataset(
                combined_table,
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

            log_phase = "staging_schema"
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=staging_write; status=completed; "
                f"run_id={run_id}; rows={len(combined_table)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            for parquet_path in staging_path.rglob("*.parquet"):
                actual_file_schema = pq.read_schema(parquet_path)
                if not actual_file_schema.equals(
                    CALENDAR_FILE_SCHEMA,
                    check_metadata=True,
                ):
                    raise TypeError(
                        "staging Parquet 文件 Schema/metadata 与契约不一致："
                        f"{parquet_path}"
                    )

            staged_dataset = ds.dataset(
                staging_path,
                format="parquet",
                partitioning=CALENDAR_PARTITIONING,
            )
            if not reconstructed_schema(
                staged_dataset,
                FUTURES_BAR_CALENDAR_SCHEMA,
            ).equals(FUTURES_BAR_CALENDAR_SCHEMA, check_metadata=True):
                raise TypeError(
                    "staging 数据集 Schema/metadata 与契约不一致。"
                )

            log_phase = "staging_readback"
            click.echo(
                f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=staging_readback; status=started; "
                f"run_id={run_id}; partitions={len(expected_tables)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            for partition_key, expected_table in expected_tables.items():
                staged_primary_key_table = staged_dataset.to_table(
                    columns=CALENDAR_PRIMARY_KEY,
                    filter=partition_expression(
                        CALENDAR_PARTITION_COLUMNS,
                        partition_key,
                    ),
                )
                staged_primary_key_df = staged_primary_key_table.to_pandas()
                if staged_primary_key_df.duplicated(
                    CALENDAR_PRIMARY_KEY
                ).any():
                    raise ValueError("staging 日历叶主键不唯一。")
                if len(staged_primary_key_df) != len(expected_table):
                    raise ValueError("staging 日历叶行数摘要不一致。")
                log_staged_partitions += 1
                click.echo(
                    f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=staging_readback; status=running; "
                    f"run_id={run_id}; partition={partition_key}; checked_partitions={log_staged_partitions}/{len(expected_tables)}; rows={len(staged_primary_key_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                )
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=staging_readback; status=completed; "
            f"run_id={run_id}; checked_partitions={log_staged_partitions}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        log_phase = "prepare_install"
        # 第二阶段：保存旧叶分区，移动已验证分区，再精确复核刚触达的正式叶。
        try:
            with StagedPathTransaction(
                root_path=target_path,
                staging_dir=staging_path,
                backup_dir=backup_path,
                quarantine_dir=quarantine_path,
                log_context=(
                    f"table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; "
                    f"run_id={run_id}"
                ),
            ) as transaction:
                for partition_key in sorted(expected_tables):
                    log_phase = "install"
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=install; status=started; "
                        f"run_id={run_id}; partition={partition_key}; verified_partitions={log_verified_partitions}/{len(expected_tables)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    relative_path = pathlib.Path(
                        *[
                            f"{name}={value}"
                            for name, value in zip(
                                CALENDAR_PARTITION_COLUMNS,
                                partition_key,
                                strict=True,
                            )
                        ]
                    )
                    source_path = staging_path / relative_path
                    destination_path = target_path / relative_path

                    if not source_path.is_dir():
                        raise FileNotFoundError(
                            f"staging 缺少日历叶分区：{relative_path}"
                        )

                    transaction.replace(
                        target_path=destination_path,
                        staged_path=source_path,
                    )
                    log_phase = "formal_readback"
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=formal_readback; status=started; "
                        f"run_id={run_id}; partition={partition_key}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
                    committed_dataset = ds.dataset(
                        destination_path,
                        format="parquet",
                        partitioning=CALENDAR_PARTITIONING,
                        partition_base_dir=str(target_path),
                    )
                    validate_read_fragments(
                        committed_dataset,
                        None,
                        CALENDAR_FILE_SCHEMA,
                        CALENDAR_TABLE_NAME,
                        "正式日历叶",
                    )
                    committed_schema = reconstructed_schema(
                        committed_dataset,
                        FUTURES_BAR_CALENDAR_SCHEMA,
                    )
                    if not physically_and_identity_compatible(
                        committed_schema, FUTURES_BAR_CALENDAR_SCHEMA
                    ):
                        raise TypeError(
                            "正式日历叶物理结构或表身份与契约不一致。"
                        )
                    committed_primary_key_table = committed_dataset.to_table(
                        columns=CALENDAR_PRIMARY_KEY,
                    )
                    committed_primary_key_df = committed_primary_key_table.to_pandas()
                    if committed_primary_key_df.duplicated(
                        CALENDAR_PRIMARY_KEY
                    ).any():
                        raise ValueError("正式日历叶主键不唯一。")
                    if len(committed_primary_key_df) != len(
                        expected_tables[partition_key]
                    ):
                        raise ValueError("正式日历叶行数与 staging 摘要不一致。")
                    log_verified_partitions += 1
                    click.echo(
                        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=formal_readback; status=completed; "
                        f"run_id={run_id}; partition={partition_key}; verified_partitions={log_verified_partitions}/{len(expected_tables)}; rows={len(committed_primary_key_df)}; batch_state=pending; elapsed_s={time.perf_counter() - log_started_at:.3f}"
                    )
        except Exception:
            # 事务进入前的异常也需清理 staging；进入后的恢复由共享模块负责。
            shutil.rmtree(staging_path, ignore_errors=True)
            raise

        click.echo(
            f"committed: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit; status=completed; "
            f"write=true; committed_partitions={len(expected_tables)}; committed_calendar_rows={len(combined_table)}; run_id={run_id}; persisted=true; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=evidence_state; status=completed; "
            f"scope=committed_calendar_leaves; partitions={len(expected_tables)}; run_id={run_id}; persisted=true; date_watermark=none; default_candidate_source=fact_futures_minute:formal_empty_session; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        return len(combined_table)
    except Exception as log_error:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=commit_calendar_partitions; phase=commit; status=failed; "
            f"failed_phase={log_phase}; error={type(log_error).__name__}; prepared_partitions={log_prepared_partitions}; verified_partitions={log_verified_partitions}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        raise


# ## CLI 调度、结果预览与运行日志
# 
# `main()` 依次完成参数门禁、候选扫描、依赖表定向读取、逐叶校对、最多 20 行预览，再按 `--write` 决定是否提交完整日历叶。无候选直接结束；只读计算完成与正式提交成功使用不同结果状态。
# 
# 日志沿用 c01、c02 的 88 个 `=` 运行边界，使用 `table/function/phase/status` 和秒数 `elapsed_s`。`reconciliation_plan:` 报告候选及旁证数量，`planning_progress:` 报告阶段与运行结果，`committed:` 由提交函数在本批验收并成功退出共享事务后输出；这些前缀均由现有 worker 识别。`reconciled` 是 `warning` 的子集，两项计数不能相加。
# 
# 读取函数、业务校验、旁证生成和提交函数报告各自起止、进度及失败阶段。入口保留参数门禁、实际数据物化、整批调度、累计计数、预览与运行结果；不重复报告函数级生成或提交起止。普通异常继续向上抛出，不输出成功结束日志。此单元格只定义 Click 命令；后面的独立执行单元格区分 Notebook、直接运行脚本和模块导入。

# ### 局部流程：入口调度与日志
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["Click 参数门禁；运行开始日志"] --> B["只打开行情日历并过滤候选"]
#     B --> C{"有候选？"}
#     C -->|否| D["报告 0 候选；结束日志"]
#     C -->|是| E["打开三张依赖表；按日历叶读取相关分区"]
#     E --> F["调用生成函数；汇集结果并报告整批进度"]
#     F --> G["报告旁证计数；预览最多 20 行"]
#     G --> H{"write？"}
#     H -->|否| I["报告跳过提交；只读结束日志"]
#     H -->|是| J["调用 commit_calendar_partitions；函数自行报告"]
#     J --> K["提交返回后只报告运行结束"]
#     J -. 异常 .-> L["异常向上抛出；无成功结束日志"]
# ```

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--contract-code", multiple=True)
@click.option("--force", is_flag=True)
@click.option("--write", is_flag=True)
def main(
    lake_root: pathlib.Path | None,
    start_date: datetime | None,
    end_date: datetime | None,
    contract_code: tuple[str, ...],
    force: bool,
    write: bool,
) -> None:
    # 1. 正式路径只来自 settings；显式范围门禁先于任何数据集读取。
    formal_lake_root = settings.futures_lake_root.resolve()
    resolved_lake_root = (lake_root or formal_lake_root).resolve()

    if (start_date is None) != (end_date is None):
        raise click.UsageError(
            "--start-date 与 --end-date 必须同时提供。"
        )

    requested_start_date = (
        start_date.date() if start_date is not None else None
    )
    requested_end_date = (
        end_date.date() if end_date is not None else None
    )
    if (
        requested_start_date is not None
        and requested_start_date > requested_end_date
    ):
        raise click.BadParameter("起始日期不得晚于结束日期。")

    requested_contract_codes = tuple(
        dict.fromkeys(
            value.strip().upper()
            for value in contract_code
            if value.strip()
        )
    )
    has_explicit_scope = (
        requested_start_date is not None
        or bool(requested_contract_codes)
    )
    if force and not has_explicit_scope:
        raise click.UsageError(
            "使用 --force 时必须同时提供日期范围或 "
            "--contract-code，以限制重算范围。"
        )
    if (
        write
        and resolved_lake_root == formal_lake_root
        and has_explicit_scope
        and not force
    ):
        raise click.UsageError(
            "正式湖的日期或合约定向写入必须同时使用 --force；"
            "普通 --write 只处理默认新增候选。"
        )

    log_started_at = time.perf_counter()
    log_boundary = "=" * 88
    log_mode = "force" if force else "new_candidates"
    click.echo(
        f"{log_boundary}\n疑似休市校对开始 / Reconciliation run started\n"
        "function=main()\n"
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=run; status=started; "
        f"mode={log_mode}; write={str(write).lower()}; lake_root={resolved_lake_root}; "
        f"start_date={requested_start_date}; end_date={requested_end_date}; "
        f"contract_codes={requested_contract_codes}\n{log_boundary}"
    )
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; "
        "phase=candidate_scan; status=started"
    )
    silver_root = resolved_lake_root / "silver"
    calendar_path = silver_root / CALENDAR_TABLE_NAME
    contract_path = silver_root / CONTRACT_TABLE_NAME
    daily_path = silver_root / DAILY_TABLE_NAME
    minute_path = silver_root / MINUTE_TABLE_NAME

    # 2. 先只打开行情日历并扫描需拉取的疑似 Session。
    calendar_dataset = open_exact_dataset(
        calendar_path,
        CALENDAR_PARTITIONING,
        FUTURES_BAR_CALENDAR_SCHEMA,
        "行情日历",
    )
    candidate_filter = (
        (ds.field("bar_frequency") == "1m")
        & (ds.field("is_fetch_required") == True)
        & (ds.field("schedule_status") == "suspected_closed")
    )
    if not force:
        candidate_filter &= (
            ds.field("evidence_source")
            == "fact_futures_minute:formal_empty_session"
        )
    if requested_start_date is not None:
        candidate_filter = (
            candidate_filter
            & (ds.field("trading_date") >= requested_start_date)
            & (ds.field("trading_date") <= requested_end_date)
        )
    if requested_contract_codes:
        candidate_filter = candidate_filter & ds.field(
            "contract_code"
        ).isin(list(requested_contract_codes))

    validate_read_fragments(
        calendar_dataset,
        candidate_filter,
        CALENDAR_FILE_SCHEMA,
        CALENDAR_TABLE_NAME,
        "行情日历候选扫描",
    )
    calendar_columns = FUTURES_BAR_CALENDAR_SCHEMA.names
    candidate_table = calendar_dataset.to_table(
        columns=calendar_columns,
        filter=candidate_filter,
    )
    candidate_df = arrow_to_pandas(
        candidate_table,
        FUTURES_BAR_CALENDAR_SCHEMA,
    )

    if candidate_df.empty:
        click.echo(
            f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=main; "
            "phase=candidate_scan; status=completed; selected_candidates=0; changed_candidates=0; "
            "committed_partitions=0"
        )
        click.echo(
            f"{log_boundary}\n疑似休市校对结束 / Reconciliation run ended\n"
            "function=main()\n"
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome=no_candidates; mode={log_mode}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        return

    click.echo(
        f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=main; "
        f"phase=candidate_scan; status=completed; selected_candidates={len(candidate_df)}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    # 3. 有候选时再打开三张直接依赖表；要求物理结构和稳定表身份兼容。
    contract_dataset = open_exact_dataset(
        contract_path,
        CONTRACT_PARTITIONING,
        FUTURES_CONTRACT_CALENDAR_SCHEMA,
        "合约 Session 日历",
    )
    daily_dataset = open_exact_dataset(
        daily_path,
        DAILY_PARTITIONING,
        FUTURES_DAILY_SCHEMA,
        "JQData 日线事实",
    )
    minute_dataset = open_exact_dataset(
        minute_path,
        MINUTE_PARTITIONING,
        FUTURES_MINUTE_SCHEMA,
        "JQData 分钟事实",
    )

    checked_at = datetime.now(timezone.utc)
    partition_frames: dict[
        tuple[object, ...], pd.DataFrame
    ] = {}
    changed_rows: list[pd.DataFrame] = []

    candidate_indices_by_partition = candidate_df.groupby(
        CALENDAR_PARTITION_COLUMNS, sort=False, dropna=False, observed=True,
    ).indices
    calendar_partition_keys = sorted(candidate_indices_by_partition)
    contract_columns = FUTURES_CONTRACT_CALENDAR_SCHEMA.names
    daily_columns = FUTURES_DAILY_SCHEMA.names
    minute_columns = FUTURES_MINUTE_SCHEMA.names

    # 4. 每个日历叶分区只读取对应合约月和候选品种月事实。
    log_processed_partitions = 0
    click.echo(
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=reconcile_batch; status=started; "
        f"planned_partitions={len(calendar_partition_keys)}; selected_candidates={len(candidate_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )
    for calendar_partition_key in calendar_partition_keys:
        calendar_partition_filter = partition_expression(
            CALENDAR_PARTITION_COLUMNS,
            calendar_partition_key,
        )
        validate_read_fragments(
            calendar_dataset,
            calendar_partition_filter,
            CALENDAR_FILE_SCHEMA,
            CALENDAR_TABLE_NAME,
            "待校对行情日历叶",
        )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=leaf_read; status=started; "
            f"partition={calendar_partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        calendar_partition_table = calendar_dataset.to_table(
            columns=calendar_columns,
            filter=calendar_partition_filter,
        )
        calendar_partition_df = arrow_to_pandas(
            calendar_partition_table,
            FUTURES_BAR_CALENDAR_SCHEMA,
        )
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=leaf_read; status=completed; "
            f"partition={calendar_partition_key}; rows={len(calendar_partition_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        partition_candidate_df = candidate_df.iloc[
            candidate_indices_by_partition[calendar_partition_key]
        ]
        candidate_keys = set(
            partition_candidate_df[CALENDAR_PRIMARY_KEY].itertuples(
                index=False,
                name=None,
            )
        )

        _, exchange_code, year, month = calendar_partition_key
        contract_partition_key = (exchange_code, year, month)
        contract_partition_filter = partition_expression(
            CONTRACT_PARTITION_COLUMNS,
            contract_partition_key,
        )
        validate_read_fragments(
            contract_dataset,
            contract_partition_filter,
            CONTRACT_FILE_SCHEMA,
            CONTRACT_TABLE_NAME,
            "合约 Session 日历叶",
        )
        click.echo(
            f"planning_progress: table={CONTRACT_TABLE_NAME}; function=main; phase=leaf_read; status=started; "
            f"partition={contract_partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )
        contract_table = contract_dataset.to_table(
            columns=contract_columns,
            filter=contract_partition_filter,
        )
        contract_df = arrow_to_pandas(
            contract_table,
            FUTURES_CONTRACT_CALENDAR_SCHEMA,
        )
        click.echo(
            f"planning_progress: table={CONTRACT_TABLE_NAME}; function=main; phase=leaf_read; status=completed; "
            f"partition={contract_partition_key}; rows={len(contract_df)}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

        # 同一个交易所月可能出现多个候选品种，逐品种读取事实叶分区后合并。
        daily_tables: list[pa.Table] = []
        minute_tables: list[pa.Table] = []
        underlying_codes = sorted(
            set(partition_candidate_df["underlying_code"].tolist())
        )
        for underlying_code in underlying_codes:
            fact_partition_key = (
                exchange_code,
                underlying_code,
                year,
                month,
            )
            fact_filter = partition_expression(
                FACT_PARTITION_COLUMNS,
                fact_partition_key,
            )
            validate_read_fragments(
                daily_dataset,
                fact_filter,
                DAILY_FILE_SCHEMA,
                DAILY_TABLE_NAME,
                "JQData 日线事实叶",
            )
            validate_read_fragments(
                minute_dataset,
                fact_filter,
                MINUTE_FILE_SCHEMA,
                MINUTE_TABLE_NAME,
                "JQData 分钟事实叶",
            )
            click.echo(
                f"planning_progress: table={DAILY_TABLE_NAME}; function=main; phase=leaf_read; status=started; "
                f"partition={fact_partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            daily_tables.append(
                daily_dataset.to_table(
                    columns=daily_columns,
                    filter=fact_filter,
                )
            )
            click.echo(
                f"planning_progress: table={DAILY_TABLE_NAME}; function=main; phase=leaf_read; status=completed; "
                f"partition={fact_partition_key}; rows={len(daily_tables[-1])}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            click.echo(
                f"planning_progress: table={MINUTE_TABLE_NAME}; function=main; phase=leaf_read; status=started; "
                f"partition={fact_partition_key}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )
            minute_tables.append(
                minute_dataset.to_table(
                    columns=minute_columns,
                    filter=fact_filter,
                )
            )
            click.echo(
                f"planning_progress: table={MINUTE_TABLE_NAME}; function=main; phase=leaf_read; status=completed; "
                f"partition={fact_partition_key}; rows={len(minute_tables[-1])}; elapsed_s={time.perf_counter() - log_started_at:.3f}"
            )

        daily_df = arrow_to_pandas(
            pa.concat_tables(daily_tables),
            FUTURES_DAILY_SCHEMA,
        )
        minute_df = arrow_to_pandas(
            pa.concat_tables(minute_tables),
            FUTURES_MINUTE_SCHEMA,
        )

        reconciled_df, changed_df, _ = reconcile_partition(
                calendar_partition_df,
                candidate_keys,
                contract_df,
                daily_df,
                minute_df,
                checked_at,
            )
        if not changed_df.empty:
            partition_frames[calendar_partition_key] = reconciled_df
            changed_rows.append(changed_df)
        log_processed_partitions += 1
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=reconcile_batch; status=running; "
            f"partition={calendar_partition_key}; processed_partitions={log_processed_partitions}/{len(calendar_partition_keys)}; changed_candidates_in_partition={len(changed_df)}; persisted=false; elapsed_s={time.perf_counter() - log_started_at:.3f}"
        )

    if changed_rows:
        changed_df = pd.concat(changed_rows, ignore_index=True)
    else:
        changed_df = arrow_to_pandas(
            pa.Table.from_batches([], schema=FUTURES_BAR_CALENDAR_SCHEMA),
            FUTURES_BAR_CALENDAR_SCHEMA,
        )

    reconciled_count = int(
        changed_df["evidence_level"].eq("reconciled").sum()
    )
    warning_count = int(
        changed_df["quality_status"].eq("warning").sum()
    )
    click.echo(
        f"reconciliation_plan: table={CALENDAR_TABLE_NAME}; function=main; phase=reconcile_batch; status=completed; "
        f"selected_candidates={len(candidate_df)}; "
        f"changed_candidates={len(changed_df)}; "
        f"reconciled={reconciled_count}; warning={warning_count}; "
        f"force={str(force).lower()}; persisted=false; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}"
    )

    if not changed_df.empty:
        preview_columns = [
            "contract_code",
            "trading_date",
            "session_number",
            "evidence_level",
            "quality_status",
            "quality_reason",
        ]
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=preview; "
            f"status=completed; rows={min(20, len(changed_df))}; total_candidates={len(changed_df)}"
        )
        preview_column_labels = {
            name: f"{name}（{FUTURES_BAR_CALENDAR_SCHEMA.field(name).metadata[b'field_name_zh'].decode('utf-8')}）"
            for name in preview_columns
        }
        click.echo(
            changed_df.loc[:, preview_columns]
            .head(20)
            .rename(columns=preview_column_labels)
            .to_string(index=False)
        )

    # 5. 不带 --write 到此结束；写入时提交的是完整叶分区，不是候选子集。
    if not write:
        click.echo(
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=commit; "
            "status=skipped; write=false; committed_partitions=0"
        )
        click.echo(
            f"{log_boundary}\n疑似休市校对结束 / Reconciliation run ended\n"
            "function=main()\n"
            f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=run; status=completed; "
            f"outcome=read_only; mode={log_mode}; write={str(write).lower()}; "
            f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
        )
        return

    committed_rows = commit_calendar_partitions(
        partition_frames,
        resolved_lake_root,
    )
    click.echo(
        f"{log_boundary}\n疑似休市校对结束 / Reconciliation run ended\n"
        "function=main()\n"
        f"planning_progress: table={CALENDAR_TABLE_NAME}; function=main; phase=run; status=completed; "
        f"outcome=committed; mode={log_mode}; write={str(write).lower()}; "
        f"elapsed_s={time.perf_counter() - log_started_at:.3f}\n{log_boundary}"
    )



# ## 执行入口
# 
# Notebook 通过 `notebook_args` 显式传入 Click 参数，与 c01—c06 保持一致。当前参数是 2026-08-01 至 2026-08-15 的成对日期只读示例，不带 `--write` 或 `--force`，只在该范围内计算默认新候选并预览；全部读取来自本地 silver。直接运行脚本使用命令行参数；在 Notebook 中导入同名 Python 模块不触发业务。具体候选范围与正式写入限制见开篇表格。

# ### 局部流程：Notebook 与脚本执行入口
# 
# 当前 Notebook 参数是成对日期只读示例；运行这个代码单元格会扫描范围内的新候选，并在有候选时读取本地事实、计算旁证和预览。流程图本身不执行代码。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart TD
#     A["执行入口单元格或运行脚本"] --> B{"Notebook 交互环境？"}
#     B -->|是| C["显式 notebook_args；不读取内核参数"]
#     C --> D["main.main：standalone_mode=False"]
#     B -->|否| E{"直接运行 Python 脚本？"}
#     E -->|是| F["main：读取命令行参数"]
#     E -->|否| G["模块导入：不触发业务"]
#     D --> H["进入运行模式分支"]
#     F --> H
# ```

# In[9]:


# 用 Jupyter / IPython 运行时，内核进程的启动命令类似: ipykernel_launcher.py -f /path/to/connection.json
# sys.argv 里会包含: ['ipykernel_launcher.py', '-f', '/path/to/connection.json']
# parser.parse_args() 会读取 sys.argv，解析器会看到 -f 选项，但命令定义里没有 -f

# main()
# └── click.Command.__call__()
#     └── Command.main(args=None)
#         ├── args 为 None，因此读取 sys.argv[1:]
#         ├── Command.make_context(...)
#         ├── Command.parse_args(...)
#         │   └── 内部 parser.parse_args(args)
#         └── Command.invoke(...)
#             └── 调用 main(...) 函数体

if "ipykernel" in sys.modules and "__file__" not in globals():

    # Notebook：显式传入 Click 参数，不读取 ipykernel 的 -f 参数。
    notebook_args = ["--start-date", "2026-08-01", "--end-date", "2026-08-15",] # 单元格内不写 --write
    main.main(
        args=notebook_args,
        prog_name="c07_suspected_session_reconciliation",
        standalone_mode=False,
    )

elif __name__ == "__main__":
    # Python 脚本：正常读取命令行参数。
    main()


# ### 局部流程：终端手动运行
# 
# 下面的代码单元格仅保存命令注释；实际启动需在终端执行对应命令。
# 
# ```mermaid
# %%{init: {"flowchart": {"nodeSpacing": 24, "rankSpacing": 24, "padding": 12, "wrappingWidth": 230}}}%%
# flowchart LR
#     A["在终端激活 latitude_env_v2"] --> B["切换到项目根目录"]
#     B --> C["手动运行对应 .py --write"]
#     C --> D["读取本地新候选及事实；整批日历叶共同提交旁证"]
# ```

# In[10]:


# conda env list
# conda activate latitude_env_v2
# cd E:\Latitude_Analytics_v2
# python 02_Market_Data\a01_Collection\b01_Futures_Market_Data\c07_suspected_session_reconciliation.py --write

