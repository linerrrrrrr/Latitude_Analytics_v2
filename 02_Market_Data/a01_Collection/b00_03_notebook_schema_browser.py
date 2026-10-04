"""在 Notebook 开篇呈现权威 Schema，并按需只读预览已有湖仓数据。"""

from __future__ import annotations

from collections.abc import Sequence
from html import escape
from itertools import islice
import os
from pathlib import Path

import ipywidgets as widgets
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from IPython.display import display

from config import data_contracts


_ACTIVE_WIDGETS: list[widgets.Widget] = []
_RAW_ACTIVE_WIDGETS: list[widgets.Widget] = []

# 这些限制作用于实际读取；不会先加载整张表再截取。
_DEMO_SCAN_ROWS = 200_000
_DEMO_FILES = 64
_DEMO_DIRECTORY_ENTRIES = 4096
_DEMO_ROW_OPTIONS = (10, 20, 50, 100)
_DEMO_VALUE_FIELDS = {
    schema.metadata[b"table_name"].decode("utf-8"): fields
    for schema, fields in (
        (data_contracts.TRADE_CALENDAR_SCHEMA, ("is_trading_day", "weekday", "is_weekend", "effective_after")),
        (data_contracts.FUTURES_VARIETY_CALENDAR_SCHEMA, ("active_contract_count",)),
        (data_contracts.FUTURES_CONTRACT_CALENDAR_SCHEMA, ("underlying_code", "session_start_at", "session_end_at", "is_night_session", "minute_count")),
        (data_contracts.FUTURES_BAR_CALENDAR_SCHEMA, ("underlying_code", "schedule_status", "is_fetch_required", "is_fetch_completed")),
        (data_contracts.FUTURES_DAILY_SCHEMA, ("open", "high", "low", "close", "volume", "open_interest")),
        (data_contracts.FUTURES_MINUTE_SCHEMA, ("trading_date", "open", "high", "low", "close", "volume")),
        (data_contracts.FUTURES_MISSING_BAR_SCHEMA, ("trading_date", "session_number", "detected_at")),
        (data_contracts.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA, ("is_fetch_completed", "actual_record_count", "quality_status")),
        (data_contracts.FUTURES_POSITION_RANK_DAILY_SCHEMA, ("volume", "long_position", "short_position")),
        (data_contracts.FUTURES_MEMBER_POSITION_DAILY_SCHEMA, ("volume", "long_position", "short_position")),
        (data_contracts.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA, ("warehouse_receipt_number", "warehouse_receipt_unit", "warehouse_receipt_number_change")),
        (data_contracts.EXTERNAL_MARKET_CALENDAR_SCHEMA, ("is_fetch_completed", "actual_record_count", "quality_status")),
        (data_contracts.OVERSEAS_FUTURES_DAILY_SCHEMA, ("snapshot_date", "open", "high", "low", "close", "volume")),
        (data_contracts.EXTERNAL_INDEX_DAILY_SCHEMA, ("index_name", "index_category", "index_value")),
        (data_contracts.MACRO_RELEASE_CALENDAR_SCHEMA, ("expected_available_date", "is_fetch_required", "is_fetch_completed", "quality_status")),
        (data_contracts.INTEREST_RATE_DAILY_SCHEMA, ("rate",)),
        (data_contracts.MACRO_RELEASE_SCHEMA, ("available_date", "value")),
    )
}
_DEMO_LABELS = {
    "bar_frequency": "频率", "exchange_code": "交易所", "underlying_code": "品种",
    "year": "年份", "month": "月份", "dataset_name": "数据集", "index_category": "分类",
    "contract_code": "合约", "source_symbol": "合约", "instrument_code": "合约",
    "index_code": "指数", "series_code": "指标", "entity_code": "实体",
    "calendar_date": "日期", "trading_date": "交易日", "observation_date": "观测日",
    "report_date": "报告期",
}
# 单个实体随日期变化的表，默认展示分区内的小段记录；明细表默认聚焦一个日期。
_DEMO_DATE_SERIES = {
    schema.metadata[b"table_name"].decode("utf-8")
    for schema in (
        data_contracts.TRADE_CALENDAR_SCHEMA, data_contracts.FUTURES_DAILY_SCHEMA,
        data_contracts.OVERSEAS_FUTURES_DAILY_SCHEMA, data_contracts.EXTERNAL_INDEX_DAILY_SCHEMA,
        data_contracts.MACRO_RELEASE_CALENDAR_SCHEMA, data_contracts.INTEREST_RATE_DAILY_SCHEMA,
        data_contracts.MACRO_RELEASE_SCHEMA,
    )
}

# 只保存阅读取舍，不复制契约值；主键字段由 Schema metadata 自动纳入。
_SCHEMA_READING_SELECTIONS = {
    schema.metadata[b"table_name"].decode("utf-8"): (metadata_keys, business_fields)
    for schema, metadata_keys, business_fields in (
        (
            data_contracts.TRADE_CALENDAR_SCHEMA,
            ("description_zh", "grain_zh"),
            ("is_trading_day", "weekday", "calendar_timezone", "effective_after"),
        ),
        (
            data_contracts.FUTURES_VARIETY_CALENDAR_SCHEMA,
            ("description_zh", "grain_zh"),
            ("active_contract_count",),
        ),
        (
            data_contracts.FUTURES_CONTRACT_CALENDAR_SCHEMA,
            ("description_zh", "grain_zh", "calendar_role_zh"),
            ("underlying_code", "session_start_at", "session_end_at", "is_night_session", "minute_count"),
        ),
        (
            data_contracts.FUTURES_BAR_CALENDAR_SCHEMA,
            ("content_zh", "grain_zh", "calendar_role_zh"),
            (
                "schedule_status", "evidence_level", "is_fetch_required", "expected_bar_count",
                "is_fetch_completed", "actual_bar_count", "missing_bar_count", "quality_status",
            ),
        ),
        (
            data_contracts.FUTURES_DAILY_SCHEMA,
            ("description_zh", "grain_zh", "content_zh"),
            ("open", "high", "low", "close", "settlement", "volume", "open_interest", "has_market_data"),
        ),
        (
            data_contracts.FUTURES_MINUTE_SCHEMA,
            ("description_zh", "grain_zh", "content_zh"),
            ("trading_date", "open", "high", "low", "close", "volume", "open_interest"),
        ),
        (
            data_contracts.FUTURES_MISSING_BAR_SCHEMA,
            ("content_zh", "grain_zh"),
            ("trading_date", "session_number", "detected_at"),
        ),
        (
            data_contracts.FUTURES_EXCHANGE_REPORT_CALENDAR_SCHEMA,
            ("description_zh", "grain_zh"),
            ("is_fetch_required", "is_fetch_completed", "actual_record_count", "quality_status"),
        ),
        (
            data_contracts.FUTURES_POSITION_RANK_DAILY_SCHEMA,
            ("description_zh", "grain_zh", "content_zh"),
            ("volume_rank", "volume", "long_position_rank", "long_position", "short_position_rank", "short_position"),
        ),
        (
            data_contracts.FUTURES_MEMBER_POSITION_DAILY_SCHEMA,
            ("description_zh", "grain_zh"),
            ("volume", "long_position", "short_position"),
        ),
        (
            data_contracts.FUTURES_WAREHOUSE_RECEIPT_DAILY_SCHEMA,
            ("description_zh", "grain_zh"),
            ("warehouse_receipt_number", "warehouse_receipt_unit", "warehouse_receipt_number_change"),
        ),
        (
            data_contracts.EXTERNAL_MARKET_CALENDAR_SCHEMA,
            ("description_zh", "grain_zh", "content_zh"),
            ("is_fetch_required", "is_fetch_completed", "actual_record_count", "quality_status"),
        ),
        (
            data_contracts.OVERSEAS_FUTURES_DAILY_SCHEMA,
            ("content_zh", "grain_zh"),
            ("snapshot_date", "instrument_name", "open", "high", "low", "close", "volume"),
        ),
        (
            data_contracts.EXTERNAL_INDEX_DAILY_SCHEMA,
            ("description_zh", "grain_zh"),
            ("index_name", "index_category", "index_value"),
        ),
        (
            data_contracts.MACRO_RELEASE_CALENDAR_SCHEMA,
            ("description_zh", "grain_zh", "availability_rule_zh"),
            ("expected_available_date", "is_fetch_required", "is_fetch_completed", "quality_status"),
        ),
        (
            data_contracts.INTEREST_RATE_DAILY_SCHEMA,
            ("content_zh", "grain_zh"),
            ("rate",),
        ),
        (
            data_contracts.MACRO_RELEASE_SCHEMA,
            ("description_zh", "grain_zh", "source_columns_zh"),
            ("available_date", "value"),
        ),
    )
}
_METADATA_LABELS = {
    "description_zh": "用途",
    "content_zh": "记录内容",
    "grain_zh": "一行代表",
    "calendar_role_zh": "日历边界",
    "availability_rule_zh": "何时可用",
    "source_columns_zh": "来源转换",
}

_TABLE_STYLE = """
<style>
.schema-browser-root {
    --sb-text: var(--vscode-editor-foreground, var(--jp-content-font-color1, CanvasText));
    --sb-muted: var(--vscode-descriptionForeground, var(--jp-content-font-color2, GrayText));
    --sb-bg: var(--vscode-editor-background, var(--jp-layout-color1, Canvas));
    --sb-soft: color-mix(in srgb, var(--sb-text) 5%, var(--sb-bg));
    --sb-line: color-mix(in srgb, var(--sb-text) 16%, var(--sb-bg));
    --sb-accent: var(--vscode-focusBorder, var(--jp-brand-color1, #397aca));
    color: var(--sb-text);
    font-family: var(--vscode-font-family, var(--jp-ui-font-family, system-ui, sans-serif));
    font-size: 13px;
    line-height: 1.5;
    color-scheme: light dark;
    container-type: inline-size;
    gap: 12px;
}
.schema-browser-root .widget-html,
.schema-browser-root .widget-html-content { margin: 0; padding: 0; width: 100%; min-width: 0; }
.schema-browser-root .widget-html-content { font: inherit; color: inherit; }
.schema-browser-root .schema-browser-heading {
    font: inherit; font-weight: 600; font-size: 14px; margin: 0 0 8px;
}
.schema-browser-root .schema-browser-count { color: var(--sb-muted); font-weight: 400; margin-left: 8px; font-size: 12px; }
.schema-browser-root .schema-browser-grid {
    display: grid; grid-template-columns: minmax(0, 0.85fr) minmax(0, 1.15fr); gap: 20px;
    align-items: start;
}
.schema-browser-root .schema-browser-card { min-width: 0; }
.schema-browser-root .schema-browser-field-content { gap: 10px; }
.schema-browser-root .schema-browser-toolbar { flex-flow: row wrap; gap: 8px 12px; align-items: center; }
.schema-browser-root .schema-browser-controls { flex-flow: row wrap; gap: 6px 12px; overflow: visible; }
.schema-browser-root .schema-browser-control { width: 222px; max-width: 100%; margin: 0; height: 32px; }
.schema-browser-root .schema-browser-control .widget-label { width: 52px; text-align: left; color: var(--sb-muted); font: inherit; }
.schema-browser-root .schema-browser-control select,
.schema-browser-root .schema-browser-control input[type=text] {
    min-width: 0; height: 32px; border: 1px solid var(--sb-line); border-radius: 5px;
    background-color: var(--sb-bg); color: var(--sb-text); padding: 4px 8px; font: inherit;
    color-scheme: inherit; box-sizing: border-box; margin: 0;
}
.schema-browser-root .schema-browser-control select:focus,
.schema-browser-root .schema-browser-control input:focus { outline: 2px solid var(--sb-accent); outline-offset: 1px; }
.schema-browser-root .schema-browser-control select { appearance: auto; background-image: none; }
.schema-browser-root .schema-browser-button {
    width: auto; min-width: 86px; height: 30px; margin: 0; padding: 0 12px;
    border: 1px solid var(--sb-line); border-radius: 5px;
    background: var(--sb-soft); color: var(--sb-text); font: inherit; box-shadow: none;
}
.schema-browser-root .schema-browser-button:hover { border-color: var(--sb-accent); }
.schema-browser-root .schema-browser-checkbox { width: auto; margin: 0; height: 30px; font: inherit; }
.schema-browser-root .schema-browser-checkbox label { color: var(--sb-muted); font: inherit; }
.schema-browser-root .schema-browser-status { margin: 2px 0 8px; color: var(--sb-muted); font-size: 12px; }
.schema-browser-root .schema-browser-section,
.schema-browser-root .schema-browser-demo-panel { border-top: 1px solid var(--sb-line); padding-top: 14px; }
.schema-browser-root .schema-browser-section { gap: 12px; }
.schema-browser-root .schema-browser-demo-panel { gap: 10px; }
.schema-browser-root .schema-browser-demo-header { justify-content: space-between; }
.schema-browser-root .schema-browser-demo-filters {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 220px));
    gap: 10px 16px; overflow: visible;
}
.schema-browser-root .schema-browser-demo-filters .schema-browser-control {
    flex-direction: column; align-items: stretch; width: 100%; height: auto; gap: 4px;
}
.schema-browser-root .schema-browser-filter-control { gap: 4px; min-width: 0; }
.schema-browser-root .schema-browser-demo-filters .widget-label {
    width: auto !important; height: auto; line-height: 20px; margin: 0;
}
.schema-browser-root .schema-browser-demo-filters select,
.schema-browser-root .schema-browser-demo-filters input[type=text] { width: 100%; flex: 0 0 32px; }
.schema-browser-root .schema-browser-demo-display { margin-top: 2px; gap: 12px 20px; overflow: visible; }
.schema-browser-root .schema-browser-demo-display .schema-browser-checkbox { height: 32px; }
.schema-browser-root .schema-browser-demo-display .schema-browser-checkbox label {
    display: flex; align-items: center; gap: 6px; height: 32px; margin: 0; padding: 0;
}
.schema-browser-root .schema-browser-demo-display .schema-browser-checkbox input { margin: 0; }
.schema-browser-root .schema-browser-date-filter { grid-column: span 2; gap: 4px; min-width: 0; }
.schema-browser-root .schema-browser-date-label { margin: 0; height: 20px; color: var(--sb-muted); font: inherit; }
.schema-browser-root .schema-browser-date-controls { gap: 8px; align-items: center; overflow: visible; }
.schema-browser-root .schema-browser-date-mode { width: 220px; flex: 0 1 220px; min-width: 0; margin: 0; }
.schema-browser-root .schema-browser-date-mode > div { display: flex; width: 100%; }
.schema-browser-root .schema-browser-date-mode button {
    flex: 1; width: auto; min-width: 0; height: 32px; margin: 0; padding: 0 8px; font: inherit;
    border: 1px solid var(--sb-line); border-radius: 0; box-shadow: none;
    background: var(--sb-bg); color: var(--sb-muted);
}
.schema-browser-root .schema-browser-date-mode button:first-child { border-radius: 5px 0 0 5px; }
.schema-browser-root .schema-browser-date-mode button:last-child { border-radius: 0 5px 5px 0; border-left: 0; }
.schema-browser-root .schema-browser-date-mode button.mod-active {
    background: var(--sb-soft); color: var(--sb-text); box-shadow: inset 0 -2px var(--sb-accent);
}
.schema-browser-root .schema-browser-date-picker { width: 180px; flex: 0 1 180px; min-width: 0; height: 32px; margin: 0; }
.schema-browser-root .schema-browser-date-picker input {
    width: 100%; min-width: 0; height: 32px; padding: 4px 8px; box-sizing: border-box;
    border: 1px solid var(--sb-line); border-radius: 5px; font: inherit;
    background: var(--sb-bg); color: var(--sb-text); color-scheme: inherit;
}
.schema-browser-root .schema-browser-row-limit { width: 132px; }
.schema-browser-root .schema-browser-path { overflow-wrap: anywhere; color: var(--sb-muted); font-size: 12px; }
[data-jp-theme-light="true"] .schema-browser-root,
.vscode-light .schema-browser-root { color-scheme: light; }
[data-jp-theme-light="false"] .schema-browser-root,
.vscode-dark .schema-browser-root,
.vscode-high-contrast .schema-browser-root { color-scheme: dark; }
.schema-browser-root .schema-browser-field-detail .jupyter-widget-Collapse-header,
.schema-browser-root .schema-browser-field-detail .p-Accordion-header,
.schema-browser-root .schema-browser-field-detail .lm-Accordion-header {
    border: 0; background: transparent; color: var(--sb-muted); padding: 5px 0; font: inherit; font-size: 12px; box-shadow: none;
}
.schema-browser-root .schema-browser-field-detail .jupyter-widget-Collapse { margin: 0; padding: 0; }
.schema-browser-root .schema-browser-field-detail .jupyter-widget-Collapse-contents,
.schema-browser-root .schema-browser-field-detail .p-Accordion-child,
.schema-browser-root .schema-browser-field-detail .lm-Accordion-child { border: 0; padding: 8px 0; }
@container (max-width: 820px) {
    .schema-browser-root .schema-browser-grid { grid-template-columns: minmax(0, 1fr); gap: 14px; }
}
@container (max-width: 460px) {
    .schema-browser-root .schema-browser-demo-filters { grid-template-columns: minmax(0, 1fr); }
    .schema-browser-root .schema-browser-date-filter { grid-column: span 1; }
}
.schema-browser-details {
    margin-top: 6px;
}
.schema-browser-details > summary {
    cursor: pointer;
    padding: 5px 0;
    color: var(--sb-muted);
    font-size: 12px;
}
.schema-browser-details[open] .schema-browser-expand,
.schema-browser-details:not([open]) .schema-browser-collapse {
    display: none;
}
.schema-browser-scroll {
    max-height: 420px;
    overflow: auto;
    border: 1px solid var(--sb-line);
    border-radius: 5px;
}
.schema-browser-scroll thead th {
    position: sticky;
    top: 0;
}
.schema-browser-root .schema-browser-table {
    color-scheme: light dark;
    border-collapse: collapse;
    width: 100%;
    table-layout: fixed;
    margin: 0;
    font: inherit;
    font-size: 13px;
    color: var(--sb-text);
    background-color: var(--sb-bg);
}
.schema-browser-root .schema-browser-table th,
.schema-browser-root .schema-browser-table td {
    border: 0;
    border-bottom: 1px solid var(--sb-line);
    padding: 7px 10px;
    text-align: left;
    vertical-align: top;
    white-space: normal;
    word-break: break-word;
    overflow-wrap: anywhere;
    color: var(--sb-text);
    line-height: 1.5;
}
.schema-browser-root .schema-browser-table thead > tr > th {
    background-color: var(--sb-soft);
    font-weight: 600;
}
.schema-browser-root .schema-browser-table tbody > tr > td {
    background: var(--sb-bg);
}
.schema-browser-root .schema-browser-table tbody > tr:hover > td {
    background: var(--sb-soft);
}
.schema-browser-root .schema-browser-table tbody > tr:last-child > td { border-bottom: 0; }
.schema-browser-summary th:first-child,
.schema-browser-summary td:first-child {
    width: 64px;
}
.schema-browser-root .schema-browser-table.schema-browser-overview {
    table-layout: auto;
    width: max-content;
    min-width: 100%;
}
.schema-browser-root .schema-browser-overview th { white-space: nowrap; }
.schema-browser-root .schema-browser-overview td { white-space: nowrap; word-break: normal; overflow-wrap: normal; }
.schema-browser-root .schema-browser-metadata th:first-child,
.schema-browser-root .schema-browser-metadata td:first-child { width: 150px; }
.schema-browser-root .schema-browser-table.schema-browser-field-catalog { min-width: 850px; }
.schema-browser-fields th:first-child,
.schema-browser-fields td:first-child {
    width: 38%;
}
.schema-browser-root .schema-browser-table.schema-browser-demo {
    table-layout: auto;
    width: max-content;
    min-width: 100%;
}
.schema-browser-root .schema-browser-demo th, .schema-browser-root .schema-browser-demo td {
    white-space: nowrap;
}
</style>
"""


def read_table_demo(
    schema: pa.Schema,
    partition_dir: Path,
    partition_values: dict[str, object],
    columns: Sequence[str],
    filters: dict[str, object],
    *,
    max_rows: int = _DEMO_ROW_OPTIONS[0],
) -> tuple[pa.Table, bool]:
    """只读一个叶分区；返回样例及是否因读取上限而未遍历完候选范围。"""
    if not 1 <= max_rows <= _DEMO_SCAN_ROWS:
        raise ValueError("样例行数超出有界读取范围。")
    result_schema = pa.schema([schema.field(name) for name in columns])
    needed_columns = [name for name in schema.names if name in set(columns).union(filters)]
    typed_filters = {
        name: pa.scalar(value).cast(schema.field(name).type).as_py()
        for name, value in filters.items()
    }
    for name, value in partition_values.items():
        if name in typed_filters and typed_filters[name] != value:
            return pa.Table.from_batches([], schema=result_schema), False

    # 仅枚举已经选定的叶目录，不在表根递归发现全历史 fragments。
    with os.scandir(partition_dir) as entries:
        entries = list(islice(entries, _DEMO_DIRECTORY_ENTRIES + 1))
    if len(entries) > _DEMO_DIRECTORY_ENTRIES:
        raise ValueError("当前分区文件过多，无法在样例读取范围内枚举。")
    files = sorted(
        (Path(entry.path) for entry in entries
         if entry.name.endswith(".parquet") and not entry.name.startswith((".", "_"))
         and entry.is_file(follow_symlinks=False)),
        reverse=True,
    )
    limited = len(files) > _DEMO_FILES
    batches: list[pa.RecordBatch] = []
    scanned_rows = 0
    selected_rows = 0
    physical_filter = None
    for name, value in typed_filters.items():
        if name not in partition_values:
            condition = ds.field(name) == value
            physical_filter = condition if physical_filter is None else physical_filter & condition

    for file_path in files[:_DEMO_FILES]:
        with pq.ParquetFile(file_path) as parquet_file:
            physical_schema = parquet_file.schema_arrow
            expected_fields = [field for field in schema if field.name not in partition_values]
            actual_fields = [field for field in physical_schema if field.name not in partition_values]
            if not pa.schema(actual_fields).equals(pa.schema(expected_fields), check_metadata=False):
                raise TypeError(f"{file_path.name} 的字段、类型或空值约束与权威 Schema 不兼容。")
            for name in partition_values:
                if name in physical_schema.names and not physical_schema.field(name).equals(schema.field(name), check_metadata=False):
                    raise TypeError(f"{file_path.name} 的分区字段 {name} 类型不兼容。")
            # 用行组统计跳过不匹配的实体/日期，实际扫描仍受行数上限约束。
            fragment = next(ds.dataset(file_path, format="parquet").get_fragments())
            row_groups = [
                group.id
                for part in fragment.split_by_row_group(filter=physical_filter, schema=physical_schema)
                for group in part.row_groups
            ]
            physical_columns = [name for name in needed_columns if name not in partition_values]
            for batch in parquet_file.iter_batches(
                batch_size=2048, row_groups=row_groups, columns=physical_columns, use_threads=False,
            ):
                remaining = _DEMO_SCAN_ROWS - scanned_rows
                if remaining <= 0:
                    return pa.Table.from_batches(batches, schema=result_schema), True
                scan_truncated = batch.num_rows > remaining
                batch = batch.slice(0, remaining)
                scanned_rows += batch.num_rows
                arrays = [
                    pa.array([partition_values[name]] * batch.num_rows, type=schema.field(name).type)
                    if name in partition_values else batch.column(batch.schema.get_field_index(name))
                    for name in needed_columns
                ]
                candidate_table = pa.Table.from_arrays(
                    arrays, schema=pa.schema([schema.field(name) for name in needed_columns]),
                )
                mask = None
                for name, value in typed_filters.items():
                    condition = pc.equal(candidate_table[name], pa.scalar(value, type=schema.field(name).type))
                    mask = condition if mask is None else pc.and_(mask, condition)
                if mask is not None:
                    candidate_table = candidate_table.filter(mask)
                selected_table = candidate_table.select(columns).slice(0, max_rows - selected_rows)
                batches.extend(selected_table.to_batches())
                selected_rows += selected_table.num_rows
                if selected_rows >= max_rows:
                    return pa.Table.from_batches(batches, schema=result_schema), True
                if scan_truncated:
                    return pa.Table.from_batches(batches, schema=result_schema), True
    return pa.Table.from_batches(batches, schema=result_schema), limited


class _TableDemo(widgets.VBox):
    """一张表的有界只读样例；分区和筛选控件在切表时原地复用。"""

    def __init__(self, schema: pa.Schema, lake_root: Path) -> None:
        self.lake_root = Path(lake_root)
        self.busy = False
        self.partition_values: dict[str, object] = {}
        self.candidates = pd.DataFrame()
        self.preview_cache: tuple[dict[str, str], pa.Table, bool] | None = None
        self.partition_selectors = [
            widgets.Dropdown(style={"description_width": "52px"}).add_class("schema-browser-control")
            for _ in range(5)
        ]
        self.filter_selectors = [
            widgets.Text(continuous_update=False,
                             style={"description_width": "52px"}).add_class("schema-browser-control")
            for _ in range(4)
        ]
        self.filter_dropdowns = [
            widgets.Dropdown(style={"description_width": "52px"}).add_class("schema-browser-control")
            for _ in range(4)
        ]
        self.filter_controls = [
            widgets.VBox([dropdown, selector]).add_class("schema-browser-filter-control")
            for dropdown, selector in zip(self.filter_dropdowns, self.filter_selectors)
        ]
        self.date_label = widgets.Label().add_class("schema-browser-date-label")
        self.date_mode = widgets.ToggleButtons(
            options=[("全部日期", False), ("指定日期", True)], value=False,
        ).add_class("schema-browser-date-mode")
        self.date_picker = widgets.DatePicker().add_class("schema-browser-date-picker")
        self.date_filter = widgets.VBox([
            self.date_label,
            widgets.HBox([self.date_mode, self.date_picker]).add_class("schema-browser-date-controls"),
        ]).add_class("schema-browser-date-filter")
        self.refresh = widgets.Button(description="刷新", icon="refresh").add_class("schema-browser-button")
        self.row_limit = widgets.Dropdown(
            options=_DEMO_ROW_OPTIONS, value=_DEMO_ROW_OPTIONS[0], description="行数",
            style={"description_width": "40px"},
        ).add_class("schema-browser-control").add_class("schema-browser-row-limit")
        self.all_columns = widgets.Checkbox(value=False, description="显示全部字段", indent=False).add_class("schema-browser-checkbox")
        self.scope = widgets.HTML()
        self.output = widgets.HTML()
        super().__init__(
            [widgets.HBox([
                widgets.HTML('<h4 class="schema-browser-heading" style="margin:0">数据样例</h4>', layout=widgets.Layout(width="auto")),
                self.refresh,
             ]).add_class("schema-browser-toolbar").add_class("schema-browser-demo-header"),
             widgets.Box([*self.partition_selectors, *self.filter_controls, self.date_filter]).add_class("schema-browser-demo-filters"),
             widgets.HBox([self.row_limit, self.all_columns]).add_class("schema-browser-toolbar").add_class("schema-browser-demo-display"),
             self.output, self.scope],
            layout=widgets.Layout(width="100%"),
        )
        self.add_class("schema-browser-demo-panel")
        for index, selector in enumerate(self.partition_selectors):
            selector.observe(lambda change, index=index: self.load_partition(index + 1), names="value")
        for index, selector in enumerate(self.filter_selectors):
            selector.observe(lambda change, index=index: self.render_rows(index + 1), names="value")
        for index, dropdown in enumerate(self.filter_dropdowns):
            dropdown.observe(lambda change, index=index: self.select_filter(index), names="value")
        self.date_mode.observe(lambda change: self.render_rows(len(self.filter_names)), names="value")
        self.date_picker.observe(lambda change: self.render_rows(len(self.filter_names)), names="value")
        self.all_columns.observe(lambda change: self.render_rows(len(self.filter_names)), names="value")
        self.row_limit.observe(lambda change: self.render_rows(len(self.filter_names)), names="value")
        self.refresh.on_click(lambda button: self.load_partition(0, preserve_filters=True))
        self.set_schema(schema)

    def select_filter(self, index: int) -> None:
        """候选选择立即筛选；手动输入保留列表外值的查询入口。"""
        if self.busy:
            return
        selector = self.filter_selectors[index]
        selected_value = self.filter_dropdowns[index].value
        selector.layout.display = "" if selected_value is None else "none"
        if selected_value is not None:
            selector.value = selected_value

    def set_schema(self, schema: pa.Schema) -> None:
        self.schema = schema
        self.table_name = schema.metadata[b"table_name"].decode("utf-8")
        self.table_dir = self.lake_root / "silver" / self.table_name
        self.partition_names = schema.metadata[b"partition_columns"].decode("utf-8").split(",")
        primary_key = schema.metadata[b"primary_key"].decode("utf-8").split(",")
        fields = set(primary_key).union(_DEMO_VALUE_FIELDS.get(self.table_name, ()))
        self.key_columns = [name for name in schema.names if name in fields]
        self.filter_names = [name for name in ("underlying_code",) if name in schema.names and name not in self.partition_names]
        for choices in (
            ("source_symbol", "contract_code"),
            ("instrument_code", "index_code", "series_code", "entity_code"),
            ("calendar_date", "trading_date", "observation_date", "report_date"),
        ):
            self.filter_names.extend(next(([name] for name in choices if name in schema.names and name not in self.partition_names), []))
        self.busy = True
        for selector in self.partition_selectors:
            selector.options = ()
        for selector in self.filter_selectors:
            selector.value = ""
        self.date_mode.value = False
        self.date_picker.value = None
        self.date_filter.layout.display = "none"
        self.all_columns.value = False
        self.busy = False
        self.load_partition(0)

    def load_partition(self, keep: int, preserve_filters: bool = False) -> None:
        if self.busy:
            return
        self.busy = True
        self.partition_values = {}
        self.candidates = pd.DataFrame(columns=self.filter_names)
        self.candidates_limited = False
        self.preview_cache = None
        self.ready = False
        self.scope.value = ""
        self.output.value = '<p class="schema-browser-status">正在读取样例…</p>'
        try:
            partition_dir = self.table_dir
            for index, selector in enumerate(self.partition_selectors):
                visible = index < len(self.partition_names)
                selector.layout.display = "" if visible else "none"
                if not visible:
                    continue
                name = self.partition_names[index]
                selector.description = _DEMO_LABELS.get(name, name)
                try:
                    with os.scandir(partition_dir) as entries:
                        entries = list(islice(entries, _DEMO_DIRECTORY_ENTRIES + 1))
                except FileNotFoundError:
                    entries = []
                if len(entries) > _DEMO_DIRECTORY_ENTRIES:
                    raise ValueError("当前层级目录过多，请缩小样例范围。")
                values = {
                    entry.name.split("=", 1)[1]: pa.scalar(entry.name.split("=", 1)[1]).cast(self.schema.field(name).type).as_py()
                    for entry in entries if entry.name.startswith(name + "=") and entry.is_dir(follow_symlinks=False)
                }
                options = sorted(values, key=values.get, reverse=name in {"year", "month"})
                previous = selector.value
                selector.options = options
                selector.value = previous if previous in options and (index < keep or preserve_filters) else (options[0] if options else None)
                selector.disabled = not options
                if selector.value is None:
                    for later in self.partition_selectors[index + 1:]:
                        later.options = ()
                        later.layout.display = "none"
                    break
                self.partition_values[name] = values[selector.value]
                partition_dir = partition_dir / f"{name}={selector.value}"
            for index, selector in enumerate(self.filter_selectors):
                dropdown = self.filter_dropdowns[index]
                self.filter_controls[index].layout.display = "" if index < len(self.filter_names) else "none"
                selector.layout.display = "none"
                if index < len(self.filter_names):
                    dropdown.description = _DEMO_LABELS.get(self.filter_names[index], self.filter_names[index])
                    selector.placeholder = "输入后按 Enter 确认"
                    if pa.types.is_date(self.schema.field(self.filter_names[index]).type):
                        self.filter_controls[index].layout.display = "none"
                        self.date_label.value = dropdown.description
                        self.date_filter.layout.display = ""
                selector.disabled = dropdown.disabled = True
                if not preserve_filters:
                    dropdown.options = ()
                    selector.value = ""
            self.partition_dir = partition_dir
            if len(self.partition_values) == len(self.partition_names):
                candidates, self.candidates_limited = read_table_demo(
                    self.schema, partition_dir, self.partition_values, self.filter_names, {}, max_rows=_DEMO_SCAN_ROWS,
                )
                self.candidates = candidates.to_pandas().drop_duplicates(ignore_index=True).astype("string")
                self.ready = True
            if self.candidates.empty:
                self.status = "本次有界读取未发现记录，请选择其他分区。" if self.candidates_limited else "当前分区尚未生成数据。"
            else:
                self.status = ""
        except Exception as error:
            self.status = f"读取失败：{type(error).__name__}：{error}"
        finally:
            self.busy = False
        self.render_rows(len(self.filter_names) if preserve_filters else 0)

    def render_rows(self, keep: int = 0) -> None:
        if self.busy:
            return
        self.busy = True
        columns = self.schema.names if self.all_columns.value else self.key_columns
        row_limit = self.row_limit.value
        preview_table = pa.Table.from_batches([], schema=pa.schema([self.schema.field(name) for name in columns]))
        status = self.status
        selected_filters: dict[str, str] = {}
        try:
            narrowed = self.candidates
            for index, name in enumerate(self.filter_names):
                is_date = pa.types.is_date(self.schema.field(name).type)
                options = sorted(narrowed[name].dropna().unique().tolist(), reverse=is_date)
                default_value = options[0] if options else ""
                if is_date and self.table_name in _DEMO_DATE_SERIES:
                    default_value = ""
                if is_date:
                    selected_value = default_value
                    if index < keep:
                        selected_value = (
                            self.date_picker.value.isoformat() if self.date_picker.value else (options[0] if options else "")
                        ) if self.date_mode.value else ""
                    self.date_mode.value = bool(selected_value)
                    self.date_picker.value = pa.scalar(selected_value).cast(self.schema.field(name).type).as_py() if selected_value else None
                    self.date_picker.layout.display = "" if self.date_mode.value else "none"
                    self.date_mode.disabled = self.date_picker.disabled = not self.ready
                else:
                    selector = self.filter_selectors[index]
                    dropdown = self.filter_dropdowns[index]
                    previous = selector.value
                    selector.disabled = dropdown.disabled = not self.ready
                    selector.value = previous if index < keep else default_value
                    selected_value = selector.value
                    # 标准下拉框始终列出全部候选，不受当前输入文本的补全过滤影响。
                    dropdown.options = [(value, value) for value in options] + [("手动输入…", None)]
                    dropdown.value = selected_value if selected_value in options else None
                    selector.layout.display = "none" if dropdown.value is not None else ""
                if selected_value:
                    selected_filters[name] = selected_value
                    narrowed = narrowed.loc[narrowed[name].eq(selected_value)]
            if self.ready:
                # 只缓存当前筛选的至多 100 行；缩小行数或切回已有列不再读盘。
                if (self.preview_cache is not None and self.preview_cache[0] == selected_filters
                        and set(columns).issubset(self.preview_cache[1].column_names)
                        and (self.preview_cache[1].num_rows >= row_limit or not self.preview_cache[2])):
                    preview_table = self.preview_cache[1].select(columns).slice(0, row_limit)
                    limited = self.preview_cache[2]
                else:
                    preview_table, limited = read_table_demo(
                        self.schema, self.partition_dir, self.partition_values, columns, selected_filters, max_rows=row_limit,
                    )
                    self.preview_cache = (selected_filters.copy(), preview_table, limited)
                # 只排序这几行，保持样例易读，不暗示它们是分区内最新记录。
                sort_columns = [name for name in self.schema.metadata[b"primary_key"].decode().split(",") if name in columns]
                preview_table = preview_table.sort_by([(name, "ascending") for name in sort_columns])
                if preview_table.num_rows:
                    status = f"显示 {preview_table.num_rows} 行 · {len(columns)} / {len(self.schema)} 个字段"
                elif selected_filters:
                    status = "本次有界读取未匹配到记录，可调整分区或筛选；不代表整表无数据。" if limited else "当前选择没有记录。"
                if self.candidates_limited:
                    status += " 候选仅覆盖本次读取范围，可手动输入其他值。"
        except Exception as error:
            status = f"读取失败：{type(error).__name__}：{error}"
        finally:
            self.busy = False
        scope = {**self.partition_values, **selected_filters}
        self.scope.value = (
            '<details class="schema-browser-details schema-browser-path"><summary>读取位置与范围</summary>'
            + escape(str(self.table_dir)) + "<br>"
            + escape(" · ".join(f"{_DEMO_LABELS.get(name, name)}：{value}" for name, value in scope.items()))
            + "</details>"
        )
        # 行、字段顺序来自 Arrow；横向滚动避免宽表撑满页面。
        self.output.value = (
            f'<p class="schema-browser-status">{escape(status)}</p><div class="schema-browser-scroll">'
            + preview_table.to_pandas().to_html(index=False, escape=True, border=0, classes="schema-browser-table schema-browser-demo")
            + "</div>"
        )


def display_schema_metadata(schemas: Sequence[pa.Schema], *, lake_root: Path | None = None) -> None:
    """展示权威契约；显式传入湖仓路径时，附加有界、只读的数据样例。"""
    global _ACTIVE_WIDGETS

    if not schemas:
        raise ValueError("至少需要一个 Arrow Schema。")

    schemas_by_table_name: dict[str, pa.Schema] = {}
    schema_options: list[tuple[str, str]] = []
    overview_rows: list[dict[str, object]] = []

    for schema in schemas:
        if not isinstance(schema, pa.Schema):
            raise TypeError(
                "schemas 中的每个对象都必须是 pyarrow.Schema；"
                f"实际包含 {type(schema).__name__}。"
            )

        schema_metadata = {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (schema.metadata or {}).items()
        }
        table_name = schema_metadata.get("table_name")
        if not table_name:
            raise ValueError("Schema metadata 缺少 table_name。")
        if table_name in schemas_by_table_name:
            raise ValueError(f"Schema 列表包含重复表名 {table_name!r}。")

        schemas_by_table_name[table_name] = schema
        table_name_zh = schema_metadata.get("table_name_zh", "未定义")
        schema_options.append((table_name_zh, table_name))
        overview_rows.append(
            {
                "英文表名": table_name,
                "中文表名": table_name_zh,
                "字段数": len(schema),
                "主键": schema_metadata.get("primary_key", "未定义"),
                "Hive 分区": schema_metadata.get("partition_columns", "未定义"),
                "Schema 版本": schema_metadata.get("schema_version", "未定义"),
            }
        )

    # 每次重新运行单元格前关闭本组件上一轮创建的通信通道，避免 Widget 累积。
    for widget in reversed(_ACTIVE_WIDGETS):
        widget.close()
    _ACTIVE_WIDGETS = []

    overview_df = pd.DataFrame(overview_rows)
    overview_html = widgets.HTML(
            _TABLE_STYLE
            + '<h4 class="schema-browser-heading">Schema 列表</h4><div class="schema-browser-scroll">'
            + overview_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table schema-browser-overview",
            ) + "</div>"
    )

    schema_selector = widgets.Dropdown(
        options=schema_options,
        value=schema_options[0][1],
        description="查看表",
        layout=widgets.Layout(
            width="540px",
            max_width="100%",
            margin="0",
        ),
        style={"description_width": "52px"},
    ).add_class("schema-browser-control")
    field_selector = widgets.Dropdown(
        description="字段",
        layout=widgets.Layout(
            width="540px",
            max_width="100%",
            margin="0",
        ),
        style={"description_width": "52px"},
    ).add_class("schema-browser-control")
    schema_metadata_html = widgets.HTML()
    field_overview_html = widgets.HTML()
    field_metadata_html = widgets.HTML()

    initial_schema = schemas_by_table_name[schema_selector.value]
    initial_field_options = [("选择字段查看详情", None)]
    for field in initial_schema:
        field_metadata = {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (field.metadata or {}).items()
        }
        initial_field_options.append(
            (
                f"{field.name}｜{field_metadata.get('field_name_zh', '未定义')}",
                field.name,
            )
        )
    field_selector.options = initial_field_options
    field_selector.value = None
    data_demo = _TableDemo(initial_schema, lake_root) if lake_root is not None else None

    def render_schema_metadata() -> None:
        selected_schema = schemas_by_table_name[schema_selector.value]
        schema_metadata = {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (selected_schema.metadata or {}).items()
        }
        schema_metadata_df = pd.DataFrame(
            {
                "metadata_key": schema_metadata.keys(),
                "metadata_value": schema_metadata.values(),
            }
        )
        metadata_keys, _ = _SCHEMA_READING_SELECTIONS.get(
            schema_selector.value, (("description_zh", "grain_zh"), ())
        )
        key_schema_metadata_df = pd.DataFrame(
            [
                {"阅读要点": _METADATA_LABELS[key], "说明": schema_metadata[key]}
                for key in metadata_keys
                if key in schema_metadata
            ],
            columns=["阅读要点", "说明"],
        )

        schema_metadata_html.value = (
            '<h4 class="schema-browser-heading">内容概要</h4><div class="schema-browser-scroll">'
            + key_schema_metadata_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table schema-browser-summary",
            )
            + '</div><details class="schema-browser-details"><summary>'
            + '<span class="schema-browser-expand">完整表说明</span>'
            + '<span class="schema-browser-collapse">收起完整表说明</span>'
            + f'（{len(schema_metadata_df)} 项）</summary><div class="schema-browser-scroll">'
            + schema_metadata_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table schema-browser-metadata",
            )
            + "</div></details>"
        )

    def render_field_overview() -> None:
        selected_schema = schemas_by_table_name[schema_selector.value]
        schema_metadata = {
            key.decode("utf-8"): value.decode("utf-8")
            for key, value in (selected_schema.metadata or {}).items()
        }
        primary_key = schema_metadata.get("primary_key", "").split(",")
        partition_columns = schema_metadata.get("partition_columns", "").split(",")

        field_rows = []
        for field in selected_schema:
            field_metadata = {
                key.decode("utf-8"): value.decode("utf-8")
                for key, value in (field.metadata or {}).items()
            }
            structure_roles = []
            if field.name in primary_key:
                structure_roles.append("主键")
            if field.name in partition_columns:
                structure_roles.append("分区")

            field_rows.append(
                {
                    "英文字段名": field.name,
                    "中文字段名": field_metadata.get("field_name_zh", "未定义"),
                    "Arrow 类型": str(field.type),
                    "结构角色": "、".join(structure_roles) or "—",
                    "语义角色": field_metadata.get("semantic_role_zh", "未定义"),
                    "来源系统": field_metadata.get("source_system_zh", "未定义"),
                }
            )

        field_overview_df = pd.DataFrame(field_rows)
        _, business_fields = _SCHEMA_READING_SELECTIONS.get(
            schema_selector.value, ((), ())
        )
        key_field_names = set(primary_key).union(business_fields)
        key_field_overview_df = field_overview_df.loc[
            field_overview_df["英文字段名"].isin(key_field_names),
            ["英文字段名", "中文字段名"],
        ].rename(columns={"英文字段名": "字段", "中文字段名": "含义"})
        field_overview_html.value = (
            f'<h4 class="schema-browser-heading">关键字段<span class="schema-browser-count">{len(key_field_overview_df)} / {len(field_overview_df)} 个</span></h4><div class="schema-browser-scroll">'
            + key_field_overview_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table schema-browser-fields",
            )
            + '</div><details class="schema-browser-details"><summary>'
            + f'<span class="schema-browser-expand">全部 {len(field_overview_df)} 个字段 · 类型、角色与来源</span>'
            + '<span class="schema-browser-collapse">收起完整字段目录</span>'
            + '</summary><div class="schema-browser-scroll">'
            + field_overview_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table schema-browser-field-catalog",
            )
            + "</div></details>"
        )

    def render_field_metadata() -> None:
        if field_selector.value is None:
            field_metadata_html.value = ""
            return
        selected_schema = schemas_by_table_name[schema_selector.value]
        selected_field = selected_schema.field(field_selector.value)
        field_metadata: dict[str, object] = {
            "field_name": selected_field.name,
            "arrow_type": str(selected_field.type),
            "nullable": selected_field.nullable,
        }
        field_metadata.update(
            {
                key.decode("utf-8"): value.decode("utf-8")
                for key, value in (selected_field.metadata or {}).items()
            }
        )
        field_metadata_df = pd.DataFrame(
            {
                "metadata_key": field_metadata.keys(),
                "metadata_value": field_metadata.values(),
            }
        )
        # 用户选中字段后才展示细节；默认解释含义、类型、单位和取值条件。
        enum_values = str(field_metadata.get("enum_values_zh", ""))
        explanation_key = (
            "enum_values_zh"
            if enum_values and not enum_values.startswith("非枚举")
            else "transformation_zh"
        )
        field_metadata_items = [
            ("description_zh", "含义"),
            ("arrow_type", "类型"),
        ]
        if field_metadata.get("unit_zh") not in (None, "", "不适用", "—"):
            field_metadata_items.append(("unit_zh", "单位"))
        field_metadata_items.append(("nullable_reason_zh", "取值与空值"))
        if field_metadata.get(explanation_key) not in (
            field_metadata.get(key) for key, _ in field_metadata_items
        ):
            field_metadata_items.append(
                (explanation_key, "枚举含义" if explanation_key == "enum_values_zh" else "如何生成")
            )
        key_field_metadata_df = pd.DataFrame(
            [
                {"阅读要点": label, "说明": field_metadata[key]}
                for key, label in field_metadata_items
                if key in field_metadata
            ],
            columns=["阅读要点", "说明"],
        )

        # HTML.value 原地更新，不再通过 Output/clear_output 往前端反复发送输出消息。
        field_metadata_html.value = (
            '<div class="schema-browser-scroll">' + key_field_metadata_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table schema-browser-summary",
            )
            + '</div><details class="schema-browser-details"><summary>'
            + '<span class="schema-browser-expand">完整字段说明</span>'
            + '<span class="schema-browser-collapse">收起完整字段说明</span>'
            + f'（{len(field_metadata_df)} 项）</summary><div class="schema-browser-scroll">'
            + field_metadata_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table schema-browser-metadata",
            )
            + "</div></details>"
        )

    def select_field(change: dict[str, object]) -> None:
        render_field_metadata()

    def select_schema(change: dict[str, object]) -> None:
        selected_schema = schemas_by_table_name[schema_selector.value]
        field_options = [("选择字段查看详情", None)]
        for field in selected_schema:
            field_metadata = {
                key.decode("utf-8"): value.decode("utf-8")
                for key, value in (field.metadata or {}).items()
            }
            field_options.append(
                (
                    f"{field.name}｜{field_metadata.get('field_name_zh', '未定义')}",
                    field.name,
                )
            )

        # 改写选项本身会触发 value 变化；临时解绑以保证这里只渲染一次。
        field_selector.unobserve(select_field, names="value")
        field_selector.options = field_options
        field_selector.value = None
        field_details.selected_index = None
        field_selector.observe(select_field, names="value")
        render_schema_metadata()
        render_field_overview()
        render_field_metadata()
        if data_demo is not None:
            data_demo.set_schema(selected_schema)

    schema_selector.observe(select_schema, names="value")
    field_selector.observe(select_field, names="value")
    render_schema_metadata()
    render_field_overview()
    render_field_metadata()

    field_details = widgets.Accordion(
        [widgets.VBox([field_selector, field_metadata_html]).add_class("schema-browser-field-content")],
        titles=("字段详情 · 按需查看",), selected_index=None,
    ).add_class("schema-browser-field-detail")
    browser = widgets.VBox(
        [
            overview_html.add_class("schema-browser-section"),
            widgets.VBox([
                schema_selector,
                widgets.Box([
                    schema_metadata_html.add_class("schema-browser-card"),
                    widgets.VBox([field_overview_html, field_details]).add_class("schema-browser-card"),
                ]).add_class("schema-browser-grid"),
            ]).add_class("schema-browser-section"),
        ] + ([data_demo] if data_demo is not None else []),
        layout=widgets.Layout(width="100%"),
    ).add_class("schema-browser-root")
    display(browser)

    # 除主 Widget 外也记录其 Layout/Style 子模型，下一次运行时一并关闭。
    tracked_widgets: list[widgets.Widget] = []
    tracked_widget_ids: set[int] = set()
    pending_widgets = [browser]
    while pending_widgets:
        widget = pending_widgets.pop()
        pending_widgets.extend(getattr(widget, "children", ()))
        for candidate in (
            widget,
            getattr(widget, "layout", None),
            getattr(widget, "style", None),
        ):
            if isinstance(candidate, widgets.Widget) and id(candidate) not in tracked_widget_ids:
                tracked_widgets.append(candidate)
                tracked_widget_ids.add(id(candidate))
    _ACTIVE_WIDGETS = tracked_widgets


def display_raw_archive_demo(
    archive_root: Path,
    *,
    filenames: Sequence[str],
    partitions: Sequence[str] = (),
    archive_options: dict[str, Path] | None = None,
) -> None:
    """展示 raw 文件存在状态、大小与已保存摘要；不读取响应正文或重新验收归档。"""
    global _RAW_ACTIVE_WIDGETS
    for widget in reversed(_RAW_ACTIVE_WIDGETS):
        widget.close()
    _RAW_ACTIVE_WIDGETS = []
    archive_root = Path(archive_root)
    if not filenames or len(filenames) > 5 or any(Path(name).name != name for name in filenames):
        raise ValueError("raw 样例只接受 1—5 个明确文件名。")
    selectors = [widgets.Dropdown(style={"description_width": "52px"}).add_class("schema-browser-control") for _ in partitions]
    case_selector = widgets.Dropdown(options=list(archive_options or {}), description="案例", layout=widgets.Layout(width="100%"), style={"description_width": "52px"}).add_class("schema-browser-control")
    case_selector.layout.display = "" if archive_options else "none"
    refresh = widgets.Button(description="刷新", icon="refresh").add_class("schema-browser-button")
    output = widgets.HTML()
    busy = False

    def render(keep: int = 0) -> None:
        nonlocal busy
        if busy:
            return
        busy = True
        archive_dir = archive_root
        rows = []
        try:
            if archive_options and case_selector.value is not None:
                archive_dir = archive_root / archive_options[case_selector.value]
            for index, (name, selector) in enumerate(zip(partitions, selectors, strict=True)):
                selector.description = _DEMO_LABELS.get(name, name) + "："
                try:
                    with os.scandir(archive_dir) as entries:
                        entries = list(islice(entries, _DEMO_DIRECTORY_ENTRIES + 1))
                except FileNotFoundError:
                    entries = []
                if len(entries) > _DEMO_DIRECTORY_ENTRIES:
                    raise ValueError("当前归档层级目录过多。")
                options = sorted(
                    [entry.name.split("=", 1)[1] for entry in entries
                     if entry.name.startswith(name + "=") and entry.is_dir(follow_symlinks=False)],
                    reverse=True,
                )
                previous = selector.value
                selector.options = options
                selector.value = previous if index < keep and previous in options else (options[0] if options else None)
                selector.disabled = not options
                if selector.value is None:
                    for later in selectors[index + 1:]:
                        later.options = ()
                        later.disabled = True
                    break
                archive_dir /= f"{name}={selector.value}"
            for filename in filenames:
                path = archive_dir / filename
                try:
                    size = path.stat().st_size
                    if not path.is_file():
                        raise IsADirectoryError(f"{filename} 不是文件。")
                    digest = "—"
                    if filename.endswith(".sha256"):
                        if size > 128:
                            raise ValueError(f"{filename} 超出摘要文本长度。")
                        with path.open("rb") as stream:
                            raw_digest = stream.read(129)
                        digest = raw_digest.decode("ascii").strip()
                        if len(digest) != 64 or any(character not in "0123456789abcdefABCDEF" for character in digest):
                            raise ValueError(f"{filename} 不是有效的 SHA-256 摘要文本。")
                    rows.append({"文件": filename, "状态": "已存在", "字节数": str(size), "已保存摘要": digest})
                except FileNotFoundError:
                    rows.append({"文件": filename, "状态": "尚未归档", "字节数": "—", "已保存摘要": "—"})
            message = "文件概览 · 存在状态不代表已通过质量验收"
        except Exception as error:
            message = f"读取失败：{type(error).__name__}：{error}"
        finally:
            busy = False
        output.value = (
            f'<p class="schema-browser-status">{escape(message)}</p>'
            + "<div class='schema-browser-scroll'>"
            + pd.DataFrame(rows, columns=["文件", "状态", "字节数", "已保存摘要"]).to_html(
                index=False, escape=True, border=0, na_rep="—", classes="schema-browser-table schema-browser-demo",
            ) + '</div><details class="schema-browser-details schema-browser-path"><summary>归档位置</summary>'
            + escape(str(archive_dir)) + "</details>"
        )

    for index, selector in enumerate(selectors):
        selector.observe(lambda change, index=index: render(index + 1), names="value")
    case_selector.observe(lambda change: render(), names="value")
    refresh.on_click(lambda button: render(len(selectors)))
    render()
    browser = widgets.VBox([
        widgets.HTML(_TABLE_STYLE),
        widgets.HBox([widgets.HTML('<h4 class="schema-browser-heading" style="margin:0">raw 归档样例</h4>', layout=widgets.Layout(width="auto")), refresh]).add_class("schema-browser-toolbar"),
        case_selector,
        widgets.HBox(selectors).add_class("schema-browser-controls"), output,
    ]).add_class("schema-browser-root")
    display(browser)
    pending_widgets = [browser]
    while pending_widgets:
        widget = pending_widgets.pop()
        pending_widgets.extend(getattr(widget, "children", ()))
        for candidate in (widget, getattr(widget, "layout", None), getattr(widget, "style", None)):
            if isinstance(candidate, widgets.Widget) and candidate not in _RAW_ACTIVE_WIDGETS:
                _RAW_ACTIVE_WIDGETS.append(candidate)
