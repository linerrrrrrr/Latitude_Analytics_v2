"""在项目 Notebook 开篇交互呈现 Arrow Schema 与字段 metadata。"""

from __future__ import annotations

from collections.abc import Sequence

import ipywidgets as widgets
import pandas as pd
import pyarrow as pa
from IPython.display import HTML, display


_ACTIVE_WIDGETS: list[widgets.Widget] = []

_TABLE_STYLE = """
<style>
.schema-browser-table {
    color-scheme: light dark;
    border-collapse: collapse;
    width: 100%;
    margin: 0;
    font-size: 13px;
    color: var(
        --vscode-editor-foreground,
        var(--jp-content-font-color1, CanvasText)
    );
    background-color: var(
        --vscode-editor-background,
        var(--jp-layout-color1, Canvas)
    );
}
.schema-browser-table th,
.schema-browser-table td {
    border: 1px solid var(
        --vscode-panel-border,
        var(--jp-border-color2, GrayText)
    );
    padding: 6px 8px;
    text-align: left;
    vertical-align: top;
    white-space: normal;
    word-break: break-word;
    color: var(
        --vscode-editor-foreground,
        var(--jp-content-font-color1, CanvasText)
    );
}
.schema-browser-table thead > tr > th {
    color: var(
        --vscode-list-activeSelectionForeground,
        HighlightText
    );
    background-color: var(
        --vscode-list-activeSelectionBackground,
        Highlight
    );
    border-bottom: 2px solid var(
        --vscode-focusBorder,
        Highlight
    );
    font-weight: 700;
}
.schema-browser-table tbody > tr:nth-child(odd) > td {
    background-color: var(
        --vscode-editor-background,
        var(--jp-layout-color1, Canvas)
    );
}
.schema-browser-table tbody > tr:nth-child(even) > td {
    background-color: var(
        --vscode-list-hoverBackground,
        var(--jp-layout-color2, ButtonFace)
    );
}
</style>
"""


def display_schema_metadata(schemas: Sequence[pa.Schema]) -> None:
    """展示 Schema 列表、表级 metadata、字段目录和字段级 metadata。"""
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
        schema_options.append((f"{table_name}｜{table_name_zh}", table_name))
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
    display(
        HTML(
            _TABLE_STYLE
            + "<h4>Schema 列表</h4>"
            + overview_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table",
            )
        )
    )

    schema_selector = widgets.Dropdown(
        options=schema_options,
        value=schema_options[0][1],
        description="Schema：",
        layout=widgets.Layout(
            width="720px",
            max_width="100%",
            margin="24px 0 4px 0",
        ),
        style={"description_width": "70px"},
    )
    field_selector = widgets.Dropdown(
        description="Field：",
        layout=widgets.Layout(
            width="720px",
            max_width="100%",
            margin="28px 0 4px 0",
        ),
        style={"description_width": "70px"},
    )
    schema_metadata_html = widgets.HTML()
    field_overview_html = widgets.HTML()
    field_metadata_html = widgets.HTML()

    initial_schema = schemas_by_table_name[schema_selector.value]
    initial_field_options = []
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
    field_selector.value = initial_field_options[0][1]

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

        schema_metadata_html.value = (
            _TABLE_STYLE
            + schema_metadata_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table",
            )
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
        field_overview_html.value = (
            _TABLE_STYLE
            + field_overview_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table",
            )
        )

    def render_field_metadata() -> None:
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

        # HTML.value 原地更新，不再通过 Output/clear_output 往前端反复发送输出消息。
        field_metadata_html.value = (
            _TABLE_STYLE
            + field_metadata_df.to_html(
                index=False,
                escape=True,
                border=0,
                classes="schema-browser-table",
            )
        )

    def select_field(change: dict[str, object]) -> None:
        render_field_metadata()

    def select_schema(change: dict[str, object]) -> None:
        selected_schema = schemas_by_table_name[schema_selector.value]
        field_options = []
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
        field_selector.value = field_options[0][1]
        field_selector.observe(select_field, names="value")
        render_schema_metadata()
        render_field_overview()
        render_field_metadata()

    schema_selector.observe(select_schema, names="value")
    field_selector.observe(select_field, names="value")
    render_schema_metadata()
    render_field_overview()
    render_field_metadata()

    browser = widgets.VBox(
        [
            schema_selector,
            schema_metadata_html,
            field_overview_html,
            field_selector,
            field_metadata_html,
        ],
        layout=widgets.Layout(width="100%"),
    )
    display(browser)

    # 除主 Widget 外也记录其 Layout/Style 子模型，下一次运行时一并关闭。
    tracked_widgets: list[widgets.Widget] = []
    tracked_widget_ids: set[int] = set()
    for widget in (
        schema_selector,
        schema_metadata_html,
        field_overview_html,
        field_selector,
        field_metadata_html,
        browser,
    ):
        for candidate in (
            widget,
            getattr(widget, "layout", None),
            getattr(widget, "style", None),
        ):
            if isinstance(candidate, widgets.Widget) and id(candidate) not in tracked_widget_ids:
                tracked_widgets.append(candidate)
                tracked_widget_ids.add(id(candidate))
    _ACTIVE_WIDGETS = tracked_widgets
