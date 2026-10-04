"""Manual desktop controller; all business parameters come from the selected file."""
from __future__ import annotations

import hashlib
import codecs
import json
import os
import pathlib
import re
import runpy
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from PySide6.QtCore import Qt, QRectF, QSettings, QSize, QTimer, QUrl
from PySide6.QtGui import QColor, QDesktopServices, QFont, QIcon, QPainter, QPainterPath, QPalette, QPen, QPixmap, QTextBlockFormat, QTextCharFormat, QTextCursor, QTextDocument, QTextFormat
from PySide6.QtWidgets import (
    QApplication, QButtonGroup, QCheckBox, QComboBox, QDialog, QFormLayout, QFrame, QGridLayout, QHBoxLayout, QHeaderView,
    QLabel, QLineEdit, QMainWindow, QMessageBox, QPlainTextEdit, QProgressBar, QPushButton,
    QScrollArea, QSplitter, QStackedWidget, QTabBar, QTabWidget, QTextBrowser,
    QTreeWidget, QTreeWidgetItem, QVBoxLayout, QWidget,
)

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        PROJECT_ROOT = candidate_root
        break
else:
    raise RuntimeError("未找到项目根目录")

OPERATIONS_ROOT = PROJECT_ROOT / "02_Market_Data/a01_Collection" / "operations"
sys.path.insert(0, str(OPERATIONS_ROOT / "runtime"))

import click
import psutil
import background_worker as worker
from invoke_exported_click_entrypoint import COLLECTION_DIRS, EntryPointContract, read_cli_contract
from config.data_contracts import TRADE_CALENDAR_SCHEMA

TRADE_CALENDAR_TABLE_NAME = TRADE_CALENDAR_SCHEMA.metadata[b"table_name"].decode("utf-8")
TRADE_CALENDAR_PARTITIONS = TRADE_CALENDAR_SCHEMA.metadata[b"partition_columns"].decode("utf-8").split(",")
TRADE_CALENDAR_PRIMARY_KEY = TRADE_CALENDAR_SCHEMA.metadata[b"primary_key"].decode("utf-8").split(",")

INVOKER_PATH = OPERATIONS_ROOT / "runtime" / "invoke_exported_click_entrypoint.py"
PYTHON_PATH = pathlib.Path(sys.executable).with_name("python.exe") if os.name == "nt" else pathlib.Path(sys.executable)
DISPLAY_TIMEZONE = timezone(timedelta(hours=8), "北京时间")
GROUP_LABELS = {"b01": "国内期货", "b02": "交易所报告", "b03": "外部市场", "b04": "宏观与利率"}
ENTRY_LABELS = {
    "c01_trade_calendar": "交易日历", "c02_futures_variety_calendar": "品种日历",
    "c03_futures_contract_calendar": "合约日历", "c04_futures_bar_calendar": "行情日历",
    "c05_futures_daily": "日线行情", "c06_futures_minute": "分钟行情",
    "c07_suspected_session_reconciliation": "疑似休市校对", "c08_full_minute_quality": "分钟缺失全量审计",
    "c01_exchange_report_calendar": "交易所报告日历", "c01a_position_rank_special_case_calibration": "排名特殊案例证据",
    "c02_futures_holding_reports": "成交持仓排名", "c03_warehouse_receipt": "仓单",
    "c01_external_market_calendar": "外部市场日历", "c02_domestic_spot_basis": "生意社原文归档",
    "c03_overseas_futures": "海外期货", "c04_external_index": "外部指数",
    "c01_macro_release_calendar": "宏观发布日历", "c02_interest_rate": "SHIBOR 利率", "c03_macro_release": "宏观事实",
}
OPTION_LABELS = {
    "--lake-root": "湖仓根目录", "--start-date": "起始日期", "--end-date": "结束日期",
    "--full": "全历史比较", "--write": "写入业务结果", "--quota-reserve": "预留配额",
    "--contract-code": "合约代码", "--force": "强制定向校对",
    "--confirm-full-quality": "确认执行全量分钟缺失审计",
    "--performance-window-size": "性能检查分区数", "--performance-max-median-seconds": "中位耗时上限（秒）",
}
STATE_LABELS = {"starting": "初始化", "waiting_for_monitor": "等待监控", "running": "运行中",
                "succeeded": "完成", "failed": "失败", "quota_stopped": "配额停止", "interrupted": "已中断"}
LAKEHOUSE_ROOT = OPERATIONS_ROOT.parent
MAINTENANCE_TOOLS = {
    "verify_runtime": ("Python 运行环境", LAKEHOUSE_ROOT / "b00_01_verify_runtime.py", (),
                       "检查 latitude_env_v2 解释器与核心依赖；不调用业务 API。"),
    "verify_control": ("Windows 控制面 I/O", OPERATIONS_ROOT / "runtime" / "verify_operations_runtime.py", (),
                       "检查长路径、原子替换、共享读取与临时 Arrow 文件；不读写业务湖。"),
    "check_code": ("Notebook 代码正文检查", LAKEHOUSE_ROOT / "b00_02_sync_notebook_exports.py", ("--check", "--check-level", "code"),
                   "检查 19 份 Notebook 与脚本的代码结构；说明、注释及排版差异只提示。这是采集启动的硬性检查。"),
    "check_full": ("Notebook 完整导出检查", LAKEHOUSE_ROOT / "b00_02_sync_notebook_exports.py", ("--check", "--check-level", "full"),
                   "逐字节比较 19 份默认导出，包括说明、注释和单元标记；只检查，不修改文件。"),
    "sync_exports": ("重新生成完整 Python 导出", LAKEHOUSE_ROOT / "b00_02_sync_notebook_exports.py", ("--write",),
                     "从 19 份 Notebook 完整生成同名 .py 并复核。会覆盖这些导出脚本；Notebook 不变。"),
}
B00_PAGES = {
    "b00_01": ("运行环境与检查", (("verify_runtime", "运行环境"), ("verify_control", "辅助检查 · Windows 控制面"))),
    "b00_02": ("Notebook 检查与同步", (("check_code", "代码检查"), ("check_full", "完整检查"), ("sync_exports", "导出同步"))),
    "b00_03": ("Schema 与样例浏览", (("schema_browser", "流程与职责"),)),
    "b00_04": ("路径提交与失败恢复", (("staged_transaction", "流程与职责"),)),
}
# Operation views share a page where they belong to the same script. Counts are event-backed.
B00_VIEWS = {
    "verify_runtime": ("01", "Python 运行环境", "核对解释器与核心依赖，逐项确认采集运行条件。", (("environment", "解释器与环境"), ("packages", "核心依赖版本"))),
    "verify_control": ("01 · I/O", "Windows 控制面", "检查后台状态通道与长路径文件读写。", (("atomic_path", "长路径原子替换"), ("shared_read", "共享读取与再次替换"), ("arrow_roundtrip", "Arrow 文件往返"))),
    "check_code": ("02 · CODE", "Notebook 代码检查", "生成预期导出，再逐文件比较 Python 代码结构。", (("export", "生成与语法验证"), ("check", "Python 正文比较"))),
    "check_full": ("02 · FULL", "Notebook 完整检查", "生成预期导出，再逐字节核对完整脚本。", (("export", "生成与语法验证"), ("check", "完整字节比较"))),
    "sync_exports": ("02 · SYNC", "Notebook 导出同步", "完整生成、写入与复核；各步骤分别记录实际完成量。", (("export", "生成与语法验证"), ("write", "写入 Python 导出"), ("check", "写后完整复核"))),
    "schema_browser": ("03", "Schema 与样例浏览", "由 Notebook 调用的展示模块；本页呈现调用流程与职责。", (("schema", "读取权威 Schema"), ("summary", "展示关键内容"), ("details", "折叠完整元数据"), ("sample", "显式启用有界样例"))),
    "staged_transaction": ("04", "路径提交与失败恢复", "由业务入口提供目标与验收；本页呈现事务流程与恢复边界。", (("staging", "调用方准备 staging"), ("install", "备份旧目标并安装"), ("verify", "调用方验收"), ("finish", "成功清理 / 失败恢复"))),
}
ENTRY_LABELS.update({key: value[0] for key, value in MAINTENANCE_TOOLS.items()})
# Dedicated calendar views describe presentation only; planning remains in each producer.
CALENDAR_VIEWS = {
    "b01/c01_trade_calendar": {
        "table": "dim_trade_calendar",
        "description": "自然日范围 → 一次交易日请求 → 构造与验收 → 年度分区提交",
        "boundary": "安装叶数属于当前共同事务；只有事务退出后的 committed 记录确认正式提交。",
        "flows": (
            ("确定自然日范围", "run existing_dataset plan"),
            ("获取交易日", "collect authenticate trade_days"),
            ("构造与比较", "build_validate reconcile"),
            ("合并与暂存", "commit merge_validate staging_write staging_readback"),
            ("安装与正式复读", "install_partitions"),
            ("共同事务确认", "transaction_complete")),
        "bars": (("本次自然日构造", "collect", "natural_days"), ("当前事务安装", "install_partitions", "installed_leaves")),
        "metrics": (("自然日结果", "collect", "rows"), ("其中交易日", "collect", "trading_day_count"),
                    ("待更新自然日", "reconcile", "missing_or_revised_grid_count"), ("当前事务分区", "merge_validate", "partitions")),
    },
    "b01/c02_futures_variety_calendar": {
        "table": "dim_futures_variety_calendar",
        "description": "上游交易日 → 完整合约目录 → 逐日展开品种 → 差异分区提交",
        "boundary": "日期展开按交易日计数。合并与安装属于当前事务；全量模式可能依次提交多个日期范围。",
        "flows": (
            ("读取上游日期", "run collect upstream_read upstream_reuse existing_dataset plan"),
            ("获取与验收目录", "authenticate contract_catalog catalog_validation"),
            ("逐日展开品种", "expand reconcile"),
            ("合并与暂存", "commit input_validation merge merge_validate staging_write staging_readback staging"),
            ("安装与正式复读", "install_partitions"),
            ("共同事务确认", "transaction_complete full_reconciliation")),
        "bars": (("本次交易日展开", "expand", "expanded_dates"), ("当前事务分区合并", "merge", "merged_leaves"),
                 ("当前事务安装", "install_partitions", "installed_leaves")),
        "metrics": (("上游交易日", "upstream_read upstream_reuse collect", "trading_dates"), ("有效合约目录", "catalog_validation", "fixed_contracts"),
                    ("生成品种日历行", "collect expand", "rows"), ("待更新格点", "reconcile", "missing_or_incomplete_grid_count")),
    },
    "b01/c03_futures_contract_calendar": {
        "table": "dim_futures_contract_calendar",
        "description": "来源合约与分批信息 → 时段展开 → 分区核对 → 逐叶提交 → 处理水位",
        "boundary": "信息批次返回、分区生成、正式叶提交分别计量。日常处理水位只在独立水位提交完成后确认。",
        "flows": (
            ("读取范围与目录", "run existing_dataset collect authenticate contract_catalog catalog_validation candidate_selection"),
            ("批量合约信息", "contract_info info_coverage catalog_merge previous_trading_dates boundary_trade_day"),
            ("展开与逐区核对", "build expand output_validation reconcile partition_detail"),
            ("逐叶暂存与验收", "commit existing_partition merge_validate staging_write staging_readback staging"),
            ("正式安装与提交", "install formal_readback commit_batch"),
            ("记录处理水位", "watermark watermark_staging_write watermark_staging_readback watermark_install")),
        "bars": (("本次合约信息请求", "contract_info", "info_batches"), ("本次分区核对", "reconcile", "compared_partitions"),
                 ("当前分区品种日展开", "build expand", "variety_dates"), ("本次正式叶提交", "commit_batch", "committed_leaves")),
        "metrics": (("待核对合约", "candidate_selection", "contracts"), ("预期 Session", "reconcile", "expected_session_count"),
                    ("缺失 Session", "reconcile", "missing_session_count"), ("待更新分区", "reconcile", "touched_partition_count")),
    },
    "b01/c04_futures_bar_calendar": {
        "table": "dim_futures_bar_calendar",
        "description": "本地文件发现 → 读取现有水位 → 按分区生成 1m / 1d 结构 → 保留状态并提交",
        "boundary": "水位步骤读取已有目标。频率进度仅属于当前分区；处理叶数包含复核后跳过的 clean 叶。",
        "flows": (
            ("发现文件与分区", "run discovery dataset_open fragment_schema partition_discovery"),
            ("读取现有水位", "watermark"),
            ("规划基础分区", "plan read read_leaf partition_detail"),
            ("生成频率结构", "build_structure minute_structure daily_structure build_fresh"),
            ("保留状态与验收", "commit input_conversion paths merge dirty_validation staging_write staging_readback"),
            ("正式安装与复读", "prepare_install install schema_marker formal_readback commit_batch")),
        "bars": (("已有水位读取", "watermark", "watermark_groups"), ("本次基础分区规划", "plan", "planned_partitions"),
                 ("当前分区结构生成", "build_structure", "frequencies"), ("本次提交计划处理", "commit_batch", "processed_leaves")),
        "metrics": (("上游文件", "discovery", "upstream_files"), ("现有目标文件", "discovery", "target_files"),
                    ("待更新分区", "plan", "touched_partition_count"), ("缺失格点", "plan", "missing_grid_count")),
    },
}
PHASE_LABELS = {
    "environment": "解释器与环境", "packages": "核心依赖版本", "atomic_path": "长路径原子替换",
    "shared_read": "共享读取与再次替换", "arrow_roundtrip": "Arrow 文件往返",
    "run": "执行", "plan": "确定待办", "planning": "确定待办", "read": "读取数据", "scan": "扫描计划",
    "upstream_read": "读取上游", "existing_read": "读取现有数据", "expand": "展开日历", "build": "生成结果",
    "fetch": "请求来源", "query": "请求来源", "request": "请求来源", "api": "请求来源", "api_window": "请求窗口",
    "normalize": "转换与验收", "validate": "业务验收", "reconcile": "对账", "compare": "比较差异",
    "staging": "暂存与复读", "commit": "提交与复读", "install": "安装目标", "rollback": "恢复现场",
    "calendar_state": "登记日历状态", "calendar_commit": "提交日历", "fact_commit": "提交事实",
    "failure_state": "记录失败", "failure_calendar_commit": "保存失败状态", "merge_fact": "合并事实",
    "session": "准备连接", "auth": "认证", "pagination": "分页请求", "export": "生成预期导出", "check": "检查导出", "write": "写入完整导出",
    "dataset_open": "打开数据集", "read_dataset": "读取数据集", "materialize": "读取数据到内存",
    "formal_readback": "正式复读", "formal_verify": "正式验收", "staging_readback": "暂存复读", "staging_write": "写入暂存",
    "generate_state": "生成状态", "generate": "生成结果", "collect": "采集", "collect_batch": "本批采集",
    "commit_batch": "本批提交", "commit_leaf": "提交当前叶", "commit_group": "提交当前组", "commit_calendar": "回写日历",
    "partition_discovery": "发现分区", "discovery": "发现输入", "partition": "处理分区",
    "read_leaf": "读取分区", "leaf_read": "读取分区", "read_leaf_summary": "读取分区摘要", "fragment_schema": "检查文件结构",
    "request_plan": "规划请求", "api_request": "请求来源", "watermark": "推进处理水位", "completion_state": "登记完成状态",
    "validate_calendar": "验收日历", "special_case": "核对特殊案例", "merge_validate": "合并并验收", "authenticate": "认证",
    "policy_plan": "应用采集范围", "merge_scope": "合并当前范围", "generate_facts": "生成事实", "generate_fact": "生成事实", "merge": "合并结果",
    "build_fact_leaves": "构造事实叶", "build_calendar_leaf": "构造日历叶", "quota_read": "检查来源配额", "verify": "核验",
    "count_grids": "统计格点", "verify_raw": "核验原文", "assemble_month": "汇总月份", "state_repair": "修复完成状态",
    "contract_catalog": "读取合约目录", "catalog_validation": "验收合约目录", "build_structure": "生成行情结构",
    "calendar_read": "读取日历", "read_calendar_plan": "读取日历待办", "completion_summary": "完成摘要",
    "auto_plan": "自动计划", "explicit_plan": "指定范围计划", "request_batch": "当前请求包", "fetch_progress": "请求接受进度",
    "pagination_progress": "分页进度", "partition_plan": "分区计划", "partition_start": "开始处理分区",
}
OBJECT_LABELS = {"contract": "合约", "contract_code": "合约", "trading_date": "交易日", "date": "日期",
                 "exchange": "交易所", "exchange_code": "交易所", "underlying_code": "品种", "variety": "品种",
                 "partition": "分区", "key": "分组", "grid": "格点", "report": "报告", "api": "接口",
                 "function": "函数", "table": "表", "dataset": "数据集", "start": "起始", "end": "结束",
                 "start_date": "起始日期", "end_date": "结束日期", "page": "当前页", "case_id": "案例",
                 "script": "脚本", "notebook": "Notebook", "check_level": "检查层级", "passed": "通过", "warnings": "提示", "failed": "失败"}
OBJECT_LABELS.update({"source_api": "接口", "underlying": "品种", "batch": "当前包", "sessions": "时段数",
                      "python": "解释器", "environment": "环境", "package": "依赖", "version": "版本", "check": "检查项目",
                      "pending_sessions": "待办 Session", "complete_sessions": "此前完成 Session", "selected_sessions": "选定 Session",
                      "pending_partitions": "待办分区", "expected_pending_rows": "理论分钟行数", "pending_grid_count": "待办格点",
                      "planned_grid_count": "计划格点", "completed_required_count": "此前完成格点", "request_batch_count": "请求包数",
                      "pending_grids": "待办格点", "required_grids": "应采格点", "repair_grids": "待修状态格点"})
PHASE_LABELS.update({
    "trade_days": "获取交易日", "build_validate": "构造并验收自然日", "upstream_reuse": "使用已读上游",
    "input_validation": "验收输入", "existing_dataset": "读取现有日历", "install_partitions": "安装与复读（待事务确认）",
    "transaction_complete": "共同事务确认", "full_reconciliation": "全历史比较完成", "candidate_selection": "确定候选合约",
    "contract_info": "分批读取合约信息", "info_coverage": "检查信息覆盖", "catalog_merge": "合并合约目录",
    "previous_trading_dates": "确定前一交易日", "boundary_trade_day": "读取边界交易日", "output_validation": "验收展开结果",
    "partition_detail": "差异分区明细", "existing_partition": "读取原分区", "watermark_staging_write": "暂存处理水位",
    "watermark_staging_readback": "复读处理水位", "watermark_install": "安装处理水位", "minute_structure": "生成 1m 结构",
    "daily_structure": "生成 1d 结构", "build_fresh": "生成新分区状态", "input_conversion": "转换输入",
    "paths": "准备提交路径", "dirty_validation": "验收变更叶", "prepare_install": "准备安装", "schema_marker": "更新 Schema 标记",
})
OBJECT_LABELS.update({"bar_frequency": "频率", "run_id": "当前事务", "mode": "模式", "year": "年", "month": "月",
                      "automatic_tail_processed_through": "处理水位"})

# Both palettes are complete: system dark mode must not supply leftover colors.
THEMES = {
    "light": {
        "canvas": "#f4f6f8", "surface": "#ffffff", "field": "#ffffff", "alternate": "#f7f9fa",
        "text": "#203247", "muted": "#53687c", "border": "#dce3e9", "hover": "#edf4f2",
        "accent": "#116a60", "accent_hover": "#0d594f", "accent_text": "#ffffff",
        "selection": "#e0f0eb", "selection_text": "#145c53",
        "disabled": "#627588", "disabled_bg": "#e7edf2",
        "danger": "#b3263d", "success": "#136b50", "info": "#1e5ba5",
    },
    "dark": {
        "canvas": "#10161f", "surface": "#18212d", "field": "#151e2a", "alternate": "#1c2735",
        "text": "#e7eef7", "muted": "#abbcd0", "border": "#303e50", "hover": "#233441",
        "accent": "#67d6bc", "accent_hover": "#85e3cc", "accent_text": "#0c2722",
        "selection": "#243f3e", "selection_text": "#b0f1de",
        "disabled": "#a0afc2", "disabled_bg": "#243143",
        "danger": "#ff9dac", "success": "#7de0bc", "info": "#94c7ff",
    },
}


def console_icon(symbol: str, color: str = "#ffffff") -> QIcon:
    """Render crisp application and appearance symbols without asset files."""
    icon = QIcon()
    for size in (16, 20, 24, 32, 48, 64, 128, 256):
        pixmap = QPixmap(size, size)
        pixmap.fill(Qt.GlobalColor.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.scale(size / 24, size / 24)
        pen = QPen(QColor(color), 1.7, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap, Qt.PenJoinStyle.RoundJoin)
        painter.setPen(pen)
        if symbol == "latitude":
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor("#116a60"))
            painter.drawRoundedRect(QRectF(0, 0, 24, 24), 6, 6)
            painter.setPen(QPen(QColor("#ffffff"), 2, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
            painter.drawLine(7, 6, 7, 17)
            painter.drawLine(7, 17, 17, 17)
            painter.drawLine(11, 12, 17, 12)
            painter.drawLine(14, 7, 17, 7)
        elif symbol == "sun":
            painter.drawEllipse(QRectF(8, 8, 8, 8))
            for angle in range(0, 360, 45):
                painter.save()
                painter.translate(12, 12)
                painter.rotate(angle)
                painter.drawLine(0, -8, 0, -10)
                painter.restore()
        elif symbol == "moon":
            moon = QPainterPath()
            moon.moveTo(19, 15)
            moon.cubicTo(12, 18, 6, 12, 9, 5)
            moon.cubicTo(2, 8, 4, 19, 12, 20)
            moon.cubicTo(15, 20, 18, 18, 19, 15)
            painter.drawPath(moon)
        else:
            painter.end()
            raise ValueError(f"Unknown console icon: {symbol}")
        painter.end()
        icon.addPixmap(pixmap)
    return icon


def discover_contracts() -> tuple[EntryPointContract, ...]:
    return tuple(read_cli_contract(path) for directory in COLLECTION_DIRS
                 for path in sorted(directory.glob("c*.py")) if path.with_suffix(".ipynb").is_file())


def prepare_request(contracts: tuple[EntryPointContract, ...], selections: dict[str, list[str]]) -> dict:
    """Type-check the finite selection without invoking business callbacks."""
    if not selections:
        raise ValueError("请至少选中一个环节。")
    if set(selections) - {contract.name for contract in contracts}:
        raise ValueError("存在不属于正式入口的环节。")
    tasks = []
    for contract in contracts:
        if contract.name not in selections:
            continue
        if hashlib.sha256(contract.path.read_bytes()).hexdigest() != contract.sha256:
            raise ValueError(f"{contract.name} 已变化，请关闭并重新打开总控台。")
        arguments = list(selections[contract.name])
        if "--help" in arguments or "--" in arguments:
            raise ValueError("批次只接受业务参数，不接受 help 或额外位置参数。")
        with contract.command.make_context(contract.name, arguments.copy()) as context:
            effective_parameters = json.loads(json.dumps(context.params, default=str, ensure_ascii=False))
        tasks.append({"name": contract.name,
                      "entrypoint": contract.path.relative_to(PROJECT_ROOT).as_posix(),
                      "source_sha256": contract.sha256, "arguments": arguments,
                      "effective_parameters": effective_parameters})
    return {"request_version": 1, "created_at": worker.utc_now_text(), "tasks": tasks}


def stages_from_request(request: dict) -> tuple[worker.StageSpec, ...]:
    if request.get("request_version") != 1 or not isinstance(request.get("tasks"), list) or not request["tasks"]:
        raise ValueError("不支持的批次请求。")
    if request.get("kind") == "maintenance":
        expected = prepare_maintenance_request(request.get("tool"))
        if request["tasks"] != expected["tasks"]:
            raise ValueError("维护工具的源码、路径或固定参数已变化。")
        task = request["tasks"][0]
        return (worker.StageSpec(task["name"], pathlib.Path(__file__).resolve(),
                                 ("--execute-tool", request["tool"], task["source_sha256"])),)
    if request.get("kind", "collection") != "collection":
        raise ValueError("未知的批次类型。")
    contracts = discover_contracts()
    by_name = {contract.name: contract for contract in contracts}
    selections = {}
    for task in request["tasks"]:
        contract = by_name.get(task.get("name"))
        if contract is None or contract.name in selections:
            raise ValueError("批次包含未知或重复环节。")
        if task.get("entrypoint") != contract.path.relative_to(PROJECT_ROOT).as_posix() or task.get("source_sha256") != contract.sha256:
            raise ValueError(f"{contract.name} 的路径或源码已变化。")
        arguments = task.get("arguments")
        if not isinstance(arguments, list) or not all(isinstance(argument, str) for argument in arguments):
            raise ValueError("环节参数必须是字符串列表。")
        selections[contract.name] = arguments
    if prepare_request(contracts, selections)["tasks"] != request["tasks"]:
        raise ValueError("批次顺序或有效参数与当前原文件声明不一致。")
    return tuple(worker.StageSpec(task["name"], INVOKER_PATH,
                                  ("--entrypoint-path", str(PROJECT_ROOT / task["entrypoint"]),
                                   "--source-sha256", task["source_sha256"], *task["arguments"]))
                 for task in request["tasks"])


def prepare_maintenance_request(tool_id: str) -> dict:
    """Freeze one allowlisted maintenance operation, never arbitrary commands."""
    if tool_id not in MAINTENANCE_TOOLS:
        raise ValueError("未知维护工具。")
    _, path, arguments, _ = MAINTENANCE_TOOLS[tool_id]
    return {"request_version": 1, "kind": "maintenance", "tool": tool_id,
            "created_at": worker.utc_now_text(), "tasks": [{
                "name": "maintenance/" + tool_id, "entrypoint": path.relative_to(PROJECT_ROOT).as_posix(),
                "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "arguments": list(arguments), "effective_parameters": {"action": tool_id},
            }]}


def read_log_chunk(path: pathlib.Path, offset: int | None) -> tuple[int, bytes, bool]:
    """Read a bounded append-only log chunk, releasing its handle immediately."""
    with path.open("rb") as stream:
        size = os.fstat(stream.fileno()).st_size
        reset = offset is None or offset > size
        start = max(0, size - 262144) if reset else offset
        stream.seek(start)
        content = stream.read(262144)
        return stream.tell(), content, reset


def read_trade_calendar_result(lake_root: str | None, preview_path: pathlib.Path | None = None) -> dict:
    """Read a bounded snapshot of this small dimension; never call a producer or API."""
    import pyarrow as pa
    import pyarrow.dataset as ds
    from config.settings import settings

    source_path = preview_path or ((pathlib.Path(lake_root) if lake_root else settings.futures_lake_root).resolve()
                                  / "silver" / TRADE_CALENDAR_TABLE_NAME)
    if not source_path.exists() or (source_path.is_dir() and next(source_path.rglob("*.parquet"), None) is None):
        return {"path": str(source_path), "exists": False, "rows": [], "read_at": worker.utc_now_text()}
    partitioning = ds.partitioning(pa.schema([TRADE_CALENDAR_SCHEMA.field(name) for name in TRADE_CALENDAR_PARTITIONS]), flavor="hive")
    calendar_dataset = ds.dataset(source_path, format="parquet", partitioning=partitioning if source_path.is_dir() else None)
    calendar_batches = []
    row_count = 0
    for batch in calendar_dataset.scanner(columns=TRADE_CALENDAR_SCHEMA.names, batch_size=4096).to_batches():
        row_count += batch.num_rows
        if row_count > 50000:
            raise ValueError("交易日历超过 50,000 行的界面读取上限，请检查目标表；未显示截断结果。")
        calendar_batches.append(batch)
    calendar_table = pa.Table.from_batches(calendar_batches).select(TRADE_CALENDAR_SCHEMA.names) if calendar_batches else None
    rows = calendar_table.sort_by([(field, "descending") for field in TRADE_CALENDAR_PRIMARY_KEY]).to_pylist() if calendar_table is not None else []
    return {"path": str(source_path), "exists": True, "rows": rows, "read_at": worker.utc_now_text()}


def start_detached_batch(request: dict) -> tuple[pathlib.Path, subprocess.Popen]:
    """Launch exactly the reviewed request in a fresh evidence directory."""
    stages_from_request(request)
    if worker.GLOBAL_LOCK_PATH.exists():
        raise RuntimeError("已有批次锁。请查看运行历史和锁现场；总控台不会自动清锁。")
    run_root = worker.RUN_HISTORY_ROOT / (datetime.now(timezone.utc).strftime("console-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:8])
    run_root.mkdir(parents=True, exist_ok=False)
    request_path = run_root / "request.json"
    with request_path.open("x", encoding="utf-8", newline="\n") as stream:
        json.dump(request, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    with (run_root / "monitor.pid").open("x", encoding="utf-8") as stream:
        stream.write(str(os.getpid()) + "\n")
    worker.atomic_write_json(run_root / "monitor.json", {
        "pid": os.getpid(), "process_created_at": psutil.Process().create_time(), "heartbeat_at": worker.utc_now_text(),
    })
    environment = os.environ.copy()
    environment.update(PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    try:
        with (run_root / "bootstrap.log").open("xb") as log_stream:
            launch_options = {"creationflags": subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
            process = subprocess.Popen(
                [str(PYTHON_PATH), str(pathlib.Path(__file__).resolve()), "--execute-request", str(request_path)],
                cwd=PROJECT_ROOT, env=environment, stdin=subprocess.DEVNULL,
                stdout=log_stream, stderr=subprocess.STDOUT, close_fds=True, **launch_options,
            )
    except Exception as error:
        worker.atomic_write_json(run_root / "launch_failure.json", {"error": str(error), "at": worker.utc_now_text()})
        raise RuntimeError(f"后台启动失败，现场保留在 {run_root}：{error}") from error
    return run_root, process


def request_interruption(run_root: pathlib.Path) -> None:
    """Repeated requests preserve the original durable evidence."""
    try:
        with (run_root / "interrupt.request").open("x", encoding="utf-8") as stream:
            stream.write("requested_at=" + worker.utc_now_text() + "\n")
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        pass


def elapsed_text(start: str | None, finish: str | None = None) -> str:
    if not start:
        return "—"
    try:
        end = datetime.fromisoformat(finish) if finish else datetime.now(timezone.utc)
        seconds = max(0, int((end - datetime.fromisoformat(start)).total_seconds()))
        return f"{seconds // 3600:02d}:{seconds // 60 % 60:02d}:{seconds % 60:02d}"
    except (TypeError, ValueError):
        return "时间不可解析"


def read_history() -> tuple[list[tuple[pathlib.Path, list[str]]], str]:
    """Read historical evidence independently of the UI thread."""
    rows = []
    run_roots = {path.parent for filename in ("status.json", "request.json", "launch_failure.json", "bootstrap.log")
                 for path in worker.RUN_HISTORY_ROOT.rglob(filename)}
    # A request is already a batch record, even if the child never published status.
    for run_root in sorted(run_roots, reverse=True):
        try:
            status_path = run_root / "status.json"
            if status_path.exists():
                status = worker.read_json_shared(status_path)
                if not isinstance(status, dict):
                    raise ValueError("status.json 必须是 JSON 对象。")
                state = str(status.get("state") or "unknown")
                values = [str(status.get("started_at") or ""), STATE_LABELS.get(state, state),
                          str(status.get("stage_name") or ""), run_root.name]
            else:
                request_path = run_root / "request.json"
                request = worker.read_json_shared(request_path) if request_path.exists() else {}
                if not isinstance(request, dict):
                    raise ValueError("request.json 必须是 JSON 对象。")
                failed = (run_root / "launch_failure.json").exists()
                values = [str(request.get("created_at") or ""), "启动失败" if failed else "未发布状态",
                          "查看启动日志与现场", run_root.name]
        except (OSError, ValueError) as error:
            values = ["不可读取", "错误", str(error), run_root.name]
        rows.append((run_root, values))
    # Sort by recorded start time, rather than an old batch's later file update.
    def started_timestamp(row):
        try:
            started_at = datetime.fromisoformat(row[1][0])
            return started_at.timestamp() if started_at.tzinfo is not None else float("-inf")
        except (TypeError, ValueError):
            return float("-inf")
    rows.sort(key=started_timestamp, reverse=True)
    lock_text = "当前没有批次锁。历史和 referance 快照保持只读。"
    if worker.GLOBAL_LOCK_PATH.exists():
        try:
            owner = worker.read_json_shared(worker.GLOBAL_LOCK_PATH / "owner.json")
            lock_text = "当前批次锁：" + json.dumps(owner, ensure_ascii=False)
        except (OSError, ValueError) as error:
            lock_text = f"锁存在但 owner 无法读取：{error}。需人工核查。"
    return rows, lock_text


class OperationsConsole(QMainWindow):
    """One Qt window owns the visible monitor; collection stays detached."""

    def __init__(self, preferences: QSettings | None = None):
        super().__init__()
        self.preferences = preferences if preferences is not None else QSettings("LatitudeAnalytics", "FuturesOperations")
        self.theme_name = "light"
        self.contracts = ()
        self.by_name = {}
        self.entry_items = {}
        self.parameter_values = {}
        self.parameter_widgets = {}
        self.parameter_pages = {}
        self.stage_items = {}
        self.worker_process = None
        self.active_run_root = None
        self.view_run_root = None
        self.close_after_stop = False
        self.monitor_error = ""
        self.io_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="console-read")
        self.contracts_future = self.io_pool.submit(discover_contracts)
        self.history_future = None
        self.log_future = None
        self.log_key = None
        self.log_offset = None
        self.log_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.view_status = {}
        self.rendered_run_root = None
        self.active_request = None
        self.catalog_reloaded_run = None
        self.b00_items = {}
        self.b00_tool_id = None
        self.b00_page_id = None
        self.b00_selected_tools = {page_id: modes[0][0] for page_id, (_, modes) in B00_PAGES.items()}
        self.b00_runs = {}
        self.b00_log_future = None
        self.b00_log_key = None
        self.b00_log_offset = None
        self.b00_log_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.calendar_name = None
        self.calendar_runs = {}
        self.calendar_requests = {}
        self.calendar_log_future = None
        self.calendar_log_key = None
        self.calendar_log_offset = None
        self.calendar_log_decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.trade_table_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="calendar-table")
        self.trade_table_future = None
        self.trade_table_key = None
        self.trade_table_refresh = 0
        self.trade_table_snapshot = None
        self.trade_table_page = 0
        self.trade_table_auto_source = True
        self.trade_table_run = None

        self.setWindowTitle("Latitude · 采集总控台")
        app_icon = console_icon("latitude")
        QApplication.instance().setWindowIcon(app_icon)
        self.setWindowIcon(app_icon)
        available = QApplication.primaryScreen().availableGeometry()
        self.resize(min(1280, available.width() - 40), min(860, available.height() - 60))
        self.setMinimumSize(900, 640)
        central = QWidget()
        self.setCentralWidget(central)
        outer = QVBoxLayout(central)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        header = QFrame()
        header.setObjectName("appHeader")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(24, 14, 24, 14)
        header_layout.setSpacing(12)
        brand_icon = QLabel()
        brand_icon.setPixmap(app_icon.pixmap(34, 34))
        header_layout.addWidget(brand_icon)
        brand = QLabel("LATITUDE")
        brand.setObjectName("brand")
        header_layout.addWidget(brand)
        product = QLabel("采集总控台")
        product.setObjectName("muted")
        header_layout.addWidget(product)
        header_layout.addStretch()
        self.tabs = QStackedWidget()
        self.configure_tab, self.monitor_tab, self.history_tab = QWidget(), QWidget(), QWidget()
        navigation = QFrame()
        navigation.setObjectName("navigation")
        navigation_layout = QHBoxLayout(navigation)
        navigation_layout.setContentsMargins(4, 4, 4, 4)
        navigation_layout.setSpacing(4)
        self.navigation_group = QButtonGroup(self)
        self.navigation_buttons = []
        for index, (page, title) in enumerate(((self.configure_tab, "采集工作台"), (self.monitor_tab, "运行监控"), (self.history_tab, "历史批次"))):
            self.tabs.addWidget(page)
            button = QPushButton(title)
            button.setObjectName("navigationButton")
            button.setCheckable(True)
            button.clicked.connect(lambda checked, index=index: self.tabs.setCurrentIndex(index))
            self.navigation_group.addButton(button)
            self.navigation_buttons.append(button)
            navigation_layout.addWidget(button)
        self.navigation_buttons[0].setChecked(True)
        self.tabs.currentChanged.connect(lambda index: self.navigation_buttons[index].setChecked(True))
        header_layout.addWidget(navigation)
        header_layout.addSpacing(8)
        self.theme_button = QPushButton()
        self.theme_button.setObjectName("themeToggle")
        self.theme_button.setFixedSize(36, 36)
        self.theme_button.setIconSize(QSize(20, 20))
        self.theme_button.clicked.connect(lambda: self.apply_theme("light" if self.theme_name == "dark" else "dark", persist=True))
        header_layout.addWidget(self.theme_button)
        outer.addWidget(header)
        self.active_batch_banner = QLabel()
        self.active_batch_banner.setObjectName("badge")
        self.active_batch_banner.setWordWrap(True)
        self.active_batch_banner.hide()
        outer.addWidget(self.active_batch_banner)
        outer.addWidget(self.tabs, 1)

        configure_layout = QVBoxLayout(self.configure_tab)
        configure_layout.setContentsMargins(24, 18, 24, 18)
        configure_layout.setSpacing(14)
        page_heading = QLabel("采集工作台")
        page_heading.setObjectName("heading")
        configure_layout.addWidget(page_heading)
        page_hint = QLabel("b00 准备与检查 · b01—b04 采集环节　｜　选择一项进入工作页，勾选采集环节可组合批次。")
        page_hint.setObjectName("muted")
        page_hint.setWordWrap(True)
        configure_layout.addWidget(page_hint)
        self.workbench_heading = page_heading
        self.workbench_hint = page_hint
        split = QSplitter(Qt.Orientation.Horizontal)
        split.setChildrenCollapsible(False)
        split.setHandleWidth(14)
        configure_layout.addWidget(split, 1)

        sidebar = QFrame()
        sidebar.setObjectName("card")
        sidebar.setMinimumWidth(258)
        sidebar_layout = QVBoxLayout(sidebar)
        sidebar_layout.setContentsMargins(12, 14, 12, 10)
        sidebar_layout.setSpacing(10)
        sidebar_caption = QLabel("工作目录")
        sidebar_caption.setObjectName("section")
        sidebar_layout.addWidget(sidebar_caption)
        self.entry_search = QLineEdit()
        self.entry_search.setPlaceholderText("搜索名称或编号")
        self.entry_search.setAccessibleName("搜索工作台环节")
        self.entry_search.setClearButtonEnabled(True)
        self.entry_search.textChanged.connect(self.filter_entries)
        sidebar_layout.addWidget(self.entry_search)
        selection_tools = QHBoxLayout()
        self.daily_button = QPushButton("日常更新配置")
        self.daily_button.setToolTip("选择日常 18 个环节并开启写入；保留其他参数，不选择 b08。")
        self.daily_button.clicked.connect(self.select_daily)
        self.daily_button.setEnabled(False)
        selection_tools.addWidget(self.daily_button, 1)
        clear_button = QPushButton("清空")
        clear_button.setObjectName("quiet")
        clear_button.clicked.connect(self.clear_selection)
        selection_tools.addWidget(clear_button)
        sidebar_layout.addLayout(selection_tools)
        self.entry_tree = QTreeWidget()
        self.entry_tree.setObjectName("entryTree")
        self.entry_tree.setHeaderHidden(True)
        self.entry_tree.setIndentation(12)
        self.entry_tree.setUniformRowHeights(True)
        self.entry_tree.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.entry_tree.currentItemChanged.connect(self.show_parameters)
        self.entry_tree.itemChanged.connect(self.update_selection)
        sidebar_layout.addWidget(self.entry_tree, 1)
        self.search_empty = QLabel("没有匹配环节")
        self.search_empty.setObjectName("muted")
        self.search_empty.hide()
        sidebar_layout.addWidget(self.search_empty)
        split.addWidget(sidebar)

        detail_card = QFrame()
        detail_card.setObjectName("card")
        detail_layout = QVBoxLayout(detail_card)
        detail_layout.setContentsMargins(22, 18, 22, 12)
        detail_layout.setSpacing(10)
        detail_header = QHBoxLayout()
        self.detail_layout = detail_layout
        self.detail_header = detail_header
        self.entry_code = QLabel("正式入口")
        self.entry_code.setObjectName("badge")
        detail_header.addWidget(self.entry_code)
        detail_header.addStretch()
        self.entry_context = QLabel()
        self.entry_context.setObjectName("muted")
        detail_header.addWidget(self.entry_context)
        detail_layout.addLayout(detail_header)
        self.entry_title = QLabel("选择一个环节")
        self.entry_title.setObjectName("heading")
        detail_layout.addWidget(self.entry_title)
        self.entry_path_label = QLabel()
        self.entry_path_label.setObjectName("code")
        self.entry_path_label.setWordWrap(True)
        self.entry_path_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        detail_layout.addWidget(self.entry_path_label)
        self.entry_summary = QLabel()
        self.entry_summary.setWordWrap(True)
        self.entry_summary.setTextFormat(Qt.TextFormat.PlainText)
        self.entry_summary.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        detail_layout.addWidget(self.entry_summary)
        self.detail_tabs = QTabWidget()
        self.detail_tabs.setObjectName("detailTabs")
        self.detail_tabs.setDocumentMode(True)
        parameter_panel = QWidget()
        parameter_layout = QVBoxLayout(parameter_panel)
        parameter_layout.setContentsMargins(0, 14, 0, 0)
        parameter_layout.setSpacing(10)
        parameter_caption = QLabel("留空沿用原文件默认值；点击运行时固定本次参数。")
        parameter_caption.setObjectName("muted")
        parameter_caption.setWordWrap(True)
        parameter_layout.addWidget(parameter_caption)
        self.parameter_stack = QStackedWidget()
        parameter_layout.addWidget(self.parameter_stack, 1)
        self.detail_tabs.addTab(parameter_panel, "运行参数")
        self.notes = QTextBrowser()
        self.notes.setObjectName("notes")
        self.notes.setOpenLinks(False)
        self.detail_tabs.addTab(self.notes, "完整环节说明")
        self.calendar_page = QStackedWidget()
        self.calendar_scroll = QScrollArea()
        self.calendar_scroll.setWidgetResizable(True)
        self.calendar_body = QWidget()
        self.calendar_scroll.setWidget(self.calendar_body)
        self.calendar_page.addWidget(self.calendar_scroll)
        calendar_layout = QVBoxLayout(self.calendar_body)
        calendar_layout.setContentsMargins(0, 12, 12, 12)
        calendar_layout.setSpacing(10)
        self.calendar_description = QLabel()
        self.calendar_description.setWordWrap(True)
        self.calendar_description.setObjectName("section")
        calendar_layout.addWidget(self.calendar_description)
        dashboard_actions = QHBoxLayout()
        self.calendar_state = QLabel("尚未运行")
        self.calendar_state.setObjectName("badge")
        dashboard_actions.addWidget(self.calendar_state)
        self.calendar_health = QLabel()
        self.calendar_health.setWordWrap(True)
        self.calendar_health.setObjectName("muted")
        dashboard_actions.addWidget(self.calendar_health, 1)
        self.calendar_interrupt = QPushButton("中断本批次")
        self.calendar_interrupt.setObjectName("dangerButton")
        self.calendar_interrupt.clicked.connect(self.interrupt)
        dashboard_actions.addWidget(self.calendar_interrupt)
        calendar_layout.addLayout(dashboard_actions)
        self.calendar_position = QLabel()
        self.calendar_position.setWordWrap(True)
        calendar_layout.addWidget(self.calendar_position)
        self.calendar_flow_widgets = []
        flow_layout = QGridLayout()
        for index in range(6):
            card = QFrame()
            card.setObjectName("card")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(10, 8, 10, 8)
            title, state_label = QLabel(), QLabel()
            title.setWordWrap(True)
            state_label.setObjectName("muted")
            state_label.setWordWrap(True)
            card_layout.addWidget(title)
            card_layout.addWidget(state_label)
            flow_layout.addWidget(card, index // 3, index % 3)
            self.calendar_flow_widgets.append((card, title, state_label))
        calendar_layout.addLayout(flow_layout)
        self.calendar_metric_widgets = []
        metric_layout = QGridLayout()
        for index in range(4):
            card = QFrame()
            card.setObjectName("card")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(10, 6, 10, 6)
            caption, value = QLabel(), QLabel("—")
            caption.setWordWrap(True)
            caption.setObjectName("muted")
            value.setObjectName("metric")
            card_layout.addWidget(caption)
            card_layout.addWidget(value)
            metric_layout.addWidget(card, 0, index)
            self.calendar_metric_widgets.append((caption, value))
        calendar_layout.addLayout(metric_layout)
        self.calendar_bar_widgets = []
        for index in range(4):
            row = QWidget()
            row_layout = QVBoxLayout(row)
            row_layout.setContentsMargins(0, 0, 0, 0)
            caption = QLabel()
            caption.setWordWrap(True)
            bar = QProgressBar()
            bar.setRange(0, 1000)
            row_layout.addWidget(caption)
            row_layout.addWidget(bar)
            calendar_layout.addWidget(row)
            self.calendar_bar_widgets.append((row, caption, bar))
        self.calendar_commit = QLabel()
        self.calendar_commit.setWordWrap(True)
        self.calendar_commit.setObjectName("section")
        calendar_layout.addWidget(self.calendar_commit)
        self.calendar_boundary = QLabel()
        self.calendar_boundary.setWordWrap(True)
        self.calendar_boundary.setObjectName("muted")
        calendar_layout.addWidget(self.calendar_boundary)
        self.calendar_context = QLabel()
        self.calendar_context.setWordWrap(True)
        self.calendar_context.setTextFormat(Qt.TextFormat.PlainText)
        self.calendar_context.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        calendar_layout.addWidget(self.calendar_context)
        self.calendar_error = QLabel()
        self.calendar_error.setWordWrap(True)
        self.calendar_error.setTextFormat(Qt.TextFormat.PlainText)
        self.calendar_error.setObjectName("error")
        calendar_layout.addWidget(self.calendar_error)
        log_controls = QHBoxLayout()
        self.calendar_log_location = QLabel("实时日志 · 尚未运行")
        self.calendar_log_location.setObjectName("muted")
        log_controls.addWidget(self.calendar_log_location, 1)
        self.calendar_follow_log = QCheckBox("跟随最新输出")
        self.calendar_follow_log.setChecked(True)
        log_controls.addWidget(self.calendar_follow_log)
        calendar_layout.addLayout(log_controls)
        self.calendar_logs = QPlainTextEdit()
        self.calendar_logs.setReadOnly(True)
        self.calendar_logs.setMaximumBlockCount(4000)
        self.calendar_logs.setMinimumHeight(190)
        calendar_layout.addWidget(self.calendar_logs)
        self.trade_result_page = QWidget()
        self.trade_result_scroll = QScrollArea()
        self.trade_result_scroll.setWidgetResizable(True)
        self.trade_result_scroll.setWidget(self.trade_result_page)
        self.calendar_page.addWidget(self.trade_result_scroll)
        trade_layout = QVBoxLayout(self.trade_result_page)
        trade_layout.setContentsMargins(0, 10, 0, 0)
        trade_layout.setSpacing(8)
        trade_heading = QHBoxLayout()
        self.trade_outcome = QLabel("交易日历 · 已落盘数据")
        self.trade_outcome.setObjectName("heading")
        self.trade_outcome.setWordWrap(True)
        trade_heading.addWidget(self.trade_outcome, 1)
        self.trade_interrupt = QPushButton("中断本批次")
        self.trade_interrupt.clicked.connect(self.interrupt)
        trade_heading.addWidget(self.trade_interrupt)
        trade_layout.addLayout(trade_heading)
        self.trade_run_summary = QLabel()
        self.trade_run_summary.setWordWrap(True)
        trade_layout.addWidget(self.trade_run_summary)
        self.trade_execution = QLabel()
        self.trade_execution.setObjectName("muted")
        self.trade_execution.setWordWrap(True)
        trade_layout.addWidget(self.trade_execution)
        self.trade_progress = QProgressBar()
        self.trade_progress.setRange(0, 1000)
        trade_layout.addWidget(self.trade_progress)
        self.trade_result_tabs = QTabWidget()
        self.trade_result_tabs.setDocumentMode(True)
        table_panel = QWidget()
        table_layout = QVBoxLayout(table_panel)
        table_layout.setContentsMargins(0, 8, 0, 0)
        source_row = QHBoxLayout()
        self.trade_source = QComboBox()
        self.trade_source.addItem("已落盘日历", "persisted")
        self.trade_source.addItem("本批生成结果", "generated")
        self.trade_source.activated.connect(self.change_trade_source)
        source_row.addWidget(self.trade_source)
        self.trade_table_summary = QLabel("正在读取日历…")
        self.trade_table_summary.setWordWrap(True)
        source_row.addWidget(self.trade_table_summary, 1)
        self.trade_reload = QPushButton("刷新数据")
        self.trade_reload.clicked.connect(self.reload_trade_calendar)
        source_row.addWidget(self.trade_reload)
        table_layout.addLayout(source_row)
        filters = QHBoxLayout()
        self.trade_year = QComboBox()
        self.trade_year.addItem("全部年份", None)
        self.trade_month = QComboBox()
        self.trade_month.addItem("全部月份", None)
        for month in range(1, 13):
            self.trade_month.addItem(f"{month:02d} 月", month)
        self.trade_day_filter = QComboBox()
        for label, value in (("全部日期", None), ("交易日", True), ("非交易日", False)):
            self.trade_day_filter.addItem(label, value)
        for combo in (self.trade_year, self.trade_month, self.trade_day_filter):
            combo.currentIndexChanged.connect(self.filter_trade_calendar)
            filters.addWidget(combo)
        filters.addStretch()
        self.trade_all_fields = QCheckBox(f"全部 {len(TRADE_CALENDAR_SCHEMA)} 列")
        self.trade_all_fields.toggled.connect(self.filter_trade_calendar)
        filters.addWidget(self.trade_all_fields)
        table_layout.addLayout(filters)
        self.trade_table = QTreeWidget()
        self.trade_table.setRootIsDecorated(False)
        self.trade_table.setAlternatingRowColors(True)
        self.trade_table.setUniformRowHeights(True)
        self.trade_table.setMinimumHeight(180)
        self.trade_table.setColumnCount(len(TRADE_CALENDAR_SCHEMA))
        captions = {"calendar_date": "日期", "date_key": "日期键", "is_trading_day": "交易日", "weekday": "星期",
                    "is_weekend": "周末", "source": "来源", "calendar_name": "日历名称", "calendar_timezone": "时区",
                    "effective_after": "生效时点", "updated_at": "更新时间（北京时间）", "year": "年份"}
        self.trade_table.setHeaderLabels([captions.get(field.name, field.name) for field in TRADE_CALENDAR_SCHEMA])
        self.trade_table.header().setStretchLastSection(False)
        self.trade_table.header().setMinimumSectionSize(80)
        for index, field in enumerate(TRADE_CALENDAR_SCHEMA):
            self.trade_table.headerItem().setToolTip(index, field.name + "\n" + field.metadata[b"description_zh"].decode("utf-8"))
            self.trade_table.header().setSectionResizeMode(index, QHeaderView.ResizeMode.ResizeToContents)
        table_layout.addWidget(self.trade_table, 1)
        self.trade_table_note = QLabel()
        self.trade_table_note.setWordWrap(True)
        self.trade_table_note.setObjectName("muted")
        table_layout.addWidget(self.trade_table_note)
        pager = QHBoxLayout()
        self.trade_page_label = QLabel()
        pager.addWidget(self.trade_page_label, 1)
        self.trade_previous = QPushButton("上一页")
        self.trade_next = QPushButton("下一页")
        self.trade_previous.clicked.connect(lambda: self.page_trade_calendar(-1))
        self.trade_next.clicked.connect(lambda: self.page_trade_calendar(1))
        pager.addWidget(self.trade_previous)
        pager.addWidget(self.trade_next)
        table_layout.addLayout(pager)
        self.trade_result_tabs.addTab(table_panel, "日历表")
        log_panel = QWidget()
        log_layout = QVBoxLayout(log_panel)
        self.trade_log_location = QLabel()
        self.trade_log_location.setWordWrap(True)
        log_toolbar = QHBoxLayout()
        log_toolbar.addWidget(self.trade_log_location, 1)
        self.trade_follow_log = QCheckBox("跟随最新输出")
        self.trade_follow_log.setChecked(True)
        self.trade_follow_log.toggled.connect(self.calendar_follow_log.setChecked)
        self.calendar_follow_log.toggled.connect(self.trade_follow_log.setChecked)
        log_toolbar.addWidget(self.trade_follow_log)
        log_layout.addLayout(log_toolbar)
        self.trade_logs = QPlainTextEdit()
        self.trade_logs.setReadOnly(True)
        self.trade_logs.setDocument(self.calendar_logs.document())
        log_layout.addWidget(self.trade_logs, 1)
        self.trade_result_tabs.addTab(log_panel, "执行日志")
        trade_layout.addWidget(self.trade_result_tabs, 1)
        self.trade_error = QLabel()
        self.trade_error.setObjectName("error")
        self.trade_error.setWordWrap(True)
        trade_layout.addWidget(self.trade_error)
        self.detail_tabs.addTab(self.calendar_page, "运行看板")
        self.detail_tabs.setTabVisible(2, False)
        detail_layout.addWidget(self.detail_tabs, 1)
        entry_actions = QHBoxLayout()
        self.entry_run_hint = QLabel("仅运行当前显示的环节，不追加上游。")
        self.entry_run_hint.setObjectName("muted")
        self.entry_run_hint.setWordWrap(True)
        entry_actions.addWidget(self.entry_run_hint, 1)
        self.run_entry_button = QPushButton("运行此环节  →")
        self.run_entry_button.setObjectName("accent")
        self.run_entry_button.setEnabled(False)
        self.run_entry_button.clicked.connect(self.run_current_entry)
        entry_actions.addWidget(self.run_entry_button)
        detail_layout.addLayout(entry_actions)
        self.workbench_stack = QStackedWidget()
        self.business_detail = detail_card
        self.workbench_stack.addWidget(detail_card)
        self.b00_page = QScrollArea()
        self.b00_page.setWidgetResizable(True)
        self.b00_body = QWidget()
        self.b00_page.setWidget(self.b00_body)
        self.workbench_stack.addWidget(self.b00_page)
        split.addWidget(self.workbench_stack)
        split.setStretchFactor(0, 0)
        split.setStretchFactor(1, 1)
        split.setSizes([286, 930])

        batch_bar = QFrame()
        batch_bar.setObjectName("card")
        batch_layout = QHBoxLayout(batch_bar)
        batch_layout.setContentsMargins(18, 12, 14, 12)
        batch_summary = QVBoxLayout()
        batch_summary.setSpacing(4)
        self.selection_label = QLabel("正在读取正式入口…")
        self.selection_label.setObjectName("section")
        batch_summary.addWidget(self.selection_label)
        footer = QLabel("按业务顺序执行，不自动补选上游；只读运行仍可能请求 API。")
        footer.setObjectName("muted")
        footer.setWordWrap(True)
        batch_summary.addWidget(footer)
        batch_layout.addLayout(batch_summary, 1)
        self.preview_button = QPushButton("预览本批命令  →")
        self.preview_button.setObjectName("accent")
        self.preview_button.setEnabled(False)
        self.preview_button.setMinimumWidth(154)
        self.preview_button.clicked.connect(self.preview)
        batch_layout.addWidget(self.preview_button)
        configure_layout.addWidget(batch_bar)

        monitor_layout = QVBoxLayout(self.monitor_tab)
        monitor_layout.setContentsMargins(24, 20, 24, 18)
        monitor_layout.setSpacing(14)
        monitor_tools = QHBoxLayout()
        monitor_heading = QLabel("运行监控")
        monitor_heading.setObjectName("heading")
        monitor_tools.addWidget(monitor_heading, 1)
        self.current_button = QPushButton("返回本窗口批次")
        self.current_button.setEnabled(False)
        self.current_button.setToolTip("本窗口尚未启动批次；已有记录请从历史批次查看。")
        self.current_button.clicked.connect(self.view_current)
        monitor_tools.addWidget(self.current_button)
        self.interrupt_button = QPushButton("请求中断本窗口批次")
        self.interrupt_button.setObjectName("dangerButton")
        self.interrupt_button.setEnabled(False)
        self.interrupt_button.clicked.connect(self.interrupt)
        monitor_tools.addWidget(self.interrupt_button)
        monitor_layout.addLayout(monitor_tools)
        self.view_context_label = QLabel("本窗口尚未启动批次")
        self.view_context_label.setObjectName("muted")
        self.view_context_label.setWordWrap(True)
        monitor_layout.addWidget(self.view_context_label)
        self.run_path_label = QLabel("在采集工作台中选择并启动环节。")
        self.run_path_label.setObjectName("code")
        self.run_path_label.setWordWrap(True)
        self.run_path_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        monitor_layout.addWidget(self.run_path_label)
        metrics = QHBoxLayout()
        metrics.setSpacing(14)
        self.run_title = QLabel("尚未启动")
        self.elapsed_label = QLabel("—")
        self.heartbeat_label = QLabel("—")
        for caption, value in (("批次状态", self.run_title), ("累计耗时", self.elapsed_label), ("后台心跳", self.heartbeat_label)):
            card = QFrame()
            card.setObjectName("card")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(18, 14, 18, 16)
            title = QLabel(caption)
            title.setObjectName("muted")
            card_layout.addWidget(title)
            value.setObjectName("metric")
            card_layout.addWidget(value)
            metrics.addWidget(card, 1)
        monitor_layout.addLayout(metrics)
        self.health_label = QLabel("启动后显示后台状态与业务输出时间。")
        self.health_label.setObjectName("muted")
        self.progress_label = QLabel("当前阶段：尚未开始")
        self.error_label = QLabel()
        self.error_label.setObjectName("error")
        self.error_label.hide()
        for label in (self.health_label, self.progress_label, self.error_label):
            label.setWordWrap(True)
            monitor_layout.addWidget(label)
        monitor_split = QSplitter(Qt.Orientation.Horizontal)
        monitor_split.setChildrenCollapsible(False)
        monitor_split.setHandleWidth(14)
        stages_panel = QFrame()
        stages_panel.setObjectName("card")
        stages_panel.setMinimumWidth(260)
        stages_layout = QVBoxLayout(stages_panel)
        stages_layout.setContentsMargins(14, 12, 14, 12)
        stages_caption = QLabel("阶段记录")
        stages_caption.setObjectName("section")
        stages_layout.addWidget(stages_caption)
        self.follow_stage = QCheckBox("跟随当前环节")
        self.follow_stage.setChecked(True)
        self.follow_stage.toggled.connect(lambda: self.render_status(self.view_status) if self.view_run_root and self.view_status else None)
        stages_layout.addWidget(self.follow_stage)
        self.stage_tree = QTreeWidget()
        self.stage_tree.setHeaderLabels(["阶段", "状态", "耗时"])
        self.stage_tree.setRootIsDecorated(False)
        self.stage_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.stage_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.stage_tree.header().setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        self.stage_tree.currentItemChanged.connect(self.select_monitor_stage)
        stages_layout.addWidget(self.stage_tree, 1)
        monitor_split.addWidget(stages_panel)
        log_panel = QFrame()
        log_panel.setObjectName("card")
        log_layout = QVBoxLayout(log_panel)
        log_layout.setContentsMargins(14, 12, 14, 12)
        log_tools = QHBoxLayout()
        log_caption = QLabel("步骤与实时日志")
        log_caption.setObjectName("section")
        log_tools.addWidget(log_caption, 1)
        folder_button = QPushButton("批次目录")
        folder_button.setObjectName("quiet")
        folder_button.clicked.connect(self.open_run_folder)
        log_tools.addWidget(folder_button)
        stage_log_button = QPushButton("阶段日志")
        stage_log_button.setObjectName("quiet")
        stage_log_button.clicked.connect(self.show_stage_log)
        log_tools.addWidget(stage_log_button)
        log_layout.addLayout(log_tools)
        self.activity_label = QLabel("选择环节后显示其步骤与日志。")
        self.activity_label.setWordWrap(True)
        log_layout.addWidget(self.activity_label)
        self.activity_progress = QProgressBar()
        self.activity_progress.setRange(0, 1)
        self.activity_progress.setValue(0)
        self.activity_progress.setFormat("尚未开始")
        log_layout.addWidget(self.activity_progress)
        self.progress_tree = QTreeWidget()
        self.progress_tree.setHeaderLabels(["步骤 / 对象", "状态", "计数 / 剩余"])
        self.progress_tree.setRootIsDecorated(False)
        self.progress_tree.header().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.progress_tree.header().setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.progress_tree.setMinimumHeight(110)
        self.progress_tree.setMaximumHeight(210)
        log_layout.addWidget(self.progress_tree)
        self.log_location = QLabel()
        self.log_location.setObjectName("muted")
        self.log_location.setWordWrap(True)
        log_layout.addWidget(self.log_location)
        log_options = QHBoxLayout()
        self.follow_log = QCheckBox("跟随末尾")
        self.follow_log.setChecked(True)
        log_options.addWidget(self.follow_log)
        self.log_search = QLineEdit()
        self.log_search.setPlaceholderText("在已加载日志中查找，回车定位")
        self.log_search.returnPressed.connect(self.find_log_text)
        log_options.addWidget(self.log_search, 1)
        log_layout.addLayout(log_options)
        self.logs = QPlainTextEdit()
        self.logs.setReadOnly(True)
        self.logs.setPlaceholderText("业务输出将在启动后显示。")
        self.logs.setMaximumBlockCount(4000)
        log_layout.addWidget(self.logs, 1)
        monitor_split.addWidget(log_panel)
        monitor_split.setSizes([300, 880])
        monitor_layout.addWidget(monitor_split, 1)

        history_layout = QVBoxLayout(self.history_tab)
        history_layout.setContentsMargins(24, 20, 24, 18)
        history_layout.setSpacing(14)
        history_tools = QHBoxLayout()
        history_heading = QLabel("历史批次")
        history_heading.setObjectName("heading")
        history_tools.addWidget(history_heading, 1)
        self.history_button = QPushButton("刷新历史")
        self.history_button.clicked.connect(self.refresh_history)
        history_tools.addWidget(self.history_button)
        open_history_button = QPushButton("查看选中批次  →")
        open_history_button.setObjectName("accent")
        open_history_button.clicked.connect(self.open_history)
        history_tools.addWidget(open_history_button)
        history_layout.addLayout(history_tools)
        history_hint = QLabel("查看状态、日志与锁现场。历史记录只读，每次启动生成独立批次。")
        history_hint.setObjectName("muted")
        history_hint.setWordWrap(True)
        history_layout.addWidget(history_hint)
        self.history_tree = QTreeWidget()
        self.history_tree.setHeaderLabels(["启动时间（北京时间）", "状态", "最后阶段", "批次目录"])
        self.history_tree.setRootIsDecorated(False)
        self.history_tree.itemDoubleClicked.connect(self.open_history)
        history_layout.addWidget(self.history_tree, 1)
        self.history_status_label = QLabel("尚未刷新")
        self.history_status_label.setObjectName("muted")
        self.history_status_label.setWordWrap(True)
        history_layout.addWidget(self.history_status_label)
        self.lock_label = QLabel("打开本页时读取历史和锁现场。")
        self.lock_label.setObjectName("muted")
        self.lock_label.setWordWrap(True)
        history_layout.addWidget(self.lock_label)
        self.tabs.currentChanged.connect(lambda index: self.refresh_history() if index == 2 else None)

        b00_layout = QVBoxLayout(self.b00_body)
        b00_layout.setContentsMargins(0, 0, 0, 0)
        b00_layout.setSpacing(10)
        b00_header = QFrame()
        b00_header.setObjectName("card")
        b00_header_layout = QVBoxLayout(b00_header)
        b00_header_layout.setContentsMargins(18, 14, 18, 14)
        title_row = QHBoxLayout()
        self.b00_code = QLabel()
        self.b00_code.setObjectName("badge")
        title_row.addWidget(self.b00_code)
        self.b00_title = QLabel()
        self.b00_title.setObjectName("heading")
        self.b00_title.setWordWrap(True)
        title_row.addWidget(self.b00_title, 1)
        self.b00_state = QLabel("未运行")
        self.b00_state.setObjectName("badge")
        title_row.addWidget(self.b00_state)
        b00_header_layout.addLayout(title_row)
        self.b00_mode_tabs = QTabBar()
        self.b00_mode_tabs.setExpanding(False)
        self.b00_mode_tabs.setAccessibleName("页内检查模式")
        self.b00_mode_tabs.currentChanged.connect(
            lambda index: self.show_b00_page(self.b00_page_id, self.b00_mode_tabs.tabData(index)) if index >= 0 else None)
        b00_header_layout.addWidget(self.b00_mode_tabs)
        self.b00_script_origin = QLabel()
        self.b00_script_origin.setObjectName("muted")
        self.b00_script_origin.setWordWrap(True)
        self.b00_script_origin.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        b00_header_layout.addWidget(self.b00_script_origin)
        self.maintenance_description = QLabel()
        self.maintenance_description.setWordWrap(True)
        b00_header_layout.addWidget(self.maintenance_description)
        self.maintenance_command = QLabel()
        self.maintenance_command.setObjectName("code")
        self.maintenance_command.setWordWrap(True)
        self.maintenance_command.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        b00_header_layout.addWidget(self.maintenance_command)
        tools_actions = QHBoxLayout()
        self.b00_source_button = QPushButton("打开源码")
        self.b00_source_button.clicked.connect(self.open_b00_source)
        tools_actions.addWidget(self.b00_source_button)
        self.reload_button = QPushButton("重读采集参数")
        self.reload_button.clicked.connect(self.reload_catalog)
        tools_actions.addWidget(self.reload_button)
        tools_actions.addStretch()
        self.b00_interrupt_button = QPushButton("中断本批次")
        self.b00_interrupt_button.setObjectName("dangerButton")
        self.b00_interrupt_button.clicked.connect(self.interrupt)
        tools_actions.addWidget(self.b00_interrupt_button)
        self.run_tool_button = QPushButton("执行检查  →")
        self.run_tool_button.setObjectName("accent")
        self.run_tool_button.clicked.connect(self.run_maintenance)
        tools_actions.addWidget(self.run_tool_button)
        b00_header_layout.addLayout(tools_actions)
        b00_layout.addWidget(b00_header)

        self.b00_runtime_panel = QWidget()
        runtime_layout = QVBoxLayout(self.b00_runtime_panel)
        runtime_layout.setContentsMargins(0, 0, 0, 0)
        runtime_layout.setSpacing(8)
        self.b00_position = QLabel("尚未运行 · 执行后显示真实计数")
        self.b00_position.setObjectName("section")
        self.b00_position.setWordWrap(True)
        runtime_layout.addWidget(self.b00_position)
        metrics_row = QHBoxLayout()
        self.b00_metrics = {}
        for key, title in (("total", "本步总量"), ("completed", "已处理"), ("remaining", "剩余"), ("failed", "异常项目")):
            card = QFrame()
            card.setObjectName("card")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(14, 8, 14, 8)
            label = QLabel(title)
            label.setObjectName("muted")
            card_layout.addWidget(label)
            value_label = QLabel("—")
            value_label.setObjectName("metric")
            card_layout.addWidget(value_label)
            self.b00_metrics[key] = value_label
            metrics_row.addWidget(card, 1)
        runtime_layout.addLayout(metrics_row)
        self.b00_phases_layout = QHBoxLayout()
        self.b00_phase_widgets = {}
        runtime_layout.addLayout(self.b00_phases_layout)
        self.b00_health = QLabel("耗时 —　·　心跳 —")
        self.b00_health.setObjectName("muted")
        self.b00_health.setWordWrap(True)
        runtime_layout.addWidget(self.b00_health)
        self.b00_error = QLabel()
        self.b00_error.setObjectName("error")
        self.b00_error.setWordWrap(True)
        self.b00_error.setTextFormat(Qt.TextFormat.PlainText)
        self.b00_error.hide()
        runtime_layout.addWidget(self.b00_error)

        detail_split = QSplitter(Qt.Orientation.Vertical)
        detail_split.setChildrenCollapsible(False)
        self.b00_results = QTreeWidget()
        self.b00_results.setRootIsDecorated(False)
        self.b00_results.setAlternatingRowColors(True)
        self.b00_results.setMinimumHeight(90)
        detail_split.addWidget(self.b00_results)
        log_panel = QWidget()
        log_layout = QVBoxLayout(log_panel)
        log_layout.setContentsMargins(0, 0, 0, 0)
        log_heading = QHBoxLayout()
        self.b00_log_location = QLabel("实时日志 · 尚未运行")
        self.b00_log_location.setObjectName("muted")
        log_heading.addWidget(self.b00_log_location, 1)
        self.b00_follow_log = QCheckBox("跟随末尾")
        self.b00_follow_log.setChecked(True)
        log_heading.addWidget(self.b00_follow_log)
        log_layout.addLayout(log_heading)
        self.b00_logs = QPlainTextEdit()
        self.b00_logs.setReadOnly(True)
        self.b00_logs.setMaximumBlockCount(4000)
        self.b00_logs.setMinimumHeight(90)
        self.b00_logs.setPlaceholderText("点击执行后，原始 stdout / stderr 将在这里持续显示。")
        log_layout.addWidget(self.b00_logs)
        detail_split.addWidget(log_panel)
        detail_split.setSizes([180, 160])
        runtime_layout.addWidget(detail_split, 1)
        b00_layout.addWidget(self.b00_runtime_panel, 1)
        self.b00_reference = QWidget()
        self.b00_reference_layout = QVBoxLayout(self.b00_reference)
        self.b00_reference_layout.setContentsMargins(0, 0, 0, 0)
        self.b00_reference_layout.setSpacing(10)
        b00_layout.addWidget(self.b00_reference, 1)

        b00_group = QTreeWidgetItem(self.entry_tree, ["b00  准备与检查"])
        b00_group.setFlags(b00_group.flags() & ~Qt.ItemFlag.ItemIsSelectable)
        for page_id, (title, _) in B00_PAGES.items():
            item = QTreeWidgetItem(b00_group, [f"{page_id}  {title}"])
            item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserCheckable)
            item.setData(0, Qt.ItemDataRole.UserRole, "b00/" + page_id)
            self.b00_items[page_id] = item
        b00_group.setExpanded(True)
        self.entry_tree.setCurrentItem(self.b00_items["b00_01"])

        theme_name = self.preferences.value("appearance/theme", "light")
        self.apply_theme(theme_name if isinstance(theme_name, str) and theme_name in THEMES else "light")

        self.loading_timer = QTimer(self)
        self.loading_timer.setInterval(100)
        self.loading_timer.timeout.connect(self.finish_loading)
        self.loading_timer.start()
        self.monitor_timer = QTimer(self)
        self.monitor_timer.setInterval(1000)
        self.monitor_timer.timeout.connect(self.refresh_monitor)
        self.monitor_timer.start()

    def apply_theme(self, name: str, *, persist: bool = False):
        """Apply one complete appearance to the window and all owned dialogs."""
        colors = THEMES[name]
        self.theme_name = name
        application = QApplication.instance()
        if application.style().objectName().lower() != "fusion":
            application.setStyle("Fusion")
        application.styleHints().setColorScheme(Qt.ColorScheme.Dark if name == "dark" else Qt.ColorScheme.Light)
        palette = QPalette(application.style().standardPalette())
        roles = {
            QPalette.ColorRole.Window: "canvas", QPalette.ColorRole.WindowText: "text",
            QPalette.ColorRole.Base: "field", QPalette.ColorRole.AlternateBase: "alternate",
            QPalette.ColorRole.Text: "text", QPalette.ColorRole.Button: "surface",
            QPalette.ColorRole.ButtonText: "text", QPalette.ColorRole.BrightText: "text",
            QPalette.ColorRole.ToolTipBase: "surface", QPalette.ColorRole.ToolTipText: "text",
            QPalette.ColorRole.Highlight: "selection", QPalette.ColorRole.HighlightedText: "selection_text",
            QPalette.ColorRole.Link: "accent", QPalette.ColorRole.LinkVisited: "info",
            QPalette.ColorRole.PlaceholderText: "muted", QPalette.ColorRole.Accent: "accent",
            QPalette.ColorRole.Light: "hover", QPalette.ColorRole.Midlight: "alternate",
            QPalette.ColorRole.Mid: "border", QPalette.ColorRole.Dark: "border", QPalette.ColorRole.Shadow: "canvas",
        }
        for group in (QPalette.ColorGroup.Active, QPalette.ColorGroup.Inactive, QPalette.ColorGroup.Disabled):
            for role, token in roles.items():
                palette.setColor(group, role, QColor(colors[token]))
        for role in (QPalette.ColorRole.WindowText, QPalette.ColorRole.Text, QPalette.ColorRole.ButtonText,
                     QPalette.ColorRole.PlaceholderText):
            palette.setColor(QPalette.ColorGroup.Disabled, role, QColor(colors["disabled"]))
        palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Button, QColor(colors["disabled_bg"]))
        application.setPalette(palette)
        application.setStyleSheet(f"""
            QWidget {{ font-family: "Microsoft YaHei UI"; font-size: 13px; color: {colors['text']}; selection-background-color: {colors['selection']}; selection-color: {colors['selection_text']}; }}
            QMainWindow, QDialog, QMessageBox {{ background-color: {colors['canvas']}; }}
            QProgressBar {{ border: 1px solid {colors['border']}; border-radius: 5px; background: {colors['field']}; text-align: center; min-height: 24px; }}
            QProgressBar::chunk {{ background: {colors['selection']}; border-radius: 4px; }}
            QComboBox {{ background: {colors['field']}; border: 1px solid {colors['border']}; border-radius: 6px; padding: 10px; }}
            QComboBox QAbstractItemView {{ background: {colors['surface']}; selection-background-color: {colors['selection']}; }}
            QFrame#appHeader {{ background-color: {colors['surface']}; border-bottom: 1px solid {colors['border']}; }}
            QFrame#card {{ background-color: {colors['surface']}; border: 1px solid {colors['border']}; border-radius: 10px; }}
            QFrame#card[current="true"] {{ border-color: {colors['accent']}; }}
            QFrame#navigation {{ background-color: {colors['canvas']}; border-radius: 8px; }}
            QWidget#parametersBody {{ background-color: {colors['surface']}; }}
            QLabel {{ background: transparent; border: none; }}
            QLabel#brand {{ font-size: 16px; font-weight: 700; letter-spacing: 2px; }}
            QLabel#heading {{ font-size: 23px; font-weight: 600; }}
            QLabel#metric {{ font-size: 24px; font-weight: 600; }}
            QLabel#metric[state="failed"], QLabel#metric[state="quota_stopped"] {{ color: {colors['danger']}; }}
            QLabel#metric[state="succeeded"] {{ color: {colors['success']}; }}
            QLabel#metric[state="running"] {{ color: {colors['info']}; }}
            QLabel#section {{ font-weight: 600; }}
            QLabel#muted {{ color: {colors['muted']}; font-size: 12px; }}
            QLabel#code {{ color: {colors['muted']}; font-family: "Consolas", "Microsoft YaHei UI"; font-size: 11px; }}
            QLabel#badge {{ background-color: {colors['selection']}; color: {colors['selection_text']}; border-radius: 4px; padding: 3px 8px; font-size: 11px; font-weight: 600; }}
            QLabel#badge[state="failed"], QLabel#badge[state="quota_stopped"] {{ color: {colors['danger']}; }}
            QLabel#error {{ color: {colors['danger']}; padding: 8px; background-color: {colors['surface']}; border-left: 3px solid {colors['danger']}; }}
            QPushButton {{ background-color: {colors['surface']}; color: {colors['text']}; border: 1px solid {colors['border']}; border-radius: 6px; padding: 8px 14px; }}
            QPushButton:hover {{ background-color: {colors['hover']}; border-color: {colors['accent']}; }}
            QPushButton:focus {{ border-color: {colors['accent']}; }}
            QPushButton:pressed {{ background-color: {colors['selection']}; color: {colors['selection_text']}; }}
            QPushButton#navigationButton {{ background: transparent; border-color: transparent; color: {colors['muted']}; padding: 7px 17px; }}
            QPushButton#navigationButton:checked {{ background-color: {colors['selection']}; color: {colors['selection_text']}; border-color: transparent; font-weight: 600; }}
            QPushButton#navigationButton:hover, QPushButton#quiet:hover, QPushButton#themeToggle:hover {{ background-color: {colors['hover']}; }}
            QPushButton#quiet, QPushButton#themeToggle {{ background: transparent; border-color: transparent; }}
            QPushButton#quiet {{ color: {colors['muted']}; padding: 6px 8px; }}
            QPushButton#themeToggle {{ padding: 0; border-radius: 18px; }}
            QPushButton#themeToggle:focus, QPushButton#quiet:focus, QPushButton#navigationButton:focus {{ border: 1px dashed {colors['accent']}; }}
            QPushButton#accent {{ background-color: {colors['accent']}; color: {colors['accent_text']}; border-color: {colors['accent']}; font-weight: 600; padding: 10px 18px; }}
            QPushButton#accent:hover {{ background-color: {colors['accent_hover']}; }}
            QPushButton#dangerButton {{ color: {colors['danger']}; }}
            QPushButton:disabled, QPushButton#accent:disabled, QPushButton#dangerButton:disabled {{ background-color: {colors['disabled_bg']}; color: {colors['disabled']}; border-color: {colors['border']}; }}
            QTabWidget::pane {{ background-color: {colors['surface']}; border: 0; border-top: 1px solid {colors['border']}; }}
            QTabBar::tab {{ background: transparent; color: {colors['muted']}; padding: 12px 12px; margin-right: 18px; border-bottom: 2px solid transparent; }}
            QTabBar::tab:selected {{ color: {colors['text']}; border-bottom-color: {colors['accent']}; font-weight: 600; }}
            QTabBar::tab:hover {{ color: {colors['accent']}; }}
            QTreeWidget {{ background-color: {colors['field']}; color: {colors['text']}; border: 1px solid {colors['border']}; border-radius: 8px; outline: 0; }}
            QTreeWidget#entryTree {{ background-color: {colors['surface']}; border: 0; }}
            QTreeWidget::item {{ padding: 8px 4px; border: none; }}
            QTreeWidget#entryTree::item {{ padding: 6px 2px; border-radius: 4px; }}
            QTreeWidget::item:selected {{ background-color: {colors['selection']}; color: {colors['selection_text']}; }}
            QTreeWidget::item:hover:!selected {{ background-color: {colors['hover']}; }}
            QHeaderView::section {{ background-color: {colors['alternate']}; color: {colors['muted']}; border: 0; border-bottom: 1px solid {colors['border']}; padding: 10px 8px; font-size: 12px; font-weight: 600; }}
            QScrollArea {{ background-color: {colors['surface']}; border: 0; }}
            QLineEdit {{ background-color: {colors['field']}; color: {colors['text']}; placeholder-text-color: {colors['muted']}; border: 1px solid {colors['border']}; border-radius: 6px; padding: 8px 10px; }}
            QLineEdit:focus {{ border-color: {colors['accent']}; }}
            QCheckBox {{ color: {colors['text']}; background: transparent; spacing: 8px; padding: 6px 0; }}
            QCheckBox::indicator, QTreeWidget::indicator {{ width: 14px; height: 14px; }}
            QCheckBox::indicator:unchecked, QTreeWidget::indicator:unchecked {{ background-color: {colors['field']}; border: 1px solid {colors['muted']}; border-radius: 3px; }}
            QCheckBox:focus {{ color: {colors['accent']}; }}
            QTextBrowser, QPlainTextEdit {{ background-color: {colors['field']}; color: {colors['text']}; border: 1px solid {colors['border']}; border-radius: 6px; padding: 10px; }}
            QTextBrowser#notes {{ border: 0; padding: 14px 0; }}
            QPlainTextEdit {{ font-family: "Consolas", "Microsoft YaHei UI"; font-size: 12px; }}
            QSplitter::handle {{ background-color: {colors['canvas']}; }}
            QSplitter::handle:hover {{ background-color: {colors['border']}; }}
            QScrollBar:vertical {{ background: transparent; width: 10px; margin: 2px; }}
            QScrollBar::handle:vertical {{ background-color: {colors['border']}; border-radius: 3px; min-height: 28px; }}
            QScrollBar::handle:vertical:hover {{ background-color: {colors['muted']}; }}
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
            QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical {{ background: transparent; }}
            QScrollBar:horizontal {{ background: transparent; height: 10px; margin: 2px; }}
            QScrollBar::handle:horizontal {{ background-color: {colors['border']}; border-radius: 3px; min-width: 28px; }}
            QScrollBar::handle:horizontal:hover {{ background-color: {colors['muted']}; }}
            QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0; }}
            QScrollBar::add-page:horizontal, QScrollBar::sub-page:horizontal {{ background: transparent; }}
            QToolTip {{ background-color: {colors['surface']}; color: {colors['text']}; border: 1px solid {colors['border']}; padding: 6px; }}
        """)
        self.theme_button.setIcon(console_icon("sun" if name == "dark" else "moon", colors["text"]))
        theme_action = "切换到亮色主题" if name == "dark" else "切换到暗色主题"
        self.theme_button.setToolTip(theme_action)
        self.theme_button.setAccessibleName(theme_action)
        self.notes.document().setDefaultStyleSheet(f"""
            body {{ color: {colors['text']}; }}
            h1 {{ font-size: 18px; }} h2 {{ font-size: 16px; }}
            table {{ border-collapse: collapse; }}
            th, td {{ border: 1px solid {colors['border']}; padding: 5px; }}
            th {{ background-color: {colors['alternate']}; }}
            a {{ color: {colors['accent']}; }}
        """)
        for item in self.stage_items.values():
            state = item.data(1, Qt.ItemDataRole.UserRole)
            token = {"failed": "danger", "succeeded": "success", "running": "info"}.get(state, "text")
            item.setForeground(1, QColor(colors[token]))
        for index in range(self.history_tree.topLevelItemCount()):
            item = self.history_tree.topLevelItem(index)
            item.setForeground(1, QColor(colors[item.data(1, Qt.ItemDataRole.UserRole)]))
        if persist:
            self.preferences.setValue("appearance/theme", name)
            self.preferences.sync()
            if self.preferences.status() != QSettings.Status.NoError:
                self.statusBar().showMessage("主题已切换，但本机偏好保存失败。")
        self.refresh_b00_dashboard()
        self.refresh_calendar_dashboard()
        if self.trade_table_snapshot and "error" not in self.trade_table_snapshot:
            self.page_trade_calendar(0)

    def finish_loading(self):
        # Only this UI-thread callback installs results from background reads.
        if self.contracts_future is not None and self.contracts_future.done():
            future, self.contracts_future = self.contracts_future, None
            try:
                selected_item = self.entry_tree.currentItem()
                selected_name = selected_item.data(0, Qt.ItemDataRole.UserRole) if selected_item else None
                checked_names = {name for name, item in self.entry_items.items() if item.checkState(0) == Qt.CheckState.Checked}
                previous_values = self.parameter_values
                self.contracts = future.result()
                self.by_name = {contract.name: contract for contract in self.contracts}
                self.entry_tree.clear()
                self.entry_items.clear()
                self.b00_items.clear()
                self.parameter_values = {}
                self.parameter_widgets.clear()
                for page in self.parameter_pages.values():
                    self.parameter_stack.removeWidget(page)
                    page.deleteLater()
                self.parameter_pages.clear()
                b00_group = QTreeWidgetItem(self.entry_tree, ["b00  准备与检查"])
                b00_group.setFlags(b00_group.flags() & ~Qt.ItemFlag.ItemIsSelectable)
                group_font = b00_group.font(0)
                group_font.setBold(True)
                b00_group.setFont(0, group_font)
                for page_id, (title, _) in B00_PAGES.items():
                    item = QTreeWidgetItem(b00_group, [f"{page_id}  {title}"])
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsUserCheckable)
                    item.setData(0, Qt.ItemDataRole.UserRole, "b00/" + page_id)
                    item.setToolTip(0, title)
                    self.b00_items[page_id] = item
                groups = {group: QTreeWidgetItem(self.entry_tree, [f"{group}  {label}"])
                          for group, label in GROUP_LABELS.items()}
                for group_item in groups.values():
                    group_font = group_item.font(0)
                    group_font.setBold(True)
                    group_item.setFont(0, group_font)
                    group_item.setFlags(group_item.flags() & ~Qt.ItemFlag.ItemIsSelectable)
                for contract in self.contracts:
                    with contract.command.make_context(contract.name, [], resilient_parsing=True) as context:
                        self.parameter_values[contract.name] = {
                            option.name: context.params.get(option.name) is True if option.is_flag else ""
                            for option in contract.command.params
                        }
                    self.parameter_values[contract.name].update({
                        key: value for key, value in previous_values.get(contract.name, {}).items()
                        if key in self.parameter_values[contract.name]
                    })
                    title = ENTRY_LABELS.get(contract.path.stem, contract.path.stem)
                    item = QTreeWidgetItem(groups[contract.name[:3]], [contract.path.stem.split("_")[0] + "  " + title])
                    item.setData(0, Qt.ItemDataRole.UserRole, contract.name)
                    item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                    item.setCheckState(0, Qt.CheckState.Checked if contract.name in checked_names else Qt.CheckState.Unchecked)
                    item.setToolTip(0, contract.name)
                    self.entry_items[contract.name] = item
                self.entry_tree.expandAll()
                selected_b00 = self.b00_items.get(str(selected_name).removeprefix("b00/"))
                self.entry_tree.setCurrentItem(selected_b00 or self.entry_items.get(selected_name) or self.b00_items["b00_01"])
                self.filter_entries(self.entry_search.text())
                self.daily_button.setEnabled(True)
                self.preview_button.setEnabled(True)
                self.update_selection()
            except Exception as error:
                self.selection_label.setText(f"入口读取失败：{error}")
                self.preview_button.setEnabled(False)
        if self.history_future is not None and self.history_future.done():
            future, self.history_future = self.history_future, None
            try:
                rows, lock_text = future.result()
                selected = self.history_tree.currentItem()
                selected_path = selected.data(0, Qt.ItemDataRole.UserRole) if selected is not None else None
                self.history_tree.clear()
                for path, values in rows:
                    displayed_values = values.copy()
                    displayed_values[0] = values[0] or "—"
                    try:
                        started_at = datetime.fromisoformat(values[0])
                        if started_at.tzinfo is not None:
                            displayed_values[0] = started_at.astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d  %H:%M:%S")
                    except (TypeError, ValueError):
                        pass
                    item = QTreeWidgetItem(self.history_tree, displayed_values)
                    item.setData(0, Qt.ItemDataRole.UserRole, path)
                    item.setToolTip(0, "原始记录时间：" + (values[0] or "未记录"))
                    token = {"失败": "danger", "启动失败": "danger", "错误": "danger", "配额停止": "danger", "完成": "success", "运行中": "info"}.get(values[1], "text")
                    item.setData(1, Qt.ItemDataRole.UserRole, token)
                    item.setForeground(1, QColor(THEMES[self.theme_name][token]))
                    item.setToolTip(3, str(path))
                    if path == selected_path:
                        self.history_tree.setCurrentItem(item)
                if rows and self.history_tree.currentItem() is None:
                    self.history_tree.setCurrentItem(self.history_tree.topLevelItem(0))
                for column in range(3):
                    self.history_tree.resizeColumnToContents(column)
                newest = self.history_tree.topLevelItem(0).text(0) if rows else "暂无"
                refreshed_at = datetime.now(DISPLAY_TIMEZONE).strftime("%m-%d %H:%M:%S")
                self.history_status_label.setText(f"已刷新 {refreshed_at}（北京时间） · {len(rows)} 条记录 · 最新启动：{newest}")
                self.lock_label.setText(lock_text)
            except Exception as error:
                self.lock_label.setText(f"历史读取失败：{error}")
                self.history_status_label.setText("刷新失败，请查看下方原因。")
            self.history_button.setEnabled(True)
        if self.contracts_future is None and self.history_future is None:
            self.loading_timer.stop()

    def show_parameters(self, current=None, previous=None):
        item = current or self.entry_tree.currentItem()
        name = item.data(0, Qt.ItemDataRole.UserRole) if item else None
        if isinstance(name, str) and name.startswith("b00/"):
            self.show_b00_page(name.split("/", 1)[1])
            return
        if name not in self.by_name:
            return
        self.b00_tool_id = None
        result_first = name == "b01/c01_trade_calendar"
        self.workbench_heading.setVisible(not result_first)
        self.workbench_hint.setVisible(not result_first)
        self.entry_path_label.setVisible(not result_first)
        if result_first:
            self.detail_header.insertWidget(1, self.entry_title)
        else:
            self.detail_layout.insertWidget(1, self.entry_title)
        changed_calendar = self.calendar_name != name
        self.calendar_name = name if name in CALENDAR_VIEWS else None
        if self.calendar_name is None and self.detail_tabs.currentWidget() is self.calendar_page:
            self.detail_tabs.setCurrentIndex(0)
        self.detail_tabs.setTabVisible(2, self.calendar_name is not None)
        self.detail_tabs.setTabText(2, "日历结果" if name == "b01/c01_trade_calendar" else "运行看板")
        if self.calendar_name and changed_calendar:
            self.detail_tabs.setCurrentWidget(self.calendar_page)
        self.workbench_stack.setCurrentWidget(self.business_detail)
        contract = self.by_name[name]
        self.entry_code.setText(name[:3] + " / " + contract.path.stem.split("_")[0])
        self.entry_context.setText(GROUP_LABELS[name[:3]] + (" · 需显式确认" if name.endswith("/c08_full_minute_quality") else ""))
        self.entry_title.setText(ENTRY_LABELS.get(contract.path.stem, contract.path.stem))
        self.entry_path_label.setText(contract.path.name)
        self.entry_path_label.setToolTip(str(contract.path))
        introduction = next((paragraph.strip() for paragraph in re.split(r"\n\s*\n", contract.notes)
                             if paragraph.strip() and not paragraph.lstrip().startswith(("#", "|", "```"))), "该入口未提供开篇说明。")
        summary_document = QTextDocument()
        summary_document.setMarkdown(introduction)
        self.entry_summary.setText(summary_document.toPlainText())
        self.notes.setMarkdown(contract.notes)
        # Markdown headings otherwise dwarf the form; keep their inherited text
        # color so an already-open document follows theme changes immediately.
        block = self.notes.document().begin()
        while block.isValid():
            cursor = QTextCursor(block)
            block_format = block.blockFormat()
            block_format.setLineHeight(135, QTextBlockFormat.LineHeightTypes.ProportionalHeight.value)
            if cursor.currentTable() is None:
                block_format.setBottomMargin(8)
            cursor.setBlockFormat(block_format)
            level = block.blockFormat().headingLevel()
            if level:
                cursor.select(QTextCursor.SelectionType.BlockUnderCursor)
                heading_format = QTextCharFormat()
                heading_format.setFontPointSize(13 if level == 1 else 11)
                heading_format.setProperty(QTextFormat.Property.FontSizeAdjustment, 0)
                heading_format.setFontWeight(QFont.Weight.DemiBold)
                cursor.mergeCharFormat(heading_format)
            block = block.next()
        if name not in self.parameter_pages:
            scroll = QScrollArea()
            scroll.setWidgetResizable(True)
            body = QWidget()
            body.setObjectName("parametersBody")
            form = QFormLayout(body)
            form.setContentsMargins(0, 10, 14, 20)
            form.setVerticalSpacing(12)
            form.setHorizontalSpacing(24)
            form.setFormAlignment(Qt.AlignmentFlag.AlignTop)
            form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
            form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
            self.parameter_widgets[name] = {}
            with contract.command.make_context(name, [], resilient_parsing=True) as context:
                for option in contract.command.params:
                    option_name = option.opts[0]
                    label = QWidget()
                    label_layout = QVBoxLayout(label)
                    label_layout.setContentsMargins(0, 7, 0, 0)
                    label_layout.setSpacing(3)
                    title_label = QLabel(OPTION_LABELS.get(option_name, option_name))
                    title_label.setWordWrap(True)
                    label_layout.addWidget(title_label)
                    option_label = QLabel(option_name)
                    option_label.setObjectName("code")
                    label_layout.addWidget(option_label)
                    holder = QWidget()
                    holder.setMaximumWidth(620)
                    field_layout = QVBoxLayout(holder)
                    field_layout.setContentsMargins(0, 0, 0, 4)
                    field_layout.setSpacing(3)
                    values = self.parameter_values[name]
                    if option.is_flag:
                        field = QCheckBox("启用（必选）" if option.required else "启用")
                        field.setChecked(values[option.name])
                        field.toggled.connect(lambda value, key=option.name, values=values: values.__setitem__(key, value))
                        field.toggled.connect(self.update_selection)
                    else:
                        field = QLineEdit(values[option.name])
                        default = context.params.get(option.name)
                        default_text = "由原入口处理" if default is None else "未指定" if default == () else str(default)
                        field.setPlaceholderText(f"默认：{default_text}")
                        field.textChanged.connect(lambda value, key=option.name, values=values: values.__setitem__(key, value))
                        field.textChanged.connect(self.update_selection)
                    self.parameter_widgets[name][option.name] = field
                    field_layout.addWidget(field)
                    hint = option.help or ""
                    if not option.is_flag:
                        hint += f"  类型：{option.type.name}。"
                        if isinstance(option.type, click.DateTime):
                            hint += " 格式：" + " / ".join(option.type.formats).replace("%Y", "YYYY").replace("%m", "MM").replace("%d", "DD")
                        if option.multiple:
                            hint += " 多值用英文逗号分隔。"
                    if option_name == "--write":
                        hint += " 未选中时不提交业务结果；仍可能请求 API。"
                    if hint.strip():
                        help_label = QLabel(hint.strip())
                        help_label.setObjectName("muted")
                        help_label.setWordWrap(True)
                        field_layout.addWidget(help_label)
                    form.addRow(label, holder)
            scroll.setWidget(body)
            self.parameter_pages[name] = scroll
            self.parameter_stack.addWidget(scroll)
        self.parameter_stack.setCurrentWidget(self.parameter_pages[name])
        self.refresh_calendar_dashboard()
        self.update_selection()

    def update_selection(self, *args):
        count = sum(item.checkState(0) == Qt.CheckState.Checked for item in self.entry_items.values())
        self.selection_label.setText(f"已选 {count} 个环节  /  共 {len(self.contracts)} 项")
        active = self.worker_process is not None and self.worker_process.poll() is None
        ready = self.contracts_future is None and bool(self.contracts)
        self.preview_button.setEnabled(count > 0 and not active and ready)
        current = self.entry_tree.currentItem()
        name = current.data(0, Qt.ItemDataRole.UserRole) if current else None
        self.run_entry_button.setEnabled(name in self.by_name and not active and ready)
        if name in self.by_name:
            values = self.parameter_values.get(name, {})
            write_enabled = any(values.get(option.name) for option in self.by_name[name].command.params if "--write" in option.opts)
            self.entry_run_hint.setText("仅运行此环节 · " + ("将写入业务结果；目标由当前参数决定。" if write_enabled else "只读运行，仍可能请求 API。"))
        self.run_tool_button.setEnabled(not active and self.contracts_future is None)
        self.reload_button.setEnabled(not active and self.contracts_future is None)

    def filter_entries(self, query: str):
        """Filter browsing only; hidden selections remain in the reviewed batch."""
        terms = query.casefold().split()
        for name, item in {**self.entry_items, **{"b00/" + key: item for key, item in self.b00_items.items()}}.items():
            search_text = f"{name} {item.text(0)} {item.parent().text(0)}".casefold()
            if name.startswith("b00/"):
                search_text += " " + " ".join(label for _, label in B00_PAGES[name.split("/", 1)[1]][1]).casefold()
            item.setHidden(not all(term in search_text for term in terms))
        visible_count = 0
        for index in range(self.entry_tree.topLevelItemCount()):
            group = self.entry_tree.topLevelItem(index)
            visible = any(not group.child(child).isHidden() for child in range(group.childCount()))
            group.setHidden(not visible)
            if terms and visible:
                group.setExpanded(True)
            visible_count += sum(not group.child(child).isHidden() for child in range(group.childCount()))
        self.search_empty.setVisible(bool(self.entry_items) and visible_count == 0)

    def select_daily(self):
        self.entry_tree.blockSignals(True)
        for name, item in self.entry_items.items():
            if name.endswith("/c08_full_minute_quality"):
                item.setCheckState(0, Qt.CheckState.Unchecked)
                continue
            item.setCheckState(0, Qt.CheckState.Checked)
            for option in self.by_name[name].command.params:
                if option.is_flag and "--write" in option.opts:
                    self.parameter_values[name][option.name] = True
                    field = self.parameter_widgets.get(name, {}).get(option.name)
                    if field is not None:
                        field.setChecked(True)
        self.entry_tree.blockSignals(False)
        self.update_selection()

    def clear_selection(self):
        self.entry_tree.blockSignals(True)
        for item in self.entry_items.values():
            item.setCheckState(0, Qt.CheckState.Unchecked)
        self.entry_tree.blockSignals(False)
        self.update_selection()

    def current_request(self, entry_name=None):
        if entry_name is not None and entry_name not in self.by_name:
            raise ValueError("请先选择一个正式环节。")
        selections = {}
        for contract in self.contracts:
            if (entry_name is not None and contract.name != entry_name) or (entry_name is None and self.entry_items[contract.name].checkState(0) != Qt.CheckState.Checked):
                continue
            arguments = []
            for option in contract.command.params:
                value = self.parameter_values[contract.name][option.name]
                if option.is_flag:
                    if value:
                        arguments.append(option.opts[0])
                    elif option.secondary_opts:
                        arguments.append(option.secondary_opts[0])
                elif value.strip():
                    values = [item.strip() for item in re.split(r"[,\n]", value) if item.strip()] if option.multiple else [value.strip()]
                    for item in values:
                        arguments.extend((option.opts[0], item))
            selections[contract.name] = arguments
        return prepare_request(self.contracts, selections)

    def run_current_entry(self):
        item = self.entry_tree.currentItem()
        name = item.data(0, Qt.ItemDataRole.UserRole) if item else None
        if name not in self.by_name:
            return
        try:
            request = self.current_request(name)
        except (ValueError, OSError, click.ClickException, click.exceptions.Exit) as error:
            QMessageBox.warning(self, "参数未通过", str(error))
            return
        self.launch(request, None)

    def open_b00_source(self):
        if self.b00_tool_id in MAINTENANCE_TOOLS:
            path = MAINTENANCE_TOOLS[self.b00_tool_id][1]
        else:
            filename = "b00_03_notebook_schema_browser.py" if self.b00_tool_id == "schema_browser" else "b00_04_staged_path_transaction.py"
            path = LAKEHOUSE_ROOT / filename
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))

    def show_b00_page(self, page_id, tool_id=None):
        self.workbench_heading.show()
        self.workbench_hint.show()
        title, modes = B00_PAGES[page_id]
        tool_id = tool_id or self.b00_selected_tools[page_id]
        if tool_id not in dict(modes):
            raise ValueError("检查模式不属于当前 b00 页面。")
        self.b00_page_id = page_id
        self.b00_selected_tools[page_id] = tool_id
        self.b00_tool_id = tool_id
        _, _, description, phases = B00_VIEWS[tool_id]
        executable = tool_id in MAINTENANCE_TOOLS
        self.workbench_stack.setCurrentWidget(self.b00_page)
        self.b00_code.setText(page_id)
        self.b00_title.setText(title)
        self.b00_mode_tabs.blockSignals(True)
        existing_modes = tuple(self.b00_mode_tabs.tabData(index) for index in range(self.b00_mode_tabs.count()))
        if existing_modes != tuple(mode_id for mode_id, _ in modes):
            while self.b00_mode_tabs.count():
                self.b00_mode_tabs.removeTab(0)
            for mode_id, label in modes:
                index = self.b00_mode_tabs.addTab(label)
                self.b00_mode_tabs.setTabData(index, mode_id)
        self.b00_mode_tabs.setCurrentIndex(next(index for index, (mode_id, _) in enumerate(modes) if mode_id == tool_id))
        self.b00_mode_tabs.setVisible(len(modes) > 1)
        self.b00_mode_tabs.blockSignals(False)
        self.b00_script_origin.setText("辅助检查 · 实际脚本归属：operations/runtime/verify_operations_runtime.py"
                                       if tool_id == "verify_control" else "")
        self.b00_script_origin.setVisible(tool_id == "verify_control")
        self.maintenance_description.setText(MAINTENANCE_TOOLS[tool_id][3] if executable else description)
        self.run_tool_button.setVisible(executable)
        self.b00_interrupt_button.setVisible(executable)
        self.reload_button.setVisible(tool_id in {"check_code", "check_full", "sync_exports"})
        self.b00_runtime_panel.setVisible(executable)
        self.b00_reference.setVisible(not executable)
        while self.b00_phases_layout.count():
            previous_card = self.b00_phases_layout.takeAt(0).widget()
            previous_card.hide()
            previous_card.deleteLater()
        self.b00_phase_widgets.clear()
        if executable:
            _, path, arguments, _ = MAINTENANCE_TOOLS[tool_id]
            self.maintenance_command.setText(subprocess.list2cmdline([str(PYTHON_PATH), str(path), *arguments]))
            self.run_tool_button.setText("预览并同步  →" if tool_id == "sync_exports" else "执行检查  →")
            for index, (phase, phase_title) in enumerate(phases, 1):
                card = QFrame()
                card.setObjectName("card")
                layout = QVBoxLayout(card)
                layout.setContentsMargins(12, 10, 12, 10)
                label = QLabel(f"{index:02d}  {phase_title}")
                label.setObjectName("section")
                label.setWordWrap(True)
                layout.addWidget(label)
                state_label = QLabel("等待执行")
                state_label.setObjectName("muted")
                layout.addWidget(state_label)
                progress_bar = QProgressBar()
                progress_bar.setRange(0, 1000)
                progress_bar.setValue(0)
                progress_bar.setFormat("总量待确认")
                layout.addWidget(progress_bar)
                self.b00_phase_widgets[phase] = (card, state_label, progress_bar)
                self.b00_phases_layout.addWidget(card, 1)
            headers = (["Notebook / 脚本", *[title for _, title in phases], "结果说明"] if tool_id in {"check_code", "check_full", "sync_exports"}
                       else ["检查项目", "结果", "版本 / 证据"])
            self.b00_results.clear()
            self.b00_results.setColumnCount(len(headers))
            self.b00_results.setHeaderLabels(headers)
            for index in range(len(headers)):
                self.b00_results.header().setSectionResizeMode(index, QHeaderView.ResizeMode.Stretch if index in {0, len(headers) - 1} else QHeaderView.ResizeMode.ResizeToContents)
            self.refresh_b00_dashboard()
        else:
            self.b00_state.setText("共用模块 · 流程说明")
            filename = "b00_03_notebook_schema_browser.py" if tool_id == "schema_browser" else "b00_04_staged_path_transaction.py"
            self.maintenance_command.setText(filename)
            if tool_id == "schema_browser":
                details = [
                    "从 config/data_contracts.py 获取表名、主键、分区和字段说明，保持单一契约来源。",
                    "Notebook 中选择关注的字段和关键内容，便于逐项阅读。",
                    "展开完整 Schema 与 metadata，查看契约细节。",
                    "仅在显式启用时展示有界数据或 raw 文件样例；不调用业务 API、不写入数据。",
                ]
                ending = "在采集或研究 Notebook 中调用 display_schema_metadata。此页是职责说明，没有独立运行任务或完成百分比。"
            else:
                details = [
                    "业务调用方准备 staging 路径，决定事务目标、共同回滚范围与业务验收。",
                    "保留旧目标备份，按目标列表安装新的 staging 路径。",
                    "业务调用方在事务内复读与验收；安装完成本身不代表正式提交成功。",
                    "成功退出后清理备份；异常时按实际移动逆序恢复。配置隔离路径时保留失败新目标，否则移除；恢复不完整保留备份。",
                ]
                ending = "此模块不能独立采集，也不能作为通用恢复工具执行。运行进度归实际调用它的业务环节。"
            while self.b00_reference_layout.count():
                previous_item = self.b00_reference_layout.takeAt(0)
                if previous_item.widget():
                    previous_item.widget().hide()
                    previous_item.widget().deleteLater()
            for index, ((_, phase_title), detail) in enumerate(zip(phases, details), 1):
                card = QFrame()
                card.setObjectName("card")
                card_layout = QHBoxLayout(card)
                card_layout.setContentsMargins(18, 18, 18, 18)
                step_number = QLabel(f"{index:02d}")
                step_number.setObjectName("badge")
                card_layout.addWidget(step_number, 0, Qt.AlignmentFlag.AlignTop)
                text_layout = QVBoxLayout()
                text_layout.setSpacing(8)
                phase_label = QLabel(phase_title)
                phase_label.setObjectName("section")
                text_layout.addWidget(phase_label)
                detail_label = QLabel(detail)
                detail_label.setWordWrap(True)
                detail_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
                text_layout.addWidget(detail_label)
                card_layout.addLayout(text_layout, 1)
                self.b00_reference_layout.addWidget(card)
                if index < len(phases):
                    arrow = QLabel("↓")
                    arrow.setObjectName("muted")
                    arrow.setContentsMargins(30, 0, 0, 0)
                    self.b00_reference_layout.addWidget(arrow)
            footer = QLabel(ending)
            footer.setObjectName("muted")
            footer.setWordWrap(True)
            self.b00_reference_layout.addWidget(footer)
            self.b00_reference_layout.addStretch()
        self.update_selection()

    def run_maintenance(self):
        if self.b00_tool_id not in MAINTENANCE_TOOLS:
            return
        if self.contracts_future is not None:
            self.maintenance_description.setText("正在读取环节参数，请稍候再运行检查。")
            return
        try:
            request = prepare_maintenance_request(self.b00_tool_id)
        except (ValueError, OSError) as error:
            QMessageBox.warning(self, "工具不可用", str(error))
            return
        if request["tool"] == "sync_exports":
            self.preview(request)
        else:
            self.launch(request, None)

    def refresh_b00_dashboard(self):
        """Render the selected tool's own run, with phase counts and its durable log."""
        tool_id = self.b00_tool_id
        if tool_id not in MAINTENANCE_TOOLS:
            return
        run_root = self.b00_runs.get(tool_id)
        status = {}
        if run_root is not None and (run_root / "status.json").is_file():
            status = worker.read_json_shared(run_root / "status.json")
        state = status.get("state")
        terminal = state in worker.TERMINAL_STATES
        scopes = status.get("progress_scopes") or {}
        phases = B00_VIEWS[tool_id][3]
        current_scope = scopes.get(status.get("current_scope_id"), {})
        current_phase = current_scope.get("parent_scope_id") or current_scope.get("phase")
        current_index = next((index for index, (phase, _) in enumerate(phases) if phase == current_phase), None)
        self.b00_state.setText(STATE_LABELS.get(state, "等待状态" if run_root else "未运行"))
        self.b00_state.setProperty("state", state or "unknown")
        self.b00_state.style().unpolish(self.b00_state)
        self.b00_state.style().polish(self.b00_state)
        self.b00_interrupt_button.setEnabled(run_root is not None and run_root == self.active_run_root
                                            and self.worker_process is not None and self.worker_process.poll() is None
                                            and not (run_root / "interrupt.request").exists())
        self.b00_health.setText(
            f"耗时 {elapsed_text(status.get('started_at'), status.get('finished_at'))}　·　"
            + ("运行已结束" if terminal else f"后台心跳 {elapsed_text(status.get('heartbeat_at'))} 前")
            + f"　·　最近进度 {elapsed_text(status.get('progress_at'), status.get('finished_at'))}"
            if status else "耗时 —　·　心跳 —　·　等待启动与状态发布")
        self.b00_health.setToolTip(str(run_root) if run_root else "本窗口尚未运行此项")
        phase_counts = {}
        completed_phases = 0
        for index, (phase, _) in enumerate(phases):
            scope = scopes.get(phase, {})
            children = [entry for entry in scopes.values() if entry.get("parent_scope_id") == phase]
            counter = next(iter((scope.get("counters") or {}).values()), {})
            total = counter.get("total")
            completed = counter.get("completed", 0)
            # Child results are emitted immediately after each operation. Parent
            # counters from older scripts may still point to the previous item.
            processed_children = sum(entry.get("state") in {"completed", "failed"} for entry in children)
            if total is not None:
                completed = min(total, max(completed, processed_children))
                phase_counts[phase] = (completed, total)
            phase_state = scope.get("state")
            failed_children = any(entry.get("state") == "failed" for entry in children)
            if phase_state == "completed":
                completed_phases += 1
            label = ("失败" if phase_state == "failed" or failed_children else "完成" if phase_state == "completed"
                     else "已停止 · 未完成" if terminal and scope else "未执行" if terminal else "进行中" if scope else "等待执行")
            card, state_label, progress_bar = self.b00_phase_widgets[phase]
            state_label.setText(label)
            card.setProperty("current", index == current_index)
            card.style().unpolish(card)
            card.style().polish(card)
            progress_bar.setRange(0, 1000)
            progress_bar.setValue(round(1000 * completed / total) if total else 0)
            progress_bar.setFormat(f"{completed:g} / {total:g}" if total is not None else "总量待确认")
            progress_bar.setToolTip(counter.get("unit", "尚无计数事件"))
        if current_index is None:
            position = "尚未运行 · 执行后呈现流程位置与实际计数" if not run_root else "等待脚本进度 · 无计数时保持未知"
        else:
            position = f"第 {current_index + 1} / {len(phases)} 步 · {phases[current_index][1]}　｜　已完成 {completed_phases} / {len(phases)} 步"
            if terminal:
                position = STATE_LABELS.get(state, state) + "　｜　" + position
        self.b00_position.setText(position)
        current_counts = phase_counts.get(current_phase)
        for key, value in zip(("completed", "total", "remaining"),
                              (current_counts[0], current_counts[1], current_counts[1] - current_counts[0]) if current_counts else (None, None, None)):
            self.b00_metrics[key].setText(f"{value:g}" if value is not None else "—")
        failed_items = {str(entry.get("object", {}).get("notebook") or scope_id)
                        for scope_id, entry in scopes.items() if entry.get("state") == "failed"
                        and (entry.get("parent_scope_id") or not any(child.get("parent_scope_id") == scope_id for child in scopes.values()))}
        self.b00_metrics["failed"].setText(str(len(failed_items)) if scopes else "—")
        errors = status.get("error_summary") or []
        if isinstance(errors, str):
            errors = [errors]
        if status.get("error"):
            errors = [str(status["error"]), *errors]
        if run_root and not status:
            failure_path = run_root / "launch_failure.json"
            if failure_path.is_file():
                errors = [str(worker.read_json_shared(failure_path).get("error", "启动失败"))]
                self.b00_state.setText("启动失败")
        self.b00_error.setText("\n".join(str(error) for error in errors))
        self.b00_error.setVisible(bool(errors))

        selected = self.b00_results.currentItem()
        selected_name = selected.text(0) if selected else None
        scroll_position = self.b00_results.verticalScrollBar().value()
        self.b00_results.clear()
        if tool_id in {"check_code", "check_full", "sync_exports"}:
            notebook_names = {str(contract.path.with_suffix(".ipynb").relative_to(LAKEHOUSE_ROOT)) for contract in self.contracts}
            notebook_names.update(entry["object"]["notebook"] for entry in scopes.values() if entry.get("object", {}).get("notebook"))
            for notebook_name in sorted(notebook_names):
                cells = [notebook_name]
                messages = []
                has_failed = False
                for phase, _ in phases:
                    scope = scopes.get(phase + ":" + notebook_name, {})
                    phase_object = scopes.get(phase, {}).get("object", {})
                    current_file = phase_object.get("notebook") or str(phase_object.get("script", "")).removesuffix(".py") + ".ipynb"
                    result_state = scope.get("state")
                    label = {"completed": "通过" if phase == "check" else "完成", "failed": "失败"}.get(result_state)
                    if scope.get("outcome") == "warning":
                        label = "通过 · 有提示"
                        messages.append("代码一致，完整文件尚未同步")
                    if not label:
                        label = "已停止" if terminal and current_file == notebook_name else "处理中" if not terminal and phase == current_phase and current_file == notebook_name else "未记录" if terminal else "等待"
                    cells.append(label)
                    if scope.get("message"):
                        messages.append(scope["message"])
                    has_failed |= result_state == "failed"
                row = QTreeWidgetItem(self.b00_results, [*cells, "；".join(messages)])
                row.setToolTip(0, notebook_name)
                row.setToolTip(len(cells), "；".join(messages))
                if has_failed:
                    row.setForeground(0, QColor(THEMES[self.theme_name]["danger"]))
                if notebook_name == selected_name:
                    self.b00_results.setCurrentItem(row)
        else:
            for scope_id, scope in scopes.items():
                if scope_id == "packages":
                    continue
                details = scope.get("object", {})
                label = details.get("package") or details.get("check") or ("Python / Conda 环境" if scope_id == "environment" else scope_id)
                result_state = scope.get("state")
                result_label = {"completed": "通过", "failed": "失败"}.get(result_state, "已停止" if terminal else "检查中")
                evidence = details.get("version") or (str(details.get("environment", "")) + " · " + str(details.get("python", "")) if scope_id == "environment" else "具体 I/O 证据见原始日志")
                row = QTreeWidgetItem(self.b00_results, [label, result_label, evidence])
                row.setToolTip(2, evidence)
                row.setForeground(1, QColor(THEMES[self.theme_name]["danger" if result_state == "failed" else "success" if result_state == "completed" else "text"]))
                if label == selected_name:
                    self.b00_results.setCurrentItem(row)
        self.b00_results.verticalScrollBar().setValue(scroll_position)
        log_path = None
        if run_root:
            candidate = pathlib.Path(status["log_path"]).resolve() if status.get("log_path") else None
            if candidate and candidate.is_relative_to(run_root.resolve()) and candidate.is_file():
                log_path = candidate
            elif (run_root / "bootstrap.log").is_file():
                log_path = run_root / "bootstrap.log"
        key = (tool_id, str(run_root), str(log_path))
        if key != self.b00_log_key:
            self.b00_log_key, self.b00_log_offset = key, None
            self.b00_log_decoder.reset()
            self.b00_logs.clear()
        self.b00_log_location.setText("实时日志 · 最近 4,000 行" if log_path else "实时日志 · 尚无日志文件" if run_root else "实时日志 · 尚未运行")
        self.b00_log_location.setToolTip(str(log_path) if log_path else "")
        if self.b00_log_future is not None and self.b00_log_future[1].done():
            future_key, future = self.b00_log_future
            self.b00_log_future = None
            try:
                offset, content, reset = future.result()
                if key == future_key:
                    if reset:
                        self.b00_logs.clear()
                        self.b00_log_decoder.reset()
                    self.b00_log_offset = offset
                    decoded_text = self.b00_log_decoder.decode(content)
                    if decoded_text:
                        previous_scroll = self.b00_logs.verticalScrollBar().value()
                        cursor = self.b00_logs.textCursor()
                        cursor.movePosition(QTextCursor.MoveOperation.End)
                        cursor.insertText(decoded_text)
                        self.b00_logs.verticalScrollBar().setValue(self.b00_logs.verticalScrollBar().maximum() if self.b00_follow_log.isChecked() else previous_scroll)
            except OSError as error:
                if future_key == key:
                    self.b00_log_location.setText(f"日志读取失败：{error}")
        if self.b00_log_future is None and log_path:
            self.b00_log_future = (key, self.io_pool.submit(read_log_chunk, log_path, self.b00_log_offset))

    def refresh_calendar_dashboard(self):
        """Render one producer's real scopes, retaining its own batch and log cursor."""
        name = self.calendar_name
        if name not in CALENDAR_VIEWS or self.b00_tool_id is not None:
            return
        view = CALENDAR_VIEWS[name]
        self.calendar_page.setCurrentWidget(self.trade_result_scroll if name == "b01/c01_trade_calendar" else self.calendar_scroll)
        run_root = self.calendar_runs.get(name)
        status = worker.read_json_shared(run_root / "status.json") if run_root and (run_root / "status.json").is_file() else {}
        stage = status if status.get("stage_name") == name else next(
            (record for record in status.get("stage_history", []) if record.get("name") == name), {})
        state = stage.get("state")
        batch_terminal = status.get("state") in {"succeeded", "failed", "quota_stopped", "interrupted"}
        terminal = state in {"succeeded", "failed", "quota_stopped", "interrupted"}
        self.calendar_description.setText(view["description"])
        self.calendar_boundary.setText(view["boundary"] + " 各条进度独立计量；流程位置不代表耗时百分比。")
        self.calendar_state.setText(STATE_LABELS.get(state, "未执行 · 批次已停止" if batch_terminal else "准备检查 / 等待本环节" if run_root else "尚未运行"))
        self.calendar_state.setProperty("state", state or "unknown")
        self.calendar_state.style().unpolish(self.calendar_state)
        self.calendar_state.style().polish(self.calendar_state)
        self.calendar_health.setText(
            f"本环节耗时 {elapsed_text(stage.get('stage_started_at') or stage.get('started_at'), stage.get('finished_at'))} · "
            + ("批次已结束" if batch_terminal else f"后台心跳 {elapsed_text(status.get('heartbeat_at'))} 前")
            + f" · 最近进度 {elapsed_text(stage.get('progress_at'), stage.get('finished_at'))}"
            if status else "耗时 — · 心跳 — · 等待本窗口启动")
        self.calendar_health.setToolTip(str(run_root) if run_root else "尚无本窗口运行记录")
        self.calendar_interrupt.setEnabled(run_root is not None and run_root == self.active_run_root
            and self.worker_process is not None and self.worker_process.poll() is None
            and not (run_root / "interrupt.request").exists())
        scopes = sorted((scope for scope in (stage.get("progress_scopes") or {}).values()
                         if scope.get("object", {}).get("table") == view["table"]), key=lambda scope: scope.get("sequence", 0))
        current = next((scope for scope in reversed(scopes) if scope.get("phase") != "run"), scopes[-1] if scopes else {})
        current_phase = current.get("phase")
        phase_labels = {**PHASE_LABELS, **({"watermark": "读取已有目标水位"} if name.endswith("c04_futures_bar_calendar") else {})}
        current_index = next((index for index, (_, phases) in enumerate(view["flows"]) if current_phase in phases.split()), None)
        self.calendar_position.setText(
            f"流程位置 {current_index + 1} / 6 · {view['flows'][current_index][0]} · {phase_labels.get(current_phase, current_phase)}"
            if current_index is not None else "尚无本环节进度记录 · 执行前先完成环境、控制面与代码检查")
        for index, ((_, phases), (card, title, label)) in enumerate(zip(view["flows"], self.calendar_flow_widgets)):
            title.setText(f"{index + 1:02d}  {view['flows'][index][0]}")
            latest = next((scope for scope in reversed(scopes) if scope.get("phase") in phases.split()), {})
            latest_state = latest.get("state")
            phase_label = phase_labels.get(latest.get("phase"), latest.get("phase", ""))
            label.setText((phase_label + " · " + {"completed": "已记录完成", "failed": "失败", "skipped": "跳过"}.get(
                latest_state, "停在此处" if terminal else "已进入")) if latest else "无记录 / 视模式执行")
            card.setProperty("current", index == current_index)
            card.style().unpolish(card)
            card.style().polish(card)
        for (caption, phases, field), (label, value_label) in zip(view["metrics"], self.calendar_metric_widgets):
            label.setText(caption)
            metric_scope = next((scope for scope in reversed(scopes)
                                 if scope.get("phase") in phases.split() and field in scope.get("object", {})), {})
            if caption.startswith("当前事务") and any(entry.get("phase") == "commit" and entry.get("sequence", 0) > metric_scope.get("sequence", -1) for entry in scopes):
                metric_scope = {}
            quantity = metric_scope.get("object", {}).get(field)
            value_label.setText(f"{int(quantity):,}" if str(quantity).isdecimal() else "—")
        for index, (row, label, bar) in enumerate(self.calendar_bar_widgets):
            row.setVisible(index < len(view["bars"]))
            if index >= len(view["bars"]):
                continue
            caption, phases, counter_name = view["bars"][index]
            scope = next((scope for scope in reversed(scopes) if scope.get("phase") in phases.split()
                          and counter_name in scope.get("counters", {})), {})
            if caption.startswith("当前事务") and any(entry.get("phase") == "commit" and entry.get("sequence", 0) > scope.get("sequence", -1) for entry in scopes):
                scope = {}  # A later date-range transaction must not inherit the previous one's bars.
            counter = scope.get("counters", {}).get(counter_name, {})
            completed, total, unit = counter.get("completed"), counter.get("total"), counter.get("unit", "")
            label.setText(caption + (f" · 总量 {total:,} · 已处理 {completed:,} · 剩余 {total - completed:,} {unit}"
                                    if total is not None and completed is not None else " · 总量 — · 已处理 — · 剩余 —"))
            bar.setValue(round(1000 * completed / total) if total and completed is not None else 0)
            bar.setFormat(f"{completed:,} / {total:,} {unit}" if total is not None and completed is not None else "尚无可量化记录")
            bar.setToolTip(str(scope.get("object", {}).get("partition") or scope.get("object", {}).get("run_id") or caption))
        task = self.calendar_requests.get(name, {})
        parameters = task.get("effective_parameters", {})
        run_scope = next((scope for scope in reversed(scopes) if scope.get("phase") == "run"), {})
        mode = run_scope.get("object", {}).get("mode")
        mode_label = {"automatic_tail": "日常尾部", "automatic": "自动", "full": "全历史比较", "explicit": "显式范围"}.get(mode, mode or "等待脚本确认范围")
        write_label = "写入业务结果" if parameters.get("write") else "只读业务运行" if task else "尚未固定运行参数"
        context_lines = [f"本次运行：{mode_label} · {write_label}"]
        if task:
            context_lines.append("已固定参数：" + (" ".join(task.get("arguments", [])) or "原入口默认参数"))
        current_object = dict(current.get("object", {}))
        if name.endswith("c04_futures_bar_calendar") and current_phase in {"build_structure", "minute_structure", "daily_structure", "build_fresh"}:
            planning = next((scope for scope in reversed(scopes) if scope.get("phase") == "plan"), {})
            if planning.get("object", {}).get("key"):
                current_object["key"] = planning["object"]["key"]
        context_lines.append("当前对象：" + (" · ".join(f"{OBJECT_LABELS.get(key, key)} {current_object[key]}" for key in (
            "partition", "key", "batch", "trading_date", "start_date", "end_date", "bar_frequency", "exchange_code", "year", "month",
            "run_id", "automatic_tail_processed_through") if key in current_object) or "尚无对象记录"))
        comparison = next((scope.get("object", {}) for scope in reversed(scopes) if scope.get("phase") in {"reconcile", "plan"}), {})
        comparison_text = " · ".join(f"{label} {comparison[field]}" for field, label in (
            ("changed_session_count", "变化 Session"), ("extra_session_count", "多余 Session"),
            ("no_valid_rule_contract_day_count", "无有效时段规则合约日"), ("changed_grid_count", "变化格点"), ("extra_grid_count", "多余格点")) if field in comparison)
        if comparison_text:
            context_lines.append("差异与来源提示：" + comparison_text)
        self.calendar_context.setText("\n".join(context_lines))
        # Durable evidence is distinct from loop progress and the worker's exit status.
        transaction = next((scope for scope in reversed(scopes) if scope.get("phase") == "transaction_complete"), {})
        install = next((scope for scope in reversed(scopes) if scope.get("phase") == "install_partitions"), {})
        if name in {"b01/c01_trade_calendar", "b01/c02_futures_variety_calendar"}:
            same_transaction = (transaction.get("object", {}).get("run_id") == install.get("object", {}).get("run_id"))
            newer_commit = transaction.get("sequence", -1) >= max((scope.get("sequence", 0) for scope in scopes
                if scope.get("phase") in {"commit", "merge", "merge_validate", "install_partitions"}), default=-1)
            if transaction.get("state") == "completed" and same_transaction and newer_commit:
                evidence = transaction.get("object", {})
                commit_text = f"最近共同事务已确认 · {evidence.get('partitions', '—')} 个分区 · {evidence.get('rows', '—')} 行"
            else:
                commit_text = "正式提交：当前事务尚无成功退出记录" if install else "正式提交：尚无提交确认记录"
        else:
            commit = next((scope for scope in reversed(scopes) if scope.get("phase") == "commit_batch"), {})
            counter = next(iter(commit.get("counters", {}).values()), {})
            commit_text = (f"已正式提交 {counter['completed']:,} 叶" if name.endswith("c03_futures_contract_calendar") else f"已处理 {counter['completed']:,} 叶（含跳过）") if counter else "正式提交：尚无批次提交计数"
            if name.endswith("c03_futures_contract_calendar"):
                watermark = next((scope for scope in reversed(scopes) if scope.get("phase") == "watermark"), {})
                commit_text += " · 处理水位已确认" if watermark.get("state") == "completed" and watermark.get("object", {}).get("outcome") == "watermark_committed" else " · 尚无本次水位提交确认"
            elif commit.get("object", {}).get("skipped_clean_partitions") is not None:
                commit_text += f" · 跳过 clean 叶 {commit['object']['skipped_clean_partitions']} · 正式提交 {commit['object'].get('partitions', '—')} 叶"
        if task and not parameters.get("write"):
            commit_text = "本次为只读运行 · 不提交业务结果"
        if any(scope.get("object", {}).get("outcome") in {"up_to_date", "source_unchanged"} for scope in scopes):
            commit_text += " · 脚本报告无需更新"
        self.calendar_commit.setText(commit_text)
        errors = [error for error in (stage.get("error_summary", []) if stage else status.get("error_summary", []))
                  if not stage or not isinstance(error, dict) or error.get("stage_name", name) == name]
        if status.get("error") and (not stage or status.get("stage_name") == name):
            errors.insert(0, status["error"])
        if run_root and not status and (run_root / "launch_failure.json").is_file():
            errors.append(worker.read_json_shared(run_root / "launch_failure.json").get("error", "启动失败"))
        self.calendar_error.setText("\n".join(str(error) for error in errors))
        self.calendar_error.setVisible(bool(errors))
        # Before this entry starts, follow only preflight logs, never another producer.
        log_status = stage or (status if status.get("phase") == "preflight" else {})
        log_path = pathlib.Path(log_status["log_path"]).resolve() if log_status.get("log_path") else None
        if not (run_root and log_path and log_path.is_relative_to(run_root.resolve()) and log_path.is_file()):
            log_path = run_root / "bootstrap.log" if run_root and not status and (run_root / "bootstrap.log").is_file() else None
        key = (name, str(run_root), str(log_path))
        if key != self.calendar_log_key:
            self.calendar_log_key, self.calendar_log_offset = key, None
            self.calendar_log_decoder.reset()
            self.calendar_logs.clear()
        self.calendar_log_location.setText(("准备检查日志" if not stage and log_path else "本环节日志") + " · 最近 4,000 行" if log_path else "本环节日志 · 尚无日志文件")
        self.calendar_log_location.setToolTip(str(log_path) if log_path else "")
        if self.calendar_log_future is not None and self.calendar_log_future[1].done():
            future_key, future = self.calendar_log_future
            self.calendar_log_future = None
            try:
                offset, content, reset = future.result()
                if key == future_key:
                    if reset:
                        self.calendar_logs.clear()
                        self.calendar_log_decoder.reset()
                    self.calendar_log_offset = offset
                    decoded_text = self.calendar_log_decoder.decode(content)
                    if decoded_text:
                        scroll = self.calendar_logs.verticalScrollBar().value()
                        cursor = self.calendar_logs.textCursor()
                        cursor.movePosition(QTextCursor.MoveOperation.End)
                        cursor.insertText(decoded_text)
                        self.calendar_logs.verticalScrollBar().setValue(self.calendar_logs.verticalScrollBar().maximum() if self.calendar_follow_log.isChecked() else scroll)
                        if name == "b01/c01_trade_calendar" and self.trade_follow_log.isChecked():
                            self.trade_logs.verticalScrollBar().setValue(self.trade_logs.verticalScrollBar().maximum())
            except OSError as error:
                if future_key == key:
                    self.calendar_log_location.setText(f"日志读取失败：{error}")
        if self.calendar_log_future is None and log_path:
            self.calendar_log_future = (key, self.io_pool.submit(read_log_chunk, log_path, self.calendar_log_offset))
        if name == "b01/c01_trade_calendar":
            self.refresh_trade_calendar_result(status, stage, scopes, task)
            self.trade_log_location.setText(self.calendar_log_location.text())
            self.trade_log_location.setToolTip(self.calendar_log_location.toolTip())

    def refresh_trade_calendar_result(self, status, stage, scopes, task):
        """Show b01's outcome and table, including the zero-work fast path."""
        run_root = self.calendar_runs.get("b01/c01_trade_calendar")
        terminal = stage.get("state") in worker.TERMINAL_STATES
        succeeded = stage.get("state") == "succeeded"
        no_update = any(scope.get("object", {}).get("outcome") == "up_to_date" for scope in scopes)
        run_scope = next((scope for scope in reversed(scopes) if scope.get("phase") == "run"), {})
        run_object = run_scope.get("object", {})
        collect = next((scope for scope in reversed(scopes) if scope.get("phase") == "collect"), {})
        collected = collect.get("object", {})
        transaction = next((scope for scope in reversed(scopes) if scope.get("phase") == "transaction_complete"), {})
        committed = transaction.get("state") == "completed"
        parameters = task.get("effective_parameters", {})
        writing = bool(parameters.get("write"))
        generated_rows = collected.get("rows")
        pending_rows = next((scope["object"]["missing_or_revised_grid_count"] for scope in reversed(scopes)
                             if "missing_or_revised_grid_count" in scope.get("object", {})), None)
        if pending_rows is None and run_object.get("mode") != "full":
            pending_rows = generated_rows
        submitted_rows = transaction.get("object", {}).get("rows") if committed else None
        if not stage and status.get("state") in worker.TERMINAL_STATES:
            headline = "批次已停止 · 本环节未执行"
            detail = "准备检查或前序环节未通过，请查看执行日志与错误信息。"
            progress_text, progress_value = "业务尚未开始", 0
        elif no_update:
            headline = "已是最新 · 本次无需更新" if succeeded else "已确认无需更新 · 等待运行结束" if not terminal else "无需更新 · 运行异常结束"
            detail = "新增 / 修订 0 行 · 本次写入 0 行"
            watermark = run_object.get("latest_calendar_date") or run_object.get("valid_end_date") or run_object.get("valid_end")
            if watermark:
                detail += f" · 已覆盖至 {watermark}"
            if run_object.get("api_calls") == "0":
                detail += " · 本次 API 请求 0 次"
            progress_text = "范围检查已完成 · 无需生成或提交"
            progress_value = 1000
        elif terminal:
            headline = ("写入完成 · 查看日历结果" if writing and committed else "只读检查完成 · 查看生成结果" if not writing
                        else "运行完成 · 尚无提交确认") if succeeded else STATE_LABELS.get(stage.get("state"), "运行停止") + " · 保留最后结果"
            submitted_label = submitted_rows if committed else "0（只读）" if not writing else "未确认"
            detail = f"本次生成 {generated_rows if generated_rows is not None else '未记录'} 行 · 待写入 {pending_rows if pending_rows is not None else '未记录'} 行 · 已提交 {submitted_label} 行"
            progress_text = "生成与验收已完成 · 只读模式未提交" if succeeded and not writing else "共同事务已成功退出" if committed else "运行已结束，请核对日志"
            progress_value = 1000 if succeeded else 0
        elif run_root:
            current = next((scope for scope in reversed(scopes) if scope.get("phase") != "run"), {})
            phase = current.get("phase")
            headline = "正在" + {"collect": "生成交易日历", "trade_days": "请求交易日", "authenticate": "连接数据源",
                "build_validate": "生成并验收日历", "install_partitions": "安装年度分区", "transaction_complete": "确认运行结果",
                "plan": "确定更新范围"}.get(phase, PHASE_LABELS.get(phase, "准备运行"))
            counter = next(iter(current.get("counters", {}).values()), {})
            total, completed = counter.get("total"), counter.get("completed")
            progress_value = round(1000 * completed / total) if total else None
            progress_text = f"已处理 {completed} / {total} {counter.get('unit', '')} · 剩余 {total - completed}" if total is not None else "正在处理，等待脚本返回结果"
            counter = collect.get("counters", {}).get("natural_days", {})
            detail = f"本次自然日 {counter.get('total', '待确定')} · 已生成 {generated_rows or 0} 行 · 已提交 {submitted_rows or 0} 行"
        else:
            headline = "交易日历 · 已落盘数据"
            detail = "每个自然日一行，直接查看交易日标记；运行后在这里查看本次结果。"
            progress_text, progress_value = "尚未启动本环节", 0
        self.trade_outcome.setText(headline)
        self.trade_run_summary.setText(detail)
        date_range = " → ".join(str(collected[key]) for key in ("start_date", "end_date") if key in collected)
        health_text = (f"本环节耗时 {elapsed_text(stage.get('stage_started_at') or stage.get('started_at'), stage.get('finished_at'))} · 已结束"
                       if terminal else self.calendar_health.text())
        self.trade_execution.setText((f"本次范围 {date_range} · " if date_range else "") + health_text)
        self.trade_progress.setRange(0, 0 if progress_value is None else 1000)
        self.trade_progress.setValue(progress_value or 0)
        self.trade_progress.setFormat(progress_text)
        self.trade_interrupt.setEnabled(self.calendar_interrupt.isEnabled())
        self.trade_interrupt.setVisible(self.calendar_interrupt.isEnabled())
        self.trade_error.setText(self.calendar_error.text())
        self.trade_error.setVisible(bool(self.calendar_error.text()))

        # Local batch preview is atomic; the lake is read on entry, explicit refresh or termination, never every tick.
        preview_path = run_root / "artifacts" / "b01_generated.parquet" if run_root else None
        preview_exists = bool(preview_path and preview_path.is_file())
        if self.trade_table_run != run_root:
            self.trade_table_run = run_root
            self.trade_table_auto_source = True
        if self.trade_table_auto_source:
            self.trade_source.setCurrentIndex(1 if preview_exists and not (succeeded and (committed or no_update)) else 0)
        generated = self.trade_source.currentData() == "generated"
        lake_root = parameters.get("lake_root") if task else self.parameter_values.get("b01/c01_trade_calendar", {}).get("lake_root")
        lake_root = str(lake_root).strip() if lake_root else None
        preview_stamp = preview_path.stat().st_mtime_ns if preview_exists else None
        key = (str(run_root), lake_root, generated, preview_stamp if generated else None,
               stage.get("finished_at") or stage.get("state") if terminal else None, self.trade_table_refresh)
        writing_now = writing and not terminal and status.get("state") not in worker.TERMINAL_STATES
        self.trade_reload.setEnabled(not writing_now or generated)
        if key != self.trade_table_key:
            self.trade_table_key = key
            self.trade_table_snapshot = None
            self.trade_table.clear()
            self.trade_page_label.clear()
            self.trade_table_note.clear()
            self.trade_previous.setEnabled(False)
            self.trade_next.setEnabled(False)
        if generated and not preview_exists:
            self.trade_table_summary.setText("本批尚无生成结果")
            self.trade_table_note.setText("无需更新时可查看已落盘日历；旧批次没有生成预览。")
            return
        if writing_now and not generated:
            self.trade_table_summary.setText("等待写入结束后读取已落盘日历")
            self.trade_table_note.setText("当前可切换到本批生成结果；生成结果与正式提交分开显示。")
            return
        if self.trade_table_future is not None and self.trade_table_future[1].done():
            future_key, future = self.trade_table_future
            self.trade_table_future = None
            try:
                snapshot = future.result()
                if future_key == key:
                    self.trade_table_snapshot = snapshot
                    rows = snapshot["rows"]
                    self.trade_year.blockSignals(True)
                    self.trade_month.blockSignals(True)
                    self.trade_year.clear()
                    self.trade_year.addItem("全部年份", None)
                    for year in sorted({row["calendar_date"].year for row in rows}, reverse=True):
                        self.trade_year.addItem(str(year), year)
                    if rows:
                        self.trade_year.setCurrentIndex(1)
                        self.trade_month.setCurrentIndex(rows[0]["calendar_date"].month)
                    self.trade_year.blockSignals(False)
                    self.trade_month.blockSignals(False)
                    self.filter_trade_calendar()
            except Exception as error:
                if future_key == key:
                    self.trade_table_snapshot = {"error": str(error), "rows": []}
                    self.trade_table_summary.setText("日历读取失败")
                    self.trade_table_note.setText(str(error) + "；可点击刷新数据重读。")
        if self.trade_table_future is None and self.trade_table_snapshot is None:
            self.trade_table_summary.setText("正在读取日历表…")
            self.trade_table_future = (key, self.trade_table_pool.submit(read_trade_calendar_result, lake_root, preview_path if generated else None))
        if self.trade_table_snapshot and "error" not in self.trade_table_snapshot:
            snapshot = self.trade_table_snapshot
            source_label = "本批生成结果（尚未提交）" if generated and not committed else "本批生成结果（事务已确认；当前显示生成时的值）" if generated else "已落盘日历"
            read_at = datetime.fromisoformat(snapshot["read_at"]).astimezone(DISPLAY_TIMEZONE).strftime("%H:%M:%S")
            self.trade_table_note.setText(source_label + " · 读取于 " + read_at + " · 每页 100 行")
            self.trade_table_note.setToolTip(snapshot["path"])

    def change_trade_source(self, *_):
        self.trade_table_auto_source = False
        self.reload_trade_calendar()

    def reload_trade_calendar(self):
        self.trade_table_refresh += 1
        self.refresh_calendar_dashboard()

    def filter_trade_calendar(self, *_):
        self.trade_table_page = 0
        self.page_trade_calendar(0)

    def page_trade_calendar(self, direction):
        snapshot = self.trade_table_snapshot
        if not snapshot or "error" in snapshot:
            return
        year, month, trading = self.trade_year.currentData(), self.trade_month.currentData(), self.trade_day_filter.currentData()
        rows = [row for row in snapshot["rows"] if (year is None or row["calendar_date"].year == year)
                and (month is None or row["calendar_date"].month == month) and (trading is None or row["is_trading_day"] == trading)]
        page_count = max(1, (len(rows) + 99) // 100)
        self.trade_table_page = min(max(0, self.trade_table_page + direction), page_count - 1)
        self.trade_table.clear()
        for row in rows[self.trade_table_page * 100:(self.trade_table_page + 1) * 100]:
            cells = []
            for field in TRADE_CALENDAR_SCHEMA:
                value = row[field.name]
                if field.name == "weekday":
                    text = "周" + "一二三四五六日"[int(value) - 1] if value in range(1, 8) else str(value)
                elif field.name == "is_trading_day":
                    text = "● 交易日" if value is True else "休市" if value is False else "空值"
                elif isinstance(value, bool):
                    text = "是" if value else "否"
                elif isinstance(value, datetime):
                    text = value.astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S") if value.tzinfo else value.isoformat(sep=" ")
                else:
                    text = str(value) if value is not None else "空值"
                cells.append(text)
            item = QTreeWidgetItem(self.trade_table, cells)
            index = TRADE_CALENDAR_SCHEMA.get_field_index("is_trading_day")
            item.setForeground(index, QColor(THEMES[self.theme_name]["success" if row["is_trading_day"] else "muted"]))
            for index, field in enumerate(TRADE_CALENDAR_SCHEMA):
                item.setToolTip(index, f"{field.name} = {row[field.name]}")
        for index, field in enumerate(TRADE_CALENDAR_SCHEMA):
            self.trade_table.setColumnHidden(index, not self.trade_all_fields.isChecked() and field.name not in {
                "calendar_date", "weekday", "is_trading_day", "is_weekend", "effective_after", "updated_at"})
            self.trade_table.header().setSectionResizeMode(index, QHeaderView.ResizeMode.ResizeToContents
                if self.trade_all_fields.isChecked() or field.name == "updated_at" else QHeaderView.ResizeMode.Stretch)
        total_rows = len(snapshot["rows"])
        trading_rows = sum(row["is_trading_day"] is True for row in snapshot["rows"])
        if total_rows:
            dates = [row["calendar_date"] for row in snapshot["rows"]]
            self.trade_table_summary.setText(f"{total_rows:,} 行 · 交易日 {trading_rows:,} · 非交易日 {total_rows - trading_rows:,}\n{min(dates)} → {max(dates)}")
        else:
            self.trade_table_summary.setText("目标尚未建表" if not snapshot.get("exists") else "日历表为空 · 0 行")
        self.trade_page_label.setText(f"筛选结果 {len(rows):,} 行 · 第 {self.trade_table_page + 1} / {page_count} 页 · 日期倒序")
        self.trade_previous.setEnabled(self.trade_table_page > 0)
        self.trade_next.setEnabled(self.trade_table_page + 1 < page_count)

    def reload_catalog(self):
        if self.contracts_future is None:
            self.contracts_future = self.io_pool.submit(discover_contracts)
            self.selection_label.setText("正在重新读取原文件参数…")
            self.loading_timer.start()
            self.update_selection()

    def preview(self, request=None):
        try:
            request = self.current_request() if request is None or isinstance(request, bool) else request
        except (ValueError, OSError, click.ClickException, click.exceptions.Exit) as error:
            QMessageBox.warning(self, "参数未通过", str(error))
            return
        dialog = QDialog(self)
        dialog.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dialog.setWindowTitle("核对本批命令")
        dialog.resize(min(1000, self.width()), min(660, self.height()))
        layout = QVBoxLayout(dialog)
        layout.setContentsMargins(22, 20, 22, 18)
        layout.setSpacing(14)
        title = QLabel("核对本批命令")
        title.setObjectName("heading")
        layout.addWidget(title)
        maintenance = request.get("kind") == "maintenance"
        summary = QLabel("将从 Notebook 覆盖生成 19 份同名 .py，并进行完整导出复核。" if maintenance else f"本批 {len(request['tasks'])} 个环节 · 按顺序执行，环节失败后停止后续")
        summary.setObjectName("muted")
        layout.addWidget(summary)
        preview_text = QPlainTextEdit()
        preview_text.setReadOnly(True)
        command_lines = []
        for index, task in enumerate(request["tasks"], 1):
            if maintenance:
                command_lines.append(MAINTENANCE_TOOLS[request["tool"]][3] + "\n" +
                                     subprocess.list2cmdline([str(PYTHON_PATH), str(PROJECT_ROOT / task["entrypoint"]), *task["arguments"]]))
                command_lines.append("覆盖范围（由同名 Notebook 生成）：\n" + "\n".join(str(path.with_suffix(".py").relative_to(PROJECT_ROOT)) for directory in COLLECTION_DIRS for path in sorted(directory.glob("c*.ipynb"))))
                continue
            write = task["effective_parameters"].get("write", False)
            target = task["effective_parameters"].get("lake_root") or "由原入口读取 .env 的 FUTURES_LAKE_ROOT"
            command_lines.append(f"{index}. {task['name']}\n{'写入业务结果' if write else '只读业务运行（可能请求 API）'} | 湖根目录：{target}\n"
                                 + subprocess.list2cmdline([str(PYTHON_PATH), str(PROJECT_ROOT / task["entrypoint"]), *task["arguments"]])
                                 + "\n有效参数：" + json.dumps(task["effective_parameters"], ensure_ascii=False))
        preview_text.setPlainText("\n\n".join(command_lines))
        layout.addWidget(preview_text, 1)
        note = QLabel("使用默认 PythonExporter；写后完整检查。结束后会重新读取环节参数，保留当前输入。" if maintenance else "预览解析原参数类型；跨参数规则和数据就绪条件仍由原业务入口检查。")
        note.setObjectName("muted")
        note.setWordWrap(True)
        layout.addWidget(note)
        buttons = QHBoxLayout()
        buttons.addStretch()
        back_button = QPushButton("返回修改")
        back_button.clicked.connect(dialog.reject)
        buttons.addWidget(back_button)
        start_button = QPushButton("确认同步导出" if maintenance else "启动本批次")
        start_button.setObjectName("accent")
        start_button.clicked.connect(lambda: self.launch(request, dialog))
        buttons.addWidget(start_button)
        layout.addLayout(buttons)
        dialog.open()

    def launch(self, request, dialog=None):
        if self.worker_process is not None and self.worker_process.poll() is None:
            QMessageBox.warning(dialog, "已有运行", "当前窗口的批次尚未停止。")
            return
        try:
            run_root, process = start_detached_batch(request)
        except Exception as error:
            QMessageBox.critical(dialog, "启动失败", str(error))
            return
        self.active_run_root = self.view_run_root = run_root
        self.worker_process = process
        self.active_request = request
        if request.get("kind") == "maintenance":
            self.b00_runs[request["tool"]] = run_root
        else:
            for task in request["tasks"]:
                if task["name"] in CALENDAR_VIEWS:
                    self.calendar_runs[task["name"]] = run_root
                    self.calendar_requests[task["name"]] = task
        self.monitor_error = ""
        self.preview_button.setEnabled(False)
        self.interrupt_button.setEnabled(True)
        self.interrupt_button.setText("请求中断本窗口批次")
        if dialog is not None:
            dialog.accept()
        if request.get("kind") == "maintenance":
            page_id = next(page_id for page_id, (_, modes) in B00_PAGES.items() if request["tool"] in dict(modes))
            self.b00_selected_tools[page_id] = request["tool"]
            item = self.b00_items[page_id]
            if self.entry_tree.currentItem() is item:
                self.show_b00_page(page_id)
            else:
                self.entry_tree.setCurrentItem(item)
            self.tabs.setCurrentWidget(self.configure_tab)
        elif len(request["tasks"]) == 1 and request["tasks"][0]["name"] in CALENDAR_VIEWS:
            self.entry_tree.setCurrentItem(self.entry_items[request["tasks"][0]["name"]])
            self.detail_tabs.setCurrentWidget(self.calendar_page)
            self.tabs.setCurrentWidget(self.configure_tab)
        else:
            self.tabs.setCurrentWidget(self.monitor_tab)
        self.refresh_monitor()

    def interrupt(self):
        if self.active_run_root is not None and self.worker_process is not None and self.worker_process.poll() is None:
            try:
                request_interruption(self.active_run_root)
                self.interrupt_button.setEnabled(False)
                self.interrupt_button.setText("已请求中断，等待后台停止")
            except OSError as error:
                QMessageBox.critical(self, "中断请求未写入", str(error))

    def refresh_monitor(self):
        try:
            active = self.worker_process is not None and self.worker_process.poll() is None
            viewing_current = self.active_run_root is not None and self.view_run_root == self.active_run_root
            self.current_button.setEnabled(self.active_run_root is not None and not viewing_current)
            self.current_button.setToolTip(
                "本窗口尚未启动批次；已有记录请从历史批次查看。" if self.active_run_root is None
                else "已在查看本窗口最近启动的批次。" if viewing_current
                else "返回本窗口最近启动的批次；不会重新执行。"
            )
            active_status = None
            if active and self.active_run_root is not None:
                status_path = self.active_run_root / "status.json"
                if status_path.exists():
                    active_status = worker.read_json_shared(status_path)
            if self.view_run_root is None:
                context = "本窗口尚未启动批次"
            elif viewing_current:
                context = "正在查看本窗口最近启动的批次"
            else:
                context = "正在查看历史记录（只读）"
            if active and not viewing_current:
                if active_status is not None:
                    active_name = active_status.get("stage_name") or "尚未开始"
                    active_label = ENTRY_LABELS.get(active_name.split("/")[-1], active_name)
                    context += (f"\n本窗口批次：{STATE_LABELS.get(active_status.get('state'), '状态未知')} · {active_label}"
                                f" · 进度 {active_status.get('progress') or '尚未提供'}"
                                f" · 耗时 {elapsed_text(active_status.get('started_at'), active_status.get('finished_at'))}"
                                f" · 心跳 {elapsed_text(active_status.get('heartbeat_at'))} 前")
                else:
                    context += "\n本窗口批次正在启动，尚未发布状态。"
            self.view_context_label.setText(context)
            self.active_batch_banner.setVisible(active)
            if active:
                if active_status:
                    scopes = active_status.get("progress_scopes") or {}
                    scope = scopes.get(active_status.get("current_scope_id"), {})
                    phase_scope = scopes.get(scope.get("parent_scope_id"), scope)
                    counts = " · ".join(f"{counter['completed']} / {counter['total'] if counter['total'] is not None else '?'} {counter['unit']}"
                                        for counter in (phase_scope.get("counters") or {}).values())
                    self.active_batch_banner.setText(
                        f"本窗口批次 · {ENTRY_LABELS.get(active_status.get('stage_name', '').split('/')[-1], active_status.get('stage_name') or '准备启动')}"
                        f" · {active_status.get('progress') or '等待进度'}" + (" · " + counts if counts else "")
                        + f" · 耗时 {elapsed_text(active_status.get('started_at'))} · 心跳 {elapsed_text(active_status.get('heartbeat_at'))} 前")
                else:
                    self.active_batch_banner.setText("本窗口批次正在启动，等待后台发布状态。")
            if self.view_run_root is not None:
                status_path = self.view_run_root / "status.json"
                if status_path.exists():
                    status = active_status if self.view_run_root == self.active_run_root and active_status else worker.read_json_shared(status_path)
                    self.render_status(status)
                else:
                    # Never leave the previously viewed batch's metrics or rows
                    # on a new target that has not published status.
                    launch_failure_path = self.view_run_root / "launch_failure.json"
                    launch_failed = launch_failure_path.exists()
                    self.run_title.setText("启动失败" if launch_failed else "正在启动后台" if active and viewing_current else "未发布状态")
                    self.run_title.setProperty("state", "failed" if launch_failed else "unknown")
                    self.run_title.style().unpolish(self.run_title)
                    self.run_title.style().polish(self.run_title)
                    self.run_path_label.setText(str(self.view_run_root))
                    self.elapsed_label.setText("—")
                    self.heartbeat_label.setText("—")
                    self.health_label.setText("本批尚未发布状态，无法确认执行结果。")
                    self.progress_label.setText("尚未发布阶段进度。")
                    self.stage_tree.clear()
                    self.stage_items.clear()
                    self.view_status = {}
                    self.progress_tree.clear()
                    self.activity_label.setText("本批尚未发布阶段信息。")
                    self.activity_progress.setRange(0, 1)
                    self.activity_progress.setValue(0)
                    self.activity_progress.setFormat("尚未发布")
                    self.log_key, self.log_offset = None, None
                    self.log_location.clear()
                    self.logs.clear()
                    bootstrap_path = self.view_run_root / "bootstrap.log"
                    if bootstrap_path.exists():
                        with bootstrap_path.open("rb") as stream:
                            stream.seek(max(0, bootstrap_path.stat().st_size - 32768))
                            self.logs.setPlainText(stream.read().decode("utf-8", errors="replace"))
                    if launch_failed:
                        launch_failure = worker.read_json_shared(launch_failure_path)
                        self.error_label.setText("后台启动失败：" + str(launch_failure.get("error", "请查看启动失败证据。")))
                    elif active and viewing_current:
                        self.error_label.clear()
                    else:
                        self.error_label.setText("请检查启动日志与锁现场。未发布状态不代表成功；不会自动续跑。")
                    self.error_label.setVisible(bool(self.error_label.text()))
            self.refresh_b00_dashboard()
            self.refresh_calendar_dashboard()
            # Only a successful UI-thread read and render renews the lease.
            if active and self.active_run_root is not None:
                worker.atomic_write_json(self.active_run_root / "monitor.json", {
                    "pid": os.getpid(), "process_created_at": psutil.Process().create_time(), "heartbeat_at": worker.utc_now_text(),
                })
                self.monitor_error = ""
            self.update_selection()
            if not active:
                self.interrupt_button.setEnabled(False)
                self.interrupt_button.setText("请求中断本窗口批次")
                if (self.active_request and self.active_request.get("tool") == "sync_exports"
                        and self.catalog_reloaded_run != self.active_run_root and self.contracts_future is None):
                    self.catalog_reloaded_run = self.active_run_root
                    self.reload_catalog()
                if self.close_after_stop:
                    self.close()
        except Exception as error:
            self.monitor_error = f"监控读取或发布失败：{error}。后台将在监控心跳失效时停止；请保留窗口查看现场。"
            self.error_label.setText(self.monitor_error)
            self.error_label.show()
            self.active_batch_banner.setText(self.monitor_error)
            self.active_batch_banner.show()
            self.b00_error.setText(self.monitor_error)
            self.b00_error.show()

    def render_status(self, status):
        self.view_status = status
        if self.rendered_run_root != self.view_run_root:
            self.rendered_run_root = self.view_run_root
            self.follow_stage.blockSignals(True)
            self.follow_stage.setChecked(True)
            self.follow_stage.blockSignals(False)
            self.stage_tree.clear()
            self.stage_items.clear()
        state = status.get("state", "unknown")
        terminal = state in worker.TERMINAL_STATES
        self.run_title.setText(STATE_LABELS.get(state, state))
        if self.run_title.property("state") != state:
            self.run_title.setProperty("state", state)
            self.run_title.style().unpolish(self.run_title)
            self.run_title.style().polish(self.run_title)
        self.run_path_label.setText(str(self.view_run_root))
        worker_alive = worker.process_is_alive(status.get("worker_pid"))
        heartbeat = "已结束" if terminal else elapsed_text(status.get("heartbeat_at")) + " 前"
        heartbeat_stale = False
        if not terminal and status.get("heartbeat_at"):
            try:
                heartbeat_stale = (datetime.now(timezone.utc) - datetime.fromisoformat(status["heartbeat_at"])).total_seconds() > 10
            except ValueError:
                heartbeat_stale = True
        if heartbeat_stale:
            heartbeat += "（过期）"
        self.elapsed_label.setText(elapsed_text(status.get("started_at"), status.get("finished_at")))
        self.heartbeat_label.setText(heartbeat)
        self.health_label.setText(f"后台 {'存活' if worker_alive else '已退出'}   ·   最近业务输出 {elapsed_text(status.get('last_output_at')) + ' 前' if not terminal else '已结束'}   ·   最近进度更新 {elapsed_text(status.get('progress_at')) + ' 前' if not terminal else '已结束'}")
        name = status.get("stage_name", "")
        self.progress_label.setText(f"当前阶段：{ENTRY_LABELS.get(name.split('/')[-1], name) or '尚未开始'}\n业务进度：{status.get('progress') or '尚未提供'}")
        error = status.get("error") or ""
        summaries = status.get("error_summary") or []
        if summaries:
            error += "\n" + "\n".join(str(item.get("message", item)) if isinstance(item, dict) else str(item) for item in summaries[:8])
            if len(summaries) > 8:
                error += f"\n另有 {len(summaries) - 8} 项，见阶段日志。"
        if not terminal and (not worker_alive or heartbeat_stale):
            error = "运行状态异常或已过期；请检查状态、日志与锁现场。\n" + error
        control_failure_path = self.view_run_root / "control_failure.json"
        if control_failure_path.exists():
            error = "控制面失败证据：" + str(worker.read_json_shared(control_failure_path).get("error")) + "\n" + error
        self.error_label.setText(error)
        self.error_label.setVisible(bool(error))
        manifest_path = self.view_run_root / "command_manifest.json"
        manifest = worker.read_json_shared(manifest_path) if manifest_path.exists() else {}
        history = {(item["phase"], item["name"]): item for item in status.get("stage_history", [])}
        rows = []
        for phase, key in (("preflight", "preflights"), ("business", "stages")):
            for entry in manifest.get(key, []):
                name = entry["name"]
                result = history.get((phase, name))
                entry_state = result["state"] if result else "pending"
                duration = elapsed_text(result.get("started_at"), result.get("finished_at")) if result else "—"
                if name == status.get("stage_name") and not result:
                    entry_state = state if terminal else "running"
                    duration = elapsed_text(status.get("stage_started_at"), status.get("finished_at"))
                elif not result and terminal:
                    entry_state = "unknown" if "stage_history" not in status else "not_run"
                label = ENTRY_LABELS.get(name.split("/")[-1], name)
                if phase == "preflight":
                    label = {1: "Python 运行环境", 2: "Windows 控制面 I/O", 3: "Notebook 导出同步"}.get(entry["index"], name)
                rows.append((phase + ":" + name, f"{entry['index']:02d}  {label}", entry_state, duration))
        if tuple(self.stage_items) != tuple(row[0] for row in rows):
            self.stage_tree.blockSignals(True)
            self.stage_tree.clear()
            self.stage_items.clear()
            for key, title, _, _ in rows:
                item = QTreeWidgetItem(self.stage_tree, [title, "", ""])
                item.setData(0, Qt.ItemDataRole.UserRole, key)
                item.setToolTip(0, key)
                self.stage_items[key] = item
            self.stage_tree.blockSignals(False)
        for key, title, entry_state, duration in rows:
            item = self.stage_items[key]
            label = STATE_LABELS.get(entry_state, {"pending": "等待", "not_run": "未执行", "unknown": "旧记录未记录"}.get(entry_state, entry_state))
            item.setText(1, label)
            item.setText(2, duration)
            item.setData(1, Qt.ItemDataRole.UserRole, entry_state)
            token = {"failed": "danger", "succeeded": "success", "running": "info"}.get(entry_state, "text")
            item.setForeground(1, QColor(THEMES[self.theme_name][token]))
        current_key = next((key for key in self.stage_items if key.split(":", 1)[1] == status.get("stage_name")), None)
        if current_key and (self.follow_stage.isChecked() or self.stage_tree.currentItem() is None):
            self.stage_tree.blockSignals(True)
            self.stage_tree.setCurrentItem(self.stage_items[current_key])
            self.stage_tree.blockSignals(False)
        self.refresh_stage_details()

    def select_monitor_stage(self, current=None, previous=None):
        if current is None:
            return
        self.follow_stage.blockSignals(True)
        self.follow_stage.setChecked(False)
        self.follow_stage.blockSignals(False)
        self.refresh_stage_details()

    def refresh_stage_details(self):
        """Present the selected stage and follow its durable log independently of status tails."""
        if self.view_run_root is None:
            return
        status = self.view_status
        item = self.stage_tree.currentItem()
        selected_key = item.data(0, Qt.ItemDataRole.UserRole) if item else None
        selected_phase, selected_name = selected_key.split(":", 1) if selected_key else (status.get("phase", ""), status.get("stage_name", ""))
        stage_result = next((record for record in status.get("stage_history", [])
                             if record.get("phase") == selected_phase and record.get("name") == selected_name), {})
        stage_status = status if selected_name == status.get("stage_name") else stage_result
        if not selected_key:
            stage_status = status
        scopes = stage_status.get("progress_scopes") or {}
        current_scope = scopes.get(stage_status.get("current_scope_id"), {})
        self.progress_tree.clear()
        for scope_id, scope in scopes.items():
            phase_title = PHASE_LABELS.get(scope.get("phase"), scope.get("phase") or scope.get("operation") or "步骤")
            objects = " · ".join(f"{OBJECT_LABELS.get(key, key)} {value}" for key, value in (scope.get("object") or {}).items())
            counters = []
            for counter in (scope.get("counters") or {}).values():
                completed, total, unit = counter.get("completed", 0), counter.get("total"), counter.get("unit", "项")
                counters.append(f"{completed} / {total} {unit} · 剩余 {max(0, total - completed)}" if total is not None else f"已处理 {completed} {unit} · 总量待确认")
            scope_state = scope.get("state", "running")
            scope_state_label = {"started": "开始", "running": "进行中", "completed": "步骤完成", "failed": "失败", "skipped": "跳过"}.get(scope_state, scope_state)
            if stage_status.get("state") in worker.TERMINAL_STATES and scope_state in {"started", "running"}:
                scope_state_label = "末次记录" if stage_status.get("state") == "succeeded" else "未完成"
            row = QTreeWidgetItem(self.progress_tree, [phase_title + (" · " + objects if objects else ""),
                scope_state_label,
                "；".join(counters)])
            row.setToolTip(0, scope.get("raw_line") or json.dumps(scope, ensure_ascii=False))
            row.setForeground(1, QColor(THEMES[self.theme_name]["danger" if scope_state == "failed" else "text"]))
            if scope_id == stage_status.get("current_scope_id"):
                self.progress_tree.setCurrentItem(row)
        if current_scope:
            phase_title = PHASE_LABELS.get(current_scope.get("phase"), current_scope.get("phase") or "当前步骤")
            objects = " · ".join(f"{OBJECT_LABELS.get(key, key)}：{value}" for key, value in (current_scope.get("object") or {}).items())
            finish = current_scope.get("updated_at") if current_scope.get("state") in {"completed", "failed", "skipped"} else stage_status.get("finished_at")
            self.activity_label.setText(f"{phase_title} · 步骤耗时 {elapsed_text(current_scope.get('started_at'), finish)}\n{objects}")
            if stage_status.get("progress_scopes_omitted"):
                self.activity_label.setText(self.activity_label.text() + "\n较早步骤已收纳到 progress.jsonl；当前显示有界步骤摘要。")
            if stage_status is status:
                self.progress_label.setText(f"当前阶段：{ENTRY_LABELS.get(selected_name.split('/')[-1], selected_name)}\n当前步骤：{phase_title}")
            counter = next((value for value in (current_scope.get("counters") or {}).values() if value.get("total") is not None), None)
            if counter is not None:
                completed, total = counter["completed"], counter["total"]
                self.activity_progress.setRange(0, 1000)
                self.activity_progress.setValue(round(1000 * completed / total) if total else 1000)
                scope_label = "局部步骤" if current_scope.get("counter_scope") == "local_step" else "当前范围"
                self.activity_progress.setFormat(f"{scope_label}：{completed} / {total} {counter['unit']} · 剩余 {max(0, total - completed)}")
            else:
                running = stage_status.get("state") not in worker.TERMINAL_STATES and current_scope.get("state") in {"running", "started"}
                self.activity_progress.setRange(0, 0 if running else 1)
                self.activity_progress.setValue(0)
                self.activity_progress.setFormat("本步骤未提供计数")
        else:
            self.activity_label.setText("当前环节尚未提供可解析的步骤计数；下方显示原始日志。" if stage_status else "此阶段尚未执行，因此没有日志。")
            self.activity_progress.setRange(0, 1)
            self.activity_progress.setValue(0)
            self.activity_progress.setFormat("无计数记录")
        log_path = None
        candidate = stage_status.get("log_path")
        if candidate:
            candidate_path = pathlib.Path(candidate).resolve()
            if candidate_path.is_relative_to(self.view_run_root.resolve()) and candidate_path.is_file():
                log_path = candidate_path
        if log_path is None and selected_name:
            safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", selected_name)
            log_path = next((self.view_run_root / "logs").glob(f"{selected_phase}_*_{safe_name}.log"), None)
        key = (str(self.view_run_root), selected_key, str(log_path))
        if key != self.log_key:
            self.log_key, self.log_offset = key, None
            self.log_decoder.reset()
            self.logs.clear()
        if log_path is None:
            self.log_location.setText("无阶段日志文件；以下为旧状态中的输出摘要。" if stage_status else "此阶段尚未执行。")
            self.logs.setPlainText("\n".join(stage_status.get("recent_lines") or []))
            return
        self.log_location.setText(f"{log_path.name} · 增量跟随 · 显示最近 4,000 行，完整记录保留在文件")
        if self.log_future is not None and self.log_future[1].done():
            future_key, future = self.log_future
            self.log_future = None
            try:
                offset, content, reset = future.result()
                if future_key == key:
                    if reset:
                        self.logs.clear()
                        self.log_decoder.reset()
                    self.log_offset = offset
                    text = self.log_decoder.decode(content)
                    if text:
                        scroll_position = self.logs.verticalScrollBar().value()
                        cursor = self.logs.textCursor()
                        cursor.movePosition(QTextCursor.MoveOperation.End)
                        cursor.insertText(text)
                        if self.follow_log.isChecked():
                            self.logs.verticalScrollBar().setValue(self.logs.verticalScrollBar().maximum())
                        else:
                            self.logs.verticalScrollBar().setValue(scroll_position)
            except OSError as error:
                if future_key == key:
                    self.log_location.setText(f"日志读取失败：{error}")
        if self.log_future is None:
            self.log_future = (key, self.io_pool.submit(read_log_chunk, log_path, self.log_offset))

    def find_log_text(self):
        self.follow_log.setChecked(False)
        if not self.logs.find(self.log_search.text()):
            self.logs.moveCursor(QTextCursor.MoveOperation.Start)
            self.logs.find(self.log_search.text())

    def refresh_history(self):
        if self.history_future is None:
            self.history_button.setEnabled(False)
            self.history_status_label.setText("正在读取历史…")
            self.history_future = self.io_pool.submit(read_history)
            self.loading_timer.start()

    def open_history(self, *args):
        item = self.history_tree.currentItem()
        if item is not None:
            self.view_run_root = item.data(0, Qt.ItemDataRole.UserRole)
            self.tabs.setCurrentWidget(self.monitor_tab)
            self.refresh_monitor()

    def view_current(self):
        if self.active_run_root is not None and self.view_run_root != self.active_run_root:
            self.view_run_root = self.active_run_root
            self.tabs.setCurrentWidget(self.monitor_tab)
            self.refresh_monitor()

    def open_run_folder(self):
        if self.view_run_root is not None and self.view_run_root.is_dir():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(self.view_run_root)))

    def show_stage_log(self):
        item = self.stage_tree.currentItem()
        if item is None or self.view_run_root is None:
            QMessageBox.information(self, "尚无阶段", "批次尚未发布阶段；启动后会自动选中当前环节。")
            return
        phase, name = item.data(0, Qt.ItemDataRole.UserRole).split(":", 1)
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name)
        paths = list((self.view_run_root / "logs").glob(f"{phase}_*_{safe_name}.log"))
        if not paths:
            QMessageBox.information(self, "尚无日志", "这个阶段没有日志文件。")
            return
        dialog = QDialog(self)
        dialog.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose)
        dialog.setWindowTitle(name + " · 日志末尾 256 KiB")
        dialog.resize(min(1040, self.width()), min(640, self.height()))
        layout = QVBoxLayout(dialog)
        text = QPlainTextEdit()
        text.setReadOnly(True)
        layout.addWidget(text)
        try:
            with paths[0].open("rb") as stream:
                stream.seek(max(0, paths[0].stat().st_size - 262144))
                text.setPlainText(stream.read().decode("utf-8", errors="replace"))
        except OSError as error:
            text.setPlainText(f"日志读取失败：{error}")
        dialog.show()

    def closeEvent(self, event):
        if self.worker_process is not None and self.worker_process.poll() is None:
            event.ignore()
            if not self.close_after_stop and QMessageBox.question(
                self, "批次仍在运行", "请求中断本批次，并在后台停止后关闭窗口？",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            ) == QMessageBox.StandardButton.Yes:
                self.interrupt()
                if (self.active_run_root / "interrupt.request").exists():
                    self.close_after_stop = True
            return
        self.monitor_timer.stop()
        self.loading_timer.stop()
        self.io_pool.shutdown(wait=False, cancel_futures=True)
        self.trade_table_pool.shutdown(wait=False, cancel_futures=True)
        event.accept()


def main(argv=None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments[:1] == ["--execute-tool"] and len(arguments) == 3:
        tool_id, expected_sha256 = arguments[1:]
        if tool_id not in MAINTENANCE_TOOLS:
            raise ValueError("未知维护工具。")
        _, path, tool_arguments, _ = MAINTENANCE_TOOLS[tool_id]
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha256:
            raise RuntimeError("维护工具源码已变化，本次不执行。请重新发起检查。")
        sys.argv = [str(path), *tool_arguments]
        runpy.run_path(str(path), run_name="__main__")
        return 0
    if arguments[:1] == ["--execute-request"] and len(arguments) == 2:
        request_path = pathlib.Path(arguments[1]).resolve()
        run_root = worker.validate_run_root(request_path.parent)
        if request_path.name != "request.json":
            raise ValueError("内部 worker 仅接受本批 request.json。")
        worker.ensure_fresh_or_monitor_only_run_root(run_root)
        try:
            request = worker.read_json_shared(request_path)
            stages = stages_from_request(request)
        except Exception as error:
            return worker.record_bootstrap_terminal_state(
                operation_name="interactive_collection", run_root=run_root, status_path=run_root / "status.json",
                stage_total=0, state="failed", stage_name="批次参数与源码核对", progress="request=invalid", error=str(error),
            )
        maintenance = request.get("kind") == "maintenance"
        return worker.run_batch(operation_name="interactive_maintenance" if maintenance else "interactive_collection",
                                run_root=run_root, stages=stages, preflight_stages=() if maintenance else None)
    if arguments:
        raise ValueError("直接运行 console.py 打开窗口；业务参数请在对应环节中填写。")
    if pathlib.Path(sys.prefix).name.casefold() != "latitude_env_v2":
        raise RuntimeError("请使用 latitude_env_v2 环境打开总控台。")
    if os.name == "nt":
        # Give the window its own taskbar identity instead of Python's icon.
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("LatitudeAnalytics.FuturesOperations")
    application = QApplication(arguments)
    application.setStyle("Fusion")
    application.setFont(QFont("Microsoft YaHei UI", 10))
    window = OperationsConsole()
    window.show()
    return application.exec()


if __name__ == "__main__":
    raise SystemExit(main())
