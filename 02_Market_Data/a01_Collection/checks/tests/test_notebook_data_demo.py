"""本地合成湖验证：只读样例、多实体筛选、读取上限与空库。"""
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
import pathlib
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import ipykernel

project_markers = [".git", ".env", "config/settings.py"]
current_path = Path.cwd().resolve()
for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        ROOT = candidate_root
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "02_Market_Data/a01_Collection"))
import b00_03_notebook_schema_browser as browser
from config import data_contracts
from config.futures_lakehouse.futures_position_rank_special_cases import POSITION_RANK_SPECIAL_CASES

SCHEMAS = [value for value in vars(data_contracts).values() if isinstance(value, pa.Schema)]


def sample_row(schema, **overrides):
    row = {}
    for field in schema:
        kind = field.type
        if pa.types.is_string(kind):
            value = "sample"
        elif pa.types.is_boolean(kind):
            value = True
        elif pa.types.is_integer(kind):
            value = 1
        elif pa.types.is_floating(kind):
            value = 12.5
        elif pa.types.is_date(kind):
            value = date(2026, 9, 25)
        elif pa.types.is_timestamp(kind):
            value = datetime(2026, 9, 25, 9, 1, tzinfo=ZoneInfo(kind.tz) if kind.tz else None)
        elif pa.types.is_time(kind):
            value = time(9, 0)
        else:
            raise AssertionError(kind)
        row[field.name] = value
    row.update({key: value for key, value in {
        "year": 2026, "month": 9, "exchange_code": "XSGE", "underlying_code": "RB",
        "contract_code": "RB2610.XSGE", "bar_frequency": "1m",
    }.items() if key in row})
    row.update(overrides)
    return row


def write_partition(root, schema, rows, filename="part.parquet", row_group_size=2):
    partition_names = schema.metadata[b"partition_columns"].decode().split(",")
    values = {name: rows[0][name] for name in partition_names}
    path = root / "silver" / schema.metadata[b"table_name"].decode()
    for name, value in values.items():
        path /= f"{name}={value}"
    path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table.drop(partition_names), path / filename, row_group_size=row_group_size)
    return path, values, table


class NotebookDataDemoTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=ROOT / "02_Market_Data/a01_Collection/checks/tests")
        self.root = Path(self.temp.name)
        self.widgets = []

    def tearDown(self):
        for root in self.widgets:
            pending = [root]
            while pending:
                widget = pending.pop()
                pending.extend(getattr(widget, "children", ()))
                for item in (getattr(widget, "layout", None), getattr(widget, "style", None), widget):
                    if item is not None:
                        item.close()
        for widget in [*browser._ACTIVE_WIDGETS, *browser._RAW_ACTIVE_WIDGETS]:
            widget.close()
        self.temp.cleanup()

    def demo(self, schema):
        widget = browser._TableDemo(schema, self.root)
        self.widgets.append(widget)
        return widget

    def test_metadata_only_does_not_touch_lake(self):
        with patch.object(browser.os, "scandir", side_effect=AssertionError("unexpected I/O")), patch.object(browser, "display"):
            browser.display_schema_metadata(SCHEMAS)

    def test_empty_lake_preserves_headers_without_creating_directories(self):
        for schema in SCHEMAS:
            widget = self.demo(schema)
            self.assertIn("尚未生成数据", widget.output.value)
            primary_key = schema.metadata[b"primary_key"].decode().split(",")
            self.assertTrue(set(primary_key).issubset(widget.key_columns))
            self.assertLessEqual(len(widget.key_columns), 8)
        self.assertFalse((self.root / "silver").exists())

    def test_all_17_schemas_reconstruct_partition_values_and_read_real_rows(self):
        for schema in SCHEMAS:
            with self.subTest(table=schema.metadata[b"table_name"]):
                path, values, expected = write_partition(self.root, schema, [sample_row(schema)])
                table, limited = browser.read_table_demo(schema, path, values, schema.names, {})
                self.assertEqual(table.to_pylist(), expected.to_pylist())
                self.assertFalse(limited)
                widget = self.demo(schema)
                self.assertIn("显示 1 行", widget.output.value)

    def test_variety_date_and_all_column_switches_are_bounded(self):
        schema = data_contracts.FUTURES_DAILY_SCHEMA
        for variety in ("CU", "RB"):
            rows = [sample_row(schema, underlying_code=variety, contract_code=f"{variety}2610.XSGE", trading_date=date(2026, 9, day)) for day in (24, 25)]
            write_partition(self.root, schema, rows)
        widget = self.demo(schema)
        self.assertIn("CU2610.XSGE", widget.output.value)
        widget.partition_selectors[1].value = "RB"
        self.assertIn("RB2610.XSGE", widget.output.value)
        self.assertNotIn("CU2610.XSGE", widget.output.value)
        self.assertIn("2026-09-25", widget.output.value)
        widget.date_mode.value = True
        widget.date_picker.value = date(2026, 9, 24)
        self.assertIn("2026-09-24", widget.output.value)
        widget.all_columns.value = True
        self.assertIn("22 / 22 个字段", widget.output.value)
        self.assertIn("previous_settlement", widget.output.value)
        widget.filter_selectors[0].value = "NO_SUCH_CONTRACT"
        self.assertIn("当前选择没有记录", widget.output.value)

    def test_entities_inside_same_partition_and_refresh(self):
        schema = data_contracts.INTEREST_RATE_DAILY_SCHEMA
        rows = [sample_row(schema, series_code=series, observation_date=date(2026, 9, day)) for series in ("ON", "1W") for day in (24, 25)]
        path, values, _ = write_partition(self.root, schema, rows)
        widget = self.demo(schema)
        widget.filter_selectors[0].value = "ON"
        self.assertIn("ON", widget.output.value)
        widget.date_mode.value = True
        widget.date_picker.value = date(2026, 9, 24)
        self.assertIn("2026-09-24", widget.output.value)
        write_partition(self.root, schema, rows + [sample_row(schema, series_code="ON", observation_date=date(2026, 9, 26))])
        widget.refresh.click()
        self.assertIn("2026-09-26", widget.candidates["observation_date"].tolist())
        self.assertEqual(widget.date_picker.value, date(2026, 9, 24))
        table, _ = browser.read_table_demo(schema, path, values, schema.names, {}, max_rows=5)
        self.assertLessEqual(table.num_rows, 5)

    def test_variety_and_contract_dropdowns_cascade_and_allow_manual_input(self):
        schema = data_contracts.FUTURES_BAR_CALENDAR_SCHEMA
        rows = [
            sample_row(schema, underlying_code=variety, contract_code=f"{variety}{month}.XSGE")
            for variety in ("CU", "RB") for month in ("2610", "2611")
        ]
        write_partition(self.root, schema, rows)
        widget = self.demo(schema)
        variety_dropdown, contract_dropdown = widget.filter_dropdowns[:2]
        self.assertEqual([value for _, value in variety_dropdown.options], ["CU", "RB", None])
        variety_dropdown.value = "RB"
        self.assertEqual([value for _, value in contract_dropdown.options], ["RB2610.XSGE", "RB2611.XSGE", None])
        contract_dropdown.value = "RB2611.XSGE"
        self.assertIn("RB2611.XSGE", widget.output.value)
        self.assertNotIn("RB2610.XSGE", widget.output.value)
        self.assertNotIn("CU2610.XSGE", widget.output.value)
        contract_dropdown.value = None
        self.assertEqual(widget.filter_selectors[1].layout.display, "")
        widget.filter_selectors[1].value = "RB2699.XSGE"
        self.assertIn("当前选择没有记录", widget.output.value)
        widget.refresh.click()
        self.assertEqual(widget.filter_selectors[1].value, "RB2699.XSGE")
        self.assertIsNone(contract_dropdown.value)
        variety_dropdown.value = "CU"
        self.assertEqual(contract_dropdown.value, "CU2610.XSGE")
        self.assertEqual(widget.filter_selectors[1].layout.display, "none")
        self.assertIn("CU2610.XSGE", widget.output.value)

    def test_column_toggle_reuses_rows_and_refresh_invalidates_cache(self):
        schema = data_contracts.FUTURES_DAILY_SCHEMA
        rows = [sample_row(schema, trading_date=date(2026, 9, day)) for day in (25, 21, 24)]
        write_partition(self.root, schema, rows)
        with patch.object(browser, "read_table_demo", wraps=browser.read_table_demo) as read:
            widget = self.demo(schema)
            self.assertEqual(read.call_count, 2)  # 窄列候选 + 有界样例。
            widget.all_columns.value = True
            self.assertEqual(read.call_count, 3)
            widget.all_columns.value = False
            widget.all_columns.value = True
            self.assertEqual(read.call_count, 3)
            self.assertLessEqual(widget.preview_cache[1].num_rows, 100)
            widget.refresh.click()
            self.assertEqual(read.call_count, 5)
            self.assertLess(widget.output.value.index("2026-09-21"), widget.output.value.index("2026-09-25"))
            widget.date_mode.value = True
            reads_before = read.call_count
            widget.date_picker.value = date(2026, 9, 24)
            self.assertEqual(read.call_count, reads_before + 1)
            self.assertEqual(widget.preview_cache[1]["trading_date"].to_pylist(), [date(2026, 9, 24)])

    def test_corruption_and_permission_failures_are_not_empty_lakes(self):
        schema = data_contracts.INTEREST_RATE_DAILY_SCHEMA
        path, _, _ = write_partition(self.root, schema, [sample_row(schema)])
        (path / "part.parquet").write_bytes(b"not parquet")
        self.assertIn("读取失败", self.demo(schema).output.value)
        pq.write_table(pa.table({"wrong_field": [1]}), path / "part.parquet")
        self.assertIn("与权威 Schema 不兼容", self.demo(schema).output.value)
        with patch.object(browser.os, "scandir", side_effect=PermissionError("denied")):
            self.assertIn("PermissionError", self.demo(schema).output.value)

    def test_row_limits_read_on_demand_and_reuse_larger_preview(self):
        schema = data_contracts.TRADE_CALENDAR_SCHEMA
        rows = [sample_row(schema, calendar_date=date(2026, 1, 1) + timedelta(days=day)) for day in range(150)]
        write_partition(self.root, schema, rows)
        with patch.object(browser, "read_table_demo", wraps=browser.read_table_demo) as read:
            widget = self.demo(schema)
            self.assertEqual(widget.row_limit.value, 10)
            self.assertEqual(widget.output.value.count("<tr>"), 10)
            self.assertEqual(read.call_count, 2)
            for limit in (20, 50, 100):
                widget.row_limit.value = limit
                self.assertEqual(widget.output.value.count("<tr>"), limit)
                self.assertEqual(read.call_args.kwargs["max_rows"], limit)
            self.assertEqual(read.call_count, 5)
            widget.row_limit.value = 10
            self.assertEqual(read.call_count, 5)
            self.assertEqual(widget.output.value.count("<tr>"), 10)
            self.assertEqual(widget.preview_cache[1].num_rows, 100)
            widget.all_columns.value = True
            self.assertEqual(widget.row_limit.value, 10)
            self.assertEqual(widget.output.value.count("<tr>"), 10)
            self.assertIn("11 / 11 个字段", widget.output.value)
            widget.date_mode.value = True
            widget.date_picker.value = date(2026, 1, 1)
            reads_before = read.call_count
            widget.row_limit.value = 100
            self.assertEqual(read.call_count, reads_before)
            self.assertEqual(widget.output.value.count("<tr>"), 1)

    def test_date_modes_filter_reset_and_keep_partition_scope(self):
        schema = data_contracts.TRADE_CALENDAR_SCHEMA
        write_partition(self.root, schema, [sample_row(schema, calendar_date=date(2026, 9, day)) for day in (24, 25)])
        write_partition(self.root, schema, [sample_row(schema, calendar_date=date(2025, 9, 24), year=2025)])
        widget = self.demo(schema)
        self.assertFalse(widget.date_mode.value)
        self.assertEqual(widget.date_picker.layout.display, "none")
        self.assertEqual(widget.preview_cache[1].num_rows, 2)
        widget.date_mode.value = True
        self.assertEqual(widget.date_picker.value, date(2026, 9, 25))
        self.assertEqual(widget.preview_cache[1].num_rows, 1)
        widget.date_picker.value = date(2026, 9, 23)
        self.assertIn("当前选择没有记录", widget.output.value)
        widget.date_mode.value = False
        self.assertIsNone(widget.date_picker.value)
        self.assertEqual(widget.preview_cache[1]["calendar_date"].to_pylist(), [date(2026, 9, 24), date(2026, 9, 25)])
        self.assertEqual(widget.partition_values, {"year": 2026})
        widget.set_schema(data_contracts.FUTURES_CONTRACT_CALENDAR_SCHEMA)
        self.assertTrue(widget.date_mode.disabled)
        self.assertIn("尚未生成数据", widget.output.value)

    def test_read_budget_is_reported_and_statistics_allow_other_entities(self):
        schema = data_contracts.INTEREST_RATE_DAILY_SCHEMA
        rows = [sample_row(schema, series_code=series) for series in ("A", "Z") for _ in range(10)]
        path, values, _ = write_partition(self.root, schema, rows, row_group_size=20)
        with patch.object(browser, "_DEMO_SCAN_ROWS", 3):
            table, limited = browser.read_table_demo(schema, path, values, schema.names, {"series_code": "M"}, max_rows=2)
            self.assertEqual(table.num_rows, 0)
            self.assertTrue(limited)
        write_partition(self.root, schema, rows, row_group_size=10)
        with patch.object(browser, "_DEMO_SCAN_ROWS", 3):
            table, _ = browser.read_table_demo(schema, path, values, schema.names, {"series_code": "Z"}, max_rows=2)
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(set(table["series_code"].to_pylist()), {"Z"})
        write_partition(self.root, schema, [sample_row(schema, series_code="Z")], filename="z.parquet")
        with patch.object(browser, "_DEMO_FILES", 1):
            table, limited = browser.read_table_demo(schema, path, values, schema.names, {"series_code": "A"})
            self.assertEqual(table.num_rows, 0)
            self.assertTrue(limited)

    def test_raw_preview_does_not_read_response_body(self):
        path = self.root / "raw/year=2026/month=09/observation_date=2026-09-25"
        path.mkdir(parents=True)
        (path / "response.html").write_bytes(b"NEVER_RENDER_OR_PARSE_THIS_HTML")
        (path / "response.sha256").write_text("a" * 64, encoding="ascii")
        original_open = Path.open
        opened = []
        def record_open(file_path, *args, **kwargs):
            opened.append(file_path.name)
            return original_open(file_path, *args, **kwargs)
        with patch.object(browser, "display") as display, patch.object(Path, "open", record_open):
            browser.display_raw_archive_demo(self.root / "raw", filenames=("response.html", "response.sha256"), partitions=("year", "month", "observation_date"))
            output = display.call_args.args[0].children[-1].value
        self.assertEqual(opened, ["response.sha256"])
        self.assertNotIn("NEVER_RENDER", output)
        self.assertIn("a" * 64, output)

    def test_all_notebook_opening_previews_work_on_empty_lake(self):
        namespace = {**vars(data_contracts), "sys": sys, "pathlib": pathlib,
                     "settings": SimpleNamespace(futures_lake_root=self.root),
                     "POSITION_RANK_SPECIAL_CASES": POSITION_RANK_SPECIAL_CASES,
                     "ARTIFACT_FILENAMES": {"response.dat", "response.sha256", "calibration.json"},
                     "RAW_RELATIVE_ROOT": Path("raw/100ppi/domestic_spot_basis"),
                     "RESPONSE_FILE_NAME": "response.html", "SHA256_FILE_NAME": "response.sha256"}
        namespace.pop("__file__", None)
        silver_calls = raw_calls = 0
        with patch.object(browser, "display"):
            for path in sorted((ROOT / "02_Market_Data/a01_Collection").glob("b*/c*.ipynb")):
                after = json.loads(path.read_text(encoding="utf-8"))
                for cell in after["cells"]:
                    source = "".join(cell["source"])
                    if cell["cell_type"] == "code" and any(marker in source for marker in ("display_schema_metadata(", "display_raw_archive_demo(")):
                        exec(compile(source, str(path), "exec"), namespace)
                        silver_calls += "display_schema_metadata(" in source
                        raw_calls += "display_raw_archive_demo(" in source
        self.assertEqual((silver_calls, raw_calls), (18, 2))
        self.assertFalse((self.root / "silver").exists())
        self.assertFalse((self.root / "raw").exists())


if __name__ == "__main__":
    unittest.main()
