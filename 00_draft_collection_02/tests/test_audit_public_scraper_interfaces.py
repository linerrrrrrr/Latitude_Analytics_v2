from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest
from datetime import date
from types import SimpleNamespace
from unittest import mock


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE_PATH = (
    PROJECT_ROOT
    / "00_draft_collection_02"
    / "scripts"
    / "audit_public_scraper_interfaces.py"
)
SPEC = importlib.util.spec_from_file_location("audit_public_scraper_interfaces", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PublicScraperAuditTests(unittest.TestCase):
    def test_robots_crawl_delay_is_parsed(self) -> None:
        robots_text = "User-agent: audit-agent\nAllow: /\nCrawl-delay: 12\n"
        response = SimpleNamespace(
            status_code=200,
            headers={"Content-Type": "text/plain"},
            content=robots_text.encode(),
            text=robots_text,
        )
        session = SimpleNamespace(get=lambda *args, **kwargs: response)

        evidence = MODULE.inspect_robots(
            session,
            "https://example.com/data",
            "audit-agent",
            30.0,
        )

        self.assertTrue(evidence["can_fetch"])
        self.assertEqual(evidence["crawl_delay_seconds"], 12)

    def test_same_host_wait_honors_larger_crawl_delay(self) -> None:
        with (
            mock.patch.object(MODULE.random, "uniform", return_value=0.0),
            mock.patch.object(MODULE.time, "monotonic", return_value=105.0),
            mock.patch.object(MODULE.time, "sleep") as sleep,
        ):
            MODULE.wait_for_host_request_slot(
                "example.com",
                {"example.com": 100.0},
                min_interval_seconds=5.0,
                jitter_seconds=1.0,
                crawl_delay_seconds_by_host={"example.com": 12.0},
            )

        sleep.assert_called_once_with(7.0)

    def test_non_text_robots_response_does_not_grant_permission(self) -> None:
        response = SimpleNamespace(
            status_code=200,
            headers={"Content-Type": "application/json;charset=UTF-8"},
            content=b'{"message":"not found"}',
            text='{"message":"not found"}',
        )
        session = SimpleNamespace(get=lambda *args, **kwargs: response)

        evidence = MODULE.inspect_robots(
            session,
            "https://example.com/data",
            "audit-agent",
            30.0,
        )

        self.assertEqual(evidence["status"], "unexpected_content")
        self.assertIsNone(evidence["can_fetch"])
        self.assertEqual(MODULE.robots_policy_outcome(evidence), "warning")

    def test_sunsirs_expected_html_is_valid(self) -> None:
        rows = "".join(
            f'<tr><td><a href="/sf/{index}.html">品种{index}</a></td>'
            "<td>1</td><td>2609</td><td>1</td><td>0</td>"
            "<td>2610</td><td>1</td><td>0</td></tr>"
            for index in range(10)
        )
        html = f"""
        <html><head><title>2026年08月21日商品现货与期货价格对比表 - 生意社</title></head>
        <body>2026年08月21日 16:30 现期差=现货价格-期货价格
        <table id="fdata"><thead><tr><th>商品</th><th>现货价格</th>
        <th>最近合约</th><th>最近价格</th><th>最近现期差</th>
        <th>主力合约</th><th>主力价格</th><th>主力现期差</th></tr></thead>
        <tbody>{rows}</tbody></table></body></html>
        """.encode()

        inspection = MODULE.inspect_sunsirs_response(html, date(2026, 8, 21))

        self.assertTrue(inspection["diagnostic_valid"])
        self.assertEqual(inspection["diagnostic_status"], "passed")
        self.assertEqual(inspection["commodity_row_count"], 10)
        self.assertEqual(inspection["publication_time"], "16:30")
        self.assertEqual(inspection["null_like_direct_cell_count"], 0)

    def test_sunsirs_mostly_empty_business_table_is_rejected(self) -> None:
        rows = "".join(
            f'<tr><td><a href="/sf/{index}.html">品种{index}</a></td>'
            "<td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>"
            for index in range(10)
        )
        html = f"""
        <html><head><title>2026年08月21日商品现货与期货价格对比表 - 生意社</title></head>
        <body>2026年08月21日 16:30 现期差=现货价格-期货价格
        <table id="fdata"><tr><th>商品</th><th>现货价格</th><th>近月合约</th>
        <th>近月价格</th><th>近月现期差</th><th>主力合约</th>
        <th>主力价格</th><th>主力现期差</th></tr>{rows}</table></body></html>
        """.encode()

        inspection = MODULE.inspect_sunsirs_response(html, date(2026, 8, 21))

        self.assertEqual(inspection["diagnostic_status"], "failed")
        self.assertIn("business_cells_mostly_empty", inspection["diagnostic_reasons"])

    def test_sunsirs_block_page_is_rejected_even_with_http_like_html(self) -> None:
        html = "<html><title>安全验证</title><body>访问过于频繁 验证码</body></html>".encode()

        inspection = MODULE.inspect_sunsirs_response(html, date(2026, 8, 21))

        self.assertFalse(inspection["diagnostic_valid"])
        self.assertIn("访问过于频繁", inspection["blocked_terms"])

    def test_eastmoney_expected_json_is_valid(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {
                "pages": 1,
                "count": 2,
                "data": [
                    {
                        "INDICATOR_ID": "EMI00107664",
                        "INDICATOR_VALUE": 2000.0,
                        "REPORT_DATE": "2026-08-20 00:00:00",
                    },
                    {
                        "INDICATOR_ID": "EMI00107664",
                        "INDICATOR_VALUE": 2010.0,
                        "REPORT_DATE": "2026-08-21 00:00:00",
                    },
                ],
            },
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["INDICATOR_ID", "INDICATOR_VALUE", "REPORT_DATE"],
            date(2026, 8, 17),
            date(2026, 8, 21),
            expected_indicator_id="EMI00107664",
            expected_frequency="weekday_descriptive",
        )

        self.assertTrue(inspection["diagnostic_valid"])
        self.assertEqual(inspection["row_count"], 2)
        self.assertEqual(inspection["latest_report_date"], "2026-08-21")
        self.assertEqual(
            inspection["numeric_field_profiles"]["INDICATOR_VALUE"],
            {
                "non_null_count": 2,
                "unique_value_count": 2,
                "adjacent_change_count": 1,
                "longest_equal_run": 1,
            },
        )

    def test_eastmoney_missing_field_is_rejected(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {
                "pages": 1,
                "count": 1,
                "data": [{"REPORT_DATE": "2026-08-21 00:00:00"}],
            },
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["REPORT_DATE", "VALUE"],
            date(2026, 8, 1),
            date(2026, 8, 21),
        )

        self.assertFalse(inspection["diagnostic_valid"])
        self.assertEqual(inspection["missing_field_counts"], {"VALUE": 1})

    def test_eastmoney_non_finite_numeric_value_is_rejected(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {
                "pages": 1,
                "count": 1,
                "data": [{
                    "REPORT_DATE": "2026-08-21 00:00:00",
                    "VALUE": "NaN",
                }],
            },
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["REPORT_DATE", "VALUE"],
            date(2026, 8, 1),
            date(2026, 8, 21),
        )

        self.assertFalse(inspection["diagnostic_valid"])
        self.assertEqual(inspection["invalid_numeric_counts"], {"VALUE": 1})

    def test_eastmoney_documented_empty_envelope_is_warning(self) -> None:
        payload = {
            "success": False,
            "code": 9201,
            "message": "返回数据为空",
            "result": None,
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload, ensure_ascii=False).encode(),
            ["REPORT_DATE", "VALUE"],
            date(2026, 8, 1),
            date(2026, 8, 21),
        )

        self.assertIsNone(inspection["diagnostic_valid"])
        self.assertEqual(inspection["diagnostic_status"], "warning")
        self.assertEqual(inspection["response_kind"], "empty_confirmed")

    def test_eastmoney_wrong_indicator_id_is_rejected(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {
                "pages": 1,
                "count": 1,
                "data": [{
                    "INDICATOR_ID": "WRONG",
                    "INDICATOR_VALUE": 100.0,
                    "REPORT_DATE": "2026-08-21 00:00:00",
                }],
            },
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["INDICATOR_ID", "INDICATOR_VALUE", "REPORT_DATE"],
            date(2026, 8, 1),
            date(2026, 8, 21),
            expected_indicator_id="EMI00107664",
        )

        self.assertEqual(inspection["diagnostic_status"], "failed")
        self.assertEqual(inspection["unexpected_indicator_id_count"], 1)

    def test_eastmoney_boundary_count_with_empty_page_is_rejected(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {"pages": 20, "count": 20, "data": []},
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["INDICATOR_ID", "INDICATOR_VALUE", "REPORT_DATE"],
            date(1900, 1, 1),
            date(9999, 12, 31),
            require_complete_result=False,
            expected_indicator_id="EMI00135906",
        )

        self.assertEqual(inspection["diagnostic_status"], "failed")
        self.assertIn(
            "incomplete_or_inconsistent_page",
            inspection["diagnostic_reasons"],
        )

    def test_eastmoney_single_boundary_row_can_validate_partial_metadata(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {
                "pages": 20,
                "count": 20,
                "data": [{
                    "INDICATOR_ID": "EMI00135906",
                    "INDICATOR_VALUE": 100.0,
                    "REPORT_DATE": "2012-01-01 00:00:00",
                }],
            },
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["INDICATOR_ID", "INDICATOR_VALUE", "REPORT_DATE"],
            date(1900, 1, 1),
            date(9999, 12, 31),
            require_complete_result=False,
            expected_indicator_id="EMI00135906",
        )

        self.assertTrue(inspection["diagnostic_valid"])
        self.assertEqual(inspection["response_kind"], "expected_json_partial")
        self.assertEqual(inspection["reported_count"], 20)

    def test_macro_missing_period_is_reported_as_bounded_warning(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {
                "pages": 1,
                "count": 2,
                "data": [
                    {"REPORT_DATE": "2026-01-01", "VALUE": 1.0},
                    {"REPORT_DATE": "2026-03-01", "VALUE": 3.0},
                ],
            },
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["REPORT_DATE", "VALUE"],
            date(2026, 1, 1),
            date(2026, 3, 1),
            expected_frequency="month_end",
        )

        self.assertEqual(inspection["diagnostic_status"], "warning")
        self.assertEqual(inspection["missing_observation_ranges"]["missing_count"], 1)
        self.assertEqual(
            inspection["missing_observation_ranges"]["ranges"],
            [{
                "start": "2026-02-01",
                "end": "2026-02-01",
                "expected_observation_count": 1,
            }],
        )

    def test_macro_period_after_latest_observation_is_unassessed_not_missing(self) -> None:
        payload = {
            "success": True,
            "code": 0,
            "message": "ok",
            "result": {
                "pages": 1,
                "count": 2,
                "data": [
                    {"REPORT_DATE": "2026-01-01", "VALUE": 1.0},
                    {"REPORT_DATE": "2026-02-01", "VALUE": 2.0},
                ],
            },
        }

        inspection = MODULE.inspect_eastmoney_response(
            json.dumps(payload).encode(),
            ["REPORT_DATE", "VALUE"],
            date(2026, 1, 1),
            date(2026, 3, 31),
            expected_frequency="month_end",
        )

        self.assertEqual(inspection["diagnostic_status"], "passed")
        self.assertEqual(inspection["missing_observation_ranges"]["missing_count"], 0)
        self.assertEqual(inspection["unassessed_pending_tail"]["missing_count"], 1)
        self.assertEqual(
            inspection["unassessed_pending_tail"]["ranges"][0]["start"],
            "2026-03-01",
        )

    def test_evidence_write_is_atomic_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_path = pathlib.Path(directory) / "evidence.json"
            MODULE.write_json_exclusive_atomic(output_path, {"status": "first"})

            with self.assertRaises(FileExistsError):
                MODULE.write_json_exclusive_atomic(output_path, {"status": "second"})

            self.assertEqual(
                json.loads(output_path.read_text(encoding="utf-8")),
                {"status": "first"},
            )

    def test_manifest_uses_authoritative_entity_counts(self) -> None:
        jobs = MODULE.build_probe_jobs(
            mode="coverage",
            observation_date=date(2026, 8, 21),
            index_lookback_days=120,
            macro_lookback_days=800,
        )

        family_counts = {}
        for job in jobs:
            family_counts[job["family"]] = family_counts.get(job["family"], 0) + 1
        self.assertEqual(family_counts["sunsirs_html"], 5)
        self.assertEqual(
            family_counts["eastmoney_industry_index_json"],
            len(MODULE.EXTERNAL_INDEX_ENTITIES),
        )
        self.assertEqual(
            family_counts["eastmoney_macro_json"],
            len(MODULE.MACRO_FIELDS_BY_REPORT),
        )

    def test_boundary_manifest_can_target_one_configured_index(self) -> None:
        jobs = MODULE.build_probe_jobs(
            mode="coverage",
            observation_date=date(2026, 8, 21),
            index_lookback_days=120,
            macro_lookback_days=800,
            only_family="index",
            index_code="MYSTEEL_TIN",
            index_boundary="boundaries",
        )

        self.assertEqual([job["name"] for job in jobs], [
            "eastmoney_index_MYSTEEL_TIN_earliest",
            "eastmoney_index_MYSTEEL_TIN_latest",
        ])
        self.assertTrue(all(job["params"]["pageSize"] == 1 for job in jobs))


if __name__ == "__main__":
    unittest.main()
