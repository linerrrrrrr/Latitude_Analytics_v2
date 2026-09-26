"""验证 c03 定向刷新例外与其他采集入口的正式湖日期禁写门禁。

测试把临时目录伪装成“正式湖”，因此即使门禁回归也不会触碰真实正式湖；
其他入口的外部访问函数使用失败哨兵，证明门禁发生在任何网络调用之前；
c03 则必须越过门禁并开始强制拉取。
"""

from __future__ import annotations

import importlib.util
import pathlib
import shutil
import tempfile
from contextlib import ExitStack
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.dataset as ds
from click.testing import CliRunner


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
COLLECTION_ROOT = PROJECT_ROOT / "02_Futures_Lakehouse"

ENTRYPOINTS = (
    COLLECTION_ROOT / "a01_Futures_Market_Data" / "b01_trade_calendar.py",
    COLLECTION_ROOT / "a01_Futures_Market_Data" / "b02_futures_variety_calendar.py",
    COLLECTION_ROOT / "a01_Futures_Market_Data" / "b04_futures_bar_calendar.py",
    COLLECTION_ROOT / "a01_Futures_Market_Data" / "b05_futures_daily.py",
    COLLECTION_ROOT / "a01_Futures_Market_Data" / "b06_futures_minute.py",
    COLLECTION_ROOT / "a01_Futures_Market_Data" / "b07_suspected_session_reconciliation.py",
    COLLECTION_ROOT / "a02_Futures_Exchange_Reports" / "b01_exchange_report_calendar.py",
    COLLECTION_ROOT / "a02_Futures_Exchange_Reports" / "b02_futures_holding_reports.py",
    COLLECTION_ROOT / "a02_Futures_Exchange_Reports" / "b03_warehouse_receipt.py",
    COLLECTION_ROOT / "a03_External_Market_Data" / "b01_external_market_calendar.py",
    COLLECTION_ROOT / "a03_External_Market_Data" / "b02_domestic_spot_basis.py",
    COLLECTION_ROOT / "a03_External_Market_Data" / "b03_overseas_futures.py",
    COLLECTION_ROOT / "a03_External_Market_Data" / "b04_external_index.py",
)

C03_ENTRYPOINT = (
    COLLECTION_ROOT
    / "a01_Futures_Market_Data"
    / "b03_futures_contract_calendar.py"
)


class ExpectedC03SourceAccess(RuntimeError):
    """证明 c03 正式日期写已越过 CLI 门禁并开始执行强制拉取。"""


def forbidden_external_access(*args: object, **kwargs: object) -> None:
    del args, kwargs
    raise AssertionError("正式湖禁写门禁之前不应访问外部数据源。")


def expected_c03_source_access(*args: object, **kwargs: object) -> None:
    del args, kwargs
    raise ExpectedC03SourceAccess


def main() -> None:
    runner = CliRunner()
    failures: list[str] = []

    with tempfile.TemporaryDirectory(
        prefix="formal-gate-",
        dir=PROJECT_ROOT / "00_draft_collection_02",
    ) as temporary_directory:
        fake_formal_root = pathlib.Path(temporary_directory).resolve()

        for index, entrypoint in enumerate(ENTRYPOINTS):
            module_name = f"formal_gate_{index}_{entrypoint.stem}"
            spec = importlib.util.spec_from_file_location(module_name, entrypoint)
            if spec is None or spec.loader is None:
                failures.append(f"{entrypoint.name}: 无法创建导入规格")
                continue

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.settings = SimpleNamespace(futures_lake_root=fake_formal_root)

            for name in (
                "authenticate_jqdata",
                "create_eastmoney_session",
                "collect",
                "collect_source_data",
                "collect_batch",
                "collect_partition",
            ):
                if hasattr(module, name):
                    setattr(module, name, forbidden_external_access)

            result = runner.invoke(
                module.main,
                (
                    "--lake-root",
                    str(fake_formal_root),
                    "--start-date",
                    "2026-07-17",
                    "--end-date",
                    "2026-08-14",
                    "--write",
                ),
            )
            output = result.output.replace("\n", " ").strip()
            passed = result.exit_code == 2 and "正式" in output and "日期" in output
            print(
                f"{entrypoint.parent.name}/{entrypoint.name}: "
                f"exit={result.exit_code}; passed={str(passed).lower()}; output={output}"
            )
            if not passed:
                failures.append(
                    f"{entrypoint}: 未在外部访问前以 UsageError 拒绝，"
                    f"exit={result.exit_code}; exception={result.exception!r}"
                )

            # 日期范围必须成对出现；只给起点也必须在读取湖仓和联网之前失败。
            pair_result = runner.invoke(
                module.main,
                (
                    "--lake-root",
                    str(fake_formal_root),
                    "--start-date",
                    "2026-07-17",
                ),
            )
            # Click 的 UsageError 固定返回 2；若越过门禁，外部访问哨兵会让结果变成 1。
            pair_passed = pair_result.exit_code == 2
            print(
                f"{entrypoint.parent.name}/{entrypoint.name}: "
                f"paired_dates_exit={pair_result.exit_code}; "
                f"passed={str(pair_passed).lower()}"
            )
            if not pair_passed:
                failures.append(
                    f"{entrypoint}: 单边日期未在外部访问前以 UsageError 拒绝，"
                    f"exit={pair_result.exit_code}; exception={pair_result.exception!r}"
                )

        # c03 是唯一允许定向刷新正式湖的例外。写入一行可信 c02 后，用来源访问
        # 哨兵证明命令越过了 CLI 门禁，但在测试中仍不会真的联网或写 c03。
        spec = importlib.util.spec_from_file_location(
            "formal_gate_c03_allowed_refresh",
            C03_ENTRYPOINT,
        )
        if spec is None or spec.loader is None:
            failures.append("b03_futures_contract_calendar.py: 无法创建导入规格")
        else:
            c03_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(c03_module)
            c03_module.settings = SimpleNamespace(
                futures_lake_root=fake_formal_root,
                jqdata_id="test-id",
                jqdata_secret="test-secret",
            )
            upstream_path = (
                fake_formal_root
                / "silver"
                / c03_module.UPSTREAM_TABLE_NAME
            )
            upstream_table = pa.Table.from_pylist(
                [
                    {
                        "underlying_code": "RB",
                        "exchange_code": "XSGE",
                        "trading_date": date(2026, 7, 17),
                        "active_contract_count": 1,
                        "source": "trusted-test-upstream",
                        "updated_at": datetime(2026, 8, 22, tzinfo=timezone.utc),
                        "year": 2026,
                        "month": 7,
                    }
                ],
                schema=c03_module.FUTURES_VARIETY_CALENDAR_SCHEMA,
            )
            ds.write_dataset(
                upstream_table,
                upstream_path,
                format="parquet",
                partitioning=c03_module.UPSTREAM_PARTITIONING,
                existing_data_behavior="delete_matching",
                basename_template="part-{i}.parquet",
            )

            with ExitStack() as stack:
                stack.enter_context(
                    patch(
                        "config.jqdata_connection.authenticate_jqdata",
                        side_effect=expected_c03_source_access,
                    )
                )
                if hasattr(c03_module, "authenticate_jqdata"):
                    stack.enter_context(
                        patch.object(
                            c03_module,
                            "authenticate_jqdata",
                            side_effect=expected_c03_source_access,
                        )
                    )
                c03_result = runner.invoke(
                    c03_module.main,
                    (
                        "--lake-root",
                        str(fake_formal_root),
                        "--start-date",
                        "2026-07-17",
                        "--end-date",
                        "2026-07-17",
                        "--write",
                    ),
                )

            c03_passed = isinstance(
                c03_result.exception,
                ExpectedC03SourceAccess,
            )
            print(
                "a01_Futures_Market_Data/b03_futures_contract_calendar.py: "
                f"formal_refresh_reached_source={str(c03_passed).lower()}; "
                f"exit={c03_result.exit_code}"
            )
            if not c03_passed:
                failures.append(
                    "b03_futures_contract_calendar.py: 正式日期写未越过 CLI 门禁；"
                    f"exit={c03_result.exit_code}; exception={c03_result.exception!r}"
                )
            shutil.rmtree(upstream_path)

        # c08 不接受日期范围，而是要求操作员显式确认不可裁剪的全量质检。
        full_quality_entrypoint = (
            COLLECTION_ROOT
            / "a01_Futures_Market_Data"
            / "b08_full_minute_quality.py"
        )
        spec = importlib.util.spec_from_file_location(
            "full_quality_confirmation_gate",
            full_quality_entrypoint,
        )
        if spec is None or spec.loader is None:
            failures.append("b08_full_minute_quality.py: 无法创建导入规格")
        else:
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            result = runner.invoke(
                module.main,
                ("--lake-root", str(fake_formal_root)),
            )
            passed = result.exit_code == 2
            print(
                "a01_Futures_Market_Data/b08_full_minute_quality.py: "
                f"confirmation_exit={result.exit_code}; passed={str(passed).lower()}"
            )
            if not passed:
                failures.append(
                    "b08_full_minute_quality.py: 缺少人工确认时没有以 UsageError 拒绝"
                )

        residual_files = [path for path in fake_formal_root.rglob("*") if path.is_file()]
        if residual_files:
            failures.append(f"伪正式湖产生了文件：{residual_files}")

    if failures:
        raise SystemExit("\n".join(failures))


if __name__ == "__main__":
    main()
