"""金融期货项目库与人工传输协议的可执行契约。

本模块不读取数据、不连接数据库、不调用聚宽 API。它集中保存第 05 项已经
冻结、并会被规划 Notebook 与导入器共同使用的稳定标识和 DuckDB DDL。
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


DATABASE_SCHEMA_VERSION = "1.0.0"

TRANSFER_FORMAT_ID = "jq_financial_futures_transfer"
TRANSFER_PROTOCOL_VERSION = "1.0.0"
TRANSFER_FIXED_FILE_NAME = "JQ_FINANCIAL_FUTURES_TRANSFER.zip"
TRANSFER_GENERATOR_VERSION = "financial_futures_joinquant_export_v1"
TRANSFER_MARKET_TIME_ZONE = "Asia/Shanghai"
TRANSFER_SOURCE_API_MODULE = "jqresearch.api"
TRANSFER_MEMBER_NAMES = (
    "manifest.json",
    "request_blocks.jsonl",
    "futures_daily.jsonl",
    "futures_minute.jsonl",
)

TRANSFER_SOURCE_PARAMETERS = {
    "skip_paused": True,
    "fq": None,
    "panel": False,
    "fill_paused": False,
    "round": False,
}
TRANSFER_MANIFEST_KEYS = (
    "format_id", "protocol_version", "fixed_file_name", "generator_version",
    "planner_version", "plan_sha256", "run_id", "run_started_at", "run_completed_at",
    "market_time_zone", "source_api_module", "environment", "publication_status",
    "source_request_count", "successful_source_request_count", "failed_source_request_count",
    "planned_request_count", "successful_request_count", "failed_request_count",
    "daily_record_count", "minute_record_count", "failed_source_requests", "failed_requests", "members",
)
TRANSFER_REQUEST_BLOCK_KEYS = (
    "observation_id", "source_request_id", "source_request_ordinal",
    "request_id", "request_ordinal", "contract_code", "bar_frequency",
    "trading_date", "session_number", "session_start_at", "session_end_at",
    "source_frequency", "source_fields", "source_parameters", "expected", "actual",
    "request_status", "response_validation_status", "coverage_status",
    "observation_eligible", "quality_status", "quality_reason",
    "request_started_at", "request_completed_at", "elapsed_seconds", "error_type", "error_message",
)
TRANSFER_DAILY_RECORD_KEYS = (
    "observation_id", "contract_code", "trading_date", "previous_close",
    "open", "high", "low", "close", "volume", "money", "open_interest",
)
TRANSFER_MINUTE_RECORD_KEYS = (
    "observation_id", "contract_code", "bar_at", "trading_date", "session_number",
    "open", "high", "low", "close", "volume", "money", "open_interest",
)

JOINQUANT_DAILY_SOURCE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "pre_close",
    "open_interest",
)
JOINQUANT_MINUTE_SOURCE_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "open_interest",
)


# 每项为 (字段名, DuckDB 声明类型, nullable)。表顺序同时是建表顺序。
DATABASE_TABLE_COLUMNS: dict[str, tuple[tuple[str, str, bool], ...]] = {
    "ingest_batch": (
        ("file_sha256", "VARCHAR", False),
        ("run_id", "VARCHAR", False),
        ("plan_sha256", "VARCHAR", False),
        ("protocol_version", "VARCHAR", False),
        ("generator_version", "VARCHAR", False),
        ("fixed_file_name", "VARCHAR", False),
        ("file_bytes", "BIGINT", False),
        ("manifest_sha256", "VARCHAR", False),
        ("publication_status", "VARCHAR", False),
        ("run_started_at", "TIMESTAMPTZ", False),
        ("run_completed_at", "TIMESTAMPTZ", False),
        ("source_request_count", "INTEGER", False),
        ("successful_source_request_count", "INTEGER", False),
        ("failed_source_request_count", "INTEGER", False),
        ("planned_request_count", "INTEGER", False),
        ("successful_request_count", "INTEGER", False),
        ("failed_request_count", "INTEGER", False),
        ("daily_record_count", "BIGINT", False),
        ("minute_record_count", "BIGINT", False),
        ("daily_inserted_count", "BIGINT", False),
        ("daily_existing_equal_count", "BIGINT", False),
        ("minute_inserted_count", "BIGINT", False),
        ("minute_existing_equal_count", "BIGINT", False),
        ("manifest_json", "VARCHAR", False),
        ("imported_at", "TIMESTAMPTZ", False),
    ),
    "fetch_observation": (
        ("observation_id", "VARCHAR", False),
        ("file_sha256", "VARCHAR", False),
        ("source_request_id", "VARCHAR", False),
        ("source_request_ordinal", "INTEGER", False),
        ("request_id", "VARCHAR", False),
        ("request_ordinal", "INTEGER", False),
        ("contract_code", "VARCHAR", False),
        ("bar_frequency", "VARCHAR", False),
        ("trading_date", "DATE", False),
        ("session_number", "TINYINT", False),
        ("session_start_at", "TIMESTAMPTZ", True),
        ("session_end_at", "TIMESTAMPTZ", True),
        ("expected_key_count", "INTEGER", False),
        ("expected_keys_sha256", "VARCHAR", False),
        ("actual_key_count", "INTEGER", False),
        ("actual_keys_sha256", "VARCHAR", False),
        ("missing_key_count", "INTEGER", False),
        ("coverage_status", "VARCHAR", False),
        ("quality_status", "VARCHAR", False),
        ("quality_reason", "VARCHAR", False),
        ("request_started_at", "TIMESTAMPTZ", False),
        ("request_completed_at", "TIMESTAMPTZ", False),
        ("elapsed_seconds", "DOUBLE", False),
    ),
    "futures_daily": (
        ("contract_code", "VARCHAR", False),
        ("trading_date", "DATE", False),
        ("previous_close", "DOUBLE", True),
        ("open", "DOUBLE", False),
        ("high", "DOUBLE", False),
        ("low", "DOUBLE", False),
        ("close", "DOUBLE", False),
        ("volume", "DOUBLE", False),
        ("money", "DOUBLE", False),
        ("open_interest", "DOUBLE", False),
        ("first_observation_id", "VARCHAR", False),
    ),
    "futures_minute": (
        ("contract_code", "VARCHAR", False),
        ("bar_at", "TIMESTAMPTZ", False),
        ("trading_date", "DATE", False),
        ("session_number", "TINYINT", False),
        ("open", "DOUBLE", False),
        ("high", "DOUBLE", False),
        ("low", "DOUBLE", False),
        ("close", "DOUBLE", False),
        ("volume", "DOUBLE", False),
        ("money", "DOUBLE", False),
        ("open_interest", "DOUBLE", False),
        ("first_observation_id", "VARCHAR", False),
    ),
}

DATABASE_TABLE_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "ingest_batch": ("file_sha256",),
    "fetch_observation": ("observation_id",),
    "futures_daily": ("contract_code", "trading_date"),
    "futures_minute": ("contract_code", "bar_at"),
}

DATABASE_TABLE_CONSTRAINTS: dict[str, tuple[str, ...]] = {
    "ingest_batch": (
        "PRIMARY KEY (file_sha256)",
        "UNIQUE (run_id)",
        "CHECK (regexp_full_match(file_sha256, '[0-9a-f]{64}'))",
        "CHECK (regexp_full_match(plan_sha256, '[0-9a-f]{64}'))",
        "CHECK (regexp_full_match(manifest_sha256, '[0-9a-f]{64}'))",
        f"CHECK (protocol_version = '{TRANSFER_PROTOCOL_VERSION}')",
        f"CHECK (generator_version = '{TRANSFER_GENERATOR_VERSION}')",
        f"CHECK (fixed_file_name = '{TRANSFER_FIXED_FILE_NAME}')",
        "CHECK (file_bytes > 0)",
        "CHECK (run_completed_at >= run_started_at)",
        "CHECK (source_request_count > 0)",
        "CHECK (successful_source_request_count >= 0)",
        "CHECK (failed_source_request_count >= 0)",
        "CHECK (successful_source_request_count + failed_source_request_count = source_request_count)",
        "CHECK (planned_request_count > 0)",
        "CHECK (successful_request_count >= 0)",
        "CHECK (failed_request_count >= 0)",
        "CHECK (successful_request_count + failed_request_count = planned_request_count)",
        "CHECK (daily_record_count >= 0 AND minute_record_count >= 0)",
        "CHECK (daily_inserted_count >= 0 AND daily_existing_equal_count >= 0)",
        "CHECK (minute_inserted_count >= 0 AND minute_existing_equal_count >= 0)",
        "CHECK (daily_inserted_count + daily_existing_equal_count = daily_record_count)",
        "CHECK (minute_inserted_count + minute_existing_equal_count = minute_record_count)",
        "CHECK ((publication_status = 'complete' AND failed_source_request_count = 0 AND failed_request_count = 0) OR (publication_status = 'partial' AND failed_source_request_count > 0 AND failed_request_count > 0))",
        "CHECK (length(manifest_json) > 0)",
    ),
    "fetch_observation": (
        "PRIMARY KEY (observation_id)",
        "UNIQUE (file_sha256, request_id)",
        "UNIQUE (file_sha256, request_ordinal)",
        "FOREIGN KEY (file_sha256) REFERENCES ingest_batch (file_sha256)",
        "CHECK (regexp_full_match(observation_id, '[0-9a-f]{64}'))",
        "CHECK (regexp_full_match(file_sha256, '[0-9a-f]{64}'))",
        "CHECK (regexp_full_match(source_request_id, '[0-9a-f]{64}'))",
        "CHECK (request_ordinal > 0 AND source_request_ordinal > 0)",
        "CHECK (bar_frequency IN ('1d', '1m'))",
        "CHECK ((bar_frequency = '1d' AND session_number = 0 AND session_start_at IS NULL AND session_end_at IS NULL AND expected_key_count = 1) OR (bar_frequency = '1m' AND session_number > 0 AND session_start_at IS NOT NULL AND session_end_at IS NOT NULL AND session_end_at > session_start_at AND date_diff('minute', session_start_at, session_end_at) = expected_key_count))",
        "CHECK (regexp_full_match(expected_keys_sha256, '[0-9a-f]{64}'))",
        "CHECK (regexp_full_match(actual_keys_sha256, '[0-9a-f]{64}'))",
        "CHECK (actual_key_count >= 0 AND missing_key_count >= 0)",
        "CHECK (actual_key_count + missing_key_count = expected_key_count)",
        "CHECK ((coverage_status = 'complete' AND actual_key_count = expected_key_count) OR (coverage_status = 'partial_missing' AND actual_key_count > 0 AND actual_key_count < expected_key_count) OR (coverage_status = 'empty' AND actual_key_count = 0))",
        "CHECK (quality_status IN ('passed', 'warning'))",
        "CHECK (coverage_status = 'complete' OR quality_status = 'warning')",
        "CHECK (length(quality_reason) > 0)",
        "CHECK (request_completed_at >= request_started_at)",
        "CHECK (isfinite(elapsed_seconds) AND elapsed_seconds >= 0)",
    ),
    "futures_daily": (
        "PRIMARY KEY (contract_code, trading_date)",
        "FOREIGN KEY (first_observation_id) REFERENCES fetch_observation (observation_id)",
        "CHECK (previous_close IS NULL OR isfinite(previous_close))",
        "CHECK (isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close))",
        "CHECK (isfinite(volume) AND volume >= 0)",
        "CHECK (isfinite(money) AND money >= 0)",
        "CHECK (isfinite(open_interest) AND open_interest >= 0)",
    ),
    "futures_minute": (
        "PRIMARY KEY (contract_code, bar_at)",
        "FOREIGN KEY (first_observation_id) REFERENCES fetch_observation (observation_id)",
        "CHECK (session_number > 0)",
        "CHECK (isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close))",
        "CHECK (isfinite(volume) AND volume >= 0)",
        "CHECK (isfinite(money) AND money >= 0)",
        "CHECK (isfinite(open_interest) AND open_interest >= 0)",
    ),
}


def database_create_table_statements() -> tuple[str, ...]:
    """按外键依赖顺序生成四张正式项目表的 DuckDB DDL。"""

    statements: list[str] = []
    for table_name, columns in DATABASE_TABLE_COLUMNS.items():
        definitions = [
            f"{column_name} {column_type}"
            + ("" if nullable else " NOT NULL")
            for column_name, column_type, nullable in columns
        ]
        definitions.extend(DATABASE_TABLE_CONSTRAINTS[table_name])
        statements.append(
            f"CREATE TABLE {table_name} (\n    "
            + ",\n    ".join(definitions)
            + "\n)"
        )
    return tuple(statements)


def validate_database_schema(connection: Any) -> None:
    """只读核对项目库四表的字段顺序、类型、nullable 与主键。"""

    actual_table_names = tuple(
        row[0]
        for row in connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        ).fetchall()
    )
    expected_table_names = tuple(sorted(DATABASE_TABLE_COLUMNS))
    if actual_table_names != expected_table_names:
        raise ValueError(
            "项目数据库表集合与契约不一致："
            f"期望 {expected_table_names}，实际 {actual_table_names}。"
        )

    canonical_type = {
        "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
    }
    for table_name, columns in DATABASE_TABLE_COLUMNS.items():
        primary_key = set(DATABASE_TABLE_PRIMARY_KEYS[table_name])
        expected_signature = [
            (
                column_name,
                canonical_type.get(column_type, column_type),
                not nullable,
                column_name in primary_key,
            )
            for column_name, column_type, nullable in columns
        ]
        actual_signature = [
            (row[1], row[2], bool(row[3]), bool(row[5]))
            for row in connection.execute(
                f"PRAGMA table_info('{table_name}')"
            ).fetchall()
        ]
        if actual_signature != expected_signature:
            raise ValueError(
                f"项目数据库表 {table_name} 的字段契约不一致："
                f"期望 {expected_signature}，实际 {actual_signature}。"
            )


def require_exact_keys(
    value: dict[str, Any],
    expected_keys: Sequence[str],
    object_name: str,
) -> None:
    """要求协议对象的键集合精确匹配，拒绝静默缺列或新增列。"""

    actual_keys = tuple(value)
    if set(actual_keys) != set(expected_keys) or len(actual_keys) != len(expected_keys):
        raise ValueError(
            f"{object_name} 键集合与协议不一致："
            f"期望 {tuple(expected_keys)}，实际 {actual_keys}。"
        )
