"""人工固定 ZIP 的快照、协议验证和 DuckDB 四表原子导入；无网络调用。"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
from pathlib import Path
import re
import sys
import tempfile
import uuid
import zipfile
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds

import financial_futures_collection_policy as policy
import financial_futures_local_contract as contract
from financial_futures_fetch_planner import FETCH_PLANNER_VERSION


MARKET_ZONE = ZoneInfo(contract.TRANSFER_MARKET_TIME_ZONE)
# 本地文件资源门禁，不改变传输字段或来源请求容量，也不自动拆包/重试。
MAX_ZIP_BYTES = 512 * 1024 * 1024
MAX_EXPANDED_BYTES = 1024 * 1024 * 1024
MAX_JSON_LINE_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024


def canonical_bytes(value: object) -> bytes:
    """协议的唯一字节表示；同时区分 bool/int 等 Python 相等但类型不同的值。"""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def key_summary(keys: list[str]) -> dict:
    """精确有序键的协议证据，不把首尾范围当作键集合。"""
    return {"key_count": len(keys), "first_key": keys[0] if keys else None,
            "last_key": keys[-1] if keys else None,
            "keys_sha256": hashlib.sha256(canonical_bytes(keys)).hexdigest()}


def protocol_time(value: object, *, minute: bool = False) -> dt.datetime:
    """拒绝无时区、非北京时间及非 canonical 的外来时间文本。"""
    pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{6})?\+08:00"
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError(f"非法协议时间：{value!r}")
    timestamp = dt.datetime.fromisoformat(value)
    if timestamp.isoformat() != value:
        raise ValueError(f"非 canonical 时间：{value!r}")
    if minute and (timestamp.second or timestamp.microsecond):
        raise ValueError(f"时间不在整分钟：{value!r}")
    return timestamp


def validate_transfer_snapshot(snapshot_path: Path) -> dict:
    """完整验证不可变 ZIP，重建计划及理论键，返回待入库的有界四表内容。

    不解压到磁盘、不执行文件内容、不调用来源 API、不连接项目库。
    计划身份是完整性证明，不是对任意外来文件的数字签名。
    """
    file_bytes = snapshot_path.stat().st_size
    if not 0 < file_bytes <= MAX_ZIP_BYTES:
        raise ValueError("ZIP 为空或超过本地 512 MiB 资源门禁")
    with snapshot_path.open("rb") as snapshot_handle:
        file_sha256 = hashlib.file_digest(snapshot_handle, "sha256").hexdigest()
    decoded_members = {}
    member_evidence = {}
    manifest_payload = None
    with zipfile.ZipFile(snapshot_path) as archive:
        entries = archive.infolist()
        if tuple(entry.filename for entry in entries) != contract.TRANSFER_MEMBER_NAMES:
            raise ValueError("ZIP 成员集合/顺序错误，拒绝探针、额外或重复成员")
        if archive.comment or sum(entry.file_size for entry in entries) > MAX_EXPANDED_BYTES:
            raise ValueError("ZIP 注释或展开大小不符合本地门禁")
        for entry in entries:
            if (entry.is_dir() or entry.flag_bits & 1
                    or entry.compress_type != zipfile.ZIP_DEFLATED
                    or (entry.external_attr >> 16) & 0o170000 == 0o120000):
                raise ValueError(f"拒绝目录、加密、链接或非 DEFLATE 成员：{entry.filename}")
        # 流式解码限制单行和总记录数，读至 EOF 同时由 zipfile 验证 CRC。
        for entry in entries:
            records = []
            payload_sha256 = hashlib.sha256()
            read_bytes = 0
            max_records = (policy.JOINQUANT_MAX_OBSERVATIONS_PER_TRANSFER
                           if entry.filename == "request_blocks.jsonl"
                           else policy.JOINQUANT_MAX_EXPECTED_KEYS_PER_TRANSFER)
            with archive.open(entry) as member_handle:
                if entry.filename == "manifest.json":
                    payload = member_handle.read(MAX_MANIFEST_BYTES + 1)
                    if len(payload) > MAX_MANIFEST_BYTES:
                        raise ValueError("manifest 超过本地资源门禁")
                    value = json.loads(payload.decode("utf-8"))
                    if type(value) is not dict or canonical_bytes(value) != payload:
                        raise ValueError("manifest 不是 canonical UTF-8 对象")
                    decoded_members[entry.filename] = value
                    manifest_payload = payload
                    continue
                while True:
                    line = member_handle.readline(MAX_JSON_LINE_BYTES + 1)
                    if not line:
                        break
                    if len(line) > MAX_JSON_LINE_BYTES or not line.endswith(b"\n"):
                        raise ValueError(f"JSONL 行过长或缺少 LF：{entry.filename}")
                    value = json.loads(line.decode("utf-8"))
                    if type(value) is not dict or canonical_bytes(value) + b"\n" != line:
                        raise ValueError(f"JSONL 非 canonical 对象：{entry.filename}")
                    records.append(value)
                    if len(records) > max_records:
                        raise ValueError(f"JSONL 记录数超过计划上限：{entry.filename}")
                    payload_sha256.update(line)
                    read_bytes += len(line)
            if read_bytes != entry.file_size:
                raise ValueError("ZIP 成员展开字节数不符")
            decoded_members[entry.filename] = records
            member_evidence[entry.filename] = {
                "bytes": read_bytes, "sha256": payload_sha256.hexdigest(),
                "record_count": len(records),
            }

    manifest = decoded_members["manifest.json"]
    contract.require_exact_keys(manifest, contract.TRANSFER_MANIFEST_KEYS, "manifest")
    stable_values = {
        "format_id": contract.TRANSFER_FORMAT_ID,
        "protocol_version": contract.TRANSFER_PROTOCOL_VERSION,
        "fixed_file_name": contract.TRANSFER_FIXED_FILE_NAME,
        "generator_version": contract.TRANSFER_GENERATOR_VERSION,
        "planner_version": FETCH_PLANNER_VERSION,
        "market_time_zone": contract.TRANSFER_MARKET_TIME_ZONE,
        "source_api_module": contract.TRANSFER_SOURCE_API_MODULE,
    }
    if any(manifest[name] != value for name, value in stable_values.items()):
        raise ValueError("manifest 稳定标识/版本不符")
    if canonical_bytes(manifest["members"]) != canonical_bytes(member_evidence):
        raise ValueError("manifest 成员字段、字节数、摘要或记录数不符")
    if (type(manifest["run_id"]) is not str
            or str(uuid.UUID(manifest["run_id"])) != manifest["run_id"]):
        raise ValueError("run_id 不是标准 UUID")
    if not isinstance(manifest["plan_sha256"], str) or not re.fullmatch(
            r"[0-9a-f]{64}", manifest["plan_sha256"]):
        raise ValueError("plan_sha256 格式错误")
    environment = manifest["environment"]
    if type(environment) is not dict:
        raise ValueError("environment 必须是对象")
    contract.require_exact_keys(environment, (
        "python_version", "pandas_version", "numpy_version", "jqdata_version"), "environment")
    if any(type(value) is not str or not value for value in environment.values()):
        raise ValueError("环境版本必须是非空文本")
    run_started_at = protocol_time(manifest["run_started_at"])
    run_completed_at = protocol_time(manifest["run_completed_at"])
    if not run_started_at <= run_completed_at <= dt.datetime.now(MARKET_ZONE):
        raise ValueError("运行时间倒置或位于本地未来")
    for name in ("source_request_count", "successful_source_request_count",
                 "failed_source_request_count", "planned_request_count",
                 "successful_request_count", "failed_request_count",
                 "daily_record_count", "minute_record_count"):
        if type(manifest[name]) is not int or manifest[name] < 0:
            raise ValueError(f"计数必须是非负整数而非 bool：{name}")
    if not 0 < manifest["source_request_count"] <= policy.JOINQUANT_MAX_SOURCE_REQUESTS_PER_TRANSFER:
        raise ValueError("来源请求数不在有界计划范围")
    if not 0 < manifest["planned_request_count"] <= policy.JOINQUANT_MAX_OBSERVATIONS_PER_TRANSFER:
        raise ValueError("观察块数不在有界计划范围")

    request_blocks = decoded_members["request_blocks.jsonl"]
    if len(request_blocks) != manifest["planned_request_count"]:
        raise ValueError("观察块总数不符")
    block_by_id = {}
    expected_keys_by_id = {}
    observed_keys_by_id = {}
    null_counts_by_id = {}
    ohlc_warnings_by_id = {}
    source_groups = []
    request_ids = set()
    expected_business_keys = set()
    total_expected_keys = 0
    reconstructed_observations = {}
    key_summary_names = ("key_count", "first_key", "last_key", "keys_sha256")
    for ordinal, block in enumerate(request_blocks, 1):
        contract.require_exact_keys(block, contract.TRANSFER_REQUEST_BLOCK_KEYS, "request block")
        if type(block["request_ordinal"]) is not int or block["request_ordinal"] != ordinal:
            raise ValueError("观察块序号非连续一基顺序")
        source_ordinal = block["source_request_ordinal"]
        if type(source_ordinal) is not int or source_ordinal < 1:
            raise ValueError("来源请求序号类型错误")
        if source_ordinal == len(source_groups) + 1:
            source_groups.append([])
        if source_ordinal != len(source_groups):
            raise ValueError("来源请求块不是连续分组的一基顺序")
        source_groups[-1].append(block)
        code = block["contract_code"]
        match = re.fullmatch(r"([A-Z]+)(\d{2})(0[1-9]|1[0-2])\.([A-Z]+)", code) if type(code) is str else None
        if (match is None or match[1] not in policy.FINANCIAL_FUTURES_UNDERLYING_CODES
                or match[4] != policy.FINANCIAL_FUTURES_EXCHANGE_CODE):
            raise ValueError(f"不是白名单固定月份合约：{code!r}")
        frequency = block["bar_frequency"]
        if frequency not in policy.FINANCIAL_FUTURES_BAR_FREQUENCIES:
            raise ValueError("频率不受支持")
        trading_date_text = block["trading_date"]
        if type(trading_date_text) is not str or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", trading_date_text):
            raise ValueError("交易日格式错误")
        trading_date = dt.date.fromisoformat(trading_date_text)
        ready_at = dt.datetime.combine(trading_date + dt.timedelta(days=1),
                                      policy.JOINQUANT_COMPLETED_DATA_READY_TIME, MARKET_ZONE)
        if ready_at > run_started_at:
            raise ValueError("请求包含运行开始时尚未就绪的交易日")
        if type(block["session_number"]) is not int:
            raise ValueError("Session 编号必须是整数")
        if frequency == "1d":
            if block["session_number"] != 0 or block["session_start_at"] is not None or block["session_end_at"] is not None:
                raise ValueError("日线 Session 结构错误")
            expected_keys = [trading_date_text]
            source_fields = list(contract.JOINQUANT_DAILY_SOURCE_FIELDS)
        else:
            if not 0 < block["session_number"] <= 127:
                raise ValueError("分钟 Session 编号越界")
            begin = protocol_time(block["session_start_at"], minute=True)
            end = protocol_time(block["session_end_at"], minute=True)
            minute_count = int((end - begin).total_seconds() / 60)
            if not 0 < minute_count <= policy.JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST:
                raise ValueError("分钟边界反向或超出单请求容量")
            expected_keys = [(begin + dt.timedelta(minutes=offset)).isoformat(timespec="seconds")
                             for offset in range(1, minute_count + 1)]
            source_fields = list(contract.JOINQUANT_MINUTE_SOURCE_FIELDS)
        if (block["source_frequency"] != ("daily" if frequency == "1d" else "1m")
                or canonical_bytes(block["source_fields"]) != canonical_bytes(source_fields)
                or canonical_bytes(block["source_parameters"]) != canonical_bytes(contract.TRANSFER_SOURCE_PARAMETERS)):
            raise ValueError("来源频率、字段或参数不符")
        if canonical_bytes(block["expected"]) != canonical_bytes(key_summary(expected_keys)):
            raise ValueError("理论键数量、首尾或摘要与边界不一致")
        total_expected_keys += len(expected_keys)
        if total_expected_keys > policy.JOINQUANT_MAX_EXPECTED_KEYS_PER_TRANSFER:
            raise ValueError("文件理论键数超过容量")
        for key in expected_keys:
            business_key = (frequency, code, key)
            if business_key in expected_business_keys:
                raise ValueError("观察块之间有重叠理论键")
            expected_business_keys.add(business_key)
        identity = {name: block[name] for name in (
            "bar_frequency", "contract_code", "session_end_at", "session_number",
            "session_start_at", "trading_date")}
        identity.update(expected_key_count=len(expected_keys),
                        expected_keys_sha256=block["expected"]["keys_sha256"])
        request_id = hashlib.sha256(canonical_bytes(identity)).hexdigest()
        observation_id = hashlib.sha256((manifest["run_id"] + "\n" + request_id).encode("utf-8")).hexdigest()
        if (block["request_id"] != request_id or block["observation_id"] != observation_id
                or request_id in request_ids):
            raise ValueError("观察身份不符或重复")
        request_ids.add(request_id)
        reconstructed_observations[observation_id] = dict(
            request_id=request_id, request_ordinal=ordinal, **identity)
        block_by_id[observation_id] = block
        expected_keys_by_id[observation_id] = set(expected_keys)
        observed_keys_by_id[observation_id] = []
        null_counts_by_id[observation_id] = dict.fromkeys(source_fields, 0)
        ohlc_warnings_by_id[observation_id] = 0
        request_started_at = protocol_time(block["request_started_at"])
        request_completed_at = protocol_time(block["request_completed_at"])
        if not run_started_at <= request_started_at <= request_completed_at <= run_completed_at:
            raise ValueError("请求时间超出运行边界")
        elapsed = block["elapsed_seconds"]
        if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
            raise ValueError("请求耗时类型错误或非有限非负数")
        if type(block["observation_eligible"]) is not bool:
            raise ValueError("observation_eligible 必须是 bool")
        if type(block["quality_reason"]) is not str:
            raise ValueError("quality_reason 必须是文本")
        actual = block["actual"]
        if actual is not None:
            if type(actual) is not dict:
                raise ValueError("actual 必须是对象或 null")
            contract.require_exact_keys(actual, (*key_summary_names, "duplicate_key_count",
                "missing_key_count", "extra_key_count", "null_count_by_field"), "actual")
            for name in ("key_count", "duplicate_key_count", "missing_key_count", "extra_key_count"):
                if type(actual[name]) is not int or not 0 <= actual[name] <= MAX_EXPANDED_BYTES:
                    raise ValueError("actual 计数类型错误")
            if (actual["duplicate_key_count"] > actual["key_count"]
                    or actual["missing_key_count"] != len(expected_keys) - actual["key_count"] + actual["duplicate_key_count"]
                    or actual["extra_key_count"] != 0):
                raise ValueError("actual 重复、缺失与归属键计数不符")
            if not isinstance(actual["keys_sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", actual["keys_sha256"]):
                raise ValueError("actual 键摘要格式错误")
            if actual["key_count"] == 0:
                if canonical_bytes({name: actual[name] for name in key_summary_names}) != canonical_bytes(key_summary([])):
                    raise ValueError("零行 actual 的键证据不符")
            elif actual["first_key"] not in expected_keys_by_id[observation_id] or actual["last_key"] not in expected_keys_by_id[observation_id]:
                raise ValueError("actual 首尾键不在观察块内")
            null_counts = actual["null_count_by_field"]
            if type(null_counts) is not dict:
                raise ValueError("空值计数必须是对象")
            contract.require_exact_keys(null_counts, source_fields, "null_count_by_field")
            if any(type(value) is not int or not 0 <= value <= actual["key_count"] for value in null_counts.values()):
                raise ValueError("逐字段空值计数错误")
        if block["observation_eligible"]:
            if (block["request_status"] != "success" or block["response_validation_status"] != "passed"
                    or actual is None or actual["duplicate_key_count"] or actual["extra_key_count"]
                    or block["error_type"] is not None or block["error_message"] is not None):
                raise ValueError("成功观察的请求状态或错误字段不符")
        else:
            status = block["request_status"]
            if (status not in ("success", "error", "not_executed")
                    or block["response_validation_status"] != ("failed" if status == "success" else "not_run")
                    or block["coverage_status"] is not None or block["quality_status"] != "failed"
                    or type(block["error_type"]) is not str or not block["error_type"]
                    or type(block["error_message"]) is not str
                    or block["quality_reason"] != block["error_message"]):
                raise ValueError("失败观察的状态或错误证据不符")
            if status in ("error", "not_executed") and actual is not None:
                raise ValueError("没有响应的请求不能伪造 actual")
            if status == "not_executed" and (block["error_type"] != "NotExecuted" or elapsed != 0):
                raise ValueError("未执行请求的错误类型或耗时不符")

    # 从所有块（包括失败/未执行）重建来源与完整计划，不能只对成功块重算。
    sources = []
    failed_sources = []
    failed_requests = []
    stop_seen = False
    previous_completed_at = run_started_at
    shared_names = ("source_request_id", "source_request_ordinal", "contract_code",
                   "bar_frequency", "source_fields", "source_frequency", "source_parameters",
                   "request_status", "response_validation_status", "observation_eligible",
                   "request_started_at", "request_completed_at", "elapsed_seconds", "error_type", "error_message")
    for source_ordinal, blocks in enumerate(source_groups, 1):
        first = blocks[0]
        shared_values = canonical_bytes({name: first[name] for name in shared_names})
        if any(canonical_bytes({name: block[name] for name in shared_names}) != shared_values for block in blocks):
            raise ValueError("同一实际来源请求的状态/时间/身份不一致")
        if sum(block["expected"]["key_count"] for block in blocks) > policy.JOINQUANT_MAX_EXPECTED_KEYS_PER_SOURCE_REQUEST:
            raise ValueError("来源请求理论键数超过容量")
        previous_key = None
        for block in blocks:
            if previous_key is not None and block["expected"]["first_key"] <= previous_key:
                raise ValueError("来源观察块顺序错误")
            previous_key = block["expected"]["last_key"]
        if protocol_time(first["request_started_at"]) < previous_completed_at:
            raise ValueError("来源请求不是串行执行")
        previous_completed_at = protocol_time(first["request_completed_at"])
        if stop_seen != (first["request_status"] == "not_executed"):
            raise ValueError("不符合首个失败后全部停止且不重试的生成器规则")
        if not first["observation_eligible"]:
            stop_seen = True
            failed_sources.append({name: first[name] for name in (
                "source_request_id", "source_request_ordinal", "error_type", "error_message")})
            failed_requests.extend({name: block[name] for name in (
                "source_request_id", "request_id", "error_type", "error_message")} for block in blocks)
        identity = {name: first[name] for name in (
            "bar_frequency", "contract_code", "source_fields", "source_frequency", "source_parameters")}
        identity.update(observation_request_ids=[block["request_id"] for block in blocks],
                        source_start_at=first["expected"]["first_key"],
                        source_end_at=blocks[-1]["expected"]["last_key"])
        source_request_id = hashlib.sha256(canonical_bytes(identity)).hexdigest()
        if first["source_request_id"] != source_request_id:
            raise ValueError("来源请求身份与重建计划不符")
        sources.append(dict(source_request_id=source_request_id,
            source_request_ordinal=source_ordinal, **identity,
            observations=[reconstructed_observations[block["observation_id"]] for block in blocks]))
    plan_payload = {name: manifest[name] for name in (
        "format_id", "protocol_version", "generator_version", "planner_version", "market_time_zone")}
    plan_payload["source_requests"] = sources
    if hashlib.sha256(canonical_bytes(plan_payload)).hexdigest() != manifest["plan_sha256"]:
        raise ValueError("完整计划 SHA-256 不匹配")
    success_source_count = len(sources) - len(failed_sources)
    expected_counts = {
        "source_request_count": len(sources), "successful_source_request_count": success_source_count,
        "failed_source_request_count": len(failed_sources),
        "successful_request_count": len(request_blocks) - len(failed_requests),
        "failed_request_count": len(failed_requests),
        "daily_record_count": len(decoded_members["futures_daily.jsonl"]),
        "minute_record_count": len(decoded_members["futures_minute.jsonl"]),
    }
    if (success_source_count == 0 or any(manifest[name] != value for name, value in expected_counts.items())
            or manifest["publication_status"] != ("partial" if failed_sources else "complete")
            or canonical_bytes(manifest["failed_source_requests"]) != canonical_bytes(failed_sources)
            or canonical_bytes(manifest["failed_requests"]) != canonical_bytes(failed_requests)):
        raise ValueError("manifest 的成功/失败状态、计数或摘要列表不一致")

    table_records = {"futures_daily": [], "futures_minute": [], "fetch_observation": []}
    for frequency, table_name, field_names in (
            ("1d", "futures_daily", contract.TRANSFER_DAILY_RECORD_KEYS),
            ("1m", "futures_minute", contract.TRANSFER_MINUTE_RECORD_KEYS)):
        previous_key = None
        for record in decoded_members[table_name + ".jsonl"]:
            contract.require_exact_keys(record, field_names, table_name)
            observation_id = record["observation_id"]
            if type(observation_id) is not str or observation_id not in block_by_id:
                raise ValueError("行情引用未知观察")
            block = block_by_id[observation_id]
            if (not block["observation_eligible"] or block["bar_frequency"] != frequency
                    or record["contract_code"] != block["contract_code"]
                    or record["trading_date"] != block["trading_date"]):
                raise ValueError("行情的观察、合约、频率或交易日归属错误")
            key = record["trading_date"] if frequency == "1d" else record["bar_at"]
            if type(key) is not str or key not in expected_keys_by_id[observation_id]:
                raise ValueError("行情键不在精确理论集合内")
            compound_key = (record["contract_code"], key)
            if previous_key is not None and compound_key <= previous_key:
                raise ValueError("文件内行情重复或非严格升序，不能静默折叠")
            previous_key = compound_key
            typed_record = dict(record)
            typed_record["trading_date"] = dt.date.fromisoformat(record["trading_date"])
            if frequency == "1m":
                if type(record["session_number"]) is not int or record["session_number"] != block["session_number"]:
                    raise ValueError("分钟 Session 归属错误")
                typed_record["bar_at"] = protocol_time(key, minute=True)
            for source_field in block["source_fields"]:
                field = "previous_close" if source_field == "pre_close" else source_field
                value = record[field]
                if value is None and field == "previous_close":
                    null_counts_by_id[observation_id][source_field] += 1
                elif (type(value) not in (int, float) or not math.isfinite(value)
                      or float(value) != value or (field in ("volume", "money", "open_interest") and value < 0)):
                    raise ValueError(f"行情值非法或不能无损表示为 DOUBLE：{field}")
                else:
                    typed_record[field] = float(value)
            if (record["high"] < max(record["open"], record["low"], record["close"])
                    or record["low"] > min(record["open"], record["high"], record["close"])):
                ohlc_warnings_by_id[observation_id] += 1
            typed_record["first_observation_id"] = typed_record.pop("observation_id")
            table_records[table_name].append(typed_record)
            observed_keys_by_id[observation_id].append(key)

    for block in request_blocks:
        observation_id = block["observation_id"]
        if not block["observation_eligible"]:
            continue
        keys = observed_keys_by_id[observation_id]
        expected_count = block["expected"]["key_count"]
        coverage = "complete" if len(keys) == expected_count else "empty" if not keys else "partial_missing"
        ohlc_warning_count = ohlc_warnings_by_id[observation_id]
        actual = dict(key_summary(keys), duplicate_key_count=0, extra_key_count=0,
                      missing_key_count=expected_count-len(keys),
                      null_count_by_field=null_counts_by_id[observation_id])
        if (canonical_bytes(block["actual"]) != canonical_bytes(actual)
                or block["coverage_status"] != coverage
                or block["quality_status"] != ("warning" if coverage != "complete" or ohlc_warning_count else "passed")
                or block["quality_reason"] != f"coverage={coverage}; ohlc_warning_count={ohlc_warning_count}"):
            raise ValueError("行情复算的实际键/空值/覆盖/质量证据与观察不一致")
        observation_record = {name: block[name] for name in (
            "observation_id", "source_request_id", "source_request_ordinal", "request_id", "request_ordinal",
            "contract_code", "bar_frequency", "session_number", "coverage_status",
            "quality_status", "quality_reason", "elapsed_seconds")}
        observation_record.update(
            file_sha256=file_sha256, trading_date=dt.date.fromisoformat(block["trading_date"]),
            session_start_at=protocol_time(block["session_start_at"]) if block["session_start_at"] else None,
            session_end_at=protocol_time(block["session_end_at"]) if block["session_end_at"] else None,
            expected_key_count=expected_count, expected_keys_sha256=block["expected"]["keys_sha256"],
            actual_key_count=len(keys), actual_keys_sha256=actual["keys_sha256"],
            missing_key_count=expected_count-len(keys),
            request_started_at=protocol_time(block["request_started_at"]),
            request_completed_at=protocol_time(block["request_completed_at"]))
        table_records["fetch_observation"].append(observation_record)

    batch_record = {name: manifest[name] for name in (
        "run_id", "plan_sha256", "protocol_version", "generator_version", "fixed_file_name",
        "publication_status", "source_request_count", "successful_source_request_count",
        "failed_source_request_count", "planned_request_count", "successful_request_count",
        "failed_request_count", "daily_record_count", "minute_record_count")}
    batch_record.update(file_sha256=file_sha256, file_bytes=file_bytes,
        manifest_sha256=hashlib.sha256(manifest_payload).hexdigest(),
        manifest_json=manifest_payload.decode("utf-8"),
        run_started_at=run_started_at, run_completed_at=run_completed_at)
    # Arrow 只是传入 DuckDB 的有类型交换对象；类型由既有 DuckDB 契约派生。
    arrow_types = {"VARCHAR": pa.string(), "DATE": pa.date32(),
        "TIMESTAMPTZ": pa.timestamp("us", tz=contract.TRANSFER_MARKET_TIME_ZONE),
        "DOUBLE": pa.float64(), "BIGINT": pa.int64(), "INTEGER": pa.int32(), "TINYINT": pa.int8()}
    tables = {}
    for table_name, records in table_records.items():
        schema = pa.schema([pa.field(name, arrow_types[sql_type], nullable=nullable)
                            for name, sql_type, nullable in contract.DATABASE_TABLE_COLUMNS[table_name]])
        tables[table_name] = pa.Table.from_pylist(records, schema=schema)
    return {"batch_record": batch_record, "tables": tables, "request_blocks": request_blocks}


def verify_database_batch(connection, validated_package: dict, *, require_committed: bool) -> dict:
    """在当前连接中复核本批账本、观察与事实；只查询本批涉及的键，不扫历史。"""
    contract.validate_database_schema(connection)
    batch = validated_package["batch_record"]
    file_sha256 = batch["file_sha256"]
    batch_rows = connection.execute(
        "SELECT * FROM ingest_batch WHERE file_sha256 = ? OR run_id = ?",
        [file_sha256, batch["run_id"]]).fetchall()
    names = [column[0] for column in contract.DATABASE_TABLE_COLUMNS["ingest_batch"]]
    existing_batch = None
    for row in batch_rows:
        existing = dict(zip(names, row))
        if existing["file_sha256"] != file_sha256:
            raise ValueError("同一 run_id 对应不同文件 SHA-256，拒绝导入")
        if any(existing[name] != value for name, value in batch.items()):
            raise ValueError("既有同摘要批次账本与文件不一致")
        if (existing["imported_at"] is None
                or not existing["run_completed_at"] <= existing["imported_at"] <= dt.datetime.now(MARKET_ZONE)
                or any(type(existing[name]) is not int or existing[name] < 0 for name in (
                    "daily_inserted_count", "daily_existing_equal_count", "minute_inserted_count", "minute_existing_equal_count"))):
            raise ValueError("既有账本导入时间或计数非法")
        for frequency in ("daily", "minute"):
            if existing[frequency+"_inserted_count"] + existing[frequency+"_existing_equal_count"] != batch[frequency+"_record_count"]:
                raise ValueError("既有账本插入/相同记录计数不守恒")
        existing_batch = existing
    if require_committed and existing_batch is None:
        raise ValueError("提交复读找不到本批账本")

    counts = {}
    for table_name, incoming_table in validated_package["tables"].items():
        view_name = "incoming_" + table_name
        connection.register(view_name, incoming_table)
        key_names = contract.DATABASE_TABLE_PRIMARY_KEYS[table_name]
        join_condition = " AND ".join(f"stored.{name} = incoming.{name}" for name in key_names)
        column_names = [column[0] for column in contract.DATABASE_TABLE_COLUMNS[table_name]]
        comparison_names = [name for name in column_names if name not in key_names and
                            (table_name == "fetch_observation" or name != "first_observation_id")]
        differences = " OR ".join(f"stored.{name} IS DISTINCT FROM incoming.{name}" for name in comparison_names)
        conflict = connection.execute(
            f"SELECT {', '.join('incoming.'+name for name in key_names)} FROM {view_name} incoming "
            f"JOIN {table_name} stored ON {join_condition} WHERE {differences} LIMIT 1").fetchone()
        if conflict is not None:
            raise ValueError(f"库内业务值/观察冲突，整批拒绝：{table_name} {conflict}")
        equal_count = connection.execute(
            f"SELECT count(*) FROM {view_name} incoming JOIN {table_name} stored ON {join_condition}").fetchone()[0]
        if existing_batch is not None and equal_count != incoming_table.num_rows:
            raise ValueError(f"已提交批次缺少事实或观察：{table_name}")
        if table_name == "fetch_observation":
            stored_count = connection.execute(
                "SELECT count(*) FROM fetch_observation WHERE file_sha256 = ?", [file_sha256]).fetchone()[0]
            if stored_count != (incoming_table.num_rows if existing_batch else 0) or (not existing_batch and equal_count):
                raise ValueError("本批观察计数或观察身份已被其他批次使用")
        else:
            prefix = "daily" if table_name == "futures_daily" else "minute"
            counts[prefix+"_existing_equal_count"] = equal_count
            counts[prefix+"_inserted_count"] = incoming_table.num_rows - equal_count
            if existing_batch:
                # 原始观察身份不可变；它应指向一条能覆盖该事实的成功观察。
                key_time = "incoming.trading_date = origin.trading_date" if prefix == "daily" else (
                    "incoming.bar_at > origin.session_start_at AND incoming.bar_at <= origin.session_end_at "
                    "AND incoming.trading_date = origin.trading_date AND incoming.session_number = origin.session_number")
                invalid_origin = connection.execute(
                    f"SELECT 1 FROM {view_name} incoming JOIN {table_name} stored ON {join_condition} "
                    "LEFT JOIN fetch_observation origin ON origin.observation_id = stored.first_observation_id "
                    f"WHERE origin.observation_id IS NULL OR origin.contract_code != incoming.contract_code "
                    f"OR origin.bar_frequency != ? OR NOT ({key_time}) LIMIT 1",
                    ["1d" if prefix == "daily" else "1m"]).fetchone()
                if invalid_origin:
                    raise ValueError("事实首次观察引用不成立")
                inserted_count = connection.execute(
                    f"SELECT count(*) FROM {table_name} stored JOIN fetch_observation origin "
                    "ON origin.observation_id = stored.first_observation_id WHERE origin.file_sha256 = ?",
                    [file_sha256]).fetchone()[0]
                if inserted_count != existing_batch[prefix+"_inserted_count"]:
                    raise ValueError("既有账本首次插入计数与事实引用不符")
    return {"existing_batch": existing_batch, "counts": counts}


def validate_requested_calendar(request_blocks: list[dict]) -> None:
    """新批次的请求坐标必须仍匹配正式结构；不重跑缺失规划或扫描事实历史。"""
    project_markers = [".git", ".env", "config/settings.py"]
    current_path = pathlib.Path.cwd().resolve()
    for candidate_root in [current_path, *current_path.parents]:
        if all((candidate_root / marker).exists() for marker in project_markers):
            sys.path.insert(0, str(candidate_root))
            break
    else:
        raise RuntimeError("未找到项目根目录")
    from config.settings import settings
    from config.data_contracts import FUTURES_BAR_CALENDAR_SCHEMA, arrow_to_pandas

    calendar_schema = FUTURES_BAR_CALENDAR_SCHEMA
    calendar_path = settings.futures_lake_root / "silver" / calendar_schema.metadata[b"table_name"].decode("utf-8")
    partition_columns = calendar_schema.metadata[b"partition_columns"].decode("utf-8").split(",")
    primary_key = calendar_schema.metadata[b"primary_key"].decode("utf-8").split(",")
    calendar_dataset = ds.dataset(calendar_path, format="parquet", partitioning=ds.partitioning(
        pa.schema([calendar_schema.field(name) for name in partition_columns]), flavor="hive"))
    requested_dates = [dt.date.fromisoformat(block["trading_date"]) for block in request_blocks]
    calendar_filter = (
        (ds.field("exchange_code") == policy.FINANCIAL_FUTURES_EXCHANGE_CODE)
        & ds.field("underlying_code").isin(sorted({block["contract_code"].split(".")[0][:-4] for block in request_blocks}))
        & ds.field("contract_code").isin(sorted({block["contract_code"] for block in request_blocks}))
        & ds.field("bar_frequency").isin(sorted({block["bar_frequency"] for block in request_blocks}))
        & (ds.field("trading_date") >= min(requested_dates))
        & (ds.field("trading_date") <= max(requested_dates)))
    calendar_table = calendar_dataset.to_table(filter=calendar_filter).select(calendar_schema.names)
    calendar_df = arrow_to_pandas(calendar_table, calendar_schema)
    structure_columns = [*primary_key, "session_start_at", "session_end_at", "expected_bar_count",
                         "schedule_status", "evidence_level"]
    structure_df = calendar_df.loc[:, structure_columns].sort_values(
        ["bar_frequency", "contract_code", "trading_date", "session_number"], kind="stable")
    # 信任上游主键证明；只验证外来计划与本次读取的权威坐标逐值匹配。
    calendar_by_key = {}
    ordinal_by_contract = {}
    for row in structure_df.to_dict("records"):
        group = (row["bar_frequency"], row["contract_code"])
        ordinal_by_contract[group] = ordinal_by_contract.get(group, 0) + 1
        row["structure_ordinal"] = ordinal_by_contract[group]
        calendar_by_key[tuple(row[name] for name in primary_key)] = row
    previous_ordinal_by_source = {}
    for block in request_blocks:
        coordinate = dict(block, trading_date=dt.date.fromisoformat(block["trading_date"]))
        row = calendar_by_key.get(tuple(coordinate[name] for name in primary_key))
        if row is None or (row["schedule_status"] == "confirmed_closed" and row["evidence_level"] == "authoritative"):
            raise ValueError("新文件请求坐标不在当前正式开市结构中；保留文件并人工核查")
        if row["expected_bar_count"] != block["expected"]["key_count"]:
            raise ValueError("新文件理论数量与当前正式日历不符")
        if block["bar_frequency"] == "1m":
            for name in ("session_start_at", "session_end_at"):
                if row[name].astimezone(MARKET_ZONE).isoformat(timespec="seconds") != block[name]:
                    raise ValueError("新文件 Session 边界与当前正式日历不符")
        source_id = block["source_request_id"]
        if source_id in previous_ordinal_by_source and row["structure_ordinal"] != previous_ordinal_by_source[source_id] + 1:
            raise ValueError("来源请求跨越了未列入计划的正式结构块")
        previous_ordinal_by_source[source_id] = row["structure_ordinal"]


def import_financial_futures_file(*, write: bool = False, data_dir: Path | None = None) -> dict:
    """只消费固定 inbox。默认只校验；write=True 提交复核后删除已消化 ZIP。

    data_dir 只为调用方显式隔离本地验收而开放，CLI 不暴露任意输出路径。
    """
    data_dir = (Path(__file__).resolve().parent / "data" if data_dir is None else Path(data_dir)).resolve()
    inbox_path = data_dir / "inbox" / contract.TRANSFER_FIXED_FILE_NAME
    staging_dir = data_dir / "staging"
    warehouse_dir = data_dir / "warehouse"
    database_path = warehouse_dir / "financial_futures.duckdb"
    for path in (inbox_path.parent, staging_dir, warehouse_dir, inbox_path, database_path):
        if path.is_symlink() or not path.resolve().is_relative_to(data_dir):
            raise ValueError(f"拒绝指向数据目录外的链接路径：{path}")
    if not inbox_path.is_file():
        raise FileNotFoundError(f"请人工放入正式固定文件；不接受 PROBE：{inbox_path}")
    staging_dir.mkdir(parents=True, exist_ok=True)
    lock_path = staging_dir / "import.lock"
    # 独占创建，只释放本进程成功创建的锁；崩溃后的残留锁必须人工核查。
    with lock_path.open("x", encoding="utf-8") as lock_handle:
        json.dump({"pid": os.getpid(), "started_at": dt.datetime.now(MARKET_ZONE).isoformat()}, lock_handle)
        lock_handle.flush()
        os.fsync(lock_handle.fileno())
    snapshot_path = None
    temporary_path = None
    candidate_database_path = None
    consumed_inbox_path = None
    commit_returned = False
    commit_attempted = False
    report = {"started_at": dt.datetime.now(MARKET_ZONE).isoformat(),
              "database_path": str(database_path), "inbox_path": str(inbox_path),
              "write_requested": write, "database_write_performed": False,
              "inbox_deleted": False, "database_state_verified": False}
    try:
        descriptor, name = tempfile.mkstemp(prefix=".inbox_", suffix=".tmp", dir=staging_dir)
        temporary_path = Path(name)
        with os.fdopen(descriptor, "wb") as target_handle, inbox_path.open("rb") as inbox_handle:
            before = os.fstat(inbox_handle.fileno())
            source_sha256 = hashlib.sha256()
            copied_bytes = 0
            while chunk := inbox_handle.read(1024 * 1024):
                copied_bytes += len(chunk)
                if copied_bytes > MAX_ZIP_BYTES:
                    raise ValueError("inbox 文件超过本地 ZIP 容量门禁")
                target_handle.write(chunk)
                source_sha256.update(chunk)
            target_handle.flush()
            os.fsync(target_handle.fileno())
            # 同一打开句柄二次摘要，检测用户原地写入；不重新打开可变 inbox 路径。
            inbox_handle.seek(0)
            repeated_sha256 = hashlib.file_digest(inbox_handle, "sha256").hexdigest()
            after = os.fstat(inbox_handle.fileno())
            if ((before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
                    or copied_bytes != before.st_size or repeated_sha256 != source_sha256.hexdigest()):
                raise ValueError("复制期间 inbox 内容发生变化；保留快照现场，请人工重新运行")
        with temporary_path.open("rb") as snapshot_handle:
            snapshot_sha256 = hashlib.file_digest(snapshot_handle, "sha256").hexdigest()
        if snapshot_sha256 != source_sha256.hexdigest():
            raise ValueError("暂存副本摘要与读取流不一致")
        snapshot_path = staging_dir / (snapshot_sha256 + ".zip")
        if snapshot_path.exists() or snapshot_path.is_symlink():
            if snapshot_path.is_symlink() or not snapshot_path.is_file():
                raise ValueError("摘要快照路径不是普通文件")
            with snapshot_path.open("rb") as old_handle, temporary_path.open("rb") as new_handle:
                while True:
                    old_chunk, new_chunk = old_handle.read(1024 * 1024), new_handle.read(1024 * 1024)
                    if old_chunk != new_chunk:
                        raise ValueError("既有同名 SHA-256 快照字节不同，保留两份现场")
                    if not old_chunk:
                        break
        else:
            # 同卷硬链接是原子且不覆盖的发布；不回退到可能覆盖既有文件的替换。
            os.link(temporary_path, snapshot_path)
        temporary_path.unlink()
        temporary_path = None
        report.update(snapshot_path=str(snapshot_path), file_sha256=snapshot_sha256, file_bytes=copied_bytes)
        validated_package = validate_transfer_snapshot(snapshot_path)
        batch_record = validated_package["batch_record"]
        if batch_record["file_sha256"] != snapshot_sha256:
            raise ValueError("校验期间摘要快照被修改")
        report.update(run_id=batch_record["run_id"], publication_status=batch_record["publication_status"],
                      successful_request_count=batch_record["successful_request_count"],
                      failed_request_count=batch_record["failed_request_count"],
                      daily_record_count=batch_record["daily_record_count"], minute_record_count=batch_record["minute_record_count"])
        existing_batch = None
        if database_path.exists():
            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                connection.execute("SET TimeZone = 'Asia/Shanghai'")
                preflight = verify_database_batch(connection, validated_package, require_committed=False)
                existing_batch = preflight["existing_batch"]
            finally:
                connection.close()
        if existing_batch:
            report["status"] = "already_imported"
            report["counts"] = {name: existing_batch[name] for name in preflight["counts"]}
        else:
            # 已有同摘要批次按不可变账本重放；新文件则拒绝上游修订造成的边界漂移。
            validate_requested_calendar(validated_package["request_blocks"])
        if existing_batch is None and not write:
            report["status"] = "validated_not_imported"
            report["snapshot_retained"] = True
            report["completed_at"] = dt.datetime.now(MARKET_ZONE).isoformat()
            return report
        elif existing_batch is None:
            new_database = not database_path.exists()
            if new_database:
                candidate_database_path = staging_dir / (".database_" + uuid.uuid4().hex + ".duckdb")
            working_database_path = candidate_database_path if new_database else database_path
            connection = duckdb.connect(str(working_database_path))
            transaction_open = False
            try:
                connection.execute("SET TimeZone = 'Asia/Shanghai'")
                connection.execute("BEGIN TRANSACTION")
                transaction_open = True
                if new_database:
                    for statement in contract.database_create_table_statements():
                        connection.execute(statement)
                preflight = verify_database_batch(connection, validated_package, require_committed=False)
                if preflight["existing_batch"]:
                    raise RuntimeError("只读检查后数据库发生并发变化；停止，不自动重试")
                committed_batch = dict(batch_record, **preflight["counts"], imported_at=dt.datetime.now(MARKET_ZONE))
                column_names = [column[0] for column in contract.DATABASE_TABLE_COLUMNS["ingest_batch"]]
                connection.execute("INSERT INTO ingest_batch VALUES (" + ",".join("?" for _ in column_names) + ")",
                                   [committed_batch[name] for name in column_names])
                connection.execute("INSERT INTO fetch_observation SELECT * FROM incoming_fetch_observation")
                for table_name in ("futures_daily", "futures_minute"):
                    join_condition = " AND ".join(f"stored.{name} = incoming.{name}"
                        for name in contract.DATABASE_TABLE_PRIMARY_KEYS[table_name])
                    connection.execute(f"INSERT INTO {table_name} SELECT incoming.* FROM incoming_{table_name} incoming "
                                       f"WHERE NOT EXISTS (SELECT 1 FROM {table_name} stored WHERE {join_condition})")
                verify_database_batch(connection, validated_package, require_committed=True)
                commit_attempted = True
                connection.execute("COMMIT")
                transaction_open = False
                commit_returned = True
                report["database_write_performed"] = True
            except BaseException as transaction_error:
                if transaction_open:
                    try:
                        connection.execute("ROLLBACK")
                    except BaseException as rollback_error:
                        transaction_error.add_note(f"回滚未确认：{rollback_error}；必须保留现场核查账本。")
                raise
            finally:
                connection.close()
            # 首库先在暂存路径复读，再原子无覆盖安装；失败不留下正式空库。
            connection = duckdb.connect(str(working_database_path), read_only=True)
            try:
                connection.execute("SET TimeZone = 'Asia/Shanghai'")
                verify_database_batch(connection, validated_package, require_committed=True)
            finally:
                connection.close()
            if new_database:
                if Path(str(candidate_database_path) + ".wal").exists():
                    raise RuntimeError("候选库仍有 WAL，不能安装为单文件库")
                warehouse_dir.mkdir(parents=True, exist_ok=True)
                os.link(candidate_database_path, database_path)
            connection = duckdb.connect(str(database_path), read_only=True)
            try:
                connection.execute("SET TimeZone = 'Asia/Shanghai'")
                verified = verify_database_batch(connection, validated_package, require_committed=True)
            finally:
                connection.close()
            report["status"] = "imported"
            report["counts"] = {name: verified["existing_batch"][name] for name in preflight["counts"]}
        report["database_state_verified"] = True
        report["rows_inserted_this_invocation"] = {
            "daily": report["counts"]["daily_inserted_count"] if report["database_write_performed"] else 0,
            "minute": report["counts"]["minute_inserted_count"] if report["database_write_performed"] else 0,
        }
        if write:
            # 先原子移走当前目录项，再核对并删除这个确定文件；不能 hash(path) 后直接
            # unlink(path)，否则用户在两步间下载的新文件可能被误删。不同内容必须恢复/留存。
            consumed_inbox_path = staging_dir / (".consumed_inbox_" + uuid.uuid4().hex + ".zip")
            os.rename(inbox_path, consumed_inbox_path)
            report["consumed_inbox_path"] = str(consumed_inbox_path)
            with consumed_inbox_path.open("rb") as consumed_handle:
                consumed_sha256 = hashlib.file_digest(consumed_handle, "sha256").hexdigest()
            if consumed_sha256 != snapshot_sha256:
                try:
                    os.link(consumed_inbox_path, inbox_path)  # 不覆盖可能又下载的新文件。
                except FileExistsError:
                    raise RuntimeError("入库期间 inbox 被替换，另一新文件也已占用入口；两份新文件均保留，停止核查。")
                consumed_inbox_path.unlink()
                consumed_inbox_path = None
                raise RuntimeError("入库期间 inbox 被替换；新文件已恢复且未删除，请重新从头运行 Notebook。")
            consumed_inbox_path.unlink()
            consumed_inbox_path = None
            report["inbox_deleted"] = True
            report["consumed_inbox_path"] = None
        if candidate_database_path is not None:
            candidate_database_path.unlink()
            candidate_database_path = None
        snapshot_path.unlink()
        report["counts_scope"] = "stored_batch_ledger_not_this_invocation"
        report["snapshot_retained"] = False
        report["completed_at"] = dt.datetime.now(MARKET_ZONE).isoformat()
        return report
    except BaseException as error:
        # COMMIT 之后的磁盘/复读错误不能谎称回滚；现场保留，重跑须完整核验账本。
        report.update(status="failed", failed_at=dt.datetime.now(MARKET_ZONE).isoformat(),
            error_type=type(error).__name__, error_message=str(error),
            error_notes=getattr(error, "__notes__", []),
            commit_attempted=commit_attempted, commit_returned=commit_returned,
            snapshot_path=str(snapshot_path) if snapshot_path else None,
            temporary_path=str(temporary_path) if temporary_path else None,
            candidate_database_path=str(candidate_database_path) if candidate_database_path else None)
        report["consumed_inbox_path"] = str(consumed_inbox_path) if consumed_inbox_path else None
        error_path = staging_dir / ("error_" + dt.datetime.now(MARKET_ZONE).strftime("%Y%m%dT%H%M%S.%f%z") + "_" + uuid.uuid4().hex + ".json")
        with error_path.open("xb") as error_handle:
            error_handle.write(canonical_bytes(report))
            error_handle.flush()
            os.fsync(error_handle.fileno())
        error.add_note(f"导入失败现场：{error_path}；不自动重试，文件保留/删除状态以报告为准。")
        raise
    finally:
        lock_path.unlink()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="完整校验提交复核后删除已消化 ZIP；省略时只校验并保留输入")
    arguments = parser.parse_args()
    if Path(sys.prefix).name.lower() != "latitude":
        raise RuntimeError(f"必须使用 latitude 环境；当前解释器：{sys.executable}")
    print(json.dumps(import_financial_futures_file(write=arguments.write),
                     ensure_ascii=False, indent=2, sort_keys=True))
