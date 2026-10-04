"""聚宽研究 Notebook 的金融期货取数协议第 04 轮 v1 文件探针。

将本文件全文复制到聚宽“投资研究”的一个全新 Python 3 Notebook 单元格中执行。
脚本只在 Notebook 当前工作目录写入专用探针文件
JQ_FINANCIAL_FUTURES_TRANSFER_PROBE.zip，并用同名原子替换验证覆盖语义。
它不请求行情、不读取账号信息、不修改本地数据库；执行后需手动下载最终 ZIP。
"""

import builtins as py_builtins
import datetime
import hashlib
import io
import json
import os
import platform
import sys
import time
import uuid
import zipfile


PROBE_VERSION = "joinquant_financial_futures_round_04_v1"
RUN_STARTED_AT = datetime.datetime.now().astimezone().isoformat()
RUN_ID = str(uuid.uuid4())
FIXED_FILE_NAME = "JQ_FINANCIAL_FUTURES_TRANSFER_PROBE.zip"
TEMP_FILE_NAME = ".JQ_FINANCIAL_FUTURES_TRANSFER_PROBE.zip.tmp"
MANIFEST_MEMBER_NAME = "manifest.json"
PAYLOAD_MEMBER_NAME = "payload.jsonl"


def sha256_bytes(payload_bytes):
    return hashlib.sha256(payload_bytes).hexdigest()


def sha256_file(file_path):
    digest = hashlib.sha256()
    with open(file_path, "rb") as file_handle:
        while True:
            block = file_handle.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def build_payload_bytes(phase):
    payload_lines = []
    for sequence_number in range(2000):
        payload_lines.append(
            json.dumps(
                {
                    "bar_at": "2024-06-28T09:{:02d}:00+08:00".format(
                        31 + sequence_number % 29
                    ),
                    "close": 3500.0 + float(sequence_number % 17) / 10.0,
                    "contract_code": "IF2409.CCFX",
                    "phase": phase,
                    "sequence_number": sequence_number,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    return ("\n".join(payload_lines) + "\n").encode("utf-8")


def build_package_bytes(phase, compression_type):
    payload_bytes = build_payload_bytes(phase)
    manifest = {
        "format": "joinquant_financial_futures_transfer_probe",
        "format_version": 1,
        "phase": phase,
        "probe_version": PROBE_VERSION,
        "run_id": RUN_ID,
        "run_started_at": RUN_STARTED_AT,
        "payload_member_name": PAYLOAD_MEMBER_NAME,
        "payload_bytes": py_builtins.len(payload_bytes),
        "payload_sha256": sha256_bytes(payload_bytes),
    }
    manifest_bytes = (
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")

    package_buffer = io.BytesIO()
    with zipfile.ZipFile(package_buffer, mode="w") as zip_file:
        for member_name, member_bytes in [
            (MANIFEST_MEMBER_NAME, manifest_bytes),
            (PAYLOAD_MEMBER_NAME, payload_bytes),
        ]:
            member_info = zipfile.ZipInfo(
                member_name,
                date_time=(2020, 1, 1, 0, 0, 0),
            )
            member_info.compress_type = compression_type
            member_info.external_attr = 0o644 << 16
            zip_file.writestr(member_info, member_bytes)

    return package_buffer.getvalue(), manifest


def file_identity(file_path):
    if not os.path.isfile(file_path):
        return {
            "exists": False,
            "bytes": None,
            "sha256": None,
        }
    return {
        "exists": True,
        "bytes": int(os.path.getsize(file_path)),
        "sha256": sha256_file(file_path),
    }


working_directory = os.path.abspath(os.getcwd())
fixed_file_path = os.path.join(working_directory, FIXED_FILE_NAME)
temp_file_path = os.path.join(working_directory, TEMP_FILE_NAME)

report = {
    "probe_version": PROBE_VERSION,
    "run_started_at": RUN_STARTED_AT,
    "run_id": RUN_ID,
    "environment": {
        "python_version": sys.version,
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "local_timezone": str(datetime.datetime.now().astimezone().tzinfo),
        "working_directory": working_directory,
        "zip_deflated_available": zipfile.zlib is not None,
    },
    "fixed_file_name": FIXED_FILE_NAME,
    "fixed_file_path": fixed_file_path,
    "temp_file_name": TEMP_FILE_NAME,
    "preexisting_fixed_file": file_identity(fixed_file_path),
    "steps": [],
}

probe_started_at = time.perf_counter()
old_package_bytes = None
latest_package_bytes = None
stored_package_bytes = None
latest_manifest = None

try:
    old_package_bytes, old_manifest = build_package_bytes(
        "old_content_to_be_replaced",
        zipfile.ZIP_DEFLATED,
    )
    latest_package_bytes, latest_manifest = build_package_bytes(
        "latest_content",
        zipfile.ZIP_DEFLATED,
    )
    stored_package_bytes, _ = build_package_bytes(
        "latest_content",
        zipfile.ZIP_STORED,
    )
    report["steps"].append(
        {
            "step": "build_packages_in_memory",
            "status": "ok",
            "old_package_bytes": py_builtins.len(old_package_bytes),
            "old_package_sha256": sha256_bytes(old_package_bytes),
            "latest_deflated_bytes": py_builtins.len(latest_package_bytes),
            "latest_deflated_sha256": sha256_bytes(latest_package_bytes),
            "latest_stored_bytes": py_builtins.len(stored_package_bytes),
            "compression_reduced_size": (
                py_builtins.len(latest_package_bytes)
                < py_builtins.len(stored_package_bytes)
            ),
        }
    )

    with open(fixed_file_path, "wb") as fixed_file:
        fixed_file.write(old_package_bytes)
        fixed_file.flush()
        os.fsync(fixed_file.fileno())
    old_disk_identity = file_identity(fixed_file_path)
    old_write_matches_memory = (
        old_disk_identity["sha256"] == sha256_bytes(old_package_bytes)
    )
    report["steps"].append(
        {
            "step": "write_old_content_to_fixed_name",
            "status": "ok" if old_write_matches_memory else "mismatch",
            "disk_identity": old_disk_identity,
        }
    )

    with open(temp_file_path, "wb") as temp_file:
        temp_file.write(latest_package_bytes)
        temp_file.flush()
        os.fsync(temp_file.fileno())
    temp_disk_identity = file_identity(temp_file_path)
    if temp_disk_identity["sha256"] != sha256_bytes(latest_package_bytes):
        raise RuntimeError("临时文件落盘摘要与内存中的最新包不一致")
    report["steps"].append(
        {
            "step": "write_and_verify_latest_temp_file",
            "status": "ok",
            "disk_identity": temp_disk_identity,
        }
    )

    os.replace(temp_file_path, fixed_file_path)
    report["steps"].append(
        {
            "step": "atomic_replace_fixed_name",
            "status": "ok",
            "temp_exists_after_replace": os.path.exists(temp_file_path),
        }
    )

    final_disk_identity = file_identity(fixed_file_path)
    with open(fixed_file_path, "rb") as fixed_file:
        final_disk_bytes = fixed_file.read()

    with zipfile.ZipFile(fixed_file_path, mode="r") as zip_file:
        bad_member = zip_file.testzip()
        member_names = zip_file.namelist()
        manifest_from_disk = json.loads(
            zip_file.read(MANIFEST_MEMBER_NAME).decode("utf-8")
        )
        payload_from_disk = zip_file.read(PAYLOAD_MEMBER_NAME)
        member_details = [
            {
                "name": member_info.filename,
                "compression_type": int(member_info.compress_type),
                "compressed_bytes": int(member_info.compress_size),
                "uncompressed_bytes": int(member_info.file_size),
                "crc32": int(member_info.CRC),
            }
            for member_info in zip_file.infolist()
        ]

    final_validation = {
        "fixed_file_exists": os.path.isfile(fixed_file_path),
        "temp_file_exists": os.path.exists(temp_file_path),
        "final_file_bytes": final_disk_identity["bytes"],
        "final_file_sha256": final_disk_identity["sha256"],
        "matches_latest_in_memory_bytes": final_disk_bytes == latest_package_bytes,
        "old_write_matches_in_memory_bytes": old_write_matches_memory,
        "old_and_latest_sha256_differ": (
            sha256_bytes(old_package_bytes) != sha256_bytes(latest_package_bytes)
        ),
        "zip_test_bad_member": bad_member,
        "zip_member_names": member_names,
        "zip_member_details": member_details,
        "manifest_matches_latest": manifest_from_disk == latest_manifest,
        "payload_sha256_from_disk": sha256_bytes(payload_from_disk),
        "payload_sha256_matches_manifest": (
            sha256_bytes(payload_from_disk)
            == manifest_from_disk["payload_sha256"]
        ),
        "compression_reduced_size": (
            py_builtins.len(latest_package_bytes)
            < py_builtins.len(stored_package_bytes)
        ),
    }
    validation_passed = (
        final_validation["fixed_file_exists"]
        and not final_validation["temp_file_exists"]
        and final_validation["matches_latest_in_memory_bytes"]
        and final_validation["old_write_matches_in_memory_bytes"]
        and final_validation["old_and_latest_sha256_differ"]
        and final_validation["zip_test_bad_member"] is None
        and final_validation["zip_member_names"]
        == [MANIFEST_MEMBER_NAME, PAYLOAD_MEMBER_NAME]
        and final_validation["manifest_matches_latest"]
        and final_validation["payload_sha256_matches_manifest"]
        and final_validation["compression_reduced_size"]
    )
    report["final_validation"] = final_validation
    report["status"] = "ok" if validation_passed else "validation_failed"
    report["download_instruction"] = {
        "file_name": FIXED_FILE_NAME,
        "expected_bytes": final_disk_identity["bytes"],
        "expected_sha256": final_disk_identity["sha256"],
        "action": (
            "从聚宽文件面板手动下载该固定名称 ZIP，并将原文件与本段完整输出一起返回。"
        ),
    }
except Exception as error:
    report["status"] = "error"
    report["error_type"] = type(error).__name__
    report["error_message"] = str(error)[:2000]
    report["fixed_file_after_error"] = file_identity(fixed_file_path)
finally:
    if os.path.isfile(temp_file_path):
        try:
            os.remove(temp_file_path)
            report["temp_cleanup"] = "removed"
        except Exception as cleanup_error:
            report["temp_cleanup"] = {
                "status": "error",
                "error_type": type(cleanup_error).__name__,
                "error_message": str(cleanup_error)[:2000],
            }
    else:
        report["temp_cleanup"] = "not_present"

report["elapsed_seconds"] = round(time.perf_counter() - probe_started_at, 6)

print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_04_BEGIN")
print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
print("JQ_FINANCIAL_FUTURES_PROBE_ROUND_04_END")
