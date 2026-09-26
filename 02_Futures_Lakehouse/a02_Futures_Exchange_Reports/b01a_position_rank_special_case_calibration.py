#!/usr/bin/env python
# coding: utf-8

# # 期货成交持仓排名特殊案例校准证据
# 
# 该正式环节位于报告日历之后、通用持仓采集之前。它只处理已人工核实并冻结在共享配置中的特殊案例：下载上期所官方历史文件，验证完整响应 SHA-256 和目标 Top 20 逐值内容，再把原始响应、摘要和校准清单原子归档到正式 raw。
# 
# 本环节不读取 JQData、不写 silver，也不把未知异常自动归入特殊情况。通用持仓入口只有在 JQData 坏数据指纹和本环节证据同时精确匹配时才应用校准。

# In[ ]:


from __future__ import annotations

import hashlib
import json
import os
import pathlib
import shutil
import sys
import uuid

# Notebook 可以从项目任意子目录启动；根目录定位方法由 .env.template 统一规定。
project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        sys.path.insert(0, str(candidate_root / "02_Futures_Lakehouse"))
        break
else:
    raise RuntimeError("未找到项目根目录")

import click
import requests

from config.futures_lakehouse.futures_position_rank_special_cases import (
    POSITION_RANK_SPECIAL_CASES,
)
from config.settings import settings

ARTIFACT_FILENAMES = {
    "response.dat",
    "response.sha256",
    "calibration.json",
}


# ## raw 归档样例
# 只查看已有文件的状态、大小和已保存摘要；空库显示尚未归档。

# In[ ]:


if "ipykernel" in sys.modules and "__file__" not in globals():
    from notebook_schema_browser import display_raw_archive_demo

    display_raw_archive_demo(
        settings.futures_lake_root / "raw",
        filenames=sorted(ARTIFACT_FILENAMES),
        archive_options={
            str(case["case_id"]): pathlib.Path(str(case["raw_relative_path"]))
            for case in POSITION_RANK_SPECIAL_CASES
        },
    )


# ## 官方响应解析与冻结清单
# 
# 完整响应必须与配置中的 SHA-256 一致；目标合约成交量排名必须恰好覆盖 1—20 名、按指标非递增，并与冻结的官方校准值逐项一致。

# In[ ]:


def parse_official_volume_rows(
    response_content: bytes,
    special_case: dict[str, object],
) -> tuple[tuple[int, str, int, int], ...]:
    try:
        payload = json.loads(response_content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("上期所特殊案例响应不是有效 UTF-8 JSON。") from error

    source_rows = payload.get("o_cursor")
    if not isinstance(source_rows, list):
        raise ValueError("上期所特殊案例响应缺少 o_cursor 列表。")

    official_rows = []
    for source_row in source_rows:
        if not isinstance(source_row, dict):
            raise ValueError("上期所特殊案例响应包含非对象记录。")
        instrument_id = str(source_row.get("INSTRUMENTID", "")).strip().upper()
        if instrument_id != special_case["official_instrument_id"]:
            continue
        rank = source_row.get("RANK")
        if isinstance(rank, bool) or not isinstance(rank, int) or not 1 <= rank <= 20:
            continue
        member_name = str(source_row.get("PARTICIPANTABBR1", "")).strip()
        indicator = source_row.get("CJ1")
        indicator_increase = source_row.get("CJ1_CHG")
        if (
            not member_name
            or isinstance(indicator, bool)
            or not isinstance(indicator, int)
            or indicator < 0
            or isinstance(indicator_increase, bool)
            or not isinstance(indicator_increase, int)
        ):
            raise ValueError("上期所特殊案例 Top 20 字段或类型不合法。")
        official_rows.append((rank, member_name, indicator, indicator_increase))

    official_rows = tuple(sorted(official_rows))
    if tuple(row[0] for row in official_rows) != tuple(range(1, 21)):
        raise ValueError("上期所特殊案例未精确覆盖成交量第 1—20 名。")
    indicators = [row[2] for row in official_rows]
    if any(left < right for left, right in zip(indicators, indicators[1:])):
        raise ValueError("上期所特殊案例成交量没有按名次非递增。")
    if official_rows != special_case["official_rows"]:
        raise ValueError("上期所特殊案例 Top 20 与冻结校准值不一致。")
    return official_rows


def calibration_manifest(
    special_case: dict[str, object],
    response_sha256: str,
    official_rows: tuple[tuple[int, str, int, int], ...],
) -> dict[str, object]:
    return {
        "case_id": special_case["case_id"],
        "trading_date": special_case["trading_date"].isoformat(),
        "exchange_code": special_case["exchange_code"],
        "underlying_code": special_case["underlying_code"],
        "source_symbol": special_case["source_symbol"],
        "rank_type_id": special_case["rank_type_id"],
        "rank_type": special_case["rank_type"],
        "official_url": special_case["official_url"],
        "official_response_sha256": response_sha256,
        "official_rows": [list(row) for row in official_rows],
    }


def validate_response(
    response_content: bytes,
    special_case: dict[str, object],
) -> dict[str, object]:
    response_sha256 = hashlib.sha256(response_content).hexdigest()
    if response_sha256 != special_case["official_response_sha256"]:
        raise ValueError(
            "上期所特殊案例完整响应 SHA-256 与冻结值不一致；"
            f"actual={response_sha256}。"
        )
    official_rows = parse_official_volume_rows(response_content, special_case)
    return calibration_manifest(special_case, response_sha256, official_rows)


def verify_artifacts(
    artifact_path: pathlib.Path,
    special_case: dict[str, object],
) -> dict[str, object]:
    actual_filenames = {path.name for path in artifact_path.iterdir()}
    if actual_filenames != ARTIFACT_FILENAMES:
        raise ValueError(
            f"特殊案例证据文件集合不一致：{sorted(actual_filenames)}。"
        )
    response_content = (artifact_path / "response.dat").read_bytes()
    expected_manifest = validate_response(response_content, special_case)
    sidecar_sha256 = (artifact_path / "response.sha256").read_text(
        encoding="ascii"
    ).strip()
    if sidecar_sha256 != expected_manifest["official_response_sha256"]:
        raise ValueError("特殊案例响应摘要 sidecar 不一致。")
    actual_manifest = json.loads(
        (artifact_path / "calibration.json").read_text(encoding="utf-8")
    )
    if actual_manifest != expected_manifest:
        raise ValueError("特殊案例校准清单与正式原文或冻结配置不一致。")
    return actual_manifest


# ## CLI：验证并原子归档正式 raw 证据
# 
# 不带 `--write` 时下载并验证但不落盘；带 `--write` 时先构造完整 candidate、复读成功后再安装整个案例目录。已有正式证据必须原样复读，任何不一致都停止，不覆盖。

# In[ ]:


@click.command()
@click.option("--lake-root", type=click.Path(path_type=pathlib.Path))
@click.option("--write", is_flag=True)
def main(lake_root: pathlib.Path | None, write: bool) -> None:
    resolved_lake_root = (lake_root or settings.futures_lake_root).resolve()
    raw_root = resolved_lake_root / "raw"
    click.echo(
        f"special_case_count={len(POSITION_RANK_SPECIAL_CASES)}; "
        f"lake_root={resolved_lake_root}; write={str(write).lower()}"
    )

    for special_case in POSITION_RANK_SPECIAL_CASES:
        artifact_path = raw_root / special_case["raw_relative_path"]
        if not artifact_path.resolve().is_relative_to(raw_root.resolve()):
            raise ValueError("特殊案例证据路径越出 raw 根目录。")
        if artifact_path.exists():
            manifest = verify_artifacts(artifact_path, special_case)
            click.echo(
                "special_case_existing_valid: "
                f"case_id={special_case['case_id']}; "
                f"sha256={manifest['official_response_sha256']}"
            )
            continue

        response = requests.get(special_case["official_url"], timeout=60)
        if response.status_code != 200:
            raise RuntimeError(
                "上期所特殊案例请求失败；"
                f"case_id={special_case['case_id']}, status={response.status_code}。"
            )
        manifest = validate_response(response.content, special_case)
        click.echo(
            "special_case_source_valid: "
            f"case_id={special_case['case_id']}; rows=20; "
            f"sha256={manifest['official_response_sha256']}"
        )
        if not write:
            continue

        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        staging_path = artifact_path.parent / (
            f".{special_case['case_id']}.staging-{uuid.uuid4().hex}"
        )
        try:
            staging_path.mkdir(parents=False, exist_ok=False)
            (staging_path / "response.dat").write_bytes(response.content)
            (staging_path / "response.sha256").write_text(
                manifest["official_response_sha256"] + "\n",
                encoding="ascii",
                newline="\n",
            )
            (staging_path / "calibration.json").write_text(
                json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            verify_artifacts(staging_path, special_case)
            if artifact_path.exists():
                raise RuntimeError("特殊案例正式证据目录在提交前并发出现。")
            os.replace(staging_path, artifact_path)
            verify_artifacts(artifact_path, special_case)
        except Exception:
            shutil.rmtree(staging_path, ignore_errors=True)
            raise
        click.echo(
            "special_case_committed: "
            f"case_id={special_case['case_id']}; path={artifact_path}"
        )

    click.echo("position_rank_special_cases_ready: true")


# ## Notebook 与脚本运行入口

# In[ ]:


if "ipykernel" in sys.modules:
    main.main(
        args=[],
        prog_name="b01a_position_rank_special_case_calibration",
        standalone_mode=False,
    )
elif __name__ == "__main__":
    main()

