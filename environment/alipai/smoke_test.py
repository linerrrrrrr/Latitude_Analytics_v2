r"""用本地 Python 3.13 / NumPy 2 验证 alipai 提交、云端 1+1 和 OSS 结果读取。

参考 R00_draft_collection_01/testing_11.ipynb，沿用本机 PAI 默认配置及认证。
运行前按 environment/README.md 配置 latitude_env_v2；SDK 的 NumPy 声明例外见该说明。
云端使用 PAI CPU 镜像自己的 Python；本地结果写入 results/<批次名>/smoke_result.json。

复跑：激活 latitude_env_v2，在本目录的 PowerShell 中执行：
python -B -u smoke_test.py --batch <新批次名>
"""

import argparse
import json
import re
import shlex
import sys
import time
from importlib.metadata import version
from pathlib import Path
from urllib.parse import urlparse

from pai.estimator import Estimator
from pai.image import retrieve
from pai.job import TrainingJob, TrainingJobStatus
from pai.session import setup_default_session


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", required=True, help="新批次名：字母、数字、下划线或连字符")
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", args.batch):
        parser.error("批次名须以字母或数字开头，且只含字母、数字、下划线或连字符。")
    record_path = Path(__file__).parent / "results" / args.batch / "smoke_result.json"
    if record_path.exists():
        parser.error("该批次已有 smoke 结果；请使用新批次名。")
    record_path.parent.mkdir(parents=True, exist_ok=True)
    session = setup_default_session(network="PUBLIC")
    cpu_image = retrieve("PyTorch", "latest", accelerator_type="CPU", session=session)
    cloud_code = (
        "import json,os,sys; from pathlib import Path; "
        "r={'value':1+1,'cloud_python':sys.version.split()[0]}; "
        "p=Path(os.environ['PAI_OUTPUT_MODEL']); p.mkdir(parents=True,exist_ok=True); "
        "(p/'result.json').write_text(json.dumps(r),encoding='utf-8'); print(r,flush=True)"
    )
    estimator = Estimator(
        image_uri=cpu_image.image_uri,
        command="python -c " + shlex.quote(cloud_code),
        job_type="PyTorchJob",
        instance_type="ecs.c6.large",
        instance_count=1,
        max_run_time=120,
        base_job_name="alipai-py313-smoke",
        output_path=f"oss://{session.oss_bucket.bucket_name}/alipai_python313_smoke/",
        session=session,
    )
    job = estimator.fit(wait=False)
    print(job.id, job.console_uri, flush=True)
    record = {
        "local_python": sys.version.split()[0], "numpy": version("numpy"),
        "alipai": version("alipai"), "job_id": job.id, "console_url": job.console_uri,
        "status": job.status,
    }
    record_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    terminal_statuses = {*TrainingJobStatus.completed_status(), TrainingJobStatus.CreateFailed}
    try:
        deadline = time.monotonic() + 480
        while job.status not in terminal_statuses:
            if time.monotonic() >= deadline:
                raise TimeoutError("等待云任务超过 8 分钟，将请求停止。")
            time.sleep(5)
            job = TrainingJob.get(job.id, session=session)
            print(job.status, flush=True)
        if job.status != TrainingJobStatus.Succeed:
            raise RuntimeError(f"{job.status}: {job.reason_message}")
        output_uri = urlparse(job.output_path("model"))
        output_key = output_uri.path.lstrip("/").rstrip("/") + "/result.json"
        cloud_result = json.loads(session.oss_bucket.get_object(output_key).read())
        record.update(cloud_result)
        if cloud_result["value"] != 2:
            raise AssertionError(cloud_result)
        print("云端 1 + 1 =", cloud_result["value"], flush=True)
    finally:
        record["status"] = job.status
        record_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
        if job.status not in terminal_statuses:
            session.training_job_api.acs_client.stop_training_job(job.id)
