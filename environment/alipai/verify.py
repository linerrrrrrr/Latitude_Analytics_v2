r"""逐项验证 alipai 0.4.13；仅操作合成样本和本实验创建的云对象。

按 environment/README.md 配置 latitude_env_v2 后运行。
同一批次依次执行：python -B -u verify.py <环节> --batch <批次名>
环节：local、single、single_requirements、distributed、failure、stop、cleanup。
single 与 single_requirements 分别检查依赖文件和显式依赖参数，不会自动重试。
每次只提交一个任务，最多 2 台 ecs.c6.large；8 分钟未完成则请求停止。
结果写入 results/<批次名>/capabilities_result.json；cleanup 只删除本批次清单中的 OSS 文件。
"""

import argparse
import hashlib
import importlib
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import distribution, version
from io import BytesIO
from pathlib import Path

import nbformat
import numpy as np
import oss2
import pandas as pd
from nbconvert import PythonExporter
from packaging.requirements import Requirement
from pai.common.oss_utils import OssUriObj, upload
from pai.estimator import Estimator
from pai.image import list_images, retrieve
from pai.job import TrainingJob, TrainingJobStatus
from pai.libs.alibabacloud_paistudio20220112.models import (
    GetTrainingJobLatestMetricsRequest, ListTrainingJobMetricsRequest,
)
from pai.processor import Processor
from pai.serializers import JsonSerializer, PyTorchSerializer
from pai.session import setup_default_session


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("case", choices=["local", "single", "single_requirements", "distributed", "failure", "stop", "cleanup"])
parser.add_argument("--batch", required=True, help="批次名：字母、数字、下划线或连字符；分项使用同一批次")
args = parser.parse_args()
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", args.batch):
    parser.error("批次名须以字母或数字开头，且只含字母、数字、下划线或连字符。")
case = args.case
report_path = Path(__file__).parent / "results" / args.batch / "capabilities_result.json"
report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {
    "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8],
    "environment": {"executable": sys.executable, "python": sys.version.split()[0],
                    **{p: version(p) for p in ("alipai", "numpy", "pandas", "pyarrow")}},
    "cases": {}, "owned_oss_keys": [],
}


def save_report():
    """每次状态更新都保留一份完整、可恢复的验证记录。"""
    temporary_path = report_path.with_suffix(".tmp")
    with temporary_path.open("w", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary_path.replace(report_path)


if case in report["cases"]:
    raise SystemExit(f"{case} 已有记录；不会自动重复提交。")
report_path.parent.mkdir(parents=True, exist_ok=True)
case_record = report["cases"][case] = {"started_at": datetime.now(timezone.utc).isoformat(), "status": "running"}
save_report()
job = None
session = None
terminal_statuses = {*TrainingJobStatus.completed_status(), TrainingJobStatus.CreateFailed}
try:
    if case == "local":
        tests = {}
        for module_name in ("pai.session", "pai.estimator", "pai.processor", "pai.job", "pai.dataset", "pai.experiment", "pai.serializers"):
            importlib.import_module(module_name)
            tests[module_name] = "passed"
        array = np.array([[1.25, 2.5], [3.75, 4.0]], dtype=np.float64)
        for name, value in (("json_numpy", array), ("json_pandas", pd.DataFrame(array))):
            restored = JsonSerializer().deserialize(JsonSerializer().serialize(value))
            np.testing.assert_array_equal(restored, array)
            tests[name] = "passed"
        for name, value in (("torch_float64", array), ("torch_int64", np.array([1, 2], dtype=np.int64)),
                            ("torch_multiple_inputs", [array, array])):
            try:
                payload = PyTorchSerializer().serialize(value)
                assert len(payload) > 0
                tests[name] = {"status": "passed", "bytes": len(payload)}
            except Exception as error:
                tests[name] = {"status": "failed", "error": f"{type(error).__name__}: {error}"}
        conflicts = []
        for dependency in distribution("alipai").requires:
            requirement = Requirement(dependency)
            if requirement.marker and not requirement.marker.evaluate():
                continue
            installed = version(requirement.name)
            if installed not in requirement.specifier:
                conflicts.append({"requirement": dependency, "installed": installed})
        case_record.update(tests=tests, dependency_conflicts=conflicts, status="completed")
    else:
        session = setup_default_session(network="PUBLIC")
        bucket = session.oss_bucket
        prefix = f"alipai_py313_check/{report['run_id']}/"
        base_uri = f"oss://{bucket.bucket_name}/{prefix}"
        if case == "cleanup":
            for prior_case in report["cases"].values():
                if "job_id" in prior_case:
                    prior_job = TrainingJob.get(prior_case["job_id"], session=session)
                    assert prior_job.status in terminal_statuses, prior_job.status
                    if prior_case.get("stop_requested"):
                        listed_jobs = session.training_job_api.list(training_job_name=prior_job.training_job_name, page_size=20)
                        listed_ids = [item.get("TrainingJobId") for item in listed_jobs.items]
                        assert prior_job.id in listed_ids
                        report["job_list_verification"] = {"status": "passed", "matched_own_job": prior_job.id,
                                                           "matching_count": len(listed_ids)}
                    if prior_case.get("cloud_metrics") is not None:
                        metrics_client = session.training_job_api.acs_client
                        prior_case["metrics_recheck"] = {
                            "latest": metrics_client.get_training_job_latest_metrics(
                                prior_job.id, GetTrainingJobLatestMetricsRequest()).body.to_map().get("Metrics", []),
                            "history": metrics_client.list_training_job_metrics(
                                prior_job.id, ListTrainingJobMetricsRequest(name="validation_mse", page_size=100)
                            ).body.to_map().get("Metrics", []),
                        }
            owned_keys = set(report["owned_oss_keys"])
            owned_keys.update(item.key for item in oss2.ObjectIterator(bucket, prefix=prefix))
            removed_bytes = 0
            for key in sorted(owned_keys):
                assert key and not key.endswith("/")
                if bucket.object_exists(key):
                    removed_bytes += bucket.head_object(key).content_length
                    bucket.delete_object(key)
                assert not bucket.object_exists(key)
            assert not list(oss2.ObjectIterator(bucket, prefix=prefix))
            case_record.update(status="passed", deleted_objects=len(owned_keys), deleted_bytes=removed_bytes)
        else:
            images = list_images("PyTorch", session=session)
            cpu_image = retrieve("PyTorch", "latest", accelerator_type="CPU", session=session)
            case_record.update(image_count=len(images), image_uri=cpu_image.image_uri)
            if case == "single":
                with tempfile.TemporaryDirectory(prefix="latitude-pai-fixture-") as temp_dir:
                    temp_path = Path(temp_dir)
                    rows_df = pd.DataFrame({
                        "timestamp": pd.date_range("2026-01-01", periods=24, freq="min"),
                        "x": np.arange(24), "y": np.arange(24) * 2 + 1,
                        "price": 100 + np.arange(24) + np.sin(np.arange(24)),
                    })
                    manifest = {"instrument": "SYNTHETIC", "start": "2026-01-01T00:00:00", "rows": 24,
                                "split": 16, "source": "synthetic-v1", "representation": "price"}
                    dataset_id = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()[:16]
                    report["dataset"] = {"id": dataset_id, "spec": manifest, "files": {}}
                    for split_name, subset in (("train", rows_df.iloc[:16]), ("test", rows_df.iloc[16:])):
                        local_path = temp_path / f"{split_name}.parquet"
                        subset.to_parquet(local_path, index=False)
                        key = prefix + f"datasets/{dataset_id}/{split_name}/{local_path.name}"
                        bucket.put_object_from_file(key, str(local_path))
                        uploaded = bucket.get_object(key).read()
                        assert uploaded == local_path.read_bytes()
                        report["dataset"]["files"][split_name] = {
                            "key": key, "sha256": hashlib.sha256(uploaded).hexdigest(), "bytes": len(uploaded)}
                    code_dir = temp_path / "code"
                    code_dir.mkdir()
                    shutil.copyfile(Path(__file__).with_name("workload.py"), code_dir / "workload.py")
                    notebook = nbformat.v4.new_notebook(cells=[nbformat.v4.new_code_cell(
                        "import numpy as np\n"
                        "def resample_prices(prices, interval):\n    return prices.resample(interval).last().dropna()\n"
                        "def volatility(prices, interval):\n"
                        "    returns = np.log(resample_prices(prices, interval)).diff().dropna()\n"
                        "    return float((returns ** 2).sum())\n"
                    )])
                    exported_code, _ = PythonExporter().from_notebook_node(notebook)
                    (code_dir / "notebook_functions.py").write_text(exported_code, encoding="utf-8")
                    # 镜像 Python 3.9 的依赖与本地 Python 3.13 环境分别管理。
                    (code_dir / "requirements.txt").write_text(
                        "numpy==1.26.4\npandas==2.2.3\nscipy==1.13.1\npyarrow==19.0.1\n"
                        "hydra-core==1.3.2\nmlflow-skinny==2.22.2\n", encoding="utf-8")
                    report["oss_code_uri"] = upload(str(code_dir), base_uri + "code/", bucket=bucket)
                    train_uri = f"oss://{bucket.bucket_name}/{report['dataset']['files']['train']['key']}"
                    test_uri = f"oss://{bucket.bucket_name}/{report['dataset']['files']['test']['key'].rsplit('/', 1)[0]}/"
                    report["inputs"] = {"train": train_uri, "test": test_uri}
                    estimator = Estimator(
                        image_uri=cpu_image.image_uri, command="python workload.py $PAI_USER_ARGS",
                        source_dir=str(code_dir), instance_type="ecs.c6.large", instance_count=1,
                        hyperparameters={"mode": "single", "interval": "2min", "split": 16},
                        environments={"CHECK_MARKER": "latitude-capabilities"},
                        metric_definitions=[{"Name": "validation_mse", "Regex": r"VALIDATION_MSE=([0-9.]+)"}],
                        max_run_time=360, base_job_name="latitude-cap-single-" + report["run_id"],
                        output_path=base_uri + "single/", checkpoints_path=base_uri + "checkpoint/", session=session,
                    )
                    save_report()
                    job = estimator.fit(inputs=report["inputs"], wait=False)
            elif case == "single_requirements":
                estimator = Estimator(
                    image_uri=cpu_image.image_uri, command="python workload.py $PAI_USER_ARGS",
                    source_dir=report["oss_code_uri"], instance_type="ecs.c6.large", instance_count=1,
                    hyperparameters={"mode": "single", "interval": "2min", "split": 16},
                    environments={"CHECK_MARKER": "latitude-capabilities"},
                    requirements=["numpy==1.26.4", "pandas==2.2.3", "scipy==1.13.1", "pyarrow==19.0.1",
                                  "hydra-core==1.3.2", "mlflow-skinny==2.22.2"],
                    metric_definitions=[{"Name": "validation_mse", "Regex": r"VALIDATION_MSE=([0-9.]+)"}],
                    max_run_time=360, base_job_name="latitude-cap-deps-" + report["run_id"],
                    output_path=base_uri + "single_requirements/",
                    checkpoints_path=base_uri + "checkpoint_requirements/", session=session,
                )
                job = estimator.fit(inputs=report["inputs"], wait=False)
            elif case == "distributed":
                assert report.get("model_uri") and report.get("checkpoint_uri")
                for item in report["dataset"]["files"].values():
                    assert hashlib.sha256(bucket.get_object(item["key"]).read()).hexdigest() == item["sha256"]
                processor = Processor(
                    image_uri=cpu_image.image_uri, command="python workload.py $PAI_USER_ARGS",
                    source_dir=report["oss_code_uri"], instance_type="ecs.c6.large", instance_count=2,
                    parameters={"mode": "distributed", "interval": "3min", "split": 16},
                    requirements=["numpy==1.26.4", "pandas==2.2.3", "scipy==1.13.1", "pyarrow==19.0.1"],
                    environments={"CHECK_MARKER": "latitude-capabilities"}, max_run_time=360,
                    base_job_name="latitude-cap-distributed-" + report["run_id"], session=session,
                )
                job = processor.run(inputs={**report["inputs"], "previous": report["model_uri"],
                                            "checkpoint": report["checkpoint_uri"]},
                                    outputs={"features": base_uri + "features/"}, wait=False)
            else:
                cloud_code = ("raise RuntimeError('LATITUDE_EXPECTED_FAILURE')" if case == "failure" else
                              "import time; print('LATITUDE_READY_TO_STOP',flush=True); "
                              "[(print('VALIDATION_MSE=0.25',flush=True),time.sleep(5)) for _ in range(48)]")
                estimator = Estimator(
                    image_uri=cpu_image.image_uri, command="python -c " + shlex.quote(cloud_code),
                    instance_type="ecs.c6.large", instance_count=1, max_run_time=300,
                    metric_definitions=[{"Name": "validation_mse", "Regex": r"VALIDATION_MSE=([0-9.]+)"}],
                    base_job_name=f"latitude-cap-{case}-" + report["run_id"],
                    output_path=base_uri + case + "/", session=session,
                )
                job = estimator.fit(wait=False)
            case_record.update(job_id=job.id, console_url=job.console_uri, job_status=job.status, transitions=[])
            # SDK 本地源码上传使用独立 pai/training_src 前缀，记录实际文件以便精确清理。
            if job.algorithm_spec and job.algorithm_spec.code_dir:
                code_key = job.algorithm_spec.code_dir.location_value.key
                assert code_key.startswith(prefix) or (report["run_id"].replace("-", "_") in code_key or report["run_id"] in code_key)
                report["owned_oss_keys"].extend(item.key for item in oss2.ObjectIterator(bucket, prefix=code_key))
            save_report()
            deadline = time.monotonic() + 480
            stop_after = None
            while job.status not in terminal_statuses:
                if time.monotonic() > deadline:
                    raise TimeoutError("云任务超过 8 分钟；请求停止，不自动重试。")
                if case == "stop" and job.status == "Running" and not case_record.get("stop_requested"):
                    if stop_after is None:
                        stop_after = time.monotonic() + 30
                    elif time.monotonic() >= stop_after:
                        session.training_job_api.acs_client.stop_training_job(job.id)
                        case_record["stop_requested"] = True
                time.sleep(5)
                job = TrainingJob.get(job.id, session=session)
                if not case_record["transitions"] or case_record["transitions"][-1] != job.status:
                    case_record["transitions"].append(job.status)
                    print(case, job.id, job.status, flush=True)
                case_record["job_status"] = job.status
                save_report()
            logs = session.training_job_api.list_logs(job.id, page_size=100, page_number=1)
            case_record["log_count"] = logs.total_count
            # 保存验证标记与最后几行诊断，避免把完整依赖安装日志积累到磁盘。
            all_logs = list(logs.items)
            for page in range(2, min(20, (logs.total_count + 99) // 100) + 1):
                all_logs.extend(session.training_job_api.list_logs(job.id, page_size=100, page_number=page).items)
            case_record["log_evidence"] = [str(line) for line in all_logs if "LATITUDE_" in str(line) or "VALIDATION_MSE=" in str(line)]
            case_record["log_tail"] = [str(line) for line in all_logs[-8:]]
            if case in ("single", "single_requirements", "distributed"):
                assert job.status == "Succeed", f"{job.status}: {job.reason_message}"
                if case in ("single", "single_requirements"):
                    report["model_uri"] = estimator.model_data()
                    report["checkpoint_uri"] = estimator.checkpoints_data()
                    output_key = OssUriObj(report["model_uri"]).object_key.rstrip("/")
                    case_record["cloud_result"] = json.loads(bucket.get_object(output_key + "/result.json").read())
                    prediction_df = pd.read_parquet(BytesIO(bucket.get_object(output_key + "/predictions.parquet").read()))
                    assert len(prediction_df) == 8
                    np.testing.assert_allclose(prediction_df.prediction, prediction_df.y)
                    checkpoint_key = OssUriObj(report["checkpoint_uri"]).object_key.rstrip("/")
                    case_record["checkpoint"] = json.loads(bucket.get_object(checkpoint_key + "/state.json").read())
                    metrics = session.training_job_api.acs_client.get_training_job_latest_metrics(
                        job.id, GetTrainingJobLatestMetricsRequest(names="validation_mse"))
                    case_record["cloud_metrics"] = metrics.body.to_map().get("Metrics", [])
                    assert any("LATITUDE_VALIDATION_OK" in str(line) for line in all_logs)
                else:
                    shards = []
                    cloud_results = []
                    for rank in range(2):
                        shards.append(pd.read_parquet(BytesIO(bucket.get_object(prefix + f"features/part_{rank}.parquet").read())))
                        cloud_results.append(json.loads(bucket.get_object(prefix + f"features/result_{rank}.json").read()))
                    merged_df = pd.concat(shards).sort_values("x")
                    assert merged_df.x.tolist() == list(range(24)) and merged_df.x.is_unique
                    assert {r["rank"] for r in cloud_results} == {0, 1}
                    assert all(r["world_size"] == 2 and r["resumed_step"] == 2 for r in cloud_results)
                    case_record.update(cloud_results=cloud_results, combined_rows=len(merged_df), reused_dataset_id=report["dataset"]["id"])
            elif case == "failure":
                assert job.status == "Failed"
                assert any("LATITUDE_EXPECTED_FAILURE" in str(line) for line in all_logs)
                try:
                    job.wait(show_logs=False)
                except Exception as error:
                    assert type(error).__name__ == "UnexpectedStatusException"
                    case_record["sdk_failure_exception"] = type(error).__name__
                else:
                    raise AssertionError("SDK 没有报告云任务失败")
            else:
                assert case_record.get("stop_requested") and job.status == "Terminated"
                case_record["cloud_metrics"] = session.training_job_api.acs_client.get_training_job_latest_metrics(
                    job.id, GetTrainingJobLatestMetricsRequest()).body.to_map().get("Metrics", [])
            case_record["status"] = "passed"
except Exception as error:
    case_record.update(status="failed", error=f"{type(error).__name__}: {error}")
    raise
finally:
    if job and job.status not in terminal_statuses:
        try:
            session.training_job_api.acs_client.stop_training_job(job.id)
            case_record["stop_requested_after_error"] = True
        except Exception as stop_error:
            case_record["stop_error"] = f"{type(stop_error).__name__}: {stop_error}"
    case_record["finished_at"] = datetime.now(timezone.utc).isoformat()
    save_report()
    print(case, case_record["status"], flush=True)
