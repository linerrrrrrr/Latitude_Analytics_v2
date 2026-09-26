"""Reproduce the finite research batch, stopping on the first failed stage."""

import datetime as dt
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time


def main():
    project_dir = pathlib.Path(__file__).resolve().parent
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    run_dir = project_dir / "runs" / run_id
    run_dir.mkdir(parents=True)
    stages = [
        "prepare_data.py", "estimate_periodicity.py", "run_simulations.py",
        "run_diagnostics.py", "run_measure_comparison.py", "run_forecasting.py",
        "run_clock_forecasting.py", "run_inference_robustness.py",
        "verify_research.py", "build_report.py",
    ]
    run_record = {
        "run_id": run_id, "python": sys.executable, "status": "running",
        "scope": "Frozen IM 995-day snapshot; 6 synthetic scenarios; no API, lake write or automatic retry",
        "stages": [],
        "code_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(project_dir.glob("*.py"))
        },
    }
    pipeline_started = time.monotonic()
    for index, stage_name in enumerate(stages, start=1):
        print(f"[{index}/{len(stages)}] {stage_name}", flush=True)
        stage_started = time.monotonic()
        log_path = run_dir / stage_name.replace(".py", ".log")
        stage_status = "failed"
        error_message = None
        try:
            with log_path.open("w", encoding="utf-8") as log_file:
                stage_run = subprocess.run(
                    [sys.executable, "-X", "utf8", str(project_dir / stage_name)],
                    cwd=project_dir, stdout=log_file, stderr=subprocess.STDOUT,
                    timeout=300, check=False,
                )
            if stage_run.returncode:
                raise RuntimeError(f"{stage_name} exited {stage_run.returncode}; see {log_path}")
            stage_status = "complete"
        except Exception as exc:
            error_message = str(exc)
        run_record["stages"].append({
            "name": stage_name, "status": stage_status,
            "elapsed_seconds": round(time.monotonic() - stage_started, 3),
            "log": log_path.name, "error": error_message,
        })
        run_record["elapsed_seconds"] = round(time.monotonic() - pipeline_started, 3)
        if stage_status == "failed":
            run_record["status"] = "failed"
        elif index == len(stages):
            run_record["status"] = "complete"
        temporary_status = run_dir / "pipeline.json.tmp"
        with temporary_status.open("w", encoding="utf-8") as status_file:
            json.dump(run_record, status_file, ensure_ascii=False, indent=2)
            status_file.flush()
            os.fsync(status_file.fileno())
        temporary_status.replace(run_dir / "pipeline.json")
        print(f"  {stage_status}; {time.monotonic()-stage_started:.1f}s; total {run_record['elapsed_seconds']:.1f}s", flush=True)
        if stage_status == "failed":
            raise RuntimeError(error_message)
    (project_dir / "results/latest_run.json").write_text(
        json.dumps({"run_id": run_id, "pipeline": f"runs/{run_id}/pipeline.json", "elapsed_seconds": run_record["elapsed_seconds"]}, indent=2),
        encoding="utf-8",
    )
    print(f"Complete: {project_dir / 'FINAL_REPORT.md'}", flush=True)


if __name__ == "__main__":
    main()
