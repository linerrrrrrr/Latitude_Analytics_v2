"""微型云端负载：验证计算与数据通道，不读取项目行情。"""

import argparse
import json
import os
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.linalg import lstsq
from scipy.stats import genhyperbolic

from notebook_functions import volatility


parser = argparse.ArgumentParser()
parser.add_argument("--mode", default="single")
parser.add_argument("--interval", default="2min")
parser.add_argument("--split", type=int, default=16)
args = parser.parse_args()
hyperparameters = json.loads((Path(os.environ["PAI_CONFIG_DIR"]) / "hyperparameters.json").read_text())
assert hyperparameters["mode"] == args.mode
assert os.environ["CHECK_MARKER"] == "latitude-capabilities"
assert os.environ["PAI_HPS_INTERVAL"] == args.interval
assert json.loads(os.environ["PAI_HPS"])["mode"] == args.mode
train_df = pd.read_parquet(os.environ["PAI_INPUT_TRAIN"])
test_df = pd.read_parquet(Path(os.environ["PAI_INPUT_TEST"]) / "test.parquet")
assert len(train_df) == args.split == 16 and len(test_df) == 8
assert train_df.timestamp.max() < test_df.timestamp.min()
rank = int(os.environ.get("RANK", "0"))
world_size = int(os.environ.get("WORLD_SIZE", "1"))
summary = {
    "mode": args.mode, "cloud_python": sys.version.split()[0],
    "versions": {p: version(p) for p in ("numpy", "pandas", "scipy", "pyarrow")},
    "train_rows": len(train_df), "test_rows": len(test_df),
    "rank": rank, "world_size": world_size, "hyperparameters": hyperparameters,
    "function_composition": float(volatility(train_df.set_index("timestamp").price, args.interval)),
    "gh_logpdf": float(genhyperbolic.logpdf(0.1, p=1, a=2, b=0)),
}
assert np.isfinite(summary["function_composition"]) and np.isfinite(summary["gh_logpdf"])

if args.mode == "single":
    coefficients = lstsq(np.c_[np.ones(len(train_df)), train_df.x], train_df.y)[0]
    test_df["prediction"] = np.c_[np.ones(len(test_df)), test_df.x] @ coefficients
    mse = float(np.mean((test_df.prediction - test_df.y) ** 2))
    assert mse < 1e-20
    model_dir = Path(os.environ["PAI_OUTPUT_MODEL"])
    checkpoint_dir = Path(os.environ["PAI_OUTPUT_CHECKPOINTS"])
    model_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    test_df.to_parquet(model_dir / "predictions.parquet", index=False)
    (checkpoint_dir / "state.json").write_text(json.dumps({"step": 1, "coefficients": coefficients.tolist()}))

    # 可选研究组件：验证 Hydra 组装配置，以及 MLflow 在云实例本地记录参数和指标。
    import hydra
    import mlflow
    from omegaconf import OmegaConf

    with tempfile.TemporaryDirectory() as temp_dir:
        config_dir = Path(temp_dir) / "config"
        config_dir.mkdir()
        (config_dir / "config.yaml").write_text("interval: 1min\nsplit: 8\n")
        with hydra.initialize_config_dir(config_dir=str(config_dir), version_base=None):
            config = hydra.compose(config_name="config", overrides=[f"interval={args.interval}", f"split={args.split}"])
        mlflow.set_tracking_uri((Path(temp_dir) / "mlruns").as_uri())
        mlflow.set_experiment("alipai-compatibility")
        with mlflow.start_run() as run:
            mlflow.log_params(OmegaConf.to_container(config))
            mlflow.log_metric("mse", mse)
        logged_run = mlflow.get_run(run.info.run_id)
        assert logged_run.data.metrics["mse"] == mse
        summary.update(hydra_config=OmegaConf.to_container(config), mlflow_metrics=logged_run.data.metrics)
    summary.update(coefficients=coefficients.tolist(), mse=mse, checkpoint_step=1)
    (model_dir / "result.json").write_text(json.dumps(summary), encoding="utf-8")
    print(f"VALIDATION_MSE={mse:.12f}", flush=True)
else:
    previous_result = json.loads((Path(os.environ["PAI_INPUT_PREVIOUS"]) / "result.json").read_text())
    checkpoint = json.loads((Path(os.environ["PAI_INPUT_CHECKPOINT"]) / "state.json").read_text())
    assert previous_result["mode"] == "single" and checkpoint["step"] == 1
    assert world_size == 2 and rank in (0, 1)
    all_rows_df = pd.concat([train_df, test_df], ignore_index=True)
    shard_df = all_rows_df.iloc[rank::world_size].copy()
    shard_df["rank"] = rank
    output_dir = Path(os.environ["PAI_OUTPUT_FEATURES"])
    output_dir.mkdir(parents=True, exist_ok=True)
    shard_df.to_parquet(output_dir / f"part_{rank}.parquet", index=False)
    summary.update(row_ids=shard_df.x.tolist(), resumed_step=checkpoint["step"] + 1)
    (output_dir / f"result_{rank}.json").write_text(json.dumps(summary), encoding="utf-8")
print("LATITUDE_VALIDATION_OK " + json.dumps(summary), flush=True)
