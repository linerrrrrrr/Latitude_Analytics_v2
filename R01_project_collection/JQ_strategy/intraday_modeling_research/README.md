# 日内波动建模研究

## Hydra + MLflow 学习入口

- [学习教程](HYDRA_MLFLOW_LEARNING_TUTORIAL.md)。
- [主力分钟拼接 Notebook](01_01_im_main_continuous.ipynb)：使用 `latitude_env_v2` 内核逐格运行，沿用 `draft_experiments_v2` 的选约与复权逻辑。运行后生成 `02_data/derived/im_main_continuous_1m.parquet`；该输出目录由 Notebook 创建。

## 当前目录结构

```text
intraday_modeling_research/
├── README.md
├── HYDRA_MLFLOW_LEARNING_TUTORIAL.md
├── 01_01_im_main_continuous.ipynb
└── 01_research_foundation/
    └── articles/
```

`01_research_foundation/articles/` 保存文献原文与中文整理，`99_notation_sources/` 保存其余符号参考原文。
