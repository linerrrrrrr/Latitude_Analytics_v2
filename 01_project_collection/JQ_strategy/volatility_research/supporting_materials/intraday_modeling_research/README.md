# IM 日内波动研究：一次性支持附件

本目录于 2026-09-08 从 JQ_strategy 收录为一次已有研究的支持附件。通用符号与方法以[主项目 README](../../README.md)为准；文件核对和路径适配记录见[附件清单](../attachment_manifest.json)。以下保留原研究的范围、入口与结果说明。

本项目独立研究日内周期估计、波动度量、波动时钟与预测之间的关系。建立日期：2026-09-07。包含公开原文、方法核对、可运行代码、受控模拟、IM 全样本实验及结果报告。运行只依赖本目录的输入快照和 Python 包，不依赖旧 Notebook、父项目配置或 API 账号。

**通用符号与方法先读 [独立波动率项目 README](../../README.md)**；本目录的[符号约定与推导](research/03_method_derivations.md)保留 IM 实现适配、代码映射和实验所用定义。文献依据见[跨文献符号核对](literature/notes/07_notation_survey.md)。原工作目录仍保留；本附件不自动同步其后续变更。

**实证结果先读 [FINAL_REPORT.md](FINAL_REPORT.md)**；交互式浏览使用 [00_research_overview.ipynb](00_research_overview.ipynb)。这两个入口直接对应已经运行的结果。

## 一键复现

在此目录打开 Terminal，运行：

```powershell
& 'E:\anaconda3\envs\latitude\python.exe' -X utf8 .\run_all.py
```

标准环境为 `latitude`，Python 3.11.15；依赖版本见 [requirements.txt](requirements.txt)。所有入口按自身文件位置定位项目，因此也可以从其他目录传入 `run_all.py` 的绝对路径。入口串行运行固定批次、首次失败即停止，逐阶段日志保存在 `runs/<run_id>/`；它不创建后台服务、定时任务或自动重试。历史输入和业务文件不会被改写。

本轮输入为2022-07-25至2026-08-28的995个交易日、238,800条IM分钟。2025用于验证选窗，2026是此前已经被观察过的回顾评估段，不称为全新测试。研究没有计算交易收益。

## 从这里开始

1. [文献目录与原文](literature/README.md)：6 篇指定论文的正式引用、实际取得的版本、阅读笔记及本地 PDF。
2. [问题地图](research/01_problem_map.md)：哪些现象需要什么证据，哪些结论暂时不能下。
3. [现有 Notebook 对照](research/02_existing_workflow.md)：当前代码真正计算的对象、时点和诊断口径。
4. [符号约定与方法推导](research/03_method_derivations.md)：统一数学符号、文献映射、代码对应及尺度关系。
5. [实验路线与覆盖](experiments/README.md)：研究单元、实际入口、对照组、评价方法和已交付产物。
6. [待解问题](research/04_open_questions.md)与[结果索引](results/README.md)：实证进展和科学解释边界。

## 当前最有用的判断

- v2 的研究对象是“每个 Session 首分钟 RV、其余分钟 BPV”的混合贡献。开盘分钟与普通分钟的分布差异，可能部分来自统计量本身。
- 当前逐 slot 中位数估计的是归一化贡献的典型形状。它与对标准化收益估计稳健尺度、再构造周期因子的路线有区别。
- 用 $f_{t,i}$ 表示周期标准差因子，方差因子为 $f_{t,i}^2$；调整收益后重算 BPV 的分母是 $\widehat f_{t,i}\widehat f_{t,i-1}$，一般不能直接换成 $\widehat f_{t,i}^2$。
- 当前诊断中的 $z_{t,i}^{\mathrm{mix}}$ 还除以目标日全日均值，适合事后研究形状；盘中使用时，需要另行定义当时可取得的尺度。
- “中位数较平、IQR/Q95 有时段结构”不足以认定固定周期失效。实验中，分布摘要更平的窗口与预测较好的窗口确实不同。
- 周期方法相对于无周期基准的收益，大于新估计器相对于已有贡献中位数的增量。日级HAR/HARP没有得到一致改善证据；完整数值、失败案例及区间均保留。

以上前三项中的本地代码事实和代数推导见对应研究文档；文献的具体主张以逐篇笔记的原文定位为准。

## 目录内容

```text
intraday_modeling_research/
  README.md / FINAL_REPORT.md
  00_research_overview.ipynb   只读结果浏览Notebook
  run_all.py                 固定批次复现入口
  data_contracts.py          本项目Arrow契约
  estimators.py              模拟/实证共用估计器
  prepare_data.py / estimate_periodicity.py
  run_simulations.py / run_diagnostics.py
  run_measure_comparison.py / run_forecasting.py
  run_clock_forecasting.py / run_inference_robustness.py
  verify_research.py / build_report.py
  requirements.txt
  data/input/                独立输入快照及来源指纹
  data/minute_observations.parquet
  literature/
    README.md                 文献导航与获取状态
    references.bib            可导入引用管理器的书目
    source_manifest.json      原文来源、版本、摘要校验和阅读范围
    papers/                   公开取得的原文 PDF
    notes/                    六篇中文笔记及各自来源记录
  research/
    01_problem_map.md
    02_existing_workflow.md
    03_method_derivations.md
    04_open_questions.md
  experiments/README.md       实验设计与实施覆盖
  results/                   逐点结果、CSV、图、分项报告和验证证据
  runs/                      每次复现的阶段日志与代码指纹
  sources/
    user_literature_brief.txt  用户提供的原始线索，保留以便核对
    notebook_snapshot.json     本次阅读的 Notebook 文件指纹与单元定位
  research_log.md             本次工作及完成边界
```

## 与已有项目的关系

建立项目时阅读了 [v2 季节性估计](../../../draft_experiments_v2/02_01_im_bpv_intraday_seasonality.ipynb)和[v2 窗口诊断](../../../draft_experiments_v2/02_02_im_bpv_window_comparison.ipynb)，用于把已有贡献中位数作为对照。[v1 时钟实验](../../../draft_experiments_v1/03_01_im_clock_resampling.ipynb)与[v1 15 分钟预测](../../../draft_experiments_v1/04_04_im_15m_volatility_forecasting.ipynb)仅作为背景。以上均不是本项目的运行依赖。2026-09-08 根据用户要求同步了两本 v2 去季节 Notebook 的数学说明，代码和输出保持原样；v1 未被改写或恢复。

本目录的数学符号约定仅适用于本研究与上述 v2 去季节流程，已由根级规范索引路由，不替代根级 [AGENTS.md](../../../../../AGENTS.md)、[环境模板](../../../../../.env.template)或[金融期货数据目录说明](../../../financial_futures_data/README.md)。2026-09-07 根据用户“完全独立项目、做全实验”的要求建立代码和结果；2026-09-08 补充跨文献符号依据与局部符号约定，仅在根级索引增加路由。原输入、数值计算及正式湖不受影响。局部研究Schema仅由本目录 [data_contracts.py](data_contracts.py)及对应输出入口定义，不进入稳定silver契约。
