"""Build the reviewable research report and a local results notebook."""

import datetime as dt
import hashlib
import json
import pathlib
import sys
import tempfile

import nbformat
import pandas as pd
from nbclient import NotebookClient
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager


def main():
    project_dir = pathlib.Path(__file__).resolve().parent
    results_dir = project_dir / "results"
    data_evidence = json.loads((results_dir / "data/input_validation.json").read_text(encoding="utf-8"))
    simulation_evidence = json.loads((results_dir / "simulations/run_metadata.json").read_text(encoding="utf-8"))
    forecast_evidence = json.loads((results_dir / "forecasting/provenance.json").read_text(encoding="utf-8"))
    verification = json.loads((results_dir / "verification.json").read_text(encoding="utf-8"))
    forecast_summary_df = pd.read_csv(results_dir / "forecasting/forecast_summary.csv")
    forecast_table_df = forecast_summary_df.loc[
        (forecast_summary_df.year == 2026) & (forecast_summary_df.state == "all")
        & (forecast_summary_df.target == "RV15") & forecast_summary_df.selected_on_2025,
        ["model", "method", "lookback_days", "qlike_difference_vs_none",
         "qlike_difference_ci_low", "qlike_difference_ci_high", "mse_ratio_vs_none"],
    ].rename(columns={
        "model": "预测器", "method": "周期方法", "lookback_days": "窗口",
        "qlike_difference_vs_none": "QLIKE差", "qlike_difference_ci_low": "区间下界",
        "qlike_difference_ci_high": "区间上界", "mse_ratio_vs_none": "MSE比",
    })
    distribution_summary_df = pd.read_csv(results_dir / "diagnostics/distribution_summary.csv")
    diagnostic_table_df = distribution_summary_df.loc[
        (distribution_summary_df.split == "retrospective_2026")
        & (distribution_summary_df["sample"] == "all_method_common")
        & (distribution_summary_df.variable == "bpv_contribution")
        & (((distribution_summary_df.method == "none") & (distribution_summary_df.lookback_days == 0))
           | ((distribution_summary_df.method.isin(["median_mixed", "boudt_wsd", "fff_wsd"])) & (distribution_summary_df.lookback_days == 120))
           | ((distribution_summary_df.method == "state_wsd") & (distribution_summary_df.lookback_days == 360))),
        ["method", "lookback_days", "median_profile_cv", "q95_profile_cv"],
    ].rename(columns={"method": "方法", "lookback_days": "窗口", "median_profile_cv": "中位曲线CV", "q95_profile_cv": "Q95曲线CV"})
    assert len(diagnostic_table_df) == 5, "Representative diagnostic rows missing"
    daily_har_df = pd.read_csv(results_dir / "forecasting/daily_har_summary.csv")
    daily_har_table = daily_har_df.loc[
        (daily_har_df.year == 2026) & daily_har_df.model.str.startswith("HARP"),
        ["horizon_days", "days", "qlike_difference_vs_har", "qlike_difference_ci_low", "qlike_difference_ci_high"],
    ]
    clock_summary_df = pd.read_csv(results_dir / "clock_forecasting/summary.csv")
    pairwise_path = results_dir / "forecasting/selected_method_comparison.csv"
    pairwise_text = ""
    if pairwise_path.exists():
        pairwise_df = pd.read_csv(pairwise_path)
        relevant_pairs = pairwise_df[(pairwise_df.target == "RV15") & (pairwise_df.model == "ridge_intraday")]
        pairwise_table = relevant_pairs[["method", "lookback_days", "baseline_lookback_days", "qlike_difference_vs_median_mixed", "qlike_difference_ci_low", "qlike_difference_ci_high", "mse_ratio_vs_median_mixed"]]
        pairwise_text = "\n**新旧方法的直接比较更克制。** Boudt相对于验证选窗后的既有混合贡献中位数，QLIKE增益远小于相对于无周期基准的增益，MSE差的区间包含零。不能把相对无周期的改善幅度当作替换现有方案的收益。\n\n" + pairwise_table.to_markdown(index=False, floatfmt=".6f") + "\n\n完整比较见 [selected_method_comparison.csv](results/forecasting/selected_method_comparison.csv)，区间敏感性见[推断稳健性](results/inference_robustness/REPORT.md)。\n"

    report_lines = [
        "# 日内波动建模：文献、模拟与 IM 实证报告", "",
        f"生成日期：{dt.datetime.now().date().isoformat()}。本报告由本项目实际运行的结果文件生成。",
        "",
        "## 结论先行", "",
        "本轮研究支持保留日内周期信息，但不支持把‘残差曲线更平’直接当作选择估计器或窗口的目标。模拟、分布诊断与预测评价回答不同问题。",
        "",
        "1. **统计量定义是第一层差异。** 当前混合贡献在每个 Session 首分钟使用 RV，其余使用 BPV；即使没有周期、跳跃或波动状态变化，两类统计量也可有不同尾部。",
        "2. **收益尺度和 BPV 配对尺度不同。** 周期标准差因子为 $f_{t,i}$，方差因子为 $f_{t,i}^2$；BPV 配对尺度为 $f_{t,i}f_{t,i-1}$。局部滤波和日级标量修正不等价；符号与假设见[统一约定](research/03_method_derivations.md)。",
        "3. **稳健和平滑各有代价。** WSD 在指定跳跃机制下改善曲线恢复；平滑在真实尖锐结构下会增加偏差。周期突变模拟中长窗口适应较慢。",
        "4. **预测结果不能从图形外推。** 2025 验证选择的多数 ridge 周期窗口是360日，2026回顾预测优于同模型无周期基准；这与短窗让部分分布摘要更平并不矛盾。",
        "5. **负结果保留。** 日级 HAR/HARP 改编没有提供跨期限一致的改善证据。本文未估计交易收益、成本或实盘可执行优势。",
        "6. **时钟闭环暴露了模型约束问题。** 初始事件线性回归出现负预测，极小正值下限放大QLIKE；保留失败后追加非负系数ridge消除了触底。稳定版本仍未明显优于直接分钟信息基线，时钟互比也没有明确优势。",
        pairwise_text,
        "## 数据、独立性与评价范围", "",
        f"本地冻结输入为 IM 原始合约 open/close 等字段，{data_evidence['first_date']} 至 {data_evidence['last_date']}，{data_evidence['days']} 个交易日、{data_evidence['rows']:,} 个分钟。每日上午/下午各120分钟，共有 {data_evidence['zero_returns']:,} 个零收益；两段首分钟不构造跨边界BPV。",
        "",
        "项目运行只读取自己的 `data/input` 快照，不导入旧 Notebook 或外部项目配置，不请求 API，不写正式湖。原始导入来源和SHA-256仍保留，因此独立运行不等于数据来源不可追溯。使用原始价格避免双向复权的全快照价格变换；选主力信号日期严格早于交易日，但本项目未重建整套上游选择流程。",
        "",
        f"截至2024年末用于初始训练；2025年验证；2026年作为回顾评估。未来15分钟预测每天182个共同起点（09:45–11:15、13:15–14:45），2025共{forecast_evidence['common_validation_origins']:,}个，2026共{forecast_evidence['common_retrospective_origins']:,}个。这不是旧实验208起点的复制：这里要求完整15分钟盘中历史。2026在此前研究中已经被观察，不能称为全新未触及测试。",
        "",
        "RV15是未来15个一分钟收益平方之和；BPV15是未来15个分钟右端点的贡献之和，首项含已知origin收益，并非仅在15根区间收益内构造14对乘积。所有方法使用相同标签。",
        "",
        "## 文献已核对到什么程度", "",
        "2026-09-07 实验使用的六篇指定论文及一篇 Metrika 辅助文献共7份PDF、261页。2026-09-08 为统一符号另行检索并归档8篇原始论文，仅定向核对相关方法页和公式；当前共15份PDF。新增资料的版本、页码、符号差异及未核式线索见[跨文献符号核对](literature/notes/07_notation_survey.md)。以下仍是原实验的实现对应关系，不代表新增8篇的方法已经实现或实验被重新运行。", "",
        "| 文献 | 已实施的对应研究 | 明确保留的边界 |", "|---|---|---|",
        "| Boudt 2011 | ShortH/WSD核心、异常污染模拟、滚动实证 | 日尺度/两Session/滚动为本项目适配；未复现TML和全部跳跃检验 |",
        "| Andersen–Bollerslev 1997 | FFF思想的分Session平滑与状态对照 | 使用log-WSD曲线投影，并非原文全部回归设定；原文已允许状态交互 |",
        "| Christensen et al. 2018 | 日内随机波动模拟、去周期后分布诊断 | 未实现原文pre-averaging噪声/跳跃稳健正式检验 |",
        "| Andersen–Thyrsgaard–Todorov 2019 | 状态时变模拟、滞后状态曲线、跨时段CDF日块区间 | 区间是描述性；不冒充原文函数型统计量与临界值 |",
        "| Dette et al. 2022 / Metrika 2023 | BPV有限样本常数、局部与标量修正、1/5分钟度量敏感性 | 两Session和混合贡献为明确推导；没有潜在真实IV可用来判定实证偏差 |",
        "| Dumitru et al. 2025 | 原尺度目标、滤波解释变量、HAR/HARP日级与15分钟改编 | 原文1000日训练大于IM现有995日，使用252日上限/126日暖机；未复现HAR-Q/CJ等全部扩展 |",
        "",
        "两处公式版本问题已单独核算：Dette工作稿BV修正的边界常数，以及DHI附录ShortH归一化的平方根。实现以自洽定义及原始Boudt公式为依据，详见阅读笔记。",
        "",
        "## 受控模拟", "",
        f"六种生成机制，每种{simulation_evidence['replicates']}次独立重复，四个窗口、四种估计器、两种训练尺度，共19,200次估计尝试；有效{simulation_evidence['metric_rows']:,}次，明确退化{simulation_evidence['estimator_failures']}次。退化集中在噪声/零收益的小窗口，结果没有悄悄补值。",
        "",
        "另用100,000个独立高斯日核对混合贡献、正确配对除数和Dette型标量修正的理论期望。模拟报告将潜在有效价格目标、含跳跃/噪声观测目标和oracle尺度明确分开。",
        "",
        "![已完成日BPV尺度下的模拟误差](results/simulations/factor_rmse_realized_bpv_day_scale.png)",
        "",
        "[完整模拟报告](results/simulations/REPORT.md) · [逐次结果](results/simulations/replicate_metrics.csv) · [代数与Monte Carlo核对](results/simulations/algebra_checks.csv)",
        "",
        "## 实证周期与残余分布", "",
        "下表是预先指定的代表配置；CV只衡量对应逐分钟摘要的相对离散程度。中位曲线更平不保证Q95、IQR或全部分布一致。", "",
        diagnostic_table_df.to_markdown(index=False, floatfmt=".4f"), "",
        "![BPV分布摘要](results/diagnostics/distribution_profiles_2026.png)", "",
        "日块区间保留日期顺序、以训练段确定CDF阈值；条件状态不通过拼接不相邻日期构造连续块。固定训练曲线在训练段属于样本内描述，只有训练结束后可作冻结基准。这些图和区间没有实施原文正式检验，不能据不拒绝而宣称不存在时变周期。",
        "",
        "[诊断报告](results/diagnostics/report.md) · [条件状态曲线](results/diagnostics/lagged_state_bpv_profiles.csv) · [分时段日块区间](results/diagnostics/day_block_bootstrap.csv)",
        "",
        "## 度量与时钟", "",
        "[度量比较](results/measures/REPORT.md)把普通BPV、日级标量修正、逐收益滤波后的BPV及混合贡献分开，并比较1/5分钟网格。没有真实IV，实证数值变化只能解释为度量敏感性，不能宣布某条结果更接近真实波动。",
        "",
        "[时钟实验](results/clocks/report.md)采用分钟末触达、保留累计余量、同分钟多阈值合并及Session重置。事前相对周期曲线总暴露固定240、阈值15，因而每日事件数的稳定很大程度由构造决定；不能把它当作预测能力证据。实现BPV时钟的未来触达时间则未知。",
        "",
        "![时钟对比](results/clocks/clock_comparison_2026.png)", "",
        "时钟与预测的直接连接见[共同15分钟目标下的事件特征回归](results/clock_forecasting/report.md)。它使用已完成事件的历史特征；没有采用分钟内插值，也不等于GARCH整事件预测向固定区间的转换。",
        "",
        "初始时间事件回归有3.12%的2026预测触及下限，实现BPV事件回归约0.16%；这些极小预测使QLIKE异常放大。探索性NNLS结构修正保留真实零标签并在训练中约束系数非负，三种事件模型触底均归零。修正后的事前周期钟相对同结构时间钟QLIKE差约−0.0032，区间跨零；实现BPV钟约+0.0074，区间也跨零。三者相对分钟基线仍较差。失败机制与修正分开登记，没有删除不利预测。",
        "",
        "## 未来15分钟预测", "",
        "各方法窗口只按2025验证损失选择；下表在2026相同起点比较。QLIKE差是候选减去同预测器的无周期基准，负数较好；MSE比小于1较好。绝对QLIKE使用固定1e8单位变换，差值不受该常数影响。区间为200次五交易日块bootstrap的逐项描述性区间，未修正多重比较或整个选窗过程的不确定性。", "",
        forecast_table_df.to_markdown(index=False, floatfmt=".6f"), "",
        "![验证选窗后的预测比较](results/forecasting/forecast_qlike_comparison.png)", "",
        "这里的改进相对于无周期基准，不自动意味着WSD显著优于现有贡献中位数。不同目标、不同模型的赢家也可以不同。原始数据已有的后验探索、代理噪声以及区间重叠限制了结果的外推。",
        "",
        "[完整预测报告](results/forecasting/REPORT.md) · [验证选窗](results/forecasting/windows_selected_on_2025.csv) · [逐点预测](results/forecasting/minute_forecasts.parquet)",
        "",
        "## 日级HAR/HARP的负结果", "",
        "以下为2026的HARP减HAR损失差；三个期限的区间均包含零，当前样本没有给出稳定改善证据。未来多日标签的训练截止已剔除尚未实现部分，验证标签也不跨入2026。", "",
        daily_har_table.to_markdown(index=False, floatfmt=".6f"), "",
        "## 验证与可复现性", "",
        "- 原文摘要、输入Schema、观测主键、收益/配对边界、因子均值和训练截止已核对。",
        "- 对2026-06-22起的全部未来收益作实质性扰动后，重新运行完整因子入口；截至该日的因子逐值不变，随后因子发生变化，证明该检查确实触及了计算路径。",
        "- 全部逐点预测核对标签长度、同Session、特征可用时点、训练/因子截止和数值有效性；另从独立输入复算抽取的标签。",
        "- 模拟、时钟及预测各自保留验证JSON，批次入口遇到失败即停止，日志和现场保留，无自动重试。",
        "",
        "```powershell", "& 'E:\\anaconda3\\envs\\latitude\\python.exe' -X utf8 .\\run_all.py", "```", "",
        "在独立项目根运行。每个入口都按自身文件位置定位项目；无需原工作区`.env`、旧Notebook或API账号。依赖版本见[requirements.txt](requirements.txt)。最新批次日志从[results/latest_run.json](results/latest_run.json)定位。",
        "",
        "## 完成边界与下一步", "",
        "已完成可在当前冻结输入上复现的文献核对、模拟、周期估计、分布/状态诊断、度量和时钟比较、15分钟与日级预测实验。科学问题仍有明确边界：原文的完整噪声稳健正式检验、全部HAR扩展、其他市场数据、未观察的新评估期以及交易执行验证，均未被本轮结果替代。",
        "",
        "下一轮最值得增加的是未被当前选窗和图表使用的新日期，以及新旧估计器之间的配对比较；不应仅凭去季节图更平就替换当前方法。",
    ]
    report_text = "\n".join(report_lines) + "\n"
    (project_dir / "FINAL_REPORT.md").write_text(report_text, encoding="utf-8")

    notebook = nbformat.v4.new_notebook()
    notebook.metadata.kernelspec = {"display_name": "Python (latitude)", "language": "python", "name": "python3"}
    notebook.metadata.language_info = {"name": "python", "version": sys.version.split()[0]}
    notebook.cells = [
        nbformat.v4.new_markdown_cell("# 日内波动建模：结果浏览\n\n方法讨论先读[符号约定与推导](research/03_method_derivations.md)，文献依据见[跨文献核对](literature/notes/07_notation_survey.md)。实证结果见 [FINAL_REPORT.md](FINAL_REPORT.md)。下列单元只读已有结果，不重跑模型；完整复现命令见根README。请将Notebook工作目录设为本项目根。"),
        nbformat.v4.new_code_cell("from pathlib import Path\nimport pandas as pd\nfrom IPython.display import display, Image\nfrom data_contracts import MINUTE_OBSERVATION_SCHEMA, PERIODICITY_FACTOR_SCHEMA\nproject_dir = Path.cwd()\nassert (project_dir / 'FINAL_REPORT.md').exists(), '请在独立项目根运行'\nfor artifact_schema in (MINUTE_OBSERVATION_SCHEMA, PERIODICITY_FACTOR_SCHEMA):\n    display(pd.DataFrame([{'field': f.name, 'arrow_type': str(f.type), 'nullable': f.nullable} for f in artifact_schema]))\n    display({key.decode(): value.decode() for key, value in artifact_schema.metadata.items()})"),
        nbformat.v4.new_markdown_cell("## 数据与因子覆盖\nSchema来自本项目唯一可执行定义，结果由脚本实际计算。"),
        nbformat.v4.new_code_cell("display(pd.read_csv(project_dir / 'results/factor_coverage.csv'))"),
        nbformat.v4.new_markdown_cell("## 模拟、分布与状态\n模拟反映指定机制，实证分布图属于描述性研究，不代替原文正式检验。"),
        nbformat.v4.new_code_cell("display(Image(filename=str(project_dir / 'results/simulations/factor_rmse_realized_bpv_day_scale.png')))\ndisplay(Image(filename=str(project_dir / 'results/diagnostics/distribution_profiles_2026.png')))"),
        nbformat.v4.new_markdown_cell("## 验证选窗后的2026回顾预测\n2026并非未被观察的测试期；QLIKE负差表示优于同预测器无周期基准。"),
        nbformat.v4.new_code_cell("forecast_summary_df = pd.read_csv(project_dir / 'results/forecasting/forecast_summary.csv')\ndisplay(forecast_summary_df[(forecast_summary_df.year == 2026) & (forecast_summary_df.state == 'all') & (forecast_summary_df.target == 'RV15') & forecast_summary_df.selected_on_2025][['model','method','lookback_days','qlike_difference_vs_none','qlike_difference_ci_low','qlike_difference_ci_high','mse_ratio_vs_none']])\ndisplay(Image(filename=str(project_dir / 'results/forecasting/forecast_qlike_comparison.png')))"),
        nbformat.v4.new_markdown_cell("## 时钟与日级负结果\n事件数稳定可能是构造性质；日级HARP未出现一致改善。"),
        nbformat.v4.new_code_cell("display(pd.read_csv(project_dir / 'results/forecasting/daily_har_summary.csv'))\ndisplay(Image(filename=str(project_dir / 'results/clocks/clock_comparison_2026.png')))"),
    ]
    # Execute read-only display cells in the same explicit interpreter. Do not
    # trust a globally named python3 kernelspec to point at the latitude environment.
    temp_parent = pathlib.Path(tempfile.gettempdir()).resolve()
    with tempfile.TemporaryDirectory(prefix="intraday_kernel_", dir=temp_parent) as temp_name:
        kernel_root = pathlib.Path(temp_name).resolve()
        assert kernel_root.parent == temp_parent and kernel_root.name.startswith("intraday_kernel_")
        kernel_dir = kernel_root / "latitude_research"
        kernel_dir.mkdir()
        (kernel_dir / "kernel.json").write_text(json.dumps({
            "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
            "display_name": "Python (latitude research)", "language": "python",
            "env": {"IPYTHONDIR": str(kernel_root / "ipython"), "JUPYTER_RUNTIME_DIR": str(kernel_root / "runtime")},
        }), encoding="utf-8")
        kernel_manager = KernelManager(
            kernel_name="latitude_research",
            kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(kernel_root)]),
        )
        try:
            NotebookClient(
                notebook, km=kernel_manager, timeout=120,
                resources={"metadata": {"path": str(project_dir)}},
            ).execute()
        finally:
            if kernel_manager.has_kernel:
                kernel_manager.shutdown_kernel(now=True)
    nbformat.write(notebook, project_dir / "00_research_overview.ipynb")
    manifest = {
        "generated_at": dt.datetime.now().astimezone().isoformat(),
        "report_sha256": hashlib.sha256(report_text.encode("utf-8")).hexdigest(),
        "data": data_evidence, "simulation": simulation_evidence,
        "forecast": forecast_evidence, "verification": verification,
    }
    (results_dir / "report_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Written FINAL_REPORT.md and 00_research_overview.ipynb", flush=True)


if __name__ == "__main__":
    main()
