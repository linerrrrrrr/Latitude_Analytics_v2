# 研究方法与实验

`a01_Methods` 维护可组合函数及 demo，`a02_Experiments` 按实验名称建立目录，以配置明确时间范围与训练／测试划分。编号标识职责明确的业务单元；同一单元的 Notebook、代理和附属资源共享编号及业务词根，详见[编号与资源归属](../AGENTS.md#层级编号与资源归属)。函数调用决定实际依赖。本说明面向熟悉 Python 和 Notebook、首次进入本目录的研究人员。

当前维护主力识别、训练／测试复权与分界采样，算法由 Notebook 的 `export` 单元格定义，同名 `.py` 仅加载定义。收益／波动率建模集中在一个 b 主题，c01 已实现分钟贡献、每日季节曲线与训练期初始回填，c02 已实现分钟去季节、采样区间汇总和模型输入转换，c03 已实现六组收益模型的 QML 联合拟合、预测和状态更新，c04 已实现四类直接波动观测模型的拟合、预测和状态更新；旧 close 重采样遵守只读保护。`a02_Experiments` 已建立按日收益／波动率实验的实验草稿，已接入数据准备、逐日计算、输入冻结及 DLC／MLflow 显式入口，尚未执行真实实验或完成云端验收。带尖括号的名称与配置是填写约定。强制规则见 [AGENTS.md](AGENTS.md)。

- [主力识别](a01_Methods/b01_MainContractSelection/c01_MainContractSelection.ipynb)：读取四张正式 silver 表，首个有效交易日按当日成交量最大合约初始化，当天生效；随后使用前一交易日信息决定保留或换约。铜 `CU` 和螺纹钢 `RB` 各保存一份 2010-01-01 至 2026-09-30 的每日映射，换约门槛 1.10；首日初始化是使用当日完整成交量的特例，不能解释为当日开盘前已知的选择。
- [训练／测试复权](a01_Methods/b02_MainContinuousAdjustment/c01_MainContinuousAdjustment.ipynb)：读取主力映射及正式分钟、Session 日历。按 `trading_date` 划分：训练期 `< 2025-01-01` 做双向复权，训练首尾实际主力分钟保持原价；测试期固定首个实际主力分钟，只累计测试内部后续换约偏移，分界不补偿。各分钟全部真实合约 OHLC 共用一个 log 偏移，数量保留原值；原始零价格保留、派生价格为空，实际换约锚点不可用时报错。理论网格缺口保留诊断，不补分钟或价格。复权方法版本为 0.5.0；CU、RB 完整范围产物均已重新生成并复读验收，分别为 19,500,750 行和 15,344,910 行。
- [采样轴、单位与位置](a01_Methods/b03_Sampling/c01_SamplingPositions.ipynb)：固定初始训练期单位 a，从明确分界 t 计算过去和未来位置，支持单分界完整序列和逐分钟滚动 K 点。
- [采样价格与执行](a01_Methods/b03_Sampling/c02_SamplingExecution.ipynb)：复用稀疏报价索引与 log 价格，分批生成各合约价格、保存结果和比较主力收益率。两本 Notebook 同属 `b03_Sampling/`，位置 demo 归 `c01_SamplingPositions_DemoData/`，价格 demo 归 `c02_SamplingExecution_DemoData/`。原独立校准入口已被这两本替代。
- 收益／波动率建模归 `b04_ReturnVolatilityModeling/`：[季节性](a01_Methods/b04_ReturnVolatilityModeling/c01_IntradaySeasonality.ipynb)提供贡献构造、每日曲线估计及训练期初始回填；[模型观测](a01_Methods/b04_ReturnVolatilityModeling/c02_ModelObservations.ipynb)提供分钟去季节、区间汇总和显式输入转换；[收益模型](a01_Methods/b04_ReturnVolatilityModeling/c03_ReturnModels.ipynb)提供六组 QML 联合拟合、下一步预测及观测更新；[直接波动观测模型](a01_Methods/b04_ReturnVolatilityModeling/c04_VolatilityModels.ipynb)提供 ARMA、ARFIMA、HAR、MEM 的拟合、预测及固定参数更新。c01—c04 的新版试错单元已逐本执行，调用真实小样本及所有对外计算函数；结果不落盘。云依赖及入口顺序修复后，完整云 demo 已提交新季节阶段 [train1v6y4ne9zpv](https://pai.console.aliyun.com/?regionId=cn-shenzhen&workspaceId=351454#/training/jobs/train1v6y4ne9zpv)，由干净 Jupyter 内核串行接续观测和模型阶段；完整成果尚未验收，不能以小样本试错代替完整验收。
- [期限结构输入](a01_Methods/b05_TermStructureModeling/c01_TermStructureInputs.ipynb)：原样接收上游指定数值，以合约最后有效日的自然日末（上海时区次日 00:00）计算自然剩余天数，保留逐合约状态和原因，不设合约数量门槛。当前采用 N15 的 σ×成交额配置，小样本已验收，完整云 demo 状态见本批记录；此前 N60 时间轴完整本地 demo 包含 21,316 点、255,758 合约行。
- [固定 λ 的 NSS 拟合](a01_Methods/b05_TermStructureModeling/c02_NSSFitting.ipynb)：定义稳定基函数、求值、单曲线及完整序列拟合，支持长期与短端水平下界；秩不足继续求解并记录参数识别状态。N15 真实小样本的 48 点拟合通过；此前 N60 时间轴完整本地 demo 的 21,316 点全部得到满秩有限解。网格校准和独立热力图见 c03。
- [NSS 网格校准](a01_Methods/b05_TermStructureModeling/c03_NSSCalibration.ipynb)：网格上下限及步长可配，仅用训练期进行累计误差、最优次数投票及旧指数权重的 log λ 平均三种选择；测试期固定 λ 重估 β，热力图独立按类型调用。当前 N15 真实小样本已验收，完整云 demo 状态见本批记录；此前 N60 时间轴完整本地 demo 覆盖 18,902 训练点 × 四个网格，三种方法各覆盖 21,316 点且全部成功。默认内嵌一张小样本误差热力图。
- 旧 close 重采样的历史路径及输入引用按[研究规则](AGENTS.md#目录与文件)保护，不恢复已删除入口，不执行旧 demo。
- [小波变换](a01_Methods/b06_WaveletAnalysis/c01_WaveletTransform.ipynb)：定义复因果 Gammatone 导数核及双边 Morlet、周期映射、显式状态和单段变换；默认因果计算连续递推，预热与缺失失败分别保留。实际验收见[小波分析](#小波分析)。
- [小波同步压缩](a01_Methods/b06_WaveletAnalysis/c02_Synchrosqueezing.ipynb)：默认正式 SST，使用核对应的导数频率映射、当前点比例／绝对阈值、固定尺度权重和可配频率网格，返回复谱与排除诊断。完整真实 demo 已验收，实际状态见[小波分析](#小波分析)。
- [完整小波序列](a01_Methods/b06_WaveletAnalysis/c03_RollingWavelet.ipynb)：将连续状态、CWT／SST、坐标和可得时刻接为宽表，绘图只消费已有结果并按参数选择一张图。两种核各 21,316 点的真实 demo 已验收，七个连续块与因果整段一致，默认内嵌一张 SST 幅值图；实际状态见[小波分析](#小波分析)。

```text
R04_Research/
├─ AGENTS.md
├─ README.md
├─ a00_notebook_loader.py
├─ a01_Methods/
│  ├─ b01_MainContractSelection/
│  │  ├─ c01_MainContractSelection.ipynb
│  │  ├─ c01_MainContractSelection.py
│  │  └─ c01_MainContractSelection_DemoData/
│  │     ├─ c01_MainContractSelection_CU_20100101-20260930_ratio1.10_demo.parquet
│  │     └─ c01_MainContractSelection_RB_20100101-20260930_ratio1.10_demo.parquet
│  ├─ b02_MainContinuousAdjustment/
│  │  ├─ c01_MainContinuousAdjustment.ipynb
│  │  ├─ c01_MainContinuousAdjustment.py
│  │  └─ c01_MainContinuousAdjustment_DemoData/
│  │     ├─ c01_MainContinuousAdjustment_CU_train20100101-20241231_test20250101-20260930_ratio1.10_log_common_demo.parquet
│  │     └─ c01_MainContinuousAdjustment_RB_train20100101-20241231_test20250101-20260930_ratio1.10_log_common_demo.parquet
│  ├─ b03_Sampling/
│  │  ├─ c01_SamplingPositions.ipynb
│  │  ├─ c01_SamplingPositions.py
│  │  ├─ c01_SamplingPositions_DemoData/
│  │  ├─ c02_SamplingExecution.ipynb
│  │  ├─ c02_SamplingExecution.py
│  │  └─ c02_SamplingExecution_DemoData/
│  ├─ b04_ReturnVolatilityModeling/  # 四本内存试错已执行；新版完整云 demo 已启动，待验收
│  │  ├─ c01_IntradaySeasonality.ipynb
│  │  ├─ c01_IntradaySeasonality.py
│  │  ├─ c02_ModelObservations.ipynb
│  │  ├─ c02_ModelObservations.py
│  │  ├─ c03_ReturnModels.ipynb
│  │  ├─ c03_ReturnModels.py
│  │  ├─ c04_VolatilityModels.ipynb
│  │  └─ c04_VolatilityModels.py
│  ├─ b05_TermStructureModeling/  # c01—c03 默认小样本；N15 完整云 demo 见批次记录
│  │  ├─ c01_TermStructureInputs.ipynb
│  │  ├─ c01_TermStructureInputs.py
│  │  ├─ c01_TermStructureInputs_DemoData/
│  │  ├─ c02_NSSFitting.ipynb
│  │  ├─ c02_NSSFitting.py
│  │  ├─ c02_NSSFitting_DemoData/
│  │  ├─ c03_NSSCalibration.ipynb
│  │  ├─ c03_NSSCalibration.py
│  │  ├─ c03_NSSCalibration.yaml
│  │  └─ c03_NSSCalibration_DemoData/
│  └─ b06_WaveletAnalysis/  # c01—c03 已实现
│     ├─ c01_WaveletTransform.ipynb
│     ├─ c01_WaveletTransform.py
│     ├─ c01_WaveletTransform_DemoData/
│     ├─ c02_Synchrosqueezing.ipynb
│     ├─ c02_Synchrosqueezing.py
│     ├─ c02_Synchrosqueezing_DemoData/
│     ├─ c03_RollingWavelet.ipynb
│     ├─ c03_RollingWavelet.py
│     └─ c03_RollingWavelet_DemoData/
├─ a02_Experiments/
│  └─ b01_ReturnVolatilityForecasting/  # 收益与波动率预测实验草稿
│     ├─ experiment.ipynb
│     ├─ experiment.py
│     └─ config.yaml
└─ referance/                 # 研究参考资料、文献和资料包
```

## 研究参考资料

[期货高维特征与机器学习研究范式资料包](referance/feature_selection_production_research_2026-08-24/README.md)保存 2026-08-24 的研究报告、材料索引、来源清单和论文。项目诊断与建议按资料日期理解，不作为当前实现或项目规则的证明。

[PAI 计价资料](referance/PAI_Pricing_2026-10-07.md)整理 DLC 按量计费、竞价、免费额度、资源报价入口及深圳 DLC 实际询价记录；当前实验取消金额上限、采用 10 节点 ecs.c6.large，官网示例不替代实际询价。

## 方法与 demo

研究主题使用 `bNN_<主题名>/`，主题内职责明确的单元独立从 `c01` 编号；相关派生主题使用同级 `bNN_01_<派生主题名>/`、`bNN_02_<派生主题名>/`。一个单元的 `cNN_Name.ipynb`、同名 `.py` 和 `cNN_Name_DemoData/` 共同归属该 c 编号。说明与演示生成代码写在 Notebook 内，数据目录按所属单元完整基名增加用途后缀；文件格式或附属目录不另占编号。demo 目录实际需要时再创建，不收纳正式实验缓存、模型或运行记录。

`cNN_Name_DemoData/` 内的数据文件按 `<Notebook基名>_<品种>_<请求日期范围>_<关键参数>_demo.parquet` 命名。`ratio1.10` 表示主力换约成交量门槛，`log_common` 表示主力与全合约共用的 log 偏移；涉及训练／测试划分的复权与采样 demo，以 `train<YYYYMMDD-YYYYMMDD>_test<YYYYMMDD-YYYYMMDD>` 分别列出两期请求范围；两段相邻，合并即完整区间，分界日属于测试期，不再重复列出整体范围。训练／测试复权条件以 metadata 中的分界与算法规则为准；复权文件保留上游门槛，便于区分不同主力路径。日期按交易日条件解释，两端包含、时区 Asia/Shanghai；初始化前未识别的日期仍在主力映射中，复权实际起点记录在 metadata。文件名不加版本号，不另建 JSON/JS 或清单文件。

每个业务配置对应一个路径。不同品种、时间范围或关键参数分别保存，同一配置重跑时比较生成身份和实际内容：一致则保留字节和生成时间，变化则更新该文件并重算受影响的下游。历史结果由 Git 或正式实验分区保存。

下游明确指定上游生产单元的 `cNN_Name_DemoData/` 和完整文件名，不自动挑选“最新”文件。生产者 Notebook 位于该数据目录的父目录，读取生成定义时须独立定位 Notebook。文件名帮助选中配置；数据内原有 `demo_identity` 继续记录精确条件、上游身份、实际输入内容、相关定义/Schema/环境及输出内容摘要。文件存在或名字相同不能证明结果可复用。

历史 demo 身份中的 `04_Research/...` 保留生成时的来源标识，当前源码位置为 `R04_Research/...`。旧标识不能直接拼接为当前文件路径；路径迁移也不能证明当前定义与产物一致。完整包导入调整涉及生成依赖原文时，严格摘要校验会判定定义已变化。这些产物保留为历史结果；按当前代码复用须先完成来源定位及身份核对，未通过时不得跳过校验或自动重算。

主力识别长表保留每日主力、选择原因及信号日期。复权长表保留 `raw_*`、`log_*`、`adjusted_log_*`、`adjusted_*` OHLC、数量、主力身份、片段和逐分钟共同偏移。下游用 `is_main` 选取主力，用 `segment_id` 定位片段；`log_*` 和 `adjusted_log_*` 已是自然对数，按分析需要选择价格域。原始零价格的三个派生价格字段为空，零偏移处也不恢复为零；换约所需的首开或末收若为零、导致 log 不可用，仍停止计算。

Notebook 按“说明与定义 → 输入和参数 → 计算 → 展示与保存”组织。主力识别和复权的 `demo_underlying_code` 选择 `CU` 或 `RB`，分别运行可生成对应文件；主力识别与新复权口径的 CU、RB demo 均已验收，表格预览需运行对应展示单元格；旧采样 Notebook 与代理不改写、不重命名、不执行 demo；其历史引用不为本次目录调整修改，新入口应使用 `cNN_Name_DemoData/` 中的上游数据。表格每个配置使用一个 Parquet，模型与表格不强求同一格式。下游不复制上游输入，导入代理不执行上游 demo。

数据预览按[展示规则](../AGENTS.md#数据呈现附带中文)使用 `英文字段名（中文含义）`，覆盖主力选择输入、映射及案例，复权范围、身份、分段、缺口和换约诊断，以及重采样参数和结果。主力映射标签定义为所属 Notebook 的 `MAIN_CONTRACT_COLUMN_LABELS`，复权扩展标签定义为 `ADJUSTMENT_COLUMN_LABELS`，下游通过原代理导入；这些纯展示常量不参与 demo 生成摘要。派生结果和落盘数据保留原字段。

研究方法的代码和 demo 数据都纳入 Git。仅迁移收纳路径且生成定义、内容未变时，原样移动文件，保留字节、metadata 和生成时间。方法或输入变化后，重新生成并验收受影响的 demo，将相关代码与对应数据一起提交。检出某次提交后，下游 demo 可以读取该次提交保存的上游结果；重算仍需取得对应的原始输入。

主力识别 demo 的算法及局部 Schema 单元格同时标记 `export` 与 `demo-dependency`，代码摘要只取这些生成依赖；绘图函数只标记 `export`。新增生成依赖时同步纳入摘要范围。保存时复读已有文件，比较生成身份和实际数据；两者一致就保留文件和原生成时间，发生变化才写入并验收。

可复用的导入、常量与定义单元格标记 `export`；demo、读写、图表和提交操作不标记。同名代理只需：

```python
from R04_Research.a00_notebook_loader import load_notebook_exports

load_notebook_exports(__file__, globals())
```

调用方先按 [.env.template](../.env.template) 定位根。在已命中根目录的分支中增加 `sys.path.insert(0, str(candidate_root))`，再按正常模块路径导入方法，例如 `from R04_Research.a01_Methods.b01_MainContractSelection.c01_MainContractSelection import build_main_contract_daily`。直接运行代理的环境也须将项目根作为模块搜索路径；代理不另写根定位代码。导入只加载定义，计算须显式调用；修改 Notebook 后重启内核。

[加载器](a00_notebook_loader.py)先编译全部选中单元格，再顺序执行，错误标明 Notebook 路径和零基单元格索引。它保留代理模块身份，支持跨方法正常导入；不执行已保存输出，不替代代码对副作用的约束。研究代理不使用采集的完整 PythonExporter 同步命令。

## 期限结构输入

<!-- nss-cloud-status:start -->
NSS N15 σ×交易额完整云 demo 已计算并验收：78,007 点、936,060 合约行、九份文件；具体失败状态见本批记录。 [批次记录](a01_Methods/b05_TermStructureModeling/c03_NSSCalibration_CloudRuns/20261007_nss_n15_31bc82d6/submission.json)。
<!-- nss-cloud-status:end -->

当前真实 demo 参数统一定义在 [c03_NSSCalibration.yaml](a01_Methods/b05_TermStructureModeling/c03_NSSCalibration.yaml)：RB、N=15、`gk_money`（σ×成交额）、全合约成交额合计、`log_interpolate`、固定分界，训练请求 2010-01-01—2024-12-31，测试请求 2025-01-01—2026-09-30。σ 取当分钟主力合约 GK 波动率；采样单位为完整训练期平均分钟轴增量的 15 倍，测试期固定此单位。实际首个交易日为 2010-01-04。上游精确文件为 `c02_SamplingExecution_RB_train20100101-20241231_test20250101-20260930_N15_gk_money_all_contracts_log_interpolate_fixed_demo.parquet`，含 78,007 点、936,060 合约行。

三个 Notebook 默认取 32 个训练点、16 个测试点，在内存中调用当前定义并保留该点全部真实合约，结果仅以中文表格和单张图内嵌，不写计算数据文件。2026-10-07 已逐代码单元实际通过这套 N15 真实试算：48 点、576 合约行，固定 λ 的 48 点与三种选择的 144 点均成功；已有 demo 文件的内容摘要和修改时间均未变化。完整云计算入口已定义；N15 全区间提交及验收状态见本批记录。完整云作业需在试算通过后冻结全区间采样、日历、参数及定义，云端计算后回收、复读和全点验收九份产物，分别归三个单元 DemoData。作业及冻结记录归 `c03_NSSCalibration_CloudRuns/<batch>/`。以下 N60 数值是此前完整本地执行记录，不能视为 N15 完整计算结果，也不能代替当前生成身份核对。

`b05_TermStructureModeling/c01_TermStructureInputs` 提供 `build_contract_terminals`、`prepare_term_structure_inputs`、`iter_term_structure_curves`。前者仅使用合约身份及 `delist_date`，以最后有效日的自然日末（上海时区次日 00:00）构造 `terminal_at`，不依赖到期日 Session；中者原样采用明确指定的 `value_col`，计算 `(terminal_at - sample_at).total_seconds() / 86400`；后者逐采样点输出成对的合约、期限和值，并保留全部合约行的状态。零期限、有限零值及负值保留，负期限排除，缺少最后有效日期和非有限输入记录失败；不设置合约数量门槛、不填补数据。分钟内插值的报价可得时点为 `bar_at`，迭代结果通过 `available_at` 明确表示。

此前 N60 时间轴完整本地 demo 直接读取 b03 的 `c02_SamplingExecution_RB_train20100101-20241231_test20250101-20260930_N60_time_all_contracts_log_interpolate_fixed_demo.parquet`，使用完整固定分界序列及只读正式合约日历中的最后有效日期。读取每个合约最后一个实际采样交易日的元信息，不读取未来到期日的 Session。`sampled_close` 是上游普通复权价格，不是原始合约价格；本单元不增加变换。产物归 `c01_TermStructureInputs_DemoData/`，日末文件名参数为 `delist_day_end`，局部 Schema 与生成身份中的终点规则均为 `end_of_delist_date`。身份包含上游产物、真实日历读取条件及内容摘要、生成定义和环境。2026-10-07 已运行并复读验收完整序列的行对应、日末期限计算、终点与最后有效日期的对应及曲线遍历：21,316 个采样点，255,758 条合约行全部有效，没有失败或排除行；日末边界处零期限及边界前 1 秒的小数期限按真实合约日期验证。实际计算在 `latitude_env_v2` 中逐代码单元格执行，Notebook 已保存表格与状态输出。

`c02_NSSFitting` 已实现 `nss_basis`、`evaluate_nss_curve`、`fit_nss_curve` 与 `fit_nss_sequence`。λ 采用每天的衰减速率，关系为 `0 < lambda2 < lambda1`；β 的经济约束按上游输入尺度显式配置。`coefficient_constraints` 可提供 `level_lower_bound`（β₀ 下界）与 `short_lower_bound`（β₀＋β₁ 下界），省略时使用普通最小二乘；启用时以两个端点水平重参数化，用 BVLS 求约束最小二乘，不裁剪系数。零期限使用极限，小期限使用稳定展开。有限解即使秩不足也保留，但标记 `rank_deficient`；没有有效输入、求解失败或不收敛按点记录，不换 λ 或重试。SSE、MSE、RMSE 及残差均处于输入尺度，跨曲线误差汇总由实验设计决定。

此前 N60 时间轴完整本地 demo 的 c02 直接读取完整 c01 RB 日末口径产物，固定 `lambda1=0.01`、`lambda2=0.003`，普通复权价格尺度的长期与短端下界均为 `1e-6`，BVLS 停止容差 `1e-10`、绝对约束验收容差 `1e-8`、迭代上限 100。这是显式演示配置，未进行 λ 校准。2026-10-07 已在 `latitude_env_v2` 中实际执行所有对外函数并验收完整序列的残差、SSE／MSE、端点约束、曲线求值和原行对应：21,316 点全部得到满秩有限解，没有失败或秩不足点。每点有效观测数为 11—12，长期下界在 5,201 点活动，短端下界未活动。拟合表保留全部 255,758 合约行，每行都有有限拟合值和残差。系数表与拟合表归 `c02_NSSFitting_DemoData/`，文件名、局部 Schema 和生成身份均明确继承 c01 的日末口径，已复读核对 Schema、生成身份和内容摘要；Notebook 保存中文表头与状态输出。这证明固定演示 λ 下的实际调用及数学验收，不代表 λ 已最优。

`c03_NSSCalibration` 提供 `build_nss_grid`、`evaluate_nss_grid`、`summarize_nss_grid`、`select_nss_lambdas` 与 `plot_nss_calibration_heatmap`。两个 λ 坐标分别提供上下限及线性步长，上限仅在整步长可达时包含；不满足 `0 < lambda2 < lambda1` 的节点保留资格原因但不拟合。评估只读取训练期，每个合法组合、每个训练点都调用 c02 并保留系数、误差、识别诊断及失败。汇总采用所有合法候选共同成功的训练点，防止失败网格借助不同样本获得比较优势，同时保留总数、成功／失败数及共同点数；无共同点时选择失败，不填误差或重试。

三种 λ 选择并列：`min_error` 默认以训练期累计 SSE 为目标，指标可选 SSE／MSE／RMSE，汇总可选 sum／mean；`legacy_frequency` 统计逐点最小 MSE 的次数，单点与总次数平票均取最小网格编号；`legacy_error_weighted` 对各组合的平均 MSE 使用样本标准差（ddof=1），构造归一化 `exp(-(e/std(e))**weight_power)`，再对两个 λ 作 log 空间几何平均。权重指数默认 3，可显式配置；几何平均保留连续 λ，不吸附回网格，不为该连续参数伪造已有网格误差。等误差或单候选采用均匀权重，其余用等价的稳定 log 权重计算防止全部指数下溢。后两种定义核对只读旧 `b03_curve_structure_NSS_02_beta.ipynb` 的对应函数，归档未执行。测试期固定训练期所选 λ，逐点 β 拟合复用 c02。

热力图只消费汇总表，`plot_type=error/frequency/weight` 一次返回一张图；计算与绘图独立，零次数及零权重保持零，无资格或无结果格点留白。Notebook 通过共用配置的 `plot.plot_type` 选择图形，默认只内嵌一张误差热力图；纯绘图定义不影响计算产物身份。

此前 N60 时间轴完整本地 demo 的 c03 训练期粗网格为 λ₁ 0.005—0.01、步长 0.005，λ₂ 0.001—0.003、步长 0.002，共四个合法组合；长期与短端下界均为 1e-6，容差和迭代上限沿用 c02。使用完整 c01 RB 日末产物，累计 SSE 目标和权重指数 3。2026-10-07 已实际完成 18,902 个训练点 × 四个网格，共 75,608 次拟合，全部成功且满秩，共同比较点覆盖全部训练点。三种方法各重估完整 21,316 点（训练 18,902、测试 2,414），全部成功且满秩。最小累计 SSE 与投票均选出 λ₁=0.01、λ₂=0.001；该网格累计 SSE 为 88,205,419.8018，逐点胜出 10,672 次。旧指数权重高度集中于这一网格，几何平均约为同一对 λ（实际 float64 值保存在选择表）；其连续参数的 SSE 通过 c02 重估，不用网格误差代替。六份产物分别为网格 4 行、网格拟合 75,608 行、汇总 4 行、选择 3 行、选定参数系数 63,948 行及拟合长表 767,274 行，均归 `c03_NSSCalibration_DemoData/`，已复读核对局部 Schema、当前算法和依赖定义、训练期身份及完整内容摘要。完整重估验证原行对应、残差与 SSE、测试期 λ 固定和测试数据未进入校准。计算在 v2 逐代码单元格执行；独立绘图切换无界面后端后完成，Notebook 全部代码单元格有执行记录、无错误输出，且仅内嵌一张误差 PNG。三种图形类型均用真实汇总检查，图像已目视核查。这是用于方法真实调用和数学验收的粗网格，不宣称连续域最优或范围已经充分。

合成边界检查位于 `R00_draft_collection_02/tests/test_term_structure_inputs.py`，使用 v2 执行 `python -B -m unittest discover -s R00_draft_collection_02/tests -p test_term_structure_inputs.py`。八项检查已通过，覆盖日末边界、最后有效日收盘后仍剩 9 小时、无 Session 列的未来到期合约、缺失／冲突最后有效日期、11 个和单个合约、零／负输入、零／负期限、稀疏失败行与显式时区；此证据不替代真实 demo。临时构建与执行工具仍归草稿区。

c02 的合成数学检查位于 `R00_draft_collection_02/tests/test_nss_fitting.py`，使用 v2 执行 `python -B -m unittest discover -s R00_draft_collection_02/tests -p test_nss_fitting.py`。八项已通过，覆盖极限及稳定基函数、精确曲线恢复、残差定义、经济约束的 KKT 条件、零／负输入、单个合约和秩不足、不收敛时不发布系数，以及序列失败行保留；与真实 demo 分别记录。

c03 合成数学及信息边界检查位于 `R00_draft_collection_02/tests/test_nss_calibration.py`，使用 v2 执行 `python -B -m unittest discover -s R00_draft_collection_02/tests -p test_nss_calibration.py`。12 项已通过，覆盖十进制节点和资格掩码、c02 拟合一致性、测试数据不参与校准、共同比较集合、不同误差目标、旧权重及几何平均、平票、指数下溢、缺输入及空训练期，以及每次单图且不调用拟合／选择；此证据不替代真实 demo。一次性工具和测试暂存草稿区，正式图像内嵌在 Notebook。

云执行边界另由 `R00_draft_collection_02/tests/test_nss_cloud_control.py` 验证：四项离线检查通过，覆盖显式 PUBLIC 默认会话、创建前唯一作业名、一次提交及身份不明时不重试、拒绝停止其他批次。`test_nss_cloud_chain.py` 的六项合成检查已通过，覆盖分块并行一致、测试期故障不影响 λ 选择、失败行保留、试算门槛及九份文件契约；合成文件不代表云计算通过。`nss_cloud_batch_20261007.py` 是本次临时后台执行工具，冻结到所属批次后只负责一个作业、独立监控、普通查询、回收及失败停止；算法仍来自 Notebook。

## 小波分析

[c01_WaveletTransform](a01_Methods/b06_WaveletAnalysis/c01_WaveletTransform.ipynb)实现 `build_wavelet_bank`、`evaluate_wavelet_response`、`initialize_wavelet_state`、`update_wavelet_state` 与 `compute_wavelet_transform`。输入值直接使用，是否 log、差分或标准化由上游决定；周期表示多少个采样点完成一周，必须严格递增且大于 2。核参数、逐周期预热长度及有效记忆容差显式可配，核组同时提供归一化、频率映射、尺度求积权重、未来支持和峰值响应滞后。

默认 `causal_gammatone` 使用复 Gammatone 的一阶导数形成零均值核，连续母核 L² 归一，单位采样间隔双线性离散化及频率预畸变，参数默认为 order=4、alpha=1、omega0=6；公式、卷积方向及 2014 年论文依据在 Notebook 中独立说明。复系数与模型导数由同一稳定级联产生，只读取当前及历史输入；c02 估计 SST 频率时识别 `bilinear_model_derivative` 并应用逆畸变，不能直接将该导数比值除以 2π。近似解析核的负频能量不严格为零，通道表记录其连续核比例；导数接口与求积权重提供 c02 的输入，不表示 SST 的重构及脊线性质已经验收。

初始化将各级低通状态设为首个有效输入值的恒定背景，不消耗采样点；前 warmup_samples 个有效点记录 `warmup/insufficient_history`，系数与导数置空。默认长度取重复极点绝对包络的尾质量容差 1e-6，与有限分子平移共同确定，不截断滤波历史。正常分块、跨日、跨 Session 与训练／测试分界不重置状态；NaN／Inf 记录 `failed/nonfinite_input`，保留原位置并中断有效段，下一有效值重新初始化及预热。每次更新返回新状态，不改写调用者传入的状态；批量因果变换直接调用逐点函数，追加输入不修订已有输出。

`morlet` 为双边离线比较核，默认 sigma=1、omega0=6、support_sigma=8，连续母核归一后采样、截断并修正离散 DC。`extension_mode` 支持 none、constant、reflect、symmetric、periodic；none 将左右支持不完整的位置记录为 `boundary_incomplete/incomplete_kernel_support`。缺失两侧分段计算，不跨 NaN／Inf 卷积；因果核仅允许 none，不接受右侧延拓。Morlet 使用后续点，不能把其输出登记为当前采样点已可得的交易特征。计算函数不绘图，不回移时间戳补偿响应滞后。

2026-10-07 已在 `latitude_env_v2` 逐代码单元格执行完整真实 demo，并将中文展示和状态输出保存回 Notebook。显式消费 b03 已验收的 RB、N=60、time、all_contracts、log_interpolate、fixed 主力序列，上游全局 log 差分为本方法的直接输入；请求训练 2010-01-01—2024-12-31、测试 2025-01-01—2026-09-30，共 21,316 点，首个差分点缺失。两种核均覆盖周期 8、16、32、64、128、256，每种核生成 127,896 个通道点。因果核成功 117,485、预热 10,405、上游缺失 6，逐周期预热长度分别为 186、341、665、1,320、2,633、5,260；Morlet 成功 120,184、左右支持不足 7,706、上游缺失 6。两者均无数值计算失败；缺失 6 行是同一首个输入点在六个通道上的保留记录。

真实验收覆盖所有对外计算入口、完整输出位置和幅值／能量关系、按训练／测试分界传递状态与整段计算逐项一致、追加未来数据不修订因果历史、DC 响应及周期峰值。两种核各保存通道表 6 行和系数表 127,896 行，四份 Parquet 归 `c01_WaveletTransform_DemoData/`，已复读核对局部 Schema、算法与上游身份和内容摘要；复数分为实部／虚部，时间戳沿用仓库微秒类型，零幅值相位为空。12 项独立合成数学与边界检查另行通过，包括连续母核积分和单位能量、独立有理传递函数、实正弦的复响应与导数、缺失后再初始化、状态不可就地修改、Morlet 直接卷积和各延拓方式。合成验证脚本和临时 Notebook 构建／执行工具暂存 `R00_draft_collection_02/tests/test_wavelet_transform.py` 与 `R00_draft_collection_02/scripts/*wavelet_transform*`；它们不作为算法导入入口，正式定义和真实验收结果保存在本单元 Notebook。

[c02_Synchrosqueezing](a01_Methods/b06_WaveletAnalysis/c02_Synchrosqueezing.ipynb)实现 `build_sst_frequency_grid`、`estimate_instantaneous_frequency` 与 `compute_sst`，直接消费 c01 的复系数、导数和核身份。默认 `method='formal'` 按一阶小波 SST 定义，以固定 `Delta a * a**(-1.5)` 沿尺度累加复系数，再除以目标频率区间宽度；定义依据为 [Daubechies、Lu、Wu 的论文](https://arxiv.org/pdf/0912.2437)，函数接口及离散约定见本单元 Notebook。`legacy_sum` 作为省略这两项权重的比较选项，数值单位不同。因果核使用 `atan(Im(D/W)/2)/pi` 逆双线性畸变，Morlet 使用 `Im(D/W)/(2pi)`；这与各自导数定义对应，不证明一般调幅调频信号的离散估计严格等于连续估计。

频率网格的周期上下限、区间数和线性／对数频率间隔均可配，网格不根据整段输入改变。默认相对阈值为当前点可用尺度最大幅值的 0.01，比例暴露为参数；绝对模式须显式传入系数幅值阈值。零系数、低于阈值、非正频率、Nyquist 及以上频率和网格以外分别记录，不将范围外频率裁剪至边缘。部分尺度未就绪或局部数值失败时输出 partial，不重估尺度权重；全部尺度不可用时为空谱，全部已就绪而无纳入贡献时为有效零谱。计算和绘图分开，本单元没有实现原信号或分量重构。

2026-10-07 已在 `latitude_env_v2` 逐代码单元格执行完整真实 c02 demo，保存中文表格及状态输出。直接消费上述 c01 两种核的全部 21,316 点，周期边界 8—256、128 个线性频率区间、相对阈值 0.01。因果核为 16,055 个 ok、5,074 个 partial、186 个 warmup、1 个 failed；Morlet 为 17,403 个 ok、3,788 个 partial、124 个 boundary_incomplete、1 个 failed。两个失败点均继承上游首个收益率缺失，没有新增数值失败；partial 分别包含逐尺度预热或离线支持不足。每种核保存频率网格 128 行、逐尺度频率与归属 127,896 行、复谱与点状态宽表 21,316 行，六份 Parquet 归 `c02_Synchrosqueezing_DemoData/`，已复读核对 Schema、生成身份和内容摘要。

完整真实验收通过纳入复系数积分恒等式、当前点阈值、范围排除及训练前缀历史不变性；同一真实输入另外验收绝对阈值、对数网格和 legacy_sum。18 项独立合成数学与边界检查通过，包括导数逆映射、实正弦频率、区间边界、复数抵消、部分尺度固定权重、空谱与零谱、数值溢出及参数拒绝。临时检查与构建／执行工具暂存 `R00_draft_collection_02/tests/test_synchrosqueezing.py` 及 `R00_draft_collection_02/scripts/*synchrosqueezing*`，不作为算法导入入口。

[c03_RollingWavelet](a01_Methods/b06_WaveletAnalysis/c03_RollingWavelet.ipynb)实现 `WaveletSequenceState`、`build_wavelet_sequence_schema`、`compute_wavelet_sequence` 与 `plot_wavelet_sequence`。完整序列函数接收采样坐标及上游数值，调用 c01 和可选 c02；周期、核、预热、延拓、网格和阈值复用前两单元的显式参数。因果续算块从上一 `final_state` 的下一采样点开始，正常跨日、跨 Session 和训练／测试分界不重置。编号跳过、块重叠或位置时刻倒退时报错，数值 NaN／Inf 留在原行按 c01 分段；不把截取绘图范围当作新的计算历史。

序列表保留 `sample_at` 和 `bar_at`。`input_available_at` 是截至当前点的所有采样位置与源报价时刻最大值，并跨块携带；因果计算已有尺度就绪时将其记为 `available_at`。这描述给定输入坐标和本方法的计算依赖，上游方法自身的可得性仍由上游契约决定。Morlet 的 `available_at` 为空，`analysis_context_end_at` 表示整段离线上下文终点。响应滞后不通过时间戳回移补偿。复系数、导数、逐尺度状态和可选 SST 复谱／归属保存在每点一行的局部 Arrow 宽表中，核和网格由 metadata 及生成身份定位。

绘图入口只读取已有序列表，每次根据 `plot_type` 返回一个 figure：CWT／SST 的 amplitude、energy 或 phase；零幅值相位为空。采样编号、训练／测试侧、周期范围、横轴标签及线性／对数色标只改变展示，不重新计算。横向保持每列一个采样点的等距位置，时间用于刻度标签，纵轴为对数采样点周期。Notebook 默认只有一张完整因果 SST 幅值图，图像 metadata 记录数据内容及绘图定义身份，纯绘图不参与数值产物身份。

2026-10-07 已在 `latitude_env_v2` 逐代码单元格完成 c03 真实 demo：两种核各覆盖全部 21,316 点，复系数／导数／状态与 c01、正式复谱与 c02 逐项一致，状态计数与上文 c02 相同，无新增数值失败。因果计算分为七个连续块，包含训练／测试分界，拼接宽表及末端状态与整段一致，完整训练前缀不因追加测试输入改变。真实 CWT-only 和 Morlet constant 延拓也已调用并验收。两种核各保存一份 21,316 行序列表到 `c03_RollingWavelet_DemoData/`，已复读核对 Schema、生成身份及内容摘要；默认单图已实际渲染检查。13 项独立合成契约与边界测试通过，包含跨块源报价时刻、缺失分段、坐标边界、Schema 往返、无重算单图和零幅值相位。

临时回归检查与构建／执行工具暂存 `R00_draft_collection_02/tests/test_rolling_wavelet.py` 及 `R00_draft_collection_02/scripts/*rolling_wavelet*`，不作为算法导入入口，去留尚待确认。

[c04_SpectralFeatures](a01_Methods/b06_WaveletAnalysis/c04_SpectralFeatures.ipynb)实现 `compute_spectral_features`、`extract_sst_features`、`build_spectral_feature_schema` 与 `plot_spectral_features`。逐点读取已有 SST 复谱，提取质心、扩散、主频与主周期、峰值占比、谱熵、高低频比、偏度与超额峰度、圆周相位一致性、有效频率数、指定分位带宽、中位频率、频率四分位距及总／两侧平方范数，共 17 项。默认按区间宽度计算功率质量，矩统计使用幅值权重；等区间权重、功率矩、分频切点、有效频率比例、带宽分位及 partial 纳入策略暴露为参数。主频依据功率密度，频率分位使用中心离散逆 CDF；平方范数不解释为原信号能量守恒。矩定义参见 [谱描述符公式](https://www.mathworks.com/help/audio/ug/spectral-descriptors.html)，相位采用 [圆周合向量长度](https://docs.scipy.org/doc/scipy-1.17.0/reference/generated/scipy.stats.circvar.html)的加权形式，具体求积及退化约定见 Notebook。

默认保留上游部分谱并标记 partial，可显式排除；零谱只定义总／两侧平方范数和有效频率数为零，主频、分布与相位未定义。单频的偏度与峰度为空，零低频分母的高低比为空；极大／极小谱先缩放计算分布，不可表示的绝对范数单独记录。逐字段原因与上游状态同时保留，不填零、加 epsilon 或删行。因果 `feature_available_at` 继承 c03 的可得时刻，Morlet 始终为空。绘图不触发计算，`plot_type` 选择一个已有特征，Notebook 默认只嵌入一张完整因果谱熵图。

2026-10-07 已在 `latitude_env_v2` 执行两种核各 21,316 点完整真实 c04 demo，直接消费 c03 的上述 RB 序列表，并实际调用纯谱入口、功率矩、区间等权和 partial 排除。因果特征状态为 16,055 个 ok、4,982 个 partial、186 个 warmup、92 个 degenerate、1 个 failed；Morlet 为 17,403 个 ok、3,743 个 partial、124 个 boundary_incomplete、45 个 degenerate、1 个 failed。failed 均继承上游首个收益率缺失；退化点是上游已有零谱，没有新增数值失败。完整真实平方范数、概率归一、矩范围、连续分块等价、训练前缀不变和信息时刻核对通过。两份特征表归 `c04_SpectralFeatures_DemoData/`，已复读核对 Schema、生成身份和内容摘要。14 项独立解析与边界测试通过，覆盖非均匀网格、圆周跨 ±π、权重切换、尺度不变性、零分母、分位端点、数值极值和无重算单图。

临时回归测试 `R00_draft_collection_02/tests/test_spectral_features.py` 可用于后续修改校验，构建／执行工具为 `R00_draft_collection_02/scripts/*spectral_features*`；它们不作为算法导入入口，去留及最终目录待用户确认。入口归属与真实 demo 要求见[研究规则](AGENTS.md#目录与文件)，本节由[根索引](../AGENTS.md)引用。

## 分界采样

`b03_Sampling/` 只有两个维护单元：`c01_SamplingPositions` 定义采样轴、初始单位校准和位置；`c02_SamplingExecution` 定义报价准备、价格生成、保存和单图交互选择。各自的代理只加载同名 Notebook 导出定义。位置和价格 demo 分别归 `c01_SamplingPositions_DemoData/`、`c02_SamplingExecution_DemoData/`；执行及绘图直接引用所需生产单元的文件。文件名前缀使用所属 Notebook 基名，并注明固定／滚动模式及 K 等关键条件。

配置为 CU、RB，N 为 10、15、30、60，轴为实际分钟、成交额与 GK 波动率乘成交额；时间轴每分钟增量为 1，不补休市分钟，校准得到 a=N；数量函数支持全部合约合计或当时主力，demo 只使用全部合约合计。GK 使用当分钟主力 OHLC 计算单分钟方差后开平方，不平滑、不年化。初始校准训练期为 b02 输入中 `trading_date < 2025-01-01` 的全部记录，单位 a 为 N 乘训练期平均每分钟轴增量；零增量分钟参与平均。校准后固定 a，滚动分界不重新校准，也不重新复权。

采样入口必须提供 t。t 属于训练期，过去序列按时间递增排列，最后一个点显式取 t 的 close，计入 K；未来从下一实际分钟开始。单分界 `K=None` 返回允许范围内完整序列；滚动时对给定的递增 t 序列逐批生成，每侧最多 K 点，不足返回实际点数。将首个实际分钟的起点作为 t，训练为空，整个输入即测试期；仍使用同一个分界采样函数和已经确定的 a。

两种方式为分钟内 log open/close 线性插值后还原普通价格，以及分钟末达到或超过 a 时取 close、清零并舍弃盈余。过去方向扫描当前分钟的增量，达到单位时取上一实际分钟的 close：从 09:35 扫描，若该分钟增量已经达 a，则新增过去点为 09:34 close。未来方向扫描 t 后的实际分钟，达到单位时取该扫描分钟 close。休市不补行，跨 Session、交易日及分界不重置；不足单位的尾量舍弃。

各合约共用位置和比例，仅输出源分钟有记录的合约，缺价格保留空值、不填充。分钟内插值需要该合约 open、close 都可用，精确末端只需要 close；缺价格仍占用采样点数。所需数量缺失或主力 OHLC 无法计算 GK 时明确报错。Notebook 每次通过品种、N、轴与方式参数运行一个配置，不包含遍历全部 48 个配置的批量执行逻辑。

累计轴及清零双向跳转表一次准备。滚动插值按各采样距离使用单调游标，清零模式沿预计算映射跳转；报价以实际分钟的稀疏合约行索引，log 价格只计算一次。Numba 内核合并索引、插值及指数映射，返回普通 float64 价格。滚动结果按批次消费和保存，不将全部时点的展开价格集中保存在内存。

价格序列和 GH 拟合分别绘制为独立图。价格图使用 log 主力价格，以采样顺序作横轴并稀疏标注交易日；分布图使用白底黑边直方图和蓝色 GH 曲线，无网格。每个 t 的过去和未来连成一个序列后计算一次 log 差分，分界两侧的相邻采样点也计算收益率；不同滚动 t 的窗口之间不差分。缺价格直接产生空收益率，不先删除缺价点再跨缺口计算。拟合使用全部非空 log 收益率，默认开启，失败明确报错。图形坐标默认按完整收益率范围和密度自动缩放；显式设置 xlim、ylim 时仅限制显示。全部 48 组均分别生成价格图和 GH 图，不增加平滑曲线、额外分布及标注。价格执行 Notebook 以 `display_sampling_demo_figures` 作为图像呈现入口，调用后在当前单元格显示品种、N、轴、采样方式、图形类型五项选择控件，并只显示一张图。选择控件采用紧凑尺寸，“复制图像”按钮将当前图片按原始分辨率复制到本地 Windows 剪贴板。需要多图对比时，在新的单元格再次调用，各次调用的状态相互独立。96 张图片及其来源信息保存在同一本 Notebook 的内嵌资源中。交互选择复用这些图像，不重新采样或拟合；Notebook 不保留逐配置的静态双图展示区。

b02 输入使用训练期双向复权、测试期固定起点的前复权。训练期首尾保持原价，偏移只使用训练期信息；测试期第一个实际主力分钟保持原价，只累计测试期内部后续换约偏移。各分钟全部合约使用同一个 log 偏移，分界不作换约补偿。两份 b02 demo 已验收；被替代的全局采样 demo 及编号变更前的 K=32 有界 demo 均已删除。读取时核对生成身份与范围，不匹配明确报错。

48 组采样与图像状态：48 个固定分界完整序列配置已保存并复读验收，共 96 份位置／价格 Parquet；全部配置的 96 张独立价格／GH 图已完成，并内嵌于价格执行 Notebook。单图选择器覆盖全部 48 组，默认显示 RB、N=10、实际分钟轴、分钟内插值的价格图；既有 8 次拟合在确认相同数据内容后复用，其余 40 次 GH 拟合分别完成。

两本采样 Notebook 的 `demo_reuse_existing` 默认设为 `True`，在准备完整分钟输入之前检查已保存结果。上游身份、参数、初始校准范围、生成定义、数值环境及结果内容摘要一致时，直接读取位置或价格，跳过分钟轴、报价准备、初始校准和采样；缺失或条件变化时才计算，损坏文件明确报错。`demo_threads=8` 与已验收产物的并行参数一致。设 `demo_reuse_existing=False` 可显式重算采样数据。图像选择器只读取内嵌图片并核对来源，缺图或图像来源变化时明确报错，不自动绘图或重新拟合。滚动 demo 使用独立文件，尚未保存时首次计算，随后可复用；滚动分界始终使用固定的 a。

复用控制只标记 `export`，不改变生成算法、局部 Schema 和已有产物身份。本次默认位置／固定价格及全部 48 组绘图的复用检查通过，已有 96 张 PNG 保持原样；检查记录暂存于 `R00_draft_collection_02/sampling_reuse_guard_20261007_73c8700a/verification.json`。滚动复用分支使用控制流夹具检查，未因此生成真实滚动数据。

本次范围为 CU、RB × N=10、15、30、60 × 实际分钟、成交额、GK×成交额 × 两种采样方式，共 48 个固定分界完整序列配置、96 份位置／价格数据文件、96 张独立图。初始单位使用完整训练历史校准，t 为训练末实际分钟，K=None；新增时间轴仍复用相同的 t、K、位置与价格函数。单配置生成和绘图保留在 Notebook，批量驱动仅在草稿区。

完整批次的源快照、逐配置摘要、监控状态和图像验收材料暂存于 `R00_draft_collection_02/sampling_single_image_batch_20261007_538fc19a/`：`final_verification.json` 记录生成结果，`postrun_selector_verification.json` 记录全部 96 种真实图像选择、单图显示和独立调用的检查；PyCharm 界面渲染尚未实测。既有两条轴的 32 组数据及 8 次 N=10 拟合验收记录位于 `R00_draft_collection_02/sampling_rebuild_20261006_9e862d81/` 与 `R00_draft_collection_02/sampling_continue_20261007_7d9ca2fc/`；复用既有拟合时须确认对应数据内容摘要一致。

## 收益／波动率建模

`b04_ReturnVolatilityModeling/` 保存同一套正式建模函数的四个单元，同名代理可安全导入。Notebook 先做真实小范围内存试算，不输出 demo 数据文件；完整 demo 使用 RB 上游 2010-01-01—2026-09-30 全区间、GK sigma×全合约成交额轴及 N=15，并由云端执行。小范围试错只在内存检查展示，不保存试错记录；完整云端状态与验收归本主题的 c01／c02／c03_CloudRuns；当前季节阶段作业为 [train1v6y4ne9zpv](https://pai.console.aliyun.com/?regionId=cn-shenzhen&workspaceId=351454#/training/jobs/train1v6y4ne9zpv)，完整 demo 尚未验收。c02 的独立区间求和已通过五组原全历史真实序列核对；保存与复读修复已通过存储回归检查。函数定义与 demo 组合调用写入所属 Notebook 的 `export` 单元格：

| 单元 | 职责 |
|---|---|
| `c01_IntradaySeasonality` | 独立构造复权分钟收益、RV 与 BPV 贡献；直接接受现成贡献，分别估计多窗口每日 Huber 曲线，并在训练边界内进行固定初始回填。 |
| `c02_ModelObservations` | 独立准备分钟观测、按既定边界比例积分；收益选择 RV 或 BPV 曲线；显式转换 log／零值并检查非负模型输入。 |
| `c03_ReturnModels` | 六组收益模型的高斯 QML 联合拟合、下一采样步预测和固定参数更新；输出原输入尺度的具名系数、条件方差及 Component 成分。 |
| `c04_VolatilityModels` | 面向 RV／BPV 输入的 ARMA、ARFIMA、HAR、MEM；HAR 使用采样点窗口及 NNLS，不要求平稳；MEM 使用非负递推与准似然；下一步非负预测要求由调用者选择。 |

c01 使用 `estimate_intraday_seasonality(minute_contributions_df, session_calendar_df, contribution_col=..., lookbacks=...)` 生成正常因果曲线；窗口名映射到交易日数或自然日期 `DateOffset`。结果中的 `curves[window_name]` 为交易日 × 分钟位置矩阵，`diagnostics` 记录有效样本、历史范围和失败原因。`backfill_initial_seasonality(..., train_start=..., test_start=...)` 返回训练期回填副本：初始窗口固定使用首次出现后的同一段样本，截断于测试开始日前；夜盘时段变化后整个夜盘进入新历史阶段。Huber 使用固定标准化 MAD，默认阈值 1.345，保留日尺度和曲线均值归一化。

c01 已通过 11 项小型合成检查，覆盖未来数据隔离、缺失窗口、夜盘变化及恢复、训练期截断、三自然年窗口、数值失败、Arrow 输入和相邻行贡献。c02 已通过 15 项合成检查，覆盖独立曲线、先调整后跨日积分、分钟内权重、正权重缺失、首个测试区间、不同 t 隔离、log／零值／MEM 限制、Arrow 输入、数值失败、巨大先前贡献与有限后续区间的精度隔离及实际 b03 位置函数衔接。c03 已通过 19 项合成检查，覆盖六组联合拟合、与 arch 方差递推对照、分数滤波、平稳／可逆根、高阶参数对应、原尺度系数还原、Component 恒等关系、整段与逐点更新等价、失败不重试及 b03→c01→c02→c03 衔接。c04 已通过 23 项合成检查，覆盖常方差 QML 闭式对照、分数滤波、HAR 无未来信息及 NNLS 对照、完整窗口数值失败隔离、不强制平稳、MEM 手算与零值、高阶初始化、输入尺度还原、负预测规则、失败不重试或推进状态、逐点与整段等价，以及 RV／BPV 的 b03→c01→c02→c04 衔接。合成检查仅验证边界与数值行为。四本 Notebook 的业务 demo 已替换为真实上游调用，本批实际执行结果单独验收；不据合成检查宣称真实链路完成。

完整方法 demo 的范围为 2010-01-01—2026-09-30，训练截至 2024-12-31、测试从 2025-01-01 开始，直接读取既有 RB 真实复权产物。c01 构造分钟贡献及 RV／BPV 一年、三年每日季节曲线，初始回填不越训练期末。c02 消费 b03 已验收的 N15 GK sigma×全合约成交额位置，使用一次训练期校准后的不清零 log_interpolate；不在本环节重新采样或校准。试错使用坐标从 0 起的分钟前缀，保留 sample_id 和 source_minute_index；不去季节及两窗口 RV／BPV 收益去季节共五个配置。c03 六组收益模型、c04 RV／BPV × 四模型的完整 demo 仍使用不去季节原尺度输入，每点使用此前最多 500 个采样观测重估；c04 的完整配置要求非负预测。云端只分配目标位置，各自读取完整此前历史，十节点回收核对覆盖后合并；三个串行阶段共 24 个产物，验收后安装到各方法 DemoData。研究输入上传后采用只读签名 HTTPS GET 下载到节点本地盘并核对摘要及输入身份，不经 OSS 挂载读取大型研究文件；下载失败保留现场，不自动重试。

四本环节 Notebook 各保留 15 格 demo，试错与正式 DLC 分章。先读完整输入再切小样本，试错不保存结果；完整任务使用同一完整输入，接续已有作业，每 60 秒查询，回收后独立复读展示。c03／c04 共用 models 作业，没有新增执行脚本、停止单元格或恢复自动化。此排列不约束实验层。旧外部执行层已删除，历史来源及失败证据保留。

修复后四本试错的读取、切片、调用检查和展示已逐本重跑：c01 八个交易日、1,800 分钟；c02 五配置各 64 点；c03 六模型、c04 八组各 64 个逐点目标。每组观测仍保留首点无前序及一个缺失贡献区间。初始有效起点在完整序列上定位一次，有效历史随目标积累，窗口内部的缺失不会被删除。c04 小试错显式使用 ARMA／ARFIMA／HAR 的 log 输入和 MEM 的原尺度输入；仅在 log 前将有效零值替换为 1e-12，输出表记录 input_transform。完整 demo 的原尺度配置未因此改变。

| 真实小试错 | 目标位置 | 有效预测 | 拟合失败 | 成功观测更新 | 更新失败 |
| --- | ---: | ---: | ---: | ---: | ---: |
| c03 六组收益模型 | 384 | 324 | 60 | 323 | 1 |
| c04 RV／BPV × 四模型 | 512 | 416 | 96 | 416 | 0 |

本次拟合失败均是起始空历史、单值无变化或观测数不足；没有优化未收敛。EGARCH 的一次更新仍失败，当前拟合下下一步 log(h) 约 3897.61，超过 float64 的正有限表示范围。保留原失败系数另行复核时 log(h) 约 4411.15，同样明确失败；未裁剪或换模型。BPV ARFIMA 的原尺度负预测案例仍按非负要求失败，log 调用允许负预测并通过该点更新。全部目标、原因和失败状态保留，未填值、删行或重试。

所有小试错结果仅留内存，DemoData 的文件内容和修改时间未变；Notebook 未保存试错输出。初始／内部缺失边界、对数方差递推、整段与顺序更新、原失败点及 log／MEM 输入边界已单独核对，其他五类收益模型与冻结原实现的真实样本结果一致。相关模型、实验调度及存储回归检查共 62 项通过。上游完整成果未就绪时，试错从全量真实分钟和既定位置切小前缀并调用当前上游函数；完整 DLC 仍须已验收完整成果，不用试错输入替代。2026-10-07 的首个新流程作业 trainf0r85m4yj28 在导入数据契约时因遗漏 polars 失败，未进入完整业务计算。部署修复补齐 polars、固定 statsmodels 的本地版本、恢复 c01 导出及步骤 metadata、修正季节入口先下载后验收；四模块上传包导入、顶层依赖覆盖、四个入口下载顺序及四项存储回归检查通过。修复后作业 train1v6y4ne9zpv 已提交，普通 Python 串行执行并每 60 秒查询同一身份。用户已明确授权失败后由本聊天排错、修复和显式重提，已创建每 30 分钟唤醒的定时任务 dlc；正常状态安静结束，24 个成果全部验收后停用。当前尚未完成云端验收；当前代码或 Schema 已变化，旧成果继续保留原身份，不能冒充本次修复后的成果。

c02 使用 `prepare_minute_observations(..., rv_curve=..., bpv_curve=..., return_seasonality=...)` 连接所选每日矩阵，再用 `aggregate_model_observations(minute_observations_df, sample_position_df)` 对完整实际分钟轴积分。每个指标保留状态和原因，同一 t 首个训练点为区间起点；首个测试点可从 t 计算。收益采用分钟贡献积分，分钟内结果不等同于采样 open／close 插值价格差分。`transform_model_input(..., observation_col=..., transform=..., zero_replacement=..., require_nonnegative=...)` 独立选择输入尺度和零值规则，不自动填缺失或改变模型。 `initial_model_input_position` 为既定完整模型序列返回首次有效观测位置，逐点调用者只定位一次；窗口内部缺失仍保留。

c03 使用 `fit_return_model(observations, model=..., mean_order=(AR,MA), volatility_order=(ARCH,GARCH), ...)` 在全训练序列上进行一次高斯 QML 联合估计。ARFIMA／FIGARCH 显式指定 `fractional_memory`，初始化和数值选项由调用者选择；FIGARCH 的 p、q 各取 0 或 1。成功结果包含具名 `parameters`、`specification`、训练路径 `in_sample`、有限历史 `state` 和 `next_forecast`。`predict_return_model(state)` 不推进状态；`update_return_model(state, observed_return)` 返回参数不变的新状态。Component 输出长期方差和可为负的短期偏离；失败返回原因且不提供可调用新状态。系数及预测均处于调用者输入尺度，数学表示、参数范围与调用示例见所属 Notebook。 EGARCH 用对数方差状态与对数域 QML，正式方差超出 float64 正有限表示范围时返回明确原因及 numerical_diagnostics，不裁剪。稳定参数范围与观测滤波可逆性的区别见 [Wintenberger](https://arxiv.org/html/1211.3292)。

c04 使用 `fit_volatility_model(observations, model=..., ...)`，ARMA／ARFIMA 指定 `mean_order`，HAR 指定 `har_windows`，MEM 指定 `mem_order`；ARFIMA 显式给出分数截断。`har_window_features` 可独立构造只使用过去观测的采样点均值。HAR 开头不足窗口的行保留状态，仅完整窗口行进入 NNLS；可用 `presample.observations` 提供前历史。`predict_volatility_model(state)` 不推进状态，`update_volatility_model(state, observed_value)` 返回固定参数的新状态；`require_nonnegative_prediction=True` 时负预测失败，False 时保留有限负值，模型不内部取 log 或指数还原。HAR 返回窗口均值及预测贡献，MEM 返回条件均值及乘性误差；ARMA／ARFIMA 的创新方差与波动观测预测明确区分。

逐日大实验与既定采样序列上的逐点重估将复用同一组函数，外层采样及拟合调度归具体实验。模型函数不硬编码实验日切分；模型直接输出输入尺度的预测，同一运行的状态留在内存；不收敛或无法有效预测时返回明确失败及原因。具体阶数、窗口、初始化等在调用时指定。

## 实验目录与对照组

首个实验草稿为 [b01_ReturnVolatilityForecasting](a02_Experiments/b01_ReturnVolatilityForecasting/experiment.ipynb)，用于 RB、CU 的“收益与波动率预测实验”。模型训练窗口采用三年、五年两档，与两品种、三种采样轴、三种尺度形成 36 个基础配置。整体范围为 2010-01-01 至 2026-06-30；整体训练期为 2010-01-01 至 2024-12-31，逐日测试覆盖 2025-01-01 至 2026-06-30 的全部交易日。采用累计刻度不清零的 log_interpolate，按分钟汇总同品种全部合约的 money（成交额），用于交易额及 GK×交易额轴。季节处理采用不去季节、120／360 个交易日，收益去季节同时纳入 RV、BPV 两种曲线。固定阶数、分数历史截断、HAR 窗口与输入尺度已写入 [config.yaml](a02_Experiments/b01_ReturnVolatilityForecasting/config.yaml) 的 54 个模型／季节组合；log 仅将零替换为 1e-12，非零值保持原值。三年／五年使用完整指定训练窗口，未覆盖时保留失败。本批执行完整 1,944 项配置，用户已取消 5 元金额上限。云端采用 testing_11 的 PUBLIC 网络、PyTorchJob、ecs.c6.large（每节点 2 核／4GB），镜像经确认采用官方 Python 3.13.9 CPU 镜像；每作业 10 个节点，作业串行提交。完整测试期作为一个日期块，各节点按 RANK／WORLD_SIZE 分配独立交易日，回收时核对十份日期与文件覆盖后合并，在原 MLflow Run 登记。沿用旧入口未指定最长运行时间的设置，不额外添加一小时试跑上限。每节点小时的实际询价为 0.432 元，10 节点对应计算资源每小时 4.32 元，不含 OSS；费用资料已保存。配置及实际代码在启动时冻结，故障停止后续作业，不自动重试。本批不做耗时试跑。Notebook 已接入配置读取、按月重新准备数据、全局季节估计、一次性阈值校准、每日重采样及拟合／预测调用，并补充固定输入、36 个基础配置的任务展开、单任务结果保存、DLC 显式提交／查询／回收和原 MLflow Run 登记。原 35 项正式入口离线检查及 3 项控制器离线检查通过，其中本地 MLflow SQLite 实际登记已验证。会话修正后的 24 项云入口离线检查通过，新增检查使用真实 Estimator 内部提交流程并模拟外部 API，确认其读取显式 PUBLIC 默认会话。RB、CU 的完整输入已冻结并复读验收，RB 输入已上传并核对远端摘要；首次 DLC 创建请求失败，尚无确认的 job ID，模型拟合／预测和云端成果验收尚未执行。本次重新读取正式 silver，并调用现有主力识别、复权及后续方法重新准备实验输入；不触发 API 采集，不直接沿用以前的研究产物或可变 demo。

正式输入批次 [20261007_4754b3d9](a02_Experiments/b01_ReturnVolatilityForecasting/Tracking/input_batches/20261007_4754b3d9/prepared_inputs.json) 已完成 RB、CU 的完整主力、复权、分钟贡献、RV／BPV 季节曲线及九组固定阈值，输入已冻结并复读验收。云批次 [20261007_692110a5](a02_Experiments/b01_ReturnVolatilityForecasting/Tracking/cloud_batches/20261007_692110a5/status.json) 已展开全部 1,944 项配置并上传 RB 输入，在首次 CreateJob 请求时连接 pai-vpc.cn-shenzhen.aliyuncs.com 出现 TLS EOF，完成数为 0/1944，批次停止。停止时按原 Run 唯一名称查询到 0 个匹配作业；提交结果保留为 submission_uncertain，没有确认的 DLC job ID。已核查 alipai 0.4.13 的 Estimator 基类和内部提交读取全局默认会话，覆盖传入会话；正式入口改用 setup_default_session 显式设置 PUBLIC 网络及 OSS 公网端点。修正仅通过离线检查，未再次调用创建接口；[故障核查记录](a02_Experiments/b01_ReturnVolatilityForecasting/Tracking/cloud_batches/20261007_692110a5/submission_failure_review.json) 保留证据及检查边界。原监控随终态退出。用户已要求先做 Notebook 内存试算，再将完整方法 demo 放到云端；完整范围为 2010-01-01—2026-09-30，采用 GK sigma×全合约成交额轴、N=15。旧方法云执行批次的 seasonality 作业 `train15v8qvee4e1` 已成功并回收，observations 作业 `train1q4oxsaj9fy` 已失败，models 未提交；2026-10-07 已直接查询确认两项作业均为终态。旧代码、云结果和失败记录保持冻结身份。旧额外执行层已删除；环节函数的 15 格 demo 已实现并通过离线检查，新流程本轮未提交云作业。实验层不采用这套 demo 排列，其独立接续改造与验收以所属运行记录为准。方法 demo 不改变本实验截至 2026-06-30 的 scope 或 1,944 项配置；完整云 demo 验收后重新准备正确来源身份的冻结输入，再接续每作业 10 节点的串行正式云批次。旧冻结输入保持原身份；c01 共用保存逻辑及 c02 数值汇总的源码摘要已变化，下一批重新准备并冻结实验输入，不能改写旧 manifest。执行状态见 [完成记录](a02_Experiments/b01_ReturnVolatilityForecasting/Tracking/cloud_completion.json)。

原 `20261007_365dd87b` 输入批次因可见监控心跳中断停止，`20261007_d20d497b` 因代码身份核对失败停止；已核实 linecache 摘要错误引用文件路径，改为实际源码行，并通过独立代码包子进程核对。本批重新生成输入，不改写旧冻结 manifest；旧产物、失败记录及启动检查均保留。旧交易所代码失败记录仍归 `20261007_f58d1195`，当前正式读取使用 XSGE。故障停止后续阶段，不自动恢复或重试。

每个实验对应一个业务一级目录，目录业务词根与实验英文名一致，中文标题使用对应译名。整体范围及训练／测试划分由 `config.yaml` 明确。每组参数不另建代码目录；下图的 `Tracking/` 仅在实验使用跟踪记录时建立：

```text
a02_Experiments/
└─ bNN_<实验英文名>/
   ├─ experiment.ipynb
   ├─ experiment.py
   ├─ config.yaml
   ├─ Data/                      # 本实验共享输入和缓存
   └─ Tracking/
      ├─ mlflow.db                   # 参数、指标、状态
      ├─ artifacts/                 # 配置快照、选定模型与预测
      └─ staging/                   # 代码包和下载暂存
```

Notebook 保存组合函数和显式启动／回收单元格；代理只加载标记定义。`config.yaml` 是基础参数唯一编辑位置，对照列表仅描述覆盖值。以下 `???` 必须由具体实验填写，不能作为运行默认值：

```yaml
scope:
  period: {start: "???", end: "???"}
  train: {start: "???", end: "???"}
  test: {start: "???", end: "???"}
  interval_closed: "???"
  timezone: "???"
  read: {start: "???", end: "???"}
  adjustment: {start: "???", end: "???"}
input: {}
parameters: {}
execution:
  target: local
```

无复权时须显式说明 adjustment 不适用。验证集、标签边界或滚动训练按研究需要加入。首次运行保存 scope 快照和摘要；已有运行成果的实验改变范围／切分须另建具有可区分名称与编号的实验目录，不在参数扫描中覆盖 scope，不跨 scope 引用研究产物。

同一实验 scope 内缓存仍需核对输入内容、品种、复权口径、方法及依赖代码、参数和拟合条件。train/test 默认按日期选取同一表。普通中间量只在内存，昂贵且复用的结果进入 `Data`；每次对照记录小指标，模型和全量预测按目的选留。

## 工具分工与云计算

| 对象 | 工具与归属 |
|---|---|
| 基础配置、参数覆盖 | Hydra 组合具体配置，Notebook 显式循环对照 |
| 使用 MLflow 的实验 | 独立本地 MLflow SQLite 数据库 |
| 一个比较主题 | MLflow Experiment |
| 一组参数与种子的一次执行 | MLflow Run，记录输入、配置、代码版本 |
| 一项云作业 | alipai 提交、查询；DLC job ID 记入 Run |
| 共享特征 | `Data`；MLflow 只记路径和摘要 |
| 选定模型、预测、图表 | 本实验 MLflow artifacts，避免第二份副本 |

Hydra [Compose API](https://hydra.cc/docs/1.3/advanced/compose_api/)适合 Notebook，不自动提供 multirun 或 DLC 调度。MLflow [数据库存储](https://mlflow.org/docs/latest/tracking/backend-stores/)集中小记录，大文件仍单独保存。以上是使用这些工具时的目录约定，实际入口由具体实验决定；当前 b01 的读取和本地复现入口不写 MLflow，也不创建 `Tracking/`。

使用本地 SQLite 时，数据库地址与附件地址必须分别设置。只设置 `tracking_uri`，新 Experiment 仍可能获得当前工作目录下的 `mlruns` 附件位置；已有 Experiment 和 Run 的附件地址也不会随新配置自动改变。按[根级输出归属](../AGENTS.md#草稿区与文件收纳规则)和[研究规则](AGENTS.md#hydramlflow-与-dlc)，写附件前先核对 Experiment 与 Run，位置不符时停止，不在仓库根目录写入或用 Git 忽略规则隐藏。

下面展示本地记录的路径约束。先按[环境模板](../.env.template)取得 `candidate_root`；调用方明确给出本实验绝对路径 `experiment_dir`、比较主题 `experiment_name`，以及 `run_id`（新建用 `None`，恢复用已有 ID）。示例不启动计算或云作业：

```python
from pathlib import Path
from mlflow.tracking import MlflowClient

experiment_dir = Path(experiment_dir)
experiments_dir = (candidate_root / "R04_Research/a02_Experiments").resolve()
if not experiment_dir.is_absolute() or experiment_dir.resolve().parent != experiments_dir:
    raise ValueError("必须指定 a02_Experiments 下本实验的绝对目录")
tracking_dir = experiment_dir.resolve() / "Tracking"
tracking_dir.mkdir(parents=True, exist_ok=True)
database_uri = "sqlite:///" + (tracking_dir / "mlflow.db").as_posix()
artifact_uri = (tracking_dir / "artifacts").as_uri()
client = MlflowClient(tracking_uri=database_uri)
experiment = client.get_experiment_by_name(experiment_name)
if experiment is None:
    if run_id is not None:
        raise ValueError("恢复 Run 时必须已有对应 Experiment")
    experiment_id = client.create_experiment(experiment_name, artifact_location=artifact_uri)
else:
    if experiment.artifact_location != artifact_uri:
        raise ValueError("已有 Experiment 的附件位置不属于本实验，停止写入")
    experiment_id = experiment.experiment_id
run = client.create_run(experiment_id) if run_id is None else client.get_run(run_id)
if run.info.experiment_id != experiment_id:
    raise ValueError("Run 不属于指定 Experiment")
if run.info.artifact_uri != f"{artifact_uri}/{run.info.run_id}/artifacts":
    raise ValueError("Run 的附件位置不属于本实验，停止写入")
# 通过后才调用 client.log_artifact(run.info.run_id, str(选定成果路径)) 等写入接口。
```

发生地址不符时，应先明确原记录和成果的整理范围，再处理该记录；不修改冻结源码、历史输入身份或已验收结果来掩盖旧路径。scope 校验、输入身份及回收仍由具体实验实现。

本地和云端调用同一组定义。仅环节函数 Notebook 的 demo 部分按“内存小试错 → 完整输入与提交 → 60 秒查询 → 回收验收与保存 → 独立展示／绘图”组织。完整提交只冻结已有 Notebook、代理、加载器和必需依赖，不生成额外执行脚本；短 python -c 命令直接加载正式定义。提交前固定代码与已解析配置，代码包排除大型已保存输出和本地凭据。云端显式读取输入通道、调用计算函数；提交代码仅在本地运行。[PAI 输入输出说明](https://help.aliyun.com/zh/pai/developer-reference/submit-a-training-job)

```text
OSS/research/<scope_id>/
├─ inputs/<input_id>/
└─ runs/<run_id>/
   ├─ code/
   └─ output/
```

过程：固定配置并创建 Run → 上传或复用本 scope 输入 → 提交并保存 job ID → 云端输出指标 JSON 和选定成果 → 本地暂存下载、复读及摘要校验 → 持久保存并更新原 Run → 标记远端输出可清理。

本地 SQLite 不由云任务共同写入，不合并云端数据库。Notebook 关闭后按已保存 job ID 查询和回收；云成功但未回收须可辨认。未回收输出、活跃输入和失败现场不按普通缓存删除。DLC 控制台提供云端监控；Notebook 每 60 秒查询，故障先保留现场并交回排错，不新建看板／监督层，不默认创建 Agent 恢复任务。b04 的聊天定时接续另有明确授权，范围及完成条件见本主题说明。具体规则见[Notebook DLC 流程](AGENTS.md#notebook-dlc-流程)。

环境及实测边界见 [环境说明](../environment/README.md)：历史验证证明云端显式依赖及 JSON／Parquet 回传，不构成完整实验链路验收。首个实验已验证本地 Hydra 配置组合、冻结代码包独立导入和草稿区临时 MLflow SQLite 登记；尚未验证本实验代理及模型在云端运行。云镜像须满足冻结的 Python 和数值依赖版本，历史 Python 3.9 CPU 镜像不能据此认定可用。本次未安装依赖、创建正式实验数据库、上传真实实验输入或提交云任务。

## 数据和维护边界

正式 silver 通过 `settings.futures_lake_root` 按[湖仓规则](../R02_Market_Data/a02_Lake/AGENTS.md)和[数据契约](../config/data_contracts.py)只读使用。校验边界遵循研究规则中的[上游契约与研究校验](AGENTS.md#上游契约与研究校验)：信任正式仓库已保证的质量，保留方法所需条件及自身输出性质的检查。demo、缓存及成果保存在研究目录，使用局部明确的 Arrow Schema；不默认写入湖内 gold，不改变17张稳定 silver 表。研究 demo 使用所属方法的局部 Schema，下游核对生成身份、实际内容摘要和适用范围后，信任已验收的上游输出契约，并检查自身方法所需的条件。

b01、b02 的正式 silver 读取直接使用 PyArrow Dataset 和统一转换入口，不额外调用 `validate_arrow_table()`，不逐 Dataset／fragment 重验 Schema／metadata，也不在读取前另行扫描行数。方法不复验正式输入已保证的字段、时区、主键和数值范围；b03 确认上游 demo 生成身份后，信任 b02 已保证的字段、主键与主力身份关系。方法仍检查自身需要的样本与连接、可空输入的实际可用性、log 正价格和换约锚点，以及采样精度和变换后的输出性质。外部或合成输入由调用方先按来源契约校验。`arrow_to_pandas()`／`arrow_to_polars()` 内部仍调用 `validate_arrow_table()`；这两个共享函数的实现保持不变。

代码、配置和方法 demo 数据进入 Git。[.gitignore](../.gitignore)统一放行 `a01_Methods/**/c[0-9][0-9]_*_DemoData/*_demo.parquet`，[.gitattributes](../.gitattributes)将其声明为二进制文件；超过 100 MiB 的 demo 按实际文件路径登记 Git LFS 属性，检出后运行 `git lfs pull` 取得原路径下的完整 Parquet，数据字节和 metadata 保持一致；其他格式的正式 demo 按实际入口补充对应规则。实验数据、SQLite 及附属文件、artifacts 和暂存包按所属目录规则留在本地。忽略规则不清理磁盘。缓存清理保护活跃输入和保留成果依赖；严格复现还需可取得的原输入和代码版本。

原 `04_Feature_Engineering` 五个工作区文件原样暂存在[草稿目录](../R00_draft_collection_02/feature_engineering_before_research/)，摘要见 [PRESERVATION.json](../R00_draft_collection_02/feature_engineering_before_research/PRESERVATION.json)。其去留、复用与最终归属待用户按[草稿区规则](../AGENTS.md#草稿区与文件收纳规则)确认。旧 README 链接及启动命令属于历史路径；缺失 `c00_lakehouse` 和日线分区缺少 `underlying_code` 的两个阻塞未修复。正式入口不从暂存目录导入，用户已删除的旧 AGENTS 未恢复。

加载器[离线测试](../R00_draft_collection_02/tests/test_research_notebook_loader.py)在仓库根使用 v2 执行：`python -B -m unittest discover -s R00_draft_collection_02/tests -p test_research_notebook_loader.py`。测试只使用临时合成 Notebook，不读取湖或调用云 API。

同步入口：[根 README](../README.md)、[根 AGENTS](../AGENTS.md)、[研究 AGENTS](AGENTS.md)、[湖仓规则](../R02_Market_Data/a02_Lake/AGENTS.md)、[环境模板](../.env.template)、[采集规则](../R02_Market_Data/a01_Collection/AGENTS.md)、[.gitignore](../.gitignore)、[.gitattributes](../.gitattributes)。

采样与复权的合成回归检查暂存于 `R00_draft_collection_02/tests/test_sampling_axis_calibration.py`，滚动位置及价格检查位于同目录的 `test_sampling_positions_execution.py`，摘要分批、偏移广播与重复保存检查位于 `test_main_adjustment_streaming.py`；运行不读取正式湖。测试材料的保留与最终归属待按草稿区规则确认。

被替代全局采样入口的批次 Notebook、日志、源快照和核验记录暂存于 `R00_draft_collection_02/sampling_axis_batch_20261006_93104684/`，路径和文件名保留历史记录。当前分界采样的有界验收及性能探索材料位于 `R00_draft_collection_02/sampling_execution_efficiency_20261006/`；其中的临时编辑、测试和批量工具不属于正式 Notebook 或研究目录入口。草稿材料的保留、删除与最终归属待使用者决定，当前均保留。
