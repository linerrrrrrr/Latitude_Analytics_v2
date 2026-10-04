# 中国商品期货日内波动预测方法复现

## 项目目标

本项目基于正式期货湖仓，复现论文 *Volatility forecasting in the Chinese commodity futures market with intraday data* 的统计方法与预测比较框架。论文原始数据来自 GTA；本项目使用 JQData 湖仓及 2010 年以后的可用样本，因此目标是方法复现，不声称复现论文中的原始数值。

论文原文位于：

`C:\Users\31918\Desktop\bookshelf_02_期货外刊\Volatility forecasting in the Chinese commodity futures market with intraday data.pdf`

项目规则见 [AGENTS.md](AGENTS.md)。项目同时遵循仓库根 [AGENTS.md](../../AGENTS.md)、[期货数据库规范](../../02_Market_Data/a02_Lake/AGENTS.md) 和权威 [Schema 契约](../../config/data_contracts.py)。

## 固定研究边界

- 只读正式 `silver` 数据；不调用 API，不写 `raw`、`silver` 或 `gold`。除第 09 项按下述契约产生的项目专属、版本化、经验证 research artifact 外，不创建普通项目缓存或持久化研究中间表。长批次控制脚本与运行现场固定放在本项目 `operations/item09/`，不再使用仓库根临时目录；它们不是研究数据，也不得被第 10—12 项消费。
- 不使用主力连续合约。对每个交易月，按合约代码解析交割年月，选择“交易月后第 3 个月交割”的固定月份合约。
- 换月时直接连接未经复权的合约价格并保留 `is_roll_day`；基线不引入论文未披露的复权规则。
- 只使用真实日盘 Session，排除夜盘；不填补 10:15—10:30 晨休，也不跨 Session、晨休或午休构造跳空收益。
- AL、CU、FU 是论文品种；RB 是项目扩展品种，所有 RB 结果必须与论文品种分开标注。
- SR 因正式湖仓当前没有分钟事实而排除。
- FU 的两个制度阶段作为独立序列估计，不跨停牌期连接收益、模型或滚动窗口。
- 每次对话只实现并验证一个统计项目；只有开始实现该项时才创建对应 Notebook，不预建空 Notebook。

## 固定品种与样本

| 研究序列 | 样本起点 | 样本终点 | 报告口径 |
|---|---:|---:|---|
| AL | 2010-01-04 | 2026-08-28 | 论文品种 |
| CU | 2010-01-04 | 2026-08-28 | 论文品种 |
| FU（旧产品段） | 2010-01-04 | 2011-09-30 | 独立序列 |
| FU（稳定重启段） | 2020-01-02 | 2026-08-28 | 独立序列 |
| RB | 2010-01-04 | 2026-08-28 | 论文外扩展品种 |

固定端点是研究门禁，不表示每个序列在区间内必然具有完全连续的有效观测。Notebook 必须报告合约、交易日、分钟槽位及关键分母的实际覆盖率。

## Notebook 清单与状态

当前不创建任何空 Notebook。状态为“待实现”的文件在对应后续对话开始实现时创建，并在同一次对话中使用 `Python (latitude_env_v2)` clean kernel 从头执行和验证。

| 编号 | Notebook | 统计项目与主要流程 | 状态 |
|---:|---|---|---|
| 01 | `01_sample_and_contract_series.ipynb` | 读取合约与品种日历；解析交割年月；构造三个月交割合约链；标记换月；划定 FU 两段样本；输出覆盖与门禁 | 已实现（clean kernel 通过） |
| 02 | `02_delivery_month_volume_and_liquidity.ipynb` | 构造距交割 0—5 月合约；五分钟聚合；计算 Roll、零收益比例、Amihud；按交割月份统计成交量；复现 Tables 1–2 方法 | 已实现（clean kernel 通过） |
| 03 | `03_multiscale_returns.ipynb` | Session 内生成一分钟收益；按交易分钟序列聚合 5/15/30/60 分钟；构造日收益；描述统计、零收益率及 30 分钟时序图 | 已实现（clean kernel 通过） |
| 04 | `04_intraday_seasonality.ipynb` | 建立日内槽位；估计公式 (11) 周期因子；去季节化；验证重新季节化；展示周期曲线 | 已实现（clean kernel 通过） |
| 05 | `05_volatility_proxies.ipynb` | 计算五分钟实现方差、median-based 方差和 Parkinson 方差；检查覆盖与非负性；比较三个代理 | 已实现（clean kernel 通过） |
| 06 | `06_intraday_garch_family.ipynb` | 联合似然实现 ARMA(1,1)-GARCH、IGARCH、FIGARCH；完成 15/30/60 分钟估计；比较参数、t 值、持久性与长记忆 | 已实现（clean kernel 通过） |
| 07 | `07_daily_garch_family.ipynb` | 对日收益联合估计三类 GARCH 模型；输出 Table 5 风格结果；与盘中估计比较 | 已实现（clean kernel 通过） |
| 08 | `08_arfima_realized_volatility.ipynb` | 构造 15/30/60 分钟 RV；对 log volatility 联合估计 ARFIMA；输出 `d`、AR/MA、显著性和长记忆结论 | 已实现（clean kernel 通过） |
| 09 | `09_rolling_variance_forecasts.ipynb` | 固定长度滚动窗；每日重估；盘中多步预测并重新季节化；汇总为次日方差；生成完整预测面板 | 已实现（clean kernel 与版本化成果门禁通过） |
| 10 | `10_rmsfe_and_forecast_errors.ipynb` | 读取并验证 README 固定的第 09 项成果；对三个代理计算 RMSFE；选择最低误差基准；绘制 Figure 2 风格误差图 | 已实现（clean kernel 通过） |
| 11 | `11_dm_west_forecast_tests.ipynb` | 构造平方损失差；Andrews–Monahan HAC；双侧 DM–West 检验；顺序更换基准复现 Tables 8、10–12 方法 | 已实现（clean kernel 通过） |
| 12 | `12_spa_and_model_synthesis.ipynb` | 10,000 次 stationary bootstrap SPA；汇总最佳与不劣模型；比较日频与盘中模型、采样频率及 RB 扩展结论 | 已实现（clean kernel 通过） |

第 01 项的正式执行结果显示：AL、CU、RB 与 FU 稳定重启段的固定三个月合约覆盖率为 100%；FU 旧产品段在 2010 年 11 月预期的 `FU1102.XSGE` 未上市，因此 22 个交易日按冻结规则显式排除，覆盖率为 94.8357%。项目不使用邻近月份合约替代这一缺口，冻结样本起止日保持不变。

第 02 项的正式执行结果覆盖 76,555 个距交割 0—5 月候选合约日和 17,224,875 个真实日盘分钟行；候选合约日全部具有 3 个完整日盘 Session、225 个分钟事实、45 个五分钟区间和 42 个 Session 内 Roll 相邻对。成交量在全部 76,555 个合约日有效；Amihud 在 75,430 个合约日具有至少一个 `money > 0` 的有效五分钟分母。FU 两段样本均没有 Nearby 合约日，结果保持缺失，不使用 1 个月合约替代。湖仓样本的 15 个“序列 × 指标”比较中只有 3 个在 3 个月处取得最低均值，因此没有机械重复论文原样本的统一三个月流动性结论；RB 仍单独报告。

第 03 项的正式执行结果覆盖 14,153 个固定三个月合约日、3,184,425 个真实日盘分钟和 1,033,169 个 5/15/30/60 分钟及日收益观察；所有合约日均通过 3 个 Session、225 分钟和 `45/15/8/4/1` 槽位门禁。五个序列的零收益比例均随频率降低而持续下降；例如 AL 从五分钟的 22.4810% 降至日频的 0.8653%，FU 旧产品段从 44.6700% 降至 6.9307%。各频率逐日加总均回到同一个 Session 内日收益，最大绝对浮点差为 `1.388e-17`。30 分钟 Figure 1 风格时序图、极端收益日期和 Table 3 风格完整统计均保留在 Notebook；FU 两段和 RB 分开报告。

第 04 项的正式执行结果基于 1,019,016 个固定三个月盘中收益，估计 360 个“序列 × 频率 × 槽位”全文样本周期因子；全部因子有限且严格为正。五个序列在 5/15/30/60 分钟四种频率下均由首槽取得最大波动尺度；五分钟峰谷比分别为 AL 3.1249、CU 3.1491、FU 旧产品段 7.0920、FU 稳定重启段 4.9877、RB 3.2945。去季节化后每个槽位的 `mean(r_tilde^2)` 均为 1，最大偏差为 `4.441e-16`；收益重新季节化最大误差为 `1.388e-17`，平方尺度恒等式最大误差为 `1.735e-18`，零收益计数保持不变。严格历史扩展因子在至少 60 个先前观察后的稳定子样本中，相对全文样本因子的中位绝对差异依序列和频率为 3.5920%—13.4175%，用于明确全文样本基线的前视敏感性，不替换论文基线。

第 05 项的正式执行结果覆盖 14,153 个固定三个月合约日、3,184,425 个真实日盘分钟、636,885 个五分钟收益和 42,459 个日方差代理值。RV、MedRV 与 Parkinson 在五个序列上均为 100% 有效覆盖，全部有限且非负；来源分钟没有 `high < low` 记录。MedRV 每日严格使用 43 个三收益中位数项及 `45/43` 有限样本修正；经典 Parkinson 公式使用 225 个日盘分钟的最高价与最低价。五个序列的 `MedRV / RV` 中位数为 0.6028—0.8726，`Parkinson / RV` 中位数为 0.6574—0.8969，三组代理配对的 Spearman 相关系数为 0.7032—0.9563。首个 AL 合约日的 45 项 RV、43 项 MedRV 与日内区间手工重算最大误差为 `2.711e-20`；FU 两段和 RB 继续单独报告。

第 06 项的正式执行结果在 5 个研究序列、15/30/60 分钟三个频率和 GARCH/FIGARCH/IGARCH 三个家族上完成 45 个初始样本联合 Student-t 似然估计；全部选择的多起点结果均收敛、可行，条件方差有限且严格为正。AL/CU/RB 初始样本各 3,745 日，FU 旧产品段 204 日，FU 稳定重启段 1,414 日，保留的样本外天数分别严格为 300/300/300/200/200。GARCH `alpha+beta` 为 0.9954—1.0000；FIGARCH `d` 为 0.2648—1.0000，其中 7/15 在可用稳健协方差下达到 1% 双侧显著；标准化 Student-t `nu` 为 2.916—4.933；20/45 个 MA 系数在 5% 水平显著为负。39/45 个模型通过 Bollerslev–Wooldridge 稳健协方差数值门禁；其余 6 个因 Hessian 非正定显式报告 `t=NA`，不使用伪逆制造精度。45 模型批次耗时 249.49 秒，ARMA 显式递推与标准化 t 手工对数似然复核误差均为 0；RB 与两个 FU 分段继续独立报告。

第 07 项的正式执行结果对 5 个研究序列分别完成 GARCH/FIGARCH/IGARCH 共 15 个日频初始样本联合 Student-t 似然估计，并在同一本 Notebook 内重新计算 45 个盘中模型作为自包含对照；60 个多起点结果全部收敛、可行，条件方差有限且严格为正。日频 GARCH `alpha+beta` 为 0.9303—0.9981，FIGARCH `d` 为 0.3473—1.0000，标准化 Student-t `nu` 为 3.429—6.178；各序列日频 `d` 均高于对应三个盘中频率的中位数 0.0705—0.6656，但 AL 与 FU 旧段的日频 `d=1` 是边界解，不能按普通内部点解释。日频 14/15、盘中 39/45、合计 53/60 个模型通过稳健协方差数值门禁；AL 日频 FIGARCH 的 Hessian 非正定，显著性明确报告为 `NA`。FU 旧段只有 203 个日频似然观察并单列弱识别诊断；RB 日频 GARCH 持久性为 0.9914、FIGARCH `d=0.5386`，且继续与论文品种分开报告。60 模型批次耗时 266.71 秒，日收益跨 15/30/60/Daily 聚合、ARMA 递推、标准化 t 对数似然和收益/参数单位回缩均通过独立门禁；这些是入样本结构比较，不构成预测优胜结论。

第 08 项的正式执行结果覆盖 14,153 个固定三个月合约日和 42,459 个“日期 × 15/30/60 分钟频率”RV 观察；冻结初始样本共 38,559 个 log-volatility 输入，15 个基线模型的有效似然观察共 38,544 个。15 个 `(1,d,0)` 基线、15 个有效零 RV 检测下限敏感性和 3 个 RB `(1,d,1)` BIC 敏感性共 33 个多起点估计全部收敛，并全部取得有效的联合观察 Hessian 协方差。基线 `d` 为 0.2980—0.4676，15/15 在 5% 水平显著；五个研究序列的 60 分钟 `d` 均低于 15 分钟 `d`，支持波动具有长记忆且估计强度随采样频率变化。有效零 RV 检测下限敏感性使 `d` 的最大绝对变化为 0.0304。RB `(1,d,1)` 的 BIC 比基线低 36.27—49.78，但三个 `d` 均触及 0.499 上界，因此只报告为边界敏感性，不能据此宣称样本外预测更优。33 模型估计批次耗时 4.98 秒；RV、分数差分权重、CSS 似然、方差还原及 22 项门禁均通过独立复核，FU 两段和 RB 继续独立报告。

第 09 项在 2026-08-30 的一次性内存批次执行到 62/120 个 streams、17,000/31,200 次 fits，累计耗时 15999.656s 后 kernel died。旧流程尚未提交合法 checkpoint，内存结果无可挽救；这次失败不构成部分成果，也不能供后续 Notebook 使用。第 09 项因此改造为下述项目内可恢复成果流程。

可恢复批次最终完成全部 120 个 streams、31,200 次逐日重新估计和 214,500 条逐步方差预测。合并成果包含 31,200 条次日方差预测、120 条 stream summary 和 27 项全部通过的 validation gates；最终 manifest、文件 SHA-256、局部 Arrow Schema、完整模型网格与 run `_SUCCESS` 均已复核。Notebook 使用 `Python (latitude)` clean kernel 连续执行 17 个代码单元格，execution count 为 1—17 且没有错误输出。完整 run 的短 ID 为 `eaf4757d3488c2b7`，完整指纹为 `eaf4757d3488c2b74f1e81fa558d8146cdee562836f7fd4084b32ac7ca6fdec6`。

第 10 项完整复核固定 Item 09 run 的四张最终 Parquet、manifest、`_SUCCESS`、文件 SHA-256、Schema 指纹、31,200 行预测和 27 项上游门禁；随后只针对 1,300 个样本外目标日从正式 silver 重算 292,500 个真实日盘分钟、58,500 个五分钟收益以及 RV、MedRV、Parkinson 三类代理。形成 93,600 条预测损失和 360 个“序列 × 代理 × 模型”RMSFE，其中论文基线为 225 个 RMSFE、无前视季节因子敏感性为 135 个。15 个“序列 × 代理”最低误差基准中，FIGARCH 占 8 个、GARCH 占 4 个、ARFIMA 占 3 个；只有 2 个基准来自日频 GARCH 家族，因此当前湖仓样本没有复制论文中 ARFIMA 的普遍优势。Figure 2 风格的 median-based 方差比较中，30 分钟 ARFIMA 的逐日绝对误差小于 30 分钟 GARCH 的比例在五个序列上为 57.00%—83.50%。无前视敏感性中 `FU_legacy / IGARCH_15min_no_lookahead / 2011-08-31` 出现 `1.208743e+09` 的极端方差预测，约为 primary 最大预测的 `1.882045e+11` 倍；它被显式保留并报告为数值或尺度不稳定，不混入第 11—12 项的 15 模型论文基线检验。Notebook 使用 `Python (latitude)` clean kernel 连续执行 12 个代码单元格，execution count 为 1—12、零错误，17 项本项门禁全部通过。

第 11 项再次完整验收固定 Item 09 run，并只用 19,500 条 primary 预测从正式 silver 重算 1,300 个目标日的三类方差代理，形成 58,500 条平方损失和 225 个 primary RMSFE。项目按“基准损失减竞争模型损失”冻结论文符号，对中心化损失差实施无截距 AR(1) 预白化、quadratic-spectral 核、Andrews AR(1) 自动带宽、全部可用滞后、重新着色与常数回归有限样本调整，再以双侧渐近正态分布推断。Table 8 风格结果包含 15 组最低 RMSFE 基准对其余模型的 210 个比较；在 5% 水平下，RV、MedRV、Parkinson 分别有 10、16、16 个比较显著支持基准更优，合计 42 个，其余 168 个未拒绝等预测能力。最强负向统计量来自 `Parkinson / RB` 的 `FIGARCH_60min` 对 `GARCH_Daily`，DM–West 为 `-3.938`。顺序 Tables 10–12 风格结果按 RMSFE 递增更换基准，完整生成 15 组各 105 个、合计 1,575 个比较；Table 8 与每组顺序表第一行完全一致。无前视敏感性流被明确排除，FU 两段和 RB 保持独立。Notebook 使用 `Python (latitude)` clean kernel 连续执行 15 个代码单元格，execution count 为 1—15、零错误，17 项本项门禁全部通过；这些仍是逐对检验，第 12 项 SPA 将处理模型集合层面的多重比较问题。

第 12 项再次完整验收固定 Item 09 run，只用 19,500 条 primary 预测并从正式 silver 重算 1,300 个目标日的 RV、MedRV、Parkinson，形成 58,500 条平方损失和 225 个 primary RMSFE。SPA 直接按 Hansen (2005) studentized 公式实现：`arch.bootstrap.StationaryBootstrap` 只生成固定随机索引，长期方差、标准化最大统计量、lower/consistent/upper 三种重心与 p 值均在 Notebook 中显式计算；固定 10,000 次、种子 `20260830`、`block_size=floor(sqrt(R))`，因此 200/300 日评价窗分别使用 14/17 的平均块长，同一评价长度复用共同随机数，不使用嵌套 bootstrap。论文 Table 9 风格的 15 个最低 RMSFE 基准全部具有非正的平均损失差，按 Hansen 零下限和零统计量约定三种 p 值均为 1，consistent SPA 在 5% 水平拒绝 0 个；这只能说明没有竞争模型在同一评价样本的平均损失上超过已选第一名，不能单独证明第一名显著优于其他模型。为完成模型综合，Notebook 另让 225 个“序列 × 代理 × 模型”逐一作为候选基准，其中 199 个在 5% consistent SPA 下未被拒绝；该集合明确标为候选筛选而不是正式 MCS。最小候选集合是 `Parkinson / FU_legacy` 的 8 个模型，最大集合保留全部 15 个。按模型类别，ARFIMA、FIGARCH、GARCH、IGARCH 的未拒绝率分别为 95.56%、88.33%、88.33%、83.33%；按频率，15/30/60 分钟与日频分别为 73.33%、91.67%、96.67%、93.33%。日频模型在 45 次机会中取得 2 次最低 RMSFE、42 次未拒绝，盘中模型在 180 次机会中分别为 13 和 157 次；论文品种（FU 两段独立）未拒绝率为 91.67%，RB 扩展为 75.56%。SPA 核心批次耗时 2.456 秒，固定种子首条路径、长期方差和 consistent p 值均通过独立手工复核；Notebook 使用 `Python (latitude)` clean kernel 连续执行 15 个代码单元格，execution count 为 1—15、零错误，21 项本项门禁全部通过。至此清单 12 项全部完成。

## 第 09 项版本化研究成果契约

第 09 项成果固定保存于 `data/item09/<run_id>/`。该目录位于项目内部、正式湖仓之外，是只读 `silver` 派生出的项目专属 research artifact；它不属于 `raw`、`silver`、`gold`，也不授权其他 Notebook 或项目创建普通缓存。

当前固定且已完整验收、允许第 10—12 项读取的第 09 项 `run_id`：**`eaf4757d3488c2b7`**。完整 `run_fingerprint`：**`eaf4757d3488c2b74f1e81fa558d8146cdee562836f7fd4084b32ac7ca6fdec6`**。`LATEST` 只能用于发现候选 run，不能被下游当作可复现输入。

- 完整 `run_fingerprint` 由冻结方法、实际输入和标准环境的规范化 SHA-256 共同决定；为保持 Windows 路径有界，目录 `run_id` 固定取该完整指纹的前 16 个十六进制字符，manifest 与每个 commit 仍保存并复核完整 64 字符指纹。
- `series_id × model_id` 是最小恢复 stream。checkpoint 扁平保存在 `<run_id>/checkpoints/`，文件名固定为 `<series_id>__<model_id>.forecast.parquet`、可选的 `.steps.parquet` 和最后提交的 `.json` marker；不再建立 `streams/<series_id>/<model_id>` 目录。每条 stream 先写临时文件，复读并验证主键、行数、日期边界、参数约束、有限性和摘要，再原子安装；没有有效 marker 的内容一律视为未提交。
- forecast、step、stream summary 与 validation gate 均使用 Notebook 内明确的局部 Arrow Schema；统计浮点量固定为 `float64`，缺失值落为 Arrow null，每个文件的 Schema 指纹与内容 SHA-256 一并写入 commit 或 manifest。
- checkpoint 缺失、损坏、摘要不符或不连续时硬失败，不得自动重试。只有方法、输入和环境指纹完全一致，且用户再次明确授权长批次时，才允许从已经验证的连续 checkpoint 显式恢复。
- 全部 120 个 streams 完成后，必须先通过 OOS 数量、31,200 个 fits、模型网格、无未来数据、FU 分段、预测有限非负、主键唯一和 checkpoint 完整性门禁，再把四张最终 Parquet、manifest 与最后写入的 `_SUCCESS` 直接放在 `<run_id>/`；不再建立 `consolidated` 子目录。
- `_SUCCESS` 存在且与 manifest、文件摘要和 run 指纹一致，才表示成果完整验收。成功 run 不得原地覆盖、补写或修复；任何变更产生新的 `run_id`。
- 第 09 项默认运行模式为 `read_validated`；该模式只读取并完整验证本 README 固定的 run。`compute_or_resume` 是需要当次明确授权的预计超过十分钟批次，不因存在失败现场或 checkpoint 自动启动。
- 第 10—12 项只可读取本 README 固定的已验收 run；缺失、损坏、指纹不符或当前固定值为“无”时必须停止，不得静默重算第 09 项，也不得自动改读 `LATEST`。

现有完整 run 已从旧深层结构无重编码迁移到 `flat-v2`：120 个 forecast checkpoint、105 个 step checkpoint 和四张最终 Parquet 的内容 SHA-256 均保持不变；最深文件路径由 244 个字符降至 215 个字符。`FU_legacy` 与 `FU_relaunched` 不再作为目录存在，其身份直接编码在 checkpoint 文件名中。

## 第 09 项项目内运行控制

- 第 09 项的统计与写入实现只存在于 `09_rolling_variance_forecasts.ipynb`；不得把模型逻辑复制到独立 Python 计算脚本。
- `operations/item09/worker.py` 只负责用 clean `latitude_env_v2` kernel 执行 Notebook、发布原子状态和保存失败时的 partial Notebook；`monitor.ps1` 只负责可见监控。两者不包含论文模型实现。
- 每次长批次的状态、日志、独立 Jupyter runtime 和 partial Notebook 放在 `operations/item09/control_history/<control_run_id>/`。该目录由局部 `.gitignore` 排除，不属于版本化研究成果，也不作为后续统计输入。
- 最初用于拼装 Notebook 的重复 builder 已删除；当前及后续实现以 Notebook 文件本身为唯一统计代码来源。
- `references/item09/` 保存本项目核对论文方法与预测结果页时生成的页面图像，不属于数据成果或执行依赖。

## 冻结的统计实现口径

### 日内收益与槽位

- 在每个 Session 的开盘价上建立首个收益；不引入隔夜、晨休或午休跳空。
- Session 内一分钟收益按全日真实交易分钟序号连接，再聚合为 5、15、30、60 分钟收益；30、60 分钟槽位允许组合休市前后两个 Session 中已经分别锚定的收益片段，但不包含休市价格跳变。
- 每个完整交易日目标为 45 个五分钟、15 个十五分钟、8 个三十分钟和 4 个六十分钟收益。
- 30 分钟与 60 分钟的最后一槽分别包含 15 和 45 个实际交易分钟；槽位季节因子吸收其时长差异，不虚构分钟数据。
- 日收益定义为同一交易日 225 个 Session 内分钟收益之和，不使用昨日收盘价；因此不包含夜盘、隔夜、晨休、午休或换月跨日跳空。
- Table 3 的零收益计数使用 `abs(return) <= 1e-12`，只吸收对数望远镜求和的浮点误差，不修改聚合收益值；峰度报告 Fisher 超额峰度。

### 流动性与波动代理

- Roll 公式中的价格变化实现为五分钟对数价格变化，以匹配论文 Table 2 的 `×10^3` 量级和跨品种可比性；相邻变化只在同一真实 Session 内配对，完整日为 42 对，协方差非负时置 0。
- Table 1 Panel D 的成交量采用可审计定义：先汇总每个固定月份合约在距交割 0—5 月研究窗口内的日盘 `volume`，再按自然交割月份对合约总量取均值；同步报告合约数、观察日和成交量覆盖率，样本边界合约不补齐窗口。
- Amihud 只在区间 `money > 0` 时计算；没有有效分母的合约日记为缺失，并报告覆盖率。
- Parkinson 方差采用经典公式 `log(high / low)^2 / (4 * log(2))`，同时注明论文公式存在排版歧义。
- 所有代理方差必须通过有限性、非负性和覆盖率检查。

### 模型与预测

- 所有 GARCH 家族模型联合估计 ARMA(1,1) 均值与条件方差，并使用标准化 Student-t；不自动切换到正态分布。
- FIGARCH 分数递推截断固定为 1000，并明确标注这是论文未披露参数的项目口径。
- Table 4 风格的盘中模型只使用冻结样本外区间之前的初始样本；周期因子仍按论文基线使用整个固定研究样本，因此含前视。
- Table 5 风格日频模型使用同一冻结初始样本的 Session 内日收益，不做日内季节调整；它不包含隔夜与 Session 间跳空，因此与论文 close-to-close 日收益的持有期不同，属于公开的方法口径偏离。
- 日频估计内部把原始 log return 乘以 100 改善数值条件；展示与后续预测时将 `mu` 除以 100、`omega` 和条件方差除以 10,000，`gamma/theta/alpha/beta/phi/d/nu` 不变。跨频率只比较无量纲结构参数，不比较 `omega`、AIC 或 BIC。
- 每个“研究序列 × 频率”按日期与槽位展平并连续递推条件状态；日界、换月和 FU 旧产品段的 2010 年 11 月已知缺口不产生跳空收益但也不重置状态。五个研究序列之间重置，两个 FU 分段绝不连接。
- ARMA 首个预样本创新固定为 0，面板首个收益只提供 AR 滞后，似然观察数为输入收益数减 1。静态盘中估计使用 SciPy SLSQP、三个固定 MA 起点、`arch 8.0.0` 的回溯方差和 variance bounds；不根据结果切换分布或约束。
- GARCH 内部以 ARCH 份额和总持久性参数化，按 `alpha=q*rho`、`beta=(1-q)*rho`、`rho<=0.999999` 保证严格平稳数值边界；展示参数和协方差用 delta 法还原到 `alpha/beta`。IGARCH 直接以 `beta=1-alpha` 缩减参数化，持久性严格为 1。
- Table 4 风格 t 值采用 Bollerslev–Wooldridge 稳健协方差；Hessian 非正定、病态或稳健方差无效时明确报告缺失，不使用伪逆。边界参数即使可计算普通 Wald t 值也必须标记并谨慎解释。
- ARFIMA 的日 RV 直接汇总原始 15/30/60 分钟 Session 内收益平方，不先做日内去季节化；目标按论文文字定义为 `0.5 * log(RV)`，预测后用 `exp(2 * y_hat)` 还原方差，不做论文未披露的对数正态偏差修正。
- 若某日某频率的全部区间收益均满足 `abs(return) <= 1e-12`，则该日视为有效零 RV。基线检测下限取各序列初始样本实质正 RV 最小值的一半；另以初始样本正 RV 的 0.5% 分位数一半做敏感性。两种下限都只由冻结初始样本决定，只替换有效零日，不读取样本外数据。
- ARFIMA 使用 type-II 有限历史分数差分、Gaussian CSS 和多起点 L-BFGS-B；`mu` 与创新方差在优化时解析 profile，首日只提供滞后、似然观察数为输入日数减 1。`abs(AR) <= 0.999`、`abs(MA) <= 0.999`、`abs(d) <= 0.499`；标准误由未 profile 的联合参数观察 Hessian 计算，Hessian 无效时不得用伪逆制造精度。
- 项目采用常规符号 `(1 - phi*L)(1 - L)^d(y - mu) = (1 + theta*L)epsilon`，以对应论文 Table 6 和正文对负 AR 的解释；同时显式披露论文公式印成 `1 + phi*L` 的符号歧义。AL、CU、FU 两段和 RB 的基线均为 `(1,d,0)`；RB 另报告 `(1,d,1)` 的 BIC 敏感性。
- 样本外保留最后 300 日用于 AL、CU、RB，最后 200 日用于每个 FU 分段。
- 滚动窗长度等于初始样本长度，每日重新估计。
- 论文基线按“研究序列 × 采样频率 × 日内槽位”使用全文样本公式 `S_n = sqrt(mean_t(r[t,n]^2))` 估计未归一化周期因子，并明确标注其前视性质；去季节化收益为 `r_tilde[t,n] = r[t,n] / S_n`，方差预测重新季节化时乘 `S_n^2`。
- 无前视敏感性对每个槽位使用严格早于当前观察的全部历史平方收益扩展估计；首个观察无因子，历史因子为零时不做除法，至少 60 个先前观察只定义稳定差异报告子样本。后续实时滚动预测仍必须在各预测原点内重新估计，不能读取本项全文样本因子作为无前视输入。

### 预测比较检验

- DM–West 使用 AR(1) 预白化、quadratic-spectral 核、Andrews 自动带宽和双侧检验。
- SPA 使用 `block_size=floor(sqrt(R))`、10,000 次 stationary bootstrap、studentized 统计量和固定种子 `20260830`。
- SPA 以 consistent p 值为主，同时展示 upper 与 lower p 值。

## Notebook 通用结构

每本 Notebook 必须保持自包含，并按以下顺序组织：

1. 在做什么与经济意义。
2. 论文公式、表图对应和当前复现等级。
3. 项目根目录、标准环境和权威 Schema 导入。
4. 开篇 Schema metadata 浏览器。
5. 参数与固定样本。
6. 直接使用 PyArrow 过滤读取并转换。
7. 从上到下可读的线性统计或模型代码。
8. 独立验证、表图、结果解释和限制。

允许为自包含而重复直接代码；不建立共享 helper、普通项目缓存或隐藏的跨 Notebook 状态。除第 09 项上述显式成果契约外，后续 Notebook 需要上游结果时仍必须在自身重新计算。第 10—12 项从 README 固定并完整验收的第 09 项 run 开始保持自包含，不得读取其他未提交中间产物。

## 环境、读取与验收

- 标准解释器是 `E:\anaconda3\envs\latitude_env_v2\python.exe`，Notebook kernel 使用 `Python (latitude_env_v2)`。
- 2026-10-01 环境迁移只配置 Python 3.13 与依赖，不重算或改写第 09 项已验收成果。该项指纹包含完整代码、解释器路径、Python 与数值库版本；迁移后当前代码和环境不再匹配历史固定 run，默认 `read_validated` 会按原门禁拒绝重新验收。第 10—12 项仍可按自身契约读取历史已提交成果；在新环境重算并接纳新 run 须另行授权完整批次。
- 项目根目录按仓库 `.env.template` 规定的标记文件搜索方式定位，再从 `config.settings.settings.futures_lake_root` 获取正式湖仓根目录；不得硬编码另一份正式湖路径。
- 表名、主键和分区顺序以 `config/data_contracts.py` 的表级 Schema metadata 为准；字段、Arrow 类型和 nullable 以其中的权威 `pa.Schema` 为准。每本 Notebook 在首次读取前展示直接参与表的 Schema 契约。
- 数据读取使用带分区或字段过滤的直接 PyArrow 调用；只读取当前统计项目需要的列和样本范围。
- 每本 Notebook 必须从 clean kernel 连续执行，execution count 连续且没有错误输出。
- 数据门禁至少覆盖：唯一三个月合约、固定样本端点、FU 分段、无夜盘、Session 内收益、每日 `45/15/8/4` 槽位以及末槽实际时长。
- 统计门禁至少覆盖：代理方差非负、Amihud 分母覆盖、换月标记、模型参数约束、预测方差有限且非负。
- 预测验收必须核对 OOS 日数、未来数据边界、RMSFE 与 DM 符号，以及可用小样本手工复核的 SPA 随机种子。
- 预计超过十分钟的完整滚动模型批次，只能在对应后续对话中获得用户对该有界批次的明确授权后执行，并遵守仓库后台 worker 与可见监控规范。
- 第 09 项 clean-kernel 验收默认执行 `read_validated`，并同时复核固定 `run_id`、manifest、`_SUCCESS`、文件摘要、完整模型网格和全部统计门禁；`compute_or_resume` 的 checkpoint 不因单条 stream 完成而成为可供下游读取的正式成果。

## 复现等级与已知限制

- 复现等级：论文方法框架复现；不是 GTA 原样本与发表表格数值复刻。
- 数据差异：使用正式 JQData 湖仓、2010 年以后样本及当前稳定 Schema。
- 品种差异：RB 是扩展检验；SR 因分钟数据不可用而不进入研究宇宙。
- 制度差异：FU 分成旧产品段与稳定重启段独立报告。
- 未披露选择：复权、FIGARCH 截断、Parkinson 排版歧义及对数预测偏差修正均按本 README 的冻结口径执行并显式披露。
