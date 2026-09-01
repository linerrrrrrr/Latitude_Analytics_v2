# 中国期货市场演变方法复现

本项目使用 `FUTURES_LAKE_ROOT` 下的正式 lakehouse 数据，逐项复现论文《从高频视角看中国期货市场的演变历程》的统计方法。项目只读 silver 数据，计算结果只保存在 Notebook 输出中，不写入 silver、gold 或其他数据文件。

项目规则与规范路由见 [AGENTS.md](AGENTS.md)。

## 工作方式

- 每次对话只实现一个统计项目；轮到该项目时才创建对应 Notebook。
- 每个 Notebook 依次说明：在做什么、经济意义、论文口径、数据覆盖、代码步骤、图表和验证。
- 默认复现论文样本；参数可以扩展到湖仓已有历史。
- 论文方法与当前数据不完全一致时，必须明确标注近似口径，不把代理指标表述成原论文指标。

## 项目清单

| 编号 | Notebook | 统计项目 | 论文对应 | 数据与复现等级 | 状态 |
|---|---|---|---|---|---|
| 01 | `01_daily_volume_evolution.ipynb` | 逐品种总成交量及三个月均线 | Figure 1 | RB、CU、NI、M、L、C；严格复现 | 已实现并执行验证 |
| 02 | `02_open_interest_to_volume.ipynb` | 持仓变化/成交量周比率及 12 周均线 | Figure 10 | 六品种；严格复现 | 已实现并执行验证 |
| 03 | `03_volume_by_months_to_maturity.ipynb` | 距到期月成交量分布、累计分布及 25%–75% 区间 | Figures 11–12、Table 2 | 六品种；以权威 `delist_date` 表示到期日 | 已实现并执行验证 |
| 04 | `04_maturity_liquidity_diversification.ipynb` | 交割月份成交量分布及归一化 KL 集中度 | Figures 2–3 | 论文图默认 NI/RB，KL 覆盖六品种 | 已实现并执行验证 |
| 05 | `05_main_returns_and_volatility.ipynb` | 主力收益分布、标准差、5% VaR、偏度、超额峰度和月度年化波动率 | Figure 13、Table 7、Figure 14 | 六品种；采用项目现有主力连续规则 | 已实现并执行验证 |
| 06 | `06_intraday_trading_intensity.ipynb` | 日盘五分钟成交强度及日间分位带 | Figure 15 | 仅 RB、CU、NI、L；M、C 缺少分钟事实 | 已实现并执行验证 |
| 07 | `07_minute_execution_deviation_proxy.ipynb` | 分钟 VWAP 相对分钟高低中点的执行偏离及 2020 前后检验 | Slippage 的代理实验 | 分钟级代理，不与论文滑点数值直接比较 | 已实现并执行验证 |

## 等待 Level 1 数据

以下论文项目依赖 0.5 秒 Level 1 的最优买卖价和对应盘口量，现有 `fact_futures_minute` 只有一分钟 OHLC、成交量、成交额和持仓量，不能原样复现：

- 买卖价差及一档、两档、三档以上占比；
- 盘口不平衡及每周分位数分布；
- 买卖价差恢复时间；
- 盘口不平衡恢复时间；
- 论文定义的滑点；
- 基于上述指标的 Mann–Whitney U 检验。

这些项目暂不创建 Notebook。未来补齐 Level 1 后，应新增原论文口径实现，而不是复用分钟代理结果冒充原指标。

## 固定口径

- 论文日频默认样本：2012-01-01 至 2021-12-31。
- 论文日内默认样本：2016-01-01 至 2021-12-31。
- 目标品种：XSGE 的 RB、CU、NI，以及 XDCE 的 M、L、C。
- “三个月均线”按 63 个品种交易日实现，只有完整 63 个观测后才输出。
- 持仓/成交量指标从 `open_interest` 逐合约复算相邻变化，先做日内跨合约平均，再做周内跨交易日平均和完整 12 周均线；非正成交量及 `|ΔOI| > volume` 的指标不适用记录单独报告并排除，不修改来源值。
- 距到期月使用 `trading_date` 与 `delist_date` 的年月差，不解析合约代码；按论文五个两年段汇总，累计曲线按“距退市至少 k 个月”反向累加，25%–75% 端点取从近到远累计成交量首次达到相应份额的离散月份。
- 交割月份集中度按 `trading_date.year` 汇总，并用 `delist_date.month` 把具体合约合并为月份桶；对年度成交量为正的 (N) 个交割月份计算 `Σ w·ln(wN) / ln(N)`，(N≤1) 时不报告 KL。
- 主力收益使用前一交易日信号、1.10 换月阈值、禁止回滚和共同锚点 log-gap 复权；Table 7 使用复权 log 收益、样本标准差、带负号的经验 5% 分位、无偏偏度和 Fisher 超额峰度，月度波动率按月内日收益样本标准差乘 `sqrt(252)` 年化。
- 日内成交强度只保留合约日历中 `is_night_session=false` 的日盘 Session，先按品种汇总全部固定月份合约的一分钟成交量，再在每个 Session 内构造五分钟桶并除以当日日盘总成交量；按 2016–17、2018–19、2020–21 计算单日散点、跨日均值和 25%–75% 分位带，只报告有分钟事实的 RB、CU、NI、L。
- 分钟执行偏离代理使用 `money / (volume × contract_multiplier)` 还原分钟 VWAP，以 `(high + low) / 2` 为参考价并取绝对相对偏离的基点值；覆盖 RB、CU、NI、L 的全部权威 Session。只有成交量、成交额、OHLC、乘数与最小变动价位适用，且 VWAP 位于分钟高低区间上下各一个 tick 容差内的合约分钟进入周统计；不相容行单独报告并排除。按周输出均值和 25%/50%/75% 分位，以品种周均值为样本检验 2020–21 是否低于 2016–19；该结果不得与论文 Level 1 滑点数值直接比较。

## 运行环境

使用项目标准解释器：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Futures_Lakehouse/verify_runtime.py
```

Notebook 通过项目规定的标记文件搜索方式定位仓库根目录，并通过 `config.settings.settings.futures_lake_root` 定位正式湖仓。
