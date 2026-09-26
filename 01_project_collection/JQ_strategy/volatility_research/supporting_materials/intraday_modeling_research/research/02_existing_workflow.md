# 现有 Notebook 与论文方法的对照

代码对照建立于2026-09-07；2026-09-08 同步[统一符号](03_method_derivations.md)及两本 Notebook 的 Markdown，代码单元未改动、未重新执行。历史结果的生成来源未因此重新验证。单元索引使用 Jupyter JSON 中从 0 开始的实际索引；2026-09-07 的原始阅读指纹见[历史快照](../sources/notebook_snapshot.json)；本次文字同步记录见[研究日志](../research_log.md)。

## v2 估计流程

来源：[02_01_im_bpv_intraday_seasonality.ipynb](../../../../draft_experiments_v2/02_01_im_bpv_intraday_seasonality.ipynb)。

| 实际单元索引 | 当前实现 | 研究含义 |
|---|---|---|
| 1、3、5 | 读取本地 IM 连续合约分钟文件中的原始 open/close；交易日历重建每日 240 个 slot | 因子估计用原始合约价格，上午与下午各 120 分钟 |
| 10 | 按交易日、合约、Session 分组；连续分钟 close 的 log 差；09:31/13:01 使用 log(close/open) | 收益不跨午休/隔夜；普通分钟间隔不为1分钟时保留缺失 |
| 14 | 普通分钟使用 $b_{t,i}=(\pi/2)\lvert r_{t,i}\rvert\lvert r_{t,i-1}\rvert$，Session 首分钟改用 $r_{t,i}^2$ | `variance_contribution` 是混合代理；`bpv_contribution` 首分钟仍无相邻配对 |
| 17、21 | 完整且已结束、日总贡献有限为正的交易日进入历史；$x_{t,i}^{\mathrm{mix}}=M c_{t,i}^{\mathrm{mix}}/\sum_j c_{t,j}^{\mathrm{mix}}$，完整日 $M=240$ | 去掉历史日的全日幅度，强调每天的相对形状 |
| 24 | `daily_shape_matrix.shift(1)` 后，按 30/60/120/360 交易日做滚动中位数，`min_periods=lookback` | 估计日 $t$ 不使用 $t$ 的贡献；窗口保留日历缺失位置，非“跳过缺失后凑满L天” |
| 29 | 每条历史中位数曲线除以其分钟均值 | 把贡献形状因子均值固定为1；不等于识别出收益方差周期 |
| 35 | $c_{t,i}^{\mathrm{mix,adj}}=c_{t,i}^{\mathrm{mix}}/\widehat g_{t,i}^{\mathrm{mix}}$，$r_{t,i}^{\mathrm{adj,mix}}=r_{t,i}/\sqrt{\widehat g_{t,i}^{\mathrm{mix}}}$ | 第一条是贡献直接调整；第二条若解释成收益去季节，需额外论证 $\widehat g^{\mathrm{mix}}$ 可作为收益方差因子 |
| 43、44 | 输出混合贡献、类型和两种调整量，按窗口落盘 | 代码已标记混合口径；文件名中的 BPV 不能替代字段定义 |

单元 29 只计算曲线，输出字段未包含逐行 `factor_fit_end_date`。2026-09-08 已修正其前置说明原先“记录历史截止日期”的误述。`shift(1)` 给出了源代码层面的滞后证据；若以后交付实时结果，可增加能落盘复核的训练截止时间记录。这是后续设计建议，本次未修改输出契约。

## v2 诊断流程

来源：[02_02_im_bpv_window_comparison.ipynb](../../../../draft_experiments_v2/02_02_im_bpv_window_comparison.ipynb)。

| 实际单元索引 | 代码事实 | 如何解释 |
|---|---|---|
| 5 | 四个窗口取共同有效且因子为正的完整交易日 | 避免直接用不同日期范围比较窗口；仍需观察被排除日期的构成 |
| 22 | 先按日归一化，目前实际执行 `mean(axis=0)`；中位数代码被注释 | 图和形状偏离指标是均值版本；2026-09-08 已同步修正 Markdown 原先的“中位数”标题 |
| 25 | `residual = adjusted / adjusted.mean(axis=1) - 1` | 这是事后日内相对贡献，非模型的一步预测误差 |
| 29 | 形状平均绝对偏离、因子日际变化、窗口 log 因子距离 | 描述性统计，尚非论文的正式假设检验 |
| 35 | $z^{\mathrm{mix}}=1+\mathrm{residual}$，逐 slot 计算中位数、两端各2.5%截尾均值、IQR、Q95 | 使用全日实现均值；图上的1参考线不表示中位数必须等于1 |

当前 $z^{\mathrm{mix}}$ 的日内平均恒等于1，但它的跨日逐 slot 中位数、IQR或Q95没有等于1的恒等约束。因为混合贡献非负且一天完整，$0\le z^{\mathrm{mix}}\le240$；同一天各 slot 受共同分母约束。这种自归一化依赖也有别于直接标准化收益的分布检验。

## 因子估计与盘中信息

历史日全日贡献用来估计第二天的因子，可以是因果的。目标日全日贡献用来定义事后诊断 $z^{\mathrm{mix}}$，也可以是合理的研究展示。问题出现在把这个 $z^{\mathrm{mix}}$ 或由它确定的目标日状态直接当作盘中可见输入；那时全日分母包含预测时点之后的信息。

本次确认的是 v2 这一段 `shift(1)` 的代码逻辑，不等于已经证明整个上游选主力、数据修订、下游模型和执行链无未来信息。

## 与 v1 的连接

v2 目前只有主力连续合约、周期估计、窗口诊断三本 Notebook。v1 有时钟及预测研究，但 [03_01](../../../../draft_experiments_v1/03_01_im_clock_resampling.ipynb) 目前定义的是时间、全合约成交量、全合约成交额时钟，并非已实现 BPV 时钟。

v1 的该入口源代码仍引用 `JQ_strategy/draft_experiments`，并使用分钟内价格插值和双向复权；[04_04](../../../../draft_experiments_v1/04_04_im_15m_volatility_forecasting.ipynb) 自身已说明其离线探索和全快照依赖。因此它们提供的是可借鉴的比较框架，不能直接当作已验证、可实时使用的新研究入口。这里仅报告本次触及的依赖，不修复或迁移 v1。

建议保留当前方法作为明确命名的基线，再新增论文方法作同口径比较。详细的尺度差异见[公式对照](03_method_derivations.md)。
