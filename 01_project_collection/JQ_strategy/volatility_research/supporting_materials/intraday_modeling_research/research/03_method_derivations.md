# 符号约定与方法推导

更新：2026-09-08。通用符号与方法以[独立波动率项目 README](../../../README.md)为总入口；本文保留本研究与 [v2 季节性 Notebook](../../../../draft_experiments_v2/02_01_im_bpv_intraday_seasonality.ipynb)、[v2 窗口诊断](../../../../draft_experiments_v2/02_02_im_bpv_window_comparison.ipynb) 的 IM 实现适配、代码映射和实验所用定义。规范路由见[根级 AGENTS.md](../../../../../../AGENTS.md)，研究入口见 [README](../README.md)。本约定不改变数据字段、Python 变量、计算方法或已有数值结果；IM 参数不扩展为其他研究项目的默认设置。

## 1. 学术依据与使用规则

数学符号并非全学界统一。项目优先沿用相关原始研究的常见写法，并固定其含义；首次出现必须同时给出中文解释和定义，不能要求读者根据字母猜测对象。原文之间的实际差异及核对范围见[跨文献符号核对](../literature/notes/07_notation_survey.md)。

- 收益采用 $r$，波动率采用 $\sigma$，方差采用 $\sigma^2$，实现方差采用 $RV$，实现双幂变差采用 $BV$（文字和既有代码中的 BPV 是同一名称的另一缩写）。这些是有文献依据的常用选择，不表示每篇论文都用相同字母。
- 周期标准差因子沿用 Boudt–Croux–Laurent 的 $f$ 体系，方差因子直接写 $f^2$，不再另设 $a$。Hecq–Laurent–Palm 和 Dumitru–Hizmeri–Izzeldin 也采用平方均值为 1 的周期标准差标尺。全日归一化为 $M^{-1}\sum_i f_{t,i}^2=1$。
- 估计量加帽，如 $\widehat f$；按上述标准差因子滤波的收益写 $r^f$。$\varepsilon$ 表示随机扰动，不用它表示日内归一化贡献。
- 项目自定义的混合贡献、相对贡献和中位数因子分别写 $c^{\mathrm{mix}}$、$x^{\mathrm{mix}}$、$\widehat g^{\mathrm{mix}}$。这些是本项目明确规定的简称，**没有在本次核对文献中找到与当前混合构造相同的通用符号**；不得标成论文原式或学界统一定义。
- 文献笔记中“原文公式”保留作者原符号并注明来源；转入“本项目解释”时使用本表。不能把另一篇论文的 $f$ 自动解释成本项目的标准差因子。
- 数学符号与代码名称通过映射表对应。代码继续使用有业务含义的描述性名称，不为模仿公式把变量批量缩成单字母。引用新公式时补充定义、量纲和可用时点；改动本约定时同步受影响的说明及 Notebook。

## 2. 基本符号

| 符号 | 中文含义与定义 | 来源或边界 |
|---|---|---|
| $t$ | 交易日序号；$t-1$ 是上一交易日 | 本项目固定下标顺序，不指前一自然日 |
| $i$ | 当天第 $i$ 个一分钟位置 | $i=1,\ldots,M$；上午首分钟为 1，下午首分钟为 121；代码 minute_index 为零基，故 $i=\mathrm{minute\_index}+1$ |
| $M$ | 全天分钟位置数 | 当前 IM 为 240；单个 Session 为 120 |
| $L$ | 回望交易日数 | 当前为 30、60、120、360 |
| $P_{t,i}$ | 第 $i$ 根分钟线的收盘价 | $P^{\mathrm{open}}_{t,i}$ 表示本根开盘价；小写 $p=\log P$ 表示对数价格 |
| $r_{t,i}$ | 一分钟对数收益率 | 通常为 $\log P_{t,i}-\log P_{t,i-1}$；Session 首分钟为 $\log P_{t,i}-\log P^{\mathrm{open}}_{t,i}$ |
| $\sigma_t$ | 简化模型中的日波动尺度 | 在下述模型下，$\sigma_t^2$ 是全天条件方差之和；不是单分钟标准差 |
| $f_{t,i}$ | 严格为正、无量纲的周期标准差因子 | $f_{t,i}^2$ 才是周期方差因子 |
| $\widehat f_{t,i}^{(L)}$ | 用之前 $L$ 个交易日估计的标准差周期因子 | 对目标日 $t$，最多用到 $t-1$ |
| $RV_t$ | 全天实现方差，$\sum_{i=1}^{M}r_{t,i}^2$ | 方差单位；实现波动率为 $\sqrt{RV_t}$ |
| $\mu_1$ | 标准正态绝对一阶矩，$E\lvert Z\rvert=\sqrt{2/\pi}$ | 因而 $\mu_1^{-2}=\pi/2$；其他论文可能使用不同常数符号 |
| $b_{t,i}$ | BV 的一个求和项，$\mu_1^{-2}\lvert r_{t,i}\rvert\lvert r_{t,i-1}\rvert$ | $b$ 是本项目对求和项的简称；只用于同 Session 的有效相邻收益对 |

当前数据不将隔夜、午休收益混入分钟收益。$i=1,121$ 没有组内前一个收益，故没有 $b_{t,i}$。$\mathcal I_t$ 表示有效配对的右端点集合；完整日有 238 个。

为理解尺度，暂用无跳跃、日内基础尺度不变的简化模型：

$$
r_{t,i}=\frac{\sigma_t}{\sqrt M}f_{t,i}\varepsilon_{t,i},
\qquad \frac1M\sum_{i=1}^{M}f_{t,i}^2=1.
$$

$\varepsilon_{t,i}$ 的条件均值为 0、条件方差为 1；下述 BPV 期望还要求相邻扰动条件独立且标准正态。该模型用于推导，不宣称 IM 真实收益满足这些条件，也不排除进一步建模日内随机波动。

研究代码的 daily_bpv_scale 实际等于 $\sqrt{238^{-1}\sum_{i\in\mathcal I_t}b_{t,i}}$，是**分钟尺度**。将它记为 $\widehat s_t$，则代码的标准化收益为 $\bar r_{t,i}=r_{t,i}/\widehat s_t$；与日标尺的关系为 $\widehat\sigma_t=\sqrt M\,\widehat s_t$。这里 $s$ 只用于映射 Boudt 的非周期尺度，不能再指代旧 Notebook 的贡献季节因子。此关系是当前两 Session 的尺度定义，不能省略边界后称为任意日级 BV 定义。

AB1997 原式采用标准差因子均值为 1。若其因子为 $s_i^{\mathrm{AB}}$，令 $k=\sqrt{M^{-1}\sum_i(s_i^{\mathrm{AB}})^2}$，则需要同时变换 $f_i=s_i^{\mathrm{AB}}/k$ 和 $\sigma^{\mathrm{project}}=k\sigma^{\mathrm{AB}}$，才能保持收益模型不变。改变归一化不是只换一个字母。

## 3. 项目混合贡献与代码的对应

| 本项目符号 | 定义和直觉 | 对应代码 |
|---|---|---|
| $c_{t,i}^{\mathrm{mix}}$ | 首分钟为 $r_{t,i}^2$，其他分钟为 $b_{t,i}$；一个非负混合波动代理 | v2 variance_contribution；研究项目 mixed_contribution |
| $x_{t,i}^{\mathrm{mix}}$ | $c_{t,i}^{\mathrm{mix}}/(M^{-1}\sum_jc_{t,j}^{\mathrm{mix}})$；分钟贡献相对于当天平均贡献 | daily_shape_matrix / mixed_shape_values |
| $\widehat g_{t,i}^{\mathrm{mix},(L)}$ | 历史相对贡献的逐分钟中位数，再归一为日内均值 1 | v2 seasonality_matrix / 输出 variance_seasonality |
| $c_{t,i}^{\mathrm{mix,adj},(L)}$ | $c_{t,i}^{\mathrm{mix}}/\widehat g_{t,i}^{\mathrm{mix},(L)}$ | variance_adjusted |
| $r_{t,i}^{\mathrm{adj,mix},(L)}$ | $r_{t,i}/\sqrt{\widehat g_{t,i}^{\mathrm{mix},(L)}}$；旧方法的收益变换 | return_adjusted；不能仅凭字段名视为 $r^f$ |
| $z_{t,i}^{\mathrm{mix},(L)}$ | 调整后贡献再除以目标日的全日平均调整后贡献 | v2 的 1 + residual、诊断 z；仅事后可得 |

对完整历史日，$x^{\mathrm{mix}}$ 的日内均值为 1。数值 2 表示该分钟贡献为当天平均分钟贡献的两倍。v2 Notebook 对不完整或日总量非正的历史日保留缺失；不跳过这些日补更早的样本。

中位数因子直接定义为：

$$
\widehat g_{t,i}^{\mathrm{mix},(L)}=
\frac{\operatorname{median}_{h=t-L,\ldots,t-1}x_{h,i}^{\mathrm{mix}}}
{M^{-1}\sum_{j=1}^{M}\operatorname{median}_{h=t-L,\ldots,t-1}x_{h,j}^{\mathrm{mix}}}.
$$

$h$、$j$ 只是历史交易日和分钟的求和下标。rolling_median_matrix 对应分子那条曲线，不必为这一中间步骤再记一个字母。

$\widehat g$ 估计混合贡献的典型相对形状，不因均值为 1 就自动等于 $\widehat f^2$。既有字段 variance_seasonality、volatility_seasonality 及 metadata seasonality_scale=variance 保留兼容；对 median_mixed 而言，它们是历史命名，不能证明已识别收益方差周期。研究输出中该方法的 variance_factor 和 contribution_factor 也都存放 $\widehat g^{\mathrm{mix}}$。

有效的 $\widehat f$ 也必须严格为正；退化估计保留缺失。对从收益尺度估计的其他方法，variance_factor 对应 $\widehat f^2$；contribution_factor 在首分钟为 $\widehat f^2$，其他分钟为 $\widehat f_{t,i}\widehat f_{t,i-1}$。代码映射须连同 method 一起阅读。以下在不比较窗口时省略上标 $(L)$。

## 4. 收益、方差和配对贡献的尺度

在第 2 节的限定模型下：

$$
E[r_{t,i}^2\mid\sigma_t,f]=\frac{\sigma_t^2}{M}f_{t,i}^2,
\qquad
E[b_{t,i}\mid\sigma_t,f]=\frac{\sigma_t^2}{M}f_{t,i}f_{t,i-1}.
$$

从收益出发过滤，沿用文献上标 $f$：

$$
r^f_{t,i}=\frac{r_{t,i}}{\widehat f_{t,i}},
\qquad
(r^f_{t,i})^2=\frac{r_{t,i}^2}{\widehat f_{t,i}^2},
\qquad
b^f_{t,i}=\frac{b_{t,i}}{\widehat f_{t,i}\widehat f_{t,i-1}}.
$$

后两式是代数恒等式，不要求正态假设。相邻因子相同时，配对除数才等于当前因子的平方；缓慢变化时可以近似，但开盘等陡变位置应单独量化。配对因子的均值通常不精确等于 1；再次归一后就不是完全相同的变换。

旧贡献路线直接计算 $c^{\mathrm{mix}}/\widehat g^{\mathrm{mix}}$。若用旧方法的 $r/\sqrt{\widehat g^{\mathrm{mix}}}$ 重算普通分钟 BPV，除数为
$\sqrt{\widehat g_{t,i}^{\mathrm{mix}}\widehat g_{t,i-1}^{\mathrm{mix}}}$，一般不同于直接除数 $\widehat g_{t,i}^{\mathrm{mix}}$。这两种结果不能互相替代。

## 5. 同尺度不保证同分布

令 $Z,Z'$ 独立标准正态。在没有周期和跳跃的例子中，收益平方项为 $Z^2$，BPV 项为 $\mu_1^{-2}|Z||Z'|$。二者期望均为 1，但方差分别为

$$
\operatorname{Var}(Z^2)=2,\qquad
\operatorname{Var}(\mu_1^{-2}|Z||Z'|)=\frac{\pi^2}{4}-1\approx1.4674.
$$

这是“同尺度、不同分布”的反例。每日随机分母归一化引入额外依赖，相邻 BPV 也共享一个收益。因此首分钟 RV 与其余分钟 BPV 的 Q95/IQR 应分层比较；本例不直接识别 IM 实际尾部差异的来源。

## 6. 日级 BV 修正与局部滤波

本项目区分未经边界修正的 $BV_t=\sum_{i\in\mathcal I_t}b_{t,i}$ 与明确标出的修正版。对一个含 $n$ 个完整收益的 Session，记其分钟集合为 $\mathcal S$、配对右端点集合为 $\mathcal I(\mathcal S)$：

$$
BV^{\mathrm{bc}}_{t,\mathcal S}
=\frac{n}{n-1}\sum_{i\in\mathcal I(\mathcal S)}b_{t,i},
\qquad
C_{t,\mathcal S}^{\mathrm{pair}}
=\frac{n}{n-1}
\frac{\sum_{i\in\mathcal I(\mathcal S)}f_{t,i}f_{t,i-1}}
{\sum_{i\in\mathcal S}f_{t,i}^2}.
$$

这里 bc 表示边界修正，$C^{\mathrm{pair}}$ 是本项目为此推导定义的期望比率。在第 2 节模型下，$BV^{\mathrm{bc}}/C^{\mathrm{pair}}$ 的条件期望等于该 Session 的条件方差之和；$b_{t,i}/(f_{t,i}f_{t,i-1})$ 调整的则是局部贡献。

若单独研究该 Session 并另令其因子平方均值为 1，比率才简化为相邻因子乘积的均值。这里区分全日 $M=240$ 与 Session 内 $n=120$。两 Session、首分钟混合 RV、缺失配对、日内随机波动或噪声都会改变目标和边界；本推导不冒充 Dette 等论文的完整方法，也不赋予混合贡献日级 BV 的理论保证。

## 7. 平滑、诊断与预测

稳健尺度估计处理历史异常污染；平滑限制日内曲线复杂度；滚动窗口和状态条件化处理跨日变化。它们是不同维度。研究代码的分 Session log-WSD Fourier 投影是项目改编，不是 Andersen–Bollerslev 全部 FFF 回归的复现。开盘与公告结构不能仅为图形平滑而先行抹去。

事后诊断为：

$$
z_{t,i}^{\mathrm{mix}}=
\frac{c_{t,i}^{\mathrm{mix,adj}}}{M^{-1}\sum_jc_{t,j}^{\mathrm{mix,adj}}}.
$$

它使用目标日全日实现值，日内均值恒为 1；跨日逐分钟中位数、IQR、Q95 没有等于 1 的约束。$z$ 不是标准正态变量，也不是盘中一步预测误差。

盘中研究可以另定义标准化配对贡献：

$$
z_{t,i}^{\mathrm{BV,pre}}=
\frac{b_{t,i}}{(\widehat\sigma_{t\mid t-1}^{\,2}/M)
\widehat f_{t,i}\widehat f_{t,i-1}}.
$$

上标 pre 表示分母事先可得，这是待研究定义，不代表当前所有模型已经使用它。分母中的日尺度和周期估计必须在观测 $r_{t,i}$ 前可得；RV 项改用 $\widehat f_{t,i}^2$。使用当日截至时点的状态也必须服从可用时点边界。

## 8. 时钟与预测标签

- **事前周期时钟**：$\tau_{t,k}=\sum_{i\le k}\widehat f_{t,i}^2$。$k$ 是当前分钟位置；$\tau$ 是累计相对方差暴露，可在开盘前形成。对混合贡献基线则明确写累计 $\widehat g^{\mathrm{mix}}$，不将它解释为已识别的收益方差暴露。
- **实现活动时钟**：例如 $A_{t,k}=\sum_{i\in\mathcal I_t,\ i\le k}b^f_{t,i}$，累计已观察贡献。阈值事前确定，不能用当天尚未知的最终总量固定事件数。$A$、$\tau$ 是此处已定义的项目时钟记号。
- **固定 15 分钟目标**：$RV_{t,k\to k+15}=\sum_{i=k+1}^{k+15}r_{t,i}^2$；$BV_{t,k\to k+15}=\sum_{i=k+1}^{k+15}b_{t,i}$。本项目 BPV15 的第一项包含已知的 $r_{t,k}$，不是仅用未来区间内部的 14 对收益。所有起点和未来收益须属于同一 Session。

只有分钟 OHLC 时，分钟内真实触达时刻不可见；sample_at 与 available_at 不是同一概念。预测返回原始 RV/BV 尺度时，未来区间的季节暴露只能使用事前估计。

实现入口仍为 run_diagnostics.py、run_measure_comparison.py、run_forecasting.py 和 run_clock_forecasting.py。结果见[总报告](../FINAL_REPORT.md)；本次符号同步没有重新估计实验结果。
