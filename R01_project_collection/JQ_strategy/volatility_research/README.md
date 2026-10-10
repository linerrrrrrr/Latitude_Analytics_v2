# 波动率研究：符号定义与方法整理

本项目是 JQ_strategy 下的波动率研究子项目，用于集中阅读和整理波动率方法。当前范围是高频实现波动、日内周期、去季节及其诊断和预测接口；本文是项目的符号与方法主入口。文献原文存放在本项目 literature 目录。建立日期：2026-09-08。规范路由见[根级 AGENTS.md](../../../AGENTS.md)。

**初次阅读先从[阅读教程](READING_TUTORIAL.md)开始**；本文用于查定义、公式和原文映射。

## 1. 目录与阅读方式

- [阅读教程](READING_TUTORIAL.md)：从前置概念开始的 10 课，逐步引入符号，配有手算、理解题、答案与 15 篇论文的阅读路线。
- 本文：中文定义、统一符号、方法公式、假设、文献定位和已有 IM 案例。
- [文献清单](literature/source_manifest.json)：15 篇文献的正式引用、实际取得版本、阅读范围、页码、来源和 SHA-256。
- [BibTeX](literature/references.bib)：引用管理；文末列出全部本地原文。
- [支持附件](supporting_materials/README.md)：原 intraday_modeling_research 的完整材料，作为一次已有研究的支持证据。

```text
volatility_research/
  README.md / READING_TUTORIAL.md
  literature/
    15 份 PDF / source_manifest.json / references.bib
  supporting_materials/
    README.md / attachment_manifest.json
    intraday_modeling_research/
```

材料来自 JQ_strategy 的研究积累。通用符号与方法以本文为准；附件保存一次 IM 研究的文献笔记、数据快照、代码、运行记录和结果。正文可以独立阅读，引用附件时需连同它的样本、参数和评估限制一起说明。

literature 收录 15 篇不同文献；附件中的原始文献副本是同一批资料，不能重复计数。JQ_strategy 内的原研究目录保留，便于原工作流继续引用；本项目中的附件不自动同步原研究的后续变更。

原有 7 份资料的历史阅读范围与后来新增 8 篇的定向公式核对分别保留；取得全文不等于通读全文。选定符号有跨文献依据，但没有声称所有学术论文采用同一字母。

## 2. 符号使用原则

1. 优先沿用相关原始文献的常见用法；第一次使用时给出中文含义、定义和尺度。
2. 固定项目内部的记号；引用论文原式时保留原符号，并说明如何映射到本项目。
3. 估计量加帽，如 $\widehat f$；标准差与方差分别写 $\sigma$、$\sigma^2$，周期标准差与周期方差分别写 $f$、$f^2$。
4. 普通学术记号与项目自定义量明确分开；不能仅换成希腊字母就称为学术通用符号。
5. 数学符号与实现变量通过表格对应，代码仍使用语义完整的名称；不为模仿公式将变量缩成单字母。
6. 方法说明按“估计对象—公式—假设—可用时点—文献定位”整理。本文调整时同步相关说明，避免一项含义使用互相冲突的定义。

### 2.1 基本对象

| 符号 | 中文含义 | 定义与尺度 |
|---|---|---|
| $t$ | 交易日序号 | $t-1$ 是上一交易日 |
| $i$ | 日内采样区间序号 | $i=1,\ldots,M$；不限定为一分钟 |
| $M$ | 一天内纳入研究的采样区间数 | 由市场、采样频率和边界规则决定，不固定为 240 |
| $L$ | 历史估计窗口的交易日数 | 是方法参数，不是理论规定 |
| $H$ | 预测区间长度 | 以当前采样区间为单位 |
| $P_{t,i}$、$p_{t,i}$ | 价格、对数价格 | $p_{t,i}=\log P_{t,i}$ |
| $r_{t,i}$ | 区间对数收益率 | $p_{t,i}-p_{t,i-1}$；跨时段边界是否纳入须单独定义 |
| $\varepsilon_{t,i}$ | 标准化随机扰动 | 相应模型中均值为 0、方差为 1；是否独立或正态另行说明 |
| $\sigma_t$ | 下述简化模型中的日波动尺度 | $\sigma_t^2$ 对应全天各区间条件方差之和，不是已观察到的 $RV_t$ |
| $f_{t,i}>0$ | 周期标准差因子 | 无量纲；$f_{t,i}^2$ 是周期方差因子 |
| $\widehat f_{t,i}^{(L)}>0$ | 历史估计的周期标准差因子 | 对目标日 $t$，只使用此前 $L$ 个交易日；退化估计不强行补正值 |
| $r^f_{t,i}$ | 去周期后的收益 | $r_{t,i}/\widehat f_{t,i}$；上标 $f$ 是滤波标记，不是幂次 |
| $RV_t$ | 实现方差 | $\sum_i r_{t,i}^2$，单位为收益平方 |
| $\sqrt{RV_t}$ | 实现波动率 | 与收益同尺度；不能与 $RV_t$ 混称 |
| $\mu_1$ | 标准正态绝对一阶矩 | $E\lvert Z\rvert=\sqrt{2/\pi}$，故 $\mu_1^{-2}=\pi/2$ |
| $\mathcal I_t$ | 有效相邻收益配对的右端点集合 | 排除不允许跨越的时段边界和无效配对 |
| $b_{t,i}$ | BV 的一个求和项 | $\mu_1^{-2}\lvert r_{t,i}\rvert\lvert r_{t,i-1}\rvert$，仅在 $i\in\mathcal I_t$ 定义；$b$ 是本文的局部简称 |
| $BV_t$ | 本文约定的实现双幂变差 | $\sum_{i\in\mathcal I_t}b_{t,i}$；文字中的 BPV 是同一名称的另一缩写 |

价格、收益和实现波动的基础见 [ABDL03]、[BNS04]。本项目采用 $RV/BV$ 缩写与 Boudt/Dumitru 的 $f$ 体系，均有直接原文依据；BNS 原文的记号并不是 $RV/BV$。

### 2.2 “波动率”不等于一个唯一可观察变量

- **模型中的波动尺度**：描述收益分布的离散程度，如 $\sigma_t$，通常需要估计。
- **实现方差**：将一段时间内已观察收益的平方加总，得到 $RV_t$。
- **实现双幂变差**：将相邻绝对收益乘积按规定常数加总，得到 $BV_t$。

$RV$、$BV$ 是不同统计量。它们与潜在连续波动或跳跃的关系依赖采样、价格过程、噪声和渐近条件；单个 $b_{t,i}$ 也不是直接观察到的真实分钟方差。[BNS04] 的跳跃稳健理论不能无条件移用到任何有限频率、任意混合构造。

### 2.3 边界修正单独标明

对一个含 $n>1$ 个完整收益的连续交易时段，内部共有 $n-1$ 对收益。本项目把有边界系数的版本另记为：

$$
BV^{\mathrm{bc}}_{t,\mathcal S}
=\frac{n}{n-1}\sum_{i\in\mathcal I(\mathcal S)}b_{t,i}.
$$

$\mathcal S$ 是该时段的区间集合；$\mathcal I(\mathcal S)$ 是其内部配对集合；上标 bc 表示边界修正。多交易时段、缺失收益、不同长度区间应分别明确规则，不能机械把 $n/(n-1)$ 套到所有数据。

不同论文可能将带系数的量直接记作 $BV$。对照公式时先核实定义，再比较修正因子。[DGK22]、[DGK23]

### 2.4 连续时间对象：进阶阅读的符号桥接

为说明实现测度的估计目标，连续时间以 $s$ 表示，$p(s)$ 为对数价格，$\sigma(s)$ 为连续部分的瞬时波动率。$\sigma(s)$ 不同于第 3 节的离散日尺度 $\sigma_t$；时间 $s$ 也不同于历史日尺度估计 $\widehat s_t$。

| 符号 | 含义 | 定义与边界 |
|---|---|---|
| $IV_t$ | 积分方差 | 将纳入第 $t$ 日研究区间的 $\sigma^2(s)$ 对时间积分 |
| $QV_t$ | 二次变差 | 在相应连续扩散加跳跃价格过程下，为积分方差加纳入区间内的对数价格跳跃平方和 |
| $\Delta p(s)$ | 时刻 $s$ 的对数价格跳跃 | $p(s)-p(s-)$；$p(s-)$ 是跳跃前的左极限，不是任意一分钟的起点价格 |

$$
IV_t=\int_{\text{交易日 }t}\sigma^2(s)\,ds,
\qquad
QV_t=IV_t+\sum_{s\text{ 为该日跳跃时刻}}(\Delta p(s))^2.
$$

积分与跳跃求和的时间范围应与所研究的交易区间一致；未纳入的隔夜、午休变化不能自动算入。无观测噪声等适当条件下，采样网格变细时 $RV$ 趋向二次变差；经典有限活动跳跃等适当设定下，$BV$ 可以趋向连续部分的积分方差。这些是依赖条件的渐近关系，不是有限样本中的逐次等式。[BNS04]、[ABDL03]

第一遍先理解这些对象的区别，再补积分、随机过程与收敛理论；具体教学路线见[阅读教程的进阶桥接](READING_TUTORIAL.md#advanced)。

## 3. 周期分解：标准差因子与方差因子

为解释尺度，先把一天划为 $M$ 个等长、纳入研究的区间，采用一个限定的离散模型：

$$
r_{t,i}=\frac{\sigma_t}{\sqrt M}f_{t,i}\varepsilon_{t,i},
\qquad
\frac1M\sum_{i=1}^{M}f_{t,i}^2=1.
$$

这里暂设条件均值为 0、无跳跃，日内基础尺度 $\sigma_t/\sqrt M$ 不变。它是便于理解的起点，不是对所有市场的事实断言；更一般模型可加入日内随机波动、均值、跳跃或噪声。

在条件扰动方差为 1 时：

$$
E[r_{t,i}^2\mid\sigma_t,f]=\frac{\sigma_t^2}{M}f_{t,i}^2.
$$

全天各区间条件方差之和为 $\sigma_t^2$；若进一步把它解释为累计收益的条件方差，还需要区间之间条件协方差为零。

这个模型区分三个对象：**日整体波动水平** $\sigma_t$、**日内重复的幅度差异** $f_{t,i}$、**剩余随机变化** $\varepsilon_{t,i}$。去除 $f$ 并不会同时去除 $\sigma_t$。[BCL11]、[HLP12]、[DHI25]

### 3.1 文献符号不能只按字母对齐

| 文献 | 原文用法与归一化 | 本项目如何阅读 |
|---|---|---|
| Boudt 等；Hecq 等 | $f$ 是标准差因子，平均 $f^2=1$ | 采用这套周期标尺 |
| Andersen–Bollerslev | $s$ 是标准差周期因子，平均 $s=1$ | 变换因子时也要同步重标度日尺度 |
| Laakkonen | $s$ 是标准差因子；$f$ 另指日尺度调整后的对数平方收益变换 | 不能把该 $f$ 当作本项目的 $f$ |
| Payne | $\psi$ 加在对数平方收益方程中 | 对应的标准差乘数是 $e^{\psi/2}$；这是由方程推导的映射 |
| Andersen–Su–Todorov–Zhang | $f$ 是平均瞬时方差的归一曲线，积分为 1 | 属于方差形状；估计目标不自动等于本项目的 $\widehat f^2$ |
| Todorov–Zhang | 扩散项含 $\sqrt{\eta}$，故 $\eta$ 是方差周期 | 其 $\widehat\eta_\phi$ 估计累计占比，不能当作点态周期因子 |

对应原文定位见第 8 节。对 AB 的标准差因子 $s_i$，设
$k=\sqrt{M^{-1}\sum_i s_i^2}$，需同时令 $f_i=s_i/k$、
$\sigma_t^{\mathrm{new}}=k\sigma_t^{\mathrm{AB}}$，收益模型才能保持不变。

## 4. 季节因子的估计方法

| 方法 | 估计对象与主要操作 | 假设、时点或限制 |
|---|---|---|
| 历史收益平方的均值/中位数 | 先按历史日尺度标准化收益，再逐日内位置汇总平方值，最后归一化 | 均值与中位数估计的分布特征不同；相对尺度解释需要相应分布假设 |
| ShortH / WSD | 对同一位置的历史标准化收益估计稳健尺度；ShortH 给初始尺度，WSD 降低异常收益影响 | 需明确标准化尺度、零收益与退化处理；不能仅因使用中位数就称为 WSD |
| FFF | 用多项式、Fourier 项、时段虚拟变量及可能的状态交互描述周期 | 原文允许状态交互；对已有曲线做平滑不等于复现整套回归 |
| 固定、滚动、状态条件化 | 决定哪些历史日进入估计，或允许周期随事前状态变化 | 是历史样本与时变假设的选择，可与上面估计器组合 |
| 相对混合贡献的历史中位数 | 对特定贡献代理先作日内归一，再按位置取历史中位数 | 是第 7 节的项目基线，没有上述文献的通用混合定义 |

参考：[AB97]、[BCL11]、[HLP12]、[LAA14]、[DHI25]。当前材料支持整理和比较这些路线，不据此指定所有市场通用的最优方法或窗口。

### 4.1 标准化收益与去周期收益不同

估计周期时，历史收益常先除以历史日的局部/基础尺度：

$$
\bar r_{t,i}=\frac{r_{t,i}}{\widehat s_t}.
$$

$\widehat s_t$ 是这里定义的历史收益尺度，和周期因子 $\widehat f_{t,i}$ 不同。$\bar r$ 用来估计周期；$r^f=r/\widehat f$ 才是去周期收益。历史完整日的实现尺度可用于次日拟合，但目标日尚未实现的全日尺度不可用于盘中预测。

### 4.2 WSD 的核心对应

Boudt 作者稿 pp.9–10，式(2.9)–(2.12)，给出有符号收益的 ShortH/WSD 核心。用初始周期因子 $\widehat f_i^{\mathrm{ShortH}}$ 定义权重：

$$
w_{h,i}=\mathbf1\!\left\{
\left(\frac{\bar r_{h,i}}{\widehat f_i^{\mathrm{ShortH}}}\right)^2\le6.635
\right\},
\qquad
WSD_i=\sqrt{1.081\,
\frac{\sum_h w_{h,i}\bar r_{h,i}^2}{\sum_h w_{h,i}}}.
$$

$h$ 是进入估计的历史交易日；$w$ 是保留权重。然后：

$$
\widehat f_i^{\mathrm{WSD}}
=\frac{WSD_i}{\sqrt{M^{-1}\sum_j WSD_j^2}}.
$$

ShortH 使用有符号样本排序后的最短半样本区间；不能替换成绝对收益中位数而仍称同一估计器。日尺度如何估计、是否分时段、滚动窗口和退化策略是实际应用还需明确的部分。[BCL11]

DHI 附录的 ShortH 归一化存在漏平方根的排版差异；本项目依据 Boudt 原始公式及平方均值为 1 的尺度条件核对。[DHI25]

## 5. 去季节与波动测度修正

### 5.1 从收益出发：保持统计量之间的代数一致性

$$
r^f_{t,i}=\frac{r_{t,i}}{\widehat f_{t,i}},
\qquad
RV_t^f=\sum_i\frac{r_{t,i}^2}{\widehat f_{t,i}^2},
\qquad
b^f_{t,i}=\frac{b_{t,i}}{\widehat f_{t,i}\widehat f_{t,i-1}}.
$$

这些是代数恒等式，不依赖正态假设。BPV 的除数含相邻两个标准差因子；它通常不等于当前分钟的方差因子 $\widehat f_{t,i}^2$。再次把配对因子归一为均值 1，会改变这一变换。

若还要求 BPV 求和项的条件期望，则在第 3 节基础上增加相邻扰动条件独立、标准正态假设：

$$
E[b_{t,i}\mid\sigma_t,f]
=\frac{\sigma_t^2}{M}f_{t,i}f_{t,i-1}.
$$

这个期望关系与上一段无分布假设的代数关系应分开使用。

### 5.2 日级标量修正：修正的对象不同

对第 2.3 节的一个时段，定义本段使用的期望比率：

$$
C_{t,\mathcal S}^{\mathrm{pair}}
=\frac{n}{n-1}
\frac{\sum_{i\in\mathcal I(\mathcal S)}f_{t,i}f_{t,i-1}}
{\sum_{i\in\mathcal S}f_{t,i}^2}.
$$

在上述限定模型下，$BV^{\mathrm{bc}}_{t,\mathcal S}/C_{t,\mathcal S}^{\mathrm{pair}}$
的条件期望等于该时段各区间的条件方差之和。它是日/时段总量上的标量操作；逐项计算 $b^f$ 则改变了被度量的收益过程，两者不必数值相同。

这是用于理解尺度的项目推导，不替代 Dette 等论文的完整方法。实际使用估计因子时还存在估计误差。其 2020 工作论文式(14)与式(3)/(12)的边界系数存在不一致，不能直接叠乘；Metrika 正式文 322 页式(11)–(12)给出自洽核对。[DGK22]、[DGK23]

## 6. 如何评价：诊断、预测和时钟

### 6.1 分布图回答什么问题

均值、中位数、IQR 和高分位数是不同统计对象。去掉乘法尺度，不保证它们全部同时变平，也不保证数据独立同分布。

例如，独立标准正态变量 $Z,Z'$ 构成的 $Z^2$ 与
$\mu_1^{-2}|Z||Z'|$ 期望都为 1，方差却分别为 2 与 $\pi^2/4-1$。
因此首分钟收益平方与其他分钟 BPV 的分布差异，不能全部解释为去季节失败。

“去周期后单日内基础波动恒定”“周期形状跨日固定”“额外因子能改善预测”是不同问题。[CHP18]、[ATT19] 的正式检验有各自假设、估计误差和噪声处理要求，不能由普通分位图替代。

### 6.2 预测的可用时点

- 目标日 $t$ 的盘前季节因子最多用到 $t-1$。
- 目标日全日均值适合事后形状诊断，盘中不能提前作为特征分母。
- 用未滤波的未来 $RV$ 作目标、用滤波后的历史 $RV^f$ 作解释变量，是 DHI/HARP 的设计之一；不要求目标也改成 $RV^f$。[DHI25]
- 方法、窗口和状态阈值的选择也需使用预测时点以前的信息；评价应采用共同日期、共同目标与共同起点。

若预测未来 $H$ 个区间，可明确写：

$$
RV_{t,k\to k+H}=\sum_{i=k+1}^{k+H}r_{t,i}^2.
$$

$k$ 是预测起点。BV 标签还必须声明：只使用未来区间内部的收益配对，还是按配对右端点归属并包含已知的 $r_{t,k}$。两者不是同一个标签。

### 6.3 事前周期时钟与实现活动时钟

事前周期暴露可以写为 $\tau_{t,k}=\sum_{i\le k}\widehat f_{t,i}^2$；
实现活动量可以写为 $A_{t,k}=\sum_{i\in\mathcal I_t,\ i\le k}b^f_{t,i}$。
$\tau$、$A$ 是本段明确规定的时钟记号。

前者可在盘前形成，后者随已观察收益增长。实现活动钟的阈值须事先定义，不能用当天尚未知的最终总量固定事件数。只有分钟 OHLC 时，分钟内真实触达时刻不可观察。

## 7. 已有 JQ / IM 实现的适配记录

本节记录[支持附件中的既有案例](supporting_materials/intraday_modeling_research/README.md)，其中的代码与结果作为这一次研究的材料保留。案例参数不能作为所有市场的默认规则。

### 7.1 混合贡献基线的项目符号

$c^{\mathrm{mix}}$、$x^{\mathrm{mix}}$、$\widehat g^{\mathrm{mix}}$ 是明确的项目定义，没有在本次核对文献中找到相同混合构造的通用符号。

$$
c_{t,i}^{\mathrm{mix}}=
\begin{cases}
r_{t,i}^2,&\text{交易时段首区间},\\
b_{t,i},&\text{其余有效区间},
\end{cases}
\qquad
x_{t,i}^{\mathrm{mix}}
=\frac{c_{t,i}^{\mathrm{mix}}}{M^{-1}\sum_jc_{t,j}^{\mathrm{mix}}}.
$$

$x$ 是日内相对贡献，完整日均值为 1。历史中位数因子为：

$$
\widehat g_{t,i}^{\mathrm{mix},(L)}
=\frac{\operatorname{median}_{h=t-L,\ldots,t-1}x_{h,i}^{\mathrm{mix}}}
{M^{-1}\sum_j\operatorname{median}_{h=t-L,\ldots,t-1}x_{h,j}^{\mathrm{mix}}}.
$$

它估计混合贡献的典型相对形状，不自动等于收益方差因子 $\widehat f^2$。

| 旧实现名称 | 本文符号或定义 |
|---|---|
| variance_contribution / mixed_contribution | $c_{t,i}^{\mathrm{mix}}$ |
| daily_shape_matrix / mixed_shape_values | $x_{t,i}^{\mathrm{mix}}$ |
| variance_seasonality | $\widehat g_{t,i}^{\mathrm{mix},(L)}$，属于混合基线的历史命名 |
| variance_adjusted | $c_{t,i}^{\mathrm{mix,adj},(L)}=c_{t,i}^{\mathrm{mix}}/\widehat g_{t,i}^{\mathrm{mix},(L)}$ |
| return_adjusted | $r_{t,i}^{\mathrm{adj,mix},(L)}=r_{t,i}/\sqrt{\widehat g_{t,i}^{\mathrm{mix},(L)}}$；不凭构造视为 $r^f$ |
| 诊断中的 1 + residual | $z_{t,i}^{\mathrm{mix},(L)}=c_{t,i}^{\mathrm{mix,adj},(L)}/(M^{-1}\sum_jc_{t,j}^{\mathrm{mix,adj},(L)})$，仅事后可得 |

旧方法直接调整贡献，与用其调整后收益重算 BPV 一般不同：前者除以当前 $\widehat g^{\mathrm{mix}}$，后者除以相邻两个 $\widehat g^{\mathrm{mix}}$ 乘积的平方根。

### 7.2 仅属于当前 IM 案例的参数和边界

- 一分钟频率，全天 $M=240$，上午/下午各 120 个位置；首分钟为 09:31、13:01，对应 $i=1,121$；内部共 238 个收益配对。
- 首分钟用本根 open-to-close 对数收益，其余用组内相邻 close 的对数差；不把午休、隔夜变化混入分钟收益。
- 既有候选回望为 30、60、120、360 个交易日。v2 对不完整历史日保留缺失，不跳过该日去补更早样本。
- 研究代码 daily_bpv_scale 为 $\sqrt{238^{-1}\sum_{i\in\mathcal I_t}b_{t,i}}$，实际是分钟尺度。若记为 $\widehat s_t$，对应日标尺为 $\sqrt M\,\widehat s_t$；这是该实现的标尺定义。
- 研究输出 variance_factor 对收益尺度方法存放 $\widehat f^2$，对 median_mixed 存放 $\widehat g^{\mathrm{mix}}$；字段解释必须连同 method 一起看。
- 分 Session 的 log-WSD Fourier 投影是已有项目的改编，不能直接标成 AB1997 全部 FFF 的复现。
- 已有固定 15 分钟 BPV 标签采用右端点归属，第一项含起点已知收益；这只是该实验的标签选择。

## 8. 原文定位、版本和符号差异

下表页码以“实际核对版本”为准。原有资料的完整历史阅读范围、新增材料的定向核对范围及下载说明，均保存在 source_manifest.json；下表只列本项目最相关的定位。

| 文献及本地全文 | 实际核对版本与定位 | 对本文的支持 |
|---|---|---|
| [Boudt–Croux–Laurent（2011）][BCL11]：Robust estimation of intraweek periodicity in volatility and jump detection | 35 页作者稿；pp.5–10，式(2.1)–(2.12) | 标准差周期 $f$、平方均值归一化、ShortH/WSD |
| [Andersen–Bollerslev（1997）][AB97]：Intraday periodicity and volatility persistence in financial markets | 期刊扫描版；p.125 式(1)，p.141 式(7)，pp.152–155 附录 | 标准差周期 $s$、均值归一化、FFF 与状态交互 |
| [Christensen–Hounyo–Podolskij（2018）][CHP18]：Is the diurnal pattern sufficient to explain intraday variation in volatility? A nonparametric assessment | CREATES 2017-30；印刷 pp.4、6–7、9–13 | 检验日内周期能否充分解释波动变化 |
| [Andersen–Thyrsgaard–Todorov（2019）][ATT19]：Time-Varying Periodicity in Intraday Volatility | 2018-06-18 作者稿；pp.7–11、14、18 | 检验周期是否随交易日与状态变化 |
| [Dette–Golosnoy–Kellermann（2022）][DGK22]：Correcting Intraday Periodicity Bias in Realized Volatility Measures | 2020-07-13 工作论文；印刷 pp.6、8、10–12 | 周期对测度的影响；式(14)边界系数问题单列 |
| [Dette–Golosnoy–Kellermann（2023）][DGK23]：The effect of intraday periodicity on realized volatility measures | Metrika 正式版；pp.318–324，尤其 p.322 式(11)–(12) | 周期对 BV 等测度及其修正的影响 |
| [Dumitru–Hizmeri–Izzeldin（2025）][DHI25]：Forecasting the realized variance in the presence of intraday periodicity | 期刊正式版；p.3 式(2)–(6)，p.4 式(13)，p.16 附录 B.1 | 标准差周期 $f$、去周期收益 $r^f$ 与 HARP |
| [Barndorff-Nielsen–Shephard（2004）][BNS04]：Power and Bipower Variation with Stochastic Volatility and Jumps | 期刊正式版；pp.2–4、8–9、12 | 双幂变差、正态绝对矩及连续波动/跳跃；原文不以 $RV/BV$ 为主要记号 |
| [Andersen–Bollerslev–Diebold–Labys（2003）][ABDL03]：Modeling and Forecasting Realized Volatility | 期刊正式版；pp.582–583、594–595，式(12) | 对数价格 $p$、收益 $r$、实现协方差 $V$ |
| [Lee–Mykland（2008）][LM08]：Jumps in Financial Markets: A New Nonparametric Test and Jump Dynamics | 期刊正式版；pp.2537、2539–2541，式(7)–(10) | 局部尺度与跳跃检验；原文 $c=E\lvert U\rvert$，与本项目混合贡献 $c^{\mathrm{mix}}$ 不同 |
| [Hecq–Laurent–Palm（2012）][HLP12]：Common Intraday Periodicity | Maastricht RM/11/010；内页日期 2010-12-23；印刷 pp.3–5 | 共同周期；$f$ 为标准差因子，平均 $f^2=1$；与 Boudt 论文共享作者 Laurent |
| [Laakkonen（2014）][LAA14]：Exchange rate volatility, macroeconomic announcements and the choice of intraday periodicity filtering method | 实存 Bank of Finland 23/2007；印刷 pp.11–12，式(2.1)、(3.1)–(3.3) | 不同滤波方法；$s$ 为标准差因子，$f$ 是另一对数变换对象 |
| [Payne（DP238）][PAYNE]：Announcement Effects and Seasonality in the Intra-day Foreign Exchange Market | 封面 1996-03，正文修订 1997-05-20；印刷 pp.11–13 | 周期 $\psi$ 加在对数方差中；映射为标准差乘数需除以 2 再取指数 |
| [Andersen–Su–Todorov–Zhang（2024）][ASTZ24]：Intraday Periodic Volatility Curves | 2023-01-24 作者稿；p.7 式(3)，p.10 式(4)–(5) | $f$ 为归一化平均瞬时方差曲线；同字母不同尺度 |
| [Todorov–Zhang（2024）][TZ24]：Intraday Volatility Patterns from Short-Dated Options | 2024-04-07 作者稿；p.5 式(1)，p.7 式(8) | 期权提取的方差周期 $\eta$；$\widehat\eta_\phi$ 是累计占比 |

其中 ASTZ 的 $f$、Laakkonen 的 $f$ 和本项目的 $f$ 不是相同对象。Todorov–Zhang 的 $\eta$ 只识别到乘法常数，其 $\widehat\eta_\phi$ 是归一化累计积分比率；不能直接当作分钟点态因子。

另保留两条尚未完成公式核验的文献线索，**不计入上表 15 份资料**：
[Taylor–Xu（1997）](https://doi.org/10.1016/S0927-5398(97)00010-8)；
[Martens–Chang–Taylor（2002）](https://doi.org/10.1111/1475-6803.t01-1-00009)。
不能根据摘要或二手引用补写它们的符号与归一化。

## 9. 本次整理的完成范围

已建立独立文献归档和本文的符号、方法说明，并收录原 intraday_modeling_research 作为一次性支持附件。保留原文版本、实际阅读范围及文件摘要；本次没有重跑实验，附件里的结果沿用原有研究范围与评估口径。

以后增加方法时，继续在本文补充对象、定义、假设、时点和原文定位；具体市场参数放在对应适配段，避免把一个市场的设置提升为通用定义。

[BCL11]: literature/boudt_croux_laurent_2011_wp.pdf
[AB97]: literature/andersen_bollerslev_1997.pdf
[CHP18]: literature/christensen_hounyo_podolskij_2018_working_paper.pdf
[ATT19]: literature/andersen_thyrsgaard_todorov_2019.pdf
[DGK22]: literature/Dette_Golosnoy_Kellermann_2020_Correcting_IP_Bias_working_paper.pdf
[DGK23]: literature/Dette_Golosnoy_Kellermann_2023_Effect_IP_Metrika_published.pdf
[DHI25]: literature/Dumitru_Hizmeri_Izzeldin_2025_Forecasting_RV_periodicity_published.pdf
[BNS04]: literature/Barndorff_Nielsen_Shephard_2004_power_bipower_published.pdf
[ABDL03]: literature/Andersen_Bollerslev_Diebold_Labys_2003_realized_volatility_published.pdf
[LM08]: literature/Lee_Mykland_2008_jumps_published.pdf
[HLP12]: literature/Hecq_Laurent_Palm_2012_common_periodicity_working_paper.pdf
[LAA14]: literature/Laakkonen_2014_filtering_methods_BoF_2007.pdf
[PAYNE]: literature/Payne_DP238_seasonality_revision_19970520.pdf
[ASTZ24]: literature/Andersen_Su_Todorov_Zhang_2024_periodic_curves_author_20230124.pdf
[TZ24]: literature/Todorov_Zhang_2024_short_dated_options_author_20240407.pdf
