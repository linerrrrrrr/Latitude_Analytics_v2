# Andersen、Bollerslev（1997）阅读笔记

文献：*Intraday periodicity and volatility persistence in financial markets*，Journal of Empirical Finance 4(2–3), 115–158。[DOI](https://doi.org/10.1016/S0927-5398(97)00004-2)。

版本与访问：[Bollerslev 的 Duke 主页](https://public.econ.duke.edu/~boller/research.html)链接的[期刊原版PDF](https://public.econ.duke.edu/~boller/Published_Papers/joef_97.pdf)，44页扫描件。见[本地原文](../papers/andersen_bollerslev_1997.pdf)及[下载校验记录](02_andersen_bollerslev_1997.metadata.json)。

阅读范围：逐页视觉阅读PDF第1–41页，即期刊115–155页，涵盖正文、图1–7、表1–5和附录A–B；末尾纯参考文献第42–44页未精读。此文件没有可提取文本，不能把空文本误认为未成功下载。以下定位使用期刊页码。

## 原文要点与公式定位

第125页式1的简化分解为 $R_{t,n}=\sigma_t s_n Z_{t,n}/\sqrt N$，并采用 $\operatorname{mean}(s_n)=1$；$Z_{t,n}$ 独立同分布、均值0、方差1且与日波动过程独立。第128页式4说明周期形状会影响绝对收益自相关。样本为5分钟德国马克/美元及S&P 500期货收益。

第141页式7推广为 $s_{t,n}$，允许形状随日状态变化；不是始终固定的一条曲线。附录B第152–155页式A.1–A.4拟合：

$$
x_{t,n}=2\log|R_{t,n}-E(R_{t,n})|-\log\sigma_t^2+\log N
=f(\theta;\sigma_t,n)+u_{t,n}.
$$

FFF 包括多项式、正余弦、特殊时段虚拟变量及日波动交互项。日波动先估计，第二步回归；最后从 $\exp(\hat f_{t,n}/2)$ 归一得到周期标准差因子，再过滤收益。第154页股票样本采用状态交互；第155页对最后三个时段单设虚拟变量。过滤后仍存在剩余动态（第143–150页）。

## 对本项目的推导与贴文校准

以下为项目解释，并非论文已证明的本地实证结果。

- “FFF 等于一条平滑曲线”可作入门简化，不能理解为必须把午休、开收盘机制差异平滑掉。可比较上午/下午分段基函数与特殊分钟项。
- 该文已允许日状态与形状交互，因此不能把所有早期模型描述为固定形状；后续时变周期检验的贡献应按各自原文界定。
- 归一的是标准差还是方差必须写清。这里的 $\operatorname{mean}(s)=1$ 与 Boudt 的 $\operatorname{mean}(f^2)=1$ 不相同；本项目可选择自己的统一标尺，但应给转换公式。
- 论文的收益过滤 $R/s$ 不等同于本项目的混合贡献过滤 $c^{\mathrm{mix}}/\widehat g^{\mathrm{mix}}$（[符号映射](../../research/03_method_derivations.md)）。迁移时先明确符号所指对象，训练阶段的日尺度与测试日可用信息也应分开。
- 平滑方法属于待比较估计器；分钟锯齿可能同时包含估计噪声和真实制度效应，需要样本外证据决定，而非仅按图形美观选择。
