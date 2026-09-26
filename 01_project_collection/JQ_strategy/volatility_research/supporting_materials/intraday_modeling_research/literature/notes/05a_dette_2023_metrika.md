# 辅助核对：Dette、Golosnoy、Kellermann（2023），Metrika

入口：[研究目录](../../README.md)；与主文献的关系见 [2022 阅读笔记](05_dette_2022.md)。

*The effect of intraday periodicity on realized volatility measures*，Metrika 86(3)，315–342；[出版社全文](https://link.springer.com/article/10.1007/s00184-022-00875-0)，DOI [10.1007/s00184-022-00875-0](https://doi.org/10.1007/s00184-022-00875-0)。2022-07-16 在线发表，2023 年刊期；本地为 [CC BY 正式 PDF](../papers/Dette_Golosnoy_Kellermann_2023_Effect_IP_Metrika_published.pdf)。它与 *Correcting…* 是两篇论文，不能共享标题、DOI 或方程号。

**阅读范围。** 阅读摘要、§2 模型与 §3.1–3.2（印刷 318–324 页，PDF 4–10 页），并以图像核对 322 页式 (11)–(12)。辅助用途限于定义、期望及修正因子的相互核对；未声称通读本文证明和所有实证表。

以下将原文周期符号映射为[项目约定](../../research/03_method_derivations.md)的 $f_i$，并非原文逐字转录。令标准差周期 \(f_i>0\)，\(\sum f_i^2=M\)。原文 BV 已含 $M/(M-1)$ 边界系数，本项目对应记为 $BV^{\mathrm{bc}}$。印刷 322 页给出的关系可重写为

\[
\widetilde{BV}
=\frac{M-1}{\sum_{i=2}^{M}f_if_{i-1}}BV^{\mathrm{bc}}
=\frac{M}{\sum_{i=2}^{M}f_if_{i-1}}\frac{\pi}{2}\sum_{i=2}^{M}|r_ir_{i-1}|.
\]

原文式 (12) 把 \(\zeta_M=BV^{\mathrm{bc}}/\widetilde{BV}\) 定义为偏差因子，所以校正时**除以** \(\zeta_M\)。不要把该因子当作直接乘数。印刷 323 页式 (13) 的 \(\xi_{M,RQ}=M^{-1}\sum f_i^4\) 与 BV 因子不同：它反映四次变差的周期放大，不能拿来校正二次变差。

数学限定：核心推导采用等间隔、无跳跃独立高斯增量，随机波动部分在一天内固定。对午休切分、首分钟平方项、夜盘或分钟贡献的中位数曲线，仍需重新确定目标统计量，本文没有给出可直接套用的中国期货实现。
