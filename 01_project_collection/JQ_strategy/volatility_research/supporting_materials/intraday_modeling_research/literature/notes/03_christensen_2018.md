# Christensen、Hounyo、Podolskij（2018）阅读笔记

Christensen, K., Hounyo, U., & Podolskij, M. (2018). *Is the diurnal pattern sufficient to explain intraday variation in volatility? A nonparametric assessment*. Journal of Econometrics, 205(2), 336–362. [DOI](https://doi.org/10.1016/j.jeconom.2018.03.016)；[大学书目核验](https://pure.au.dk/portal/en/persons/mark-podolskij%2854da8331-1355-4ae6-85dc-0cff03fe2be8%29/publications/is-the-diurnal-pattern-sufficient-to-explain-intraday-variation-in-volatility-a-nonparametric-assessment%284666a669-005b-4576-b47b-0bcc17f9850b%29.html)。

实际阅读的是 [CREATES 2017-30 原文](https://repec.econ.au.dk/repec/creates/rp/17/rp17_30.pdf)，封面日期 July 2017，64 个 PDF 页面；不是期刊排版版。下文“页”指文内页码，PDF 页码为其加 2。

## 原文要点

固定周期乘以随机波动是建模前提；检验的零假设更强：去周期后的波动在单日内恒定。预平均处理噪声、截断处理跳跃，再比较两类 bipower 统计量。小跳跃会扭曲有限样本检验，bootstrap 改善尺寸。美国股票实证中，去周期后异方差减少但仍存在。原文并未检验 IM 分钟 BPV。

## 回读定位

| 内容 | 文内页 / 定位 |
|---|---|
| 分解、平稳与周期归一化假设 | 4，D1–D3；噪声限制见 6，式 (4)–(5) |
| 单日恒定的零假设 | 6–7，式 (6) 与 §2.3 |
| 截断预平均 bipower 与差异统计量 | 9–11，式 (20)、(23)、(26)–(27) |
| 周期估计与估计误差条件 | 11–13，式 (31)–(38) |
| bootstrap 与一般异方差噪声边界 | 14–19，§4、Remark 3 |
| 实证与结论 | 35–40，§6–7、表 5、图 6–7 |

实际阅读范围：§1–3；§4 的构造与可行统计量（14–19）；§5 的结果讨论（32–35）；§6–7。附录证明未逐行复核。已图像复核 PDF 第 6、13、38 页。

## 对本项目的启发与待验证假说

“平均日内曲线存在”“曲线跨日固定”“曲线解释全部单日变化”应分别提出问题。前两项即使成立，也不自动保证第三项成立。这一区分是本项目安排实验的逻辑，不是已经得到的 IM 结果。

拟先在训练区间估计分钟尺度，再检查去尺度后的日内残差与分位数。若直接使用已有一分钟 BPV，不应把普通 BPV 或逐时段方差检验命名为本文的完整检验；后者涉及预平均、截断、噪声结构和 bootstrap 校准。正式复现前需核对这些数据要求。

本文的“同方差”结论针对扩散波动；整段收益还可能包含漂移、跳跃和噪声。由此推导任意有限频率 BPV 的整分布一致，需要另行写明观测及联合分布假设，不能仅凭一个周期均值归一化。

本笔记记录文献阅读与研究设想，不是生产契约。独立项目后来完成的模拟和实证见[总报告](../../FINAL_REPORT.md)，不将其冒称为本篇正式检验的完整复现。
