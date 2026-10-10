# 文献目录与获取状态

原有六篇指定论文已全部取得公开原文版本，另补充一篇 Metrika 论文核对 BPV 修正常数。2026-09-08 又为统一符号新增八篇定向方法页核对，见[跨文献符号核对](notes/07_notation_survey.md)。原有七份资料的历史阅读范围与本轮新增核对分开登记；不表示本轮通读了十五篇全文。论文的发表年与实际取得稿件年份分开记录。

| 文献 | 正式引用 | 实际取得版本 | 阅读笔记 / 原文 |
|---|---|---|---|
| Robust estimation of intraweek periodicity in volatility and jump detection | 2011 · [Journal of Empirical Finance](https://doi.org/10.1016/j.jempfin.2010.11.005) | 35-page author manuscript linked by Sébastien Laurent; exact manuscript date is not printed; not the 15-page journal pagination | [笔记](notes/01_boudt_2011.md) · [PDF](papers/boudt_croux_laurent_2011_wp.pdf) |
| Intraday periodicity and volatility persistence in financial markets | 1997 · [Journal of Empirical Finance](https://doi.org/10.1016/S0927-5398(97)00004-2) | Published journal version, scanned PDF, 44 pages, journal pp.115-158 | [笔记](notes/02_andersen_bollerslev_1997.md) · [PDF](papers/andersen_bollerslev_1997.pdf) |
| Is the diurnal pattern sufficient to explain intraday variation in volatility? A nonparametric assessment | 2018 · [Journal of Econometrics](https://doi.org/10.1016/j.jeconom.2018.03.016) | CREATES Research Paper 2017-30, July 2017; working paper rather than the 2018 publisher-typeset version | [笔记](notes/03_christensen_2018.md) · [PDF](papers/christensen_hounyo_podolskij_2018_working_paper.pdf) |
| Time-Varying Periodicity in Intraday Volatility | 2019 · [Journal of the American Statistical Association](https://doi.org/10.1080/01621459.2018.1512864) | Author-hosted manuscript dated June 18, 2018; journal publication in 2019, not publisher-typeset version | [笔记](notes/04_andersen_thyrsgaard_todorov_2019.md) · [PDF](papers/andersen_thyrsgaard_todorov_2019.pdf) |
| Correcting Intraday Periodicity Bias in Realized Volatility Measures | 2022 · [Econometrics and Statistics](https://doi.org/10.1016/j.ecosta.2021.03.002) | SFB823 21/2020 working paper, 2020-07-13; not publisher final | [笔记](notes/05_dette_2022.md) · [PDF](papers/Dette_Golosnoy_Kellermann_2020_Correcting_IP_Bias_working_paper.pdf) |
| The effect of intraday periodicity on realized volatility measures | 2023 · [Metrika](https://doi.org/10.1007/s00184-022-00875-0) | Published version; online 2022-07-16, issue 2023; CC BY 4.0 | [笔记](notes/05a_dette_2023_metrika.md) · [PDF](papers/Dette_Golosnoy_Kellermann_2023_Effect_IP_Metrika_published.pdf) |
| Forecasting the realized variance in the presence of intraday periodicity | 2025 · [Journal of Banking & Finance](https://doi.org/10.1016/j.jbankfin.2024.107342) | Published version; online 2024-11-23, issue 2025; CC BY 4.0 | [笔记](notes/06_dumitru_hizmeri_izzeldin_2025.md) · [PDF](papers/Dumitru_Hizmeri_Izzeldin_2025_Forecasting_RV_periodicity_published.pdf) |

## 阅读顺序

讨论日内去季节符号时，先读[跨文献符号核对](notes/07_notation_survey.md)，再看[项目统一符号和方法公式](../research/03_method_derivations.md)。该核对记录覆盖本次新增八篇研究的版本、公开原文、本地 PDF、页码、公式和归一化差异；它是研究证据文档，不另立符号规范。

建议先读 Boudt → Andersen–Thyrsgaard–Todorov → Christensen → Dette，再回看 Andersen–Bollerslev 的函数形式，最后读 Dumitru 的预测设计。辅助 Metrika 用于核算普通 BPV 的有限样本常数。

逐篇笔记记录真实阅读范围、页码和方程号。作者稿的页码不能与期刊页码混用；未取得的独立补充附录不算已读。

## 对原始线索的核对

- Andersen–Bollerslev 1997 已讨论日波动水平与周期函数的交互；“FFF 只是一条固定平滑曲线”过于简化。
- 固定周期的分布推论依赖平稳随机波动等附加假设，不能直接用当前混合贡献 z 的图替代 ATT 检验。
- Christensen 的日内恒定性与 ATT 的跨时刻分布稳定性是不同零假设。
- Dette 2020 工作论文的式(14)与其式(3)/(12)存在边界常数不一致；辅助正式 Metrika 文章给出自洽关系。
- Dumitru 2025 的目标仍是未滤波 RV；改善并非在每个模型和损失指标上都成立。其 ShortH 附录归一化排版也应回查 Boudt 原文。

以上定位和限定均在对应笔记中说明。原始用户线索保留在 [sources](../sources/user_literature_brief.txt)，其内容不作为未经核对的结论。

全部十五份资料的引用管理：[BibTeX](references.bib)；机器可读来源、版本、核对页码和文件摘要统一登记在 [source_manifest.json](source_manifest.json)。其中 core/auxiliary 保留原七份资料的身份，notation_survey 标记本轮新增八篇。

资料保存供本地研究；各文件的版本/许可依其原文和来源网站说明。
