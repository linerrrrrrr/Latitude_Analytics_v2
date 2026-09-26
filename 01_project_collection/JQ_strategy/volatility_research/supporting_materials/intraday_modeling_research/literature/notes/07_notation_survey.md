# 日内周期符号的跨文献定向核对

核对日期：2026-09-08。入口：[研究项目](../../README.md)；项目统一符号与公式解释见[方法与公式对照](../../research/03_method_derivations.md)。本文保存符号选择的研究证据，不另立一套项目规范。

## 1. 研究范围与证据层级

本次问题是“哪些记号具有文献依据，以及相同字母是否总指同一对象”，不是比较估计器预测优劣，也不是测量所有论文采用某个字母的比例。检索以已实现波动、BPV、日内周期、宏观新闻过滤和期权周期估计为线索，优先核对作者、大学、央行和期刊公开原文。

- **既有背景：7 份 PDF。** 原项目六篇指定研究及一篇 Metrika 辅助研究的历史阅读范围，见各篇笔记和 [source_manifest.json](../source_manifest.json)。本次重新核对其中 Boudt、Andersen–Bollerslev、Dumitru 的相关方法页；没有把原有 7 篇全部从头重读。
- **本轮新增：8 篇方法页核对。** 下文登记版本、原文入口、实际方法页和符号角色；均取得可读全文，但只针对有关定义和公式阅读，不表示通读了全部正文、证明或补充材料。
- **检索线索：2 篇，未计入公式证据。** Taylor–Xu（1997）的 [The incremental volatility information in one million foreign exchange quotations](https://doi.org/10.1016/S0927-5398(97)00010-8) 只核实出版信息和摘要；Martens–Chang–Taylor（2002）的 [A Comparison of Seasonal Adjustment Methods When Forecasting Intraday Volatility](https://doi.org/10.1111/1475-6803.t01-1-00009) 找到[政大学术库](https://ah.lib.nccu.edu.tw/item?item_id=6998)和 SSRN 入口，但未完成原文公式核验。不根据二手引用补写其符号或归一化公式。

“原有 7 份 + 新增 8 篇”是本地资料范围，不是 15 篇独立同分布的学术用法样本。Hecq 与 Boudt 共享 Laurent，其他研究也有引用继承关系；以下结论应理解为跨作者、方法与年代的定向核对。

## 2. 既有文献中的关键对照

| 文献与版本 | 已核定义及定位 | 对符号选择的意义 |
| --- | --- | --- |
| [Boudt、Croux、Laurent（2011）作者稿笔记](01_boudt_2011.md) | 作者稿 pp.5–6，式(2.1)–(2.2)：\(r_i=f_i s_i u_i\)，局部窗口内平均 \(f_i^2=1\)；p.8，式(2.6)–(2.7)：\(\bar r_i=r_i/\hat s_i\)；p.10，式(2.10)–(2.12)：ShortH/WSD。 | \(f\) 是标准差周期因子，\(s\) 是局部收益尺度；\(u\) 是创新，不能把这里的 \(u\) 解释成项目相对贡献。 |
| [Andersen–Bollerslev（1997）正式版笔记](02_andersen_bollerslev_1997.md) | 期刊 p.125，式(1)：日内收益分解为 \(R_{t,n}=\sigma_t s_n Z_{t,n}/\sqrt N\)；这里采用平均 \(s_n=1\)。 | \(s\) 同样在标准差尺度上，但归一化不同。不能仅将字母 \(s\) 换为 \(f\) 就视为完全相同的参数化。 |
| [Dumitru、Hizmeri、Izzeldin（2025）正式版笔记](06_dumitru_hizmeri_izzeldin_2025.md) | p.3，式(2)–(6)：\(BV_t\)、\(M^{-1}\sum_i f_i^2=1\)、\(r^f_{t,i}=r_{t,i}/\hat f_i^{WSD}\)，并定义 \(RV_t^f\)、\(BV_t^f\)。 | 为项目 \(r,f,RV,BV\) 及滤波上标 \(f\) 提供直接对应；附录 ShortH 排版问题仍按原笔记回查 Boudt。 |

若将 Andersen–Bollerslev 的尺度转换到 Boudt 式归一化，设 \(k=\sqrt{N^{-1}\sum_n s_n^2}\)，则同时令 \(f_n=s_n/k\)、\(\sigma_t^{\mathrm{new}}=k\sigma_t^{\mathrm{AB}}\)，才能保持收益分解不变。这里是参数重标度的代数关系，不是新的估计方法。

## 3. 新增原文核对

### 3.1 Barndorff-Nielsen–Shephard（2004）：原始 BPV 记号

*Power and Bipower Variation with Stochastic Volatility and Jumps*，Journal of Financial Econometrics 2(1), 1–37；[DOI](https://doi.org/10.1093/jjfinec/nbh001)。核对的是 [Duke 保存的期刊正式 PDF](https://public.econ.duke.edu/~get/browse/courses/883/Spr16/COURSE-MATERIALS/Z_Papers/BNSJFEC2004.pdf)，见[本地原文](../papers/Barndorff_Nielsen_Shephard_2004_power_bipower_published.pdf)。[Shephard 作者页](https://shephard.scholars.harvard.edu/publications/power-and-bipower-variation-stochastic-volatility-and-jumps)另链接 2003-11-02 作者稿；不能把两者版本混写。

本轮核对期刊 pp.2–9、12 的定义，p.8 另核图像：p.2 用 \(y^*(t)\) 表示对数价格、\(y_{j,i}\) 表示第 \(i\) 日第 \(j\) 个收益；p.3 式(1)用 \(\{y_M^*\}^{[2]}_i\) 表示平方收益和；p.4 式(3)用 \(\{y_M^*\}^{[1,1]}_i\) 表示相邻绝对收益乘积之和，原始定义未乘 \(\pi/2\)。

p.8 Theorem 1 定义 \(\mu_r=E|u|^r\)，\(u\sim N(0,1)\)；p.9 §1.4、p.12 的缩放采用 \(\mu_r^{-1}\mu_s^{-1}\)。取两个幂次均为 1，得到 \(\mu_1=\sqrt{2/\pi}\)、\(\mu_1^{-2}=\pi/2\)。这支持项目使用 \(\mu_1\) 写 BPV 常数，但不能宣称原文把该统计量统一写作 \(BV\)。p.3 脚注2还明确提醒，经济计量文献有时也把平方收益和称为 realized volatility；项目仍需明确区分方差与其平方根。

### 3.2 Andersen–Bollerslev–Diebold–Labys（2003）：价格、收益与已实现协方差

*Modeling and Forecasting Realized Volatility*，Econometrica 71(2), 579–625；[DOI](https://doi.org/10.1111/1468-0262.00418)。核对 [Bollerslev 作者页链接的正式 PDF](https://econ.duke.edu/~boller/Published_Papers/ecta_03.pdf)，见[本地原文](../papers/Andersen_Bollerslev_Diebold_Labys_2003_realized_volatility_published.pdf)。

本轮核对期刊 pp.582–583、594–595，p.594 另核图像：对数价格向量为 \(p(t)\)，收益为 \(r(t,h)=p(t)-p(t-h)\)；p.594 式(12)将已实现协方差矩阵记作 \(V_{t,h}=\sum_j r_jr_j'=R_{t,h}'R_{t,h}\)，p.595 将其对角方差记作 \(v_{t,1},v_{t,2}\)。

这为 \(p/r\) 的角色提供独立依据，也表明 \(RV\) 并不是每篇经典文章使用的同一缩写。本文的 \(V\) 是收益平方尺度的矩阵，与项目“分钟贡献”不是同一对象；此处没有用于本项目的季节因子归一化。

### 3.3 Lee–Mykland（2008）：同名尺度仍需检查常数

*Jumps in Financial Markets: A New Nonparametric Test and Jump Dynamics*，Review of Financial Studies 21(6), 2535–2563；[DOI](https://doi.org/10.1093/rfs/hhm056)。核对 [Mykland 作者页链接的正式 PDF](https://galton.uchicago.edu/~mykland/paperlinks/LeeMykland-2535.pdf)，见[本地原文](../papers/Lee_Mykland_2008_jumps_published.pdf)。

本轮核对期刊 pp.2537、2539–2541，p.2541 另核图像：式(1)–(2)用 \(S(t)\) 表示价格，收益写成 \(d\log S(t)\)，\(\sigma(t)\) 表示瞬时波动率。p.2540 式(7)–(8)的检验统计量 \(\mathcal L(i)\) 用当前对数收益除以前序绝对收益配对均值的平方根；这个局部估计没有乘 \(\pi/2\)。

p.2541 式(10)后定义 \(c=E|U_i|=\sqrt{2/\pi}\)，无跳极限方差是 \(1/c^2\)。因此，这里的 \(c\) 与 BNS 的 \(\mu_1\) 数值相同，但不是项目混合贡献；局部 \(\hat\sigma\) 也不能与 Boudt 经常数缩放的尺度直接视为同一个估计量。本文没有给项目日内周期设定均值归一化。

### 3.4 Hecq–Laurent–Palm（2012）：标准差因子 \(f\) 的另一直接实例

*Common Intraday Periodicity*，Journal of Financial Econometrics 10(2), 325–353；[期刊 DOI](https://doi.org/10.1093/jjfinec/nbr012)。实际核对 [Maastricht RM/11/010 全文](https://cris.maastrichtuniversity.nl/ws/files/519390/guid-0fe69702-30ac-4e90-9d1e-45ce6aeb6774-ASSET1.0.pdf)，内文日期 2010-12-23，工作论文登记年 2011；见[本地原文](../papers/Hecq_Laurent_Palm_2012_common_periodicity_working_paper.pdf)。这不是 2012 期刊排版页码。

本轮核对印刷 pp.3–5（PDF pp.5–7），包括公式图像：式(1)–(2)为 \(r_{j,t,i}=\sigma_{j,t,i}u_{j,t,i}\)、\(\sigma=s f\)；p.4 Assumption 2、式(3)采用 \(M^{-1}\sum_i f_{j,t,i}^2=1\)，脚注3区分 Andersen–Bollerslev 的平均因子为1；p.5 式(10)将估计尺度除以其均方根，延续该归一化。

这里 \(f\) 是标准差乘法因子，\(u\) 是标准正态创新，与项目选择 Boudt 体系相容。由于共享作者和文献传统，这是一条额外直接证据，不能据此推断所有作者群都采用 \(f\)。

### 3.5 Laakkonen（2007 工作论文 / 2014 期刊）：\(s\) 与 \(f\) 的不同角色

核对 *Exchange Rate Volatility, Macro Announcements and the Choice of Intraday Seasonality Filtering Method*，Bank of Finland Research Discussion Papers 23/2007；[仓储 PDF](https://www.econstor.eu/bitstream/10419/212081/1/bof-rdp2007-023.pdf)、[央行登记页](https://publications.bof.fi/items/472c9877-c1df-4720-bc0b-e6b973152ec8)、[本地原文](../papers/Laakkonen_2014_filtering_methods_BoF_2007.pdf)。后续期刊标题使用 *macroeconomic announcements* 与 *periodicity filtering method*，发表于 Quantitative Finance 14(12), 2093–2104（2014）；[DOI](https://doi.org/10.1080/14697688.2012.739727)。本轮公式来自 2007 工作论文。

本轮核对印刷 pp.11–12（PDF pp.14–15）及图像：p.11 式(2.1)将估计值除以全样本平均，得到均值为1的 \(\tilde s\)，收益除以 \(\tilde s\)；该式 \(T\) 是全样本观测数，不是交易日数。p.12 式(3.1)为 \(R_{t,n}=E(R_{t,n})+\sigma_t N^{-1/2}s_{t,n}Z_{t,n}\)，\(s\) 是标准差周期因子。这里归一的是因子均值，不是平方因子均值。

该文式(3.2)–(3.3)另用 \(f\) 表示经过日尺度调整的对数平方收益变换。阅读时必须根据公式角色映射；不能见到字母 \(f\) 就按项目“标准差周期因子”解释。

### 3.6 Payne DP238：季节项进入对数方差

*Announcement Effects and Seasonality in the Intra-day Foreign Exchange Market*；[LSE 原文](https://www.fmg.ac.uk/sites/default/files/2020-09/dp238.pdf)、[本地原文](../papers/Payne_DP238_seasonality_revision_19970520.pdf)。外封为 March 1996，内文标题页为 May 20, 1997；本轮核对的是这份公开修订稿，不能称为逐式核对了最初 1996 版本。

本轮核对印刷 pp.11–13（PDF pp.13–15），式(1)、(3)、(5)、(7)、(8)及图像：\(r_t=\sigma\epsilon_t e^{h_t/2}\)，其中 \(\sigma\) 为尺度参数，\(h_t\) 进入对数方差；季节项 \(\psi_{mt}\) 加在对数平方收益方程中。相关模型页没有显式的“周期因子平均为1”约束。

由该加法项可推导相应标准差乘数 \(e^{\psi/2}\) 和方差乘数 \(e^\psi\)。这是为项目解释量纲所作的代数转换，不是原文直接把 \(\psi\) 定义为可除收益的乘法因子。

### 3.7 Andersen–Su–Todorov–Zhang（2024）：\(f\) 也可表示方差形状

*Intraday Periodic Volatility Curves*，JASA 119(546), 1181–1191；[DOI](https://doi.org/10.1080/01621459.2023.2177546)。核对 [2023-01-24 作者稿](https://www.kellogg.northwestern.edu/faculty/todorov/htm/papers/cal.pdf)，见[本地原文](../papers/Andersen_Su_Todorov_Zhang_2024_periodic_curves_author_20230124.pdf)。

本轮核对作者稿 p.7 式(3)及图像、p.10 式(4)–(5)：\(E[\sigma^2(t)]=g(t-\lfloor t\rfloor)\)，\(f(\kappa)=g(\kappa)/\int_0^1g(u)du\)。因此这里的 \(f\) 是平均瞬时方差的归一曲线，满足 \(\int_0^1f(\kappa)d\kappa=1\)，不是标准差因子。式(5)通过跨日平均局部方差估计再归一化估计该曲线。

它的量纲角色对应项目“方差形状”，但目标是平均方差曲线；不能无附加假设将其估计值与项目 WSD 的 \(\hat f^2\) 视为同一个量。这是“同字母、不同尺度”的直接反例。

### 3.8 Todorov–Zhang（2024）：\(\eta\) 的方差角色与累计比率

*Intraday Volatility Patterns from Short-Dated Options*，Journal of Econometrics；[DOI](https://doi.org/10.1016/j.jeconom.2024.105732)。核对 [2024-04-07 作者稿](https://www.kellogg.northwestern.edu/faculty/todorov/htm/papers/odp.pdf)，见[本地原文](../papers/Todorov_Zhang_2024_short_dated_options_author_20240407.pdf)。

本轮核对作者稿 pp.5–7，并核对 p.5 图像：式(1)的扩散项是 \(\sqrt{\eta_{t/\kappa-\lfloor t/\kappa\rfloor}}\,\sigma_t\,dW_t\)，因此 \(\eta\) 在方差尺度上。周期长度 \(\kappa\) 为相邻收盘之间一个交易日；式(2)实际是期权执行价网格，不能引用它作为波动分解公式。

p.7 式(8)及紧接正文说明，\(\hat\eta_\phi\) 估计 \(\int_\phi^1\eta_sds/\int_0^1\eta_sds\)，且 \(\eta\) 仅识别至乘法常数。不能把 \(\hat\eta_\phi\) 当作点态周期因子，也不能声称原文已强制 \(\eta\) 均值为1。其方差尺度、周期范围和期权观测来源都要与项目定义分开。

## 4. 对本项目符号选择的结论

1. **沿用有明确文献依据的常见角色。** 对数价格 \(p\)、收益 \(r\)、标准差尺度 \(\sigma\)、方差尺度 \(\sigma^2\)，以及 \(RV\)、\(BV\)、正态绝对矩 \(\mu_1\) 都有上述原文支持；不同论文也会使用 \(y^*,y,V,S,c\) 等记号。固定项目写法并保留映射，比宣称学界强制统一字母更准确。
2. **项目选择 Boudt/WSD 的 \(f\) 体系。** \(f_{t,i}\) 表示标准差周期，\(f_{t,i}^2\) 表示方差周期，采用 \(M^{-1}\sum_i f_{t,i}^2=1\)。这是有明确来源的选择；ASTZ 的方差 \(f\)、Laakkonen 的变换量 \(f\) 等仍按原文定义理解。
3. **标准化与滤波分开。** Boudt 的 \(\bar r\) 除以局部收益尺度，Dumitru 的 \(r^f\) 除以周期因子。项目 `daily_bpv_scale` 是 238 个同 session BPV 项均值的平方根，实际在分钟尺度上；它不能仅因变量名含 daily 就被写成日总波动率。
4. **保留项目自定义量的身份。** 混合 RV/BPV 分钟贡献 \(c^{\mathrm{mix}}\)、日内相对贡献 \(x^{\mathrm{mix}}\)、其中位数季节因子 \(\hat g^{\mathrm{mix}}\) 没有从本次文献核对得到公认的专属字母。它们应明确标注“项目定义”，并链接具体公式；不能仅换为希腊字母就称为学术通用符号。

以上是符号证据与映射结论，不提供当前混合贡献等于真实分钟方差、所有分位数应同时变平，或任一方法在 IM 上必然改善预测的保证。
