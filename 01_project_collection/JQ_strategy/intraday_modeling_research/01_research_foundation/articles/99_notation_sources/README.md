# 符号整理原文池

本目录集中保存尚未单篇精炼、但对学术符号定义和方法边界有用的原文。这里是证据池，不是最终符号规范；跨文献整理完成后，统一结论写入 `../../notation_and_measures.md`。

## 收录原则

- 只保存原始 PDF 和本索引，不为每篇论文预建空笔记。
- 同一 PDF 只在新项目中保存一份；已经进入前六篇核心目录的论文通过相对链接引用，不重复复制。
- 整理符号时记录“对象、尺度、归一化、索引范围、原文定位”五项，不根据单个字母猜测含义。
- 原文符号与项目符号分栏保存；项目统一写法不能反向改写论文原意。
- 工作论文、作者稿和正式版必须明确区分，排印问题保留版本定位。

## 本目录的 9 篇原文

| 原文 | 版本 | 主要符号任务 |
|---|---|---|
| [Andersen、Bollerslev、Diebold、Labys（2003）](andersen_bollerslev_diebold_labys_2003_published.pdf) | *Econometrica* 正式版 | 对数价格、收益、已实现方差/协方差及预测目标尺度 |
| [Barndorff-Nielsen、Shephard（2004）](barndorff_nielsen_shephard_2004_published.pdf) | *Journal of Financial Econometrics* 正式版 | 幂变差、双幂变差、正态绝对矩和连续/跳跃极限 |
| [Dette、Golosnoy、Kellermann（2023）](dette_golosnoy_kellermann_2023_published.pdf) | *Metrika* 正式开放版 | BV、RQ 的周期偏差倍数及二阶/四阶尺度区别 |
| [Hecq、Laurent、Palm（2012）](hecq_laurent_palm_2012_working_paper.pdf) | Maastricht 工作论文 | 多资产共同日内周期、资产与时点双索引、共同因子归一化 |
| [Laakkonen（2014）](laakkonen_2014_bof_working_paper.pdf) | 2007 年 Bank of Finland 工作论文版本 | 多种周期过滤法、标准差尺度与对数变换符号 |
| [Lee、Mykland（2008）](lee_mykland_2008_published.pdf) | *Review of Financial Studies* 正式版 | 局部尺度、跳跃统计量和正态绝对矩常数 |
| [Payne（1996）](payne_1996_discussion_paper_revision.pdf) | 1997-05-20 公开修订稿 | 公告效应、日内季节调整和回归分量符号 |
| [Andersen、Su、Todorov、Zhang（2024）](andersen_su_todorov_zhang_2024_author_manuscript.pdf) | 2023-01-24 作者稿 | 随时间变化的日内周期曲线、曲线对象与估计量索引 |
| [Todorov、Zhang（2024）](todorov_zhang_2024_author_manuscript.pdf) | 2024-04-07 作者稿 | 短期期权隐含的日内波动模式及风险中性量与实际测度量的区分 |

## 必须交叉引用的六篇核心论文

这些论文已在前序目录中保存并完成中文精炼，符号整理时直接引用，不复制 PDF：

| 核心论文 | 符号贡献 |
|---|---|
| [Boudt、Croux、Laurent（2011）](../01_boudt_croux_laurent_2011/boudt_croux_laurent_2011_zh.md) | $r$、$f$、局部尺度 $s$、创新 $u$、ShortH 与 WSD |
| [Andersen、Bollerslev（1997）](../02_andersen_bollerslev_1997/andersen_bollerslev_1997_zh.md) | 日内标准差周期、日级波动状态和 FFF 回归 |
| [Christensen、Hounyo、Podolskij（2018）](../03_christensen_hounyo_podolskij_2018/christensen_hounyo_podolskij_2018_zh.md) | 确定性日内形状与随机波动、可分离性检验 |
| [Andersen、Thyrsgaard、Todorov（2019）](../04_andersen_thyrsgaard_todorov_2019/andersen_thyrsgaard_todorov_2019_zh.md) | 时变周期函数、估计窗口和日内函数对象 |
| [Dette、Golosnoy、Kellermann（2022）](../05_dette_golosnoy_kellermann_2022/dette_golosnoy_kellermann_2022_zh.md) | 估计量专属周期偏差和日级标量修正 |
| [Dumitru、Hizmeri、Izzeldin（2025）](../06_dumitru_hizmeri_izzeldin_2025/dumitru_hizmeri_izzeldin_2025_zh.md) | $RV$、$BV$、$RQ$、过滤上标 $f$ 及 HAR/HARP 预测符号 |

## 后续整理顺序

1. 先固定基础对象：时间、资产、合约、session、价格和收益。
2. 再固定尺度：现货标准差/方差、积分方差、二次变差和四次变差。
3. 再整理日内周期：标准差因子、方差因子、归一化和时变索引。
4. 再整理测度：RV、BV、RQ、跳跃与连续成分。
5. 最后整理预测：目标、预测值、误差、期限、损失和风险溢价。

若同一字母在不同论文中指向不同对象，最终表格保留论文原义，并另给项目固定符号，不追求字母表面的统一。
