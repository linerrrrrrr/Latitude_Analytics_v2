# Dumitru、Hizmeri、Izzeldin（2025）：周期滤波如何进入波动预测

入口：[研究目录](../../README.md)。本文记录文献结论与本地方法的差异，不是已运行的模型结果。

*Forecasting the realized variance in the presence of intraday periodicity*，Journal of Banking & Finance 170，107342；[出版页](https://www.sciencedirect.com/science/article/pii/S0378426624002565)，DOI [10.1016/j.jbankfin.2024.107342](https://doi.org/10.1016/j.jbankfin.2024.107342)。2024-11-23 上线，2025 年刊期。用户给的 [Liverpool 仓储](https://livrepository.liverpool.ac.uk/3188055/) 在本次访问返回 406；已从 [作者网站](https://rodrigohizmeri.com/wp-content/uploads/2024/11/dhi_jbf.pdf) 取得 [17 页正式开放 PDF](../papers/Dumitru_Hizmeri_Izzeldin_2025_Forecasting_RV_periodicity_published.pdf)，首页注明 CC BY 4.0。

**阅读范围。** 阅读正文 §§0–5、表 1–10、Appendices A/B（PDF 页码与文内页码相同），核对方法页 3、HARP 页 4 和附录页 16 图像；未取得独立 Internet Appendix，文中提及的额外稳健性表不算已读。

**方法。** 页 3 式 (3)–(6) 把标准差周期 \(f_i\) 归一化为 \(M^{-1}\sum f_i^2=1\)，使用 Boudt 等人的 WSD 估计周期，再令 \(r^f_{t,i}=r_{t,i}/\hat f_i\)，重算 \(RV^f_t=\sum_i(r^f_{t,i})^2\)。页 4 式 (13) 的 HARP 保留原始 RV 为预测目标，解释变量换成滤波后的日、周、月 RV：

\[
RV_{t,t+h-1}=\beta_0+\beta_dRV^f_{t-1}
+\beta_w\overline{RV^f}_{t-5:t-1}
+\beta_m\overline{RV^f}_{t-22:t-1}+\varepsilon_{t,t+h-1}.
\]

这不是分钟时间轴重参数化。预测时只滤波解释变量，不把待预测目标一起改成 \(RV^f\)。

**证据与限定。** 简化 AR(1) 推导说明周期会增加 RV 测量变异；收益滤波能否抵消它还取决于周期估计误差。实证是 2000–2020 年 SPY 与 30 只美股，5 分钟采样；周期取前 20 个交易日，回归取 1000 日滚动窗口，预测 1/5/22 日。多数比较获益，但表 6 明确存在反例：SPY 一日 HARP-Q 的 QLIKE 损失比为 1.355，显著劣于 HAR-Q。不能将摘要概括为每种模型、市场及损失函数都改善。

| 进一步定位 | 页码 |
| --- | --- |
| WSD、收益滤波、BV 与零收益修正说明 | 3，式 (2)–(6) |
| HARP、HARP-J/CJ/Q 定义 | 4，式 (11)–(19) |
| MSE、QLIKE 与模拟样本外设置 | 6，式 (21) |
| 无前视窗口、样本外 DM/MCS | 6–7、9–12，表 6–7 |
| 比率估计误差推导；WSD 细节 | 15–16，Appendix A.2/B.1 |

**复现注意。** Appendix B.1 在页 16 将 ShortH 周期的归一化分母排成 \(M^{-1}\sum ShortH_j^2\)，没有平方根；图像确认并非文本提取丢符号。这与尺度归一化目标、正文 WSD 式 (5) 的平方根分母不一致。本次已回查 [Boudt 作者稿](01_boudt_2011.md) 印刷 10 页式 (2.10)，其分母确有平方根；本项目按该原始公式核对，而不照搬 DHI 此处排版。

**对当前项目的推论。** 按[项目符号](../../research/03_method_derivations.md)，当前 v2 的 $\widehat g_{t,i}^{\mathrm{mix}}$ 来自日内相对混合贡献的滞后滚动中位数，WSD 从日尺度标准化收益估计 $\widehat f_{t,i}$；不能直接宣称 $\widehat g_{t,i}^{\mathrm{mix}}=\widehat f_{t,i}^2$。旧方法从 $r/\sqrt{\widehat g^{\mathrm{mix}}}$ 重算 BPV 的除数为相邻两个 $\widehat g^{\mathrm{mix}}$ 乘积的平方根，不同于当前直接用 $c^{\mathrm{mix}}/\widehat g^{\mathrm{mix}}$。采用 HARP 时仍需验证中国期货 Session、首分钟及合约连续性口径，并在原始目标尺度上比较预测损失；不把曲线更平当作预测改善的替代证据。

本次已运行的 15 分钟预测与日级 HAR/HARP 改编见 [预测报告](../../results/forecasting/REPORT.md)。日级实验使用 252 日训练，已明确区别于论文的 1000 日设置。
