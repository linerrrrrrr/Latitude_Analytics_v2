# Huber 季节性退化条件：独立对照测试

本目录只放可独立执行的合成样本测试和方法说明。它不读取真实行情，不改动季节性估计实现、验收结果、provenance 或失败记录，也不提交任何云任务。

运行：

```bash
python test_huber_seasonality_diagnostics.py
```

脚本仅使用 Python 标准库。它按 `statsmodels.robust.norms.estimate_location` 的 Huber 加权迭代定义，复现实现中与其一致的固定 MAD、Huber 阈值、停止容差和迭代上限，并覆盖生产包装器对常量、零尺度和 `ValueError` 的状态处理。参考：[statsmodels `estimate_location` 源码](https://github.com/statsmodels/statsmodels/blob/main/statsmodels/robust/norms.py)；项目实现：[c01_IntradaySeasonality.ipynb](../../../a01_Methods/b04_ReturnVolatilityModeling/c01_IntradaySeasonality.ipynb)。

## 最小样本与复现结果

| 退化条件 | 合成有效样本 | 固定标准化 MAD | 当前状态 | 复现要点 |
| --- | --- | ---: | --- | --- |
| 非恒定但零 MAD | `[0, 0, 0, 0, 1]` | 0 | `zero_scale` | 中位数为 0，绝对偏差为 `[0,0,0,0,1]`，其中位数仍为 0；标准化不能产生正尺度。不是恒定序列。 |
| 30 次内未收敛 | `[0]×6 + [100]×2 + [1000]×7` | 148.260222 | `not_converged` | 固定尺度下，默认初始化与 Huber 更新在第 32 次才满足容差；`maxiter=30` 因而抛出收敛异常。存在解，失败来自迭代预算与收敛判据，不代表模型优化器失败。 |

在当前曲线流程里，上述状态都不给出位置估计，季节因子留缺失；后续有限观测检查因此阻止相应拟合。对照测试单独复现这一链路，不重新运行原实验。`[0]×6` 表示 0 重复 6 次。

## 统计及数值原因

当前尺度定义为标准化 MAD：

\[
 s = \frac{\operatorname{median}_i |x_i-\operatorname{median}_j(x_j)|}{0.6744897501960817}.
\]

在零 MAD 样本中，虽然存在一个 1，其余四个观测为 0，但偏差的中位数为 0，因此尺度没有定义为正数。固定尺度 Huber 位置方程为：

\[
 \sum_{i=1}^{n}\psi\!\left(\frac{x_i-\mu}{s}\right)=0,
 \qquad \psi(u)=\begin{cases}-1.345,&u<-1.345,\\u,&|u|\le1.345,\\1.345,&u>1.345.\end{cases}
\]

这里，\(x_i\) 是该分钟位置的有效历史日贡献，\(n\) 是有效样本数，\(\mu\) 是待估位置，\(s\) 是固定尺度，\(\psi\) 是 Huber 截断影响函数。尺度固定时，极端观测对位置方程的影响有界；但当 \(s=0\) 时标准化残差不可用。`c01_IntradaySeasonality` 在进入迭代前检测到此情形并返回 `zero_scale`，这是明确的失败诊断，不是数值溢出。

另一样本的尺度为正，但迭代 30 次未通过 `|μ_new−μ| < s × 10⁻⁶`。把上限增加到 32 次可通过同一停止条件。这说明固定点迭代可能在给定上限内太慢；增大上限只是一个诊断对照，不足以证明真实窗口的统计估计稳定，也不应自动用于已验收实验。

## 替代定义对照

脚本在两个有效样本上比较如下定义。这里所有输入值都是真实的合成观测；测试对非有限值直接报错，不做填充、不插值，也不静默删样本。

| 方法 | 定义变化 | 零 MAD 样本 | 迭代样本 | 主要取舍 |
| --- | --- | ---: | ---: | --- |
| 20% 对称截尾均值 | 每侧去掉 `floor(0.2n)` 个极端次序统计量，再对剩余观测取算术平均。 | 0.000000 | 466.666667 | 不需要尺度或迭代；约 20% 尾部污染抗性。估计对象改成中间样本的均值；小样本中 `floor(0.2n)=0` 时退化为普通均值。 |
| 20% 对称 Winsor 均值 | 将每侧 `floor(0.2n)` 个尾部数值截到保留区边界，再对全部观测取平均。 | 0.000000 | 480.000000 | 不丢弃观测，但尾部数值被边界替换；估计对象是截尾后分布的均值。大量并列值时可能与原均值相同。 |
| Huber M 位置 + 均值绝对偏差尺度 | 保留 Huber 影响函数，但用 `mean(|x−mean(x)|)` 作正尺度，并以均值初始化。 | 0.107600 | 480.000000 | 给零 MAD 提供尺度并避开本例 30 次失败；该尺度对极端值不稳健，本例第二组权重全部落在线性区，结果等于普通均值。适合作为对照，不建议无条件替代。 |
| 普通算术均值（参照） | 直接平均全部观测。 | 0.200000 | 480.000000 | 无迭代、无缺失；但对极端值不稳健，仅用作解释标尺。 |

截尾均值是首选的独立稳健基线：它无需尺度估计、不会因 MAD 为零而失效，也不靠补值制造完整曲线。它改变了原 Huber M 位置定义，不能视为等价修复。Winsor 均值保留样本数但改变尾部数值；Huber 更换尺度则保留 M 方程形式、改变每个残差的标准化强度，并可能削弱稳健性。

还可测试基于两两差值的 Qn/Sn 稳健尺度，再解 Huber 位置方程。它会改变固定尺度统计量，且仍需检查重复值导致的零尺度与迭代不收敛；这类尺度替换不能保证消除所有退化。对已有分钟贡献数据，需按每个分钟位置的有效样本数、零值比例和并列结构另做敏感性对照。

## 使用边界与下一步

这些结果只证明退化机制可被小样本重现，并展示几个定义不同的估计量在同一输入上的差别。尚不能据此决定生产季节曲线的替代方法。若继续比较，应在冻结数据副本上评估逐分钟位置的覆盖率、曲线均值归一化后的变化、日间稳定性和收益／波动率预测指标；单独写入新结果目录，保留原 2026-10-07 验收交付及失败记录。
#!/usr/bin/env python3
"""Minimal, dependency-free reproductions of seasonal Huber failure modes.

This script mirrors the relevant statsmodels estimate_location iteration and
c01_IntradaySeasonality wrapper decisions. Inputs are finite observed samples;
no missing observations are filled or silently dropped here.
"""
from __future__ import annotations

import math
from statistics import median

MAD_NORMALIZER = 1.482602218505602
HUBER_T = 1.345
HUBER_TOL = 1e-6
HUBER_MAXITER = 30


def require_finite(values:
