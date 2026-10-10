# Hydra + MLflow 教程：主力片段拼接后的分层组合研究

学习顺序：

**先做主力片段拼接 → 再列日内季节性方案 → 再列波动率模型 → 再列重采样方案 → 组合各方案 → 后续展开各方案内部参数。**

参考实现来自 [draft_experiments_v2](../draft_experiments_v2)。配置和练习代码为教学示例，需按各节说明准备后运行。

## 1. 先明确这次研究的组织方式

### 1.1 一个公共起点，三个主要方案维度

第一步生成并固定一份 IM 主力片段拼接结果。第一轮方案比较共同消费这一份结果。

随后依次回答：

1. 日内季节性有哪些方案？
2. 波动率模型有哪些方案？
3. 重采样有哪些方案？
4. 每一组“季节性 × 模型 × 重采样”产生什么结果？

可以把研究树画成：

~~~text
主力片段拼接版本 A
├── 不做季节性调整
│   ├── GARCH
│   │   ├── 原始一分钟网格
│   │   ├── 固定时钟采样
│   │   └── 业务时间采样
│   ├── FIGARCH
│   └── ARIMA / ARFIMA / 其他模型
├── RV 贡献季节性
│   └── 同样展开模型与重采样
├── 不跨交易日的 BPV 贡献季节性
│   └── 同样展开模型与重采样
├── 跨交易日的 BPV 贡献季节性
│   └── 同样展开模型与重采样
└── draft_v2 混合贡献季节性
    └── 同样展开模型与重采样
~~~

这里“RV / BPV”首先是季节性估计所用的贡献构造。预测模型可以另外使用 RV、BPV 或其他量作为目标。两者角色需要明确，但第一阶段无需把每个角色都升成独立搜索维度。

例如：

~~~text
季节性方案：按不跨日 BPV 贡献估计
波动率模型：GARCH
重采样方案：固定时钟
评价目标：固定定义的未来区间 RV
~~~

这一组合完全可以成立。季节性估计使用 BPV，并不意味着模型的输入必须是日级 BPV，也不意味着评价目标必须改成 BPV。

### 1.2 方案组合与参数组合分两阶段

第一阶段比较“方法选谁”：

~~~text
seasonality = rv_profile / bpv_within_day / bpv_cross_day / mix_v2 / none
model       = garch / figarch / seasonal_garch / seasonal_figarch / arima / arfima
resampling  = identity_1m / clock_fixed / volume_clock / seasonal_clock
~~~

这些是候选菜单。当前参考项目只实现其中一部分；配置名称不代表模型或采样代码已经存在。

第二阶段才比较“选中方法怎样设参数”：

~~~text
seasonality.lookback_trading_days = 30 / 60 / 120 / 360
model.p、model.q、model.distribution
model.training_window_trading_days
resampling.interval_minutes
resampling.volume_threshold
~~~

第一阶段每个方案保留一套明确的基准参数，例如混合贡献季节性暂用 120 个交易日。120 是教学基准，不是通过比较得出的最佳窗口。

5 分钟与 15 分钟属于同一个“固定时钟采样”方案的不同参数。第一阶段用一套固定间隔代表该方案；第二阶段再扫间隔。这样配置目录与研究思路保持一致。

### 1.3 组合树的顺序与计算依赖

配置按“季节性 → 模型 → 重采样”依次选择。程序计算时，仍需按所选模型的输入依赖安排。

常见情况：

~~~text
主力分钟片段
→ 一分钟收益与历史季节因子
→ 按所选重采样规则构造模型输入
→ 拟合所选模型
→ 预测并评价
~~~

若未来采用“模型预测方差驱动的业务时间”，则会增加：

~~~text
历史数据拟合驱动模型 → 生成因果采样时钟 → 下游模型拟合与评价
~~~

这时要区分驱动模型和评价模型，并把它封装成一个明确定义的重采样方案。不能让模型依赖采样结果、采样又依赖同一个尚未拟合模型，形成循环。

第一轮默认季节因子在公共一分钟网格上估计，然后进入下游采样。这与参考项目的阶段划分一致。“先采样再重新估季节性”将来可以作为另一项实验展开。

## 2. 从 draft_experiments_v2 读出的实际流程

### 2.1 第一个 Notebook：主力片段拼接

[01_01_im_main_continuous.ipynb](../draft_experiments_v2/01_01_im_main_continuous.ipynb) 当前执行逻辑是：

1. 从项目 DuckDB 只读 IM 日线。
2. 按正式合约日历确定合约存续期与交易日。
3. 使用前一交易日作为信号日，为当前交易日选主力。
4. 初次按成交量降序、持仓量降序、到期日升序、合约代码升序选择。
5. 后续只允许向更晚到期合约切换。
6. 后月成交量严格超过当前合约的 1.10 倍时触发切换。
7. 当前合约到期或零成交量时强制寻找可用后月。
8. 应有候选缺日线时直接报错。
9. 按每日主力映射选择一分钟行情。
10. 保存原始行情、换月信息及双端点 log 复权列。

输出是：

~~~text
im_main_continuous_1m.parquet
~~~

**片段拼接与价格复权分别记录。** 拼接回答“这个交易日用哪个实际合约”；复权回答“怎样调整各片段价格的水平”。

Notebook 中的双端点复权使用整段快照：

~~~text
a_i = -(G_i - G_0) + w_i × (G_last - G_0)
~~~

其中 G 是累计换月 log 价差，w 从首分钟的 0 递增到末分钟的 1。代码明确保存 price_adjustment_available_at，且 metadata 标为 retrospective。

因此历史预测实验需要知道实际消费哪一列价格。该复权包含全段信息，不能仅凭主力信号滞后一天，就认定复权收益也具有逐时点可用性。

### 2.2 第二个 Notebook 实际用的是原始片段价格

[02_01_im_bpv_intraday_seasonality.ipynb](../draft_experiments_v2/02_01_im_bpv_intraday_seasonality.ipynb) 读取拼接表中的：

~~~text
contract_code
trading_date
session_number
bar_at
open
close
~~~

它使用原始 open/close，没有消费双端点复权价格列。

收益在“交易日＋合约＋Session”内计算：

- 09:31、13:01：log(close / open)；
- 其余分钟：同 Session 内连续一分钟 close 的 log 差；
- 盘中时间缺口：保留空值；
- 隔夜与午休价格变化：不并入一分钟收益。

### 2.3 当前季节性是混合贡献方案

令 r(t,i) 是分钟收益。参考 Notebook 构造：

~~~text
Session 首分钟：c(t,i) = r(t,i)^2
其余有效同 Session 相邻收益：
c(t,i) = (π/2) × |r(t,i)| × |r(t,i-1)|
~~~

它是“首分钟 RV 项＋其余分钟 BPV 项”的混合贡献。每日求和不直接称为标准 BPV。

季节因子估计顺序：

~~~text
每个历史日的贡献除以该日平均贡献
→ 对齐完整交易日序列
→ shift(1)
→ 最近 L 个交易日逐分钟取中位数
→ 将历史中位数曲线归一到分钟均值为 1
~~~

当前代码同时计算 L = 30、60、120、360，且要求窗口内 L 个历史日全部合格。缺失日不通过向前额外寻找历史来补足。

输出的 variance_seasonality 是混合贡献季节因子。参考文档已明确：不能由定义直接认定它等于收益方差周期 f²。

贡献除以 g，与收益除以 sqrt(g) 后重新计算 BPV，也是两个不同操作：

~~~text
直接调整 BPV 项：b_i / g_i
先调整收益后重算 BPV：b_i / sqrt(g_i × g_(i-1))
~~~

第一轮将现有方法命名为 mix_v2，保留这套完整含义。

### 2.4 第三个 Notebook 已经展示了第二阶段的雏形

[02_02_im_bpv_window_comparison.ipynb](../draft_experiments_v2/02_02_im_bpv_window_comparison.ipynb) 比较 30、60、120、360 日窗口，并使用共同有效交易日。

它研究的是同一个季节性方案内部的窗口参数。新教程把这种比较放到“先完成方案组合之后”的参数阶段。

其中对目标日全日贡献归一化的残余形状是事后诊断。它可以作为 MLflow artifact，不能作为目标日盘中预测时已知特征。

## 3. Hydra 如何表达这套层次

Hydra 的一个配置组对应一个环节，一个 YAML option 对应该环节的一种方案。

第一版只设四个主要组：

~~~text
main_series
seasonality
model
resampling
~~~

数据路径、预测任务、评价规则与时间切分先固定在主配置中。以后确有独立比较需要时，再把它们拆成组。

Hydra 的 defaults list 决定加载哪些 option；组名覆盖替换方案，点号覆盖修改方案内部参数。见 [Defaults List 官方说明](https://hydra.cc/docs/1.3/advanced/defaults_list/)。

### 3.1 项目位置与练习文件

练习在本项目目录内进行。以下为尚未创建的配置预览示例文件：

~~~text
E:/Latitude_Analytics_v2/R01_project_collection/JQ_strategy/intraday_modeling_research/
├── inspect_combination.py
├── conf/
│   ├── config.yaml
│   ├── main_series/
│   │   └── im_segments_v2.yaml
│   ├── seasonality/
│   │   ├── none.yaml
│   │   ├── mix_v2.yaml
│   │   ├── rv_profile.yaml
│   │   ├── bpv_within_day.yaml
│   │   └── bpv_cross_day.yaml
│   ├── model/
│   │   ├── garch.yaml
│   │   ├── figarch.yaml
│   │   ├── seasonal_garch.yaml
│   │   ├── seasonal_figarch.yaml
│   │   ├── arima.yaml
│   │   └── arfima.yaml
│   ├── resampling/
│   │   ├── identity_1m.yaml
│   │   ├── clock_fixed.yaml
│   │   ├── volume_clock.yaml
│   │   └── seasonal_clock.yaml
│   └── experiment/
│       └── mix_garch_clock.yaml
└── learning_outputs/
~~~

主力拼接使用 [01_01_im_main_continuous.ipynb](01_01_im_main_continuous.ipynb)；`inspect_combination.py` 演示命令行配置组合。

### 3.2 主配置：只选择方案，不展开窗口组合

conf/config.yaml：

~~~yaml
defaults:
  - main_series: im_segments_v2
  - seasonality: mix_v2
  - model: garch
  - resampling: identity_1m
  - _self_

stage: scheme_comparison
experiment_recipe: manual
batch_label: first_scheme_matrix

paths:
  strategy_dir: E:/Latitude_Analytics_v2/R01_project_collection/JQ_strategy
  learning_outputs: ${paths.strategy_dir}/intraday_modeling_research/learning_outputs

tracking:
  uri: http://127.0.0.1:5000
  experiment_name: intraday_scheme_learning

evaluation:
  protocol: fixed_common_target_v1
  evaluate_test: false

hydra:
  job:
    chdir: false
  run:
    dir: ${paths.learning_outputs}/single/${now:%Y-%m-%d}/${now:%H-%M-%S_%f}
  sweep:
    dir: ${paths.learning_outputs}/sweep/${now:%Y-%m-%d}/${now:%H-%M-%S_%f}
    subdir: ${hydra.job.num}
~~~

这里 evaluation.protocol 是待后续建模课具体实现的协议名称，配置检查练习不会执行评价。进入训练前必须补齐目标、预测原点、horizon、切分日期和共同评价键，不能仅凭该名称宣布结果可比较。

输出由 `paths.learning_outputs` 显式定位到本项目目录，不随终端当前目录改变。运行命令前先进入上述 `intraday_modeling_research` 目录以定位示例脚本；使用数字 job 子目录可避免 Windows 路径因参数名过长或非法字符出错。

### 3.3 公共主力片段配置

conf/main_series/im_segments_v2.yaml：

~~~yaml
name: im_segments_v2
underlying_code: IM
source_path: ${paths.strategy_dir}/draft_experiments_v2/im_main_continuous_1m.parquet
selection_rule: previous_trading_day_volume
roll_volume_ratio: 1.10
roll_direction: later_expiry_only
tie_break: [volume_desc, open_interest_desc, expiry_asc, contract_code_asc]
price_columns: raw_open_close
return_boundary: within_contract_trading_day_session
session_first_return: log_close_over_open
missing_interval: keep_missing
~~~

此配置使用 `draft_experiments_v2/im_main_continuous_1m.parquet` 作为现有参考输入。本项目 Notebook 的输出位置是 `02_data/derived/im_main_continuous_1m.parquet`，目前尚未生成；生成后可将 `source_path` 指向该文件。第一阶段固定同一份输入，不 sweep main_series，也不重复重建拼接结果。

配置参数应与实际拼接规则及输入 manifest 一致。修改 roll_volume_ratio=1.20 不会改变已保存的 Parquet，需使用对应规则生成的产物。

## 4. 第一环节：先完成主力片段拼接与身份记录

### 4.1 学习目标

先理解参考项目的每日主力映射，再把这个阶段的配置与产物记录到 MLflow。

建议记录一个独立 main_series run：

~~~text
stage = main_series
main_series.name = im_segments_v2
roll_volume_ratio = 1.10
signal_lag = one_trading_day
price_columns_used_downstream = raw_open_close
~~~

产物包括：

~~~text
main_mapping.parquet
roll_mapping.parquet
main_series_manifest.json
~~~

这些是实验记录的建议产物，现有 Notebook 仅输出拼接 Parquet。

manifest 记录：

- 拼接文件绝对路径、字节数与 SHA-256；
- 源数据库读取范围及相关来源批次；
- 主力规则、代码版本、信号日期与执行日期；
- 首尾交易日、分钟数、换月次数；
- 包含哪些原始/复权价格列；
- 下游实际选用的价格列。

### 4.2 用一次构建支撑后续所有组合

后续每个组合 run 记录：

~~~text
main_series_id
main_series_source_sha256
main_series_run_id
~~~

同一份输入共享同一 main_series_id。换季节性、换模型、换重采样都不会自动改变主力拼接结果。

这能帮助你判断：结果差异来自研究方案，还是来自输入数据或拼接规则变化。

### 4.3 动手检查

打开本项目的 [主力拼接 Notebook](01_01_im_main_continuous.ipynb)，沿着以下对象读：

~~~text
daily_df
→ contract_df / trading_dates
→ main_mapping_df
→ roll_gap_df
→ minute_df
→ im_main_continuous_1m.parquet
~~~

练习回答：

1. 为什么第一天只提供信号，第二天才产生主力映射？
2. 后月成交量恰好等于 1.10 倍时，是否触发切换？答案：不触发。
3. 换月价差参考日期是否晚于信号日？答案：不晚于。
4. 季节性 Notebook 实际读取哪个 close？答案：原始 close。
5. 为什么双端点复权列带有可用时间？答案：它依赖全段快照。

先完成这一环节，再开始下游组合。

## 5. 第二环节：建立日内季节性方案菜单

### 5.1 把每种方案作为一个完整方法包

| 方案 | 贡献或输入 | 估计方式 | 当前状态 |
|---|---|---|---|
| none | 不估计季节曲线 | 单位因子 | 可作为基准 |
| mix_v2 | Session 首分钟 RV＋其余同 Session BPV | 历史归一化形状中位数 | 参考 Notebook 已有 |
| rv_profile | 所有分钟平方收益 | 历史归一化形状汇总 | 待实现 |
| bpv_within_day | 不跨交易日的相邻收益乘积 | 历史形状汇总 | 待明确午间与首项规则后实现 |
| bpv_cross_day | 允许跨交易日相邻配对 | 历史形状汇总 | 待明确边界与换月规则后实现 |

这些名称是配置建议。两个 BPV 候选在实现前需确定完整统计定义，包括是否纳入隔夜价格跳变。

return_policy、bpv_pairing 等放入对应方案的 YAML，第一轮只比较方案名。

### 5.2 现有方案配置

conf/seasonality/mix_v2.yaml：

~~~yaml
name: mix_v2
contribution: rv_at_session_open_bpv_elsewhere
pairing_scope: contract_trading_day_session
history_grid: complete_trading_calendar
history_lag_trading_days: 1
lookback_trading_days: 120
min_periods: 120
daily_normalization: divide_by_daily_mean
historical_aggregation: median
curve_normalization: mean_one
missing_history: invalidate_window
output_semantics: mixed_contribution_factor
return_adjustment: divide_by_sqrt_contribution_factor
~~~

conf/seasonality/none.yaml：

~~~yaml
name: none
output_semantics: identity
return_adjustment: identity
~~~

none 没有回望窗口；不要给它增加一个不起作用的 120 日参数，以免产生重复实验。

### 5.3 RV 方案配置

conf/seasonality/rv_profile.yaml：

~~~yaml
name: rv_profile
contribution: squared_return
history_grid: complete_trading_calendar
history_lag_trading_days: 1
lookback_trading_days: 120
min_periods: 120
daily_normalization: divide_by_daily_mean
historical_aggregation: median
curve_normalization: mean_one
missing_history: invalidate_window
output_semantics: squared_return_contribution_factor
return_adjustment: divide_by_sqrt_contribution_factor
~~~

第一轮只改变贡献构造，保持历史汇总方式和窗口相同，使解释更清楚。以后再展开 median、mean、稳健估计等内部选项。

### 5.4 跨日 BPV 的边界写在方案内

需要记录至少三个选择：

~~~yaml
# 放在对应 BPV 方案内，逐项实现前确定
return_boundary: ???
pairing_boundary: ???
roll_boundary: ???
~~~

必须分清：

- 收益跨日：是否引用上一交易日价格；
- 配对跨日：是否把前一日最后收益与本日首收益相乘；
- 换月跨界：若实际合约变了，怎样处理两侧价格与收益。

“相邻乘积跨日”不自动意味着“将隔夜涨跌并入首分钟收益”。

主力片段在换月时来自不同实际合约；直接将原始新旧 close 相除包含换月价差。需要明确使用同合约参考价、因果价差调整，或在该边界断开。参考项目的 Session 内收益自然断开这些边界；新方案应单独定义。

### 5.5 季节性环节的完成标准

对每种方案保存：

~~~text
seasonality_scheme
main_series_id
lookback_trading_days（适用时）
target_trading_date
history_end_trading_date
factor_by_minute
factor_semantics
valid_history_count
~~~

第一轮固定一个窗口，先看方案输出是否符合定义。窗口比较进入第二阶段。

## 6. 第三环节：建立波动率模型菜单

### 6.1 先选模型家族，每个家族固定一套基准参数

| 模型方案 | 输入对象 | 需明确的输出 |
|---|---|---|
| GARCH | 原始或季节调整收益 | 条件方差预测 |
| FIGARCH | 原始或季节调整收益 | 长记忆条件方差预测 |
| seasonal-GARCH | 收益与季节信息 | 带明确周期结构的方差预测 |
| seasonal-FIGARCH | 收益与季节信息 | 带明确周期结构的长记忆方差预测 |
| ARIMA | 明确定义的波动测度序列，例如 log RV | 测度水平预测 |
| ARFIMA | 明确定义的长记忆测度序列 | 测度水平预测 |
| HAR（可选） | 日/周/月聚合测度 | 下一期测度预测 |

这一菜单容纳你提出的模型，第一轮不会把任务预先锁定为 HAR。

同一季节性方案可以供多个模型消费，但消费方式有所不同。对 GARCH 可以构造调整收益；对 ARIMA 可以先构造调整域波动测度序列。该转换必须写清楚。

### 6.2 GARCH 配置

conf/model/garch.yaml：

~~~yaml
name: garch
family: return_conditional_variance
p: 1
q: 1
mean: zero
distribution: normal
return_scale: 100.0
seasonality_use: adjusted_returns
~~~

conf/model/figarch.yaml：

~~~yaml
name: figarch
family: return_conditional_variance
p: 1
q: 1
truncation: 1000
mean: zero
distribution: normal
return_scale: 100.0
seasonality_use: adjusted_returns
~~~

这里阶数、分布、缩放、截断都是教学基准值。第一轮更换模型名，暂不搜索这些值。

模型乘 100 后得到的方差要除以 100² 映射回原收益尺度。若还使用季节因子，映射回原尺度时还需处理相应季节尺度，不能只换输出列名。

### 6.3 seasonal 模型如何接上季节性菜单

seasonal-GARCH/FIGARCH 可以消费上游选定的季节性信息；具体方程需要在实现环节冻结。

例如有两种不同路径：

~~~text
mix_v2 → r / sqrt(g) → 普通 GARCH
~~~

~~~text
mix_v2 → 将 g 作为给定周期曲线 → 显式乘法周期 GARCH 方程
~~~

第一条已经定义了一个两阶段构造。第二条要进一步定义方差方程及 g 在其中的统计角色。当前混合贡献 g 不能自动当作理论收益方差周期。

如果以后选择模型内部联合估计季节因子，则上游季节性选择应是 internal，而不再同时对同一个外部曲线进行重复调整。这样的组合依赖在模型课展开。

### 6.4 ARIMA / ARFIMA 的位置

ARIMA 是所选测度序列的动态模型。配置中记录 input_measure 和 transform，例如：

~~~yaml
name: arima
family: measure_series
input_measure: rv
transform: log
order: [1, 0, 1]
seasonality_use: measure_from_adjusted_returns
~~~

ARFIMA 需要明确分数参数估计、截断、初始化和预测反变换。当前环境里有 arch 和 statsmodels，尚未选定 ARFIMA 的实现。可先列入计划表，状态标为“待实现”，不在组合预览中假报训练完成。

模型的可用方法仍应按实际安装版本核对：[arch 波动过程](https://arch.readthedocs.io/en/stable/univariate/volatility.html)、[statsmodels ARIMA](https://www.statsmodels.org/stable/generated/statsmodels.tsa.arima.model.ARIMA.html)。

## 7. 第四环节：建立重采样方案菜单

### 7.1 第一轮比较采样方式

| 方案 | 思路 | 第一轮固定内容 |
|---|---|---|
| identity_1m | 保留一分钟网格 | 公共基准 |
| clock_fixed | 按固定时钟间隔取样 | 例如先固定 5 分钟 |
| volume_clock | 累计成交量达到阈值形成观测 | 固定阈值校准规则 |
| seasonal_clock | 按历史季节曲线形成业务时间 | 固定历史估计与边界规则 |

volume_clock、seasonal_clock 是为扩展菜单提供的例子，参考目录当前没有这些实现。之后可以按你的实际研究方案替换或增加。

### 7.2 可用于配置课的两个 option

conf/resampling/identity_1m.yaml：

~~~yaml
name: identity_1m
kind: identity
base_frequency: 1min
~~~

conf/resampling/clock_fixed.yaml：

~~~yaml
name: clock_fixed
kind: fixed_clock
interval_minutes: 5
anchor: daily_open
history_direction: backward
intraday_direction: forward
daily_sample_count: variable
minute_allocation: uniform_fraction
price_interpolation: linear_log_price
proxy_aggregation: not_applicable
closed: right
label: right
incomplete_bin: reject
~~~

第一轮扫描 identity_1m 与 clock_fixed。上面默认值 not_applicable 对应后文以收益为输入的 GARCH/FIGARCH 配置课，不增加重复训练。使用波动率代理作为模型目标时，必须显式选择 proxy_aggregation=sum 或 mean，并将二者同时保留为比较方案；第二轮才把 clock_fixed.interval_minutes 展开成 5、10、15 等。开盘锚定、分钟内均匀比例分配和对数价格插值是固定设计，不参与方案扫描。以后新增 volume_clock、seasonal_clock 等重采样配置时，同样显式包含这些固定字段。

### 7.3 每日开盘锚定，向后构造历史、向前构造当日序列

以下规则适用于所有实际执行重采样的方案。

设交易日 d 的开盘时刻为 a_d。每次日频重新拟合模型时，执行：

1. 以 a_d 为唯一的当日采样锚点，使用开盘前可用的信息估计并冻结当天采样参数。
2. 从 a_d 向后回望采样，得到历史采样点；完成定位后按时间升序排列，用于当天重新拟合。不能直接沿用昨天锚点生成的历史网格。
3. 从同一个 a_d 向前采样，得到当天陆续实现的后半段序列；历史与当日共用当天冻结的采样规则。
4. 拼接时锚点只保留一次；它是价格参考点，不是一个零收益或零方差的人工观测。首个历史点没有前一区间时，不伪造其收益。
5. 每个交易日单独生成这一套序列，保留 anchor_date/anchor_at 身份。相同历史日期在不同锚点下可能有不同采样边界，不得把不同日的历史窗口拼成一条重复训练序列。

~~~text
历史起点 … 历史采样点 → 当日开盘锚点 → 当日采样点 … 当日最后完整区间
          开盘重新拟合所用数据          当日逐步实现的预测目标/评估数据
~~~

“作为当日模型的完整研究序列”不表示开盘拟合能使用当天尚未实现的后半段。开盘只拟合历史部分，后半段用于当日预测与事后评价；本节不额外引入日内重新拟合。

每天日内采样点数 N_d 可以不同，少数点或较多点都是正常结果。不强制等长、不补零、不为凑齐数量修改当天阈值；若历史有效样本不足则显式记录无法拟合。中午休市是交易时间的间断，不是第二个日频重新拟合锚点。交易时间不包含休市，收益、跨日贡献与换约边界仍遵循各自已选规则，不能靠插值自动抹平。

配置示例的 incomplete_bin=reject 仅保留完整采样区间，未完成的收盘尾部和历史左端残余需记录未覆盖量；不能把丢弃尾部后的区间总和称为全天总量。

### 7.4 分钟内均匀比例分配

用 c_i 表示原始分钟 i 的方差贡献代理。采样区间 k 在该分钟覆盖的起止比例分别为 u、v，则：

\[
w_{k,i}=v-u,\qquad c_{k,i}=w_{k,i}c_i.
\]

边界落在分钟的 2/3 处，前段取得 2c_i/3，后段取得 c_i/3。同一分钟内出现多个边界时，分别按各段比例分配。只采用这一种分钟内均匀分配规则，不增加其他分配方法或参数组合。

这是一项固定的分钟内路径近似，不是声称观测到了真实分钟内波动路径。RV 贡献、日内配对 BPV 贡献、跨日配对 BPV 贡献及混合贡献，均先按各方案定义得到 c_i，再统一按该规则分配；不借重采样改变原有 BPV 配对与归属定义。标准差不是可直接按此规则相加的方差贡献。

覆盖完整分钟的所有区间比例合计为 1，分配后贡献总量守恒。未覆盖的头尾残余单独记录；缺失分钟不能视为零贡献。

### 7.5 累计与平均两种方案

对于重采样区间 k，保存：

\[
V_k=\sum_i w_{k,i}c_i,\qquad
D_k=\sum_i w_{k,i},\qquad
q_k=V_k/D_k.
\]

这里原始频率为一分钟，D_k 的单位是有效交易分钟，不含午休或隔夜自然时间。

| 配置 | 模型目标 | 经济含义 |
|---|---|---|
| proxy_aggregation: sum | V_k | 该采样区间累计经历的方差贡献，即风险总量 |
| proxy_aggregation: mean | q_k | 该采样区间平均每交易分钟的方差贡献，即风险积累强度 |

mean 的分母是覆盖比例之和 D_k，不是“碰到过几个原始分钟”的整数计数。例如覆盖 2/3 分钟、1 分钟和 1/3 分钟，分母为 2，而不是 3。两种方案使用相同采样边界，均保存 V_k、D_k 和 q_k，只改变波动率代理模型采用的目标，不重复估计时钟。

累计方案不因区间长短不一而失去意义；平均方案也不自动优于累计方案。如果以 c_i 的累计值达到阈值 H 来决定边界，完整区间的 V_k 会被构造为 H，此时应记录目标近乎常数的结构，不能把它解释为模型预测能力强。

### 7.6 分钟内对数价格路径的比例插值

插值公式参考 [v1 时钟 Notebook](../draft_experiments_v1/03_01_im_clock_resampling.ipynb)，分钟收益边界沿用 [v2 季节性入口](../draft_experiments_v2/02_01_im_bpv_intraday_seasonality.ipynb)。v2 尚无独立的重采样入口。

设一个有效原始分钟的左、右端价格为 P_L、P_R，采样边界位于从左往右的比例 f 处。均匀分配的是该分钟的对数价格增量：

\[
\ell(f)=\log P_L+f(\log P_R-\log P_L),\qquad
P(f)=\exp(\ell(f)).
\]

例如边界在 2/3 处，就取该分钟对数价格增量的 2/3，再加回左端对数价格；不是把整个价格 P_R 乘以 2/3。100 到 121 的分钟在中点得到价格 110。

结合 v2 的当前收益口径，同一时段内连续有效分钟使用上一分钟 close 与当前 close 作为端点，时段首分钟使用本分钟 open 与 close。缺失分钟、隔夜和换约不自动当成一条可均匀插值的连续一分钟路径；跨日 BPV 配对是独立贡献定义，不意味着构造了隔夜分钟价格。

回望定位得到的是从右往左的比例时，先换成从左往右的 f，再使用同一个公式；不能因为采样方向不同而改变同一边界的价格。

在采样边界 b_k 取得价格后，直接形成：

\[
\text{log\_price}_k=\ell(b_k),\qquad
\text{log\_return}_k=\ell(b_k)-\ell(b_{k-1}).
\]

收益只对所选收益边界规则允许连接的相邻点计算。分钟内部的插值点依赖该分钟右端观测，因此 available_at 至少是所需分钟结束时刻，而非插值后的 sample_at；开盘回望必须依据 available_at 过滤。

### 7.7 模型输入分流：价格/收益模型不强制改用波动率代理

| 模型实际拟合对象 | 直接使用的重采样序列 | sum/mean 是否展开 |
|---|---|---|
| log price | 边界插值得到的 log_price | 否 |
| log return | 相邻有效采样边界的 log_return | 否 |
| 波动率代理 | 同边界的 V_k 或 q_k，及模型明确需要的变换 | 是 |

例如对收益拟合的 GARCH/FIGARCH 继续输入重采样 log_return；对 log price 拟合的模型继续输入重采样 log_price。不能因为教程讨论 RV/BPV，就把这些模型的输入替换成累计或平均代理。季节模型若需要过滤收益，在明确的季节处理环节完成，并记录所用尺度；不由 proxy_aggregation 暗中改变价格或收益。

价格/收益模型的配置可将 proxy_aggregation 设为 not_applicable，避免生成两次相同训练。若仅想用累计和平均代理分别评价同一个价格/收益模型，可复用一次拟合结果，分别记录评价尺度，而不是宣称有两次不同拟合。

两类计算必须保持区分：

\[
(\text{重采样区间收益})^2\ne\sum_i w_{k,i}c_i
\]

尤其当一分钟 log return 按 f 分配时，分段收益平方是 f^2 r_i^2；原始 RV 贡献按本节规则分配则是 f r_i^2。二者不矛盾：前者来自插值价格路径，后者是分钟风险贡献的分配，不能互相替换。

#### 动手练习：只对代理目标展开 sum/mean

在支持波动率代理输入的模型配置中，明确填写实际 target（例如 sampled_variance_proxy），并使用 clock_fixed 的以下两种覆盖：

~~~text
resampling=clock_fixed resampling.proxy_aggregation=sum
resampling=clock_fixed resampling.proxy_aggregation=mean
~~~

实际业务接入后，两次运行的采样边界、价格序列和 D_k 应完全相同，目标满足 V_k=q_k*D_k。拟合 log_price/log_return 的组合不执行这个二选一扫描。后续扩展其他重采样方式时重复这一条件展开，不做无效的全量笛卡尔积。

每条区间记录至少保留 anchor_at、sample_at、previous_sample_at、available_at、trading_duration_minutes、variance_sum、variance_mean、log_price、log_return，以及来源分钟索引和覆盖比例。MLflow 保存当天采样参数、输入类型、代理聚合方式、历史有效点数与日内点数；只记录与实际模型输入相符的方案身份。

### 7.8 业务时间下保持评价目标一致

按成交量或季节时钟采样后，每个事件持续的物理时间可能不同。

比较模型时，固定同一评价区间，例如“下一段 15 个物理分钟”，并将所有预测映射到该区间。若比较“下一事件”，就要承认不同采样方案的目标不同，分组报告。

评价目标的粒度在建模课具体冻结；第一轮配置学习只建立组合身份。

## 8. 第一轮组合：只组合方案名

假设第一轮纳入：

~~~text
季节性：none、mix_v2、rv_profile                  共 3 种
模型：garch、figarch                            共 2 种
重采样：identity_1m、clock_fixed                  共 2 种
主力片段：im_segments_v2                         固定 1 份
~~~

理论组合数：

~~~text
1 × 3 × 2 × 2 = 12
~~~

此时每种季节性固定 120 日窗口，每个模型固定一套参数，clock_fixed 固定 5 分钟。

组合表应包含：

~~~text
main_series_id | seasonality | model | resampling | implementation_status
~~~

例如：

~~~text
A | none       | garch   | identity_1m | 待执行
A | none       | garch   | clock_fixed | 待执行
A | mix_v2     | figarch | identity_1m | 待执行
A | rv_profile | figarch | clock_fixed | 待实现
~~~

理论组合数、已实现数、已运行数、成功数分别统计。

你列出的全菜单可形成更多组合，但不能把未实现 ARFIMA、seasonal 模型或业务时钟计入成功运行。

## 9. 动手课一：完整的 Hydra 组合预览

这一课可以在依赖与配置文件准备好后直接运行。它展示真实组合配置和已有拼接输入身份，不调用尚未实现的模型。

### 9.1 环境准备

项目标准解释器：

~~~powershell
$latitudePython = 'E:\anaconda3\envs\latitude_env_v2\python.exe'
& $latitudePython -c "import sys; print(sys.executable)"
~~~

依赖安装命令：

~~~powershell
& $latitudePython -m pip install "hydra-core>=1.3,<1.4" "mlflow>=3,<4"
~~~

安装后记录实际版本；上述版本范围不是复现锁文件。

### 9.2 完整预览脚本

将前文 config.yaml，以及 main_series、none/mix_v2/rv_profile、garch/figarch、identity_1m/clock_fixed 的 YAML 保存到建议的练习目录。

inspect_combination.py：

~~~python
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import hydra
from hydra.core.hydra_config import HydraConfig
from omegaconf import DictConfig, OmegaConf


@hydra.main(version_base="1.3", config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:
    OmegaConf.to_container(cfg, resolve=True, throw_on_missing=True)

    main_series_path = Path(cfg.main_series.source_path)
    if not main_series_path.is_file():
        raise FileNotFoundError(main_series_path)

    resolved_config = OmegaConf.to_container(cfg, resolve=True)
    resolved_config_text = OmegaConf.to_yaml(cfg, resolve=True)

    # 当前课记录文件身份；正式批量课在公共准备阶段计算一次再复用。
    main_series_digest = hashlib.sha256()
    with main_series_path.open("rb") as main_series_file:
        for chunk in iter(lambda: main_series_file.read(1024 * 1024), b""):
            main_series_digest.update(chunk)

    method_config = {
        "main_series": resolved_config["main_series"],
        "seasonality": resolved_config["seasonality"],
        "model": resolved_config["model"],
        "resampling": resolved_config["resampling"],
        "evaluation": resolved_config["evaluation"],
        "source_sha256": main_series_digest.hexdigest(),
    }
    method_config_bytes = json.dumps(
        method_config, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")

    combination_manifest = {
        "stage": "configuration_preview",
        "main_series": cfg.main_series.name,
        "seasonality": cfg.seasonality.name,
        "model": cfg.model.name,
        "resampling": cfg.resampling.name,
        "main_series_source_sha256": main_series_digest.hexdigest(),
        "method_config_sha256": hashlib.sha256(method_config_bytes).hexdigest(),
        "execution_status": "configuration_only",
    }

    output_dir = Path(HydraConfig.get().runtime.output_dir)
    (output_dir / "resolved_config.yaml").write_text(
        resolved_config_text, encoding="utf-8",
    )
    (output_dir / "combination_manifest.json").write_text(
        json.dumps(combination_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(combination_manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
~~~

这个脚本没有定位仓库根后导入 settings 的需求，输入路径由配置显式提供。后续实现 silver 读取时，必须采用根 AGENTS.md 规定的标记搜索并经 settings 定位正式湖。

配置摘要不等于完整实验身份：还要记录代码摘要、软件环境和所有输入数据身份。这里先学习方法配置与源文件身份的关系。

### 9.3 单次预览

先进入练习目录：

~~~powershell
Set-Location 'E:\Latitude_Analytics_v2\R01_project_collection\JQ_strategy\intraday_modeling_research'
& $latitudePython .\inspect_combination.py --cfg job
& $latitudePython .\inspect_combination.py
~~~

第一条只展示配置，第二条运行预览并输出文件身份与组合 manifest。

### 9.4 方案替换

~~~powershell
& $latitudePython .\inspect_combination.py seasonality=none model=figarch resampling=clock_fixed
~~~

这条命令应产生：

~~~text
公共输入：im_segments_v2
季节性：none
模型：figarch
重采样：clock_fixed
状态：configuration_only
~~~

### 9.5 十二种方案组合

~~~powershell
& $latitudePython .\inspect_combination.py --multirun 'seasonality=none,mix_v2,rv_profile' 'model=garch,figarch' 'resampling=identity_1m,clock_fixed'
~~~

逗号表示枚举选项；多个组产生笛卡尔积。每个 job 都保存自己的配置。默认 launcher 在本地串行运行，具体行为见 [Hydra Multi-run](https://hydra.cc/docs/1.3/tutorials/basic/running_your_app/multi-run/)。

练习验收：

1. 恰有 12 个 job；
2. main_series 的文件身份全部相同；
3. 只有三个方案维度发生预定变化；
4. lookback、模型阶数、采样间隔没有同时展开；
5. 没有任何结果被标成训练成功。

## 10. 动手课二：把组合记录到 MLflow

### 10.1 先理解四种记录

| 记录类型 | 本项目例子 |
|---|---|
| Param | seasonality=mix_v2、model=garch、resampling=clock_fixed |
| Tag | stage=configuration_preview、batch_label=first_scheme_matrix |
| Metric | 后续训练的 QLIKE、耗时、有效样本数 |
| Artifact | resolved_config.yaml、拼接 manifest、因子表、预测表 |

参数、指标和文件分别用 log_params、log_metrics、log_artifact 等 API 记录。详见 [MLflow Tracking API](https://mlflow.org/docs/latest/ml/tracking/tracking-api)。

### 10.2 在独立终端启动服务

在独立终端执行；存储使用本练习所属目录的绝对路径，不依赖终端启动位置：

~~~powershell
$learningTrackingDir = 'E:\Latitude_Analytics_v2\R01_project_collection\JQ_strategy\intraday_modeling_research\learning_outputs\mlflow'
New-Item -ItemType Directory -Force -Path $learningTrackingDir
$learningDatabasePath = (Join-Path $learningTrackingDir 'mlflow.db').Replace('\', '/')
$learningArtifactDir = Join-Path $learningTrackingDir 'artifacts'
& 'E:\anaconda3\envs\latitude_env_v2\Scripts\mlflow.exe' server --backend-store-uri "sqlite:///$learningDatabasePath" --serve-artifacts --default-artifact-root 'mlflow-artifacts:/' --artifacts-destination $learningArtifactDir --host 127.0.0.1 --port 5000
~~~

在浏览器打开 [本地 MLflow](http://127.0.0.1:5000)。服务前台运行于该终端，Ctrl+C 停止。后续启动时复用上述绝对路径；`--default-artifact-root` 明确使用服务代理，`--artifacts-destination` 明确服务器实际写入位置。

SQLite 保存实验元数据，artifact 目录保存文件。已有 Experiment／Run 的附件地址不会因重启服务或修改参数自动更新，必须在写附件前核对，地址不符时停止并明确整理原记录。仓库根目录禁止生成 `mlruns/`、`mlartifacts/` 或 `mlflow.db`，见[输出归属规则](../../../AGENTS.md#草稿区与文件收纳规则)。详见 [Tracking Server](https://mlflow.org/docs/latest/self-hosting/architecture/tracking-server/)。

### 10.3 为预览脚本加入记录

在脚本顶部增加：

~~~python
import mlflow
from mlflow.tracking import MlflowClient
~~~

在 main() 末尾、写完两份输出文件之后加入：

~~~python
    if cfg.tracking.uri != "http://127.0.0.1:5000":
        raise ValueError("本练习必须连接上述本地服务，不回退到默认存储")
    mlflow.set_tracking_uri(cfg.tracking.uri)
    client = MlflowClient(tracking_uri=cfg.tracking.uri)
    artifact_location = "mlflow-artifacts:/"
    experiment = client.get_experiment_by_name(cfg.tracking.experiment_name)
    if experiment is None:
        experiment_id = client.create_experiment(
            cfg.tracking.experiment_name, artifact_location=artifact_location
        )
    else:
        if experiment.artifact_location != artifact_location:
            raise ValueError("已有 Experiment 未使用本练习附件代理，停止写入")
        experiment_id = experiment.experiment_id

    run_name = (
        f"{cfg.seasonality.name}__"
        f"{cfg.model.name}__"
        f"{cfg.resampling.name}"
    )
    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
        if run.info.experiment_id != experiment_id:
            raise ValueError("Run 不属于本练习 Experiment，停止写入")
        if run.info.artifact_uri != f"{artifact_location}{run.info.run_id}/artifacts":
            raise ValueError("Run 未使用本练习附件代理，停止写入")
        mlflow.set_tags({
            "stage": "configuration_preview",
            "batch_label": cfg.batch_label,
            "execution_status": "configuration_only",
        })
        mlflow.log_params({
            "main_series": cfg.main_series.name,
            "seasonality": cfg.seasonality.name,
            "model": cfg.model.name,
            "resampling": cfg.resampling.name,
        })
        mlflow.set_tag(
            "main_series_source_sha256",
            combination_manifest["main_series_source_sha256"],
        )
        mlflow.set_tag(
            "method_config_sha256",
            combination_manifest["method_config_sha256"],
        )
        mlflow.log_artifact(str(output_dir / "resolved_config.yaml"))
        mlflow.log_artifact(str(output_dir / "combination_manifest.json"))
~~~

重新运行 12 个组合，现在 UI 中会出现 12 条配置预览 run。

MLflow 的 FINISHED 表示这次预览程序正常结束。stage 与 execution_status 明确说明它尚未训练，不能用 FINISHED 推断模型可用。

### 10.4 在 UI 中做三个动作

1. 显示 main_series、seasonality、model、resampling 四列。
2. 筛选 model=figarch，观察该模型下有哪些季节性和采样组合。
3. 打开一条 run 的 resolved_config.yaml，检查 120 日窗口和固定采样参数。

## 11. 从配置预览进入真实计算

接下来逐环节实现，每个环节先跑通一套基准方案。

建议课程顺序：

~~~text
主力片段与 manifest
→ mix_v2 与 none
→ RV / BPV 季节性方案
→ 一种模型的基准拟合与预测
→ 第二种模型
→ identity_1m 与 clock_fixed
→ 真实组合表与共同评价
→ 扩展模型和采样方式
~~~

模型业务实现保持明确的输入类型：

- GARCH/FIGARCH：消费有定义的收益序列；
- ARIMA/ARFIMA：消费有定义的波动测度序列；
- seasonal 模型：额外消费有明确含义的季节信息。

统一的是组合配置、记录方式与预测结果字段，不必强迫所有模型使用同一内部 fit 接口。

可以先用直接的模型分支：

~~~python
# 训练入口结构示例，拟合与预测逻辑待实现。
if cfg.model.name == "garch":
    # 使用 arch 对当前组合的收益输入拟合并预测
    ...
elif cfg.model.name == "arima":
    # 使用 statsmodels 对当前组合的测度输入拟合并预测
    ...
else:
    raise NotImplementedError(cfg.model.name)
~~~

模型、季节性和采样算法本身可以是独立领域函数。不要为了组织几个分支先增加 manager、registry 或通用 adapter 层。

### 11.1 公共产物复用

若有 3 种季节性、2 种模型、2 种采样：

- 主力拼接准备一次；
- 每个季节性方案在同一输入与固定参数下估计一次；
- 可复用的采样产物按其真实依赖复用；
- 每个完整组合独立记录拟合和评价。

复用键至少包括上游身份、当前环节配置、代码版本、历史截止时点。滚动预测中，不同历史截止点的因子不能误命中同一个缓存。

Hydra 不会自动管理这些依赖和缓存。第一版可以显式引用准备阶段的文件与 manifest，后续再按实际需要增加缓存。

### 11.2 MLflow 的运行关系

建议公共准备 run 与组合 run 使用链接标签：

~~~text
main_series run A
seasonality run S1 → main_series_run_id=A
seasonality run S2 → main_series_run_id=A

组合 run C1 → main_series_run_id=A, seasonality_run_id=S1
组合 run C2 → main_series_run_id=A, seasonality_run_id=S1
组合 run C3 → main_series_run_id=A, seasonality_run_id=S2
~~~

一个季节性结果能被多个模型复用，不需要重复上传。MLflow 自带父子关系只有一个 parent；多上游依赖用 run_id 标签或 lineage manifest 表达更清楚。

## 12. 组合评价应该看什么

第一阶段的比较表：

~~~text
main_series_id
seasonality
model
resampling
evaluation_protocol_id
evaluation_keys_sha256
observation_count
qlike
convergence_rate
elapsed_seconds
run_id
~~~

按同一目标和同一评价键集合比较。类似参考窗口比较 Notebook 的 common_day_mask，保留共同样本数量，并同时报告各方案损失了多少样本。

正式预测表建议保存：

~~~text
forecast_origin
target_start_at
target_end_at
actual_variance
predicted_variance
training_end_at
seasonality_history_end_at
converged
~~~

方差为正时，可使用：

~~~text
QLIKE = mean(actual / predicted - log(actual / predicted) - 1)
~~~

如果目标可为零，必须另外冻结零值处理或采用允许零目标的损失形式，不能在代码中静默删掉零值。参数选择期间只看验证集，方案和参数确定后再评估测试集。

季节性诊断图和预测指标并列保存：残余曲线更平不自动意味着样本外预测更好。

## 13. 第二阶段：再展开方案内部参数

这一节在第一轮方案比较完成后执行。

### 13.1 只研究一个季节性方案的回望窗口

固定 mix_v2、garch、clock_fixed，比较 30、60、120、360 日窗口：

~~~powershell
& $latitudePython .\inspect_combination.py --multirun seasonality=mix_v2 model=garch resampling=clock_fixed 'seasonality.lookback_trading_days=30,60,120,360'
~~~

注意 mix_v2 示例中 min_periods 原本写死为 120。进入窗口扫描前应将该项改为插值：

~~~yaml
lookback_trading_days: 120
min_periods: ${seasonality.lookback_trading_days}
~~~

这保证扫 L 时同时要求 L 个有效历史观测，保持参考代码的完整窗口语义。

上述命令仍调用预览脚本；真实训练课程完成后，将入口替换成实际训练脚本。配置实验和业务执行不能混淆。

### 13.2 在固定方案上比较采样间隔

~~~powershell
& $latitudePython .\inspect_combination.py --multirun seasonality=mix_v2 model=garch resampling=clock_fixed 'resampling.interval_minutes=5,10,15'
~~~

这属于 clock_fixed 方案内部的参数比较。

### 13.3 模型参数组合

~~~powershell
& $latitudePython .\inspect_combination.py --multirun seasonality=mix_v2 model=garch resampling=clock_fixed 'model.p=1,2' 'model.q=1,2'
~~~

这是四个 GARCH 参数组合。FIGARCH 的阶数限制与 GARCH 不同，应按具体实现分别校验，不能把这一网格不加修改地复制给所有模型。

### 13.4 最后才做跨环节参数联合扫描

~~~text
固定一条方案：
mix_v2 × garch × clock_fixed

回望窗口 4 种
× GARCH 参数配置 4 种
× 采样间隔 3 种
= 48 个参数组合
~~~

第一阶段的 12 个方案组合与这里的 48 个参数组合分开理解、分开记录 stage。

none 等无窗口方法不参与窗口维度，否则只是把同一方法重复运行四次。不要假设每种方案拥有相同参数结构。

当前窗口样例是 (30,60,120,360)，实际样本比较沿用完整交易日序列和共同有效日期，不把 360 日理解为 360 个自然日。

## 14. 保存一个可复用的方案配方

conf/experiment/mix_garch_clock.yaml：

~~~yaml
# @package _global_
defaults:
  - override /seasonality: mix_v2
  - override /model: garch
  - override /resampling: clock_fixed

experiment_recipe: mix_garch_clock
~~~

调用：

~~~powershell
& $latitudePython .\inspect_combination.py +experiment=mix_garch_clock --cfg job
~~~

再对这个固定配方展开窗口：

~~~powershell
& $latitudePython .\inspect_combination.py --multirun +experiment=mix_garch_clock 'seasonality.lookback_trading_days=30,60,120,360'
~~~

前提是已按第 13 节将 min_periods 改为插值。这样“方法配方”和“参数变化”都能在最终配置中看见。

## 15. 如何逐步动手，而不一次展开全部内容

| 课次 | 你实际完成的东西 | 本课先固定什么 |
|---|---|---|
| 1 | 理解主力映射、原始片段与复权列 | 现有 1.10 换月规则 |
| 2 | 记录公共拼接输入身份 | 同一份主力产物 |
| 3 | 把 mix_v2 写成一个季节性配置 | 一个回望窗口 |
| 4 | 列出 RV、跨日/不跨日 BPV 等方案 | 每种完整边界规则 |
| 5 | 列出 GARCH、FIGARCH、seasonal、ARIMA、ARFIMA | 每模型一套参数 |
| 6 | 列出原始网格、固定时钟等重采样 | 每方案一套参数 |
| 7 | 用 Hydra 输出组合表 | 只扫方案名 |
| 8 | 用 MLflow 保存组合身份与配置 | 预览阶段 |
| 9 | 按环节接入真实实现 | 一次接入一种方案 |
| 10 | 比较共同目标上的真实组合结果 | 固定验证口径 |
| 11 | 重现窗口比较 Notebook 的思路 | 固定方案后扫窗口 |
| 12 | 展开模型与采样参数 | 清楚记录参数组合规模 |

实际长批次按根 AGENTS.md 的人工授权与监控规则执行。预计超过 10 分钟时，先明确本批组合数和范围。

## 16. 相关文件

- 数据来源：[financial_futures_data/README.md](../financial_futures_data/README.md)。
- 片段拼接：[01_01_im_main_continuous.ipynb](01_01_im_main_continuous.ipynb)。
- 一个完整季节性方案：[02_01_im_bpv_intraday_seasonality.ipynb](../draft_experiments_v2/02_01_im_bpv_intraday_seasonality.ipynb)。
- 同方案内部的参数比较：[02_02_im_bpv_window_comparison.ipynb](../draft_experiments_v2/02_02_im_bpv_window_comparison.ipynb)。
- 项目入口：[README.md](README.md)。
