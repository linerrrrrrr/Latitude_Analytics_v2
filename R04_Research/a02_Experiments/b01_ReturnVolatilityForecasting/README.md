# 收益与波动率预测实验

本实验比较螺纹钢（RB）和铜（CU）在三种采样轴、三种季节性处理下的 ARMA-GARCH 收益与条件方差预测。结果覆盖18组配置和三个指定测试日，每个测试日的完整采样位置均有记录。

## 范围与方法

| 项目 | 固定条件 |
| --- | --- |
| 读取与复权范围 | 2010-01-01—2026-06-30 |
| 采样尺度校准训练期 | 2010-01-01—2024-12-31 |
| 测试期 | 2025-01-01—2026-06-30 |
| 实际计算日期 | 2025-01-02、2025-09-25、2026-06-30 |
| 品种与采样 | RB、CU；时间、成交额、GK×成交额；N=15，log_interpolate |
| 模型 | ARMA(1,1)-GARCH(1,1)，含均值项 |
| 日度训练窗口 | 测试日前完整三个自然年，历史不足时保留失败 |

日期按交易日解释，范围两端包含，时区为 Asia/Shanghai。三个计算日期分别是固定测试期359个交易日中位置0、179、358的日期；结果不覆盖其他测试日，也不代表原规划1,944组配置的完整实验。

时间轴每个实际分钟增加1；成交额轴使用当分钟全合约成交额；GK×成交额轴使用当分钟主力合约的 Garman-Klass 波动率乘全合约成交额。各轴的固定采样单位等于校准训练期平均分钟增量的15倍，测试期沿用该单位。log_interpolate 按对数价格插值。每个测试日以当日边界重新锚定训练／测试采样，使用此前完整三年历史拟合一次；日内逐点先预测，再用已实现观测更新状态，参数不重新估计。

三种观测处理分别为 raw（不去季节）、TD120-RV 和 TD120-BPV。TD120表示120个交易日窗口，RV为实现方差，BPV为双幂变差；后两种配置使用对应季节曲线处理分钟收益，再汇总到采样区间。参数和时间口径见 [config.yaml](config.yaml)，实际计算定义保存在 [sources.zip](sources.zip) 中。

## 结果与适用边界

| 记录 | 成功 | 失败 | 总数 |
| --- | ---: | ---: | ---: |
| 日度拟合 | 30 | 24 | 54 |
| 预测位置 | 619 | 398 | 1,017 |

18组配置的54条拟合和1,017个预测位置已完成独立覆盖核对。24条失败拟合在观测有限性检查处停止，优化器未执行；398个对应预测位置保留失败状态和原因。BPV季节曲线含固定迭代 Huber 非收敛或零MAD因素；RB的TD120-RV配置在2025-09-25、2026-06-30的训练历史含零MAD缺失。失败没有通过换模型、补值、删除缺失观测或重试拟合消除。基础设施失败为0。

`previous_fit.parquet` 和 `previous_prediction.parquet` 保存另两组RB时间轴、N=5的ARMA-GARCH结果（raw和TD120-RV）：718条拟合、49,014个预测位置，其中242条拟合失败、16,609个预测位置失败。这两组记录不计入上述18组配置，原Run和任务身份保存在 `source_run_id`、`source_task_id` 中。

## 文件与读取

| 文件 | 用途 |
| --- | --- |
| [experiment.ipynb](experiment.ipynb) | 读取配置、核对结果并展示中文摘要；定义显式本地复现函数 |
| [experiment.py](experiment.py) | 加载同名Notebook中标记为export的定义 |
| [config.yaml](config.yaml) | 固定范围、参数、计算日期和相对文件路径 |
| [provenance.json](provenance.json) | 执行来源、输入身份、Schema、SHA256、逐项覆盖及失败记录 |
| [sources.zip](sources.zip) | 实际云作业使用的源码和固定依赖快照 |
| `Data/fit.parquet`、`Data/prediction.parquet` | 18组配置的拟合和完整预测位置记录 |
| `Data/previous_fit.parquet`、`Data/previous_prediction.parquet` | 两组N=5配置的合并结果 |
| `Data/RB_*.parquet`、`Data/CU_*.parquet` | 每品种七张冻结输入：三轴、分钟贡献、交易时段、RV和BPV季节曲线 |

在仓库目录中使用 `latitude_env_v2` 内核，顺序运行Notebook即可读取和展示已保存结果。默认运行不认证、不联网、不提交、不拟合。结果与输入路径相对于本实验目录，18张Parquet表均位于一层 `Data/` 中。调用 `read_delivery(experiment_dir, verify_inputs=True)` 可额外核对14张冻结输入和源码快照。

## 本地复现与追溯

显式调用 `reproduce_day_locally(experiment_dir, work_dir=新空工作目录, variety="RB", axis="time", case_id="arma_garch_no_seasonality", experiment_date="2025-01-02")` 可复现单组配置的一个测试日。函数使用冻结源码、输入及完整此前历史，在指定的外部工作目录生成副本和结果；默认不执行，不覆盖本目录成果。使用新的Python 3.13内核和快照中的固定依赖，版本见源码包内的 `requirements.txt`。原云运行Python为3.13.9，输入冻结Python为3.13.15，补丁版本分别记录在provenance中。

原云作业、Run及代码摘要定位已验收的执行来源。当前说明文字的修改不改写这些身份，也不改写源码快照。提交入口和原1,944组配置队列的继续入口已关闭，监督状态为PAUSED。

清理范围与限制见 `provenance.json` 的 `cleanup`：旧Tracking的正常回收站还原未核实；另3份旧输入身份manifest在明确的一次性永久删除范围内移除，未单独保留。
