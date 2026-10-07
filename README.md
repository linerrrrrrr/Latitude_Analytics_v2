# Latitude_Analytics_v2

本仓库包含市场数据采集、正式数据湖、研究项目和实验工作流。目录级规则由 [AGENTS.md](AGENTS.md) 统一索引；协作中的纠错与执行状态遵循[对应规则](AGENTS.md#协作纠错与执行状态)。

本地入口统一使用 v2，安装、alipai 依赖例外和云端环境边界见[环境说明](environment/README.md)。

市场数据和 `04_Research` 的数据预览采用 `英文字段名（中文含义）` 表头。中文只作用于展示，silver 含义取权威 Schema，研究派生字段由所属方法定义；范围和边界见[展示规则](AGENTS.md#数据呈现附带中文)。

```text
00_draft_collection_01/       # 探索 Notebook
  testing_N/         # 对应 Notebook 的本地数据与显式导出文件
00_draft_collection_02/       # 临时或待用户决定去留、复用与最终归属的材料
01_project_collection/       # 各研究项目
02_Market_Data/
  a01_Collection/            # 数据采集、更新和运维
    checks/                  # 数据源检查、采集本地测试与检查结果
    operations/console.py    # 采集 GUI 入口
    operations/run_history/  # 采集运行历史、日志和状态
  a02_Lake/                  # 正式数据湖及读取示例
    raw/                     # 来源原文与证据
    silver/                  # 17 张稳定契约表
04_Research/                 # 方法、可串联 demo 和按主题命名的实验
  a01_Methods/               # bNN 研究主题；cNN 单元的 Notebook、代理和同基名 DemoData 共同归属
  a02_Experiments/           # 按实验命名；收益与波动率预测入口已建立
  referance/                # 研究参考资料、文献和资料包
05_Old_Projects/             # 只读历史归档，含 collection_maintenance_20261002.zip
config/                      # 项目共享配置与可执行数据契约
environment/                 # 环境安装、依赖清单、验证代码与证据
  alipai/                    # SDK 验证入口与分批结果
  rebuild_20261001/          # 已有环境重建证据
```

目录编号标识业务单元，同一单元的文件与附属资源共享编号和业务词根，详见[编号与资源归属](AGENTS.md#层级编号与资源归属)。研究 demo 使用 `cNN_Name_DemoData/`；实验共有材料使用 `Data/`、`Tracking/`，不占 c 单元编号。收益／波动率建模集中在一个 `b04_ReturnVolatilityModeling/` 主题：c01 已实现分钟贡献、每日季节曲线与训练期初始回填；c02 已实现分钟去季节、采样区间汇总和模型输入转换；c03 已实现六组收益模型的 QML 联合拟合、预测及状态更新；c04 已实现 ARMA、ARFIMA、HAR、MEM 的直接观测拟合、下一步预测和固定参数更新。

- 采集入口、GUI 启动和维护命令见 [采集说明](02_Market_Data/a01_Collection/README.md) 与 [运维说明](02_Market_Data/a01_Collection/operations/README.md)。
- 研究各环节的业务 demo 使用真实上游产物，合成边界测试与真实链路验收分别记录；收益／波动率 demo 已加入完整采样序列逐点重估，当前执行状态见[研究说明](04_Research/README.md#收益波动率建模)。
- 期限结构建模归 `b05_TermStructureModeling/`，已实现 [c01 输入构造](04_Research/a01_Methods/b05_TermStructureModeling/c01_TermStructureInputs.ipynb)、[c02 固定 λ 的 NSS 拟合](04_Research/a01_Methods/b05_TermStructureModeling/c02_NSSFitting.ipynb)及 [c03 网格校准](04_Research/a01_Methods/b05_TermStructureModeling/c03_NSSCalibration.ipynb)：以合约最后有效日的自然日末（上海时区次日 00:00）计算剩余期限，保持上游输入尺度，按显式经济约束拟合并保留失败记录。c03 暴露网格上下限与步长，只用训练期选择 λ，测试期固定 λ 重估 β，计算和单张热力图分开调用；真实 demo 验收状态见[期限结构说明](04_Research/README.md#期限结构输入)。 当前 [共用配置](04_Research/a01_Methods/b05_TermStructureModeling/c03_NSSCalibration.yaml) 为 RB、N15 的 σ×成交额采样；三个 Notebook 默认不落盘小样本，完整云 demo 状态见本批记录。

<!-- nss-cloud-status:start -->
NSS N15 σ×交易额完整云 demo 正在后台执行；已确认作业 `train1loekui35a9`，九份产物尚未完成本地验收。 [批次记录](04_Research/a01_Methods/b05_TermStructureModeling/c03_NSSCalibration_CloudRuns/20261007_nss_n15_31bc82d6/submission.json)。
<!-- nss-cloud-status:end -->

- 来源质量、连接排查和采集本地测试见 [采集检查](02_Market_Data/a01_Collection/checks/README.md)。
- 小波分析归 `b06_WaveletAnalysis/`，[c01 小波变换](04_Research/a01_Methods/b06_WaveletAnalysis/c01_WaveletTransform.ipynb)提供复因果连续递推及离线 Morlet，周期按采样点数，预热与失败分别记录；[c02 同步压缩](04_Research/a01_Methods/b06_WaveletAnalysis/c02_Synchrosqueezing.ipynb)默认采用正式 SST，支持当前点比例／绝对阈值、固定频率网格与排除诊断；[c03 完整序列](04_Research/a01_Methods/b06_WaveletAnalysis/c03_RollingWavelet.ipynb)接入连续状态、可得时刻对齐和按参数选择的独立单张图；[c04 谱特征](04_Research/a01_Methods/b06_WaveletAnalysis/c04_SpectralFeatures.ipynb)从已有 SST 提取逐点统计，暴露权重、分频和带宽参数，保留退化原因及信息时刻。四本 Notebook 的完整真实 demo 均已验收，方法与实际状态见[小波分析说明](04_Research/README.md#小波分析)。
- 正式湖通过 `.env` 中的 `FUTURES_LAKE_ROOT=02_Market_Data/a02_Lake` 定位；配置说明见 [.env.template](.env.template)，读取示例见 [read_futures_lake_demo.ipynb](02_Market_Data/a02_Lake/read_futures_lake_demo.ipynb)。
- 草稿区材料经用户确认后删除或移至最终目录；全部待办处理完且无新增材料时应为空，持续工作中允许暂时非空。价值判断、复用和处置边界见[草稿区规则](AGENTS.md#草稿区与文件收纳规则)。
- 探索 Notebook 的配套文件统一收纳到 `00_draft_collection_01/<notebook 名>/`；图表默认内嵌，独立文件显式导出，输出目录留在本地。目录规则与 `testing_10` 分钟数据的跨 Notebook 读取路径见[实验草稿区 AGENTS](00_draft_collection_01/AGENTS.md)。
- 固定产生的运行历史、日志和状态保存在产生它们的项目目录内；研究实验记录由所属研究项目保存。
- 研究结构、Notebook 代理、demo 引用及 Hydra/MLflow/DLC 接入见 [研究说明](04_Research/README.md)。主力识别已保存并验收 CU、RB 两份 2010-01-01 至 2026-09-30 demo；训练期双向、测试期固定起点复权的 CU、RB 数据已重算并验收，数据归生产单元的 `cNN_Name_DemoData/`。复权锚点使用片段首末实际分钟，理论网格缺口保留为诊断；原始零价格保留、对应 log 和复权字段置空。复权与采样文件名分别列出训练范围和测试范围，两段合并即完整区间；以品种、范围和关键参数选择，精确条件及输入身份沿用 Parquet metadata。分界采样统一归 `b03_Sampling/`，使用 `c01_SamplingPositions`、`c02_SamplingExecution` 两本 Notebook 和各自代理；轴及初始单位校准归位置 Notebook，滚动时固定 a 和复权输入。过期采样 demo 已删除，完整范围重新生成及验收状态见研究说明；价格与 GH 分开绘图，覆盖 CU、RB × N=10、15、30、60 × 三条轴 × 两种方式的 48 组配置，并可在独立单元格逐张选择图像进行比较。旧 close 重采样按只读规则保护。首个按日建模实验已建立实验草稿，数据准备、逐日计算、输入冻结及 DLC／MLflow 显式入口已接入，RB、CU 的完整输入准备及冻结验收已完成，首次云创建请求因 SDK 默认会话使用 VPC 端点而 TLS 失败停止，尚无确认的 DLC job ID；会话初始化已修正并通过离线检查，模型拟合／预测尚未执行，云端链路尚未验收。方法代码和 demo 数据都纳入 Git，方法或输入变化时同步更新受影响的产物。原特征工程留存与运行边界见研究说明。
