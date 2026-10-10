# Latitude_Analytics_v2

面向中国期货市场的数据采集与量化研究仓库。市场数据由正式采集入口更新，以统一的 Arrow/Parquet 契约供下游读取；研究工作包括主力合约识别、连续复权、采样、收益与波动率建模、期限结构及小波分析，也包含独立的论文复现和策略研究项目。

## 常用入口

| 要做的事 | 从这里开始 |
| --- | --- |
| 配置本地 Python、Notebook 或云端 SDK 环境 | [环境说明](environment/README.md)、[依赖清单](environment/requirements.txt) |
| 读取已有市场数据 | [数据湖读取 Demo](R02_Market_Data/a02_Lake/read_futures_lake_demo.ipynb)、[湖仓规则](R02_Market_Data/a02_Lake/AGENTS.md) |
| 采集、更新数据或查看运行历史 | [采集说明](R02_Market_Data/a01_Collection/README.md)、[总控台操作说明](R02_Market_Data/a01_Collection/operations/README.md) |
| 排查数据来源、连接或采集代码问题 | [采集检查](R02_Market_Data/a01_Collection/checks/README.md) |
| 使用研究方法、查看 demo 或组织实验 | [研究说明](R04_Research/README.md)、[收益与波动率预测实验](R04_Research/a02_Experiments/b01_ReturnVolatilityForecasting/README.md) |
| 查找论文复现、波动率学习材料或金融期货数据工作流 | [研究项目入口](#研究项目) |
| 修改代码、文档或数据契约 | [全仓规则与规范索引](AGENTS.md)，再读取目标目录的规则 |

## 目录与职责

```text
Latitude_Analytics_v2/
├─ R00_draft_collection_01/     # 探索 Notebook
│  └─ testing_N/               # 同名 Notebook 的本地配套输出
├─ R00_draft_collection_02/     # 临时材料与待确认归属的文件
├─ R01_project_collection/     # 独立研究、策略与论文复现项目
├─ R02_Market_Data/
│  ├─ a01_Collection/          # 正式采集入口及共用支撑模块
│  │  ├─ checks/              # 来源检查、采集本地测试及检查结果
│  │  └─ operations/          # 采集总控台、监控及运行历史
│  └─ a02_Lake/               # 正式数据湖及读取 Demo
│     ├─ raw/                 # 来源原文与证据
│     └─ silver/              # 17 张稳定契约表
├─ R04_Research/
│  ├─ a01_Methods/             # 可组合方法、Notebook 代理及方法 demo
│  ├─ a02_Experiments/         # 按实验名称组织的配置、计算与成果
│  └─ referance/               # 研究参考资料、文献和资料包
├─ R05_Old_Projects/           # 只读历史归档
├─ config/                    # 共享配置、连接及可执行数据契约
├─ environment/               # 环境安装、依赖、验证入口与证据
├─ .env.template              # 本地配置模板与项目根定位约定
└─ AGENTS.md                  # 全仓规则与目录规范索引
```

顶层 `RNN` 标识仓库分区，分区内的 `a`、`b`、`c` 编号标识业务层级。同一业务单元的 Notebook、Python 文件、配置和附属资源共享编号及业务词根；实际依赖由调用关系确定。命名规则见[层级编号与资源归属](AGENTS.md#层级编号与资源归属)。

## 本地准备

### Python 与 Notebook

本地开发、采集和研究统一使用 `latitude_env_v2`，Notebook 选择 `Python (latitude_env_v2)` 内核。新机器按[环境说明](environment/README.md)重建；alipai 的依赖例外、Jupyter 配置及本地／云端验证边界也在该说明中定义。

本机可在仓库根目录运行环境检查：

```powershell
& 'E:\anaconda3\envs\latitude_env_v2\python.exe' 'R02_Market_Data\a01_Collection\b00_01_verify_runtime.py'
```

### 本地配置与数据位置

按 [.env.template](.env.template) 在仓库根目录准备 `.env`，填写所用数据来源的凭据。模板中的采集起点和正式湖路径为：

```dotenv
FUTURES_DATA_START_DATE=2010-01-01
FUTURES_LAKE_ROOT=R02_Market_Data/a02_Lake
```

正式湖路径由 `config.settings.settings.futures_lake_root` 读取，相对路径按仓库根解析。项目根定位统一使用模板中的约定，Python 导入从仓库根使用完整包路径。

`.env` 和物化的正式湖数据保留在本地；克隆代码后，需要另行准备数据才能运行湖仓读取和依赖它的研究。Git 收纳范围见 [.gitignore](.gitignore)。

### 研究 demo 与 Git LFS

方法 demo 随代码版本保存，其中超过 100 MiB 的 Parquet 使用 Git LFS。首次克隆后，在仓库根目录运行：

```powershell
git lfs install --local
git lfs pull
```

后续检出版本也需保持 Git LFS 可用，以取得原路径下的完整数据文件。具体文件属性见 [.gitattributes](.gitattributes)。

## 市场数据

`R02_Market_Data/a01_Collection/` 维护国内期货日历与行情、交易所报告、外部市场、宏观与利率四组采集业务。19 个正式入口的来源、更新范围、写入和验收要求见[采集说明](R02_Market_Data/a01_Collection/README.md)。

日常操作通过 [console.py](R02_Market_Data/a01_Collection/operations/console.py) 总控台选择环节、配置参数、运行批次及查看历史，启动命令见[总控台操作说明](R02_Market_Data/a01_Collection/operations/README.md)。日常快捷配置包含 18 个阶段；全量分钟质检 c08 需人工显式选择和确认。业务入口用 `--write` 控制提交，不带该参数仍可能访问来源 API；具体执行边界由[采集规则](R02_Market_Data/a01_Collection/AGENTS.md)及 [operations 规则](R02_Market_Data/a01_Collection/operations/AGENTS.md)定义。

正式湖根为 `R02_Market_Data/a02_Lake/`。`raw/` 保存生产者约定的原文与证据，生意社链路只归档原始响应字节及摘要；`silver/` 提供 7 张日历维度表和 10 张事实表。稳定表的字段、类型、主键、分区和 metadata 统一定义在 [config/data_contracts.py](config/data_contracts.py)。`gold` 的产物与 Schema 由所属工作流定义。

读取时从[数据湖 Demo](R02_Market_Data/a02_Lake/read_futures_lake_demo.ipynb)开始：17 张 silver 表各有独立示例，另有生意社 raw 字节与摘要核对。修改生产者或消费者时遵循[湖仓规则](R02_Market_Data/a02_Lake/AGENTS.md)。

## 研究方法与实验

`R04_Research/a01_Methods/` 按研究主题组织可组合的计算单元：

| 主题 | 主要内容 |
| --- | --- |
| [b01_MainContractSelection](R04_Research/a01_Methods/b01_MainContractSelection/) | 主力合约识别与每日映射 |
| [b02_MainContinuousAdjustment](R04_Research/a01_Methods/b02_MainContinuousAdjustment/) | 训练／测试划分下的连续复权 |
| [b03_Sampling](R04_Research/README.md#分界采样) | 采样轴、固定单位、位置与价格执行 |
| [b04_ReturnVolatilityModeling](R04_Research/README.md#收益波动率建模) | 日内季节性、模型观测、收益及波动率模型 |
| [b05_TermStructureModeling](R04_Research/a01_Methods/b05_TermStructureModeling/) | 期限结构输入、固定 λ 的 NSS 拟合与网格校准 |
| [b06_WaveletAnalysis](R04_Research/README.md#小波分析) | 小波变换、同步压缩、连续序列与谱特征 |

研究算法由 Notebook 的 `export` 单元格定义，同名 `.py` 通过[加载器](R04_Research/a00_notebook_loader.py)导入定义。采集目录采用 Notebook 完整导出为 Python 的方式；两者的同步要求分别见[研究规则](R04_Research/AGENTS.md)和[采集规则](R02_Market_Data/a01_Collection/AGENTS.md)。

方法 demo 使用真实上游产物，保存在生产单元的 `cNN_Name_DemoData/`；下游按品种、范围、参数和生成身份选择输入。各方法的定义、demo 覆盖范围及实际验收状态见[研究说明](R04_Research/README.md)。

<!-- nss-cloud-status:start -->
NSS 完整 demo 的配置、验收结果与批次证据见[期限结构说明](R04_Research/README.md#期限结构输入)。
<!-- nss-cloud-status:end -->

`a02_Experiments/` 将方法组合为具体实验，以实验配置明确样本、训练／测试范围和比较条件。已有[收益与波动率预测实验](R04_Research/a02_Experiments/b01_ReturnVolatilityForecasting/README.md)的范围、结果、失败记录及复现入口由其项目说明集中维护。Hydra、MLflow 和 DLC 的分工与成果归属见[工具与云计算说明](R04_Research/README.md#工具分工与云计算)。

## 研究项目

`R01_project_collection/` 保存独立项目。以下入口分别说明各自的研究口径、数据依赖和成果边界：

| 项目入口 | 用途 |
| --- | --- |
| [中国期货市场演变复现](R01_project_collection/china_futures_market_evolution_reproduction/README.md) | 基于正式 silver 逐项复现市场演变统计方法 |
| [中国商品期货日内波动预测复现](R01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction/README.md) | 复现日内波动预测方法及比较框架 |
| [波动率阅读教程](R01_project_collection/JQ_strategy/volatility_research/READING_TUTORIAL.md)与[符号和方法](R01_project_collection/JQ_strategy/volatility_research/README.md) | 学习高频实现波动、日内周期与去季节方法 |
| [金融期货本地数据工作流](R01_project_collection/JQ_strategy/financial_futures_data/README.md) | 人工聚宽传输、缺失检测与项目数据库管理 |

## 文件归属与修改约定

文件按产生它们的项目和业务单元收纳：

| 材料 | 位置与规则 |
| --- | --- |
| 采集运行历史、日志和状态 | `R02_Market_Data/a01_Collection/operations/run_history/`；来源检查结果归 `checks/results/` |
| 研究方法 demo | 所属方法的 `cNN_Name_DemoData/`，按[研究规则](R04_Research/AGENTS.md)纳入 Git |
| 研究实验数据与追踪记录 | 所属实验的 `Data/`、`Tracking/`，本地收纳边界见[研究说明](R04_Research/README.md#数据和维护边界) |
| 探索 Notebook 的配套输出 | `R00_draft_collection_01/<Notebook 基名>/`，图表默认内嵌；见[探索规则](R00_draft_collection_01/AGENTS.md) |
| 临时文件与尚待决定的材料 | `R00_draft_collection_02/`，由用户确认保留、删除、复用及最终去向；见[草稿区规则](AGENTS.md#草稿区与文件收纳规则) |

运行输出须显式指向所属目录；MLflow 数据库与附件位置分别配置，仓库根目录禁止产生 `mlruns/`、`mlartifacts/`、`mlflow.db` 等默认输出。

修改前读取 [AGENTS.md](AGENTS.md) 及目标目录的规则；修改规范或可执行契约时，同步检查索引中的关联说明、示例和代码。市场数据与研究数据预览按[中文展示规则](AGENTS.md#数据呈现附带中文)呈现字段含义，计算和落盘保留原字段名。[R05_Old_Projects](R05_Old_Projects/AGENTS.md) 递归只读，历史代码与证据按归档规则保护。
