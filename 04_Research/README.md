# 研究方法与实验

`a01_Methods` 维护可组合函数及 demo，`a02_Experiments` 按时间范围与训练／测试划分组织对照实验。编号用于浏览排序，函数调用决定实际依赖。本说明面向熟悉 Python 和 Notebook、首次进入本目录的研究人员。

当前已建立通用 Notebook 加载器、参考资料目录及[逐交易日主力合约识别 Notebook](a01_Methods/b01_MainContractSelection/b01_MainContractSelection.ipynb)。该方法已实现逐交易日递推函数、同名导入代理、四张正式 silver 输入的只读读取和 RB demo。Notebook 内嵌展示数据、候选证据、每日映射、换约明细及输出稳定性检查；验证不覆盖正式实验组合与 Hydra/MLflow 链路。`a02_Experiments` 尚无具体时间分区。带尖括号的名称与配置均为填写约定，不是已创建的入口。强制规则见 [AGENTS.md](AGENTS.md)。

方法[主力片段与全合约价格复权](a01_Methods/b02_MainContinuousAdjustment/b02_MainContinuousAdjustment.ipynb)已实现日级分段、分钟与自然对数输入准备、log 域共同偏移复权及导入代理，显式核对上游主力 demo 并只读使用正式分钟和 Session 日历。RB 演示固定为 2023-10-25 至 2025-10-20（交易日两端包含，Asia/Shanghai），包含 1,972,620 条全合约分钟、164,385 条主力分钟、7 个片段和 6 次换约。正式日历记录 13 个疑似休市夜盘，主力缺失 1,560 分钟；保留缺失诊断，不补价、不改主力。累计换月 log 差值按全部相邻实际分钟间隔均摊，换月衔接也参与，首尾 OHLC 保持原值；片段首末或换约锚点缺失时拒绝计算。确定性边界例子、全区间数学核对及独立真实换约调用已通过。完整全合约长表已保存为本方法 demo，回读核对实际内容；同一身份与内容重跑时保留文件字节和生成时间。

方法[全合约 close 重采样](a01_Methods/b03_CloseResampling/b03_CloseResampling.ipynb)读取 b02 复权分钟 demo，提供固定区间采样与每个分钟末端向后 K 点的分批采样。所有合约共用源分钟和插值比例，在复权 log open/close 之间插值，仅输出 close 的 log 与价格表示。按用户选定的 N 分钟校准成交量／额间隔 a，预览超过 a 的分钟及实际多次采样情况。RB 15 分钟演示的三种采样轴各有 10,959 个点，合计 394,524 条合约记录，已保存并回读核对；逐分钟向后 32 点已遍历全部 164,385 个末端，并通过边界和逐点参考检查。未建立实验时间分区。

```text
04_Research/
├─ AGENTS.md
├─ README.md
├─ a00_notebook_loader.py
├─ a01_Methods/
│  ├─ b01_MainContractSelection/
│  │  ├─ b01_MainContractSelection.ipynb  # 方法定义与 RB demo
│  │  ├─ b01_MainContractSelection.py     # 仅加载 Notebook export 定义
│  │  └─ b01_MainContractSelection_demo.parquet  # 纳入 Git，供下游 demo 读取
│  ├─ b02_MainContinuousAdjustment/
│  │  ├─ b02_MainContinuousAdjustment.ipynb  # 日级分段、分钟输入、共同偏移复权与 RB demo
│  │  ├─ b02_MainContinuousAdjustment.py     # 仅加载 Notebook export 定义
│  │  └─ b02_MainContinuousAdjustment_demo.parquet  # 完整全合约复权分钟长表，纳入 Git
│  └─ b03_CloseResampling/
│     ├─ b03_CloseResampling.ipynb  # 共同位置插值、间隔预览、两种采样方式与 RB demo
│     ├─ b03_CloseResampling.py     # 仅加载 Notebook export 定义
│     └─ b03_CloseResampling_demo.parquet  # 时间／成交量／成交额三种轴的全合约 close
├─ a02_Experiments/
└─ referance/                 # 研究参考资料、文献和资料包
```

## 研究参考资料

[期货高维特征与机器学习研究范式资料包](referance/feature_selection_production_research_2026-08-24/README.md)保存 2026-08-24 的研究报告、材料索引、来源清单和论文。项目诊断与建议按资料日期理解，不作为当前实现或项目规则的证明。

## 方法与 demo

方法目录采用 `bNN_<方法名>/`，派生方法使用同级 `bNN_01_<派生方法名>/`、`bNN_02_<派生方法名>/`。每方法通常只有同名 Notebook、代理 `.py` 和一个 `<方法Notebook名>_demo.parquet`，demo 前缀与 Notebook 一致并保留编号；方法说明和演示写在 Notebook 内。主力识别 Notebook 的 `export` 单元格定义局部 Schema、`build_main_contract_daily()` 和 `plot_main_contract_daily()`；同名代理只加载这些定义。RB 每日主力映射已保存为本方法目录的 [b01_MainContractSelection_demo.parquet](a01_Methods/b01_MainContractSelection/b01_MainContractSelection_demo.parquet)，生成条件、输入与代码摘要写入 metadata，并通过复读验收。Notebook 提供下游显式读取及摘要核对示例。

复权方法保存的 [b02_MainContinuousAdjustment_demo.parquet](a01_Methods/b02_MainContinuousAdjustment/b02_MainContinuousAdjustment_demo.parquet)保留 `raw_*`、`log_*`、`adjusted_log_*`、`adjusted_*` OHLC、数量、主力身份、片段与逐分钟共同偏移。下游用 `is_main` 选取主力，用 `segment_id` 定位片段；`log_*` 和 `adjusted_log_*` 已是自然对数，直接选用相应价格域。metadata 包含上游身份、实际分钟和 Session 输入内容摘要、固定范围、复权参数、相关定义/Schema/环境及输出内容摘要。

Notebook 按“说明与定义 → demo 输入和参数 → 本地或云端计算 → 展示与保存”组织。下游 demo 显式读取上游文件，不复制上游输入。数据 metadata 保存生成条件、时间范围和上游摘要；上游改变后检查下游是否需要重算。表格优先单个 Parquet，模型与表格不强求同一格式。

研究方法的代码和 demo 数据都纳入 Git。方法或输入变化后，重新生成并验收受影响的 demo，将相关代码与对应数据一起提交。检出某次提交后，下游 demo 可以读取该次提交保存的上游结果；重算仍需取得对应的原始输入。

主力识别 demo 的算法及局部 Schema 单元格同时标记 `export` 与 `demo-dependency`，代码摘要只取这些生成依赖；绘图函数只标记 `export`。新增生成依赖时同步纳入摘要范围。保存时复读已有文件，比较生成身份和实际数据；两者一致就保留文件和原生成时间，发生变化才写入并验收。

可复用的导入、常量与定义单元格标记 `export`；demo、读写、图表和提交操作不标记。同名代理只需：

```python
from a00_notebook_loader import load_notebook_exports

load_notebook_exports(__file__, globals())
```

调用方先按 [.env.template](../.env.template) 定位根。在已命中根目录的分支中增加 `sys.path.insert(0, str(candidate_root / "04_Research"))`，再按正常模块路径导入方法。直接运行代理的环境也须提供研究根模块路径；代理不另写根定位代码。导入只加载定义，计算须显式调用；修改 Notebook 后重启内核。

[加载器](a00_notebook_loader.py)先编译全部选中单元格，再顺序执行，错误标明 Notebook 路径和零基单元格索引。它保留代理模块身份，支持跨方法正常导入；不执行已保存输出，不替代代码对副作用的约束。研究代理不使用采集的完整 PythonExporter 同步命令。

## 时间分区与对照组

每个时间范围及切分对应一个业务一级目录。每组参数不另建代码目录：

```text
a02_Experiments/
└─ bNN_<整体时间范围>__test_<测试起点>/
   ├─ experiment.ipynb
   ├─ experiment.py
   ├─ config.yaml
   ├─ c01_Data/                      # 本分区共享输入和缓存
   └─ c02_Tracking/
      ├─ mlflow.db                   # 参数、指标、状态
      ├─ artifacts/                 # 配置快照、选定模型与预测
      └─ staging/                   # 代码包和下载暂存
```

Notebook 保存组合函数和显式启动／回收单元格；代理只加载标记定义。`config.yaml` 是基础参数唯一编辑位置，对照列表仅描述覆盖值。以下 `???` 必须由具体实验填写，不能作为运行默认值：

```yaml
scope:
  period: {start: "???", end: "???"}
  train: {start: "???", end: "???"}
  test: {start: "???", end: "???"}
  interval_closed: "???"
  timezone: "???"
  read: {start: "???", end: "???"}
  adjustment: {start: "???", end: "???"}
input: {}
parameters: {}
execution:
  target: local
```

无复权时须显式说明 adjustment 不适用。验证集、标签边界或滚动训练按研究需要加入。首次运行保存 scope 快照和摘要；修改范围／切分必须新建分区，不在扫描中覆盖 scope，不跨分区引用研究产物。

分区内缓存仍需核对输入内容、品种、复权口径、方法及依赖代码、参数和拟合条件。train/test 默认按日期选取同一表。普通中间量只在内存，昂贵且复用的结果进入 `c01_Data`；每次对照记录小指标，模型和全量预测按目的选留。

## 工具分工与云计算

| 对象 | 工具与归属 |
|---|---|
| 基础配置、参数覆盖 | Hydra 组合具体配置，Notebook 显式循环对照 |
| 一个时间分区 | 独立本地 MLflow SQLite 数据库 |
| 一个比较主题 | MLflow Experiment |
| 一组参数与种子的一次执行 | MLflow Run，记录输入、配置、代码版本 |
| 一项云作业 | alipai 提交、查询；DLC job ID 记入 Run |
| 共享特征 | `c01_Data`；MLflow 只记路径和摘要 |
| 选定模型、预测、图表 | 本分区 MLflow artifacts，避免第二份副本 |

Hydra [Compose API](https://hydra.cc/docs/1.3/advanced/compose_api/)适合 Notebook，不自动提供 multirun 或 DLC 调度。MLflow [数据库存储](https://mlflow.org/docs/latest/tracking/backend-stores/)集中小记录，大文件仍单独保存。scope 校验、缓存身份及回收由实际实验明确实现；当前骨架未提供自动缓存和调度服务。

本地和云端调用同一组定义。提交前固定代码与已解析配置，代码包包含需要的 Notebook、代理和加载器，排除 demo、大型已保存输出和本地凭据。云端显式读取输入通道、调用计算函数；提交代码仅在本地运行。[PAI 输入输出说明](https://help.aliyun.com/zh/pai/developer-reference/submit-a-training-job)

```text
OSS/research/<scope_id>/
├─ inputs/<input_id>/
└─ runs/<run_id>/
   ├─ code/
   └─ output/
```

过程：固定配置并创建 Run → 上传或复用本 scope 输入 → 提交并保存 job ID → 云端输出指标 JSON 和选定成果 → 本地暂存下载、复读及摘要校验 → 持久保存并更新原 Run → 标记远端输出可清理。

本地 SQLite 不由云任务共同写入，不合并云端数据库。Notebook 关闭后按已保存 job ID 查询和回收；云成功但未回收须可辨认。未回收输出、活跃输入和失败现场不按普通缓存删除。长任务遵守[根级授权与可见监控](../AGENTS.md#长时间任务的人工启动后台执行与可见监控)。

环境及实测边界见 [环境说明](../environment/README.md)：已验证云端显式依赖及 JSON／Parquet 回传，尚不能据此认定代理在云端、完整实验链路或本地 Hydra/MLflow 已验证。本次未安装依赖、创建数据库或提交云任务。

## 数据和维护边界

正式 silver 通过 `settings.futures_lake_root` 按[湖仓规则](../02_Market_Data/a02_Lake/AGENTS.md)和[数据契约](../config/data_contracts.py)只读使用。demo、缓存及成果保存在研究目录，使用局部明确的 Arrow Schema；不默认写入湖内 gold，不改变17张稳定 silver 表。

代码、配置和方法 demo 数据进入 Git。[.gitignore](../.gitignore)统一放行 `a01_Methods/**/*_demo.parquet`，[.gitattributes](../.gitattributes)将其声明为二进制文件；其他格式的正式 demo 按实际入口补充对应规则。实验数据、SQLite 及附属文件、artifacts 和暂存包按所属目录规则留在本地。忽略规则不清理磁盘。缓存清理保护活跃输入和保留成果依赖；严格复现还需可取得的原输入和代码版本。

原 `04_Feature_Engineering` 五个工作区文件原样暂存在[草稿目录](../00_draft_collection_02/feature_engineering_before_research/)，摘要见 [PRESERVATION.json](../00_draft_collection_02/feature_engineering_before_research/PRESERVATION.json)。其去留、复用与最终归属待用户按[草稿区规则](../AGENTS.md#草稿区与文件收纳规则)确认。旧 README 链接及启动命令属于历史路径；缺失 `c00_lakehouse` 和日线分区缺少 `underlying_code` 的两个阻塞未修复。正式入口不从暂存目录导入，用户已删除的旧 AGENTS 未恢复。

加载器[离线测试](../00_draft_collection_02/tests/test_research_notebook_loader.py)在仓库根使用 v2 执行：`python -B -m unittest discover -s 00_draft_collection_02/tests -p test_research_notebook_loader.py`。测试只使用临时合成 Notebook，不读取湖或调用云 API。

同步入口：[根 README](../README.md)、[根 AGENTS](../AGENTS.md)、[研究 AGENTS](AGENTS.md)、[环境模板](../.env.template)、[采集规则](../02_Market_Data/a01_Collection/AGENTS.md)、[.gitignore](../.gitignore)、[.gitattributes](../.gitattributes)。
