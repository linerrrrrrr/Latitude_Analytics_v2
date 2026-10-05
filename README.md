# Latitude_Analytics_v2

本仓库包含市场数据采集、正式数据湖、研究项目和实验工作流。目录级规则由 [AGENTS.md](AGENTS.md) 统一索引；协作中的纠错与执行状态遵循[对应规则](AGENTS.md#协作纠错与执行状态)。

本地入口统一使用 v2，安装、alipai 依赖例外和云端环境边界见[环境说明](environment/README.md)。

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
04_Research/                 # 方法、可串联 demo 和按时间分区的实验
  a01_Methods/               # 主力识别、分钟复权方法与 RB demo
  a02_Experiments/           # 时间范围及训练／测试切分；尚无具体分区
  referance/                # 研究参考资料、文献和资料包
05_Old_Projects/             # 只读历史归档，含 collection_maintenance_20261002.zip
config/                      # 项目共享配置与可执行数据契约
environment/                 # 环境安装、依赖清单、验证代码与证据
  alipai/                    # SDK 验证入口与分批结果
  rebuild_20261001/          # 已有环境重建证据
```

- 采集入口、GUI 启动和维护命令见 [采集说明](02_Market_Data/a01_Collection/README.md) 与 [运维说明](02_Market_Data/a01_Collection/operations/README.md)。
- 来源质量、连接排查和采集本地测试见 [采集检查](02_Market_Data/a01_Collection/checks/README.md)。
- 正式湖通过 `.env` 中的 `FUTURES_LAKE_ROOT=02_Market_Data/a02_Lake` 定位；配置说明见 [.env.template](.env.template)，读取示例见 [read_futures_lake_demo.ipynb](02_Market_Data/a02_Lake/read_futures_lake_demo.ipynb)。
- 草稿区材料经用户确认后删除或移至最终目录；全部待办处理完且无新增材料时应为空，持续工作中允许暂时非空。价值判断、复用和处置边界见[草稿区规则](AGENTS.md#草稿区与文件收纳规则)。
- 探索 Notebook 的配套文件统一收纳到 `00_draft_collection_01/<notebook 名>/`；图表默认内嵌，独立文件显式导出，输出目录留在本地。目录规则与 `testing_10` 分钟数据的跨 Notebook 读取路径见[实验草稿区 AGENTS](00_draft_collection_01/AGENTS.md)。
- 固定产生的运行历史、日志和状态保存在产生它们的项目目录内；研究实验记录由所属研究项目保存。
- 研究结构、Notebook 代理、demo 引用及 Hydra/MLflow/DLC 接入见 [研究说明](04_Research/README.md)；当前已实现骨架和通用加载器，并实现主力识别方法、导入代理与 RB 只读 demo；复权方法已完成上游核对、日级分段、正式分钟读取和 log 域共同偏移复权，已用固定 RB 区间验证，保存全合约复权分钟 demo，缺失诊断保留。close 重采样方法已实现固定区间和逐分钟向后采样、间隔预览及全合约共同位置插值，并保存 RB 采样 demo；尚无具体实验时间分区。研究方法的代码和 demo 数据都纳入 Git。更新方法并重新生成 demo 后，将相关代码与对应数据一起提交。原特征工程文件按原字节保留在草稿区，位置与运行限制见研究说明。
