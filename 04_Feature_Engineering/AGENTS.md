# 适用范围与规范地位

- 本文件是 `E:\Latitude_Analytics_v2\04_Feature_Engineering` 整棵目录树的目录级 Agent 规则入口，适用于目录本身以及任意层级的当前和未来子目录。
- 本目录是从原量化交易目录独立出来的特征工程项目；本次只确认结构迁移和规范路由，不代表现有业务入口已经恢复运行。
- 本目录继承根目录 [AGENTS.md](../AGENTS.md) 的全部规则。任何读取 17 张稳定 silver 表、解释 Schema metadata、构造 PyArrow Dataset 或验证字段与分区的改动，还必须读取并遵循 [03_Futures_Database/AGENTS.md](../03_Futures_Database/AGENTS.md) 和 [config/data_contracts.py](../config/data_contracts.py)。

# 当前结构迁移状态与已知阻塞

- `b01_main_continuous_daily.ipynb` 及其 PythonExporter 导出脚本仍导入迁移前 a01 中、当前已经不存在的 `c00_lakehouse`。因此该入口当前不是可运行状态；不得恢复 `c00_lakehouse.py` 薄封装，也不得以路径兼容层掩盖阻塞。
- 同一入口把 silver Hive 分区硬编码为 `exchange_code/year/month` 并同时用于品种日历、合约日历和 `fact_futures_daily`。当前 `fact_futures_daily` 的权威及正式物理分区是 `exchange_code/underlying_code/year/month`，而两张 calendar 仍是三层分区；旧定义会使日线 Dataset 缺失 `underlying_code` 分区字段并导致后续读取失败。
- 未来恢复必须在 Notebook 源文件中移除对采集项目内部实现的依赖，按三张权威 Schema metadata 分别构造表路径与 partitioning，并直接使用 PyArrow 读取；随后用默认 PythonExporter 重新生成同名 `.py`。至少在 `--help` 和正式湖只读集成均通过后，才可把该入口描述为恢复运行。
- `b00_sync_notebook_exports.py --check` 只证明 Notebook 结构、PythonExporter 一致性和脚本语法；它不执行模块导入或 silver 读取，单独通过不得作为上述两个阻塞已经解除的证据。
- 未经用户另行授权，本次结构迁移不得顺带修复业务代码、改写 gold 语义或执行该入口。

# `.ipynb` 与 `.py` 双轨

- 本目录中项目自研的特征工程业务工作流入口必须同时保留同目录、同基名的 `.ipynb` 与 `.py`。Notebook 是唯一允许直接编辑的业务源文件；同名 `.py` 必须由标准 `latitude` 环境中的默认 `nbconvert.exporters.PythonExporter` 完整生成并逐字节一致。
- 修改业务 Notebook 后使用本目录的 `b00_sync_notebook_exports.py --write` 生成脚本，交付前使用 `--check` 复核。同步入口是运维与验证脚本，不要求同名 Notebook。
- `.py` 不得直接修改或格式化。参数、默认值、silver 输入、Schema、分区、过滤、更新水位与 gold 输出语义必须先在 Notebook 中完成，再重新导出。
- `__init__.py`、配置模块、测试、验证脚本和同步脚本默认不要求同名 Notebook；实际承担独立特征工程业务任务的文件不得套用该例外。

# silver 消费与 gold 边界

- silver 表名、字段、主键和分区只能从 `config/data_contracts.py` 的具名权威 Schema 及 metadata 读取，不得依赖采集目录私有模块或另建平行定义。消费者信任生产者正式提交已经证明的表级业务质量，只检查权威物理契约及本工作流额外需要的局部前提。
- `.env` 的 `FUTURES_LAKE_ROOT` 仍是正式湖仓唯一定位来源；本目录通过 `config.settings.settings.futures_lake_root` 读取其 `silver` 输入并定位实验性 `gold` 输出，不保存另一个正式路径常量。
- gold 不属于 17 张稳定 silver 表，也不进入 `config/data_contracts.py` 或数据库级标准读取 Demo。表名、字段、分区和更新方式由本目录工作流局部定义、校验和说明。
- 当前 b01 主力连续合约、log 双向复权、期限结构边界和两个已知运行阻塞的操作说明以 [README.md](README.md) 为准；不得把结构迁移解读为对这些实验语义的重新验收。

# 规范索引与同步

- [根目录 AGENTS.md](../AGENTS.md)：项目级变量命名、最小改动、运行环境、根目录定位与规范路由。
- [.env.template](../.env.template)：正式湖仓根路径 `FUTURES_LAKE_ROOT` 的权威模板。
- [02_Futures_Lakehouse/AGENTS.md](../02_Futures_Lakehouse/AGENTS.md)：上游生产与运维目录规则。
- [数据采集 README](../02_Futures_Lakehouse/README.md)：17 张稳定 silver 表的生产粒度、分区与运行边界。
- [03_Futures_Database/AGENTS.md](../03_Futures_Database/AGENTS.md)：字段、类型、Schema metadata、分区和直接 PyArrow 读取规范。
- [config/data_contracts.py](../config/data_contracts.py)：17 张稳定 silver 表的唯一可执行 Arrow Schema。
- [数据湖读取 Demo](../03_Futures_Database/read_futures_lake_demo.ipynb)：17 张稳定 silver 表的逐表契约化读取示例。
- [README.md](README.md)：当前实验说明、结构迁移状态与已知阻塞。
- 修改本文件或 README 中具有规范作用的内容时，必须同步检查根索引、上游生产规范和数据库规范；不得形成孤立约束或错误的可运行声明。
