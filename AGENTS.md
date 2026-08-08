# 规范索引与同步要求

- [AGENTS.md](AGENTS.md)：项目运行环境、根目录定位与规范路由的项目级强制规则。
- [.env.template](.env.template)：项目根目录定位代码的权威模板。
- [02_Quant_Trading/AGENTS.md](02_Quant_Trading/AGENTS.md)：`E:\Latitude_Analytics_v2\02_Quant_Trading` 整棵目录树的目录级 Agent 规则入口，包含双轨、PythonExporter、`c00` 同步入口及 `old_01_Data_Fetching` 旧项目只读规定。
- [03_Futures_Database/AGENTS.md](03_Futures_Database/AGENTS.md)：`E:\Latitude_Analytics_v2\03_Futures_Database` 整棵目录树的目录级 Agent 规则入口，以及字段命名、跨引擎类型与数据湖 Schema 文本规范。
- [03_Futures_Database/read_futures_lake_demo.ipynb](03_Futures_Database/read_futures_lake_demo.ipynb)：八张 silver 表与两张 gold 派生表的契约化读取示例；每张表必须由独立代码单元格演示。
- [02_Quant_Trading/a01_Data_Collection/README.md](02_Quant_Trading/a01_Data_Collection/README.md)：数据采集链路、双轨同步入口、表粒度、主键、分区与更新水位规范。
- [02_Quant_Trading/a02_Feature_Engineering/README.md](02_Quant_Trading/a02_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及 gold 派生表规范。
- [config/data_contracts.py](config/data_contracts.py)：silver 与 gold 数据湖 Schema 及 Pandas、Polars、Arrow 转换的可执行契约。
- [00_draft_collection_02](00_draft_collection_02)：尚未经用户确认接纳为正式项目代码的脚本、测试、审计与验证工具的统一暂存目录。
- 修改以上任一规范、模板或可执行契约前，必须检查其余索引项，并在同一次变更中同步所有受影响的描述、示例与代码。
- 新增具有规范作用的文本时，必须将其加入本索引，并在其他相关规范文本中添加反向索引；不得形成无法从本索引发现的孤立规范。
- 若不同规范之间存在冲突，必须先消除冲突再完成任务，不得选择性遵循其中一份。

# 项目运行环境

- 本仓库的标准 Python 环境是名为 `latitude` 的 Conda 环境。
- 在本工作站上，使用 `E:\anaconda3\envs\latitude\python.exe` 运行 Python，或使用 `conda run -n latitude python`。
- 不要根据裸 `python` 或裸 `pip` 的结果判断依赖缺失；它们可能指向 Conda 的 `base` 环境。
- 报告环境或依赖问题前，先输出 `sys.executable`，并使用标准解释器检查相关包。
- 使用标准解释器执行 `python -m pip`，不要使用裸 `pip`。
- 运行 `E:\anaconda3\envs\latitude\python.exe 00_draft_collection_02\scripts\verify_runtime.py`，验证当前运行环境及核心 DataFrame 依赖。

# 草稿脚本与测试收纳规则

- 项目根目录不得新建或恢复 `scripts`、`tests` 目录；原有内容统一暂存于 `00_draft_collection_02/scripts` 和 `00_draft_collection_02/tests`。
- 未经用户明确确认正式归属的新脚本、测试、一次性迁移代码、审计工具与验证工具，必须先放入 `00_draft_collection_02`，不得散落在项目根目录或任意业务子目录。
- 将 `00_draft_collection_02` 中的代码提升到正式目录前，必须由用户明确确认目标文件及归属；不得由 Agent 自行认定为正式项目代码。
- 移入草稿区不等于用户已经接受该代码。若既有正式入口仍引用被移动文件，必须同步修正路径以避免断链，并在审计结果中明确列出该依赖，等待用户决定保留、改造或移除。
- 审计发现的其他疑似未获接受脚本，只能先报告清单；未经用户对具体目标授权，不得继续移动、删除或改写。

# 项目根目录定位约定

- `.env.template` 中记录的项目根目录定位约定，是 AI 修改本仓库时必须遵循的权威规范。
- 从子目录运行的代码如果需要先定位项目根目录，再导入 `config.settings.settings`，必须使用下面的标记文件搜索方式。不要引入其他项目根目录定位方法。

```python
import pathlib
import sys

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.settings import settings
```

# 目录级规范路由

- 在 `02_Quant_Trading` 目录树内工作时，必须读取并遵循 [02_Quant_Trading/AGENTS.md](02_Quant_Trading/AGENTS.md)；其中 `old_01_Data_Fetching` 是只读参考项目，未经用户针对具体目标明确授权不得修改。
- 在 `03_Futures_Database` 目录树内工作，或在任何目录修改数据湖字段、Schema、生产者、读取者、类型转换及相关验证时，必须读取并遵循 [03_Futures_Database/AGENTS.md](03_Futures_Database/AGENTS.md) 和 `config/data_contracts.py`。
- 目录级业务细则只在对应目录的 `AGENTS.md` 中定义；根文件只负责项目级通用规则与规范路由，不复制目录细则。
