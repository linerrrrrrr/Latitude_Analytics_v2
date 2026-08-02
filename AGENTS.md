# 规范索引与同步要求

- [AGENTS.md](AGENTS.md)：项目运行环境、根目录定位与规范路由的项目级强制规则。
- [02_Quant_Trading/AGENTS.md](02_Quant_Trading/AGENTS.md)：`E:\Latitude_Analytics_v2\02_Quant_Trading` 整棵目录树的目录级 Agent 规则入口，包含双轨、PythonExporter、`c00` 同步入口及 `old_01_Data_Fetching` 旧项目只读规定。
- [.env.template](.env.template)：项目根目录定位代码的权威模板。
- [03_Futures_Database/AGENTS.md](03_Futures_Database/AGENTS.md)：`E:\Latitude_Analytics_v2\03_Futures_Database` 整棵目录树的目录级 Agent 规则入口，以及字段命名、跨引擎类型与数据湖 Schema 文本规范。
- [03_Futures_Database/read_futures_lake_demo.ipynb](03_Futures_Database/read_futures_lake_demo.ipynb)：八张当前数据湖表的契约化读取示例；每张表必须由独立代码单元格演示。
- [02_Quant_Trading/a01_Data_Collection/README.md](02_Quant_Trading/a01_Data_Collection/README.md)：数据采集链路、双轨同步入口、表粒度、主键、分区与更新水位规范。
- [config/data_contracts.py](config/data_contracts.py)：数据湖 Schema 及 Pandas、Polars、Arrow 转换的可执行契约。
- 修改以上任一规范、模板或可执行契约前，必须检查其余索引项，并在同一次变更中同步所有受影响的描述、示例与代码。
- 新增具有规范作用的文本时，必须将其加入本索引，并在其他相关规范文本中添加反向索引；不得形成无法从本索引发现的孤立规范。
- 若不同规范之间存在冲突，必须先消除冲突再完成任务，不得选择性遵循其中一份。

# 项目运行环境

- 本仓库的标准 Python 环境是名为 `latitude` 的 Conda 环境。
- 在本工作站上，使用 `E:\anaconda3\envs\latitude\python.exe` 运行 Python，或使用 `conda run -n latitude python`。
- 不要根据裸 `python` 或裸 `pip` 的结果判断依赖缺失；它们可能指向 Conda 的 `base` 环境。
- 报告环境或依赖问题前，先输出 `sys.executable`，并使用标准解释器检查相关包。
- 使用标准解释器执行 `python -m pip`，不要使用裸 `pip`。
- 运行 `E:\anaconda3\envs\latitude\python.exe scripts\verify_runtime.py`，验证当前运行环境及核心 DataFrame 依赖。

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
