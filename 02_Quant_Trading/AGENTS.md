# 适用范围与规范地位

- 本文件是 `E:\Latitude_Analytics_v2\02_Quant_Trading` 整棵目录树的目录级 Agent 规则入口，适用于目录本身以及任意层级的当前子目录、只读旧项目和未来新增目录。
- 本文件用于集中定义或索引量化交易目录的全部 Agent 规则；`.ipynb/.py` 双轨及 PythonExporter 只是当前其中一个专题，不代表本文件只负责双轨。
- 覆盖范围包括但不限于 `a01_Data_Collection`、`a02_Feature_Engineering`、`old_01_Data_Fetching` 及其全部后代目录；其中 `old_01_Data_Fetching` 只适用下述“旧项目只读保护”，不得把其他目录的可写规则套用于该目录。
- 除明确标注适用于只读旧项目的规则外，本文件的业务开发、双轨生成和交付规则只适用于 `old_01_Data_Fetching` 之外的可写目录。未来新增量化交易目录级规则时，应直接加入本文件或由本文件建立明确索引，不得另建无法从这里发现的孤立规范。
- 本目录继承根目录 [AGENTS.md](../AGENTS.md) 的全部规则。下级 `AGENTS.md` 或 README 可以增加更严格的要求，但未经用户明确同意，不得豁免、弱化或缩小本文件的覆盖范围。

# `old_01_Data_Fetching` 旧项目只读保护

- `E:\Latitude_Analytics_v2\02_Quant_Trading\old_01_Data_Fetching` 是仅供查询、对照和迁移思路参考的旧项目，不是当前开发或交付目录；该目录及其全部后代必须保持只读。
- Agent 只允许读取、搜索、比较和分析其中的内容。禁止在该目录内新增、修改、删除、移动、重命名、格式化或重新导出任何文件，也禁止补齐 `.ipynb/.py` 双轨、修复旧代码、写入缓存、测试输出、数据文件或新的规范文本。
- 下文的双轨、PythonExporter、完成条件和存量迁移规则均不得作为修改该旧项目的依据。需要复用旧项目思路时，只能把必要逻辑迁入当前可写目录，并在当前目录按适用规范实现。
- 不得执行可能在该目录内产生 `__pycache__`、Notebook checkpoint、日志或其他副作用的旧项目代码；只读检查必须使用不会写回该目录的方式。
- 只有用户明确点名该目录中的具体目标并明确授权修改时，才可在该次任务的授权范围内例外处理。笼统的“实现双轨”“同步整个 `02_Quant_Trading`”或一般重构请求不构成修改旧项目的授权。

# `.ipynb` 与 `.py` 双轨强制规则

- `02_Quant_Trading` 下除只读 `old_01_Data_Fetching` 外，所有项目自研的业务工作流入口都必须同时保留同一目录、同一基名的 `.ipynb` 与 `.py`，例如 `strategy.ipynb` 与 `strategy.py`。
- 业务工作流入口包括数据采集、清洗与加工、因子研究、策略研究、回测、建模、评估、报告和可视化等可独立运行的任务；当前由哪种文件起步不影响双轨要求。只读旧项目不属于本规则的改造范围。
- `.ipynb` 用于交互式分步执行、观察中间结果和记录业务解释；`.py` 用于批量执行、模块导入、测试和调度。两者都是正式交付物，未经用户明确同意不得删除、遗漏或用其中一轨替代另一轨。
- `.ipynb` 是唯一允许直接编辑的源文件；同名 `.py` 必须使用项目标准 `latitude` 环境中的默认 `nbconvert.exporters.PythonExporter` 完整生成，并与导出结果逐字节一致。
- 两轨必须保持核心业务逻辑一致，包括参数与默认值、数据源、字段和 Schema、路径、过滤条件、更新水位及输出语义。Notebook 输出以及 PythonExporter 未导出的元数据可以只存在于 `.ipynb`；Markdown、代码单元格顺序和展示代码若会进入导出结果，就必须同步出现在 `.py` 中。
- 不得直接修改或重新格式化导出的 `.py`；PythonExporter 生成的 shebang、编码声明、`# In[...]` 标记、Markdown 注释、空行与单元格顺序必须原样保留。
- 可复用逻辑应放入共享模块并由 Notebook 导入；同名 `.py` 仍只能是该 Notebook 的完整导出结果。

# 标准生成与验证

`a01_Data_Collection` 必须使用同目录的 `c00_sync_notebook_exports.py` 作为双轨生成与检查入口：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a01_Data_Collection/c00_sync_notebook_exports.py --check
```

- `--write` 使用默认 PythonExporter 重新生成本目录 `c01` 起始业务 Notebook 的同名 `.py`，随后执行 Notebook、语法和逐字节检查。
- `--check` 是默认模式，只检查而不写文件，适合交付复核和 CI。
- 该脚本只扫描自身所在目录且不递归；不得扩展到或传入只读 `old_01_Data_Fetching`。修改 `a01_Data_Collection` 的业务 Notebook 后，必须运行 `--write`；交付前必须运行 `--check`。
- `c00_sync_notebook_exports.py` 是运维与验证脚本，不是业务工作流入口，不要求创建同名 Notebook。

`a02_Feature_Engineering` 同样使用自身目录的 `c00_sync_notebook_exports.py`，只扫描本目录的
`c01` 起始业务 Notebook：

```powershell
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/c00_sync_notebook_exports.py --write
E:\anaconda3\envs\latitude\python.exe 02_Quant_Trading/a02_Feature_Engineering/c00_sync_notebook_exports.py --check
```

主力连续合约的表级契约、gold 存放位置、log 双向复权与期限结构边界以
[a02_Feature_Engineering/README.md](a02_Feature_Engineering/README.md) 为准。

以下代码是该脚本及本目录树内其他允许修改业务入口从 `.ipynb` → `.py` 的唯一底层生成方式。不得对只读旧项目执行导出，也不得传入自定义模板、preprocessor 或其他改变导出文本的配置：

```python
from pathlib import Path

import nbformat
from nbconvert.exporters import PythonExporter

notebook_path = Path("path/to/task.ipynb")
script_path = notebook_path.with_suffix(".py")

notebook = nbformat.read(str(notebook_path), as_version=4)
nbformat.validate(notebook)
script_source, _ = PythonExporter().from_notebook_node(notebook)
script_path.write_text(script_source, encoding="utf-8", newline="\n")

if script_path.read_bytes() != script_source.encode("utf-8"):
    raise RuntimeError(".py 与 PythonExporter 导出结果不一致")
```

# Agent 完成条件

- Agent 新增、实质修改、移动或重命名任何业务工作流入口前，必须先查找同名另一轨；若缺失，必须在同一次任务中补齐。
- 所有业务改动必须先落实到 `.ipynb`，随后用默认 PythonExporter 重新生成 `.py`。若任务起点是对 `.py` 的修改，必须先把改动迁回 Notebook，再重新导出；不得保留手改脚本。
- 交付前至少确认两个同名文件均存在，使用 `nbformat.validate()` 校验 Notebook，并逐字节比较 `.py` 与默认 PythonExporter 的输出；比较不一致不得交付。
- 导出后的 `.py` 还必须通过语法检查；能够安全执行时再运行相应验证。不得通过格式化导出的 `.py` 修复问题，必须回到 Notebook 修改并重新导出。
- 存量单轨工作流不要求在无关任务中一次性批量改造，但一旦进入该工作流做实质修改，就必须先补齐双轨再完成任务。
- 查找存量单轨、导出不一致或语法问题时必须排除只读 `old_01_Data_Fetching`；不得对该目录进行补轨、重导出或修复。

# 生产数据更新的手动触发规则

- `a01_Data_Collection` 的生产数据更新采用“人工启动、程序按状态批量执行”的半自动模式。每一批更新都必须由操作者在本机显式运行命令；脚本内部可以遍历分区、检查配额、记录完成状态并支持断点续拉。
- 禁止由 LLM/Agent 心跳、定时任务、线程唤醒、cron、后台守护程序或其他无人值守机制发起下一批生产更新。达到 API 配额边界后必须正常停止，等待操作者在额度重置后手动再次执行。
- LLM/Agent 不作为常规生产更新执行者，不得仅因历史对话中的“继续”或既有计划而在未来自动恢复拉取；可以检查代码、解释状态和提供人工执行命令。只有用户在当前交互中明确要求执行具体批次时，才可在该次授权范围内运行。
- 分钟线全部完成后的缺失检测和全链路验证同样由操作者手动触发，不得隐式串接到定时任务。

# 默认例外

- `__init__.py`、配置模块、共享库模块、测试、验证或运维脚本（包括 `a01_Data_Collection/c00_sync_notebook_exports.py`）、第三方或 vendored 源码、自动生成文件默认不要求创建同名 Notebook。
- 除上文明确只读的 `old_01_Data_Fetching` 外，若文件实际承担可独立运行的业务任务，或用户及所属目录规范明确将其列为业务工作流入口，则不得以文件名或所在目录为由套用例外。

# 规范索引与同步

- [根目录 AGENTS.md](../AGENTS.md)：项目运行环境、根目录定位和规范路由的项目级强制规则。
- [.env.template](../.env.template)：项目根目录定位代码的权威模板。
- [03_Futures_Database/AGENTS.md](../03_Futures_Database/AGENTS.md)：数据库目录级 Agent 规则，以及字段命名、跨引擎类型与数据湖 Schema 文本规范。
- [a01_Data_Collection/README.md](a01_Data_Collection/README.md)：当前国内期货数据采集链路、具体双轨清单及 `c00` 同步入口。
- [a02_Feature_Engineering/README.md](a02_Feature_Engineering/README.md)：主力连续合约、log 双向复权、期限结构边界及 gold 派生表规范。
- [config/data_contracts.py](../config/data_contracts.py)：silver 与 gold 数据湖 Schema 及 DataFrame 类型转换的可执行契约。
- 修改本文件时，必须同步检查根目录规范索引和受影响子目录的规范；不得形成只在本文件可见的孤立约束。
- 修改数据湖字段、Schema、生产者、读取者、类型转换或相关验证时，必须同时读取并遵循 `03_Futures_Database/AGENTS.md` 与 `config/data_contracts.py`。
