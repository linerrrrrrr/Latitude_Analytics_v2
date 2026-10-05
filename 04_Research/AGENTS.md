# 研究目录规则

适用于 `04_Research` 全树，继承[根规则](../AGENTS.md)。目录和使用说明见 [README](README.md)，环境边界见 [环境说明](../environment/README.md)。当前已建立骨架、加载器与主力识别方法，包含递推函数、导入代理和 RB 只读 demo；尚无具体实验时间分区。

## 目录与文件

- `referance/` 保存研究参考资料、文献和资料包，保留各资料的日期与证据边界；资料中的建议不自动成为项目规则或已验收的研究成果。资料索引见 [README](README.md#研究参考资料)。
- `a01_Methods/` 保存方法。目录使用 `bNN_Name`，相关派生方法使用同级 `bNN_01_Name`、`bNN_02_Name`。编号决定浏览顺序，函数调用决定执行顺序。
- 每方法以同名 `.ipynb`、`.py` 两个维护文件为主，必要配置最多再增加一个。说明与 demo 代码写入 Notebook，不机械增加每方法 README、启动器或工具模块。
- demo 归所属方法，表格优先单个 `<方法Notebook名>_demo.parquet`，前缀使用 Notebook 去掉扩展名的完整名称（含编号），例如 `b01_MainContractSelection_demo.parquet`。下游显式读取上游 demo，不复制上游文件、不在导入时执行上游。生成条件、时间口径及上游摘要写入数据 metadata；摘要不匹配时明确提示重算。
- 方法代码和 demo 数据都纳入 Git；方法、生成条件或输入变化后，重新生成并验收受影响的 demo，将相关代码与对应产物在同一次提交中更新。表格产物由[根忽略规则](../.gitignore)按 `a01_Methods/**/*_demo.parquet` 放行，并由[文件属性](../.gitattributes)声明为二进制。其他格式的正式 demo 按实际入口同步加入收纳规则，不用宽泛的文件类型例外代替明确归属。
- demo 的生成身份覆盖输入、参数、相关代码、运行环境和输出内容。代码摘要须明确包含算法、局部 Schema 及实际依赖，纯绘图定义不参与数据产物身份；新增依赖时同步更新摘要范围和下游核对。已有文件复读后，若生成身份和实际数据均一致，保留其字节和生成时间；仅有变化时重新写入并复读验收。
- `a02_Experiments/` 的业务一级目录仅按时间范围及训练／测试划分建立，名称包含编号和可辨认的时间标签。准确条件以本目录 `config.yaml` 为准，不从文件名推断。
- 每时间分区以 `experiment.ipynb`、代理 `experiment.py`、`config.yaml` 三个维护文件为主。共享数据归 `c01_Data/`，运行记录归 `c02_Tracking/`；不为每组参数复制代码目录。
- `01_project_collection` 的专题项目及记录保留自身归属。旧项目保持只读。尚未确认正式归属的新测试暂存 `00_draft_collection_02/tests/`，去留、复用与最终归属遵循[根级草稿区规则](../AGENTS.md#草稿区与文件收纳规则)。

## Notebook 与代理

- Notebook 是算法和组合代码唯一来源。代理只调用 [a00_notebook_loader.py](a00_notebook_loader.py) 加载同名 Notebook 的 `export` 标签代码，不完整导出或复制算法。采集目录的 PythonExporter 规则不变。
- `export` 单元格仅放可安全导入的普通 Python 导入、常量、函数和类定义。数据读取、拟合、demo、图表、保存、认证及 DLC 提交放在未标记单元格，或封装为显式调用的函数；装饰器和默认参数也不得隐式启动这些操作。
- 加载器先校验、编译全部选中代码，再按顺序执行。它不是沙箱；运行异常不回滚已执行定义。未标记代码和已保存输出均不执行。不自动猜测导出、不执行依赖 demo、不热重载；修改 Notebook 后重启内核。
- 根定位仅使用 [.env.template](../.env.template) 的约定。命中项目根后，调用方显式将 `candidate_root / "04_Research"` 加入模块路径。代理和加载器不另搜根；云端从代码包根导入同样的模块，算法定义不依赖本地 `.env`。

## 时间范围与数据

- `scope` 明确整体范围、训练／测试区间、端点规则、时区、实际读取历史及复权计算范围；需要验证集、标签边界剔除或滚动拟合时一并固定。
- 首次运行保存 scope 快照与摘要。已有分区不得通过配置或 Hydra overrides 改变 scope；时间口径变化建立新分区。不同时间分区之间不引用或复用研究产物，即使数值恰好一致。
- 分区内复用仍须匹配品种、实际输入内容与角色、上游身份、复权口径、所用函数及依赖代码、参数，以及影响结果的拟合范围、随机设置和数值环境。文件存在不代表可复用。
- train/test 默认是同一数据的选取条件。普通中间量留在内存，只缓存昂贵且需要复用的结果。清理须保护活跃输入与保留成果依赖；不默认启动自动清理。
- 正式实验不直接消费可变 demo。silver 按[湖仓规则](../02_Market_Data/a02_Lake/AGENTS.md)和[权威契约](../config/data_contracts.py)只读使用。研究 Parquet 使用所属方法／实验的局部 Arrow Schema，写入后复读验收。
- demo、缓存、模型、预测和记录保存在研究目录，不写回正式 raw/silver，也不默认迁入湖内 gold。摘要只证明身份，严格重跑所需输入和代码版本必须仍可取得。

## Hydra、MLflow 与 DLC

- Hydra 组合配置；Notebook Compose API 的对照循环显式编写，不将其称作自动 multirun 或 DLC 调度。提交前固定本批已解析配置和实际代码，运行中修改不影响已提交任务。
- 每时间分区的 `c02_Tracking/mlflow.db` 保存本地记录；比较主题对应 Experiment，一组参数与随机种子的一次运行对应 Run。`artifacts/` 保存选定成果；共享数据只记引用和摘要，不逐 Run 复制。
- alipai 提交、查询、回收在 Notebook 中显式进行，不另建通用云调度器。OSS 使用 `research/<scope_id>/inputs/<input_id>/` 与 `research/<scope_id>/runs/<run_id>/`，研究输入不跨 scope 复用。代码包及下载暂存归本分区 `c02_Tracking/staging/`。
- 本地数据库不交给云任务共同写入。提交后立即保存 DLC job ID；云端返回指标 JSON 和必要成果，本地按原 Run 登记。云成功但未回收须标明待回收，验收完成后才标记成果已完成。
- 本地暂存下载、复读及摘要校验、持久安装、运行记录确认后，远端输出才可清理。共享输入须无活跃任务使用；失败现场和未回收输出不进入普通过期清理。MLflow 软删除不等于物理文件回收。
- 安装和验证范围以 [环境说明](../environment/README.md) 为准；骨架不代表本地 Hydra/MLflow、云端代理或完整链路已经验证。长任务继承根级有界授权、独立可见监控及失败停止规则，目录创建和测试不启动计费任务。

## 同步入口

修改本规则或加载契约时检查[根索引](../AGENTS.md)、[根 README](../README.md)、[研究说明](README.md)、[环境模板](../.env.template)、[湖仓规则](../02_Market_Data/a02_Lake/AGENTS.md)、[采集规则](../02_Market_Data/a01_Collection/AGENTS.md)、[.gitignore](../.gitignore)和 [.gitattributes](../.gitattributes)。不扩展为其他研究项目或采集双轨的批量改造。
