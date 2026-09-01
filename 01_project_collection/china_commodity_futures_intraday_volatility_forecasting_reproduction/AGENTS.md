# 中国商品期货日内波动预测方法复现项目规则

## 规范路由

- 本文件适用于 `01_project_collection/china_commodity_futures_intraday_volatility_forecasting_reproduction` 及其全部后代路径，并继承仓库根 [AGENTS.md](../../AGENTS.md)。
- [README.md](README.md) 是本项目目标、固定样本、统计口径、Notebook 清单和实施状态的权威说明；实现或修改任何统计项目时必须同步维护其状态和实际复现等级。
- README 同时固定第 09 项已经完整验收、允许第 10—12 项消费的准确 `run_id`；`LATEST` 只能帮助发现候选 run，不能替代 README 中的固定值。
- `operations/item09/` 是第 09 项长批次控制脚本与本地运行现场的唯一项目内位置；统计实现仍只存在于 Notebook，运行控制不得成为第二份模型来源。
- 任何正式 `silver` 读取都必须遵循 [03_Futures_Database/AGENTS.md](../../03_Futures_Database/AGENTS.md) 和 [config/data_contracts.py](../../config/data_contracts.py)。
- 如果本文件、README、数据库规范或权威 Schema 之间出现冲突，必须先消除冲突，不得选择性执行。

## 数据访问边界

- 本项目是正式 `silver` 的只读研究消费者：不得调用采集 API，不得写入或修改 `raw`、`silver`、`gold`。唯一允许持久化的研究成果是 `data/item09/<run_id>/` 下的项目专属、版本化、经验证 research artifact；它不是正式湖层，也不是可任意扩展的普通缓存或研究中间表。`operations/item09/control_history/` 只允许保存长批次状态、日志、隔离 runtime 与 partial Notebook，必须由局部 `.gitignore` 排除，且不得作为研究数据或下游输入。
- 正式湖仓根目录只能通过仓库 `.env.template` 规定的项目根定位代码和 `config.settings.settings.futures_lake_root` 获取，不得硬编码另一份正式路径。
- 表名、主键和分区顺序以 `config/data_contracts.py` 的表级 Schema metadata 为准；字段、Arrow 类型和 nullable 以其中的权威 `pa.Schema` 为准；不得复制并维护一套局部 Schema 常量。
- 第 09 项成果的局部 Parquet 结构、主键、指纹、原子提交和验收规则由本文件与 README 共同约束，不加入 `config/data_contracts.py` 的稳定 silver 清单，也不得被其他项目当作公共数据库表。成果结构固定为 `data/item09/<run_id>/`，stream checkpoint 只放在一个有明确恢复语义的 `checkpoints/` 子目录，最终表与提交文件直接位于 `<run_id>/`；不得恢复无区分作用的 `runs/streams/consolidated` 包装层。
- Notebook 必须使用直接 PyArrow 过滤读取，只选择当前统计项目需要的字段、品种与日期范围；不得通过全量读取规避过滤设计。
- 每本 Notebook 开篇必须浏览并展示直接参与表的权威 Schema metadata，随后才可读取业务数据。

## Notebook 实施边界

- 每次对话只聚焦 README 清单中的一个统计项目。只有在开始完整实现该项目时，才创建其对应 Notebook；不得预建空 Notebook。
- 新建 Notebook 必须在同一次对话中写明其统计目的、经济意义、论文公式或表图对应、代码步骤、验证、结果解释和限制，并使用 `Python (latitude)` clean kernel 从头执行成功。
- 每本 Notebook 必须自包含。允许重复直接且清晰的 PyArrow、Pandas、NumPy、SciPy、Statsmodels 或 `arch` 调用；不得建立共享 helper、隐藏的跨 Notebook 状态或普通持久化缓存来缩短 Notebook。第 09 项经本项目成果契约提交的 research artifact 是唯一例外。
- 第 09 项的模型、数据变换和成果写入逻辑只允许存在于 `09_rolling_variance_forecasts.ipynb`。项目内 worker 只能执行 Notebook 和发布控制状态，monitor 只能显示状态；不得恢复用于拼装 Notebook 的重复 builder。
- 除第 09 项成果这一显式契约边界外，后续统计项目需要前序结果时仍必须在当前 Notebook 内直接重新计算，不得读取未经授权的中间文件。第 10—12 项只可读取 README 固定且已经完整验收的第 09 项 `run_id`；固定 run 缺失、损坏或指纹不符时必须硬失败，不得静默重算或改读 `LATEST`。
- 代码按从上到下可读的线性工作流组织。只有独立语义、重要契约或实质复杂度能够证明时才允许引入局部函数；不得仅为缩短单元格或消除少量重复而抽象。
- Notebook 状态只能在文件已经完整实现并经过 clean-kernel 验证后从“待实现”更新；部分结果必须明确标记，不得写成已完成。

## 固定研究口径

- AL、CU、FU 是论文品种；RB 是论文外扩展品种，结果必须分开标注；SR 因无分钟事实而排除。
- FU 的旧产品段与稳定重启段是两个独立序列，不得跨停牌期连接收益、估计模型或建立滚动窗口。
- 不使用主力连续合约。按合约代码交割年月选择交易月后第 3 个月交割的固定月份合约，并保留换月标记和未经复权的基线连接。
- 只使用真实日盘 Session，排除夜盘；不填补休市分钟，也不得把隔夜、晨休或午休价格变化计入日内收益。
- README 中冻结的槽位、代理方差、GARCH、ARFIMA、滚动预测、DM–West 与 SPA 细节是项目基线。修改这些口径前必须取得用户明确确认，并同步 README、相关 Notebook 与受影响的规范索引。

## 验证与长任务

- Notebook 必须使用仓库标准 `latitude` 环境执行；不得根据裸 `python` 或裸 `pip` 判断依赖状态。
- 每个统计项目必须保留能独立复核的数据覆盖、边界、公式、参数约束和有限性断言；不能只展示最终表图。
- 预计超过十分钟的完整滚动模型或 bootstrap 批次，必须在对应对话中取得用户对边界清楚批次的明确授权，并遵守根规范中的 detached worker、用户可见监控、有界健康检查和失败保留要求。
- 第 09 项默认以 `read_validated` 模式读取 README 固定 run；`compute_or_resume` 仍是预计超过十分钟的有界批次，每一次启动或恢复都必须取得当次明确授权，不得因已有 checkpoint 自动续跑或自动重试。
- 第 09 项的 detached worker、可见 monitor 与控制现场必须从 `operations/item09/` 启动并写入其 `control_history/<control_run_id>/`；不得在仓库根目录创建或使用临时脚本、kernel spec、日志、状态或 partial Notebook。
- 第 09 项完整 `run_fingerprint` 必须由冻结方法、实际输入和标准环境的规范化 SHA-256 共同决定；目录 `run_id` 只取其前 16 个十六进制字符以保持 Windows 路径有界，manifest 与 commit 必须保存并复核完整 64 字符指纹。同一 `series_id × model_id` 是最小恢复 stream；每条 stream 的 Parquet checkpoint 必须先临时写入、复读验证并原子安装，最后才写对应 commit marker。缺失 marker、摘要不符、主键不完整或内容损坏均硬失败。
- 只有全部必需 stream 通过完整性、无未来数据、参数约束、有限性、行数和主键门禁后，才可生成 consolidated Parquet、manifest 与最后写入的 `_SUCCESS`。成功 run 不得原地覆盖、补写或修复；失败现场保留，后续只能在指纹完全一致时显式恢复，或以新 `run_id` 重新计算。
- 本项目不因方法复现需要而修改稳定 silver Schema、生产者或仓库依赖；若后续确有这种需求，必须作为新的明确任务处理。
