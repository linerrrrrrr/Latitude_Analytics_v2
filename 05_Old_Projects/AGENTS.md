# 旧项目只读归档规则

- 本目录是与 `00_draft_collection_01`、`02_Futures_Lakehouse`、`03_Futures_Database`、`04_Feature_Engineering` 同级的旧项目归档目录。
- 本目录及其全部后代只允许读取、搜索、比较、审计和迁移来源追溯；禁止新增、修改、删除、重命名、格式化、执行或重新导出归档项目文件。
- 禁止运行可能在归档目录中产生 `__pycache__`、Notebook checkpoint、日志、数据文件或其他副作用的归档代码。
- `a01_Data_Collection_pre_rebuild_20260809` 是重建前的当前采集实现快照；`old_01_Data_Fetching` 是更早的历史采集项目；`old_02_Feature_Engineering` 是旧特征工程项目。
- 归档代码中存在历史硬编码凭据；这些值只能作为安全审计证据，不得继续使用、复制到新代码、日志、metadata 或版本化配置中。凭据应在外部系统轮换或吊销。
- 当前采集项目只在 `02_Futures_Lakehouse/a01_Data_Collection` 中维护，不得从本目录原地恢复运行；当前独立特征工程项目位于 `04_Feature_Engineering`，也不得通过修改归档副本替代正式项目改动。

# 规范索引

- [根目录 AGENTS.md](../AGENTS.md)：项目级规范索引、运行环境和目录路由。
- [期货湖仓 AGENTS.md](../02_Futures_Lakehouse/AGENTS.md)：当前采集项目的双轨、执行、运维路由和交付规则。
- [当前特征工程 AGENTS.md](../04_Feature_Engineering/AGENTS.md)：独立特征工程项目的现行规范与结构迁移阻塞。
