# staging 安装与恢复抽象：实现记录

2026-09-27 已经用户明确确认正式归属并接入。

## 正式代码

- [a00_04_staged_path_transaction.py](../../02_Futures_Lakehouse/a00_04_staged_path_transaction.py)：唯一正式实现。
- [b01 Notebook](../../02_Futures_Lakehouse/a01_Futures_Market_Data/b01_trade_calendar.ipynb)、[b02 Notebook](../../02_Futures_Lakehouse/a01_Futures_Market_Data/b02_futures_variety_calendar.ipynb)：已接入；同名 Python 通过标准入口导出。
- 根目录 AGENTS、湖仓 AGENTS、湖仓 README 已同步编号、索引和当前接入范围。
- 本目录中的同名模块和 Notebook/Python 是本轮已验证候选的审阅快照，正式入口不依赖草稿目录。本文是实施记录，不新增规范。

## 责任与变化

共享操作通过 `with StagedPathTransaction(...) as transaction` 记录一组实际移动；调用方逐项 `transaction.replace(target_path=..., staged_path=...)` 并直接正式复读。异常导致整组倒序恢复，只有完整成功后才完成提交。`staged_path=None` 是显式删除，缺失来源不能暗示删除。

合并、选择范围、业务校验、Parquet 写入、物理复读、主键/行数检查、逐叶进度和业务返回值继续留在 b01/b02。没有添加完整历史业务复查。b02 空表 marker 先写入 staging，然后作为同组文件安装、正式复读；失败时删除新 marker。b01 清理失败的新叶，b02 保留隔离的新叶；恢复失败时保留旧备份。

原 `shutil.move` 改为同一文件系统内的 `os.replace`，不自动退化为复制后删除；每次成功移动后才登记旧目标/新目标状态。目标边界、重复/父子目标和显式删除语义由共享操作负责。它没有跨目录原子可见性、并发协调、进程中断恢复或自动重试。

b01 的 commit 单元格由 290 行变为 229 行，b02 由 540 行变为 485 行（含注释和空行）。新增共享模块并非零成本；本次收益是两份安装/恢复逻辑由一个正式位置维护，不能把局部删减 116 行说成全项目净减少 116 行。

## 验证证据

- 标准 latitude 环境预检通过。
- 在正式导出和正式共享模块上运行 44 项测试通过：原 33 项业务/提交语义回归，加 11 项共享文件事务测试。既有故障注入点从 shutil.move 移至实际 os.replace 边界；业务断言保留。
- 10 组抽象前/后差分比较通过：b01 空输入、跨年初建、尾部追加、跨年修订；b02 空库 marker、多交易所多月份初建、闭区间替换、部分月份清退、整表清空、新月份追加。返回值与全部 Parquet 文件字节逐项一致。
- 合并后业务校验次数、正式逐文件物理检查次数和 b02 上游读取次数仍由既有断言验证，未增加重复完整校验。
- 标准 `a00_02_sync_notebook_exports.py --write` 和最终 `--check` 均通过，workflow_count=19。
- 原 Notebook 输出、执行次数、单元格 metadata 和 Notebook metadata 保持；除导入与 commit 单元格之外的代码逐字一致。每份 Notebook 另更新总流程、提交说明和提交局部流程。
- 用本机 PyCharm 2026.1.2 的 Mermaid renderer 解析及渲染 4 张变更流程图成功。
- 其余 17 个业务入口的 Notebook/Python 与本轮快照逐字节一致。
- `git diff --check` 报告默认 PythonExporter 生成的 `# ` Markdown 空行尾空格；按双轨规则保留标准导出，已确认没有其他尾空格问题。Notebook 保持 LF。
- 未认证、调用真实 API 或读写正式湖。测试只使用临时目录和模拟来源。

候选差异见 [b01.diff](b01_trade_calendar.diff)、[b02.diff](b02_futures_variety_calendar.diff)；正式文档改动见 [documentation_changes.patch](documentation_changes.patch)。构造、差分脚本依赖本轮 baseline.json 指向的本机临时快照，属于本轮审阅材料，不是长期生产入口。
