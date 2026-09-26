# Notebook 直接消化 ZIP 的入口修正

用户要求：“我要求不是通过powershell而是在notebook就可以直接消化掉压缩包”。

本地只读复核时间：2026-09-04T22:47:03.608803+08:00；目录时间取此时间，不是新的聚宽执行时间。没有重新取数，也没有开始建设清单第 10 项。

## 改动

现有 plan_financial_futures_fetch.ipynb 新增第 10 节“下载完成后，直接消化 ZIP 入库”。用户先运行开篇环境格，再运行该格即可；它直接 import 正式函数并传入 write=True，不执行 PowerShell、shell、子进程或手工解压，不复制导入算法。函数负责固定 inbox 文件的快照、校验、事务和幂等。

Notebook 分两阶段，下载前只运行到第 9 节；下载覆盖后才显式运行第 10 节，不在下载前 Run All。入库尝试前清空内存旧计划、旧代码和旧导入结果；代码生成格在计划失效时要求重新运行规划。成功后重新运行第 3—9 节回查。AGENTS.md 与 README 已同步；原有 CLI 保留兼容，但不再是用户的日常必经操作。

原有 21 个单元格只修改开篇说明和生成格的计划失效保护，新追加一个说明格和一个导入格。原有输出及用户末尾空单元格逐对象比较不变。导入器和规划器源码摘要均未改变。

## 真实 Notebook 调用及验证驱动异常

使用 nbclient 启动新的 Jupyter 内核，内核 argv 明确使用 E:/anaconda3/envs/latitude/python.exe，执行现有开篇环境格和新增入库格。导入格成功返回：随后验证格已通过 status=already_imported、database_write_performed=False、fetch_plan/旧代码为空以及生成格旧计划保护四组断言。

验证格随后在读数据库摘要时引用了未在内核中定义的 database_path，发生 NameError。此变量只属于外层测试驱动，不属于正式 Notebook 入库代码。完整错误和测试源码分别保存在 kernel_validation_failure.txt、kernel_validation_source.py.txt。错误发生前的控制流证明上述断言已通过；但驱动未成功输出其最终报告，没有单独落盘该次 import_report 的完整值和开始／完成时间，因此不补造这些字段，也不把整套驱动描述为完全成功。

错误后没有重新调用导入函数。只读复核确认压缩包、数据库摘要与调用前及第 09 项证据完全相同，四表计数仍为 1 个批次、6 个观察、1 条日线、240 条分钟，staging 没有运行遗留。正式 Notebook 全部代码格语法通过，入库格没有 shell 调用。详情见 readonly_verification.json；测试只读复核已通过。

新单元格实际调用的是已经提交的真实包，证明 Notebook 入口与幂等流程衔接；它不冒充新批次插入测试。新批次的事务验证仍由既有第 08 项测试和第 09 项真实提交证据提供。Jupyter 启动时的 profile 权限警告与内核最终关闭提示一并保存在原始日志；未另行修改用户 profile。

当前可直接在 Notebook 入库，不需要用户切换命令行。所有原始验证记录按此目录保留，不改写前一轮证据。SHA256SUMS 与 sidecar 用于复核本条记录。
