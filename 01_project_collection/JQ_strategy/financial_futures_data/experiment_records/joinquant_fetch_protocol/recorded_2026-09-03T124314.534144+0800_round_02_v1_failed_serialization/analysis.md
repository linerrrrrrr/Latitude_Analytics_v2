# 聚宽金融期货取数协议实验：第 02 轮 v1 序列化失败

本文件是失败现场的非权威分析。该次运行没有形成完整 BEGIN/END JSON，不得据此认定任何请求块成功、失败、为空或完成。

## 记录身份

- 逻辑轮次：02
- 探针版本：joinquant_financial_futures_round_02_v1
- 聚宽探针开始时间：返回文本中不可获得；不得推测
- 本地失败记录时间：2026-09-03T12:43:14.5341442+08:00
- 目录时间语义：因 run_started_at 未进入返回文本，使用明确带 recorded_ 前缀的本地记录时间异常目录，不冒充探针执行时间
- 门禁判定：serialization_failed；优先从当前 Notebook 内存恢复报告，否则执行修正版 v2

## 完整性

- raw_output.txt：3,062 字节；SHA-256 为 37b1b16c0663776cc8ddfbb0b332817006342bef0970b107e4e64237fddac0f5
- probe_source.py.txt：13,998 字节；SHA-256 为 570c9a7a28315b5ad4f64591555f3e52ed9ebd8a32d4ff1be4727d85ebb733c1
- BEGIN/END 信封：未返回
- result.json：不存在
- 源码快照：与执行时第 02 轮 v1 草稿字节一致

## 原因

异常发生在脚本最后的 json.dumps(report)，所以不是 jqresearch.api 调用栈直接抛出的行情异常。v1 在 comparisons 中两次使用未限定名称 all(generator_expression)。聚宽 Notebook 的持久全局命名空间包含由平台或前序单元格注入的同名函数；在该 Python 3.6 / NumPy 1.14 环境中，这条调用没有保证使用 builtins.all，最终有 generator 对象进入 report，导致 JSON 编码失败。

该解释与错误类型、源码中仅有的两个 generator expression 以及 v3 已观察到的污染全局相符。由于没有完整报告，不能反向声称 18 个请求的实际状态。

## 处置

- 当前内核未重启时，使用独立恢复单元格重新计算两个 comparisons 字段并打印现存 report，不重复请求行情。
- 当前内核已丢失 report 或相关索引变量时，运行修正版第 02 轮 v2。
- 修正版不再使用未限定 all，也不会把 generator 写入报告。
