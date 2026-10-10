# 聚宽金融期货取数协议实验：第 01 轮 v2

本文件是对同目录原始证据的非权威解释。生产代码不得读取本文件形成业务规则；实验结论只有在五轮闭合并同步到项目规范、策略配置和正式实现后才生效。

## 记录身份

- 逻辑轮次：01
- 探针版本：joinquant_financial_futures_round_01_v2
- 探针报告初始化时间：2026-09-03T12:16:32.973484+08:00
- 本地正式记录完成时间：2026-09-03T12:19:12.1470467+08:00
- 用户消息接收时间：当前客户端没有向 Agent 暴露精确时间，因此不作推测
- 证据来源：用户作为 UTF-8 文本附件返回的聚宽研究 Notebook 可见输出
- 门禁判定：namespace_unresolved；第 01 轮尚未闭合，必须先运行命名空间诊断版 v3

目录时间取 JSON 的 legacy 字段 executed_at。该字段在 v2 报告对象初始化时生成，可解释为本轮报告初始化时间，但不是服务器证明的精确 API 请求开始时间。

## 完整性

- 原附件：6,929 字节，216 个 CRLF，无结尾换行；SHA-256 为 baa2c2a099056d99b288499ca040db91fa6d556b2705abbb5d5c1f75ca19a320
- raw_output.txt：6,714 字节；SHA-256 为 67541bc3d7cb1270fcafef455b3d938ab47543399550d3ecd3e741d0479ec584
- result.json：6,632 字节；SHA-256 为 f6ea4078ec38770b4d5ff97887cb369975a1c39b920a2c38e62e5d6412ccde12
- probe_source.py.txt：14,720 字节；SHA-256 为 af611d62ed404e3bd67671017b1e36863b2398a471b7c2941f8173a2b0803dbc
- BEGIN 标记：完整且唯一
- END 标记：完整且唯一
- BEGIN/END 之间的 JSON：解析成功，并与 result.json 的首次落盘文本一致
- 目录轮次和版本：与 result.json 的 probe_version 一致
- 探针源码快照：与本轮返回时草稿入口的字节和 SHA-256 完全一致

为了通过文本补丁进入版本控制，raw_output.txt 将附件的 CRLF 统一为 LF，并补一个结尾换行；未改变任何可见字符或 JSON 值。原附件的字节数、换行特征和摘要已在上方单独冻结，因此没有把规范化后的摘要冒充为附件原始摘要。

## 已确认

- from jqdata import * 成功完成，没有导入异常。
- get_all_securities 在 import 之前已经是平台全局 callable，签名为 (types=[], date=None)；它不是 jqdata 顶层模块属性。
- get_security_info 在 import * 后成为全局 callable，同时也是 jqdata 顶层模块属性，签名为 (code, date=None)。
- get_price、get_extras 和 get_query_count 在三个已检查位置——平台预注入全局、import * 后全局、jqdata 顶层模块属性——都不是 callable。
- 环境版本与 v1 一致：Python 3.6.7、jqdata 1.33.38、Pandas 0.23.4、NumPy 1.14.6，没有 PyArrow。
- IF2409.CCFX 与 TF1303.CCFX 的证券元数据结果与 v1 一致。

## 尚未确认

- get_price 和 get_extras 是否位于 jqdata 的其他公开命名空间，或者当前 Notebook 类型根本不暴露这两个接口。
- get_bars、history、attribute_history 等候选行情接口在该会话中的可用性与所属模块。
- 日线与分钟线的返回类型、字段、索引、边界、行数和空响应行为。
- 早期 TF 行情、结算价、当前数据就绪时点、容量、配额和静默截顶。

v2 的所有行情 case 都在真正发出请求前被本地解析器拒绝。因此本轮比 v1 更明确地证明了三个已检查命名空间中没有目标 callable，但仍不能解释为行情不存在、权限不足或服务端拒绝。

## 下一探针改动

v3 只做命名空间诊断，不立即重复全部行情请求：

- 同时检查 get_price、get_bars、get_ticks、history、attribute_history、get_current_data、get_extras 等候选函数。
- 记录平台预注入全局、import * 后全局、jqdata 顶层模块及已知 callable 的实际 __module__。
- 记录 jqdata 的公开属性、__all__、包路径、可发现子模块和当前已加载的 jqdata 模块。
- 检查已知 callable 所属模块是否还暴露目标行情函数。

只有定位到研究环境实际支持的行情接口后，才重新执行第 01 轮的小样本行情 case；尚未进入第 02 轮 Session 边界实验。
