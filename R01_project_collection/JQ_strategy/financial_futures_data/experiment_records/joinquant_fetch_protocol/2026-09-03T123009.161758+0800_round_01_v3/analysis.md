# 聚宽金融期货取数协议实验：第 01 轮 v3

本文件是对同目录原始证据的非权威解释。生产代码不得读取本文件形成业务规则；实验结论只有在五轮闭合并同步到项目规范、策略配置和正式实现后才生效。

## 记录身份

- 逻辑轮次：01
- 探针版本：joinquant_financial_futures_round_01_v3
- 探针开始时间：2026-09-03T12:30:09.161758+08:00
- 本地正式记录完成时间：2026-09-03T12:31:52.0966772+08:00
- 用户消息接收时间：当前客户端没有向 Agent 暴露精确时间，因此不作推测
- 证据来源：用户作为 UTF-8 文本附件返回的聚宽研究 Notebook 可见输出
- 门禁判定：namespace_resolved；第 01 轮尚未闭合，下一步使用 jqresearch.api 执行行情小样本 v4

目录时间直接取 JSON 的 run_started_at；该字段由 v3 在任何 jqdata 导入与命名空间检查之前生成。

## 完整性

- raw_output.txt：1,155,301 字节；SHA-256 为 14b7d3b4fb1cc4216ebb9abd5f5199abc100859c15cac81aa576586ef2331830
- result.json：1,155,207 字节；SHA-256 为 9ceac430b757ad0f768ad9540a75f561201326f4580980a72e77cc48bce72c60
- probe_source.py.txt：11,582 字节；SHA-256 为 c7f6caba7c6af0ae99ddd9c7fb125fac802f0ee55392026a73a2d7d49720c4ef
- BEGIN 标记：完整且唯一
- END 标记：完整且唯一
- BEGIN/END 之间的 JSON：解析成功，并与 result.json 的首次落盘字节完全一致
- 目录轮次和版本：与 result.json 的 probe_version 一致
- 探针源码快照：与本轮执行时的 v3 草稿文件字节和 SHA-256 完全一致

raw_output.txt 原字节保留了 END 后的一个可见 1 和一个零宽字符；二者不属于 JSON 协议信封，也不参与实验结论。v3 为搜索命名空间输出了约 1.1 MB 的冗长模块清单；后续探针不会复制这种全量枚举方式。

## 已确认

- 平台研究 API 的实际模块是 jqresearch.api。
- jqresearch.api.get_price 存在，签名为 (security, start_date=None, end_date=None, frequency='daily', fields=None, skip_paused=False, fq='pre', count=None, panel=True, fill_paused=True, round=True)。
- jqresearch.api.get_extras 存在，签名为 (info, security_list, start_date=None, end_date='2015-12-31', df=True, count=None)。
- jqresearch.api 还暴露 get_bars、get_ticks、history、attribute_history、get_all_securities 和 get_security_info。
- get_bars 直接作为平台全局 callable 暴露；get_price 和 get_extras 没有直接暴露为当前全局，但可以从 jqresearch.api 解析。
- jqdata.apis 与 jqdata.apis.data 也含 get_price/get_extras 等实现；后续优先使用更符合聚宽研究平台身份的 jqresearch.api 边界，不依赖 jqdata 内部实现模块。

## 会话污染边界

本次并未在全新内核中执行。证据包括：运行开始时的全局命名空间保留了 v2 的 API_NAMES_TO_PROBE、DAILY_FIELDS、call_api 等变量；get_security_info 也已经在 preimport 阶段存在；本次 from jqdata import * 没有新增名称。因此：

- 不能用 v3 的 preimport/post-import 差异描述一个干净聚宽内核的默认状态。
- 可以使用模块对象和 callable 自身报告的 __module__ 与签名来定位 jqresearch.api；这些发现不依赖全局变量是否残留。
- v4 将显式导入 jqresearch.api，不再依赖内核预注入或前序单元格状态。

## 尚未确认

- jqresearch.api.get_price 和 get_extras 的真实请求是否成功。
- 日线与分钟线的返回类型、字段、索引、边界、行数和空响应行为。
- 早期 TF 行情、结算价、当前数据就绪时点、容量、配额和静默截顶。

## 下一探针改动

v4 将显式加载 jqresearch.api，并恢复 v2 中未实际发出的有界行情 case：正常 IF 日线与分钟线、双合约 panel=False、结算价/持仓附加数据、分钟边界对照，以及早期 TF 日线与分钟线。每个 case 继续独立捕获异常；不写文件、不修改本地数据库，也不进入第 02 轮。
