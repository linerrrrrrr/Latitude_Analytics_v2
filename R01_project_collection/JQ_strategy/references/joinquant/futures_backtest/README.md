# 聚宽期货回测参考资料

本目录保存供后续策略设计、本地回测和聚宽策略脚本编写查阅的外部资料快照。它不是项目规范，也不定义数据库或生产流程；项目规则仍以仓库根 `AGENTS.md` 为准。

金融期货人工取数与项目本地数据实现已经路由到 [`financial_futures_data/AGENTS.md`](../../../financial_futures_data/AGENTS.md) 和 [`financial_futures_data/README.md`](../../../financial_futures_data/README.md)。本目录只提供外部接口材料，不覆盖该子项目已经确认的白名单、应有格点、缺失语义或后续实现契约。

## 本项目的阅读范围

- 只讨论期货，不把股票、基金或期权纳入策略范围。
- 商品期货数据默认读取本地数据库。
- 金融期货目前在本地数据库中缺失；数据由用户在聚宽研究环境（Jupyter）中手动拉取到研究目录，再手动下载到本地。本项目不调用 JQData 接口。
- 回测引擎、调度、下单与撮合、期货账户与持仓、保证金、手续费、滑点、交割和风险指标，按聚宽平台语义查阅。

聚宽把通用回测 API 与各资产类别放在同一份平台文档中，因此 `platform_api.md` 仍会看到股票示例；这些示例只是原始文档的一部分，不表示本项目扩大到股票。

## 文件

| 文件 | 用途 |
| --- | --- |
| [`official_snapshot/platform_api.md`](official_snapshot/platform_api.md) | 平台 API 全文。重点查阅“策略引擎介绍”“策略程序架构”“策略设置函数”“交易函数”“对象”“策略组合操作”“Tick 级策略专用函数”和“期货策略专用函数”。 |
| [`official_snapshot/futures_data.md`](official_snapshot/futures_data.md) | 期货数据页。包含合约信息、行情、bar、tick、持仓量、手续费、保证金、金融期货列表、主力合约与交割相关接口。金融期货章节用于聚宽研究环境中的人工取数，不代表本地 JQData 接口具有权限。 |
| [`official_snapshot/faq.md`](official_snapshot/faq.md) | 平台 FAQ。用于核对未来函数、回测与研究环境差异、期货主力/连续合约等边界。 |
| [`official_snapshot/source_html/`](official_snapshot/source_html) | 聚宽内容接口返回的原始 HTML 正文；同名 Markdown 由 Pandoc 机械转换，便于本地阅读和搜索。 |
| [`MANIFEST.json`](MANIFEST.json) | 官方 URL、抓取接口、抓取时间、转换方式、文件大小和 SHA-256。 |

建议先读 `platform_api.md` 中的回测引擎与期货专用章节，再读 `futures_data.md`；遇到具体环境差异时再搜索 `faq.md`。

## 来源与时效

规范入口 `www.joinquant.com` 经当前网络出口会返回地区限制页，但聚宽同域的 `test.demo.joinquant.com` 文档页面和内容接口可以公开读取。本次先建立匿名临时会话，再按页面返回的临时 token 调用内容接口，直接取得 `api`、`faq` 和 `Future` 三个模块的 HTML 正文；没有使用账号、JQData 凭据或第三方镜像正文。

对应的聚宽规范入口是：

- [聚宽平台 API 文档](https://www.joinquant.com/help/api/help?name=api)
- [聚宽常见问题](https://www.joinquant.com/help/api/help?name=faq)
- [聚宽期货数据](https://www.joinquant.com/help/api/help?name=Future)

HTML 正文抓取于 2026-09-02。Markdown 是从同批 HTML 机械转换的阅读版本；精确内容核对以 `source_html` 和 `MANIFEST.json` 中的哈希为准。由于读取的是聚宽同域演示入口而不是被地域限制的规范入口，正式采用行为敏感 API 前，仍应在 `www.joinquant.com` 恢复可访问后做一次差异核对。

## `config/jqdata_connection.py` 的边界

[`config/jqdata_connection.py`](../../../../../config/jqdata_connection.py) 不承担本项目的连接、认证或取数职责，本项目也不以它作为运行依赖。它只在出现“被聚宽拒绝访问”时作为已有实现参考；不得据此把 JQData 接口引入本项目，或把它解释成商品期货、金融期货的默认数据通路。

## 金融期货人工数据通路

当前约定的数据流是：聚宽研究环境（Jupyter）内取数 → 保存到聚宽研究目录 → 用户手动下载 → 本地读取或入库。`futures_data.md` 用于在研究环境中查期货取数接口，`faq.md` 用于查研究目录与文件交互说明。商品期货不经过这条流程，直接从本地数据库读取。

本资料包不自动登录聚宽、不自动下载研究文件，也不负责决定导出格式、Schema 或入库规则。项目本地落点、金融期货白名单和应有格点语义已经在 `financial_futures_data` 中确认；其余边界按该子项目建设清单逐项冻结。
