# 公网爬虫接口来源与经济语义研究

> **非规范、只读诊断。** 本文是 2026-08-24 的来源研究草稿，只用于解释和审查
> 当前公网抓取链路。它不修改或替代 `AGENTS.md`、采集 README、
> `config/data_contracts.py`、`config/futures_lakehouse/external_market_entities.py`、
> `config/futures_lakehouse/macro_release_entities.py` 或任何正式生产契约，也不表示用户已接纳本文建议。

## 1. 范围与权威边界

本草稿项目只对当前正式代码直接使用的三类 HTTP/网页来源做低频、只读研究：

1. 生意社商品现货与期货价格对比页面；
2. 东方财富数据中心行业指数 JSON；
3. 东方财富数据中心 CPI、PPI、PMI、GDP JSON。

JQData 和 Tushare 是认证 SDK/API，不属于本次网页反爬实测范围。不过 SHIBOR、境外
期货和外部指数之间存在日历依赖，因此本文引用其官方定义和发布时间作为正式链路风险的
旁证。

项目内的权威定义仍然是：

- [`config/futures_lakehouse/external_market_entities.py`](../../../config/futures_lakehouse/external_market_entities.py)：19 个
  Eastmoney 指标 ID、稳定代码、名称和项目内分类；
- [`config/futures_lakehouse/macro_release_entities.py`](../../../config/futures_lakehouse/macro_release_entities.py)：8 个
  SHIBOR 与 17 个宏观系列、来源列、理论频率和项目可用日规则；
- [`config/data_contracts.py`](../../../config/data_contracts.py)：日历与事实表的字段、
  主键、分区、缺失和质量语义；
- [`R02_Market_Data/a01_Collection/README.md`](../README.md)：
  当前生产差集、确认空、正式复读和 raw 归档政策。

## 2. 接口在网上的公开形式

| 来源 | 面向人的公开形式 | 当前程序使用的形式 | 认证/SLA 状态 | 当前正式产物 |
|---|---|---|---|---|
| 生意社 | `https://www.100ppi.com/sf/` 及 `day-YYYY-MM-DD.html` 日页面 | 普通 HTML GET；页面内含 `table#fdata` | 无认证；公开网页，不是有 SLA 的开放 API | 仅 raw 原字节和 SHA-256，不解析为 silver 事实 |
| 东方财富行业指数 | `https://data.eastmoney.com/cjsj/hyzs_list_<INDICATOR_ID>.html` | `https://datacenter-web.eastmoney.com/api/data/v1/get`，`reportName=RPT_INDUSTRY_INDEX` | 无认证；网页 JSON 后端，未发现面向开发者的版本/SLA 承诺 | `fact_external_index_daily` |
| 东方财富宏观 | 数据中心 CPI/PPI/PMI/GDP 页面 | 同一 JSON 后端，报告名为 `RPT_ECONOMY_CPI/PPI/PMI/GDP` | 无认证；网页 JSON 后端，未发现面向开发者的版本/SLA 承诺 | `fact_macro_release` |

东方财富 `robots.txt` 探测得到 HTTP 200 的 JSON，而不是可解释的 robots 文本；因此本次
证据既不能证明明确允许，也不能证明明确禁止目标路径。访问策略应继续保持串行、低频，并
把服务条款和发布方许可作为独立合规问题处理。

HTTP 200 不是业务成功：生意社必须核对标题日期、主题、表结构、行数、编码和拦截页词；
东方财富必须核对 JSON envelope、`success/code/message`、分页元数据、字段、日期范围、
重复键和数值。东方财富本次虽返回 JSON，但 `Content-Type` 是
`text/plain;charset=UTF-8`，不能只靠 MIME 类型判定。

## 3. 生意社现货—期货页面与基差

### 3.1 经济定义

生意社页面明确写明：

```text
现期差 = 现货价格 - 期货价格（期货价格为结算价）
```

这与商品期货中常见的基差定义一致。基差可以为正或负，反映现货与指定期货合约之间的
价差，并受到交割地点、运输、仓储、品质、规格、税费、合约月份和供需的共同影响。

不能把页面上的所有现期差当作完全可交割的无摩擦套利价差。页面自身提示，例如焦炭现货
和期货规格不同，铁矿石分别使用湿吨和干吨，燃料油及烧碱也存在规格差异。

公开依据：

- [生意社现期表](https://www.100ppi.com/sf/)；
- [CME：Basis 为现金价减期货价](https://www.cmegroup.com/education/courses/introduction-to-grains-and-oilseeds/learn-about-basis-grains)。

### 3.2 频率与发布时间

页面按中国期货交易日形成日页面，本次页面标示 `16:30` 发布。对当天页面做完整性检查时，
建议不早于北京时间 `17:00`；更保守可在 `17:30` 后运行。正式 raw 生产契约仍以 HTTP 200
原字节及摘要复读为完成，不因本诊断建议而改变。

### 3.3 缺失与替代来源

当前正式链路没有“业务空页面”语义：HTTP 200 的任意正文，包括空正文，只要 raw 原字节与
SHA-256 正式复读一致，就记作 `success/count=1/passed`，`is_data_missing=false`。这是原文
归档契约，不等于页面业务内容正确。

若未来需要结构化基差，优先选择：

1. 从同一生意社页面提取现货价、合约和结算价，并保留规格说明；或
2. 将发布方现货基准价与交易所官方结算价按明确合约选择规则自行计算。

第二种方案在经济上是新数据产品，必须保存现货品种、地点、品质、单位、合约选择和换月
规则，不能冒充当前生意社页面值。

## 4. 19 个外部指数

下表中的“项目名称”来自当前配置；“公开核验解释”区分了可以由发布方确认的含义和仍需
核对的供应商映射。`shipping/energy/ferrous/nonferrous` 是项目内部分区，不是所有发布方
共同采用的官方分类体系。

| ID | 项目代码 | 项目名称 | 分类 | 公开核验解释与主要风险 |
|---|---|---|---|---|
| EMI00107664 | BDI | 波罗的海干散货指数 | shipping | Capesize、Panamax、Supramax 时租篮子的干散货综合指数 |
| EMI00107665 | BPI | 波罗的海巴拿马型运费指数 | shipping | Panamax 干散货船型与代表性航线运价 |
| EMI00107666 | BCI | 波罗的海海岬型运费指数 | shipping | Capesize 干散货船型与代表性航线运价 |
| EMI00107667 | BSI | 波罗的海超灵便型船运价指数 | shipping | Supramax 干散货船型与代表性航线运价 |
| EMI00107668 | BDTI | 波罗的海原油运输指数 | shipping | dirty tanker，即原油及其他未精炼油品运输市场 |
| EMI00107669 | BCTI | 波罗的海成品油运输指数 | shipping | clean tanker，即成品油及其他清洁货运输市场 |
| EMI01508580 | WTI_CONC | NYMEX WTI 连续商品指数 | energy | Eastmoney 的“美原油指数 CONC”；供应商连续拼接序列，不是单一可交割合约，换月方法尚未公开核实 |
| EMI00662539 | SUNSIRS_ENERGY | 生意社能源指数 | energy | 生意社能源板块价格指数代理；精确样本、权重和基期仍应向发布方核对 |
| EMI00018828 | MYSTEEL_COKE | 钢联中国焦炭价格指数 | ferrous | Eastmoney 页面标题为“焦炭指数:综合”；当前“钢联中国”归属需用 Choice 元数据再次确认 |
| EMI00662545 | SUNSIRS_STEEL | 生意社钢铁指数 | ferrous | 生意社钢铁板块价格指数代理；精确样本、权重和基期仍应核对 |
| EMI00064821 | MYSTEEL_STEEL_COMPOSITE | 钢联普钢综合价格指数 | ferrous | Eastmoney 页面标题为“普钢指数:综合”；近期待测表现为日频工作日量级 |
| EMI00064805 | XINHUA_IRON_ORE | 新华中国铁矿石价格指数 | ferrous | **身份存在冲突**：Eastmoney 页面只显示“铁矿石指数:综合”，近期返回日频工作日量级；真正“新华—中国铁矿石价格指数”公开说明为每周二发布。不能在未核对 Choice 元数据前断言两者相同 |
| EMI00662542 | SUNSIRS_NONFERROUS | 生意社有色金属指数 | nonferrous | 生意社有色板块价格指数代理；近期样本包含自然日值 |
| EMI00135907 | MYSTEEL_NICKEL | 钢联镍价格指数 | nonferrous | Eastmoney 页面显示“有色金属指数:镍” |
| EMI00135906 | MYSTEEL_TIN | 钢联锡价格指数 | nonferrous | **当前 ID 全量为空**；应用内浏览器打开 `hyzs_` 与 `hyzs_list_` 两个入口均显示“页面不存在或已删除”，行业菜单也没有锡。更像 ID 下线/改号或转发终止，不是锡市场没有价格 |
| EMI00135905 | MYSTEEL_ZINC | 钢联锌价格指数 | nonferrous | Eastmoney 页面显示“有色金属指数:锌” |
| EMI00135904 | MYSTEEL_LEAD | 钢联铅价格指数 | nonferrous | Eastmoney 页面显示“有色金属指数:铅” |
| EMI00135903 | MYSTEEL_ALUMINUM | 钢联铝价格指数 | nonferrous | Eastmoney 页面显示“有色金属指数:铝” |
| EMI00135902 | MYSTEEL_COPPER | 钢联铜价格指数 | nonferrous | Eastmoney 页面显示“有色金属指数:铜” |

### 4.1 发布方定义与频率依据

- [Baltic Exchange FAQ](https://www.balticexchange.com/en/who-we-are/faqs.html) 说明 BDI、
  BCI、BPI、BSI、BDTI、BCTI 的构成和用途；
- [Baltic Guide to Market Benchmarks](https://www.balticexchange.com/content/dam/balticexchange/consumer/data-services-/documents-/ocean-bulk/GMB.pdf)
  给出伦敦时间发布窗口：Capesize 通常 11:00，BDI/Panamax/Supramax 通常 13:00，
  BDTI/BCTI 通常 16:00；
- [Baltic 非发布日](https://www.balticexchange.com/en/data-services/market-information0/holidays---non-publications.html)
  说明其按自身工作日发布，不能用“周一至周五”完全替代发布日历；
- [CME WTI](https://www.cmegroup.com/markets/energy/wti-crude-oil-futures.html) 说明 WTI
  是全球原油基准，但不说明 Eastmoney `CONC` 的连续换月算法；
- [Mysteel 大宗指数编制说明](https://index.mysteel.com/dzinfo.html) 说明其大宗行业指数
  一般按行业产值加权，日度指数 17:30 发布；单个 Eastmoney ID 是否恰好使用这一方法仍需核对；
- [新华指数](https://indices.cnfin.com/index-xh08/) 说明“新华—中国铁矿石价格指数”
  每周二发布；
- [生意社 BPI 说明](https://100ppi.dazpin.com/cindex/) 可用于理解生意社定基商品价格
  指数，但不能自动证明三个板块 ID 使用完全相同的样本和权重。

### 4.2 安全更新时间建议

这些时间是诊断建议，不是正式调度授权：

- 生意社、Mysteel 相关国内指数：北京时间 18:00 后；
- Baltic dry：等待伦敦 13:00 发布并留出 Eastmoney 转发延迟；
- BDTI/BCTI：等待伦敦 16:00 后；
- WTI 连续序列：等待美国交易日结算和供应商转发，优先在北京次日上午采前一日期；
- 不应在北京时间当天白天把尚未到海外发布窗口的空响应永久记为 `empty_confirmed`。

### 4.3 精确替代源

| 当前系列 | 可用替代/旁证 | 是否可直接沿用原代码 |
|---|---|---|
| Baltic 6 指数 | [Baltic Exchange Data Services](https://www.balticexchange.com/en/data-services.html)，通常涉及许可 | 只有同一官方指数才可保持语义；需确认授权 |
| WTI_CONC | CME 官方结算自行按公开换月规则重建，或 EIA Cushing 现货 | 不可；连续期货与现货、不同换月法均是不同系列 |
| 生意社板块指数 | 生意社发布方页面 | 只有确认同一指数方法和基期才可 |
| Mysteel 指数 | [Mysteel/中国价格协会指数目录](https://www.mysteel.com/china-price-index/) | 需确认同一指数、单位、方法和许可 |
| 铁矿石 | 新华指数、CISA/CIOPI 或交易所价格 | 不可静默替换；这些是不同指数或不同市场对象 |
| 单金属 | Mysteel、LME、SHFE 官方数据 | 不可静默替换；指数、现货价和期货价语义不同 |

任何替代都应创建新的 `series_code/index_code/source` 或经过明确迁移，不得把代理值写在原
系列名下。

## 5. SHIBOR 与 17 个宏观系列

### 5.1 SHIBOR

SHIBOR 是由高信用商业银行报价形成的人民币同业拆出利率，是单利、无担保、批发性利率。
官方有隔夜、1 周、2 周、1 月、3 月、6 月、9 月和 1 年八个期限，每个业务日北京时间
11:00 发布。

项目稳定代码为：

```text
SHIBOR_ON, SHIBOR_1W, SHIBOR_2W, SHIBOR_1M,
SHIBOR_3M, SHIBOR_6M, SHIBOR_9M, SHIBOR_1Y
```

项目以 Tushare `pro.shibor` 获取，Tushare 权限页写明每日约 12:00 更新。项目理论日历当前
仅用 `weekday < 5`，因此会把落在周一至周五的中国法定假日也生成 required，这与官方
“业务日”并不完全相同。

公开依据：

- [CFETS/Shibor 官方定义](https://www.shibor.sh.cn/english/bmkshb/)；
- [CFETS 历史数据服务](https://www.shibor.sh.cn/shibor/dataservicesen/)；
- [Tushare 权限与更新时间](https://tushare.pro/document/1?doc_id=108)。

### 5.2 CPI

CPI 衡量居民消费商品和服务价格水平随时间的变动。项目为月频月末报告期，共 9 系列：

```text
CPI_NATIONAL_YOY, CPI_NATIONAL_MOM, CPI_NATIONAL_YTD,
CPI_CITY_YOY, CPI_CITY_MOM, CPI_CITY_YTD,
CPI_RURAL_YOY, CPI_RURAL_MOM, CPI_RURAL_YTD
```

其中累计来源列是以 100 为基准的指数，项目减 100 后保存为累计同比百分比。

官方定义：[国家统计局：居民消费价格指数](https://www.stats.gov.cn/zs/tjll/tjzs/202302/t20230224_1918473.html)。

### 5.3 PPI

PPI 反映工业企业产品第一次出售时出厂价格总水平的变动趋势和幅度。项目为月频月末报告
期，保存：

```text
PPI_YOY, PPI_YTD
```

`PPI_YOY` 读取 `BASE_SAME`；`PPI_YTD` 读取 `BASE_ACCUMULATE` 并减 100。

官方定义：[国家统计局：什么是 PPI](https://www.stats.gov.cn/zs/tjws/zytjzbqs/gyscz/202409/t20240910_1956355.html)。

### 5.4 PMI

PMI 是采购经理调查形成的月度景气扩散指数。制造业 PMI 由新订单、生产、从业人员、
供应商配送时间和原材料库存五项加权形成；50 是扩张与收缩临界点。项目保存：

```text
PMI_MANUFACTURING, PMI_NON_MANUFACTURING
```

第二个系列的中文名实际是“非制造业商务活动指数”，不应误解为与制造业 PMI 完全同构的
综合指数。

官方定义：[国家统计局：什么是 PMI](https://www.stats.gov.cn/zs/tjws/tjzb/202301/t20230101_1903972.html)。

### 5.5 GDP

GDP 是一国或地区所有常住单位在一定时期内生产活动的最终成果。项目不是保存 GDP 绝对
总量，而是保存季度末报告期的同比增速：

```text
GDP_YOY, GDP_PRIMARY_YOY, GDP_SECONDARY_YOY, GDP_TERTIARY_YOY
```

后 3 项分别是第一、第二、第三产业增加值同比。

官方定义：[国家统计局：什么是 GDP](https://www.stats.gov.cn/zs/tjws/tjzb/202301/t20230101_1903699.html)。

## 6. 理论频率、可用日与实际发布日期

| 系列 | 项目理论频率 | 当前项目可用日规则 | 官方/供应商事实 | 风险 |
|---|---|---|---|---|
| SHIBOR | 周一至周五 | 同日 | 官方业务日 11:00；Tushare 约 12:00 | 上午运行或法定假日可能过早确认空 |
| CPI/PPI | 月末 | 次月 9 日，若周末仅顺延到周一 | 国家统计局每年公布具体日程，常为 9:30；2026 年存在 11 日、14 日 | 静态规则可能早于实际发布 |
| PMI | 月末 | 一般月末；2 月报告期用 3 月 4 日 | 通常调查月最后一日 9:30，具体按年度日程 | 同日过早运行仍可能确认空 |
| GDP | 季末 | 季末后第 16 天 | 通常季后约 15 日，具体按年度日程；数据会修订 | 某些季度第 16 天仍可能早于发布 |

项目配置已经明确声明 `expected_available_date` 不是 API 实际发布日期。2026 年国家统计局
日程表显示 CPI/PPI 在部分月份为 11 日或 14 日；2026 年一季度 GDP 初步核算于 4 月 17 日
发布。这说明静态可用日不是可靠的“数据已经出现”证明。

公开依据：

- [国家统计局 2026 发布日程](https://www.stats.gov.cn/xw/tjxw/tzgg/202512/t20251224_1962137.html)；
- [国家统计局：PMI 一般在调查月最末日 9:30 发布](https://www.stats.gov.cn/hd/cjwtjd/202302/t20230207_1902267.html)；
- [国家统计局：季度 GDP 初步核算及修订](https://www.stats.gov.cn/sj/zxfbhjd/202604/t20260417_1963336.html)。

宏观最稳妥的调度不是固定“9 日/+16 日”，而是使用国家统计局年度发布日程并加供应商
转发缓冲；在未到正式窗口时保持 pending，不应把首次空结果视为永久完成。

## 7. 项目中的缺失语义

| 情形 | 当前项目语义 | 是否是业务缺失 |
|---|---|---|
| 网络超时、403、429、5xx、HTML 拦截页 | 请求错误/停止同主机后续检查 | 否，不能直接记 missing |
| Eastmoney 完整 JSON、精确格点有值 | 事实 1 行，`success` | 否 |
| Eastmoney 完整 JSON、精确格点无值 | 事实 0 行，`empty_confirmed + warning` | 是，但前提应是已经到真实发布窗口 |
| SHIBOR 完整响应中单一期限为 `None/pd.NA/NaN` | 事实 0 行，确认空 | 是 |
| 缺列、重复、越界、布尔、Inf、非法数值 | Schema/质量失败 | 否，不能伪装成空 |
| 生意社 HTTP 200 任意正文 | raw 复读成功后 `success/count=1/passed` | 当前契约固定不标缺失 |

宏观与外部指数事实都不写 null 占位行；格点只能形成 1 条有限值事实或 0 条确认空事实。
因此“过早请求后确认空”比普通延迟更危险：完成状态可能阻止日常入口再次拉取。

## 8. 浏览器核验结论

1. 生意社页面标题、日期、`table#fdata`、54 个商品行、16:30 发布时间和基差公式均能在
   面向人的网页中看到；
2. `EMI00135906` 的 `hyzs_` 与 `hyzs_list_` 页面均显示“不存在或已删除”，而相邻的镍、锌页面正常；
3. Eastmoney 行业菜单列出镍、锌、铅、铝、铜但未列锡；
4. Mysteel/中国价格协会公开目录仍列出“镍锡”，所以 `MYSTEEL_TIN` 空不等于锡市场没有
   价格，更可能是 Eastmoney ID 失效、改号或 Choice 转发覆盖终止；
5. `EMI00064805` 页面标题仅为“铁矿石指数:综合”，没有“新华”字样；项目稳定名称和发布方
   身份需再次核实；
6. Eastmoney 页面声明数据来源为 Choice，并提示不保证准确性、完整性和及时性，进一步说明
   网页后端不能当作有 SLA 的正式数据 API。

相关页面：

- [失效疑似 Tin 页面](https://data.eastmoney.com/cjsj/hyzs_list_EMI00135906.html)；
- [正常 Nickel 页面](https://data.eastmoney.com/cjsj/hyzs_list_EMI00135907.html)；
- [铁矿石综合指数页面](https://data.eastmoney.com/cjsj/hyzs_list_EMI00064805.html)；
- [Mysteel/中国价格协会指数目录](https://www.mysteel.com/china-price-index/)。

## 9. 替代源优先级

1. CPI/PPI/PMI/GDP：优先国家统计局发布稿与[国家数据](https://data.stats.gov.cn/)；
2. SHIBOR：优先 CFETS/Shibor 官网历史数据；
3. Baltic：优先 Baltic Exchange 官方许可数据；
4. WTI：优先 CME 官方结算，按透明换月规则另建连续序列；若使用 EIA 现货，必须另设系列；
5. Mysteel、生意社、新华：优先各自发布方页面或正式数据服务；
6. 任何代理指标必须使用新的稳定代码和来源标签，不得静默覆盖旧系列。

国家统计局说明其“国家数据”库可能在发布后约 3 个工作日更新全部进度指标；若需要发布
时点精确性，应抓取官方发布稿并保存版本，而不是假设数据库和 Eastmoney 同时更新。GDP
还存在初步核算、最终核实和历史修订，研究系统应保存数据版本或至少记录采集时点。

## 10. 待进一步联网核验的问题

- 19 个 Eastmoney ID 的 Choice 原始发布方、方法、单位、基期、起止有效期和非发布日；
- `EMI00135906` 是否存在替代 ID，还是该系列已经永久撤销；
- `EMI00064805` 的真实发布方是否为新华，若不是，当前项目名称应如何迁移；
- WTI `CONC` 的换月日、主力选择、复权或价差调整方法；
- 三个生意社板块指数的精确样本、权重、基期和修订政策；
- Eastmoney 对各发布方的日内转发延迟分布；
- 当前正式湖实际缺失范围。本文只报告低频网页样本，不扫描或改写正式湖；
- 对网页后端进行持续访问的服务条款和许可边界。

## 11. 建议的长期元数据

若用户另行授权修改正式链路，建议每个外部实体显式保存：

```text
publisher
public_page_url
source_indicator_id
economic_definition
unit_and_base
frequency
publication_calendar
publication_time
publication_timezone
availability_lag
active_from / active_to
revision_policy
license_note
fallback_series_code
```

同时把“尚未到发布窗口”“已到窗口但空”“接口拒绝/拦截”“来源实体失效”分成不同状态。
这比增加请求频率更能降低误报、重复访问和被临时封禁的风险。
