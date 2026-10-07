# 采集检查

本目录收纳数据源质量、连接检查和采集代码本地测试。来源检查确认接口能否连接、返回内容是否符合预期，区分权限限制、空数据、拦截页面和接口变化；本地测试用模拟来源和临时湖验证采集逻辑。在线检查需人工选择并启动，不写正式湖、不更新采集状态，也不自动加入日常采集批次。

遵循[采集规则](../AGENTS.md)和[环境说明](../../../environment/README.md)。以下命令从仓库根目录运行。

| 文件 | 用途 |
|---|---|
| [audit_jqdata_futures_api.py](audit_jqdata_futures_api.py) | JQData 期货 API 的返回结构、空结果、异常及账号权限检查 |
| [audit_public_scraper_interfaces.py](audit_public_scraper_interfaces.py) | 生意社、东方财富来源的响应质量、时延和覆盖检查 |
| [audit_night_session_attribution.py](audit_night_session_attribution.py) | JQData 日 K 与夜盘／日盘分钟边界、完整周期 OHLCV 的对照检查 |
| [tests/test_jqdata_connection.py](tests/test_jqdata_connection.py) | 模拟 Windows TUN 路由，验证 JQData 物理出口绑定 |
| [tests/test_audit_public_scraper_interfaces.py](tests/test_audit_public_scraper_interfaces.py) | 离线验证网页响应判定、限速和证据保存 |

`tests/` 统一保存来源检查与采集代码的本地测试，包括 [数据契约转换](tests/test_data_contracts.py)、更新规划、事务恢复、写入门禁和 Notebook 导出验证。`tests/audit_*.py` 是人工选择目标后才运行的只读湖审计，不随 `unittest discover` 执行。GUI 测试见 [operations 验证说明](../operations/README.md#验证)。

## JQData 接口检查

脚本使用 `.env` 中的 JQData 账号和仓库内 SDK，以代码列出的固定合约和日期样本查询，结果以 JSON 输出到终端。运行会访问真实服务并消耗相应查询额度；成功只证明所查样本及当时账号权限，不代表全量数据质量或全部 API 兼容性。

```powershell
& 'E:\anaconda3\envs\latitude_env_v2\python.exe' 02_Market_Data\a01_Collection\checks\audit_jqdata_futures_api.py
```

[2026-07-25 审计记录](../../../01_project_collection/jqdatasdk/FUTURES_API_AUDIT.md)保留当时环境、路径和权限结果；当前执行路径以上述命令为准。

## 夜盘归属检查

合约清单与周期对照的终端表格使用 `英文字段名（中文含义）`，遵循[展示规则](../../../AGENTS.md#数据呈现附带中文)；保存的 CSV 字段和历史报告保持原口径。

固定核对 RB、CU、AU 在 2025-02-10 至 2025-02-14 的日 K 与分钟数据。[已有结果](results/night_session_attribution_20250210_20250214/experiment_report.md)中，15 根日 K 均与完整夜盘加日盘周期的 OHLCV 一致；结论只覆盖这些样本，不推断节假日或全历史规则。

```powershell
E:\anaconda3\envs\latitude_env_v2\python.exe 02_Market_Data\a01_Collection\checks\audit_night_session_attribution.py
```

人工运行会查询 JQData，并在 `results/night_session_attribution_<运行时间>/` 保存 CSV 和报告；每次新建目录，不覆盖已有证据。

## 公网来源检查

公网审计脚本检查正式采集代码中直接使用 HTTP/网页抓取的三类来源：

1. 生意社商品现货与期货价格对比页；
2. 东方财富数据中心行业指数 JSON；
3. 东方财富数据中心 CPI、PPI、PMI、GDP JSON。

JQData 和 Tushare 是认证 SDK/API，不属于公网脚本的检查范围。该脚本不会写入
`FUTURES_LAKE_ROOT`，也不更改任何正式采集状态。

### 文件与历史证据

- `audit_public_scraper_interfaces.py`：低频只读探测入口；
- `tests/test_audit_public_scraper_interfaces.py`：纯本地响应结构测试；
- [SOURCE_RESEARCH.md](SOURCE_RESEARCH.md)：公开形态、指标定义、更新节奏和替代源调查；
- [REPORT_2026-08-24.md](REPORT_2026-08-24.md)：2026-08-24 实测结论；
- `results/*.json`：逐请求的状态、延迟、响应摘要与结构证据，不保存完整网页或业务数据；
  默认文件名含微秒，采用同目录临时文件原子安装，已存在的目标一律拒绝覆盖。

### 在线检查边界

- 串行调用，不并发；同一主机的所有请求（包括 `robots.txt`）默认至少间隔 5 秒并
  增加 0—1 秒抖动；最快间隔硬限制为 3 秒；单次最多重复 5 轮；
- 若 `robots.txt` 发布了更长的 `Crawl-delay`，以更长间隔为准；明确禁止目标路径时
  跳过该主机，robots 连接失败或非安全 HTTP 状态则停止该主机。HTTP 200 但不是
  robots 文本（Eastmoney 当前会返回 JSON catch-all）只记 `warning/inconclusive`，
  不是许可证明；业务探测仍按同主机最小间隔继续；
- 不自动重试业务请求；连接错误或任一响应被判为 `failed` 后，立即停止同一主机的
  后续探测。只有明确列入安全白名单的 `warning`（例如来源确认空、近期完整空页、
  合法结构中的缺失/恒值信号）允许继续低频探测；
- HTTP 200 不是成功判据。生意社还需核对标题日期、`table#fdata`、行数、编码和
  拦截词、八列商品行、“最近合约/主力合约”等列头语义、发布时间、现期差定义及
  业务单元格非空率；这些诊断不改变正式生意社 raw 链路“HTTP 200 原文字节归档”
  的契约；
- 东方财富还需核对 JSON envelope、分页、字段、请求 `INDICATOR_ID`、日期边界、
  重复键、数值类型与空值。边界探测在 `count>0` 时必须恰好收到一行；
- 每个响应使用 `passed / warning / failed` 三态。`warning` 是未能绿灯确认的
  inconclusive 结果，不等于失败，也不触发同主机停机；
- 缺失范围按族输出且最多保留 20 个区间：行业指数使用描述性工作日序列，未扣除
  各来源休市日；宏观报告按共享配置中的月度/季度频率生成理论来源日期。前者不能
  单独证明数据缺失。两类来源都只把截至最新返回 `REPORT_DATE` 的内部缺口列为
  `missing_observation_ranges`；其后的查询尾部单列为 `unassessed_pending_tail`，不得把
  尚未到发布日期的当期宏观报告误称为缺失。

### 运行

先确认标准环境：

```powershell
E:\anaconda3\envs\latitude_env_v2\python.exe 02_Market_Data\a01_Collection\b00_01_verify_runtime.py
```

低频性能样本（6 个业务逻辑请求，重复 3 轮；默认另有每主机一次 robots 请求）：

```powershell
E:\anaconda3\envs\latitude_env_v2\python.exe `
  02_Market_Data\a01_Collection\checks\audit_public_scraper_interfaces.py `
  --mode smoke `
  --observation-date 2026-08-21 `
  --repeats 3 `
  --min-interval-seconds 5
```

近期覆盖检查（5 个生意社工作日、全部 19 个行业指数、4 个宏观报告，各 1 次；
默认另有每主机一次 robots 请求）：

```powershell
E:\anaconda3\envs\latitude_env_v2\python.exe `
  02_Market_Data\a01_Collection\checks\audit_public_scraper_interfaces.py `
  --mode coverage `
  --observation-date 2026-08-21 `
  --repeats 1 `
  --min-interval-seconds 5
```

来源检查与采集代码本地测试使用模拟来源和临时目录：

```powershell
E:\anaconda3\envs\latitude_env_v2\python.exe -B -m unittest discover `
  -s 02_Market_Data\a01_Collection\checks\tests -v
```

只查某个配置指数的历史首尾边界（2 个请求，不下载全历史）：

```powershell
E:\anaconda3\envs\latitude_env_v2\python.exe `
  02_Market_Data\a01_Collection\checks\audit_public_scraper_interfaces.py `
  --mode coverage `
  --only-family index `
  --index-code MYSTEEL_TIN `
  --index-boundary boundaries `
  --repeats 1 `
  --min-interval-seconds 5
```

### 浏览器交叉验证

脚本证据不能替代浏览器观察。每次形成正式结论前，只对代表性页面和所有
`warning/failed` 项做一次低频浏览器复核：比较地址、页面标题/报告名、观测日期、
可见行数、请求指标 ID 及少量可见值，并把浏览器时间、观察结果和对应 JSON 文件名
记录到 `REPORT_2026-08-24.md`（后续批次使用自己的报告）。浏览器也不得用于循环刷新
或压力测试。

退出码与 JSON 顶层 `audit_status` 一致：`0=passed`、`2=warning`、`1=failed`。
`warning` 包括来源明确返回 9201 空响应、成功 envelope 的近期空页、配置频率下的
理论缺口及业务空值/恒值信号；它不是绿灯。无论退出码如何，脚本都会先完整写出证据，
供人工区分临时拒绝、反爬页、上游变更和真实空数据。
