# 聚宽金融期货取数协议实验：第 01 轮 v4

本文件是对同目录原始证据的非权威解释。生产代码不得读取本文件形成业务规则；实验结论只有在全部五轮闭合并同步到项目规范、策略配置和正式实现后才生效。

## 记录身份

- 逻辑轮次：01
- 探针版本：joinquant_financial_futures_round_01_v4
- 探针开始时间：2026-09-03T12:35:47.489279+08:00
- 本地正式记录完成时间：2026-09-03T12:36:56.3795317+08:00
- 用户消息接收时间：当前客户端没有向 Agent 暴露精确时间，因此不作推测
- 证据来源：用户作为 UTF-8 文本附件返回的聚宽研究 Notebook 可见输出
- 门禁判定：round_01_passed；进入第 02 轮 Session、空响应、早期 TF 与就绪时点定向实验

目录时间直接取 JSON 的 run_started_at；该字段由 v4 在加载 jqresearch.api 和发出任何请求之前生成。

## 完整性

- raw_output.txt：21,408 字节；SHA-256 为 e6130c09eed160c4333db4cf07e546de52de83321fd835a24e77628c657621f4
- result.json：21,324 字节；SHA-256 为 32c5d050122181d119662f4fb135e28ab62c34c4b4c7ce17bdf6849f7fe6ecf9
- probe_source.py.txt：14,713 字节；SHA-256 为 41047842b4d6a080b3bc034cfc84b43ee626fb723b873da8e90be0f07f9db69a
- BEGIN 标记：完整且唯一
- END 标记：完整且唯一
- BEGIN/END 之间的 JSON：解析成功，并与 result.json 的首次落盘字节完全一致
- 目录轮次和版本：与 result.json 的 probe_version 一致
- 探针源码快照：与本轮执行时的 v4 草稿文件字节和 SHA-256 完全一致
- 12 个 case：全部 status=ok；没有信封外输出

## 已确认

- jqresearch.api 可以显式导入，get_price、get_extras、get_bars、get_ticks、history、attribute_history、get_all_securities 和 get_security_info 都是 callable。
- 单合约日线返回 DataFrame，DatetimeIndex 无时区，8 个字段全部为 float64：open、high、low、close、volume、money、pre_close、open_interest。IF2409.CCFX 在 2024-06-28 返回恰好 1 行，无重复、无空值。
- fill_paused=False 与 round=False 参数被接受；该样本与基线值一致。
- 双合约 panel=False 返回长表 DataFrame，以 RangeIndex 编号，显式包含 time、code 和 8 个行情字段；IF2409.CCFX 与 IH2409.CCFX 各 1 行。
- futures_sett_price 返回“日期索引 × 合约列”的宽表，本样本结算价为 IF 3423.6、IH 2361.4。
- futures_positions 同样返回宽表，本样本值与日线 open_interest 一致：IF 113011、IH 53729。
- IF2409.CCFX 上午分钟请求返回 120 行、7 个 float64 字段，无重复、无空值；索引从 09:31 到 11:30 且无时区。
- 请求区间 09:30—11:30 与放宽后的 09:29—11:31 得到相同的行数、首尾键和预览值。该结果与本地 Session 按 (09:30, 11:30] 展开相容。
- 分钟请求若 start_date 与 end_date 都只给 2024-06-28，则实际查询的是午夜零长度区间并成功返回零行，不能用日期字符串表达整日分钟请求。
- TF1303.CCFX / 2012-06-11 的日线和分钟请求都成功返回具有预期列但零行；这是来源成功空响应，不是异常或权限失败。

所有行情索引都是无时区的 DatetimeIndex。结合报告时区 CST 和中金所 Session 坐标，可以推断它们表示北京时间墙钟；该推断仍需在最终协议中明确写成解析规则，不能把 naive 时间直接当 UTC。

## 第 01 轮验收

第 01 轮要求的环境、API 模块与签名、单/多合约返回形状、日线/分钟字段、索引、时区表现、正常 IF 和早期 TF 已取得实际证据。v1—v3 的调用与命名空间问题均由后续版本显式解释，v4 所有真实请求成功，因此第 01 轮通过。

第 04 项整体仍未完成；本判定只允许进入第 02 轮，不冻结传输 Schema、批次容量或生产取数代码。

## 第 02 轮待验证

- 上午、下午、午间间隔与整日分钟请求的精确起止行为，以及切块时是否重复或漏键。
- 早期 TF 是仅首日为空还是整个合约生命周期均为空，并寻找后续首个有实际行情的 TF 样本。
- 成功空响应的稳定返回形状。
- 最新已完成交易日、前一日和当日进行中数据的实际可见性；若一次运行不足以确定就绪时点，继续以有时间戳的子版本观察，不凭推断冻结。
