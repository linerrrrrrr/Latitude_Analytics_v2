# 278 个字段的证据索引

每个字段保留权威契约元数据及独立证据状态；[机器可读完整索引](field_evidence_inventory.json)。未标来源对照的字段，不得因表的其他字段通过而视为已证明。

## dim_external_market_calendar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| dataset_name | string | 外部数据集类型 | 未逐字段独立业务证明 |
| entity_code | string | 请求实体代码 | 未逐字段独立业务证明 |
| observation_date | date32[day] | 预期观测或请求日期 | 未逐字段独立业务证明 |
| is_fetch_required | bool | 本格点是否需要发起请求 | 未逐字段独立业务证明 |
| requirement_reason | string | 需要或无需拉取的中文原因 | 未逐字段独立业务证明 |
| is_fetch_completed | bool | 请求、下游产物提交和复读是否完成 | 状态语义须限制解释 |
| fetch_result_status | string | 最近一次请求结果 | 状态语义须限制解释 |
| is_data_missing | bool | 应有下游产物但没有形成完整产物 | 未逐字段独立业务证明 |
| actual_record_count | int32 | 对应正式下游产物复读数量 | 未逐字段独立业务证明 |
| quality_status | string | 格点综合质检状态 | 状态语义须限制解释 |
| quality_reason | string | 质检结论中文解释 | 状态语义须限制解释 |
| fetch_run_id | string | 最近一次采集批次号 | 未逐字段独立业务证明 |
| fetch_completed_at | timestamp[us, tz=UTC] | 最近一次完成时间 | 未逐字段独立业务证明 |
| quality_checked_at | timestamp[us, tz=UTC] | 最近一次质量检查时间 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本日历行最后更新时间 | 未逐字段独立业务证明 |
| year | int16 | 观测年份 | 未逐字段独立业务证明 |
| month | int8 | 观测月份 | 未逐字段独立业务证明 |

## dim_futures_bar_calendar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| bar_frequency | string | 本格点规定的行情频率 | 未逐字段独立业务证明 |
| contract_code | string | 应采集合约代码 | 未逐字段独立业务证明 |
| exchange_code | string | 合约所属交易所 | 未逐字段独立业务证明 |
| underlying_code | string | 合约所属品种 | 未逐字段独立业务证明 |
| trading_date | date32[day] | 行情归属交易日 | 未逐字段独立业务证明 |
| session_number | int8 | Session 编号 | 未逐字段独立业务证明 |
| session_text | string | 分钟格点的原始 Session 文本 | 未逐字段独立业务证明 |
| session_start_at | timestamp[us, tz=Asia/Shanghai] | 分钟格点的 Session 开始时刻 | 未逐字段独立业务证明 |
| session_end_at | timestamp[us, tz=Asia/Shanghai] | 分钟格点的 Session 结束时刻 | 未逐字段独立业务证明 |
| is_night_session | bool | 分钟格点是否夜盘 | 未逐字段独立业务证明 |
| schedule_status | string | Session 的开市判断状态 | 未逐字段独立业务证明 |
| schedule_signal_reason | string | 形成开市判断的中文原因 | 未逐字段独立业务证明 |
| evidence_level | string | 开市判断证据等级 | 未逐字段独立业务证明 |
| evidence_source | string | 开市判断的具体证据来源 | 未逐字段独立业务证明 |
| is_fetch_required | bool | 本格点按当前白名单是否需要拉取 | 未逐字段独立业务证明 |
| expected_bar_count | int32 | 本格点理论应有 bar 数 | 未逐字段独立业务证明 |
| selection_reason | string | 本格点纳入或排除事实采集的中文规则 | 未逐字段独立业务证明 |
| is_fetch_completed | bool | 事实是否已成功提交的可信完成凭证 | 状态语义须限制解释 |
| actual_bar_count | int32 | 事实表复读后的实际 bar 数 | 未逐字段独立业务证明 |
| is_data_missing | bool | 是否存在应有但没有的数据 | 未逐字段独立业务证明 |
| missing_bar_count | int32 | 缺失 bar 数 | 未逐字段独立业务证明 |
| fetch_run_id | string | 产生当前完成状态的成功运行批次 | 未逐字段独立业务证明 |
| fetch_completed_at | timestamp[us, tz=UTC] | 最近一次事实写入复读成功时间 | 未逐字段独立业务证明 |
| missing_checked_at | timestamp[us, tz=UTC] | 最近一次核对实际/理论条数的时间 | 未逐字段独立业务证明 |
| quality_status | string | 本格点综合质检状态 | 状态语义须限制解释 |
| quality_reason | string | 综合质检结论的中文解释 | 状态语义须限制解释 |
| daily_open | double | 疑似休市交易日的 JQData 日线开盘价证据 | 未逐字段独立业务证明 |
| daily_high | double | JQData 日线最高价证据 | 未逐字段独立业务证明 |
| daily_low | double | JQData 日线最低价证据 | 未逐字段独立业务证明 |
| daily_close | double | JQData 日线收盘价证据 | 未逐字段独立业务证明 |
| daily_volume | double | JQData 日线成交量证据 | 未逐字段独立业务证明 |
| daily_money | double | JQData 日线成交额证据 | 未逐字段独立业务证明 |
| daily_open_interest | double | JQData 日线收盘持仓量证据 | 未逐字段独立业务证明 |
| aggregated_open | double | 从保留分钟 Session 重聚合的日开盘价 | 未逐字段独立业务证明 |
| aggregated_high | double | 分钟重聚合的日最高价 | 未逐字段独立业务证明 |
| aggregated_low | double | 分钟重聚合的日最低价 | 未逐字段独立业务证明 |
| aggregated_close | double | 分钟重聚合的日收盘价 | 未逐字段独立业务证明 |
| aggregated_volume | double | 分钟重聚合的日成交量 | 未逐字段独立业务证明 |
| aggregated_money | double | 分钟重聚合的日成交额 | 未逐字段独立业务证明 |
| aggregated_open_interest | double | 分钟重聚合的收盘持仓量 | 未逐字段独立业务证明 |
| ohlc_matches_daily | bool | 分钟重聚合 OHLC 是否与 JQData 日线一致 | 未逐字段独立业务证明 |
| volume_matches_daily | bool | 聚合成交量是否与日线成交量一致 | 未逐字段独立业务证明 |
| money_matches_daily | bool | 聚合成交额是否与日线成交额一致 | 未逐字段独立业务证明 |
| open_interest_matches_daily | bool | 分钟末持仓是否与日线持仓一致 | 未逐字段独立业务证明 |
| quality_checked_at | timestamp[us, tz=UTC] | 最近一次综合或定向质检时间 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本日历格点最后更新时间 | 未逐字段独立业务证明 |
| year | int16 | 归属交易年份 | 未逐字段独立业务证明 |
| month | int8 | 归属交易月份 | 未逐字段独立业务证明 |

## dim_futures_contract_calendar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| contract_code | string | 聚宽标准固定月份合约代码 | 未逐字段独立业务证明 |
| exchange_code | string | 合约所属聚宽交易所代码 | 未逐字段独立业务证明 |
| underlying_code | string | 合约所属期货品种代码 | 未逐字段独立业务证明 |
| trading_date | date32[day] | Session 归属的交易日 | 限定样本来源/湖对照 |
| list_date | date32[day] | 合约上市日期 | 未逐字段独立业务证明 |
| delist_date | date32[day] | 合约最后有效日期/退市日期 | 未逐字段独立业务证明 |
| contract_multiplier | double | 合约乘数 | 限定样本来源/湖对照 |
| tick_size | double | 合约最小变动价位 | 限定样本来源/湖对照 |
| rule_effective_date | date32[day] | 本行交易时间规则的生效日期 | 未逐字段独立业务证明 |
| rule_expiry_date | date32[day] | 本行交易时间规则的失效日期 | 未逐字段独立业务证明 |
| session_number | int8 | 同一合约交易日内的 Session 顺序号 | 未逐字段独立业务证明 |
| session_text | string | API 交易时间规则中的原始 Session 文本 | 未逐字段独立业务证明 |
| session_start_at | timestamp[us, tz=Asia/Shanghai] | Session 在自然时间上的开始时刻 | 限定样本来源/湖对照 |
| session_end_at | timestamp[us, tz=Asia/Shanghai] | Session 在自然时间上的结束时刻 | 限定样本来源/湖对照 |
| is_night_session | bool | 是否为夜盘 Session | 未逐字段独立业务证明 |
| spans_midnight | bool | Session 是否跨自然日午夜 | 未逐字段独立业务证明 |
| minute_count | int16 | Session 理论包含的分钟 bar 数 | 限定样本来源/湖对照 |
| source | string | 合约日历来源组合 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后构建时间 | 未逐字段独立业务证明 |
| year | int16 | 归属交易年份 | 未逐字段独立业务证明 |
| month | int8 | 归属交易月份 | 未逐字段独立业务证明 |

## dim_futures_exchange_report_calendar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| dataset_name | string | 本格点对应的报告数据集 | 未逐字段独立业务证明 |
| exchange_code | string | 品种所属项目标准交易所代码 | 未逐字段独立业务证明 |
| underlying_code | string | 应采集报告的期货品种代码 | 未逐字段独立业务证明 |
| trading_date | date32[day] | 报告归属交易日 | 未逐字段独立业务证明 |
| is_fetch_required | bool | 本格点是否位于该 API 的覆盖范围并需要尝试 | 未逐字段独立业务证明 |
| requirement_reason | string | 需要或无需拉取的中文原因 | 未逐字段独立业务证明 |
| is_fetch_completed | bool | 是否已有事实正式提交和复读形成的完成凭证 | 状态语义须限制解释 |
| fetch_result_status | string | 最近一次采集结果 | 状态语义须限制解释 |
| is_data_missing | bool | 应有数据但 API 或事实表没有记录 | 未逐字段独立业务证明 |
| expected_record_count | int32 | 可明确预期的最小记录数 | 未逐字段独立业务证明 |
| actual_record_count | int32 | 对应事实表复读行数 | 未逐字段独立业务证明 |
| quality_status | string | 格点综合质检状态 | 状态语义须限制解释 |
| quality_reason | string | 质检结论的中文解释 | 状态语义须限制解释 |
| fetch_run_id | string | 最近一次采集批次号 | 未逐字段独立业务证明 |
| fetch_completed_at | timestamp[us, tz=UTC] | 最近一次成功完成时间 | 未逐字段独立业务证明 |
| quality_checked_at | timestamp[us, tz=UTC] | 最近一次质检时间 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本日历行最后更新时间 | 未逐字段独立业务证明 |
| year | int16 | 报告交易年份 | 未逐字段独立业务证明 |
| month | int8 | 报告交易月份 | 未逐字段独立业务证明 |

## dim_futures_variety_calendar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| underlying_code | string | 期货品种代码，如 CU、RB | 限定样本来源/湖对照 |
| exchange_code | string | 聚宽标准交易所代码 | 限定样本来源/湖对照 |
| trading_date | date32[day] | 品种应交易的交易日 | 限定样本来源/湖对照 |
| active_contract_count | int16 | 该品种当日上市区间内的固定月份合约数 | 限定样本来源/湖对照 |
| source | string | 本日历的来源组合 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后生成或修订时间 | 未逐字段独立业务证明 |
| year | int16 | 交易年份 | 未逐字段独立业务证明 |
| month | int8 | 交易月份 | 未逐字段独立业务证明 |

## dim_macro_release_calendar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| dataset_name | string | 指标所属数据集类型 | 未逐字段独立业务证明 |
| series_code | string | 项目稳定指标系列代码 | 未逐字段独立业务证明 |
| report_date | date32[day] | SHIBOR 观测日或宏观指标报告期日期 | 未逐字段独立业务证明 |
| expected_available_date | date32[day] | 项目规则推定的最早可用日期 | 未逐字段独立业务证明 |
| is_fetch_required | bool | 当前是否应拉取该系列报告期 | 未逐字段独立业务证明 |
| requirement_reason | string | 需要或无需拉取的中文原因 | 未逐字段独立业务证明 |
| is_fetch_completed | bool | 请求、事实写入和复读是否完成 | 状态语义须限制解释 |
| fetch_result_status | string | 最近一次采集结果 | 状态语义须限制解释 |
| is_data_missing | bool | 已到可用日期但事实仍缺失 | 未逐字段独立业务证明 |
| actual_record_count | int32 | 对应事实表复读记录数 | 未逐字段独立业务证明 |
| quality_status | string | 格点综合质检状态 | 状态语义须限制解释 |
| quality_reason | string | 质检结论中文解释 | 状态语义须限制解释 |
| fetch_run_id | string | 最近一次采集批次号 | 未逐字段独立业务证明 |
| fetch_completed_at | timestamp[us, tz=UTC] | 最近一次完成时间 | 未逐字段独立业务证明 |
| quality_checked_at | timestamp[us, tz=UTC] | 最近一次质检时间 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本日历行最后更新时间 | 未逐字段独立业务证明 |
| year | int16 | 报告/观测年份 | 未逐字段独立业务证明 |
| month | int8 | 报告/观测月份 | 未逐字段独立业务证明 |

## dim_trade_calendar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| calendar_date | date32[day] | 日历日期 | 限定样本来源/湖对照 |
| date_key | string | 日期键，供跨引擎稳定连接 | 未逐字段独立业务证明 |
| is_trading_day | bool | 该自然日是否为中国期货交易日 | 限定样本来源/湖对照 |
| weekday | int8 | ISO 周几 | 未逐字段独立业务证明 |
| is_weekend | bool | 是否周六或周日 | 未逐字段独立业务证明 |
| source | string | 交易日判定的数据来源 | 未逐字段独立业务证明 |
| calendar_name | string | 日历业务名称 | 未逐字段独立业务证明 |
| calendar_timezone | string | 日历所采用的业务时区 | 未逐字段独立业务证明 |
| effective_after | time64[us] | 当日完整日级数据可用于增量任务的最早北京时间 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后生成或修订时间 | 未逐字段独立业务证明 |
| year | int16 | 日历年份，也是 Hive 分区列 | 未逐字段独立业务证明 |

## fact_external_index_daily

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| observation_date | date32[day] | 指数报告/观测日期 | 限定样本来源/湖对照 |
| index_code | string | 项目稳定指数代码 | 限定样本来源/湖对照 |
| index_name | string | 指数中文或通用名称 | 未逐字段独立业务证明 |
| index_category | string | 指数业务分类 | 未逐字段独立业务证明 |
| index_value | double | 指数观测值 | 限定样本来源/湖对照 |
| source_indicator_id | string | Eastmoney 原始指标 ID | 限定样本来源/湖对照 |
| source | string | 事实来源 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | 观测年份 | 未逐字段独立业务证明 |
| month | int8 | 观测月份 | 未逐字段独立业务证明 |

## fact_futures_daily

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| contract_code | string | 项目标准固定月份合约代码 | 未逐字段独立业务证明 |
| exchange_code | string | 项目标准交易所代码 | 未逐字段独立业务证明 |
| underlying_code | string | 合约所属品种代码 | 未逐字段独立业务证明 |
| trading_date | date32[day] | 日线归属交易日 | 未逐字段独立业务证明 |
| previous_close | double | 上一交易日收盘价 | 语义已证伪 |
| previous_settlement | double | 上一交易日结算价 | 未逐字段独立业务证明 |
| open | double | 当日开盘价 | 限定样本来源/湖对照 |
| high | double | 当日最高价 | 限定样本来源/湖对照 |
| low | double | 当日最低价 | 限定样本来源/湖对照 |
| close | double | 当日收盘价 | 限定样本来源/湖对照 |
| settlement | double | 当日结算价 | 未逐字段独立业务证明 |
| close_change_from_previous_settlement | double | 收盘价相对昨结算价的涨跌额 | 未逐字段独立业务证明 |
| settlement_change_from_previous_settlement | double | 结算价相对昨结算价的涨跌额 | 未逐字段独立业务证明 |
| volume | double | 当日成交量 | 限定样本来源/湖对照 |
| money | double | 当日成交金额，项目统一为元 | 限定样本来源/湖对照 |
| open_interest | double | 当日收盘持仓量 | 限定样本来源/湖对照 |
| open_interest_change | double | 当日持仓量变化 | 未逐字段独立业务证明 |
| has_market_data | bool | API 是否返回了有效市场行情 | 未逐字段独立业务证明 |
| source | string | 行情来源 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | 交易年份 | 未逐字段独立业务证明 |
| month | int8 | 交易月份 | 未逐字段独立业务证明 |

## fact_futures_member_position_daily

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| trading_date | date32[day] | 汇总记录交易日 | 无事实行可验证 |
| exchange_code | string | 项目标准交易所代码 | 无事实行可验证 |
| underlying_code | string | 汇总记录所属品种 | 无事实行可验证 |
| source_symbol | string | API 原样合约或产品代码 | 无事实行可验证 |
| participant_type | string | 汇总参与者类型 | 无事实行可验证 |
| volume | double | 该参与者类型成交量 | 无事实行可验证 |
| volume_change | double | 成交量变化 | 无事实行可验证 |
| long_position | double | 持买仓量 | 无事实行可验证 |
| long_position_change | double | 持买仓变化 | 无事实行可验证 |
| short_position | double | 持卖仓量 | 无事实行可验证 |
| short_position_change | double | 持卖仓变化 | 无事实行可验证 |
| source | string | 事实来源 | 无事实行可验证 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 无事实行可验证 |
| year | int16 | 报告年份 | 无事实行可验证 |
| month | int8 | 报告月份 | 无事实行可验证 |

## fact_futures_minute

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| contract_code | string | 项目标准固定月份合约代码 | 限定样本来源/湖对照 |
| exchange_code | string | 合约所属交易所 | 未逐字段独立业务证明 |
| underlying_code | string | 合约所属品种 | 未逐字段独立业务证明 |
| trading_date | date32[day] | bar 归属的交易日，而非其自然日期 | 限定样本来源/湖对照 |
| session_number | int8 | bar 所属 Session 编号 | 未逐字段独立业务证明 |
| bar_at | timestamp[us, tz=Asia/Shanghai] | 一分钟 bar 的结束时刻 | 限定样本来源/湖对照 |
| open | double | 本分钟开盘价 | 限定样本来源/湖对照 |
| high | double | 本分钟最高价 | 限定样本来源/湖对照 |
| low | double | 本分钟最低价 | 限定样本来源/湖对照 |
| close | double | 本分钟收盘价 | 限定样本来源/湖对照 |
| volume | double | 本分钟成交量 | 限定样本来源/湖对照 |
| money | double | 本分钟成交金额 | 限定样本来源/湖对照 |
| open_interest | double | 本分钟结束时持仓量 | 限定样本来源/湖对照 |
| source | string | 行情来源与关键参数 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | 归属交易年份 | 未逐字段独立业务证明 |
| month | int8 | 归属交易月份 | 未逐字段独立业务证明 |

## fact_futures_missing_bar

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| bar_frequency | string | 缺失 bar 的频率 | 未逐字段独立业务证明 |
| contract_code | string | 发生缺失的合约代码 | 未逐字段独立业务证明 |
| exchange_code | string | 合约所属交易所 | 未逐字段独立业务证明 |
| underlying_code | string | 合约所属品种 | 未逐字段独立业务证明 |
| trading_date | date32[day] | 缺失时点归属交易日 | 未逐字段独立业务证明 |
| session_number | int8 | 缺失时点所属 Session 编号 | 未逐字段独立业务证明 |
| expected_bar_at | timestamp[us, tz=Asia/Shanghai] | 理论应存在但实际不存在的 bar 结束时刻 | 未逐字段独立业务证明 |
| detected_at | timestamp[us, tz=UTC] | 本缺口最近一次被检测到的时间 | 未逐字段独立业务证明 |
| year | int16 | 归属交易年份 | 未逐字段独立业务证明 |
| month | int8 | 归属交易月份 | 未逐字段独立业务证明 |

## fact_futures_position_rank_daily

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| trading_date | date32[day] | 排名报告交易日 | 未逐字段独立业务证明 |
| exchange_code | string | 项目标准交易所代码 | 未逐字段独立业务证明 |
| underlying_code | string | 报告所属期货品种 | 未逐字段独立业务证明 |
| source_symbol | string | JQData 原样返回的合约代码 | 未逐字段独立业务证明 |
| contract_code | string | 可明确识别时的项目标准固定月份合约代码 | 限定样本来源/湖对照 |
| member_name | string | 期货公司会员简称 | 限定样本来源/湖对照 |
| volume_rank | int16 | 该会员成交量名次 | 限定样本来源/湖对照 |
| volume | double | 该会员进入成交排名时的成交量 | 限定样本来源/湖对照 |
| volume_change | double | 该会员成交量较上一交易日变化 | 限定样本来源/湖对照 |
| long_position_rank | int16 | 该会员持买仓名次 | 限定样本来源/湖对照 |
| long_position | double | 该会员持买仓量 | 限定样本来源/湖对照 |
| long_position_change | double | 持买仓量较上一交易日变化 | 限定样本来源/湖对照 |
| short_position_rank | int16 | 该会员持卖仓名次 | 限定样本来源/湖对照 |
| short_position | double | 该会员持卖仓量 | 限定样本来源/湖对照 |
| short_position_change | double | 持卖仓量较上一交易日变化 | 限定样本来源/湖对照 |
| source | string | 事实来源 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | 报告年份 | 未逐字段独立业务证明 |
| month | int8 | 报告月份 | 未逐字段独立业务证明 |

## fact_futures_warehouse_receipt_daily

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| trading_date | date32[day] | 仓单日报日期 | 未逐字段独立业务证明 |
| exchange_code | string | 品种所属项目标准交易所代码 | 未逐字段独立业务证明 |
| underlying_code | string | 仓单所属期货品种 | 未逐字段独立业务证明 |
| warehouse_name | string | 交割仓库或仓单统计地点名称 | 限定样本来源/湖对照 |
| warehouse_receipt_number | double | 该仓库当日仓单数量 | 限定样本来源/湖对照 |
| warehouse_receipt_unit | string | 来源仓单计量单位 | 限定样本来源/湖对照 |
| warehouse_receipt_number_change | double | 仓单数量较昨日变化 | 限定样本来源/湖对照 |
| source | string | 事实来源 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | 报告年份 | 未逐字段独立业务证明 |
| month | int8 | 报告月份 | 未逐字段独立业务证明 |

## fact_interest_rate_daily

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| series_code | string | SHIBOR 期限系列代码 | 限定样本来源/湖对照 |
| observation_date | date32[day] | SHIBOR 观测日期 | 限定样本来源/湖对照 |
| rate | double | 对应期限的 SHIBOR 利率 | 限定样本来源/湖对照 |
| source | string | 事实来源 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | 观测年份 | 未逐字段独立业务证明 |
| month | int8 | 观测月份 | 未逐字段独立业务证明 |

## fact_macro_release

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| series_code | string | 项目稳定宏观系列代码 | 限定样本来源/湖对照 |
| report_date | date32[day] | 归一化后的宏观报告期月末或季末日期 | 限定样本来源/湖对照 |
| available_date | date32[day] | 项目规则认定可在回测中使用该值的保守日期 | 作为真实可用日已证伪 |
| value | double | 对应宏观系列的观测值 | 限定样本来源/湖对照 |
| source | string | 事实来源报告 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | 报告期年份 | 未逐字段独立业务证明 |
| month | int8 | 报告期月份 | 未逐字段独立业务证明 |

## fact_overseas_futures_daily

| 字段 | 类型 | 契约含义 | 核验状态 |
|---|---|---|---|
| snapshot_date | date32[day] | 本批原始日文件/查询的日历请求日期 | 未逐字段独立业务证明 |
| trading_date | date32[day] | API 记录的境外期货交易日 | 未逐字段独立业务证明 |
| source_instrument_id | string | 聚宽财务库原始记录/品种标识 | 未逐字段独立业务证明 |
| instrument_code | string | 境外期货品种代码 | 未逐字段独立业务证明 |
| instrument_name | string | 境外期货品种名称 | 限定样本来源/湖对照 |
| open | double | 当日开盘价 | 限定样本来源/湖对照 |
| high | double | 当日最高价 | 限定样本来源/湖对照 |
| low | double | 当日最低价 | 限定样本来源/湖对照 |
| close | double | 当日收盘价 | 限定样本来源/湖对照 |
| volume | double | 当日成交量 | 限定样本来源/湖对照 |
| change_pct | double | 当日涨跌幅 | 限定样本来源/湖对照 |
| amplitude | double | 当日振幅 | 限定样本来源/湖对照 |
| previous_close | double | 上一交易日收盘价 | 限定样本来源/湖对照 |
| source | string | 事实来源 | 未逐字段独立业务证明 |
| updated_at | timestamp[us, tz=UTC] | 本行最后写入时间 | 未逐字段独立业务证明 |
| year | int16 | API 交易年份 | 未逐字段独立业务证明 |
| month | int8 | API 交易月份 | 未逐字段独立业务证明 |
