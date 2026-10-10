"""Independent research artifacts: explicit Arrow types, keys and units.

These schemas describe this project's snapshots only. They are not silver contracts.
"""

# 兼容字段的数学解释见 [统一符号约定](research/03_method_derivations.md)：
# 既有 metadata 中的 a_m 对应收益尺度方法的 f_i²，sqrt(a_m*a_previous)
# 对应 f_i*f_(i-1)；median_mixed 的 variance_factor 存放带帽的 g_mix
# 混合贡献形状因子，不能仅凭字段名视为收益方差因子。Schema 和 metadata 字节值保留。

import pyarrow as pa


INPUT_SNAPSHOT_SCHEMA = pa.schema([
    pa.field("trading_date", pa.date32(), nullable=False),
    pa.field("minute_index", pa.int16(), nullable=False),
    pa.field("session_number", pa.int8(), nullable=False),
    pa.field("bar_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
    pa.field("contract_code", pa.string(), nullable=False),
    pa.field("open", pa.float64(), nullable=False),
    pa.field("close", pa.float64(), nullable=False),
    pa.field("volume", pa.float64(), nullable=False),
    pa.field("money", pa.float64(), nullable=False),
    pa.field("signal_trading_date", pa.date32(), nullable=False),
], metadata={
    b"table_name": b"im_input_snapshot",
    b"primary_key": b"trading_date,minute_index",
    b"price_basis": b"Raw contract OHLC; no retrospectively adjusted prices",
    b"scope": b"Independent project snapshot; not a silver table",
})

MINUTE_OBSERVATION_SCHEMA = pa.schema([
    pa.field("trading_date", pa.date32(), nullable=False),
    pa.field("minute_index", pa.int16(), nullable=False),
    pa.field("session_number", pa.int8(), nullable=False),
    pa.field("bar_at", pa.timestamp("us", tz="Asia/Shanghai"), nullable=False),
    pa.field("contract_code", pa.string(), nullable=False),
    pa.field("log_return", pa.float64()),
    pa.field("rv_contribution", pa.float64()),
    pa.field("bpv_contribution", pa.float64()),
    pa.field("mixed_contribution", pa.float64()),
], metadata={
    b"table_name": b"minute_observations",
    b"primary_key": b"trading_date,minute_index",
    b"return_definition": b"Session first minute log(close/open); else contiguous log(close/previous_close)",
    b"bpv_definition": b"pi/2 * abs(r_m*r_previous); session first minute null",
    b"mixed_definition": b"RV at session first minute; BPV otherwise",
    b"units": b"Log return and squared log-return contributions; float64",
    b"available_at": b"bar_at",
})

PERIODICITY_FACTOR_SCHEMA = pa.schema([
    pa.field("trading_date", pa.date32(), nullable=False),
    pa.field("minute_index", pa.int16(), nullable=False),
    pa.field("method", pa.string(), nullable=False),
    pa.field("lookback_days", pa.int16(), nullable=False),
    pa.field("variance_factor", pa.float64()),
    pa.field("contribution_factor", pa.float64()),
    pa.field("factor_fit_end_date", pa.date32()),
], metadata={
    b"table_name": b"periodicity_factors",
    b"primary_key": b"trading_date,minute_index,method,lookback_days",
    b"variance_factor": b"Mean-one return variance factor; median_mixed is a contribution-shape compatibility baseline only",
    b"contribution_factor": b"First slot a_m; other slots sqrt(a_m*a_previous). median_mixed uses its direct shape instead",
    b"fit_cutoff": b"All fitted data strictly precede trading_date; none is constant one",
    b"missing": b"Warmup, incomplete training window or degenerate scale => Arrow null",
})
