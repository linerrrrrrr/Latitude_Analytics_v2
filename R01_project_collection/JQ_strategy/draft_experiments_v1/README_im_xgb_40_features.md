# IM XGB 40 个波动率代理特征

正式入口是 `05_01_im_xgb_40_volatility_features.py`。它不再遍历实验网格，而是只计算已经冻结的 40 个方案：36 个分层核心方案，加 2 个 FIGARCH、2 个 GARCH 锚点方案。

## 计算口径

- 每个源分钟收盘后重新定锚，分别向后构造 time、volume、money 三类时钟的 5、10、15 等效分钟序列。
- volume 和 money 使用全部 IM 固定月合约的分钟活动量，不能用主力连续合约自身的 volume/money 替代。
- 每个端点只构造一次 9 条基础序列，40 个特征复用这些序列。
- 季节窗口只保留 0、60、120 个交易日；30、360 个交易日不再计算。
- HAR 只拟合并预测下一采样步，不计算 15 分钟 HAR。
- 6/12 个月是日历窗口；原生模型至少需要 500 个有效事件，HAR 至少需要 100 个有效训练对。
- 输出的 40 列是下一采样事件的原始正波动代理值。建议在后续 XGB 输入矩阵中统一做 `log(max(value, epsilon))`，但本表保留原值以便审计。

`FEATURE_SPECS` 是唯一的冻结清单，顺序也是输出字段顺序。特征筛选只使用三个 validation fold；`final_2026` 没有参与选择。

## 输入

分钟连续序列及 60/120 日季节因子默认读取本目录中的：

- `im_main_continuous_1m.parquet`
- `im_bpv_seasonality_1m_60td.parquet`
- `im_bpv_seasonality_1m_120td.parquet`

活动量输入有两种方式：

1. PAI 推荐 `--activity-input`，指向一个 Parquet 文件或 Parquet 数据集目录。它必须逐分钟唯一并包含 `bar_at`、`all_contract_volume`、`all_contract_money`。
2. 本地可用 `--activity-duckdb` 直接只读查询 `futures_minute`。若两项都省略，程序尝试使用本项目的 `financial_futures.duckdb`。

PAI 上建议预先从数据库导出聚合后的活动量 Parquet，只上传约 23.9 万行的窄表，而不是上传整个 DuckDB。聚合范围必须是正则 `IM[0-9]{4}[.]CCFX` 的全部固定月合约。

可在 DuckDB 中一次性执行：

```sql
COPY (
    SELECT
        bar_at,
        sum(volume)::DOUBLE AS all_contract_volume,
        sum(money)::DOUBLE AS all_contract_money
    FROM futures_minute
    WHERE regexp_full_match(contract_code, 'IM[0-9]{4}[.]CCFX')
    GROUP BY bar_at
    ORDER BY bar_at
) TO 'im_all_contract_activity_1m.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD);
```

## PAI 运行

依赖版本冻结在 `requirements_im_xgb_40_features.txt`。PAI 数据集或 OSS 文件应先挂载成本地路径；该入口不把 Windows 路径写入代码。

单个端点验证示例：

```bash
python 05_01_im_xgb_40_volatility_features.py \
  --minute-input /mnt/input/im_main_continuous_1m.parquet \
  --activity-input /mnt/input/im_all_contract_activity_1m.parquet \
  --seasonality-60-input /mnt/input/im_bpv_seasonality_1m_60td.parquet \
  --seasonality-120-input /mnt/input/im_bpv_seasonality_1m_120td.parquet \
  --output-dir /mnt/output/im_xgb_40 \
  --endpoint-at "2026-08-28 14:59:00" \
  --allow-retrospective-adjustment \
  --require-all-features
```

日期批次采用交易日公历序号取模的确定性分片；改变起止日期不会让同一交易日在分片间漂移。假设 PAI 启动 16 个实例，每个实例分别传入自己的 0—15 `shard-index`：

```bash
python 05_01_im_xgb_40_volatility_features.py \
  --minute-input /mnt/input/im_main_continuous_1m.parquet \
  --activity-input /mnt/input/im_all_contract_activity_1m.parquet \
  --seasonality-60-input /mnt/input/im_bpv_seasonality_1m_60td.parquet \
  --seasonality-120-input /mnt/input/im_bpv_seasonality_1m_120td.parquet \
  --output-dir /mnt/output/im_xgb_40 \
  --start-date 2025-01-02 \
  --end-date 2026-08-28 \
  --shard-index 0 \
  --shard-count 16 \
  --resume \
  --allow-retrospective-adjustment \
  --require-all-features
```

每个 PAI 实例建议先使用 4 vCPU、16 GiB 内存、单 Python 进程。扩大吞吐时增加日期分片实例，不要在一个实例内再复制多份完整输入。当前工作站实测单端点约 1.6—1.7 秒，240 分钟约 6.5—7 分钟；PAI 实际时间应先用若干完整交易日校准。

## 输出与恢复

输出按 `trading_day=YYYY-MM-DD/features.parquet` 分区；目录键故意与文件内 date32 类型的 `trading_date` 字段使用不同名称，整棵目录可直接作为 Hive Parquet 数据集读取。每个交易日只有一个与分片编号无关的标准文件，调整分片数也不会生成重复日分片。一天完成后才通过同目录临时文件原子安装。`--resume` 只跳过 Schema、方法 metadata 和预期行数都完整的日文件；不完整或不同版本的文件会直接停止，不会静默混用。

每行包含：

- `as_of_at` 等 5 个身份字段；
- 恰好 40 个特征字段；
- `valid_feature_count`、`feature_failures_json`、`compute_seconds` 三个控制字段。

默认允许单个模型失败并把该特征写成 null，同时在 `feature_failures_json` 留原因。正式建模批次建议增加 `--require-all-features`，使任一缺失立即停止当前日且不发布日文件。

## 调整价格的时间可用性

实验排名使用 `bidirectional_adjusted_close`，当前文件的该列是整份快照的回望式双端复权，历史时点当时并不可用。为了忠实复现实验，程序默认仍选该列，但历史批次必须显式传入 `--allow-retrospective-adjustment` 才会运行；输出 metadata 会记录所用价格列。

若目标是严格的实时可交易特征，应使用 `--event-price-column close`，或先生产真正按当时可用信息构造的复权价格。切换为 raw `close` 后，原 40 方案的排序只能视为候选依据，需重新进行一次无泄漏验证。
