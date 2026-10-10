# 期货夜盘归属实验

本目录保存“期货日 K 的开盘价、收盘价分别对应哪些分钟 Session 边界”的来源检查证据。[检查脚本](../../audit_night_session_attribution.py)不写正式湖，也不修改数据契约。

实验固定选择 2025-02-10 至 2025-02-14 这一普通交易周，并覆盖三种典型夜盘长度：

- `RB`：夜盘通常在 23:00 左右结束；
- `CU`：夜盘通常在 01:00 左右结束；
- `AU`：夜盘通常在 02:30 左右结束。

脚本会为每个品种查询该周主力固定月份合约，拉取同一合约的 JQData 日 K 和 1 分钟 K，并比较：

1. 上一交易日日盘收盘后至当前交易日日盘收盘的全部分钟 K 聚合；
2. 当前自然日仅日盘分钟 K 聚合；
3. 日 K 开盘价与夜盘、日盘 Session 首根分钟 K 开盘价；
4. 日 K 收盘价与夜盘、日盘 Session 末根分钟 K 收盘价。

运行命令：

```powershell
E:\anaconda3\envs\latitude_env_v2\python.exe 02_Market_Data\a01_Collection\checks\audit_night_session_attribution.py
```

本目录的 CSV 保留原始日 K、分钟 K、Session 边界和逐交易日匹配明细，[experiment_report.md](experiment_report.md)汇总实验结果。手动复跑会查询 JQData，并在 `checks/results/night_session_attribution_<运行时间>/` 新建结果，不覆盖本目录。
