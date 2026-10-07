# alipai 环境验证

本目录保存 alipai SDK 的验证入口与实验依据。先按[项目环境说明](../README.md)配置正式 `latitude_env_v2`；alipai 0.4.13 的 `numpy<2` 声明与本项目 NumPy 2 冲突，采用分步安装并保留该依赖检查告警，不修改官方包或降低核心依赖版本。

本地 SDK 与云端容器分别验收。CPU 实测镜像为 Python 3.9.5，显式 `requirements=[...]` 可安装依赖；源码目录的依赖文件未自动安装、PAI 指标查询为空，均未证明由本地 Python 3.13 引起。多数组序列化错误在 v1 也存在。云端 Hydra/MLflow 微型记录通过，不代表本地安装状态或远程跟踪服务已验证。GPU/CUDA、云端 Python 3.13、真实模型跨版本恢复尚未验证。 收益／波动率实验已只读查询到深圳官方 Python 3.13.9 CPU 镜像并取得 DLC 报价；该实验要求 Python 主／次版本一致、数值依赖包精确匹配，并记录本地 3.13.15 与云端实际版本。离线检查允许补丁版本差异，不等于云端真实计算已通过；执行状态见[研究说明](../../04_Research/README.md#实验目录与对照组)。alipai 0.4.13 的 Estimator 基类及内部提交读取全局默认会话，单独传入 session 不能保证网络配置生效；显式调用 setup_default_session 设置 PUBLIC 后再构造 Estimator。收益／波动率入口已修正，真实 SDK 内部提交的离线回归通过；首次实际创建请求的 TLS 失败记录保留，新云批次尚未启动。

| 入口或证据 | 用途 |
| --- | --- |
| [smoke_test.py](smoke_test.py) | 提交 CPU 的 1+1 任务并从 OSS 读取结果 |
| [verify.py](verify.py) / [workload.py](workload.py) | 本地逐项验证入口 / 随任务上传的合成样本负载 |
| [1+1 结果](results/smoke_train8t3nkabdbg3/smoke_result.json) | 对应任务 `train8t3nkabdbg3` |
| [功能报告](results/20261002T174304Z-7f65e796/REPORT.txt) / [原始结果](results/20261002T174304Z-7f65e796/capabilities_result.json) | CPU 输入输出、函数组合、双机分片、checkpoint、失败识别、停止及限制 |

复跑时在仓库根目录使用 v2，显式指定新批次名（字母、数字、下划线或连字符）：

```powershell
python -B -u environment/alipai/smoke_test.py --batch <新批次名>
python -B -u environment/alipai/verify.py local --batch <新批次名>
```

`verify.py` 按同一批次依次执行 `local`、`single`、`single_requirements`、`distributed`、`failure`、`stop`、`cleanup`，每次只执行指定项；`single` 用于检查依赖文件安装，已有实验中该项失败。`cleanup` 仅回收本批次清单中的 OSS 对象。除 `local` 外均涉及云端，遵循[根级批次授权规则](../../AGENTS.md#长时间任务的人工启动后台执行与可见监控)，使用本机已有 PAI 配置与认证。

新结果写入 `results/<批次名>/`；已有 smoke 结果或同名验证项不覆盖。历史报告保留实验当时的环境状态、路径和文件名，其中 `results.json` 对应同目录的 `capabilities_result.json`，报告内旧入口位置以本页为准。功能批次的 OSS 对象已经清理，原 URI 仅用于追溯。原始 JSON 留在本地，代码和报告可纳入版本控制；不在材料中保存凭据。
