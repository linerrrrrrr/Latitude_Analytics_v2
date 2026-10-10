# PAI 计价资料：DLC 实验费用

核对日期：2026-10-07。本文用于配置 DLC 研究任务和计算费用，整理阿里云官方计费入口及本账号实际取得的深圳 DLC 按量报价。报价对应指定地域、机型和查询时点，不代表其他产品或资源池的价格。

## 按量任务怎样计费

公共资源按节点占用时长收费，费用由节点数、节点小时单价和使用时长决定。任务结束或停止后停止计算资源计费。[DLC 计费说明](https://help.aliyun.com/zh/pai/product-overview/billing-of-dlc)

设单价为每节点每小时 P 元，使用 K 个节点、运行 H 小时，则计算资源费用为 K × P × H。减少并发会延长完成时间；总节点小时相同时，并发数本身不会减少这项费用。这是对上述计费公式的推导。

官方示例使用上海的 ecs.g6.2xlarge，8 vCPU、32 GB 内存，示例单价为每节点小时 2.02 元。此上海示例不能替代深圳实际报价。[DLC 计费说明](https://help.aliyun.com/zh/pai/product-overview/billing-of-dlc)

## 怎样取得适用报价

| 官方入口 | 用途与适用范围 |
| --- | --- |
| [DLC 计费说明](https://help.aliyun.com/zh/pai/product-overview/billing-of-dlc) | 区分公共资源按量任务与预付费 AI 计算资源，查看计费公式和机型示例。 |
| [PAI 产品购买指引](https://help.aliyun.com/zh/pai/pai-product-purchase-guidelines) | 区分 DLC、DSW、EAS 的购买入口、公共资源和已关联资源配额，避免套用其他产品的报价。 |
| [AI 计算资源计费说明](https://help.aliyun.com/zh/pai/product-overview/ai-computing-resource-billing-description) | 查询通用计算资源、灵骏智算资源的购买和计费方式；这类资源池的费用不能直接当作一项 DLC 公共资源任务的费用。 |
| [ListEcsSpecs 接口](https://help.aliyun.com/zh/pai/developer-reference/api-pai-dlc-2020-12-03-listecsspecs) | 查询地域支持的 CPU、内存、机型可用性和支付类型；该接口文档列出的结果不是节点报价。 |
| [费用管理与账单 FAQ](https://help.aliyun.com/zh/pai/faq-about-billing) | 定位费用明细，并解释为什么 DSW API 价格可能与控制台报价不同。 |

选择任务时，应在目标地域的 DLC 新建任务页面确认机型和报价，保留地域、机型、节点数、计费方式、币种、价格单位及报价日期。使用预付费资源时，须确认已有资源配额和费用归属。

## 本次深圳 DLC 实际询价

PAI `ListEcsSpecs` 已返回本地域可用 CPU 机型。费用查询使用 BSS OpenAPI 的 DLC 产品 `learn / learn_dlcPayAsYouGo_public_cn / PayAsYouGo`；通过 `QueryProductList` 和 `DescribePricingModule` 确定产品与 `Usage` 模块，再调用 `GetPayAsYouGoPrice`，配置为 `Usage:60,Region:cn-shenzhen,ResourceType:<机型>`。60 分钟按官方 DLC 分钟计费口径换算为一节点小时。[报价接口](https://help.aliyun.com/zh/user-center/developer-reference/api-bssopenapi-2017-12-14-getpayasyougoprice)

| 机型 | CPU／内存 | 单节点一小时询价（人民币） | 原始响应 |
| --- | --- | --- | --- |
| ecs.c6.large | 2 核／4GB | 0.432 元 | [报价记录](../a02_Experiments/b01_ReturnVolatilityForecasting/Tracking/cloud_readiness_20261007/quote_ecs.c6.large.json) |
| ecs.g6.large | 2 核／8GB | 0.552 元 | [报价记录](../a02_Experiments/b01_ReturnVolatilityForecasting/Tracking/cloud_readiness_20261007/quote_ecs.g6.large.json) |
| ecs.g6.xlarge | 4 核／16GB | 1.098 元 | [报价记录](../a02_Experiments/b01_ReturnVolatilityForecasting/Tracking/cloud_readiness_20261007/quote_ecs.g6.xlarge.json) |

响应币种为 CNY，折扣额为 0。当前实验选择 ecs.c6.large，其报价请求 ID 为 `01A113F2-6501-55C0-BE16-CFFCDFAD5D61`。原始产品、计价模块、机型及镜像查询记录保存在同一 `Tracking/cloud_readiness_20261007/`，不包含认证凭据。询价是只读调用，不会创建计算任务。

## 竞价与免费额度

竞价资源随市场价格和库存变化，可能被回收；可用最高出价或折扣限定竞价条件。具体价格区间须在目标地域控制台查看，官网“最高节约 90%”不等于本任务实际获得的折扣。[使用竞价任务](https://help.aliyun.com/zh/pai/use-preemptible-job)

DLC、DSW、EAS 的试用额度分别使用，不能通用。免费额度须核实领取状态、可抵扣机型、剩余额度和有效期；额度耗尽或到期后继续使用会产生按量费用。[免费试用领取、使用和释放](https://help.aliyun.com/zh/pai/product-overview/free-trial-guide)

## 当前实验的执行费用口径

[收益与波动率预测实验配置](../a02_Experiments/b01_ReturnVolatilityForecasting/config.yaml)当前没有金额上限。每作业采用 10 个 ecs.c6.large 节点，PUBLIC 网络、PyTorchJob、官方 Python 3.13.9 CPU 镜像；全配置队列按作业串行提交，各节点分配独立测试交易日。节点数为 10 时，按本次询价计算，计算资源费用为每作业小时 4.32 元；这只给出费用速率，不是总耗时或总费用预测。

用户已撤销此前的 5 元限制，入口不再采用累计预算预约或一小时任务截断，最长运行时间沿用未指定时的 SDK／服务端默认。本批没有进行耗时试跑。失败、输入不完整或独立可见监控中断时停止后续阶段，并停止本批可确认身份的活动作业，不自动重试。

DLC 计算费用与 OSS 存储、请求和下载费用分别计费；本次没有核定最终 OSS 账单。最终费用以阿里云账单为准，长期保留对象及账号其他资源的费用也须按各自归属核对。
