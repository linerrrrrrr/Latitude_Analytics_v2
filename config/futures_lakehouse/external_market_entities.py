"""外部市场日历、原始页面归档与事实生产者共用的请求实体配置。

这里保存会直接改变 ``dim_external_market_calendar`` 理论格点的稳定业务配置。
配置模块不调用外部 API，也不负责是否写入；业务入口仍从正式上游水位自动求差，
并由各自的 ``--write`` 决定是否提交。

生意社链路按日期请求整张页面并只归档原始响应，不解析为稳定 silver 事实，因此其
请求实体是 ``ALL``。``FUT_GLOBAL_DAILY`` 按日期返回当日整张表，境外期货事实生产者
同样使用 ``ALL``；Eastmoney 行业指数事实生产者按 ``INDICATOR_ID`` 分别请求，因此
指数日历使用来源指标 ID。项目指数代码、中文名和分类由同一条配置提供给事实生产者。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


EXTERNAL_MARKET_ENTITY_CONFIG_VERSION = "1.0.0"

DOMESTIC_SPOT_BASIS_ENTITY_CODE = "ALL"
OVERSEAS_FUTURES_ENTITY_CODE = "ALL"


@dataclass(frozen=True)
class ExternalIndexEntity:
    """一个 Eastmoney 指数请求实体及其稳定项目映射。"""

    source_indicator_id: str
    index_code: str
    index_name_zh: str
    index_category: str
    active_from: date | None = None
    active_to: date | None = None


EXTERNAL_INDEX_ENTITIES = (
    ExternalIndexEntity("EMI00107664", "BDI", "波罗的海干散货指数", "shipping"),
    ExternalIndexEntity("EMI00107665", "BPI", "波罗的海巴拿马型运费指数", "shipping"),
    ExternalIndexEntity("EMI00107666", "BCI", "波罗的海海岬型运费指数", "shipping"),
    ExternalIndexEntity("EMI00107667", "BSI", "波罗的海超灵便型船运价指数", "shipping"),
    ExternalIndexEntity("EMI00107668", "BDTI", "波罗的海原油运输指数", "shipping"),
    ExternalIndexEntity("EMI00107669", "BCTI", "波罗的海成品油运输指数", "shipping"),
    ExternalIndexEntity("EMI01508580", "WTI_CONC", "NYMEX WTI 连续商品指数", "energy"),
    ExternalIndexEntity("EMI00662539", "SUNSIRS_ENERGY", "生意社能源指数", "energy"),
    ExternalIndexEntity("EMI00018828", "MYSTEEL_COKE", "钢联中国焦炭价格指数", "ferrous"),
    ExternalIndexEntity("EMI00662545", "SUNSIRS_STEEL", "生意社钢铁指数", "ferrous"),
    ExternalIndexEntity(
        "EMI00064821",
        "MYSTEEL_STEEL_COMPOSITE",
        "钢联普钢综合价格指数",
        "ferrous",
    ),
    ExternalIndexEntity("EMI00064805", "XINHUA_IRON_ORE", "新华中国铁矿石价格指数", "ferrous"),
    ExternalIndexEntity(
        "EMI00662542",
        "SUNSIRS_NONFERROUS",
        "生意社有色金属指数",
        "nonferrous",
    ),
    ExternalIndexEntity("EMI00135907", "MYSTEEL_NICKEL", "钢联镍价格指数", "nonferrous"),
    ExternalIndexEntity("EMI00135906", "MYSTEEL_TIN", "钢联锡价格指数", "nonferrous"),
    ExternalIndexEntity("EMI00135905", "MYSTEEL_ZINC", "钢联锌价格指数", "nonferrous"),
    ExternalIndexEntity("EMI00135904", "MYSTEEL_LEAD", "钢联铅价格指数", "nonferrous"),
    ExternalIndexEntity(
        "EMI00135903",
        "MYSTEEL_ALUMINUM",
        "钢联铝价格指数",
        "nonferrous",
    ),
    ExternalIndexEntity("EMI00135902", "MYSTEEL_COPPER", "钢联铜价格指数", "nonferrous"),
)


# 配置在模块导入时立即做轻量一致性检查，避免日历和事实消费者看到歧义映射。
source_ids = [entity.source_indicator_id for entity in EXTERNAL_INDEX_ENTITIES]
index_codes = [entity.index_code for entity in EXTERNAL_INDEX_ENTITIES]

if len(source_ids) != len(set(source_ids)):
    raise ValueError("外部指数配置中的 Eastmoney INDICATOR_ID 不唯一。")
if len(index_codes) != len(set(index_codes)):
    raise ValueError("外部指数配置中的项目指数代码不唯一。")
if any(
    entity.active_from is not None
    and entity.active_to is not None
    and entity.active_from > entity.active_to
    for entity in EXTERNAL_INDEX_ENTITIES
):
    raise ValueError("外部指数配置存在 active_from 晚于 active_to 的记录。")
