"""宏观发布日历、SHIBOR 与宏观事实共用的系列和可用日规则。

本模块只保存稳定系列映射并执行版本化日期规则；不读取数据湖、不调用 API，
也不决定是否写入。b01 使用这些规则生成理论格点，b02 使用同一映射转换
SHIBOR，b03 使用同一来源列和数值偏移转换 Eastmoney 宏观报告；业务目录不得维护第二份列表。
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal


DatasetName = Literal["interest_rate", "macro_release"]
Frequency = Literal["business_day", "month_end", "quarter_end"]
AvailabilityRule = Literal[
    "same_day",
    "next_month_day_9_next_weekday",
    "month_end_february_march_4",
    "quarter_end_plus_16_days",
]


MACRO_RELEASE_CONFIG_VERSION = "1.1.0"
AVAILABILITY_RULE_VERSION = "availability-v1.0.0"


@dataclass(frozen=True, slots=True)
class MacroReleaseSeries:
    """一个稳定系列及其理论频率、可用日、来源列和数值偏移。"""

    dataset_name: DatasetName
    series_code: str
    series_name_zh: str
    frequency: Frequency
    availability_rule: AvailabilityRule
    source_system: str
    source_api: str
    source_column: str
    unit_zh: str
    source_value_offset: float = 0.0


MACRO_RELEASE_SERIES = (
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_ON",
        series_name_zh="隔夜 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="on",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_1W",
        series_name_zh="1 周 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="1w",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_2W",
        series_name_zh="2 周 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="2w",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_1M",
        series_name_zh="1 个月 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="1m",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_3M",
        series_name_zh="3 个月 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="3m",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_6M",
        series_name_zh="6 个月 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="6m",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_9M",
        series_name_zh="9 个月 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="9m",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="interest_rate",
        series_code="SHIBOR_1Y",
        series_name_zh="1 年 SHIBOR",
        frequency="business_day",
        availability_rule="same_day",
        source_system="Tushare Pro",
        source_api="pro.shibor",
        source_column="1y",
        unit_zh="百分比年利率",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_NATIONAL_YOY",
        series_name_zh="全国居民消费价格同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="NATIONAL_SAME",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_NATIONAL_MOM",
        series_name_zh="全国居民消费价格环比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="NATIONAL_SEQUENTIAL",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_NATIONAL_YTD",
        series_name_zh="全国居民消费价格累计同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="NATIONAL_ACCUMULATE",
        unit_zh="百分比",
        source_value_offset=-100.0,
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_CITY_YOY",
        series_name_zh="城市居民消费价格同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="CITY_SAME",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_CITY_MOM",
        series_name_zh="城市居民消费价格环比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="CITY_SEQUENTIAL",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_CITY_YTD",
        series_name_zh="城市居民消费价格累计同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="CITY_ACCUMULATE",
        unit_zh="百分比",
        source_value_offset=-100.0,
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_RURAL_YOY",
        series_name_zh="农村居民消费价格同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="RURAL_SAME",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_RURAL_MOM",
        series_name_zh="农村居民消费价格环比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="RURAL_SEQUENTIAL",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="CPI_RURAL_YTD",
        series_name_zh="农村居民消费价格累计同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_CPI",
        source_column="RURAL_ACCUMULATE",
        unit_zh="百分比",
        source_value_offset=-100.0,
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="PPI_YOY",
        series_name_zh="工业生产者出厂价格同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_PPI",
        source_column="BASE_SAME",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="PPI_YTD",
        series_name_zh="工业生产者出厂价格累计同比",
        frequency="month_end",
        availability_rule="next_month_day_9_next_weekday",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_PPI",
        source_column="BASE_ACCUMULATE",
        unit_zh="百分比",
        source_value_offset=-100.0,
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="PMI_MANUFACTURING",
        series_name_zh="制造业采购经理指数",
        frequency="month_end",
        availability_rule="month_end_february_march_4",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_PMI",
        source_column="MAKE_INDEX",
        unit_zh="指数点",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="PMI_NON_MANUFACTURING",
        series_name_zh="非制造业商务活动指数",
        frequency="month_end",
        availability_rule="month_end_february_march_4",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_PMI",
        source_column="NMAKE_INDEX",
        unit_zh="指数点",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="GDP_YOY",
        series_name_zh="国内生产总值同比",
        frequency="quarter_end",
        availability_rule="quarter_end_plus_16_days",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_GDP",
        source_column="SUM_SAME",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="GDP_PRIMARY_YOY",
        series_name_zh="第一产业增加值同比",
        frequency="quarter_end",
        availability_rule="quarter_end_plus_16_days",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_GDP",
        source_column="FIRST_SAME",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="GDP_SECONDARY_YOY",
        series_name_zh="第二产业增加值同比",
        frequency="quarter_end",
        availability_rule="quarter_end_plus_16_days",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_GDP",
        source_column="SECOND_SAME",
        unit_zh="百分比",
    ),
    MacroReleaseSeries(
        dataset_name="macro_release",
        series_code="GDP_TERTIARY_YOY",
        series_name_zh="第三产业增加值同比",
        frequency="quarter_end",
        availability_rule="quarter_end_plus_16_days",
        source_system="东方财富数据中心",
        source_api="RPT_ECONOMY_GDP",
        source_column="THIRD_SAME",
        unit_zh="百分比",
    ),
)


MACRO_RELEASE_SERIES_BY_KEY = {
    (series.dataset_name, series.series_code): series
    for series in MACRO_RELEASE_SERIES
}


def is_valid_report_date(series: MacroReleaseSeries, report_date: date) -> bool:
    """判断日期是否属于该系列的理论频率格点。"""

    if series.frequency == "business_day":
        return report_date.weekday() < 5

    month_end = calendar.monthrange(report_date.year, report_date.month)[1]
    if report_date.day != month_end:
        return False

    if series.frequency == "month_end":
        return True
    if series.frequency == "quarter_end":
        return report_date.month in {3, 6, 9, 12}
    raise ValueError(f"未知宏观理论频率：{series.frequency}")


def expected_available_date(
    series: MacroReleaseSeries,
    report_date: date,
) -> date:
    """按版本化项目规则计算最早可用日期；它不是 API 实际发布日期。"""

    if not is_valid_report_date(series, report_date):
        raise ValueError(
            f"{series.series_code} 的报告/观测日期不符合 {series.frequency}："
            f"{report_date}"
        )

    if series.availability_rule == "same_day":
        return report_date

    if series.availability_rule == "next_month_day_9_next_weekday":
        if report_date.month == 12:
            available_date = date(report_date.year + 1, 1, 9)
        else:
            available_date = date(report_date.year, report_date.month + 1, 9)
        while available_date.weekday() >= 5:
            available_date += timedelta(days=1)
        return available_date

    if series.availability_rule == "month_end_february_march_4":
        if report_date.month == 2:
            return date(report_date.year, 3, 4)
        return report_date

    if series.availability_rule == "quarter_end_plus_16_days":
        return report_date + timedelta(days=16)

    raise ValueError(f"未知宏观可用日规则：{series.availability_rule}")


if len(MACRO_RELEASE_SERIES_BY_KEY) != len(MACRO_RELEASE_SERIES):
    raise ValueError("宏观系列配置存在重复的数据集类型—系列代码。")
if len({series.series_code for series in MACRO_RELEASE_SERIES}) != len(
    MACRO_RELEASE_SERIES
):
    raise ValueError("宏观系列配置存在跨数据集重复的系列代码。")
if len({
    (series.source_api, series.source_column)
    for series in MACRO_RELEASE_SERIES
}) != len(MACRO_RELEASE_SERIES):
    raise ValueError("宏观系列配置存在重复的来源 API—原列映射。")
if sum(series.dataset_name == "interest_rate" for series in MACRO_RELEASE_SERIES) != 8:
    raise ValueError("宏观系列配置必须恰好包含 8 个 SHIBOR 系列。")
if sum(series.dataset_name == "macro_release" for series in MACRO_RELEASE_SERIES) != 17:
    raise ValueError("宏观系列配置必须恰好包含 17 个宏观发布系列。")

_EXPECTED_FREQUENCY_BY_RULE = {
    "same_day": "business_day",
    "next_month_day_9_next_weekday": "month_end",
    "month_end_february_march_4": "month_end",
    "quarter_end_plus_16_days": "quarter_end",
}
for configured_series in MACRO_RELEASE_SERIES:
    if (
        _EXPECTED_FREQUENCY_BY_RULE.get(configured_series.availability_rule)
        != configured_series.frequency
    ):
        raise ValueError(
            f"{configured_series.series_code} 的理论频率与可用日规则不匹配。"
        )
    if configured_series.dataset_name == "interest_rate" and (
        configured_series.frequency != "business_day"
        or configured_series.source_api != "pro.shibor"
        or configured_series.source_value_offset != 0.0
    ):
        raise ValueError(
            f"{configured_series.series_code} 的 SHIBOR 数据集、频率、来源或数值变换不一致。"
        )
    if configured_series.dataset_name == "macro_release" and (
        configured_series.frequency == "business_day"
        or not configured_series.source_api.startswith("RPT_ECONOMY_")
    ):
        raise ValueError(
            f"{configured_series.series_code} 的宏观数据集、频率或来源不一致。"
        )
    if configured_series.source_value_offset not in {0.0, -100.0}:
        raise ValueError(
            f"{configured_series.series_code} 使用了未经约定的来源数值偏移。"
        )
