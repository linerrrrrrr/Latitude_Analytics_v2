#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[2]:


external_integrated_data_dir = os.path.join(base_dir, "External_Integrated_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 时间序列文件夹路径 EID_timeseries_path
EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

# 时序数据已完成日历路径 EID_dimension_timeseries_completed_calendar_path
EID_dimension_timeseries_completed_calendar_path = os.path.join(EID_timeseries_path, 'dimension_timeseries_completed_calendar.csv')
dimension_timeseries_completed_calendar = pd.read_csv(EID_dimension_timeseries_completed_calendar_path, dtype = {'date': str})
dimension_timeseries_completed_calendar


# In[25]:


def get_tqdm(enable: bool = True):
    """
    返回适用于当前环境的 tqdm 对象。
    Args: enable (bool): 是否启用进度条。默认为 True。
    Returns: tqdm 对象。
    """
    if not enable:
        # 如果进度条被禁用，返回一个不显示进度条的 tqdm 对象
        return lambda iterable, *args, **kwargs: iterable

    try:
        # 尝试检查是否在 jupyter notebook 环境中，有利于退出进度条
        # noinspection PyUnresolvedReferences
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell": from tqdm.notebook import tqdm
        else: from tqdm import tqdm
    except (NameError, ImportError):
        # 如果不在 Jupyter 环境中，就使用标准 tqdm
        from tqdm import tqdm
    return tqdm

def index_cpi() -> pd.DataFrame:
    """
    中国 CPI 同比指数（含可用日期，避免未来数据泄露）
    https://data.eastmoney.com/cjsj/cpi.html

    发布规则：月度数据次月 9 日发布（遇节假日顺延）
    :return: CPI 数据，包含全国、城市、农村的同比、环比、累计等数据及可用日期
    :rtype: pandas.DataFrame
    """
    import calendar

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "columns": "REPORT_DATE,TIME,NATIONAL_SAME,NATIONAL_BASE,NATIONAL_SEQUENTIAL,NATIONAL_ACCUMULATE,CITY_SAME,CITY_BASE,CITY_SEQUENTIAL,CITY_ACCUMULATE,RURAL_SAME,RURAL_BASE,RURAL_SEQUENTIAL,RURAL_ACCUMULATE",
        "pageNumber": "1",
        "pageSize": "500",
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
        "reportName": "RPT_ECONOMY_CPI",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm = get_tqdm()

    for page in tqdm(range(1, total_page + 1), leave = False, desc = "正在获取CPI数据"):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)

    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "报告日期",
        "时间描述",
        "全国同比",
        "全国定基",
        "全国环比",
        "全国累计",
        "城市同比",
        "城市定基",
        "城市环比",
        "城市累计",
        "农村同比",
        "农村定基",
        "农村环比",
        "农村累计",
    ]
    big_df = big_df[[
        "报告日期",
        "全国同比", "全国环比", "全国累计",
        "城市同比", "城市环比", "城市累计",
        "农村同比", "农村环比", "农村累计",
    ]]
    big_df["报告日期"] = pd.to_datetime(big_df["报告日期"]).dt.date
    for col in big_df.columns:
        if col not in ["报告日期"]:
            big_df[col] = pd.to_numeric(big_df[col], errors = "coerce")
    big_df.sort_values(["报告日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)

    # 创建可用日期（次月 9 日发布，遇周末顺延到周一）
    def get_cpi_available_date(report_date):
        report_date = pd.to_datetime(report_date)
        if report_date.month  ==  12:
            available = report_date.replace(year = report_date.year + 1, month = 1, day = 9)
        else:
            available = report_date.replace(month = report_date.month + 1, day = 9)
        # 周末顺延
        if available.weekday()  ==  5:  # 周六
            available +=  datetime.timedelta(days = 2)
        elif available.weekday()  ==  6:  # 周日
            available +=  datetime.timedelta(days = 1)
        return available.date()

    big_df["可用日期"] = big_df["报告日期"].apply(get_cpi_available_date)

    cols = ["报告日期", "可用日期"] + [c for c in big_df.columns if c not in ["报告日期", "可用日期", "时间描述"]] # 调整列顺序：报告日期、可用日期、时间描述、其他指标
    big_df = big_df[cols]

    return big_df

def index_ppi() -> pd.DataFrame:
    """
    中国 PPI 同比指数（工业生产者出厂价格指数，含可用日期）
    https://data.eastmoney.com/cjsj/ppi.html

    发布规则：月度数据次月 9 日发布（遇节假日顺延）
    :return: PPI 数据，包含同比、累计同比及可用日期
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "columns": "REPORT_DATE,BASE,BASE_ACCUMULATE",
        "pageNumber": "1",
        "pageSize": "500",
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
        "reportName": "RPT_ECONOMY_PPI",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm = get_tqdm()

    for page in tqdm(range(1, total_page + 1), leave = False, desc = "正在获取PPI数据"):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)

    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "报告日期",
        "PPI同比",
        "PPI累计同比",
    ]
    big_df["报告日期"] = pd.to_datetime(big_df["报告日期"]).dt.date
    big_df["PPI同比"] = pd.to_numeric(big_df["PPI同比"], errors = "coerce")
    big_df["PPI累计同比"] = pd.to_numeric(big_df["PPI累计同比"], errors = "coerce")
    big_df.sort_values(["报告日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)

    # 创建可用日期（次月9日发布，遇周末顺延到周一）
    def get_ppi_available_date(report_date):
        report_date = pd.to_datetime(report_date)
        if report_date.month  ==  12:
            available = report_date.replace(year = report_date.year + 1, month = 1, day = 9)
        else:
            available = report_date.replace(month = report_date.month + 1, day = 9)
        if available.weekday()  ==  5:  # 周六
            available +=  datetime.timedelta(days = 2)
        elif available.weekday()  ==  6:  # 周日
            available +=  datetime.timedelta(days = 1)
        return available.date()

    big_df["可用日期"] = big_df["报告日期"].apply(get_ppi_available_date)

    big_df = big_df[["报告日期", "可用日期", "PPI同比", "PPI累计同比"]] # 调整列顺序

    return big_df

def index_pmi() -> pd.DataFrame:
    """
    中国 PMI 指数（采购经理指数，含可用日期）
    https://data.eastmoney.com/cjsj/pmi.html

    发布规则：当月最后一天 9:30 发布（遇节假日顺延），2 月数据通常 3 月 4 日发布（春节影响）
    :return: PMI 数据，包含制造业 PMI、非制造业 PMI 及可用日期
    :rtype: pandas.DataFrame
    """
    import calendar

    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "columns": "REPORT_DATE,MAKE_INDEX,NMAKE_INDEX",
        "pageNumber": "1",
        "pageSize": "500",
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
        "reportName": "RPT_ECONOMY_PMI",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm = get_tqdm()

    for page in tqdm(range(1, total_page + 1), leave = False, desc = "正在获取PMI数据"):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)

    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "报告日期",
        "制造业PMI",
        "非制造业PMI",
    ]
    big_df["报告日期"] = pd.to_datetime(big_df["报告日期"]).dt.date
    big_df["制造业PMI"] = pd.to_numeric(big_df["制造业PMI"], errors = "coerce")
    big_df["非制造业PMI"] = pd.to_numeric(big_df["非制造业PMI"], errors = "coerce")
    big_df.sort_values(["报告日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)

    # 创建可用日期（当月最后一天，2 月特殊处理为 3 月 4 日，遇周末顺延）
    def get_pmi_available_date(report_date):
        report_date = pd.to_datetime(report_date)
        # 特殊情况：2 月 PMI 通常在 3 月 4 日发布（春节假期影响）
        if report_date.month  ==  2:
            available = report_date.replace(month = 3, day = 4)
        else:
            # 当月最后一天
            last_day = calendar.monthrange(report_date.year, report_date.month)[1]
            available = report_date.replace(day = last_day)

        # 周末顺延到周一
        if available.weekday()  ==  5:  # 周六
            available +=  datetime.timedelta(days = 2)
        elif available.weekday()  ==  6:  # 周日
            available +=  datetime.timedelta(days = 1)
        return available.date()

    big_df["可用日期"] = big_df["报告日期"].apply(get_pmi_available_date)

    big_df = big_df[["报告日期", "可用日期", "制造业PMI", "非制造业PMI"]] # 调整列顺序

    return big_df

def index_gdp() -> pd.DataFrame:
    """
    中国 GDP 季度数据（国内生产总值，含可用日期）
    https://data.eastmoney.com/cjsj/gdp.html

    发布规则：季度数据在季度结束后约 15 天发布（遇节假日顺延）
    一季度：4 月 16 日左右；二季度：7 月 16 日左右；三季度：10 月 16 日左右；四季度：次年 1 月 16 日左右
    :return: GDP 季度数据，包含总值、各产业同比增速及可用日期
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "columns": "REPORT_DATE,SUM_SAME,FIRST_SAME,SECOND_SAME,THIRD_SAME",
        "pageNumber": "1",
        "pageSize": "500",
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "source": "WEB",
        "client": "WEB",
        "reportName": "RPT_ECONOMY_GDP",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm = get_tqdm()

    for page in tqdm(range(1, total_page + 1), leave = False, desc = "正在获取GDP数据"):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)

    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "报告日期",
        "GDP同比",
        "第一产业同比",
        "第二产业同比",
        "第三产业同比",
    ]
    big_df["报告日期"] = pd.to_datetime(big_df["报告日期"]).dt.date
    for col in ["GDP同比", "第一产业同比", "第二产业同比", "第三产业同比"]:
        big_df[col] = pd.to_numeric(big_df[col], errors = "coerce")
    big_df.sort_values(["报告日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)

    # 内嵌：创建可用日期（季度后约15天发布，遇周末顺延到周一）
    def get_gdp_available_date(report_date):
        report_date = pd.to_datetime(report_date)
        # 根据季度末月份确定发布日期
        if report_date.month  ==  3:  # 一季度
            available = report_date.replace(month = 4, day = 16)
        elif report_date.month  ==  6:  # 二季度
            available = report_date.replace(month = 7, day = 16)
        elif report_date.month  ==  9:  # 三季度
            available = report_date.replace(month = 10, day = 16)
        elif report_date.month  ==  12:  # 四季度
            available = report_date.replace(year = report_date.year + 1, month = 1, day = 16)
        else:
            # 非季度末数据，默认15天后
            available = report_date + datetime.timedelta(days = 15)

        # 周末顺延到周一
        if available.weekday()  ==  5:  # 周六
            available +=  datetime.timedelta(days = 2)
        elif available.weekday()  ==  6:  # 周日
            available +=  datetime.timedelta(days = 1)
        return available.date()

    big_df["可用日期"] = big_df["报告日期"].apply(get_gdp_available_date)

    big_df = big_df[["报告日期", "可用日期", "GDP同比", "第一产业同比", "第二产业同比", "第三产业同比"]]  # 调整列顺序

    return big_df


# In[27]:


def refresh_macro_indicators_timeseries(
    get_tqdm, index_cpi, index_ppi, index_pmi, index_gdp
):

    # 时间序列文件夹路径 EID_timeseries_path
    EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

    # 宏观经济指数文件夹路径 EID_macro_indicators_timeseries_path
    EID_macro_indicators_timeseries_path = os.path.join(EID_timeseries_path, "macro_indicators_timeseries")
    os.makedirs(EID_macro_indicators_timeseries_path, exist_ok = True)

    # 定义指标获取函数与输出文件名的映射
    indices_mapping = {

        # 中国 CPI 同比指数
        "CPI_Index.csv": index_cpi,

        # 中国 PPI 同比指数（工业生产者出厂价格指数）
        "PPI_Index.csv": index_ppi,

        # 中国 PMI 指数（采购经理指数）
        "PMI_Index.csv": index_pmi,

        # 中国 GDP 季度数据（国内生产总值）
        "GDP_Index.csv": index_gdp,
    }

    tqdm_obj = get_tqdm()
    for filename, fetch_func in tqdm_obj(indices_mapping.items(), desc = "Fetching Macro Indicators"):
        filepath = os.path.join(EID_macro_indicators_timeseries_path, filename)
        try:
            df = fetch_func()
            df.to_csv(filepath, index = False)
            print(f"{filename}: {len(df)} 条记录已写入")
        except Exception as e:
            print(f"{filename}: 获取失败 - {str(e)}")

        time.sleep(3) # 冷却以防止被 ban


# In[28]:


if __name__ == "__main__":
    refresh_macro_indicators_timeseries(
        get_tqdm, index_cpi, index_ppi, index_pmi, index_gdp
    )


# In[ ]:




