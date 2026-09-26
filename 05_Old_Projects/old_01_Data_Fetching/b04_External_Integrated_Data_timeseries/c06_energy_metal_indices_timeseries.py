#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[4]:


external_integrated_data_dir = os.path.join(base_dir, "External_Integrated_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 时间序列文件夹路径 EID_timeseries_path
EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

# 时序数据已完成日历路径 EID_dimension_timeseries_completed_calendar_path
EID_dimension_timeseries_completed_calendar_path = os.path.join(EID_timeseries_path, 'dimension_timeseries_completed_calendar.csv')
dimension_timeseries_completed_calendar = pd.read_csv(EID_dimension_timeseries_completed_calendar_path, dtype = {'date': str})
dimension_timeseries_completed_calendar


# In[5]:


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

def NYMEX_WTI_Continuous_Commodity_Index() -> pd.DataFrame:
    """
    美原油指数 (CONC) NYMEX WTI Continuous Commodity Index
    NYMEX WTI (西德克萨斯中质原油)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI01508580.html
    :return: 美原油指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI01508580")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "美原油指数",
        "日期",
    ]
    big_df = big_df[["日期", "美原油指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["美原油指数"] = pd.to_numeric(big_df["美原油指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def nyzs_energy_index() -> pd.DataFrame:
    """
    生意社能源指数 (nyzs energy index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00662539.html
    https://www.100ppi.com/cindex/nyzs.html # 生意社网址
    https://m1.100ppi.com/hyzs/nyzs # 能源指数手机端页面网址

    :return: 能源指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00662539")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "能源指数",
        "日期",
    ]
    big_df = big_df[["日期", "能源指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["能源指数"] = pd.to_numeric(big_df["能源指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_China_Coke_Price_Index() -> pd.DataFrame:
    """
    钢联中国焦炭价格指数 (Mysteel China Coke Price Index)

    https://data.eastmoney.com/cjsj/hyzs_list_EMI00018828.html
    https://index.mysteel.com/xpic/detail.html?tabName=jiaotan # 我的钢铁

    :return: 焦炭指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00018828")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "焦炭指数",
        "日期",
    ]
    big_df = big_df[["日期", "焦炭指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["焦炭指数"] = pd.to_numeric(big_df["焦炭指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def sys_gtzs_steel_index() -> pd.DataFrame:
    """
    生意社钢铁指数 (sys gtzs steel index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00662545.html
    https://www.100ppi.com/cindex/gtzs.html # 生意社钢铁指数

    :return: 钢铁指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00662545")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "钢铁指数",
        "日期",
    ]
    big_df = big_df[["日期", "钢铁指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["钢铁指数"] = pd.to_numeric(big_df["钢铁指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_Steel_Price_Index() -> pd.DataFrame:
    """
    钢联普钢综合价格指数 (Mysteel Steel Price Index - Ordinary Steel Composite)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00064821.html
    :return: 普钢指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00064821")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "普钢指数",
        "日期",
    ]
    big_df = big_df[["日期", "普钢指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["普钢指数"] = pd.to_numeric(big_df["普钢指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Xinhua_China_Iron_Ore_Price_Index() -> pd.DataFrame:
    """
    新华社中国铁矿石价格指数 (Xinhua-China Iron Ore Price Index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00064805.html

    :return: 铁矿石指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00064805")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "铁矿石指数",
        "日期",
    ]
    big_df = big_df[["日期", "铁矿石指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["铁矿石指数"] = pd.to_numeric(big_df["铁矿石指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def sys_yousezhishu_nonferrous_metals_index() -> pd.DataFrame:
    """
    生意社有色指数 (sys yousezhishu nonferrous metals index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00662542.html
    https://www.100ppi.com/cindex/yszs.html # 生意社

    :return: 有色指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00662542")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "有色指数",
        "日期",
    ]
    big_df = big_df[["日期", "有色指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["有色指数"] = pd.to_numeric(big_df["有色指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_Nickel_Price_Index() -> pd.DataFrame:
    """
    钢联镍价格指数 (Mysteel Nickel Price Index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00135907.html
    https://index.mysteel.com/xpic/detail.html?tabName=youse # 钢联有色指数

    https://index.mysteel.com/ysinfo.html # 指数介绍

    :return: 镍指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00135907")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "镍指数",
        "日期",
    ]
    big_df = big_df[["日期", "镍指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["镍指数"] = pd.to_numeric(big_df["镍指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_Tin_Price_Index() -> pd.DataFrame:
    """
    钢联锡价格指数 (Mysteel Tin Price Index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00135906.html
    https://index.mysteel.com/xpic/detail.html?tabName=youse # 钢联有色指数

    https://index.mysteel.com/ysinfo.html # 指数介绍

    :return: 锡指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00135906")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "锡指数",
        "日期",
    ]
    big_df = big_df[["日期", "锡指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["锡指数"] = pd.to_numeric(big_df["锡指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_Zinc_Price_Index() -> pd.DataFrame:
    """
    钢联锌价格指数 (Mysteel Zinc Price Index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00135906.html
    https://index.mysteel.com/xpic/detail.html?tabName=youse # 钢联有色指数

    https://index.mysteel.com/ysinfo.html # 指数介绍


    :return: 锌指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00135905")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "锌指数",
        "日期",
    ]
    big_df = big_df[["日期", "锌指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["锌指数"] = pd.to_numeric(big_df["锌指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_Lead_Price_Index() -> pd.DataFrame:
    """
    钢联铅价格指数 (Mysteel Lead Price Index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00135906.html
    https://index.mysteel.com/xpic/detail.html?tabName=youse # 钢联有色指数

    https://index.mysteel.com/ysinfo.html # 指数介绍

    :return: 铅指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00135904")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "铅指数",
        "日期",
    ]
    big_df = big_df[["日期", "铅指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["铅指数"] = pd.to_numeric(big_df["铅指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_Aluminum_Price_Index() -> pd.DataFrame:
    """
    钢联铝价格指数 (Mysteel Aluminum Price Index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00135906.html
    https://index.mysteel.com/xpic/detail.html?tabName=youse # 钢联有色指数

    https://index.mysteel.com/ysinfo.html # 指数介绍

    :return: 铝指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00135903")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "铝指数",
        "日期",
    ]
    big_df = big_df[["日期", "铝指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["铝指数"] = pd.to_numeric(big_df["铝指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df

def Mysteel_Copper_Price_Index() -> pd.DataFrame:
    """
    钢联铜价格指数 (Mysteel Copper Price Index)
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00135906.html
    https://index.mysteel.com/xpic/detail.html?tabName=youse # 钢联有色指数

    https://index.mysteel.com/ysinfo.html # 指数介绍

    :return: 铜指数
    :rtype: pandas.DataFrame
    """
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    params = {
        "sortColumns": "REPORT_DATE",
        "sortTypes": "-1",
        "pageSize": "500",
        "pageNumber": "1",
        "reportName": "RPT_INDUSTRY_INDEX",
        "columns": "INDICATOR_ID,INDICATOR_VALUE,REPORT_DATE",
        "filter": '(INDICATOR_ID="EMI00135902")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params=params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave=False):
        params.update({"pageNumber": page})
        r = requests.get(url, params=params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index=True)
    big_df.drop_duplicates(inplace=True)
    big_df.columns = [
        "指标ID",
        "铜指数",
        "日期",
    ]
    big_df = big_df[["日期", "铜指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["铜指数"] = pd.to_numeric(big_df["铜指数"])
    big_df.sort_values(["日期"], inplace=True)
    big_df.reset_index(inplace=True, drop=True)
    return big_df


# In[6]:


def refresh_energy_metal_indices_timeseries(
    get_tqdm,
    NYMEX_WTI_Continuous_Commodity_Index,
    nyzs_energy_index,
    Mysteel_China_Coke_Price_Index,
    sys_gtzs_steel_index,
    Mysteel_Steel_Price_Index,
    Xinhua_China_Iron_Ore_Price_Index,
    sys_yousezhishu_nonferrous_metals_index,
    Mysteel_Nickel_Price_Index,
    Mysteel_Tin_Price_Index,
    Mysteel_Zinc_Price_Index,
    Mysteel_Lead_Price_Index,
    Mysteel_Aluminum_Price_Index,
    Mysteel_Copper_Price_Index
):

    # 时间序列文件夹路径 EID_timeseries_path
    EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

    # 能源、金属指数文件夹路径 EID_energy_metal_indices_timeseries_path
    EID_energy_metal_indices_timeseries_path = os.path.join(EID_timeseries_path, "energy_metal_indices_timeseries")
    os.makedirs(EID_energy_metal_indices_timeseries_path, exist_ok = True)

    # 定义指标获取函数与输出文件名的映射
    indices_mapping = {

        # 美原油指数 (CONC) NYMEX WTI Continuous Commodity Index
        "CONC_WTI_Index.csv": NYMEX_WTI_Continuous_Commodity_Index,

        # 生意社能源指数 (nyzs energy index)
        "Nyzs_Energy_Index.csv": nyzs_energy_index,

        # 钢联中国焦炭价格指数 (Mysteel China Coke Price Index)
        "Mysteel_Coke_Index.csv": Mysteel_China_Coke_Price_Index,

        # 生意社钢铁指数 (sys gtzs steel index)
        "Sys_Steel_Index.csv": sys_gtzs_steel_index,

        # 钢联普钢综合价格指数 (Mysteel Steel Price Index - Ordinary Steel Composite)
        "Mysteel_Steel_Composite_Index.csv": Mysteel_Steel_Price_Index,

        # 新华社中国铁矿石价格指数 (Xinhua-China Iron Ore Price Index)
        "Xinhua_Iron_Ore_Index.csv": Xinhua_China_Iron_Ore_Price_Index,

        # 生意社有色指数 (sys yousezhishu nonferrous metals index)
        "Sys_Nonferrous_Index.csv": sys_yousezhishu_nonferrous_metals_index,

        # 钢联镍价格指数 (Mysteel Nickel Price Index)
        "Mysteel_Nickel_Index.csv": Mysteel_Nickel_Price_Index,

        # 钢联锡价格指数 (Mysteel Tin Price Index)
        "Mysteel_Tin_Index.csv": Mysteel_Tin_Price_Index,

        # 钢联锌价格指数 (Mysteel Zinc Price Index)
        "Mysteel_Zinc_Index.csv": Mysteel_Zinc_Price_Index,

        # 钢联铅价格指数 (Mysteel Lead Price Index)
        "Mysteel_Lead_Index.csv": Mysteel_Lead_Price_Index,

        # 钢联铝价格指数 (Mysteel Aluminum Price Index)
        "Mysteel_Aluminum_Index.csv": Mysteel_Aluminum_Price_Index,

        # 钢联铜价格指数 (Mysteel Copper Price Index)
        "Mysteel_Copper_Index.csv": Mysteel_Copper_Price_Index,
    }

    tqdm_obj = get_tqdm()
    for filename, fetch_func in tqdm_obj(indices_mapping.items(), desc = "Fetching Energy & Metal Indices"):
        filepath = os.path.join(EID_energy_metal_indices_timeseries_path, filename)
        try:
            df = fetch_func()
            df.to_csv(filepath, index = False)
            print(f"{filename}: {len(df)} 条记录已写入")
        except Exception as e:
            print(f"{filename}: 获取失败 - {str(e)}")

        time.sleep(3) # 冷却以防止被 ban


# In[8]:


if __name__ == '__main__':
    refresh_energy_metal_indices_timeseries(
        get_tqdm,
        NYMEX_WTI_Continuous_Commodity_Index,
        nyzs_energy_index,
        Mysteel_China_Coke_Price_Index,
        sys_gtzs_steel_index,
        Mysteel_Steel_Price_Index,
        Xinhua_China_Iron_Ore_Price_Index,
        sys_yousezhishu_nonferrous_metals_index,
        Mysteel_Nickel_Price_Index,
        Mysteel_Tin_Price_Index,
        Mysteel_Zinc_Price_Index,
        Mysteel_Lead_Price_Index,
        Mysteel_Aluminum_Price_Index,
        Mysteel_Copper_Price_Index
    )


# In[ ]:




