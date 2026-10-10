#!/usr/bin/env python
# coding: utf-8

# In[14]:


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


# In[7]:


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

def Baltic_Dry_Index() -> pd.DataFrame:
    """
    波罗的海干散货指数 (BDI) Baltic Dry Index
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00107664.html
    :return: 波罗的海干散货指数
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
        "filter": '(INDICATOR_ID="EMI00107664")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave = False):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)
    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "指标ID",
        "波罗的海干散货指数",
        "日期",
    ]
    big_df = big_df[["日期", "波罗的海干散货指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["波罗的海干散货指数"] = pd.to_numeric(big_df["波罗的海干散货指数"])
    big_df.sort_values(["日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)
    return big_df

def Baltic_Panamax_Index() -> pd.DataFrame:
    """
    巴拿马型运费指数 (BPI) Baltic Panamax Index
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00107665.html
    :return: 巴拿马型运费指数
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
        "filter": '(INDICATOR_ID="EMI00107665")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave = False):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)
    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "指标ID",
        "巴拿马型运费指数",
        "日期",
    ]
    big_df = big_df[["日期", "巴拿马型运费指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["巴拿马型运费指数"] = pd.to_numeric(big_df["巴拿马型运费指数"])
    big_df.sort_values(["日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)
    return big_df

def Baltic_Capesize_Index() -> pd.DataFrame:
    """
    海岬型运费指数 (BCI) Baltic Capesize Index
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00107666.html
    :return: 海岬型运费指数
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
        "filter": '(INDICATOR_ID="EMI00107666")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave = False):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)
    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "指标ID",
        "海岬型运费指数",
        "日期",
    ]
    big_df = big_df[["日期", "海岬型运费指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["海岬型运费指数"] = pd.to_numeric(big_df["海岬型运费指数"])
    big_df.sort_values(["日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)
    return big_df

def Baltic_Supramax_Index() -> pd.DataFrame:
    """
    超灵便型船运价指数 (BSI) Baltic Supramax Index
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00107667.html
    :return: 超灵便型船运价指数
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
        "filter": '(INDICATOR_ID="EMI00107667")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave = False):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)
    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "指标ID",
        "超灵便型船运价指数",
        "日期",
    ]
    big_df = big_df[["日期", "超灵便型船运价指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["超灵便型船运价指数"] = pd.to_numeric(big_df["超灵便型船运价指数"])
    big_df.sort_values(["日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)
    return big_df

def Baltic_Dirty_Tanker_Index() -> pd.DataFrame:
    """
    原油运输指数 (BDTI) Baltic Dirty Tanker Index
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00107668.html
    :return: 原油运输指数
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
        "filter": '(INDICATOR_ID="EMI00107668")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave = False):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)
    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "指标ID",
        "原油运输指数",
        "日期",
    ]
    big_df = big_df[["日期", "原油运输指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["原油运输指数"] = pd.to_numeric(big_df["原油运输指数"])
    big_df.sort_values(["日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)
    return big_df

def Baltic_Clean_Tanker_Index() -> pd.DataFrame:
    """
    成品油运输指数 (BCTI) Baltic Clean Tanker Index
    https://data.eastmoney.com/cjsj/hyzs_list_EMI00107669.html
    :return: 成品油运输指数
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
        "filter": '(INDICATOR_ID="EMI00107669")',
        "source": "WEB",
        "client": "WEB",
    }
    r = requests.get(url, params = params)
    data_json = r.json()
    total_page = data_json["result"]["pages"]
    big_df = pd.DataFrame()
    tqdm_obj = get_tqdm()
    for page in tqdm_obj(range(1, total_page + 1), leave = False):
        params.update({"pageNumber": page})
        r = requests.get(url, params = params)
        data_json = r.json()
        temp_df = pd.DataFrame(data_json["result"]["data"])
        big_df = pd.concat([big_df, temp_df], ignore_index = True)
    big_df.drop_duplicates(inplace = True)
    big_df.columns = [
        "指标ID",
        "成品油运输指数",
        "日期",
    ]
    big_df = big_df[["日期", "成品油运输指数", "指标ID"]]
    big_df["日期"] = pd.to_datetime(big_df["日期"]).dt.date
    big_df["成品油运输指数"] = pd.to_numeric(big_df["成品油运输指数"])
    big_df.sort_values(["日期"], inplace = True)
    big_df.reset_index(inplace = True, drop = True)
    return big_df


# In[11]:


def refresh_baltic_shipping_timeseries(
    get_tqdm, Baltic_Dry_Index, Baltic_Panamax_Index, Baltic_Capesize_Index, Baltic_Supramax_Index, Baltic_Dirty_Tanker_Index, Baltic_Clean_Tanker_Index
):

    # 时间序列文件夹路径 EID_timeseries_path
    EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

    # 航运指数文件夹路径 EID_baltic_shipping_timeseries_path
    EID_baltic_shipping_timeseries_path = os.path.join(EID_timeseries_path, "baltic_shipping_timeseries")
    os.makedirs(EID_baltic_shipping_timeseries_path, exist_ok = True)

    # 定义指标获取函数与输出文件名的映射
    indices_mapping = {

        # 波罗的海干散货指数 (BDI) Baltic Dry Index
        "Baltic_Dry_Index.csv": Baltic_Dry_Index,

        # 巴拿马型运费指数 (BPI) Baltic Panamax Index
        "Baltic_Panamax_Index.csv": Baltic_Panamax_Index,

        # 海岬型运费指数 (BCI) Baltic Capesize Index
        "Baltic_Capesize_Index.csv": Baltic_Capesize_Index,

        # 超灵便型船运价指数 (BSI) Baltic Supramax Index
        "Baltic_Supramax_Index.csv": Baltic_Supramax_Index,

        # 原油运输指数 (BDTI) Baltic Dirty Tanker Index
        "Baltic_Dirty_Tanker_Index.csv": Baltic_Dirty_Tanker_Index,

        # 成品油运输指数 (BCTI) Baltic Clean Tanker Index
        "Baltic_Clean_Tanker_Index.csv": Baltic_Clean_Tanker_Index,
    }

    tqdm_obj = get_tqdm()
    for filename, fetch_func in tqdm_obj(indices_mapping.items(), desc = "Fetching Baltic Indices"):
        filepath = os.path.join(EID_baltic_shipping_timeseries_path, filename)
        try:
            df = fetch_func()
            df.to_csv(filepath, index = False)
            print(f"{filename}: {len(df)} 条记录已写入")
        except Exception as e:
            print(f"{filename}: 获取失败 - {str(e)}")

        time.sleep(3) # 冷却以防止被 ban


# In[13]:


if __name__ == "__main__":
    refresh_baltic_shipping_timeseries(
        get_tqdm, Baltic_Dry_Index, Baltic_Panamax_Index, Baltic_Capesize_Index, Baltic_Supramax_Index, Baltic_Dirty_Tanker_Index, Baltic_Clean_Tanker_Index
    )


# In[ ]:




