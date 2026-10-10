#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pn.extension()  # 启用 Jupyter 支持
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[2]:


external_integrated_data_dir = os.path.join(base_dir, "External_Integrated_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 交易所日历路径 EID_dimension_exchange_calendar_path
EID_dimension_exchange_calendar_path = os.path.join(external_integrated_data_dir, 'dimension_exchange_calendar.csv')

# 原始数据文件夹路径 EID_initial_path
EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")

# 原始数据日历路径 EID_dimension_initial_calendar_path
EID_dimension_initial_calendar_path = os.path.join(EID_initial_path, 'dimension_initial_calendar.csv')

# 原始数据日历 dimension_initial_calendar
dimension_initial_calendar = pd.read_csv(EID_dimension_initial_calendar_path, dtype = {'date': str})
dimension_initial_calendar


# In[3]:


def fetch_xianqi_table(
    date_str: str,
    timeout: int,
):

    date_str = datetime.datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    url = f"https://www.100ppi.com/sf/day-{date_str}.html"

    default_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Referer': 'https://www.100ppi.com/sf/'
    }

    try:
        response = requests.get(url, headers = default_headers, timeout = timeout)
        response.encoding = 'utf-8'
        response.raise_for_status() # 对返回 404 或 200 等不报错的拒绝和丢失进行中断
    except requests.RequestException as e:
        raise ConnectionError(f"获取页面失败: {e}")

    return response.text


# In[4]:


def fact_spot_price_html_initial_refreshion(deep_validation_switch = False):

    # 原始数据文件夹路径 EID_initial_path
    EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")

    # 原始数据日历路径 EID_dimension_initial_calendar_path
    EID_dimension_initial_calendar_path = os.path.join(EID_initial_path, 'dimension_initial_calendar.csv')

    # 原始数据日历 dimension_initial_calendar
    dimension_initial_calendar = pd.read_csv(EID_dimension_initial_calendar_path, dtype = {'date': str})

    date_to_process = dimension_initial_calendar.loc[dimension_initial_calendar['spot_price'] == 1, 'date']
    for date in date_to_process:

        # EID 原始数据日期路径文件夹路径 EID_initial_date_path
        EID_initial_date_path = os.path.join(EID_initial_path, date)

        # 现货原始数据路径 fact_spot_price_html_initial_path
        fact_spot_price_html_initial_path = os.path.join(EID_initial_date_path, 'fact_spot_price_html_initial.html')

        if os.path.exists(fact_spot_price_html_initial_path):
            if deep_validation_switch:
                try:
                    from pathlib import Path
                    html_content = Path(fact_spot_price_html_initial_path).read_text(encoding='utf-8')
                    if html_content and len(html_content.strip()) > 100: continue

                except Exception as e: pass
            else: continue

        # 现货原始 html 数据 fact_spot_price_html_initial
        fact_spot_price_html_initial = fetch_xianqi_table(date_str = str(date), timeout = 30)

        from pathlib import Path
        Path(fact_spot_price_html_initial_path).write_text(fact_spot_price_html_initial, encoding = 'utf-8')
        time.sleep(0.1)


# In[7]:


if '__main__' == __name__:

    html_content = fetch_xianqi_table(date_str = '20110104', timeout = 30)
    print(html_content)

    fact_spot_price_html_initial_refreshion(deep_validation_switch = False)


# In[ ]:




