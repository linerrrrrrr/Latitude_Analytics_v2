#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

# pn.extension()  # 启用 Jupyter 支持
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[2]:


external_integrated_data_dir = os.path.join(base_dir, "External_Integrated_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 交易所日历路径 EID_dimension_exchange_calendar_path
EID_dimension_exchange_calendar_path = os.path.join(external_integrated_data_dir, 'dimension_exchange_calendar.csv')

# 交易所日历 dimension_exchange_calendar
dimension_exchange_calendar = pd.read_csv(EID_dimension_exchange_calendar_path, dtype = {'date': str})
dimension_exchange_calendar


# In[3]:


def refresh_dimension_initial_calendar(save_switch = False):

    # 交易所日历路径 EID_dimension_exchange_calendar_path
    EID_dimension_exchange_calendar_path = os.path.join(external_integrated_data_dir, 'dimension_exchange_calendar.csv')

    # 原始数据文件夹路径 EID_initial_path
    EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")
    os.makedirs(EID_initial_path, exist_ok = True)

    # 原始数据日历 EID_dimension_initial_calendar_path
    EID_dimension_initial_calendar_path = os.path.join(EID_initial_path, 'dimension_initial_calendar.csv')

    # 交易所日历 dimension_exchange_calendar
    dimension_exchange_calendar = pd.read_csv(
        EID_dimension_exchange_calendar_path, dtype = {'date': str}
    )
    effective_date_str = dimension_exchange_calendar['date'].max()

    print("=" * 50)
    print("正在更新原始数据日历 dimension_initial_calendar......")
    print(f"EID_dimension_exchange_calendar_path: {EID_dimension_exchange_calendar_path}")
    print(f"EID_dimension_initial_calendar_path: {EID_dimension_initial_calendar_path}")
    print(f"effective_date_str: {effective_date_str}")


    date_column = dimension_exchange_calendar['date']
    SHFE_column = dimension_exchange_calendar['SHFE']  # 单独提取为变量，可用于遍历和赋值

    # 原始数据日历 dimension_initial_calendar
    # 添加 spot_price、yield_curve_central_bond、futures_foreign 列，内容与上期所开市日期保持一致
    dimension_initial_calendar = pd.DataFrame({
        'date': date_column,
        'spot_price': np.where(date_column >= '20110104', SHFE_column, 0), # spot_price 数据源开始日期为 20110104
        'futures_foreign': SHFE_column
    })

    print("完成更新原始数据日历 dimension_initial_calendar")
    print("=" * 50, end="\n\n")

    date_to_process = dimension_exchange_calendar.loc[dimension_exchange_calendar['SHFE'] == 1, 'date']
    for date in date_to_process.to_list():
        EID_initial_date_path =  os.path.join(EID_initial_path, date)
        os.makedirs(EID_initial_date_path, exist_ok = True)

    if save_switch:
        dimension_initial_calendar.to_csv(EID_dimension_initial_calendar_path, index = False)
    return dimension_initial_calendar


# In[4]:


dimension_initial_calendar = None
if __name__ == '__main__':
    dimension_initial_calendar = refresh_dimension_initial_calendar(save_switch = True)
dimension_initial_calendar


# In[ ]:




