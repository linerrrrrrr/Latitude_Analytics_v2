#!/usr/bin/env python
# coding: utf-8

# In[58]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pn.extension()  # 启用 Jupyter 支持
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[59]:


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


# In[69]:


def fact_futures_foreign_initial_refreshion(deep_validation_switch = False):

    # 原始数据文件夹路径 EID_initial_path
    EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")

    # 原始数据日历路径 EID_dimension_initial_calendar_path
    EID_dimension_initial_calendar_path = os.path.join(EID_initial_path, 'dimension_initial_calendar.csv')

    # 原始数据日历 dimension_initial_calendar
    dimension_initial_calendar = pd.read_csv(EID_dimension_initial_calendar_path, dtype = {'date': str})

    date_to_process = dimension_initial_calendar.loc[dimension_initial_calendar['futures_foreign'] == 1, 'date']
    for date in date_to_process:

        # EID 原始数据日期路径文件夹路径 EID_initial_date_path
        EID_initial_date_path = os.path.join(EID_initial_path, date)

        # 外盘数据路径 fact_futures_foreign_initial_path
        fact_futures_foreign_initial_path = os.path.join(EID_initial_date_path, 'fact_futures_foreign_initial.csv')

        if os.path.exists(fact_futures_foreign_initial_path):
            if deep_validation_switch:
                fact_futures_foreign_initial = pd.read_csv(fact_futures_foreign_initial_path)
                if not fact_futures_foreign_initial.empty: continue
            else: continue

        date_str = datetime.datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
        fact_futures_foreign_initial = jqdatasdk.finance.run_query(
            jqdatasdk.query(
                jqdatasdk.finance.FUT_GLOBAL_DAILY
            ).filter(jqdatasdk.finance.FUT_GLOBAL_DAILY.day == date_str)
        )
        fact_futures_foreign_initial.to_csv(fact_futures_foreign_initial_path, index = False)
        time.sleep(0.1)


# In[70]:


if __name__ == '__main__':
    fact_futures_foreign_initial_refreshion(deep_validation_switch = False)


# In[ ]:




