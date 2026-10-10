#!/usr/bin/env python
# coding: utf-8

# In[6]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[7]:


native_exchange_data_dir = os.path.join(base_dir, "Native_Exchange_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

if not os.path.exists(native_exchange_data_dir):
    os.makedirs(native_exchange_data_dir)


# In[8]:


def get_effective_datetime(hour = 0, minute = 0, second = 0, microsecond = 0):
    now = datetime.datetime.now()
    cutoff_time = now.replace(hour = hour, minute = minute, second = second, microsecond = microsecond)

    # 若已超过 cutoff_time，使用今天；否则使用昨天
    effective_datetime = now if now >= cutoff_time else now - datetime.timedelta(days = 1) # 若已过 cutoff_time，返回今天；否则返回昨天
    effective_datetime = effective_datetime.strftime("%Y%m%d")
    return effective_datetime


# In[4]:


def refresh_dimension_exchange_calendar(effective_datetime, save_switch = False):

    # 交易所日历路径 NED_dimension_exchange_calendar_path
    NED_dimension_exchange_calendar_path = os.path.join(
        native_exchange_data_dir, 'dimension_exchange_calendar.csv'
    )
    calendar_list = []
    exchange_list = [
        "SHFE", # 上期所
        "CZCE", # 郑商所
        "DCE", # 大商所
        "CFFEX", # 中金所
        "INE", # 上能源
        "GFEX", # 广期所
    ]

    print( "=" * 50)
    print("正在更新交易所日历 dimension_exchange_calendar......")
    print(f"NED_dimension_exchange_calendar_path: {NED_dimension_exchange_calendar_path}")
    print(f'effective_datetime: {effective_datetime}')

    for exchange in exchange_list:
        for _ in range(3):
            try:
                calendar_df = pro.trade_cal(exchange = exchange, start_date = '20100101', end_date = effective_datetime)
                print(f"获取 {exchange} 日历完成...")
                time.sleep(0.3)
                break
            except Exception as e:
                print(f"第 { _ +1} 次获取 {exchange} 日历失败, {e}...")

        calendar_df = calendar_df[["cal_date", "is_open"]].copy()
        calendar_df = calendar_df.rename(columns = {'is_open': f'{exchange}', 'cal_date': 'date', }) # 重命名列防止冲突
        calendar_list.append(calendar_df)

    dimension_exchange_calendar = calendar_list[0]
    for calendar_df in calendar_list[1:]: # 合并所有日历
        dimension_exchange_calendar = pd.merge(dimension_exchange_calendar, calendar_df, how = 'outer', on = 'date')
    dimension_exchange_calendar = dimension_exchange_calendar.sort_values('date').reset_index(drop = True)

    cols_to_fill = [col for col in dimension_exchange_calendar.columns if not col == 'date']
    mask = dimension_exchange_calendar['date'].isin(['20221231', ]) # 20221231数据异常，SHFE CZCE DCE 为空
    dimension_exchange_calendar.loc[mask, cols_to_fill] = (
        dimension_exchange_calendar
        .loc[mask, cols_to_fill]
        .fillna(0)
    )

    print("完成更新交易所日历 dimension_exchange_calendar")
    print( "=" * 50, end = "\n\n")

    if save_switch :
        dimension_exchange_calendar.to_csv(NED_dimension_exchange_calendar_path, index = False)
    return dimension_exchange_calendar


# In[5]:


dimension_exchange_calendar = None

if __name__ == '__main__':

    effective_datetime = get_effective_datetime(
        hour = 21, minute = 0, second = 0, microsecond = 0
    ) # 21:00 当天的基准时间
    dimension_exchange_calendar = refresh_dimension_exchange_calendar(effective_datetime, save_switch = True)

dimension_exchange_calendar


# In[ ]:




