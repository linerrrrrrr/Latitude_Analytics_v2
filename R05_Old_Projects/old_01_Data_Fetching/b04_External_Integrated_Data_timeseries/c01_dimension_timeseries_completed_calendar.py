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

# 时间序列文件夹路径 EID_timeseries_data
EID_timeseries_data = os.path.join(external_integrated_data_dir, "timeseries_data")
os.makedirs(EID_timeseries_data, exist_ok = True)

# 交易所日历路径 EID_dimension_exchange_calendar_path
EID_dimension_exchange_calendar_path = os.path.join(external_integrated_data_dir, 'dimension_exchange_calendar.csv')

# 交易所日历 dimension_exchange_calendar
dimension_exchange_calendar = pd.read_csv(EID_dimension_exchange_calendar_path, dtype = {'date': str})
dimension_exchange_calendar


# In[5]:


def refresh_dimension_timeseries_completed_calendar(save_switch = False):

    # 交易所日历路径 EID_dimension_exchange_calendar_path
    EID_dimension_exchange_calendar_path = os.path.join(external_integrated_data_dir, 'dimension_exchange_calendar.csv')

    # 时间序列文件夹路径 EID_timeseries_data
    EID_timeseries_data = os.path.join(external_integrated_data_dir, "timeseries_data")

    # 时序数据已完成日历路径 EID_dimension_timeseries_completed_calendar_path
    EID_dimension_timeseries_completed_calendar_path = os.path.join(EID_timeseries_data, 'dimension_timeseries_completed_calendar.csv')

    # 读取交易所日历
    dimension_exchange_calendar = pd.read_csv(
       EID_dimension_exchange_calendar_path,
       dtype = {'date': str}
    )
    effective_date_str = dimension_exchange_calendar['date'].max()

    print("=" * 50)
    print("正在更新时序数据已完成日历 dimension_timeseries_completed_calendar......")
    print(f"EID_dimension_exchange_calendar_path: {EID_dimension_exchange_calendar_path}")
    print(f"EID_dimension_timeseries_completed_calendar_path: {EID_dimension_timeseries_completed_calendar_path}")
    print(f"effective_date_str: {effective_date_str}")

    date_column = dimension_exchange_calendar['date']
    SHFE_column = dimension_exchange_calendar['SHFE']

    if os.path.exists(EID_dimension_timeseries_completed_calendar_path):
        dimension_timeseries_completed_calendar = pd.read_csv(EID_dimension_timeseries_completed_calendar_path, dtype = {'date': str})
    else:
        dimension_timeseries_completed_calendar = pd.DataFrame(columns = [
            'date', 'shibor', 'spot_price', 'futures_foreign', 'futures_inventory', 'baltic_shipping', 'energy_metal_indices',
        ])

    # 检查并补全缺失日期
    existing_dates = set(dimension_timeseries_completed_calendar['date'])
    all_dates = set(date_column)
    missing_dates = sorted(list(all_dates - existing_dates))

    if missing_dates:
        # 创建date到SHFE值的映射
        date_to_shfe = dict(zip(date_column, SHFE_column))

        new_rows = []
        for date in missing_dates:
            shfe_value = date_to_shfe[date]
            new_row = {
                'date': date,
                'shibor': shfe_value,
                'spot_price': shfe_value,
                'futures_foreign': shfe_value,
                'futures_inventory': shfe_value,
                'baltic_shipping': shfe_value,
                'energy_metal_indices': shfe_value,
            }
            new_rows.append(new_row)

        # 追加新行并按日期排序
        dimension_timeseries_completed_calendar = pd.concat([
            dimension_timeseries_completed_calendar, pd.DataFrame(new_rows)
        ], ignore_index = True)
        dimension_timeseries_completed_calendar = dimension_timeseries_completed_calendar.sort_values('date').reset_index(drop = True)

        print(f"补全了 {len(missing_dates)} 个缺失日期: {missing_dates}")


    # 数据的起始日期配置
    column_inception_dict = {
        'shibor': '20070101',
        'spot_price': '20110104',
    }
    for column, start_date in column_inception_dict.items():

        mask = dimension_timeseries_completed_calendar['date'] < start_date
        dimension_timeseries_completed_calendar.loc[mask, column] = 0


    print("正在更新时序数据已完成日历 dimension_timeseries_completed_calendar")
    print("=" * 50, end = "\n\n")

    if save_switch:
       dimension_timeseries_completed_calendar.to_csv(EID_dimension_timeseries_completed_calendar_path, index = False)

    return dimension_timeseries_completed_calendar


# In[6]:


dimension_timeseries_completed_calendar = None
if __name__ == "__main__":
    dimension_timeseries_completed_calendar = refresh_dimension_timeseries_completed_calendar(save_switch = True)
dimension_timeseries_completed_calendar


# In[ ]:




