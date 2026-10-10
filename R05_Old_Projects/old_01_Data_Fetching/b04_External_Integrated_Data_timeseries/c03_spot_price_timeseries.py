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


# In[19]:


def refresh_spot_price_timeseries():

    # 原始数据文件夹路径 EID_initial_path
    EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")

    # 时间序列文件夹路径 EID_timeseries_path
    EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

    # 现货数据文件夹路径 EID_spot_price_timeseries_path
    EID_spot_price_timeseries_path = os.path.join(EID_timeseries_path, "spot_price_timeseries")
    os.makedirs(EID_spot_price_timeseries_path, exist_ok = True)


    date_to_process = dimension_timeseries_completed_calendar.loc[
        dimension_timeseries_completed_calendar['spot_price'] == 1
    ]['date'].to_list()
    date_to_process = sorted([str(date) for date in date_to_process])

    # 扫描源数据目录，获取所有实际存在的日期文件
    available_dates_dict = {}
    for date in date_to_process:
        date_path = os.path.join(EID_initial_path, date)
        spot_file = os.path.join(date_path, 'fact_spot_price_initial.csv')
        if os.path.exists(spot_file): available_dates_dict[date] = spot_file

    # 使用最新日期的数据确定品种集合
    latest_date = max(available_dates_dict.keys())
    latest_table = pd.read_csv(available_dates_dict[latest_date])
    all_varieties = latest_table['product'].unique().tolist()

    print(f"品种全集 (基于 {latest_date}): {all_varieties}")
    print(f"待扫描源日期数: {len(available_dates_dict)}")




    def get_existing_dates(variety_name):
        """ 检测某品种时间序列文件已存在的日期 """
        file_path = os.path.join(EID_spot_price_timeseries_path, f'{variety_name}.csv')
        if not os.path.exists(file_path):
            return set()

        try:
            df_existing = pd.read_csv(file_path, usecols = ['date'])
            return set(df_existing['date'].astype(str).tolist())
        except (pd.errors.EmptyDataError, KeyError, FileNotFoundError):
            return set()

    variety_update_plan = {}
    for variety in all_varieties:
        existing_dates = get_existing_dates(variety)
        dates_to_add = [date for date in available_dates_dict.keys() if date not in existing_dates]
        if dates_to_add:
            variety_update_plan[variety] = {
                'existing_dates': existing_dates,
                'dates_to_add': sorted(dates_to_add),
                'file_path': os.path.join(EID_spot_price_timeseries_path, f'{variety}.csv')
            }

    print(f"需更新的品种数: {len(variety_update_plan)}")
    for variety, plan in variety_update_plan.items():
        print(f"  {variety}: 新增 {len(plan['dates_to_add'])} 个日期")





    def extract_variety_data_from_daily_file(file_path, variety_name, date_str):
        """ 从单日文件中提取特定品种完整数据行，如不存在返回 None """
        try:
            table_daily = pd.read_csv(file_path)
            table_match = table_daily[table_daily['product'] == variety_name]

            if len(table_match) == 0: return None
            if len(table_match) > 1: print(f"警告: {date_str} {variety_name} 存在重复行，取首行")

            # 提取完整行数据（所有列）并加入日期字段
            dict_record = table_match.iloc[0].to_dict()
            dict_record['date'] = date_str
            return dict_record

        except Exception as e:
            print(f"读取失败 {file_path}: {e}")
            return None

    # 基于最新数据建立标准列结构
    spot_price_standard_columns = latest_table.columns.tolist()
    dict_empty_record_template = {column: np.nan for column in spot_price_standard_columns} # 构造空记录模板

    update_summary = [] # 记录更新状况
    for variety, plan in variety_update_plan.items():
        print(f"\n处理品种: {variety}")
        list_new_records = []

        dict_variety_empty = dict_empty_record_template.copy() # 为当前品种定制空记录模板
        dict_variety_empty['product'] = variety

        for date in plan['dates_to_add']:
            file_path = available_dates_dict[date]
            dict_record = extract_variety_data_from_daily_file(file_path, variety, date)

            if dict_record is None:
                # 该日期无此品种数据，使用空模板并填充日期
                dict_record = dict_variety_empty.copy()
                dict_record['date'] = date

            list_new_records.append(dict_record)

        if not list_new_records:
            continue

        df_new = pd.DataFrame(list_new_records)

        # 规范列顺序 date 列置于最前，其余保持原始顺序
        list_cols_ordered = ['date'] + [column for column in df_new.columns if column != 'date']
        df_new = df_new[list_cols_ordered]

        if os.path.exists(plan['file_path']):
            df_existing = pd.read_csv(plan['file_path'])

            # 对齐列结构：若历史文件缺少新列，填充NaN
            for column in df_new.columns:
                if column not in df_existing.columns:
                    df_existing[column] = np.nan

            # 若历史文件有多余列，也在新数据中补充（保持兼容性）
            for column in df_existing.columns:
                if column not in df_new.columns:
                    df_new[column] = np.nan

            df_combined = pd.concat([df_existing, df_new], ignore_index = True)
            df_combined.drop_duplicates(subset = ['date'], keep = 'last', inplace = True) # 按日期去重
            df_combined.sort_values('date', inplace = True)
        else:
            df_combined = df_new.sort_values('date')

        # 保存 保持列顺序一致性：日期 + 标准列 + 可能的历史遗留列
        list_final_cols = ['date'] + [
            column for column in spot_price_standard_columns if column != 'date'
        ]
        list_extra_cols = [column for column in df_combined.columns if column not in list_final_cols] # 补充可能存在的其他列
        df_combined = df_combined[list_final_cols + list_extra_cols]

        df_combined.to_csv(plan['file_path'], index = False)

        update_summary.append({
            'variety': variety,
            'total_dates': len(df_combined),
            'new_dates': len(list_new_records),
            'valid_spot_prices': df_combined['spot_price'].notna().sum(),
            'missing_spot_prices': df_combined['spot_price'].isna().sum(),
            'date_range': f"{df_combined['date'].min()} ~ {df_combined['date'].max()}"
        })


    summary_df = pd.DataFrame(update_summary)
    return summary_df


# In[20]:


summary_df = None
if __name__ == '__main__':
    summary_df = refresh_spot_price_timeseries()
summary_df


# In[ ]:




