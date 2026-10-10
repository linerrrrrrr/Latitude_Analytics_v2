#!/usr/bin/env python
# coding: utf-8

# In[2]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[3]:


external_integrated_data_dir = os.path.join(base_dir, "External_Integrated_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 时间序列文件夹路径 EID_timeseries_path
EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

# 时序数据已完成日历路径 EID_dimension_timeseries_completed_calendar_path
EID_dimension_timeseries_completed_calendar_path = os.path.join(EID_timeseries_path, 'dimension_timeseries_completed_calendar.csv')
dimension_timeseries_completed_calendar = pd.read_csv(EID_dimension_timeseries_completed_calendar_path, dtype = {'date': str})
dimension_timeseries_completed_calendar


# In[4]:


def refresh_futures_foreign_timeseries():

    # 原始数据文件夹路径 EID_initial_path
    EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")

    # 时间序列文件夹路径 EID_timeseries_path
    EID_timeseries_path = os.path.join(external_integrated_data_dir, "timeseries_data")

    # 外盘行情数据文件夹路径 EID_futures_foreign_timeseries_path
    EID_futures_foreign_timeseries_path = os.path.join(EID_timeseries_path, "futures_foreign_timeseries")
    os.makedirs(EID_futures_foreign_timeseries_path, exist_ok = True)

    date_to_process = dimension_timeseries_completed_calendar.loc[
        dimension_timeseries_completed_calendar['futures_foreign'] == 1
    ]['date'].to_list()
    date_to_process = sorted([str(date) for date in date_to_process])

    # 扫描源数据目录，获取所有实际存在的日期文件
    available_dates_dict = {}
    for date in date_to_process:
        date_path = os.path.join(EID_initial_path, date)
        foreign_file = os.path.join(date_path, 'fact_futures_foreign_initial.csv')
        if os.path.exists(foreign_file):
            available_dates_dict[date] = foreign_file

    # 使用最新日期的数据确定品种集合
    latest_date = max(available_dates_dict.keys())
    latest_table = pd.read_csv(available_dates_dict[latest_date])
    all_varieties = latest_table['code'].unique().tolist()

    print(f"外盘品种全集 (基于 {latest_date}): {all_varieties}")
    print(f"待扫描源日期数: {len(available_dates_dict)}")


    def get_existing_dates_foreign(variety_code):
        """ 检测外盘品种时间序列文件已存在的日期 """
        file_path = os.path.join(EID_futures_foreign_timeseries_path, f'{variety_code}.csv')
        if not os.path.exists(file_path):
            return set()

        try:
            df_existing = pd.read_csv(file_path, usecols = ['date'])
            return set(df_existing['date'].astype(str).tolist())
        except (pd.errors.EmptyDataError, KeyError, FileNotFoundError):
            return set()


    variety_update_plan = {}
    for variety in all_varieties:
        existing_dates = get_existing_dates_foreign(variety)
        dates_to_add = [date for date in available_dates_dict.keys() if date not in existing_dates]
        if dates_to_add:
            variety_update_plan[variety] = {
                'existing_dates': existing_dates,
                'dates_to_add': sorted(dates_to_add),
                'file_path': os.path.join(EID_futures_foreign_timeseries_path, f'{variety}.csv')
            }

    print(f"需更新的外盘品种数: {len(variety_update_plan)}")
    for variety, plan in variety_update_plan.items():
        print(f"  {variety}: 新增 {len(plan['dates_to_add'])} 个日期")


    def extract_foreign_data_from_daily_file(file_path, variety_code, date_str):
        """从单日外盘文件中提取特定品种完整数据行，如不存在返回 None"""
        try:
            df_daily = pd.read_csv(file_path)
            df_match = df_daily[df_daily['code'] == variety_code]

            if len(df_match) == 0:
                return None
            if len(df_match) > 1:
                print(f"警告: {date_str} {variety_code} 存在重复行，取首行")

            # 提取完整行数据并加入统一日期字段（原数据 day 列保留，新增 date 列用于标识）
            dict_record = df_match.iloc[0].to_dict()
            dict_record['date'] = date_str  # 使用统一的日期标识（来自文件夹名）
            return dict_record

        except Exception as e:
            print(f"读取失败 {file_path}: {e}")
            return None


    # 基于最新数据建立标准列结构
    foreign_standard_columns = latest_table.columns.tolist()
    dict_empty_record_template = {column: np.nan for column in foreign_standard_columns}

    update_summary = []
    for variety, plan in variety_update_plan.items():
        print(f"\n处理外盘品种: {variety}")
        list_new_records = []

        # 为当前品种定制空记录模板
        dict_variety_empty = dict_empty_record_template.copy()
        dict_variety_empty['code'] = variety
        # 尝试从最新数据中获取该品种的 name 作为默认值
        variety_name_row = latest_table[latest_table['code'] == variety]
        if not variety_name_row.empty:
            dict_variety_empty['name'] = variety_name_row.iloc[0]['name']

        # 逐日读取新增数据
        for date in plan['dates_to_add']:
            file_path = available_dates_dict[date]
            dict_record = extract_foreign_data_from_daily_file(file_path, variety, date)

            if dict_record is None:
                # 该日期无此品种数据：使用空模板并填充日期
                dict_record = dict_variety_empty.copy()
                dict_record['date'] = date

            list_new_records.append(dict_record)

        if not list_new_records:
            continue

        df_new = pd.DataFrame(list_new_records)

        # 规范列顺序 date 列置于最前，其余保持原始顺序，day 列也保留在原位置
        list_cols_ordered = ['date'] + [column for column in df_new.columns if column != 'date']
        df_new = df_new[list_cols_ordered]

        # 合并已存在数据
        if os.path.exists(plan['file_path']):
            df_existing = pd.read_csv(plan['file_path'])

            # 对齐列结构：双向补 NaN 确保兼容性
            for column in df_new.columns:
                if column not in df_existing.columns:
                    df_existing[column] = np.nan
            for column in df_existing.columns:
                if column not in df_new.columns:
                    df_new[column] = np.nan

            df_combined = pd.concat([df_existing, df_new], ignore_index = True)
            df_combined.drop_duplicates(subset = ['date'], keep = 'last', inplace = True)
            df_combined.sort_values('date', inplace = True)
        else:
            df_combined = df_new.sort_values('date')

        # 保存：date + 标准列 + 历史遗留列
        list_final_cols = ['date'] + [column for column in foreign_standard_columns if column != 'date']
        list_extra_cols = [column for column in df_combined.columns if column not in list_final_cols]
        df_combined = df_combined[list_final_cols + list_extra_cols]

        df_combined.to_csv(plan['file_path'], index = False)

        update_summary.append({
            'variety': variety,
            'total_dates': len(df_combined),
            'new_dates': len(list_new_records),
            'valid_close_prices': df_combined['close'].notna().sum(),
            'missing_close_prices': df_combined['close'].isna().sum(),
            'date_range': f"{df_combined['date'].min()} ~ {df_combined['date'].max()}"
        })

    summary_df = pd.DataFrame(update_summary)
    return summary_df


# In[5]:


summary_df = None
if __name__ == '__main__':
    refresh_futures_foreign_timeseries()
summary_df


# In[ ]:




