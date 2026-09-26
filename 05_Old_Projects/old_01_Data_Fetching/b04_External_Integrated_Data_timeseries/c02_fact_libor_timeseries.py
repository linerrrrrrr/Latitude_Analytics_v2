#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[17]:


external_integrated_data_dir = os.path.join(base_dir, "External_Integrated_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 时间序列文件夹路径 EID_timeseries_data
EID_timeseries_data = os.path.join(external_integrated_data_dir, "timeseries_data")

# 时序数据已完成日历路径 EID_dimension_timeseries_completed_calendar_path
EID_dimension_timeseries_completed_calendar_path = os.path.join(EID_timeseries_data, 'dimension_timeseries_completed_calendar.csv')
dimension_timeseries_completed_calendar = pd.read_csv(EID_dimension_timeseries_completed_calendar_path, dtype = {'date': str})
dimension_timeseries_completed_calendar


# In[27]:


def refresh_fact_shibor_timeseries():

    # 时间序列文件夹路径 EID_timeseries_data
    EID_timeseries_data = os.path.join(external_integrated_data_dir, "timeseries_data")

    # 时序数据已完成日历路径 EID_dimension_timeseries_completed_calendar_path
    EID_dimension_timeseries_completed_calendar_path = os.path.join(EID_timeseries_data, 'dimension_timeseries_completed_calendar.csv')
    dimension_timeseries_completed_calendar = pd.read_csv(EID_dimension_timeseries_completed_calendar_path, dtype = {'date': str})

    # 上海银行间同业拆借利率路径 fact_shibor_timeseries_path
    fact_shibor_timeseries_path = os.path.join(EID_timeseries_data, "fact_shibor_timeseries.csv")

    if os.path.exists(fact_shibor_timeseries_path):
        fact_shibor_timeseries = pd.read_csv(fact_shibor_timeseries_path, dtype = {'date': str})
    else:
        fact_shibor_timeseries = pd.DataFrame(columns = ['date', 'on', '1w', '2w', '1m', '3m', '6m', '9m', '1y'])


    date_to_process = dimension_timeseries_completed_calendar.loc[
        dimension_timeseries_completed_calendar['shibor'] == 1
    ]['date'].to_list()
    date_to_process = set(date_to_process) - set(fact_shibor_timeseries['date'])
    print("待更新 date_to_process 长度:", len(date_to_process))

    if date_to_process:

        # 将 date_to_process 转为 datetime 对象并排序
        date_objecties = [datetime.datetime.strptime(date, "%Y%m%d") for date in date_to_process]
        sorted_pairs = sorted(zip(date_objecties, date_to_process))
        sorted_dates = [date_str for _, date_str in sorted_pairs]

        max_span_days = 120  # 最大允许的自然日间隔跨度
        group_size = 120  # 每组最多包含的日期数量

        # 动态分组 保证每组内 end_date - start_date <= max_span_days 且 len(group) <= group_size
        group_list = []  # 遍历组 每次完成一个 current_group 就 append 进入 groups
        current_group = []  # 遍历日期 检验日期是否符合放入 组成 current_group

        for date_str in sorted_dates:
            if not current_group:
                current_group.append(date_str)
            else:
                start_date_str = current_group[0]
                end_date_str = date_str

                start_dt = datetime.datetime.strptime(start_date_str, "%Y%m%d")
                end_dt = datetime.datetime.strptime(end_date_str, "%Y%m%d")
                span_days = (end_dt - start_dt).days

                # 同时检查：跨度不超过 max_span_days 且 当前组未满 group_size
                if span_days <= max_span_days and len(current_group) < group_size:
                    current_group.append(date_str)
                else:
                    # 超出跨度 或 达到最大组大小，结束当前组，开启新组
                    group_list.append(current_group)
                    current_group = [date_str]

        if current_group:  # sorted_dates 遍历时 每次开新组才会把旧组放入 groups
            group_list.append(current_group)  # 故最后一个组在完成逐个日期遍历时不一定会被放入 groups

        for group in group_list:
            start_date = group[0]
            end_date = group[-1]
            print(f"分组遍历 group: [{start_date} ...., {end_date}], length: {len(group_list)} ")

            for _ in range(3):

                new_rows = pro.shibor(
                    start_date = start_date,
                    end_date = end_date,
                )
                time.sleep(0.15) # 数据请求冷却
                break


            fact_shibor_timeseries = pd.concat([fact_shibor_timeseries, new_rows], ignore_index = True)

        fact_shibor_timeseries.sort_values(by = ['date'], inplace = True)
        fact_shibor_timeseries = fact_shibor_timeseries.drop_duplicates(subset = ['date'], keep = 'first')

        # 将更新后的 fact_shibor_timeseries 写入本地 csv
        fact_shibor_timeseries.to_csv(fact_shibor_timeseries_path, index = False)


    return fact_shibor_timeseries


# In[29]:


fact_shibor_timeseries = None
if __name__ == "__main__":
    fact_shibor_timeseries = refresh_fact_shibor_timeseries()
fact_shibor_timeseries


# In[ ]:




