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


# # 品种文件夹路径 NED_variety_path
# NED_variety_path = os.path.join(native_exchange_data_dir, variety)
#
# # 合约日历路径 NED_dimension_symbol_calendar_path
# NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')
#
# # 原始数据文件夹路径 NED_variety_initial_path
# NED_variety_initial_path = os.path.join(NED_variety_path , "initial_data")
#
# # 品种日期文件夹路径 NED_variety_initial_date_path
# NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date)
#
# # 分钟行情文件夹路径 NED_futures_minutely_initial_path
# NED_futures_minutely_initial_path = os.path.join(NED_variety_initial_date_path, "futures_minutely_initial")
#
# # 分钟行情文件路径 NED_fact_futures_minutely_initial_path
# NED_fact_futures_minutely_initial_path = os.path.join(NED_futures_minutely_initial_path, f"{symbol}.csv")


# In[7]:


native_exchange_data_dir = os.path.join(base_dir, "Native_Exchange_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

def get_exchange_variety_map():
    # 交易所日历路径 NED_dimension_exchange_calendar_path
    NED_dimension_exchange_calendar_path = os.path.join(native_exchange_data_dir, 'dimension_exchange_calendar.csv')
    dimension_exchange_calendar = pd.read_csv(NED_dimension_exchange_calendar_path, dtype = {'date': str})

    # 获取 交易所-品种 表单
    exchange_list = [col for col in dimension_exchange_calendar.columns if col != 'date'] # 交易所列表
    map_list = []
    for exchange in exchange_list:

        exchange_variety_map = pd.DataFrame()
        for attempt in range(3):
            try:
                exchange_variety_map = pro.fut_basic(exchange = exchange, fut_type = "1", fields = ["fut_code"])
                break
            except Exception as e:
                print(f"第 {attempt + 1} 次获取 {exchange} 品种信息失败: {e}")

        exchange_variety_map = exchange_variety_map.drop_duplicates()
        exchange_variety_map["exchange"] = exchange
        map_list.append(exchange_variety_map)

    exchange_variety_map = pd.concat(map_list, ignore_index = True)
    exchange_variety_map = exchange_variety_map.groupby('exchange')['fut_code'].apply(list)
    exchange_variety_map = exchange_variety_map.to_dict()
    return exchange_variety_map

if __name__ == '__main__':
    exchange_variety_map = get_exchange_variety_map()
    exchange_variety_map.pop("CFFEX")
    print(exchange_variety_map)


# In[8]:


exchange = None
variety = None

if __name__ == '__main__':
    # 交易所选择组件
    exchange_radio = widgets.RadioButtons(
        options = list(exchange_variety_map.keys()),
        description = '交易所选择 exchange:',
        layout = {'width': '400px'}
    )

    # 品种选择组件（初始禁用）
    species_toggle = widgets.ToggleButtons(
        options = [],
        description = '品种选择 variety:',
        disabled = True,
        button_style = '',
        style = {'button_width': 'auto'},
        layout = widgets.Layout(width = '600px'),
    )

    # 输出提示区域
    out = widgets.Textarea(
        value = '请先选择交易所，然后选择品种',
        layout = {'width': '400px', 'height': '50px'},
        disabled = True
    )

    display(widgets.VBox([exchange_radio, species_toggle, out]))

    def on_exchange_change(change):
        """当交易所改变时，更新可选品种"""
        global exchange
        if change['name'] == 'value':
            exchange = change['new']
            varieties = exchange_variety_map.get(exchange, [])
            species_toggle.options = sorted(varieties)
            species_toggle.disabled = len(varieties) == 0
            if varieties:
                species_toggle.value = varieties[0]  # 默认选第一个
                out.value = f'已选择交易所：{exchange}，请选择品种'
            else:
                out.value = f'交易所 {exchange} 无可用品种'

    def on_species_change(change):
        """当品种改变时，记录到全局变量"""
        global variety
        if change['name'] == 'value' and species_toggle.options:
            variety = change['new']
            out.value = f'已选择交易所 exchange: {exchange}\n已选择品种 variety: {variety}'

    # 绑定事件
    exchange_radio.observe(on_exchange_change, names = 'value')
    species_toggle.observe(on_species_change, names = 'value')


# In[11]:


def refresh_fact_member_broker_LSV_timeseries(variety):

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 时间序列文件夹路径 NED_variety_timeseries_path
    NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")

    # 前 20 合约日历 NED_dimension_rank_table_20_calendar_path
    NED_dimension_rank_table_20_calendar_path = os.path.join(NED_variety_path, 'dimension_rank_table_20_calendar.csv')
    dimension_rank_table_20_calendar = pd.read_csv(NED_dimension_rank_table_20_calendar_path, dtype = {'date': str})

    data_values = dimension_rank_table_20_calendar.iloc[:, 1:] # 跳过第一列 date 提取除之外的所有数值列
    date_to_process = dimension_rank_table_20_calendar.loc[
        (data_values == 2).any(axis = 1) # 检查每一行是否有至少一个 2
    ]['date'].tolist()


    # 经纪参与者交易与持仓文件路径 NED_fact_member_broker_LSV_timeseries_path
    NED_fact_member_broker_LSV_timeseries_path = os.path.join(NED_variety_timeseries_path, 'fact_member_broker_LSV_timeseries.csv')
    if os.path.exists(NED_fact_member_broker_LSV_timeseries_path):
        fact_member_broker_LSV_timeseries = pd.read_csv(NED_fact_member_broker_LSV_timeseries_path, dtype = {'date': str})
    else:
        fact_member_broker_LSV_timeseries = pd.DataFrame({'date': []})




    print("=" * 50)
    print("经纪参与者交易与持仓 fact_member_broker_LSV_timeseries")
    print(f"当前品种 variety: {variety} ......", end = '\n')

    # 取差集作为 date_to_process
    date_to_process = sorted(list(set(date_to_process) - set(fact_member_broker_LSV_timeseries['date'])))
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

                new_rows = pro.fut_holding(
                    start_date = start_date,
                    end_date = end_date,
                    symbol = variety,
                )
                time.sleep(0.05) # 数据请求冷却
                break

            new_rows = new_rows[new_rows['broker'] == '期货公司'][
                ['trade_date', 'vol', 'vol_chg', 'long_hld', 'long_chg', 'short_hld', 'short_chg']
            ].rename(columns = {'trade_date': 'date'})

            if fact_member_broker_LSV_timeseries.empty and not new_rows.empty:
                fact_member_broker_LSV_timeseries = new_rows
            elif not fact_member_broker_LSV_timeseries.empty and not new_rows.empty:
                fact_member_broker_LSV_timeseries = pd.concat([fact_member_broker_LSV_timeseries, new_rows], ignore_index = True)

        fact_member_broker_LSV_timeseries.sort_values(by = ['date'], inplace = True)
        fact_member_broker_LSV_timeseries = fact_member_broker_LSV_timeseries.drop_duplicates(subset = ['date'], keep = 'first')


    print(f"\n已完成刷新，品种 {variety} 期货经纪商持仓与交易表 fact_member_broker_LSV_timeseries")
    print( "=" * 50, end = '\n\n')

    fact_member_broker_LSV_timeseries.to_csv(NED_fact_member_broker_LSV_timeseries_path, index = False)
    return fact_member_broker_LSV_timeseries


# In[12]:


fact_member_broker_LSV_timeseries = None
if __name__ == "__main__":
    fact_member_broker_LSV_timeseries = fact_member_broker_LSV_timeseries = refresh_fact_member_broker_LSV_timeseries(variety)
fact_member_broker_LSV_timeseries


# In[ ]:




