#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

pn.extension()  # 启用 Jupyter 支持
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[3]:


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


# In[4]:


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
    print(exchange_variety_map)


# In[5]:


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


# In[6]:


def get_date_to_process(variety):

    NED_dimension_futures_trading_calendar_path = os.path.join(native_exchange_data_dir, r"dimension_futures_trading_calendar.csv")
    dimension_futures_trading_calendar = pd.read_csv(NED_dimension_futures_trading_calendar_path, dtype = {'date': str})

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 原始数据文件夹路径 NED_variety_initial_path
    NED_variety_initial_path = os.path.join(NED_variety_path , "initial_data")

    print("=" * 50)
    print("仓单时间序列数据 fact_warehouse_receipt_timeseries")
    print(f"当前品种 variety: {variety} ......", end = '\n')



    date_to_process = [] # 将所有需要收集的日期
    for date in dimension_futures_trading_calendar.loc[
        dimension_futures_trading_calendar[variety] == 1, 'date'
    ].to_list():
        NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date) # 日期文件夹路径 NED_variety_initial_date_path
        NED_fact_warehouse_receipt_initial_path = os.path.join(NED_variety_initial_date_path, 'fact_warehouse_receipt_initial.csv') # 仓单日报原始数据文件路径 NED_fact_warehouse_receipt_initial_path

        if os.path.exists(NED_fact_warehouse_receipt_initial_path): # 当当前循环日期的 fact_warehouse_receipt_initial 文件存在时
            date_to_process.append(date) # 将日期添加进 date_to_process

    date_to_process = sorted(date_to_process) # 完成添加日期后，排序以方便后续处理
    return date_to_process


# In[9]:


def refresh_fact_warehouse_receipt_timeseries(date_to_process, variety):

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 时间序列文件夹路径 NED_variety_timeseries_path
    NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")

    # 原始数据文件夹路径 NED_variety_initial_path
    NED_variety_initial_path = os.path.join(NED_variety_path , "initial_data")

    # 仓单日报时间序列文件路径 NED_fact_warehouse_receipt_timeseries_path
    NED_fact_warehouse_receipt_timeseries_path = os.path.join(NED_variety_timeseries_path, 'fact_warehouse_receipt_timeseries.csv')
    if os.path.exists(NED_fact_warehouse_receipt_timeseries_path):
        fact_warehouse_receipt_timeseries = pd.read_csv(NED_fact_warehouse_receipt_timeseries_path, dtype = {'date': str})
    else:
        fact_warehouse_receipt_timeseries = pd.DataFrame({'date': []})


    # 取差集作为 date_to_process
    date_to_process = sorted(list(
        set(date_to_process) - set(fact_warehouse_receipt_timeseries['date'].to_list())
    ))

    print("date_to_process:", date_to_process)
    if date_to_process:
        for date in date_to_process:

            NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date) # 日期文件夹路径 NED_variety_initial_date_path
            NED_fact_warehouse_receipt_initial_path = os.path.join(NED_variety_initial_date_path, 'fact_warehouse_receipt_initial.csv') # 仓单日报原始数据文件路径 NED_fact_warehouse_receipt_initial_path
            fact_warehouse_receipt_initial = pd.read_csv(NED_fact_warehouse_receipt_initial_path)
            time.sleep(0.05) # 冷却以防止硬盘过热与文件损坏

            new_row = fact_warehouse_receipt_initial.groupby('warehouse_name')['warehouse_receipt_number'].sum() # 按 area 分组后，将 vol 列求和，返回 pd.core.series.Series 对象
            new_row = new_row.to_frame().T # 将 series 对象 to_frame 方便转置成一行
            new_row.insert(0, 'date', date) # 在最前面插入 date 列

            fact_warehouse_receipt_timeseries = pd.concat([fact_warehouse_receipt_timeseries, new_row], ignore_index = True)
            fact_warehouse_receipt_timeseries.sort_values('date', ascending = True)

        fact_warehouse_receipt_timeseries.to_csv(NED_fact_warehouse_receipt_timeseries_path, index = False)

    print(f"品种 {variety} 完成刷新")
    print("=" * 50)


# In[10]:


if __name__ == '__main__':

    date_to_process = get_date_to_process(variety)
    refresh_fact_warehouse_receipt_timeseries(date_to_process, variety)


# In[ ]:




