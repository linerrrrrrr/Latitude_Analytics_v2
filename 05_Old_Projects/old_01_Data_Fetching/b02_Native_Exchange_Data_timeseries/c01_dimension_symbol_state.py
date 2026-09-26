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


# In[3]:


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


# In[4]:


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


# In[5]:


def refresh_dimension_symbol_state( exchange, variety ):

    print( "=" * 50)
    print("正在刷新品种合约状态表 dimension_symbol_state")
    print("品种 variety: ", variety)

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 合约日历路径 NED_dimension_symbol_calendar_path
    NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')
    dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str, })

    # 时间序列文件夹路径 NED_variety_timeseries_path
    NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")
    os.makedirs(NED_variety_timeseries_path, exist_ok = True) # 允许文件夹已存在

    # 合约状态表路径 NED_dimension_symbol_state_path
    NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')
    columns_list = [
        'symbol', 'state',
        'future_daily', 'future_minutely',
        'rank_table_20_long', 'rank_table_20_short', 'rank_table_20_vol'
    ] # 表头 dimension_symbol_state，
    # 用于 dimension_symbol_state 初始化 与 symbol_to_process 时的 new_rows 初始化




    # 获取或初始化 dimension_symbol_state
    if os.path.exists(NED_dimension_symbol_state_path):
        print("检测到文件存在:", NED_dimension_symbol_state_path)
        dimension_symbol_state = pd.read_csv(NED_dimension_symbol_state_path, dtype = {'date': str, })
    else:
        print("检测到文件不存在, 正在初始化 dimension_symbol_state")
        dimension_symbol_state = pd.DataFrame(columns = columns_list)

    # 无论 dimension_symbol_state 是否是新创建的，都使用相同的 合约列 更新流程
    existing_symbol = set(dimension_symbol_state['symbol']) # 合约状态表 dimension_symbol_state 中已存在的合约集合
    full_symbol = {symbol for symbol in dimension_symbol_calendar.columns.to_list() if not symbol == 'date'} # 合约日历 dimension_symbol_calendar 的合约总集

    symbol_to_process = full_symbol - existing_symbol # 合约状态表 dimension_symbol_state 缺失的合约
    if symbol_to_process: # 判断 symbol_to_process 是否为空，如果为空说明 合约状态表 dimension_symbol_state 中已存在的合约已经对齐了 合约日历 dimension_symbol_calendar
        new_rows = pd.DataFrame(columns = columns_list) # 用于 concat 的新行初始化时用 columns_list
        new_rows['symbol'] = sorted(list(symbol_to_process)) # 将 symbol_to_process 填充新行的 symbol 列

        new_rows.fillna(0, inplace = True) # 填充完 symbol 后其他列为空 pd.Nan，用 fillna 处理
        dimension_symbol_state = pd.concat([dimension_symbol_state, new_rows], ignore_index = True)




    # 合约闭合状态 state 列更新
    symbol_listing_info = pro.fut_basic(exchange = exchange, fut_type = "1", fields = ["fut_code", "symbol", "delist_date"]) # 从 tushare 获取最新的 symbol_listing_info，获取时要求 fut_code 列，用于筛选出对应品种的合约信息
    symbol_listing_info = symbol_listing_info[symbol_listing_info['fut_code'] == variety][["symbol", "delist_date"]] # 筛选完品种后，只保留合约 symbol 与下市日期 delist_date
    symbol_listing_info = symbol_listing_info.set_index('symbol')

    tmp_now = datetime.datetime.now()
    if tmp_now.hour >= 20:
        current_date = tmp_now.strftime("%Y%m%d")
    else:
        current_date = (tmp_now - datetime.timedelta(days = 1)).strftime("%Y%m%d")

    delist_dates = symbol_listing_info.loc[dimension_symbol_state['symbol'], 'delist_date'].values # 从 symbol_listing_info 提取对应 symbol 的 delist_date
    dimension_symbol_state['state'] = (current_date >= delist_dates).astype(int) # 向量化比较
    dimension_symbol_state.to_csv(NED_dimension_symbol_state_path, index = False)

    print(f"品种 {variety} 刷新完成")
    print( "=" * 50, end = '\n\n')
    return dimension_symbol_state


# In[6]:


dimension_symbol_state = None
if __name__ == '__main__':
    dimension_symbol_state = refresh_dimension_symbol_state( exchange, variety )
dimension_symbol_state


# In[8]:


# exchange_variety_map = get_exchange_variety_map()
# for exchange, varieties in exchange_variety_map.items():
#     for variety in varieties:
#
#         if exchange in ["CZCE", ]:
#             continue # 特殊交易所不做处理
#         else:
#
#             # 品种文件夹路径 NED_variety_path
#             NED_variety_path = os.path.join(native_exchange_data_dir, variety)
#
#             # 合约日历路径 NED_dimension_symbol_calendar_path
#             NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')
#             dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str, })
#
#             # 时间序列文件夹路径 NED_variety_timeseries_path
#             NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")
#             os.makedirs(NED_variety_timeseries_path, exist_ok = True) # 允许文件夹已存在
#
#             # 合约状态表路径 NED_dimension_symbol_state_path
#             NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')
#             if os.path.exists(NED_dimension_symbol_state_path):
#                 os.remove(NED_dimension_symbol_state_path)
#
#             refresh_dimension_symbol_state(exchange,  variety)


# In[ ]:




