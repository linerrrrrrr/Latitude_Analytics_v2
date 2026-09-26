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
# # 时间序列文件夹路径 NED_variety_timeseries_path
# NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")
#
# # 合约状态表路径 NED_dimension_symbol_state_path
# NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')
#
# # 日线行情文件夹路径
# NED_futures_daily_timeseries_path = os.path.join(NED_variety_timeseries_path, " futures_daily_timeseries")
#
# # 合约日线行情路径
# NED_fact_futures_daily_timeseries_path = os.path.join(NED_futures_daily_timeseries_path, f'{symbol}.csv')


# In[6]:


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


# In[3]:


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


# In[4]:


def refresh_fact_futures_daily_timeseries(exchange, variety, deep_validation_switch = False):

    exchange_suffix = {
        'CFFEX': 'CFX',
        'CZCE': 'ZCE',
        'DCE': 'DCE',
        'GFEX': 'GFE',
        'INE': 'INE',
        'SHFE': 'SHF',
    }[exchange]

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(str(native_exchange_data_dir), str(variety))

    # 时间序列文件夹路径 NED_variety_timeseries_path
    NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")

    # 日线行情文件夹
    NED_futures_daily_timeseries_path = os.path.join(NED_variety_timeseries_path, "futures_daily_timeseries")
    os.makedirs(NED_futures_daily_timeseries_path, exist_ok = True) # 创建必要目录，若不存在

    # 合约日历路径 NED_dimension_symbol_calendar_path
    NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')
    dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str})

    # 合约状态表路径 NED_dimension_symbol_state_path
    NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')
    dimension_symbol_state = pd.read_csv(NED_dimension_symbol_state_path, dtype = {'date': str})




    print("=" * 50)
    print("日线行情更新 fact_futures_daily_timeseries")
    print(f"当前品种 variety: {variety} ......")
    print(f"exchange: {exchange}, suffix: \".{exchange_suffix}\"")
    print(f"deep_validation_switch: {deep_validation_switch}", end = '\n\n')

    for index, row in dimension_symbol_state.iterrows():

        symbol = row['symbol']
        symbol_state = row['state']
        future_daily_state = row['future_daily']

        if not future_daily_state or deep_validation_switch:
            print(f"当前循环合约: {symbol}")

            # 合约日线行情路径
            NED_fact_futures_daily_timeseries_path = os.path.join(NED_futures_daily_timeseries_path, f'{symbol}.csv')
            if os.path.exists(NED_fact_futures_daily_timeseries_path):
                fact_futures_daily_timeseries = pd.read_csv(NED_fact_futures_daily_timeseries_path, dtype = {'date': str})
                time.sleep(0.3) # 冷却以防止硬盘过热
            else:
                fact_futures_daily_timeseries = pd.DataFrame({'date': []})

            # 待检测日期 date_to_process
            date_to_process = dimension_symbol_calendar.loc[
                dimension_symbol_calendar[symbol], 'date'
            ]

            if len(fact_futures_daily_timeseries) == len(date_to_process) and (fact_futures_daily_timeseries['date'].values == date_to_process.values).all():
                print(symbol, f"检测到 {symbol} 已完成")
            else:

                # fact_futures_daily_timeseries 数据请求
                ts_code = f"{symbol}.{exchange_suffix}" # 拼接 合约代码 与 交易所尾缀 以对齐 tushare 格式
                fact_futures_daily_timeseries = pro.fut_daily(ts_code = ts_code).rename(
                    columns = {'trade_date': 'date', 'ts_code': 'symbol', } # 行重命名
                )
                fact_futures_daily_timeseries['symbol'] = (
                    fact_futures_daily_timeseries['symbol'] # 将合约代码统一为不带尾缀的格式
                    .str.split('.')
                    .str[0]
                )

                fact_futures_daily_timeseries.to_csv(NED_fact_futures_daily_timeseries_path, index = False)
                time.sleep(0.5) # 冷却以防止接口请求频率过高，防止磁盘过热


        # 完成数据更新后，在 dimension_symbol_calendar 标记是否已经完成
        dimension_symbol_state.at[index, 'future_daily'] = 1 if symbol_state == 1 else 0

    print("=" * 50)
    # 更新合约状态表到路径 dimension_symbol_state
    dimension_symbol_state.to_csv(NED_dimension_symbol_state_path, index = False)


# In[5]:


if __name__ == "__main__":
    refresh_fact_futures_daily_timeseries(exchange, variety, deep_validation_switch = False)


# In[1]:


# fact_futures_daily_timeseries = pro.fut_daily(ts_code = 'CU1811.SHF').rename(
#     columns = {'trade_date': 'date', 'ts_code': 'symbol', }
# )
# fact_futures_daily_timeseries['symbol'] = (
#     fact_futures_daily_timeseries['symbol']
#     .str.split('.')
#     .str[0]
# )


# In[ ]:




