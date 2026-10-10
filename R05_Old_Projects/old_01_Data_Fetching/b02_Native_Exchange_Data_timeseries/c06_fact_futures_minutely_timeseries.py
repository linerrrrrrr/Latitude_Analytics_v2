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
    exchange_variety_map.pop("CZCE")
    exchange_variety_map.pop("DCE")
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


# In[32]:


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



# # 时间序列文件夹路径 NED_variety_timeseries_path
# NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")
#
# # 合约状态表路径 NED_dimension_symbol_state_path
# NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')
#
# # 分钟行情时间序列文件夹路径 NED_futures_minutely_timeseries_path
# NED_futures_minutely_timeseries_path = os.path.join(NED_variety_timeseries_path, "futures_minutely")
#
# # 分钟行情时间序列文件路径
# NED_fact_futures_minutely_timeseries_path = os.path.join(NED_futures_minutely_timeseries_path, f"{symbol}.csv")


# In[6]:


def refresh_fact_futures_minutely_timeseries(
    variety, start_date = '20251010', deep_validation_switch = False
):

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(str(native_exchange_data_dir), str(variety))

    # 合约日历路径 NED_dimension_symbol_calendar_path
    NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')
    dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str, })

    # 时间序列文件夹路径 NED_variety_timeseries_path
    NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")

    # 合约状态表路径 NED_dimension_symbol_state_path
    NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')
    dimension_symbol_state = pd.read_csv(NED_dimension_symbol_state_path, dtype = {'date': str, })

    # 分钟行情时间序列文件夹路径 NED_futures_minutely_timeseries_path
    NED_futures_minutely_timeseries_path = os.path.join(NED_variety_timeseries_path, "futures_minutely_timeseries")
    os.makedirs(NED_futures_minutely_timeseries_path, exist_ok = True) # 创建必要目录，若不存在




    print( "=" * 50)
    print("分钟行情更新 fact_futures_minutely_timeseries")
    print(f"当前品种 variety: {variety} ......")
    print(f"deep_validation_switch: {deep_validation_switch}", end = '\n\n')

    for index, row in dimension_symbol_state.iterrows():

        symbol = row['symbol']
        symbol_state = row['state']
        future_minutely_state = row['future_minutely']

        date_to_process = dimension_symbol_calendar.loc[
            dimension_symbol_calendar[symbol] == 1, 'date'
        ].to_list()

        print(f'当前合约 {symbol}, 是否闭合 symbol_state: {symbol_state}')
        print(f'当前是否以完成更新 future_minutely_state: {future_minutely_state}')

        if not future_minutely_state or deep_validation_switch:

            # 分钟行情时间序列文件路径
            NED_fact_futures_minutely_timeseries_path = os.path.join(NED_futures_minutely_timeseries_path, f"{symbol}.csv")
            if os.path.exists(NED_fact_futures_minutely_timeseries_path):
                fact_futures_minutely_timeseries = pd.read_csv(NED_fact_futures_minutely_timeseries_path)
            else:
                if not date_to_process[0] <= start_date:
                    fact_futures_minutely_timeseries = pd.DataFrame({'date': []})

                else:
                    print(f'{symbol} 数据缺失')
                    continue


            existing_date = fact_futures_minutely_timeseries['date']
            existing_date = pd.to_datetime(existing_date) # existing_date 转换为 datetime 类型

            existing_date = existing_date[existing_date.dt.time <= pd.Timestamp('21:00:00').time()] # 筛选 21:00:00 之前的数据
            existing_date = existing_date.dt.strftime('%Y%m%d') # 转换回 str 形式


            date_to_process = sorted(list(set(date_to_process) - set(existing_date)))
            print(f"本次更新 date_to_process: {date_to_process}")
            if date_to_process:

                new_row = pd.DataFrame()
                for date in date_to_process:
                    print(f'{symbol} {date}')

                    # 原始数据文件夹路径 NED_variety_initial_path
                    NED_variety_initial_path = os.path.join(NED_variety_path , "initial_data")

                    # 品种日期文件夹路径 NED_variety_initial_date_path
                    NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date)

                    # 分钟行情文件夹路径 NED_futures_minutely_initial_path
                    NED_futures_minutely_initial_path = os.path.join(NED_variety_initial_date_path, "futures_minutely_initial")

                    # 分钟行情文件路径 NED_fact_futures_minutely_initial_path
                    NED_fact_futures_minutely_initial_path = os.path.join(NED_futures_minutely_initial_path, f"{symbol}.csv")

                    # 分钟行情数据 fact_futures_minutely_initial
                    fact_futures_minutely_initial = pd.read_csv(NED_fact_futures_minutely_initial_path)
                    new_row = pd.concat([new_row, fact_futures_minutely_initial], ignore_index = True) # 拼接 fact_futures_minutely_initial 数据

                fact_futures_minutely_timeseries = pd.concat([fact_futures_minutely_timeseries, new_row], ignore_index = True)
                fact_futures_minutely_timeseries = fact_futures_minutely_timeseries.sort_values(by = ['date'])
                fact_futures_minutely_timeseries.to_csv(NED_fact_futures_minutely_timeseries_path, index = False)

        dimension_symbol_state.at[index, 'future_minutely'] = 1 if symbol_state == 1 else 0
        print('完成 dimension_symbol_state 更新', end = '\n\n')

    # 合约状态表更新 NED_dimension_symbol_state_path
    dimension_symbol_state.to_csv(NED_dimension_symbol_state_path, index = False)
    print( "=" * 50)


# In[7]:


if __name__ == '__main__':
    refresh_fact_futures_minutely_timeseries(variety, start_date = '20251010', deep_validation_switch = False)


# In[ ]:


# symbol = ''
# date_strat_str = ''
# date_end_str = ''
# NED_fact_futures_minutely_timeseries_path = ''
#
# fact_futures_minutely_initial = jqdatasdk.get_price(
#     security = jqdatasdk.normalize_code(symbol),
#     start_date = date_strat_str,
#     end_date = date_end_str,
#     frequency = '1m', # [ 'Xm'(X 分钟) , 'Xd'(X 天) , 'Daily'(即 1 天,等于 1d) , '1m'(即 1 分钟，等于 1m)]
#     fields = ['open','high','low','close','volume','money', 'open_interest'],
#     skip_paused = True, # 如果不跳过, 停牌时会使用停牌前的数据填充(如 fill_paused = True)，上市前或者退市后数据都为 nan
#     fq = 'none', # 复权选项 'none'：不复权, 返回实际价格
#     count = None # 表示获取 end_date 之前几个 frequency 的数据
# )
# fact_futures_minutely_initial.index.name = 'date'
# fact_futures_minutely_initial = fact_futures_minutely_initial.reset_index(drop = False)
# fact_futures_minutely_initial.to_csv(NED_fact_futures_minutely_timeseries_path, index = False)


# In[ ]:




