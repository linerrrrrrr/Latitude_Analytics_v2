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


# In[137]:


def refresh_dimension_symbol_calendar(exchange, variety, save_switch = False):

    # 品种日历 dimension_futures_trading_calendar
    NED_dimension_futures_trading_calendar_path = os.path.join(native_exchange_data_dir, 'dimension_futures_trading_calendar.csv')
    dimension_futures_trading_calendar = pd.read_csv(NED_dimension_futures_trading_calendar_path, dtype = {'date': str})

    # 从品种日历提取 date 列与指定 variety 的交易状态掩码
    date_list = dimension_futures_trading_calendar['date'].values  # shape: (N,1)
    min_date, max_date = date_list.min(), date_list.max()
    is_trading_day = dimension_futures_trading_calendar[variety].values  # 1 or 0, shape: (N,1)

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)
    if not os.path.exists(NED_variety_path):
        os.makedirs(NED_variety_path)

    # 合约日历路径 NED_dimension_symbol_calendar_path
    NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, f'dimension_symbol_calendar.csv')

    print( "=" * 50)
    print("正在更新合约日历 dimension_symbol_calendar......")
    print(f"NED_dimension_symbol_calendar_path: {NED_dimension_symbol_calendar_path}")
    print(f"exchange: {exchange}, variety: {variety}")
    print(f"date range: [{min_date}, {max_date}]")


    # 检测是否已完成更新
    if os.path.exists(NED_dimension_symbol_calendar_path):
        dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str})
        if len(dimension_symbol_calendar) == len(date_list) and (dimension_symbol_calendar['date'].values == date_list).all():

            print(f'检测到更新已完成')
            print( "=" * 50, end = "\n\n")

            return dimension_symbol_calendar


    symbol_listing_info = pro.fut_basic(
        exchange = exchange, fut_type = "1", fields = ['fut_code', 'symbol', "list_date", "delist_date"]
    )
    symbol_listing_info = symbol_listing_info[symbol_listing_info['fut_code'] == variety]

    symbol_listing_info = symbol_listing_info[
        (symbol_listing_info['list_date'] >= min_date) & # 合约上市日期必须晚于 min_date
    (symbol_listing_info['list_date'] <= max_date) # 合约的上市日期必须早于 max_date，而下市日期不必早于 max_date
    ]
    symbol_listing_info = symbol_listing_info.sort_values('symbol')

    symbol_list = symbol_listing_info['symbol'].tolist()
    dimension_symbol_calendar = pd.DataFrame( # 初始化 dimension_symbol_calendar 表格
        0, # 初始值
        index = date_list,
        columns = symbol_list,
        dtype = float
    )

    # 对每个 symbol，计算其活跃区间
    for _, row in symbol_listing_info.iterrows():
        sym = row['symbol']
        list_d = row['list_date']
        delist_d = row['delist_date']

        active_mask = (date_list >= list_d) & (date_list <= delist_d)
        valid_mask = active_mask & (is_trading_day == 1) # 同时要求是交易日
        dimension_symbol_calendar.loc[valid_mask, sym] = 1

    # 重置 index，使 date 成为列
    dimension_symbol_calendar = dimension_symbol_calendar.reset_index().rename(columns = {'index': 'date'})


    # 硬编码数据修正
    corrections = {
        'SI': [
            ('SI2312', '20230116', '20230116'),
            ('SI2312', '20231214', '20231214'),
        ],
        'AL': [
            ('AL1103', '20100316', '20100316'),
            ('AL1104', '20100416', '20100421'),
            ('AL1106', '20100618', '20100618'),
            ('AL1107', '20100716', '20100720'),
        ],
        'FU': [
            ('FU1101', '20100104', '20100105'),
            ('FU1105', '20100504', '20100504'),
            ('FU1106', '20100601', '20100602'),
            ('FU1107', '20100701', '20100701'),
            ('FU1807', '20180627', '20180628'),
            ('FU1808', '20180627', '20180628'),
            ('FU1809', '20180627', '20180628'),
            ('FU1810', '20180627', '20180628'),
            ('FU1811', '20180627', '20180628'),
            ('FU1812', '20180627', '20180628'),
        ],
        'ZN':[
            ('ZN1102', '20100222', '20100224'),
            ('ZN1103', '20100316', '20100316'),
            ('ZN1106', '20100618', '20100618'),
        ],
        'FB': [
            ('FB2001', '20190116', '20191203'),
            ('FB2001', '20200115', '20200115'),
            ('FB2002', '20190222', '20191129'),
            ('FB2003', '20190315', '20191129'),
            ('FB2004', '20190416', '20191129'),
            ('FB2005', '20190520', '20191129'),
            ('FB2006', '20190618', '20191129'),
            ('FB2007', '20190715', '20191129'),
            ('FB2008', '20190815', '20191129'),
            ('FB2009', '20190917', '20191129'),
        ],
        'AU':[
            ('AU1101', '20100118', '20100120'),
            ('AU1102', '20100222', '20100225'),
            ('AU1103', '20100316', '20100326'),
            ('AU1104', '20100416', '20100421'),
            ('AU1105', '20100518', '20100521'),
            ('AU1909', '20190912', '20190916'),
            ('AU1911', '20191029', '20191115'),
        ],
        # 'WR': [
        #     ('WR1809', '20180903', '20180917'),
        #     ('WR1810', '20180903', '20181015'),
        #     ('WR1811', '20180903', '20181115'),
        #     ('WR1812', '20180903', '20181217'),
        #     ('WR1901', '20180903', '20190115'),
        #     ('WR1902', '20180903', '20190215'),
        #     ('WR1903', '20180316', '20181015'),
        #     # 20190315
        #     # 20190415
        #     # 20190515
        #     # 20190617
        #     # 20190715
        # ]
    }

    if variety in corrections:
        for symbol, start_date, end_date in corrections[variety]:
            if symbol in dimension_symbol_calendar.columns:
                mask = (
                    (dimension_symbol_calendar['date'] >= start_date) &
                    (dimension_symbol_calendar['date'] <= end_date)
                )
                dimension_symbol_calendar.loc[mask, symbol] = 0



    print(f"共计生成合约日历 {len(symbol_list)} 个合约")
    print("完成更新合约日历 dimension_symbol_calendar")
    print( "=" * 50, end = "\n\n")


    if save_switch:
        dimension_symbol_calendar.to_csv(NED_dimension_symbol_calendar_path, index = False)
        time.sleep(0.05)

    return dimension_symbol_calendar


# In[138]:


dimension_symbol_calendar = None
if __name__ == "__main__":
    dimension_symbol_calendar = refresh_dimension_symbol_calendar(exchange, variety, save_switch = True)
dimension_symbol_calendar


# In[107]:


# data = pro.fut_basic(
#         exchange = exchange, fut_type = "1", fields = ['fut_code', 'symbol', "list_date", "delist_date"]
#     )
# data = data[data['fut_code'] == variety]
# data


# In[8]:


# exchange_variety_map = get_exchange_variety_map()
# for exchange in exchange_variety_map.keys():
#     for variety in exchange_variety_map[exchange]:
#
#         NED_variety_path = os.path.join(native_exchange_data_dir, variety)
#         NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, f'dimension_symbol_calendar.csv')
#         os.remove(NED_dimension_symbol_calendar_path)
#         refresh_dimension_symbol_calendar(exchange, variety, save_switch = True)


# In[80]:


# symbol = 'AU1001'
# date_strat_str = '20090120'
# date_end_str = '20110114'
# NED_fact_futures_minutely_timeseries_path = r"E:\Latitude_Analytics\03_Futures_Database\Native_Exchange_Data\AU\timeseries_data\futures_minutely_timeseries\AU1001.csv"
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




