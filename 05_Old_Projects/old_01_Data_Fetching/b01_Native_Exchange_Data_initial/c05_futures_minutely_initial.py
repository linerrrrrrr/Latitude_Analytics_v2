#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

# pn.extension()  # 启用 Jupyter 支持
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[67]:


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


# In[ ]:





# In[4]:


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


# In[68]:


def get_date_variety_symbol_map(native_exchange_data_dir, exchange_variety_map, start_date = '20251010'):

    # 交易所日历路径 NED_dimension_exchange_calendar_path
    NED_dimension_exchange_calendar_path = os.path.join(native_exchange_data_dir, 'dimension_exchange_calendar.csv')
    dimension_exchange_calendar = pd.read_csv(NED_dimension_exchange_calendar_path, dtype = {'date': str, })

    # 待检测日期列表 date_list
    dimension_exchange_calendar = dimension_exchange_calendar[dimension_exchange_calendar['SHFE'] == 1] # 以上期所有交易日为判准
    date_list = [date for date in dimension_exchange_calendar['date'] if date >= start_date] # 制作 date_list 以交易日与大于起始日期为条件

    # 品种列表 variety_list
    exchange_variety_map.pop('CZCE', None) # 郑商所 CZCE 暂不考虑
    exchange_variety_map.pop('CFFEX', None) # 金融期货 CFFEX 暂不考虑

    if 'INE' in exchange_variety_map and 'SCTAS' in exchange_variety_map['INE']:
        exchange_variety_map['INE'].remove('SCTAS') # SC，中质含硫原油，Sour Crude；TAS 是交易指令类型，表示以结算价交易

    if 'DCE' in exchange_variety_map:
        if 'L_F' in exchange_variety_map['DCE']:
            exchange_variety_map['DCE'].remove('L_F') # Linear Low Density Polyethylene（LLDPE，线性低密度聚乙烯），即"塑料"，tushare 带 F 后缀可能有特殊意义
        if 'V_F' in exchange_variety_map['DCE']:
            exchange_variety_map['DCE'].remove('V_F') # 聚氯乙烯（Polyvinyl Chloride），大连商品交易所（DCE）上市的化工品期货，即塑料板、PVC，tushare 带 F 后缀可能有特殊意义
        if 'PP_F' in exchange_variety_map['DCE']:
            exchange_variety_map['DCE'].remove('PP_F') # 聚丙烯（Polypropylene），大连商品交易所（DCE）上市的化工品期货，tushare 带 F 后缀可能有特殊意义

    variety_list = sum(exchange_variety_map.values(), [])

    print("=" * 50)
    print("分钟行情数据 get_date_variety_symbol_map")
    print("正在生成 品种-合约日历 字典 variety_symbol_calendar_map ......")
    print(f"获取分钟行情待处理日期数: {len(date_list)}", end = '\n\n')
    print(f"获取分钟行情日期列表: {date_list}", end = '\n\n')
    print(f"待处理品种列表: {variety_list}", end = '\n\n')

    # 品种-合约日历 字典 variety_symbol_calendar_map
    variety_symbol_calendar_map = dict() # 待填充
    for variety in variety_list:

        # 品种文件夹路径 NED_variety_path
        NED_variety_path = os.path.join(native_exchange_data_dir, variety)

        # 合约日历路径 NED_dimension_symbol_calendar_path
        NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')

        # 填充 variety_symbol_calendar_map
        NED_dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str, })
        NED_dimension_symbol_calendar = NED_dimension_symbol_calendar[NED_dimension_symbol_calendar['date'] >= start_date]
        variety_symbol_calendar_map[variety] = NED_dimension_symbol_calendar


    # 日期-品种-合约列表 字典 date_variety_symbol_map
    date_variety_symbol_map = dict()
    for date in date_list:

        # 当前循环中的 品种-合约列表 字典 tmp_variety_symbol_map
        tmp_variety_symbol_map = dict()
        for variety, NED_dimension_symbol_calendar in variety_symbol_calendar_map.items():

            index = NED_dimension_symbol_calendar.index[ # 将 pandas.core.indexes.base.Index 用掩码切割出指定值 如: Index([4363], dtype='int64')
                NED_dimension_symbol_calendar['date'] == date # 用 date 列匹配当前循环的日期制作 mask
            ][0] # 完成切割的 pandas.core.indexes.base.Index 对象用 [0] 提取 目标行的 index
            row = NED_dimension_symbol_calendar.loc[index] # .loc 方法传入唯一的 index 值后返回 series 对象，行的列名转为索引

            symbol_list = row[row == 1].index.tolist() # series 获取值为 1 的索引，即 symbol_list
            tmp_variety_symbol_map[variety] = symbol_list

        # 把当前循环中的 品种-合约列表 字典 tmp_variety_symbol_map 以当前日期为键，
        # 添加进 日期-品种-合约列表 字典 date_variety_symbol_map
        date_variety_symbol_map[date] = tmp_variety_symbol_map

    # print('date_variety_symbol_map: ', date_variety_symbol_map) # date_variety_symbol_map 太长不打印
    print( "=" * 50, end = "\n\n")
    return date_variety_symbol_map # 品种-合约日历 字典 variety_symbol_calendar_map


# In[69]:


def get_date_variety_symbol_path_dict(native_exchange_data_dir, date_variety_symbol_map):

    print("=" * 50)
    print("分钟行情数据 get_date_variety_symbol_path_dict")
    print("正在生成 日期-品种-合约分钟行情路径 字典 date_variety_symbol_path_dict ......")
    print(f"获取待处理日期数量: {len(date_variety_symbol_map.keys())}", end = '\n\n')
    print(f"待处理日期列表: {date_variety_symbol_map.keys()}", end = '\n\n')
    print("当前循环日期: ")

    # 函数的返回值 日期-品种-合约分钟行情路径 字典 date_variety_symbol_path_dict
    date_variety_symbol_path_dict = dict()

    for date in date_variety_symbol_map.keys():

        date_variety_symbol_path_dict[date] = dict() # 更新 日期-品种-合约分钟行情路径 字典 的日期键
        tmp_variety_symbol_map = date_variety_symbol_map[date]

        print(date, end = ', ')
        for variety in tmp_variety_symbol_map.keys():

            # 品种文件夹路径 NED_variety_path
            NED_variety_path = os.path.join(native_exchange_data_dir, variety)

            # 原始数据文件夹路径 NED_variety_initial_path
            NED_variety_initial_path = os.path.join(NED_variety_path, "initial_data")

            # 品种日期文件夹路径 NED_variety_initial_date_path
            NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date)
            os.makedirs(NED_variety_initial_date_path, exist_ok = True) # 创建必要目录，若不存在

            date_variety_symbol_path_dict[date][variety] = dict() # 更新 日期-品种-合约分钟行情路径 字典 在本次循环中日期对应的品种键
            for symbol in tmp_variety_symbol_map[variety]:

                # 分钟行情文件夹路径 NED_futures_minutely_initial_path
                NED_futures_minutely_initial_path = os.path.join(NED_variety_initial_date_path, "futures_minutely_initial")
                os.makedirs(NED_futures_minutely_initial_path, exist_ok = True) # 创建必要目录，若不存在

                # 合约的当日分钟行情文件路径 NED_fact_futures_minutely_initial_path
                NED_fact_futures_minutely_initial_path = os.path.join(NED_futures_minutely_initial_path, f'{symbol}.csv')

                # 更新 日期-品种-合约分钟行情路径 字典 在本次循环中日期对应的 本次循环的品种对应的 本次循环的合约的 分钟行情路径 NED_fact_futures_minutely_initial_path
                date_variety_symbol_path_dict[date][variety][symbol] = NED_fact_futures_minutely_initial_path

    print("\n完成路径赋值")
    print( "=" * 50, end = "\n\n")
    # 函数的返回值 日期-品种-合约分钟行情路径 字典 date_variety_symbol_path_dict
    return date_variety_symbol_path_dict


# In[70]:


def get_date_variety_symbol_existence_dict(date_variety_symbol_path_dict, deep_validation_switch = False):

    print("=" * 50)
    print("分钟行情数据 get_date_variety_symbol_existence_dict")
    print("正在生成 日期-品种-合约分钟行情-文件检测 字典 date_variety_symbol_existence_dict ......")
    print("deep_validation_switch: ", deep_validation_switch)
    print(f"获取待处理日期数量: {len(date_variety_symbol_path_dict.keys())}", end = '\n\n')
    print(f"待处理日期列表: {date_variety_symbol_path_dict.keys()}")

    # 函数的返回值 日期-品种-合约分钟行情-文件检测 字典 date_variety_symbol_existence_dict
    date_variety_symbol_existence_dict = dict()

    for date in date_variety_symbol_path_dict.keys():

        date_variety_symbol_existence_dict[date] = dict() # 更新 日期-品种-合约分钟行情-文件检测 字典 的日期键
        tmp_variety_symbol_map = date_variety_symbol_path_dict[date]
        for variety in tmp_variety_symbol_map.keys():

            date_variety_symbol_existence_dict[date][variety] = dict() # 更新 日期-品种-合约分钟行情-文件检测 字典 在本次循环中日期对应的品种键
            for symbol in tmp_variety_symbol_map[variety]:

                # 获取 日期-品种-合约分钟行情路径 字典 在本次循环中日期对应的 本次循环的品种对应的 本次循环的合约的 分钟行情路径 NED_fact_futures_minutely_initial_path
                NED_fact_futures_minutely_initial_path = date_variety_symbol_path_dict[date][variety][symbol]

                # 日期-品种-合约分钟行情-文件检测 字典 记录 NED_fact_futures_minutely_initial_path 是否有数据
                date_variety_symbol_existence_dict[date][variety][symbol] = True # 以文件存在为初始值 True

                if not os.path.exists(NED_fact_futures_minutely_initial_path):
                    date_variety_symbol_existence_dict[date][variety][symbol] = False # 文件不存在则记录为 False

                if os.path.exists(NED_fact_futures_minutely_initial_path) and deep_validation_switch:
                    try:
                        if pd.read_csv(NED_fact_futures_minutely_initial_path).empty: # 读取成功但数据为空，则记录为 False
                            date_variety_symbol_existence_dict[date][variety][symbol] = False
                            time.sleep(0.02) # 冷却以防止硬盘过热与文件损坏

                    except Exception as e:
                        date_variety_symbol_existence_dict[date][variety][symbol] = False # 读取失败则记录为 False

    print("\n已完成 日期-品种-合约分钟行情-文件检测 字典 date_variety_symbol_existence_dict")
    print( "=" * 50, end = "\n\n")
    # 函数的返回值 日期-品种-合约分钟行情-文件检测 字典 date_variety_symbol_existence_dict
    return date_variety_symbol_existence_dict


# In[71]:


def get_variety_symbol_booled_dict(date_variety_symbol_existence_dict, date):
    variety_symbol_existence_dict = date_variety_symbol_existence_dict[date]

    print("=" * 50)
    print("分钟行情数据 逐日验证 get_variety_symbol_booled_dict")
    print("当前循环日期: ", {date})
    print(f"待处理字典长度: {len(variety_symbol_existence_dict.keys())}", end = '\n\n')
    print(f"待处理 variety_symbol_existence_dict.keys(): {variety_symbol_existence_dict.keys()}")

    # 只保留值为 False 的合约
    variety_symbol_booled_dict = {
        variety: {
            contract: exists
            for contract, exists in contracts.items()
            if exists is False  # 严格判断 False
        }
        for variety, contracts in variety_symbol_existence_dict.items()
    }

    # 移除没有任何 False 合约的品种
    variety_symbol_booled_dict = {
        variety: contracts
        for variety, contracts in variety_symbol_booled_dict.items()
        if contracts  # 非空字典才保留
    }

    print("variety_symbol_booled_dict: ", variety_symbol_booled_dict)
    print("=" * 50, end = "\n\n")
    if variety_symbol_booled_dict:
        return True
    else:
        return False


# In[1]:


def Get_futures_minutely_initial_JQDataSDK(date_variety_symbol_map, date_variety_symbol_path_dict, date_variety_symbol_existence_dict):

    print("=" * 50)
    print("分钟行情数据 Get_futures_minutely_initial_JQDataSDK")
    print("正在通过 jqdatasdk 获取分钟行情数据 ......")
    print(f"获取待处理日期数量: {len(date_variety_symbol_map.keys())}", end = '\n\n')
    print(f"待处理日期列表: {date_variety_symbol_map.keys()}")

    date_to_process = sorted(date_variety_symbol_map.keys())
    for date in date_to_process:

        date_objective = datetime.datetime.strptime(date, '%Y%m%d')
        date_strat_objective = date_objective.replace(hour = 9, minute = 0, second = 0)
        date_end_objective = (date_objective + datetime.timedelta(days = 1)).replace(hour = 6, minute = 0, second = 0)

        date_strat_str = datetime.datetime.strftime(date_strat_objective, '%Y-%m-%d %H-%M-%S')
        date_end_str = datetime.datetime.strftime(date_end_objective, '%Y-%m-%d %H-%M-%S')

        variety_list = date_variety_symbol_map[date].keys()
        for variety in variety_list:

            symbol_list = date_variety_symbol_map[date][variety]
            for symbol in symbol_list:

                # 本次循环的品种对应的 本次循环的合约的 分钟行情路径 NED_fact_futures_minutely_initial_path
                NED_fact_futures_minutely_initial_path = date_variety_symbol_path_dict[date][variety][symbol]
                symbol_existence = date_variety_symbol_existence_dict[date][variety][symbol] # 是否文件已经存在

                if not symbol_existence: # 若文件不存在，则获取数据并写入 NED_fact_futures_minutely_initial_path

                    fact_futures_minutely_initial = jqdatasdk.get_price(
                        security = jqdatasdk.normalize_code(symbol),
                        start_date = date_strat_str,
                        end_date = date_end_str,
                        frequency = '1m', # [ 'Xm'(X 分钟) , 'Xd'(X 天) , 'Daily'(即 1 天,等于 1d) , '1m'(即 1 分钟，等于 1m)]
                        fields = ['open','high','low','close','volume','money', 'open_interest'],
                        skip_paused = True, # 如果不跳过, 停牌时会使用停牌前的数据填充(如 fill_paused = True)，上市前或者退市后数据都为 nan
                        fq = 'none', # 复权选项 'none'：不复权, 返回实际价格
                        count = None # 表示获取 end_date 之前几个 frequency 的数据
                    )
                    fact_futures_minutely_initial.index.name = 'date'
                    fact_futures_minutely_initial = fact_futures_minutely_initial.reset_index(drop = False)
                    fact_futures_minutely_initial.to_csv(NED_fact_futures_minutely_initial_path, index = False)

    print("\n已完成分钟行情数据 Get_futures_minutely_initial_JQDataSDK")
    print("=" * 50, end = "\n\n")


# In[73]:


if __name__ == '__main__':

    # 品种-合约日历 字典 variety_symbol_calendar_map
    date_variety_symbol_map = get_date_variety_symbol_map(native_exchange_data_dir, exchange_variety_map, start_date = '20251010')

    # 日期-品种-合约分钟行情路径 字典 date_variety_symbol_path_dict
    date_variety_symbol_path_dict = get_date_variety_symbol_path_dict(native_exchange_data_dir, date_variety_symbol_map)

    # 日期-品种-合约分钟行情-文件检测 字典 date_variety_symbol_existence_dict
    date_variety_symbol_existence_dict = get_date_variety_symbol_existence_dict(date_variety_symbol_path_dict, deep_validation_switch = True)

    warnings.filterwarnings("ignore",  message = ".*align should be passed as Python or NumPy boolean.*")
    Get_futures_minutely_initial_JQDataSDK(date_variety_symbol_map, date_variety_symbol_path_dict, date_variety_symbol_existence_dict)


# In[ ]:




