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


# In[4]:


def rank_table_20_initial_refreshion(variety, deep_validation_switch = False):

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 原始数据根路径 NED_variety_initial_path
    NED_variety_initial_path = os.path.join(NED_variety_path, "initial_data")

    # 合约日历路径 NED_dimension_symbol_calendar_path
    NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')



    dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str})

    # 加载或初始化 dimension_rank_table_20_calendar
    NED_dimension_rank_table_20_calendar_path = os.path.join(NED_variety_path, 'dimension_rank_table_20_calendar.csv')
    if not os.path.exists(NED_dimension_rank_table_20_calendar_path):
        dimension_rank_table_20_calendar = dimension_symbol_calendar.copy()


    else: # 从本地读取 dimension_rank_table_20_calendar 后需要判断是否进行日历更新
        dimension_rank_table_20_calendar = pd.read_csv(NED_dimension_rank_table_20_calendar_path, dtype = {'date': str})

        # 长度不一致表明 dimension_symbol_calendar 有变动，首先对比 date 列检测出哪些列是需要新增的，而后 concat 合并新的 row
        # 接下来判断 column 是否有新合约，将 dimension_symbol_calendar 的新 column 整行合并到 dimension_rank_table_20_calendar 中
        if not dimension_symbol_calendar['date'].equals(dimension_rank_table_20_calendar['date']):

            existing_dates = set(dimension_rank_table_20_calendar['date'])
            date_list = [date for date in dimension_symbol_calendar['date'] if date not in existing_dates]

            if date_list: # 从dimension_symbol_calendar 中提取新日期对应的完整行
                new_rows = dimension_symbol_calendar[dimension_symbol_calendar['date'].isin(date_list)].copy()

                # 裁剪 new_rows，只保留 dimension_rank_table_20_calendar 当前已有的列，避免宽度不一致
                common_columns = [symbol for symbol in dimension_rank_table_20_calendar.columns if symbol in new_rows.columns]
                new_rows = new_rows[common_columns]

                dimension_rank_table_20_calendar = pd.concat([ # 将新行追加到现有表格底部
                    dimension_rank_table_20_calendar,
                    new_rows
                ], ignore_index = True)

        # 只添加新合约列，保留原有各列的内容
        new_symbols = set(dimension_symbol_calendar.columns) - set(dimension_rank_table_20_calendar.columns) - {'date'}
        for symbol in new_symbols:
            # 创建一个从 date 到 symbol 值的映射，使用 map 映射
            date_to_value_map = dimension_symbol_calendar.set_index('date')[symbol].to_dict()
            dimension_rank_table_20_calendar[symbol] = dimension_rank_table_20_calendar['date'].map(date_to_value_map)

        # 重新排序列，确保与 dimension_symbol_calendar 的列顺序一致
        final_columns = ['date'] + sorted([col for col in dimension_symbol_calendar.columns if col != 'date'])
        dimension_rank_table_20_calendar = dimension_rank_table_20_calendar[final_columns]






    # 提取 symbol_list
    symbol_list = [col for col in dimension_symbol_calendar.columns if col != 'date']

    print("=" * 50)
    print(f"正在处理品种 {variety} 持仓排名前20日报......")
    print(f"合约数量: {len(symbol_list)}")
    print(f"文件损坏检测 deep_validation_switch: {deep_validation_switch}", end = '\n\n')

    for symbol in symbol_list:

        mask_status_1 = dimension_rank_table_20_calendar[symbol] == 1  # 在 dimension_rank_table_20_calendar 状态为 1 的日期
        dates_status_1 = dimension_rank_table_20_calendar.loc[mask_status_1, 'date'].tolist()

        mask_status_2 = dimension_rank_table_20_calendar[symbol] == 2  # 在 dimension_rank_table_20_calendar 状态为 2 的日期
        dates_status_2 = dimension_rank_table_20_calendar.loc[mask_status_2, 'date'].tolist()

        date_to_process = []
        for date in (dates_status_1 + dates_status_2):

            # 品种 initial 数据日期文件夹路径 NED_variety_initial_date_path
            NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date)

            # 前 20 表单 initial 文件夹路径 NED_rank_table_20_initial_path
            NED_rank_table_20_initial_path = os.path.join(NED_variety_initial_date_path, "rank_table_20_initial")
            NED_fact_rank_table_20_initial_path = os.path.join(NED_rank_table_20_initial_path, f"{symbol}.csv")

            if not os.path.exists(NED_fact_rank_table_20_initial_path): # 检查是否文件存在
                date_to_process.append(date)
                continue
            else:
                dimension_rank_table_20_calendar.at[ # 将前 20 持仓日历的日期、合约位置标记为 2
                    dimension_rank_table_20_calendar.index[dimension_rank_table_20_calendar['date'] == date].item(),
                    symbol
                ] = 2

            if deep_validation_switch: # 深度检查
                if os.path.exists(NED_fact_rank_table_20_initial_path):
                    try:
                        fact_rank_table_20_initial_rangetotal = pd.read_csv(NED_fact_rank_table_20_initial_path)
                        time.sleep(0.05) # 冷却以防止硬盘过热
                        if fact_rank_table_20_initial_rangetotal.empty: # 确保文件不为空
                            date_to_process.append(date)
                            continue

                    except Exception as e:
                        date_to_process.append(date)
                        continue

        # 去重并保持顺序
        date_to_process = list(dict.fromkeys(date_to_process))
        if not date_to_process:
            continue



        # 将 date_to_process 转为 datetime 对象并排序
        date_objecties = [datetime.datetime.strptime(date, "%Y%m%d") for date in date_to_process]
        sorted_pairs = sorted(zip(date_objecties, date_to_process))
        sorted_dates = [date_str for _, date_str in sorted_pairs]

        max_span_days = 30  # 最大允许的自然日间隔跨度
        group_size = 30     # 每组最多包含的日期数量

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

        print(f"\n正在处理合约 {symbol}")
        for group in group_list:
            print('\n', group)

            start_date = group[0]
            end_date = group[-1]
            for _ in range(3):
                try:

                    fact_rank_table_20_initial_rangetotal = pro.fut_holding(
                        start_date = start_date,
                        end_date = end_date,
                        symbol = symbol
                    )
                    if not fact_rank_table_20_initial_rangetotal.empty:
                        time.sleep(0.5) # 数据请求冷却

                except Exception as e:
                    fact_rank_table_20_initial_rangetotal = pd.DataFrame()

            # 处理该组内每个日期
            for date in group:
                if not fact_rank_table_20_initial_rangetotal.empty and date in fact_rank_table_20_initial_rangetotal['trade_date'].values:

                    # 品种 initial 数据日期文件夹路径 NED_variety_initial_date_path
                    NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date)

                    # 前 20 表单 initial 文件夹路径 NED_rank_table_20_initial_path
                    NED_rank_table_20_initial_path = os.path.join(NED_variety_initial_date_path, "rank_table_20_initial")
                    os.makedirs(NED_rank_table_20_initial_path, exist_ok = True) # NED_rank_table_20_initial_path 不存在时创建

                    fact_rank_table_20_initial = fact_rank_table_20_initial_rangetotal[fact_rank_table_20_initial_rangetotal['trade_date'] == date]
                    fact_rank_table_20_initial.to_csv(os.path.join(NED_rank_table_20_initial_path, f"{symbol}.csv"), index = False)

                    # 更新状态为 2
                    status = dimension_rank_table_20_calendar.index[dimension_rank_table_20_calendar['date'] == date].item()
                    dimension_rank_table_20_calendar.at[status, symbol] = 2
                    print(f"保存持仓排名: {date} | {symbol} | {os.path.join(NED_rank_table_20_initial_path, f'{symbol}.csv')}")

                else:

                    # 无数据，标记为 -1
                    status = dimension_rank_table_20_calendar.index[dimension_rank_table_20_calendar['date'] == date].item()
                    dimension_rank_table_20_calendar.at[status, symbol] = -1
                    print(f"持仓表不存在: {date} | {symbol}")

    # 保存更新后的日历
    dimension_rank_table_20_calendar.to_csv(NED_dimension_rank_table_20_calendar_path, index = False)
    time.sleep(0.05)


    print("=" * 50)
    return dimension_rank_table_20_calendar


# In[5]:


dimension_rank_table_20_calendar = None
if __name__ == "__main__":
    dimension_rank_table_20_calendar = rank_table_20_initial_refreshion(variety, deep_validation_switch = False)
dimension_rank_table_20_calendar


# In[ ]:





# In[6]:


# exchange_variety_map = get_exchange_variety_map()
# for variety in sum(exchange_variety_map.values(), []):
#
#     if variety in sum([i for key, i in exchange_variety_map.items()], []):
#         continue
#
#     # 品种文件夹路径 NED_variety_path
#     NED_variety_path = os.path.join(native_exchange_data_dir, variety)
#
#     # 原始数据根路径 NED_variety_initial_path
#     NED_variety_initial_path = os.path.join(NED_variety_path, "initial_data")
#
#     # 合约日历路径 NED_dimension_symbol_calendar_path
#     NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')
#     dimension_symbol_calendar = pd.read_csv(NED_dimension_symbol_calendar_path, dtype = {'date': str})
#
#     # 加载或初始化 dimension_rank_table_20_calendar
#     NED_dimension_rank_table_20_calendar_path = os.path.join(NED_variety_path, 'dimension_rank_table_20_calendar.csv')
#     if os.path.exists(NED_dimension_rank_table_20_calendar_path):
#         os.remove(NED_dimension_rank_table_20_calendar_path)
#
#     rank_table_20_initial_refreshion(variety, deep_validation_switch = False)


# In[ ]:




