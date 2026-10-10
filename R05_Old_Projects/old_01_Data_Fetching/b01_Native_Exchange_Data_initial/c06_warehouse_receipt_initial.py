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


# In[6]:


def fact_warehouse_receipt_initial_refreshion(variety, deep_validation_switch = False):

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 品种内原始数据文件夹路径 NED_variety_initial_path
    NED_variety_initial_path = os.path.join(NED_variety_path, "initial_data")
    os.makedirs(NED_variety_initial_path, exist_ok = True) # 创建必要目录，若不存在

    # 品种日历路径 NED_dimension_futures_trading_calendar_path
    NED_dimension_futures_trading_calendar_path = os.path.join(
        native_exchange_data_dir, 'dimension_futures_trading_calendar.csv'
    )
    dimension_futures_trading_calendar = pd.read_csv(NED_dimension_futures_trading_calendar_path, dtype = {'date': str, })
    # 从品种日历提取 date 列与指定 variety 的交易状态掩码
    date_list = dimension_futures_trading_calendar['date'].values  # shape: (N,1)
    min_date, max_date = date_list.min(), date_list.max()
    is_trading_day = dimension_futures_trading_calendar[variety].values  # 1 or 0, shape: (N,1)


    print( "=" * 50)
    print(f"正在处理品种 {variety} 仓单日报......")
    print(f"日期范围: [{min_date} ~ {max_date}]")
    print(f"有效交易日: {sum(is_trading_day)}")
    print(f"文件损坏检测 deep_validation_switch: {deep_validation_switch}", end = '\n\n')

    # 每个品种有数据的起始日期
    date_list = list(enumerate(date_list))
    # date_list = [
    #     date for date in date_list
    #     if date[1] >= warehouse_receipt_dict.get(variety, '00000000')
    # ]

    # 提取需要处理的日期 检查文件是否存在 深度验证
    date_to_process = []
    for i, date in date_list:
        if not is_trading_day[i] == 1: # 跳过非交易日
            continue

        # 品种原始数据路径 NED_variety_initial_date_path
        NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date)

        # 仓单日报原始数据路径 NED_fact_warehouse_receipt_initial_path
        NED_fact_warehouse_receipt_initial_path = os.path.join(NED_variety_initial_date_path, 'fact_warehouse_receipt_initial.csv')

        if os.path.exists(NED_fact_warehouse_receipt_initial_path):
            if deep_validation_switch:
                try:
                    tmp_warehouse_receipt_data = pd.read_csv(NED_fact_warehouse_receipt_initial_path)
                    if not tmp_warehouse_receipt_data.empty:
                        continue  # 验证通过，跳过
                except Exception as e:
                    pass  # 验证失败，加入待处理
            else:
                continue  # 文件存在且不深度验证，跳过

        date_to_process.append(date)

    if not date_to_process:
        print("无需处理新日期。")
        print( "=" * 50)
        return

    # 将 date_to_process 转为 datetime 对象并排序
    date_objecties = [datetime.datetime.strptime(date, "%Y%m%d") for date in date_to_process]
    sorted_pairs = sorted(zip(date_objecties, date_to_process))
    sorted_dates = [date_str for _, date_str in sorted_pairs]

    max_span_days = 30 # 最大允许的自然日跨度
    group_size = 30   # 每组最大日期数量（与原逻辑一致）

    # 动态分组 保证每组内 end_date - start_date <= max_span_days
    group_list = [] # 遍历组 每次完成一个 current_group 就 append 进入 groups
    current_group = [] # 遍历日期 检验日期是否符合放入 组成 current_group

    for date_str in sorted_dates:
        if not current_group:
            current_group.append(date_str)
        else:
            start_date_str = current_group[0]
            end_date_str = date_str

            start_dt = datetime.datetime.strptime(start_date_str, "%Y%m%d")
            end_dt = datetime.datetime.strptime(end_date_str, "%Y%m%d")
            span_days = (end_dt - start_dt).days

            if span_days <= max_span_days and len(current_group) < group_size:  # group_size 限制
                current_group.append(date_str)
            else:
                # 超出跨度，结束当前组，开启新组
                group_list.append(current_group)
                current_group = [date_str]

    if current_group: # sorted_dates 遍历时 每次开新组才会把旧组放入 groups
        group_list.append(current_group) # 故最后一个组在完成逐个日期遍历时不一定会被放入 groups

    print(f"动态分组完成，共 {len(group_list)} 组")

    for group in group_list:
        print('\n', group)

        for date in group:

            # 日期文件夹路径 NED_variety_initial_date_path
            NED_variety_initial_date_path = os.path.join(NED_variety_initial_path, date)
            os.makedirs(NED_variety_initial_date_path, exist_ok = True)

            # 仓单日报文件路径 NED_fact_warehouse_receipt_initial_path
            NED_fact_warehouse_receipt_initial_path = os.path.join(NED_variety_initial_date_path, 'fact_warehouse_receipt_initial.csv')
            print(f"正在处理 {date} 路径: {NED_fact_warehouse_receipt_initial_path}")

            # 日期格式转换：YYYYMMDD -> YYYY-MM-DD
            date_objective = datetime.datetime.strptime(date, '%Y%m%d')
            date_str = datetime.datetime.strftime(date_objective, '%Y-%m-%d')

            # 获取期货仓单日报数据 API 调用
            # jqdatasdk: 聚宽数据 SDK 模块，提供金融数据访问接口
            # finance: 财务数据命名空间/模块，对应数据库中的 finance 数据库/Schema
            #          类似 SQL: USE finance; 或 SELECT ... FROM finance.table_name
            # FUT_WAREHOUSE_RECEIPT: 期货仓单数据表，对应 SQL: FROM FUT_WAREHOUSE_RECEIPT
            # jqdatasdk.finance: 指定数据所在的命名空间（数据库 Schema）
            warehouse_query = jqdatasdk.query(
                jqdatasdk.finance.FUT_WAREHOUSE_RECEIPT.day,
                jqdatasdk.finance.FUT_WAREHOUSE_RECEIPT.warehouse_name,
                jqdatasdk.finance.FUT_WAREHOUSE_RECEIPT.warehouse_receipt_number
            ).filter(
                jqdatasdk.finance.FUT_WAREHOUSE_RECEIPT.underlying_code == variety,
                jqdatasdk.finance.FUT_WAREHOUSE_RECEIPT.day == date_str
            ).order_by(jqdatasdk.finance.FUT_WAREHOUSE_RECEIPT.warehouse_receipt_number.desc())
            # filter()：链式方法 WHERE ... AND ... 添加筛选条件
            # .warehouse_receipt_number.desc(): 按仓单数量降序排列

            # run_query(): 执行查询方法，将构建的 ORM 查询转换为 SQL 并发送到服务器执行，对应 SQL: 执行完整的 SELECT 语句并返回结果集
            fact_warehouse_receipt_initial = jqdatasdk.finance.run_query(warehouse_query)
            time.sleep(0.05) # 冷却以防止接口数据限制

            fact_warehouse_receipt_initial = fact_warehouse_receipt_initial[['warehouse_name', 'warehouse_receipt_number']].copy()
            if fact_warehouse_receipt_initial.empty:
                print(f"数据获取失败 jqdatasdk.finance.run_query 返回为空")
                continue

            fact_warehouse_receipt_initial.to_csv(NED_fact_warehouse_receipt_initial_path, index = False)
            print(f"保存仓单日报: {date} | {variety} | {NED_fact_warehouse_receipt_initial_path}")

    print( "=" * 50)


# In[7]:


if __name__ == "__main__":
    warnings.filterwarnings("ignore",  message = ".*align should be passed as Python or NumPy boolean.*")
    fact_warehouse_receipt_initial_refreshion(variety, deep_validation_switch = False)


# In[ ]:





# In[ ]:




