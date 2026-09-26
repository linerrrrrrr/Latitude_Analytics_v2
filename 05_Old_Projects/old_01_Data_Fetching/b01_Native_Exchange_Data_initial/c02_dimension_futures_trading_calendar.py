#!/usr/bin/env python
# coding: utf-8

# In[1]:


from c00_utils import *
# exec(open('c00_utils.py').read())

# pn.extension()  # 启用 Jupyter 支持
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[2]:


native_exchange_data_dir = os.path.join(base_dir, "Native_Exchange_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 交易所日历路径 NED_dimension_exchange_calendar_path
NED_dimension_exchange_calendar_path = os.path.join(native_exchange_data_dir, 'dimension_exchange_calendar.csv')

# 交易所日历 dimension_exchange_calendar
dimension_exchange_calendar = pd.read_csv(NED_dimension_exchange_calendar_path, dtype = {'date': str})
dimension_exchange_calendar


# In[3]:


def refresh_dimension_futures_trading_calendar(save_switch = False):

    # 交易所日历路径 NED_dimension_exchange_calendar_path
    NED_dimension_exchange_calendar_path = os.path.join(
        native_exchange_data_dir, 'dimension_exchange_calendar.csv'
    )
    # 品种日历路径 NED_dimension_futures_trading_calendar_path
    NED_dimension_futures_trading_calendar_path = os.path.join(
        native_exchange_data_dir, 'dimension_futures_trading_calendar.csv'
    )

    # 交易所日历 dimension_exchange_calendar
    dimension_exchange_calendar = pd.read_csv(
        NED_dimension_exchange_calendar_path, dtype = {'date': str}
    )
    effective_date_str = dimension_exchange_calendar['date'].max()

    print("=" * 50)
    print("正在更新品种日历 dimension_futures_trading_calendar......")
    print(f"NED_dimension_exchange_calendar_path: {NED_dimension_exchange_calendar_path}")
    print(f"NED_dimension_futures_trading_calendar_path: {NED_dimension_futures_trading_calendar_path}")
    print(f"effective_date_str: {effective_date_str}")

    exchange_list = [col for col in dimension_exchange_calendar.columns if col != 'date'] # 交易所列表
    variety_list = []
    for exchange in exchange_list:

        variety_df = pd.DataFrame()
        for attempt in range(3):
            try:
                variety_df = pro.fut_basic(exchange = exchange, fut_type = "1", fields = ["fut_code", "list_date", "delist_date"])
                print(f"获取 {exchange} 品种完成，共计 {len(variety_df)} 行......")
                time.sleep(0.3)
                break
            except Exception as e:
                print(f"第 {attempt + 1} 次获取 {exchange} 品种信息失败: {e}")

        variety_df = variety_df.groupby("fut_code", as_index = False).agg({
            "list_date": "min", "delist_date": "max",
        }) # groupby 聚合，保留每个品种最早上市日、最晚退市日、所属交易所
        variety_df["exchange"] = exchange
        variety_list.append(variety_df)

    variety_info = pd.concat(variety_list, ignore_index = True)
    dimension_futures_trading_calendar = dimension_exchange_calendar[["date"]].copy()

    # 基于 dimension_exchange_calendar 构建品种交易日标记
    for _, row in variety_info.iterrows():
        variety = row["fut_code"]
        exchange = row["exchange"]
        list_date = row["list_date"]
        delist_date = row["delist_date"]

         # 从已有日历中取出该交易所的交易状态，NaN 视为非交易日
        is_open_series = dimension_exchange_calendar[exchange].fillna(0).astype(int) # 交易所日历 1 为交易日，0 为非交易日
        mask = (
            (dimension_exchange_calendar["date"] >= list_date) &
            (dimension_exchange_calendar["date"] <= delist_date)
        )
        dimension_futures_trading_calendar[variety] = (is_open_series & mask).astype(int) # 有效交易日 = 交易所开市 & 在合约生命周期内

    print(f"共计更新日历 {len(variety_info)} 个品种")
    print("完成更新品种日历 dimension_futures_trading_calendar")
    print("=" * 50, end="\n\n")

    if save_switch:
        dimension_futures_trading_calendar.to_csv(NED_dimension_futures_trading_calendar_path, index = False)
    return dimension_futures_trading_calendar


# In[4]:


dimension_futures_trading_calendar = None
if __name__ == '__main__':
    dimension_futures_trading_calendar = refresh_dimension_futures_trading_calendar(save_switch = True)
dimension_futures_trading_calendar


# In[ ]:




