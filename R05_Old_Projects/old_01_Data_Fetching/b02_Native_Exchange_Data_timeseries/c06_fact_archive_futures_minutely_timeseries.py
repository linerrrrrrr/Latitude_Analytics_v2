#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os

from c00_utils import *
# exec(open('c00_utils.py').read())

pn.extension()  # 启用 Jupyter 支持
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_colwidth', 100)


# In[3]:


native_exchange_data_dir = os.path.join(base_dir, "Native_Exchange_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])

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


# # 品种文件夹路径 NED_variety_path
# NED_variety_path = os.path.join(native_exchange_data_dir, variety)
#
# # 合约日历路径 NED_dimension_symbol_calendar_path
# NED_dimension_symbol_calendar_path = os.path.join(NED_variety_path, 'dimension_symbol_calendar.csv')
#
#
#
#
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


# In[4]:


allfutures_1min_varity_path_dict = dict()
allfutures_1min_path = r"E:\BaiduNetdiskDownload\期货全部合约一分钟-10月19\期货全部合约一分钟"

for exchange in os.listdir(allfutures_1min_path):
    exchange_path = os.path.join(allfutures_1min_path, exchange)
    print(exchange_path)

    for variety in os.listdir(exchange_path):
        variety_path = os.path.join(exchange_path, variety)
        print(variety_path)

        for symbol in os.listdir(variety_path):
            symbol_path = os.path.join(variety_path, symbol)

            # 检查是否是文件且以.csv结尾
            if os.path.isfile(symbol_path) and symbol.endswith('.csv'):
                # 分离文件名和扩展名
                filename, ext = os.path.splitext(symbol)

                # 如果文件名中包含点，去掉最后一个点及后面的内容
                if '.' in filename:
                    new_filename = filename.split('.')[0] + ext
                    new_symbol_path = os.path.join(variety_path, new_filename)

                    os.rename(symbol_path, new_symbol_path)
                    print(f"重命名: {symbol} -> {new_filename}") # 重命名

                    symbol_path = new_symbol_path

                allfutures_1min_varity_path_dict[variety] = allfutures_1min_varity_path_dict.get(variety, []) + [symbol_path]

allfutures_1min_varity_path_dict


# In[5]:


all_variety_list = []
for exchange, variety_list in exchange_variety_map.items():
    all_variety_list += variety_list

print(all_variety_list)
for variety in all_variety_list:

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 时间序列文件夹路径 NED_variety_timeseries_path
    NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")

    # 合约状态表路径 NED_dimension_symbol_state_path
    NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')

    # 分钟行情时间序列文件夹路径 NED_futures_minutely_timeseries_path
    NED_futures_minutely_timeseries_path = os.path.join(NED_variety_timeseries_path, "futures_minutely")
    if os.path.exists(NED_dimension_symbol_state_path):  print(True)
    else:  print(False)


# In[6]:


for variety in allfutures_1min_varity_path_dict.keys():

    # 品种文件夹路径 NED_variety_path
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)

    # 时间序列文件夹路径 NED_variety_timeseries_path
    NED_variety_timeseries_path = os.path.join(NED_variety_path , "timeseries_data")

    # 合约状态表路径 NED_dimension_symbol_state_path
    NED_dimension_symbol_state_path = os.path.join(NED_variety_timeseries_path, 'dimension_symbol_state.csv')

    # 分钟行情时间序列文件夹路径 NED_futures_minutely_timeseries_path
    NED_futures_minutely_timeseries_path = os.path.join(NED_variety_timeseries_path, "futures_minutely_timeseries")

    print(variety, end = '\t')
    if os.path.exists(NED_variety_path):  print(True)
    else:
        print(False)
        continue



# In[20]:


import shutil

print("\n开始迁移合约文件...")

for variety, symbol_path_list in allfutures_1min_varity_path_dict.items():
    # 构造目标路径（保持与上文一致的命名规范）
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)
    NED_variety_timeseries_path = os.path.join(NED_variety_path, "timeseries_data")
    NED_futures_minutely_timeseries_path = os.path.join(NED_variety_timeseries_path, "futures_minutely_timeseries")

    # 确保目标目录存在（递归创建）
    if not os.path.exists(NED_futures_minutely_timeseries_path):
        os.makedirs(NED_futures_minutely_timeseries_path, exist_ok = True)
        print(f"创建目录: {NED_futures_minutely_timeseries_path}")

    # 迁移该品种下的所有合约文件
    for symbol_path in symbol_path_list:
        symbol_filename = os.path.basename(symbol_path)
        dest_path = os.path.join(NED_futures_minutely_timeseries_path, symbol_filename)

        try:
            # 使用copy2保留元数据，如不需要保留源文件可改为shutil.move
            if not os.path.exists(dest_path):
                shutil.copy2(symbol_path, dest_path)
                print(f"[复制] {variety}/{symbol_filename}")
            else:
                print(f"[跳过] {variety}/{symbol_filename} 已存在")
        except Exception as e:
            print(f"[错误] {variety}/{symbol_filename}: {str(e)}")

print("迁移完成")


# In[ ]:


r"E:\Latitude_Analytics\03_Futures_Database\Native_Exchange_Data\RB\timeseries_data\futures_minutely"
r"E:\Latitude_Analytics\03_Futures_Database\Native_Exchange_Data\RB\timeseries_data\futures_minutely_timeseries"


# In[7]:


import os
import shutil

print("开始路径修正：删除错误B路径，重命名A为B...")

# 遍历所有品种（基于之前代码中的字典）
for variety in allfutures_1min_varity_path_dict.keys():

    # 构造timeseries_data路径
    NED_variety_path = os.path.join(native_exchange_data_dir, variety)
    NED_variety_timeseries_path = os.path.join(NED_variety_path, "timeseries_data")

    # 如果上层目录不存在则跳过
    if not os.path.exists(NED_variety_timeseries_path):
        continue

    # A路径 (当前存在的正确数据文件夹，但命名不符)
    path_A = os.path.join(NED_variety_timeseries_path, "futures_minutely")
    # B路径 (错误创建的文件夹名，可能为空或需要清理)
    path_B = os.path.join(NED_variety_timeseries_path, "futures_minutely_timeseries")

    # 步骤1: 删除错误的B路径（如果存在）
    if os.path.exists(path_B):
        # ignore_errors=True 确保权限问题不会中断流程
        shutil.rmtree(path_B, ignore_errors=True)
        print(f"[清理] 已删除错误路径: {variety}/futures_minutely_timeseries")

    # 步骤2: 将A重命名为B（轻量级操作，仅修改目录项，不复制文件数据）
    if os.path.exists(path_A):
        os.rename(path_A, path_B)
        print(f"[修正] 已重命名: {variety}/futures_minutely -> futures_minutely_timeseries")
    else:
        print(f"[跳过] {variety}: A路径不存在")

print("路径修正完成")


# In[ ]:




