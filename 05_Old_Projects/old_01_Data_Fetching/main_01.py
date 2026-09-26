#!/usr/bin/env python
# coding: utf-8

# # main_01
# 
# 旧版原生交易所初始数据采集总入口。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


from b01_Native_Exchange_Data_initial.c00_utils import *

# 获取项目根目录路径 project_root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 获取 old_01_Data_Fetching 文件夹路径 a01_Data_Fetching_path
a01_Data_Fetching_path = os.path.join(project_root, 'old_01_Data_Fetching')
# 获取 b01_Native_Exchange_Data_initial 文件夹路径 b01_Native_Exchange_Data_initial_path
b01_Native_Exchange_Data_initial_path = os.path.join(a01_Data_Fetching_path, 'b01_Native_Exchange_Data_initial')

# 将 b01_Native_Exchange_Data_initial_path 添加到环境中
sys.path.append(b01_Native_Exchange_Data_initial_path)  # 添加到 Python 搜索路径
print(f'添加 Python 搜索路径: {b01_Native_Exchange_Data_initial_path}', end = '\n\n')


# In[ ]:


# 获取 c01_dimension_exchange_calendar 函数
from b01_Native_Exchange_Data_initial.c01_dimension_exchange_calendar import (refresh_dimension_exchange_calendar, get_effective_datetime)
# 进行 c01_dimension_exchange_calendar 执行更新
effective_datetime = get_effective_datetime(
    hour = 20, minute = 0, second = 0, microsecond = 0
)  # 20:00 当天的基准时间
dimension_exchange_calendar = refresh_dimension_exchange_calendar(effective_datetime, save_switch = True)


# In[ ]:


# 获取 c02_dimension_futures_trading_calendar 函数
from b01_Native_Exchange_Data_initial.c02_dimension_futures_trading_calendar import refresh_dimension_futures_trading_calendar
# 进行 c02_dimension_futures_trading_calendar 执行更新
refresh_dimension_futures_trading_calendar(save_switch = True)


# In[ ]:


# 获取 c03_dimension_symbol_calendar 函数
from b01_Native_Exchange_Data_initial.c03_dimension_symbol_calendar import get_exchange_variety_map, refresh_dimension_symbol_calendar
# 进行 c03_dimension_symbol_calendar 执行更新
exchange_variety_map = get_exchange_variety_map()
for exchange in exchange_variety_map.keys():
    for variety in exchange_variety_map[exchange]:
        refresh_dimension_symbol_calendar(exchange, variety, save_switch = True)


# In[ ]:


# 获取 c04_rank_table_20_initial 函数
from b01_Native_Exchange_Data_initial.c04_rank_table_20_initial import get_exchange_variety_map, rank_table_20_initial_refreshion
# 进行 c04_rank_table_20_initial 执行更新
exchange_variety_map = get_exchange_variety_map()
for variety in sum(exchange_variety_map.values(), []):
    rank_table_20_initial_refreshion(variety, deep_validation_switch = False)
print(end = '\n\n')


# In[ ]:


# 获取 c05_futures_minutely_initial 函数
from b01_Native_Exchange_Data_initial.c05_futures_minutely_initial import (
    get_exchange_variety_map,
    get_date_variety_symbol_map,
    get_date_variety_symbol_path_dict,
    get_date_variety_symbol_existence_dict,
    Get_futures_minutely_initial_JQDataSDK
)
# 进行 c05_futures_minutely_initial 执行更新
exchange_variety_map = get_exchange_variety_map()
native_exchange_data_dir = os.path.join(base_dir, "Native_Exchange_Data")
# 品种-合约日历 字典 variety_symbol_calendar_map
date_variety_symbol_map = get_date_variety_symbol_map(native_exchange_data_dir, exchange_variety_map, start_date = '20251010')
# 日期-品种-合约分钟行情路径 字典 date_variety_symbol_path_dict
date_variety_symbol_path_dict = get_date_variety_symbol_path_dict(native_exchange_data_dir, date_variety_symbol_map)

# 日期-品种-合约分钟行情-文件检测 字典 date_variety_symbol_existence_dict
date_variety_symbol_existence_dict = get_date_variety_symbol_existence_dict(date_variety_symbol_path_dict, deep_validation_switch = False)
warnings.filterwarnings("ignore",  message = ".*align should be passed as Python or NumPy boolean.*")
Get_futures_minutely_initial_JQDataSDK(date_variety_symbol_map, date_variety_symbol_path_dict, date_variety_symbol_existence_dict)


# In[ ]:


# 获取 c06_warehouse_receipt_initial 函数
from b01_Native_Exchange_Data_initial.c06_warehouse_receipt_initial import get_exchange_variety_map, fact_warehouse_receipt_initial_refreshion
exchange_variety_map = get_exchange_variety_map()
exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
exchange_variety_map.pop('CFFEX', None)  # 金融期货 CFFEX 暂不考虑

if 'INE' in exchange_variety_map:
    if 'SCTAS' in exchange_variety_map['INE']:
        exchange_variety_map['INE'].remove('SCTAS')  # SC，中质含硫原油，Sour Crude；TAS 是交易指令类型，表示以结算价交易

if 'DCE' in exchange_variety_map:
    if 'L_F' in exchange_variety_map['DCE']:
        exchange_variety_map['DCE'].remove('L_F')  # Linear Low Density Polyethylene（LLDPE，线性低密度聚乙烯），即"塑料"，tushare 带 F 后缀可能有特殊意义
    if 'V_F' in exchange_variety_map['DCE']:
        exchange_variety_map['DCE'].remove('V_F')  # 聚氯乙烯（Polyvinyl Chloride），大连商品交易所（DCE）上市的化工品期货，即塑料板、PVC，tushare 带 F 后缀可能有特殊意义
    if 'PP_F' in exchange_variety_map['DCE']:
        exchange_variety_map['DCE'].remove('PP_F')  # 聚丙烯（Polypropylene），大连商品交易所（DCE）上市的化工品期货，tushare 带 F 后缀可能有特殊意义

for variety in sum(exchange_variety_map.values(), []):
    if variety in [
        # 大连商品交易所 (DCE)
        'JD',  # 鸡蛋 (Egg) - DCE
        'CS',  # 玉米淀粉 (Corn Starch) - DCE
        'JM',  # 焦煤 (Coking Coal) - DCE
        'C',  # 玉米 (Corn) - DCE
        'M',  # 豆粕 (Soybean Meal) - DCE
        'V',  # PVC/聚氯乙烯 (Polyvinyl Chloride) - DCE
        'EB',  # 苯乙烯 (Styrene) - DCE
        'EG',  # 乙二醇 (Ethylene Glycol) - DCE
        'P',  # 棕榈油 (Palm Oil) - DCE
        'Y',  # 豆油 (Soybean Oil) - DCE
        'L',  # 聚乙烯/LLDPE (Linear Low Density Polyethylene) - DCE
        'FB',  # 纤维板 (Fiberboard) - DCE
        'A',  # 豆一/黄大豆1号 (Soybean No.1, Non-GMO) - DCE
        'B',  # 豆二/黄大豆2号 (Soybean No.2, Import) - DCE
        'BB',  # 胶合板 (Blockboard) - DCE
        'I',  # 铁矿石 (Iron Ore) - DCE
        'J',  # 焦炭 (Coke) - DCE
        'PP',  # 聚丙烯 (Polypropylene) - DCE
        'RR',  # 粳米 (Round-grained Rice/Japonica Rice) - DCE
        'LH',  # 生猪 (Live Hog) - DCE
        'PG',  # 液化石油气/LPG (Liquefied Petroleum Gas) - DCE
        'LG',  # 原木 (Log/Lumber) - DCE [2024年上市]
        'BZ',  # [待确认] 未知品种代码 (非标准代码，可能为特定数据源自定义或笔误)

        # # 广州期货交易所 (GFEX)
        # 'LC',  # 碳酸锂 (Lithium Carbonate) - GFEX
        # 'SI',  # 工业硅 (Industrial Silicon) - GFEX
        # 'PS',  # [待确认] 非广期所标准代码 (广期所品种: LC, SI; 可能为特定数据源自定义)

        # # 上海期货交易所 (SHFE)
        # 'PD',  # [待确认] 非标准代码 (可能为钯金Palladium，标准代码通常为PA; 或特定数据源自定义)
        # 'PT',  # 铂金 (Platinum) - SHFE
        # 'WR',  # 线材 (Wire Rod) - SHFE
        # 'OP',  # [待确认] 非标准代码 (可能为期权Option缩写或特定数据源自定义)

        # # 上海国际能源交易中心 (INE)
        # 'NR',  # 20号胶 (Natural Rubber TSR20) - INE
        # 'LU',  # 低硫燃料油 (Low Sulfur Fuel Oil) - INE
        # 'BC',  # 国际铜 (Copper, Bonded Warehouse) - INE
        'EC',  # 集运指数(欧线) (Containerized Freight Index, Europe Route) - INE
    ]: continue

    warnings.filterwarnings("ignore", message = ".*align should be passed as Python or NumPy boolean.*")
    fact_warehouse_receipt_initial_refreshion(variety, deep_validation_switch = False)

