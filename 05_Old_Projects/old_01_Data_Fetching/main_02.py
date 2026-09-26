#!/usr/bin/env python
# coding: utf-8

# # main_02
# 
# 旧版原生交易所时序数据采集总入口。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


from b02_Native_Exchange_Data_timeseries.c00_utils import *

# 获取项目根目录路径 project_root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 获取 old_01_Data_Fetching 文件夹路径 a01_Data_Fetching_path
a01_Data_Fetching_path = os.path.join(project_root, 'old_01_Data_Fetching')
# 获取 b02_Native_Exchange_Data_timeseries 文件夹路径 b02_Native_Exchange_Data_timeseries
b02_Native_Exchange_Data_timeseries = os.path.join(a01_Data_Fetching_path, 'b02_Native_Exchange_Data_timeseries')

# 将 b02_Native_Exchange_Data_timeseries 添加到环境中
sys.path.append(b02_Native_Exchange_Data_timeseries)  # 添加到 Python 搜索路径
print(f'添加 Python 搜索路径: {b02_Native_Exchange_Data_timeseries}', end = '\n\n')

# exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
# 郑商所（CZCE）的合约命名，带 .1、.2 后缀表示同月份的连续合约或替代合约
# SR107   → 2021年7月交割的标准白糖合h约
# SR107.1 → 2021年7月交割的第一替代白糖合约
# SR107.2 → 2021年7月交割的第二替代白糖合约（如有）
# 交割仓库差异: 不同仓库（如广西库 vs 云南库）注册不同合约
# 品质/品牌差异: 不同等级或产地的产品
# 期转现分流: 满足特定交割需求的并行合约


# In[ ]:


# 获取 c01_dimension_symbol_state.ipynb 函数
from b02_Native_Exchange_Data_timeseries.c01_dimension_symbol_state import get_exchange_variety_map, refresh_dimension_symbol_state
# 进行 c01_dimension_symbol_state.ipynb 执行更新
exchange_variety_map = get_exchange_variety_map()
exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
# exchange_variety_map.pop("DCE", None)
exchange_variety_map.pop('CFFEX', None)  # 金融期货 CFFEX 暂不考虑
for exchange, varieties in exchange_variety_map.items():
    for variety in varieties:
        refresh_dimension_symbol_state(exchange, variety)


# In[ ]:


# 获取 c02_fact_warehouse_receipt_timeseries.ipynb 函数
from b02_Native_Exchange_Data_timeseries.c02_fact_warehouse_receipt_timeseries import get_exchange_variety_map, get_date_to_process, refresh_fact_warehouse_receipt_timeseries
# 进行 c02_fact_warehouse_receipt_timeseries.ipynb 执行更新
exchange_variety_map = get_exchange_variety_map()
exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
# exchange_variety_map.pop("DCE", None)
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

    date_to_process = get_date_to_process(variety)
    refresh_fact_warehouse_receipt_timeseries(date_to_process, variety)


# In[ ]:


# 获取 c03_fact_member_broker_LSV_timeseries.ipynb 函数
from b02_Native_Exchange_Data_timeseries.c03_fact_member_broker_LSV_timeseries import get_exchange_variety_map, refresh_fact_member_broker_LSV_timeseries
# 进行 c03_fact_member_broker_LSV_timeseries.ipynb 执行更新
exchange_variety_map = get_exchange_variety_map()
exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
# exchange_variety_map.pop("DCE", None)
exchange_variety_map.pop('CFFEX', None)  # 金融期货 CFFEX 暂不考虑

for variety in sum(
    exchange_variety_map.values(), []
):
    refresh_fact_member_broker_LSV_timeseries(variety)


# In[ ]:


# 获取 c04_fact_member_non_broker_LSV_timeseries.ipynb 函数
from b02_Native_Exchange_Data_timeseries.c04_fact_member_non_broker_LSV_timeseries import get_exchange_variety_map, refresh_fact_member_non_broker_LSV_timeseries
# 进行 c04_fact_member_non_broker_LSV_timeseries.ipynb 执行更新
exchange_variety_map = get_exchange_variety_map()
exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
# exchange_variety_map.pop("DCE", None)
exchange_variety_map.pop('CFFEX', None)  # 金融期货 CFFEX 暂不考虑

for variety in sum(
    exchange_variety_map.values(), []
): refresh_fact_member_non_broker_LSV_timeseries(variety)


# In[ ]:


# 获取 c05_futures_daily_timeseries.ipynb 函数
from b02_Native_Exchange_Data_timeseries.c05_futures_daily_timeseries import get_exchange_variety_map, refresh_fact_futures_daily_timeseries
# 进行 c05_futures_daily_timeseries.ipynb 执行更新
exchange_variety_map = get_exchange_variety_map()
exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
# exchange_variety_map.pop("DCE", None)
exchange_variety_map.pop('CFFEX', None)  # 金融期货 CFFEX 暂不考虑
for exchange, varieties in exchange_variety_map.items():
    for variety in varieties:
        refresh_fact_futures_daily_timeseries(exchange, variety, deep_validation_switch = False)


# In[ ]:


# c06_fact_futures_minutely_timeseries
# 获取 c06_fact_futures_minutely_timeseries.ipynb 函数
from b02_Native_Exchange_Data_timeseries.c06_fact_futures_minutely_timeseries import get_exchange_variety_map, refresh_fact_futures_minutely_timeseries
# 进行 c06_fact_futures_minutely_timeseries.ipynb 执行更新
exchange_variety_map = get_exchange_variety_map()
exchange_variety_map.pop('CZCE', None)  # 郑商所 CZCE 暂不考虑
# exchange_variety_map.pop("DCE", None)
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

if 'SHFE' in exchange_variety_map:
    if 'WR' in exchange_variety_map['SHFE']:
        exchange_variety_map['SHFE'].remove('WR')

for variety in sum(
    exchange_variety_map.values(), []
):
    # if variety not in  ['AU']: continue
    refresh_fact_futures_minutely_timeseries(variety, start_date = '20251010', deep_validation_switch = False)

