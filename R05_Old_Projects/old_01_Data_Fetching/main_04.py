#!/usr/bin/env python
# coding: utf-8

# # main_04
# 
# 旧版外部集成时序数据采集总入口。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


from b04_External_Integrated_Data_timeseries.c00_utils import *

# 获取项目根目录路径 project_root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 获取 old_01_Data_Fetching 文件夹路径 a01_Data_Fetching_path
a01_Data_Fetching_path = os.path.join(project_root, 'old_01_Data_Fetching')
# 获取 b04_External_Integrated_Data_timeseries 文件夹路径 b04_External_Integrated_Data_timeseries
b04_External_Integrated_Data_timeseries = os.path.join(a01_Data_Fetching_path, 'b04_External_Integrated_Data_timeseries')

# 将 b04_External_Integrated_Data_timeseries 添加到环境中
sys.path.append(b04_External_Integrated_Data_timeseries)  # 添加到 Python 搜索路径
print(f'添加 Python 搜索路径: {b04_External_Integrated_Data_timeseries}', end = '\n\n')


# In[ ]:


# 获取 c01_dimension_timeseries_completed_calendar.ipynb 函数
from b04_External_Integrated_Data_timeseries.c01_dimension_timeseries_completed_calendar import refresh_dimension_timeseries_completed_calendar
# 进行 c01_dimension_timeseries_completed_calendar.ipynb 执行更新
refresh_dimension_timeseries_completed_calendar(save_switch = True)


# In[ ]:


# 获取 c02_fact_libor_timeseries.ipynb 函数
from b04_External_Integrated_Data_timeseries.c02_fact_libor_timeseries import refresh_fact_shibor_timeseries
# 进行 c02_fact_libor_timeseries.ipynb 执行更新
refresh_fact_shibor_timeseries()


# In[ ]:


# 获取 c03_spot_price_timeseries.ipynb 函数
from b04_External_Integrated_Data_timeseries.c03_spot_price_timeseries import refresh_spot_price_timeseries
# 进行 c03_spot_price_timeseries.ipynb 执行更新
refresh_spot_price_timeseries()


# In[ ]:


# 获取 c04_futures_foreign_timeseries.ipynb 函数
from b04_External_Integrated_Data_timeseries.c04_futures_foreign_timeseries import refresh_futures_foreign_timeseries
# 进行 c04_futures_foreign_timeseries.ipynb 执行更新
refresh_futures_foreign_timeseries()


# In[ ]:


# 获取 c05_baltic_shipping_timeseries.ipynb 函数
from b04_External_Integrated_Data_timeseries.c05_baltic_shipping_timeseries import (
    refresh_baltic_shipping_timeseries, get_tqdm, Baltic_Dry_Index, Baltic_Panamax_Index, Baltic_Capesize_Index, Baltic_Supramax_Index, Baltic_Dirty_Tanker_Index, Baltic_Clean_Tanker_Index
)
# 进行 c05_baltic_shipping_timeseries.ipynb 执行更新
refresh_baltic_shipping_timeseries(
    get_tqdm, Baltic_Dry_Index, Baltic_Panamax_Index, Baltic_Capesize_Index, Baltic_Supramax_Index, Baltic_Dirty_Tanker_Index, Baltic_Clean_Tanker_Index
)


# In[ ]:


# 获取 c06_energy_metal_indices_timeseries.ipynb 函数
from b04_External_Integrated_Data_timeseries.c06_energy_metal_indices_timeseries import (
    refresh_energy_metal_indices_timeseries,
    get_tqdm,
    NYMEX_WTI_Continuous_Commodity_Index,
    nyzs_energy_index,
    Mysteel_China_Coke_Price_Index,
    sys_gtzs_steel_index,
    Mysteel_Steel_Price_Index,
    Xinhua_China_Iron_Ore_Price_Index,
    sys_yousezhishu_nonferrous_metals_index,
    Mysteel_Nickel_Price_Index,
    Mysteel_Tin_Price_Index,
    Mysteel_Zinc_Price_Index,
    Mysteel_Lead_Price_Index,
    Mysteel_Aluminum_Price_Index,
    Mysteel_Copper_Price_Index
)
# 进行 c06_energy_metal_indices_timeseries.ipynb 执行更新
refresh_energy_metal_indices_timeseries(
    get_tqdm,
    NYMEX_WTI_Continuous_Commodity_Index,
    nyzs_energy_index,
    Mysteel_China_Coke_Price_Index,
    sys_gtzs_steel_index,
    Mysteel_Steel_Price_Index,
    Xinhua_China_Iron_Ore_Price_Index,
    sys_yousezhishu_nonferrous_metals_index,
    Mysteel_Nickel_Price_Index,
    Mysteel_Tin_Price_Index,
    Mysteel_Zinc_Price_Index,
    Mysteel_Lead_Price_Index,
    Mysteel_Aluminum_Price_Index,
    Mysteel_Copper_Price_Index
)


# In[ ]:


# 获取 c07_macro_indicators_timeseries.ipynb 函数
from b04_External_Integrated_Data_timeseries.c07_macro_indicators_timeseries import (
    refresh_macro_indicators_timeseries, get_tqdm, index_cpi, index_ppi, index_pmi, index_gdp
)
# 进行 c07_macro_indicators_timeseries.ipynb 执行更新
refresh_macro_indicators_timeseries(
    get_tqdm, index_cpi, index_ppi, index_pmi, index_gdp
)

