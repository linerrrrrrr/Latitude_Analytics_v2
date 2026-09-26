#!/usr/bin/env python
# coding: utf-8

# # main_03
# 
# 旧版外部集成初始数据采集总入口。
# 
# 本 Notebook 是该业务工作流的唯一可编辑源文件；同名 `.py` 由项目标准 `latitude` 环境中的默认 PythonExporter 完整生成。

# In[ ]:


from b03_External_Integrated_Data_initial.c00_utils import *

# 获取项目根目录路径 project_root
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# 获取 old_01_Data_Fetching 文件夹路径 a01_Data_Fetching_path
a01_Data_Fetching_path = os.path.join(project_root, 'old_01_Data_Fetching')
# 获取 b03_External_Integrated_Data_initial 文件夹路径 b03_External_Integrated_Data_initial
b03_External_Integrated_Data_initial = os.path.join(a01_Data_Fetching_path, 'b03_External_Integrated_Data_initial')

# 将 b03_External_Integrated_Data_initial 添加到环境中
sys.path.append(b03_External_Integrated_Data_initial)  # 添加到 Python 搜索路径
print(f'添加 Python 搜索路径: {b03_External_Integrated_Data_initial}', end = '\n\n')


# In[ ]:


# 获取 c01_dimension_exchange_calendar.ipynb 函数
from b03_External_Integrated_Data_initial.c01_dimension_exchange_calendar import get_effective_datetime, refresh_dimension_exchange_calendar
# 进行 c01_dimension_exchange_calendar.ipynb 执行更新
effective_datetime = get_effective_datetime(
    hour=21, minute = 0, second = 0, microsecond = 0
)  # 21:00 当天的基准时间
dimension_exchange_calendar = refresh_dimension_exchange_calendar(effective_datetime, save_switch = True)


# In[ ]:


# 获取 c02_dimension_initial_calendar.ipynb 函数
from b03_External_Integrated_Data_initial.c02_dimension_initial_calendar import refresh_dimension_initial_calendar
# 进行 c02_dimension_initial_calendar.ipynb 执行更新
refresh_dimension_initial_calendar(save_switch = False)


# In[ ]:


# 获取 c03_fact_spot_price_html_initial.ipynb 函数
from b03_External_Integrated_Data_initial.c03_fact_spot_price_html_initial import fetch_xianqi_table, fact_spot_price_html_initial_refreshion
# 进行 c03_fact_spot_price_html_initial.ipynb 执行更新
fact_spot_price_html_initial_refreshion(deep_validation_switch = False)


# In[ ]:


# 获取 c04_fact_spot_price_initial.ipynb 函数
from b03_External_Integrated_Data_initial.c04_fact_spot_price_initial import parse_spot_futures_html, _parse_diff_cell, _clean_number, fact_spot_price_initial_refreshion
# 进行 c04_fact_spot_price_initial.ipynb 执行更新
fact_spot_price_initial_refreshion(deep_validation_switch = False)


# In[ ]:


# 获取 c05_fact_futures_foreign_initial.ipynb 函数
from b03_External_Integrated_Data_initial.c05_fact_futures_foreign_initial import fact_futures_foreign_initial_refreshion
# 进行 c05_fact_futures_foreign_initial.ipynb 执行更新
fact_futures_foreign_initial_refreshion(deep_validation_switch = False)

