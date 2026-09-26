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


external_integrated_data_dir = os.path.join(base_dir, "External_Integrated_Data")
pro = ts.pro_api(secret_dict['tushare']['api_key'])
jqdatasdk.auth(secret_dict['jqdatasdk']['ID'],secret_dict['jqdatasdk']['SECRET'])

# 交易所日历路径 EID_dimension_exchange_calendar_path
EID_dimension_exchange_calendar_path = os.path.join(external_integrated_data_dir, 'dimension_exchange_calendar.csv')

# 原始数据文件夹路径 EID_initial_path
EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")

# 原始数据日历路径 EID_dimension_initial_calendar_path
EID_dimension_initial_calendar_path = os.path.join(EID_initial_path, 'dimension_initial_calendar.csv')

# 原始数据日历 dimension_initial_calendar
dimension_initial_calendar = pd.read_csv(EID_dimension_initial_calendar_path, dtype = {'date': str})
dimension_initial_calendar


# In[8]:


def parse_spot_futures_html(fact_spot_price_html_initial):
    """ 解析生意社现期表 HTML 文件 """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(fact_spot_price_html_initial, 'html.parser')
    table = soup.find('table', {'id': 'fdata'})

    if '暂无数据' in fact_spot_price_html_initial: return pd.DataFrame()  # 生意社 https://www.100ppi.com 在空数据时，html 页面会写 "暂无数据"，则返回空 DataFrame
    if not table: return pd.DataFrame()  # 没有写  "暂无数据" 但确实没有 <table> 这一结构元素 Structural Element (流内容 Flow Content)，则也返回空 DataFrame

    rows = [child for child in table.children if child.name == 'tr']
    data = []
    current_exchange = None

    for row in rows:
        tds = row.find_all('td', recursive=False)

        if len(tds) == 1 and tds[0].get('colspan') == '8':
            exchange_text = tds[0].get_text(strip=True)
            if '交易所' in exchange_text:
                current_exchange = exchange_text
            continue

        if len(tds) == 8:
            first_link = tds[0].find('a')
            if first_link:
                product_name = first_link.get_text(strip=True)
                spot_price = _clean_number(tds[1].get_text(strip=True))
                nearest_code = tds[2].get_text(strip=True)
                nearest_price = _clean_number(tds[3].get_text(strip=True))
                diff1_value, diff1_pct = _parse_diff_cell(tds[4])
                main_code = tds[5].get_text(strip=True)
                main_price = _clean_number(tds[6].get_text(strip=True))
                diff2_value, diff2_pct = _parse_diff_cell(tds[7])

                data.append({
                    'exchange': current_exchange,
                    'product': product_name,
                    'spot_price': spot_price,
                    'nearest_contract_code': nearest_code,
                    'nearest_contract_price': nearest_price,
                    'diff1_value': diff1_value,
                    'diff1_pct': diff1_pct,
                    'main_contract_code': main_code,
                    'main_contract_price': main_price,
                    'diff2_value': diff2_value,
                    'diff2_pct': diff2_pct,
                })

    return pd.DataFrame(data)

def _parse_diff_cell(cell):
    nested_table = cell.find('table')
    if nested_table:
        nested_tds = nested_table.find_all('td')
        if len(nested_tds) >= 2:
            value = _clean_number(nested_tds[0].get_text(strip = True))
            pct = _clean_number(nested_tds[1].get_text(strip = True).replace('%', ''))
            return value, pct
    return None, None

def _clean_number(text):
    if text is None:
        return None
    text = str(text).strip().replace('&nbsp;', '').replace(',', '').replace('%', '')
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


# In[13]:


def fact_spot_price_initial_refreshion(deep_validation_switch = False):

    # 原始数据文件夹路径 EID_initial_path
    EID_initial_path = os.path.join(external_integrated_data_dir, "initial_data")

    # 原始数据日历路径 EID_dimension_initial_calendar_path
    EID_dimension_initial_calendar_path = os.path.join(EID_initial_path, 'dimension_initial_calendar.csv')

    # 原始数据日历 dimension_initial_calendar
    dimension_initial_calendar = pd.read_csv(EID_dimension_initial_calendar_path, dtype = {'date': str})

    date_to_process = dimension_initial_calendar.loc[dimension_initial_calendar['spot_price'] == 1, 'date']
    for date in date_to_process:

        # EID 原始数据日期路径文件夹路径 EID_initial_date_path
        EID_initial_date_path = os.path.join(EID_initial_path, date)

        # 现货原始 html 数据路径 fact_spot_price_html_initial_path
        fact_spot_price_html_initial_path = os.path.join(EID_initial_date_path, 'fact_spot_price_html_initial.html')

        # 现货原始数据路径 fact_spot_price_initial_path
        fact_spot_price_initial_path = os.path.join(EID_initial_date_path, 'fact_spot_price_initial.csv')

        if os.path.exists(fact_spot_price_initial_path):
            if deep_validation_switch:
                try:
                    fact_spot_price_initial = pd.read_csv(fact_spot_price_initial_path)
                    if not fact_spot_price_initial.empty: continue

                except Exception as e: pass
            else: continue

        # 现货原始数据 fact_spot_price_html_initial
        from pathlib import Path
        fact_spot_price_html_initial = Path(fact_spot_price_html_initial_path).read_text(encoding = 'utf-8')

        fact_spot_price_initial = parse_spot_futures_html(fact_spot_price_html_initial)
        fact_spot_price_initial.to_csv(fact_spot_price_initial_path, index = False)


# In[14]:


if __name__ == '__main__':
    fact_spot_price_initial_refreshion(deep_validation_switch = False)


# In[ ]:




