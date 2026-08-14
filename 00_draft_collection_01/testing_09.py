# ==============================================================================
# 沃尔玛 O2O 外卖时段单量预测模型（XGBoost + Prophet 混合预测版）
#
# 混合融合策略：
# 1. 早晨段 (07:00 ~ 10:00)：采用 XGBoost 机器学习模型 (含 1.90 黄金微调系数)
# 2. 其他时段 (11:00 ~ 23:00)：采用 Meta Prophet 时间序列平滑模型
# 3. 数据预处理：包含 2026-07-24~26 台风天极端爆单平滑降噪
# ==============================================================================

import os
import warnings
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error

# 尝试导入 Prophet 库
try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False
    print("⚠️ 警告: 未安装 prophet 库！请先运行 `pip install prophet`。如果未安装，程序将退化为全时段 XGBoost。")

warnings.filterwarnings('ignore')

# ==================== 1. 中国法定节假日与调休补班日映射 ====================
HOLIDAYS_2025_2026 = set([
    '2025-01-01', '2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31',
    '2025-02-01', '2025-02-02', '2025-02-03', '2025-02-04', '2025-04-04',
    '2025-04-05', '2025-04-06', '2025-05-01', '2025-05-02', '2025-05-03',
    '2025-05-04', '2025-05-05', '2025-05-31', '2025-06-01', '2025-06-02',
    '2025-10-01', '2025-10-02', '2025-10-03', '2025-10-04', '2025-10-05',
    '2025-10-06', '2025-10-07', '2025-10-08', '2026-01-01', '2026-01-02',
    '2026-01-03', '2026-02-16', '2026-02-17', '2026-02-18', '2026-02-19',
    '2026-02-20', '2026-02-21', '2026-02-22', '2026-02-23', '2026-04-04',
    '2026-04-05', '2026-04-06', '2026-05-01', '2026-05-02', '2026-05-03',
    '2026-05-04', '2026-05-05', '2026-06-19', '2026-06-20', '2026-06-21',
    '2026-09-25', '2026-09-26', '2026-09-27', '2026-10-01', '2026-10-02',
    '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07'
])

WORKDAYS_TO_MAKEUP = set([
    '2025-01-26', '2025-02-08', '2025-04-27', '2025-09-28', '2025-10-11',
    '2026-02-08', '2026-02-28', '2026-04-26', '2026-05-09', '2026-09-20', '2026-10-10'
])

# 🌪️ 台风异常影响日期列表（7月24日~26日）
TYPHOON_DATES = ['2026-07-24', '2026-07-25', '2026-07-26']


# ==================== 2. 数据读取与台风天平滑清洗 ====================
def preprocess_data(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"未找到输入数据文件: {file_path}")

    print(f"📂 正在读取数据文件: {file_path} ...")
    df = pd.read_excel(file_path)
    df.columns = df.columns.astype(str).str.strip()

    time_col = None
    for col in ['预计送达时间', '预计送达大升', '预计送达大时', '预计送达小时', '时段', '预计送达时段', "预计送达开始时段"]:
        if col in df.columns:
            time_col = col
            break

    if time_col is None:
        raise KeyError(f"❌ 未找到时段列！")

    df['hour_start'] = df[time_col].apply(lambda x: str(x).split('-')[0].zfill(5))
    df['ds'] = pd.to_datetime(df['日期'].astype(str) + ' ' + df['hour_start'])
    df['date_str'] = df['ds'].dt.strftime('%Y-%m-%d')
    df['hour'] = df['ds'].dt.hour
    df['dayofweek'] = df['ds'].dt.dayofweek

    if '订单数' not in df.columns:
        raise KeyError(f"❌ 未找到订单数列！")

    df['y'] = df['订单数'].astype(float)
    df = df.sort_values('ds').reset_index(drop=True)

    # ----------------- 🌪️ 台风天极端爆单清洗逻辑 -----------------
    print("🧹 正在对 2026-07-24 ~ 07-26 台风天异常数据进行平滑降噪...")
    normal_df = df[~df['date_str'].isin(TYPHOON_DATES)]
    normal_stats = normal_df.groupby(['dayofweek', 'hour'])['y'].median().reset_index().rename(
        columns={'y': 'y_normal_baseline'}
    )
    df = pd.merge(df, normal_stats, on=['dayofweek', 'hour'], how='left')

    typhoon_mask = df['date_str'].isin(TYPHOON_DATES)
    typhoon_count = typhoon_mask.sum()
    df.loc[typhoon_mask, 'y'] = df.loc[typhoon_mask, 'y_normal_baseline']

    print(f"✅ 台风天数据清洗完成！已成功平滑 {typhoon_count} 条极端爆单记录。")
    return df


# ==================== 3. 多维爆单特征工程构建 (XGBoost 专属) ====================
def build_advanced_features(df):
    print("🛠️ 正在构建【周一补货+节假日+调休+周五/周末爆单】精准特征矩阵...")
    df_feat = df.copy()

    df_feat['month'] = df_feat['ds'].dt.month
    df_feat['is_monday'] = (df_feat['dayofweek'] == 0).astype(int)

    df_feat['is_official_holiday'] = df_feat['date_str'].isin(HOLIDAYS_2025_2026).astype(int)
    df_feat['is_makeup_workday'] = df_feat['date_str'].isin(WORKDAYS_TO_MAKEUP).astype(int)

    df_feat['is_real_off_day'] = (
        (df_feat['is_official_holiday'] == 1) |
        ((df_feat['dayofweek'] >= 5) & (df_feat['is_makeup_workday'] == 0))
    ).astype(int)

    next_day_dates = (df_feat['ds'] + pd.Timedelta(days=1)).dt.strftime('%Y-%m-%d')
    next_day_dow = (df_feat['ds'] + pd.Timedelta(days=1)).dt.dayofweek
    is_next_day_holiday = (
        next_day_dates.isin(HOLIDAYS_2025_2026) |
        ((next_day_dow >= 5) & (~next_day_dates.isin(WORKDAYS_TO_MAKEUP)))
    )

    df_feat['pre_holiday_afternoon_burst'] = (is_next_day_holiday & (df_feat['hour'] >= 15)).astype(int)
    df_feat['is_friday'] = (df_feat['dayofweek'] == 4).astype(int)
    df_feat['fri_afternoon_night_burst'] = ((df_feat['dayofweek'] == 4) & (df_feat['hour'] >= 15)).astype(int)

    df_feat['is_weekend_real'] = ((df_feat['dayofweek'] >= 5) & (df_feat['is_makeup_workday'] == 0)).astype(int)
    df_feat['weekend_all_day_burst'] = ((df_feat['is_weekend_real'] == 1) & (df_feat['hour'] >= 7) & (df_feat['hour'] <= 23)).astype(int)

    df_feat['is_morning_trough'] = df_feat['hour'].isin([7, 8, 9, 10]).astype(int)
    df_feat['is_lunch_peak'] = df_feat['hour'].isin([11, 12, 13]).astype(int)
    df_feat['is_dinner_peak'] = df_feat['hour'].isin([17, 18, 19, 20]).astype(int)

    df_feat['weekend_lunch_burst'] = df_feat['is_weekend_real'] * df_feat['is_lunch_peak']
    df_feat['weekend_dinner_burst'] = df_feat['is_weekend_real'] * df_feat['is_dinner_peak']

    dow_hour_stat = df_feat.groupby(['dayofweek', 'hour'])['y'].agg('median').reset_index().rename(columns={'y': 'hist_dow_hour_median'})
    df_feat = pd.merge(df_feat, dow_hour_stat, on=['dayofweek', 'hour'], how='left')

    df_feat['lag_1d'] = df_feat['y'].shift(24)
    df_feat['lag_7d'] = df_feat['y'].shift(24 * 7)
    df_feat['lag_14d'] = df_feat['y'].shift(24 * 14)
    df_feat['diff_lag_1d'] = df_feat['lag_1d'] - df_feat['lag_7d']
    df_feat['rolling_3d_mean'] = df_feat['y'].shift(24).rolling(window=3).mean()
    df_feat['rolling_7d_mean'] = df_feat['y'].shift(24).rolling(window=7).mean()

    return df_feat.sort_values('ds').reset_index(drop=True)


# ==================== 4. 混合模型 (XGBoost 早上 + Prophet 其他时段) ====================
def train_hybrid_model(df, test_days=7):
    test_hours = test_days * 24
    train_df = df.iloc[:-test_hours].copy()
    test_df = df.iloc[-test_hours:].copy()

    print("\n" + "=" * 60)
    print(f"🚀 开始训练【混合模型架构】：早晨段 (07-10点) -> XGBoost | 其他时段 (11-23点) -> Prophet")
    print("=" * 60)

    # ---------------- 1) XGBoost 分支 (预测 07:00 ~ 10:00 早晨段) ----------------
    feature_cols = [
        'hour', 'dayofweek', 'month', 'is_monday',
        'is_official_holiday', 'is_makeup_workday', 'is_real_off_day',
        'pre_holiday_afternoon_burst', 'is_friday', 'fri_afternoon_night_burst',
        'is_weekend_real', 'weekend_all_day_burst',
        'is_morning_trough', 'is_lunch_peak', 'is_dinner_peak',
        'weekend_lunch_burst', 'weekend_dinner_burst',
        'hist_dow_hour_median',
        'lag_1d', 'lag_7d', 'lag_14d', 'diff_lag_1d',
        'rolling_3d_mean', 'rolling_7d_mean'
    ]

    train_clean = train_df.dropna(subset=feature_cols + ['y'])
    X_train = train_clean[feature_cols]
    y_train = train_clean['y']
    X_test = test_df[feature_cols].ffill().bfill()

    xgb_model = XGBRegressor(
        n_estimators=325, max_depth=8, learning_rate=0.0357,
        subsample=0.7500, colsample_bytree=0.8000, min_child_weight=3,
        reg_alpha=0.6820, reg_lambda=0.3902, random_state=42, n_jobs=-1
    )
    print("[1/2 XGBoost] 正在拟合早晨段预测模型...")
    xgb_model.fit(X_train, y_train)

    xgb_raw_pred = xgb_model.predict(X_test)

    # 早晨微调字典 (按要求保留 07:00=1.90, 08-10:00=0.85)
    hourly_adjust = test_df['hour'].map({
        7: 1.90, 8: 0.85, 9: 0.85, 10: 0.85
    }).fillna(1.0).values

    xgb_final_pred = np.round(xgb_raw_pred * hourly_adjust)

    # ---------------- 2) Prophet 分支 (预测 11:00 ~ 23:00 其他时段) ----------------
    if PROPHET_AVAILABLE:
        print("[2/2 Prophet] 正在拟合其他时段时间序列模型...")
        prophet_train_df = train_df[['ds', 'y']].rename(columns={'ds': 'ds', 'y': 'y'})

        # 构建法定节假日数据框给 Prophet
        holiday_dates = pd.to_datetime(list(HOLIDAYS_2025_2026))
        holidays_df = pd.DataFrame({
            'holiday': 'national_holiday',
            'ds': holiday_dates,
            'lower_window': 0,
            'upper_window': 0,
        })

        prophet_model = Prophet(
            holidays=holidays_df,
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=True,
            changepoint_prior_scale=0.05
        )
        prophet_model.fit(prophet_train_df)

        future = test_df[['ds']].copy()
        prophet_forecast = prophet_model.predict(future)
        prophet_pred = np.round(prophet_forecast['yhat'].values)
    else:
        print("⚠️ 由于未安装 Prophet，其他时段将自动回退使用 XGBoost 预测值。")
        prophet_pred = np.round(xgb_raw_pred)

    # ---------------- 3) 结果拼接融合 (早上使用 XGBoost，其他时间使用 Prophet) ----------------
    results = test_df[['ds', 'hour', 'y']].copy()

    # 判别机制：小时在 [7, 8, 9, 10] 内用 XGBoost，其他时间用 Prophet
    is_morning_mask = results['hour'].isin([7, 8, 9, 10])

    final_hybrid_pred = np.where(is_morning_mask, xgb_final_pred, prophet_pred)
    final_hybrid_pred = np.clip(final_hybrid_pred, 0, None)

    results['pred'] = final_hybrid_pred.astype(int)
    results['diff'] = results['pred'] - results['y']

    # 评估指标计算
    rmse = np.sqrt(mean_squared_error(results['y'], results['pred']))
    mae = mean_absolute_error(results['y'], results['pred'])

    print("\n" + "=" * 60)
    print(f"📊 混合模型 (XGBoost 早上 + Prophet 其他) 评估结果:")
    print(f"  - MAE  (平均绝对误差): {mae:.2f} 单/小时")
    print(f"  - RMSE (均方根误差):   {rmse:.2f} 单/小时")
    print("=" * 60)

    return results


# ==================== 5. 主程序入口 ====================
if __name__ == "__main__":
    # 使用相对路径自动寻找同目录或桌面上的文件，防止 FileNotFoundError 路径报错
    current_dir = os.path.dirname(os.path.abspath(__file__))
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

    # 优先查找当前工作目录或桌面下的输入文件
    input_filename = "深圳1059时段单量合并.xlsx"
    if os.path.exists(os.path.join(current_dir, input_filename)):
        input_file = os.path.join(current_dir, input_filename)
    elif os.path.exists(os.path.join(desktop_path, input_filename)):
        input_file = os.path.join(desktop_path, input_filename)
    else:
        input_file = os.path.join(desktop_path, input_filename) # 默认回退路径

    output_file = os.path.join(desktop_path, "节假日爆单优化预测结果.xlsx")

    try:
        df_raw = preprocess_data(input_file)
        df_features = build_advanced_features(df_raw)
        results = train_hybrid_model(df_features, test_days=60)

        print("\n📋 混合模型预测结果预览:")
        print(results.head(15).to_string(index=False))

        results.drop(columns=['hour']).to_excel(output_file, index=False)
        print(f"\n💾 混合预测 Excel 已成功导出至: {output_file}")

    except Exception as e:
        print(f"\n❌ 程序执行遇到错误: {e}")
