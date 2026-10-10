from utils_00 import *

#%%
import lightgbm as lgb
import optuna
from sklearn.preprocessing import RobustScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error

#%%
def tune_hyperparameters(
    X_train, y_train,
    n_trials = 50,
    n_splits = 5,
    param_space = None
):
    """
    param_space = {

        'learning_rate': (0.0005, 0.5, 'log'),      # 学习率 learning_rate，对数尺度采样
        'num_leaves': (10, 500, 'int'),             # 叶子节点数量 num_leaves，控制模型复杂度
        'max_depth': (2, 100, 'int'),               # 树的最大深度 max_depth，防止过拟合
        'min_child_samples': (5, 200, 'int'),       # min_child_samples 叶子节点最少样本数
        'subsample': (0.3, 1.0, 'float'),           # subsample 训练样本采样比例
        'colsample_bytree': (0.6, 1.0, 'float'),    # colsample_bytree 特征采样比例

        'reg_alpha': (1e-8, 20, 'log'),         # L1 reg_alpha 正则化系数
        'reg_lambda': (1e-8, 20, 'log'),        # L2 reg_lambda 正则化系数

        'max_bin': (50, 2000, 'int'),       # max_bin 直方图装箱数
        'min_data_in_bin': (1, 50, 'int'),  # min_data_in_bin 每箱最少数据量
    }
    """
    print(f"训练集大小: {X_train.shape}")

    if param_space is None: # 确保函数接收超参数搜索空间
        print('param_space is None')
        return


    scaler = RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train) # 只对训练集进行缩放

    validation_results = { # 存储验证结果
        'predictions': pd.DataFrame(index = X_train.index, columns = ['prediction']),  # 存储预测值的 DataFrame
        'best_iterations': [],  # 存储每折最佳迭代次数的列表
        'all_scores': []    # 存储所有分数的列表
    }
    validation_results['predictions']['true'] = y_train # 将目标变量命名为 true 并入 predictions 表格

    def objective(trial): # Optuna 优化目标函数
    # 在 Optuna 中，trial 对象是一个 Trial 类的实例，它代表了单次超参数组合的评估。trial 对象的主要作用是为目标函数提供超参数采样的接口，并记录该次试验（trial）的相关信息。
    # 主要方法和属性：
    # 参数采样方法：
    # suggest_categorical(name, choices)：从分类分布中采样。
    # suggest_float(name, low, high, step=None, log=False)：从均匀分布或对数均匀分布中采样浮点数。
    # suggest_int(name, low, high, step=1, log=False)：从整数均匀分布或对数均匀分布中采样整数。
    # 属性：
    # number：当前 trial 的序号。
    # params：当前 trial 采样的超参数字典。
    # user_attrs：用户自定义属性，可以用来存储额外的信息。
    # system_attrs：系统属性，由Optuna内部使用。
    # 报告方法：
    # set_user_attr(key, value)：设置用户自定义属性。
    # report(value, step)：在迭代优化中报告中间目标值（用于Pruner）。
    # 在代码中，我们使用 trial 的 suggest_float、suggest_int 等方法来采样超参数。

        params = {
            'objective': 'regression',          # 任务类型 - 回归问题
            'metric': 'mse',                    # 评估指标
            'boosting_type': 'gbdt',            # 提升算法类型 - 梯度提升决策树
            'random_state': 42,                 # 随机种子，保证结果可复现
            'verbosity': -1                     # 日志输出级别：-1表示静默模式
        }

        for param_name, param_range in param_space.items():  # 从参数空间采样
            low, high, param_type = param_range  # 从传入的超参数检索空间 param_space 解包得到各个维度的检索范围

            if param_type == 'float':
                params[param_name] = trial.suggest_float(param_name, low, high)

            elif param_type == 'int':
                params[param_name] = trial.suggest_int(param_name, low, high)

            elif param_type == 'log':
                params[param_name] = trial.suggest_float(param_name, low, high, log = True)


            mae_scores = []              # 存储每折的 MAE 分数
            current_iterations = []      # 存储每折的最佳迭代次数
            current_predictions = []     # 存储最后一次 trial 的预测结果

        # 时间序列交叉验证
        tss = TimeSeriesSplit(n_splits = n_splits)
        for fold, (train_idx, val_idx) in enumerate(tss.split(X_train_scaled)):
        # fold: 当前折的索引
        # train_idx, val_idx: 训练集和验证集的索引

            # 划分训练/验证集
            X_fold_train = X_train_scaled[train_idx]
            y_fold_train = y_train.iloc[train_idx]
            X_fold_val = X_train_scaled[val_idx]
            y_fold_val = y_train.iloc[val_idx]

            # 创建数据集
            train_set = lgb.Dataset(X_fold_train, label = y_fold_train)
            val_set = lgb.Dataset(X_fold_val, label = y_fold_val, reference = train_set) # reference = train_set 参考数据集
            # 特征一致性，确保验证集与训练集使用相同的特征名称和类型
            # 类别编码，对于分类特征，使用相同的类别到整数的映射
            # 特征重要性，保持特征索引一致


            model = lgb.train(
                params,    # 模型参数组合
                train_set,    # 训练数据
                num_boost_round = 1000,   # 迭代轮数
                valid_sets = [val_set],  # 验证数据集
                callbacks = [
                    lgb.early_stopping(stopping_rounds = 100, verbose = False),
                    lgb.log_evaluation(period = 100, show_stdv = False),  # 每 100 次迭代，输出日志到控制台
                ]  # 回调函数
            )

            val_preds = model.predict(X_fold_val)   # 拟合该折的验证集
            mae = mean_absolute_error(y_fold_val, val_preds)  # 评估模型性能
            mae_scores.append(mae)

            if hasattr(model, 'best_iteration'):  # 记录最佳迭代次数
                current_iterations.append(model.best_iteration)
            else:
                current_iterations.append(1000)  # 如果没有早停，使用最大迭代次数

            # 如果是最后一次 trial，保存预测结果
            if trial.number == n_trials - 1:
                for i, idx in enumerate(val_idx):
                    current_predictions.append((idx, val_preds[i]))

        if trial.number == n_trials - 1 and current_predictions:  # 最后一次 trial 保存所有预测结果
            for idx, pred in current_predictions:
                validation_results['predictions'].iloc[idx, validation_results['predictions'].columns.get_loc('prediction')] = pred

        validation_results['best_iterations'].extend(current_iterations)  # 记录本次 trial 的迭代次数
        return np.mean(mae_scores)  # 返回平均 MAE 作为优化目标

    # 运行 Optuna 优化
    study = optuna.create_study(direction = 'minimize')
    study.optimize(objective, n_trials = n_trials, show_progress_bar = True)

    # 保存早停迭代次数
    best_iterations = validation_results['best_iterations']

    # 清理验证集预测结果
    validation_results['predictions'] = validation_results['predictions'].dropna(
        subset = ['prediction']
    )

    return {
        'study': study,                     # 完整的 Optuna 对象
        'best_params': study.best_params,   # 最佳超参数组合
        'best_value': study.best_value,     # 最佳损失函数得分
        'best_iterations': best_iterations,   # 平均最佳迭代次数
        'validation_results': validation_results,   # 验证结果
    }

#%%
def train_final_model(
    X_train, y_train,
    X_test, y_test,
    best_params,
    best_iteration,
):
    """
    Args:

        X_train (pd.DataFrame): 训练集特征
        y_train (pd.Series): 训练集目标
        X_test (pd.DataFrame): 测试集特征
        y_test (pd.Series): 测试集目标

        best_params (dict): 最佳超参数组合
        best_iteration (int): 平均最佳迭代次数
        scaler (object, optional): 预训练的缩放器

    Returns:  dict: 训练好的模型及相关结果
    """
    # 数据缩放

    scaler = RobustScaler()
    X_train_scaled = scaler.fit_transform(X_train) # 只对训练集进行缩放
    X_test_scaled = scaler.transform(X_test)

    # 准备 LightGBM 参数
    model_params = {
        'objective': 'regression',
        'metric': 'mse',
        'boosting_type': 'gbdt',
        'verbosity': -1,
        'random_state': 42
    }
    model_params.update(best_params)

    # 创建数据集并训练最终模型
    train_set = lgb.Dataset(X_train_scaled, label = y_train)
    final_model = lgb.train(
        model_params,
        train_set,
        num_boost_round = int(best_iteration * 1.1),
        callbacks = [lgb.log_evaluation(period = 100)]
    )

    # 测试集预测
    test_preds = final_model.predict(X_test_scaled)
    test_predictions = pd.DataFrame({
        'true': y_test.values,
        'prediction': test_preds
    })
    if hasattr(y_test, 'index'):
        test_predictions.index = y_test.index

    # 特征重要性
    feature_names = X_train.columns if hasattr(X_train, 'columns') else [
        f'feature_{i}' for i in range(X_train.shape[1])
    ]
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': final_model.feature_importance()
    }).sort_values('importance', ascending = False)

    return {
        'model': final_model,
        'scaler': scaler,
        'test_predictions': test_predictions,
        'feature_importance': feature_importance,
        'model_params': model_params,
        'X_train_scaled': X_train_scaled,
        'X_test_scaled': X_test_scaled,
        'y_train': y_train,
        'y_test': y_test
    }

#%%