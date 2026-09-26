from utils_00 import *

#%%
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset

import pytorch_lightning as pl
from pytorch_lightning.callbacks.early_stopping import EarlyStopping
from pytorch_lightning.callbacks import ModelCheckpoint

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, KFold

import itertools
import optuna
from optuna.samplers import TPESampler
import gc  # 垃圾回收模块 Garbage Collector
import warnings
warnings.filterwarnings('ignore')

print(torch.__version__) # 打印 PyTorch 版本
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu') # 检查是否有 GPU 可用
print(f"使用设备: {device}")

# 设置随机种子以确保可重复性
random_state = 42
torch.manual_seed(random_state) # PyTorch 随机种子 manual_seed()
np.random.seed(random_state)  # Numpy 随机种子 random.seed()
#%%
class VAE(pl.LightningModule): # VAE 实现
    def __init__(
        self,
        input_dim = 408,
        latent_dim = 80,
        hidden_dims = [256, 128],
        beta = 0.0001,
        dropout_rate = 0.1,
        lr = 1e-5,
        weight_decay = 1e-5
    ):

        super().__init__()
        self.input_dim = input_dim         # input dimension 输入数据的特征维度
        self.latent_dim = latent_dim       # latent dimension 潜在空间的维度
        self.hidden_dims = hidden_dims     # hidden dimension list 隐藏层维度列表
        self.beta = beta                   # β-VAE 损失函数中 KL散度 的权重值
        self.dropout_rate = dropout_rate   # 训练时按比例 dropout_rate 随机关闭神经元
        self.lr = lr                       # 学习率，控制参数更新步长
        self.weight_decay = weight_decay   # 权重衰减，即 L2 正则化项，用于防止过拟合


        # 编码器
        # nn 为 PyTorch 神经网络模块 neural network module
        encoder_layers = []    # 编码器层列表
        prev_dim = input_dim   # 初始层维度数

        for hidden_dim in hidden_dims: # hidden dimensions 编码器的隐藏层维度
        # hidden_dims 结构：[层1] → [层2] → [层3] → 潜在空间(如：80维)
        # hidden_dim 描述每层维度数

            encoder_layers.extend([
                nn.Linear(prev_dim, hidden_dim),  # 全连接层/线性层 Fully Connected Layer，神经网络的基本构建块，进行线性变换 y = Wx + b
                nn.BatchNorm1d(hidden_dim),       # 一维批量归一化工具，对每个特征维度进行归一化
                nn.ReLU(),                      # 线性整流函数 ReLU(x) = max(0, x)，激活函数，给神经网络添加非线性
                nn.Dropout(dropout_rate)        # 随机失活，训练时随机关闭一些神经元，正则化防止过拟合
            ])
            prev_dim = hidden_dim

        self.encoder = nn.Sequential( * encoder_layers) # 用 * 对 encoder_layers 解包
        # 顺序容器 Sequential，把多个层按顺序连接起来


        # 潜在空间
        # 普通自编码器：输入 → 编码器 → 压缩表示（固定值） → 解码器 → 重建
        # 变分自编码器：输入 → 编码器 → 概率分布（均值和方差） → 采样 → 解码器 → 重建
        self.fc_mu = nn.Linear(prev_dim, latent_dim)      # fully_connected_mean 全连接均值层 μ
        self.fc_logvar = nn.Linear(prev_dim, latent_dim)  # fully_connected_log_variance 全连接对数方差层 log(σ²)

        # 解码器
        # 将潜在空间的编码"翻译"回原始数据：低维潜在编码 → 逐渐扩展 → 原始维度
        decoder_layers = []               # 解码器层列表
        decoder_dims = hidden_dims[::-1]  # 解码器的隐藏层维度，由编码层隐藏维度 [::-1] 反转得到
        prev_dim = latent_dim             # 初始层维度数

        for hidden_dim in decoder_dims: # hidden dimensions 解码器的隐藏层维度
        # hidden_dims 结构：潜在空间 (如：80维) → [层1] → [层2] → …… → [层n] (decoder_dims 不包括最后的原始维度输出)
        # hidden_dim 描述每层维度数

            decoder_layers.extend([
                nn.Linear(prev_dim, hidden_dim),  # 全连接层，进行线性变换
                nn.BatchNorm1d(hidden_dim),       # 批量归一化层
                nn.ReLU(),                     # 线性整流函数 ReLU(x) = max(0, x)
                nn.Dropout(dropout_rate),      # 随机按比例 n(如 n = 0.1) "关闭" 神经元
            ])
            prev_dim = hidden_dim # 更新上一个层的维度

        decoder_layers.append(nn.Linear(prev_dim, input_dim)) # 最后一层，没有激活函数，按原始输入维度输出
        self.decoder = nn.Sequential( * decoder_layers) # 用 * 对 encoder_layers 解包
        # 顺序容器 Sequential，把多个层按顺序连接起来


    def encode(self, x): # 将输入 x 编码成潜在空间的均值和对数方差
    # 输入数据 x: 输入数据形状为 [batch_size, input_dim]
    # batch_size: 批次中的样本数量
    # input_dim: 每个样本的特征数量

        # 隐藏表示 hidden representation，数据输入经过编码器（encoder）网络后得到的特征向量
        h = self.encoder(x)  # self.encoder 编码器网络（前文定义的 nn.Sequential）将输入 x 通过编码器网络进行变换

        mu = self.fc_mu(h)          # 将 hidden representation 通过全连接层得到均值 mu
        logvar = self.fc_logvar(h)  # 将 hidden representation 通过全连接层得到对数方差 logvar
        return mu, logvar


    def reparameterize(self, mu, logvar):  # 使用重参数化技巧从潜在分布中采样
        std = torch.exp(0.5 * logvar) # 由于 logvar = log(σ²) 故 σ = sqrt(σ²) = sqrt(exp(logvar)) = exp(0.5 * logvar)
        eps = torch.randn_like(std)   # 随机噪声 ε eps(epsilon) 为从正态分布 N(0,1) 中采样的随机数
        # torch.randn_like(std) 用于从 N(0,1) 生成与 std 张量形状完全相同的随机张量，并保持数据类型一致性如 float32

        z = mu + eps * std # 计算从潜在分布中采样的点 z = μ + ε·σ
        return z # z："潜在变量" latent variable


    def decode(self, z): # 将潜在变量映射回原始数据空间，潜在变量 z ~ N(μ,σ²) 服从学到的分布
        return self.decoder(z) # 将潜在变量映射回原始数据空间，输出重建数据 recon_x


    def forward(self, x):
        mu, logvar = self.encode(x)  # 编码将数据压缩为概率分布参数 x → h → fc_mu/fc_logvar → mu, logvar
        z = self.reparameterize(mu, logvar)  # 从分布中随机采样 mu, logvar → 重参数化 → z
        recon_x = self.decode(z) # 解码将潜在点重建为原始数据 z → decoder → recon_x
        return recon_x, mu, logvar  # 返回重建数据，以及均值与对数方差用于 KL 散度


    def loss_function(self, recon_x, x, mu, logvar):
        recon_loss = F.mse_loss(recon_x, x, reduction = 'mean')   # 计算重建损失 reconstruction loss
        # 损失函数使用 均方误差损失（Mean Squared Error Loss） MSE = 1/N Σ (recon_x_i - x_i)²

        kl_loss = -0.5 * torch.mean(1 + logvar - mu.pow(2) - logvar.exp())  # KL散度损失（KL divergence loss）
        # Kullback-Leibler 散度（KL Divergence） KL(N(μ,σ²) || N(0,1)) = -0.5 * Σ(1 + log(σ²) - μ² - σ²)
        # 展开为 1 + logvar - mu.pow(2) - logvar.exp() = 1 + log(σ²) - μ² - σ²

        total_loss = recon_loss + self.beta * kl_loss # # 总损失函数
        # 因为 KL 损失通常比重建损失大很多，故降低 KL 散度的权重避免其主导训练
        return {
            'loss': total_loss,     # 总损失，用于反向传播
            'recon_loss': recon_loss, # 重建损失，用于监控
            'kl_loss': kl_loss  # KL损失，用于监控
        }


    def training_step(self, batch, batch_idx):
        x = batch[0]   # 提取输入数据
        recon_x, mu, logvar = self(x)  # 前向传播：将输入x传递给模型，即调用 self(x)，得到重建数据 recon_x、均值和对数方差
        loss_dict = self.loss_function(recon_x, x, mu, logvar)  # 计算损失：通过 loss_function 计算重建损失、KL损失和总损失，并返回一个字典

        self.log('train_loss', loss_dict['loss'], prog_bar=True)
        self.log('train_recon_loss', loss_dict['recon_loss'])
        self.log('train_kl_loss', loss_dict['kl_loss'])
        return loss_dict['loss']


    def validation_step(self, batch, batch_idx): # 与 training_step 类似，但是用于验证阶段
    # 在验证阶段，我们不会进行梯度计算和参数更新，只是计算损失并记录
        x = batch[0]
        recon_x, mu, logvar = self(x)
        loss_dict = self.loss_function(recon_x, x, mu, logvar)

        self.log('val_loss', loss_dict['loss'], prog_bar=True)
        self.log('val_recon_loss', loss_dict['recon_loss'])
        self.log('val_kl_loss', loss_dict['kl_loss'])
        return loss_dict['loss']


    def configure_optimizers(self): # PyTorch Lightning 遵循约定优于配置原则
    # 每个 LightningModule 必须有 configure_optimizers 方法，以保证模块完整性，故此处指定 optim.Adam
    # 优化器 optimizer 使用 Adam 优化器

        return torch.optim.Adam(
            self.parameters(),
            lr = self.lr,   # 学习率，控制参数更新步长
            weight_decay = self.weight_decay  # 权重衰减，即 L2 正则化项，用于防止过拟合
        )


    def encode_latent(self, x):  # 提取潜在表示
        with torch.no_grad(): # 禁用梯度计算，使用 torch.no_grad() 上下文管理器避免在此过程中计算和存储梯度，节省内存和计算资源
            mu, _ = self.encode(x) # 调用 encode 函数得到均值和对数方差，但这里只返回均值作为潜在表示
            return mu

#%%
def train_vae_with_cv(
    train_data,
    n_splits,
    param_grid,
    callbacks,
    max_combinations,
    random_state = 42
):

    """
    使用 K 折交叉验证训练 VAE 模型的贝叶斯优化函数

    参数:
    train_data: 训练数据集
    n_splits: K 折交叉验证的折数
    param_grid: 超参数搜索空间
    max_combinations: 最大试验次数
    random_state: 随机种子

    返回:
    all_results: 所有超参数组合的结果
    best_params: 最佳超参数
    best_score: 最佳验证损失
    """

    print(f"使用 Optuna 进行贝叶斯优化")
    print(f"最大试验次数: {max_combinations}")

    # 定义 n_splits 折切割器
    kf = KFold(
        n_splits = n_splits,
        random_state = random_state,
        shuffle = True,
    )

    all_results = []  # 存储所有结果
    def objective(trial):  # 定义目标函数

        # hidden_dims: 分类变量，从预定义的选项中选择
        # 注意：Optuna的categorical 变量需要列表中的元素是可哈希的，列表不可哈希，所以转换为元组
        hidden_dims_options = [tuple(dims) for dims in param_grid['hidden_dims']]
        hidden_dims_selected = trial.suggest_categorical('hidden_dims', hidden_dims_options)

        params = {}  # 从搜索空间中采样参数
        params['hidden_dims'] = list(hidden_dims_selected)  # 转换回列表

        latent_dim_info = param_grid['latent_dim']  # latent_dim: 整数参数
        params['latent_dim'] = trial.suggest_int('latent_dim', latent_dim_info[0], latent_dim_info[1])

        beta_info = param_grid['beta']  # beta: 对数尺度浮点数
        params['beta'] = trial.suggest_float('beta', beta_info[0], beta_info[1], log = True)

        dropout_info = param_grid['dropout_rate']  # dropout_rate: 线性尺度浮点数
        params['dropout_rate'] = trial.suggest_float('dropout_rate', dropout_info[0], dropout_info[1])

        lr_info = param_grid['lr']  # lr: 对数尺度浮点数
        params['lr'] = trial.suggest_float('lr', lr_info[0], lr_info[1], log = True)

        weight_decay_info = param_grid['weight_decay']  # weight_decay: 对数尺度浮点数
        params['weight_decay'] = trial.suggest_float('weight_decay', weight_decay_info[0], weight_decay_info[1], log = True)

        batch_info = param_grid['batch_size']  # batch_size: 整数参数
        params['batch_size'] = trial.suggest_int('batch_size', batch_info[0], batch_info[1])

        print(f"\n试验 {trial.number+1}/{max_combinations}")
        print(f"参数: {params}")

        # 执行 K 折交叉验证
        fold_results = []
        for fold_idx, (train_idx, val_idx) in enumerate(kf.split(train_data)):
            # 标准化数据
            scaler = StandardScaler()
            X_train_fold = scaler.fit_transform(train_data[train_idx])
            X_val_fold = scaler.transform(train_data[val_idx])

            # 转换为 PyTorch 张量
            device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
            X_train_tensor = torch.tensor(X_train_fold, dtype = torch.float32, device = torch.device('cpu'))
            X_val_tensor = torch.tensor(X_val_fold, dtype = torch.float32, device = torch.device('cpu'))

            # 创建数据集和数据加载器
            train_dataset = TensorDataset(X_train_tensor)
            val_dataset = TensorDataset(X_val_tensor)

            train_loader = DataLoader(train_dataset, batch_size = params['batch_size'], shuffle = True, num_workers = 0, pin_memory = True, drop_last = True)
            val_loader = DataLoader(val_dataset, batch_size = params['batch_size'], shuffle = False, num_workers = 0, pin_memory = True, drop_last = True)

            # 初始化 VAE
            vae = VAE(
                input_dim = train_data.shape[1],
                latent_dim = params['latent_dim'],
                hidden_dims = params['hidden_dims'],
                beta = params['beta'],
                dropout_rate = params['dropout_rate'],
                lr = params['lr'],
                weight_decay = params['weight_decay']
            )

            # 设置回调函数
            if callbacks is not None:
                current_callbacks = callbacks
            else:
                current_callbacks = [
                    EarlyStopping(
                        monitor = 'val_loss',
                        patience = 6,
                        mode = 'min',
                        verbose = False
                    ),
                ]

            # 创建训练器
            trainer = pl.Trainer(
                max_epochs = 30,
                callbacks = current_callbacks,
                enable_progress_bar = False,
                enable_model_summary = False,
                logger = False,
                accelerator = ('gpu' if torch.cuda.is_available() else 'cpu'),
                devices = 'auto',
            )

            # 训练模型
            trainer.fit(vae, train_loader, val_loader)

            # 获取最佳验证损失
            best_val_loss = trainer.callback_metrics.get('val_loss')
            if best_val_loss is None:
                val_result = trainer.validate(vae, val_loader)
                best_val_loss = val_result[0]['val_loss']

            fold_results.append(float(best_val_loss))

        # 计算平均验证损失
        avg_val_loss = np.mean(fold_results)
        std_val_loss = np.std(fold_results)

        # 记录结果
        result = {
            'param_id': trial.number,
            'params': params.copy(),
            'avg_val_loss': avg_val_loss,
            'std_val_loss': std_val_loss,
            'fold_results': [{'fold': i, 'val_loss': fold_loss} for i, fold_loss in enumerate(fold_results)]
        }
        all_results.append(result)

        print(f"平均验证损失: {avg_val_loss:.6f}")

        return avg_val_loss

    # 创建Optuna研究
    study = optuna.create_study(
        direction = 'minimize',
        sampler = TPESampler(seed = random_state),
        study_name = 'vae_hyperparameter_optimization'
    )

    # 运行优化
    study.optimize(
        objective,
        n_trials = max_combinations,
        show_progress_bar = True
    )

    # 获取最佳结果
    best_params = study.best_params
    best_score = study.best_value

    print(f"\n优化完成!")
    print(f"最佳参数: {best_params}")
    print(f"最佳验证损失: {best_score:.6f}")

    # 确保all_results中的顺序与试验编号一致
    all_results.sort(key = lambda x: x['param_id'])

    return all_results, best_params, best_score

#%%
def train_final_VAE_model(
    train_data,
    test_data,
    best_params
):
    """ 基于最优超参数，使用全量数据训练最终 VAE 模型
    Parameters
    train_data : pd.DataFrame or np.ndarray 训练集特征矩阵，用于拟合 Scaler 和训练模型
    test_data : pd.DataFrame or np.ndarray 测试集特征矩阵，仅用于 Scaler 变换保持尺度一致
    best_params : dict 包含最优超参数的字典（hidden_dims, latent_dim, beta 等）

    Returns: dict 包含训练好的模型、Scaler、数据加载器等关键对象 """

    scaler = StandardScaler()   # 只对训练集进行 fit，然后 transform 训练集和验证集
    X_train = scaler.fit_transform( train_data )   # 训练集标准化
    X_test = scaler.transform( test_data )   # 验证集使用训练集的 scaler 进行 transform

    # 转换为 PyTorch 张量 Tensor
    # torch.HalfTensor 16位浮点、FloatTensor 32位浮点、DoubleTensor 64位浮点
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu') # 创建数据加载器，用于将数据载入 gpu
    X_train_tensor = torch.tensor(X_train, dtype = torch.float32, device = torch.device('cpu'))
    X_test_tensor = torch.tensor(X_test, dtype = torch.float32, device = torch.device('cpu')) # 由于数据在预处理过程中 StandardScaler 完成 z-score 标准化，不考虑数值溢出 32 位浮点
    # 将数据载入到 cpu 而不是 gpu，以使用 pin_memory
    # pin_memory 是一种 CPU 内存优化技术，它通过创建页锁定内存来加速 CPU 到 GPU 的数据传输。优化数据供给管道，使 GPU 不用等待数据，实现计算-传输重叠
    # 直接载入数据到 gpu 会占用宝贵的显存，且无法实现这种流水线优化

    # 使用 TensorDataset 将多个张量 Tensors 打包成一个数据集对象 dataset
    train_dataset = TensorDataset( X_train_tensor )
    test_dataset = TensorDataset( X_test_tensor )

    # 创建数据加载器 DataLoader 将数据集分成小批次动态加载，避免一次性加载所有数据到内存
    # 创建 DataLoader 时，并没有立即加载数据，我们只是传递了一个数据集和一些参数
    # DataLoader 迭代器会根据这些参数，在每次迭代时从数据集中提取一个批次的数据，这种惰性加载（lazy loading）的方式可以节省内存
    train_loader = DataLoader(train_dataset, batch_size = best_params['batch_size'], shuffle = True, num_workers = 0, pin_memory = True, drop_last = True)
    test_loader = DataLoader(
        test_dataset,               # 告诉加载器数据在哪
        batch_size = best_params['batch_size'],   # 每个批次 batch_size 个样本；即告诉加载器如何分批
        shuffle = False,    # shuffle = True 时每个 epoch 打乱数据顺序 (注：Epoch 是训练中的一个完整周期，表示模型已经看过了整个训练数据集一次)
        num_workers = 0,    # 告诉加载器用多少进程，Windows 上 num_workers 即工作进程设为 0 (单进程)
        pin_memory = True,  # 启用内存固定 pin_memory
        drop_last = True    # 丢弃最后一个不完整的批次
    )

    vae_model = VAE(  # 创建模型
        input_dim = train_data.shape[1],
        latent_dim = best_params['latent_dim'],
        hidden_dims = best_params['hidden_dims'],
        beta = best_params['beta'],
        dropout_rate = best_params['dropout_rate'],
        lr = best_params['lr'],
        weight_decay = best_params['weight_decay']
    )

    # 创建 PyTorch Lightning 训练器
    trainer = pl.Trainer(
        max_epochs = 30,               # 最大训练轮数
        enable_progress_bar = False,   # 启用进度条显示
        enable_model_summary = False,  # 禁用模型结构摘要
        logger = False,                # 禁用日志以简化输出
        accelerator = (                # 使用 CPU
            'gpu' if torch.cuda.is_available() else 'cpu'
        ),
        devices = 'auto',
    )

    # 训练模型
    trainer.fit(vae_model, train_loader)
    return vae_model, scaler

#%%
def save_lightning_vae(
    vae_model, vae_class, scaler,
    model_filepath = None, class_filepath = None
):  # VAE 模型与 VAE 类定义分开保存到两个文件
    if model_filepath is None or class_filepath is None: return
    params_dict = {}
    for name in ['input_dim', 'latent_dim', 'hidden_dims', 'beta', 'dropout_rate', 'lr', 'weight_decay']:
        params_dict[name] = getattr(vae_model, name)

    model_save_dict = {
        'model_state_dict': vae_model.state_dict(),     # 模型权重参数
        'params_dict': params_dict,                     # 超参数组合
        'scaler': scaler,                               # 数据标准化器
    }
    torch.save( model_save_dict, model_filepath )   # 使用 torch 保存模型权重
    print( f"模型权重已保存到: {model_filepath}" )
    print( f" - 权重参数: {len( model_save_dict['model_state_dict'] )} 个张量" )

    class_save_dict = {'model_class': vae_class, }  # 类对象本身
    with open(class_filepath, 'wb') as file:
        import dill
        dill.dump(class_save_dict, file)  # 使用 dill 保存类定义
    print( f"类定义已保存到: {class_filepath}" )
    print( f" - 类名: {vae_class.__name__}" )

#%%
def load_lightning_vae(
    model_filepath, class_filepath, device = 'cpu'
):  # 从分开的文件加载 VAE 模型
    if model_filepath is None or class_filepath is None: return

    print(f"正在加载模型...")
    print(f" - 模型权重文件: {model_filepath}")
    print(f" - 类定义文件: {class_filepath}")

    with open(class_filepath, 'rb') as file:
        import dill
        class_data = dill.load(file)
    VAE_Class = class_data['model_class']
    model_data = torch.load(model_filepath, map_location = device, weights_only = False)

    model = VAE_Class( ** model_data['params_dict'])  # 使用 params_dict 创建 VAE 模型实例，同时也定义模型结构如 隐藏层、潜在空间维度 等
    model.load_state_dict(model_data['model_state_dict'])
    model.to(device)
    model.eval()
    scaler = model_data['scaler']
    return model, scaler