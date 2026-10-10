# 项目环境

本地开发、采集、研究、Notebook 和 alipai 入口统一使用 `latitude_env_v2`：Python 3.13.15、NumPy 2.4.3、alipai 0.4.13。解释器为 `E:\anaconda3\envs\latitude_env_v2\python.exe`；IDE 选择该解释器，Notebook 选择 `Python (latitude_env_v2)`。v1（Python 3.11.15）保留作回退副本，不作日常切换。历史输出及其原环境指纹保持原样；切换解释器不代表历史成果已在新环境重新验收，成果仍按所属项目契约验收。

新机器在仓库根目录重建：

```powershell
conda create -n latitude_env_v2 python=3.13.15 pip
conda activate latitude_env_v2
python -m pip install -r environment/requirements.txt
python -m pip install --no-deps alipai==0.4.13
python -m ipykernel install --user --name latitude_env_v2 --display-name "Python (latitude_env_v2)"
python -m pip check
```

PyCharm 托管 Jupyter 可显式加载项目内的 [jupyter_server_config.py](jupyter_server_config.py)。脚本根据自身位置设置 `ServerApp.root_dir`，使新内核的工作目录位于项目内；共享 Python 环境无需安装项目级配置。在 Notebook 工具栏的服务器菜单中打开“配置 Jupyter 服务器”，为本项目选择或新建托管连接，关闭自动检测执行模式并选择 Jupyter 服务器模式，在命令行参数中设置：

```text
--no-browser --allow-root --config="E:/Latitude_Analytics_v2/environment/jupyter_server_config.py"
```

将此连接用于本项目的 Notebook。配置只作用于显式传入该参数的服务器；项目迁移时只需更新参数中的配置路径。更改后停止并重新启动该 Jupyter 服务器，仅重启内核不会重新加载服务器配置。从终端启动时可在 `python -m jupyterlab` 后传入同样的参数。Notebook 的项目根定位仍按[环境模板](../.env.template)从工作目录向上查找。

[requirements.txt](requirements.txt) 先安装核心库和 SDK 所需依赖，再单独安装 alipai。这是已验证路径的依赖声明例外：官方包要求 `numpy<2`，不能与本项目 NumPy 2 放入同一个普通解析清单。保留官方包及元数据，不降级 NumPy；`pip check` 仍会报告这一条冲突，其他告警须另查。`--no-deps` 只跳过解析，不代表全面兼容。GPU 依赖按清单的可选说明另配。

[环境重建验收](rebuild_20261001/migration_result.json)支持核心库、Schema 往返和采集运维检查；[alipai 实测](alipai/results/20261002T174304Z-7f65e796/REPORT.txt)支持 CPU 提交、输入输出、函数组合、双机分片、checkpoint、失败识别与停止。正式安装已核对同版 SDK 源码、依赖差异及干净内核，并只读查询成功任务；已有依赖未变更。这些微型验证不覆盖完整研究或所有 SDK 接口。

云端容器单独管理 Python 和依赖，已验收的历史 CPU 镜像为 Python 3.9.5；收益／波动率实验已选定官方 Python 3.13.9 镜像；首次创建请求因 SDK 默认会话使用 VPC 端点而 TLS 失败，尚待修正后真实任务验收。SDK 问题、云端依赖方式及未验证范围见 [alipai 说明](alipai/README.md)。

本目录收纳全项目环境的安装说明、依赖清单、验证代码与证据，继承[根规则](../AGENTS.md)。`rebuild_20261001/` 和 `alipai/results/` 保留历史原文与原路径，不能当作当前安装清单；新验证另建批次。后续 CUDA 配置与验证归同级 `cuda/`，实际开展时再建立。
