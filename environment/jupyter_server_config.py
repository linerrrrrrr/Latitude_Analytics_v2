"""本项目的 Jupyter Server 配置，将服务器根目录设为仓库根目录。

由 Jupyter 启动时通过 --config 参数加载。

命令行示例（先激活 latitude_env_v2 环境）：
    python -m jupyterlab --config="E:/Latitude_Analytics_v2/environment/jupyter_server_config.py"

PyCharm 托管服务器：
    将上述 --config 参数填入 Jupyter 服务器的启动参数。
"""

from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
project_markers = (".git", ".env", "config/settings.py")
if not all((project_root / marker).exists() for marker in project_markers):
    raise RuntimeError(f"Jupyter 配置所在项目缺少根目录标记：{project_root}")

c = get_config()
c.ServerApp.root_dir = str(project_root)
