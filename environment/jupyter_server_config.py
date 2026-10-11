"""由本项目的 Jupyter 启动参数显式加载，设置服务器及新内核的根目录。"""

from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
project_markers = (".git", ".env", "config/settings.py")
if not all((project_root / marker).exists() for marker in project_markers):
    raise RuntimeError(f"Jupyter 配置所在项目缺少根目录标记：{project_root}")

c = get_config()
c.ServerApp.root_dir = str(project_root)
