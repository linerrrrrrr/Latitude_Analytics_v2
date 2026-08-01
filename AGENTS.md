# 项目运行环境

- 本仓库的标准 Python 环境是名为 `latitude` 的 Conda 环境。
- 在本工作站上，使用 `E:\anaconda3\envs\latitude\python.exe` 运行 Python，或使用 `conda run -n latitude python`。
- 不要根据裸 `python` 或裸 `pip` 的结果判断依赖缺失；它们可能指向 Conda 的 `base` 环境。
- 报告环境或依赖问题前，先输出 `sys.executable`，并使用标准解释器检查相关包。
- 使用标准解释器执行 `python -m pip`，不要使用裸 `pip`。
- 运行 `E:\anaconda3\envs\latitude\python.exe scripts\verify_runtime.py`，验证当前运行环境及核心 DataFrame 依赖。

# 项目根目录定位约定

- `.env.template` 中记录的项目根目录定位约定，是 AI 修改本仓库时必须遵循的权威规范。
- 从子目录运行的代码如果需要先定位项目根目录，再导入 `config.settings.settings`，必须使用下面的标记文件搜索方式。不要引入其他项目根目录定位方法。

```python
import pathlib
import sys

project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

from config.settings import settings
```
