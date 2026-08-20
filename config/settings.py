import os
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd


pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", 300)

# =============================================================================
# .env 文件加载
# =============================================================================
def load_env(
    env_file: Optional[Path] = None
) -> None:

    if env_file is None:
        project_root = (
            Path(__file__)      # <DRIVE>:\Latitude_Analytics_v2\config\settings.py
            .parent             # <DRIVE>:\Latitude_Analytics_v2\config
            .parent             # <DRIVE>:\Latitude_Analytics_v2
        )
        env_file = project_root / r".env"
    
    if not env_file.exists(): return
    
    with open(env_file, mode = "r", encoding = "utf-8") as file:
        for line in file:
            line = line.strip()

            if not line or line.startswith("#"): continue
            if "=" in line:
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()


# =============================================================================
# 配置项
# =============================================================================
class Settings:

    def __init__(self) -> None: load_env()

    @property
    def futures_data_start_date(self) -> date:
        """当前稳定采集统一正式起点；属性名为兼容既有期货入口而保留。"""

        value = os.environ.get("FUTURES_DATA_START_DATE", "").strip()
        if not value:
            raise ValueError("环境变量 FUTURES_DATA_START_DATE 未定义或为空，请在 .env 文件中设置")
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                "环境变量 FUTURES_DATA_START_DATE 必须使用 YYYY-MM-DD 格式"
            ) from exc

    @property
    def futures_lake_root(self) -> Path:
        value = os.environ.get("FUTURES_LAKE_ROOT", "").strip()
        if not value:
            raise ValueError("环境变量 FUTURES_LAKE_ROOT 未定义或为空，请在 .env 文件中设置")

        lake_root = Path(value).expanduser()
        if not lake_root.is_absolute():
            project_root = Path(__file__).resolve().parent.parent
            lake_root = project_root / lake_root
        return lake_root.resolve()

    @property
    def tushare_token(self) -> str:
        value = os.environ.get("TUSHARE_TOKEN", "")
        if value is None or not value.strip(): 
            raise ValueError("环境变量 TUSHARE_TOKEN 未定义或为空，请在 .env 文件中设置")
        return value
    
    @property
    def jqdata_id(self) -> str:
        return os.environ.get("JQDATA_ID", "")
    
    @property
    def jqdata_secret(self) -> str:
        value = os.environ.get("JQDATA_SECRET", "")
        if value is None or not value.strip(): 
            raise ValueError("环境变量 JQDATA_SECRET 未定义或为空，请在 .env 文件中设置")
        return value
    
    @property
    def juejinshuju_url(self) -> str:
        value = os.environ.get("JUEJINSHUJU_URL", "")
        if value is None or not value.strip(): 
            raise ValueError("环境变量 JUEJINSHUJU_URL 未定义或为空，请在 .env 文件中设置")
        return value
    
    @property
    def juejinshuju_user(self) -> str:
        value = os.environ.get("JUEJINSHUJU_USER", "")
        if value is None or not value.strip(): 
            raise ValueError("环境变量 JUEJINSHUJU_USER 未定义或为空，请在 .env 文件中设置")
        return value
    
    @property
    def juejinshuju_password(self) -> str:
        value = os.environ.get("JUEJINSHUJU_PASSWORD", "")
        if value is None or not value.strip(): 
            raise ValueError("环境变量 JUEJINSHUJU_PASSWORD 未定义或为空，请在 .env 文件中设置")
        return value
    
    @property
    def juejinshuju_file(self) -> str:
        value = os.environ.get("JUEJINSHUJU_FILE", "")
        if value is None or not value.strip(): 
            raise ValueError("环境变量 JUEJINSHUJU_FILE 未定义或为空，请在 .env 文件中设置")
        return value
    

# =============================================================================
# 实例化
# =============================================================================
settings = Settings()
