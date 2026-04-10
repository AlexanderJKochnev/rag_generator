# app.support.clickhouse.import_service.project_config.py
from pathlib import Path
from typing import Dict, List
from pydantic_settings import BaseSettings, SettingsConfigDict
import os


def get_path_to_root(name: str = '.env'):
    """
        get path to file or directory in root directory
    """
    try:
        for k in range(1, 10):
            env_path = Path(__file__).resolve().parents[k] / name
            if env_path.exists():
                break
        else:
            env_path = None
            raise Exception('environment file is not found')
        return env_path
    except Exception:
        return None


def strtolist(data: str, delim: str = ',') -> List[str]:
    """ строка с разделителями в список"""
    if isinstance(data, str):
        return [a.strip() for a in data.split(delim)]
    else:
        return []


def strtodict(data: str, delim1: str = ',', delim2: str = ':') -> Dict[str, str]:
    tmp = strtolist(data, delim1)
    result: dict = {}
    for item in tmp:
        key, val = item.split(delim2)
        result[key.strip()] = val.strip()
    return result


class Settings(BaseSettings):
    """ Project Settings """
    # внешний адрес и порт

    # === CLICKHOUSE ===
    CH_HOST: str = 'localhost'
    CH_PORT: int = 8123
    CH_USER: str = 'secret_user'
    CH_PASSWORD: str = 'top_secret'
    CH_LIMIT: int = 1000  # ограничение кол-ва записей - защита от перегрузки

    model_config = SettingsConfigDict(env_file=get_path_to_root(),
                                      env_file_encoding='utf-8',
                                      extra='ignore')

settings = Settings()
