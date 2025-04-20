"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import os
import typing
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "settings.env"))
DATABASE_PATH = os.environ.get("DATABASE_PATH")
MEMCACHE_PATH = os.environ.get("CACHE_PATH")
# Далее константы классов
# Tool
RELEASE_INTERVAL_SECONDS: float = 5.0
CACHE_LIFETIME_HOURS: int = 1 * 60 * 60
# SQLAlchemyQueryManager
MAX_RETRIES: typing.Union[int, typing.Literal["no-limit"]] = "no-limit"
# ResultORMCollection
ADD_TABLE_NAME_PREFIX: typing.Literal["auto", "add", "no-prefix"] = "auto"
# PointerCacheTools
WRAP_ITEM_MAX_LENGTH = 30
