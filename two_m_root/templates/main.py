"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import os
import typing
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "settings.env"))
DATABASE_PATH = os.environ.get("DATABASE_PATH")
MEMCACHE_PATH = os.environ.get("CACHE_PATH")
BASE_DIR_ROOT = os.path.dirname(__file__)

# Далее константы классов
# Tool
RELEASE_INTERVAL_SECONDS: float = 20.0
CACHE_LIFETIME_HOURS: int = 1 * 60 * 60
RETRYING_CLIENT_ATTEMPTS: int = 10
RETRYING_CLIENT_RETRY_DELAY: float = 0.3

# NodeDataManager
IGNORE_NODE_PRIMARY_KEY_ERROR: bool = False
INCOMING_DATA_VALIDATION_LEVEL: typing.Literal["filter", "strong"] = "strong"

# SQLAlchemyQueryManager
MAX_RETRIES: typing.Union[int, typing.Literal["no-limit"]] = "no-limit"

# ResultORMCollection
ADD_TABLE_NAME_PREFIX: typing.Literal["auto", "add", "no-prefix"] = "auto"

# PointerCacheTools
WRAP_ITEM_MAX_LENGTH = 30

# ConnectionManager
DATABASE_CONNECTION_ALIVE_SEC = 3
CACHE_CONNECTION_ALIVE_SEC = 3

# ResultPaginatorMixin
ITEMS_ON_PAGE = 25

# OrderByMixin
BY_PRIMARY_KEY = True
BY_COLUMN_NAME = None
BY_CREATE_TIME = False
BY_ALPHABET = True
BY_STRING_LENGTH = False
REVERSED = False

# BaseSliceResultMixin
UNIFORM_SAMPLING_DB_AND_CACHE = False  # Производить выборку данных из кеша и базы данных равномерно - половина на половину
# Или сначала в результат пойдёт одна из категорий до исчерпания, а потом вторая
# Внимание. Если данный режим включён, то минимальное количество элементов в срезе может сильно разниться,
# и не будет соответствовать ожидаемой длине!

#########
# DEBUG #
#########
ITER_ONLY_VISIBLE_ITEMS_AS_DEFAULT = True  # Скрывать или не скрывать скрытые ноды из итерируемой последовательности
