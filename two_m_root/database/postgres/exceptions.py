"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Модуль исключений для использования в качестве БД postgres и её драйвер psycopg2 соответственно
"""
from psycopg2.errors import Error


DatabaseException = Error
