"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Класс-адаптер нужен в качестве параметра serde для инициализации класса Client pymemcache.
Добавление поддержки модуля dill в качестве сериализатора для pymemcache.
"""
from dill import dumps, loads


class DillSerde:
    @staticmethod
    def serialize(key, data):
        if isinstance(data, bytes):
            return data, 1
        return dumps(data), 2

    @staticmethod
    def deserialize(key, value, flag):
        if flag == 1:
            return loads(value)
        if flag == 2:
            return loads(value)
        raise Exception("Неизвестное состояние флага")
