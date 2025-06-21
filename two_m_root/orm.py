"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Для конечного пользования применяются только методы set_item, get_items
### insert/update/delete(DML) новой записи через set_item:
    - pk передан явно
    - генерится автоинкрементивный ключ, который не пойдёт в insert запись, если autoincrement=True
    - в целевой таблице, как и в ноде присутствует столбец unique=True,
        тогда запись с таким столбцом и значением будет искаться сначала в локальной очереди, а затем и в базе данных.
### get_items и join_select
    Возвращает экземпляр Result и JoinSelectResult соответственно.
    Выполняет 2 запроса: 1 в базу данных, второй в локальную очередь. Полученные записи накладываются друг на друг
"""
import os
import sys
import copy
import threading
import string
import warnings
import datetime
import itertools
import importlib
import hashlib
import operator
import uuid
from itertools import zip_longest
from abc import ABC, abstractmethod, abstractproperty
from weakref import ref, ReferenceType
from typing import Union, Iterator, Iterable, Optional, Literal, Type, Any
from collections import ChainMap
from pymemcache.client.base import PooledClient
from sqlalchemy import create_engine, delete, insert, update, text, select
from sqlalchemy.sql.expression import func, desc
from sqlalchemy.sql.dml import Insert, Update, Delete
from sqlalchemy.orm import Query, sessionmaker as session_factory, scoped_session
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from two_m_root.dill.serde import DillSerde
from two_m_root.datatype import LinkedList, LinkedListItem
from two_m_root.exceptions import *
from two_m_root.database.postgres.exceptions import DatabaseException
from two_m_root.conf import RESERVED_WORDS, CustomModel
from two_m.main import MEMCACHE_PATH, DATABASE_PATH, RELEASE_INTERVAL_SECONDS, CACHE_LIFETIME_HOURS, \
    MAX_RETRIES, WRAP_ITEM_MAX_LENGTH, ADD_TABLE_NAME_PREFIX  # from user's package


class ModelTools:
    @staticmethod
    def is_valid_model_instance(item):
        if isinstance(item, (int, str, float, bytes, bytearray, set, dict, list, tuple, type(None))):
            raise InvalidModel(f"type - {type(item)}")
        if hasattr(item, "__new__"):
            item = item()  # __new__
            if not hasattr(item, "column_names"):
                raise InvalidModel
            keys = {"type", "nullable", "primary_key", "autoincrement", "unique", "default"}
            if any(map(lambda x: keys - set(x), item.column_names.values())):
                raise ModelsConfigurationError
            return
        raise InvalidModel

    @classmethod
    def is_autoincrement_primary_key(cls, model: Type[CustomModel]) -> bool:
        cls.is_valid_model_instance(model)
        for column_name, data in model().column_names.items():
            if data["autoincrement"]:
                return True
        return False

    @classmethod
    def get_primary_key_python_type(cls, model: Type[CustomModel]) -> Type:
        cls.is_valid_model_instance(model)
        for column_name, data in model().column_names.items():
            if data["primary_key"]:
                return data["type"]

    @classmethod
    def get_unique_columns(cls, model, data: dict) -> Iterator[str]:
        """ Получить названия столбцов с UNIQUE=TRUE (их значения присутствуют в ноде) """
        cls.is_valid_model_instance(model)
        model_data = model().column_names
        if "ui_hidden" in data:
            del data["ui_hidden"]
        for column_name in model_data:
            if model_data[column_name]["unique"]:
                yield column_name

    @classmethod
    def get_default_column_value_or_function(cls, model: Type[CustomModel], column_name: str) -> Optional[Any]:
        cls.is_valid_model_instance(model)
        if type(column_name) is not str:
            raise TypeError
        return model().column_names[column_name]["default"]

    @classmethod
    def get_primary_key_column_name(cls, model: Type[CustomModel]):
        cls.is_valid_model_instance(model)
        for column_name, data in model().column_names.items():
            if data["primary_key"]:
                return column_name

    @classmethod
    def get_foreign_key_columns(cls, model: Type[CustomModel]) -> tuple[str]:
        cls.is_valid_model_instance(model)
        return model().foreign_keys

    @classmethod
    def get_nullable_columns(cls, model: Type[CustomModel], nullable=True) -> Iterator[str]:
        cls.is_valid_model_instance(model)
        if type(nullable) is not bool:
            raise TypeError
        for column_name, data in model().column_names.items():
            if data["primary_key"]:
                continue
            if nullable:
                if data["nullable"]:
                    yield column_name
            else:
                if not data["nullable"]:
                    yield column_name

    @classmethod
    def get_column_names_with_default_procedure_or_value(cls, model: Type[CustomModel], has_default=True) -> Iterator[str]:
        cls.is_valid_model_instance(model)
        if type(has_default) is not bool:
            raise TypeError
        for column_name, data in model().column_names.items():
            if has_default:
                if data["default"]:
                    yield column_name
            else:
                if not data["default"]:
                    yield column_name

    @staticmethod
    def import_model(name: str):
        if type(name) is not str:
            raise TypeError
        if not name:
            raise ValueError
        model_instance = getattr(importlib.import_module("models",
                                 package=os.path.dirname(__file__)), name, None)
        if model_instance is None:
            raise InvalidModel(f"Класс-модель '{name}' в модуле models не найден")
        return model_instance

    @classmethod
    def get_column_python_type(cls, model: Type[CustomModel], column_name: str) -> Union[Type, AttributeError]:
        """ Получить питоновский тип от передаваемого столбца """
        cls.is_valid_model_instance(model)
        if not cls.is_exists_column(model, column_name):
            raise AttributeError
        return model().column_names[column_name]["type"]

    @classmethod
    def is_exists_column(cls, model: Type[CustomModel], name: str) -> bool:
        cls.is_valid_model_instance(model)
        return name in model().column_names


class NodeTools:
    """ Средства валидации для всех объектов, производных от QueueItem """
    @staticmethod
    def _is_valid_primary_key(d: dict, model: CustomModel):
        if not isinstance(d, dict):
            raise TypeError
        if not d:
            raise NodePrimaryKeyError
        ModelTools.is_valid_model_instance(model)
        primary_key_column_name = ModelTools.get_primary_key_column_name(model)
        if primary_key_column_name not in d:
            raise NodePrimaryKeyError("Столбец первичного не обнаружен!")
        if not isinstance(d[primary_key_column_name], (int, str,)):
            raise NodePrimaryKeyError

    @staticmethod
    def _is_valid_column_type_in_sql_type(node: "QueueItem"):
        """ Проверить соответствие данных в ноде на предмет типизации.
         Если тип данных отличается от табличного в БД, то возбудить исключение"""
        data = node.model().column_names
        for column_name in data:
            if column_name not in node.value:
                continue
            if not isinstance(node.value[column_name], data[column_name]["type"]):
                if node.value[column_name] is None and data[column_name]["nullable"]:
                    continue
                raise NodeColumnValueError(text=f"Столбец {column_name} должен быть производным от "
                                                f"{str(data[column_name]['type'])}, "
                                                f"по факту {type(node.value[column_name])}")

    @staticmethod
    def _field_names_validation(node, values: dict):
        """ Соотнести все столбцы ноды в словаре value со столбцами из класса Model """
        if type(values) is not dict:
            raise TypeError
        if not isinstance(node, (ServiceOrmItem, QueueItem,)):
            raise TypeError
        for name in values:
            if not isinstance(values[name], (str, int, bool, float, bytes, bytearray, type(None),)):
                raise NodeColumnValueError(values[name])
        any_ = set(values) - set(node.model().column_names)
        if any_:
            raise NodeColumnError(any_, model_name=node.model.__name__)

    @staticmethod
    def _is_valid_dml_type(*dml):
        if not len(dml) == 3:
            raise ValueError
        if not all(map(lambda i: type(i) is bool, dml)):
            raise TypeError
        if not sum(dml) == 1:
            raise NodeDMLTypeError

    @staticmethod
    def _check_not_null_fields_in_node_value(node: "QueueItem"):
        """ Проверить все поля на предмет nullable """
        model_attributes = node.model().column_names
        for k, attributes in model_attributes.items():
            if not attributes["nullable"]:
                if k not in node.value:
                    return False
                if type(node.value[k]) is None:
                    return False
        return True


class AbsNode(ABC):
    @property
    @abstractmethod
    def model(self):
        pass

    @abstractmethod
    def get_primary_key_and_value(self, only_key=False, only_value=False, as_tuple=False):
        pass


class QueueItem(LinkedListItem, ModelTools, NodeTools, AbsNode):
    """ Нода для постановки в очередь с последующей инъекцией в базу данных. """
    def __init__(self, _insert=False, _update=False, _delete=False,
                 _model=None, _create_at=None, _count_retries=0, _ready=False,  **node_data):
        super().__init__(**node_data)
        self.__model: CustomModel = _model
        self.is_valid_model_instance(self.__model)
        self.__insert = _insert
        self.__update = _update
        self.__delete = _delete
        self.__is_ready = _ready
        self._create_at = _create_at if _create_at is not None else datetime.datetime.now()
        self.__transaction_counter = _count_retries  # Инкрементируется при вызове self.make_query()
        # Подразумевая тем самым, что это попытка сделать транзакцию в базу
        primary_key = ModelTools.get_primary_key_column_name(self.__model)
        try:
            self.__primary_key = {primary_key: self._val[primary_key]}
        except KeyError:
            raise NodePrimaryKeyError("Первичный ключ не передан в ноду")
        self._is_valid_dml_type(self.__insert, self.__update, self.__delete)
        self._field_names_validation(self, self.value)
        self._is_valid_column_type_in_sql_type(self)
        self._is_valid_primary_key(self.__primary_key, self.__model)
        self.__foreign_key_fields = self.get_foreign_key_columns(self.__model)

    @property
    def model(self):
        return self.__model

    @property
    def retries(self):
        return self.__transaction_counter

    def get(self, k, default_value=None):
        try:
            value = self.__getitem__(k)
        except KeyError:
            value = default_value
        return value

    def get_primary_key_and_value(self, as_tuple=False, only_key=False, only_value=False) -> Union[dict, tuple, int, str]:
        if only_key:
            return tuple(self.__primary_key.keys())[0]
        if only_value:
            return tuple(self.__primary_key.values())[0]
        return tuple(self.__primary_key.items())[0] if as_tuple else self.__primary_key.copy()

    @property
    def created_at(self):
        return self._create_at

    @property
    def ready(self) -> bool:
        if self.__delete:
            return self.__is_ready
        self.__is_ready = self._check_not_null_fields_in_node_value(self)
        return self.__is_ready

    @property
    def type(self) -> str:
        return "_insert" if self.__insert else "_update" if self.__update else "_delete"

    def get_attributes(self, with_update: Optional[dict] = None) -> dict:
        if with_update is not None and type(with_update) is not dict:
            raise TypeError
        result = {"_create_at": self.created_at}
        result.update(self.value)
        result.update({"_model": self.__model, "_insert": False,
                       "_update": False, "_ready": self.__is_ready,
                       "_delete": False, "_count_retries": self.retries})
        result.update({self.type: True}) if self.type is not None else None
        result.update(with_update) if with_update else None
        return result

    def make_query(self) -> Optional[Query]:
        query = None
        primary_key, pk_value = self.get_primary_key_and_value(as_tuple=True)
        if self.__insert:
            query = insert(self.model).values(**self.value)
        if self.__update:
            query = update(self.model).where(text(f"{self.model.__tablename__}.{primary_key}={pk_value}")).values(**self.value)
        if self.__delete:
            query = delete(self.model).where(text(f"{self.model.__tablename__}.{primary_key}={pk_value}"))
        self.__transaction_counter += 1
        return query

    def __len__(self):
        return len(self._val)

    def __contains__(self, item: str):
        if not isinstance(item, str):
            raise TypeError
        if ":" not in item:
            raise KeyError("Требуется формат 'key:value'")
        key, value = item.split(":")
        if key not in self.value:
            return False
        val = self.value[key]
        return value == val

    def __bool__(self):
        if self.__delete:
            return True
        try:
            next(iter(self._val))
        except StopIteration:
            return False
        else:
            return True

    def __repr__(self):
        return f"{type(self).__name__}({self.__str__()})"

    def __str__(self):
        attributes = self.get_attributes()
        create_at = attributes.pop("_create_at")
        attributes.update({"_create_at": create_at.strftime("%d:%m:%S")})
        return ', '.join(map(lambda i: '='.join([str(e) for e in i]), attributes.items()))

    def __getitem__(self, item: str):
        if type(item) is not str:
            raise TypeError
        if item not in self.value:
            raise KeyError
        return self.value[item]


class ResultORMItem(LinkedListItem, NodeTools, AbsNode):
    def __init__(self, _model, _primary_key: Optional[dict], _ui_hidden=False, **k):
        self._primary_key = _primary_key
        self._model = _model
        self._hidden = _ui_hidden
        super().__init__(**self.__clean_kwargs(k))
        self.__is_valid()

    @property
    def model(self):
        return self._model

    @property
    def hidden(self):
        return self._hidden

    @property
    def hash_by_pk(self):
        pk = self.get_primary_key_and_value()
        str_ = "".join(map(lambda x: f"{x[0]}{x[1]}", zip(pk.keys(), pk.values())))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def get_primary_key_and_value(self, only_key=False, only_value=False, as_tuple=False):
        if type(only_key) is not bool:
            raise TypeError
        if not isinstance(only_value, bool):
            raise TypeError
        if type(as_tuple) is not bool:
            raise TypeError
        if sum((only_value, only_key, as_tuple)) not in [0, 1]:
            raise ValueError
        if only_key:
            return tuple(self._primary_key.keys())[0]
        if only_value:
            return tuple(self._primary_key.values())[0]
        return self._primary_key.copy()

    def get(self, name: str, def_val=None):
        if not isinstance(name, str):
            raise TypeError
        try:
            value = self.value[name]
        except KeyError:
            return def_val
        else:
            return value

    def add_model_name_prefix(self, column_names: Optional[tuple] = None):
        """ Добавить каждому столбцу префикс с названием таблицы """
        self.__is_valid_column_names_arg(column_names)
        new_values = self.value
        for column_name, value in self.value.items():
            if column_names is not None:
                if column_name not in column_names:
                    continue
            if "." in column_name:
                exists_prefix = column_name[0:column_name.index(".")]
                if exists_prefix == self.model.__name__:
                    continue
                del new_values[column_name]
                new_values.update({f"{self.model.__name__}.{column_name}": value})
                continue
            del new_values[column_name]
            new_values.update({f"{self.model.__name__}.{column_name}": value})
        self._val = new_values

    def remove_model_name_prefix(self, column_names: Optional[tuple] = None):
        self.__is_valid_column_names_arg(column_names)
        new_values = self.value
        for column_name, value in self.value.items():
            parts = column_name.split(".")
            if not parts:
                continue
            if column_names is not None:
                if parts[1] not in column_names:
                    continue
            model_name = self.model.__name__
            if model_name not in parts:
                continue
            parts.remove(model_name)
            del new_values[column_name]
            new_values.update({".".join(parts): value})
        self._val = new_values

    def get_attributes(self):
        return {"_model": self._model, "_primary_key": self._primary_key, "_ui_hidden": self._hidden, **self._val}

    def __bool__(self):
        if not self.value:
            return False
        return True

    def __getitem__(self, key):
        return self.value.__getitem__(key)

    def __contains__(self, item: Union[str, Literal["str:str"]]):
        if type(item) is not str:
            raise TypeError
        if ":" in item:
            key, value = item.split(":")
            if not key:
                return False
            if not value:
                return False
            val = self.value.get(key, None)
            if val is None:
                return False
            if val == value:
                return True
            return False
        if item in self.value:
            return True
        return False

    def __hash__(self):
        data = self.value
        data.update(self.get_primary_key_and_value())
        str_ = "".join(map(str, itertools.chain(*data.items())))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def __repr__(self):
        return f"{self.__class__.__name__}({self.__str__()})"

    def __str__(self):
        return f"{self.model}, {self._primary_key}, {', '.join(map(lambda x: '='.join(map(str, x)) ,self._val.items()))}"

    @staticmethod
    def __clean_kwargs(kwargs_dict) -> dict:
        return {key: value for key, value in kwargs_dict.items() if not key.startswith("_")}

    def __is_valid(self):
        if type(self._hidden) is not bool:
            raise TypeError
        if type(self._val) is not dict:
            raise TypeError
        ModelTools.is_valid_model_instance(self._model)
        if not self.value:
            raise ValueError
        self._is_valid_primary_key(self._primary_key, self._model)

    @staticmethod
    def __is_valid_column_names_arg(names: tuple):
        if names is None:
            return
        if type(names) is not tuple:
            raise TypeError
        if not names:
            raise ValueError
        if any(map(lambda x: not isinstance(x, str), names)):
            raise TypeError
        if any(filter(lambda x: not any(x), names)):
            raise ValueError


class QueueNodeSearchTool(LinkedList):
    """ Данный класс делает поиск ноды в очереди(по первичному ключу и значению) за O(1), вместо O(n).
    Добавление ноды в конец за O(1) - вместо O(1)
    Добавление нод по setitem O(n) - вместо O(n)
    Замена ноды за O(1) - вместо O(1)
    Удаление ноды за O(n) - вместо O(n)
    get_node ноды за O(1) - вместо O(n)
    Жертвуем O(n) памяти."""
    LinkedListItem = ...

    def __init__(self, *args, **kwargs):
        self.__items_hash_map = {}  # primary_key_val_hash: node_item
        self.__node_index_hash_map = {}  # node_index: primary_key_val_hash
        super().__init__(*args, **kwargs)
        [self.__add_node(node) for node in self]

    def get_node(self, model, **primary_key_data):  # O(1)
        if not len(primary_key_data) == 1:
            raise ValueError
        try:
            hash_val = self.__get_hash_value(model.__name__, *tuple(primary_key_data.items())[0])
            return self.__items_hash_map[hash_val]
        except KeyError:
            return

    def append(self, *args, node_item=None, **kwargs):  # O(1)
        if node_item is not None:
            self.__is_valid_node(node_item)  # O(1)
            new_item = self.LinkedListItem(**node_item.get_attributes())
        else:
            new_item = self.LinkedListItem(*args, **kwargs)
        new_item = super().append(node_item=new_item)  # O(1)
        self.__add_node(new_item)  # O(1)

    def add_to_head(self, new_item=None, **kwargs):  # O(n)
        if new_item is not None:
            self.__is_valid_node(new_item)
            new_item = self.LinkedListItem(**new_item.get_attributes())
        else:
            new_item = self.LinkedListItem(**kwargs)  # O(1)
        new_item = super().add_to_head(node_item=new_item)  # O(n)
        self.__reset_mappings_and_indexes()  # O(n)
        self.__add_node(new_item)  # O(1)

    def replace(self, old_node, new_node):  # O(1)
        self.__is_valid_node(old_node)  # O(1)
        self.__is_valid_node(new_node)  # O(1)
        new_node.index = old_node.index  # O(1)
        super().replace(old_node, new_node)  # O(1)
        hash_val = self.__get_hash_value(old_node.model.__name__,
                                         *old_node.get_primary_key_and_value(as_tuple=True))  # O(k)
        self.__items_hash_map[hash_val] = new_node  # O(1)
        self.__node_index_hash_map[new_node.index] = hash_val  # O(1)

    def __getitem__(self, node_index):  # O(1)
        if not isinstance(node_index, (int, slice,)):
            raise TypeError
        if type(node_index) is slice:
            return super().__getitem__(node_index)
        node_index = self._support_negative_index(node_index)  # O(1)
        try:
            hash_val = self.__node_index_hash_map[node_index]
            return self.__items_hash_map[hash_val]
        except KeyError:
            raise IndexError

    def __setitem__(self, key, value):  # O(n)
        new_node = value if type(value) is self.LinkedListItem else self.LinkedListItem(**value)  # O(1)
        self.__is_valid_node(new_node)  # O(1)
        super().__setitem__(key, new_node)  # O(1)
        new_hash_value = self.__get_hash_value(new_node.model.__name__,
                                               *new_node.get_primary_key_and_value(as_tuple=True))  # O(k)
        self.__node_index_hash_map[new_node.index] = new_hash_value  # O(1)
        self.__items_hash_map[new_hash_value] = new_node  # O(1)
        self.__reset_mappings_and_indexes()  # O(n)

    def __delitem__(self, node_index):  # O(n)
        self._is_valid_index(node_index)  # O(1)
        node_index = self._support_negative_index(node_index)  # O(1)
        node_hash = self.__node_index_hash_map[node_index]  # O(1)
        node = self.__items_hash_map[node_hash]  # O(1)
        del self.__items_hash_map[node_hash]  # O(1)
        del self.__node_index_hash_map[node_index]  # O(1)
        if node_index == 0:
            next_node = node.next
            if next_node is not None:
                self._head = next_node
                next_node.prev = None
            else:
                self._head = None
            node.next = None
            self.__reset_mappings_and_indexes()  # O(n)
            return
        if node_index == self._tail.index:
            prev_node = node.prev()
            prev_node.next = None
            self._tail = prev_node
            node.prev = None
            return
        next_node = node.next
        prev_node = node.prev()
        prev_node.next = next_node
        next_node.prev = prev_node
        self.__reset_mappings_and_indexes()  # O(n)

    def __contains__(self, item: LinkedListItem):  # O(1)
        try:
            self.__is_valid_node(item)
        except AttributeError:
            return False
        except ValueError:
            return False
        except TypeError:
            return False
        return self.__get_hash_value(item.model.__name__, *item.get_primary_key_and_value(as_tuple=True)) in self.__items_hash_map

    def __add_node(self, new_item):  # O(1)
        """ Ноде уже должен быть присвоен индекс(порядковый номер) в связанном списке, до момента вызова этого метода. """
        self.__is_valid_node(new_item)
        hash_value = self.__get_hash_value(new_item.model.__name__, *new_item.get_primary_key_and_value(as_tuple=True))
        self.__items_hash_map[hash_value] = new_item
        self.__node_index_hash_map[new_item.index] = hash_value

    def __reset_mappings_and_indexes(self):   # O(3n)
        for i, node in enumerate(self):
            node.index = i
        self.__node_index_hash_map = dict(zip(range(len(self)),
                                              map(lambda n: self.__get_hash_value(n.model.__name__,
                                                                                  *n.get_primary_key_and_value(as_tuple=True)), self)))
        self.__items_hash_map = dict(zip(self.__node_index_hash_map.values(), self))

    @staticmethod
    def __get_hash_value(*args):
        return int.from_bytes(hashlib.md5("".join(map(str, args)).encode("utf-8")).digest(), "big")

    @classmethod
    def __is_valid_node(cls, node):
        if not hasattr(node, "get_primary_key_and_value"):
            raise AttributeError
        if not callable(getattr(node, "get_primary_key_and_value")):
            raise TypeError
        if not isinstance(node.get_primary_key_and_value(), dict):
            raise ValueError
        if not len(node.get_primary_key_and_value()) == 1:
            raise ValueError
        if type(node.get_primary_key_and_value(only_key=True)) is not str:
            raise TypeError
        if not isinstance(node.get_primary_key_and_value(only_value=True), (int, str)):
            raise TypeError
        if not hasattr(node, "model"):
            raise AttributeError
        ModelTools.is_valid_model_instance(node.model)

    def __get_slice(self, obj: slice):
        if not isinstance(obj, slice):
            raise TypeError


class Queue(LinkedList):
    """
    Очередь на основе связанного списка.
    Управляется через адаптер Tool.
    Класс-контейнер умеет только ставить в очередь ((enqueue) зашита особая логика) и снимать с очереди (dequeue)
    см логику в методе _replication.
    """
    LinkedListItem = QueueItem

    def __init__(self, items: Optional[Iterable[dict]] = None):
        super().__init__(items)
        if items is not None:
            for inner in items:
                self.enqueue(**inner)

    def enqueue(self, **attrs):  # O(n * log(n))
        """ Установка ноды в конец очереди с хитрой логикой проверки на совпадение. """
        exists_item, new_item = self._replication(**attrs)  # O(1)
        self._remove_from_queue(exists_item) if exists_item is not None else None
        self._remove_other_node_with_current_foreign_key_value(new_item)  # O(n * log(n))
        self._check_primary_key_unique(new_item)  # O(n)
        self._check_unique_values(new_item)  # O(k)
        self.append(**new_item.get_attributes())  # O(1)

    def dequeue(self) -> Optional[QueueItem]:
        """ Извлечение ноды с начала очереди """
        left_node = self._head
        if left_node is None:
            return
        self._remove_from_queue(left_node)
        return left_node

    def remove(self, model, pk_field_name, pk_field_value):
        left_node = self.get_node(model, **{pk_field_name: pk_field_value})
        if left_node:
            self._remove_from_queue(left_node)
            return left_node

    def get_related_nodes(self, main_node: QueueItem, other_container=None) -> "Queue":
        """ Получить все связанные (внешним ключом) с передаваемой нодой ноды.
        O(i) * O(1) + O(n) = O(n)"""
        root = self
        if other_container is not None:
            if type(other_container) is not self.__class__:
                raise TypeError
            root = other_container
        container = self.__class__()
        for related_node in root:  # O(n) * O(j) * O(m) * O(n) * O(1) = O(n)
            if related_node == main_node:  # O(g) * O(j) = O (j)
                continue
            pk_field, pk_value = related_node.get_primary_key_and_value(as_tuple=True)
            for fk_column in ModelTools.get_foreign_key_columns(main_node.model):  # O(i)
                if pk_field == fk_column:
                    if fk_column not in main_node.value:
                        continue
                    if pk_value == main_node.value[fk_column]:
                        container.append(**related_node.get_attributes())  # O(1)
        return container

    def search_nodes(self, model: Type[CustomModel], negative_selection=False,
                     **_filter: dict[str, Union[str, int, Literal["*"]]]) -> "Queue":  # O(n)
        """
        Искать ноды по совпадениям любых полей.
        :param model: кастомный объект, смотри модуль database/models
        :param _filter: словарь содержащий набор полей и их значений для поиска, вместо значений допустим знак '*',
        который будет засчитывать любые значения у полей.
        :param negative_selection: режим отбора нод (найти ноды КРОМЕ ... [filter])
        """
        QueueItem.is_valid_model_instance(model)
        items = self.__class__()
        nodes = iter(self)
        while nodes:
            try:
                left_node: QueueItem = next(nodes)
            except StopIteration:
                return items
            if left_node.model.__name__ == model.__name__:  # O(u * k)
                if not _filter and not negative_selection:
                    items.append(**left_node.get_attributes())
                for field_name, value in _filter.items():
                    if field_name in left_node.value:
                        if negative_selection:
                            if value == "*":
                                if field_name not in left_node.value:
                                    items.append(**left_node.get_attributes())
                                continue
                            if not left_node.value[field_name] == value:
                                items.append(**left_node.get_attributes())
                                break
                        else:
                            if value == "*":
                                if field_name in left_node.value:
                                    items.append(**left_node.get_attributes())
                                    continue
                            if left_node.value[field_name] == value:
                                items.append(**left_node.get_attributes())
                                break
        return items

    def get_node(self, model: CustomModel, **primary_key_data) -> Optional[QueueItem]:
        """
        Данный метод используется при инициализации - _replication
        :param model: объект модели
        :param primary_key_data: словарь вида - {имя_первичного_ключа: значение}
        """
        QueueItem.is_valid_model_instance(model)
        if not len(primary_key_data) == 1:
            raise NodePrimaryKeyError
        nodes = iter(self)
        while nodes:
            try:
                left_node: Optional[QueueItem] = next(nodes)
            except StopIteration:
                break
            if left_node.model.__name__ == model.__name__:  # O(k) * O(x)
                if left_node.get_primary_key_and_value() == primary_key_data:  # O(k1) * O(x1) * # O(k2) * O(x2)
                    return left_node

    def __repr__(self):
        return f"{self.__class__.__name__}({tuple(repr(m) for m in self)})"

    def __str__(self):
        return "\n".join(tuple(str(m) for m in self))

    def __add__(self, other):  # O(n * k) = O(n)
        if type(other) is not self.__class__:
            raise TypeError
        result_instance = self.__class__()
        [result_instance.append(**n.get_attributes()) for n in self]  # O(1) * O(n)
        [result_instance.enqueue(**n.get_attributes()) for n in other]  # O(n * log(n)) * O(k)
        return result_instance

    def __iadd__(self, other):  # O(n)
        if not isinstance(other, type(self)):
            raise TypeError
        result = self + other
        self._head = result.head
        self._tail = result.tail
        return result

    def __sub__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError
        result_instance = copy.deepcopy(self)
        [result_instance.remove(n.model, *n.get_primary_key_and_value(as_tuple=True)) for n in other]
        return result_instance

    def __and__(self, other: "Queue"):
        if type(other) is not self.__class__:
            raise TypeError
        output = self.__class__()
        for right_node in other:  # O(k)
            left_node = self.get_node(right_node.model, **right_node.get_primary_key_and_value())  # O(1)
            if left_node is not None:
                data = left_node.get_attributes()
                data.update(right_node.get_attributes())
                output.append(**data)
            else:
                output.append(**right_node.get_attributes())
        return output

    def _replication(self, **new_node_complete_data: dict) -> tuple[Optional[QueueItem], QueueItem]:  # O(l * k) + O(n) + O(1) = O(n)
        """
        Создавать ноды для добавления можно только здесь! Логика для постаовки в очередь здесь.
        1) Инициализация ноды: первичного ключа (согласно атрибутам класса модели), данных в ней и др
        2) Попытка найти ноду от той же модели с таким же первичным ключом -->
        заменяем ноду в очерени новой, смешивая value, если найдена, return
        Иначе
        3) Получаем список столбцов модели с unique=True
        Если столбца нету заменяем ноду в очерени новой, смешивая value, если найдена, return
        """
        potential_new_item = self.LinkedListItem(**new_node_complete_data)  # O(1)
        new_item = None

        def merge(old_node: QueueItem, new_node: QueueItem, dml_type: str) -> QueueItem:
            new_node_data = old_node.get_attributes()
            old_value, new_value = old_node.value, new_node.value
            old_value.update(new_value)
            new_node_data.update(old_value)
            new_node_data.update({"_insert": False, "_update": False, "_delete": False})
            new_node_data.update({dml_type: True, "_ready": new_node.ready})
            new_node_data.update({"_create_at": new_node.created_at})
            return self.LinkedListItem(**new_node_data)
        exists_item = self.get_node(potential_new_item.model, **potential_new_item.get_primary_key_and_value())
        if not exists_item:
            new_item = potential_new_item
            return None, new_item
        new_item_is_update = new_node_complete_data.get("_update", False)
        new_item_is_delete = new_node_complete_data.get("_delete", False)
        new_item_is_insert = new_node_complete_data.get("_insert", False)
        if new_item_is_update:
            if exists_item.type == "_insert" or exists_item.type == "_update":
                if exists_item.type == "_insert":
                    new_item = merge(exists_item, potential_new_item, "_insert")
                if exists_item.type == "_update":
                    new_item = merge(exists_item, potential_new_item, "_update")
            if exists_item.type == "_delete":
                new_item = potential_new_item
        if new_item_is_delete:
            new_item = potential_new_item
        if new_item_is_insert:
            if exists_item.type == "_insert" or exists_item.type == "_update":
                new_item = merge(exists_item, potential_new_item, "_insert")
            if exists_item.type == "_delete":
                new_item = potential_new_item
        return exists_item, new_item

    def _remove_from_queue(self, left_node: QueueItem) -> None:
        if type(left_node) is not self.LinkedListItem:
            raise TypeError
        del self[left_node.index]

    def _check_unique_values(self, node):
        """
        Валидация на предмет нарушения уникальности в полях разных нод
        :param node: Инициализированная для добавления нода
        :return: None
        """
        if type(node) is not self.LinkedListItem:
            raise TypeError
        if not self:
            return
        unique_fields = ModelTools.get_unique_columns(node.model, node.value)
        for unique_field in unique_fields:
            if unique_field not in node.value:
                continue
            value = node.value[unique_field]
            for n in self:
                if n.model.__name__ == node.model.__name__:
                    if unique_field in n.value:
                        if n.value[unique_field] == value:
                            raise NodeColumnValueError

    def _check_primary_key_unique(self, node):
        if not isinstance(node, self.LinkedListItem):
            raise TypeError
        if not self:
            return
        all_primary_keys = [n.get_primary_key_and_value(only_value=True) for n in self
                            if node.model.__name__ == n.model.__name__]
        if all_primary_keys.count(node.get_primary_key_and_value(only_value=True)) > 1:
            raise NodePrimaryKeyError("Нарушение уникальности первичных ключей в очереди. Первичный ключ повторяется.")

    def _remove_other_node_with_current_foreign_key_value(self, node):
        """ Если в очереди есть нода, той же модели, что и добавляемая,
         если у старой и добавляемой ноды есть одно и то же значение внешнего ключа,
         то из найденной в очереди ноды это значение нужно убрать
         Простыми словами: 2 ноды не могут ссылаться своими внешними ключами на какую-то одну ноду
         """
        for ex_node in self:
            if not node.model.__name__ == ex_node.model.__name__:
                continue
            all_foreign_key_columns = frozenset(ModelTools.get_foreign_key_columns(node.model))
            current_fk_values = frozenset(node.value) & all_foreign_key_columns & frozenset(ex_node.value)
            for fk_name in current_fk_values:
                if node.value[fk_name] == ex_node.value[fk_name]:
                    new_values = ex_node.get_attributes()
                    del new_values[fk_name]
                    self._remove_from_queue(ex_node)
                    self.append(**new_values)
                    break


class Queue(QueueNodeSearchTool, Queue):
    LinkedListItem = QueueItem


class ResultORMCollection:
    """ Иммутабельная коллекция с набором результата, закрытая на добавление новых элементов """
    ADD_TABLE_NAME_PREFIX: Literal["auto", "add", "no-prefix"] = ADD_TABLE_NAME_PREFIX

    def __init__(self, collection: "ServiceOrmContainer" = None, prefix_mode=None):
        def is_valid():
            if type(self.__collection) is not ServiceOrmContainer:
                raise TypeError
            if type(self._prefix_mode) is not str:
                raise TypeError
            if self._prefix_mode not in ("auto", "add", "no-prefix",):
                raise ValueError
        self.__collection = collection
        self._prefix_mode = prefix_mode if prefix_mode is not None else self.ADD_TABLE_NAME_PREFIX
        if collection is None:
            self.__collection = ServiceOrmContainer()
        is_valid()
        self.__collection = self.__convert_node_data(self.__collection)
        self.remove_model_prefix()
        if self._prefix_mode == "add":
            self.add_model_name_prefix()
        if self._prefix_mode == "no-prefix":
            self.remove_model_prefix()
        if self._prefix_mode == "auto":
            self.auto_model_prefix()

    @property
    def prefix(self):
        return self._prefix_mode

    @property
    def get_all_visible_items(self):
        new_items = self.__collection.__class__()
        new_items.LinkedListItem = ResultORMItem
        [new_items.append(**node.get_attributes())
         if not node.hidden else None
         for node in self.__collection]
        return new_items

    @property
    def hash_by_pk(self):
        return sum(map(lambda x: x.hash_by_pk, self.__collection))

    @property
    def container_cls(self):
        return type(self.__collection)

    def add_model_name_prefix(self):
        """ Изменит всю коллекцию, добавив префиксы названия таблицы к каждому значению полей у каждой ноды """
        self._prefix_mode = "add"
        new_collection = ServiceOrmContainer()
        new_collection.LinkedListItem = ResultORMItem
        i = iter(self)
        while True:
            try:
                node: ResultORMItem = next(i)
            except StopIteration:
                break
            else:

                node.add_model_name_prefix()
                new_collection.append(**node.get_attributes())
        self.__collection = new_collection

    def remove_model_prefix(self):
        """ Изменит всю коллекцию, удалив префиксы названия таблицы к каждому значению полей у каждой ноды """
        self._prefix_mode = "no-prefix"
        new_collection = ServiceOrmContainer()
        new_collection.LinkedListItem = ResultORMItem
        i = iter(self)
        while True:
            try:
                node: ResultORMItem = next(i)
            except StopIteration:
                break
            node.remove_model_name_prefix()
            new_collection.append(**node.get_attributes())
        self.__collection = new_collection

    def auto_model_prefix(self):
        """ Установить префикс с названием таблицы, только для столбцов нод,
        чьи наименования повторяются также в нодах от других таблиц, в остальных случаях - удалить префиксы """
        self.remove_model_prefix()
        self._prefix_mode = "auto"
        collection_copy = copy.deepcopy(self.__collection)
        for node in self.__collection:
            for other_node in collection_copy:
                if node.model.__name__ == other_node.model.__name__:
                    continue
                names_to_set_prefix = set(node.value).intersection(set(other_node.value))
                names_to_set_prefix.remove("ui_hidden")
                if not names_to_set_prefix:
                    continue
                node.add_model_name_prefix(tuple(names_to_set_prefix))

    def get_node(self, model, primary_key, value):
        return self.__collection.get_node(model, **{primary_key: value})

    def search_nones(self, model, **kwargs):
        return self.__collection.search_nodes(model, **kwargs)

    def all_nodes(self) -> Iterator:
        """ Для служебного пользования. Для UI использовать iter """
        return self.__collection.__iter__()

    def __iter__(self):
        return iter(self.get_all_visible_items)

    def __bool__(self):
        try:
            next(self.__iter__())
        except StopIteration:
            return False
        else:
            return True

    def __len__(self):
        return sum(map(lambda _: 1, self))

    def __getitem__(self, item) -> ResultORMItem:
        if not isinstance(item, (str, int)):
            raise TypeError
        if type(item) is int:
            if len(str(item)) > 3:
                for result in self:
                    if result.hash_by_pk == item:
                        return result
                    if result.__hash__() == item:
                        return result
        return self.__collection.__getitem__(item)  # По имени таблицы, по индексу

    def __contains__(self, item: Union):
        if type(item) is ResultORMItem:
            if hash(item) in map(hash, self):
                return True
            return False
        try:
            _ = self.__getitem__(item)
        except DoesNotExists:
            return False
        except TypeError:
            return False
        except IndexError:
            return False
        else:
            return True

    def __hash__(self):
        return hash(self.__collection)

    def __str__(self):
        return str(tuple([s.__str__() for s in self]))

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    @staticmethod
    def __convert_node_data(collection):
        """ В экземпляр поступает любой объект, производный от LinkedList.
         Конвертировать его в ServiceOrmContainer для инкапсуляции в текущий экземпляр. """
        new_collection = ServiceOrmContainer()
        new_collection.LinkedListItem = ResultORMItem
        [new_collection.append(node.model, node.get_primary_key_and_value(),
                               **({"ui_hidden": True
                                  if node.type == "_delete" else False}),
                               **node.value)
         for node in collection]
        return new_collection


class AbsSort(ABC):
    def __init__(self, model, column_name, *args, reverse=False, **kw):
        ModelTools.is_valid_model_instance(model)
        ModelTools.is_exists_column(model, column_name)
        if type(column_name) is not str:
            raise TypeError
        if not column_name:
            raise ValueError
        if type(reverse) is not bool:
            raise TypeError
        self._reverse = reverse
        self._field = column_name
        super().__init__(model, column_name, *args, reverse=reverse, **kw)
    
    @abstractmethod
    def _create_mapping(self) -> dict:
        """ Python >= 3.7! Реализовать словарь с определённым кол-вом ключей, расположенных в правильном порядке.
        В качестве значений установить пустые контейнеры, для нод."""
        ...

    @staticmethod
    def __is_model_has_column(model, column_name):
        if column_name not in model().column_names:
            raise AttributeError(f"У таблицы {model.__name__} отсутствует столбец {column_name}.")


class BaseSortSingleNodes:
    def __init__(self, model, column_name, nodes, *args, **kwargs):
        self._is_valid_node_items(nodes)
        self._input_nodes = nodes
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        self._nodes_in_sort = ServiceOrmContainer()  # Ноды, которые принимают участие в сортировке
        self._other_items = ServiceOrmContainer()  # Ноды, которые не участвуют в сортировке (доб в конец)

    def _select_nodes_to_sort(self):
        """ Вернуть ноды, которые будут участвовать в сортировке """
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        self._nodes_in_sort = ServiceOrmContainer()
        for node in self._input_nodes:
            if self._field in node.value:
                self._nodes_in_sort.append(**node.get_attributes())

    def _slice_other_nodes(self):
        """ Вырезать из коллекции ноды, ключевые поля у которых не заполнены.
        Не изменять исходную коллекцию. Присвоить в self._other_items.
        В дальнейшем их планируется добавить в конец сортированной коллекции """
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        self._other_items = ServiceOrmContainer()
        for node in self._input_nodes:
            if self._field not in node.value:
                self._other_items.append(**node.get_attributes())

    @staticmethod
    def _merge_mapping(data: dict):
        """ Словарь, который отсортирован, - 'сжать' его значения воедино, сохраняя последовательность """
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        output = ServiceOrmContainer()
        for val in data.values():
            output += val
        return output

    @staticmethod
    def _is_valid_node_items(items: "ServiceOrmContainer"):
        if not isinstance(items, ServiceOrmContainer):
            raise TypeError
        if not items:
            return
        if type(items[0]) is not ServiceOrmItem:
            raise TypeError


class BaseSortJoinedNodes:
    def __init__(self, model: CustomModel, field_name: str, items_group: tuple["ServiceOrmContainer"], **kwags):
        self._is_valid_nodes_chain(items_group)
        self._nodes_chain = items_group

    def _select_indexes(self, in_sort=True):
        """ Выбрать индексы коллекции, которые участвуют в сортировке или не участвуют """
        for n, collection in enumerate(self._nodes_chain):
            for node in collection:
                if in_sort:
                    if self._field in node.value:
                        if node.value[self._field] is not None:
                            yield n
                            continue
                        break
                    continue
                if self._field not in node.value:
                    break
                if node.value[self._field] is not None:
                    break
                yield n

    def _joined_nodes_hash_to_index_in_input_collection(self):
        return {self._nodes_chain[index].__hash__(): index for index in self._select_indexes()}

    def _node_primary_key_hash_to_joined_nodes_hash(self):
        d = {}
        for index in self._select_indexes():
            items_group = self._nodes_chain[index]
            for node in items_group:
                if self._field in node.value:
                    if node.value[self._field] is None:
                        continue
                    d.update({node.hash_by_pk: hash(items_group)})
        return d

    @staticmethod
    def _is_valid_nodes_chain(items_group: tuple["ServiceOrmContainer"]):
        if type(items_group) is not tuple:
            raise TypeError
        if any(map(lambda x: type(x) is not ServiceOrmContainer, items_group)):
            raise TypeError
        if not items_group:
            return
        if not items_group[0]:
            return
        if not isinstance(items_group[0][0], ServiceOrmItem):
            raise TypeError


class BaseLetterSort(AbsSort):
    def __init__(self, model, column_name, *args, **kwargs):
        super().__init__(model, column_name, *args, **kwargs)
        if not ModelTools.get_column_python_type(model, column_name) is str:
            raise TypeError("Тип значения столбца от ноды, с заданным столбцом для сортировки, "
                            "не является корректным для соритровки данного типа. Ожидался тип - str.")

    def _create_mapping(self) -> dict[str, Type[LinkedList]]:
        """  Заполнить словарь ключами """
        pattern = string.ascii_lowercase
        if not self._reverse:
            pattern = list(pattern)
            pattern.reverse()
            pattern = "".join(pattern)
        keys = map(lambda x: (x.upper(), x,), pattern)
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        return {key: ServiceOrmContainer() for key in keys}

    @staticmethod
    def _fill_mapping(data, nodes, target_column_name):
        for item in nodes:
            p = item.value[target_column_name][0]
            data[(p.upper(), p.lower(),)].append(**item.get_attributes())


class LetterSortSingleNodes(BaseLetterSort, BaseSortSingleNodes):
    def sort_by_alphabet(self):
        """ Инициализировать словарь,
        в котором ключами выступит первая буква из значения нашего ключевого слова, а значениями - очередь с нодой или нодами,
        содержащими данное поле и значение"""
        data_to_fill = self._create_mapping()
        self._select_nodes_to_sort()
        self._slice_other_nodes()
        self._fill_mapping(data_to_fill, self._nodes_in_sort, self._field)
        output = self._merge_mapping(data_to_fill)
        output += self._other_items
        return output

    def sort_by_string_length(self):
        def create_mapping(nodes):
            """ Создать словарь, где ключи - длина """
            ServiceOrmContainer.LinkedListItem = ServiceOrmItem
            d = {}
            for node in nodes:
                key = len(node.value[self._field])
                block = d.get(key, ServiceOrmContainer())
                block.append(**node.get_attributes())
                if key not in d:
                    d.update({key: block})
            return d
        self._slice_other_nodes()
        self._select_nodes_to_sort()
        output = ServiceOrmContainer()
        mapping = create_mapping(self._nodes_in_sort)
        mapping = dict(sorted(mapping.items(), key=lambda x: x[0], reverse=self._reverse))
        [output.__iadd__(nodes) for nodes in mapping.values()]
        return output + self._other_items


class LetterSortNodesChain(BaseLetterSort, BaseSortJoinedNodes):
    def sort_by_alphabet(self):
        """
        1) Выделить все участвующие в сортировке ноды в отдельный контейнер
        2) В словарь, с упорядоченными по алфавиту ключами, разложить в соответствии с первой буквой
        3) Создать словарь вида {хеш-ноды: хеш-исходной-группы-нод}
        4) Создать словарь вида {хеш-исходной-группы-нод: индекс-в-исходном-списке}
        5) Обойти словарь по values, обойти каждый контейнер по нодам,
        брать хеш ноды, извлекать хеш контейнера по словарю из п.3. Извлекать исходный индекс по словарю из п.4
        6) Обойти полученные в п.5 индексы, собрав элементы из исходного списка в результирующий список, вернуть.
        """
        def add_all_nodes_in_one_container():
            c = ServiceOrmContainer()
            ServiceOrmContainer.LinkedListItem = ServiceOrmItem
            for index in self._select_indexes():
                items_group = self._nodes_chain[index]
                for node in items_group:
                    if self._field in node.value:
                        c.append(**node.get_attributes())
            return c

        def fill_result(mapping):
            for group_by_letter in mapping.values():
                for node in group_by_letter:
                    nodes_group_hash = hash_item_to_group_mapping[node.hash_by_pk]
                    index_in_input = group_nodes_to_index_mapping[nodes_group_hash]
                    yield self._nodes_chain[index_in_input]

        mapping = self._create_mapping()
        joined_nodes = add_all_nodes_in_one_container()
        self._fill_mapping(mapping, joined_nodes, self._field)
        hash_item_to_group_mapping = self._node_primary_key_hash_to_joined_nodes_hash()
        group_nodes_to_index_mapping = self._joined_nodes_hash_to_index_in_input_collection()
        result = list(fill_result(mapping))
        result.extend([self._nodes_chain[index] for index in self._select_indexes(in_sort=False)])
        return result

    def sort_by_string_length(self):
        def create_mapping():
            m = {}
            for i in self._select_indexes():
                items_group = self._nodes_chain[i]
                ln = []
                for node in items_group:
                    if self._field in node.value:
                        ln.append(len(node.value[self._field]))
                if not ln:
                    continue
                m.update({min(ln): items_group})
            return m
        mapping = create_mapping()
        mapping = dict(sorted(mapping.items(), key=lambda k: k[0], reverse=self._reverse))
        output = list(mapping.values())
        output.extend([self._nodes_chain[i] for i in self._select_indexes(in_sort=False)])
        return output


class BaseNumberSort(AbsSort):
    def __init__(self, model, column_name, nodes, reverse=False):
        super().__init__(model, column_name, nodes, reverse=reverse)
        if not ModelTools.get_column_python_type(model, column_name) is int:
            raise TypeError("Тип значения столбца от ноды, с заданным столбцом для сортировки, "
                            "не является корректным для соритровки данного типа. Ожидался тип - int.")

    def _create_mapping(self, nodes: "ServiceOrmContainer") -> dict:
        return {node.value[self._field]: node for node in nodes}


class NumberSortSingleNodes(BaseNumberSort, BaseSortSingleNodes):
    def sort(self):
        self._select_nodes_to_sort()
        self._slice_other_nodes()
        mapping = self._create_mapping(self._nodes_in_sort)
        mapping = dict(sorted(mapping.items(), key=lambda x: x[0], reverse=self._reverse))
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        output = ServiceOrmContainer()
        [output.append(node_item=node) for node in mapping.values()]
        return output + self._other_items



class NumberSortNodesChain(BaseNumberSort, BaseSortJoinedNodes):
    def __init__(self, model: CustomModel, field_name: str, nodes_group: tuple["ServiceOrmContainer"], reverse=False):
        self._is_valid_nodes_chain(nodes_group)
        if ModelTools.get_column_python_type(model, field_name) is not int:
            raise TypeError("Тип значения столбца от ноды, с заданным столбцом для сортировки, "
                            "не является корректным для соритровки данного типа. Ожидался тип - int")
        super().__init__(model, field_name, nodes_group, reverse=reverse)

    def sort(self):
        nodes_in_sort = self._get_nodes_in_sort()
        mapping = self._create_mapping(nodes_in_sort)
        hash_item_to_group_mapping = self._node_primary_key_hash_to_joined_nodes_hash()
        group_nodes_to_index_mapping = self._joined_nodes_hash_to_index_in_input_collection()
        mapping = dict(sorted(mapping.items(), key=lambda x: x[0], reverse=self._reverse))
        output = []
        for node in mapping.values():
            items_group_hash = hash_item_to_group_mapping[node.hash_by_pk]
            index_from_input_collection = group_nodes_to_index_mapping[items_group_hash]
            output.append(self._nodes_chain[index_from_input_collection])
        output.extend(list(map(lambda index: self._nodes_chain[index], self._select_indexes(in_sort=False))))
        return output

    def _get_nodes_in_sort(self):
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        container = ServiceOrmContainer()
        for i in self._select_indexes():
            group = self._nodes_chain[i]
            for node in group:
                if self._field in node.value:
                    if node.value[self._field] is None:
                        continue
                    container.append(node_item=node)
        return container


class OrderByMixin(ABC):
    """ Реализация функционала для сортировки экземпляров ResultORMCollection в виде примеси для класса Result* """
    def __init__(self: Union["Result", "JoinSelectResult"], *args, **kwargs):
        def is_valid():
            if not hasattr(self, "_get_local_nodes"):
                raise AttributeError
            if not hasattr(self, "_get_nodes_from_database"):
                raise AttributeError
            if not callable(self.get_local_nodes):
                raise TypeError
            if not callable(self.get_nodes_from_database):
                raise AttributeError
        if not isinstance(self, (Result, JoinSelectResult)):
            raise TypeError("Использовать данный класс в наследовании! Как миксин")
        super().__init__(*args, **kwargs)
        self._is_sort = False
        self._by_primary_key: Optional[bool] = None
        self._by_column_name: Optional[str] = None
        self._by_time: Optional[bool] = None
        self._by_alphabet: Optional[bool] = None
        self._by_string_length: Optional[bool] = None
        self._reversed = False
        is_valid()

    def order_by(self, by_column_name, by_primary_key, by_create_time, length, alphabet, decr):
        self._by_primary_key = by_primary_key
        self._by_column_name = by_column_name
        self._by_alphabet = alphabet
        self._by_string_length = length
        self._by_time = by_create_time
        self._reversed = decr
        self._is_sort = True

    def get_nodes_from_database(self, **kwargs):
        if self._is_sort:
            kwargs.update(self._create_params_to_load_items())
        if hasattr(super(), "get_nodes_from_database"):
            nodes = super().get_nodes_from_database(**kwargs)
        else:
            nodes = self._get_nodes_from_database(**kwargs)
        if not self._is_sort:
            return nodes
        # todo Здесь можно сортировать ноды из базы и локалки между собой, но, пока что,
        #  я полагаю оставлять все локальные ноды "внизу"
        return nodes

    def get_local_nodes(self, **kwargs):
        if self._is_sort:
            kwargs.update(self._create_params_to_load_items())
        if hasattr(super(), "get_local_nodes"):
            return super().get_local_nodes(**kwargs)
        return self._get_local_nodes(**kwargs)

    def _create_params_to_load_items(self) -> dict:
        """ Создать параметры, передаваемые в геттер данных, на основе параметров,
        переданных и мемоизированных, со стороны пользователя. """
        output = {"model_in_sort": self._model}
        if not self._is_sort:
            return {}
        if self._by_primary_key:
            primary_key = ModelTools.get_primary_key_column_name(self._model)
            if ModelTools.get_column_python_type(self._model, primary_key) is int:
                output.update({"int_sort": primary_key})
            else:
                output.update({"string_sort": primary_key})
        if self._by_column_name:
            if ModelTools.get_column_python_type(self._model, self._by_column_name) is int:
                output.update({"int_sort": self._by_column_name})
            else:
                output.update({"string_sort": self._by_column_name})
        if self._by_string_length:
            output.update({"by_length": True})
        if self._by_alphabet:
            output.update({"by_alphabet": True})
        if self._by_time:
            output.update({"by_create_time": True})
        if self._reversed:
            output.update({"reversed_": True})
        return output

    @staticmethod
    def _check_exists_column_name(model, col_name):
        if col_name not in model().column_names:
            raise KeyError(f"В данной таблице отсутствует столбец {col_name}")

    def _is_valid_order_by_params(self, model, by_column_name, by_primary_key, by_create_time, length, alphabet, decr):
        if by_column_name is not None:
            if type(by_column_name) is not str:
                raise TypeError
            if not by_column_name:
                raise ValueError
            self._check_exists_column_name(model, by_column_name)
        if by_primary_key is not None:
            if type(by_primary_key) is not bool:
                raise TypeError
        if by_create_time is not None:
            if type(by_create_time) is not bool:
                raise TypeError
        if not isinstance(length, bool):
            raise TypeError
        if not isinstance(alphabet, bool):
            raise TypeError
        if type(decr) is not bool:
            raise TypeError
        if not sum([bool(by_column_name), bool(by_primary_key), bool(by_create_time)]) == 1:
            raise ValueError("Нужно выбрать один из вариантов")
        if by_create_time:
            if sum((length, alphabet,)):
                raise ValueError
        else:
            if ModelTools.get_column_python_type(model, ModelTools.get_primary_key_column_name(model)) is not int:
                if not sum((length, alphabet)) == 1:
                    raise ValueError
        if by_primary_key:
            if ModelTools.get_column_python_type(model, ModelTools.get_primary_key_column_name(model)) is int:
                if length or alphabet:
                    raise ValueError
        if by_column_name:
            if ModelTools.get_column_python_type(model, by_column_name) is int:
                if length or alphabet:
                    raise ValueError


class OrderBySingleResultMixin(OrderByMixin):
    """ Реализация для 'одиночного результата',- запрос к одной таблице. См Tool.get_items() """
    def __init__(self: Union["Result", "OrderBySingleResultMixin"], *a, **k):
        if not isinstance(self, Result):
            raise TypeError
        super().__init__(*a, **k)
        if not hasattr(self, "_model"):
            raise AttributeError
        ModelTools.is_valid_model_instance(self._model)

    def order_by(self, by_column_name: Optional[str] = None, by_primary_key: Optional[bool] = None,
                 by_create_time: Optional[bool] = None, length: bool = False, alphabet: bool = False,
                 decr: bool = False):
        self._is_valid_order_by_params(self._model, by_column_name, by_primary_key, by_create_time, length, alphabet,
                                       decr)
        super().order_by(by_column_name, by_primary_key, by_create_time, length, alphabet,
                                       decr)


class OrderByJoinResultMixin(OrderByMixin, ModelTools):
    """ Реализация для запросов с join. См Tool.join_select() """
    def __init__(self: Union["JoinSelectResult", "OrderByJoinResultMixin"], *a, **k):
        if not isinstance(self, JoinSelectResult):
            raise TypeError
        self._model = None
        super().__init__(*a, **k)
        if not hasattr(self, "_models"):
            raise AttributeError
        if type(self._models) is not tuple:
            raise TypeError
        [ModelTools.is_valid_model_instance(model) for model in self._models]

    def order_by(self, model, by_column_name: Optional[str] = None,
                 by_primary_key: Optional[bool] = None,
                 by_create_time: Optional[bool] = None, length: bool = False, alphabet: bool = False,
                 decr: bool = False):
        self.is_valid_model_instance(model)
        self._is_valid_order_by_params(model, by_column_name, by_primary_key, by_create_time, length, alphabet,
                                       decr)
        self._model = model
        super().order_by(by_column_name, by_primary_key, by_create_time, length, alphabet, decr)

    def __sort_nodes_by_create_time(self):
        def nodes_map():
            for nodes_group in self:
                yield min(map(lambda node: node.created_at, nodes_group)), nodes_group
        return list(dict(sorted(nodes_map(), key=operator.itemgetter(0))).values())

    def _is_valid_order_by_params(self, model, by_column_name, by_primary_key, by_create_time, length, alphabet, decr):
        QueueItem.is_valid_model_instance(model)
        if by_column_name is not None:
            self.__check_exists_column_name(model, by_column_name)
        if model not in self._models:
            raise ValueError
        self._model = model
        super()._is_valid_order_by_params(model, by_column_name, by_primary_key, by_create_time, length, alphabet, decr)


class SQLAlchemyQueryManager:
    MAX_RETRIES: Union[int, Literal["no-limit"]] = MAX_RETRIES

    def __init__(self, connection_path: str, nodes: "Queue"):
        def valid_node_type():
            if type(nodes) is not Queue:
                raise ValueError
        if not isinstance(nodes, Queue):
            raise TypeError
        if type(connection_path) is not str:
            raise TypeError
        valid_node_type()
        self.path = connection_path
        self._node_items = nodes
        self.remaining_nodes = Queue()  # Отложенные для следующей попытки
        self._sorted: list[Queue] = []  # [[save_point_group {pk: val,}], [save_point_group]...]
        self._query_objects: dict[Union[Insert, Update, Delete]] = {}  # {node_index: obj}

    def start(self):
        self._sort_nodes()  # Упорядочить, разбить по savepoint
        self._manage_queries()  # Обратиться к left_node.make_query, - собрать объекты sql-иньекций
        self._open_connection_and_push()

    def _manage_queries(self):
        if self._query_objects:
            return self._query_objects
        for node_grop in self._sort_nodes():
            for left_node in node_grop:
                query = left_node.make_query()
                self._query_objects.update({left_node.index: query}) if query is not None else None
        return self._query_objects

    def _open_connection_and_push(self):
        sorted_data = self._sort_nodes()
        if not sorted_data:
            return
        if not self._query_objects:
            return
        session = Tool.connection.database
        while sorted_data:
            node_group = sorted_data.pop()
            if not node_group:
                break
            multiple_items_in_transaction = True if len(node_group) > 1 else False
            if multiple_items_in_transaction:
                point = session.begin_nested()
            else:
                point = session
            items_to_commit = []
            for node in node_group:
                dml = self._query_objects.get(node.index)
                items_to_commit.append(dml)
            if multiple_items_in_transaction:
                try:
                    session.add_all(items_to_commit)
                except SQLAlchemyError:
                    self.remaining_nodes += node_group
                    point.rollback()
            else:
                try:
                    session.execute(items_to_commit.pop(0))
                except SQLAlchemyError:
                    self.remaining_nodes += node_group
            try:
                session.commit()
            except SQLAlchemyError:
                self.remaining_nodes += node_group
            except DatabaseException:
                self.remaining_nodes += node_group
        self._sorted = []
        self._query_objects = {}

    def _sort_nodes(self) -> list[Queue]:
        """ Сортировать ноды по признаку внешних ключей, определить точки сохранения для транзакций """
        def make_sort_container(n: QueueItem, linked_nodes: Queue):
            """
            Рекурсивно искать ноды с внешними ключами
            O(m) * (O(n) + O(j)) = O(n) * O(m) = O(n)
            """
            related_nodes = self._node_items.get_related_nodes(n)  # O(n)
            linked_nodes.add_to_head(new_item=n) if n.ready else None
            if not related_nodes:
                return linked_nodes
            for node in related_nodes:
                return make_sort_container(node, linked_nodes)
        if self._sorted:
            return self._sorted
        node_ = self._node_items.dequeue()
        while node_:
            if self.MAX_RETRIES == "no-limit" or node_.retries < self.MAX_RETRIES:
                if node_.ready:
                    recursion_result = make_sort_container(node_, Queue())
                    self._sorted.append(recursion_result)
                else:
                    self.remaining_nodes.append(**node_.get_attributes())
            node_ = self._node_items.dequeue()
        return self._sorted


class ServiceOrmItem(QueueItem, AbsNode):
    """ Данный тип нод используется для вывода результата """
    @property
    def hash_by_pk(self):
        str_ = "".join(map(str, self.get_primary_key_and_value(as_tuple=True)))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def __hash__(self):
        value = self.value
        if ModelTools.is_autoincrement_primary_key(self.model):
            del value[self.get_primary_key_and_value(only_key=True)]
        str_ = "".join(map(lambda x: str(x), itertools.chain(*value.items())))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def __eq__(self, other: "QueueItem"):
        if type(other) is not type(self):
            return False
        return self.__hash__() == hash(other)

    @staticmethod
    def _is_valid_dml_type(*a, **k):
        """ Валидация типа ноды в рамках DML(SQL). Отключить валидацию """
        pass

    @staticmethod
    def _field_names_validation(*a, **k):
        """ Так как данный тип нод используется в формировании результатов, отключить валидацию """
        pass

    @staticmethod
    def _is_valid_column_type_in_sql_type(*a):
        """ Так как данный тип нод используется в формировании результатов, отключить валидацию """
        pass

    @staticmethod
    def _is_valid_primary_key(d: dict, model: CustomModel):
        pass

    @property
    def retries(self):
        return None

    def make_query(self):
        pass


class ServiceOrmContainer(Queue):
    """ Контейнер с композицией результата """
    LinkedListItem: Union[ServiceOrmItem, ResultORMItem] = ServiceOrmItem

    @property
    def hash_by_pk(self):
        return sum(map(lambda x: x.hash_by_pk, self))

    def __getitem__(self, model_name_or_index: Union[str, int]) -> Union[DoesNotExists, "ServiceOrmContainer", "ResultORMItem"]:
        if not isinstance(model_name_or_index, (str, int,)):
            raise TypeError
        if type(model_name_or_index) is int:
            return super().__getitem__(model_name_or_index)
        nodes = self.__class__()
        nodes.LinkedListItem = self.LinkedListItem
        for node in self:
            if node.model.__name__ == model_name_or_index:
                nodes.append(**node.get_attributes())
        if len(nodes) > 1:
            return nodes
        if nodes:
            return nodes[0]
        raise DoesNotExists

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if not len(self) == len(other):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return sum(map(hash, self))


class ConnectionManager:
    """ Данный класс отвечает за установку соединений с базой данных и кеширующим сервером """
    DATABASE_CONNECTION_ALIVE_SEC = 3
    CACHE_CONNECTION_ALIVE_SEC = 3
    _database_connection_timer = None
    _cache_connection_timer = None
    _database_session = None
    _cache_client = None

    def __new__(cls):
        super().__new__(cls)
        engine = create_engine(DATABASE_PATH)
        cls.session_f = session_factory(bind=engine)
        cls.cache_pool = PooledClient(MEMCACHE_PATH, max_pool_size=20, serde=DillSerde)
        return cls

    @classmethod
    @property
    def cache(cls):
        if not cls._cache_client:
            cls._cache_client = cls.__create_cache_connection()
            cls.__start_timer_cache_connection()
        return cls._cache_client

    @classmethod
    def drop_cache(cls):
        cls.cache.flush_all()

    @classmethod
    @property
    def items(cls) -> Queue:
        """ Вернуть локальные элементы """
        return cls.cache.get("ORMItems", Queue())

    @classmethod
    @property
    def database(cls):
        if not cls._database_session:
            cls._database_session = cls.__create_database_connection()
            cls.__start_timer_database_connection()
        return cls._database_session

    @classmethod
    def __create_cache_connection(cls):
        return cls.cache_pool

    @classmethod
    def __create_database_connection(cls):
        return scoped_session(cls.session_f)

    @classmethod
    def __start_timer_database_connection(cls):
        cls._database_connection_timer = threading.Timer(float(cls.DATABASE_CONNECTION_ALIVE_SEC),
                                                         cls.__close_connection_database)
        cls._database_connection_timer.start()

    @classmethod
    def __start_timer_cache_connection(cls):
        cls._cache_connection_timer = threading.Timer(float(cls.CACHE_CONNECTION_ALIVE_SEC), cls.__close_connection_cache)
        cls._cache_connection_timer.start()

    @classmethod
    def __close_connection_database(cls):
        cls._database_session.remove()
        cls._database_session = None

    @classmethod
    def __close_connection_cache(cls):
        cls._cache_client.close()


class PrimaryKeyFactory(ModelTools):
    connection = ConnectionManager()

    @classmethod
    def create_primary(cls, model, **data) -> dict:
        cls.is_valid_model_instance(model)
        name = cls.get_primary_key_column_name(model)
        pk = cls._select_primary_key_value_from_node_data(model, data)
        if pk is not None:
            pk_from_db = cls._get_highest_autoincrement_pk_from_database(model)
            if pk_from_db is not None:
                if pk_from_db >= pk[name]:
                    data = cls._update_node_data_from_database_by_pk(model, pk, data)
                    data.update(data)
            return data
        data = cls._update_node_data_from_database_by_unique_column(model, data)
        if name in data:
            pk = {name: data[name]} if data else None
            data = cls._update_node_data_from_local_nodes_by_unique_column(model, data)
            del data[name]
            data.update(pk)
            return data
        data = cls._update_node_data_from_local_nodes_by_unique_column(model, data)
        if name in data:
            return data
        default_value = cls.get_default_column_value_or_function(model, name)
        if default_value is not None:
            data.update({name: default_value.arg(None)})
            return data
        if cls.is_autoincrement_primary_key(model):
            pk_value_db = cls._get_highest_autoincrement_pk_from_database(model)
            pk_value_local = cls._get_highest_autoincrement_pk_from_local(model)
            if pk_value_local and pk_value_db:
                data.update({name: pk_value_local + pk_value_db + 1})
                return data
            if pk_value_db:
                data.update({name: pk_value_db + 1})
                return data
            if pk_value_local:
                data.update({name: pk_value_local + 1})
                return data
            data.update({name: 1})
            return data
        raise NodePrimaryKeyError

    @classmethod
    def _update_node_data_from_database_by_pk(cls, model, primary_key: dict, data: dict) -> dict:
        cls.is_valid_model_instance(model)
        if type(primary_key) is not dict:
            raise TypeError
        if type(data) is not dict:
            raise TypeError
        pk = cls._select_primary_key_value_from_node_data(model, data)
        if not pk == primary_key:
            raise NodePrimaryKeyError
        select_result = cls.connection.database.query(model).filter_by(**primary_key).all()
        if select_result:
            select_result = select_result[0].__dict__
            select_result.update(data)
            select_result = cls.__replace_insert_dml_on_update(select_result)
            return cls.__clear_node_data(model, select_result)
        data = cls.__replace_insert_dml_on_insert(data)
        return data

    @classmethod
    def _update_node_data_from_database_by_unique_column(cls, model, data: dict) -> dict:
        cls.is_valid_model_instance(model)
        if not isinstance(data, dict):
            raise TypeError
        unique_columns = tuple(cls.get_unique_columns(model, data))
        unique_data = {key: data[key] for key in data if key in unique_columns}
        select_result = None
        if unique_data:
            select_result = cls.connection.database.query(model).filter_by(**unique_data).all()
        if select_result:
            select_result = select_result[0].__dict__
            select_result.update(data)
            select_result = cls.__replace_insert_dml_on_update(select_result)
            return cls.__clear_node_data(model, select_result)
        return data

    @classmethod
    def _update_node_data_from_local_nodes_by_unique_column(cls, model, data: dict):
        unique_columns = tuple(cls.get_unique_columns(model, data))
        unique_data = {key: data[key] for key in data if key in unique_columns}
        node = None
        if unique_data:
            node = cls.connection.items.search_nodes(model, **unique_data)
        if not node:
            return data
        value = node[0].value
        value.update(data)
        return value

    @classmethod
    def _select_primary_key_value_from_node_data(cls, model, data) -> Optional[dict]:
        cls.is_valid_model_instance(model)
        field_name = cls.get_primary_key_column_name(model)
        try:
            value = data[field_name]
        except KeyError:
            return
        else:
            return {field_name: value}

    @classmethod
    def _get_highest_autoincrement_pk_from_local(cls, model) -> Optional[int]:
        try:
            val = max(map(lambda x: x.get_primary_key_and_value(only_value=True),
                          cls.connection.items.search_nodes(model)))
        except ValueError:
            return None
        return val

    @classmethod
    def _get_highest_autoincrement_pk_from_database(cls, model) -> int:
        cls.is_valid_model_instance(model)
        return cls.connection.database.query(func.max(getattr(model, ModelTools.get_primary_key_column_name(model)))).scalar()

    @classmethod
    def __clear_node_data(cls, model, data):
        """ Отфильтровать возможные лишние данные при получении данных из бд """
        cls.is_valid_model_instance(model)
        result = {}
        for name in model().column_names:
            result.update({name: data[name]})
        for key, value in data.items():
            if key in RESERVED_WORDS:
                result.update({key: value})
        return result

    @staticmethod
    def __replace_insert_dml_on_update(node_data: dict):
        if node_data["_delete"]:
            return node_data
        node_data.update({"_insert": False, "_update": True})
        return node_data

    @staticmethod
    def __replace_insert_dml_on_insert(node_data: dict):
        if node_data["_delete"]:
            return node_data
        node_data.update({"_insert": True, "_update": False})
        return node_data


class Tool(ModelTools):
    """
    Главный класс
    1) Инициализация
        LinkToObj = Tool()
    2) Установка ссылки на класс модели Flask-SqlAlchemy
        LinkToObj.set_model(CustomModel)
    3) Использование
        LinkToObj.set_item(name, data, **kwargs) - Установка в очередь, обнуление таймера
        LinkToObj.get_item(name, **kwargs) - получение данных из бд и из нод в локальном расположении
        LinkToObj.get_items(model=None) - получение данных из бд и из нод в локальном расположении
        LinkToObj.release() - высвобождение очереди с попыткой сохранить объекты в базе данных
        в случае неудачи нода переносится в конец очереди
        LinkToObj.remove_items - принудительное изъятие ноды из очереди.
    """
    RELEASE_INTERVAL_SECONDS = RELEASE_INTERVAL_SECONDS
    CACHE_LIFETIME_HOURS = CACHE_LIFETIME_HOURS
    _timer: Optional[threading.Timer] = None
    _model_obj: Optional[Type[CustomModel]] = None  # Текущий класс модели, присваиваемый автоматически всем экземплярам при добавлении в очередь
    _was_initialized = False
    connection = ConnectionManager()

    @classmethod
    def set_model(cls, obj):
        """
        :param obj: Кастомный класс модели Flask-SQLAlchemy из модуля models
        """
        cls.is_valid_model_instance(obj)
        cls._model_obj = obj
        cls._is_valid_config()
        return cls

    @classmethod
    def set_item(cls, _model=None, _insert=False, _update=False,
                 _delete=False, _ready=False, **value):
        model = _model or cls._model_obj
        if isinstance(_model, str):
            model = ModelTools.import_model(_model)
        cls.is_valid_model_instance(model)
        if not all((isinstance(v, (int, str, type(None))) for v in value.values())):
            raise NodeColumnValueError
        items: Queue = cls.connection.items
        items.LinkedListItem = QueueItem
        attrs = {"_model": model, "_ready": _ready,
                 "_insert": _insert, "_update": _update,
                 "_delete": _delete}
        attrs.update(PrimaryKeyFactory.create_primary(model, **{**value, **attrs}))
        items.enqueue(**attrs)
        cls.__set_cache(items)
        cls._timer = None
        cls._timer = cls._init_timer()

    @classmethod
    def get_items(cls, _model: Optional[Type[CustomModel]] = None, _db_only=False, _queue_only=False, **attrs) -> "Result":  # todo: придумать пагинатор
        """
        1) Получаем запись из таблицы в виде словаря (CustomModel.query.all())
        2) Получаем данные из кеша, все элементы, у которых данная модель
        3) db_data.update(quque_data)
        """
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)

        def select_from_db(offset=0, limit=float("inf"),
                           int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                           by_length=False, by_alphabet=False, by_create_time=False,
                           reversed_=False, model_in_sort=None):
            try:
                items_db = cls.connection.database.query(model).offset(offset).limit(limit)
            except OperationalError:
                print("Ошибка соединения с базой данных! Смотри константу 'DATABASE_PATH' в модуле models.py, "
                      "такая проблема обычно возникает из-за авторизации. Смотри пароль!!!")
                raise OperationalError
            if attrs:
                items_db = items_db.filter_by(**attrs)
            if int_sort or string_sort:
                if reversed_:
                    items_db = items_db.order_by(desc(int_sort or string_sort))
                else:
                    items_db = items_db.order_by(int_sort or string_sort)
            items_db = items_db.all()

            def add_to_queue():
                result = Queue()
                result.LinkedListItem = ServiceOrmItem
                for item in items_db:
                    col_names = model().column_names
                    result.append(**{key: item.__dict__[key] for key in col_names}, _insert=True, _model=model)
                return result
            return add_to_queue()

        def select_from_cache(left_border=0, right_border=float("inf"),
                              int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                              by_length=False, by_alphabet=False, by_create_time=False,
                              reversed_=False, model_in_sort=None):
            return cls.connection.items.search_nodes(model, **attrs)
        return Result(get_nodes_from_database=select_from_db, get_local_nodes=select_from_cache,
                      only_local=_queue_only, only_database=_db_only, model=model, where=attrs)

    @classmethod
    def join_select(cls, *models: Iterable[CustomModel], _on: Optional[dict] = None,
                    _db_only=False, _queue_only=False, **where) -> "JoinSelectResult":
        """
        join_select(model_a, model,b, on={model_b: 'model_a.column_name'})

        :param where: modelName: {column_name: some_val}
        :param _on: modelName.column1: modelName2.column2
        :param _db_only: извлечь только sql inner join
        :param _queue_only: извлечь только из queue
        :return: специальный итерируемый объект класса JoinSelectResult, который содержит смешанные данные из локального
        хранилища и БД
        """
        def valid_params():
            def is_self_references():
                """ Ссылается ли таблица своим внешним ключом сама на себя """
                if len(models) > 1:
                    return False
                s = set()
                for left, right in _on.items():
                    left_model = left.split(".")[0]
                    right_model = right.split(".")[0]
                    s.update((left_model, right_model,))
                if len(s) > 1:
                    return False
                return True

            def check_transitivity_on_params():
                """ Проверка условия транзитивности для параметра on, в котором указывается взаимосвязь между таблицами.
                Если таблиц n, то валидация ситуативная, исходя из их кол-ва:
                Если > 1, то важно проверить следующий момент: A->B;C->D (Исключить не участвующие таблицы)
                Если >= 2, то проверить рекурсивную связь, которой быть не должно
                Если == 1 (таблица ссылается сама на себя), проверить лишние таблицы в параметре on
                 """
                if is_self_references():
                    unique_tables = set()
                    for left_table_dot_field, right_table_dot_field in _on.items():
                        left_table = left_table_dot_field.split(".")[0]
                        right_table = right_table_dot_field.split(".")[0]
                        unique_tables.update((left_table, right_table,))
                    if len(unique_tables) > 1:
                        raise ValueError
                    if not models[0].__name__ == next(iter(unique_tables)):
                        raise ValueError
                    return
                if len(models) >= 2:
                    models_complimentary = []
                    for left_table_dot_field, right_table_dot_field in _on.items():
                        left_model = left_table_dot_field.split(".")[0]
                        right_model = right_table_dot_field.split(".")[0]
                        models_complimentary.append((left_model, right_model,))
                    reversed_models = [list(t).reverse() for t in models_complimentary]
                    for t in models_complimentary:
                        if t in reversed_models:
                            raise ValueError("Рекурсивная связь в аргументе ON недопустима")
                if len(models) > 1:
                    total_models = {model.__name__ for model in models}
                    models_at_on = set()
                    for left_table_dot_field, right_table_dot_field in _on.items():
                        left_model = left_table_dot_field.split(".")[0]
                        right_model = right_table_dot_field.split(".")[0]
                        models_at_on.update((left_model, right_model,))
                    if not len(total_models) == len(models_at_on):
                        raise ValueError
            [cls.is_valid_model_instance(m) for m in models]
            if not models:
                raise ValueError
            if _on is None:
                raise ValueError("Необходим аргумент on={model_b.column_name: 'model_a.column_name'}")
            if type(_on) is not dict:
                raise TypeError
            if where:
                if type(where) is not dict:
                    raise TypeError
                for v in where.values():
                    if not isinstance(v, dict):
                        raise TypeError
                    for key, value in v.items():
                        if type(key) is not str:
                            raise TypeError("Наименование столбца может быть только строкой")
                        if not isinstance(value, (str, int,)):
                            raise TypeError
            for left_table_dot_field, right_table_dot_field in _on.items():
                if not type(left_table_dot_field) is str or not isinstance(right_table_dot_field, str):
                    raise TypeError("...on={model_b.column_name: 'model_a.column_name'}")
                if not all(itertools.chain(*[[len(x) for x in i.split(".")] for t in _on.items() for i in t])):
                    raise AttributeError("...on={model_b.column_name: 'model_a.column_name'}")
                left_model = left_table_dot_field.split(".")[0]
                right_model = right_table_dot_field.split(".")[0]
                if len(left_table_dot_field.split(".")) != 2 or len(right_table_dot_field.split(".")) != 2:
                    raise AttributeError("...on={model_b.column_name: 'model_a.column_name'}")
                if left_model not in (m.__name__ for m in models):
                    raise ValueError(f"Класс модели {left_model} не найден")
                if right_model not in (m.__name__ for m in models):
                    raise ValueError(f"Класс модели {right_model} не найден")
                left_model_field = left_table_dot_field.split(".")[1]
                right_model_field = right_table_dot_field.split(".")[1]
                if not getattr({m.__name__: m for m in models}[left_model], left_model_field, None):
                    raise AttributeError(f"Столбец {left_model_field} у таблицы {left_model} не найден")
                if not getattr({m.__name__: m for m in models}[right_model], right_model_field, None):
                    raise AttributeError(f"Столбец {right_model_field} у таблицы {right_model} не найден")
            if not is_self_references():
                if len(models) == 2:
                    if not len(_on.keys()) + len(_on.values()) == len(models):
                        raise ValueError(
                            "Правильный способ работы с данным методом: join_select(model_a, model,b, on={model_b.column_name: 'model_a.column_name'})"
                        )
                if len(models) > 2:
                    if not len(_on.keys()) + len(_on.values()) == len(models) + 1:
                        raise ValueError(
                            "Правильный способ работы с данным методом: join_select(model_a, model,b, on={model_b.column_name: 'model_a.column_name'})"
                        )
            else:
                if not len(models) == 1:
                    raise ValueError("При join запросе таблицы 'самой на себя', должна быть указана 1 таблица, "
                                     "к которой выполняется запрос")
            check_transitivity_on_params()
        valid_params()

        def collect_db_data(offset=0, limit=float("inf"),
                            int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                            by_length=False, by_alphabet=False, by_create_time=False,
                            reversed_=False, model_in_sort=None):
            def create_request() -> str:  # O(n) * O(m)
                s = f"db.query({', '.join(map(lambda x: x.__name__, models))}).filter("
                on_keys_counter = 0
                for left_table_dot_field, right_table_dot_field in _on.items():  # O(n)
                    s += f"{left_table_dot_field} == {right_table_dot_field}"
                    on_keys_counter += 1
                    if not on_keys_counter == len(_on):
                        s += ", "
                s += ")" if offset is None else f").offset({offset})"
                s += f".limit({limit})" if limit is not None else ""
                if where:
                    on_keys_counter = 0
                    s += f".filter("
                    for table_name, column_and_value in where.items():
                        for left_table_and_column, right_table_and_column in column_and_value.items():  # O(t)
                            s += f"{table_name}.{left_table_and_column} == '{right_table_and_column}'"
                            if on_keys_counter < len(where) - 1:  # O(1)
                                s += ", "
                            on_keys_counter += 1
                        s += ")" if on_keys_counter == where.__len__() else ""
                if model_in_sort:
                    if by_length:
                        s += f".order_by("
                        if reversed_:
                            s += "desc("
                        s += f"func.length({model_in_sort.__tablename__}.{int_sort or string_sort}))"
                        if reversed_:
                            s += ")"
                    if by_alphabet:
                        if reversed_:
                            s += "desc("
                        s += f".order_by({model_in_sort.__tablename__}.{int_sort or string_sort})"
                return s

            def add_db_items_to_orm_queue() -> Iterator[ServiceOrmContainer]:  # O(i) * O(k) * O(m) * O(n) * O(j) * O(l)
                data = query.all()
                for data_row in data:  # O(i)
                    row = ServiceOrmContainer()
                    row.LinkedListItem = ServiceOrmItem
                    for join_select_result in data_row:
                        all_column_names = getattr(type(join_select_result), "column_names")
                        r = {col_name: col_val for col_name, col_val in join_select_result.__dict__.items()
                             if col_name in all_column_names}  # O(n) * O(j)
                        row.append(_model=join_select_result.__class__, _insert=True, **r)  # O(l)
                    yield row
            sql_text = create_request()
            query: Query = eval(sql_text, {"db": cls.connection.database}, ChainMap(*list(map(lambda x: {x.__name__: x}, models)),
                                                                                    {"desc": desc, "func": func}))
            return add_db_items_to_orm_queue()

        def collect_all_local_nodes():
            heap = Queue()
            Queue.LinkedListItem = ServiceOrmItem
            temp = cls.connection.items
            for model in models:  # O(n)
                heap += temp.search_nodes(model, **where.get(model.__name__, {}))
            return heap

        def collect_local_data(left_border=0, right_border=float("inf"),
                               int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                               by_length=False, by_alphabet=False, by_create_time=False,
                               reversed_=False, model_in_sort=None) -> Iterator[ServiceOrmContainer]:
            def collect_node_values(on_keys_or_values: Union[dict.keys, dict.values]):
                for node in collect_all_local_nodes():
                    for table_and_column in on_keys_or_values:
                        table, table_column = table_and_column.split(".")
                        if table == node.model.__name__:
                            if table_column in node.value:
                                yield {node.model.__name__: node}

            def compare_by_matched_fk() -> Iterator:
                model_left_primary_key_and_value = collect_node_values(_on.keys())  # O(u)
                model_right_primary_key_and_value = tuple(collect_node_values(_on.values()))  # O(2u)
                for left_data in model_left_primary_key_and_value:  # O(n)
                    left_model_name, left_node = itertools.chain.from_iterable(left_data.items())  # O(j)
                    for right_data in model_right_primary_key_and_value:  # O(k)
                        right_model_name, right_node = itertools.chain.from_iterable(right_data.items())  # O(l)
                        raw = ServiceOrmContainer()  # O(1)
                        raw.LinkedListItem = ServiceOrmItem
                        for left_table_dot_field, right_table_dot_field in _on.items():  # O(b)
                            left_table_name_in_on, left_table_field_in_on = left_table_dot_field.split(".")  # O(a)
                            right_table_name_in_on, right_table_field_in_on = right_table_dot_field.split(".")  # O(a)
                            if left_model_name == left_table_name_in_on and right_model_name == right_table_name_in_on:  # O(c * v) + O(c1 * v1)
                                if left_node.value.get(left_table_field_in_on, None) == \
                                        right_node.value.get(right_table_field_in_on, None):  # O(1) + O(m1) * O(1) + O(m2) = O(m1 * m2)
                                    raw.append(**left_node.get_attributes())
                                    raw.append(**right_node.get_attributes())
                        if raw:
                            yield raw
                            
            def sort_(node_items: tuple[ServiceOrmContainer]):
                if int_sort:
                    return NumberSortNodesChain(model_in_sort, int_sort, node_items, reverse=reversed_).sort()
                if string_sort:
                    instance = LetterSortNodesChain(model_in_sort, string_sort, node_items, reverse=reversed_)
                    if by_alphabet:
                        return instance.sort_by_alphabet()
                    if by_length:
                        return instance.sort_by_string_length()
                if by_create_time:
                    ...  # todo
                return node_items
            return sort_(tuple(compare_by_matched_fk()))
        return JoinSelectResult(get_nodes_from_database=collect_db_data, get_local_nodes=collect_local_data,
                                only_database=_db_only, only_local=_queue_only, get_all_local_nodes=collect_all_local_nodes,
                                models=models, where=where, on=_on)

    @classmethod
    def get_node_dml_type(cls, node_pk_value: Union[str, int], model=None) -> Optional[str]:
        """ Получить тип операции с базой, например '_update', по названию ноды, если она найдена, иначе - None
        :param node_pk_value: значение поля первичного ключа
        :param model: кастомный объект, смотри модуль database/models
        """
        model = model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(node_pk_value, (str, int,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        left_node = cls.connection.items.get_node(model, **{primary_key_field_name: node_pk_value})
        return left_node.type if left_node is not None else None

    @classmethod
    def remove_items(cls, node_or_nodes: Union[Union[int, str], Iterable[Union[str, int]]], model=None):
        """
        Удалить ноду из очереди на сохранение
        :param node_or_nodes: значение для поля первичного ключа, одно или несколько
        :param model: кастомный объект, смотри модуль database/models
        """
        model = model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(node_or_nodes, (tuple, list, set, frozenset, str, int,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        items = cls.connection.items
        if isinstance(node_or_nodes, (str, int,)):
            items.remove(model, **{primary_key_field_name: node_or_nodes})
        if isinstance(node_or_nodes, (tuple, list, set, frozenset)):
            for pk_field_value in node_or_nodes:
                if not isinstance(pk_field_value, (int, str,)):
                    raise TypeError
                items.remove(model, **{primary_key_field_name: pk_field_value})
        cls.__set_cache(items)

    @classmethod
    def remove_field_from_node(cls, pk_field_value, field_or_fields: Union[Iterable[str], str], _model=None):
        """
        Удалить поле или поля из ноды, которая в очереди
        :param pk_field_value: значения поля первичного ключа (по нему ищется нода)
        :param field_or_fields: изымаемые поля
        :param _model: кастомная модель SQLAlchemy
        """
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(field_or_fields, (tuple, list, set, frozenset, str,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        old_node = cls.connection.items.get_node(model, **{primary_key_field_name: pk_field_value})
        if not old_node:
            return
        node_data = old_node.get_attributes()
        if isinstance(field_or_fields, (list, tuple, set, frozenset)):
            if set.intersection(set(field_or_fields), set(RESERVED_WORDS)):
                raise NodeAttributeError
            if primary_key_field_name in field_or_fields:
                raise NodePrimaryKeyError("Нельзя удалить поле, которое является первичным ключом")
            for field in field_or_fields:
                if field in node_data:
                    del node_data[field]
        if type(field_or_fields) is str:
            if field_or_fields in RESERVED_WORDS:
                raise NodeAttributeError
            if primary_key_field_name == field_or_fields:
                raise NodePrimaryKeyError("Нельзя удалить поле, которое является первичным ключом")
            if field_or_fields in node_data:
                del node_data[field_or_fields]
        container = cls.connection.items
        container.enqueue(**node_data)
        cls.__set_cache(container)

    @classmethod
    def is_node_from_cache(cls, _model=None, **attrs) -> bool:
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)
        items = cls.connection.items.search_nodes(model, **attrs)
        if len(items) > 1:
            warnings.warn(f"Нашлось больше одной ноды/нод, - {len(items)}: {items}")
        if items:
            return True
        return False

    @classmethod
    def is_node_ready(cls, _model=None, **attrs):
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)
        items = cls.connection.items.search_nodes(model, **attrs)
        if not items:
            return
        if not len(items) == 1:
            return
        return items[0].ready

    @classmethod
    def release(cls) -> None:
        """
        Этот метод стремится высвободить очередь сохраняемых объектов,
        путём итерации по ним, и попыткой сохранить в базу данных.
        :return: None
        """
        def actualize_node_data(remaining_nodes: Queue):
            """ Обновить данные нод, которые не удалось закоммитить, из базы данных """
            updated_remaining_nodes = Queue()
            updated_remaining_nodes.LinkedListItem = QueueItem
            for node in remaining_nodes:
                node_data = PrimaryKeyFactory.create_primary(node.model, **node.get_attributes())
                updated_remaining_nodes.enqueue(**node_data)
            return updated_remaining_nodes
        database_adapter = SQLAlchemyQueryManager(DATABASE_PATH, cls.connection.items)
        database_adapter.start()
        cls.__set_cache(actualize_node_data(database_adapter.remaining_nodes))
        sys.exit()

    @classmethod
    def _init_timer(cls):
        timer = threading.Timer(cls.RELEASE_INTERVAL_SECONDS, cls.release)
        timer.daemon = False
        timer.setName("Tool(database push queue)")
        timer.start()
        return timer

    @classmethod
    def _is_valid_config(cls):
        if type(cls.CACHE_LIFETIME_HOURS) is not int and type(cls.CACHE_LIFETIME_HOURS) is not bool:
            raise TypeError
        if not isinstance(cls.RELEASE_INTERVAL_SECONDS, (int, float,)):
            raise TypeError
        if cls.CACHE_LIFETIME_HOURS <= cls.RELEASE_INTERVAL_SECONDS:
            raise ORMInitializationError("Срок жизни кеша, который хранит очередь сохраняемых объектов не может быть меньше, "
                                         "чем интервал отправки объектов в базу данных.")
        cls._was_initialized = True

    @classmethod
    def __set_cache(cls, nodes):
        if nodes is None:
            raise TypeError("Покытка установить в кеш None вместо ORMQueue. Это недопустимо")
        cls.connection.cache.set("ORMItems", nodes, cls.CACHE_LIFETIME_HOURS)

    @staticmethod
    def __detect_primary_key(model, value: dict):
        pk = None
        for column_name, attrs_dict in model().column_names.items():
            if attrs_dict["primary_key"]:
                pk = column_name
                break
        if pk in value:
            return {pk: value[pk]}


class ResultCacheTools(Tool):
    TEMP_HASH_PREFIX: str = ...
    __iter__ = abstractmethod(lambda self: ...)

    def __init__(self, id_: int, *args, **kw):
        super().__init__()
        if not issubclass(type(self), BaseResult):
            raise TypeError
        if type(id_) is not int:
            raise TypeError
        self._id = str(id_)
        if not self._id:
            raise ValueError
        self.__key = f"{self.TEMP_HASH_PREFIX}{self._id[-5:]}"

    def _set_hash(self, nodes):
        self.__is_valid_nodes(nodes)
        hash_sum = set(map(str, map(hash, nodes)))
        self.connection.cache.set(self.__key, hash_sum)
        self._add_to_all_nodes_hash_has_been_in_result(hash_sum)

    def _add_hash_item(self, value):
        self.__is_valid_hash_key(value)
        current_hash = self._get_hash()
        current_hash.add(value)
        self.connection.cache.set(self.__key, current_hash)

    def _get_hash(self) -> set[str]:
        return self.connection.cache.get(self.__key, set())

    def _is_node_hash_has_been_in_result(self, value):
        """ Была ли данная нода(её хеш-сумма) в результатах когда-либо ранее"""
        self.__is_valid_hash_key(value)
        return value in self.__get_all_nodes_has_been_in_result()

    def _add_to_all_nodes_hash_has_been_in_result(self, items: Iterable[str]):
        [self.__is_valid_hash_key(i) for i in items]
        checked = self.__get_all_nodes_has_been_in_result()
        checked.update(items)
        self.connection.cache.set(f"{self.__key}-all", checked)

    def _is_hash_from_checked(self, val):
        return val in self.connection.cache.get(f"{self.__key}-checked", set())

    def _add_hash_to_checked(self, values):
        [self.__is_valid_hash_key(n) for n in values]
        checked = self._get_checked_hash_items()
        checked.update(values)
        self.connection.cache.set(f"{self.__key}-checked", checked)

    def _get_checked_hash_items(self):
        return self.connection.cache.get(f"{self.__key}-checked", set())

    def _remove_hash_from_checked(self, val):
        checked = self._get_checked_hash_items()
        if val not in checked:
            return
        checked.remove(val)
        self.connection.cache.set(f"{self.__key}-checked", checked)

    def _set_primary_keys(self, nodes):
        self.__is_valid_nodes(nodes)
        self.connection.cache.set(f"{self.__key}-pk", set(map(str, map(lambda x: x.hash_by_pk, nodes))))

    def _get_primary_keys_hash(self) -> set[str]:
        return self.connection.cache.get(f"{self.__key}-pk", set())

    def __get_all_nodes_has_been_in_result(self) -> set:
        return self.connection.cache.get(f"{self.__key}-all", set())

    @staticmethod
    def __is_valid_hash_key(hash_key):
        if type(hash_key) is not str:
            raise TypeError
        if not hash_key:
            raise ValueError

    @staticmethod
    def __is_valid_nodes(nodes: Union[Iterable[ResultORMCollection], ResultORMItem]):
        if isinstance(nodes, (tuple, list, set, frozenset)):
            if any(map(lambda x: type(x) is not ResultORMCollection, nodes)):
                raise TypeError
            return
        if type(nodes) is not ResultORMCollection:
            raise TypeError


class SliceResult:
    """ Функционал для контроля численности выборки в виде реализации среза. Мемоизация параметров среза."""
    def __init__(self: Union["SliceResult", "BaseResult"]):
        if not issubclass(self.__class__, BaseResult):
            raise Exception

        def is_valid():
            if not hasattr(self, "_get_local_nodes"):
                raise AttributeError
            if not hasattr(self, "_get_nodes_from_database"):
                raise AttributeError
            if not callable(self.get_local_nodes):
                raise TypeError
            if not callable(self.get_nodes_from_database):
                raise AttributeError
        self._is_slice = False
        self.__left_border = None
        self.__right_border = None
        is_valid()

    def __getitem__(self, item: slice):
        if type(item) is not slice:
            raise TypeError
        self.__is_valid_slice(item)
        self.__left_border = item.start
        self.__right_border = item.stop
        self._is_slice = True

    def get_nodes_from_database(self, **kwargs):
        if hasattr(super(), "get_nodes_from_database"):
            items = super().get_nodes_from_database(offset=self.__left_border,
                                                    limit=self.__right_border, **kwargs)
        else:
            items = self._get_nodes_from_database(offset=self.__left_border,
                                                  limit=self.__right_border, **kwargs)
        return items

    def get_local_nodes(self, **kwargs):
        if hasattr(super(), "get_local_nodes"):
            items = super().get_local_nodes(left_border=self.__left_border,
                                            right_border=self.__right_border, **kwargs)
        else:
            items = self._get_local_nodes(left_border=self.__left_border,
                                          right_border=self.__right_border, **kwargs)
        return items

    @staticmethod
    def __is_valid_slice(obj: slice):
        if obj.step is not None:
            if not obj.step == 1:
                raise ValueError("Выборка с шагом не поддерживается, шаг всегда 1")
        start = obj.start if obj.start is not None else 0
        end = obj.stop if obj.stop is not None else float("inf")
        if start < 0:
            raise ValueError
        if end < 0:
            raise ValueError
        if start > end:
            raise ValueError


class BaseResult(ABC, ResultCacheTools, SliceResult):
    TEMP_HASH_PREFIX: str = ...
    _merge = abstractmethod(lambda: ResultORMCollection())  # Функция, которая делает репликацию нод из кеша поверх нод из бд
    _get_node_by_joined_primary_key_and_value = abstractmethod(lambda model_pk_val_str,
                                                               sep="...": ...)  # Вернуть ноду по
    # входящей строке вида: 'имя_таблицы:primary_key:значение'

    def __init__(self, get_nodes_from_database=None, get_local_nodes=None, only_local=False, only_database=False, **kwargs):
        self._get_nodes_from_database: Optional[callable] = get_nodes_from_database  # Функция, в которой происходит получение контейнера с нодами из бд
        self._get_local_nodes: Optional[callable] = get_local_nodes  # Функция, в которой происходит получение контейнера с нодами из кеша
        self._id = self.__gen_id(**{**kwargs, "only_local": only_local, "only_database": only_database})
        self._only_queue = only_local
        self._only_db = only_database
        self._pointer: Optional["Pointer"] = None
        self._is_slice = False
        self._is_sort = False
        self.__merged_data: Union[list[ResultORMCollection], ResultORMCollection] = []
        self.__is_valid()
        super().__init__(self._id)
        self._set_hash(self.items)
        self._set_primary_keys(self.__merged_data)

    def get_nodes_from_database(self, **kwargs):
        if self._only_queue:
            return []
        if hasattr(super(), "get_nodes_from_database"):
            return super().get_nodes_from_database(**kwargs)
        return self._get_nodes_from_database(**kwargs)

    def get_local_nodes(self, **kwargs):
        if self._only_db:
            return []
        if hasattr(super(), "get_local_nodes"):
            return super().get_local_nodes(**kwargs)
        return self._get_local_nodes(**kwargs)

    def has_changes(self, hash_value=None) -> Optional[Union[bool, ValueError]]:
        """ Изменились ли значения в результатах с момента последнего запроса has_changes.
        :param hash_value: Если передан, то будет проверяться 1 конкретный результат из всей коллекции результатов
        Например в случае,
        когда has_changes запрашивается впервые, или, когда, просто напросто, кеш не помнит данных о "прошлых" результатов.
        """
        if hash_value is not None:
            if type(hash_value) is not int:
                raise TypeError
            hash_value = str(hash_value)
        nodes = self.items
        if hash_value is None:
            old_hash = self._get_hash()
            new_hash = set(map(str, map(hash, nodes))) - self._get_checked_hash_items()
            self._set_hash(nodes)
            if not new_hash:
                return False
            if not new_hash == old_hash:
                return True
            return False
        if hash_value in map(str, map(hash, nodes)):
            self._set_hash(nodes)
            return False
        if not self._is_node_hash_has_been_in_result(hash_value):
            self._set_hash(nodes)
            return
        if hash_value in self._get_primary_keys_hash():
            if hash_value not in [node.hash_by_pk for node in nodes]:
                self._set_hash(nodes)
                return True
            if nodes[hash_value].__hash__() not in self._get_hash():
                self._set_hash(nodes)
                return True
            self._set_hash(nodes)
            return False
        self._set_hash(nodes)
        if hash_value in self._get_checked_hash_items():
            self._remove_hash_from_checked(hash_value)
            return False
        self._add_hash_to_checked([hash_value])
        return True

    def has_new_entries(self) -> bool:
        nodes = self.items
        status = not (self._get_primary_keys_hash() == set(map(str, map(lambda v: v.hash_by_pk, nodes))))
        self._set_primary_keys(nodes)
        return status

    @property
    def items(self):
        self.__merged_data = self._merge()
        return self.__merged_data

    @property
    def pointer(self):
        return ref(self._pointer)()

    @pointer.setter
    def pointer(self: Union["Result", "JoinSelectResult"], wrap_items: list):
        items = self.items
        self._set_hash(items)
        self._pointer = Pointer(self, wrap_items)

    def __iter__(self):
        self.__merged_data = self._merge()
        return iter(self.__merged_data)

    def __len__(self):
        return sum((1 for _ in self))

    def __bool__(self):
        try:
            next(iter(self))
        except StopIteration:
            return False
        return True

    def __contains__(self, item: Union[str, int]):
        try:
            _ = self[item]
        except KeyError:
            return False
        else:
            return True

    def __getitem__(self, item: Union[str, int, slice]):
        if type(item) is slice:
            return super().__getitem__(item)
        return self.items[item]

    def __str__(self):
        return f"{self.__class__.__name__}({self.items})"

    @staticmethod
    def _parse_joined_primary_key_and_value(value, sep=":"):
        if not isinstance(sep, str):
            raise TypeError
        if not sep:
            raise ValueError
        if sep not in value:
            raise ValueError
        model_name, primary_key, value = value.split(sep)
        if not all((model_name, primary_key, value)):
            raise ValueError
        model_instance = ModelTools.import_model(model_name)
        if model_instance is None:
            raise InvalidModel(f"Класс-модель '{model_name}' в модуле models не найден")
        return model_instance, primary_key, value

    @staticmethod
    def __gen_id(**kwargs):
        """ Сгенерировать id, соответствующий параметрам запроса """
        str_ = "".join(map(lambda c: "".join(str(c)), kwargs.items()))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def __is_valid(self):
        if not all(map(lambda i: isinstance(i, bool), (self._only_queue, self._only_db,))):
            raise TypeError
        if not sum((self._only_queue, self._only_db,)) in (0, 1,):
            raise ValueError
        if not self._only_queue:
            if not callable(self.get_local_nodes):
                raise ValueError
        if not self._only_db:
            if not callable(self.get_nodes_from_database):
                raise ValueError

    @staticmethod
    def __get_hash_in_new_collection(collection, pk_hash) -> Optional[int]:
        for node_or_group in collection:
            if str(node_or_group.hash_by_pk) == pk_hash:
                return hash(node_or_group)


class Result(OrderBySingleResultMixin, BaseResult, ModelTools):
    """ Экземпляр данного класса возвращается функцией Tool.get_items() """
    TEMP_HASH_PREFIX = "simple_item_hash"

    def __init__(self, *args, model=None, where=None, **kwargs):
        def is_valid():
            self.is_valid_model_instance(model)
            if where is not None:
                if not isinstance(where, dict):
                    raise TypeError
            if set(where) - set(model().column_names):
                raise InvalidModel
        is_valid()
        self._model = model
        super().__init__(*args, model=model, where=where, **kwargs)

    def _merge(self):
        output = ServiceOrmContainer()
        output.LinkedListItem = ServiceOrmItem
        local_items = self.get_local_nodes()
        database_items = self.get_nodes_from_database()
        [output.enqueue(**node.get_attributes())
         for collection in (database_items, local_items,) for node in collection]
        return ResultORMCollection(output)

    def _get_node_by_joined_primary_key_and_value(self, value: Union[str, int]) -> Optional[QueueItem]:
        model, pk, val = self._parse_joined_primary_key_and_value(value)
        return self.items.get_node(model, **{pk: val})


class JoinSelectResult(OrderByJoinResultMixin, BaseResult, ModelTools):
    """
    Экземпляр этого класса возвращается функцией Tool.join_select()
    1 экземпляр этого класса 1 результат вызова Tool.join_select()
    Использовать следующим образом:
        Делаем join_select
        Результаты можем вывести в какой-нибудь Q...Widget, этот результат (строки) можно привязать к содержимому,
        чтобы вносить правки со стороны UI, ни о чём лишнем не думая
        JoinSelectResultInstance.pointer = ['Некое значение из виджета1', 'Некое значение из виджета2',...]
        Теперь нужный инстанс ServiceOrmContainer можно найти:
        JoinSelectResultInstance.pointer['Некое значение из виджета1'] -> ServiceOrmContainer(node_model_a, node_model_b, node_model_c)
        Если нода потеряла актуальность(удалена), то вместо неё будет заглушка - Экземпляр EmptyORMItem
        ServiceOrmContainer имеет свойство - is_actual на которое можно опираться
    """
    TEMP_HASH_PREFIX = "join_select_hash"

    def __init__(self, *args, models=None, where=None, on=None, **kwargs):
        def is_valid():
            if not models:
                raise ValueError
            [self.is_valid_model_instance(m) for m in models]
            if where is not None:
                if type(where) is not dict:
                    raise TypeError
                if set(where) - set(map(lambda m: m.__name__, models)):
                    raise InvalidModel
                for model_name, data in where.items():
                    if type(data) is not dict:
                        raise TypeError
                    columns = set(self.import_model(model_name)().column_names)
                    if [k for k in data if k not in columns]:
                        raise ValueError
                if not isinstance(on, dict):
                    raise TypeError
                if not on:
                    raise ValueError
                for left, right in on.items():
                    l_table, l_column = left.split(".")
                    r_table, r_column = right.split(".")
                    l_columns = self.import_model(l_table)().column_names
                    r_columns = self.import_model(r_table)().column_names
                    if l_column not in l_columns:
                        raise ValueError
                    if r_column not in r_columns:
                        raise ValueError
                    if l_table == r_table and l_column == r_column:
                        raise ValueError
        is_valid()
        self._models = models
        super().__init__(*args, on=on, where=where, models=models, **kwargs)

    @property
    def items(self) -> tuple[ResultORMCollection]:
        """ Выполнить запрос в базу данных и/или в кеш. """
        if self._is_sort:
            result = tuple(super().items)
        else:
            result = tuple(self._merge())
        return result

    def __getitem__(self, item: int) -> ResultORMCollection:
        data = self.items
        if type(item) is not int:
            raise TypeError
        if item in range(len(data)):
            return data[item]
        for items_group in self:
            if hash(items_group) == item:
                return items_group
            if items_group.hash_by_pk == item:
                return items_group

    def __contains__(self, item: Union[int, ResultORMCollection, ResultORMItem]):
        if type(item) is int:
            nodes = self.items
            if item in map(hash, nodes):
                return True
            if item in (nodes.hash_by_pk for nodes in nodes):
                return True
            return False
        if isinstance(item, (ResultORMItem, ResultORMCollection,)):
            return hash(item) in map(hash, self)
        return False

    def _merge(self) -> tuple[ResultORMCollection]:
        """
        0) Есть 2 двумерные коллекции: ноды_из_кэша и ноды_из_базы
        Например: [ Container(node, node, node), Container(node, node) ]
        1) Собираем все ноды из локальных, у которых есть значение любого FK столбца
        2) Обходим все ноды из базы, если значение столбца ноды из базы совпадает с нодой из локалки,
        то эту ноду УДАЛЯЕМ ИЗ НОД ИЗ БАЗЫ. Если длина этой группы нод <=1, то мы эту группу удаляем целиком
        3) На фильтрованные ноды из базы накладываем ноды из кеша, оставшиеся добавляем в хвост
        """
        def get_local_nodes_with_any_value_in_fk(local_nodes: list[ServiceOrmContainer]):
            """ Все локальные ноды, смешанные в кучу, в которых, в значениях внешних ключей, стоит какое-то значение """
            def collect_foreign_keys_at_local_items():
                for items_group in local_nodes:
                    for node in items_group:
                        foreign_keys = ModelTools.get_foreign_key_columns(node.model)
                        if foreign_keys:
                            yield node.model.__name__, foreign_keys
            res = ServiceOrmContainer()
            foreign_keys_at_local_items = tuple(collect_foreign_keys_at_local_items())
            for items_group in local_nodes:
                tables_map = {node.model.__name__: node.model for node in items_group}
                for table_name, foreign_keys in foreign_keys_at_local_items:
                    find_nodes = items_group.search_nodes(tables_map[table_name], **{k: "*" for k in foreign_keys})
                    if find_nodes:
                        res.append(**find_nodes[0].get_attributes())
            return res

        def get_filtered_database_items():
            """ Оборвать связи между нодами из БД, если эта связь оборвана в локальных нодах """
            currently_added_hash = []
            nodes_with_foreign_keys_at_local_items = get_local_nodes_with_any_value_in_fk(local_items)
            for db_nodes_group in self.get_nodes_from_database():
                for local_node_with_fk in nodes_with_foreign_keys_at_local_items:
                    result: ServiceOrmItem = db_nodes_group.get_node(local_node_with_fk.model,
                                                                     **local_node_with_fk.get_primary_key_and_value())
                    if not result:
                        if db_nodes_group.hash_by_pk not in currently_added_hash:
                            if len(db_nodes_group) == 1:
                                continue
                            yield db_nodes_group
                            currently_added_hash.append(db_nodes_group.hash_by_pk)
                        continue
                    for foreign_key_column in ModelTools.get_foreign_key_columns(local_node_with_fk.model):
                        if foreign_key_column not in result.value:
                            break
                        if not result.value[foreign_key_column] == local_node_with_fk.value[foreign_key_column]:
                            db_nodes_group.remove(result.model, *result.get_primary_key_and_value(as_tuple=True))
                if len(db_nodes_group) > 1:
                    if db_nodes_group.hash_by_pk not in currently_added_hash:
                        yield db_nodes_group
                        currently_added_hash.append(db_nodes_group.hash_by_pk)

        def merge(db_items, local_items_):
            """
            :param db_items: Коллекция из базы данных
            :param local_items_:Коллекция из локальных элементов очереди на коммит в базу
            """
            for db_group in db_items:
                nodes = db_group.__iter__()
                while nodes:
                    try:
                        rand_node = nodes.__next__()
                    except StopIteration:
                        rand_node = None
                    if rand_node is None:
                        break
                    for local_group in local_items_:
                        find_node = local_group.search_nodes(rand_node.model, **rand_node.get_primary_key_and_value())
                        if find_node:
                            db_items.remove(db_group)
                            local_items_.remove(local_group)
                            if len(db_group) <= 1:
                                if len(local_group) > 1:
                                    yield local_group
                                continue
                            yield db_group + local_group
                            break
            for nodes_group in db_items:
                if len(nodes_group) <= 1:
                    db_items.remove(nodes_group)
            for nodes_group in local_items_:
                if len(nodes_group) <= 1:
                    local_items_.remove(nodes_group)
            for item in db_items:
                yield item
            for item in local_items_:
                yield item
        local_items = list(self.get_local_nodes())
        return tuple(ResultORMCollection(item) for item in merge(list(get_filtered_database_items()), local_items))

    def _get_node_by_joined_primary_key_and_value(self, joined_pk: str):
        model_name, primary_key, value = self._parse_joined_primary_key_and_value(joined_pk)
        model_instance = ModelTools.import_model(model_name)
        for collection in self:
            node = collection.get_node(model_instance, **{primary_key: value})
            if node:
                return node


class PointerCacheTools(Tool):
    POINTER_CACHE_PREFIX = "p_id"
    WRAP_ITEM_MAX_LENGTH = WRAP_ITEM_MAX_LENGTH

    def __init__(self, id_: str):
        self._id = id_  # uuid4
        self.__cache_key = f"{self.POINTER_CACHE_PREFIX}_{self._id[-5:]}"
        self.__is_valid_config()

    def _set_pointer_configuration(self, items: Iterable[str]):
        """ Хранить данные в виде: wrap_str:pk_hash:hash_sum """
        def is_valid():
            if not isinstance(items, (list, tuple, set, frozenset)):
                raise TypeError
            if not items:
                return
            for n in items:
                if type(n) is not str:
                    raise TypeError
                values = n.split(":")
                if not len(values) == 3:
                    raise ValueError
                if len(values[0]) > self.WRAP_ITEM_MAX_LENGTH:
                    raise PointerWrapperLengthError
                if not values[1].isdigit() or not values[2].isdigit():
                    raise TypeError
        is_valid()
        self.connection.cache.set(self.__cache_key, ",".join(items), self.CACHE_LIFETIME_HOURS)

    def _replace_pointer_cache_item(self, primary_key_hash: str, new_hash_sum: str):
        data = self.connection.cache.get(self.__cache_key, None)
        if data is None:
            return
        item_str = None
        index = None
        all_items = data.split(",")
        for index, item in enumerate(all_items):
            wrap, pk, sum_ = item.split(":")
            if pk == primary_key_hash:
                item_str = f"{wrap}:{pk}:{new_hash_sum}"
                break
        if item_str is not None:
            all_items[index] = item_str
        self.connection.cache.set(self.__cache_key, ",".join(all_items))

    def _get_primary_keys_hash(self) -> Iterator[str]:
        return self.__parse_cache_items(1)

    def _get_hash_sum(self) -> Iterator[str]:
        return self.__parse_cache_items(2)

    def _get_wrappers(self) -> Iterator[str]:
        return self.__parse_cache_items(0)

    def __parse_cache_items(self, val_index):
        data = self.connection.cache.get(self.__cache_key, None)
        if data is None:
            return
        for item in data.split(","):
            if not item:
                return
            yield item.split(":")[val_index]

    @abstractmethod
    def __is_valid_config(self):
        if type(self._id) is not str:
            raise TypeError
        if not self._id:
            raise ValueError


class Pointer(PointerCacheTools):
    """ Экземпляр данного объекта - оболочка для содержимого, обеспечивающая доступ к данным.
    Объект этого класса создан для 'слежки' за содержимым из результатов запроса.
    Если количество данных в результате начнёт разниться,
    по сравнению с предыдущим взаимодействием [с данным экземпляром], то он становится бесполезен
    и требуется создание нового объекта, с новым списком wrap_items.
    """
    def __init__(self, result_item: Union[Result, JoinSelectResult], wrap_items: list[str]):
        self._id = str(uuid.uuid4())
        self._result_item = result_item
        self._wrap_items = wrap_items
        self.__is_invalid = False
        super().__init__(self._id)
        self._is_valid_config()
        self._set_pointer_configuration(self.__create_cache_data())

    @property
    def wrap_items(self) -> list[str]:
        if not self._is_valid():
            return list()
        return copy.copy(self._wrap_items)

    @property
    def items(self) -> Optional[dict[str, Union[ResultORMCollection, list[ResultORMCollection]]]]:
        nodes = tuple(self._select_result_getter())
        if not self._is_valid(actual_primary_key_hash=[str(n.hash_by_pk) for n in nodes]):
            return
        self._set_pointer_configuration(self.__create_cache_data(items=nodes))
        return dict(zip(self._wrap_items, nodes))

    @property
    def is_valid(self):
        return self._is_valid()

    def has_changes(self, name: str) -> Optional[Union[bool, Exception]]:
        """ Получить статус состояния результатов, на которые ранее был задан экземпляр Pointer.
         :param name: имя одного конкретного результата, одно из многих, которые хранятся в wrap_items
         Например в случае,
         когда has_changes запрашивается впервые, или, когда, просто напросто, кеш не помнит данных о "прошлых" результатов.
         """
        if type(name) is not str:
            raise TypeError
        if not name:
            raise ValueError
        if name not in self._wrap_items:
            raise KeyError
        if self.__is_invalid:
            return
        result = tuple(self._select_result_getter())
        actual_primary_keys_hash = [str(n.hash_by_pk) for n in result]
        if not self._is_valid(actual_primary_key_hash=actual_primary_keys_hash):  # Если изменилось кол-во нод или есть другие (с другим pk) ноды, включая соблюдение последовательности
            return True
        current_hash_sum = [int(val) for val in self._get_hash_sum()]
        index = list(self._get_wrappers()).index(name)
        self._replace_pointer_cache_item(actual_primary_keys_hash[index], str([hash(n) for n in result][index]))
        return self._result_item.has_changes(hash_value=current_hash_sum[index])

    def replace_wrap(self, item: str, old_wrapper: Optional[str] = None, hash_: Optional[int] = None,
                     primary_key_hash: Optional[int] = None, index: Optional[int] = None):
        """
        Установить новый строковой эквивалент записи взамен старой.
        :param item: Новая строка
        :param old_wrapper: Старая строка из wrap_items
        :param hash_: Полная хеш сумма результата, у которой следует заменить строку-указатель
        :param primary_key_hash: Хеш-сумма первичного ключа и значения результата, у которого следует заменить строку-указатель
        :param index: Индекс записи в результате, у которой следует заменить строку-указатель
        :return: None
        """
        if type(item) is not str:
            raise TypeError
        if not item:
            raise ValueError
        if not sum((bool(old_wrapper), bool(hash_), bool(primary_key_hash), bool(index),)) == 1:
            raise ValueError
        if old_wrapper is not None:
            if type(old_wrapper) is not str:
                raise TypeError
        if hash_ is not None:
            if not isinstance(hash_, int):
                raise TypeError
        if primary_key_hash is not None:
            if type(primary_key_hash) is not int:
                raise TypeError
        if index is not None:
            if not isinstance(index, int):
                raise TypeError
        if index is not None:
            if index < 0:
                index = len(self._wrap_items) - index
            if len(self._wrap_items) - 1 < index:
                return
            self._wrap_items[index] = item
        if hash_ is not None:
            current_hash = tuple(map(int, self._get_hash_sum()))
            if hash_ not in current_hash:
                return
            self._wrap_items[current_hash.index(hash_)] = item
        if primary_key_hash is not None:
            current_pk_hash = tuple(map(int, self._get_primary_keys_hash()))
            if primary_key_hash not in current_pk_hash:
                return
            self._wrap_items[current_pk_hash.index(primary_key_hash)] = item
        if old_wrapper is not None:
            if old_wrapper not in self._wrap_items:
                return
            self._wrap_items[self._wrap_items.index(old_wrapper)] = item

    def __getitem__(self, item: str) -> Optional[Union[ResultORMItem, ResultORMCollection]]:
        data = tuple(self._select_result_getter())
        if not self._is_valid(actual_primary_key_hash=[str(r.hash_by_pk) for r in data]):
            return
        if not isinstance(item, str):
            raise TypeError
        if data is None:
            return
        if item not in self._wrap_items:
            return
        return dict(zip(self._wrap_items, data))[item]

    def __contains__(self, item: str):
        if self[item]:
            return True
        return False

    def __bool__(self):
        return self._is_valid()

    def __len__(self):
        if not self._is_valid():
            return 0
        return len(self._wrap_items)

    def __str__(self):
        status = self._is_valid()
        str_ = r", \r".join(map(lambda x: f"{x[0]}:{x[1]}",
                                zip_longest(self._wrap_items, list(self._select_result_getter()), fillvalue="[X]")))
        str_ = f"{str_}, valid: {status}"
        return str_

    def _is_valid(self, old_primary_key_hash: Optional[list[str]] = None,
                  actual_primary_key_hash: Optional[list[str]] = None):
        """ Актуален ли текущий экземпляр к данному моменту.
         Под актуальностью понимается сохранение количества элементов в результатах и отсутствие новых,
         а также упорядоченность.
         Если экземпляр стал неактуален, то он становится таким навсегда.
         """
        def validate_params():
            nonlocal actual_primary_key_hash
            nonlocal old_primary_key_hash
            if actual_primary_key_hash is None:
                actual_primary_key_hash = [str(node_or_group.hash_by_pk) for node_or_group in self._select_result_getter()]
            if old_primary_key_hash is None:
                old_primary_key_hash = list(self._get_primary_keys_hash())
            if type(actual_primary_key_hash) is not list:
                raise TypeError
            if type(old_primary_key_hash) is not list:
                raise TypeError
            if not all([True if type(val) is str else False for val in old_primary_key_hash]):
                raise TypeError
            if any(map(lambda i: not isinstance(i, str), actual_primary_key_hash)):
                raise TypeError
        validate_params()
        if self.__is_invalid:
            return False
        if not actual_primary_key_hash == old_primary_key_hash:
            self.__is_invalid = True
            return False
        return True

    def _select_result_getter(self) -> Iterator[ResultORMCollection]:
        """ Порядок получаемых с запроса нод мог измениться.
         Отсортируем ноды по порядку первичных ключей, которые были записаны в кеш при инициализации"""
        cached_primary_keys = tuple(self._get_primary_keys_hash())
        node_items = self._result_item.items
        if not cached_primary_keys:
            for n in node_items:
                yield n
            return
        for primary_key_hash in cached_primary_keys:
            for node_or_group in node_items:
                if str(node_or_group.hash_by_pk) == primary_key_hash:
                    yield node_or_group
        for node_or_group in node_items:
            if str(node_or_group.hash_by_pk) not in cached_primary_keys:
                yield node_or_group

    def _is_valid_config(self):
        iterable_result = None
        if type(self._wrap_items) is not list:
            raise PointerWrapperTypeError("В качестве элементов wrapper принимается список строк")
        if not all(map(lambda x: isinstance(x, str), self._wrap_items)):
            raise PointerWrapperTypeError
        if not self._wrap_items:
            iterable_result = tuple(self._select_result_getter())
            if not iterable_result:
                return
            raise PointerWrapperLengthError("Контейнер с обёрткой содержимого не может быть пустым")
        if not isinstance(self._result_item, (Result, JoinSelectResult,)):
            raise JoinedItemPointerError(
                "Экземпляр класса JoinSelectResult или Result не установлен в атрибут класса result_item"
            )
        if not len(self._wrap_items) == len(set(self._wrap_items)):
            raise PointerRepeatedWrapper
        iterable_result = tuple(self._select_result_getter()) if iterable_result is None else iterable_result
        if not len(iterable_result) == self._wrap_items.__len__():
            raise PointerWrapperLengthError
        super()._is_valid_config()

    def __create_cache_data(self, items=None):
        data = tuple(self._select_result_getter()) if items is None else items
        hash_ = tuple(map(hash, data))
        pk = (n.hash_by_pk for n in data)
        wrap_items = list(self._wrap_items)
        return tuple(map(lambda x: f"{wrap_items.pop(0)}:{x[0]}:{x[1]}", zip(pk, hash_)))
