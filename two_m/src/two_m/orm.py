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
from pymemcache.client.base import Client
from pymemcache.exceptions import MemcacheError
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.sql.dml import Insert, Update, Delete
from sqlalchemy.sql.expression import select
from sqlalchemy.orm import Query, sessionmaker as session_factory, Session, scoped_session
from sqlalchemy.exc import DisconnectionError, OperationalError, SQLAlchemyError
from two_m.src.two_m.dill.serde import DillSerde
from two_m.src.two_m.datatype import LinkedList, LinkedListItem
from two_m.src.two_m.database.postgres.exceptions import DatabaseException
from two_m.src.two_m.conf import RESERVED_WORDS, CustomModel
from two_m.src.two_m.exceptions import *


class ORMAttributes:
    @classmethod
    def is_valid_node(cls, node: Union["QueueItem", "ServiceOrmItem", "ResultORMItem"]):
        if not isinstance(node, (ServiceOrmItem, QueueItem, ResultORMItem,)):
            raise TypeError
        cls.is_valid_model_instance(node.model)

    @staticmethod
    def is_valid_model_instance(item):
        if isinstance(item, (int, str, float, bytes, bytearray, set, dict, list, tuple, type(None))):
            raise InvalidModel
        if hasattr(item, "__new__"):
            item = item()  # __new__
            if not hasattr(item, "column_names"):
                raise InvalidModel
            keys = {"type", "nullable", "primary_key", "autoincrement", "unique", "default"}
            if any(map(lambda x: keys - set(x), item.column_names.values())):
                raise ModelsConfigurationError
            return
        raise InvalidModel


class ModelTools(ORMAttributes):
    def get_required_columns(self, model: Type[CustomModel], values_data: Optional[dict] = None) -> frozenset[str]:
        self.is_valid_model_instance(model)
        primary_key = self.get_primary_key_column_name(model)
        if values_data is not None:
            if type(values_data) is not dict:
                raise TypeError
        else:
            values_data = {}
        columns_that_should_be_with_data = frozenset(self.get_nullable_columns(model,  nullable=False)).intersection(
            frozenset(self.get_column_names_with_default_procedure_or_value(model, has_default=False)))
        columns_that_should_be_with_data -= frozenset(values_data)
        return columns_that_should_be_with_data.union(
            frozenset([primary_key] if not self.is_autoincrement_primary_key(model) and primary_key not in values_data else [])
        )

    @classmethod
    def is_autoincrement_primary_key(cls, model: Type[CustomModel]) -> bool:
        cls.is_valid_model_instance(model)
        for column_name, data in model().column_names.items():
            if data["autoincrement"]:
                return True
        return False

    def get_primary_key_python_type(self, model: Type[CustomModel]) -> Type:
        self.is_valid_model_instance(model)
        for column_name, data in model().column_names.items():
            if data["primary_key"]:
                return data["type"]

    @staticmethod
    def get_unique_columns(node) -> Iterator[str]:
        """ Получить названия столбцов с UNIQUE=TRUE (их значения присутствуют в ноде) """
        ORMAttributes.is_valid_node(node)
        model_data = node.model().column_names
        value = node.value
        if "ui_hidden" in value:
            del value["ui_hidden"]
        for column_name in value:
            if model_data[column_name]["unique"]:
                yield column_name

    def get_default_column_value_or_function(self, model: Type[CustomModel], column_name: str) -> Optional[Any]:
        self.is_valid_model_instance(model)
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
    def _select_primary_key_value_from_scalars(cls, node: "QueueItem", field_name: str) -> Optional[Union[str, int]]:
        """ Выбрать значние из значений ноды,
        если primary_key=True установлено для строки или другого поля со скалярными значениями"""
        try:
            value = node.value[field_name]
        except KeyError:
            return
        else:
            return value

    @staticmethod
    def _get_highest_autoincrement_pk_from_local(node) -> int:
        try:
            val = max(map(lambda x: x.get_primary_key_and_value(only_value=True),
                          node.container.search_nodes(node.model)))
        except ValueError:
            return 1
        else:
            val += 1
        return val

    @classmethod
    def _check_unique_values(cls, node) -> bool:
        """ Валидация на предмет нарушения уникальности в полях разных нод """
        unique_fields = cls.get_unique_columns(node)
        for unique_field in unique_fields:
            value = node.value[unique_field]
            if not node.container:
                return True  # Ожидается, что ленивая ссылка умрёт, если в контейнере оставался 1 элемент
            for n in node.container:
                if n.model.__name__ == node.model.__name__:
                    if unique_field in n.value:
                        if n.value[unique_field] == value:
                            return False
        return True

    @staticmethod
    def _check_not_null_fields_in_node_value(node: "QueueItem") -> bool:
        """ Проверить все поля на предмет nullable """
        model_attributes: dict[dict] = node.model().column_names
        for k, attributes in model_attributes.items():
            if not attributes["nullable"]:
                if k not in node.value:
                    return False
                if type(node.value[k]) is None:
                    return False
        return True

    @staticmethod
    def _is_valid_column_type_in_sql_type(node: "QueueItem") -> bool:
        """ Проверить соответствие данных в ноде на предмет типизации.
         Если тип данных отличается от табличного в БД, то возбудить исключение"""
        data = node.model().column_names
        for column_name in node.value:
            if column_name not in data:
                return False
            if not isinstance(node.value[column_name], data[column_name]["type"]):
                if node.value[column_name] is None and data[column_name]["nullable"]:
                    continue
                raise NodeColumnValueError(text=f"Столбец {column_name} должен быть производным от "
                                                f"{str(data[column_name]['type'])}, "
                                                f"по факту {type(node.value[column_name])}")
        return True

    @classmethod
    def _create_select_in_database_and_put_primary_key_by_any_unique_field(cls, node: "QueueItem") -> Optional[dict]:
        """ Произвести запрос в базу данных для поиска записи со столбцами unique constraint,
        для получения первчиного ключа и значения (из базы) """
        unique_columns = tuple(cls.get_unique_columns(node))
        if not unique_columns:
            return
        unique_data = {key: node.value[key] for key in node.value if key in unique_columns}
        select_result = Main.database.query(node.model).filter_by(**unique_data).all()
        if not select_result:
            return
        pk = cls.get_primary_key_column_name(node.model)
        return {pk: select_result[0].__dict__[pk]}


class NodeTools:
    @staticmethod
    def is_valid_primary_key(d: dict, model: CustomModel):
        ModelTools.is_valid_model_instance(model)
        if not isinstance(d, dict):
            raise TypeError
        if not len(d) == 1:
            raise ValueError
        if not all(map(lambda y: type(y[0]) is str and isinstance(y[1], (str, int,)), d.items())):
            raise ValueError
        if not ModelTools.get_primary_key_column_name(model) == tuple(d)[0]:
            raise ValueError("Столбец первичного ключа для этой таблицы называется иначе!")


class QueueSearchTools:
    """ Инструменты для поиска нод. Поиск идентичных [переданной] нод в указанном контейнере. """
    @classmethod
    def get_node_by_unique_fields(cls, queue, right_node):
        nodes = queue.__class__()
        cls.__is_valid(queue, right_node)
        unique_columns_at_cur_table = tuple(ModelTools.get_unique_columns(right_node))
        if not unique_columns_at_cur_table:
            return
        unique_columns_in_node = set(right_node.value) & set(unique_columns_at_cur_table)
        if not unique_columns_in_node:
            return
        for left_node in queue:
            if not left_node.model.__name__ == right_node.model.__name__:
                continue
            matched = set(left_node.value) & unique_columns_in_node
            for k in matched:
                if right_node.value[k] == left_node.value[k]:
                    nodes.enqueue(**left_node.get_attributes(new_container=nodes))
        if nodes:
            if len(nodes) > 1:
                raise ORMException
            return nodes[0]

    @staticmethod
    def __is_valid(queue, node):
        ORMAttributes.is_valid_model_instance(node.model)
        if not isinstance(queue, (Queue, ServiceOrmContainer, ResultORMCollection,)):
            raise TypeError


class QueueItem(LinkedListItem, ModelTools, NodeTools):
    """ Иммутабельный класс ноды для Queue. Нода для иньекции в базу. """
    def __init__(self, _primary_key_from_ui=None, _container=None, _insert=False, _update=False, _delete=False,
                 _model=None, _where=None, _create_at=None,
                 **kw):
        """
            :arg _model: Расширенный клас model SQLAlchemy
            :arg _insert: Опицонально bool
            :arg _update: Опицонально bool
            :arg _delete: Опицонально bool
            :arg _ready: Если __delete=True - Необязательный
            :arg _where: Опицонально dict
            Все остальные параметры являются парами 'поле-значение'
            """
        self._is_valid_container(_container)
        self._container: ReferenceType[Union["Queue", "ServiceOrmContainer"]] = ref(_container)
        self.__model: CustomModel = _model
        self.is_valid_model_instance(self.__model)
        if _primary_key_from_ui:
            self.is_valid_primary_key(_primary_key_from_ui, self.__model)
        self.__insert = _insert
        self.__update = _update
        self.__delete = _delete
        self.__is_ready = kw.pop("_ready", True if self.__delete else False)
        self.__where = _where
        self._create_at = _create_at
        self.__transaction_counter = kw.pop('_count_retries', 0)  # Инкрементируется при вызове self.make_query()
        # Подразумевая тем самым, что это попытка сделать транзакцию в базу
        super().__init__(**kw)
        self.__foreign_key_fields = self.get_foreign_key_columns(self.__model)

        def is_valid_dml_type():
            """ Только одино свойство, обозначающее тип sql-dml операции, может быть True """
            if not isinstance(self.__insert, bool) or not isinstance(self.__update, bool) or \
                    not isinstance(self.__delete, bool):
                raise TypeError
            if not sum((self.__insert, self.__update, self.__delete,)) == 1:
                raise NodeDMLTypeError
        is_valid_dml_type()
        self._field_names_validation()
        self.__pk_set_by_ui = False
        if _primary_key_from_ui:
            self.__pk_set_by_ui = True
        self.__primary_key = self.__create_primary_key() if not _primary_key_from_ui else _primary_key_from_ui
        self._val.update(self.__primary_key)
        _ = self.ready
        self.__relative_primary_key = False

        def check_current_primary_key_is_relative():
            """ Является ли первичный ключ относительным.
            Под этим понимается то, что при последующих репликациях во время
            enqueue значение первичного ключа является автоинкрементом или неким вычисляемым значением по умолчанию,
            предугадать которое не представляется возможным,- такой первичный ключ мы будем называть относительным."""
            if not self.__pk_set_by_ui:
                self.__relative_primary_key = True
        check_current_primary_key_is_relative()

    @property
    def is_relative_primary_key(self):
        """ Был ли первичный ключ задан явно или сгенерирован в орм """
        return self.__relative_primary_key

    @property
    def container(self) -> "Queue":
        return self._container()

    @container.setter
    def container(self, cnt):
        if type(cnt) is not Queue:
            raise TypeError
        self._container = ref(cnt)

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
        """
        :param as_tuple: ключ-значение в виде кортежа
        :param only_key: только название столбца - PK
        :param only_value: только значение столбца первичного ключа
        """
        if only_key:
            return tuple(self.__primary_key.keys())[0]
        if only_value:
            return tuple(self.__primary_key.values())[0]
        return tuple(self.__primary_key.items())[0] if as_tuple else self.__primary_key

    @property
    def created_at(self):
        return self._create_at

    @property
    def where(self) -> dict:
        return self.__where.copy() if self.__where else {}

    @property
    def ready(self) -> bool:
        self.__is_ready = self._is_valid_column_type_in_sql_type(self)
        self.__is_ready = self._check_unique_values(self) if self.__is_ready else False
        if self.__insert:
            self.__is_ready = self._check_not_null_fields_in_node_value(self) if self.__is_ready else False
        return self.__is_ready

    @ready.setter
    def ready(self, status: bool):
        if not isinstance(status, bool):
            raise NodeAttributeError("Статус готовности - это тип данных boolean")
        self.container.enqueue(**{**self.get_attributes(), "_ready": status})

    @property
    def type(self) -> str:
        return "_insert" if self.__insert else "_update" if self.__update else "_delete"

    def get_attributes(self, with_update: Optional[dict] = None, new_container: Optional["Queue"] = None) -> dict:
        if with_update is not None and type(with_update) is not dict:
            raise TypeError
        if new_container:
            if self.container is not None:
                if not isinstance(new_container, type(self.container)):
                    raise TypeError
        result = {"_create_at": self.created_at}
        result.update(self.value)
        if self.__update or self.__delete:
            if self.__where:
                result.update({"_where": self.where})
        result.update({"_model": self.__model, "_insert": False,
                       "_update": False, "_ready": self.__is_ready,
                       "_delete": False, "_count_retries": self.retries})
        result.update({"_container": self.container}) if self.container is not None else None
        result.update({self.type: True})
        result.update(with_update) if with_update else None
        if new_container is not None:
            result.update({"_container": new_container})
        if self.__pk_set_by_ui:
            result.update({"_primary_key_from_ui": self.__primary_key})
        return result

    def make_query(self) -> Optional[Query]:
        query = None
        value: dict = self.value
        primary_key = self.get_primary_key_and_value(only_key=True)
        where = self.__where
        if self.is_relative_primary_key:
            del value[primary_key]
            where = value if not where else where
        else:
            where = where if where else self.get_primary_key_and_value()
        if self.__insert:
            query = insert(self.model).values(**value)
        if self.__update or self.__delete:
            query = Main.database.query(self.model).filter_by(**where).first()
            if query is not None:
                if self.__update:
                    [setattr(query, key, value) for key, value in value.items()]
                if self.__delete:
                    query = delete(self.model).where(text(
                        ", ".join(map(lambda x: f"{self.model.__tablename__}.{x[0]}='{x[1]}'", value.items()))
                    ))
        self.__transaction_counter += 1
        return query

    def __len__(self):
        return len(self._val)

    def __eq__(self, other: "QueueItem"):
        if type(other) is not type(self):
            return False
        return self.__hash__() == hash(other)

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
        del attributes["_container"]
        attributes.update({"_create_at": create_at.strftime("%d:%m:%S")})
        return ', '.join(map(lambda i: '='.join([str(e) for e in i]), attributes.items()))

    def __hash__(self):
        value = self.value
        if self.is_relative_primary_key:
            del value[self.get_primary_key_and_value(only_key=True)]
        str_ = "".join(map(lambda x: str(x), itertools.chain(*value.items())))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def __getitem__(self, item: str):
        if type(item) is not str:
            raise TypeError
        if item not in self.value:
            raise KeyError
        return self.value[item]

    def _field_names_validation(self, from_polymorphizm=False) -> Optional[set[str]]:
        """ соотнести все столбцы ноды в словаре value со столбцами из класса Model """
        def clear_names():
            """ ORM могла добавить префиксы вида 'ModelClassName.column_name', очистить имена от них """
            value = (k[k.index(".") + 1:] if "." in k else k for k in self.value)
            return value
        for name in clear_names():
            if not isinstance(self.value[name], (str, int, bool, float, bytes, bytearray, type(None),)):
                raise NodeColumnValueError(self.value[name])
        field_names = self.model().column_names
        any_ = set(clear_names()) - set(field_names)
        if any_:
            if from_polymorphizm:
                return any_
            raise NodeColumnError(any_, model_name=self.model.__name__)

    @staticmethod
    def _is_valid_container(container):
        if type(container) is not Queue:
            raise TypeError(f"Вмето контейнера класса Queue, который предназначен под ноды этого типа, "
                            f"задан контейнер - {type(container).__name__}")

    def __create_primary_key(self) -> dict[str, Union[str, int]]:
        """Повторный вызов недопустим. Вызывать в первую очередь! до добаления ноды в связанный список"""
        def convert_join_select_result_type(input_: ResultORMCollection) -> Queue:
            result = Queue()
            NodeTypeConverter.input_cls = ResultORMItem
            NodeTypeConverter.output_cls = QueueItem
            for node in input_:
                result.append(**NodeTypeConverter.convert(node).get_attributes(new_container=result,
                                                                 with_update={"_create_at":  datetime.datetime.now()}))
            return result
        name = self.get_primary_key_column_name(self.model)
        value = self._select_primary_key_value_from_scalars(self, name)
        if value:
            return {name: value}
        if not self.__insert:
            node = QueueSearchTools.get_node_by_unique_fields(self.container, self)
            if node is not None:
                return node.get_primary_key_and_value()
            single_items_from_cache = Main.cache.get(Result.RESULT_CACHE_KEY,
                                                          ResultORMCollection())
            node = QueueSearchTools.get_node_by_unique_fields(single_items_from_cache, self)
            if node is not None:
                return node.get_primary_key_and_value()
            multiple_items_from_cache = Main.cache.get(JoinSelectResult.RESULT_CACHE_KEY,
                                                            [ResultORMCollection()])
            for collection in multiple_items_from_cache:
                converted = convert_join_select_result_type(collection)
                node = QueueSearchTools.get_node_by_unique_fields(converted, self)
                if node:
                    break
            if node:
                return node.get_primary_key_and_value()
            pk_from_db = self._create_select_in_database_and_put_primary_key_by_any_unique_field(self)
            if pk_from_db:
                return pk_from_db
        default_value = self.get_default_column_value_or_function(self.model, name)
        if default_value is not None:
            return {name: default_value.arg(None)}
        if self.is_autoincrement_primary_key(self.model):
            return {name: self._get_highest_autoincrement_pk_from_local(self)}
        raise NodePrimaryKeyError


class EmptyOrmItem(LinkedListItem):
    """
    Пустой класс для возврата пустой "ноды". Заглушка
    """
    def __eq__(self, other):
        if type(other) is type(self):
            return True
        return False

    def __len__(self):
        return 0

    def __bool__(self):
        return False

    def __repr__(self):
        return f"{type(self).__name__}()"

    def __str__(self):
        return "None"

    def __hash__(self):
        return 0


class ResultORMItem(LinkedListItem, ORMAttributes, NodeTools):
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

    def get_primary_key_and_value(self, only_key=False, only_val=False):
        if type(only_key) is not bool:
            raise TypeError
        if not isinstance(only_val, bool):
            raise TypeError
        if only_val and only_key:
            raise ValueError
        if only_key:
            return tuple(self._primary_key.keys())[0]
        if only_val:
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

    def get_attributes(self, *args, **kwargs):
        return {"_model": self._model, "_primary_key": self._primary_key, **self._val}

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
        self.is_valid_model_instance(self._model)
        if not self.value:
            raise ValueError
        self.is_valid_primary_key(self._primary_key, self._model)

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


class Queue(LinkedList, QueueSearchTools):
    """
    Очередь на основе связанного списка.
    Управляется через адаптер Main.
    Класс-контейнер умеет только ставить в очередь ((enqueue) зашита особая логика) и снимать с очереди (dequeue)
    см логику в методе _replication.
    """
    LinkedListItem = QueueItem

    def __init__(self, items: Optional[Iterable[dict]] = None):
        super().__init__(items)
        if items is not None:
            for inner in items:
                self.enqueue(**inner)

    def enqueue(self, **attrs):
        """ Установка ноды в конец очереди с хитрой логикой проверки на совпадение. """
        def remove_other_foreign_key_value():
            """ Если в очереди есть нода, той же модели, что и добавляемая.
             Если у старой и добавляемой ноды есть одно и то же значение внешнего ключа,
             то из найденной в очереди ноды это значение нужно убрать"""
            for node in self:
                if not new_item.model.__name__ == node.model.__name__:
                    continue
                all_foreign_key_columns = frozenset(ModelTools.get_foreign_key_columns(node.model))
                current_fk_values = frozenset(node.value) & all_foreign_key_columns & frozenset(new_item.value)
                for fk_name in current_fk_values:
                    if node.value[fk_name] == new_item.value[fk_name]:
                        new_values = node.get_attributes()
                        del new_values[fk_name]
                        self._remove_from_queue(node)
                        self.append(**new_values)
                        break

        exists_item, new_item = self._replication(**attrs)
        self._remove_from_queue(exists_item) if exists_item else None
        remove_other_foreign_key_value()
        self.append(**new_item.get_attributes())

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

    def order_by(self, model: Type[CustomModel],
                 by_column_name: Optional[str] = None, by_primary_key: bool = False,
                 by_create_time: bool = False, decr: bool = False):
        super().order_by(model, by_column_name, by_primary_key, by_create_time, decr)

    def get_related_nodes(self, main_node: QueueItem, other_container=None) -> "Queue":
        """ Получить все связанные (внешним ключом) с передаваемой нодой ноды.
        O(i) * O(1) + O(n) = O(n)"""
        def get_column_name(data: dict, value) -> Optional[str]:
            """ Обойти все поля, и, если значение поля совпадает с value, то вернём это название """
            for name, val in data.items():
                if val == value:
                    return name
        root = self
        if other_container is not None:
            if type(other_container) is not self.__class__:
                raise TypeError
            root = other_container
        container = self.__class__()
        for related_node in root:  # O(n) * O(j) * O(m) * O(n) * O(1) = O(n)
            if not related_node == main_node:  # O(g) * O(j) = O (j)
                for column_name in ModelTools.get_foreign_key_columns(main_node.model):  # O(i)
                    if main_node.model.__name__ == related_node.model.__tablename__:
                        pk_field, pk_value = related_node.get_primary_key_and_value(as_tuple=True)
                        if pk_field == column_name:
                            main_node_fk_column_name = get_column_name(main_node.value, pk_value)
                            if main_node_fk_column_name is not None:
                                container.append(**related_node.get_attributes())  # O(1)
        return container

    def search_nodes(self, model: Type[CustomModel], negative_selection=False,
                     **_filter: dict[str, Union[str, int, Literal["*"]]]) -> "Queue":  # O(n)
        """
        Искать ноды по совпадениям любых полей.
        :arg model: кастомный объект, смотри модуль database/models
        :arg _filter: словарь содержащий набор полей и их значений для поиска, вместо значений допустим знак '*',
        который будет засчитывать любые значения у полей.
        :arg negative_selection: режим отбора нод (найти ноды КРОМЕ ... [filter])
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
                    items.append(**left_node.get_attributes(new_container=items))
                for field_name, value in _filter.items():
                    if field_name in left_node.value:
                        if negative_selection:
                            if value == "*":
                                if field_name not in left_node.value:
                                    items.append(**left_node.get_attributes())
                                continue
                            if not left_node.value[field_name] == value:
                                items.append(**left_node.get_attributes(new_container=items))
                                break
                        else:
                            if value == "*":
                                if field_name in left_node.value:
                                    items.append(**left_node.get_attributes())
                                    continue
                            if left_node.value[field_name] == value:
                                items.append(**left_node.get_attributes(new_container=items))
                                break
        return items

    def get_node(self, model: CustomModel, **primary_key_data) -> Optional[QueueItem]:
        """
        Данный метод используется при инициализации - _replication
        :arg model: объект модели
        :arg primary_key_data: словарь вида - {имя_первичного_ключа: значение}
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

    def __contains__(self, item: QueueItem) -> bool:
        if type(item) is not QueueItem:
            return False
        for node in self:
            if hash(node) == item.__hash__():
                return True
        return False

    def __add__(self, other: "Queue"):
        if not type(other) is self.__class__:
            raise TypeError
        result_instance = self.__class__()
        [result_instance.append(**n.get_attributes(new_container=result_instance)) for n in self]  # O(n)
        [result_instance.enqueue(**n.get_attributes(new_container=result_instance)) for n in other]  # O(n**2) todo n**2!
        return result_instance

    def __iadd__(self, other):
        if not isinstance(other, type(self)):
            raise TypeError
        result: Queue = self + other
        self._head = result.head
        self._tail = result.tail
        return result

    def __sub__(self, other: "Queue"):
        if not isinstance(other, self.__class__):
            raise TypeError
        result_instance = copy.deepcopy(self)
        [result_instance.remove(n.model, *n.get_primary_key_and_value(as_tuple=True)) for n in other]
        return result_instance

    def __and__(self, other: "Queue"):
        if type(other) is not self.__class__:
            raise TypeError
        output = self.__class__()
        for right_node in other:
            left_node = self.get_node(right_node.model, **right_node.get_primary_key_and_value())
            if left_node is not None:
                output.enqueue(**left_node.get_attributes())
                output.enqueue(**right_node.get_attributes())
        return output

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if not len(self) == len(other):
            return False
        return hash(self) == hash(other)

    def __hash__(self):
        return sum(map(hash, self))

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
        if type(self) is Queue:
            if self.LinkedListItem is not QueueItem:
                raise TypeError("Метод _replication, как и класс Queue предназначен для нод типа QueueItem")
        if type(self) is ServiceOrmContainer:
            if self.LinkedListItem is not ServiceOrmItem:
                raise TypeError("Метод _replication, как и класс ServiceOrmContainer предназначен для нод типа ServiceOrmItem")
        potential_new_item = self.LinkedListItem(**new_node_complete_data)  # O(1)
        new_item = None

        def merge(old_node: QueueItem, new_node: QueueItem, dml_type: str) -> QueueItem:
            new_node_data = old_node.get_attributes()
            old_where, new_where = old_node.where, new_node.where
            old_where.update(new_where)
            new_node_data.update({"_where": old_where})
            old_value, new_value = old_node.value, new_node.value
            old_value.update(new_value)
            new_node_data.update(old_value)
            new_node_data.update({"_insert": False, "_update": False, "_delete": False})
            new_node_data.update({dml_type: True, "_ready": new_node.ready})
            new_node_data.update({"_container": self})
            new_node_data.update({"_create_at": new_node.created_at})
            if not new_node.is_relative_primary_key:
                new_node_data.update({"_primary_key_from_ui": new_node.get_primary_key_and_value()})
            else:
                if not old_node.is_relative_primary_key:
                    new_node_data.update({"_primary_key_from_ui": old_node.get_primary_key_and_value()})
            return self.LinkedListItem(**new_node_data)

        def add_pk_from_old_node_in_new_node(old_node: "QueueItem", new_node: "QueueItem") -> "QueueItem":
            if not old_node:
                return new_node
            if not new_node.is_relative_primary_key:
                return new_node
            if old_node.is_relative_primary_key:
                return new_node
            return new_node.__class__(**new_node.get_attributes(),
                                      _primary_key_from_ui=old_node.get_primary_key_and_value())
        exists_item = self.get_node(potential_new_item.model, **potential_new_item.get_primary_key_and_value())  # O(n)
        if exists_item is not None and exists_item.is_relative_primary_key:
            exists_item = None
        if not exists_item:
            exists_item = self.get_node_by_unique_fields(self, potential_new_item)
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
                new_item = add_pk_from_old_node_in_new_node(exists_item, potential_new_item)
        if new_item_is_delete:
            new_item = add_pk_from_old_node_in_new_node(exists_item, potential_new_item)
        if new_item_is_insert:
            if exists_item.type == "_insert" or exists_item.type == "_update":
                new_item = merge(exists_item, potential_new_item, "_insert")
            if exists_item.type == "_delete":
                new_item = add_pk_from_old_node_in_new_node(exists_item, potential_new_item)
        return exists_item, new_item

    def _remove_from_queue(self, left_node: QueueItem) -> None:
        if type(left_node) is not self.LinkedListItem:
            raise TypeError
        del self[left_node.index]


class ResultORMCollection:
    """ Иммутабельная коллекция с набором результата, закрытая на добавление новых элементов """
    ADD_TABLE_NAME_PREFIX: Literal["auto", "add", "no-prefix"] = "auto"

    def __init__(self, collection: "ServiceOrmContainer" = None, prefix_mode=None):
        def is_valid():
            if not issubclass(type(self.__collection), LinkedList):
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


class Sort:
    def __init__(self, container):
        self._reverse = False
        self._field = None
        self.__container = container

    def _create_mapping(self) -> dict[str, Type[LinkedList]]:
        """  Заполнить словарь ключами """
        keys = map(lambda x: (x.upper(), x,), string.ascii_lowercase)
        return {key: type(self.__container)() for key in keys}

    @staticmethod
    def _fill_mapping(data, nodes, target_column_name):
        for item in nodes:
            p = item.value[target_column_name][0]
            data[(p.upper(), p,)].append(**item.get_attributes())


class LettersSortSingleNodes(Sort):
    def __init__(self, nodes: ResultORMCollection):
        super().__init__(nodes)
        self._input_nodes = nodes
        if not isinstance(nodes, ResultORMCollection):
            raise TypeError
        self._nodes_in_sort = None  # Ноды, которые принимают участие в сортировке
        self._other_items = None  # Ноды, которые не участвуют в сортировке (доб в конец)

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
            return {len(node.value[self._field]): node for node in nodes}

        self._slice_other_nodes()
        self._select_nodes_to_sort()
        mapping = create_mapping(self._nodes_in_sort)
        mapping = dict(sorted(mapping.items(), key=lambda x: x[0]))
        return self._merge_mapping(mapping) + self._other_items

    def _select_nodes_to_sort(self):
        """ Вернуть ноды, которые будут участвовать в сортировке """
        self._nodes_in_sort = self._input_nodes.container_cls.__class__()
        for node in self._input_nodes:
            if self._field in node.value:
                self._nodes_in_sort.append(**node.get_attributes())

    def _slice_other_nodes(self):
        """ Вырезать из коллекции ноды, ключевые поля у которых не заполнены.
        Не изменять исходную коллекцию. Присвоить в self._other_items.
        В дальнейшем их планируется добавить в конец сортированной коллекции """
        self._other_items = self._input_nodes.container_cls.__class__()
        for node in self._input_nodes:
            if self._field not in node.value:
                self._other_items.append(**node.get_attributes())

    def _merge_mapping(self, data):
        """ Словарь, который отсортирован, - 'сжать' его значения воедино, сохраняя последовательность """
        output = self._input_nodes.container_cls.__class__()
        for val in data.values():
            output.append(val)
        return output


class LettersSortNodesChain(Sort):
    def __init__(self, group: list[ResultORMCollection]):
        super().__init__(group[0] if group else None)
        self._nodes_chain = group
        if type(group) is not list:
            raise TypeError
        if any(map(lambda x: type(x) is not ResultORMCollection, group)):
            raise TypeError

    def sort_by_alphabet(self):
        """ 1) Свалить все ноды в кучу
         2) Разложить по алфавиту в словарь
         3) создать результирующий список с группами
         4) Разложить группы нод парами, как было изначально, но на новые позиции"""
        def add_all_nodes_in_one_container():
            c = self._nodes_chain[0].container_cls
            for index in self.__select_indexes():
                group = self._nodes_chain[index]
                c += group
            return c

        def get_new_positions_for_groups(mapping):
            def get_node_index_in_mapping(node: "QueueItem", mapping) -> int:
                key = node.get_primary_key_and_value(only_key=True)
                key = (key.upper(), key,)
                if key in mapping:
                    return tuple(mapping).index(key)
            for index in self.__select_indexes():
                for group in self._nodes_chain[index]:
                    pos = []
                    for node in group:
                        index = get_node_index_in_mapping(node, mapping)
                        if index is not None:
                            pos.append(index)
                    if pos:
                        yield min(pos)

        def fill_result(mapping) -> list:
            result = []
            for group_index in get_new_positions_for_groups(mapping):
                nodes_group = self._nodes_chain[group_index]
                result.append(nodes_group)
            return result

        mapping = self._create_mapping()
        joined_nodes = add_all_nodes_in_one_container()
        self._fill_mapping(mapping, joined_nodes, self._field)
        return fill_result(mapping).extend(self.__select_indexes(in_sort=False))

    def sort_by_string_length(self):
        def create_mapping():
            m = {}
            for i in self.__select_indexes():
                group = self._nodes_chain[i]
                ln = []
                for node in group:
                    if self._field in node.value:
                        ln.append(len(node.value[self._field]))
                if not ln:
                    continue
                m.update({min(ln): group})
            return m

        mapping = create_mapping()
        mapping = dict(sorted(mapping.items(), key=lambda k: k[0]))
        return list(mapping.values()).extend(self.__select_indexes(in_sort=False))

    def __select_indexes(self, in_sort=True):
        """ Выбрать индексы коллекции, которые участвуют в сортировке или не участвуют """
        for n, collection in enumerate(self._nodes_chain):
            for node in collection:
                if in_sort:
                    if self._field not in node.value:
                        continue
                    yield n
                    continue
                if self._field in node.value:
                    continue
                yield n


class LettersSort(LettersSortSingleNodes, LettersSortNodesChain):
    """ Сортировка нод по ключевому полю.
     Простейшая сортировка при помощи встроенной функции sorted. """
    def __init__(self, field_name, nodes: ResultORMCollection = None,
                 nodes_group_chain: list[ResultORMCollection] = None, decr=True):
        self._nodes_chain = nodes_group_chain
        self._input_nodes = nodes
        self._reverse = decr
        self._field = field_name
        if nodes is not None:
            super(LettersSortSingleNodes, self).__init__(nodes)
        if nodes_group_chain is not None:
            super(LettersSortNodesChain, self).__init__(nodes_group_chain)
        if not sum((bool(nodes), bool(nodes_group_chain),)) == 1:
            raise ValueError
        if type(self._reverse) is not bool:
            raise TypeError
        if not isinstance(self._field, str):
            raise TypeError
        if not self._field:
            raise ValueError("Данная строка не может быть пустой")

    def sort_by_alphabet(self):
        return super().sort_by_alphabet() \
            if self._nodes_chain else \
            super(LettersSortNodesChain, self).sort_by_alphabet()

    def sort_by_string_length(self):
        return super().sort_by_string_length() \
            if self._nodes_chain else \
            super(LettersSortNodesChain, self).sort_by_string_length()


class OrderByMixin(ABC):
    """ Реализация функционала для сортировки экземпляров ResultORMCollection в виде примеси для класса Result* """
    items = abstractproperty(lambda: ...)
    _merge = abstractmethod(lambda: ...)
    __iter__ = abstractmethod(lambda: ...)

    def __init__(self: Union["Result", "JoinSelectResult"], *args, **kwargs):
        if not isinstance(self, (Result, JoinSelectResult)):
            raise TypeError("Использовать данный класс в наследовании! Как миксин")
        super().__init__(*args, **kwargs)
        self._order_by_args = None
        self._order_by_kwargs = None
        self._is_sort = False

    def order_by(self, *args, **kwargs):
        """ Включить сортировку для экземпляра целевого класса и запомнить аргументы """
        self._order_by_args = args
        self._order_by_kwargs = kwargs
        self._is_sort = True
        self.__is_valid_order_by_params(*args, **kwargs)

    @property
    def items(self) -> Union[list["ResultORMCollection"], "ResultORMCollection"]:
        if not self._is_sort:
            return super().items
        return self._order_by(super().items)

    def __iter__(self):
        if not self._is_sort:
            return super().__iter__()
        return iter(self.items)

    @abstractmethod
    def _order_by(self, nodes: Union["ResultORMCollection", tuple["ResultORMCollection"]]) -> \
            Union["ResultORMCollection", tuple["ResultORMCollection"]]:
        ...

    def __is_valid_order_by_params(self, model, by_column_name, by_primary_key, by_create_time, length, alphabet, decr):
        QueueItem.is_valid_model_instance(model)
        if by_column_name is not None:
            if type(by_column_name) is not str:
                raise TypeError
            if not by_column_name:
                raise ValueError
            self.__check_exists_column_name(model, by_column_name)
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
        if not sum((length, alphabet,)) == 1:
            raise ValueError

    @staticmethod
    def __check_exists_column_name(model, col_name):
        if col_name not in model().column_names:
            raise KeyError(f"В данной таблице отсутствует столбец {col_name}")
        
        
class OrderBySingleResultMixin(OrderByMixin):
    """ Реализация для 'одиночного результата',- запрос к одной таблице. См Main.get_items() """
    def order_by(self, by_column_name: Optional[str] = None, by_primary_key: Optional[bool] = None,
                 by_create_time: Optional[bool] = None, length: bool = False, alphabet: bool = False,
                 decr: bool = False):
        return super().order_by(self._model, by_column_name=by_column_name,
                                by_primary_key=by_primary_key, by_create_time=by_create_time,
                                length=length, alphabet=alphabet, decr=decr)

    def _order_by(self, nodes: ResultORMCollection):
        k = self._order_by_kwargs
        by_column_name, by_primary_key, by_create_time = \
            k["by_column_name"], k["by_primary_key"], k["by_create_time"]
        sorted_nodes = None
        if by_primary_key:
            if nodes:
                pk_string = tuple(nodes[0].get_primary_key_and_value())[0]
                by_column_name = pk_string
        if by_column_name:
            nodes = nodes.get_all_visible_items
            sorting = LettersSort(nodes, by_column_name, decr=k["decr"])
            sorted_nodes = sorting.sort_by_alphabet()
        if by_create_time:
            items = map(lambda node: (node, node.created_at,), nodes)
            getter = operator.itemgetter(1)
            sorted_nodes = sorted(items, key=getter)
        return self.__add_to_output_collection(map(lambda n: n[0], sorted_nodes), type_=nodes.container_cls)

    @staticmethod
    def __add_to_output_collection(nodes, type_=None):
        """ Упаковать выходной результат в экземпляр соответствующего класса коллекции """
        inner = type_()
        [inner.append(n) for n in nodes]
        return ResultORMCollection(inner)


class OrderByJoinResultMixin(OrderByMixin, ModelTools):
    """ Реализация для запросов с join. См Main.join_select() """
    def order_by(self: "JoinSelectResult", model, by_column_name: Optional[str] = None,
                 by_primary_key: Optional[bool] = None,
                 by_create_time: Optional[bool] = None, length: bool = False, alphabet: bool = False,
                 decr: bool = False):
        self.is_valid_model_instance(model)
        if model not in self._models:
            raise ValueError
        return super().order_by(model, by_column_name=by_column_name,
                                by_primary_key=by_primary_key, by_create_time=by_create_time,
                                length=length, alphabet=alphabet, decr=decr)

    def _order_by(self, nodes: Iterable["ResultORMCollection"]) -> list["ResultORMCollection"]:
        model = self._order_by_args[0]
        k = self._order_by_kwargs
        by_column_name, by_primary_key, by_create_time = \
            k["by_column_name"], k["by_primary_key"], k["by_create_time"]
        sort_by_length = k["length"]
        nodes = list(self)
        if by_primary_key:
            if not nodes:
                return []
            by_column_name = nodes[0][0].get_primary_key_and_value()[0]
        if by_column_name:
            instance = LettersSort(by_column_name, nodes_group_chain=nodes, decr=k["decr"])
            if sort_by_length:
                return instance.sort_by_string_length()
            return instance.sort_by_alphabet()
        if by_create_time:
            return self.__sort_nodes_by_create_time()

    def __sort_nodes_by_create_time(self):
        def nodes_map():
            for nodes_group in self:
                yield min(map(lambda node: node.created_at, nodes_group)), nodes_group
        return list(dict(sorted(nodes_map(), key=operator.itemgetter(0))).values())


class SQLAlchemyQueryManager:
    MAX_RETRIES: Union[int, Literal["no-limit"]] = "no-limit"

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
        session = Main.database
        while sorted_data:
            node_group = sorted_data.pop(-1)
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
                except SQLAlchemyError as error:
                    print(error)
                    self.remaining_nodes += node_group
                    point.rollback()
            else:
                try:
                    session.execute(items_to_commit.pop())
                except SQLAlchemyError as error:
                    print(error)
                    self.remaining_nodes += node_group
            try:
                print("COMMIT")
                session.commit()
            except SQLAlchemyError as error:
                print(error)
                self.remaining_nodes += node_group
            except DatabaseException as error:
                self.remaining_nodes += node_group  # todo: O(n**2)!
                print(error)
        self._sorted = []
        self._query_objects = {}

    def _sort_nodes(self) -> list[Queue]:
        """ Сортировать ноды по признаку внешних ключей, определить точки сохранения для транзакций """
        def make_sort_container(n: QueueItem, linked_nodes: Queue, has_related_nodes):
            """
            Рекурсивно искать ноды с внешними ключами
            O(m) * (O(n) + O(j)) = O(n) * O(m) = O(n)
            """
            related_nodes = self._node_items.get_related_nodes(n)  # O(n)
            linked_nodes.add_to_head(**n.get_attributes())
            if not related_nodes:
                if has_related_nodes:
                    return linked_nodes
                return
            else:
                has_related_nodes = True
            for node in related_nodes:
                return make_sort_container(node, linked_nodes, has_related_nodes)
        if self._sorted:
            return self._sorted
        node_ = self._node_items.dequeue()
        other_single_nodes = Queue()
        while node_:
            if self.MAX_RETRIES == "no-limit" or node_.retries < self.MAX_RETRIES:
                if node_.ready:
                    recursion_result = make_sort_container(node_, Queue(), False)
                    if recursion_result is not None:
                        self._sorted.append(recursion_result)
                    else:
                        if node_ not in itertools.chain(*self._sorted):
                            other_single_nodes.append(**node_.get_attributes())
                else:

                    self.remaining_nodes.append(**node_.get_attributes(new_container=self.remaining_nodes))
            node_ = self._node_items.dequeue()
        self._sorted.append(other_single_nodes) if other_single_nodes else None
        return self._sorted


class ServiceOrmItem(QueueItem):
    def get(self, name, default_val=None):
        try:
            result = self.__getitem__(name)
        except KeyError:
            return default_val
        return result

    @property
    def hash_by_pk(self):
        str_ = "".join(map(str, self.get_primary_key_and_value(as_tuple=True)))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def _field_names_validation(self):
        column_names = set(self.model().column_names)
        loss_fields = super()._field_names_validation(from_polymorphizm=True)
        while loss_fields and column_names:
            field = loss_fields.pop()
            if f"{self.model.__name__}.{field}" not in column_names:
                if field not in column_names:
                    loss_fields.add(field)
            column_names.remove(field)
        if loss_fields:
            raise NodeColumnError

    @classmethod
    def _is_valid_container(k, item):
        if not isinstance(item, ServiceOrmContainer):
            raise TypeError(f"Данная нода должна быть в {k.__name__}, а не в {type(item).__name__}")


class ServiceOrmContainer(Queue):
    """ Данный контейнер для использования в JoinSelectResult (результат вызова Main.join_select) """
    LinkedListItem: Union[ServiceOrmItem, ResultORMItem] = ServiceOrmItem

    @property
    def hash_by_pk(self):
        return sum(map(lambda x: x.hash_by_pk, self))

    def __getitem__(self, model_name_or_index: Union[str, int]) -> Union[DoesNotExists, "ServiceOrmContainer", LinkedListItem]:
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


class NodeTypeConverter:
    TYPES = (QueueItem, ResultORMItem, ServiceOrmItem,)
    input_cls = None
    output_cls = None

    @classmethod
    def convert(cls, node):
        cls.__is_valid_configuration()
        if cls.input_cls is ResultORMItem and cls.output_cls is QueueItem:
            return cls._result_orm_item_to_orm_item_queue(node)

    @staticmethod
    def _result_orm_item_to_orm_item_queue(node: ResultORMItem):
        values = node.value
        del values["ui_hidden"]
        return QueueItem(_model=node.model, _update=True, **values, _container=Queue())

    @classmethod
    def __is_valid_configuration(cls):
        if all(map(lambda type_: cls.input_cls is not type_, cls.TYPES)):
            raise TypeError
        if all(map(lambda type_: cls.output_cls is not type_, cls.TYPES)):
            raise TypeError


class Main(ORMAttributes):
    """
    Главный класс
    1) Инициализация
        LinkToObj = Main
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
    CACHE_PATH = ...
    DATABASE_PATH = ...
    RELEASE_INTERVAL_SECONDS = 5.0
    RELEASE_INTERVAL_SECONDS_DEBUG = 0.5
    CACHE_LIFETIME_HOURS = 6 * 60 * 60
    JOIN_SELECT_DIFF_CACHE_MINUTES = 8 * 60
    _memcache_connection: Optional[Client] = None
    _database_session = None
    _timer: Optional[threading.Timer] = None
    _model_obj: Optional[Type[CustomModel]] = None  # Текущий класс модели, присваиваемый автоматически всем экземплярам при добавлении в очередь
    _was_initialized = False

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
    @property
    def cache(cls):
        if cls._memcache_connection is None:
            try:
                cls._memcache_connection = Client(cls.CACHE_PATH, serde=DillSerde)
            except MemcacheError:
                print("Нет соединения с сервисом кеширования!")
                raise MemcacheError
            else:
                print("Подключение к серверу memcached")
        return cls._memcache_connection

    @classmethod
    def drop_cache(cls):
        cls.cache.flush_all()

    @classmethod
    @property
    def database(cls) -> Session:
        if cls._database_session is None:
            engine = create_engine(cls.DATABASE_PATH)
            try:
                session_f = session_factory(bind=engine)
                cls._database_session = scoped_session(session_f)
            except DisconnectionError:
                print("Ошибка соединения с базой данных!")
                raise DisconnectionError
            else:
                print("Подключение к базе данных")
        return cls._database_session()

    @classmethod
    @property
    def items(cls) -> Queue:
        """ Вернуть локальные элементы """
        return cls.cache.get("ORMItems", Queue())

    @classmethod
    def set_item(cls, _model=None, _insert=False, _update=False,
                 _delete=False, _ready=False, _where=None, **value):
        model = _model or cls._model_obj
        if isinstance(_model, str):
            model = ModelTools.import_model(_model)
        cls.is_valid_model_instance(model)
        items: Queue = cls.items
        items.enqueue(_model=model, _ready=_ready,
                      _insert=_insert, _update=_update,
                      _delete=_delete, _where=_where, _create_at=datetime.datetime.now(), _container=items,
                      _primary_key_from_ui=cls.__detect_primary_key(model, value), **value)
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

        def select_from_db():
            if not attrs:
                try:
                    items_db = cls.database.query(model).all()
                except OperationalError:
                    print("Ошибка соединения с базой данных! Смотри константу 'DATABASE_PATH' в модуле models.py, "
                          "такая проблема обычно возникает из-за авторизации. Смотри пароль!!!")
                    raise OperationalError
            else:
                try:
                    items_db = cls.database.query(model).filter_by(**attrs).all()
                except OperationalError:
                    print("Ошибка соединения с базой данных! Смотри константу 'DATABASE_PATH' в модуле models.py, "
                          "такая проблема обычно возникает из-за авторизации. Смотри пароль!!!")
                    raise OperationalError

            def add_to_queue():
                result = Queue()
                for item in items_db:
                    col_names = model().column_names
                    result.append(**{key: item.__dict__[key] for key in col_names}, _insert=True, _model=model,
                                  _container=result, _primary_key_from_ui=cls.__detect_primary_key(model, item.__dict__))
                return result
            return add_to_queue()

        def select_from_cache():
            return cls.items.search_nodes(model, **attrs)
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
                    raise ValueError("При join запросе таблицы 'замой на себя', должна быть указана 1 таблица, "
                                     "к которой выполняется запрос")
            check_transitivity_on_params()
        valid_params()

        def collect_db_data():
            def create_request() -> str:  # O(n) * O(m)
                s = f"orm_helper.database.query({', '.join(map(lambda x: x.__name__, models))}).filter("  # O(l) * O(1)
                on_keys_counter = 0
                for left_table_dot_field, right_table_dot_field in _on.items():  # O(n)
                    s += f"{left_table_dot_field} == {right_table_dot_field}"
                    on_keys_counter += 1
                    if not on_keys_counter == len(_on):
                        s += ", "
                s += ")"
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
                        row.append(_model=join_select_result.__class__, _insert=True, _container=row,
                                   _primary_key_from_ui=cls.__detect_primary_key(join_select_result.__class__,
                                                                                 join_select_result.__dict__), **r)  # O(l)
                    yield row
            sql_text = create_request()
            query: Query = eval(sql_text, {"orm_helper": cls}, ChainMap(*list(map(lambda x: {x.__name__: x}, models)), {"select": select}))
            return add_db_items_to_orm_queue()

        def collect_all_local_nodes():  # n**2!
            heap = Queue()
            temp = cls.items
            for model in models:  # O(n)
                heap += temp.search_nodes(model, **where.get(model.__name__, {}))  # O(n * k)
            return heap

        def collect_local_data() -> Iterator[ServiceOrmContainer]:
            def collect_node_values(on_keys_or_values: Union[dict.keys, dict.values]):  # f(n) = O(n) * (O(k) * O(u) * (O(l) * O(m)) * O(y)); g(n) = O(n * k)
                for node in collect_all_local_nodes():  # O(n)
                    for table_and_column in on_keys_or_values:  # O(k)
                        table, table_column = table_and_column.split(".")  # O(u)
                        if table == node.model.__name__:  # O(l) * O(m)
                            if table_column in node.value:  # O(y)
                                yield {node.model.__name__: node}

            def compare_by_matched_fk() -> Iterator:  # g(n) = O(n * k)
                # f(n) = O(u) + O(2u) + O(n) * (O(j) * O(k) * (O(l) * O(1) * O(b) * (O(a) + O(a) + O(c * v) + O(c1 * v1) + O(m1 * m2) + O(1) + O(1))))
                # f(n) = O(u) + O(2u) + O(n) * (O(j) * O(k) * (O(l) * O(1) * O(b) * O(m * v)))
                # f(n) = O(u) + O(2u) + O(n) * (O(j) * O(k) * O(l * v))
                # f(n) = O(u) + O(2u) + O(n) * O(j * k)
                # f(n) = O(3u) + O(n) * O(j * k)
                # f(n) = O(3u) + O(n * k)
                # g(n) = O(n * k)
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
                                    raw.enqueue(**left_node.get_attributes(new_container=raw))  # todo fixit O(n ** 2)
                                    raw.enqueue(**right_node.get_attributes(new_container=raw))  # todo fixit O(n ** 2)
                        if raw:
                            yield raw
            return compare_by_matched_fk()
        return JoinSelectResult(get_nodes_from_database=collect_db_data, get_local_nodes=collect_local_data,
                                only_database=_db_only, only_local=_queue_only, get_all_local_nodes=collect_all_local_nodes,
                                models=models, where=where, on=_on)

    @classmethod
    def get_node_dml_type(cls, node_pk_value: Union[str, int], model=None) -> Optional[str]:
        """ Получить тип операции с базой, например '_update', по названию ноды, если она найдена, иначе - None
        :arg node_pk_value: значение поля первичного ключа
        :arg model: кастомный объект, смотри модуль database/models
        """
        model = model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(node_pk_value, (str, int,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        left_node = cls.items.get_node(model, **{primary_key_field_name: node_pk_value})
        return left_node.type if left_node is not None else None

    @classmethod
    def remove_items(cls, node_or_nodes: Union[Union[int, str], Iterable[Union[str, int]]], model=None):
        """
        Удалить ноду из очереди на сохранение
        :arg node_or_nodes: значение для поля первичного ключа, одно или несколько
        :arg model: кастомный объект, смотри модуль database/models
        """
        model = model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(node_or_nodes, (tuple, list, set, frozenset, str, int,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        items = cls.items
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
        old_node = cls.items.get_node(model, **{primary_key_field_name: pk_field_value})
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
        container = cls.items
        container.enqueue(**node_data)
        cls.__set_cache(container)

    @classmethod
    def is_node_from_cache(cls, _model=None, **attrs) -> bool:
        model = _model or cls._model_obj
        cls.is_valid_model_instance(_model)
        items = cls.items.search_nodes(model, **attrs)
        if len(items) > 1:
            warnings.warn(f"Нашлось больше одной ноды/нод, - {len(items)}: {items}")
        if items:
            return True
        return False

    @classmethod
    def is_node_ready(cls, _model=None, **attrs):
        model = _model or cls._model_obj
        cls.is_valid_model_instance(_model)
        items = cls.items.search_nodes(model, **attrs)
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
        database_adapter = SQLAlchemyQueryManager(cls.DATABASE_PATH, cls.items)
        database_adapter.start()
        cls.__set_cache(database_adapter.remaining_nodes or None)
        sys.exit()

    @classmethod
    def _init_timer(cls):
        timer = threading.Timer(cls.RELEASE_INTERVAL_SECONDS, cls.release)
        timer.daemon = True
        timer.setName("Main(database push queue)")
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
        #  cls.drop_cache()

    @classmethod
    def __set_cache(cls, nodes):
        cls.cache.set("ORMItems", nodes, cls.CACHE_LIFETIME_HOURS)

    @staticmethod
    def __detect_primary_key(model, value: dict):
        pk = None
        for column_name, attrs_dict in model().column_names.items():
            if attrs_dict["primary_key"]:
                pk = column_name
                break
        if pk in value:
            return {pk: value[pk]}


class ResultCacheTools(Main):
    TEMP_HASH_PREFIX: str = ...
    RESULT_CACHE_KEY: str = ...
    __iter__ = abstractmethod(lambda self: ...)

    def __init__(self, id_: int, *args, **kw):
        if not issubclass(type(self), BaseResult):
            raise TypeError
        if type(id_) is not int:
            raise TypeError
        self._id = str(id_)
        if not self._id:
            raise ValueError
        self.__key = f"{self.TEMP_HASH_PREFIX}{self._id[-5:]}"

    @classmethod
    def _save_result_collection(cls, items: Iterable):
        """ Сохранить выводимый в ui результат в кеш.
        В дальнейшем из него можно будет доставать первичные ключи """
        cls.cache.set(cls.RESULT_CACHE_KEY, items, cls.CACHE_LIFETIME_HOURS)

    def _set_hash(self, nodes):
        self.__is_valid_nodes(nodes)
        hash_sum = set(map(str, map(hash, nodes)))
        self.cache.set(self.__key, hash_sum)
        self._add_to_all_nodes_hash_has_been_in_result(hash_sum)

    def _add_hash_item(self, value):
        self.__is_valid_hash_key(value)
        current_hash = self._get_hash()
        current_hash.add(value)
        self.cache.set(self.__key, current_hash)

    def _get_hash(self) -> set[str]:
        return self.cache.get(self.__key, set())

    def _is_node_hash_has_been_in_result(self, value):
        """ Была ли данная нода(её хеш-сумма) в результатах когда-либо ранее"""
        self.__is_valid_hash_key(value)
        return value in self.__get_all_nodes_has_been_in_result()

    def _add_to_all_nodes_hash_has_been_in_result(self, items: Iterable[str]):
        [self.__is_valid_hash_key(i) for i in items]
        checked = self.__get_all_nodes_has_been_in_result()
        checked.update(items)
        self.cache.set(f"{self.__key}-all", checked)

    def _is_hash_from_checked(self, val):
        return val in self.cache.get(f"{self.__key}-checked", set())

    def _add_hash_to_checked(self, values):
        [self.__is_valid_hash_key(n) for n in values]
        checked = self._get_checked_hash_items()
        checked.update(values)
        self.cache.set(f"{self.__key}-checked", checked)

    def _get_checked_hash_items(self):
        return self.cache.get(f"{self.__key}-checked", set())

    def _set_primary_keys(self, nodes):
        self.__is_valid_nodes(nodes)
        self.cache.set(f"{self.__key}-pk", set(map(str, map(lambda x: x.hash_by_pk, nodes))))

    def _get_primary_keys(self) -> set[str]:
        return self.cache.get(f"{self.__key}-pk", set())

    def __get_all_nodes_has_been_in_result(self) -> set:
        return self.cache.get(f"{self.__key}-all", set())

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


class BaseResult(ABC, ResultCacheTools):
    RESULT_CACHE_KEY: str = ...
    TEMP_HASH_PREFIX: str = ...
    _merge = abstractmethod(lambda: ResultORMCollection())  # Функция, которая делает репликацию нод из кеша поверх нод из бд
    _get_node_by_joined_primary_key_and_value = abstractmethod(lambda model_pk_val_str,
                                                               sep="...": ...)  # Вернуть ноду по
    # входящей строке вида: 'имя_таблицы:primary_key:значение'

    def __init__(self, get_nodes_from_database=None, get_local_nodes=None, only_local=False, only_database=False, **kwargs):
        self.get_nodes_from_database: Optional[callable] = get_nodes_from_database  # Функция, в которой происходит получение контейнера с нодами из бд
        self.get_local_nodes: Optional[callable] = get_local_nodes  # Функция, в которой происходит получение контейнера с нодами из кеша
        self._id = self.__gen_id(**{**kwargs, "only_local": only_local, "only_database": only_database})
        self._only_queue = only_local
        self._only_db = only_database
        self._pointer: Optional["Pointer"] = None
        self.__merged_data = []
        self._is_sort = False
        self.__is_valid()
        super().__init__(self._id)
        self._set_hash(self.items)
        self._set_primary_keys(self.__merged_data)

    def has_changes(self, hash_value=None) -> Optional[Union[bool, ValueError]]:
        """ Изменились ли значения в результатах с момента последнего запроса has_changes.
        :arg hash_value: Если передан, то будет проверяться 1 конкретный результат из всей коллекции результатов
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
        self._set_hash(nodes)
        if self._is_hash_from_checked(hash_value):
            return False
        if hash_value in map(str, map(hash, nodes)):
            return False
        if not self._is_node_hash_has_been_in_result(hash_value):
            return
        self._add_hash_to_checked([hash_value])
        return True

    def has_new_entries(self) -> bool:
        nodes = self.items
        status = not (self._get_primary_keys() == set(map(str, map(lambda v: v.hash_by_pk, nodes))))
        self._set_primary_keys(nodes)
        return status

    @property
    def items(self):
        self.__merged_data = self._merge()
        self._save_result_collection(self.__merged_data)
        return self.__merged_data

    @property
    def pointer(self):
        return ref(self._pointer)()

    @pointer.setter
    def pointer(self: Union["Result", "JoinSelectResult"], wrap_items: tuple):
        items = self.items
        self._save_result_collection(items)
        self._set_hash(items)
        self._pointer = Pointer(self, wrap_items)

    def __iter__(self):
        self.__merged_data = self._merge()
        self._save_result_collection(self.__merged_data)
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
        return True

    def __getitem__(self, item: Union[str, int]):
        if not item:
            raise KeyError
        if type(item) is str:
            node = self._get_node_by_joined_primary_key_and_value(item)
            if node is None:
                raise KeyError
            return node
        if type(item) is int:
            items = self.items
            return dict(zip([node_or_node_group.__hash__() for node_or_node_group in items], items))[item]
        raise TypeError

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
    """ Экземпляр данного класса возвращается функцией Main.get_items() """
    RESULT_CACHE_KEY = "simple_result"
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
        local_items = self.get_local_nodes()
        database_items = self.get_nodes_from_database()
        [output.enqueue(**node.get_attributes(new_container=output))
         for collection in (database_items, local_items,) for node in collection]
        return ResultORMCollection(output)

    def _get_node_by_joined_primary_key_and_value(self, value: Union[str, int]) -> Optional[QueueItem]:
        model, pk, val = self._parse_joined_primary_key_and_value(value)
        return self.items.get_node(model, **{pk: val})


class JoinSelectResult(OrderByJoinResultMixin, BaseResult, ModelTools):
    """
    Экземпляр этого класса возвращается функцией Main.join_select()
    1 экземпляр этого класса 1 результат вызова Main.join_select()
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
    RESULT_CACHE_KEY = "join_result"

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
            self._save_result_collection(result)
        return result

    def __getitem__(self, item: int) -> ServiceOrmContainer:
        if not isinstance(item, int):
            raise TypeError
        if item not in self:
            raise DoesNotExists
        for group in self:
            if hash(group) == item:
                return group

    def __contains__(self, item: Union[int, ServiceOrmContainer, ServiceOrmItem]):
        if not isinstance(item, (ServiceOrmContainer, ServiceOrmItem, int,)):
            return False
        if type(item) is int:
            return item in map(lambda x: hash(x), self)
        if isinstance(item, (ServiceOrmItem, ServiceOrmContainer,)):
            return hash(item) in map(lambda x: x.__hash__(), self)
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
                for group in local_nodes:
                    for node in group:
                        foreign_keys = ModelTools.get_foreign_key_columns(node.model)
                        if foreign_keys:
                            yield node.model.__name__, foreign_keys
            res = ServiceOrmContainer()
            foreign_keys_at_local_items = tuple(collect_foreign_keys_at_local_items())
            for group in local_nodes:
                tables_map = {node.model.__name__: node.model for node in group}
                for table_name, foreign_keys in foreign_keys_at_local_items:
                    find_nodes = group.search_nodes(tables_map[table_name], **{k: "*" for k in foreign_keys})
                    if find_nodes:
                        res.append(**find_nodes[0].get_attributes())
            return res

        def get_filtered_database_items():
            """ Оборвать связи между нодами из БД, если эта связь оборвана в локальных нодах """
            currently_added_hash = []
            nodes_with_foreign_keys_at_local_items = get_local_nodes_with_any_value_in_fk(local_items)
            for db_nodes_group in self.get_nodes_from_database() if not self._only_queue else []:
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
        local_items = list(self.get_local_nodes()) if not self._only_db else []
        return tuple(ResultORMCollection(item) for item in merge(list(get_filtered_database_items()), local_items))

    def _get_node_by_joined_primary_key_and_value(self, joined_pk: str):
        model_name, primary_key, value = self._parse_joined_primary_key_and_value(joined_pk)
        model_instance = ModelTools.import_model(model_name)
        for collection in self:
            node = collection.get_node(model_instance, **{primary_key: value})
            if node:
                return node


class PointerCacheTools(Main):
    POINTER_CACHE_PREFIX = "p_id"
    WRAP_ITEM_MAX_LENGTH = 30

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
                raise ValueError
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
        self.cache.set(self.__cache_key, ",".join(items), self.CACHE_LIFETIME_HOURS)

    def _replace_pointer_cache_item(self, primary_key_hash: str, new_hash_sum: str):
        data = self.cache.get(self.__cache_key, None)
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
        self.cache.set(self.__cache_key, ",".join(all_items))

    def _get_primary_keys(self) -> Iterator[str]:
        return self.__parse_cache_items(1)

    def _get_hash_sum(self):
        return self.__parse_cache_items(2)

    def _get_wrappers(self):
        return self.__parse_cache_items(0)

    def __parse_cache_items(self, val_index):
        data = self.cache.get(self.__cache_key, None)
        if data is None:
            return
        for item in data.split(","):
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
    def __init__(self, result_item: Union[Result, JoinSelectResult], wrap_items: tuple[str]):
        self._id = str(uuid.uuid4())
        self._result_item = result_item
        self._wrap_items = wrap_items
        self.__is_invalid = False
        super().__init__(self._id)
        self._is_valid_config()
        self._set_pointer_configuration(self.__create_cache_data())

    @property
    def wrap_items(self):
        if not self._is_valid():
            return tuple()
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

    def has_changes(self, name: str) -> Optional[Union[bool, Type[Exception]]]:
        """ Получить статус состояния результатов, на которые ранее был задан экземпляр Pointer.
         :arg name: имя одного конкретного результата, одно из многих, которые хранятся в wrap_items
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
                old_primary_key_hash = list(self._get_primary_keys())
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
        cached_primary_keys = tuple(self._get_primary_keys())
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
        if type(self._wrap_items) is not tuple:
            raise PointerWrapperTypeError("В качестве элементов wrapper принимается кортеж строк")
        if not all(map(lambda x: isinstance(x, str), self._wrap_items)):
            raise PointerWrapperTypeError
        if not self._wrap_items:
            raise PointerWrapperLengthError("Контейнер с обёрткой содержимого не может быть пустым")
        if not isinstance(self._result_item, (Result, JoinSelectResult,)):
            raise JoinedItemPointerError(
                "Экземпляр класса JoinSelectResult или Result не установлен в атрибут класса result_item"
            )
        if not len(self._wrap_items) == len(set(self._wrap_items)):
            raise PointerRepeatedWrapper
        t = tuple(self._select_result_getter())
        if not len(tuple(self._select_result_getter())) == self._wrap_items.__len__():
            raise PointerWrapperLengthError
        super()._is_valid_config()

    def __create_cache_data(self, items=None):
        data = tuple(self._select_result_getter()) if items is None else items
        hash_ = tuple(map(hash, data))
        pk = (n.hash_by_pk for n in data)
        wrap_items = list(self._wrap_items)
        return tuple(map(lambda x: f"{wrap_items.pop(0)}:{x[0]}:{x[1]}", zip(pk, hash_)))
