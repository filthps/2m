"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import copy
import datetime
import itertools
import hashlib
from weakref import ref
from typing import Union, Optional, Literal
from sqlalchemy import delete, insert, update, text
from sqlalchemy.orm import Query
from two_m_root.conf import CustomModel
from two_m_root.abstractions import AbstractNode, AbsQueueNode
from two_m_root.tools import ModelTools, NodeTools
from two_m_root.exceptions import NodePrimaryKeyError


class LinkedListItem(AbstractNode):
    def __init__(self, **values):
        self._val = values
        self._index = 0
        self.__next = None
        self.__prev = None

    def get_attributes(self):
        return self.value

    @property
    def next(self):
        return self.__next

    @next.setter
    def next(self, val: Optional["LinkedListItem"]):
        self._is_valid_item(val)
        self.__next = val
        if not val:
            return
        val.index = self._index + 1

    @property
    def prev(self):
        return self.__prev() if self.__prev is not None else None

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        if not isinstance(value, int):
            raise TypeError
        if value < 0:
            raise ValueError
        self._index = value

    @prev.setter
    def prev(self, item: Optional["LinkedListItem"]):
        if item is None:
            return
        self._is_valid_item(item)
        self.__prev = ref(item)
        if not self._index:
            item.index = 0
            self._index = 1
        else:
            item.index = self._index - 1

    @property
    def value(self) -> dict:
        return copy.copy(self._val)

    @classmethod
    def _is_valid_item(cls, item):
        if item is None:
            return
        if not type(item) is cls:
            raise TypeError

    def __eq__(self, other: "LinkedListItem"):
        if other is None:
            return False
        try:
            self._is_valid_item(other)
        except TypeError:
            return False
        return self._val == other.value

    def __repr__(self):
        return f"{type(self).__name__}({str(self)})"

    def __str__(self):
        return str(self._val)


class QueueItem(LinkedListItem, ModelTools, NodeTools, AbsQueueNode):
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


class ServiceOrmItem(QueueItem, AbsQueueNode):
    """ Данный тип нод используется для вывода результата """
    @property
    def hash_by_pk(self):
        str_ = "".join(map(str, self.get_primary_key_and_value(as_tuple=True)))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    @property
    def retries(self):
        """ Функционал не предназначенный для данного типа нод """
        return None

    def make_query(self):
        """ Функционал не предназначенный для данного типа нод """
        pass

    def __hash__(self):
        value = self.value
        if ModelTools.is_autoincrement_primary_key(self.model):
            del value[self.get_primary_key_and_value(only_key=True)]
        str_ = "".join(map(str, itertools.chain(*value.items())))
        return int.from_bytes(hashlib.md5(str_.encode("utf-8")).digest(), "big")

    def __eq__(self, other: "ServiceOrmItem"):
        if type(other) is not type(self):
            return False
        return str(self.hash_by_pk) == str(other.hash_by_pk)

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


class ResultORMItem(LinkedListItem, NodeTools, AbsQueueNode):
    def __init__(self, model=None, primary_key=None, created_at=None, _ui_hidden=False, **k):
        self._primary_key = primary_key
        self._model = model
        self._hidden = _ui_hidden  # Является ли нода скрытой от отображения при вызове __str__ или __repr__ контейнера
        self._time = created_at
        super().__init__(**self.__clean_kwargs(k))
        self.__is_valid()

    @property
    def model(self):
        return self._model

    @property
    def hidden(self):
        return self._hidden

    @property
    def created_at(self):
        return self._time

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
        return {"model": self._model, "primary_key": self._primary_key, "created_at": self._time,
                "_ui_hidden": self._hidden, **self._val}

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
        return f"model={self.model}, primary_key={self._primary_key}, created_at={self._time}, " \
               f"{', '.join(map(lambda x: '='.join(map(str, x)) , self._val.items()))}"

    @staticmethod
    def __clean_kwargs(kwargs_dict) -> dict:
        return {key: value for key, value in kwargs_dict.items() if not key.startswith("_")}

    def __is_valid(self):
        if type(self._hidden) is not bool:
            raise TypeError
        if not isinstance(self._time, datetime.datetime):
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
