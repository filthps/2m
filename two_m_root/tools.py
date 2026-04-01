"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import importlib
from typing import Union, Iterator, Optional, Type, Any
from two_m_root.conf import CustomModel
from two_m_root.exceptions import ModelsConfigurationError, InvalidModel, NodePrimaryKeyError, \
    NodeColumnValueError, NodeColumnError, NodeDMLTypeError
from two_m.main import BASE_DIR_ROOT


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
    def get_unique_columns(cls, model, data: Optional[dict] = None) -> Iterator[str]:
        """ Получить названия столбцов с UNIQUE=TRUE (их значения присутствуют в ноде) """
        cls.is_valid_model_instance(model)
        model_data = model().column_names
        if data is not None:
            if "_ui_hidden" in data:
                del data["_ui_hidden"]
        for column_name in model_data:
            if model_data[column_name]["unique"]:
                if data is None:
                    yield column_name
                    continue
                if column_name in data:
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
                                 package=BASE_DIR_ROOT), name, None)
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
        from two_m_root.nodes import QueueItem, ServiceOrmItem
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
