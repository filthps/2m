"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
from typing import Union, Iterator, Optional, Literal
from abc import ABC, abstractmethod
from two_m_root.tools import ModelTools


class AbstractResultMixin:
    def __init__(self, *r, only_local=None, only_database=None, get_nodes_from_database=None,
                 get_local_nodes=None, **w):
        self._only_local = only_local
        self._only_db = only_database
        self._get_nodes_from_database: Optional[callable] = get_nodes_from_database
        self._get_local_nodes: Optional[callable] = get_local_nodes
        super().__init__(*r, only_local=only_local, only_database=only_database,
                         get_nodes_from_database=get_nodes_from_database, get_local_nodes=get_local_nodes, **w)

    @abstractmethod
    def get_local_nodes(self, *args, **kwargs) -> Union[tuple["ServiceOrmContainer"], "ServiceOrmContainer"]:
        """ Перегружаем этот метод в миксине, производя манипуляции с данными или передавая дополнительные аргументы """
        ...

    @abstractmethod
    def get_nodes_from_database(self, *args, **kwargs) -> Union[tuple["ServiceOrmContainer"], "ServiceOrmContainer"]:
        """ Перегружаем этот метод в миксине, производя манипуляции с данными или передавая дополнительные аргументы """
        ...

    @property
    @abstractmethod
    def items(self):
        ...

    @abstractmethod
    def __iter__(self):
        ...


class AbstractNode(ABC):
    @property
    @abstractmethod
    def value(self) -> dict:
        """ Непосредственно - содержимое ноды. """
        ...

    @property
    @abstractmethod
    def next(self):
        """ Ссылка на следующую ноду. """
        ...

    @next.setter
    @abstractmethod
    def next(self, node: "LinkedListItem"):
        """ Ссылка на следующую ноду. """
        ...

    @property
    @abstractmethod
    def prev(self):
        """ Ссылка на предыдущую ноду. """
        ...

    @prev.setter
    @abstractmethod
    def prev(self, node: "LinkedListItem"):
        ...


class AbsQueueNode(ABC):
    @property
    @abstractmethod
    def model(self):
        pass

    @abstractmethod
    def get_primary_key_and_value(self, only_key=False, only_value=False, as_tuple=False):
        pass

    @abstractmethod
    def get_attributes(self):
        pass


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

    def _create_mapping(self, nodes: "ServiceOrmContainer") -> dict:
        """
        :arg nodes: Ноды, которые должны участвовать в сортировке
        Python >= 3.7! Реализовать словарь с определённым кол-вом ключей, расположенных в правильном порядке.
        В качестве значений установить пустые контейнеры, для нод."""
        return {node.value[self._field]: node for node in nodes}

    def _fill_mapping(self, data, *args, **kwargs):
        """ Заполнить словарь с отсортированными ключами, или вернуть новый словарь с отсортированными ключами """
        return dict(sorted(data.items(), key=lambda x: x[0], reverse=self._reverse))

    @staticmethod
    def __is_model_has_column(model, column_name):
        if column_name not in model().column_names:
            raise AttributeError(f"У таблицы {model.__name__} отсутствует столбец {column_name}.")


class AbstractResult:
    @abstractmethod
    def get_nodes_from_database(self, *args, **kwargs) -> Union["ServiceOrmContainer", tuple["ServiceOrmContainer"]]:
        if hasattr(self, "get_nodes_from_database"):
            return super().get_nodes_from_database(*args, **kwargs)

    @abstractmethod
    def get_local_nodes(self, *args, **kwargs) -> Union["ServiceOrmContainer", tuple["ServiceOrmContainer"]]:
        """ Геттер данных из локальной очереди нод. """
        if hasattr(self, "get_local_nodes"):
            return super().get_local_nodes(*args, **kwargs)

    @abstractmethod
    def items(self) -> Union[tuple["ResultORMCollection"], "ResultORMCollection"]:
        """ Возможность производить итерации по содержимому. см __iter__ """
        ...
        return self._create_output(...)

    @abstractmethod
    def __iter__(self) -> Iterator[Union[tuple["ResultORMCollection"], "ResultORMCollection"]]:
        """ Возможность производить итерации по содержимому. """
        ...
        return self._create_output(...)

    @abstractmethod
    def _merge(self, *args, **kwargs) -> Union[tuple["ServiceOrmContainer"], "ServiceOrmContainer"]:
        """ Функция, которая делает репликацию нод из кеша поверх нод из бд """
        ...
        return self._create_output(...)

    @abstractmethod
    def visible_items(self):
        ...
        return self._create_output(...)

    @staticmethod
    @abstractmethod
    def _create_output(data: Union[tuple["ServiceOrmContainer"], "ServiceOrmContainer"], show_hidden_items: Optional[bool] = None) -> Union[tuple["ResultORMCollection"], "ResultORMCollection"]:
        """ Сформировать результирующую последовательность соответственного типа,
        доступную для использования конечным пользователем.
        Отфильтровать ноды или коллекции нод, если в них присутствуют скрытые ноды.
        Обычно удобно скрывать ноды с dml _delete - True. """
        ...
        return ...

    @abstractmethod
    def _final_sort_items(self, merged_data: Union[tuple["ServiceOrmContainer"], "ServiceOrmContainer"]):
        """ Окончательная сортировка предварительно отсортированных нод из 2 разных источников:
         из локальных и из бд """
        ...


class AbstractSliceMixin:
    @abstractmethod
    def reset_slice(self):
        """ 'Сброс' сортировки, снятие играничений длины контейнера """
        ...

    @abstractmethod
    def _get_slice_index(self, current_type: Literal["db", "local"]) -> tuple[int, Union[int, float]]:
        """ Вычислить левый и правый индексы среза, передать именованными параметрами в функции get_nodes_from_database и
         get_local_nodes """
        ...

    @abstractmethod
    def __getitem__(self, item: slice):
        """ Активация среза с его последующей мемоизацией в атрибуты экземпляра класса _left_border и _right_border """
        ...
