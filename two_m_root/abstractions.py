"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
from abc import ABC, abstractmethod


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


class AbstractResultMixin:
    def __init__(self, *a, **kw):
        if not hasattr(self, "_get_local_nodes"):
            raise AttributeError
        if not hasattr(self, "_get_nodes_from_database"):
            raise AttributeError
        if not callable(self.get_local_nodes):
            raise TypeError
        if not callable(self.get_nodes_from_database):
            raise AttributeError
        if not isinstance(self, (Result, JoinSelectResult,)):
            raise TypeError
        if not issubclass(self.__class__, BaseResult):
            raise TypeError
        super().__init__(*a, **kw)

    def get_local_nodes(self, *args, **kwargs) -> Union[tuple[ServiceOrmContainer], ServiceOrmContainer]:
        """ Перегружаем этот метод в миксине, производя манипуляции с данными или передавая дополнительные аргументы """
        ...

    def get_nodes_from_database(self, *args, **kwargs) -> Union[tuple[ServiceOrmContainer], ServiceOrmContainer]:
        """ Перегружаем этот метод в миксине, производя манипуляции с данными или передавая дополнительные аргументы """
        ...

    @property
    def items(self):
        ...

    def __iter__(self):
        ...


class AbstractResult:
    @abstractmethod
    def get_nodes_from_database(self, *args, **kwargs) -> Union[ServiceOrmContainer, tuple[ServiceOrmContainer]]:
        if hasattr(self, "get_nodes_from_database"):
            return super().get_nodes_from_database(*args, **kwargs)

    @abstractmethod
    def get_local_nodes(self, *args, **kwargs) -> Union[ServiceOrmContainer, tuple[ServiceOrmContainer]]:
        """ Геттер данных из локальной очереди нод. """
        if hasattr(self, "get_local_nodes"):
            return super().get_local_nodes(*args, **kwargs)

    @abstractmethod
    def items(self) -> Union[tuple[ResultORMCollection], ResultORMCollection]:
        """ Возможность производить итерации по содержимому. см __iter__ """
        ...
        return self._create_output(...)

    @abstractmethod
    def __iter__(self) -> Iterator[Union[tuple[ResultORMCollection], ResultORMCollection]]:
        """ Возможность производить итерации по содержимому. """
        ...
        return self._create_output(...)

    @abstractmethod
    def _merge(self, *args, **kwargs) -> Union[tuple[ServiceResultOrmContainer], ServiceResultOrmContainer]:
        """ Функция, которая делает репликацию нод из кеша поверх нод из бд """
        ...
        return self._create_output(...)

    @abstractmethod
    def visible_items(self):
        ...
        return self._create_output(...)

    @staticmethod
    @abstractmethod
    def _create_output(self, data) -> Union[tuple[ResultORMCollection], ResultORMCollection]:
        """ Сформировать результирующую последовательность соответственного типа,
        доступную для использования конечным пользователем """
        ...
        return ...

    @abstractmethod
    def _filter_items(self, data) -> Union[tuple[ServiceResultOrmContainer], ServiceResultOrmContainer]:
        """ Отфильтровать ноды или коллекции нод, если в них присутствуют скрытые ноды.
        Обычно удобно скрывать ноды с dml _delete - True/ """
        ...


