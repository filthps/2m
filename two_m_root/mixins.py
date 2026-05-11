"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import math
from abc import abstractmethod
from typing import Union, Optional, Literal
from itertools import cycle
from two_m_root.abstractions import AbstractResultMixin, AbstractSliceMixin
from two_m_root.conf import CustomModel
from two_m_root.tools import ModelTools
from two_m_root.containers import ServiceOrmContainer, ResultORMCollection
from two_m_root.nodes import QueueItem
from two_m_root.sort import LetterSortSingleNodes, LetterSortNodesChain, NumberSortSingleNodes, NumberSortNodesChain
from two_m.main import ITEMS_ON_PAGE, BY_PRIMARY_KEY, BY_COLUMN_NAME, BY_CREATE_TIME, BY_ALPHABET, BY_STRING_LENGTH, REVERSED


class OrderByMixin(AbstractResultMixin):
    """ Реализация функционала для сортировки экземпляров ResultORMCollection
    в виде примеси для классов Result и JoinSelectResult """
    BY_PRIMARY_KEY = BY_PRIMARY_KEY  # Дефолтные режимы сортировки
    BY_COLUMN_NAME = BY_COLUMN_NAME
    BY_CREATE_TIME = BY_CREATE_TIME
    BY_ALPHABET = BY_ALPHABET
    BY_STRING_LENGTH = BY_STRING_LENGTH
    REVERSED = REVERSED

    def __init__(self: Union["Result", "JoinSelectResult"], *args, **kwargs):
        from two_m_root.result import BaseResult, Result, JoinSelectResult
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
        super().__init__(*args, **kwargs)
        self._is_sort = False
        if self.BY_STRING_LENGTH or self.BY_COLUMN_NAME or self.BY_CREATE_TIME:
            self._is_sort = True
        self._by_primary_key = self.BY_PRIMARY_KEY
        self._by_column_name = self.BY_COLUMN_NAME
        self._by_time = self.BY_CREATE_TIME
        self._by_alphabet = self.BY_ALPHABET
        self._by_string_length = self.BY_STRING_LENGTH
        self._reversed = self.REVERSED

    @abstractmethod
    def order_by(self, by_column_name, by_primary_key, by_create_time, length, alphabet, decr):
        if not by_create_time and not by_primary_key and not by_column_name:
            self._by_time = self.BY_CREATE_TIME
            self._by_primary_key = self.BY_PRIMARY_KEY
            self._by_column_name = self.BY_COLUMN_NAME
        else:
            self._by_time = by_create_time
            self._by_primary_key = by_primary_key
            self._by_column_name = by_column_name
        if not length and not alphabet:
            self._by_alphabet = self.BY_ALPHABET
            self._by_string_length = self.BY_STRING_LENGTH
        else:
            self._by_alphabet = alphabet
            self._by_string_length = length
        self._reversed = decr if decr is not None else self.REVERSED
        self._is_sort = True

    def get_nodes_from_database(self, **kwargs):
        if self._is_sort:
            kwargs.update(self._create_params_to_sort_items())
        if hasattr(super(), "get_nodes_from_database"):
            nodes = super().get_nodes_from_database(**kwargs)
            if nodes is not None:  # is None if abstract
                return self._sort_items(nodes, **kwargs)
        nodes = self._get_nodes_from_database(**kwargs)
        return self._sort_items(nodes, **kwargs)

    def get_local_nodes(self, **kwargs):
        if self._is_sort:
            kwargs.update(self._create_params_to_sort_items())
        if hasattr(super(), "get_local_nodes"):
            nodes = super().get_local_nodes(**kwargs)
            if nodes is not None:  # is None if abstract
                return self._sort_items(nodes, **kwargs)
        nodes = self._get_local_nodes(**kwargs)
        return self._sort_items(nodes, **kwargs)

    @abstractmethod
    def _sort_items(self, items, int_sort: Union[bool, str] = False,
                   string_sort: Union[bool, str] = False,
                   by_length=False, by_alphabet=False, by_create_time=False,
                   reversed_=False):
        """ Произвести сортировку контейнеров содержимого согласно переданным параметрам """
        ...

    def _create_params_to_sort_items(self) -> dict:
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
                if self._by_string_length:
                    output.update({"by_length": True})
                if self._by_alphabet:
                    output.update({"by_alphabet": True})
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
        if not isinstance(decr, (type(None), bool,)):
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
    def __init__(self, *a, **k):
        from two_m_root.result import Result
        if not isinstance(self, Result):
            raise TypeError
        if not hasattr(self, "_model"):
            raise AttributeError
        ModelTools.is_valid_model_instance(self._model)
        super().__init__(*a, **k)

    def order_by(self, by_column_name: Optional[str] = None, by_primary_key: Optional[bool] = None,
                 by_create_time: Optional[bool] = None, length: bool = False, alphabet: bool = False,
                 decr: Optional[bool] = None):
        self._is_valid_order_by_params(self._model, by_column_name, by_primary_key, by_create_time, length, alphabet,
                                       decr)
        super().order_by(by_column_name, by_primary_key, by_create_time, length, alphabet,
                                       decr)

    def _sort_items(self, items: ServiceOrmContainer, int_sort: Union[bool, str] = False,
                    string_sort: Union[bool, str] = False,
                    by_length=False, by_alphabet=False, by_create_time=False,
                    reversed_=False, **other_params):
        self.__is_valid_data(items)
        sorted_nodes = None
        if string_sort:
            sorted_nodes = LetterSortSingleNodes(self._model, string_sort, items, reverse=reversed_)
            if by_alphabet:
                return sorted_nodes.sort_by_alphabet()
            if by_length:
                return sorted_nodes.sort_by_string_length()
        if int_sort:
            sorted_nodes = NumberSortSingleNodes(self._model, int_sort, items, reverse=reversed_)
            return sorted_nodes.sort()
        if by_create_time:
            sorted_nodes = ...
        return items

    @staticmethod
    def __is_valid_data(items):
        if not isinstance(items, ServiceOrmContainer):
            raise TypeError


class OrderByJoinResultMixin(OrderByMixin, ModelTools):
    """ Реализация для запросов с join. См Tool.join_select() """
    def __init__(self, *a, **k):
        from two_m_root.result import JoinSelectResult
        if not isinstance(self, JoinSelectResult):
            raise TypeError
        self._model = None
        super().__init__(*a, **k)
        if not hasattr(self, "_models"):
            raise AttributeError
        if type(self._models) is not tuple:
            raise TypeError
        [ModelTools.is_valid_model_instance(model) for model in self._models]

    def order_by(self, model: CustomModel, by_column_name: Optional[str] = None,
                 by_primary_key: bool = False, by_create_time: bool = False, length: bool = False,
                 alphabet: bool = False, decr: Optional[bool] = None):
        self.is_valid_model_instance(model)
        self._is_valid_order_by_params(model, by_column_name, by_primary_key, by_create_time, length, alphabet,
                                       decr)
        self._model = model
        super().order_by(by_column_name, by_primary_key, by_create_time, length, alphabet, decr)

    def _sort_items(self, items: tuple[ServiceOrmContainer], int_sort: Union[bool, str] = False,
                    string_sort: Union[bool, str] = False,
                    by_length=False, by_alphabet=False, by_create_time=False,
                    reversed_=False, model_in_sort=None, **other_params):
        self.__is_valid_data(items)
        if int_sort:
            return NumberSortNodesChain(model_in_sort, int_sort, items, reverse=reversed_).sort()
        if string_sort:
            instance = LetterSortNodesChain(model_in_sort, string_sort, items, reverse=reversed_)
            if by_alphabet:
                return instance.sort_by_alphabet()
            if by_length:
                return instance.sort_by_string_length()
        if by_create_time:
            ...  # todo
        return items

    def _is_valid_order_by_params(self, model, by_column_name, by_primary_key, by_create_time, length, alphabet, decr):
        QueueItem.is_valid_model_instance(model)
        if by_column_name is not None:
            self._check_exists_column_name(model, by_column_name)
        if model not in self._models:
            raise ValueError
        self._model = model
        super()._is_valid_order_by_params(model, by_column_name, by_primary_key, by_create_time, length, alphabet, decr)

    @staticmethod
    def __is_valid_data(items):
        if type(items) is not tuple:
            raise TypeError
        for nodes_group in items:
            if not isinstance(nodes_group, ServiceOrmContainer):
                raise TypeError


class BaseSliceResultMixin:
    """ Функционал для контроля численности выборки в виде реализации среза. Мемоизация параметров среза."""
    UNIFORM_SAMPLING_DB_AND_CACHE = False  # Производить выборку данных из кеша и базы данных равномерно - половина на половину
    # Или сначала в результат пойдёт одна из категорий до исчерпания, а потом вторая
    # Внимание. Если данный режим включён, то минимальное количество элементов в срезе может сильно разниться,
    # и не будет соответствовать ожидаемой длине!

    def __init__(self, *a, **kwargs):
        super().__init__(*a, **kwargs)
        self._is_slice = False
        self._left_border = 0
        self._right_border = float("inf")
        self._call_counter = cycle((1, 2,))
        self._current_call_counter = None

    def reset_slice(self):
        self._is_slice = False
        self._left_border, self._right_border = 0, float("inf")

    @classmethod
    def change_slice_value_on_items_length(cls, items, left, right):
        """ Ограничить срез длиной срезаемых результатов """
        if not hasattr(items, "__iter__"):
            raise TypeError("Не является итерируемым объектом")
        cls._is_valid_slice_params(left, right)
        items_length = len(items)
        return left, right if items_length > right else items_length

    def __getitem__(self, item: slice):
        """ Срез начинается с 1, правая граница не входит, шаг всегда 1 """
        if type(item) is not slice:
            raise TypeError
        start = item.start if item.start is not None else 1
        stop = item.stop if item.stop is not None else float("inf")
        if item.start == 0:
            raise ValueError("Срез начинается с 1")
        self._is_valid_slice(start - 1, stop - 1, item.step)
        self._left_border = start - 1
        self._right_border = stop - 1
        self._is_slice = True

    def _is_valid_slice(self, start, stop, step):
        """ Левая часть среза начинается с 1, правая часть не входит """
        if step is not None:
            if not step == 1:
                raise ValueError("Выборка с шагом не поддерживается, шаг всегда 1")
        start = start if start is not None else 0
        end = stop if stop is not None else float("inf")
        self._is_valid_slice_params(start, end)

    @staticmethod
    def _is_valid_slice_params(start, end):
        if type(start) is not int:
            raise TypeError
        if not isinstance(end, (int, float)):
            raise TypeError
        if type(end) is float:
            if not end == float("inf"):
                raise ValueError
        if start < 0:
            raise ValueError
        if end < 0:
            raise ValueError
        if start > end:
            raise ValueError

    @staticmethod
    def _is_valid_result_items(items: Union[tuple["ServiceOrmContainer"], "ServiceOrmContainer"]):
        if not isinstance(items, (ResultORMCollection, tuple,)):
            raise TypeError
        if type(items) is tuple:
            if not items:
                return
            if type(items[0]) is not ResultORMCollection:
                raise TypeError


class SliceResultMultiTypeDataMixin(BaseSliceResultMixin, AbstractSliceMixin, AbstractResultMixin):
    """ Равномерный срез элементов из разных источников (бд и локальные).
    Например, длина элементов в серезе - 10, тогда в результате будет 5 из локальных элементов и 5 из базы данных. """
    ROUNDING_POLICY: Literal["+", "-"] = "+"  # Округление количества элементов в + или в -
    RESIDUAL_ITEM: Literal["db", "local", "none"] = "db"  # Пример: в результате есть 4 элема БД и 3 из локалки,
    # - кому будет отдано предпочтение оказаться лишним - четвёртым,
    # если общее кол-во элемов в срезе 6, а частное от деления на 2 - 3.
    # или отдавать предпочтение сначала одному из типов до исчерпания, а затем давать из второго типа
    
    def get_nodes_from_database(self, **kwargs):
        if not self.UNIFORM_SAMPLING_DB_AND_CACHE:
            if hasattr(super(), "get_nodes_from_database"):
                return super().get_nodes_from_database(**kwargs)
            return self._get_nodes_from_database(**kwargs)
        self._current_call_counter = self._call_counter.__next__()
        left, right = self._get_slice_index(current_type="db")
        if hasattr(super(), "get_nodes_from_database"):
            items = super().get_nodes_from_database(left_border=left, right_border=right, **kwargs)
            if items is not None:  # if abstract
                return items
        return self._get_nodes_from_database(left_border=left, right_border=right, **kwargs)

    def get_local_nodes(self, **kwargs):
        if not self.UNIFORM_SAMPLING_DB_AND_CACHE:
            if hasattr(super(), "get_local_nodes"):
                return super().get_local_nodes(**kwargs)
            return self._get_local_nodes(**kwargs)
        self._current_call_counter = next(self._call_counter)
        left, right = self._get_slice_index(current_type="local")
        if hasattr(super(), "get_local_nodes"):
            items = super().get_local_nodes(left_border=left, right_border=right, **kwargs)
            if items is not None:  # if abstract
                return items
        return self._get_local_nodes(left_border=left, right_border=right, **kwargs)

    def _get_slice_index(self, current_type):
        if current_type not in ("db", "local",):
            raise ValueError
        if not self._is_slice:
            return self._left_border, self._right_border
        if self._only_db or self._only_local:
            return self._left_border, self._right_border
        if not self._right_border - self._left_border:
            return 0, 0
        length = (self._right_border - self._left_border) / 2
        is_float = (self._right_border - self._left_border) % 2 if not self.RESIDUAL_ITEM == "none" else False
        if not length == float("inf"):
            length = math.floor(length) if self.ROUNDING_POLICY == "+" else math.ceil(length)
        if not length:
            length = 1
        if length == 1:
            left, right = self._left_border, self._right_border
            if self.RESIDUAL_ITEM == current_type:
                left, right = left, right + 1
            if self._current_call_counter == 1:
                left, right = left, right - 1
            if self._current_call_counter == 2:
                left, right = left + 1, right
            if self.RESIDUAL_ITEM == "none" or not is_float:
                return left, right
        if length == float("inf"):
            return self._left_border, float("inf")
        left, right = 0, 0
        if self._current_call_counter == 1:
            left, right = self._left_border, self._right_border - length
        elif self._current_call_counter == 2:
            left, right = length + self._left_border, self._right_border
        if not is_float:
            return left, right
        if self.RESIDUAL_ITEM == "none":
            return left, right
        if self.RESIDUAL_ITEM == "db" and current_type == "db":
            return left, right + 1
        if self.RESIDUAL_ITEM == "local" and current_type == "local":
            return left, right + 1


class SliceResultSingleTypeDataMixin(BaseSliceResultMixin, AbstractSliceMixin, AbstractResultMixin):
    """ В срезе сначала будут представлены данные из одного из источников, а потом, по мере исчерпания, из другого.
     Например: срез [0:10] - Будет состоять только из локальных нод, а срез [10: 20] - из нод из бд. """
    FIRST_ITEMS_TYPE: Literal["db", "local"] = "local"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.FIRST_ITEMS_TYPE not in ("db", "local",):
            raise ValueError

    def get_local_nodes(self, *args, **kwargs):
        if self.UNIFORM_SAMPLING_DB_AND_CACHE:
            if hasattr(super(), "get_local_nodes"):
                return super().get_local_nodes(*args, **kwargs)
            return super()._get_local_nodes(*args, **kwargs)
        self._current_call_counter = self._call_counter.__next__()
        left, right = self._get_slice_index("local")
        if self._current_call_counter == 1:
            if self.FIRST_ITEMS_TYPE == "local":
                if hasattr(super(), "get_local_nodes"):
                    items = super().get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                    if items is None:
                        return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                    return items
                return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
            else:
                if hasattr(super(), "get_nodes_from_database"):
                    items = super().get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                    if items is None:
                        return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                    return items
                return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
        if self._current_call_counter == 2:
            if self.FIRST_ITEMS_TYPE == "local":
                if hasattr(super(), "get_nodes_from_database"):
                    items = super().get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                    if items is None:
                        return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                    return items
                return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
            else:
                if hasattr(super(), "get_local_nodes"):
                    items = super().get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                    if items is None:
                        return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                    return items
                return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)

    def get_nodes_from_database(self, *args, **kwargs):
        if self.UNIFORM_SAMPLING_DB_AND_CACHE:
            if hasattr(super(), "get_nodes_from_database"):
                return super().get_nodes_from_database(*args, **kwargs)
            return self._get_nodes_from_database(*args, **kwargs)
        self._current_call_counter = next(self._call_counter)
        left, right = self._get_slice_index("db")
        if self._current_call_counter == 1:
            if self.FIRST_ITEMS_TYPE == "db":
                if hasattr(super(), "get_nodes_from_database"):
                    items = super().get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                    if items is None:
                        return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                    return items
                return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
            if hasattr(super(), "get_local_nodes"):
                items = super().get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                if items is None:
                    return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                return items
            return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
        if self._current_call_counter == 2:
            if self.FIRST_ITEMS_TYPE == "db":
                if hasattr(super(), "get_local_nodes"):
                    items = super().get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                    if items is None:
                        return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
                    return items
                return self._get_local_nodes(*args, left_border=left, right_border=right, **kwargs)
            if hasattr(super(), "get_nodes_from_database"):
                items = super().get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                if items is None:
                    return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)
                return items
            return self._get_nodes_from_database(*args, left_border=left, right_border=right, **kwargs)

    def _get_slice_index(self, current_type):
        if not self._is_slice:
            return self._left_border, self._right_border
        if self._only_local or self._only_db:
            return self._left_border, self._right_border
        if self._current_call_counter == 1:
            if self.FIRST_ITEMS_TYPE == current_type:
                return self._left_border, self._right_border
            return 0, 0
        if self._current_call_counter == 2:
            if not self.FIRST_ITEMS_TYPE == current_type:
                return self._left_border, self._right_border
            return 0, 0


class ResultPaginator:
    ITEMS_ON_PAGE = ITEMS_ON_PAGE  # float("inf") - пагинатор выключен

    def __init__(self, *a, items_on_page=None, current_page=None, **k):
        self.__page = 1
        self.__items_on_page = items_on_page or self.ITEMS_ON_PAGE
        self.__is_valid(items_on_page=self.__items_on_page, current_page=self.__page)
        if current_page is not None:
            self.paginate(current_page=current_page, items_on_page=self.__items_on_page)

    def paginate(self, items_on_page=None, current_page=1):
        if items_on_page is None:
            self.__items_on_page = self.ITEMS_ON_PAGE
        self.__is_valid(items_on_page=items_on_page, current_page=current_page)
        self.__page = current_page
        self.__items_on_page = items_on_page
        self.page = self.__page

    @property
    def page(self):
        return self.__page

    @page.setter
    def page(self, page_num: int):
        """ Установить номер текущей страницы """
        self.__is_valid(current_page=page_num, items_on_page=self.__items_on_page)
        if self.__items_on_page == float("inf"):
            left_border = 1
            right_border = float("inf")
        else:
            left_border = page_num * self.__items_on_page if page_num > 1 else 1
            right_border = left_border + self.__items_on_page
        self[left_border:right_border]  # use slice mixin
        self.__page = page_num if self else self.__page

    @property
    def next_page(self):
        self.page = self.page + 1
        return self.page

    @property
    def prev_page(self):
        prev_page = self.page - 1
        if prev_page <= 1:
            prev_page = 1
        self.page = prev_page
        if not self.page == prev_page:  # Счётчик не изменился, это говорит о том, что элементов нету
            self.__page = 1

    @property
    def pages_count(self):
        return math.ceil(self.__len__() / self.__items_on_page)

    @classmethod
    def __is_valid(cls, items_on_page=None, current_page=None):
        if not issubclass(cls, SliceResultSingleTypeDataMixin) \
                    or not issubclass(cls, SliceResultMultiTypeDataMixin):
            raise RuntimeError
        if not isinstance(items_on_page, (int, float,)):
            raise TypeError
        if type(items_on_page) is float:
            if not items_on_page == float("inf"):
                raise ValueError
        if items_on_page <= 0:
            raise ValueError
        if not isinstance(current_page, int):
            raise TypeError
        if current_page <= 0:
            raise ValueError
