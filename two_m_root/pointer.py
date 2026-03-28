"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import copy
import uuid
from itertools import zip_longest
from abc import abstractmethod
from typing import Union, Iterator, Iterable, Optional
from two_m_root.core import Tool
from two_m_root.containers import ResultORMCollection
from two_m_root.nodes import ResultORMItem
from two_m_root.result import JoinSelectResult, Result
from two_m_root.exceptions import PointerWrapperLengthError, PointerWrapperTypeError, PointerRepeatedWrapper, \
    JoinedItemPointerError
from two_m.main import WRAP_ITEM_MAX_LENGTH


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
