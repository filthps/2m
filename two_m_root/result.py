"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import hashlib
from abc import ABC, abstractmethod
from weakref import ref
from typing import Union, Optional, Iterable
from two_m_root.abstractions import AbstractResult
from two_m_root.containers import ResultORMCollection, ServiceOrmContainer
from two_m_root.nodes import ResultORMItem
from two_m_root.tools import ModelTools
from two_m_root.core import Tool
from two_m_root.mixins import SliceResultMultiTypeDataMixin, SliceResultSingleTypeDataMixin, \
    OrderBySingleResultMixin, OrderByJoinResultMixin, ResultPaginator
from two_m.main import ITER_ONLY_VISIBLE_ITEMS_AS_DEFAULT


class ResultCacheTools(Tool):
    TEMP_HASH_PREFIX: str = ...
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
        super().__init__(id_, *args, **kw)

    def _set_hash(self, nodes):
        self.__is_valid_nodes(nodes)
        hash_sum = set(map(str, map(hash, nodes)))
        self.connection.cache.set(self.__key, hash_sum)
        pk_hash_sum = set(map(str, map(lambda node: node.hash_by_pk, nodes)))
        hash_sum_and_pk_hash_sum = hash_sum.union(pk_hash_sum)
        self._add_to_all_nodes_hash_has_been_in_result(hash_sum_and_pk_hash_sum)

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


class BaseResult(SliceResultSingleTypeDataMixin, SliceResultMultiTypeDataMixin, ResultCacheTools, AbstractResult, ABC):
    TEMP_HASH_PREFIX: str = ...
    ITER_ONLY_VISIBLE_ITEMS_AS_DEFAULT = ITER_ONLY_VISIBLE_ITEMS_AS_DEFAULT

    def __init__(self, get_nodes_from_database=None, get_local_nodes=None,
                 only_local=False, only_database=False, **kwargs):
        self._get_nodes_from_database: Optional[callable] = get_nodes_from_database  # Функция, в которой происходит получение контейнера с нодами из бд
        self._get_local_nodes: Optional[callable] = get_local_nodes  # Функция, в которой происходит получение контейнера с нодами из кеша
        self._id = self.__gen_id(**{**kwargs, "only_local": only_local, "only_database": only_database})
        self._only_queue = only_local
        self._only_db = only_database
        self._pointer: Optional["Pointer"] = None
        self._is_slice = False
        self._is_sort = False
        self.__is_valid()
        super().__init__(self._id, only_local=only_local, only_database=only_database,
                         get_local_nodes=get_local_nodes, get_nodes_from_database=get_nodes_from_database, **kwargs)
        merged_data = self.items
        self._set_hash(merged_data)
        self._set_primary_keys(merged_data)

    def get_nodes_from_database(self, **kwargs):
        if self._only_queue:
            return []
        if hasattr(super(), "get_nodes_from_database"):
            items = super().get_nodes_from_database(**kwargs)
            if items is not None:  # if abstract
                return items
        return self._get_nodes_from_database(**kwargs)

    def get_local_nodes(self, **kwargs):
        if self._only_db:
            return []
        if hasattr(super(), "get_local_nodes"):
            items = super().get_local_nodes(**kwargs)
            if items is not None:  # if abstract
                return items
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
            if hash_value not in [str(node.hash_by_pk) for node in nodes]:
                self._set_hash(nodes)
                return True
            if set(filter(lambda x: x == hash_value, map(str, [hash(n) for n in nodes]))) in self._get_hash():
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
        merged_data = self._merge()
        sorted_data = self._final_sort_items(merged_data)
        return self._create_output(sorted_data,
                                   show_hidden_items=False if self.ITER_ONLY_VISIBLE_ITEMS_AS_DEFAULT else None)

    @property
    def visible_items(self):
        merged_data = self._merge()
        sorted_data = self._final_sort_items(merged_data)
        return self._create_output(sorted_data, show_hidden_items=False)

    @property
    def hidden_items(self):
        merged_data = self._merge()
        sorted_data = self._final_sort_items(merged_data)
        return self._create_output(sorted_data, show_hidden_items=True)

    @property
    def pointer(self):
        return ref(self._pointer)()

    @pointer.setter
    def pointer(self: Union["Result", "JoinSelectResult"], wrap_items: list):
        from two_m_root.pointer import Pointer
        items = self.items
        self._set_hash(items)
        self._pointer = Pointer(self, wrap_items)

    def __iter__(self):
        merged_data = self._merge()
        merged_data = self._final_sort_items(merged_data)
        result = self._create_output(merged_data,
                                     show_hidden_items=False if self.ITER_ONLY_VISIBLE_ITEMS_AS_DEFAULT else None)
        return result.__iter__()

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
        if isinstance(item, slice):
            return super().__getitem__(item)
        return self.items[item]

    def __str__(self):
        return f"{self.__class__.__name__}({self.items})"

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
                raise TypeError
        if not self._only_db:
            if not callable(self.get_nodes_from_database):
                raise TypeError


class Result(OrderBySingleResultMixin, BaseResult, ResultPaginator, ModelTools):
    """ Экземпляр данного класса возвращается функцией Tool.get_items() """
    TEMP_HASH_PREFIX = "simple_item_hash"

    def __init__(self, *args, model=None, **kwargs):
        self._model = model
        self.__is_valid()
        super().__init__(*args, model=model, **kwargs)

    def _merge(self):
        output = ServiceOrmContainer()
        local_items = self.get_local_nodes()
        database_items = self.get_nodes_from_database()
        [output.enqueue(**node.get_attributes())
         for collection in (database_items, local_items,) for node in collection]
        return output

    def _final_sort_items(self, merged_data):
        """ Окончательная сортировка. После смешивания данных из базы данных и локальных данных """
        if not self._is_sort:
            return merged_data
        if not issubclass(self.__class__, OrderBySingleResultMixin):
            raise Exception
        return self.sort_items(self._model, merged_data, **self._create_params_to_sort_items())

    @staticmethod
    def _create_output(data, show_hidden_items=False):
        """ Упаковать результат,
        готовый для использования конечным пользователем,
        в специальный защищённый контейнер """
        return ResultORMCollection(data, show_hidden_nodes=show_hidden_items)

    def __is_valid(self):
        self.is_valid_model_instance(self._model)


class JoinSelectResult(OrderByJoinResultMixin, BaseResult, ResultPaginator, ModelTools):
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

    def __init__(self, *args, models=None, get_nodes_from_local_with_null_fk=None,
                 get_local_nodes_without_foreign_key_nodes=None,
                 get_all_local_nodes=None, on=None, **kwargs):
        self._models = models
        self.__get_nodes_from_local_with_null_fk = get_nodes_from_local_with_null_fk
        self.__get_local_nodes_without_foreign_key_nodes = get_local_nodes_without_foreign_key_nodes
        self.__get_all_local_nodes = get_all_local_nodes
        self.__on_items = {tuple(key.split(".")): tuple(value.split(".")) for key, value in on.items()}
        self.__is_valid()
        super().__init__(*args, models=models, **kwargs)

    def __getitem__(self, item: Union[slice, int]):
        if type(item) is not int:
            return super().__getitem__(item)
        data = self.items
        if type(item) is not int:
            raise TypeError
        if item in range(len(data)):
            return data[item]
        for items_group in data:
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

    def _merge(self):
        def check_input_items(items):
            """ Тестировать входящие результаты на соответствие. """
            if not isinstance(items, list):
                raise TypeError
            all_nodes_per_group_counter = []  # Для проверки попадания ноды сразу в несколько групп результатов, а также,
            # для проверки повторения ноды в каждой конкретной группе результатов
            fk_node_counter = {}  # Для проверки мультиссылочности конкретной ноды на несколько нод в рамках 1 группы результатов
            group_counter = []  # Для проверки повторения группы во всей коллекции результатов
            for items_group in items:
                group_counter.append(str(items_group.hash_by_pk))
                all_nodes_per_group_counter.append([str(n.hash_by_pk) for n in items_group])
                fk_node_counter = {}
                for node in items_group:
                    d = dict.fromkeys(ModelTools.get_foreign_key_columns(node.model), 0)
                    fk_node_counter.update({str(node.hash_by_pk): d})
                for node in items_group:
                    fk_column_names = ModelTools.get_foreign_key_columns(node.model)
                    for key in fk_column_names:
                        if key in node.value:
                            fk_node_counter[str(node.hash_by_pk)][key] += 1
                if any([True if value > 1 else False
                        for node in fk_node_counter.values()
                        for value in node.values()]):
                    raise ValueError("Более одной ноды ссылается на одну и ту же ноду, в рамках одной группы результата.")
            if not len(group_counter) == len(set(group_counter)):
                raise ValueError("В коллекции результатов присутствует несколько идентичных групп нод.")
            if any(map(lambda x: not len(x) == len(set(x)), all_nodes_per_group_counter)):
                raise ValueError("В одной из групп нод есть повторяемая (по первичному ключу) нода.")
            if not len([n for group in all_nodes_per_group_counter for n in group]) == \
                    len(set([n for group in all_nodes_per_group_counter for n in group])):
                raise ValueError("В нескольких коллекциях присутствует одна и так же нода")

        def filter_relationship_preliminarily(db, local):
            """ Предварительная фильтрация.
            Удалить из обеих выборок (local и БД) ноды, в которых поля отношений содержат null.
            Если в данной группе (PK'node - FK'node - ...) осталась только одна нода,
            то полностью удаляем эту группу. """
            null_foreign_key_nodes = self.__get_nodes_from_local_with_null_fk()
            if not null_foreign_key_nodes:
                return
            for all_items_by_current_type in [db, local]:
                for i, nodes_group in enumerate(all_items_by_current_type):
                    for node_with_null_fk in null_foreign_key_nodes:
                        if node_with_null_fk in nodes_group:
                            del nodes_group[node_with_null_fk.model.__name__]
                            if len(nodes_group) == 1:
                                del all_items_by_current_type[i]

        def filter_relationship_final(db, local):
            for local_node_group in local:
                for pk_data, fk_data in self.__on_items.items():
                    pk_model_name, _ = pk_data
                    fk_model_name, _ = fk_data
                    pk_node = local_node_group[pk_model_name]
                    fk_node = local_node_group[fk_model_name]
                    if fk_node is None:
                        continue
                    for index, db_node_group in enumerate(db):
                        fk_node_was_removed = False
                        if pk_node in db_node_group:
                            fk_node_db = db_node_group[fk_model_name]
                            if fk_node_db is None:
                                break
                            if not fk_node_db == fk_node:
                                db_node_group.remove(fk_node_db.model, *fk_node_db.get_primary_key_and_value(as_tuple=True))
                                fk_node_was_removed = True
                        if fk_node_was_removed and len(db_node_group) == 1:
                            del db[index]

        def fix_local_fk_value(merged_data):
            """ Редкий случай, когда в локальной ноде было установлено несуществующее значение для внешнего ключа,
             но нода нашлась в базе данных, имея другое значение первичного ключа.
             Учитывая особенности процесса репликации, ноды из локального расположения переписывают значения нод из бд,
              но в данном конкретном случае это неправильно. """
            for data in merged_data:
                for pk_data, fk_data in self.__on_items.items():
                    pk_model_name, primary_key = pk_data
                    fk_model_name, foreign_key = fk_data
                    pk_node = data[pk_model_name]
                    fk_node = data[fk_model_name]
                    if not fk_node.value[foreign_key] == pk_node.value[primary_key]:
                        new_values = fk_node.get_attributes(with_update={foreign_key: pk_node.value[primary_key]})
                        data.replace(fk_node, fk_node.__class__(**new_values))
                yield data

        def merge(db_items, local_items_):
            for db_nodes_group in db_items:
                exist = False
                for pk_items in self.__on_items:
                    model_name, _ = pk_items
                    pk_node = db_nodes_group[model_name]
                    for local_node_group in local_items_:
                        if local_node_group.get_node(pk_node.model,
                                                     **pk_node.get_primary_key_and_value()) is not None:
                            yield db_nodes_group + local_node_group
                            exist = True
                            db_items.remove(db_nodes_group)
                            local_items_.remove(local_node_group)
                            break
                    if exist:
                        break
            for db_nodes_group in db_items:
                yield db_nodes_group
            for local_node_group in local_items_:
                yield local_node_group

        def update_node_data(merged_data):
            """ Пройтись по всем нодам в результатах.
            Обновить содержимое нод, согласно содержимому из локальных данных. """
            all_nodes = self.__get_all_local_nodes()
            for group in merged_data:
                c = ServiceOrmContainer()
                for merged_node in group:
                    find_merged_node_in_local_nodes = False
                    for node in all_nodes:
                        if merged_node == node:
                            data = merged_node.get_attributes()
                            data.update(node.value)
                            c.append(**data)
                            find_merged_node_in_local_nodes = True
                    if not find_merged_node_in_local_nodes:
                        c.append(node_item=merged_node)
                yield c
        local_items = list(self.get_local_nodes())
        all_nodes_from_database = list(self.get_nodes_from_database())
        if not local_items:
            return all_nodes_from_database
        if not all_nodes_from_database:
            return local_items
        check_input_items(local_items)
        check_input_items(all_nodes_from_database)
        filter_relationship_preliminarily(all_nodes_from_database, local_items)
        filter_relationship_final(all_nodes_from_database, local_items)
        result_data = merge(all_nodes_from_database, local_items)
        result_data = fix_local_fk_value(result_data)
        result_data = update_node_data(result_data)
        return result_data

    @staticmethod
    def _create_output(data, show_hidden_items=False) -> tuple[ResultORMCollection]:
        result = []
        for item in data:
            nodes = ResultORMCollection(item, show_hidden_nodes=show_hidden_items)
            result.append(nodes) if nodes else None
        return tuple(result)

    def _final_sort_items(self, merged_data):
        """ Окончательная сортировка. После смешивания данных из базы данных и локальных данных """
        if not self._is_sort:
            return merged_data
        if not issubclass(self.__class__, OrderByJoinResultMixin):
            raise Exception
        sort_params = self._create_params_to_sort_items()
        return self.sort_items(self._model, tuple(merged_data), **sort_params)

    def __is_valid(self):
        if type(self.__on_items) is not dict:
            raise TypeError
        for key, value in self.__on_items.items():
            if type(key) is not tuple:
                raise TypeError
            if not isinstance(value, tuple):
                raise TypeError
            if not len(key) == 2:
                raise ValueError
            if not len(value) == 2:
                raise ValueError
        if not self._models:
            raise ValueError
        [self.is_valid_model_instance(m) for m in self._models]
        if not callable(self.__get_nodes_from_local_with_null_fk):
            raise ValueError
        if not callable(self.__get_local_nodes_without_foreign_key_nodes):
            raise ValueError
        if not callable(self.__get_all_local_nodes):
            raise ValueError
