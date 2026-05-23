"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Python >= 3.7!
"""
import string
import datetime
from two_m_root.conf import CustomModel
from two_m_root.tools import ModelTools
from two_m_root.containers import ServiceOrmContainer
from two_m_root.nodes import ServiceOrmItem


class BaseSort:
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
        self._model = model
        super().__init__(model, column_name, *args, reverse=reverse, **kw)

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


class BaseSortSingleNodes:
    def __init__(self, model, column_name, nodes, *args, **kwargs):
        self._is_valid_node_items(nodes)
        self._input_nodes = nodes
        self._nodes_in_sort = ServiceOrmContainer()  # Ноды, которые принимают участие в сортировке
        self._other_items = ServiceOrmContainer()  # Ноды, которые не участвуют в сортировке (доб в конец)
        self._field = column_name
        self._model = model

    def _select_nodes_to_sort(self):
        """ Вернуть ноды, которые будут участвовать в сортировке """
        self._nodes_in_sort = ServiceOrmContainer()
        for node in self._input_nodes:
            if not node.model.__name__ == self._model.__name__:
                continue
            if self._field in node.get_attributes():
                self._nodes_in_sort.append(**node.get_attributes())

    def _slice_other_nodes(self):
        """ Вернуть ноды НЕ участвующие в сортировке """
        self._other_items = ServiceOrmContainer()
        for node in self._input_nodes:
            if not self._model.__name__ == node.model.__name__:
                self._other_items.append(**node.get_attributes())
                continue
            if self._field not in node.get_attributes():
                self._other_items.append(**node.get_attributes())

    @staticmethod
    def _merge_mapping(data: dict):
        """ Словарь, который отсортирован, - 'сжать' его значения воедино, сохраняя последовательность """
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
    def __init__(self, _, column_name, items_group, **k):
        self._is_valid_nodes_chain(items_group)
        self._nodes_chain = items_group
        self._field = column_name

    def _select_indexes(self, in_sort=True):
        """ Выбрать индексы коллекции, которые участвуют в сортировке или не участвуют """
        for n, collection in enumerate(self._nodes_chain):
            for node in collection:
                if in_sort:
                    if self._field in node.get_attributes():
                        if node.get_attributes()[self._field] is not None:
                            yield n
                            continue
                        break
                    continue
                if self._field not in node.get_attributes():
                    break
                if node.get_attributes()[self._field] is not None:
                    break
                yield n

    def _select_nodes(self, table_name=None, column_name=None, in_sort=True):
        if table_name is None and column_name is None:
            raise ValueError
        u = ServiceOrmContainer()
        for index in self._select_indexes(in_sort=in_sort):
            for node in self._nodes_chain[index]:
                if table_name is not None:
                    if node.model.__name__ == table_name:
                        if column_name is not None:
                            if column_name in node.get_attributes():
                                u.append(node_item=node)
                        else:
                            u.append(node_item=node)
                else:
                    if column_name in node.get_attributes():
                        u.append(node_item=node)
        return u

    def _joined_nodes_hash_to_index_in_input_collection(self):
        return {self._nodes_chain[index].__hash__(): index for index in self._select_indexes()}

    def _node_primary_key_hash_to_joined_nodes_hash(self, model=None):
        d = {}
        for index in self._select_indexes():
            items_group = self._nodes_chain[index]
            for node in items_group:
                if model is not None:
                    if not model.__name__ == node.model.__name__:
                        continue
                if self._field in node.get_attributes():
                    if node.get_attributes()[self._field] is None:
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


class BaseLetterSort(BaseSort):
    def __init__(self, model, column_name, *args, **kwargs):
        super().__init__(model, column_name, *args, **kwargs)
        if not ModelTools.get_column_python_type(model, column_name) is str:
            raise TypeError("Тип значения столбца от ноды, с заданным столбцом для сортировки, "
                            "не является корректным для соритровки данного типа. Ожидался тип - str.")

    def _create_mapping(self, *args) -> dict[tuple[str, str], "ServiceOrmContainer"]:
        """  Заполнить словарь ключами """
        pattern = string.ascii_lowercase
        if not self._reverse:
            pattern = list(pattern)
            pattern.reverse()
            pattern = "".join(pattern)
        keys = map(lambda x: (x.upper(), x,), pattern)
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
        mapping = super(BaseLetterSort, self)._fill_mapping(mapping)
        [output.__iadd__(nodes) for nodes in mapping.values()]
        return output + self._other_items


class LetterSortNodesChain(BaseLetterSort, BaseSortJoinedNodes):
    def sort_by_alphabet(self):
        """
        1) Выделить все участвующие в сортировке ноды в отдельный контейнер
        2) Создать словарь, где в качестве ключей выступают кортежи с буквами, расположенные в словаре упорядоченно,
        согласно алфавиту, а значениями - группы нод
        3) Создать словарь вида {хеш_ноды_участвующей_в_сортировке: хеш_исходной_группы_нод}
        4) Создать словарь вида {хеш_исходной_группы_нод: индекс_в_исходном_списке}
        5) Обойти отсортированный словарь по values, обойти каждый контейнер по нодам,
        брать хеш ноды, извлекать хеш контейнера по словарю из п.3. Извлекать исходный индекс по словарю из п.4
        6) Обойти полученные в п.5 индексы, собрав элементы из исходного списка в результирующий список, вернуть.
        """
        def fill_result(mapping):
            for group_by_letter in mapping.values():
                for node in group_by_letter:
                    nodes_group_hash = hash_item_to_group_mapping[node.hash_by_pk]
                    index_in_input = group_nodes_to_index_mapping[nodes_group_hash]
                    yield self._nodes_chain[index_in_input]

        mapping = self._create_mapping()
        joined_nodes = self._select_nodes(table_name=self._model.__name__, column_name=self._field)
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
        mapping = super(BaseLetterSort, self)._fill_mapping(mapping)
        output = list(mapping.values())
        output.extend([self._nodes_chain[i] for i in self._select_indexes(in_sort=False)])
        return output


class BaseNumberSort(BaseSort):
    def __init__(self, model, column_name, *a, **k):
        super().__init__(model, column_name, *a, **k)
        if not ModelTools.get_column_python_type(model, column_name) is int:
            raise TypeError("Тип значения столбца от ноды, с заданным столбцом для сортировки, "
                            "не является корректным для соритровки данного типа. Ожидался тип - int.")


class NumberSortSingleNodes(BaseNumberSort, BaseSortSingleNodes):
    def __init__(self, model: CustomModel, column_name: str, nodes: "ResultORMCollection", reverse=False):
        super().__init__(model, column_name, nodes, reverse=reverse)

    def sort(self):
        self._select_nodes_to_sort()
        self._slice_other_nodes()
        mapping = self._create_mapping(self._nodes_in_sort)
        mapping = self._fill_mapping(mapping)
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
        self._model = model

    def sort(self):
        nodes_in_sort = self._select_nodes(self._model.__name__, in_sort=True)
        mapping = self._create_mapping(nodes_in_sort)
        hash_item_to_group_mapping = self._node_primary_key_hash_to_joined_nodes_hash()
        group_nodes_to_index_mapping = self._joined_nodes_hash_to_index_in_input_collection()
        mapping = self._fill_mapping(mapping)
        output = []
        for node in mapping.values():
            items_group_hash = hash_item_to_group_mapping[node.hash_by_pk]
            index_from_input_collection = group_nodes_to_index_mapping[items_group_hash]
            output.append(self._nodes_chain[index_from_input_collection])
        output.extend(list(map(lambda index: self._nodes_chain[index], self._select_indexes(in_sort=False))))
        return output


class BaseTimeSort(BaseSort):
    DEFAULT_CREATE_TIME_COLUMN_NAME = "_create_at"

    def __init__(self, model, *args, **kwargs):
        self._field = self.DEFAULT_CREATE_TIME_COLUMN_NAME
        self.__check_create_time_column(model)
        super().__init__(model, self.DEFAULT_CREATE_TIME_COLUMN_NAME, *args, **kwargs)

    def _create_mapping(self, nodes: "ServiceOrmContainer") -> dict:
        out = {}
        for node in nodes:
            k = ServiceOrmContainer()
            out.update({node.get_attributes()[self._field]: k})
            k.append(node_item=node)
        return out

    @classmethod
    def __check_create_time_column(cls, model):
        if not ModelTools.is_exists_column(model, cls.DEFAULT_CREATE_TIME_COLUMN_NAME):
            raise ValueError
        if not ModelTools.get_column_python_type(model, cls.DEFAULT_CREATE_TIME_COLUMN_NAME) is datetime.datetime:
            raise TypeError


class TimeSortSingleNodes(BaseTimeSort, BaseSortSingleNodes):
    """ Сортировка нод в контейнере по скрытому столбцу '_created_at'.
        Сортировка происходит в словаре, где в роли ключей выступает объект datetime. """
    def sort(self):
        self._select_nodes_to_sort()
        self._slice_other_nodes()
        mapping = self._create_mapping(self._nodes_in_sort)
        mapping = self._fill_mapping(mapping)
        result = self._merge_mapping(mapping)
        return result + self._other_items


class TimeSortNodesChain(BaseTimeSort, BaseSortJoinedNodes):
    """
    1) Выделить все участвующие в сортировке ноды в отдельный контейнер
    2) Создать словарь, где в качестве ключей выступают объекты datetime, а значениями - группы нод
    3) сортировать словарь
    4) Создать словарь вида {хеш_ноды_участвующей_в_сортировке: хеш_исходной_группы_нод}
    5) Создать словарь вида {хеш_исходной_группы_нод: индекс_в_исходном_списке}
    6) Обойти отсортированный словарь по values, обойти каждый контейнер по нодам,
    брать хеш ноды, извлекать хеш контейнера по словарю из п.4. Извлекать исходный индекс по словарю из п.5
    7) Обойти полученные в п.6 индексы, собрав элементы из исходного списка в результирующий список, вернуть.
    """
    def sort(self):
        joined_nodes = self._select_nodes(self._model.__name__, self._field, in_sort=True)
        mapping = self._create_mapping(joined_nodes)
        mapping = self._fill_mapping(mapping)
        hash_item_to_group_mapping = self._node_primary_key_hash_to_joined_nodes_hash()
        group_nodes_to_index_mapping = self._joined_nodes_hash_to_index_in_input_collection()
        output = []
        for node in mapping.values():
            nodes_group_hash = hash_item_to_group_mapping[node.hash_by_pk]
            index_in_input_collection = group_nodes_to_index_mapping[nodes_group_hash]
            output.append(self._nodes_chain[index_in_input_collection])
        output.extend([self._nodes_chain[index] for index in self._select_indexes(in_sort=False)])
        return output
