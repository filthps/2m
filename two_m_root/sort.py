"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import string
import datetime
import operator
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
        super().__init__(model, column_name, nodes, *args, **kwargs)

    def _select_nodes_to_sort(self):
        """ Вернуть ноды, которые будут участвовать в сортировке """
        self._nodes_in_sort = ServiceOrmContainer()
        for node in self._input_nodes:
            if self._field in node.value:
                self._nodes_in_sort.append(**node.get_attributes())

    def _slice_other_nodes(self):
        """ Вернуть ноды НЕ участвующие в сортировке """
        self._other_items = ServiceOrmContainer()
        for node in self._input_nodes:
            if self._field not in node.value:
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
    def __init__(self, model, column_name, items_group, **k):
        self._is_valid_nodes_chain(items_group)
        self._nodes_chain = items_group
        self._field = column_name
        super().__init__(model, column_name, items_group, **k)

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

    def _select_nodes(self, table_name, in_sort=True):
        u = ServiceOrmContainer()
        for index in self._select_indexes(in_sort=in_sort):
            for node in self._nodes_chain[index]:
                if node.model.__name__ == table_name:
                    u.append(node_item=node)
        return u

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


class BaseTimeSort(BaseSort):
    DEFAULT_CREATE_TIME_COLUMN_NAME = "_create_at"

    def __init__(self, model, *args, **kwargs):
        super().__init__(model, *args, **kwargs)
        self._field = self.DEFAULT_CREATE_TIME_COLUMN_NAME
        self.__check_create_time_column(model)

    @classmethod
    def __check_create_time_column(cls, model):
        ModelTools.is_exists_column(model, cls.DEFAULT_CREATE_TIME_COLUMN_NAME)
        if not ModelTools.get_column_python_type(model, cls.DEFAULT_CREATE_TIME_COLUMN_NAME) is datetime:
            raise TypeError


class LetterSortSingleNodes(BaseSortSingleNodes, BaseLetterSort):
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


class LetterSortNodesChain(BaseSortJoinedNodes, BaseLetterSort):
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


class NumberSortSingleNodes(BaseSortSingleNodes, BaseNumberSort):
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


class NumberSortNodesChain(BaseSortJoinedNodes, BaseNumberSort):
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


class TimeSortSingleNodes(BaseSortSingleNodes, BaseTimeSort):
    def sort(self):
        self._select_nodes_to_sort()
        self._slice_other_nodes()
        mapping = self._create_mapping(self._nodes_in_sort)
        mapping = self._fill_mapping(mapping)
        result = self._merge_mapping(mapping)
        return result + self._other_items


class TimeSortNodesChain(BaseSortJoinedNodes):
    def __sort_nodes_by_create_time(self):
        def nodes_map():
            for nodes_group in self:
                yield min(map(lambda node: node.created_at, nodes_group)), nodes_group
        return list(dict(sorted(nodes_map(), key=operator.itemgetter(0))).values())
