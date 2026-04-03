"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import copy
import hashlib
import weakref
from itertools import zip_longest
from typing import Union, Iterator, Iterable, Optional, Literal, Type, Any
from two_m_root.tools import ModelTools
from two_m_root.conf import CustomModel
from two_m.main import ADD_TABLE_NAME_PREFIX
from two_m_root.nodes import LinkedListItem, QueueItem, ServiceOrmItem, ResultORMItem
from two_m_root.exceptions import NodePrimaryKeyError, NodeColumnValueError, DoesNotExists


class LinkedList:
    LinkedListItem = LinkedListItem

    def __init__(self, items: Optional[Iterable[Any]] = None):
        self._head: Optional[LinkedListItem] = None
        self._tail: Optional[LinkedListItem] = None
        if items is not None:
            [self.append(**item) for item in items]

    @property
    def head(self):
        return self._head

    @property
    def tail(self):
        return self._tail

    @property
    def mutable_iterator(self):
        return self.__gen(self._head, edit_mode=True)

    def append(self, *args, node_item=None, **kwargs):
        """
        Добавить ноду в нонец
        """
        if node_item is not None:
            self._is_valid_node(node_item)
            new_element = self.LinkedListItem(**node_item.get_attributes())
        else:
            new_element = self.LinkedListItem(*args, **kwargs)
        if self:
            last_elem = self._tail
            self.__set_next(last_elem, new_element)
            self.__set_prev(new_element, last_elem)
            self._tail = new_element
        else:
            self._head = self._tail = new_element
        return new_element

    def add_to_head(self, node_item=None, **kwargs):
        """
        Добавить ноду в начало
        """
        if node_item is not None:
            self._is_valid_node(node_item)
            node = self.LinkedListItem(**node_item.get_attributes())
        else:
            node = self.LinkedListItem(**kwargs)
        if not self:
            self._head = self._tail = node
            return node
        first_elem = self._head
        if self._head == self._tail:
            node.index = 0
            first_elem.index = 1
        first_elem = self._head
        self._head = node
        node.next = first_elem
        first_elem.prev = node
        node.index = first_elem.index
        self.__reset_indexes()
        return node

    def replace(self, old_node: LinkedListItem, new_node: LinkedListItem):
        if not isinstance(old_node, self.LinkedListItem) or not isinstance(new_node, self.LinkedListItem):
            raise TypeError
        if not self:
            return
        target = self.__forward_move(old_node.index)
        prev_node = target.prev
        next_node = target.next
        if prev_node is not None:
            prev_node.next = new_node
        if next_node is not None:
            next_node.prev = new_node

    def __getitem__(self, index):
        if type(index) is slice:
            self._is_valid_slice(index)
            return self._get_slice(index)
        index = self._support_negative_index(index)
        self._is_valid_index(index)
        result = self.__forward_move(index)
        if result is None:
            raise IndexError
        return result

    def __setitem__(self, index, value):
        self._is_valid_index(index)
        index = self._support_negative_index(index)
        new_element = self.LinkedListItem(**value)
        if self:
            last_element = self.__forward_move(index)
            self.replace(last_element, new_element)
        else:
            self._head = self._tail = new_element

    def __delitem__(self, index):  # O(n)
        index = self._support_negative_index(index)
        self._is_valid_index(index)
        if index == self._tail.index:
            current_item = self._tail
            prev_item = current_item.prev() if current_item.prev is not None else None
            current_item.prev = None
            if prev_item is not None:
                prev_item.next = None
            if prev_item is None:
                self._head = None
            self._tail = prev_item
            return current_item
        if index == self._head.index:
            current_item = self._head
            next_item = current_item.next
            self._head = next_item
            if next_item is None:
                self._tail = None
            self.__reset_indexes()
            return current_item
        current_item = self.__forward_move(index)
        prev_item = current_item.prev() if current_item.prev is not None else None
        next_item = current_item.next
        next_item.prev = prev_item
        if prev_item is not None:
            prev_item.next = next_item
        else:
            self._head = self._tail = next_item
        self.__reset_indexes()
        return current_item

    def __iter__(self):
        return self.__gen(self._head, edit_mode=False)

    def __repr__(self):
        return f"{self.__class__}({tuple(self)})"

    def __str__(self):
        return str([str(x) for x in self])

    def __len__(self):
        return sum((1 for _ in self))

    def __bool__(self):
        try:
            next(self.__iter__())
        except StopIteration:
            return False
        else:
            return True

    def __contains__(self, item):
        if type(item) is not self.LinkedListItem:
            return False
        if not item:
            return False
        for node in self:
            if node == item:
                return True
        return False

    def __eq__(self, other):
        if type(other) is not self.__class__:
            return False
        return all(map(lambda x: x[0] == x[1], zip_longest(self, other)))

    def _is_valid_slice(self, slice_: slice):
        if slice_.step is not None:
            raise ValueError("Шаг не поддерживается. В этом нету необходимости.")
        if slice_.stop == float("inf") and slice_.start == 0:
            return
        if self._tail is None and self._head is None:
            return
        start = slice_.start if slice_.start is not None else 0
        stop = slice_.stop if slice_.stop is not None else self._tail.index
        if type(start) is not int:
            raise TypeError
        if not isinstance(stop, int):
            raise TypeError
        if start < 0:
            start = self._support_negative_index(start)
        if stop < 0:
            stop = self._support_negative_index(stop)
        if stop < start:
            raise IndexError
        if start < self._head.index:
            raise IndexError
        if stop < self._head.index:
            raise IndexError
        if stop > self._tail.index + 1:
            raise IndexError

    def _get_slice(self, item: slice) -> "LinkedList":
        self._is_valid_slice(item)
        if self._tail is None and self._head is None:
            return self.__class__()
        left = self._support_negative_index(item.start if item.start is not None else 0)
        right = self._support_negative_index(item.stop if item.stop is not None and not item.stop == float("inf")
                                             else self._tail.index + 1)
        elem = self.__forward_move(left)
        counter = left
        instance = self.__class__()
        while counter < right:
            instance.append(**elem.get_attributes())
            counter += 1
            elem = elem.next
        return instance

    def _support_negative_index(self, index: int):
        if index < 0:
            if self._tail is None:
                return index
            index = self._tail.index + 1 + index
        return index

    def _replace_inner(self, new_head: LinkedListItem, new_tail: LinkedListItem):
        """
        Заменить значения инкапсулированных атрибутов head и tail на новые
        """
        if type(new_head) is not self.LinkedListItem or type(new_tail) is not self.LinkedListItem:
            raise TypeError
        self._head = new_head
        self._tail = new_tail

    def _is_valid_index(self, index):
        if not isinstance(index, int):
            raise TypeError
        if self._tail is None:
            return
        if index not in range(self._tail.index + 1):
            raise IndexError

    def _is_valid_node(self, node):
        if not isinstance(node, self.LinkedListItem):
            raise TypeError

    @staticmethod
    def __set_next(left_item: Union[LinkedListItem, weakref.ref], right_item: Union[LinkedListItem, weakref.ref]):
        left_item = left_item() if hasattr(left_item, "__call__") else left_item  # Check item is WeakRef
        right_item = right_item() if hasattr(right_item, "__call__") else right_item  # Check item is WeakRef
        right_item.index = left_item.index + 1
        left_item.next = right_item

    @staticmethod
    def __set_prev(right_item: Union[LinkedListItem, weakref.ref], left_item: Union[LinkedListItem, weakref.ref]):
        left_item = left_item() if hasattr(left_item, "__call__") else left_item  # Check item is WeakRef
        right_item = right_item() if hasattr(right_item, "__call__") else right_item  # Check item is WeakRef
        right_item.prev = left_item

    def __forward_move(self, index=0):
        element = self._head
        if element is None:
            return
        if element.index == index:
            return element
        for _ in range(self._support_negative_index(index) + 1):
            next_element = element.next
            if next_element is None:
                raise IndexError
            element = next_element
            if element.index == index:
                return element

    def __reset_indexes(self):
        counter = 0
        node = self._head
        while node is not None:
            node.index = counter
            counter += 1
            node = node.next

    @staticmethod
    def __gen(start_item: Optional[LinkedListItem] = None, edit_mode=False) -> Iterator:
        """ :arg edit_mode: если в процессе обхода итератора есть потребность изменения связанного списка
        (добавление нового элемента),
        то данный режим следует включить """
        current_item = copy.deepcopy(start_item) if edit_mode else start_item
        while current_item is not None:
            yield current_item
            current_item = current_item.next


class SuperQueue(LinkedList):
    """
    Переиздание класса Queue, попытка расплатиться O(n) памяти за n**2 big O.
    Данный класс делает поиск ноды в очереди(по первичному ключу и значению) за O(1), вместо O(n).
    Добавление ноды в конец за O(1) - вместо O(1)
    Добавление нод по setitem O(n) - вместо O(n)
    Замена ноды за O(1) - вместо O(1)
    Удаление ноды за O(n) - вместо O(n)
    get_node ноды за O(1) - вместо O(n)
    Жертвуем O(n) памяти."""
    LinkedListItem = ...

    def __init__(self, *args, **kwargs):
        self.__items_hash_map = {}  # primary_key_val_hash: node_item
        self.__node_index_hash_map = {}  # node_index: primary_key_val_hash
        super().__init__(*args, **kwargs)
        [self.__add_node(node) for node in self]

    def get_node(self, model, **primary_key_data):  # O(1)
        if not len(primary_key_data) == 1:
            raise ValueError
        try:
            hash_val = self.__get_hash_value(model.__name__, *tuple(primary_key_data.items())[0])
            return self.__items_hash_map[hash_val]
        except KeyError:
            return

    def append(self, *args, node_item=None, **kwargs):  # O(1)
        if node_item is not None:
            self.__is_valid_node(node_item)  # O(1)
            new_item = self.LinkedListItem(**node_item.get_attributes())
        else:
            new_item = self.LinkedListItem(*args, **kwargs)
        new_item = super().append(node_item=new_item)  # O(1)
        self.__add_node(new_item)  # O(1)

    def add_to_head(self, node_item=None, **kwargs):  # O(n)
        if node_item is not None:
            self.__is_valid_node(node_item)
            new_item = self.LinkedListItem(**node_item.get_attributes())
        else:
            new_item = self.LinkedListItem(**kwargs)  # O(1)
        super().add_to_head(node_item=node_item)  # O(n)
        self.__reset_mappings_and_indexes()  # O(n)
        self.__add_node(new_item)  # O(1)

    def replace(self, old_node, new_node):  # O(1)
        self.__is_valid_node(old_node)  # O(1)
        self.__is_valid_node(new_node)  # O(1)
        new_node.index = old_node.index  # O(1)
        super().replace(old_node, new_node)  # O(1)
        hash_val = self.__get_hash_value(old_node.model.__name__,
                                         *old_node.get_primary_key_and_value(as_tuple=True))  # O(k)
        self.__items_hash_map[hash_val] = new_node  # O(1)
        self.__node_index_hash_map[new_node.index] = hash_val  # O(1)

    def __getitem__(self, node_index):  # O(1)
        if not isinstance(node_index, (int, slice,)):
            raise TypeError
        if type(node_index) is slice:
            return super().__getitem__(node_index)
        node_index = self._support_negative_index(node_index)  # O(1)
        try:
            hash_val = self.__node_index_hash_map[node_index]
            node = self.__items_hash_map[hash_val]
        except KeyError:
            raise IndexError
        else:
            return node

    def __setitem__(self, key, value):  # O(n)
        new_node = self.LinkedListItem(**value)  # O(1)
        self.__is_valid_node(new_node)  # O(1)
        super().__setitem__(key, new_node)  # O(1)
        new_hash_value = self.__get_hash_value(new_node.model.__name__,
                                               *new_node.get_primary_key_and_value(as_tuple=True))  # O(k)
        self.__node_index_hash_map[new_node.index] = new_hash_value  # O(1)
        self.__items_hash_map[new_hash_value] = new_node  # O(1)
        self.__reset_mappings_and_indexes()  # O(n)

    def __delitem__(self, node_index):  # O(n)
        self._is_valid_index(node_index)  # O(1)
        node_index = self._support_negative_index(node_index)  # O(1)
        node_hash = self.__node_index_hash_map[node_index]  # O(1)
        node = self.__items_hash_map[node_hash]  # O(1)
        del self.__items_hash_map[node_hash]  # O(1)
        del self.__node_index_hash_map[node_index]  # O(1)
        if node_index == 0:
            next_node = node.next
            if next_node is not None:
                self._head = next_node
                next_node.prev = None
            else:
                self._head = None
            node.next = None
            self.__reset_mappings_and_indexes()  # O(n)
            return
        if node_index == self._tail.index:
            prev_node = node.prev
            prev_node.next = None
            self._tail = prev_node
            node.prev = None
            self.__reset_mappings_and_indexes()
            return
        next_node = node.next
        prev_node = node.prev
        prev_node.next = next_node
        next_node.prev = prev_node
        self.__reset_mappings_and_indexes()  # O(n)

    def __contains__(self, item: LinkedListItem):  # O(1)
        try:
            self.__is_valid_node(item)
        except AttributeError:
            return False
        except ValueError:
            return False
        except TypeError:
            return False
        return self.__get_hash_value(item.model.__name__, *item.get_primary_key_and_value(as_tuple=True)) in self.__items_hash_map

    def __and__(self, other):
        if not isinstance(other, self.__class__):
            return self.__class__()
        result = type(self)()
        for node in other:
            try:
                self.__is_valid_node(node)
            except AttributeError:
                return self.__class__()
            except ValueError:
                return self.__class__()
            except TypeError:
                return self.__class__()
            value = self.__get_hash_value(node.model.__name__, *node.get_primary_key_and_value(as_tuple=True))
            exist_node = self.__items_hash_map.get(value, None)
            if exist_node is not None:
                result.append(node_item=exist_node)
        return result

    def __add_node(self, new_item):  # O(1)
        """ Ноде уже должен быть присвоен индекс(порядковый номер) в связанном списке, до момента вызова этого метода. """
        self.__is_valid_node(new_item)
        hash_value = self.__get_hash_value(new_item.model.__name__, *new_item.get_primary_key_and_value(as_tuple=True))
        self.__items_hash_map[hash_value] = new_item
        self.__node_index_hash_map[new_item.index] = hash_value

    def __reset_mappings_and_indexes(self):   # O(3n)
        for i, node in enumerate(self):
            node.index = i
        self.__node_index_hash_map = dict(zip(range(len(self)),
                                              map(lambda n: self.__get_hash_value(n.model.__name__,
                                                                                  *n.get_primary_key_and_value(as_tuple=True)), self)))
        self.__items_hash_map = dict(zip(self.__node_index_hash_map.values(), self))

    @staticmethod
    def __get_hash_value(*args):
        return int.from_bytes(hashlib.md5("".join(map(str, args)).encode("utf-8")).digest(), "big")

    @classmethod
    def __is_valid_node(cls, node):
        if not hasattr(node, "get_primary_key_and_value"):
            raise AttributeError
        if not callable(getattr(node, "get_primary_key_and_value")):
            raise TypeError
        if not isinstance(node.get_primary_key_and_value(), dict):
            raise ValueError
        if not len(node.get_primary_key_and_value()) == 1:
            raise ValueError
        if type(node.get_primary_key_and_value(only_key=True)) is not str:
            raise TypeError
        if not isinstance(node.get_primary_key_and_value(only_value=True), (int, str)):
            raise TypeError
        if not hasattr(node, "model"):
            raise AttributeError
        ModelTools.is_valid_model_instance(node.model)


class Queue(LinkedList):
    """
    Очередь на основе связанного списка.
    Управляется через адаптер Tool.
    Класс-контейнер умеет только ставить в очередь ((enqueue) зашита особая логика) и снимать с очереди (dequeue)
    см логику в методе _replication.
    """
    LinkedListItem = QueueItem

    def __init__(self, items: Optional[Iterable[dict]] = None):
        super().__init__(items)
        if items is not None:
            for inner in items:
                self.enqueue(**inner)

    def enqueue(self, _remove_fk=True, **attrs):  # O(n * log(n))
        """ Установка ноды в конец очереди с хитрой логикой проверки на совпадение. """
        exists_item, new_item = self._replication(**attrs)  # O(1)
        self._remove_from_queue(exists_item) if exists_item is not None else None
        if _remove_fk:
            self._remove_other_node_with_current_foreign_key_value(new_item)  # O(n * log(n))
        self._check_primary_key_unique(new_item)  # O(n)
        self._check_unique_values(new_item, check_fk_values=_remove_fk)  # O(k)
        self.append(**new_item.get_attributes())  # O(1)

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

    def get_related_nodes(self, main_node: QueueItem, other_container=None) -> "Queue":
        """ Получить все связанные (внешним ключом) с передаваемой нодой ноды. """
        root = self
        if other_container is not None:
            if type(other_container) is not self.__class__:
                raise TypeError
            root = other_container
        container = self.__class__()
        for related_node in root:  # O(n) * O(j) * O(m) * O(n) * O(1) = O(n)
            if related_node == main_node:  # O(g) * O(j) = O (j)
                continue
            pk_field, pk_value = related_node.get_primary_key_and_value(as_tuple=True)
            for fk_column in ModelTools.get_foreign_key_columns(main_node.model):  # O(i)
                if pk_field == fk_column:
                    if fk_column not in main_node.value:
                        continue
                    if pk_value == main_node.value[fk_column]:
                        container.append(**related_node.get_attributes())  # O(1)
        return container

    def search_nodes(self, model: Type[CustomModel], negative_selection=False, or_mode=True,
                     **_filter: dict[str, Union[str, int, Literal["*"]]]) -> "Queue":  # O(n)
        """
        Искать ноды по совпадениям любых полей.
        :param model: кастомный объект, смотри модуль database/models
        :param _filter: словарь содержащий набор полей и их значений для поиска, вместо значений допустим знак '*',
        который будет засчитывать любые значения у полей.
        :param negative_selection: режим отбора нод (найти ноды КРОМЕ ... [filter])
        :param or_mode: Режим, когда работает правило ИЛИ при передаче нескольких стольцов и значений
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
                    items.append(**left_node.get_attributes())
                bool_value = True
                for field_name, value in _filter.items():
                    if field_name in left_node.value:
                        if negative_selection:
                            if value == "*":
                                if field_name not in left_node.value:
                                    items.append(**left_node.get_attributes())
                                continue
                            if not left_node.value[field_name] == value:
                                if not or_mode:
                                    if bool_value:
                                        items.enqueue(**left_node.get_attributes())
                                    else:
                                        items.remove(model, *left_node.get_primary_key_and_value(as_tuple=True))
                                        break
                                else:
                                    items.append(**left_node.get_attributes())
                                    break
                            else:
                                bool_value = False
                        else:
                            if value == "*":
                                if field_name in left_node.value:
                                    items.append(**left_node.get_attributes())
                                    continue
                            if left_node.value[field_name] == value:
                                if not or_mode:
                                    if bool_value:
                                        items.enqueue(**left_node.get_attributes())
                                    else:
                                        items.remove(model, *left_node.get_primary_key_and_value(as_tuple=True))
                                        break
                                else:
                                    items.append(**left_node.get_attributes())
                                    break
                            else:
                                bool_value = False
        return items

    def get_node(self, model: CustomModel, **primary_key_data) -> Optional[QueueItem]:
        """
        Данный метод используется при инициализации - _replication
        :param model: объект модели
        :param primary_key_data: словарь вида - {имя_первичного_ключа: значение}
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

    def __add__(self, other):  # O(n * k) = O(n)
        if type(other) is not self.__class__:
            raise TypeError
        result_instance = self.__class__()
        [result_instance.append(**n.get_attributes()) for n in self]  # O(1) * O(n)
        [result_instance.enqueue(**n.get_attributes()) for n in other]  # O(n * log(n)) * O(k)
        return result_instance

    def __iadd__(self, other):  # O(n)
        if not isinstance(other, type(self)):
            raise TypeError
        result = self + other
        self._head = result.head
        self._tail = result.tail
        return result

    def __sub__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError
        result_instance = copy.deepcopy(self)
        [result_instance.remove(n.model, *n.get_primary_key_and_value(as_tuple=True)) for n in other]
        return result_instance

    def __isub__(self, other):
        result = self - other
        self._head = result._head
        self._tail = result._tail

    def __and__(self, other: "Queue"):
        if type(other) is not self.__class__:
            raise TypeError
        output = self.__class__()
        for right_node in other:  # O(k)
            left_node = self.get_node(right_node.model, **right_node.get_primary_key_and_value())  # O(1)
            if left_node is not None:
                data = left_node.get_attributes()
                data.update(right_node.get_attributes())
                output.append(**data)
            else:
                output.append(**right_node.get_attributes())
        return output

    def __eq__(self, other):
        if type(other) is not type(self):
            return False
        return [node.get_primary_key_and_value() for node in self] == [node.get_primary_key_and_value() for node in other]

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
        potential_new_item = self.LinkedListItem(**new_node_complete_data)  # O(1)
        new_item = None

        def merge(old_node: QueueItem, new_node: QueueItem, dml_type: str) -> QueueItem:
            new_node_data = old_node.get_attributes()
            old_value, new_value = old_node.value, new_node.value
            old_value.update(new_value)
            new_node_data.update(old_value)
            new_node_data.update({"_insert": False, "_update": False, "_delete": False})
            new_node_data.update({dml_type: True, "_ready": new_node.ready})
            new_node_data.update({"_create_at": new_node.created_at})
            return self.LinkedListItem(**new_node_data)
        exists_item = self.get_node(potential_new_item.model, **potential_new_item.get_primary_key_and_value())
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
                new_item = potential_new_item
        if new_item_is_delete:
            new_item = potential_new_item
        if new_item_is_insert:
            if exists_item.type == "_insert" or exists_item.type == "_update":
                new_item = merge(exists_item, potential_new_item, "_insert")
            if exists_item.type == "_delete":
                new_item = potential_new_item
        return exists_item, new_item

    def _remove_from_queue(self, left_node: QueueItem) -> None:
        if type(left_node) is not self.LinkedListItem:
            raise TypeError
        del self[left_node.index]

    def _check_unique_values(self, node, check_fk_values=True):
        """
        Валидация на предмет нарушения уникальности в полях разных нод
        :param check_fk_values: Столбцы отношений тоже являются unique constraint, но не всегда требудется учитывать их
        :param node: Инициализированная для добавления нода
        :return: None
        """
        if type(node) is not self.LinkedListItem:
            raise TypeError
        if not self:
            return
        foreign_key_columns = ModelTools.get_foreign_key_columns(node.model)
        unique_fields = ModelTools.get_unique_columns(node.model)
        for unique_field in unique_fields:
            if unique_field not in node.value:
                continue
            value = node.value[unique_field]
            for n in self:
                if n.model.__name__ == node.model.__name__:
                    if unique_field in n.value:
                        if n.value[unique_field] == value:
                            if not check_fk_values:
                                if unique_field in foreign_key_columns:
                                    continue
                            raise NodeColumnValueError

    def _check_primary_key_unique(self, node):
        if not isinstance(node, self.LinkedListItem):
            raise TypeError
        if not self:
            return
        all_primary_keys = [n.get_primary_key_and_value(only_value=True) for n in self
                            if node.model.__name__ == n.model.__name__]
        if all_primary_keys.count(node.get_primary_key_and_value(only_value=True)) > 1:
            raise NodePrimaryKeyError("Нарушение уникальности первичных ключей в очереди. Первичный ключ повторяется.")

    def _remove_other_node_with_current_foreign_key_value(self, node):
        """ Если в очереди есть нода, той же модели, что и добавляемая,
         если у старой и добавляемой ноды заполнен один и тот же столбец FK,
         то из найденной в очереди ноды это значение нужно убрать
         Простыми словами: 2 ноды не могут ссылаться своими внешними ключами на какую-то одну ноду
         """
        for ex_node in self:
            if not node.model.__name__ == ex_node.model.__name__:
                continue
            all_foreign_key_columns = frozenset(ModelTools.get_foreign_key_columns(node.model))
            current_fk_values = frozenset(node.value) & all_foreign_key_columns & frozenset(ex_node.value)
            for fk_name in current_fk_values:
                if node.value[fk_name] == ex_node.value[fk_name]:
                    new_values = ex_node.get_attributes()
                    del new_values[fk_name]
                    self._remove_from_queue(ex_node)
                    self.append(**new_values)
                    break


class Queue(SuperQueue, Queue):
    LinkedListItem = QueueItem


class ServiceOrmContainer(Queue):
    """ Контейнер с композицией результата """
    LinkedListItem = ServiceOrmItem

    @property
    def hash_by_pk(self):
        return sum(map(lambda x: x.hash_by_pk, self))

    def __getitem__(self, model_name_or_index: Union[str, int]) -> Union[DoesNotExists, "ServiceOrmItem", "ResultORMItem"]:
        if not isinstance(model_name_or_index, (str, int,)):
            raise TypeError
        if type(model_name_or_index) is int:
            return super().__getitem__(model_name_or_index)
        nodes = self.__class__()
        for node in self:
            if node.model.__name__ == model_name_or_index:
                nodes.append(**node.get_attributes())
        if len(nodes) > 1:
            return nodes
        if nodes:
            return nodes[0]
        raise DoesNotExists

    def __delitem__(self, key):
        node = self[key]
        return super().__delitem__(node.index)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return str(self.hash_by_pk) == str(other.hash_by_pk)

    def __hash__(self):
        return sum(map(hash, self))


class ServiceResultOrmContainer(ServiceOrmContainer):
    """ Контейнер для хранения выводимых результатов """
    LinkedListItem = ResultORMItem

    @property
    def has_hidden_nodes(self):
        return any(map(lambda node: node.hidden, self))


class ResultORMCollection:
    """ Иммутабельная коллекция с набором результата, закрытая на добавление новых элементов. """
    ADD_TABLE_NAME_PREFIX: Literal["auto", "add", "no-prefix"] = ADD_TABLE_NAME_PREFIX
    CONTAINER = ServiceResultOrmContainer  # Тип, хранимый внутри, имутабелен

    def __init__(self, collection: "ServiceOrmContainer", prefix_mode=None, show_hidden_nodes=None):
        def is_valid(items):
            if type(items) is not ServiceOrmContainer:
                raise TypeError
            if type(self._prefix_mode) is not str:
                raise TypeError
            if self._prefix_mode not in ("auto", "add", "no-prefix",):
                raise ValueError
            if not items:
                return
            if type(items[0]) is not ServiceOrmItem:
                raise TypeError
            if not isinstance(self._show_hidden, (bool, type(None))):
                raise TypeError
        self._prefix_mode = prefix_mode if prefix_mode is not None else self.ADD_TABLE_NAME_PREFIX
        self._show_hidden = show_hidden_nodes
        is_valid(collection)
        self.__collection = self.__convert_node_data(collection)
        self.__collection = self.__filter_nodes(self.__collection)
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
    def hash_by_pk(self):
        return sum(map(lambda x: x.hash_by_pk, self.__collection))

    @property
    def container_cls(self):
        return self.__collection.__class__

    @property
    def has_hidden_nodes(self):
        return self.__collection.has_hidden_nodes

    def add_model_name_prefix(self):
        """ Изменит всю коллекцию, добавив префиксы названия таблицы к каждому значению полей у каждой ноды """
        self._prefix_mode = "add"
        new_collection = self.CONTAINER()
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
        new_collection = self.CONTAINER()
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
                names_to_set_prefix.remove("_ui_hidden")
                if not names_to_set_prefix:
                    continue
                node.add_model_name_prefix(tuple(names_to_set_prefix))

    def get_node(self, model, primary_key, value):
        return self.__collection.get_node(model, **{primary_key: value})

    def search_nones(self, model, **kwargs):
        return self.__collection.search_nodes(model, **kwargs)

    def __iter__(self):
        return iter(self.__collection)

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

    def __contains__(self, item: ResultORMItem):
        if type(item) is ResultORMItem:
            if str(item.__hash__()) in map(str, map(hash, self)):
                return True
        return False

    def __hash__(self):
        return hash(self.__collection)

    def __str__(self):
        return str(tuple([s.__str__() for s in self]))

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    @classmethod
    def __convert_node_data(cls, collection: ServiceOrmContainer):
        new_collection = cls.CONTAINER()
        [new_collection.append(node.model, node.get_primary_key_and_value(),
                               **({"_ui_hidden": True
                                  if node.type == "_delete" else False}),
                               **node.value)
         for node in collection]
        return new_collection

    def __filter_nodes(self, collection: ServiceResultOrmContainer) -> ServiceResultOrmContainer:
        if type(collection) is not self.CONTAINER:
            raise TypeError
        if self._show_hidden is None:
            return collection
        new_items = self.CONTAINER()
        if not self._show_hidden:
            [new_items.append(**node.get_attributes())
             for node in self
             if not node.hidden]
        if self._show_hidden:
            [new_items.append(**node.get_attributes())
             for node in self
             if node.hidden]
        return new_items
