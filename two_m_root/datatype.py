""" Copyright (C) 2025 Литовченко Виктор Иванович (filthps) """
import weakref
import copy
from weakref import ref
from abc import ABC, abstractmethod
from itertools import zip_longest
from typing import Optional, Iterable, Union, Any, Iterator


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
        return self.__prev

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
        if self._head.index == self._tail.index:
            self._head = self._tail = new_node
            return
        next_node = old_node.next
        previous_node = old_node.prev
        if old_node.index == self._tail.index:
            self._tail = new_node
        if old_node.index == 0:
            self._head = new_node
        previous_node.next = new_node
        next_node.prev = new_node
        return new_node

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
        if stop > self._tail.index:
            raise IndexError

    def _get_slice(self, item: slice) -> "LinkedList":
        self._is_valid_slice(item)
        if self._tail is None and self._head is None:
            return self.__class__()
        left = self._support_negative_index(item.start if item.start is not None else 0)
        right = self._support_negative_index(item.stop if item.stop is not None and not item.stop == float("inf")
                                             else self._tail.index)
        if item.stop is not None:
            if not item.stop == 0:
                right += 1
        if item.stop is None:
            right += 1
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
