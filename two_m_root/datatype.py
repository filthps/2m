import weakref
import copy
from weakref import ref
from typing import Optional, Iterable, Union, Any, Iterator


class LinkedListItem:
    def __init__(self, **values):
        self._val = values
        self._index = 0
        self.__next = None
        self.__prev = None

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
        self._is_valid_item(other)
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

    def append(self, *args, node_item=None, **kwargs):
        """
        Добавить ноду в нонец
        """
        new_element = self.LinkedListItem(*args, **kwargs) if node_item is None else node_item
        if self:
            last_elem = self._tail
            self.__set_next(last_elem, new_element)
            self.__set_prev(new_element, last_elem)
            self._tail = new_element
        else:
            self._head = self._tail = new_element

    def add_to_head(self, **kwargs):
        """
        Добавить ноду в начало
        """
        node = self.LinkedListItem(**kwargs)
        if not self:
            self._head = self._tail = node
            return
        first_elem = self._head
        if self._head == self._tail:
            node.index = 0
            first_elem.index = 1
        first_elem = self._head
        self._head = node
        node.next = first_elem
        first_elem.prev = node
        node.index = first_elem.index
        self.__incr_indexes(first_elem)

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
        index = self._support_negative_index(index)
        self._is_valid_index(index)
        result = self.__forward_move(index)
        if result is None:
            raise IndexError
        return result

    def _support_negative_index(self, index: int):
        if index < 0:
            if self._tail is None:
                return index
            index = self._tail.index + 1 + index
        return index

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
            self.__decr_indexes(next_item)
            return current_item
        current_item = self.__forward_move(index)
        prev_item = current_item.prev() if current_item.prev is not None else None
        next_item = current_item.next
        next_item.prev = prev_item
        if prev_item is not None:
            prev_item.next = next_item
        else:
            self._head = self._tail = next_item
        self.__decr_indexes(next_item)
        return current_item

    def __iter__(self):
        return self.__gen(self._head)

    def __repr__(self):
        return f"{self.__class__}({tuple(self)})"

    def __str__(self):
        return str([str(x) for x in self])

    def _replace_inner(self, new_head: LinkedListItem, new_tail: LinkedListItem):
        """
        Заменить значения инкапсулированных атрибутов head и tail на новые
        """
        if type(new_head) is not self.LinkedListItem or type(new_tail) is not self.LinkedListItem:
            raise TypeError
        self._head = new_head
        self._tail = new_tail

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

    def _is_valid_index(self, index):
        if not isinstance(index, int):
            raise TypeError
        if self._tail is None:
            return
        if index not in range(self._tail.index + 1):
            raise IndexError

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

    def __forward_move(self, index=-1):
        element = self._head
        for _ in range(self._support_negative_index(index)):
            next_element = element.next
            if next_element is None:
                raise IndexError
            element = next_element
        return element

    @staticmethod
    def __incr_indexes(node):
        while node is not None:
            node.index += 1
            node = node.next

    @staticmethod
    def __decr_indexes(node):
        while node is not None:
            node.index -= 1
            node = node.next

    @staticmethod
    def __gen(start_item: Optional[LinkedListItem] = None) -> Iterator:
        current_item = start_item
        while current_item is not None:
            yield current_item
            current_item = current_item.next
