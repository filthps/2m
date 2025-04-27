import weakref
import copy
from weakref import ref
from typing import Optional, Iterable, Union, Any, Iterator
from itertools import islice


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
        self._is_valid_item(item)
        if not item:
            return
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

    def __init__(self, items: Optional[Iterable[Any]] = None,
                 nodes: Optional[Union[Iterable[LinkedListItem], Iterator[LinkedListItem]]] = None):
        if items is not None and nodes is not None:
            raise ValueError
        self._head: Optional[LinkedListItem] = None
        self._tail: Optional[LinkedListItem] = None
        if items is not None:
            [self.append(**item) for item in items]
        if nodes is not None:
            nodes = list(nodes)
            self._head = self._tail = nodes.pop(0)
            for node in nodes:
                if type(node) is not self.LinkedListItem:
                    raise TypeError
                tail = self._tail
                self.__set_next(tail, node)
                self.__set_prev(node, tail)

    @property
    def head(self):
        return self._head

    @property
    def tail(self):
        return self._tail

    def append(self, *args, **kwargs):
        """
        Добавить ноду в нонец
        """
        new_element = self.LinkedListItem(*args, **kwargs)
        if len(self) == 1:
            last_elem = self._tail
            last_elem.next = new_element
            new_element.prev = last_elem
            self._tail = new_element
            return
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
        if not len(self):
            return
        if len(self) == 1:
            self._head = self._tail = new_node
            return
        next_node = old_node.next
        previous_node = old_node.prev
        if old_node.index == len(self) - 1:
            self._tail = new_node
        if old_node.index == 0:
            self._head = new_node
        previous_node.next = new_node
        next_node.prev = new_node
        return new_node

    def __getitem__(self, index):
        if type(index) is slice:
            return self.__slice(index)
        index = self.__support_negative_index(index)
        self._is_valid_index(index)
        result = self.__forward_move(index)
        if result is None:
            raise IndexError
        return result

    def __support_negative_index(self, index: int):
        if index < 0:
            index = len(self) + index
        return index

    def __setitem__(self, index, value):
        self._is_valid_index(index)
        index = self.__support_negative_index(index)
        new_element = self.LinkedListItem(**value)
        if self:
            last_element = self.__forward_move(index)
            self.replace(last_element, new_element)
        else:
            self._head = self._tail = new_element

    def __delitem__(self, index):  # O(n)
        index = self.__support_negative_index(index)
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

    def __reversed__(self):
        return self.__class__(list(self.__backward_move_gen()))

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
        for _ in range(self.__support_negative_index(index)):
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

    def __backward_move_gen(self):
        element = self.tail
        while element is not None:
            yield element
            element = element.prev

    def __slice(self, slice_item: slice):
        if slice_item.start < 0:
            slice_item.start = len(self) - slice_item.start
        if slice_item.stop < 0:
            slice_item.stop = len(self) - slice_item.stop
        if slice_item.start >= self.__len__():
            raise IndexError
        if len(self) >= slice_item.stop:
            raise IndexError
        if slice_item.step == 0 or slice_item.step >= self.__len__():
            raise IndexError
        if slice_item.step < 0:
            return self.__class__(islice(reversed(self), slice_item.start, slice_item.stop, slice_item.step))
        return self.__class__(islice(tuple(self), slice_item.start, slice_item.stop, slice_item.step))
