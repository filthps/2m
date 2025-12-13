import os
import unittest
import time
from typing import Optional
from sqlalchemy import text, select
from dotenv import load_dotenv
from sqlalchemy.orm.scoping import ScopedSession
from procedures import init_all_triggers
from models import *
from two_m_root.orm import *
from two_m_root.exceptions import *

load_dotenv(os.path.join(os.path.dirname(__file__), "settings.env"))
CACHE_PATH = os.environ.get("CACHE_PATH")
DATABASE_PATH = os.environ.get("DATABASE_PATH")


def is_database_empty(session, empty=True, tables=15, procedures=52, test_db_name="testdb"):
    table_counter = session.execute(text('SELECT COUNT(table_name) '
                                         'FROM information_schema."tables" '
                                         'WHERE table_type=\'BASE TABLE\' AND table_schema=\'public\';')).scalar()
    procedures_counter = session.execute(text(f'SELECT COUNT(*) '
                                              f'FROM information_schema."triggers" '
                                              f'WHERE trigger_schema=\'public\' AND '
                                              f'trigger_catalog=\'{test_db_name}\' AND '
                                              f'event_object_catalog=\'{test_db_name}\';')).scalar()
    print(f"procedures_counter {procedures_counter}")
    print(f"table_counter {table_counter}")
    if empty:
        if table_counter or procedures_counter:
            return is_database_empty(session, empty=empty, tables=tables, procedures=procedures,
                                     test_db_name=test_db_name)
        return True
    if table_counter < tables or procedures_counter < procedures:
        return is_database_empty(session, empty=empty, tables=tables, procedures=procedures,
                                 test_db_name=test_db_name)
    return True


def db_reinit(m):
    def wrap(self: "TestToolHelper"):
        drop_db()
        if is_database_empty(self.orm_manager.connection.database):
            create_db()
            init_all_triggers()
            if is_database_empty(self.orm_manager.connection.database, empty=False):
                return m(self)
    return wrap


def drop_cache(callable_):
    def w(self: "TestToolHelper"):
        self.orm_manager.connection.drop_cache()
        return callable_(self)
    return w


class SetUp:
    orm_manager: Optional[Tool] = None

    def set_data_into_database(self):
        """
        1) Cnc NC210 id1 - Machine Heller machineid1

        :return:
        """
        self.orm_manager.connection.database.add(Cnc(name="NC210", commentsymbol=","))
        self.orm_manager.connection.database.add(Cnc(name="NC211", commentsymbol=","))
        self.orm_manager.connection.database.add(Cnc(name="NC212", commentsymbol=","))
        self.orm_manager.connection.database.add(Cnc(name="NC213", commentsymbol=","))
        self.orm_manager.connection.database.add(Cnc(name="NC214", commentsymbol=","))
        self.orm_manager.connection.database.add(Cnc(name="NC215", commentsymbol=","))
        self.orm_manager.connection.database.add(Cnc(name="NC216", commentsymbol=","))
        self.orm_manager.connection.database.add(Numeration())
        self.orm_manager.connection.database.add(Comment(findstr="test_str", iffullmatch=True))
        self.orm_manager.connection.database.commit()
        self.orm_manager.connection.database.add(Machine(machinename="Heller",
                                                 cncid=self.orm_manager.connection.database.scalar(
                                                    select(Cnc).where(Cnc.name == "NC210")
                                                 ).cncid,
                                                 inputcatalog=r"C:\Windows",
                                                 outputcatalog=r"X:\path"))
        self.orm_manager.connection.database.add(OperationDelegation(
            numerationid=1,
            operationdescription="Нумерация. Добавил сразу в БД"
        ))
        self.orm_manager.connection.database.add(OperationDelegation(commentid=self.orm_manager.connection.database.scalar(select(Comment)).commentid))
        self.orm_manager.connection.database.add(Machine(machinename="Fidia_db", inputcatalog=r"C:\Wfghfg", outputcatalog=r"X:\pthnt", cncid=2))
        self.orm_manager.connection.database.add(Machine(machinename="Rambauidu_db", inputcatalog=r"C:\Windows", outputcatalog=r"X:\pah", cncid=3))
        self.orm_manager.connection.database.add(Machine(machinename="Her", inputcatalog=r"C:\ows", outputcatalog=r"X:\pa", cncid=4))
        self.orm_manager.connection.database.commit()

    def set_data_into_queue(self):
        """
        1) Cnc id1 Newcnc Tesm id1
        2) Cnc id2 Ram Machine id2 Fidia
        3) Machine 65a90 id3
        4) Machine Rambaudi id4
        """
        self.orm_manager.set_item(_model=Numeration, numerationid=2, endat=269, _insert=True)
        self.orm_manager.set_item(_insert=True, _model=OperationDelegation, numerationid=2, operationdescription="Нумерация кадров")
        self.orm_manager.set_item(_model=Comment, findstr="test_string_set_from_queue", ifcontains=True, _insert=True, commentid=2)
        self.orm_manager.set_item(_model=OperationDelegation, commentid=2, _insert=True, operationdescription="Комментарий")
        self.orm_manager.set_item(_model=Cnc, _insert=True, cncid=2, name="Ram", commentsymbol="#")
        self.orm_manager.set_item(_model=Cnc, _insert=True, cncid=1, name="Newcnc", commentsymbol="!")
        self.orm_manager.set_item(_model=Machine, machineid=2, cncid=2, machinename="Fidia", inputcatalog=r"D:\Heller",
                                  outputcatalog=r"C:\Test", _update=True)
        self.orm_manager.set_item(_model=Machine, machinename="Tesm", _insert=True, machineid=1, cncid=1, inputcatalog=r"D:\Test",
                                  outputcatalog=r"C:\anef")
        self.orm_manager.set_item(_model=Machine, machinename="65A90", _insert=True, inputcatalog=r"D:\Test",
                                  outputcatalog=r"C:\anef")
        self.orm_manager.set_item(_model=Machine, machinename="Rambaudi", _insert=True, inputcatalog=r"D:\Test",
                                  outputcatalog=r"C:\anef")

    def update_exists_items(self):
        self.orm_manager.set_item(cncid=1, name="nameeg", _model=Cnc, _update=True)
        self.orm_manager.set_item(_update=True, _model=Machine, machineid=2, inputcatalog=r"D:\other_path")
        self.orm_manager.set_item(numerationid=2, endat=4, _model=Numeration, _update=True)
        self.orm_manager.set_item(_model=Comment, commentid=2, findstr="test_str_new", _update=True)
        self.orm_manager.set_item(_model=Machine, machinename="testnameret", machineid=1, _update=True)


class TestLinkedList(unittest.TestCase):
    def test_init(self) -> None:
        LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                    {"node3_val": 4}, {"node4_val": 5}])
        LinkedList()

    def test_getitem(self):
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node4_val": 4}, {"node5_val": 5}])
        linked_list.__getitem__(4)
        linked_list[1]
        linked_list[-2]
        linked_list.__getitem__(-4)
        with self.assertRaises(IndexError):
            linked_list.__getitem__(8)
            linked_list[0]
            linked_list[-5]
        with self.assertRaises(TypeError):
            linked_list[{}]
            linked_list["w"]
            linked_list["34"]
            linked_list[None]
            linked_list[False]
            linked_list[True]
        self.assertEqual(1, linked_list[0].value["node_val"])
        self.assertEqual(2, linked_list[1].value["nod2_val"])
        self.assertEqual(3, linked_list[2].value["node3_val"])
        self.assertEqual(4, linked_list[3].value["node4_val"])

    def test_getitem_slice(self):
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        self.assertEqual(len(linked_list[:-2]), LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3}, {"node3_val": 4}]).__len__())
        self.assertEqual(linked_list[:-2], LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3}, {"node3_val": 4}]))
        self.assertEqual(linked_list[:-2], LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3}, {"node3_val": 4}]))
        self.assertEqual(linked_list[:2], linked_list[:-3])
        self.assertEqual(linked_list[:3], linked_list[:-2])
        self.assertEqual(linked_list[:1], linked_list[:1])
        self.assertEqual(linked_list[:-2], linked_list[:-2])
        self.assertEqual(linked_list[1:3], linked_list[1:-2])
        self.assertEqual(linked_list[:], linked_list)
        linked_list[0:float("inf")]
        with self.assertRaises(IndexError):
            linked_list[5:5]
            linked_list[5:]
            linked_list[:6]
            linked_list[6:]
            linked_list[:-5]
            linked_list[-5:]
            linked_list[10:]
            linked_list[99:]
            linked_list[-11:]
            linked_list[10:]
            linked_list[-99:]
            linked_list[11:]
            linked_list[10:]
            linked_list[99:]
            linked_list[-0:]
        with self.assertRaises(TypeError):
            linked_list[float(10):]
            linked_list[float("inf"):]
            linked_list["3":]
            linked_list["0":]
            linked_list[:"7"]
            linked_list[[]:]
            linked_list[["4"]:]
            linked_list[:5.7]
            linked_list[[None]:]
            linked_list[None:]
            linked_list[:[None]]
            linked_list[[2]:]
            linked_list[:0]
            linked_list[[None]:2]

    def test_setitem(self):
        linked_list = LinkedList()
        self.assertEqual(linked_list.__len__(), 0)
        with self.assertRaises(IndexError):
            linked_list[1] = {"val": "val"}
            linked_list[1] = {"val": "val"}
            linked_list[5] = {"val": "val"}
            linked_list[-1] = {"val": "val"}
        with self.assertRaises(TypeError):
            linked_list[None] = {"val": "val"}
            linked_list["sdf"] = {"val": "val"}
            linked_list["0"] = {"val": "val"}
        linked_list.__setitem__(0, {"node_val": "test_val"})
        linked_list.__setitem__(0, {"node_val": "test_val"})
        with self.assertRaises(IndexError):
            linked_list[4] = "nodeval"

    def test_bool(self):
        linked_list = LinkedList()
        self.assertFalse(linked_list)
        self.assertFalse(linked_list)
        linked_list.__setitem__(0, {"val": "val"})
        self.assertTrue(linked_list)
        del linked_list[0]
        self.assertFalse(linked_list)
        linked_list[0] = {"val1": 1}
        self.assertTrue(linked_list)
        linked_list.__setitem__(0, {"val2": 4})
        self.assertTrue(linked_list)
        linked_list.__setitem__(0, {"val3": 3})
        self.assertTrue(linked_list)
        del linked_list[0]
        self.assertFalse(linked_list)

    def test_len(self):
        linked_list = LinkedList()
        self.assertEqual(linked_list.__len__(), 0)
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        self.assertEqual(len(linked_list), 5)
        del linked_list[-1]
        self.assertEqual(len(linked_list), 4)
        linked_list.append(nval=1, val2="dfgdfg")
        self.assertEqual(len(linked_list), 5)

    def test_delitem(self):
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        linked_list.__delitem__(0)
        linked_list.__delitem__(-1)
        linked_list.__delitem__(2)
        linked_list.__delitem__(1)
        self.assertEqual(len(linked_list), 1)
        linked_list.__delitem__(-1)
        self.assertEqual(linked_list.__len__(), 0)
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        del linked_list[-2]
        del linked_list[-1]
        del linked_list[1]
        del linked_list[0]
        self.assertEqual(linked_list[0].value, {"node3_val": 3})
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        linked_list.__delitem__(3)
        linked_list.__delitem__(3)

    def test_iter(self):
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        items = [{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}]
        self.assertTrue(hasattr(linked_list, "__iter__"))
        iterator = iter(linked_list)
        counter = 0
        while iterator:
            try:
                node = next(iterator)
                if counter == len(items):
                    assert False
                self.assertEqual(node.value, items[counter])
            except StopIteration:
                break
            else:
                counter += 1
        if not counter == len(linked_list):
            assert False

    def test_contains(self):
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        for node in linked_list:
            if node not in linked_list:
                assert False

        other_linked_list = LinkedList([{"other_node_val": 1}, {"other2_node_val": 2}, {"other3_node_val": 3},
                                        {"other4_node_val": 4}, {"other5_node_val": 5}])
        for node in other_linked_list:
            self.assertFalse(linked_list.__contains__(node))
        self.assertFalse(linked_list.__contains__(None))
        self.assertFalse(linked_list.__contains__("1"))
        self.assertFalse(linked_list.__contains__(1))
        self.assertFalse(linked_list.__contains__(1.6))
        self.assertFalse(linked_list.__contains__([1]))

    def test_append(self):
        linked_list = LinkedList([{"node_val": 1}, {"nod2_val": 2}, {"node3_val": 3},
                                  {"node3_val": 4}, {"node4_val": 5}])
        self.assertEqual(linked_list.__len__(), 5)
        self.assertEqual(linked_list[-1].value, {"node4_val": 5})
        self.assertEqual(linked_list[4].value, {"node4_val": 5})

        linked_list.append(new_value_after_append=100)

        self.assertEqual(linked_list.__len__(), 6)
        self.assertEqual(linked_list[-1].value, {"new_value_after_append": 100})
        self.assertEqual(linked_list[5].value, {"new_value_after_append": 100})

        linked_list = LinkedList()
        self.assertEqual(linked_list.__len__(), 0)
        with self.assertRaises(IndexError):
            linked_list[-1]
            linked_list[4]
        linked_list.append(new_value_after_append=100)
        linked_list.append(new_value1_after_append=100)
        self.assertEqual(linked_list.__len__(), 2)
        self.assertEqual(linked_list[0].value, {"new_value_after_append": 100})
        self.assertEqual(linked_list[-1].value, {"new_value1_after_append": 100})


class TestToolItemQueue(unittest.TestCase):
    def setUp(self) -> None:
        Tool.CACHE_PATH = CACHE_PATH
        Tool.DATABASE_PATH = DATABASE_PATH
        Queue.LinkedListItem = QueueItem

    def test_init(self):
        Queue()
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Test", "machineid": 1},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Name", "machineid": 4},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "NewTest", "machineid": 2
                 }]
        Queue(data)
        # invalid primary key
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Test", "machineid": {}},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Name", "machineid": 4},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "NewTest", "machineid": int
                 }]
        with self.assertRaises(NodeColumnValueError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Test", "machineid": ""}]
        with self.assertRaises(NodeColumnValueError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Test", "machineid": "12"}]
        with self.assertRaises(NodeColumnValueError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Test", "machineid": None}]
        with self.assertRaises(NodeColumnValueError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Test"}]
        with self.assertRaises(NodePrimaryKeyError):
            Queue(data)
        # повторение столбца c unique constraint - machinename

    def test_enqueue(self):
        queue = Queue()
        data__len_3 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "NewTest", "machineid": 3
                        }]
        self.assertIsNone(queue.dequeue())
        self.assertEqual(queue.__len__(), 0)
        with self.assertRaises(IndexError):
            queue.__getitem__(0)
            queue[-1]
            queue[-4]
            queue[2]
            queue[1]
            queue[10]
        with self.assertRaises(StopIteration):
            next(iter(queue))
        queue.enqueue(**data__len_3[0])
        self.assertEqual(len(queue), 1)
        self.assertIsNotNone(queue[0])
        self.assertIsNotNone(queue[-1])
        queue.enqueue(**data__len_3[1])
        self.assertEqual(len(queue), 2)
        self.assertIsNotNone(queue[0])
        self.assertIsNotNone(queue[1])
        self.assertIsNotNone(queue[-1])
        self.assertIsNotNone(queue[-2])
        queue.append(**data__len_3[2])
        self.assertEqual(len(queue), 3)
        self.assertEqual(queue[-1].value["machinename"], "NewTest")
        self.assertEqual(queue[0].value["machinename"], "Test")
        self.assertEqual(queue[1].value["machinename"], "Test1")
        #
        # Столбец machinename с uniqie=True: произойдёт репликация без добавления новой ноды,
        # вместо этого будет замена старой ноды с дополнением её содержимого
        #
        queue = Queue()
        data__len_1 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test", "xover": 10, "machineid": 3},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test1", "yover": 10, "machineid": 3},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "zover": 10, "machineid": 3
                        }]
        [queue.enqueue(**data__len_1[i]) for i in range(len(data__len_1))]
        self.assertEqual(queue.__len__(), 1)
        #  Проверить, что новые данные, которые добавлялись за 3 итерации, вошли в результирующую ноду
        self.assertEqual(len(set(queue[0].value).intersection(set({"xover": 10, "yover": 10, "zover": 10}))), 3)
        queue = Queue()
        data_with_primary_key_from_ui = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                          "_delete": False, "_create_at": datetime.datetime.now(), 
                                          "machinename": "FirstTest", "xover": 10, "machineid": 3},
                                         {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                          "_delete": False, "_create_at": datetime.datetime.now(), 
                                          "machinename": "Test", "yover": 10, "machineid": 3},
                                         {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                          "_delete": False, "_create_at": datetime.datetime.now(), 
                                          "machinename": "LastName", "zover": 10, "xover": 0, "machineid": 3
                                          }]
        [queue.enqueue(**data) for data in data_with_primary_key_from_ui]
        self.assertEqual(queue.__len__(), 1)
        for key, value in {"zover": 10, "xover": 0, "yover": 10, "machinename": "LastName"}.items():
            if key not in queue[0].value:
                assert False
            if not queue[0].value[key] == value:
                assert False

    def test_dequeue(self):
        queue = Queue()
        data__len_3 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test", "machineid": 3},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Ram", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "NewTest", "machineid": 1
                        }]
        with self.assertRaises(IndexError):
            queue[0]
            queue[-1]
            queue[1]
            queue[2]
        [queue.enqueue(**data) for data in data__len_3]
        queue[1]
        queue[0]
        queue[2]
        queue[-1]
        queue[-2]
        self.assertEqual(len(data__len_3), len(queue))
        self.assertEqual(queue.dequeue().value["machinename"], "Test")
        self.assertEqual(2, queue.__len__())
        self.assertEqual(queue.dequeue().value["machinename"], "Ram")
        self.assertEqual(1, queue.__len__())
        self.assertEqual(queue.dequeue().value["machinename"], "NewTest")
        self.assertEqual(0, len(queue))
        with self.assertRaises(IndexError):
            queue[0]
            queue[-1]
            queue[1]
            queue[2]

    def test_remove_node_from_queue(self):
        queue = Queue()
        data__len_3 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "NewTest", "machineid": 3
                        }]
        [queue.enqueue(**data) for data in data__len_3]
        self.assertEqual(3, len(queue))
        queue[0]
        queue[1]
        queue[2]
        queue[-1]
        queue[-2]
        with self.assertRaises(IndexError):
            queue[-3]
            queue[3]
        queue.remove(Machine, "machineid", 1)
        queue.remove(Machine, "machineid", 2)
        queue.remove(Machine, "machineid", 3)
        self.assertEqual(0, len(queue))

    def test_add(self):
        queue = Queue([{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(),
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(),
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(),
                        "machinename": "NewTest", "machineid": 3
                        }])
        other_queue = Queue([{"_model": Condition, "cnd": str(uuid4()), "_insert": True},
                             {"_model": Cnc, "cncid": 2, "_insert": True}])
        self.assertEqual(5, (other_queue + queue).__len__())
        queue_after_concat = queue + other_queue
        first_node = queue_after_concat[0]
        last_node = queue_after_concat[-1]
        self.assertEqual("Test", first_node["machinename"])
        self.assertEqual(last_node["cncid"], 2)
        self.assertEqual(3, queue_after_concat[2]["machineid"])
        self.assertEqual(queue.__len__(), 3)
        self.assertEqual(2, other_queue.__len__())

    def test_iadd(self):
        queue = Queue([{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(),
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(),
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(),
                        "machinename": "NewTest", "machineid": 3
                        }])
        other_queue = Queue([{"_model": Condition, "cnd": str(uuid4()), "_insert": True},
                             {"_model": Cnc, "cncid": 2, "_insert": True}])
        self.assertEqual(queue.__len__(), 3)
        self.assertEqual(2, other_queue.__len__())
        queue += other_queue
        self.assertEqual(2, len(other_queue))
        self.assertEqual(5, queue.__len__())
        first_node = queue[0]
        last_node = queue[-1]
        self.assertEqual("Test", first_node["machinename"])
        self.assertEqual(last_node["cncid"], 2)
        self.assertEqual(3, queue[2]["machineid"])


class TestResultORMCollection(unittest.TestCase):
    def setUp(self) -> None:
        Tool.CACHE_PATH = CACHE_PATH
        Tool.DATABASE_PATH = DATABASE_PATH
        queue = ServiceOrmContainer()
        data__len_3 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), 
                        "machinename": "NewTest", "machineid": 3
                        }]
        [queue.enqueue(**item) for item in data__len_3]
        self.result_collection = ResultORMCollection(queue)

    def test_result_orm_collection(self):
        self.assertEqual(self.result_collection.__len__(), 3)
        self.assertTrue(self.result_collection)
        hash_val = hash(self.result_collection)
        queue = ServiceOrmContainer()
        changed_data__len_3 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                "_delete": False, "_create_at": datetime.datetime.now(), 
                                "machinename": "Tdfgdfgerest", "machineid": 1},
                               {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                "_delete": False, "_create_at": datetime.datetime.now(), 
                                "machinename": "Test1", "machineid": 2},
                               {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                "_delete": False, "_create_at": datetime.datetime.now(), 
                                "machinename": "NewTgest", "machineid": 3
                                }]
        [queue.enqueue(**item) for item in changed_data__len_3]
        result_queue = ResultORMCollection(queue)
        self.assertEqual(result_queue.__len__(), 3)
        self.assertTrue(result_queue)
        self.assertEqual(3, len(result_queue))
        self.assertNotEqual(hash_val, result_queue.__hash__())

    def test_add_model_prefix(self):
        self.result_collection.add_model_name_prefix()
        self.assertEqual(self.result_collection.prefix, "add")
        self.assertEqual([node for node in self.result_collection
                          for value in node.value if not value.startswith("Machine.")], [])
        self.assertTrue(all([[len(frozenset(filter(lambda x: 1 if x == "." else 0, val)))]
                            for node in self.result_collection
                            for val in node.value]))
        self.result_collection.remove_model_prefix()
        self.assertEqual(self.result_collection.prefix, "no-prefix")
        self.assertEqual([node for node in self.result_collection
                          for column in node.value if column.startswith("Machine.")], [])

    def test_remove_model_prefix(self):
        self.result_collection.add_model_name_prefix()
        self.result_collection.remove_model_prefix()
        self.assertFalse(all([val.startswith("Machine.") if True else False
                              for node in self.result_collection
                              for val in node.value]))

    def test_auto_mode_prefix(self):
        queue = ServiceOrmContainer()
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "machinename": "Test", "cncid": 1, "machineid": 1},
                {"_model": Cnc, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), 
                 "name": "Testcnc", "cncid": 1},
                {"_model": OperationDelegation, "replaceid": 1,
                 "_create_at": datetime.datetime.now(),  "_insert": True, "opid": str(uuid4())},
                {"_model": Replace, "replaceid": 1, "findstr": "trststr",
                 "_create_at": datetime.datetime.now(),  "_insert": True}
                ]
        [queue.enqueue(**n) for n in data]
        self.result_collection = ResultORMCollection(queue)
        self.result_collection.auto_model_prefix()
        self.assertEqual("auto", self.result_collection.prefix)
        # Столбец cncid встречается в обеих нодах, должно произойти добавление префикса с названием таблицы
        # к одноимённым столбцам обеих нод
        self.assertIn("Cnc.cncid", self.result_collection[1].value)
        self.assertIn("OperationDelegation.replaceid", self.result_collection[2].value)
        self.assertIn("Replace.replaceid", self.result_collection[3].value)


class TestToolHelper(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        Tool.CACHE_LIFETIME_HOURS = 60
        self.orm_manager = Tool()

    def test_cache_property(self):
        """ Что вернёт это свойство: Если эклемпляр Client, то OK """
        self.assertIsInstance(self.orm_manager.connection.cache, PooledClient,
                              msg=f"Свойство должно было вернуть эклемпляр класса MockMemcacheClient, "
                                  f"а на деле {self.orm_manager.connection.cache.__class__.__name__}")

    def test_cache(self):
        self.orm_manager.connection.cache.set("1", 1)
        value = self.orm_manager.connection.cache.get("1")
        self.assertEqual(value, 1, msg="Результирующее значение, полученное из кеша отличается от заданного в тесте")

    def test_not_configured_model(self):
        """ Предварительно не был вызван метод set_model. Неправильная конфигурация"""
        self.orm_manager.get_items(_model=Machine, machinename="test_name")
        self.orm_manager.get_items(_model=Machine)
        with self.assertRaises(InvalidModel):
            self.orm_manager.set_item(_insert=True, machinename="Heller", _ready=True)

    def test_drop_cache(self):
        self.orm_manager.connection.cache.set("1", "test")
        self.orm_manager.connection.cache.set("3", "test")
        self.orm_manager.drop_cache()
        self.assertIsNone(self.orm_manager.connection.cache.get("1"))
        self.assertIsNone(self.orm_manager.connection.cache.get("3"))

    def test_database_property(self):
        self.assertIsInstance(self.orm_manager.connection.database, ScopedSession)

    @db_reinit
    def test_database_insert_and_select_single_entry(self):
        session = self.orm_manager.connection.database
        session.add(Machine(machinename="Test", inputcatalog=r"C:\Test", outputcatalog="C:\\TestPath"))
        session.commit()
        self.assertEqual(self.orm_manager.connection.database.execute(text("SELECT COUNT(machineid) FROM machine")).scalar(), 1)
        data = self.orm_manager.connection.database.execute(select(Machine).filter_by(machinename="Test")).scalar().__dict__
        self.assertEqual(data["machinename"], "Test")
        self.assertEqual(data["inputcatalog"], "C:\\Test")
        self.assertEqual(data["outputcatalog"], "C:\\TestPath")

    @db_reinit
    def test_database_insert_and_select_two_joined_entries(self):
        session = self.orm_manager.connection.database
        session.add(Cnc(name="testcnc", commentsymbol="*"))
        session.add(Machine(machinename="Test", inputcatalog="C:\\Test", outputcatalog="C:\\TestPath", cncid=1))
        session.commit()
        self.assertEqual(self.orm_manager.connection.database.execute(text("SELECT COUNT(*) "
                                                                "FROM machine "
                                                                "INNER JOIN cnc "
                                                                "ON machine.cncid=cnc.cncid "
                                                                "WHERE machine.machinename='Test' AND cnc.name='testcnc'"
                                                                )
                                                           ).scalar(), 1)
        self.assertEqual(self.orm_manager.connection.database.execute(text("SELECT COUNT(*) "
                                                                "FROM machine "
                                                                "WHERE machine.cncid=(SELECT cncid FROM cnc WHERE name = 'testcnc')"
                                                                )
                                                           ).scalar(), 1)

    @drop_cache
    @db_reinit
    def test_items_property(self):
        self.set_data_into_queue()
        self.assertIsInstance(self.orm_manager.connection.cache.get("ORMItems"), type(self.orm_manager.connection.items))
        self.orm_manager.set_item(_insert=True, _model=Cnc, name="F")
        self.assertEqual(len(self.orm_manager.connection.items), 11)

    @drop_cache
    @db_reinit
    def test_set_item(self):
        # GOOD
        self.orm_manager.set_item(_insert=True, _model=Cnc, name="Fid", commentsymbol="$")
        self.assertIsNotNone(self.orm_manager.connection.cache.get("ORMItems"))
        self.assertIsInstance(self.orm_manager.connection.cache.get("ORMItems"), Queue)
        self.assertEqual(self.orm_manager.connection.cache.get("ORMItems").__len__(), 1)
        self.assertTrue(self.orm_manager.connection.items[0]["name"] == "Fid")
        self.orm_manager.set_item(_insert=True, _model=Machine, machinename="Helller",
                                  inputcatalog=r"C:\\wdfg", outputcatalog=r"D:\\hfghfgh", _ready=True)
        self.assertEqual(len(self.orm_manager.connection.items), 2)
        self.assertEqual(len(self.orm_manager.connection.items), len(self.orm_manager.connection.cache.get("ORMItems")))
        self.assertTrue(any(map(lambda x: x.value.get("machinename", None), self.orm_manager.connection.items)))
        self.assertIs(self.orm_manager.connection.items[1].model, Machine)
        self.assertIs(self.orm_manager.connection.items[0].model, Cnc)
        self.orm_manager.set_item(_model=OperationDelegation, _update=True, operationdescription="text")
        self.assertEqual(self.orm_manager.connection.items[2].value["operationdescription"], "text")
        self.orm_manager.set_item(_insert=True, _model=Condition, findfull=True, parentconditionbooleanvalue=True)
        self.assertEqual(self.orm_manager.connection.items.__len__(), 4)
        self.orm_manager.set_item(_delete=True, machinename="Some_name", _model=Machine, inputcatalog=r"D:\Test",
                                  outputcatalog=r"C:\anef")
        self.orm_manager.set_item(_delete=True, machinename="Some_name_2", _model=Machine)
        result = self.orm_manager.get_items(_model=Machine, machinename="Helller", _db_only=True)
        import time
        time.sleep(self.orm_manager.RELEASE_INTERVAL_SECONDS + 1)
        self.assertTrue(result)
        # start Invalid ...
        # плохой path
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, _insert=True, _model=Machine, input_path="path")  # inputcatalog
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, _insert=True, _model=Machine, output_path="path")  # outputcatalog
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _insert=True, _model=Machine, inputcatalog=4)
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _insert=True, _model=Machine, outputcatalog=7)
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _insert=True, _model=Machine, outputcatalog=None)
        # Invalid model
        self.assertRaises(InvalidModel, self.orm_manager.set_item, machinename="Test", _update=True)  # model = None
        self.assertRaises(InvalidModel, self.orm_manager.set_item, machinename="Test", _insert=True, _model=2)  # model: Type[int]
        self.assertRaises(InvalidModel, self.orm_manager.set_item, machinename="Test", _update=True, _model="test")  # model: Type[str]
        self.assertRaises(InvalidModel, self.orm_manager.set_item, machinename="Test", _insert=True, _model=self.__class__)
        self.assertRaises(InvalidModel, self.orm_manager.set_item, machinename="Heller", _delete=True, _model=None)
        self.assertRaises(InvalidModel, self.orm_manager.set_item, machinename="Heller", _delete=True, _model={1: True})
        self.assertRaises(InvalidModel, self.orm_manager.set_item, machinename="Heller", _delete=True, _model=['some_str'])
        # invalid field
        # field name | такого поля нет в таблице
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, invalid_="testval", _model=Machine, _insert=True)
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, invalid_field="val", other_field=2,
                          other_field_5="name", _model=Cnc, _update=True)  # Поля нету в таблице
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, field="value", _model=OperationDelegation, _delete=True)  # Поля нету в таблице
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, inv="testl", _model=Machine, _insert=True)  # Поля нету в таблице
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, machinename=object(), _model=SearchString, _insert=True)
        self.assertRaises(NodeColumnError, self.orm_manager.set_item, name="123", _model=SearchString, _insert=True)
        # field value | значение не подходит
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _model=Machine, _update=True, machinename=Machine())
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _model=Machine, _update=True, machinename=Cnc())
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _model=Machine, _update=True, machinename=int)
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _model=OperationDelegation, _update=True, operationdescription=lambda x: x)
        self.assertRaises(NodeColumnValueError, self.orm_manager.set_item, _model=OperationDelegation, _update=True, operationdescription=4)

    @drop_cache
    @db_reinit
    def test_get_items(self):
        self.assertIsInstance(self.orm_manager.get_items(_model=Machine), Result)
        self.assertEqual(self.orm_manager.get_items(_model=Machine).__len__(), 0)
        # Элементы с _delete=True игнорируются в выборке через метод get_items,- согласно замыслу
        # Тем не менее, в очереди они должны присутствовать: см свойство items

        self.orm_manager.set_item(_model=Machine, machinename="Fidia", inputcatalog="C:\\path", _insert=True, outputcatalog=r"T:\ddfg")
        self.assertEqual(self.orm_manager.get_items(_model=Machine).__len__(), 1)
        self.orm_manager.set_item(_model=Condition, condinner="text", less=True, _insert=True)
        self.orm_manager.set_item(_model=Cnc, name="Fid", cncid=3, commentsymbol="$", _update=True)
        self.assertEqual(self.orm_manager.get_items(_model=Machine).__len__(), 1)
        self.assertEqual(self.orm_manager.get_items(_model=Condition).__len__(), 1)
        self.assertEqual(self.orm_manager.get_items(_model=Cnc).__len__(), 1)
        self.assertEqual(self.orm_manager.get_items(_model=Cnc, commentsymbol="$").__len__(), 1)
        self.assertEqual(self.orm_manager.get_items(_model=Cnc, name="Fid").__len__(), 1)
        self.orm_manager.set_item(_model=Machine, machinename="Fidia", inputcatalog="C:\\pathnew", _insert=True, outputcatalog=r"T:\name")
        self.assertEqual(self.orm_manager.get_items(Machine, machinename="Fidia")[0]["machinename"], "Fidia")
        self.assertEqual(self.orm_manager.get_items(Machine, machinename="Fidia").__len__(), 1)

    @drop_cache
    @db_reinit
    def test_join_select_merge(self):
        self.set_data_into_database()
        self.set_data_into_queue()
        result = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}, _use_join=False)
        # Провокационный момент:
        # Ставим в столбец отношения внешнего ключа значение, чьего PK не существует
        # тогда будет взята связка из базы данных! с прежним pk-fk
        result.order_by(by_primary_key=True, model=Machine)
        self.assertFalse(any(filter(lambda x: not len(x) == 2, result)))
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=9, _update=True)
        self.assertFalse(any(filter(lambda x: not len(x) == 2, result)))
        self.assertEqual(4, result.__len__())
        self.assertEqual(result.items[0]["Cnc"]["cncid"], 1, result.items[0]["Machine"]["cncid"])
        self.assertEqual(result.items[0]["Cnc"]["name"], "Newcnc")
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Tesm")
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=2, _update=True)  # найдётся по столбцу machinename, потому что (unique constraint)
        self.assertFalse(any(filter(lambda x: not len(x) == 2, result)))
        # Изначально было 2 связки, но так как 1 разрушили, то осталась всего одна
        self.assertEqual(3, result.__len__())
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Tesm")
        self.assertEqual(result.items[0]["Cnc"]["name"], "Ram")
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=1, _update=True)
        self.assertEqual(4, result.__len__())
        self.orm_manager.set_item(Machine, cncid=2, machineid=2, _update=True)
        self.assertEqual(4, len(result))
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Tesm")
        self.assertEqual(result.items[0]["Cnc"]["name"], "Newcnc")
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=1, _update=True)  # Ничего не должно измениться
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=1, _update=True)  # Ничего не должно измениться
        self.assertEqual(4, len(result))
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Tesm")
        self.assertEqual(result.items[0]["Cnc"]["name"], "Newcnc")
        # Нарушить связь PK - FK, но в бд по-прежнему cncid=1 - machineid=1, поэтому появится в результатах
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=9, _update=True)
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Tesm")
        self.assertEqual(result.items[0]["Cnc"]["name"], "Newcnc")

    @drop_cache
    @db_reinit
    def test_join_select__join_select_option(self):
        # Добавить в базу и кеш данные
        self.set_data_into_database()
        self.set_data_into_queue()
        # Возвращает ли метод экземпляр класса JoinSelectResult?
        self.assertIsInstance(self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}, _use_join=True),
                              JoinSelectResult)
        # GOOD (хороший случай)
        # Найдутся ли записи с pk равными значениям, которые мы добавили
        # Machine - Cnc
        result = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"},
                                              _use_join=True)
        result.order_by(by_primary_key=True, model=Machine)
        self.assertEqual("Newcnc", result.items[0]["Cnc"]["name"])
        self.assertEqual(1, result.items[0]["Cnc"]["cncid"])
        self.assertEqual("Tesm", result.items[0]["Machine"]["machinename"])
        self.assertEqual("Ram", result.items[1]["Cnc"]["name"])
        self.assertEqual("Fidia", result.items[1]["Machine"]["machinename"])
        self.assertNotEqual(result.items[0]["Cnc"]["cncid"], result.items[1]["Cnc"]["cncid"])
        self.assertEqual(result.items[0]["Cnc"]["cncid"], result.items[0]["Machine"]["cncid"])
        #
        # Numeration - Operationdelegation
        #
        result = self.orm_manager.join_select(OperationDelegation, Numeration,
                                              _on={"Numeration.numerationid": "OperationDelegation.numerationid"},
                                              _use_join=True)
        self.assertEqual("Нумерация. Добавил сразу в БД", result.items[0]["OperationDelegation"]["operationdescription"])
        self.assertNotEqual("Нумерация. Добавил сразу в БД", result.items[1]["OperationDelegation"]["operationdescription"])
        self.assertEqual("Нумерация кадров", result.items[1]["OperationDelegation"]["operationdescription"])
        self.assertEqual(result.items[0]["Numeration"]["numerationid"], 1)
        self.assertEqual(269, result.items[1]["Numeration"]["endat"])
        #
        # Comment - OperationDelegation
        #
        result = self.orm_manager.join_select(Comment, OperationDelegation, _on={"Comment.commentid": "OperationDelegation.commentid"},
                                              _use_join=True)
        self.assertEqual("test_string_set_from_queue", result.items[1]["Comment"]["findstr"])
        self.assertNotEqual("test_string_set_from_queue", result.items[0]["Comment"]["findstr"])
        self.assertEqual("test_str", result.items[0]["Comment"]["findstr"])
        self.assertNotEqual("test_str", result.items[1]["Comment"]["findstr"])
        self.assertEqual(result.items[0]["Comment"]["iffullmatch"], True)
        self.assertNotIn("iffullmatch", result.items[1]["Comment"])
        self.assertEqual(True, result.items[1]["Comment"]["ifcontains"])
        self.assertFalse(result.items[0]["Comment"]["ifcontains"])
        #
        # Отбор только из локальных данных (очереди), но в базе данных их пока что быть не должно
        #
        # Machine - Cnc
        #
        local_data = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"},
                                                  _queue_only=True, _use_join=True)
        database_data = self.orm_manager.join_select(Cnc, Machine, _on={"Cnc.cncid": "Machine.cncid"},
                                                     _db_only=True, _use_join=True)
        local_data.order_by(by_primary_key=True, model=Machine)
        database_data.order_by(by_primary_key=True, model=Machine)
        self.assertEqual(local_data.items[0]["Machine"]["cncid"], local_data.items[0]["Cnc"]["cncid"])
        self.assertEqual(database_data.items[0]["Cnc"]["cncid"], database_data.items[0]["Machine"]["cncid"])
        self.assertIn("machineid", local_data.items[0]["Machine"])
        self.assertIn("machineid", database_data.items[0]["Machine"])
        self.assertNotEqual(local_data.items[0]["Machine"]["machinename"], database_data.items[0]["Machine"]["machinename"])
        self.assertEqual("Fidia", local_data.items[1]["Machine"]["machinename"])
        self.assertEqual("Newcnc", local_data.items[0]["Cnc"]["name"])
        self.assertNotEqual(local_data.items[0]["Cnc"]["name"], database_data.items[0]["Cnc"]["name"])
        #
        # Comment - OperationDelegation
        #
        local_data = self.orm_manager.join_select(Comment, OperationDelegation, _use_join=True,
                                                  _on={"Comment.commentid": "OperationDelegation.commentid"}, _queue_only=True)
        database_data = self.orm_manager.join_select(Comment, OperationDelegation, _use_join=True,
                                                     _on={"Comment.commentid": "OperationDelegation.commentid"}, _db_only=True)
        self.assertNotEqual(local_data.items[0]["Comment"]["commentid"], database_data.items[0]["Comment"]["commentid"])
        self.assertEqual(local_data.items[0]["Comment"]["commentid"], local_data.items[0]["OperationDelegation"]["commentid"])
        self.assertEqual(database_data.items[0]["Comment"]["commentid"], database_data.items[0]["OperationDelegation"]["commentid"])
        #
        # Плохие аргументы ...
        # invalid model
        #
        self.assertRaises(InvalidModel, self.orm_manager.join_select, "str", Machine, _on={"Cnc.cncid": "Machine.cncid"})
        self.assertRaises(InvalidModel, self.orm_manager.join_select, Machine, 5, _on={"Cnc.cncid": "Machine.cncid"})
        self.assertRaises(InvalidModel, self.orm_manager.join_select, Machine, "str", _on={"Cnc.cncid": "Machine.cncid"})
        self.assertRaises((ValueError, InvalidModel,), self.orm_manager.join_select, "str", object())
        #
        # invalid named on...
        #
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc, _on=6)
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on=object())
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc, _on=[])
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc, _on="[]")
        #
        # Модели, переданные в аргументах (позиционных), не связаны с моделями и полями в именованном аргументе 'on'.
        # join_select(a_model, b_model _on={"a_model.column_name": "b_model.column_name"})
        #
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "SomeModel.other_field"})
        #
        # Именованный параметр on содержит недействительные данные
        #
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"invalid_field": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"Machine.invalid_field": ".other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={".invalid_field": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "SomeModel."})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.": "SomeModel."})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "."})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={".": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": " "})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={" ": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "-"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": 5})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": 2.3})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={2.9: 5})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={4: "Machine.machinename"})

    @drop_cache
    @db_reinit
    def test_join_select__non_join_select_option(self):
        # Добавить в базу и кеш данные
        self.set_data_into_database()
        self.set_data_into_queue()
        # Возвращает ли метод экземпляр класса JoinSelectResult?
        self.assertIsInstance(self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}, _use_join=False),
                              JoinSelectResult)
        # GOOD (хороший случай)
        # Найдутся ли записи с pk равными значениям, которые мы добавили
        # Machine - Cnc
        result = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}, _use_join=False)
        result.order_by(Machine, by_primary_key=True, decr=False)
        self.assertEqual("Newcnc", result.items[0]["Cnc"]["name"])
        self.assertEqual("Tesm", result.items[0]["Machine"]["machinename"])
        self.assertEqual("Ram", result.items[1]["Cnc"]["name"])
        self.assertEqual("Fidia", result.items[1]["Machine"]["machinename"])
        self.assertNotEqual(result.items[0]["Cnc"]["cncid"], result.items[1]["Cnc"]["cncid"])
        self.assertEqual(result.items[0]["Cnc"]["cncid"], result.items[0]["Machine"]["cncid"])
        #
        # Numeration - Operationdelegation
        #
        result = self.orm_manager.join_select(Numeration, OperationDelegation,
                                              _on={"Numeration.numerationid": "OperationDelegation.numerationid"}, _use_join=False)
        result.order_by(Numeration, by_primary_key=True, decr=False)
        self.assertEqual("Нумерация кадров", result.items[1]["OperationDelegation"]["operationdescription"])
        self.assertEqual("Нумерация. Добавил сразу в БД", result.items[0]["OperationDelegation"]["operationdescription"])
        self.assertEqual("Нумерация кадров", result.items[1]["OperationDelegation"]["operationdescription"])
        self.assertEqual(result.items[0]["Numeration"]["numerationid"], 1)
        self.assertEqual(269, result.items[1]["Numeration"]["endat"])
        #
        # Comment - OperationDelegation
        #
        result = self.orm_manager.join_select(Comment, OperationDelegation, _on={"Comment.commentid": "OperationDelegation.commentid"},
                                              _use_join=False)
        self.assertEqual("test_string_set_from_queue", result.items[1]["Comment"]["findstr"])
        self.assertNotEqual("test_string_set_from_queue", result.items[0]["Comment"]["findstr"])
        self.assertEqual("test_str", result.items[0]["Comment"]["findstr"])
        self.assertNotEqual("test_str", result.items[1]["Comment"]["findstr"])
        self.assertEqual(result.items[0]["Comment"]["iffullmatch"], True)
        self.assertNotIn("iffullmatch", result.items[1]["Comment"])
        self.assertEqual(True, result.items[1]["Comment"]["ifcontains"])
        self.assertFalse(result.items[0]["Comment"]["ifcontains"])
        #
        # Отбор только из локальных данных (очереди), но в базе данных их пока что быть не должно
        #
        # Machine - Cnc
        #
        local_data = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}, _queue_only=True, _use_join=False)
        database_data = self.orm_manager.join_select(Cnc, Machine, _on={"Cnc.cncid": "Machine.cncid"}, _db_only=True, _use_join=False)
        local_data.order_by(by_primary_key=True, model=Machine)
        database_data.order_by(by_primary_key=True, model=Machine)
        self.assertEqual(local_data.items[0]["Machine"]["cncid"], local_data.items[0]["Cnc"]["cncid"])
        self.assertEqual(database_data.items[0]["Cnc"]["cncid"], database_data.items[0]["Machine"]["cncid"])
        self.assertIn("machineid", local_data.items[0]["Machine"])
        self.assertIn("machineid", database_data.items[0]["Machine"])
        self.assertNotEqual(local_data.items[0]["Machine"]["machinename"], database_data.items[0]["Machine"]["machinename"])
        self.assertEqual("Fidia", local_data.items[1]["Machine"]["machinename"])
        self.assertEqual("Newcnc", local_data.items[0]["Cnc"]["name"])
        self.assertNotEqual(local_data.items[0]["Cnc"]["name"], database_data.items[0]["Cnc"]["name"])
        #
        # Comment - OperationDelegation
        #
        local_data = self.orm_manager.join_select(Comment, OperationDelegation, _on={"Comment.commentid": "OperationDelegation.commentid"},
                                                  _queue_only=True, _use_join=False)
        database_data = self.orm_manager.join_select(Comment, OperationDelegation, _on={"Comment.commentid": "OperationDelegation.commentid"},
                                                     _db_only=True, _use_join=False)
        self.assertNotEqual(local_data.items[0]["Comment"]["commentid"], database_data.items[0]["Comment"]["commentid"])
        self.assertEqual(local_data.items[0]["Comment"]["commentid"], local_data.items[0]["OperationDelegation"]["commentid"])
        self.assertEqual(database_data.items[0]["Comment"]["commentid"], database_data.items[0]["OperationDelegation"]["commentid"])
        #
        # Плохие аргументы ...
        # invalid model
        #
        self.assertRaises(InvalidModel, self.orm_manager.join_select, "str", Machine, _on={"Cnc.cncid": "Machine.cncid"})
        self.assertRaises(InvalidModel, self.orm_manager.join_select, Machine, 5, _on={"Cnc.cncid": "Machine.cncid"})
        self.assertRaises(InvalidModel, self.orm_manager.join_select, Machine, "str", _on={"Cnc.cncid": "Machine.cncid"})
        self.assertRaises((ValueError, InvalidModel,), self.orm_manager.join_select, "str", object())
        #
        # invalid named on...
        #
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc, _on=6)
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on=object())
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc, _on=[])
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc, _on="[]")
        #
        # Модели, переданные в аргументах (позиционных), не связаны с моделями и полями в именованном аргументе 'on'.
        # join_select(a_model, b_model _on={"a_model.column_name": "b_model.column_name"})
        #
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "SomeModel.other_field"})
        #
        # Именованный параметр on содержит недействительные данные
        #
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"invalid_field": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"Machine.invalid_field": ".other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={".invalid_field": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "SomeModel."})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.": "SomeModel."})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "."})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={".": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": " "})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={" ": "SomeModel.other_field"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": "-"})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": 5})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={"InvalidModel.invalid_field": 2.3})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={2.9: 5})
        self.assertRaises((AttributeError, TypeError, ValueError,), self.orm_manager.join_select, Machine, Cnc,
                          _on={4: "Machine.machinename"})

    def test_join_select_triple_models__join_select_option(self):
        """ Тестировать запрос с join select, в котором участвуют 3 таблицы """
        # Добавить в базу и кеш данные
        self.set_data_into_database()
        self.set_data_into_queue()
        # Возвращает ли метод экземпляр класса JoinSelectResult?
        self.assertIsInstance(self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid",

                                                                              }, _use_join=False),
                              JoinSelectResult)

    @drop_cache
    @db_reinit
    def test_has_changes(self):
        self.set_data_into_database()
        self.set_data_into_queue()
        select_result = self.orm_manager.get_items(Cnc)
        self.assertFalse(select_result.has_changes())
        pk_0_index = select_result.items[0].get_primary_key_and_value()
        pk_1_index = select_result.items[1].get_primary_key_and_value()
        hash_from_cncid0 = hash(select_result.items[0])
        hash_from_cncid1 = hash(select_result.items[1])
        self.assertFalse(select_result.has_changes())
        self.orm_manager.set_item(_model=Cnc, **pk_0_index, name="newtestname", _update=True, commentsymbol="$")
        self.assertTrue(select_result.has_changes(hash_from_cncid0))
        self.assertFalse(select_result.has_changes(hash_from_cncid1))
        self.assertFalse(select_result.has_changes())
        self.orm_manager.set_item(_model=Cnc, name="testname", _update=True, **pk_1_index, commentsymbol="^")
        self.assertTrue(select_result.has_changes(hash_from_cncid1))
        self.assertFalse(select_result.has_changes())
        self.assertFalse(select_result.has_changes())
        self.orm_manager.set_item(_model=Cnc, _insert=True, name="newname", commentsymbol="&")
        new_hash_val = select_result.items[-1].__hash__()
        self.assertTrue(select_result.has_changes())
        self.assertFalse(select_result.has_changes(new_hash_val))
        self.assertFalse(select_result.has_changes())

    @drop_cache
    @db_reinit
    def test_join_select__has_changes(self):
        """ Метод has_changes класса JoinSelectResult принимает в качестве аргумента хеш-сумму от одного контейнера
        со связанными моделями. """
        self.set_data_into_database()
        self.set_data_into_queue()
        join_select_result = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}, _use_join=False)
        join_select_result.order_by(Cnc, by_primary_key=True)
        self.assertFalse(join_select_result.has_changes())
        self.orm_manager.set_item(_model=Cnc, name="name_n", _update=True, cncid=1)
        self.assertTrue(join_select_result.has_changes())
        hash_value_from_0_index = join_select_result[0].__hash__()  # Хеш-сумма от пары, которая изменится строкой ниже
        hash_value_from_2_index = join_select_result[2].hash_by_pk  # Хеш-сумма от пары, которая не изменялась, должен вернуться False
        self.orm_manager.set_item(_model=Machine, _update=True, machinename="name", machineid=1)
        self.assertFalse(join_select_result.has_changes(hash_value_from_2_index))
        self.assertTrue(join_select_result.has_changes(hash_value_from_0_index))
        self.orm_manager.set_item(_model=Cnc, name="name_n", _update=True, commentsymbol="#")
        self.assertTrue(join_select_result.has_changes())
        self.orm_manager.set_item(_model=Cnc, name="naаке", _update=True, cncid=2)
        self.assertTrue(join_select_result.has_changes())
        self.assertFalse(join_select_result.has_changes())
        # Добавить новую связку, которой не было в результатах ранее
        self.orm_manager.set_item(Cnc, name="testname", _insert=True, cncid=21)
        self.orm_manager.set_item(Machine, machinename="Somenamef", machineid=20, _insert=True, cncid=21)
        self.assertTrue(join_select_result.has_changes())
        self.assertFalse(join_select_result.has_changes())

    @drop_cache
    @db_reinit
    def test_has_new_entries(self):
        result = self.orm_manager.get_items(Numeration)
        self.assertFalse(result.has_new_entries())
        self.orm_manager.set_item(_model=Numeration, _insert=True)
        self.assertTrue(result.has_new_entries())
        self.assertFalse(result.has_new_entries())
        self.assertFalse(result.has_new_entries())
        self.assertFalse(result.has_new_entries())
        self.orm_manager.set_item(_model=Numeration, _insert=True, numerationid=2)
        other_result = self.orm_manager.get_items(_model=Machine)
        self.assertFalse(other_result)
        self.assertTrue(result.has_new_entries())
        self.assertFalse(result.has_new_entries())
        self.set_data_into_queue()
        self.assertTrue(other_result.has_new_entries())
        self.assertFalse(other_result.has_new_entries())
        self.assertTrue(result)
        self.set_data_into_database()
        self.assertFalse(other_result.has_new_entries())  # False потому что были станки с id с 1 по 4 в локальных нодах. Остались те же
        self.assertTrue(other_result.has_changes())  # Но изменения есть! Тк столбцы изменились
        self.assertFalse(other_result.has_changes())
        self.assertFalse(other_result.has_new_entries())

    @drop_cache
    @db_reinit
    def test_has_new_entries_join_select(self):
        result = self.orm_manager.join_select(Cnc, Machine, _on={"Cnc.cncid": "Machine.cncid"})
        self.assertFalse(result.has_new_entries())
        self.orm_manager.set_item(_model=Cnc, name="Test", _insert=True)
        self.orm_manager.set_item(_model=Machine, machinename="newmachine", _insert=True, machineid=1)
        # В тесте ниже возвращаемый результат - False, потому как, несмотря на то,
        # что мы добавили 2 записи, они не указывают друг на друга по внешнему ключу
        # и поэтому не затрагивают нашу выборку
        self.assertFalse(result.has_new_entries())
        # Добавим связь и убедимся, что результатом на наш запрос вернётся - True
        self.orm_manager.set_item(_model=Machine, machinename="newmachine", _update=True, cncid=1)
        self.assertTrue(result.has_new_entries())
        self.assertFalse(result.has_new_entries())
        self.orm_manager.set_item(_model=Cnc, name="Test_new", _insert=True)
        self.orm_manager.set_item(_model=Machine, machinename="othermachine", _insert=True, cncid=2)
        self.assertTrue(result.has_new_entries())
        self.assertFalse(result.has_new_entries())
        self.assertFalse(result.has_new_entries())
        # Разъединим связь machineid==1 и cncid=1 и убедимся, что появились изменения
        self.orm_manager.set_item(_model=Machine, machineid=1, _update=True, cncid=None)
        self.assertTrue(result.has_new_entries())
        self.set_data_into_database()
        self.assertTrue(result.has_new_entries())


class TestResultPointer(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        Tool.CACHE_LIFETIME_HOURS = 60
        self.orm_manager = Tool()

    @drop_cache
    @db_reinit
    def test_pointer(self):
        from itertools import repeat
        self.set_data_into_database()
        self.set_data_into_queue()
        result = self.orm_manager.get_items(Machine)
        with self.assertRaises(PointerRepeatedWrapper):
            result.pointer = list(repeat("any_str", 10))  # 1 или более повторяющихся элементов обёртки
        with self.assertRaises(PointerRepeatedWrapper):
            result.pointer = list(repeat("r", 2))  # 1 или более повторяющихся элементов обёртки
        with self.assertRaises(PointerRepeatedWrapper):
            result.pointer = list(repeat("any_s", 4))  # 1 или более повторяющихся элементов обёртки
        with self.assertRaises(PointerWrapperLengthError):
            result.pointer = ["Станок 1", "Станок 2", "Станок 3", "Станок 4", "Станок 5",
                              "Станок 6", "Станка 7 нету, этот лишний"]
        with self.assertRaises(PointerWrapperLengthError):
            result.pointer = ["Станок 1", "Станок 2"]  # Не хватает 4 элементов в списке!
        with self.assertRaises(PointerWrapperLengthError):
            result.pointer = list()
        with self.assertRaises(PointerWrapperTypeError):
            result.pointer = ""
        with self.assertRaises(PointerWrapperTypeError):
            result.pointer = 9
        with self.assertRaises(PointerWrapperTypeError):
            result.pointer = b"st"
        with self.assertRaises(PointerWrapperTypeError):
            result.pointer = 0
        result.pointer = ["Станок 1", "Станок 2", "Станок 3", "Станок 4", "Станок 5", "Станок 6"]  # GOOD Теперь
        # До тех пор, пока не появятся новые записи, или, пока не удалится одна/несколько/все из текущих,
        # Есть возможность удобного обращения через __getitem__!
        self.assertEqual(result.pointer.wrap_items, ["Станок 1", "Станок 2", "Станок 3", "Станок 4",
                                                     "Станок 5", "Станок 6"])
        self.assertIsInstance(result.pointer.items, dict)
        self.assertEqual(6, result.pointer.items.__len__())
        #
        # Пока мы ничего не изменяли столбцы(или не добавляли новые) отслеживаемых через Pointer() нод,
        # мы логично получим ответ False,
        # После вызова метода has_changes
        self.assertFalse(result.pointer.has_changes("Станок 1"))
        self.assertFalse(result.pointer.has_changes("Станок 3"))
        self.assertFalse(result.pointer.has_changes("Станок 5"))
        self.assertFalse(result.pointer.has_changes("Станок 2"))
        self.assertFalse(result.pointer.has_changes("Станок 6"))
        self.assertFalse(result.pointer.has_changes("Станок 4"))

        self.assertTrue(result.pointer.is_valid)
        # А теперь изменим сами(со стороны нашего ui) запись, которая ассоциируется со 'Станок 3'
        self.orm_manager.set_item(Machine, machineid=2, machinename="test",
                                  _update=True)
        self.assertTrue(result.pointer.is_valid)
        self.assertFalse(result.pointer.has_changes("Станок 2"))
        self.assertTrue(result.pointer.has_changes("Станок 3"))  # Как и ожидалось!!!
        # После появления в локальной очереди или базе данных новой записи
        self.assertFalse(result.pointer.has_changes("Станок 4"))
        self.assertFalse(result.pointer.has_changes("Станок 1"))
        self.orm_manager.set_item(_model=Machine, machinename="somenewmachinename", _insert=True)
        # Строкой выше мы добавили новую запись, поэтому данный pointer больше недействителен и станет закрыт для любого взаимодействия
        # Убедимся, что экземпляр pointer "перестал со мной сотрудничать"
        self.assertIsNone(result.pointer.items)
        self.assertFalse(result.pointer)
        self.assertEqual(0, result.pointer.__len__())
        self.assertIsNone(result.pointer.has_changes("Станок 1"))
        self.assertIsNone(result.pointer.has_changes("Станок 3"))
        self.assertIsNone(result.pointer.has_changes("Станок 2"))
        self.assertIsNone(result.pointer.has_changes("Станок 4"))
        with self.assertRaises(KeyError):  # А вот такого вообще не было в текущем wrapper
            self.assertIsNone(result.pointer.has_changes("Станок 9"))
        # С ним покончено((((
        # К счастью, текущий экземпляр result может получить новый pointer!, для этого
        # Нужно снова ассоциировать с сеттером pointer правильный кортеж(по длине) и содержимому без повторений
        result.pointer = ["Станок 1", "Станок 2", "Станок 3", "Станок 4", "Станок 5", "Станок 6", "Станок 7"]
        self.assertEqual(result.pointer["Станок 7"]["machinename"], "somenewmachinename")

    @drop_cache
    @db_reinit
    def test_join_select_pointer(self):
        """ Тестирование Pointer
        Pointer нужен для связывания данных на стороне UI с готовыми инструментами для повторного запроса на эти данные,
        тем самым перекладывая часть рутинной работы с UI на Tool.
        """
        self.set_data_into_database()
        self.set_data_into_queue()
        result = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"})
        result.order_by(Cnc, by_primary_key=True)
        result.pointer = ["Результат в списке 1", "Результат в списке 2", "Результат в списке 3", "Результат в списке 4"]
        #
        # Тест wrap_items
        #
        self.assertEqual(result.pointer.wrap_items, ["Результат в списке 1", "Результат в списке 2",
                                                     "Результат в списке 3", "Результат в списке 4"])
        #
        #  Тестировать refresh
        #
        self.assertFalse(result.pointer.has_changes("Результат в списке 1"))
        self.assertFalse(result.pointer.has_changes("Результат в списке 1"))
        #
        # Добавить изменения и проверить повторно
        self.orm_manager.set_item(cncid=1, name="nameeg", _model=Cnc, _update=True)
        self.orm_manager.set_item(_update=True, _model=Machine, machineid=2, xover=60)
        self.orm_manager.set_item(numerationid=2, endat=4, _model=Numeration, _update=True)
        self.orm_manager.set_item(_model=Comment, commentid=2, findstr="test_str_new", _update=True)
        self.orm_manager.set_item(_model=Machine, machinename="testnamesdfs", machineid=1, _update=True)
        #
        self.assertTrue(result.pointer.has_changes("Результат в списке 2"))
        self.assertRaises(KeyError, result.pointer.has_changes, "Не установленный во wrapper элемент")
        self.assertRaises(KeyError, result.pointer.has_changes, "Во wrapper этого не было")
        self.assertTrue(result.pointer.has_changes("Результат в списке 1"))
        self.assertRaises(KeyError, result.pointer.has_changes, "Не установленный во wrapper элемент")
        self.assertRaises(KeyError, result.pointer.has_changes, "Ещё Не установленный во wrapper элемент",)
        self.assertRaises(KeyError, result.pointer.has_changes, "Другой не установленный во wrapper элемент")
        self.assertFalse(result.pointer.has_changes("Результат в списке 1"))


class TestSliceMixin(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        Tool.CACHE_LIFETIME_HOURS = 60
        self.orm_manager = Tool()

    @drop_cache
    @db_reinit
    def test_slice(self):
        self.set_data_into_database()
        self.set_data_into_queue()
        result_obj = self.orm_manager.get_items(_model=Machine)
        result_obj.order_by(by_primary_key=True)
        result_obj[1:2]  # return None - OK
        result_obj.RESIDUAL_ITEM = "none"
        self.assertEqual(0, result_obj.__len__())
        result_obj.RESIDUAL_ITEM = "db"
        self.assertEqual(1, len(result_obj))




class LetterSort(unittest.TestCase):
    def test_init(self):
        LetterSortSingleNodes(Machine, "machinename", ServiceOrmContainer())
        LetterSortNodesChain(Cnc, "name", (ServiceOrmContainer(),))
        with self.assertRaises((TypeError, ValueError,)):
            LetterSortSingleNodes()
            LetterSortSingleNodes("field_n")
            LetterSortSingleNodes(4)
            LetterSortSingleNodes(None, None)
            LetterSortSingleNodes(b"0xe45")
            LetterSortSingleNodes("field", [])
            LetterSortSingleNodes("field", 5)
            LetterSortSingleNodes("field", b"23dfg")
            LetterSortSingleNodes("field", None)
            LetterSortSingleNodes("field", False)
            LetterSortSingleNodes("field", True)
            LetterSortSingleNodes("field", "we3rfasdf")
            LetterSortSingleNodes("field", 6.8)
            LetterSortSingleNodes(6, [ResultORMCollection()])
            LetterSortSingleNodes(None, [ResultORMCollection()])
            LetterSortSingleNodes("", [ResultORMCollection()])
            LetterSortSingleNodes("column", "str")
            LetterSortSingleNodes("column", [])
            LetterSortSingleNodes("column", b"0x245")
            LetterSortSingleNodes("column", {"1": True})
            LetterSortSingleNodes("column", ResultORMCollection(), [ResultORMCollection()])   
            LetterSortNodesChain()
            LetterSortNodesChain("field_n")
            LetterSortNodesChain(4)
            LetterSortNodesChain(None, None)
            LetterSortNodesChain(b"0xe45")
            LetterSortNodesChain("field", [])
            LetterSortNodesChain("field", 5)
            LetterSortNodesChain("field", b"23dfg")
            LetterSortNodesChain("field", None)
            LetterSortNodesChain("field", False)
            LetterSortNodesChain("field", True)
            LetterSortNodesChain("field", "we3rfasdf")
            LetterSortNodesChain("field", 6.8)
            LetterSortNodesChain(6, [ResultORMCollection()])
            LetterSortNodesChain(None, [ResultORMCollection()])
            LetterSortNodesChain("", [ResultORMCollection()])
            LetterSortNodesChain("column", "str")
            LetterSortNodesChain("column", [])
            LetterSortNodesChain("column", b"0x245")
            LetterSortNodesChain("column", {"1": True})
            LetterSortNodesChain("column", ResultORMCollection(), [ResultORMCollection()])
        with self.assertRaises(AttributeError):
            LetterSortSingleNodes(Machine, "str_er", ServiceOrmContainer())


class TestLettersSortSingleResult(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "Test", "machineid": 1},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "Name", "machineid": 4},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "NewTest", "machineid": 2
                 }, {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                  "_delete": False, "_create_at": datetime.datetime.now(),
                  "machinename": "Test4", "machineid": 3},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "Amacgdfg", "machineid": 8},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "ZName", "machineid": 419}]
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        self.test_result_collection = ServiceOrmContainer(data)

    def test_original_ordering(self):
        """ Убедимся, что ноды расположены в исходном порядке,- в том, в котором они были переданы при инициализации.
        Убедимся, что наш контейнер не перевирает очерёдность."""
        machine_names = ["Test", "Name", "NewTest", "Test4", "Amacgdfg", "ZName"]
        self.assertEqual([i["machinename"] for i in self.test_result_collection], machine_names)
        machine_id = [1, 4, 2, 3, 8, 419]
        self.assertEqual(list(map(lambda x: x["machineid"], self.test_result_collection)), machine_id)

    def test_sort_single_init(self):
        _ = LetterSortSingleNodes(Machine, "machinename", self.test_result_collection)

    def test_receive_invalid_instance(self):
        with self.assertRaises((TypeError, ValueError, InvalidModel, AttributeError)):
            LetterSortSingleNodes("field_name", Queue())
            LetterSortSingleNodes("field_name", object())
            LetterSortSingleNodes("field_name", Queue())
            LetterSortSingleNodes("field_name", 12)
            LetterSortSingleNodes("field_name", [1,2,5])
            LetterSortSingleNodes(4, Queue())
            LetterSortSingleNodes(Queue(), Queue())
            LetterSortSingleNodes(ResultORMCollection(), Queue())
            LetterSortSingleNodes(4, self.test_result_collection)
            LetterSortSingleNodes(["stry", "gd"], self.test_result_collection)
            LetterSortSingleNodes(["stry"], self.test_result_collection)
            LetterSortSingleNodes("field", ResultORMCollection())
            LetterSortSingleNodes(4, ResultORMCollection())
            LetterSortSingleNodes("field", 45)
            LetterSortSingleNodes("field", "34535")
            LetterSortSingleNodes(Cnc, "field", 45)
            LetterSortSingleNodes(HeadVarible, "field", "34535")
            LetterSortSingleNodes(Machine, "", ServiceOrmContainer())
            LetterSortSingleNodes(Machine, "undefined_column", ServiceOrmContainer())

    def test_original_ordering_is_rand(self):
        """ Убедимся, что исходное расположение не является верным ни для одного из вариантов сортировки,
         дабы избежать совпадения. """
        id_ = sorted([1, 4, 2, 3, 8, 419])
        id_decr = sorted(id_, reverse=True)
        lengths = [node["machinename"].__len__() for node in self.test_result_collection]
        self.assertNotEqual(lengths, sorted(lengths))
        self.assertNotEqual(lengths, sorted(lengths, reverse=True))
        self.assertNotEqual([node["machineid"] for node in self.test_result_collection], id_)
        self.assertNotEqual([node["machineid"] for node in self.test_result_collection], id_decr)
        machine_names_sorted_by_alphabet = ['Amacgdfg', 'Name', 'NewTest', 'Test', 'Test4', 'ZName']
        machine_names_sorted_by_alphabet_reversed = sorted(machine_names_sorted_by_alphabet, reverse=True)
        machine_names_sorted_by_length = ['Amacgdfg', 'NewTest', 'Test4', 'ZName', 'Test', 'Name']
        machine_names_sorted_by_length_reversed = ['Test', 'Name', 'Test4', 'ZName', 'NewTest', 'Amacgdfg']
        self.assertNotEqual([node["machinename"] for node in self.test_result_collection], machine_names_sorted_by_alphabet)
        self.assertNotEqual([node["machinename"] for node in self.test_result_collection], machine_names_sorted_by_alphabet_reversed)
        self.assertNotEqual([node["machinename"] for node in self.test_result_collection], machine_names_sorted_by_length)
        self.assertNotEqual([node["machinename"] for node in self.test_result_collection], machine_names_sorted_by_length_reversed)

    def test_alphabet_sort_decr(self):
        """ Тестировать сортировку по столбцу со строкой, на убывание.
        Сортировка производится по первой букве, согласно алфавитному порядку. """
        sorted_items = LetterSortSingleNodes(Machine, "machinename", self.test_result_collection, reverse=True)
        sorted_collection = sorted_items.sort_by_alphabet()
        self.assertEqual(['Amacgdfg', 'Name', 'NewTest', 'Test', 'Test4', 'ZName'],
                         [node["machinename"] for node in sorted_collection])

    def test_alphabet_sort_incr(self):
        """ Тестировать сортировку по столбцу со строкой, на убывание.
        Сортировка производится по первой букве, согласно алфавитному порядку. """
        sorted_items = LetterSortSingleNodes(Machine, "machinename", self.test_result_collection, reverse=False)
        sorted_collection = sorted_items.sort_by_alphabet()
        self.assertEqual(['ZName', 'Test', 'Test4', 'Name', 'NewTest', 'Amacgdfg'],
                         [node["machinename"] for node in sorted_collection])

    def test_sort_by_string_length_decr(self):
        valid_names = ('Amacgdfg', 'NewTest', 'Test4', 'ZName', 'Test', 'Name')
        sorted_items = LetterSortSingleNodes(Machine, "machinename", self.test_result_collection, reverse=True)
        sorted_collection = sorted_items.sort_by_string_length()
        self.assertEqual(valid_names, tuple(map(lambda node: node["machinename"], sorted_collection)))

    def test_sort_by_string_length_incr(self):
        valid_names = ('Test', 'Name', 'Test4', 'ZName', 'NewTest', 'Amacgdfg')
        sorted_items = LetterSortSingleNodes(Machine, "machinename", self.test_result_collection, reverse=False)
        sorted_collection = sorted_items.sort_by_string_length()
        self.assertEqual(tuple(map(lambda node: node["machinename"], sorted_collection)), valid_names)


class LetterSortJoinResult(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        pairs_data = [
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Some", "machineid": 5, "cncid": 5},
             {"cncid": 5, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "cndsf"}],
            [{"_model": Machine, "_ready": True, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Som54he", "machineid": 6, "cncid": 6},
             {"cncid": 6, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_update": True,
              "_create_at": datetime.datetime.now(), "name": "cncdsf"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Test12", "machineid": 2, "cncid": 2},
             {"cncid": 2, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "dsff454g"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Test12345g", "machineid": 3, "cncid": 3},
             {"cncid": 3, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_update": True,
              "_create_at": datetime.datetime.now(), "name": "cnc657"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "SomeTest12", "machineid": 4, "cncid": 4},
             {"cncid": 4, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "dsf"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Test", "machineid": 1, "cncid": 1},
             {"cncid": 1, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "name"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "TestMachineName", "machineid": 7, "cncid": 7},
             {"cncid": 7, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "Aname"}]
        ]
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        self.joined_data = []
        for pair in pairs_data:
            self.joined_data.append(ServiceOrmContainer(pair))
        self.joined_data = tuple(self.joined_data)

    def test_original_ordering_is_rand(self):
        """ Убедимся, что исходное расположение не является верным ни для одного из вариантов сортировки,
         дабы избежать совпадения. """
        id_ = list(range(1, 8))
        id_rev = id_.copy()
        id_rev.reverse()

        def id_gen(column: str):
            for group in self.joined_data:
                for node in group:
                    if node.get_primary_key_and_value(only_key=True) == column:
                        yield node[column]
        self.assertNotEqual(list(id_gen("cncid")), id_)
        self.assertNotEqual(list(id_gen("cncid")), id_rev)
        self.assertNotEqual(list(id_gen("machineid")), id_)
        self.assertNotEqual(list(id_gen("machineid")), id_rev)
        # todo

    def test_alphabet_sort_decr(self):
        """ Тестировать сортировку по столбцу со строкой, на убывание.
        Сортировка производится по первой букве, согласно алфавитному порядку. """
        instance = LetterSortNodesChain(Cnc, "name", self.joined_data, reverse=True)
        sorted_ = instance.sort_by_alphabet()
        self.assertEqual([n["Cnc"]["name"] for n in sorted_], ['Aname', 'cndsf', 'cncdsf', 'cnc657', 'dsff454g', 'dsf', 'name'])
        self.assertTrue(all(map(lambda x: len(x) == 2, sorted_)))
        self.assertTrue(all(map(lambda joined_item: joined_item["Cnc"]["cncid"] == joined_item["Machine"]["cncid"], sorted_)))

    def test_alphabet_sort_incr(self):
        """ Тестировать сортировку по столбцу со строкой, на возрастание.
        Сортировка производится по первой букве, согласно алфавитному порядку. """
        instance = LetterSortNodesChain(Cnc, "name", self.joined_data, reverse=False)
        sorted_ = instance.sort_by_alphabet()
        self.assertEqual(['name', 'dsff454g', 'dsf', 'cndsf', 'cncdsf', 'cnc657', 'Aname'], [n["Cnc"]["name"] for n in sorted_])
        self.assertTrue(all(map(lambda x: len(x) == 2, sorted_)))
        self.assertTrue(all(map(lambda joined_item: joined_item["Cnc"]["cncid"] == joined_item["Machine"]["cncid"], sorted_)))

    def test_sort_by_string_length_decr(self):
        instance = LetterSortNodesChain(Cnc, "name", self.joined_data, reverse=True)
        sorted_ = instance.sort_by_string_length()
        self.assertEqual([node["Cnc"]["name"] for node in sorted_], ['dsff454g', 'cnc657', 'Aname', 'name', 'dsf'])
        self.assertTrue(all(map(lambda x: len(x) == 2, sorted_)))
        self.assertTrue(all(map(lambda joined_item: joined_item["Cnc"]["cncid"] == joined_item["Machine"]["cncid"], sorted_)))

    def test_sort_by_string_length_incr(self):
        instance = LetterSortNodesChain(Cnc, "name", self.joined_data, reverse=False)
        sorted_ = instance.sort_by_string_length()
        self.assertEqual([node["Cnc"]["name"] for node in sorted_], ['dsf', 'name', 'Aname', 'cnc657', 'dsff454g'])
        self.assertTrue(all(map(lambda x: len(x) == 2, sorted_)))
        self.assertTrue(all(map(lambda joined_item: joined_item["Cnc"]["cncid"] == joined_item["Machine"]["cncid"], sorted_)))


class TestNumberSort(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "Test", "machineid": 1},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "Name", "machineid": 4},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "NewTest", "machineid": 2
                 }, {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                  "_delete": False, "_create_at": datetime.datetime.now(),
                  "machinename": "Test4", "machineid": 3},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "Amacgdfg", "machineid": 8},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(),
                 "machinename": "ZName", "machineid": 419}]
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        self.single_result_collection = ServiceOrmContainer(data)
        pairs_data = [
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Some", "machineid": 5, "cncid": 5},
             {"cncid": 5, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "cndsf"}],
            [{"_model": Machine, "_ready": True, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Som54he", "machineid": 6, "cncid": 6},
             {"cncid": 6, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_update": True,
              "_create_at": datetime.datetime.now(), "name": "cncdsf"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Test12", "machineid": 2, "cncid": 2},
             {"cncid": 2, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "dsff454g"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Test12345g", "machineid": 3, "cncid": 3},
             {"cncid": 3, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_update": True,
              "_create_at": datetime.datetime.now(), "name": "cnc657"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "SomeTest12", "machineid": 4, "cncid": 4},
             {"cncid": 4, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "dsf"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "Test", "machineid": 1, "cncid": 1},
             {"cncid": 1, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "name"}],
            [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
              "_delete": False, "_create_at": datetime.datetime.now(),
              "machinename": "TestMachineName", "machineid": 7, "cncid": 7},
             {"cncid": 7, "_model": Cnc, "commentsymbol": "%", "_ready": True, "_insert": True,
              "_create_at": datetime.datetime.now(), "name": "Aname"}]
        ]
        ServiceOrmContainer.LinkedListItem = ServiceOrmItem
        self.joined_data = []
        for pair in pairs_data:
            self.joined_data.append(ServiceOrmContainer(pair))
        self.joined_data = tuple(self.joined_data)

    def test_init(self):
        NumberSortSingleNodes(Machine, "machineid", ServiceOrmContainer())
        NumberSortNodesChain(Cnc, "cncid", (ServiceOrmContainer(),))
        with self.assertRaises((TypeError, ValueError,)):
            NumberSortSingleNodes(Machine, "machinename", ServiceOrmContainer())
            NumberSortSingleNodes()
            NumberSortSingleNodes("field_n")
            NumberSortSingleNodes(4)
            NumberSortSingleNodes(None, None)
            NumberSortSingleNodes(b"0xe45")
            NumberSortSingleNodes("field", [])
            NumberSortSingleNodes("field", 5)
            NumberSortSingleNodes("field", b"23dfg")
            NumberSortSingleNodes("field", None)
            NumberSortSingleNodes("field", False)
            NumberSortSingleNodes("field", True)
            NumberSortSingleNodes("field", "we3rfasdf")
            NumberSortSingleNodes("field", 6.8)
            NumberSortSingleNodes(6, [ResultORMCollection()])
            NumberSortSingleNodes(None, [ResultORMCollection()])
            NumberSortSingleNodes("", [ResultORMCollection()])
            NumberSortSingleNodes("column", "str")
            NumberSortSingleNodes("column", [])
            NumberSortSingleNodes("column", b"0x245")
            NumberSortSingleNodes("column", {"1": True})
            NumberSortSingleNodes("column", ResultORMCollection(), [ResultORMCollection()])
            NumberSortNodesChain()
            NumberSortNodesChain("field_n")
            NumberSortNodesChain(4)
            NumberSortNodesChain(None, None)
            NumberSortNodesChain(b"0xe45")
            NumberSortNodesChain("field", [])
            NumberSortNodesChain("field", 5)
            NumberSortNodesChain("field", b"23dfg")
            NumberSortNodesChain("field", None)
            NumberSortNodesChain("field", False)
            NumberSortNodesChain("field", True)
            NumberSortNodesChain("field", "we3rfasdf")
            NumberSortNodesChain("field", 6.8)
            NumberSortNodesChain(6, [ResultORMCollection()])
            NumberSortNodesChain(None, [ResultORMCollection()])
            NumberSortNodesChain("", [ResultORMCollection()])
            NumberSortNodesChain("column", "str")
            NumberSortNodesChain("column", [])
            NumberSortNodesChain("column", b"0x245")
            NumberSortNodesChain("column", {"1": True})
            NumberSortNodesChain("column", ResultORMCollection(), [ResultORMCollection()])
        with self.assertRaises(AttributeError):
            NumberSortSingleNodes(Machine, "str_er", ServiceOrmContainer())

    def test_original_ordering(self):
        """ Убедимся, что ноды расположены в исходном порядке,- в том, в котором они были переданы при инициализации.
        Убедимся, что наш контейнер не перевирает очерёдность."""
        machine_names = ["Test", "Name", "NewTest", "Test4", "Amacgdfg", "ZName"]
        self.assertEqual([i["machinename"] for i in self.single_result_collection], machine_names)
        machine_id = [1, 4, 2, 3, 8, 419]
        self.assertEqual(list(map(lambda x: x["machineid"], self.single_result_collection)), machine_id)
        id_ = list(range(1, 8))
        id_rev = id_.copy()
        id_rev.reverse()

        def id_gen(column: str):
            for group in self.joined_data:
                for node in group:
                    if node.get_primary_key_and_value(only_key=True) == column:
                        yield node[column]
        self.assertNotEqual(list(id_gen("cncid")), id_)
        self.assertNotEqual(list(id_gen("cncid")), id_rev)
        self.assertNotEqual(list(id_gen("machineid")), id_)
        self.assertNotEqual(list(id_gen("machineid")), id_rev)

    def test_sort_single_result_items_incr(self):
        instance = NumberSortSingleNodes(Machine, "machineid", self.single_result_collection, reverse=False)
        sorter_elems = instance.sort()
        self.assertEqual([n["machineid"] for n in sorter_elems], [1, 2, 3, 4, 8, 419])

    def test_sort_single_result_items_decr(self):
        instance = NumberSortSingleNodes(Machine, "machineid", self.single_result_collection, reverse=True)
        sorter_elems = instance.sort()
        self.assertEqual([n["machineid"] for n in sorter_elems], [419, 8, 4, 3, 2, 1])

    def test_sort_group_result_items_incr(self):
        instance = NumberSortNodesChain(Machine, "machineid", self.joined_data, reverse=False)
        elems = instance.sort()
        self.assertEqual([n["Machine"]["machineid"] for n in elems], [1, 2, 3, 4, 5, 6, 7])

    def test_sort_group_result_items_decr(self):
        instance = NumberSortNodesChain(Machine, "machineid", self.joined_data, reverse=True)
        elems = instance.sort()
        self.assertEqual([n["Machine"]["machineid"] for n in elems], [7, 6, 5, 4, 3, 2, 1])


class TestSortSingleResultMixin(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        drop_db()
        create_db()
        init_all_triggers(DATABASE_PATH)
        Tool.CACHE_LIFETIME_HOURS = 60
        self.orm_manager = Tool()
        self.orm_manager.connection.drop_cache()
        self.set_data_into_database()
        self.set_data_into_queue()

    def test_sort_by_primary_key(self):
        query = self.orm_manager.get_items(_model=Machine, _db_only=True)
        query.order_by(by_primary_key=True, decr=False)
        self.assertEqual([i["machineid"] for i in query], [1, 2, 3, 4])
        query.order_by(by_primary_key=True, decr=True)
        self.assertEqual([i["machineid"] for i in query], [4, 3, 2, 1])


class TestSortJoinResultMixin(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        drop_db()
        create_db()
        init_all_triggers(DATABASE_PATH)
        Tool.CACHE_LIFETIME_HOURS = 60
        self.orm_manager = Tool()
        self.orm_manager.connection.drop_cache()
        self.set_data_into_database()
        self.set_data_into_queue()

    def test_sort_by_primary_key(self):
        query = self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}, _db_only=True, _use_join=True)
        query.order_by(Machine, by_primary_key=True)
        query.order_by(Machine, by_primary_key=True, decr=False)
        self.assertEqual([i["Machine"]["machineid"] for i in query], [1, 2, 3, 4])
        query.order_by(Machine, by_primary_key=True, decr=True)
        self.assertEqual([i["Machine"]["machineid"] for i in query], [4, 3, 2, 1])
