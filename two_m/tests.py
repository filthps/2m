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
                                  outputcatalog=r"C:\Test", _insert=True)
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
                                  {"node3_val": 4}, {"node4_val": 5}])
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

    def test_init(self):
        Queue()
        queue = Queue()
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Test", "machineid": 1},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Name", "machineid": 4},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "NewTest", "machineid": 2
                 }]
        Queue(data)
        # invalid primary key
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Test", "machineid": {}},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Name", "machineid": 4},
                {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "NewTest", "machineid": int
                 }]
        with self.assertRaises(NodeColumnValueError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Test", "machineid": ""}]
        with self.assertRaises(NodeColumnValueError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Test", "machineid": "12"}]
        with self.assertRaises(NodeColumnValueError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Test", "machineid": None}]
        with self.assertRaises(NodePrimaryKeyError):
            Queue(data)
        data = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Test"}]
        with self.assertRaises(NodePrimaryKeyError):
            Queue(data)
        # повторение столбца c unique constraint - machinename

    def test_enqueue(self):
        queue = Queue()
        data__len_3 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
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
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test", "xover": 10, "machineid": 3},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test", "yover": 10, "machineid": 3},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test", "zover": 10, "machineid": 3
                        }]
        [queue.enqueue(**data__len_1[i]) for i in range(len(data__len_1))]
        self.assertEqual(queue.__len__(), 1)
        #  Проверить, что новые данные, которые добавлялись за 3 итерации, вошли в результирующую ноду
        self.assertEqual(len(set(queue[0].value).intersection(set({"xover": 10, "yover": 10, "zover": 10}))), 3)
        queue = Queue()
        data_with_primary_key_from_ui = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                          "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                                          "machinename": "FirstTest", "xover": 10, "machineid": 3},
                                         {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                          "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                                          "machinename": "Test", "yover": 10, "machineid": 3},
                                         {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                          "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
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
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test", "machineid": 3},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
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
        self.assertEqual(queue.dequeue().value["machinename"], "Test1")
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
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
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


class TestResultORMCollection(unittest.TestCase):
    def setUp(self) -> None:
        Tool.CACHE_PATH = CACHE_PATH
        Tool.DATABASE_PATH = DATABASE_PATH
        queue = Queue()
        data__len_3 = [{"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test", "machineid": 1},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                        "machinename": "Test1", "machineid": 2},
                       {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                        "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
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
                                "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                                "machinename": "Tdfgdfgerest", "machineid": 1},
                               {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                                "machinename": "Test1", "machineid": 2},
                               {"_model": Machine, "_ready": False, "_insert": False, "_update": True,
                                "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
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
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "machinename": "Test", "cncid": 1, "machineid": 1},
                {"_model": Cnc, "_ready": False, "_insert": False, "_update": True,
                 "_delete": False, "_create_at": datetime.datetime.now(), "_container": queue,
                 "name": "Testcnc", "cncid": 1},
                {"_model": OperationDelegation, "replaceid": 1,
                 "_create_at": datetime.datetime.now(), "_container": queue, "_insert": True, "opid": str(uuid4())},
                {"_model": Replace, "replaceid": 1, "findstr": "trststr",
                 "_create_at": datetime.datetime.now(), "_container": queue, "_insert": True}
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
        self.assertEqual(self.orm_manager.connection.cache.get("ORMItems"), self.orm_manager.connection.items)
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
                                  inputcatalog=r"C:\\wdfg", outputcatalog=r"D:\\hfghfgh")
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
    def test_join_select(self):
        # Добавить в базу и кеш данные
        self.set_data_into_database()
        self.set_data_into_queue()
        # Возвращает ли метод экземпляр класса JoinSelectResult?
        self.assertIsInstance(self.orm_manager.join_select(Machine, Cnc, _on={"Cnc.cncid": "Machine.cncid"}), JoinSelectResult)
        # GOOD (хороший случай)
        # Найдутся ли записи с pk равными значениям, которые мы добавили
        # Machine - Cnc
        result = self.orm_manager.join_select(Machine, Cnc, _on={"Machine.cncid": "Cnc.cncid"})
        self.assertEqual("Newcnc", result.items[0]["Cnc"]["name"])
        self.assertEqual("Tesm", result.items[0]["Machine"]["machinename"])
        self.assertEqual("Ram", result.items[1]["Cnc"]["name"])
        self.assertEqual("Fidia", result.items[1]["Machine"]["machinename"])
        self.assertNotEqual(result.items[0]["Cnc"]["cncid"], result.items[1]["Cnc"]["cncid"])
        self.assertEqual(result.items[0]["Cnc"]["cncid"], result.items[0]["Machine"]["cncid"])
        #
        # Numeration - Operationdelegation
        #
        result = self.orm_manager.join_select(OperationDelegation, Numeration,
                                              _on={"OperationDelegation.numerationid": "Numeration.numerationid"})
        self.assertEqual("Нумерация. Добавил сразу в БД", result.items[0]["OperationDelegation"]["operationdescription"])
        self.assertNotEqual("Нумерация. Добавил сразу в БД", result.items[1]["OperationDelegation"]["operationdescription"])
        self.assertEqual("Нумерация кадров", result.items[1]["OperationDelegation"]["operationdescription"])
        self.assertEqual(result.items[0]["Numeration"]["numerationid"], 1)
        self.assertEqual(269, result.items[1]["Numeration"]["endat"])
        #
        # Comment - OperationDelegation
        #
        result = self.orm_manager.join_select(Comment, OperationDelegation, _on={"Comment.commentid": "OperationDelegation.commentid"})
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
        local_data = self.orm_manager.join_select(Machine, Cnc, _on={"Machine.cncid": "Cnc.cncid"}, _queue_only=True)
        database_data = self.orm_manager.join_select(Cnc, Machine, _on={"Cnc.cncid": "Machine.cncid"}, _db_only=True)
        self.assertEqual(local_data.items[0]["Machine"]["cncid"], local_data.items[0]["Cnc"]["cncid"])
        self.assertEqual(database_data.items[0]["Cnc"]["cncid"], database_data.items[0]["Machine"]["cncid"])
        self.assertIn("machineid", local_data.items[0]["Machine"])
        self.assertIn("machineid", database_data.items[0]["Machine"])
        self.assertNotEqual(local_data.items[0]["Machine"]["machinename"], database_data.items[0]["Machine"]["machinename"])
        self.assertEqual("Tesm", local_data.items[1]["Machine"]["machinename"])
        self.assertEqual("Ram", local_data.items[0]["Cnc"]["name"])
        self.assertNotEqual(local_data.items[0]["Cnc"]["name"], database_data.items[0]["Cnc"]["name"])
        #
        # Comment - OperationDelegation
        #
        local_data = self.orm_manager.join_select(Comment, OperationDelegation, _on={"Comment.commentid": "OperationDelegation.commentid"}, _queue_only=True)
        database_data = self.orm_manager.join_select(Comment, OperationDelegation, _on={"Comment.commentid": "OperationDelegation.commentid"}, _db_only=True)
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
        self.assertRaises(InvalidModel, self.orm_manager.join_select, "str", object())
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
    def test_join_select_merge(self):
        self.set_data_into_database()
        self.set_data_into_queue()
        result = self.orm_manager.join_select(Machine, Cnc, _on={"Machine.cncid": "Cnc.cncid"})
        # Провокационный момент:
        # Ставим в столбец отношения внешнего ключа значение, чьего PK не существует
        # тогда будет взята связка из базы данных! с прежним pk-fk
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=9, _update=True)
        self.assertEqual(2, result.__len__())
        self.assertEqual(result.items[0]["Cnc"]["cncid"], 1, result.items[0]["Machine"]["cncid"])
        self.assertEqual(result.items[0]["Cnc"]["name"], "NC210")  # Из базы
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Heller")  # Тоже из базы
        self.orm_manager.set_item(Machine, machinename="Heller", cncid=2, _update=True)  # найдётся по столбцу machinename, потому что (unique constraint)
        # Изначально было 2 связки, но так как 1 разрушили, то осталась всего одна
        self.assertEqual(1, result.__len__())
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Heller")
        self.assertEqual(result.items[0]["Cnc"]["name"], "Ram")
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=1, _update=True)
        # Восстановили связь, теперь снова 2 связки в результатах
        self.assertEqual(2, result.__len__())
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Heller")
        self.assertEqual(result.items[0]["Cnc"]["name"], "Newcnc")
        # Нарушить связь PK - FK
        self.orm_manager.set_item(_model=Machine, machineid=1, cncid=9, _update=True)
        self.assertEqual(result.items[0]["Machine"]["machinename"], "Heller")  # Теперь убедимся, что видим результат из базы данных,
        # потому как cncid == 9 не существует ни в результатах из базы, ни в локальных результатах
        self.assertEqual(result.items[0]["Cnc"]["name"], "NC210")

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
        self.set_data_into_queue()
        self.set_data_into_database()
        join_select_result = self.orm_manager.join_select(Machine, Cnc, _on={"Machine.machineid": "Cnc.cncid"})
        #  Первый запрос has_changes всегда вернёт None
        self.assertFalse(join_select_result.has_changes())  # Для всей выборки результатов (не указан хеш)
        invalid_hash = 34535566543  # Совершенно постороннее значение, взятое с потолка
        self.assertIsNone(join_select_result.has_changes(invalid_hash))  # Для всей выборки результатов (не указан хеш)
        self.update_exists_items()
        self.assertTrue(join_select_result.has_changes())
        self.assertFalse(join_select_result.has_changes())
        self.assertFalse(join_select_result.has_changes())
        val_from_0 = join_select_result.items[0].__hash__()
        val_from_1 = hash(join_select_result.items[1])
        self.orm_manager.set_item(_model=Cnc, name="name_n", _update=True, cncid=1)
        self.assertTrue(join_select_result.has_changes())
        self.orm_manager.set_item(_model=Machine, _update=True, machinename="name", machineid=1)
        self.orm_manager.set_item(_model=Cnc, name="name_n", _update=True, commentsymbol="#")
        self.orm_manager.set_item(_model=Cnc, name="naаке", _update=True, cncid=2)
        self.orm_manager.set_item(_model=Cnc, name="naаке", _update=True, cncid=1)
        self.assertTrue(join_select_result.has_changes(val_from_0))
        self.assertTrue(join_select_result.has_changes(val_from_1))
        self.assertFalse(join_select_result.has_changes())
        self.assertFalse(join_select_result.has_changes(val_from_1))
        self.assertFalse(join_select_result.has_changes(val_from_0))
        invalid_hash_1 = 345352340678  # Совершенно постороннее значение, взятое с потолка
        self.assertIsNone(join_select_result.has_changes(hash_value=invalid_hash_1))  # Для всей выборки результатов (не указан хеш)

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
        self.assertTrue(result.has_new_entries())
        self.assertFalse(result.has_new_entries())

    @drop_cache
    @db_reinit
    def test_has_new_entries_join_select(self):
        result = self.orm_manager.join_select(Cnc, Machine, _on={"Machine.cncid": "Cnc.cncid"})
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
            result.pointer = ["Станок 1", "Станок 2", "Станок 3", "Станок 4", "Станка 5 нету этот лишний"]
        with self.assertRaises(PointerWrapperLengthError):
            result.pointer = ["Станок 1", "Станок 2"]  # Не хватает 2 элементов в списке!
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
        result.pointer = ["Станок 1", "Станок 2", "Станок 3", "Станок 4"]  # GOOD Теперь
        # До тех пор, пока не появятся новые записи, или, пока не удалится одна/несколько/все из текущих,
        # Есть возможность удобного обращения через __getitem__!
        self.assertEqual(result.pointer.wrap_items, ["Станок 1", "Станок 2", "Станок 3", "Станок 4"])
        self.assertIsInstance(result.pointer.items, dict)
        self.assertEqual(4, result.pointer.items.__len__())
        #
        # Пока мы ничего не изменяли столбцы(или не добавляли новые) отслеживаемых через Pointer() нод,
        # мы логично получим ответ False,
        # После вызова метода has_changes
        self.assertFalse(result.pointer.has_changes("Станок 1"))
        self.assertFalse(result.pointer.has_changes("Станок 3"))
        self.assertFalse(result.pointer.has_changes("Станок 2"))
        self.assertFalse(result.pointer.has_changes("Станок 4"))
        self.assertTrue(result.pointer.is_valid)
        # А теперь изменим сами(со стороны нашего ui) первую запись, которая ассоциируется со 'Станок 1'
        self.orm_manager.set_item(Machine, machineid=2, machinename="test",
                                  _update=True)
        self.assertTrue(result.pointer.is_valid)
        self.assertFalse(result.pointer.has_changes("Станок 3"))
        self.assertFalse(result.pointer.has_changes("Станок 2"))
        self.assertFalse(result.pointer.has_changes("Станок 4"))
        self.assertTrue(result.pointer.has_changes("Станок 1"))  # Как и ожидалось!!!
        # После появления в локальной очереди или базе данных новой записи
        self.orm_manager.set_item(_model=Machine, machinename="somenewmachinename", _insert=True)
        # Экземпляр станет недействителен и станет закрыт для любого взаимодействия
        # Убедимся, что экземпляр pointer "перестал со мной сотрудничать"
        self.assertIsNone(result.pointer.items)
        self.assertFalse(result.pointer)
        self.assertEqual(0, result.pointer.__len__())
        self.assertIsNone(result.pointer.has_changes("Станок 1"))
        self.assertIsNone(result.pointer.has_changes("Станок 3"))
        self.assertIsNone(result.pointer.has_changes("Станок 2"))
        self.assertIsNone(result.pointer.has_changes("Станок 4"))
        with self.assertRaises(KeyError):  # А вот такого вообще не было в текущем wrapper
            self.assertIsNone(result.pointer.has_changes("Станок 5"))
        # С ним покончено((((
        # К счастью, текущий экземпляр result может получить новый pointer!, для этого
        # Нужно снова ассоциировать с сеттером pointer правильный кортеж(по длине) и содержимому без повторений
        result.pointer = ["Станок 1", "Станок 2", "Станок 3", "Станок 4", "Станок 5"]
        self.assertEqual(result.pointer["Станок 5"]["machinename"], "somenewmachinename")

    @drop_cache
    @db_reinit
    def test_join_select_pointer(self):
        """ Тестирование Pointer
        Pointer нужен для связывания данных на стороне UI с готовыми инструментами для повторного запроса на эти данные,
        тем самым перекладывая часть рутинной работы с UI на Tool.
        """
        self.set_data_into_database()
        self.set_data_into_queue()
        result = self.orm_manager.join_select(Machine, Cnc, _on={"Machine.cncid": "Cnc.cncid"})
        result.pointer = ["Результат в списке 1", "Результат в списке 2"]
        #
        # Тест wrap_items
        #
        self.assertEqual(result.pointer.wrap_items, ["Результат в списке 1", "Результат в списке 2"])
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

"""  not supported - ver 1.
class TestQueueOrderBy(unittest.TestCase, SetUp):
    def setUp(self) -> None:
        Tool.CACHE_LIFETIME_HOURS = 60
        self.orm_manager = Tool()

    @db_reinit
    @drop_cache
    def test_order_by_field__alphabet(self):
        self.set_data_into_database()
        self.set_data_into_queue()
        result = self.orm_manager.get_items(Machine)
        # Передача правильных параметров
        result.order_by(by_create_time=True, alphabet=True)
        result.order_by(by_column_name="machinename", length=True)
        result.order_by(by_primary_key=True, alphabet=True)
        result.order_by(by_create_time=True, decr=True, alphabet=True)
        result.order_by(by_column_name="machinename", decr=True, length=True)
        result.order_by(by_primary_key=True, decr=True, length=True)
        result.order_by(by_create_time=True, decr=False, length=True)
        result.order_by(by_column_name="machinename", decr=False, alphabet=True)
        result.order_by(by_primary_key=True, decr=False, length=True)
        # Передача неправильных параметров
        self.assertRaises(ValueError, result.order_by)
        self.assertRaises(TypeError, result.order_by, by_create_time=4)
        self.assertRaises(TypeError, result.order_by, by_create_time="strf")
        self.assertRaises(ValueError, result.order_by, by_create_time=None)
        self.assertRaises(TypeError, result.order_by, by_create_time=8.9)
        self.assertRaises(TypeError, result.order_by, by_create_time=datetime.datetime.now())
        self.assertRaises(TypeError, result.order_by, by_create_time=b"0x43")
        self.assertRaises(TypeError, result.order_by, by_create_time=0)
        self.assertRaises(TypeError, result.order_by, by_primary_key=4)
        self.assertRaises(TypeError, result.order_by, by_primary_key="strf")
        self.assertRaises(ValueError, result.order_by, by_primary_key=None)
        self.assertRaises(TypeError, result.order_by, by_primary_key=8.9)
        self.assertRaises(TypeError, result.order_by, by_primary_key=datetime.datetime.now())
        self.assertRaises(TypeError, result.order_by, by_primary_key=b"0x43")
        self.assertRaises(TypeError, result.order_by, by_primary_key=0)
        self.assertRaises(TypeError, result.order_by, by_column_name=4)
        self.assertRaises(TypeError, result.order_by, by_column_name=True)
        self.assertRaises(TypeError, result.order_by, by_column_name=False)
        self.assertRaises(ValueError, result.order_by, by_column_name=None)
        self.assertRaises(TypeError, result.order_by, by_column_name=8.9)
        self.assertRaises(TypeError, result.order_by, by_column_name=datetime.datetime.now())
        self.assertRaises(TypeError, result.order_by, by_column_name=b"0x43")
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=4)
        self.assertRaises(TypeError, result.order_by, by_create_time=True, decr=None)
        self.assertRaises(TypeError, result.order_by, by_primary_key=True, decr=6.8)
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr="teststr")
        self.assertRaises(ValueError, result.order_by, by_column_name="machinename", decr=True)
        self.assertRaises(ValueError, result.order_by, by_column_name="machinename", decr=True, length=True, alphabet=True)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, length="123", alphabet=True)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, length=True, alphabet=3)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, length=True, alphabet=None)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, length=True, alphabet=9.7)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, length=True, alphabet=0)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, length=0, alphabet=0)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, alphabet="123", length=True)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, alphabet=True, length=3)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, alphabet=True, length=None)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, alphabet=True, length=9.7)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, alphabet=True, length=0)
        self.assertRaises((TypeError, ValueError), result.order_by, by_column_name="machinename", decr=True, alphabet=0, length=0)
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, length="123")
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=0)
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=None)
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=6)
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=0.7)
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=b'')
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=b'0x3')
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=[])
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=tuple())
        self.assertRaises(TypeError, result.order_by, by_column_name="machinename", decr=True, alphabet=object())
        #
        # Проверка соответствия результатов
        #
        # Сортировка по алфавиту  todo
        ...

        # Сортировка по длине строки значения todo

    @drop_cache
    @db_reinit
    def test_order_by_time(self):
        self.set_data_into_database()
        self.set_data_into_queue()
        container = self.orm_manager.connection.items
        container.order_by(Machine, by_create_time=True)
        print(container.search_nodes(Machine))
"""
