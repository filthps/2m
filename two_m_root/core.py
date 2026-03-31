"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import threading
import warnings
from typing import Union, Iterator, Iterable, Optional, Literal, Type
from collections import ChainMap
from pymemcache.client.base import PooledClient
from sqlalchemy import create_engine, text, CursorResult, or_, and_
from sqlalchemy.sql.expression import func, desc, join, select
from sqlalchemy.sql.dml import Insert, Update, Delete
from sqlalchemy.orm import sessionmaker as session_factory, scoped_session
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from two_m_root.conf import RESERVED_WORDS, CustomModel
from two_m_root.dill.serde import DillSerde
from two_m_root.containers import Queue, ServiceOrmContainer
from two_m_root.nodes import QueueItem, ServiceOrmItem
from two_m_root.mixins import SliceResultMixin
from two_m_root.tools import ModelTools
from two_m_root.database.postgres.exceptions import DatabaseException
from two_m_root.exceptions import NodePrimaryKeyError, NodeColumnError, NodeColumnValueError, NodeAttributeError, \
    ORMInitializationError
from two_m.main import MEMCACHE_PATH, DATABASE_PATH, RELEASE_INTERVAL_SECONDS, CACHE_LIFETIME_HOURS, \
    MAX_RETRIES, INCOMING_DATA_VALIDATION_LEVEL, \
    IGNORE_NODE_PRIMARY_KEY_ERROR, DATABASE_CONNECTION_ALIVE_SEC, CACHE_CONNECTION_ALIVE_SEC


class ConnectionManager:
    """ Данный класс отвечает за установку соединений с базой данных и кеширующим сервером """
    DATABASE_CONNECTION_ALIVE_SEC = DATABASE_CONNECTION_ALIVE_SEC
    CACHE_CONNECTION_ALIVE_SEC = CACHE_CONNECTION_ALIVE_SEC
    _database_connection_timer = None
    _cache_connection_timer = None
    _database_session = None
    _cache_client = None

    def __new__(cls):
        super().__new__(cls)
        engine = create_engine(DATABASE_PATH)
        cls.session_f = session_factory(bind=engine)
        cls.cache_pool = PooledClient(MEMCACHE_PATH, max_pool_size=20, serde=DillSerde)
        return cls

    @classmethod
    @property
    def cache(cls):
        if not cls._cache_client:
            cls._cache_client = cls.__create_cache_connection()
            cls.__start_timer_cache_connection()
        return cls._cache_client

    @classmethod
    def drop_cache(cls):
        cls.cache.flush_all()

    @classmethod
    @property
    def items(cls) -> Queue:
        """ Вернуть локальные элементы """
        return cls.cache.get("ORMItems", Queue())

    @classmethod
    @property
    def database(cls):
        if not cls._database_session:
            cls._database_session = cls.__create_database_connection()
            cls.__start_timer_database_connection()
        return cls._database_session

    @classmethod
    def __create_cache_connection(cls):
        return cls.cache_pool

    @classmethod
    def __create_database_connection(cls):
        return scoped_session(cls.session_f)

    @classmethod
    def __start_timer_database_connection(cls):
        cls._database_connection_timer = threading.Timer(float(cls.DATABASE_CONNECTION_ALIVE_SEC),
                                                         cls.__close_connection_database)
        cls._database_connection_timer.start()

    @classmethod
    def __start_timer_cache_connection(cls):
        cls._cache_connection_timer = threading.Timer(float(cls.CACHE_CONNECTION_ALIVE_SEC), cls.__close_connection_cache)
        cls._cache_connection_timer.start()

    @classmethod
    def __close_connection_database(cls):
        cls._database_session.remove()
        cls._database_session = None

    @classmethod
    def __close_connection_cache(cls):
        cls._cache_client.close()


class Tool(ModelTools):
    """
    Главный класс
    1) Инициализация
        LinkToObj = Tool()
    2) Установка ссылки на класс модели Flask-SqlAlchemy
        LinkToObj.set_model(CustomModel)
    3) Использование
        LinkToObj.set_item(name, data, **kwargs) - Установка в очередь, обнуление таймера
        LinkToObj.get_item(name, **kwargs) - получение данных из бд и из нод в локальном расположении
        LinkToObj.get_items(model=None) - получение данных из бд и из нод в локальном расположении
        LinkToObj.release() - высвобождение очереди с попыткой сохранить объекты в базе данных
        в случае неудачи нода переносится в конец очереди
        LinkToObj.remove_items - принудительное изъятие ноды из очереди.
    """
    RELEASE_INTERVAL_SECONDS = RELEASE_INTERVAL_SECONDS
    CACHE_LIFETIME_HOURS = CACHE_LIFETIME_HOURS
    _timer: Optional[threading.Timer] = None
    _model_obj: Optional[Type[CustomModel]] = None  # Текущий класс модели, присваиваемый автоматически всем экземплярам при добавлении в очередь
    _was_initialized = False
    connection = ConnectionManager()

    @classmethod
    def set_model(cls, obj):
        """
        :param obj: Кастомный класс модели Flask-SQLAlchemy из модуля models
        """
        cls.is_valid_model_instance(obj)
        cls._model_obj = obj
        cls._is_valid_config()
        return cls

    @classmethod
    def set_item(cls, _model=None, _insert=False, _update=False,
                 _delete=False, _ready=False, **value):
        model = _model or cls._model_obj
        if isinstance(_model, str):
            model = ModelTools.import_model(_model)
        cls.is_valid_model_instance(model)
        items: Queue = cls.connection.items
        attrs = {"_model": model, "_ready": _ready,
                 "_insert": _insert, "_update": _update,
                 "_delete": _delete}
        actual_node_data = NodeDataManager.sync_node_data(**{**value, **attrs})
        if not actual_node_data:
            return
        attrs.update(actual_node_data)
        items.enqueue(**attrs)
        cls.__set_cache(items)
        cls._timer = cls._init_timer()

    @classmethod
    def get_items(cls, _model: Optional[Type[CustomModel]] = None, _db_only=False, _queue_only=False, **attrs) -> "Result":
        """
        1) Получаем запись из таблицы в виде словаря (CustomModel.query.all())
        2) Получаем данные из кеша, все элементы, у которых данная модель
        3) db_data.update(quque_data)
        """
        from two_m_root.result import Result
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)

        def select_from_db(left_border=0, right_border=float("inf"),
                           int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                           by_length=False, by_alphabet=False, by_create_time=False,
                           reversed_=False, model_in_sort=model):
            cls.__is_valid_params__data_getter(left_border=left_border, right_border=right_border,
                                               int_sort=int_sort, string_sort=string_sort,
                                               by_length=by_length, by_alphabet=by_alphabet, by_create_time=by_create_time,
                                               reversed_=reversed_, model_in_sort=model_in_sort)
            if left_border == right_border:
                return ServiceOrmContainer()
            try:
                items_db = cls.connection.database.query(model)
                if int_sort or string_sort:
                    if reversed_:
                        items_db = items_db.order_by(desc(int_sort or string_sort))
                    else:
                        items_db = items_db.order_by(int_sort or string_sort)
                if by_create_time:
                    items_db = items_db.order_by("_create_at")
                if left_border:
                    items_db = items_db.offset(left_border)
                if right_border and not right_border == float("inf"):
                    items_db = items_db.limit(right_border)
            except OperationalError:
                print("Ошибка соединения с базой данных! Смотри константу 'DATABASE_PATH' в модуле models.py, "
                      "такая проблема обычно возникает из-за авторизации. Смотри пароль!!!")
                raise OperationalError
            if attrs:
                items_db = items_db.filter_by(**attrs)
            return cls.__add_single_db_result_to_queue(items_db.all(), model)

        def select_from_cache(left_border=0, right_border=float("inf"),
                              int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                              by_length=False, by_alphabet=False, by_create_time=False,
                              reversed_=False, model_in_sort=None):
            cls.__is_valid_params__data_getter(left_border=left_border, right_border=right_border,
                                               int_sort=int_sort, string_sort=string_sort,
                                               by_length=by_length, by_alphabet=by_alphabet, by_create_time=by_create_time,
                                               reversed_=reversed_, model_in_sort=model_in_sort)
            if left_border == right_border:
                return ServiceOrmContainer()
            nodes = cls.connection.items.search_nodes(model, **attrs)
            left_border, right_border = SliceResultMixin.change_slice_value_on_items_length(nodes, left_border, right_border)
            output = ServiceOrmContainer()
            [output.append(node_item=n) for n in nodes[left_border:right_border]]
            return output
        return Result(get_nodes_from_database=select_from_db, get_local_nodes=select_from_cache,
                      only_local=_queue_only, only_database=_db_only, model=model, where=attrs)

    @classmethod
    def join_select(cls, *models: Iterable[CustomModel], _on: Optional[dict] = None,
                    _db_only=False, _queue_only=False, _use_join=True, **where) -> "JoinSelectResult":
        """
        join_select(model_a, model,b, on={model_b.column_name: 'model_a.column_name'}).
        В ключах таблицы с внешними ключами, в values таблицы с первичными ключами.
        :param where: modelName: {column_name: some_val}
        :param _on: modelName.column1: modelName2.column2
        :param _db_only: извлечь только sql inner join
        :param _queue_only: извлечь только из queue
        :param _use_join: использовать select с join или использовать подзапросы
        :return: специальный итерируемый объект класса JoinSelectResult, который содержит смешанные данные из локального
        хранилища и БД
        """
        from two_m_root.result import JoinSelectResult

        def is_valid():
            if sum((_db_only, _queue_only,)) not in [0, 1]:
                raise ValueError
            if not _on:
                raise ValueError
            [cls.is_valid_model_instance(model) for model in models]
            models_map = {model.__name__: model for model in models}
            for left, right in _on.items():
                left_model, foreign_key = left.split(".")
                right_model, primary_key = right.split(".")
                if not left_model or not right_model or not foreign_key or not primary_key:
                    raise ValueError
                if left_model not in models_map:
                    raise ValueError
                if right_model not in models_map:
                    raise ValueError
                cls.is_exists_column(models_map[left_model], foreign_key)
                cls.is_exists_column(models_map[right_model], primary_key)
                if not cls.get_primary_key_column_name(models_map[left_model]) == primary_key:
                    raise ValueError
                if foreign_key not in cls.get_foreign_key_columns(models_map[right_model]):
                    raise ValueError
            if type(_use_join) is not bool:
                raise TypeError
        is_valid()
        tables = {table.__name__: table for table in models}

        def collect_db_data(left_border=0, right_border=float("inf"),
                            int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                            by_length=False, by_alphabet=False, by_create_time=False,
                            reversed_=False, model_in_sort=None):
            """
            Функция для извлечения данных из базы данных
            :param left_border: Левая часть среза результата
            :param right_border: Правая часть среза результата
            :param int_sort: Сортировка значения с целым числом (False или строка-наименование столбца)
            :param string_sort: Сортировка строки (False или строка-наименование столбца)
            :param by_length: Сортировать строку по длине
            :param by_alphabet: Сортировать строку в алфавитном порядке
            :param by_create_time: Сортировать результат по столбцу даты создания
            :param reversed_: Сортировать на убывание или возрастание
            :param model_in_sort: Таблица, по столбцу которой происходит сортировка
            """
            def create_request() -> str:
                """ Создать строку, содержащую запрос, которую можно будет запустить """
                if _use_join:
                    select_text = f"select(*models)"
                    for model in models:  # O(n)
                        if not model.__name__ == models[0].__name__:
                            select_text += f".join({model.__name__})"
                    select_text += ".filter("
                    for u, data in enumerate(_on.items()):  # O(n)
                        left, right = data
                        left_table, pk_column = tuple(left.split("."))[0], tuple(left.split("."))[1]
                        right_table, fk_column = tuple(right.split("."))[0], tuple(right.split("."))[1]
                        select_text += f"{right_table}.{fk_column} == {left_table}.{pk_column}"
                        if u < len(_on) - 1:
                            select_text += ", "
                    select_text += ")"
                else:
                    select_text = "select(*models).filter("
                    for index, data in enumerate(_on.items()):
                        left_table_dot_field, right_table_dot_field = data
                        left_table, fk_column = left_table_dot_field.split(".")
                        right_table, pk_column = right_table_dot_field.split(".")
                        select_text += f"{left_table}.{fk_column} " \
                            f"== {right_table}.{pk_column}"
                        if not index == len(_on) - 1:
                            select_text += ", "
                    select_text += ")"
                select_text += f".offset({left_border})" if left_border else ""
                select_text += f".limit({right_border})" if right_border and not right_border == float("inf") else ""
                if where:
                    select_text += f".where("
                    for i, table_column_value in enumerate(where.items()):
                        table_name_column_name, value = table_column_value
                        table_name, column_name = table_name_column_name.split(".")
                        value = f"'{value}'" if not value.isdigit() else value
                        select_text += f"{table_name}.{column_name} == {value}"
                        if i < len(where) - 1:
                            select_text += ", "
                    select_text += ")"
                if string_sort:
                    select_text += ".order_by("
                    if by_length:
                        if reversed_:
                            select_text += "desc("
                        select_text += f"func.length({model_in_sort.__name__}.{string_sort})"
                        if reversed_:
                            select_text += ")"
                    if by_alphabet:
                        if reversed_:
                            select_text += "desc("
                        select_text += f"{model_in_sort.__name__}.{string_sort}"
                        if reversed_:
                            select_text += ")"
                    select_text += ")"
                if int_sort:
                    select_text += ".order_by("
                    if reversed_:
                        select_text += "desc("
                    select_text += f"{model_in_sort.__name__}.{int_sort}"
                    if reversed_:
                        select_text += ")"
                    select_text += ")"
                if by_create_time:
                    select_text += ".order_by("
                    if reversed_:
                        select_text += "desc("
                    select_text += f"{model_in_sort.__name__}._create_at"
                    if reversed_:
                        select_text += ")"
                    select_text += ")"
                return f"db.execute({select_text})"

            def add_joined_db_items_to_orm_queue(query: CursorResult) -> Iterator[ServiceOrmContainer]:
                for data_row in query.mappings().all():  # O(i)
                    row = ServiceOrmContainer()
                    for model in models:
                        current_node_data = {}
                        for table_name, instance in data_row.items():
                            if not instance.__tablename__ == model.__tablename__:
                                continue
                            total_column_names = tables[table_name]().column_names
                            [current_node_data.update({column_name: instance.__dict__[column_name]})
                             for column_name in total_column_names]
                        if current_node_data:
                            row.append(_model=model, _insert=True, **current_node_data)  # O(l)
                    yield row
            cls.__is_valid_params__data_getter(left_border=left_border, right_border=right_border,
                                               int_sort=int_sort, string_sort=string_sort,
                                               by_length=by_length, by_alphabet=by_alphabet, by_create_time=by_create_time,
                                               reversed_=reversed_, model_in_sort=model_in_sort)
            sql_text = create_request()
            query: CursorResult = eval(sql_text, {
                "db": cls.connection.database,
            }, {
                **ChainMap(*tuple(map(lambda x: {x.__name__: x}, models))),
                "models": models,
                "select": select, "desc": desc,
                "join": join, "text": text
            })
            return tuple(add_joined_db_items_to_orm_queue(query))

        def collect_all_local_nodes():
            heap = Queue()
            temp = cls.connection.items
            for model in models:  # O(n)
                heap += temp.search_nodes(model, **where.get(model.__name__, {}))
            return heap

        def collect_node_values(on_keys_or_values: Union[dict.keys, dict.values], null_values=False):
            for node in collect_all_local_nodes():
                for table_and_column in on_keys_or_values:
                    table, table_column = table_and_column.split(".")
                    if table == node.model.__name__:
                        if not null_values:
                            if table_column in node.value:
                                yield node.model.__name__, node, table_column
                        if null_values:
                            if table_column not in node.value:
                                continue
                            if node.value[table_column] is None:
                                yield node.model.__name__, node, table_column

        def collect_local_data(left_border=0, right_border=float("inf"),
                               int_sort: Union[bool, str] = False, string_sort: Union[bool, str] = False,
                               by_length=False, by_alphabet=False, by_create_time=False,
                               reversed_=False, model_in_sort=None) -> Iterator[ServiceOrmContainer]:
            def compare_by_matched_fk() -> Iterator:
                """ Получить полноценные связки нод [PK - FK]. """
                model_left_primary_key_and_value = collect_node_values(_on.keys())  # O(u)
                model_right_primary_key_and_value = tuple(collect_node_values(_on.values()))  # O(2u)
                for left_data in model_left_primary_key_and_value:  # O(n)
                    left_model_name, left_node, *_ = left_data  # O(j)
                    for right_data in model_right_primary_key_and_value:  # O(k)
                        right_model_name, right_node, *_ = right_data  # O(l)
                        raw = ServiceOrmContainer()  # O(1)
                        for left_table_dot_field, right_table_dot_field in _on.items():  # O(b)
                            left_table_name_in_on, left_table_field_in_on = left_table_dot_field.split(".")  # O(a)
                            right_table_name_in_on, right_table_field_in_on = right_table_dot_field.split(".")  # O(a)
                            if left_model_name == left_table_name_in_on and right_model_name == right_table_name_in_on:  # O(c * v) + O(c1 * v1)
                                if left_node.value.get(left_table_field_in_on, None) == \
                                        right_node.value.get(right_table_field_in_on, None):  # O(1) + O(m1) * O(1) + O(m2) = O(m1 * m2)
                                    raw.append(**left_node.get_attributes())
                                    raw.append(**right_node.get_attributes())
                        if raw:
                            yield raw
            cls.__is_valid_params__data_getter(left_border=left_border, right_border=right_border,
                                               int_sort=int_sort, string_sort=string_sort,
                                               by_length=by_length, by_alphabet=by_alphabet, by_create_time=by_create_time,
                                               reversed_=reversed_, model_in_sort=model_in_sort)
            output = tuple(compare_by_matched_fk())
            left_border, right_border = SliceResultMixin.change_slice_value_on_items_length(output, left_border, right_border)
            return output[left_border:right_border]

        def get_local_nodes_without_foreign_key_nodes():
            """ Ноды, которые не найдены в локальном расположении.
             1) Получить ноды, чей внешний ключ указывает на ноду с первичным ключом, которого нету в локальны элементах.
             2) Сделать запрос в базу данных, попытаться найти такую ноду там.
             3) Те ноды, которые не удалось найти в базе данных, присвоить им предыдущую связь, если такая есть.
             """
            def search_data() -> Iterator:
                all_nodes = collect_all_local_nodes()
                for left, right in _on.items():
                    right_table, fk_column = right.split(".")
                    left_table, pk_column = left.split(".")
                    for fk_node in all_nodes.search_nodes(tables[right_table], **{fk_column: "*"}):
                        if fk_node[fk_column] is None:
                            continue
                        if not all_nodes.search_nodes(tables[left_table], **{pk_column: fk_node.value[fk_column]}):
                            yield left_table, pk_column, right_table, fk_column, fk_node.value[fk_column]

            def group_by_table_name(data: Iterator):
                """ Подготовить данные для запросов в базу данных.
                Сгруппировать разрозненные данные по ключам словаря, чтобы избежать избыточных запросов """
                result = {}
                for table_name, column, *_, value in data:
                    model_data = result.get(table_name, {})
                    if not model_data:
                        result.update({table_name: model_data})
                    column_value = model_data.get(column, [])
                    if not column_value:
                        model_data.update({column: column_value})
                    column_value.append(value)
                return result

            def create_request(data: dict[dict[list]]) -> Iterator:
                for model_name, column_data in data.items():
                    where = []
                    for column_name, values in column_data.items():
                        s = str(tuple(values))[:-2]
                        s += ")"
                        where.append(f"{column_name} IN {s}")
                    s = f"SELECT * FROM {tables[model_name].__tablename__} WHERE "
                    s += ", ".join(where)
                    request_data = cls.connection.database.execute(text(s.lower())).mappings().all()
                    item_or_items = cls.__add_single_db_result_to_queue(request_data, tables[model_name])
                    yield model_name, item_or_items

            def create_output_collection(items_from_database: dict):
                all_local_nodes = collect_all_local_nodes()
                for left_table, primary_key, right_table, foreign_key, value in search_data():
                    container = ServiceOrmContainer()
                    left_node = all_local_nodes.search_nodes(tables[right_table], **{primary_key: value})
                    if left_node:
                        left_node = left_node[0]
                        left_node = ServiceOrmItem(**left_node.get_attributes())
                    if left_node is not None:
                        container.append(node_item=left_node)
                        if any(items_from_database.values()):
                            right_node = items_from_database[left_table].search_nodes(tables[left_table], **{foreign_key: value})
                            if right_node:
                                right_node = right_node[0]
                                container.append(node_item=right_node)
                    else:
                        continue
                    yield left_node, container
            request_data = search_data()
            request_data = group_by_table_name(request_data)
            node_data = dict(create_request(request_data))
            output_collection = create_output_collection(node_data)
            return output_collection

        def get_local_nodes_with_null_fk():
            """ Получить все ноды, у которых значения столбцов внешних ключей - null. """
            data = ServiceOrmContainer()
            for _, node, *_ in collect_node_values(_on.values(), null_values=True):
                data.append(node_item=node)
            return data
        return JoinSelectResult(models=models, only_database=_db_only, only_local=_queue_only,
                                get_nodes_from_database=collect_db_data, get_local_nodes=collect_local_data,
                                get_all_local_nodes=collect_all_local_nodes,
                                get_nodes_from_local_with_null_fk=get_local_nodes_with_null_fk,
                                get_local_nodes_without_foreign_key_nodes=get_local_nodes_without_foreign_key_nodes,
                                on=_on
                                )

    @classmethod
    def get_node_dml_type(cls, node_pk_value: Union[str, int], model=None) -> Optional[str]:
        """ Получить тип операции с базой, например '_update', по названию ноды, если она найдена, иначе - None
        :param node_pk_value: значение поля первичного ключа
        :param model: кастомный объект, смотри модуль database/models
        """
        model = model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(node_pk_value, (str, int,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        left_node = cls.connection.items.get_node(model, **{primary_key_field_name: node_pk_value})
        return left_node.type if left_node is not None else None

    @classmethod
    def remove_items(cls, node_or_nodes: Union[Union[int, str], Iterable[Union[str, int]]], model=None):
        """
        Удалить ноду из очереди на сохранение
        :param node_or_nodes: значение для поля первичного ключа, одно или несколько
        :param model: кастомный объект, смотри модуль database/models
        """
        model = model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(node_or_nodes, (tuple, list, set, frozenset, str, int,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        items = cls.connection.items
        if isinstance(node_or_nodes, (str, int,)):
            items.remove(model, **{primary_key_field_name: node_or_nodes})
        if isinstance(node_or_nodes, (tuple, list, set, frozenset)):
            for pk_field_value in node_or_nodes:
                if not isinstance(pk_field_value, (int, str,)):
                    raise TypeError
                items.remove(model, **{primary_key_field_name: pk_field_value})
        cls.__set_cache(items)

    @classmethod
    def remove_field_from_node(cls, pk_field_value, field_or_fields: Union[Iterable[str], str], _model=None):
        """
        Удалить поле или поля из ноды, которая в очереди
        :param pk_field_value: значения поля первичного ключа (по нему ищется нода)
        :param field_or_fields: изымаемые поля
        :param _model: кастомная модель SQLAlchemy
        """
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)
        if not isinstance(field_or_fields, (tuple, list, set, frozenset, str,)):
            raise TypeError
        primary_key_field_name = ModelTools.get_primary_key_column_name(model)
        old_node = cls.connection.items.get_node(model, **{primary_key_field_name: pk_field_value})
        if not old_node:
            return
        node_data = old_node.get_attributes()
        if isinstance(field_or_fields, (list, tuple, set, frozenset)):
            if set.intersection(set(field_or_fields), set(RESERVED_WORDS)):
                raise NodeAttributeError
            if primary_key_field_name in field_or_fields:
                raise NodePrimaryKeyError("Нельзя удалить поле, которое является первичным ключом")
            for field in field_or_fields:
                if field in node_data:
                    del node_data[field]
        if type(field_or_fields) is str:
            if field_or_fields in RESERVED_WORDS:
                raise NodeAttributeError
            if primary_key_field_name == field_or_fields:
                raise NodePrimaryKeyError("Нельзя удалить поле, которое является первичным ключом")
            if field_or_fields in node_data:
                del node_data[field_or_fields]
        container = cls.connection.items
        container.enqueue(**node_data)
        cls.__set_cache(container)

    @classmethod
    def is_node_from_cache(cls, _model=None, **attrs) -> bool:
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)
        items = cls.connection.items.search_nodes(model, **attrs)
        if len(items) > 1:
            warnings.warn(f"Нашлось больше одной ноды/нод, - {len(items)}: {items}")
        if items:
            return True
        return False

    @classmethod
    def is_node_ready(cls, _model=None, **attrs):
        model = _model or cls._model_obj
        cls.is_valid_model_instance(model)
        items = cls.connection.items.search_nodes(model, **attrs)
        if not items:
            return
        if not len(items) == 1:
            return
        return items[0].ready

    @classmethod
    def release(cls):
        """
        Этот метод стремится высвободить очередь сохраняемых объектов,
        путём итерации по ним, и попыткой сохранить в базу данных.
        :return: None
        """
        def group_nodes_by_table_names(nodes: Queue) -> dict[str, Queue]:
            result = {}
            for node in nodes:
                current_nodes = result.get(node.model.__name__, None)
                if current_nodes is None:
                    t = type(nodes)()
                    t.append(**node.get_attributes())
                    result.update({node.model.__name__: t})
                else:
                    current_nodes.append(**node.get_attributes())
            return result
        database_adapter = SQLAlchemyQueryManager(cls.connection.items)
        database_adapter.start()
        if not database_adapter.remaining_nodes:
            return
        new_queue = Queue()
        for model_name, node_group in group_nodes_by_table_names(database_adapter.remaining_nodes).items():
            [new_queue.append(ModelTools.import_model(model_name), **data)
             for data in NodeDataManager.sync_node_data_many(model_name, node_group)]
        cls.__set_cache(new_queue)

    @classmethod
    def _init_timer(cls):
        if cls._timer:
            cls._timer.cancel()
        timer = threading.Timer(cls.RELEASE_INTERVAL_SECONDS, cls.release)
        timer.daemon = False
        timer.setName("Tool(database push queue)")
        timer.start()
        return timer

    @classmethod
    def _is_valid_config(cls):
        if type(cls.CACHE_LIFETIME_HOURS) is not int and type(cls.CACHE_LIFETIME_HOURS) is not bool:
            raise TypeError
        if not isinstance(cls.RELEASE_INTERVAL_SECONDS, (int, float,)):
            raise TypeError
        if cls.CACHE_LIFETIME_HOURS <= cls.RELEASE_INTERVAL_SECONDS:
            raise ORMInitializationError("Срок жизни кеша, который хранит очередь сохраняемых объектов не может быть меньше, "
                                         "чем интервал отправки объектов в базу данных.")
        cls._was_initialized = True

    @staticmethod
    def __is_valid_params__data_getter(left_border=0, right_border=float("inf"),
                                       int_sort=False, string_sort=False,
                                       by_length=False, by_alphabet=False, by_create_time=False,
                                       reversed_=False, model_in_sort=None):
        """ Валидация параметров для функций, берущих данные. Эти функции ищи внутри методов get_items, join_select. """
        if type(left_border) is not int:
            raise TypeError
        if left_border < 0:
            raise ValueError("Левая граница среза не может быть отрицательной")
        if not isinstance(right_border, (float, int,)):
            raise TypeError
        if type(right_border) is float:
            if not right_border == float("inf"):
                raise ValueError("Правая граница среза может принадлежать к типу float, если ")
        if not isinstance(int_sort, (bool, str)):
            raise TypeError
        if type(int_sort) is str:
            if not int_sort:
                raise ValueError("Не может быть пустой строки")
        if type(int_sort) is bool:
            if int_sort:
                raise ValueError("Нужно передать строку-наименование столбца, "
                                 "по которому будет производиться сортировка")
        if not isinstance(string_sort, (bool, str,)):
            raise TypeError
        if type(string_sort) is str:
            if not string_sort:
                raise ValueError("Не может быть пустой строки")
        if type(string_sort) is bool:
            if string_sort:
                raise ValueError("Нужно передать строку-наименование столбца, "
                                 "по которому будет производиться сортировка")
        if not type(by_create_time) is bool:
            raise TypeError
        if sum(map(lambda x: bool(x), (int_sort, string_sort, by_create_time,))) not in (0, 1,):
            raise ValueError("Нужно использовать только один из этих вариантов: "
                             "int_sort, string_sort, by_create_time")
        if type(by_length) is not bool:
            raise TypeError
        if type(by_alphabet) is not bool:
            raise TypeError
        if not int_sort and not by_create_time and not string_sort:
            return
        if string_sort:
            if not sum((by_length, by_alphabet,)) == 1:
                raise ValueError
        if type(reversed_) is not bool:
            raise TypeError
        if model_in_sort is None:
            raise TypeError("Таблица, по которой происходит сортировка - необходима")
        else:
            ModelTools.is_valid_model_instance(model_in_sort)

    @classmethod
    def __set_cache(cls, nodes):
        if type(nodes) is not Queue:
            raise TypeError
        cls.connection.cache.set("ORMItems", nodes, cls.CACHE_LIFETIME_HOURS)

    @staticmethod
    def __detect_primary_key(model, value: dict):
        pk = None
        for column_name, attrs_dict in model().column_names.items():
            if attrs_dict["primary_key"]:
                pk = column_name
                break
        if pk in value:
            return {pk: value[pk]}

    @staticmethod
    def __add_single_db_result_to_queue(items_db: Iterable[dict], model: CustomModel):
        """ Упаковать один или несколько результатов select к одной таблице в соответствующий контейнер. """
        result = ServiceOrmContainer()
        [result.append(**{key: item.__dict__[key] for key in getattr(item.__class__, "column_names")}, _model=model)
         for item in items_db]
        return result


class SQLAlchemyQueryManager:
    MAX_RETRIES: Union[int, Literal["no-limit"]] = MAX_RETRIES

    def __init__(self, nodes: "Queue"):
        def valid_node_type():
            if type(nodes) is not Queue:
                raise ValueError
        if not isinstance(nodes, Queue):
            raise TypeError
        valid_node_type()
        self._node_items = nodes
        self.remaining_nodes = Queue()  # Отложенные для следующей попытки
        self._sorted: list[Queue] = []  # [[save_point_group {pk: val,}], [save_point_group]...]
        self._query_objects: dict[Union[Insert, Update, Delete]] = {}  # {node_index: obj}

    def start(self):
        self._sort_nodes()  # Упорядочить, разбить по savepoint
        self._manage_queries()  # Обратиться к left_node.make_query, - собрать объекты sql-инъекций
        self._push()

    def _manage_queries(self):
        if self._query_objects:
            return self._query_objects
        for node_grop in self._sort_nodes():
            for left_node in node_grop:
                query = left_node.make_query()
                self._query_objects.update({left_node.index: query}) if query is not None else None
        return self._query_objects

    def _push(self):
        sorted_data = self._sort_nodes()
        if not sorted_data:
            return
        if not self._query_objects:
            return
        session = Tool.connection.database
        while sorted_data:
            node_group = sorted_data.pop()
            if not node_group:
                break
            multiple_items_in_transaction = True if len(node_group) > 1 else False
            if multiple_items_in_transaction:
                point = session.begin_nested()
            else:
                point = session
            items_to_commit = []
            for node in node_group:
                dml = self._query_objects.get(node.index)
                items_to_commit.append(dml)
            if multiple_items_in_transaction:
                try:
                    session.add_all(items_to_commit)
                except SQLAlchemyError:
                    self.remaining_nodes += node_group
                    point.rollback()
            else:
                try:
                    session.execute(items_to_commit.pop(0))
                except SQLAlchemyError:
                    self.remaining_nodes += node_group
            try:
                session.commit()
            except SQLAlchemyError:
                self.remaining_nodes += node_group
            except DatabaseException:
                self.remaining_nodes += node_group
        self._sorted = []
        self._query_objects = {}

    def _sort_nodes(self) -> list[Queue]:
        """ Сортировать ноды по признаку внешних ключей, определить точки сохранения для транзакций """
        def make_sort_container(all_nodes: Queue, n: QueueItem, linked_nodes: Queue):
            """
            Рекурсивно искать ноды с внешними ключами
            O(m) * (O(n) + O(j)) = O(n) * O(m) = O(n)
            """
            related_nodes = all_nodes.get_related_nodes(n)  # O(n)
            linked_nodes.add_to_head(node_item=n) if n.ready else None
            if not related_nodes:
                return linked_nodes
            [make_sort_container(all_nodes, node, linked_nodes) for node in related_nodes]
        if self._sorted:
            return self._sorted
        node_ = self._node_items.dequeue()
        while node_:
            if self.MAX_RETRIES == "no-limit" or node_.retries < self.MAX_RETRIES:
                if node_.ready:
                    recursion_result = make_sort_container(self._node_items, node_, Queue())
                    self._sorted.append(recursion_result)
                else:
                    self.remaining_nodes.append(**node_.get_attributes())
            node_ = self._node_items.dequeue()
        return self._sorted


class NodeDataManager(ModelTools):
    """ Актуализация данных, содержащихся внутри нод. Синхронизация с данными извне. """
    IGNORE_NODE_PRIMARY_KEY_ERROR = IGNORE_NODE_PRIMARY_KEY_ERROR  # Возбуждать или не возбуждать исключение,
    # если не удалось определить значение первичного ключа для будущей ноды. В противном случае вернуть пустой словарь,
    # вместо данных
    INCOMING_DATA_VALIDATION_LEVEL = INCOMING_DATA_VALIDATION_LEVEL  # Игнорировать попытку установить в ноду несуществующий столбец,
    # или сделать валидацию более строгой
    connection = ConnectionManager()

    @classmethod
    def sync_node_data(cls, _model=None, _insert=False, _update=False, _delete=False, **data) -> dict:
        """ Синхронизация данных нод из внешних расположений для одной ноды. """
        cls.is_valid_model_instance(_model)
        if type(data) is not dict:
            raise TypeError
        if type(cls.IGNORE_NODE_PRIMARY_KEY_ERROR) is not bool:
            raise TypeError
        data.update({"_insert": _insert, "_update": _update, "_delete": _delete})
        cls.__check_node_data(_model, **data)
        data, is_exist = cls._update_node_data_from_database_by_unique_column(_model, data)
        data = cls._update_node_data_from_local_nodes_by_unique_column(_model, data)
        if is_exist:
            if not _delete:
                cls.__change_dml(data, update=True)
            return data
        primary_key = cls.get_primary_key_column_name(_model)
        pk_from_received_data = cls._select_primary_key_value_from_node_data(_model, data)
        if pk_from_received_data is not None:
            data, is_exist = cls._update_node_data_from_database_by_pk(_model, pk_from_received_data, data)
            data = cls._update_node_data_from_local_by_pk(_model, {primary_key: pk_from_received_data}, data)
            if is_exist:
                if not _delete:
                    cls.__change_dml(data, update=True)
            return data
        if _update:
            if pk_from_received_data is None:
                if not cls.IGNORE_NODE_PRIMARY_KEY_ERROR:
                    raise NodePrimaryKeyError
        if _insert:
            if cls.is_autoincrement_primary_key(_model):
                pk_value_db = cls._get_highest_autoincrement_pk_from_database(_model) or 0
                pk_value_local = cls._get_highest_autoincrement_pk_from_local(_model) or 0
                data.update({primary_key: pk_value_local + pk_value_db + 1})
                return data
            default_value = cls.get_default_column_value_or_function(_model, primary_key)
            if default_value is not None:
                data.update({primary_key: default_value.arg(None)})
                return data
        if not cls.IGNORE_NODE_PRIMARY_KEY_ERROR:
            raise NodePrimaryKeyError
        if cls.INCOMING_DATA_VALIDATION_LEVEL == "strong":
            exist_model_column_names = _model().column_names
            for column_name, value in cls.__clear_node_data(_model, data).items():
                if column_name not in exist_model_column_names:
                    raise NodeColumnError(column_name)
                if not isinstance(value, ModelTools.get_column_python_type(_model, column_name)):
                    raise NodeColumnValueError
        return {}

    @classmethod
    def sync_node_data_many(cls, model, data: list[dict]) -> Iterator[dict]:
        """ Синхронизация данных нод из внешних расположений для одной ноды.
        Все ноды должны принадлежать одной и той же таблице. """
        def remove_item_from_data_if_in_exist_items(items, exist_items, primary_key):
            for i, current_data in enumerate(items):
                pk = current_data.get(primary_key, None)
                if pk is None:
                    continue
                if pk in exist_items:
                    yield current_data
                    del items[i]
        if not cls.is_valid_model_instance(model):
            raise TypeError
        if type(data) is not list:
            raise TypeError
        [cls.__check_node_data(model, **n) for n in data]
        if type(cls.IGNORE_NODE_PRIMARY_KEY_ERROR) is not bool:
            raise TypeError
        primary_key_column_name = cls.get_primary_key_column_name(model)
        data, exist_item_pk = cls._update_node_data_from_database_by_unique_column_multiple(model, data)
        cls._update_node_data_from_local_by_unique_column_multiple(model, data)
        for n in remove_item_from_data_if_in_exist_items(data, exist_item_pk, primary_key_column_name):
            if n["_insert"]:
                cls.__change_dml(n, update=True)
            yield n
        if cls.is_autoincrement_primary_key(model):
            autoincrement_value_db = cls._get_highest_autoincrement_pk_from_database(model)
            autoincrement_value_local = cls._get_highest_autoincrement_pk_from_local(model)
            current_pk_counter = autoincrement_value_db + autoincrement_value_local + 1
            for i, node_data in enumerate(data):
                if not node_data["_insert"]:
                    continue
                if node_data[primary_key_column_name] in range(current_pk_counter - 1):
                    cls.__change_dml(node_data, update=True)
                else:
                    node_data[primary_key_column_name] = current_pk_counter
                    current_pk_counter += 1
                yield node_data
                del data[i]
        data, exist_item_pk = cls._update_node_data_from_database_by_pk_multiple(model, data)
        for n in remove_item_from_data_if_in_exist_items(data, exist_item_pk, primary_key_column_name):
            if n["_insert"]:
                cls.__change_dml(n, update=True)
            yield n
        sql_procedure = cls.get_default_column_value_or_function(model, primary_key_column_name)
        for node_data in data:
            value = node_data.get(primary_key_column_name, None)
            if value is None:
                if node_data["_update"]:
                    cls.__change_dml(node_data, insert=True)
                if sql_procedure is None:
                    if cls.IGNORE_NODE_PRIMARY_KEY_ERROR:
                        continue
                    raise NodePrimaryKeyError
                node_data[primary_key_column_name] = sql_procedure.arg(None)
            else:
                if not node_data["_delete"]:
                    cls.__change_dml(node_data, update=True)
            yield node_data

    @classmethod
    def _update_node_data_from_database_by_pk(cls, model, primary_key: dict, data: dict) -> tuple[dict, bool]:
        cls.is_valid_model_instance(model)
        if type(primary_key) is not dict:
            raise TypeError
        if type(data) is not dict:
            raise TypeError
        select_result: dict = cls.connection.database.query(model).filter_by(**primary_key).all()
        if select_result:
            select_result = select_result[0].__dict__
            cls.__remove_local_data_from_database_data(model, primary_key, select_result)
            select_result.update(primary_key)
            select_result.update(data)
            return cls.__clear_node_data(model, select_result), True
        return data, False

    @classmethod
    def _update_node_data_from_local_by_pk(cls, model, primary_key: dict, data: dict):
        node = cls.connection.items.get_node(model, **primary_key)
        if node is None:
            return data
        data.update(node.value)
        return data

    @classmethod
    def _update_node_data_from_database_by_pk_multiple(cls, model, local_data: list[dict]) -> tuple[list[dict], set]:
        cls.is_valid_model_instance(model)
        if type(local_data) is not list:
            raise TypeError
        if not local_data:
            return local_data, set()
        [cls.__check_node_data(model, node_data) for node_data in local_data]
        pk_data = []
        primary_key = cls.get_primary_key_column_name(model)
        for current_data in local_data:
            value = cls._select_primary_key_value_from_node_data(model, current_data)
            if value:
                pk_data.append(getattr(model, primary_key) == value[primary_key])
        select_result: dict = cls.connection.database.query(model).filter_by(or_(*pk_data)).all()
        pk_from_db = set()
        for value in select_result:
            db_data = cls.__clear_node_data(model, value.__dict__)
            for index, current_local_item in enumerate(local_data):
                val = current_local_item[primary_key]
                if val == db_data[primary_key]:
                    cls.__remove_local_data_from_database_data(model, {primary_key: val}, db_data)
                    db_data.update({primary_key: val})
                    db_data.update(current_local_item)
                    local_data[index] = db_data
                    pk_from_db.add(val)
        return local_data, pk_from_db

    @classmethod
    def _update_node_data_from_database_by_unique_column_multiple(cls, model, local_data: list[dict]) -> tuple[list[dict], set]:
        cls.is_valid_model_instance(model)
        if type(local_data) is not list:
            raise TypeError
        if not local_data:
            return local_data, set()
        [cls.__check_node_data(model, **node_data) for node_data in local_data]
        pk_data = []
        for current_data in local_data:
            unique_data = cls.get_unique_columns(model, current_data)
            if not unique_data:
                continue
            pk_data.append(and_([getattr(model, q) == current_data[q] for q in unique_data]))
        select_result: dict = cls.connection.database.query(model).filter_by(or_(*pk_data)).all()
        primary_key = cls.get_primary_key_column_name(model)
        pk_from_db = set()
        for value in select_result:
            db_data = cls.__clear_node_data(model, value.__dict__)
            for index, current_local_item in enumerate(local_data):
                val = current_local_item[primary_key]
                if val == db_data[primary_key]:
                    cls.__remove_local_data_from_database_data(model, {primary_key: val}, db_data)
                    db_data.update({primary_key: val})
                    db_data.update(current_local_item)
                    local_data[index] = db_data
                    pk_from_db.add(val)
        return local_data, pk_from_db

    @classmethod
    def _update_node_data_from_local_by_unique_column_multiple(cls, model, local_data: list[dict]) -> None:
        cls.is_valid_model_instance(model)
        if not isinstance(local_data, list):
            raise TypeError
        if not local_data:
            return
        [cls.__check_node_data(model, node_data) for node_data in local_data]
        for index, current_data in enumerate(local_data):
            unique_data = cls.get_unique_columns(model, current_data)
            if not unique_data:
                continue
            nodes = cls.connection.items.search_nodes(model, **{name: current_data[name] for name in unique_data},
                                                      or_mode=False)
            if not nodes:
                continue
            if len(nodes) > 1:
                raise NodeColumnError
            data = nodes[0].value
            data.update(current_data)
            local_data[index] = data

    @classmethod
    def _update_node_data_from_database_by_unique_column(cls, model, data: dict) -> tuple[dict, bool]:
        cls.is_valid_model_instance(model)
        if not isinstance(data, dict):
            raise TypeError
        unique_columns = list(cls.get_unique_columns(model, data))
        unique_data = {key: data[key] for key in data if key in unique_columns}
        select_result = None
        if unique_data:
            select_result = cls.connection.database.query(model).filter_by(**unique_data).all()
        if select_result:
            select_result = select_result[0].__dict__
            pk_column = ModelTools.get_primary_key_column_name(model)
            primary_key_data = {pk_column: select_result[pk_column]}
            cls.__remove_local_data_from_database_data(model, primary_key_data, select_result)
            select_result.update(primary_key_data)
            select_result.update(data)
            return cls.__clear_node_data(model, select_result), True
        return data, False

    @classmethod
    def _update_node_data_from_local_nodes_by_unique_column(cls, model, data: dict):
        unique_columns = list(cls.get_unique_columns(model, data))
        unique_data = {key: data[key] for key in data if key in unique_columns}
        node = None
        if unique_data:
            node = cls.connection.items.search_nodes(model, **unique_data, or_mode=False)
        if not node:
            return data
        value = node[0].value
        value.update(data)
        return value

    @classmethod
    def _select_primary_key_value_from_node_data(cls, model, data) -> Optional[dict]:
        cls.is_valid_model_instance(model)
        field_name = cls.get_primary_key_column_name(model)
        try:
            value = data[field_name]
        except KeyError:
            return
        else:
            return {field_name: value}

    @classmethod
    def _get_highest_autoincrement_pk_from_local(cls, model) -> Optional[int]:
        try:
            val = max(map(lambda x: x.get_primary_key_and_value(only_value=True),
                          cls.connection.items.search_nodes(model)))
        except ValueError:
            return None
        return val

    @classmethod
    def _get_highest_autoincrement_pk_from_database(cls, model) -> Optional[int]:
        cls.is_valid_model_instance(model)
        return cls.connection.database.query(func.max(getattr(model, ModelTools.get_primary_key_column_name(model)))).scalar()

    @classmethod
    def __clear_node_data(cls, model, data) -> dict:
        """ Отфильтровать возможные лишние данные при получении данных из бд """
        if cls.INCOMING_DATA_VALIDATION_LEVEL not in ("filter", "strong",):
            raise ValueError
        result = {}
        cls.is_valid_model_instance(model)
        if cls.INCOMING_DATA_VALIDATION_LEVEL == "filter":
            for name in model().column_names:
                value = data.get(name, None)
                if value is not None:
                    result.update({name: value})
        if cls.INCOMING_DATA_VALIDATION_LEVEL == "strong":
            for name, value in data.items():
                if name in RESERVED_WORDS:
                    continue
                result.update({name: value})
        return result

    @classmethod
    def __remove_local_data_from_database_data(cls, model, primary_key: dict, database_data: dict):
        """ Из данных, полученных из базы, нужно удалить значения,
        столбцы от которых дублируются в локальных данных,- потому как локальные данные
         более свежие. """
        local_item_columns = tuple()
        local_node = cls.connection.items.get_node(model, **primary_key)
        if local_node is not None:
            local_item_columns = tuple(local_node.value.keys())
        [database_data.pop(column) for column in local_item_columns]

    @classmethod
    def __check_node_data(cls, model: CustomModel, _insert=False, _update=False, _delete=False, **node_data):
        primary_key = cls.get_primary_key_column_name(model)
        if primary_key in node_data:
            if type(node_data[primary_key]) is not cls.get_column_python_type(model, primary_key):
                raise TypeError
        if type(_insert) is not bool or not isinstance(_update, bool) or type(_delete) is not bool:
            raise TypeError
        if not sum((_insert, _update, _delete,)) == 1:
            raise ValueError
        if not all((isinstance(v, (int, str, type(None))) for v in node_data.values())):
            raise NodeColumnValueError

    @staticmethod
    def __change_dml(node_data, insert=False, update=False, delete=False):
        t = (insert, update, delete,)
        if any(map(lambda i: not isinstance(i, bool), t)):
            raise TypeError
        if not sum(t) == 1:
            raise ValueError
        node_data.update({"_delete": False, "_insert": False, "_update": False})
        node_data.update({"_delete": delete, "_update": update, "_insert": insert})
