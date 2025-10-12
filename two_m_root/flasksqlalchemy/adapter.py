"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Адаптер для flask-sqlalchemy https://flask-sqlalchemy.readthedocs.io
"""
from sqlalchemy.orm import InstrumentedAttribute
from two_m_root.conf import RESERVED_WORDS, AbstractModelController


class ModelController(AbstractModelController):
    __table__ = ...
    column_names = ...
    foreign_keys = ...

    def __new__(cls, **k):
        cls.column_names = {}  # Если база данных инициализирована вручную, средствами sql,
        # то заполнить даный словарь вручную

        def check_class_attributes():
            """ Предотвратить использование заерезервированных в классе orm.Main слов """
            for special_word in RESERVED_WORDS:
                if hasattr(cls, f"__{cls.__name__}{special_word}"):
                    raise AttributeError(
                        f"Не удалось инциализировать класс-модель {cls.__name__}. "
                        f"Атрибут {special_word} использовать нельзя, тк он зарезервирован."
                    )

        def collect_column_attributes():
            """ Собрать в атрибут класса column_names все имена столбцов таблицы """
            column_names = cls.column_names
            for value in cls.__dict__.values():
                if type(value) is InstrumentedAttribute and hasattr(value.expression, "name"):
                    column_names.update({value.expression.name: {"type": value.expression.type.python_type,
                                                                 "nullable": value.expression.nullable,
                                                                 "primary_key": value.expression.primary_key,
                                                                 "autoincrement": True if not value.expression.autoincrement == "auto" else False,
                                                                 "unique": value.expression.unique,
                                                                 "default": value.expression.default}})  # todo: Доработать остальные аналоги default, согласно документации https://docs.sqlalchemy.org/en/20/core/defaults.html
                    if hasattr(value, "length"):
                        column_names[value.expression.name].update({"length": value.length})

        def collect_foreign_keys():
            cls.foreign_keys = tuple([instance.column.name for instance in cls.__table__.foreign_keys])
        check_class_attributes()
        collect_column_attributes() if not cls.column_names else None
        collect_foreign_keys()
        return super().__new__(cls)
