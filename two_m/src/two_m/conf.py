"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
"""
import datetime
from abc import abstractmethod
from sqlalchemy import Column, DateTime

RESERVED_WORDS = ("__insert", "__update", "__delete", "__ready", "__model", "column_names")


class AbstractModelController:
    @abstractmethod
    def __new__(cls):
        column_names = ...
        return super().__new__(cls)


class CustomModel(AbstractModelController):  # Нужно для аннотации
    __tablename__ = ...


class GlobalFields:
    _create_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)
