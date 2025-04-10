"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Данный модуль является шаблоном для написания классов-моделей таблиц
"""
import os
from dotenv import load_dotenv
from flask import Flask
from flask_sqlalchemy import SQLAlchemy as FlaskSQLAlchemy
from two_m.conf import GlobalFields
from two_m.flasksqlalchemy.adapter import ModelController

load_dotenv(os.path.join(os.path.dirname(__file__), "settings.env"))
DATABASE_PATH = os.environ.get("DATABASE_PATH")


def create_app(path=None, app_name=None):
    app = Flask(app_name or __name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = path or DATABASE_PATH
    return app


app = create_app()
db = FlaskSQLAlchemy(app)


class TableName(ModelController, db.Model, GlobalFields):
    __tablename__ = "tablename"
    # Write your models here
    ...
    ...


def drop_db():
    app.app_context().push()
    db.drop_all()


def create_db():
    app.app_context().push()
    db.create_all()


if __name__ == "__main__":
    drop_db()
    create_db()
