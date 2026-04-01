"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Данный модуль является шаблоном для написания хранимых процедур
"""
import os
from dotenv import load_dotenv
from sqlalchemy import DDL, create_engine
from sqlalchemy.orm import create_session

load_dotenv(os.path.join(os.path.dirname(__file__), "settings.env"))
DB_PATH = os.environ.get("DATABASE_PATH")
engine = create_engine(DB_PATH)
session = create_session(bind=engine)


def init_procedure():
    procedure_name = DDL("""
    
    """)
    session.execute(procedure_name)
    session.commit()


def init_procedures():
    init_procedure()
    ...
    ...


if __name__ == "__main__":
    init_procedures()
