"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Данный модуль является шаблоном для написания хранимых процедур
"""
import os
from dotenv import load_dotenv
from sqlalchemy import DDL, create_engine
from sqlalchemy.orm import Session, create_session

load_dotenv(os.path.join(os.path.dirname(__file__), "database.env"))
DB_PATH = os.environ.get("DATABASE_PATH")


def init_procedure(s: Session):
    procedure_name = DDL("""
    
    """)
    s.execute(procedure_name)
    s.commit()


if __name__ == "__main__":
    engine = create_engine(DB_PATH)
    session = create_session(bind=engine)
    # init_procedure(session)
