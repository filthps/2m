import os
import itertools
from uuid import uuid4
from dotenv import load_dotenv
from flask import Flask
from sqlalchemy import String, Integer, Column, ForeignKey, Boolean, SmallInteger, Text, CheckConstraint
from sqlalchemy.orm import relationship
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


def get_uuid():
    return str(uuid4())


OPERATION_TYPES = (
    ("a", "Добавить"),
    ("r", "Переименовать"),
    ("d", "Удалить"),
    ("c", "Закомментировать"),
    ("uc", "Раскомментировать"),
)


class TaskDelegation(ModelController, db.Model, GlobalFields):
    __tablename__ = "taskdelegate"
    id = Column(String, primary_key=True, default=get_uuid)
    machineid = Column(ForeignKey("machine.machineid"), nullable=False)
    operationid = Column(ForeignKey("operationdelegation.opid"), nullable=False)


class Machine(ModelController, db.Model, GlobalFields):
    __tablename__ = "machine"
    machineid = Column(Integer, primary_key=True, autoincrement=True)
    cncid = Column(Integer, ForeignKey("cnc", ondelete="SET NULL", onupdate="SET NULL"), nullable=True)
    machinename = Column(String(100), unique=True, nullable=False)
    xover = Column(Integer, nullable=True, default=None)
    yover = Column(Integer, nullable=True, default=None)
    zover = Column(Integer, nullable=True, default=None)
    xfspeed = Column(Integer, nullable=True, default=None)
    yfspeed = Column(Integer, nullable=True, default=None)
    zfspeed = Column(Integer, nullable=True, default=None)
    spindelespeed = Column(Integer, nullable=True, default=None)
    inputcatalog = Column(String, nullable=False)
    outputcatalog = Column(String, nullable=False)
    operations = relationship("OperationDelegation", secondary=TaskDelegation.__table__)
    __table_args__ = (
        CheckConstraint("machinename!=''", name="machine_name_empty"),
        CheckConstraint(r"SUBSTRING(machinename, 1, 2) NOT SIMILAR TO '[0-9_\!@#\$%\^&\*\(\)\-\= ]*'", name="invalid_machine_name_reg"),
        CheckConstraint(r"machinename SIMILAR TO '\S*'", name="valid_machine_mame_reg"),
        CheckConstraint(r"inputcatalog SIMILAR TO '[A-Z]\:(\\[^\*\?«\<\>|:\/\.\,]+)+[^\s]'", name="input_catalog_reg"),
        CheckConstraint(r"outputcatalog SIMILAR TO '[A-Z]\:(\\[^\*\?«\<\>|:\/\.\,]+)+[^\s]'", name="output_catalog_reg"),
        CheckConstraint("inputcatalog!=''", name="input_catalog_is_not_empty"),
        CheckConstraint("outputcatalog!=''", name="output_catalog_is_not_empty"),
        CheckConstraint("xover>=0", name="x_over_must_be_positive"),
        CheckConstraint("yover>=0", name="y_over_must_be_positive"),
        CheckConstraint("zover>=0", name="z_over_must_be_positive"),
        CheckConstraint("xfspeed>=0", name="x_fspeed_must_be_positive"),
        CheckConstraint("yfspeed>=0", name="y_fspeed_must_be_positive"),
        CheckConstraint("zfspeed>=0", name="z_fspeed_must_be_positive"),
        CheckConstraint("spindelespeed>=0", name="spindele_speed_must_be_positive"),
    )


class OperationDelegation(ModelController, db.Model, GlobalFields):
    __tablename__ = "operationdelegation"
    opid = Column(String, primary_key=True, default=get_uuid)
    conditionid = Column(String, ForeignKey("cond.cnd"), nullable=True, default=None)
    insertid = Column(Integer, ForeignKey("insert.insid"), nullable=True, default=None)
    commentid = Column(Integer, ForeignKey("comment.commentid"), nullable=True, default=None)
    uncommentid = Column(Integer, ForeignKey("uncomment.uid"), nullable=True, default=None)
    removeid = Column(Integer, ForeignKey("remove.removeid"), nullable=True, default=None)
    renameid = Column(Integer, ForeignKey("renam.renameid"), nullable=True, default=None)
    replaceid = Column(Integer, ForeignKey("repl.replaceid"), nullable=True, default=None)
    numerationid = Column(Integer, ForeignKey("num.numerationid"), nullable=True, default=None)
    isactive = Column(Boolean, default=True, nullable=False)
    operationdescription = Column(String(300), default="", nullable=False)


class Condition(ModelController, db.Model, GlobalFields):
    __tablename__ = "cond"
    cnd = Column(String, primary_key=True, default=get_uuid)
    parent = Column(String, ForeignKey("cond.cnd", ondelete="SET NULL", onupdate="CASCADE"), nullable=True, default=None)
    hvarid = Column(ForeignKey("headvar.varid", ondelete="CASCADE", onupdate="CASCADE"), nullable=True, default=None)
    stringid = Column(ForeignKey("sstring.strid", ondelete="CASCADE", onupdate="CASCADE"), nullable=True, default=None)
    condinner = Column(String(50), nullable=False, default="")
    conditionbooleanvalue = Column(Boolean, default=True, nullable=False)
    isntfindfull = Column(Boolean, default=False, nullable=False)
    isntfindpart = Column(Boolean, default=False, nullable=False)
    findfull = Column(Boolean, default=False, nullable=False)
    findpart = Column(Boolean, default=False, nullable=False)
    parentconditionbooleanvalue = Column(Boolean, default=True, nullable=False)
    equal = Column(Boolean, default=False, nullable=False)
    less = Column(Boolean, default=False, nullable=False)
    larger = Column(Boolean, default=False, nullable=False)
    __table_args__ = (
        CheckConstraint("condinner!=''", name="condinner_empty"),
        CheckConstraint("hvarid IS NOT NULL OR stringid IS NOT NULL", name="check_cnd_target"),
    )


class Cnc(ModelController, db.Model, GlobalFields):
    __tablename__ = "cnc"
    cncid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(20), unique=True, nullable=False)
    commentsymbol = Column(String(1), nullable=False)
    exceptsymbols = Column(String(50), nullable=True, default=None)
    __table_args__ = (
        CheckConstraint("commentsymbol!=''", name="comment_symbol_empty"),
        CheckConstraint("name!=''", name="name_empty"),
    )


class HeadVarible(ModelController, db.Model, GlobalFields):
    __tablename__ = "headvar"
    varid = Column(String, default=get_uuid, primary_key=True)
    cncid = Column(ForeignKey("cnc.cncid", ondelete="CASCADE", onupdate="CASCADE"), nullable=False)
    strid = Column(ForeignKey("sstring.strid", ondelete="CASCADE", onupdate="CASCADE"), unique=True, nullable=False)
    name = Column(String, unique=True, nullable=False)
    __table_args__ = (
        CheckConstraint("name!=''", name="headvar_empty_name"),
    )


class Insert(ModelController, db.Model, GlobalFields):
    __tablename__ = "insert"
    insid = Column(Integer, primary_key=True, autoincrement=True)
    after = Column(Boolean, default=False, nullable=False)
    before = Column(Boolean, default=False, nullable=False)
    target = Column(String, nullable=False)
    item = Column(String, nullable=False)
    __table_args__ = (
        CheckConstraint("after!=before", name="after_equal_before"),
        CheckConstraint("target!=''", name="empty_target"),
        CheckConstraint("item!=''", name="empty_item"),
        CheckConstraint("target!=item", name="target_equal_item"),
    )


class Comment(ModelController, db.Model, GlobalFields):
    __tablename__ = "comment"
    commentid = Column(Integer, primary_key=True, autoincrement=True)
    findstr = Column(String(100), nullable=False)
    iffullmatch = Column(Boolean, default=False, nullable=False)
    ifcontains = Column(Boolean, default=False, nullable=False)


class Uncomment(ModelController, db.Model, GlobalFields):
    __tablename__ = "uncomment"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    findstr = Column(String(100), nullable=False)
    iffullmatch = Column(Boolean, default=False, nullable=False)
    ifcontains = Column(Boolean, default=False, nullable=False)
    __table_args__ = (
        CheckConstraint("findstr!=''", name="empty_findstr"),
    )


class Remove(ModelController, db.Model, GlobalFields):
    __tablename__ = "remove"
    removeid = Column(Integer, primary_key=True, autoincrement=True)
    iffullmatch = Column(Boolean, default=False, nullable=False)
    ifcontains = Column(Boolean, default=False, nullable=False)
    findstr = Column(String(100), nullable=False)
    __table_args__ = (
        CheckConstraint("findstr!=''", name="empty_findstr"),
        CheckConstraint("iffullmatch!=ifcontains", name="equal_iffullmatch_and_ifcontains"),
    )


class HeadVarDelegation(ModelController, db.Model, GlobalFields):
    __tablename__ = "varsec"
    secid = Column(String, default=get_uuid, primary_key=True)
    varid = Column(String, ForeignKey("headvar.varid"), nullable=False)
    insertid = Column(Integer, ForeignKey("insert.insid"), nullable=True, default=None)
    renameid = Column(Integer, ForeignKey("renam.renameid"), nullable=True, default=None)
    __table_args__ = (
        CheckConstraint("(insertid IS NULL OR renameid IS NULL)=TRUE AND (insertid IS NULL AND renameid IS NULL)=FALSE", name="check_one_item_delegation"),
    )


class Rename(ModelController, db.Model, GlobalFields):
    __tablename__ = "renam"
    renameid = Column(Integer, primary_key=True, autoincrement=True)
    uppercase = Column(Boolean, default=False, nullable=False)
    lowercase = Column(Boolean, default=False, nullable=False)
    prefix = Column(String(10), nullable=True, default=None)
    postfix = Column(String(10), nullable=True, default=None)
    nametext = Column(String(20), nullable=True, default=None)
    removeextension = Column(Boolean, default=False, nullable=False)
    setextension = Column(String(10), nullable=True, default=None)
    #  varibles = relationship("HeadVarDelegation")


class Numeration(ModelController, db.Model, GlobalFields):
    __tablename__ = "num"
    numerationid = Column(Integer, autoincrement=True, primary_key=True)
    startat = Column(Integer, nullable=False, default=1)
    endat = Column(Integer, nullable=True, default=None)
    __table_args__ = (
        CheckConstraint("startat!=endat", name="startat_equal_endat"),
        CheckConstraint("startat>=0", name="negatory_startat_value"),
        CheckConstraint("endat>=0", name="negatory_endat_value"),
        CheckConstraint("startat<endat", name="startat_more_then_endat"),
    )


class Replace(ModelController, db.Model, GlobalFields):
    __tablename__ = "repl"
    replaceid = Column(Integer, primary_key=True, autoincrement=True)
    findstr = Column(String(100), nullable=False)
    ifcontains = Column(Boolean, default=False, nullable=False)
    iffullmatch = Column(Boolean, default=False, nullable=False)
    item = Column(String(100), nullable=False)
    __table_args__ = (
        CheckConstraint("ifcontains!=iffullmatch", name="ifcontains_equal_iffullmatch"),
        CheckConstraint("item!=''", name="empty_item"),
        CheckConstraint("findstr!=''", name="empty_findstr"),
        CheckConstraint("findstr!=item", name="findstr_equal_item"),
    )


class SearchString(ModelController, db.Model, GlobalFields):
    __tablename__ = "sstring"
    strid = Column(String, default=get_uuid, primary_key=True)
    inner_ = Column(Text, default="", nullable=False)
    ignorecase = Column(Boolean, default=True, nullable=False)
    lindex = Column(SmallInteger, default=0, nullable=False)
    rindex = Column(SmallInteger, default=-1, nullable=False)
    lignoreindex = Column(SmallInteger, default=0, nullable=True)
    rignoreindex = Column(SmallInteger, default=0, nullable=True)
    __table_args = (
        CheckConstraint("inner_!=''", name="required_inner"),
        CheckConstraint("lindex<0", name="invalid_l_sep_left_border"),
        CheckConstraint("rindex<=0", name="invalid_r_sep_left_border"),
        CheckConstraint("lindex>=LENGTH(inner_)-1", name="invalid_l_sep_right_border"),
        CheckConstraint("rindex>LENGTH(inner_)-1", name="invalid_r_sep_right_border"),
        CheckConstraint("lignoreindex<0", name="invalid_l_ignore_sep_left_border"),
        CheckConstraint("rignoreindex<=0", name="invalid_r_ignore_sep_left_border"),
        CheckConstraint("lignoreindex>=LENGTH(inner_)-1", name="invalid_l_ignore_sep_right_border"),
        CheckConstraint("rignoreindex>LENGTH(inner_)-1", name="invalid_r_ignore_sep_right_border"),
        CheckConstraint("lindex=rindex", name="empty_main_place"),
        CheckConstraint("lignoreindex=rignoreindex", name="empty_ignore_place"),
        CheckConstraint("lindex=lignoreindex AND rindex=rignoreindex", name="empty_matched"),
        CheckConstraint("lignoreindex IS NULL OR rignoreindex IS NULL", name="any_ignore_sep_isnt_set"),
        CheckConstraint("rindex!=-1 AND lindex>rindex", name="invalid_seq_ordering"),
        CheckConstraint("lignoreindex>rignoreindex", name="invalid_ignore_sep_ordering"),
    )


def check_bad_attribute_name():
    TaskDelegation()
    Machine()
    OperationDelegation()
    Condition()
    Cnc()
    HeadVarible()
    Insert()
    Comment()
    Uncomment()
    Remove()
    HeadVarDelegation()
    Rename()
    Numeration()
    Replace()
    SearchString()


def test_unique_primary_key_column_name(field_name: str):
    """ Уникальность названия для столбца первичного ключа по всем таблицам """
    def primary_keys():
        for m in (TaskDelegation(), Machine(), OperationDelegation(), Condition(), Cnc(), HeadVarible(), Insert(), Comment(),
                  Uncomment(), Remove(), HeadVarDelegation(), Rename(), Numeration(), Replace(), SearchString(),):
            yield {m.__class__.__name__: getattr(m, field_name)}
    repeat_table_names = []
    primary_key_field_names = list(itertools.chain(*tuple(map(lambda x: tuple(x.values()), primary_keys()))))
    for elem in primary_keys():
        model_name, pk_key = tuple(elem.keys())[0], tuple(elem.values())[0]
        if primary_key_field_names.count(pk_key) > 1:
            repeat_table_names.append(model_name)
    if repeat_table_names:
        raise KeyError(f"Во всём проекте названия полей-первичных ключей должны быть уникальными! "
                       f" Повторы в таблицах: {', '.join(repeat_table_names)}")


def drop_db():
    app.app_context().push()
    db.drop_all()


def create_db():
    check_bad_attribute_name()
    app.app_context().push()
    db.create_all()


if __name__ == "__main__":
    drop_db()
    create_db()
