from sqlalchemy import Column, String, create_engine
from sqlalchemy import Table, MetaData, Integer, Float, String, exc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import time

db_engine = None #create_engine("mssql+pymssql://rds:Nuctech_50@rm-uf6e7713601veidmx4o.sqlserver.rds.aliyuncs.com:3433/umdb",deprecate_large_types=True, encoding="utf-8",echo=False)
DBSession = None
Base = declarative_base()

def init_sqlalchemy(dbname="mssql+pymssql://rds:Nuctech_50@rm-uf6e7713601veidmx4o.sqlserver.rds.aliyuncs.com:3433/umdb"):
    global db_engine
    global DBSession

    db_engine = create_engine(dbname, deprecate_large_types=True, encoding="utf-8",echo=False)
    DBSession = sessionmaker(bind=db_engine)
    Base.metadata.drop_all(db_engine)
    Base.metadata.create_all(db_engine)


class AutoMpg(Base):
    __tablename__ = 'autompg_Table'
    id = Column(Integer, autoincrement=True, primary_key=True)
    mpg = Column(Float)
    cylinders = Column(Integer)
    displacement = Column(Float)
    horsepower = Column(Float)
    weight = Column(Float)
    acceleration = Column(Float)
    model_year = Column(Integer)
    origin = Column(Integer)
    carname = Column(String)

    def __repr__(self):
        return "carname is:%r" % self.carname


def GetTableRecords():
    try:
        session = DBSession()
        results = session.query(AutoMpg).all()

        session.close()
        return results
    except exc.SQLAlchemyError as e:
        print("error"+e)
        session.close()
        return None

def InsertOdpsRecordToMssql(session, record):
    if session:
        mpgRecord = AutoMpg(mpg = record['mpg'],
                            cylinders = record['cylinders'],
                            displacement = record['displacement'],
                            horsepower = record['horsepower'],
                            weight = record['weight'],
                            acceleration = record['acceleration'],
                            model_year = record['model_year'],
                            origin = record['origin'],
                            carname = record['carname']
                            )
        session.add(mpgRecord)

    return

from odps import ODPS
from odps.df import DataFrame
from odps.models import Schema, Column, Partition

odps = ODPS('LTAI43aG1FXDDpM5', 'DH0BDyyAC4ryyWirbUDjoapBfP3xFS',
            'sk_odps_test06', 'https://service.odps.aliyun.com/api')

def get_odps_results():
    t0 = time.time()
    mpgLists = []
    for record in odps.read_table('sk_mpg_table'):
        mpgRecord = AutoMpg(mpg = record['mpg'],
                            cylinders = record['cylinders'],
                            displacement = record['displacement'],
                            horsepower = record['horsepower'],
                            weight = record['weight'],
                            acceleration = record['acceleration'],
                            model_year = record['model_year'],
                            origin = record['origin'],
                            carname = record['carname']
                            )
        mpgLists.append(mpgRecord)

    t1 = time.time()
    print("add_all, time used:" + str(t1-t0) + "sec")
    return mpgLists

def one_by_one():
    t0 = time.time()
    session = DBSession()
    for record in odps.read_table('sk_mpg_table'):
        InsertOdpsRecordToMssql(session, record)
    session.commit()
    session.close()
    t1 = time.time()
    print("one_by_one, time used:" + str(t1-t0) + "sec")

def add_all():
    t0 = time.time()
    session = DBSession()
    mpgLists = []
    for record in odps.read_table('sk_mpg_table'):
        mpgRecord = AutoMpg(mpg = record['mpg'],
                            cylinders = record['cylinders'],
                            displacement = record['displacement'],
                            horsepower = record['horsepower'],
                            weight = record['weight'],
                            acceleration = record['acceleration'],
                            model_year = record['model_year'],
                            origin = record['origin'],
                            carname = record['carname']
                            )
        mpgLists.append(mpgRecord)
    session.add_all(mpgLists)
    session.commit()
    session.close()
    t1 = time.time()
    print("add_all, time used:" + str(t1-t0) + "sec")

def bulk_add():
    t0 = time.time()
    session = DBSession()
    mpgLists = []

    for record in odps.read_table('sk_mpg_table'):
        mpgRecord = AutoMpg(mpg = record['mpg'],
                            cylinders = record['cylinders'],
                            displacement = record['displacement'],
                            horsepower = record['horsepower'],
                            weight = record['weight'],
                            acceleration = record['acceleration'],
                            model_year = record['model_year'],
                            origin = record['origin'],
                            carname = record['carname']
                            )

        mpgLists.append(mpgRecord)

    print(len(mpgLists))
    session.bulk_save_objects(mpgLists)
    session.commit()
    session.close()
    t1 = time.time()
    print("bulk_add, time used:" + str(t1-t0) + "sec")

def test_bulk_insert_mapping():
    t0 = time.time()
    session = DBSession()
    mpgLists = []

    for record in odps.read_table('sk_mpg_table'):
        mpgRecord={}
        mpgRecord['mpg', 'cylinders', 'displacement', 'horsepower', 'weight', 'acceleration', 'model_year', 'origin', 'carname'] = \
            record['mpg', 'cylinders', 'displacement', 'horsepower', 'weight', 'acceleration', 'model_year', 'origin', 'carname']

        mpgLists.append(mpgRecord)

    session.bulk_insert_mappings(AutoMpg, mpgLists)
    session.commit()
    session.close()
    t1 = time.time()
    print("test_bulk_insert_mapping, time used:" + str(t1-t0) + "sec")


def test_sqlalchemy_core():
    t0 = time.time()
    mpgLists = []

    for record in odps.read_table('sk_mpg_table'):
        mpgRecord={}
        mpgRecord['mpg', 'cylinders', 'displacement', 'horsepower', 'weight', 'acceleration', 'model_year', 'origin', 'carname'] = \
            record['mpg', 'cylinders', 'displacement', 'horsepower', 'weight', 'acceleration', 'model_year', 'origin', 'carname']

        mpgLists.append(mpgRecord)

    db_engine.execute(
        AutoMpg.__table__.insert(), mpgLists)  ##==> engine.execute('insert into ttable (name) values ("NAME"), ("NAME2")')

    t1 = time.time()
    print("test_sqlalchemy_core, time used:" + str(t1-t0) + "sec")


def clearMpgTable():
    session = DBSession()
    session.query(AutoMpg).delete()
    session.commit()
    session.close()


init_sqlalchemy()
clearMpgTable()
one_by_one()
clearMpgTable()
add_all()
clearMpgTable()
bulk_add()
clearMpgTable()
test_bulk_insert_mapping()
clearMpgTable()
test_sqlalchemy_core()
