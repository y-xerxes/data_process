# -*- coding: UTF-8 -*-
import functools
from airflow.hooks.mysql_hook import MySqlHook
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class BaseService(object):
    """
    所有在dag中运行的Python任务实际上都是继承与这个service, 这个service实例一旦被创建, 会做以下的事情:

    * 初始化Maintenance的模型的数据库类型
    """

    def __init__(self, *_args, **_kwargs):
        self.kwargs = _kwargs
        self.maintenance_session = None

    @staticmethod
    def initialize_maintenance_objects():
        hook = MySqlHook(mysql_conn_id="maintenance",
                         schema="database_service_maintenance")
        connection = hook.get_conn()
        engine = create_engine("mysql://", creator=lambda: connection, echo=True, encoding="utf-8")
        session_klz = sessionmaker(bind=engine, autocommit=True, autoflush=True)
        session = session_klz()
        """:type:Session"""
        return session

    def execute(self, ds):
        self.prepare()
        pass

    def prepare(self):
        self.amintenance_session = self.initialize_maintenance_objects()

if __name__ == "__main__":
    baseservice = BaseService()
    session = baseservice.initialize_maintenance_objects()