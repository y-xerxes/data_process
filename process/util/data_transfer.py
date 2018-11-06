import MySQLdb
from MySQLdb import Connection
from MySQLdb.cursors import Cursor, DictCursor
from jinja2 import Template
from pyspark import SparkContext, RDD
from typing import Iterator, List, Union

from process.util.data_context import DataContext


class DatabaseContext(object):
    def __init__(self, db_config: dict):
        super(DatabaseContext, self).__init__()
        self.db_config = db_config

    def connection(self, tag: str):
        """
        :return: any connection for any database
        """
        raise (NotImplementedError())


class MysqlDatabaseContext(DatabaseContext):
    @staticmethod
    def build_connection_for(db_config: dict):
        database_name = db_config.get("database_name")
        if database_name is None:
            return MySQLdb.connect(host=db_config["host"], port=db_config["port"], user=db_config["username"],
                                   passwd=db_config["password"], charset="utf8")
        else:
            return MySQLdb.connect(host=db_config["host"], port=db_config["port"], user=db_config["username"],
                                   passwd=db_config["password"], db=db_config.get("database_name"), charset="utf8")

    def connection(self, tag: str) -> Connection:
        db_config = self.db_config_for(tag)
        return MysqlDatabaseContext.build_connection_for(db_config)

    def db_config_for(self, tag: str) -> dict:
        return self.db_config[tag].copy()


class RetailerDatabaseContext(MysqlDatabaseContext):
    def __init__(self, db_config: dict, org_code: str, dw_name: str):
        super(RetailerDatabaseContext, self).__init__(db_config=db_config)
        self.org_code = org_code
        self.dw_name = dw_name

    def db_config_for(self, tag: str):
        db_config = super(RetailerDatabaseContext, self).db_config_for(tag)
        if tag == "retailer":
            db_config["database_name"] = self.dw_name

        return db_config


class BaseReader(object):
    def read(self) -> Union[List[dict], None]:
        raise (NotImplemented)


class MysqlBaseReader(BaseReader):
    def __init__(self,
                 sql_template: str,
                 additional_template_args: dict = None):
        super(MysqlBaseReader, self).__init__()
        self.sql_template = sql_template
        self.additional_template_args = additional_template_args

    def read(self) -> Union[List[dict], None]:
        connection = self.connection()
        connection.begin()
        cursor = connection.cursor(DictCursor)
        sql = self.sql()
        print("load via sql: ",sql)
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        return results

    def connection(self) -> Connection:
        raise(NotImplementedError())

    def sql(self) -> str:
        args = {}
        if self.additional_template_args is not None:
            args.update(self.additional_template_args)

        return Template(self.sql_template).render(**args)


class RetailerMysqlBaseReader(MysqlBaseReader):
    def __init__(self, retailer_database_context: RetailerDatabaseContext, tag: str = None, **kwargs):
        super(RetailerMysqlBaseReader, self).__init__(**kwargs)
        self.retailer_database_context = retailer_database_context
        self.tag = tag
        if self.tag is None:
            self.tag = "retailer"

    def connection(self) -> Connection:
        return self.retailer_database_context.connection(self.tag)

    def sql(self):
        args = {"retailer_database_context": self.retailer_database_context}
        if self.additional_template_args is not None:
            args.update(self.additional_template_args)
        return Template(self.sql_template).render(**args)


class ParallelingRetailerDataLoader(object):
    """
    在我们系统中, 存在着一种场景:
    在通过Spark处理数据的时候, 需要同时从商户DW中载入同种类型的数据, 通常是使用同一个SQL, 不同的DW库

    本Loader就是为了解决这样的问题:
    1. 传入多个RetailerDatabaseContext和对应的sql
    2. 通过Spark并发的从数据库中载入
    3. 返回按照商户分区的RDD
    """

    @staticmethod
    def read(retailer_database_context: RetailerDatabaseContext, sql_template: str,
             additional_template_args: dict = None, tag=None) -> list:
        list_data = RetailerMysqlBaseReader(retailer_database_context=retailer_database_context,
                                            sql_template=sql_template,
                                            tag=tag,
                                            additional_template_args=additional_template_args).read()
        result = []
        for data in list_data:
            result.append([retailer_database_context, data])

        return result

    def __init__(self,
                 data_context: DataContext,
                 retailer_database_contexts: Iterator[RetailerDatabaseContext],
                 sql_template: str,
                 additional_template_args: dict = None,
                 tag: str = None) -> None:
        super(ParallelingRetailerDataLoader, self).__init__()
        self.data_context = data_context
        self.retailer_database_contexts = retailer_database_contexts
        self.sql_template = sql_template
        self.additional_template_args = additional_template_args
        self.tag = tag

    def load(self) -> RDD:
        sc = self.data_context.spark.sparkContext   # type: SparkContext
        rdd = sc.parallelize(self.retailer_database_contexts, len(list(self.retailer_database_contexts)))
        local_sql = self.sql_template
        local_additional_template_args = self.additional_template_args
        local_tag = self.tag

        def local_load(retailer_database_context: RetailerDatabaseContext):
            return ParallelingRetailerDataLoader.read(retailer_database_context=retailer_database_context,
                                                      sql_template=local_sql,
                                                      tag=local_tag,
                                                      additional_template_args=local_additional_template_args)
        return rdd.flatMap(local_load)