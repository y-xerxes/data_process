"""
通过spark的api, 或者直接调用MysqlDB的api从业务数据源中获取数据
"""
from typing import Tuple, List, Union

import datetime
from pyspark import Row, RDD
from pyspark.sql import SparkSession, DataFrame

from process.spark.context import RetailerContext


class MysqlDataLoader(object):
    def __init__(self, retailer_context):
        # type: (RetailerContext) -> None
        super(MysqlDataLoader, self).__init__()
        self.config = retailer_context.config
        self.spark = retailer_context.spark
        self.retailer_context = retailer_context

    def retailer_data_frame(self, database_name=None, table_name=None, sql=None):
        # type: (str, str, str) -> DataFrame
        return self._data_frame("retailer", database_name=database_name, table_name=table_name, sql=sql)

    def _data_frame(self, database_type, database_name=None, table_name=None,
                    sql=None, partition_column=None, lower_bound=None, upper_bound=None, num_partitions=None):
        database_config = self.config[database_type]
        jdbc_url = "jdbc:mysql://{0}:{1}".format(database_config["host"], database_config["port"])
        user_name = database_config["username"]
        password = database_config["password"]

        frame = self.spark.read.format("jdbc")\
            .option("url", jdbc_url)\
            .option("user", user_name).option("password", password)

        if partition_column is not None:
            frame = frame.option("partitionColumn", partition_column)\
                .option("lowerBound", lower_bound).option("upperBound", upper_bound)\
                .option("numPartitions", num_partitions)
        if sql is not None:
            print(sql)
            frame = frame.option("dbtable", "({0}) as tmp".format(sql))
        else:
            frame = frame.option("dbtable", "{0}.{1}".format(database_name, table_name))

        return frame.load()
