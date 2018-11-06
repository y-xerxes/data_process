#!/usr/env/python
#encoding: utf-8

from typing import List, Union, Tuple
import datetime
from pyspark.streaming import StreamingContext

from pyspark import Row
from pyspark.sql import SparkSession, DataFrame

class SparkUtil(object):
    @staticmethod
    def get_spark():
        # type: () -> SparkSession
        global_spark = globals().get("spark", None)
        if global_spark is None:
            from pyspark.shell import spark
            return spark
        else:
            return global_spark


# if __name__ == "__main__":
#     sparkutil = SparkUtil()
#     spark = sparkutil.get_spark()
#     sc = spark.sparkContext
#     ssc = StreamingContext(sc, batchDuration=60)
#     print(ssc)