"""
从DatabaseAgent的config字段类似的配置，构建一个从数据库读取数据的rdd
"""

import datetime
from pyspark import RDD, Row, SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from process.util.util import name_with_prefix

class DatabaseRddGenerator(object):
    def __init__(self, config, spark):
        self.config = config
        self.spark = spark