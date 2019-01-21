"""
很多对象中需要访问一些常规的属性, 封装起来作为个context
"""
from hdfs import Client
from pyhive import hive
from pyhive.hive import Connection
from pyspark.sql import SparkSession

from process.util.util import prefixed_name


class JwNormalContext(object):
    def __init__(self, config):
        super(JwNormalContext, self).__init__()
        self.config = config
        self.hdfs_client = Client(url=";".join(config["hdfs"]["name_node"]), proxy='joowing')
        self.env_prefix = config.get("prefix", None)

    def retailer_database_config(self) -> dict:
        return self.config["retailer"]

    def build_hive_connection(self) -> Connection:
        return hive.connect(self.config["hive"]["host"], port=int(self.config["hive"]["port"]))

    def build_hdfs_client(self) -> Client:
        return Client(url=";".join(self.config["hdfs"]["name_node"]), proxy='joowing')

    def prefixed_name(self, name):
        return prefixed_name(name, self.env_prefix)


class JwContext(JwNormalContext):
    def __init__(self, spark, config, generator):
        # type: (SparkSession, dict, DatabaseRddGenerator) -> None
        super(JwContext, self).__init__(config=config)
        self.spark = spark
        self.generator = generator


class JwRetailerContext(JwContext):
    def __init__(self, jw_context, org_code, dw_name):
        super(JwRetailerContext, self).__init__(spark=jw_context.spark,
                                                config=jw_context.config,
                                                generator=jw_context.generator)
        self.org_code = org_code
        self.dw_name = dw_name
