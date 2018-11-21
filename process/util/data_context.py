from hdfs import Client
from pyspark.sql import SparkSession
from process.util.database_rdd_generator import DatabaseRddGenerator

class DataContext(object):
    def __init__(self, spark, config, generator):
        super(DataContext, self).__init__()
        self.spark = spark
        self.config = config
        self.generator = generator
        self.env_prefix = config.get("prefix", None)
        self.hdfs_client = Client(url=";".join(config["hdfs"]["name_node"]), proxy="joowing")

    def retailer_database_config(self):
        return self.config["retailer"]