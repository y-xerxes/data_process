import datetime
import json
import sys
import os

from pyspark import RDD, SparkContext

from process.app.stream_app import StreamApp
from process.model.retailer_config import DatabaseConfig
from process.service.base_service import BaseService
from process.streaming_helper import StreamingHelper

from pyspark.streaming import StreamingContext

from process.util.data_context import DataContext
from process.util.database_rdd_generator import DatabaseRddGenerator
from process.util.spark_util import SparkUtil

spark = SparkUtil.get_spark()
sc = spark.sparkContext

ssc = StreamingContext(sc, batchDuration=60)
dstream = StreamingHelper.load_streaming_context(ssc, "fast_ss", topics=["fast_ss_notify1"])
session = BaseService.initialize_maintenance_objects()
global_config = DatabaseConfig.current_config(session)
generator = DatabaseRddGenerator(global_config, spark)
data_context = DataContext(spark=spark, config=global_config, generator=generator)
stream_app = StreamApp(data_context=data_context)

def process_notify(rdd):
    hour = datetime.datetime.now().hour
    minute = datetime.datetime.now().minute
    if hour == 1 and 1 <= minute <= 2:
        exit(-1)

    if hour == 7 and 1 <= minute <= 2:
        exit(-1)

    raw_data = rdd.collect()
    rdd_data_collect = []
    exists_retailer_codes = []

    for retailer_detail in raw_data:
        org_code = retailer_detail["org_code"]
        if org_code not in exists_retailer_codes:
            rdd_data_collect.append(retailer_detail)

    if len(rdd_data_collect) == 0:
        return

    retailer_details = []
    for rdd_data in rdd_data_collect:
        retailer_details.append({"org_code":rdd_data["org_code"], "dw_name":rdd_data["dw_name"]})
        if len(retailer_details) >= 16:
            stream_app.execute(retailer_details)
            retailer_details.clear()

    print("process rdd: ", retailer_details)
    if len(retailer_details) != 0:
        stream_app.execute(retailer_details)

dstream.map(lambda rdd: json.loads(rdd[1])).foreachRDD(process_notify)
ssc.start()
ssc.awaitTermination()