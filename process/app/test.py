import datetime

from joowing.share.spark.base.application import DataApplication
from joowing.share.spark.buz.base.calculator import ToOrgCodeKV, OrgCodePartitioner
from joowing.share.spark.buz.base.data_fetcher import ShopDetailFetcher
from joowing.share.spark.buz.retailer_statistics.loader import RetailerSaleStatisticsDayLoader, GuiderAmountLoader
from joowing.share.spark.buz.retailer_statistics.stage import PrtSaleToFctOrders, PrtSaleItemToFctOrderItems, \
    GuiderSaleStatistics, ShopSaleStatistics
from joowing.share.spark.cache_support import DailyCache, ContextCache
from joowing.share.spark.database_rdd_generator import DatabaseRddGenerator
from joowing.share.spark.jw_context import JwContext, JwRetailerContext
from joowing.share.spark.spark_util import SparkUtil
from joowing.share.spark.util.data_transfer import RetailerDatabaseContext
from typing import List

from joowing.share.spark.util.rdd_union import RDDUnion


class StreamApp(DataApplication):
    """
    实时流的r运算App, 常驻在mesos上, 根据实时同步结束后的信号量来计算商户实时数据的几张表
    """
    def __init__(self, **kwargs):
        super(StreamApp, self).__init__(**kwargs)
        self.daily_cache = DailyCache(jw_context=self.jw_context)
        self._prepare_data_fetchers()

    def execute(self, retailer_details: List[dict]):
        sales_rdd = None  # type: RDD
        guider_amount_rdd = None  # type: RDD
        dim_date_id = int(datetime.datetime.now().strftime("%Y%m%d"))
        # dim_date_id = 20180422
        context_cache = ContextCache()
        org_count = len(retailer_details)
        should_retry = True
        retry_times = 0

        retailer_database_contexts = []
        for retailer_detail in retailer_details:
            org_code = retailer_detail["org_code"]
            dw_name = retailer_detail['dw_name']
            retailer_context = JwRetailerContext(jw_context=self.jw_context, org_code=org_code, dw_name=dw_name)

            sales = RetailerSaleStatisticsDayLoader(retailer_context=retailer_context, dim_date_id=dim_date_id).load()          #df
            sales = context_cache.cache(sales)                                                                                  #df
            sales = ToOrgCodeKV.calculate(sales.rdd, org_code).partitionBy(
                numPartitions=org_count, partitionFunc=OrgCodePartitioner.org_code_partitioner)                                 #rdd
            sales_rdd = RDDUnion.union(sales_rdd, sales)

            guider_amount = GuiderAmountLoader(retailer_context=retailer_context, dim_date_id=dim_date_id).load()               #rdd
            guider_amount = ToOrgCodeKV.calculate(guider_amount, org_code)
            guider_amount_rdd = RDDUnion.union(guider_amount_rdd, guider_amount)

        sales_rdd = context_cache.cache(sales_rdd)
        guider_amount_rdd = context_cache.cache(guider_amount_rdd)
        shop_total_sales = ShopSaleStatistics(daily_cache=self.daily_cache, sales_rdd=sales_rdd,
                                              context_cache=context_cache, retailer_details=retailer_details,
                                              jw_context=self.jw_context, guider_amount_rdd=guider_amount_rdd,
                                              dim_date_id=dim_date_id).execute()

        GuiderSaleStatistics(daily_cache=self.daily_cache, sales_rdd=sales_rdd, context_cache=context_cache,
                             retailer_details=retailer_details, jw_context=self.jw_context,
                             guider_amount_rdd=guider_amount_rdd, dim_date_id=dim_date_id,
                             shop_total_sales=shop_total_sales).execute()

    def _prepare_data_fetchers(self):
        """
        针对业务场景, 准备合适的DataFetcher
        :return:
        """
        self.daily_cache.register(ShopDetailFetcher())



spark = SparkUtil.get_spark()
global_config = { "buz" : { "host" : "", "port" : 3306, "password" : "", "username" : "" },
"hdfs" : { "name_node" : [ "", "" ]},
"hive" : { "host" : "", "port" : 10000 },
"livy" : { "address" : "" },
"kafka" : { "bootstrap_servers" : [ "", "" ]},
"redis" : { "host" : "", "port" : 6379 },
"rabbitmq" : { "host" : "", "pass" : "", "port" : "", "user" : "", "vhost" : "" },
"retailer" : { "host" : "222.73.36.230", "port" : 3336, "password" : "datadev!v33.6", "username" : "datadev" },
"real_time" : { "host" : "222.73.36.230", "port" : 3337, "password" : "datadev!v33.7", "username" : "datadev" },
"site_name" : "",
"buz_mongodb" : { "hosts" : [ "", "", "" ]},
"maintenance" : { "host" : "222.73.36.230", "port" : 3337, "password" : "datadev!v33.7", "username" : "datadev", "database_name" : "database_service_maintenance" },
"rdb_ib_host" : "",
"non_real_time" : { "host" : "222.73.36.230", "port" : 3336, "password" : "datadev!v33.6", "username" : "datadev" },
"datax_service_host" : ""}
generator = DatabaseRddGenerator(global_config, spark)
jw_context = JwContext(spark=spark, config=global_config, generator=generator)
retailer_details = [{"org_code": "mtyunzhiai", "dw_name": "mtyunzhiaidw"}]
stream_app = StreamApp(jw_context=jw_context)
stream_app.execute(retailer_details)