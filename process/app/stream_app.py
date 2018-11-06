import datetime
from typing import List

from pyspark import RDD

from process.app.application import DataApplication
from process.buz.retailer_statistics.stage import PrtSaleToFctOrders
from process.util.cache_support import DailyCache, ContextCache
from process.util.data_transfer import RetailerDatabaseContext


class StreamApp(DataApplication):
    """
    实时流的运算app，用实时任务结束过后的信号量来计算实时数据
    """
    def __init__(self, **kwargs):
        super(StreamApp, self).__init__(**kwargs)
        self.daily_cache = DailyCache(data_context=self.data_context)

    def execute(self, retailer_details):
        sales_rdd = None
        guider_amount_rdd = None
        dim_date_id = int(datetime.datetime.now().strftime("%Y%m%d"))
        context_cache = ContextCache()
        org_count = len(retailer_details)
        should_retry = True
        retry_times = 0

        while should_retry:
            try:
                retailer_database_contexts = []
                for retailer_detail in retailer_details:
                    retailer_database_contexts.append(RetailerDatabaseContext(
                        db_config=self.data_context.config,
                        org_code=retailer_detail["org_code"],
                        dw_name=retailer_detail["dw_name"]
                    ))

                PrtSaleToFctOrders(data_context=self.data_context,
                                   retailer_database_contexts=retailer_database_contexts,
                                   dim_date_id=dim_date_id).execute()


                sales_rdd = context_cache.cache(sales_rdd)
                if sales_rdd is None:
                    context_cache.clear()
                    should_retry = False
                    return True


            except Exception as error:
                err_msg = str(error)
                print("stream app get error: {0}, retailer_details: {1}".format(err_msg, str(retailer_details)))
                # 目前在线上存在这样的情况
                # 跑着跑着，python从spark driver的进程中去读取rdd数据，类似collect， collectAsMap这样的调用，
                # 偶尔会出现could not open socket的异常
                #
                # 具体表现如下：
                # 当我们在python里面调用collect，或者collectAsMap的时候，会触发数据从spark的driver侧传输到python侧
                # scala里面的具体做法是这样的：
                # 1. spark driver侧打开一个本地端口的监听，代码在PythonRDD.scala的serveIterator
                # 2. 然后python侧用socket去链接本地的端口，通过socket将数据传输到python
                #
                # 出故障的表现是这样：
                # 在第一步的时候，监听设置了3秒超时，但是python侧没有在3秒中链接过去，导致报错
                #
                # 没有查到根原因，暂时retry一下，观察不影响后续任务执行
                #
                # added 2018-7-31 via @sx: 去掉retry， retry会导致spark任务卡主， 原因未知
                #
                if "could not open socket" in err_msg:
                    if retry_times < 1:
                        print("current retry time: {0}, less then 1, retry once".format(str(retry_times)))
                        retry_times += 1
                        should_retry = False  #暂时去掉， 我还会回来的，(╯‵□′)╯︵┻━┻
                    else:
                        print("current retry time: {0}, more then 1, do not retry!".format(str(retry_times)))
                        retry_times = 0
                        should_retry = False
                else:
                    print("not socket error!, (╯‵□′)╯︵┻━┻")
                    retry_times = 0
                    should_retry = False

            finally:
                context_cache.clear()

                # pyspark里面的RDD的GC必须是要python侧的RDD对象也被释放掉， 否则，spark中的rdd始终被python侧reference
                # 导致spark中无法GC
                import gc
                gc.collect()

        return True