"""
提供了每日缓存的能力
"""
import datetime
from typing import Union, List, Dict

from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame



class DataFetcher:
    fetcher_name = "must defined in subclass"

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def fetch(self,
              dim_date_id: int,
              name: str,
              retailer_context: JwRetailerContext,
              daily_cache: 'DailyCache') -> Union[RDD, DataFrame, None]:
        return None


class DailyCache(object):
    """
    在很多场景下面, 需要历史数据来参与计算,
    尤其是在实时模式下, 需要频繁获取这些数据

    为了避免多次从数据库中获取数据, 将这些数据通过spark的cache机制缓存到内存中

    通常的缓存清理是按天清理
    """
    @staticmethod
    def today():
        # type: () -> int
        return int(datetime.datetime.now().strftime("%Y%m%d"))
        # return 20180215

    def __init__(self, jw_context: JwContext) -> None:
        super(DailyCache, self).__init__()
        self.dim_date_id = DailyCache.today()
        self.jw_context = jw_context
        self.cached_rdd = {}  # type: Dict[str, Union[RDD, DataFrame]]
        self.data_fetchers = {}  # type: Dict[str, DataFetcher]

    def contain(self, name):
        return name in self.data_fetchers.keys()

    def register(self, rdd_fetcher: DataFetcher, name: str=None) -> 'DailyCache':
        if name is None:
            name = rdd_fetcher.fetcher_name

        if name not in self.data_fetchers.keys():
            self.data_fetchers[name] = rdd_fetcher

        return self

    def get(self, name, retailer_context):
        # type: (str, JwRetailerContext) -> Union[RDD, DataFrame, None]
        org_code = retailer_context.org_code
        today = DailyCache.today()
        if today != self.dim_date_id:
            for rdd_or_df in self.cached_rdd.values():  # type: Union[RDD, DataFrame]
                rdd_or_df.unpersist()
            self.cached_rdd.clear()
            self.dim_date_id = today

        key_name = "{0}_{1}".format(org_code, name)
        if key_name in self.cached_rdd.keys():
            print("CacheSupport get cached rdd: {}".format(key_name))
            return self.cached_rdd[key_name]
        else:
            print("CacheSupport failed get cached rdd: {}".format(key_name))
            if name in self.data_fetchers.keys():
                fetcher = self.data_fetchers[name]  # type: DataFetcher
                rdd_or_df = fetcher.fetch(
                    dim_date_id=today, name=name,
                    retailer_context=retailer_context, daily_cache=self)

                if isinstance(rdd_or_df, DataFrame):
                    saved_rdd = self.jw_context.spark.createDataFrame(rdd_or_df.collect())
                else:
                    sc = self.jw_context.spark.sparkContext  # type: SparkContext
                    rdd_or_df.cache()

                    def load_rdd_data(target_rdd: RDD):
                        try_times = 0
                        result = None
                        while try_times <= 3:
                            try:
                                result = target_rdd.collect()
                                try_times = 4
                            except Exception as e:
                                print("load rdd {0}, error: {1}".format(key_name, str(e)))
                                try_times += 1

                        if result is None:
                            raise Exception("load rdd failed: {0}".format(key_name))
                        return result

                    saved_rdd = sc.parallelize(load_rdd_data(rdd_or_df), rdd_or_df.getNumPartitions())
                    rdd_or_df.unpersist()
                    saved_rdd.setName(key_name)

                saved_rdd.cache()
                self.cached_rdd[key_name] = saved_rdd
                return saved_rdd
            else:
                return None


class ContextCache(object):
    def __init__(self):
        super(ContextCache, self).__init__()
        self.caches = []  # type: List[Union[RDD, DataFrame]]

    def cache(self, data: Union[RDD, DataFrame, None]) -> Union[RDD, DataFrame, None]:
        if data is not None:
            data = data.cache()
            self.caches.append(data)
        return data

    def clear(self):
        for cache in self.caches:
            cache.unpersist()
        self.caches.clear()
