"""
提供了每日缓存的能力
"""
import datetime
from typing import Union, List, Dict

from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame



class DailyCache(object):
    """
    在很多场景下面, 需要历史数据来参与计算,
    尤其是在实时模式下, 需要频繁获取这些数据

    为了避免多次从数据库中获取数据, 将这些数据通过spark的cache机制缓存到内存中

    通常的缓存清理是按天清理
    """
    @staticmethod
    def today():
        return int(datetime.datetime.now().strftime("%Y%m%d"))

    def __init__(self, data_context):
        super(DailyCache, self).__init__()
        self.dim_date_id = DailyCache.today()
        self.data_context = data_context
        self.cached_rd = {}
        self.data_fetchers = {}


class ContextCache(object):
    def __init__(self):
        super(ContextCache, self).__init__()
        self.caches = []

    def cache(self, data: Union[RDD, DataFrame, None]) -> Union[RDD, DataFrame, None]:
        if data is not None:
            data = data.cache()
            self.caches.append(data)
        return data

    def clear(self):
        for cache in self.caches:
            cache.unpersist()
        self.caches.clear()