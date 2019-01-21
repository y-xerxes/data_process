from typing import Union

from pyspark import RDD
from pyspark.sql import DataFrame

from process.spark.context import RetailerContext
from process.util.cache_support import DataFetcher, DailyCache


class ShopDetailFetcher(DataFetcher):
    fetcher_name = "shop_detail"

    def fetch(self,
              dim_date_id: int,
              name: str,
              retailer_context: RetailerContext,
              daily_cache: 'DailyCache') -> Union[RDD, DataFrame, None]:
        shop_rdd = DimShopDetailLoader(retailer_context=retailer_context).load().rdd
        return ToOrgCodeListKV.calculate(shop_rdd, retailer_context.org_code)