from pyspark import RDD
from pyspark.sql import DataFrame

from typing import Union, Iterator, Callable, Tuple

from process.buz.retailer_statistics.calculator import PrtSalesCalculator
from process.buz.retailer_statistics.helper import BroadcastJoin
from process.buz.retailer_statistics.loader import ParallelingPrtSaleLoader, ParallelingSCGLoader
from process.util.data_context import DataContext
from process.util.data_transfer import RetailerDatabaseContext


class PrtSaleToFctOrders(object):
    def __init__(self,
                 data_context: DataContext,
                 dim_date_id: int,
                 retailer_database_contexts: Iterator[RetailerDatabaseContext]) -> None:
        super(PrtSaleToFctOrders, self).__init__()
        self.data_context = data_context
        self.dim_date_id = dim_date_id
        self.retailer_database_contexts = retailer_database_contexts

    def execute(self):
        prt_sales = ParallelingPrtSaleLoader(
            data_context = self.data_context,
            retailer_database_contexts = self.retailer_database_contexts,
            dim_date_id = self.dim_date_id
        ).load()
        # prt_sales.foreach(print)

        scgs = ParallelingSCGLoader(
            data_context = self.data_context,
            retailer_database_contexts = self.retailer_database_contexts
        ).load().map(PrtSalesCalculator.scg_to_org_code_user_no_kv)
        # scgs.foreach(print)

        prt_sales = prt_sales.map(PrtSalesCalculator.to_org_code_user_no_kv)
        prt_sales = BroadcastJoin.join(prt_sales, scgs.collectAsMap())