from typing import Iterator

from process.util.data_context import DataContext
from process.util.data_transfer import ParallelingRetailerDataLoader, RetailerDatabaseContext


class ParallelingPrtSaleLoader(ParallelingRetailerDataLoader):
    def __init__(self,
                 data_context: DataContext,
                 dim_date_id: int,
                 retailer_database_contexts: Iterator[RetailerDatabaseContext]) -> None:
        sql_template = """
        SELECT CAST(SUBSTRING(`ps`.`date_id`, 5, 2) AS UNSIGNED) as m_id,
               1 as static_code,
               trim(ps.order_no) AS order_no,
               ps.sales_time,
               ps.date_id        AS dim_date_id,
               ps.member_code,
               ps.member_no,
               ps.shop_code,
               trim(ps.guider_code) as sale_guider_code,
               # scg.id            AS guider_id,
               # scg.`name`        AS guider_name,
               # scg.user_no,
               ps.real_amount,
               ps.due_amount,
               ps.discount_amount,
               ps.nrp_amount
          FROM {{retailer_database_context.dw_name}}.prt_sales ps
        """
        super(ParallelingPrtSaleLoader, self).__init__(data_context=data_context,
                                                       retailer_database_contexts=retailer_database_contexts,
                                                       sql_template=sql_template,
                                                       additional_template_args={"dim_date_id": dim_date_id})


class ParallelingSCGLoader(ParallelingRetailerDataLoader):
    def __init__(self,
                 data_context: DataContext,
                 retailer_database_contexts: Iterator[RetailerDatabaseContext]) -> None:
        sql_template = """
        SELECT sg.id, TRIM(sg.user_no) AS user_no, sg.name, n.code AS shop_code, sg.org_code AS org_code
          FROM pomelo_backend_production.shopping_consultant_guides sg
        LEFT JOIN ris_production.nodes n ON n.id = sg.shop_id
         WHERE sg.org_code='{{retailer_database_context.org_code}}' 
           AND sg.user_no != '' 
           AND sg.is_deleted=0 
           AND sg.is_dimission=0
        """
        super(ParallelingSCGLoader, self).__init__(data_context=data_context,
                                                   retailer_database_contexts=retailer_database_contexts,
                                                   tag="retailer",
                                                   sql_template=sql_template)