"""
从DatabaseAgent的config字段类似的配置, 构建一个从数据库读取数据的rdd
"""
from typing import Any, List

import datetime
from pyspark import RDD, Row, SparkContext
from pyspark.sql import SparkSession, DataFrame

from joowing.retailer.spark.schemas.fct_order_items import fct_order_items_tmp_schema
from joowing.share.spark.spark_util import SparkDateUtil
from pyspark.sql import functions as F

from joowing.share.util import name_with_prefix


class DatabaseRddGenerator(object):
    def __init__(self, config, spark):
        # type: (dict, SparkSession) -> None
        self.config = config  # type: dict
        self.spark = spark  # type: SparkSession

    def shop_mom_and_yoy(self, org_code, begin_month_id):
        sql = """
        select 
            rsss.dim_shop_code as may_dim_shop_code,
            rsss.dim_month_id as may_dim_month_id,
            rsss.real_amount_hb, 
            rsss.real_amount_tb,
            rsss.real_amount_mom,
            rsss.real_amount_yoy,
            rsss.amount_hb, 
            rsss.amount_tb,
            rsss.amount_mom,
            rsss.amount_yoy,
            rsss.nrp_amount_hb, 
            rsss.nrp_amount_tb,
            rsss.nrp_amount_mom,
            rsss.nrp_amount_yoy
        from `retailer_statistics`.`retailer_shop_sales_statistics` rsss
        inner join 
            (
                select 
                    org_code,
                    dim_month_id, 
                    dim_shop_code, 
                    max(dim_date_id) as max_dim_date_id
                from `retailer_statistics`.`retailer_shop_sales_statistics` s1
                where s1.dim_month_id >= {0} and s1.org_code = '{1}'
                group by dim_month_id, dim_shop_code
            ) as s2
        on rsss.org_code = s2.org_code 
        and rsss.dim_shop_code = s2.dim_shop_code 
        and rsss.dim_date_id = s2.max_dim_date_id
        where rsss.org_code = '{1}'
        """.format(begin_month_id, org_code)
        return self._data_frame("real_time", sql=sql)

    def retailer_goal(self, org_code, begin_month_id, end_month_id):
        sql = """
        select png.org_code as goal_org_code, 
               png.shop_code as goal_shop_code,
               png.dim_month as goal_dim_month, 
               newcustomer_goal as new_customer_goal, 
               psg.sale_goal as sales_goal, 
               nm.sales_flag
        from data_service.panel_newcustomer_goal  png
          left join (select org_code, sale_goal, shop_code, dim_month
                       from data_service.panel_sale_goal
                      where org_code = "{0}") as psg
               on psg.org_code = png.org_code
              and psg.shop_code = png.shop_code
              AND png.dim_month = psg.dim_month
          left join (select org_code, sales_flag
                  from data_service.organization_for_NM
                 where org_code = "{0}") as nm
            on png.org_code = nm.org_code  
        where png.org_code = "{0}" and png.dim_month >= {1} and png.dim_month <= {2}
        group by goal_shop_code, goal_dim_month
        """.format(org_code, str(begin_month_id), str(end_month_id))
        return self._data_frame("non_real_time", sql=sql)

    def retailer_create_member_count(self, dw_name, begin_date, end_date):
        sql = """
        SELECT dy.reg_shop_code,
               dy.create_date,
               dy.create_member_num + IF(create_member_num1 IS NULL, 0, create_member_num1) as create_member_num
          FROM (select dm.reg_shop_code,
                       CAST(DATE_FORMAT(dm.create_date, '%Y%m%d') AS DECIMAL) as create_date,
                       count(1) as create_member_num,
                       dx.create_member_num1
                  from {0}.dim_member dm
                  LEFT JOIN (select rgst_shop_code, count(1) as create_member_num1
                               from {0}.prt_member
                              group by rgst_shop_code) dx
                    ON dm.reg_shop_code = dx.rgst_shop_code
                 where create_date >= '{1}'
                   and create_date <= '{2}'
                 group by dm.reg_shop_code, dm.create_date) dy
         GROUP BY dy.reg_shop_code, dy.create_date
        """.format(dw_name, begin_date, end_date)
        return self._data_frame("retailer", sql=sql)

    def retailer_first_shopping_member_count(self, dw_name, begin_date, end_date):
        sql = """
        SELECT dy.reg_shop_code,
               dy.first_shoping,
               dy.first_shoping_member_num + IF(first_shoping_member_num1 IS NULL,0,first_shoping_member_num1) as  first_shoping_member_num
          FROM (select dm.reg_shop_code,
                       CAST(DATE_FORMAT(dm.first_shoping, '%Y%m%d') AS DECIMAL) as first_shoping,
                       count(1) as first_shoping_member_num,
                       dx.first_shoping_member_num1
                  from {0}.dim_member dm
                  LEFT JOIN (select rgst_shop_code,
                                    count(1) as first_shoping_member_num1
                               from {0}.prt_member
                              group by rgst_shop_code) dx
                    ON dm.reg_shop_code = dx.rgst_shop_code
                 where dm.first_shoping >= '{1}'
                   and dm.first_shoping <= '{2}'
                 group by dm.reg_shop_code, dm.first_shoping) dy
         GROUP BY dy.reg_shop_code, dy.first_shoping
        """.format(dw_name, begin_date, end_date)
        return self._data_frame("retailer", sql=sql)

    def retailer_first_shopping_member_count_m(self, dw_name, begin_date, end_date):
        sql = """
        SELECT dy.reg_shop_code,
               dy.first_shoping_month,
               dy.first_shoping_member_num + IF(first_shoping_member_num1 IS NULL, 0, first_shoping_member_num1) as first_shoping_member_num
          FROM (select dm.reg_shop_code,
                       CAST(DATE_FORMAT(dm.first_shoping, '%Y%m') AS DECIMAL) as first_shoping_month,
                       count(1) as first_shoping_member_num,
         first_shoping_member_num1
                  from {0}.dim_member dm
                  LEFT JOIN (select rgst_shop_code,
                                   count(1) as first_shoping_member_num1
                              from {0}.prt_member
                             group by rgst_shop_code) dx
                    ON dm.reg_shop_code = dx.rgst_shop_code
                 where first_shoping >= '{1}'
                   and first_shoping <= '{2}'
                 group by reg_shop_code, first_shoping_month) dy
         GROUP BY dy.reg_shop_code, dy.first_shoping_month
        """.format(dw_name, begin_date, end_date)
        return self._data_frame("retailer", sql=sql)

    def retailer_create_member_count_m(self, dw_name, begin_date, end_date):
        sql = """
        SELECT dy.reg_shop_code,
               dy.create_date_month,
               dy.q + IF(q1 IS NULL, 0, q1) as create_member_num
          FROM (select dm.reg_shop_code,
                       CAST(DATE_FORMAT(dm.create_date, '%Y%m') AS DECIMAL) as create_date_month,
                       count(1) as q,
                                     dx.q1
                  from {0}.dim_member dm
                  LEFT JOIN (select rgst_shop_code, count(1) as q1
                              from {0}.prt_member
                             group by rgst_shop_code) dx
                    ON dm.reg_shop_code = dx.rgst_shop_code
                 where create_date >= '{1}'
                   and create_date <= '{2}'
                 group by dm.reg_shop_code, create_date_month) dy
         GROUP BY dy.reg_shop_code, dy.create_date_month
        """.format(dw_name, begin_date, end_date)
        return self._data_frame("retailer", sql=sql)

    def retailer_dim_shop_codes(self, dw_name):
        sql = """
select distinct shop_code as shop_code from {0}.dim_shop shop where shop.online = 1
        """.format(dw_name)
        return self._data_frame("retailer", sql=sql).rdd.map(lambda row: row['shop_code']).collect()

    def retailer_shopping_consultant_guides_data_frame(self, org_code):
        sql = """
SELECT * from (
      select sg.id, TRIM(sg.user_no) as user_no, sg.name, sg.shop_code 
      from pomelo_backend_production.shopping_consultant_guides sg
      where sg.org_code='{0}' and sg.user_no != '' AND sg.is_deleted=0 AND sg.is_dimission=0
) A GROUP BY user_no
        """.format(org_code)
        return self._data_frame("buz", sql=sql)

    def real_time_table(self, org_code, database_name, table_name):
        sql = """
select * from {0}.{1} where org_code = '{2}'
        """.format(database_name, table_name, org_code)
        return self._data_frame("real_time", sql=sql)

    def buz_shops_data_frame(self, org_code):
        shop_query_sql = """
        SELECT
           `n`.`id` AS `shop_id`,
           `n`.`code` AS `shop_code`,
           `n`.`nickname` AS `shop_name`,
           `r`.`code` AS `org_code`,
           `r`.`id` AS `org_id`,
           `r`.`name` AS `org_name`,
           `n`.`floor_size` AS `floor_size`
        FROM `ris_production`.`nodes` n
        join `ris_production`.`global_retailers` r 
          on `n`.`retailer_id` = `r`.`id`
        where `n`.`type` = 'Shop'
          and `n`.`shop_type` = 'offline'
          and `n`.`buz_state` = 'opened'
          and `r`.`code` = '{0}'       
                """.format(org_code)
        return self._data_frame("buz", sql=shop_query_sql)

    def retailer_sales_statistics(self, dim_date_id, org_code):
        sql = """
        select 
            org_code,
            dim_year_id,
            dim_mont_id,
            dim_date_id,
            quantity,
            profit,
            nrp_amount,
            rb_amount,
            amount,
            discount_amount,
            real_amount
        from `retailer_statistics`.`retailer_sales_statistics`
        where dim_date_id > {0} and org_code = '{1}'
        """.format(str(dim_date_id), org_code)
        return self._data_frame("real_time", sql=sql).rdd

    def prt_sales_to_fct_orders_data_frame(self, dw_name, dim_date_id):
        prt_sales_load_sql = """
        SELECT trim(ps.order_no) AS order_no,
               ps.sales_time,
               ps.date_id        AS dim_date_id,
               ps.member_code,
               ps.member_no,
               ps.shop_code,
               ps.guider_code,
               # scg.id            AS guider_id,
               # scg.`name`        AS guider_name,
               # scg.user_no,
               ps.real_amount,
               ps.due_amount,
               ps.discount_amount,
               ps.nrp_amount
          FROM {0}.prt_sales ps
         # INNER JOIN remote_database.shopping_consultant_guides scg
         #    ON scg.user_no = ps.guider_code
         where ps.date_id = {1}
        """.format(dw_name, dim_date_id)

        return self._data_frame("retailer", sql=prt_sales_load_sql), [dim_date_id]

    def fct_billing_to_fct_orders_data_frame(self, dw_name, min_date_id=None, max_date_id=None):
        fct_billing_load_sql = """
        SELECT fb.id ,
               SUBSTRING(`fb`.`dim_date_id`, 5, 2) as m_id,
               fb.order_no,
               fb.sales_time,
               fb.dim_date_id,
               fb.member_code,
               fb.member_no,
               fb.shop_code,
               # scg.id                             AS guider_id,
               # scg.`name`                         AS guider_name,
               # scg.user_no,
               fb.guider_code,
               fb.real_amount,
               fb.due_amount,
               fb.discount_amount,
               fb.nrp_amount
          FROM {0}.fct_billing fb
         # INNER JOIN remote_database.shopping_consultant_guides scg
         #    ON fb.guider_code = scg.user_no
        """.format(dw_name)

        query = []
        if min_date_id is not None:
            query.append("fb.dim_date_id >= {0}".format(str(min_date_id)))

        if max_date_id is not None:
            query.append("fb.dim_date_id <= {0}".format(str(max_date_id)))

        if len(query) > 0:
            fct_billing_load_sql = fct_billing_load_sql + """
  WHERE {0}
            """.format(" AND ".join(query))

        partition_column = None
        lower_bound = None
        upper_bound = None
        num_partitions = None
        if len(query) == 0:
            lower_bound = 1
            upper_bound = 12
            partition_column = "m_id"
            num_partitions = 12

        dim_date_query = ""
        if len(query) > 0:
            dim_date_query = "WHERE {0}".format(" AND ".join(query))

        dim_date_id_rows = self.retailer_data_frame(sql="""
        select distinct(dim_date_id) as dim_date_id from {0}.fct_billing fb {1}
                """.format(dw_name, dim_date_query)).rdd.collect()

        dim_dated_ids = [x["dim_date_id"] for x in dim_date_id_rows]

        return self._data_frame("retailer", sql=fct_billing_load_sql, lowerBound=lower_bound,
                                upperBound=upper_bound, partitionColumn=partition_column,
                                numPartitions=num_partitions), dim_dated_ids

    def prt_sales_item_to_fct_order_items_data_frame(self, dw_name, org_code, dim_date_id):
        prt_sales_item_load_sql = """
select ps.date_id     AS dim_date_id,
       ds.dim_shop_id,
       ps.shop_code as dim_shop_code,
       ps.sales_time,
       ps.order_no,
       ps.member_code,
       ps.amount,
       ps.profit,
       ps.quantity,
       ps.guider_code,
       ps.price as fact_price,
       dp.dim_product_id,
       dp.std_categoryi_code,
       pci.dim_category_id   as dim_categoryi_id,
       pcii.dim_category_id  as dim_categoryii_id,
       pciii.dim_category_id as dim_categoryiii_id,
       pbrand.dim_brand_id   as dim_brand_id,
       ps.product_code
  FROM {0}.prt_sales_item as ps
  INNER JOIN (select *
               from {0}.dim_product
              where product_code in
                    (select DISTINCT (product_code) from {0}.prt_sales_item)) as dp
    on dp.product_code = ps.product_code
  INNER join {0}.dim_product_category AS pci
    on dp.categoryi_code = pci.category_code
   and pci.category_type = 1
  INNER join {0}.dim_product_category AS pcii
    on dp.categoryii_code = pcii.category_code
   and pcii.category_type = 2
  INNER join {0}.dim_product_category AS pciii
    on dp.categoryiii_code = pciii.category_code
   and pciii.category_type = 3
  INNER join {0}.dim_product_brand pbrand
    on dp.brand_code = pbrand.brand_code
  INNER JOIN (select *
               from {0}.dim_member
              where member_no in
                    (select DISTINCT (member_no) from {0}.prt_sales_item)) as dm
    on dm.member_no = ps.member_no  
  INNER JOIN {0}.dim_shop ds
    ON ps.shop_code = ds.shop_code
  WHERE ps.date_id = {1}
        """.format(dw_name, str(dim_date_id))

        shops_df = self.buz_shops_data_frame(org_code)
        shopping_consultant_guides = self.retailer_shopping_consultant_guides_data_frame(org_code)
        shops_df = F.broadcast(shops_df.select("floor_size", "shop_code"))
        shopping_consultant_guides = shopping_consultant_guides.select("user_no", "id", "name", "shop_code")\
            .withColumnRenamed("id", "guider_id").withColumnRenamed("name", "guider_name")\
            .withColumnRenamed("shop_id", "guider_shop_id").withColumnRenamed("shop_code", "guider_shop_code")

        def process_fct_sales_row(org_code, batch):
            result_collect = []
            for row in batch:
                # type: (str, Row) -> dict
                rb_amount = 0.0
                if row["quantity"] < 0:
                    rb_amount = -row["amount"]

                data = row.asDict()
                data["org_code"] = org_code
                data["rb_amount"] = rb_amount
                data["dim_month_id"] = SparkDateUtil.dim_date_id_to_dim_month_id(data["dim_date_id"])
                data["dim_year_id"] = SparkDateUtil.dim_date_id_to_dim_year_id(data["dim_date_id"])
                data["quantity"] = int(data["quantity"])
                data["order_no"] = data["order_no"].strip()

                result_collect.append(data)

            return result_collect

        sales_rdd = self._data_frame(database_type="retailer", sql=prt_sales_item_load_sql)\
            .rdd.mapPartitions(lambda batch_data: process_fct_sales_row(org_code, batch_data))

        sales_df = self.spark.createDataFrame(sales_rdd, fct_order_items_tmp_schema)
        sales_df = sales_df.join(shops_df, shops_df.shop_code == sales_df.dim_shop_code, "left")
        sales_df = sales_df.join(shopping_consultant_guides,
                                 shopping_consultant_guides.user_no == sales_df.guider_code, "left")
        sales_df = sales_df.drop("m_id")\
            .coalesce(1).repartition("org_code", "dim_date_id")
        return sales_df, [dim_date_id]

    def fct_sales_to_fct_order_items_data_frame(self, dw_name, org_code, min_date_id=None, max_date_id=None):
        fct_sales_load_sql = """
SELECT  
        SUBSTRING(`fsr`.`dim_date_id`, 5, 2) as m_id, 
        fsr.dim_date_id,
        fsr.dim_shop_id,
        fsr.shop_code   AS dim_shop_code,
        fsr.sales_time,
        fsr.order_no,
        dm.member_code,
        fsr.amount,
        fsr.profit,
        -- SUM(CASE WHEN ps.quantity < 0 THEN -ps.amount ELSE 0 END) AS rb_amount,
        fsr.quantity,
        fsr.fact_price,
        fsr.guider_code,
        fsr.std_categoryi_code,
        fsr.dim_categoryi_id,
        fsr.dim_categoryii_id,
        fsr.dim_categoryiii_id,
        fsr.dim_product_id,
        fsr.dim_brand_id,
        dp.product_code
   FROM (SELECT fs.dim_date_id,
                fs.item_id,
                ds.shop_code,
                fs.order_no,
                fs.sales_time,
                fs.amount,
                fs.profit,
                fs.quantity,
                fs.fact_price,
                fs.std_categoryi_code,
                fs.dim_categoryi_id,
                fs.dim_categoryii_id,
                fs.dim_categoryiii_id,
                fs.dim_product_id,
                fs.dim_brand_id,
                fs.dim_shop_id,
                fs.dim_member_id,
                fs.guider_code
           FROM {0}.fct_sales fs
           LEFT JOIN {0}.dim_shop ds
             ON fs.dim_shop_id = ds.dim_shop_id) fsr
   INNER JOIN {0}.dim_member dm
     ON fsr.dim_member_id = dm.dim_member_id
   INNER JOIN {0}.dim_product dp
     ON fsr.dim_product_id = dp.dim_product_id
        """.format(dw_name)

        shops_df = self.buz_shops_data_frame(org_code)
        shopping_consultant_guides = self.retailer_shopping_consultant_guides_data_frame(org_code)

        query = []
        if min_date_id is not None:
            query.append("fsr.dim_date_id >= {0}".format(str(min_date_id)))

        if max_date_id is not None:
            query.append("fsr.dim_date_id <= {0}".format(str(max_date_id)))

        if len(query) > 0:
            fct_sales_load_sql = fct_sales_load_sql + """
          WHERE {0}
                    """.format(" AND ".join(query))

        shops_df = F.broadcast(shops_df.select("floor_size", "shop_code"))
        shopping_consultant_guides = shopping_consultant_guides.select("user_no", "id", "name", "shop_code") \
            .withColumnRenamed("id", "guider_id").withColumnRenamed("name", "guider_name") \
            .withColumnRenamed("shop_id", "guider_shop_id").withColumnRenamed("shop_code", "guider_shop_code")

        partition_column = None
        lower_bound = None
        upper_bound = None
        num_partitions = None

        if len(query) == 0:
            lower_bound = 1
            upper_bound = 13
            partition_column = "m_id"
            num_partitions = 12

        dim_date_query = ""
        if len(query) > 0:
            dim_date_query = "WHERE {0}".format(" AND ".join(query))

        dim_date_id_rows = self.retailer_data_frame(sql="""
select distinct(dim_date_id) as dim_date_id from {0}.fct_sales fsr {1}
        """.format(dw_name, dim_date_query)).rdd.collect()

        dim_dated_ids = [x["dim_date_id"] for x in dim_date_id_rows]

        def process_fct_sales_row(org_code, batch):
            result_collect = []
            for row in batch:
                # type: (str, Row) -> dict
                rb_amount = 0.0
                if row["quantity"] < 0:
                    rb_amount = -row["amount"]

                data = row.asDict()
                data["org_code"] = org_code
                data["rb_amount"] = rb_amount
                data["dim_month_id"] = SparkDateUtil.dim_date_id_to_dim_month_id(data["dim_date_id"])
                data["dim_year_id"] = SparkDateUtil.dim_date_id_to_dim_year_id(data["dim_date_id"])
                data["quantity"] = int(data["quantity"])

                result_collect.append(data)

            return result_collect

        sales_rdd = self._data_frame(database_type="retailer",
                                     sql=fct_sales_load_sql, lowerBound=lower_bound,
                                     upperBound=upper_bound, partitionColumn=partition_column,
                                     numPartitions=num_partitions)\
            .rdd.mapPartitions(lambda batch_data: process_fct_sales_row(org_code, batch_data))

        sales_df = self.spark.createDataFrame(sales_rdd, fct_order_items_tmp_schema)
        sales_df = sales_df.join(shops_df, shops_df.shop_code == sales_df.dim_shop_code, "left")
        sales_df = sales_df.join(shopping_consultant_guides,
                                 shopping_consultant_guides.user_no == sales_df.guider_code, "left")
        sales_df = sales_df.drop("m_id")\
            .coalesce(1).repartition("org_code", "dim_date_id")

        # print("=" * 160)
        # print(sales_df.take(1))
        return sales_df, dim_dated_ids

    def retailer_data_frame(self, database_name=None, table_name=None, sql=None):
        # type: (str, str, str) -> DataFrame
        return self._data_frame("retailer", database_name=database_name, table_name=table_name, sql=sql)

    def retailer_rdd(self, database_name=None, table_name=None, sql=None):
        # type: (str, str, str) -> RDD
        return self.retailer_data_frame(database_name=database_name, table_name=table_name, sql=sql).rdd

    def _data_frame(self, database_type, database_name=None, table_name=None,
                    sql=None, partitionColumn=None, lowerBound=None, upperBound=None, numPartitions=None):
        datatabase_config = self.config[database_type]
        jdbc_url = "jdbc:mysql://{0}:{1}".format(datatabase_config["host"], datatabase_config["port"])
        user_name = datatabase_config["username"]
        password = datatabase_config["password"]

        frame = self.spark.read.format("jdbc")\
            .option("url", jdbc_url)\
            .option("user", user_name).option("password", password)

        if partitionColumn is not None:
            frame = frame.option("partitionColumn", partitionColumn)\
                .option("lowerBound", lowerBound).option("upperBound", upperBound)\
                .option("numPartitions", numPartitions)
        if sql is not None:
            print(sql)
            frame = frame.option("dbtable", "({0}) as tmp".format(sql))
        else:
            frame = frame.option("dbtable", "{0}.{1}".format(database_name, table_name))

        return frame.load()

    def _rdd(self, database_type, database_name, table_name, sql=None):
        return self._data_frame(database_type, database_name, table_name, sql).rdd



