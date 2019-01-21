from process.spark.mysql_data_loader import MysqlDataLoader


class DimShopDetailLoader(MysqlDataLoader):
    def load(self):
        dim_shop_sql = """
            SELECT ds.dim_shop_id, TRIM(ds.shop_code) AS dim_shop_code, ds.shop_name AS dim_shop_name
            FROM {0}.dim_shop ds WHERE ds.online = 1
               """.format(self.retailer_context.dw_name)
        return self.retailer_data_frame(sql=dim_shop_sql)

