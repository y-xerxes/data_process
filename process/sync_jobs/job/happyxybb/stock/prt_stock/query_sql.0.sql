SELECT
date_format(curdate(),'%Y%m%d')  dim_date_id,
rtrim(a.goods_id) product_code,
rtrim(a.department_id) shop_code,
a.number stock_qty ,
now()  updated_at
FROM
inv_goods_stock AS a