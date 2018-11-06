SELECT
a.id order_no,
-- b.RowNo item_seq,
a.sales_date sales_time,
a.salesdepart_id shop_code,
a.member_id member_code,
b.goods_id product_code,
b.sales_price/10000 sales_price,
b.deal_price/10000 fact_price,
b.deal_price/10000*b.sales_number amount,
b.sales_number quantity,
b.good_profit/10000 profit,
a.guide_id guider_code
FROM
goods_sales AS a
inner join goods_sales_detail b on a.id=b.sales_id
where a.sales_status='1' and a.del_flag<>1
and  sales_date>= date_sub(curdate(),interval 7 day) and sales_date<curdate()