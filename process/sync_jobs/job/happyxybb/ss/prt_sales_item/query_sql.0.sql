SELECT
a.id  order_no,
date_format(a.sales_date,'%Y%m%d') date_id,
a.sales_date  sales_time,
a.salesdepart_id  shop_code,
a.member_id member_code,
c.no  member_no,
b.goods_id  product_code,
b.barcode product_barcode,
b.deal_price/10000 price,
b.deal_price/10000*b.sales_number  amount,
b.sales_number  quantity,
b.good_profit/10000 profit,
a.guide_id guider_code
FROM
goods_sales AS a
inner join goods_sales_detail b on a.id=b.sales_id
left join ms_member_def c on a.member_id=c.id
where  a.sales_status='1' and a.del_flag<>1
and  a.sales_date>=curdate()