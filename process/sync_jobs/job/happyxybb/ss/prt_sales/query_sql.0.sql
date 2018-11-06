select a.id order_no,
date_format(a.sales_date,'%Y%m%d') date_id,
a.sales_date  sales_time,
a.salesdepart_id  shop_code,
a.member_id  member_code,
c.no  member_no,
a.real_pay/10000 real_amount,
a.due_pay/10000 due_amount,
a.guide_id  guider_code
from goods_sales AS a
left join ms_member_def c on a.member_id=c.id
where  a.sales_status='1' and a.del_flag<>1
and  a.sales_date>=curdate()