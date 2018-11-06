select a.id order_no,
a.real_pay/10000 real_amount
from goods_sales  a
where a.sales_status='1' and a.del_flag<>1
and a.sales_date>= date_sub(curdate(),interval 7 day) and a.sales_date<curdate()
