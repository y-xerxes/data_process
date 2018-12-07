select
order_no,
DATE_FORMAT(trade_date,"%Y%m%d") date_id,
min(created_at) sales_time,
shop_code shop_code,
member_no member_code,
member_no member_no,
sum(pay_total) real_amount,
sum(pay_total) due_amount,
0 discount_amount,
guide guider_code
from order_history_logs  a
where org_code='msyy'
and trade_date = CURDATE()
group by order_no ,shop_code ,member_no ,guide