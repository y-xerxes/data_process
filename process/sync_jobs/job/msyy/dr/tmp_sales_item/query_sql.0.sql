select
	ohl.order_no,
	ooil.id item_seq,
	ooil.created_at sales_time,
	ooil.shop shop_code,
	ohl.member_no,
	ooil.sku product_code,
	ooil.price sales_price,
	ooil.price fact_price,
	ooil.total amount,
	ooil.num quantity,
	0 profit,
	ohl.guide guider_code
from order_order_item_logs ooil
inner join order_history_logs as ohl
on ooil.history_log_id=ohl.id
where ohl.org_code='msyy'
	AND ohl.trade_date > DATE_SUB(CURDATE(), INTERVAL 7 DAY)
	AND ohl.trade_date < CURDATE()