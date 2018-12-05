select
	ohl.order_no order_no,
	DATE_FORMAT(ohl.trade_date,"%Y%m%d") date_id,
	ooil.created_at sales_time,
	ooil.shop shop_code,
	ohl.member_no member_code,
	ohl.member_no,
	ooil.sku product_code,
	ooil.sku product_barcode,
	ooil.price price,
	ooil.total amount,
	ooil.num quantity,
	0 profit,
	ohl.guide guider_code
from order_order_item_logs ooil
inner join order_history_logs as ohl
on ooil.history_log_id=ohl.id
where ohl.org_code='mishiyy'
	AND ohl.trade_date = CURDATE()