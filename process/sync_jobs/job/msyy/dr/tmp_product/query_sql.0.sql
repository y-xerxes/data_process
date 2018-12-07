select 
	product_code, 
	barcode,
	product_name,
	categoryiii_code,
	categoryii_code,
	categoryiii_name,
	categoryii_name,
	categoryi_code,
	categoryi_name,
	sales_price,
	member_price,
	product_state,
	created_at pos_created_at
from product_info 
	where created_at>DATE_SUB(now(),INTERVAL 7 day)
	or updated_at>DATE_SUB(now(),INTERVAL 7 day)