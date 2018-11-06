DROP PROCEDURE IF EXISTS `r02_shop_day`;
CREATE PROCEDURE `r02_shop_day`()
BEGIN
 SET @end_d  = DATE_FORMAT(start_date,'%Y%m%d');
 SET @end_t = (select dim_date_id  from Dim_Date_Day a Inner join 
                   (select date_yearweek_code-100 as date_yearweek_code, date_weekday_code from Dim_Date_Day where dim_date_id=@end_d) b 
                   on a.date_yearweek_code=b.date_yearweek_code and a.date_weekday_code=b.date_weekday_code);
 

Select x.time_type, x.type_name, x.shop_count,
	ifnull(n_sum_amount,0) as n_sum_amount, 
	ifnull(t_sum_amount,0) as t_sum_amount, 
	ifnull((n_sum_amount-t_sum_amount)/ABS(t_sum_amount),0) as amount_z,
  ifnull(n_sum_quantity,0) as n_sum_quantity, 
	ifnull(t_sum_quantity,0) as t_sum_quantity, 
	ifnull((n_sum_quantity-t_sum_quantity)/ABS(t_sum_quantity),0) as quantity_z,
	ifnull(n_order_num,0) as n_order_num, 
	ifnull(t_order_num,0) as t_order_num,
	ifnull((n_order_num-t_order_num)/ABS(t_order_num),0) as order_z, 

	ifnull(n_sum_amount/ABS(n_order_num),0) as n_order_price,
	ifnull(t_sum_amount/(t_order_num),0) as t_order_price,
	ifnull(((n_sum_amount/ABS(n_order_num))-(t_sum_amount/ABS(t_order_num)))/ABS((t_sum_amount/t_order_num)),0) as order_price_z 
from(
	select '日' as time_type,
			d.shopii_name as type_name,
			count(distinct d.shop_code) as shop_count,
			sum(amount) as n_sum_amount,
			sum(quantity) as n_sum_quantity, 
			sum(count_order) as n_order_num
		from sum_day_sales_shop as s,  dim_shop d
		where s.dim_shop_id = d.dim_shop_id
			and s.dim_date_id =  @end_d
			and d.shopii_name is not null
		group by d.shopii_name
	)as x  left join
(
		select '日' as time_type,
			d.shopii_name as type_name,
			sum(amount) as t_sum_amount, 
      sum(quantity) as t_sum_quantity, 
			sum(count_order) as t_order_num
		from sum_day_sales_shop as s,  dim_shop d
		where s.dim_shop_id = d.dim_shop_id
			and s.dim_date_id =  @end_t
			and d.shopii_name is not null
		group by d.shopii_name
)as y
on x.type_name = y.type_name;

END